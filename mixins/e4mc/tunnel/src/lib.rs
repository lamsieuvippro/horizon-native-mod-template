//! e4mc tunnel client, ported from TheMade4/e4mc's `client` and `common`.
//!
//! Protocol (from e4mc's source):
//! * Connect a WebSocket to the relay (default `wss://ingress.e4mc.link`).
//! * Text frames from the relay are JSON, serde's externally tagged enum:
//!   `{"DomainAssigned":"x.e4mc.link"}`, `{"ChannelOpen":[7,"1.2.3.4:5"]}`,
//!   `{"ChannelClosed":7}`.
//! * Binary frames both ways: first byte is the channel id, the rest is the
//!   raw bytes of that player's TCP connection.
//! * On `ChannelOpen` the client connects to the local server and bridges it.
//! * The client may send the text frame `{"ChannelClosed":7}` (the common
//!   crate's `ServerboundControlMessage`) when the local side hangs up.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_tungstenite::tokio::connect_async;
use async_tungstenite::tungstenite::Message;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

const DEFAULT_RELAY: &str = "wss://ingress.e4mc.link";
/// After a tunnel ends (relay down, bad network) wait this long before the next try,
/// so a per-tick caller does not hammer the relay.
const RETRY_AFTER: Duration = Duration::from_secs(15);

struct Running {
    generation: u64,
    domain: Arc<Mutex<Option<String>>>,
    /// Dropping this stops the tunnel thread.
    _stop: oneshot::Sender<()>,
}

static STATE: Mutex<Option<Running>> = Mutex::new(None);
static LAST_ATTEMPT: Mutex<Option<Instant>> = Mutex::new(None);
static GENERATION: AtomicU64 = AtomicU64::new(0);

type Channels = Arc<Mutex<HashMap<u8, UnboundedSender<Chan>>>>;

enum Chan {
    Data(Vec<u8>),
    Close,
}

#[derive(Debug, PartialEq, Eq)]
enum Control {
    Domain(String),
    Open(u8),
    Closed(u8),
}

pub fn is_running() -> bool {
    STATE.lock().unwrap().is_some()
}

/// The public domain, once the relay has assigned one.
pub fn domain() -> Option<String> {
    let state = STATE.lock().unwrap();
    let running = state.as_ref()?;
    let found = running.domain.lock().unwrap().clone();
    found
}

/// Start tunnelling `local_port` (a server on 127.0.0.1). Returns at once; the work
/// runs on its own thread. Does nothing while running or inside the retry delay.
/// The relay can be changed with the E4MC_RELAY environment variable.
pub fn start(local_port: u16) -> Result<(), String> {
    let mut state = STATE.lock().unwrap();
    if state.is_some() {
        return Ok(());
    }
    {
        let mut last = LAST_ATTEMPT.lock().unwrap();
        if last.map_or(false, |t| t.elapsed() < RETRY_AFTER) {
            return Ok(());
        }
        *last = Some(Instant::now());
    }
    let relay = std::env::var("E4MC_RELAY").unwrap_or_else(|_| DEFAULT_RELAY.to_string());
    let generation = GENERATION.fetch_add(1, Ordering::SeqCst) + 1;
    let domain = Arc::new(Mutex::new(None));
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    *state = Some(Running { generation, domain: domain.clone(), _stop: stop_tx });

    let spawned = std::thread::Builder::new()
        .name("e4mc-tunnel".to_string())
        .spawn(move || {
            match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
            {
                Ok(rt) => rt.block_on(async {
                    tokio::select! {
                        res = run(relay, local_port, domain) => {
                            if let Err(e) = res {
                                eprintln!("[e4mc] tunnel ended: {e}");
                            }
                        }
                        _ = stop_rx => {}
                    }
                }),
                Err(e) => eprintln!("[e4mc] could not start the runtime: {e}"),
            }
            let mut state = STATE.lock().unwrap();
            if state.as_ref().map_or(false, |r| r.generation == generation) {
                *state = None;
            }
        });
    if let Err(e) = spawned {
        *state = None;
        return Err(e.to_string());
    }
    Ok(())
}

/// Close the tunnel (all players' connections through it end).
pub fn stop() {
    *STATE.lock().unwrap() = None;
    *LAST_ATTEMPT.lock().unwrap() = None;
}

async fn run(
    relay: String,
    port: u16,
    domain: Arc<Mutex<Option<String>>>,
) -> Result<(), String> {
    let (ws, _) = connect_async(relay).await.map_err(|e| e.to_string())?;
    let (mut ws_tx, mut ws_rx) = ws.split();
    let (out_tx, mut out_rx) = unbounded_channel::<Message>();
    let channels: Channels = Arc::new(Mutex::new(HashMap::new()));

    let writer = async move {
        while let Some(message) = out_rx.recv().await {
            if ws_tx.send(message).await.is_err() {
                return Err("relay write failed".to_string());
            }
        }
        Ok(())
    };

    let reader = {
        let out_tx = out_tx.clone();
        let channels = channels.clone();
        async move {
            while let Some(message) = ws_rx.next().await {
                match message.map_err(|e| e.to_string())? {
                    Message::Text(text) => match parse_control(text.as_str()) {
                        Some(Control::Domain(d)) => {
                            *domain.lock().unwrap() = Some(d);
                        }
                        Some(Control::Open(ch)) => {
                            let (tx, rx) = unbounded_channel();
                            channels.lock().unwrap().insert(ch, tx);
                            tokio::spawn(bridge(port, ch, rx, out_tx.clone(), channels.clone()));
                        }
                        Some(Control::Closed(ch)) => {
                            let tx = channels.lock().unwrap().remove(&ch);
                            if let Some(tx) = tx {
                                let _ = tx.send(Chan::Close);
                            }
                        }
                        None => {}
                    },
                    Message::Binary(data) => {
                        let data: &[u8] = &data;
                        if let Some((&ch, payload)) = data.split_first() {
                            let tx = channels.lock().unwrap().get(&ch).cloned();
                            if let Some(tx) = tx {
                                let _ = tx.send(Chan::Data(payload.to_vec()));
                            }
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            Ok::<(), String>(())
        }
    };

    tokio::select! {
        r = writer => r,
        r = reader => r,
    }
}

/// One player's connection: relay channel <-> a new TCP connection to the local server.
async fn bridge(
    port: u16,
    ch: u8,
    mut rx: UnboundedReceiver<Chan>,
    out: UnboundedSender<Message>,
    channels: Channels,
) {
    let local_closed_first = match TcpStream::connect(("127.0.0.1", port)).await {
        Ok(stream) => {
            let (mut rd, mut wr) = stream.into_split();
            let to_local = async {
                while let Some(chan) = rx.recv().await {
                    match chan {
                        Chan::Data(bytes) => {
                            if wr.write_all(&bytes).await.is_err() {
                                break;
                            }
                        }
                        Chan::Close => break,
                    }
                }
            };
            let from_local = async {
                let mut buf = [0u8; 16 * 1024];
                loop {
                    match rd.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            let mut packet = Vec::with_capacity(n + 1);
                            packet.push(ch);
                            packet.extend_from_slice(&buf[..n]);
                            if out.send(Message::Binary(packet.into())).is_err() {
                                break;
                            }
                        }
                    }
                }
            };
            tokio::select! {
                _ = to_local => false,
                _ = from_local => true,
            }
        }
        Err(_) => true,
    };
    channels.lock().unwrap().remove(&ch);
    if local_closed_first {
        let _ = out.send(Message::Text(format!("{{\"ChannelClosed\":{ch}}}").into()));
    }
}

fn parse_control(text: &str) -> Option<Control> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    let (tag, body) = value.as_object()?.iter().next()?;
    match tag.as_str() {
        "DomainAssigned" => Some(Control::Domain(body.as_str()?.to_string())),
        "ChannelOpen" => Some(Control::Open(u8::try_from(body.as_array()?.first()?.as_u64()?).ok()?)),
        "ChannelClosed" => Some(Control::Closed(u8::try_from(body.as_u64()?).ok()?)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_three_control_messages() {
        assert_eq!(
            parse_control(r#"{"DomainAssigned":"abc.e4mc.link"}"#),
            Some(Control::Domain("abc.e4mc.link".to_string()))
        );
        assert_eq!(parse_control(r#"{"ChannelOpen":[7,"1.2.3.4:5"]}"#), Some(Control::Open(7)));
        assert_eq!(parse_control(r#"{"ChannelClosed":7}"#), Some(Control::Closed(7)));
    }

    #[test]
    fn ignores_garbage() {
        assert_eq!(parse_control("nope"), None);
        assert_eq!(parse_control(r#"{"ChannelOpen":[300,"x"]}"#), None);
        assert_eq!(parse_control(r#"{"Other":1}"#), None);
    }
}
