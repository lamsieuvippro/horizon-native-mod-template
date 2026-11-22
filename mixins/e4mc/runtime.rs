// Pure logic of the e4mc mod. This file is both the standalone Cargo library
// and the injected `crate::game::e4mc` module, so it names no game types.

use std::collections::HashSet;
use std::sync::Mutex;

static ANNOUNCED: Mutex<Option<HashSet<String>>> = Mutex::new(None);

/// The line a player reads in chat.
pub fn announce_message(domain: &str) -> String {
    format!("[e4mc] Friends can join with: {domain}")
}

/// True once per key until `reset` (so the domain is not repeated every tick).
pub fn should_announce(key: &str) -> bool {
    let mut guard = ANNOUNCED.lock().unwrap();
    guard.get_or_insert_with(HashSet::new).insert(key.to_string())
}

pub fn reset() {
    *ANNOUNCED.lock().unwrap() = None;
}

/// The local port to tunnel. Horizon's docs show no hook that tells a mod the
/// LAN port, so for now it comes from the E4MC_PORT environment variable.
pub fn lan_port() -> Option<u16> {
    parse_port(std::env::var("E4MC_PORT").ok().as_deref())
}

pub fn parse_port(value: Option<&str>) -> Option<u16> {
    value?.trim().parse::<u16>().ok().filter(|p| *p != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn announces_each_key_once() {
        reset();
        assert!(should_announce("a"));
        assert!(!should_announce("a"));
        assert!(should_announce("b"));
        reset();
        assert!(should_announce("a"));
    }

    #[test]
    fn port_parsing() {
        assert_eq!(parse_port(Some(" 25565 ")), Some(25565));
        assert_eq!(parse_port(Some("0")), None);
        assert_eq!(parse_port(Some("abc")), None);
        assert_eq!(parse_port(None), None);
    }

    #[test]
    fn message_has_domain() {
        assert!(announce_message("x.e4mc.link").contains("x.e4mc.link"));
    }
}
