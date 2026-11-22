// Runs before the server tick. Starts the tunnel once, then tells every
// player the public domain once per domain. `Host` method names and the
// player's field types follow native-mods.md's helper list (without the
// `native_` prefix); adjust to what the compiler reports.
fn server_tick(game: &mut crate::game::Game<'_>) {
    let host: &mut dyn horizon_client::mod_api::Host = game;

    if !e4mc_tunnel::is_running() {
        if let Some(port) = crate::game::e4mc::lan_port() {
            if let Err(err) = e4mc_tunnel::start(port) {
                eprintln!("[e4mc] could not start the tunnel: {err}");
            }
        }
        return;
    }
    let Some(domain) = e4mc_tunnel::domain() else { return };

    for player in host.server_players() {
        // One announcement per player and per domain, so a reconnect that gets a
        // new domain tells everyone again.
        let key = format!("{}@{}", player.uuid, domain);
        if crate::game::e4mc::should_announce(&key) {
            let _ = host.tell(&player.uuid, &crate::game::e4mc::announce_message(&domain));
        }
    }
}
