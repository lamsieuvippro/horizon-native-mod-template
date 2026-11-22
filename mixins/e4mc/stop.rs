// The server is stopping: close the tunnel and forget who was told.
fn server_stopping() {
    e4mc_tunnel::stop();
    crate::game::e4mc::reset();
}
