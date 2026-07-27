//! Loopback HTTP server scaffolding for cloud-backend tests.
//!
//! Lives outside the `oauth-providers`-gated `http` module because the S3
//! backend's tests need it too — this module compiles in every test build.

/// Bind an ephemeral loopback port, serve `app` on it, and hand back the
/// endpoint plus the trigger that stops it.
///
/// Dropping the sender stops the server too, so a test that forgets to send is
/// still torn down. This is `spawn_fake_s3` generalised — every backend's test
/// module had grown its own copy of the same bind-format-spawn dance.
pub(super) async fn spawn_test_server(
    app: axum::Router,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test endpoint");
    let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("test endpoint failed");
    });
    (endpoint, shutdown_tx)
}
