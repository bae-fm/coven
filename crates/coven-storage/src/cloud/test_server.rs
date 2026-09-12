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
pub(crate) async fn spawn_test_server(
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

/// A response body that hands over `delivered` and then fails. Paired with a
/// `Content-Length` naming the whole object, it is the provider that stops
/// part-way through a body — the case a reader must refuse rather than accept
/// as a short object.
#[cfg(feature = "oauth-providers")]
pub(crate) fn cut_body(delivered: Vec<u8>) -> axum::body::Body {
    // The delivered bytes are handed over on their own poll, so the response
    // reaches the client before the failure does. Failing in the same poll
    // would abort the whole response and the client would never see a body.
    axum::body::Body::from_stream(futures_util::stream::unfold(
        Some(delivered),
        |delivered| async move {
            match delivered {
                Some(delivered) => Some((Ok(bytes::Bytes::from(delivered)), None)),
                None => {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    Some((
                        Err(std::io::Error::other("the provider cut the body")),
                        None,
                    ))
                }
            }
        },
    ))
}
