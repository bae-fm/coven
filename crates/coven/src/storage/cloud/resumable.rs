//! The resumable-session [`PartSink`] shared by Google Drive and OneDrive.
//!
//! Both upload a large file by PUTting fixed-size windows to a pre-authenticated
//! session URL with a `Content-Range: bytes {start}-{end}/{total}` header, where
//! every non-final window returns an "incomplete" status (Drive 308, OneDrive 202)
//! and the final window returns 200/201. The only differences are that accepted
//! intermediate status, the session URL, the part size, and how a failure body
//! maps to a friendly error — so one sink serves both.

use async_trait::async_trait;
use bytes::Bytes;
use reqwest::StatusCode;

use super::http::range_content_header;
use super::CloudHomeError;

/// Maps a non-success upload response `(status, body)` to a `CloudHomeError` —
/// the per-provider quota/error classifier.
pub(super) type ClassifyWrite = Box<dyn Fn(StatusCode, &str) -> CloudHomeError + Send + Sync>;

/// A [`PartSink`](super::PartSink) over a resumable upload session: each part is a
/// `Content-Range` PUT to the pre-authenticated `session_url` (no bearer token),
/// the last of which commits the file. `finish` is a no-op.
pub(super) struct RangePutSink {
    client: reqwest::Client,
    session_url: String,
    /// The accepted non-final status (Drive `308`, OneDrive `202`).
    intermediate_status: u16,
    total: u64,
    part_size: usize,
    /// The blob key, for error messages.
    key: String,
    classify: ClassifyWrite,
}

impl RangePutSink {
    pub(super) fn new(
        client: reqwest::Client,
        session_url: String,
        intermediate_status: u16,
        total: u64,
        part_size: usize,
        key: String,
        classify: ClassifyWrite,
    ) -> Self {
        RangePutSink {
            client,
            session_url,
            intermediate_status,
            total,
            part_size,
            key,
            classify,
        }
    }
}

#[async_trait]
impl super::PartSink for RangePutSink {
    fn part_size(&self) -> usize {
        self.part_size
    }

    async fn send_part(
        &mut self,
        part: Bytes,
        offset: u64,
        is_last: bool,
    ) -> Result<(), CloudHomeError> {
        let end = offset + part.len() as u64 - 1;
        // The session URL is already a signed one-time URL, so the part PUTs carry
        // no bearer token.
        let resp = self
            .client
            .put(&self.session_url)
            .header("Content-Length", part.len())
            .header(
                "Content-Range",
                range_content_header(offset, end, self.total),
            )
            .body(part)
            .send()
            .await
            .map_err(|e| CloudHomeError::Transport(format!("upload chunk {}: {e}", self.key)))?;
        let status = resp.status();
        // Intermediate parts return the provider's "incomplete" status; the final
        // part returns 200/201. Anything else is a failure.
        if status.is_success() || (!is_last && status.as_u16() == self.intermediate_status) {
            Ok(())
        } else {
            let body = super::http::body_text(resp).await;
            Err((self.classify)(status, &body))
        }
    }

    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::PartSink;
    use axum::body::Body;
    use axum::http::{Method, Response, StatusCode};
    use axum::Router;

    async fn incomplete_upload_endpoint(method: Method) -> Response<Body> {
        if method != Method::PUT {
            return Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Body::from("expected PUT"))
                .expect("build method response");
        }
        Response::builder()
            .status(StatusCode::PERMANENT_REDIRECT)
            .body(Body::from("upload incomplete"))
            .expect("build incomplete response")
    }

    async fn spawn_incomplete_upload_endpoint() -> (String, tokio::sync::oneshot::Sender<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind upload endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let app = Router::new().fallback(incomplete_upload_endpoint);

        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("upload endpoint failed");
        });

        (endpoint, shutdown_tx)
    }

    #[tokio::test]
    async fn final_part_rejects_incomplete_status() {
        let (endpoint, shutdown) = spawn_incomplete_upload_endpoint().await;
        let mut sink = RangePutSink::new(
            reqwest::Client::new(),
            endpoint,
            StatusCode::PERMANENT_REDIRECT.as_u16(),
            4,
            4,
            "objects/blob".to_string(),
            Box::new(|status, body| CloudHomeError::Transport(format!("{status}: {body}"))),
        );

        let err = sink
            .send_part(Bytes::from_static(b"data"), 0, true)
            .await
            .expect_err("final incomplete status must fail");
        assert_eq!(
            err.to_string(),
            "transport error: 308 Permanent Redirect: upload incomplete"
        );
        let _ = shutdown.send(());
    }
}
