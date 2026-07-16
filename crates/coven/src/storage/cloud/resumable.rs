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
pub(super) struct RangePutUploader {
    client: reqwest::Client,
    session_url: String,
    /// The accepted non-final status (Drive `308`, OneDrive `202`).
    intermediate_status: u16,
    total: u64,
    part_size: usize,
    /// The blob key, for error messages.
    key: String,
    classify: ClassifyWrite,
    completed: bool,
}

impl RangePutUploader {
    pub(super) fn new(
        client: reqwest::Client,
        session_url: String,
        intermediate_status: u16,
        total: u64,
        part_size: usize,
        key: String,
        classify: ClassifyWrite,
    ) -> Self {
        RangePutUploader {
            client,
            session_url,
            intermediate_status,
            total,
            part_size,
            key,
            classify,
            completed: false,
        }
    }

    pub(super) fn part_size(&self) -> usize {
        self.part_size
    }

    pub(super) async fn send_part(
        &mut self,
        part: Bytes,
        offset: u64,
        is_last: bool,
    ) -> Result<Option<reqwest::Response>, CloudHomeError> {
        self.send_part_with_completion(part, offset, is_last, true)
            .await
    }

    pub(super) async fn send_deferred_part(
        &mut self,
        part: Bytes,
        offset: u64,
        is_last: bool,
    ) -> Result<Option<reqwest::Response>, CloudHomeError> {
        self.send_part_with_completion(part, offset, is_last, false)
            .await
    }

    async fn send_part_with_completion(
        &mut self,
        part: Bytes,
        offset: u64,
        is_last: bool,
        completes_on_final_part: bool,
    ) -> Result<Option<reqwest::Response>, CloudHomeError> {
        let end = offset + part.len() as u64 - 1;
        let response = match self
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
        {
            Ok(response) => response,
            Err(error) => {
                let operation =
                    CloudHomeError::Transport(format!("upload chunk {}: {error}", self.key));
                let cleanup = self.abort().await;
                return Err(combine_cleanup_failure(operation, cleanup));
            }
        };
        let status = response.status();
        if (!is_last || !completes_on_final_part) && status.as_u16() == self.intermediate_status {
            return Ok(None);
        }
        if is_last && matches!(status, StatusCode::OK | StatusCode::CREATED) {
            self.completed = true;
            return Ok(Some(response));
        }
        let body = super::http::body_text(response).await;
        let operation = (self.classify)(status, &body);
        let cleanup = self.abort().await;
        Err(combine_cleanup_failure(operation, cleanup))
    }

    pub(super) async fn abort(&mut self) -> Result<(), CloudHomeError> {
        if self.completed {
            return Ok(());
        }
        self.completed = true;
        let response = self
            .client
            .delete(&self.session_url)
            .send()
            .await
            .map_err(|error| {
                CloudHomeError::Transport(format!("upload chunk {}: {error}", self.key))
            })?;
        let status = response.status();
        if status.is_success() || status == StatusCode::NOT_FOUND {
            return Ok(());
        }
        let body = super::http::body_text(response).await;
        Err(CloudHomeError::Transport(format!(
            "cancel upload session {} (HTTP {status}): {body}",
            self.key
        )))
    }

    pub(super) fn mark_completed(&mut self) {
        self.completed = true;
    }
}

fn combine_cleanup_failure(
    operation: CloudHomeError,
    cleanup: Result<(), CloudHomeError>,
) -> CloudHomeError {
    match cleanup {
        Ok(()) => operation,
        Err(cleanup) => CloudHomeError::CleanupFailed {
            operation: Box::new(operation),
            cleanup: Box::new(cleanup),
        },
    }
}

impl Drop for RangePutUploader {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let request = self.client.delete(self.session_url.clone());
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = request.send().await;
            });
        }
    }
}

pub(super) struct RangePutSink(RangePutUploader);

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
        Self(RangePutUploader::new(
            client,
            session_url,
            intermediate_status,
            total,
            part_size,
            key,
            classify,
        ))
    }
}

#[async_trait]
impl super::PartSink for RangePutSink {
    fn part_size(&self) -> usize {
        self.0.part_size()
    }

    async fn send_part(
        &mut self,
        part: Bytes,
        offset: u64,
        is_last: bool,
    ) -> Result<(), CloudHomeError> {
        self.0.send_part(part, offset, is_last).await.map(drop)
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
    use axum::extract::State;
    use axum::http::{Method, Response, StatusCode};
    use axum::Router;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    async fn incomplete_upload_endpoint(method: Method) -> Response<Body> {
        if method == Method::DELETE {
            return Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .expect("build cancellation response");
        }
        if method == Method::PUT {
            return Response::builder()
                .status(StatusCode::PERMANENT_REDIRECT)
                .body(Body::from("upload incomplete"))
                .expect("build incomplete response");
        }
        Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::from("expected PUT or DELETE"))
            .expect("build method response")
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

    async fn successful_upload_endpoint() -> Response<Body> {
        Response::builder()
            .status(StatusCode::CREATED)
            .body(Body::from(r#"{"id":"provider-file-id"}"#))
            .expect("build completion response")
    }

    async fn spawn_successful_upload_endpoint() -> (String, tokio::sync::oneshot::Sender<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind upload endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            axum::serve(listener, Router::new().fallback(successful_upload_endpoint))
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("upload endpoint failed");
        });
        (endpoint, shutdown_tx)
    }

    async fn failed_upload_endpoint(
        State(delete_count): State<Arc<AtomicUsize>>,
        method: Method,
    ) -> Response<Body> {
        match method {
            Method::PUT => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("upload failed"))
                .expect("build upload failure response"),
            Method::DELETE => {
                delete_count.fetch_add(1, Ordering::SeqCst);
                Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .body(Body::empty())
                    .expect("build cancellation response")
            }
            _ => Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Body::empty())
                .expect("build method response"),
        }
    }

    async fn spawn_failed_upload_endpoint(
    ) -> (String, Arc<AtomicUsize>, tokio::sync::oneshot::Sender<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind upload endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let delete_count = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .fallback(failed_upload_endpoint)
            .with_state(delete_count.clone());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("upload endpoint failed");
        });
        (endpoint, delete_count, shutdown_tx)
    }

    async fn failed_upload_and_cancel_endpoint(method: Method) -> Response<Body> {
        let body = match method {
            Method::PUT => "upload failed",
            Method::DELETE => "cancellation failed",
            _ => "unexpected method",
        };
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(body))
            .expect("build failure response")
    }

    async fn spawn_failed_upload_and_cancel_endpoint() -> (String, tokio::sync::oneshot::Sender<()>)
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind upload endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(failed_upload_and_cancel_endpoint),
            )
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

    #[tokio::test]
    async fn nonfinal_part_rejects_premature_success() {
        let (endpoint, shutdown) = spawn_successful_upload_endpoint().await;
        let mut uploader = RangePutUploader::new(
            reqwest::Client::new(),
            endpoint,
            StatusCode::PERMANENT_REDIRECT.as_u16(),
            8,
            4,
            "objects/blob".to_string(),
            Box::new(|status, body| CloudHomeError::Transport(format!("{status}: {body}"))),
        );

        let error = uploader
            .send_part(Bytes::from_static(b"data"), 0, false)
            .await
            .expect_err("premature completion must fail");
        assert!(error.to_string().contains("201 Created"), "{error}");
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn final_part_returns_provider_response_body() {
        let (endpoint, shutdown) = spawn_successful_upload_endpoint().await;
        let mut uploader = RangePutUploader::new(
            reqwest::Client::new(),
            endpoint,
            StatusCode::PERMANENT_REDIRECT.as_u16(),
            4,
            4,
            "objects/blob".to_string(),
            Box::new(|status, body| CloudHomeError::Transport(format!("{status}: {body}"))),
        );

        let body = uploader
            .send_part(Bytes::from_static(b"data"), 0, true)
            .await
            .expect("final part succeeds")
            .expect("final part returns response")
            .bytes()
            .await
            .expect("read final response body");
        assert_eq!(body.as_ref(), br#"{"id":"provider-file-id"}"#);
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn part_failure_cancels_the_session_before_returning() {
        let (endpoint, delete_count, shutdown) = spawn_failed_upload_endpoint().await;
        let mut uploader = RangePutUploader::new(
            reqwest::Client::new(),
            endpoint,
            StatusCode::PERMANENT_REDIRECT.as_u16(),
            8,
            4,
            "objects/blob".to_string(),
            Box::new(|status, body| CloudHomeError::Transport(format!("{status}: {body}"))),
        );

        uploader
            .send_part(Bytes::from_static(b"data"), 0, false)
            .await
            .expect_err("failed part must surface");
        assert_eq!(delete_count.load(Ordering::SeqCst), 1);
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn part_failure_preserves_the_operation_and_cancellation_errors() {
        let (endpoint, shutdown) = spawn_failed_upload_and_cancel_endpoint().await;
        let mut uploader = RangePutUploader::new(
            reqwest::Client::new(),
            endpoint,
            StatusCode::PERMANENT_REDIRECT.as_u16(),
            8,
            4,
            "objects/blob".to_string(),
            Box::new(|status, body| CloudHomeError::AlreadyExists(format!("{status}: {body}"))),
        );

        let error = uploader
            .send_part(Bytes::from_static(b"data"), 0, false)
            .await
            .expect_err("failed part and cancellation must surface");
        let (operation, cleanup) = error
            .cleanup_causes()
            .expect("both failure causes must be retained");
        assert!(matches!(operation, CloudHomeError::AlreadyExists(_)));
        assert!(
            cleanup.to_string().contains("cancellation failed"),
            "{cleanup}"
        );
        assert!(!error.is_retryable());
        let _ = shutdown.send(());
    }
}
