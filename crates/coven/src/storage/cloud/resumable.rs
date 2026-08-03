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
use super::{combine_cleanup_failure, CloudHomeError};

/// Maps a non-success upload response `(status, body)` to a `CloudHomeError` —
/// the per-provider quota/error classifier.
pub(super) type ClassifyWrite = Box<dyn Fn(StatusCode, &str) -> CloudHomeError + Send + Sync>;

pub(super) type CancellationSucceeded = fn(StatusCode) -> bool;

#[derive(Clone)]
struct ResumableSession {
    client: reqwest::Client,
    url: String,
    key: String,
    cancellation_succeeded: CancellationSucceeded,
}

impl ResumableSession {
    async fn send_part(
        &self,
        part: Bytes,
        offset: u64,
        end: u64,
        total: u64,
    ) -> Result<reqwest::Response, CloudHomeError> {
        self.client
            .put(&self.url)
            .header("Content-Length", part.len())
            .header("Content-Range", range_content_header(offset, end, total))
            .body(part)
            .send()
            .await
            .map_err(|error| {
                CloudHomeError::Transport(format!("upload chunk {}: {error}", self.key))
            })
    }

    async fn cancel(&self) -> Result<(), CloudHomeError> {
        let response = self
            .client
            .delete(&self.url)
            .send()
            .await
            .map_err(|error| {
                CloudHomeError::Transport(format!(
                    "cancel upload session {} at {}: {error}",
                    self.key, self.url
                ))
            })?;
        let status = response.status();
        if (self.cancellation_succeeded)(status) {
            return Ok(());
        }
        let body = super::http::body_text(response).await;
        Err(CloudHomeError::Transport(format!(
            "cancel upload session {} at {} (HTTP {status}): {body}",
            self.key, self.url
        )))
    }
}

/// A [`PartSink`](super::PartSink) over a resumable upload session: each part is a
/// `Content-Range` PUT to the pre-authenticated `session_url` (no bearer token),
/// the last of which commits the file. `finish` is a no-op.
pub(super) struct RangePutUploader {
    session: ResumableSession,
    /// The accepted non-final status (Drive `308`, OneDrive `202`).
    intermediate_status: u16,
    total: u64,
    part_size: usize,
    /// The blob key, for error messages.
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
        cancellation_succeeded: CancellationSucceeded,
    ) -> Self {
        RangePutUploader {
            session: ResumableSession {
                client,
                url: session_url,
                key,
                cancellation_succeeded,
            },
            intermediate_status,
            total,
            part_size,
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
        let response = match self.session.send_part(part, offset, end, self.total).await {
            Ok(response) => response,
            Err(operation) => {
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
        self.session.cancel().await?;
        self.completed = true;
        Ok(())
    }

    pub(super) fn mark_completed(&mut self) {
        self.completed = true;
    }
}

impl Drop for RangePutUploader {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let session = self.session.clone();
        let cancellation_key = session.key.clone();
        let cancellation_url = session.url.clone();
        let worker = match std::thread::Builder::new()
            .name("coven-resumable-cancel".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| {
                        CloudHomeError::Transport(format!(
                            "build resumable cancellation runtime: {error}"
                        ))
                    })?;
                runtime.block_on(session.cancel())
            }) {
            Ok(worker) => worker,
            Err(error) => {
                tracing::error!(%error, key = %cancellation_key, session_url = %cancellation_url, "resumable upload cancellation thread failed to start");
                std::process::abort();
            }
        };
        match worker.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(%error, key = %cancellation_key, session_url = %cancellation_url, "resumable upload cancellation failed");
                std::process::abort();
            }
            Err(_) => {
                tracing::error!(key = %cancellation_key, session_url = %cancellation_url, "resumable upload cancellation thread panicked");
                std::process::abort();
            }
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
        cancellation_succeeded: CancellationSucceeded,
    ) -> Self {
        Self(RangePutUploader::new(
            client,
            session_url,
            intermediate_status,
            total,
            part_size,
            key,
            classify,
            cancellation_succeeded,
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

    async fn abort(&mut self) -> Result<(), CloudHomeError> {
        self.0.abort().await
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
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn cancellation_succeeded(status: StatusCode) -> bool {
        status.is_success() || status == StatusCode::NOT_FOUND
    }

    fn spawn_cancellation_endpoint(
        status: &'static str,
        release: std::sync::mpsc::Receiver<()>,
    ) -> (
        String,
        std::sync::mpsc::Receiver<()>,
        std::thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind cancellation endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept cancellation request");
            let mut request = Vec::new();
            let mut chunk = [0; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut chunk).expect("read cancellation request");
                assert_ne!(count, 0, "cancellation request ended before its headers");
                request.extend_from_slice(&chunk[..count]);
            }
            let request = String::from_utf8(request).expect("cancellation request is UTF-8");
            assert!(request.starts_with("DELETE "), "{request}");
            started_tx
                .send(())
                .expect("cancellation observer still exists");
            release
                .recv()
                .expect("cancellation response release signal");
            write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .expect("write cancellation response");
        });
        (endpoint, started_rx, server)
    }

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
        crate::storage::cloud::test_server::spawn_test_server(
            Router::new().fallback(incomplete_upload_endpoint),
        )
        .await
    }

    async fn successful_upload_endpoint() -> Response<Body> {
        Response::builder()
            .status(StatusCode::CREATED)
            .body(Body::from(r#"{"id":"provider-file-id"}"#))
            .expect("build completion response")
    }

    async fn spawn_successful_upload_endpoint() -> (String, tokio::sync::oneshot::Sender<()>) {
        crate::storage::cloud::test_server::spawn_test_server(
            Router::new().fallback(successful_upload_endpoint),
        )
        .await
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
        let delete_count = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .fallback(failed_upload_endpoint)
            .with_state(delete_count.clone());
        let (endpoint, shutdown_tx) =
            crate::storage::cloud::test_server::spawn_test_server(app).await;
        (endpoint, delete_count, shutdown_tx)
    }

    async fn failed_upload_and_cancel_endpoint(
        State(delete_count): State<Arc<AtomicUsize>>,
        method: Method,
    ) -> Response<Body> {
        match method {
            Method::PUT => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("upload failed"))
                .expect("build upload failure response"),
            Method::DELETE if delete_count.fetch_add(1, Ordering::SeqCst) == 0 => {
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from("cancellation failed"))
                    .expect("build cancellation failure response")
            }
            Method::DELETE => Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .expect("build cancellation response"),
            _ => Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Body::from("unexpected method"))
                .expect("build method response"),
        }
    }

    async fn spawn_failed_upload_and_cancel_endpoint() -> (String, tokio::sync::oneshot::Sender<()>)
    {
        let delete_count = Arc::new(AtomicUsize::new(0));
        crate::storage::cloud::test_server::spawn_test_server(
            Router::new()
                .fallback(failed_upload_and_cancel_endpoint)
                .with_state(delete_count),
        )
        .await
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
            cancellation_succeeded,
        );

        let err = sink
            .send_part(Bytes::from_static(b"data"), 0, true)
            .await
            .expect_err("final incomplete status must fail");
        assert_eq!(
            err.to_string(),
            "transport error: 308 Permanent Redirect: upload incomplete"
        );
        shutdown.send(()).expect("shut down upload endpoint");
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
            cancellation_succeeded,
        );

        let error = uploader
            .send_part(Bytes::from_static(b"data"), 0, false)
            .await
            .expect_err("premature completion must fail");
        assert!(error.to_string().contains("201 Created"), "{error}");
        shutdown.send(()).expect("shut down upload endpoint");
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
            cancellation_succeeded,
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
        shutdown.send(()).expect("shut down upload endpoint");
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
            cancellation_succeeded,
        );

        uploader
            .send_part(Bytes::from_static(b"data"), 0, false)
            .await
            .expect_err("failed part must surface");
        assert_eq!(delete_count.load(Ordering::SeqCst), 1);
        shutdown.send(()).expect("shut down upload endpoint");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
            cancellation_succeeded,
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
        drop(uploader);
        shutdown.send(()).expect("shut down upload endpoint");
    }

    #[tokio::test]
    async fn drop_without_a_runtime_waits_for_cancellation_to_finish() {
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let (endpoint, started_rx, server) =
            spawn_cancellation_endpoint("204 No Content", release_rx);
        let uploader = RangePutUploader::new(
            reqwest::Client::new(),
            endpoint,
            StatusCode::PERMANENT_REDIRECT.as_u16(),
            8,
            4,
            "objects/blob".to_string(),
            Box::new(|status, body| CloudHomeError::Transport(format!("{status}: {body}"))),
            cancellation_succeeded,
        );
        let (finished_tx, finished_rx) = std::sync::mpsc::sync_channel(1);
        let dropper = std::thread::spawn(move || {
            drop(uploader);
            finished_tx.send(()).expect("drop observer still exists");
        });

        started_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("cancellation request started");
        assert!(matches!(
            finished_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        release_tx
            .send(())
            .expect("cancellation endpoint still exists");
        finished_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("drop finished after cancellation response");
        dropper.join().expect("drop thread succeeded");
        server.join().expect("cancellation endpoint succeeded");
    }

    #[test]
    fn cancellation_failure_terminates_the_process() {
        const CHILD: &str = "COVEN_RANGE_PUT_CANCEL_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
            let (endpoint, started_rx, _server) =
                spawn_cancellation_endpoint("500 Internal Server Error", release_rx);
            let releaser = std::thread::spawn(move || {
                started_rx
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .expect("cancellation request started");
                release_tx
                    .send(())
                    .expect("cancellation endpoint still exists");
            });
            let uploader = RangePutUploader::new(
                reqwest::Client::new(),
                endpoint,
                StatusCode::PERMANENT_REDIRECT.as_u16(),
                8,
                4,
                "objects/blob".to_string(),
                Box::new(|status, body| CloudHomeError::Transport(format!("{status}: {body}"))),
                cancellation_succeeded,
            );
            drop(uploader);
            releaser.join().expect("cancellation releaser succeeded");
            std::process::exit(0);
        }

        let status = std::process::Command::new(
            std::env::current_exe().expect("locate resumable test executable"),
        )
        .arg("cancellation_failure_terminates_the_process")
        .arg("--nocapture")
        .env(CHILD, "1")
        .status()
        .expect("run resumable cancellation subprocess");
        assert!(!status.success(), "cancellation subprocess survived");
    }
}
