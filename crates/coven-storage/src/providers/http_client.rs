//! Coven's one HTTPS client, which every provider and admin call sends through
//! (§6.7). It checks certificates with the platform's verifier, follows no
//! redirect and retries nothing itself. A request has no deadline: it fails
//! only when no bytes move, in either direction, for `STALL_WINDOW`.

use super::http::transport;
use crate::{ByteProgress, CloudProvider, StorageError, StorageFailure};
use bytes::Bytes;
use coven_foundation::clock::ClockRef;
use reqwest::{IntoUrl, Method};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

/// How long an exchange may move no bytes before it is abandoned (§6.7).
/// Phones pause for tens of seconds while they change networks or lose
/// coverage, and their links recover; the AWS SDK's 5-second default cut such
/// downloads off. A minute with nothing sent or received means the connection
/// is gone.
pub(crate) const STALL_WINDOW: Duration = Duration::from_secs(60);

/// The most bytes of a request body handed to the connection at once, so an
/// upload's progress shows as it goes rather than once per body: the stall
/// window's, and the bytes a caller's `ByteProgress` hears.
const UPLOAD_STEP: usize = 64 * 1024;

/// A reqwest client with the clock its stall window is measured by.
#[derive(Clone)]
pub(crate) struct HttpClient {
    client: reqwest::Client,
    clock: ClockRef,
}

impl fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpClient").finish_non_exhaustive()
    }
}

impl HttpClient {
    /// Build the client. A TLS setup failure, such as a platform verifier
    /// that cannot start, is returned, never a panic.
    #[allow(
        clippy::disallowed_methods,
        reason = "the one place a reqwest client is built"
    )]
    pub(crate) fn new(clock: ClockRef) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()?;
        Ok(Self { client, clock })
    }

    /// Start a request; `send` sends it.
    pub(crate) fn request(&self, method: Method, url: impl IntoUrl) -> reqwest::RequestBuilder {
        self.client.request(method, url)
    }

    /// Send a request built by `request`, labelling a failure with `provider`.
    pub(crate) async fn send(
        &self,
        provider: CloudProvider,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, StorageError> {
        let request = request
            .build()
            .map_err(|error| transport(provider, error))?;
        self.execute(request, &ByteProgress::unread())
            .await
            .map_err(|error| error.into_storage(provider))
    }

    /// Send `request`, telling `sent` each step of its body the connection
    /// takes. It fails when no bytes move for `STALL_WINDOW`: connecting,
    /// sending its body, waiting for the reply, and reading the returned
    /// response's body each count.
    #[allow(
        clippy::disallowed_methods,
        reason = "the one place a request is sent, under the stall window"
    )]
    pub(crate) async fn execute(
        &self,
        mut request: reqwest::Request,
        sent: &ByteProgress,
    ) -> Result<reqwest::Response, SendError> {
        let progress = ExchangeProgress::new(self.clock.clone());
        if let Some(body) = request.body_mut().take() {
            *request.body_mut() = Some(reqwest::Body::wrap(UploadBody {
                inner: body,
                pending: Bytes::new(),
                progress: progress.clone(),
                sent: sent.clone(),
                taken: 0,
            }));
        }
        let response = tokio::select! {
            response = self.client.execute(request) => response.map_err(SendError::Http)?,
            () = progress.stalled() => return Err(SendError::Stalled(Stalled)),
        };
        progress.mark();
        let response = ::http::Response::<reqwest::Body>::from(response).map(|body| {
            reqwest::Body::wrap(DownloadBody {
                inner: body,
                stalled: Mutex::new(Some(progress.stalled())),
                progress,
            })
        });
        Ok(reqwest::Response::from(response))
    }
}

/// Why a request failed before its response arrived.
#[derive(Debug)]
pub(crate) enum SendError {
    /// reqwest's own failure: building, connecting, TLS or the exchange.
    Http(reqwest::Error),
    /// No bytes moved for `STALL_WINDOW`.
    Stalled(Stalled),
}

impl SendError {
    pub(crate) fn into_storage(self, provider: CloudProvider) -> StorageError {
        match self {
            Self::Http(error) => transport(provider, error),
            Self::Stalled(stalled) => StorageError::Provider {
                provider,
                failure: StorageFailure::Network,
                source: Box::new(stalled),
            },
        }
    }
}

/// No bytes moved, in either direction, for `STALL_WINDOW`.
#[derive(Debug)]
pub(crate) struct Stalled;

impl fmt::Display for Stalled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "no bytes moved for {} s", STALL_WINDOW.as_secs())
    }
}

impl std::error::Error for Stalled {}

/// When one exchange last moved bytes, by the injected clock.
#[derive(Clone)]
struct ExchangeProgress {
    clock: ClockRef,
    last: Arc<AtomicU64>,
}

impl ExchangeProgress {
    /// ExchangeProgress starting now.
    fn new(clock: ClockRef) -> Self {
        let progress = Self {
            clock,
            last: Arc::new(AtomicU64::new(0)),
        };
        progress.mark();
        progress
    }

    /// Record that bytes moved now.
    fn mark(&self) {
        let now = u64::try_from(self.clock.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.last.fetch_max(now, Ordering::Relaxed);
    }

    /// Resolve once `STALL_WINDOW` passes with no progress.
    fn stalled(&self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let progress = self.clone();
        Box::pin(async move {
            loop {
                let last = Duration::from_nanos(progress.last.load(Ordering::Relaxed));
                let idle = progress.clock.elapsed().saturating_sub(last);
                if idle >= STALL_WINDOW {
                    return;
                }
                progress.clock.sleep(STALL_WINDOW - idle).await;
            }
        })
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A request body handed over in steps, each step recorded as progress and
/// told to the caller as the bytes taken so far.
struct UploadBody {
    inner: reqwest::Body,
    pending: Bytes,
    progress: ExchangeProgress,
    sent: ByteProgress,
    /// The bytes of this body handed over so far.
    taken: u64,
}

impl http_body::Body for UploadBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, BoxError>>> {
        loop {
            if !self.pending.is_empty() {
                let step = self.pending.len().min(UPLOAD_STEP);
                let step = self.pending.split_to(step);
                self.progress.mark();
                self.taken += step.len() as u64;
                self.sent.moved(self.taken);
                return Poll::Ready(Some(Ok(http_body::Frame::data(step))));
            }
            match std::task::ready!(Pin::new(&mut self.inner).poll_frame(cx)) {
                Some(Ok(frame)) => match frame.into_data() {
                    Ok(data) => self.pending = data,
                    Err(frame) => return Poll::Ready(Some(Ok(frame))),
                },
                Some(Err(error)) => return Poll::Ready(Some(Err(error.into()))),
                None => return Poll::Ready(None),
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.pending.is_empty() && self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        let inner = self.inner.size_hint();
        let pending = self.pending.len() as u64;
        let mut hint = http_body::SizeHint::new();
        hint.set_lower(inner.lower() + pending);
        if let Some(upper) = inner.upper() {
            hint.set_upper(upper + pending);
        }
        hint
    }
}

/// A response body that records each frame as progress, and fails once none
/// arrives for `STALL_WINDOW`.
struct DownloadBody {
    inner: reqwest::Body,
    progress: ExchangeProgress,
    /// The stall watch, polled while a read waits. `None` once it fired. The
    /// lock is never taken: it only makes the body `Sync`, as reqwest requires.
    stalled: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
}

impl http_body::Body for DownloadBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, BoxError>>> {
        match Pin::new(&mut self.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                self.progress.mark();
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => {
                let stalled = self
                    .stalled
                    .get_mut()
                    .unwrap_or_else(PoisonError::into_inner);
                let fired = match stalled {
                    Some(watch) => watch.as_mut().poll(cx).is_ready(),
                    None => true,
                };
                if fired {
                    *stalled = None;
                    Poll::Ready(Some(Err(Box::new(Stalled))))
                } else {
                    Poll::Pending
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
#[path = "http_client_tests.rs"]
mod tests;
