//! Shared OAuth token lifecycle for the consumer-cloud backends.
//!
//! Google Drive, Dropbox, and OneDrive all cache an access token, refresh it on
//! expiry (persisting the new tokens to the keyring), and retry a request once
//! on a 401. This holds that logic in one place; each backend owns an
//! `OAuthSession` and routes its requests through `api_call`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use reqwest::StatusCode;
use tokio::sync::RwLock;
use tracing::{info, warn};

use super::CloudHomeError;
use crate::clock::ClockRef;
use crate::keys::KeyService;
use crate::oauth::{self, OAuthConfig, OAuthTokens};

/// Waits out one retry delay. A field so tests exercise the retry schedule
/// (attempt count, honored `Retry-After`) without sleeping real seconds.
type Sleeper = Arc<dyn Fn(Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// A 429 or 5xx retries this many times before the failure surfaces. Chosen with
/// [`RETRY_BASE_DELAY`]/[`RETRY_MAX_DELAY`] so quota pressure (routine 429s on a
/// large Drive sync) degrades a cycle to slow rather than failed, while a hard
/// outage exhausts the attempts in a few seconds and fails loud to the cycle,
/// which then applies its own minutes-long backoff.
const MAX_TRANSIENT_RETRIES: u32 = 4;
/// The first retry waits this long (before jitter); each further retry doubles it.
const RETRY_BASE_DELAY: Duration = Duration::from_millis(500);
/// Ceiling on any single wait, applied to both the exponential term and a
/// server-supplied `Retry-After`, so one response can't stall a cycle for minutes.
const RETRY_MAX_DELAY: Duration = Duration::from_secs(32);

/// 429 and 5xx are the transient failures worth retrying; every other 4xx
/// (including 401, handled separately by token refresh) is a caller error a retry
/// won't fix.
fn is_transient(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

/// The `Retry-After` header as a duration when the server sent one as an integer
/// number of seconds — the form Drive/Dropbox/OneDrive use. Absent or non-integer
/// header falls through to computed backoff.
fn parse_retry_after(resp: &reqwest::Response) -> Option<Duration> {
    let secs: u64 = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(secs))
}

/// Owns a provider's OAuth tokens (refreshing them as needed) and the
/// `reqwest::Client` its requests go out on — every OAuth backend shared the same
/// client field and token lifecycle, so both live here once.
pub struct OAuthSession {
    client: reqwest::Client,
    tokens: RwLock<OAuthTokens>,
    key_service: KeyService,
    clock: ClockRef,
    config: OAuthConfig,
    /// Human-readable provider name, used only in log lines.
    provider_label: &'static str,
    sleeper: Sleeper,
}

impl OAuthSession {
    pub fn new(
        tokens: OAuthTokens,
        key_service: KeyService,
        clock: ClockRef,
        config: OAuthConfig,
        provider_label: &'static str,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            tokens: RwLock::new(tokens),
            key_service,
            clock,
            config,
            provider_label,
            sleeper: Arc::new(|delay| Box::pin(tokio::time::sleep(delay))),
        }
    }

    /// The shared HTTP client requests go out on.
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// The current access token, refreshing if it's expired or about to expire.
    async fn access_token(&self) -> Result<String, CloudHomeError> {
        let tokens = self.tokens.read().await;
        match tokens.expires_at {
            Some(expires_at) if self.clock.now().timestamp() < expires_at - 60 => {
                return Ok(tokens.access_token.clone());
            }
            // No expiry info: assume valid.
            None => return Ok(tokens.access_token.clone()),
            _ => {}
        }
        drop(tokens);
        self.refresh().await
    }

    /// Refresh the tokens and persist them to the keyring.
    async fn refresh(&self) -> Result<String, CloudHomeError> {
        let mut tokens = self.tokens.write().await;

        // Another task may have refreshed while we waited for the write lock.
        if let Some(expires_at) = tokens.expires_at {
            if self.clock.now().timestamp() < expires_at - 60 {
                return Ok(tokens.access_token.clone());
            }
        }

        let refresh_token = tokens.refresh_token.as_deref().ok_or_else(|| {
            CloudHomeError::Storage(format!(
                "Your {} sign-in is missing a refresh token. Reconnect to keep syncing.",
                self.provider_label,
            ))
        })?;

        let new_tokens = oauth::refresh(&self.config, refresh_token, self.clock.as_ref())
            .await
            .map_err(|e| match e {
                oauth::OAuthError::Reauthorize(detail) => CloudHomeError::Storage(format!(
                    "Your {} access was revoked or expired. Reconnect to keep syncing. ({detail})",
                    self.provider_label,
                )),
                other => CloudHomeError::Storage(format!("OAuth refresh failed: {other}")),
            })?;

        self.key_service
            .set_cloud_home_oauth_tokens(&new_tokens)
            .map_err(|e| CloudHomeError::Storage(format!("persist refreshed OAuth tokens: {e}")))?;

        let access_token = new_tokens.access_token.clone();
        *tokens = new_tokens;

        info!("Refreshed {} OAuth tokens", self.provider_label);
        Ok(access_token)
    }

    /// Build and send a request with the current token, refreshing and retrying
    /// once on a 401. This is the single send path; [`api_call`](Self::api_call)
    /// wraps it with transient-failure retries, and the resumable part sinks call
    /// it through [`api_call_no_transient_retry`](Self::api_call_no_transient_retry).
    async fn send_with_refresh<F>(
        &self,
        build_request: &F,
    ) -> Result<reqwest::Response, CloudHomeError>
    where
        F: Fn(&str) -> reqwest::RequestBuilder,
    {
        let token = self.access_token().await?;
        let resp = build_request(&token)
            .send()
            .await
            .map_err(|e| CloudHomeError::Storage(format!("request failed: {e}")))?;

        if resp.status() == StatusCode::UNAUTHORIZED {
            let new_token = self.refresh().await?;
            build_request(&new_token)
                .send()
                .await
                .map_err(|e| CloudHomeError::Storage(format!("retry request failed: {e}")))
        } else {
            Ok(resp)
        }
    }

    /// Send a request, retrying transient failures (429 and 5xx) with bounded
    /// exponential backoff, jitter, and honored `Retry-After`, so Drive, Dropbox,
    /// and OneDrive all inherit quota/outage tolerance. Non-transient responses
    /// (2xx, 404, other 4xx) return unchanged for the caller to interpret.
    ///
    /// The request is rebuilt from `build_request` on every attempt, so its body
    /// must be replayable — which the `Fn` signature enforces. Requests whose
    /// success mutates non-idempotent server state (resumable upload parts) must
    /// not come through here; see
    /// [`api_call_no_transient_retry`](Self::api_call_no_transient_retry).
    pub async fn api_call<F>(&self, build_request: F) -> Result<reqwest::Response, CloudHomeError>
    where
        F: Fn(&str) -> reqwest::RequestBuilder,
    {
        let mut attempt = 0u32;
        loop {
            let resp = self.send_with_refresh(&build_request).await?;
            let status = resp.status();
            if attempt < MAX_TRANSIENT_RETRIES && is_transient(status) {
                let delay = self.retry_delay(&resp, attempt);
                warn!(
                    "{} request returned {status}, retrying in {delay:?} (attempt {}/{})",
                    self.provider_label,
                    attempt + 1,
                    MAX_TRANSIENT_RETRIES,
                );
                (self.sleeper)(delay).await;
                attempt += 1;
                continue;
            }
            return Ok(resp);
        }
    }

    /// Send a request whose transient-failure retry must be owned by a higher
    /// layer rather than this session. A resumable upload part advances a
    /// server-side session offset on success, so re-sending it after a lost
    /// response collides with that advanced offset; the recovery is to re-run the
    /// whole upload from the source, which the blob engine does when this call's
    /// error surfaces. Still refreshes and retries once on a 401.
    pub async fn api_call_no_transient_retry<F>(
        &self,
        build_request: F,
    ) -> Result<reqwest::Response, CloudHomeError>
    where
        F: Fn(&str) -> reqwest::RequestBuilder,
    {
        self.send_with_refresh(&build_request).await
    }

    /// The wait before the next retry: a server-supplied `Retry-After` when
    /// present, else exponential backoff with full jitter — a random point in
    /// `[0, base·2^attempt]` so a fleet hitting the same quota window doesn't
    /// resynchronize onto the same retry instant. Both are clamped to
    /// [`RETRY_MAX_DELAY`].
    fn retry_delay(&self, resp: &reqwest::Response, attempt: u32) -> Duration {
        if let Some(after) = parse_retry_after(resp) {
            return after.min(RETRY_MAX_DELAY);
        }
        let ceiling = RETRY_BASE_DELAY
            .saturating_mul(1u32 << attempt.min(16))
            .min(RETRY_MAX_DELAY);
        let jittered = rand::rng().random_range(0..=ceiling.as_millis() as u64);
        Duration::from_millis(jittered)
    }
}

#[cfg(all(test, not(target_arch = "wasm32"), feature = "oauth-providers"))]
mod tests {
    use super::*;
    use crate::clock::{FixedClock, SystemClock};
    use crate::oauth::test_support::{oauth_config, serve_token_response};
    use axum::body::Body;
    use axum::http::Response;
    use axum::Router;
    use chrono::{TimeZone, Utc};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::oneshot;

    /// A [`Sleeper`] that records the delays it's asked to wait instead of
    /// sleeping, so a test can assert the retry schedule and honored `Retry-After`
    /// without spending real seconds.
    fn recording_sleeper() -> (Sleeper, Arc<std::sync::Mutex<Vec<Duration>>>) {
        let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = recorded.clone();
        let sleeper: Sleeper = Arc::new(move |delay| {
            sink.lock().expect("record sleep delay").push(delay);
            Box::pin(async {})
        });
        (sleeper, recorded)
    }

    /// A session pointed at a mock endpoint with a non-expiring token (so
    /// `access_token` never refreshes) and the injected `sleeper`.
    fn retry_test_session(sleeper: Sleeper) -> OAuthSession {
        let mut session = OAuthSession::new(
            OAuthTokens {
                access_token: "access".to_string(),
                refresh_token: None,
                expires_at: None,
            },
            KeyService::new("oauth-retry".to_string()),
            Arc::new(SystemClock),
            oauth_config("http://token.invalid/token".to_string()),
            "Provider",
        );
        session.sleeper = sleeper;
        session
    }

    /// Serve the programmed `(status, retry_after_secs)` responses in order, then
    /// repeat the last one for any further request. Returns the URL, a shared hit
    /// counter, and a shutdown sender.
    async fn spawn_status_server(
        responses: Vec<(u16, Option<u64>)>,
    ) -> (String, Arc<AtomicUsize>, oneshot::Sender<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind status server");
        let url = format!("http://{}", listener.local_addr().expect("local addr"));
        let hits = Arc::new(AtomicUsize::new(0));
        let plan = Arc::new(responses);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handler_hits = hits.clone();
        let app = Router::new().fallback(move || {
            let plan = plan.clone();
            let hits = handler_hits.clone();
            async move {
                let i = hits.fetch_add(1, Ordering::SeqCst);
                let (status, retry_after) = plan
                    .get(i)
                    .or_else(|| plan.last())
                    .copied()
                    .expect("at least one programmed response");
                let mut builder = Response::builder().status(status);
                if let Some(secs) = retry_after {
                    builder = builder.header("Retry-After", secs.to_string());
                }
                builder.body(Body::from("body")).expect("build response")
            }
        });
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("status server serves");
        });
        (url, hits, shutdown_tx)
    }

    #[tokio::test]
    async fn retries_after_429_and_honors_retry_after() {
        let (url, hits, shutdown) = spawn_status_server(vec![(429, Some(2)), (200, None)]).await;
        let (sleeper, recorded) = recording_sleeper();
        let session = retry_test_session(sleeper);

        let resp = session
            .api_call(|token| session.client().get(&url).bearer_auth(token))
            .await
            .expect("call succeeds after the 429 clears");

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(hits.load(Ordering::SeqCst), 2, "one retry after the 429");
        assert_eq!(
            recorded.lock().expect("read sleeps").as_slice(),
            &[Duration::from_secs(2)],
            "the single backoff honored Retry-After",
        );
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn persistent_5xx_exhausts_attempts_and_surfaces_the_failure() {
        let (url, hits, shutdown) = spawn_status_server(vec![(500, None)]).await;
        let (sleeper, recorded) = recording_sleeper();
        let session = retry_test_session(sleeper);

        let resp = session
            .api_call(|token| session.client().get(&url).bearer_auth(token))
            .await
            .expect("the exhausted response returns for the caller to fail on");

        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            (MAX_TRANSIENT_RETRIES + 1) as usize,
            "initial attempt plus every retry",
        );
        let waits = recorded.lock().expect("read sleeps");
        assert_eq!(waits.len(), MAX_TRANSIENT_RETRIES as usize);
        assert!(
            waits.iter().all(|w| *w <= RETRY_MAX_DELAY),
            "no computed backoff exceeds the ceiling: {waits:?}",
        );
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn non_transient_4xx_is_not_retried() {
        let (url, hits, shutdown) = spawn_status_server(vec![(400, None)]).await;
        let (sleeper, recorded) = recording_sleeper();
        let session = retry_test_session(sleeper);

        let resp = session
            .api_call(|token| session.client().get(&url).bearer_auth(token))
            .await
            .expect("a 400 returns unchanged");

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(hits.load(Ordering::SeqCst), 1, "the 400 is sent once");
        assert!(
            recorded.lock().expect("read sleeps").is_empty(),
            "a 400 triggers no backoff",
        );
        let _ = shutdown.send(());
    }

    fn fail_next_cloud_credentials_write(key_service: &KeyService) {
        let entry = key_service
            .cloud_home_credentials_entry_for_test()
            .expect("create mock keyring entry");
        let credential = entry
            .as_any()
            .downcast_ref::<keyring_core::mock::Cred>()
            .expect("mock keyring credential");
        credential.set_error(keyring_core::Error::Invalid(
            "keyring unavailable".to_string(),
            "test failure".to_string(),
        ));
    }

    #[tokio::test]
    async fn refresh_returns_error_when_token_persist_fails() {
        let (token_url, request_body, server) = serve_token_response(
            r#"{"access_token":"new-access","refresh_token":"new-refresh","expires_in":3600}"#,
        )
        .await;
        crate::keys::test_keyring::install();
        let key_service = KeyService::new("oauth-persist-failure".to_string());
        fail_next_cloud_credentials_write(&key_service);
        let session = OAuthSession::new(
            OAuthTokens {
                access_token: "old-access".to_string(),
                refresh_token: Some("old-refresh".to_string()),
                expires_at: Some(1_700_000_000),
            },
            key_service,
            Arc::new(FixedClock(Utc.timestamp_opt(1_700_000_120, 0).unwrap())),
            oauth_config(token_url),
            "Provider",
        );

        let error = session
            .refresh()
            .await
            .expect_err("persist failure returns an error");
        assert!(error.to_string().contains("keyring unavailable"));

        let tokens = session.tokens.read().await;
        assert_eq!(tokens.access_token, "old-access");
        assert_eq!(tokens.refresh_token.as_deref(), Some("old-refresh"));

        let _request = request_body.await.expect("receive refresh request");
        server.await.expect("token server exits");
    }
}
