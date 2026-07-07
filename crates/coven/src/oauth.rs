//! OAuth 2.0 helper for consumer cloud provider authentication.
//!
//! Provides PKCE-based authorization code flow with a localhost callback server.
//! Used by Google Drive, Dropbox, and OneDrive cloud home backends.

use std::collections::HashMap;
use std::sync::OnceLock;

#[cfg(any(test, all(not(target_arch = "wasm32"), feature = "oauth-providers")))]
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
#[cfg(any(test, all(not(target_arch = "wasm32"), feature = "oauth-providers")))]
use rand::RngCore;
use serde::{Deserialize, Serialize};
#[cfg(any(test, all(not(target_arch = "wasm32"), feature = "oauth-providers")))]
use sha2::{Digest, Sha256};
#[cfg(any(test, all(not(target_arch = "wasm32"), feature = "oauth-providers")))]
use thiserror::Error;
// `info`/`warn` are used only by the native-only localhost-callback `authorize`,
// which is gated on `oauth-providers` too.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
use tracing::{info, warn};

/// OAuth provider configuration.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
#[derive(Clone, Debug)]
pub struct OAuthConfig {
    pub client_id: String,
    /// None for public clients (PKCE-only, no client secret needed).
    pub client_secret: Option<String>,
    pub auth_url: String,
    pub token_url: String,
    pub scopes: Vec<String>,
    /// Localhost callback port. Default: 19284.
    pub redirect_port: u16,
    /// Extra params appended to the authorization URL (e.g. Google's
    /// `access_type=offline` or Dropbox's `token_access_type=offline`).
    pub extra_auth_params: Vec<(String, String)>,
}

/// OAuth client credentials for one provider — the consuming app's registered
/// OAuth application. coven ships no app credentials of its own; the host
/// registers them at startup via [`set_oauth_client_creds`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OAuthClientCreds {
    pub client_id: String,
    /// None for public (PKCE-only) clients.
    pub client_secret: Option<String>,
}

/// Registering OAuth client credentials a second time with values that differ
/// from the first registration — a startup contradiction the host must resolve,
/// not a value coven may silently pick between.
#[derive(Debug, thiserror::Error)]
#[error(
    "OAuth client credentials are already registered; cannot re-register with different values"
)]
pub struct OAuthClientCredsConflict;

static OAUTH_CLIENT_CREDS: OnceLock<HashMap<String, OAuthClientCreds>> = OnceLock::new();

/// Register the host's OAuth client credentials, keyed by provider name
/// (`"google_drive"`, `"dropbox"`, `"onedrive"`). Call once at startup, before
/// any OAuth flow. Providers absent from the map get empty credentials.
/// Re-registering the same map is a no-op; a differing map is a startup
/// contradiction and returns [`OAuthClientCredsConflict`].
pub fn set_oauth_client_creds(
    creds: HashMap<String, OAuthClientCreds>,
) -> Result<(), OAuthClientCredsConflict> {
    if let Err(attempted) = OAUTH_CLIENT_CREDS.set(creds) {
        let registered = OAUTH_CLIENT_CREDS
            .get()
            .expect("OnceLock::set failed only when a value is already stored");
        if *registered != attempted {
            return Err(OAuthClientCredsConflict);
        }
    }
    Ok(())
}

/// A provider's OAuth client credentials were requested before the host
/// registered them via [`set_oauth_client_creds`]: either no registration ran at
/// all, or the registered map has no entry for this provider. Surfaced at the
/// OAuth-flow boundary so a mis-configured host gets a typed error naming the
/// startup step, not an empty `client_id` that fails deep inside a provider flow.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
#[derive(Debug, thiserror::Error)]
pub enum OAuthClientCredsError {
    #[error(
        "no OAuth client credentials are registered; the host must call set_oauth_client_creds at startup before any OAuth flow"
    )]
    NotRegistered,
    #[error(
        "no OAuth client credentials registered for provider {0:?}; the host must include it in the set_oauth_client_creds map at startup"
    )]
    MissingProvider(String),
}

/// The credentials registered for a provider. `Err` when the host never ran
/// [`set_oauth_client_creds`], or ran it without an entry for this provider.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
pub fn oauth_client_creds(provider: &str) -> Result<OAuthClientCreds, OAuthClientCredsError> {
    let registered = OAUTH_CLIENT_CREDS
        .get()
        .ok_or(OAuthClientCredsError::NotRegistered)?;
    registered
        .get(provider)
        .cloned()
        .ok_or_else(|| OAuthClientCredsError::MissingProvider(provider.to_string()))
}

/// Register a consistent set of client credentials for the OAuth provider
/// backends' tests, which construct provider homes and so read creds. Idempotent
/// — every caller registers the same map, so the process-global `OnceLock` is set
/// once and later calls are no-ops.
#[cfg(all(test, not(target_arch = "wasm32"), feature = "oauth-providers"))]
pub(crate) fn install_test_client_creds() {
    let creds = HashMap::from([
        (
            "google_drive".to_string(),
            OAuthClientCreds {
                client_id: "test-client".to_string(),
                client_secret: None,
            },
        ),
        (
            "dropbox".to_string(),
            OAuthClientCreds {
                client_id: "test-client".to_string(),
                client_secret: None,
            },
        ),
        (
            "onedrive".to_string(),
            OAuthClientCreds {
                client_id: "test-client".to_string(),
                client_secret: None,
            },
        ),
    ]);
    set_oauth_client_creds(creds).expect("register consistent test client credentials");
}

/// Tokens returned from an OAuth authorization or refresh.
///
/// `Debug` is hand-written: `access_token` and `refresh_token` are bearer
/// credentials and print as `<redacted>` so `{:?}` in an error path cannot
/// leak them.
#[derive(Clone, Serialize, Deserialize)]
pub struct OAuthTokens {
    pub access_token: String,
    pub refresh_token: Option<String>,
    /// Unix timestamp when the access token expires. None if unknown.
    pub expires_at: Option<i64>,
}

impl std::fmt::Debug for OAuthTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthTokens")
            .field("access_token", &"<redacted>")
            // Presence (whether the session can refresh) is observable; the
            // token itself is redacted.
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "<redacted>"),
            )
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[cfg(any(test, all(not(target_arch = "wasm32"), feature = "oauth-providers")))]
#[derive(Error, Debug)]
pub enum OAuthError {
    #[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
    #[error("failed to open browser: {0}")]
    BrowserOpen(String),
    #[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
    #[error("callback server error: {0}")]
    Server(String),
    #[error("token exchange error: {0}")]
    TokenExchange(String),
    #[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
    #[error("account email fetch error: {0}")]
    AccountFetch(String),
    #[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
    #[error("authorization denied: {0}")]
    Denied(String),
    #[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
    #[error("timeout waiting for authorization callback")]
    Timeout,
    /// The refresh token is no longer accepted (revoked, expired, password
    /// changed, …). Only a fresh OAuth authorization flow recovers — there
    /// is no point retrying the refresh.
    #[error("re-authorization required: {0}")]
    Reauthorize(String),
    #[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
    #[error(transparent)]
    ClientCreds(#[from] OAuthClientCredsError),
}

/// Guards the localhost OAuth-callback server task — native-only, like
/// [`authorize`], the only thing that spawns that task; also gated on
/// `oauth-providers`.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
struct AbortOnDrop(Option<tokio::task::JoinHandle<()>>);

#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
impl AbortOnDrop {
    fn new(handle: tokio::task::JoinHandle<()>) -> Self {
        Self(Some(handle))
    }

    /// Take the join handle for the success path, where the caller wants to
    /// await its termination (e.g. with a timeout) so the listener's port is
    /// released before another flow tries to bind it. Disarms the Drop.
    fn take_handle(mut self) -> Option<tokio::task::JoinHandle<()>> {
        self.0.take()
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(h) = self.0.take() {
            h.abort();
        }
    }
}

/// Token response from the OAuth provider (internal deserialization).
///
/// `access_token` is optional because error responses (`{"error": "invalid_grant", …}`)
/// omit it — making it required forces parsing to fail before the typed
/// error branch can classify the failure, surfacing every provider error as
/// "parse response: missing field `access_token`".
#[cfg(any(test, all(not(target_arch = "wasm32"), feature = "oauth-providers")))]
#[derive(Deserialize)]
struct TokenResponse {
    #[serde(default)]
    access_token: Option<String>,
    refresh_token: Option<String>,
    expires_in: Option<i64>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    error_description: Option<String>,
}

#[cfg(any(test, all(not(target_arch = "wasm32"), feature = "oauth-providers")))]
impl TokenResponse {
    /// Convert a parsed response into typed `OAuthTokens`, classifying the
    /// failure modes both exchange and refresh share: `invalid_grant` /
    /// `unauthorized_client` (the refresh-token-no-longer-accepted family)
    /// become `OAuthError::Reauthorize`; other provider errors become
    /// `TokenExchange`; a missing `access_token` on a non-error response is
    /// reported as a malformed success.
    fn into_tokens(
        self,
        status: reqwest::StatusCode,
        clock: &dyn crate::clock::Clock,
    ) -> Result<OAuthTokens, OAuthError> {
        if let Some(error) = self.error {
            let detail = match self.error_description.as_deref() {
                Some(d) => format!("{error}: {d}"),
                None => error.clone(),
            };
            if matches!(error.as_str(), "invalid_grant" | "unauthorized_client") {
                return Err(OAuthError::Reauthorize(detail));
            }
            return Err(OAuthError::TokenExchange(format!(
                "provider error (HTTP {status}): {detail}"
            )));
        }

        let access_token = self.access_token.ok_or_else(|| {
            OAuthError::TokenExchange(format!(
                "provider response missing access_token (HTTP {status})"
            ))
        })?;

        let expires_at = self.expires_in.map(|secs| clock.now().timestamp() + secs);

        Ok(OAuthTokens {
            access_token,
            refresh_token: self.refresh_token,
            expires_at,
        })
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
fn oauth_callback_html(title: &str, message: &str) -> String {
    include_str!("oauth_success.html")
        .replace("{{title}}", title)
        .replace("{{message}}", message)
}

#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
async fn post_token_request(
    config: &OAuthConfig,
    params: Vec<(&str, String)>,
    clock: &dyn crate::clock::Clock,
) -> Result<OAuthTokens, OAuthError> {
    let client = reqwest::Client::new();
    let resp = client
        .post(&config.token_url)
        .form(&params)
        .send()
        .await
        .map_err(|e| OAuthError::TokenExchange(format!("request failed: {e}")))?;

    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| OAuthError::TokenExchange(format!("read body: {e}")))?;

    let token_resp: TokenResponse = serde_json::from_str(&body).map_err(|e| {
        OAuthError::TokenExchange(format!("parse token response (HTTP {status}): {e}"))
    })?;

    token_resp.into_tokens(status, clock)
}

/// Generate a random PKCE code verifier (43-128 URL-safe characters).
#[cfg(any(test, all(not(target_arch = "wasm32"), feature = "oauth-providers")))]
pub fn generate_code_verifier() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Compute the S256 PKCE code challenge from a verifier.
#[cfg(any(test, all(not(target_arch = "wasm32"), feature = "oauth-providers")))]
pub fn code_challenge(verifier: &str) -> String {
    let hash = Sha256::digest(verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(hash)
}

/// An authorization request the host drives itself: the URL to open plus the
/// PKCE verifier and state value coven checks during exchange. For hosts that
/// capture the redirect outside coven's localhost callback server — e.g. a
/// mobile OS auth session (ASWebAuthenticationSession / Custom Tabs)
/// redirecting to a custom URI scheme, where binding a localhost port and
/// `open::that` don't apply.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
#[derive(Clone, Debug)]
pub struct AuthorizeRequest {
    pub auth_url: String,
    verifier: String,
    state: String,
}

#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
impl AuthorizeRequest {
    pub fn verify_callback_state(&self, callback_state: Option<&str>) -> Result<(), OAuthError> {
        verify_callback_state(callback_state, &self.state).map_err(OAuthError::Denied)
    }
}

/// Build the authorization URL + PKCE verifier for `config`, redirecting to
/// `redirect_uri`. The caller opens the URL, captures the `code` and `state`
/// from the redirect itself, then calls [`exchange_authorize_request`] with the
/// same `redirect_uri` and returned request. [`authorize`] is this plus coven's
/// localhost callback server for desktop.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
pub fn build_authorize_url(
    config: &OAuthConfig,
    redirect_uri: &str,
) -> Result<AuthorizeRequest, OAuthError> {
    let verifier = generate_code_verifier();
    let state = generate_code_verifier();
    let challenge = code_challenge(&verifier);

    let mut auth_params = vec![
        ("response_type", "code".to_string()),
        ("client_id", config.client_id.clone()),
        ("redirect_uri", redirect_uri.to_string()),
        ("code_challenge", challenge),
        ("code_challenge_method", "S256".to_string()),
        ("state", state.clone()),
    ];

    for (k, v) in &config.extra_auth_params {
        auth_params.push((k.as_str(), v.clone()));
    }

    if !config.scopes.is_empty() {
        auth_params.push(("scope", config.scopes.join(" ")));
    }

    let auth_url = format!(
        "{}?{}",
        config.auth_url,
        serde_urlencoded::to_string(&auth_params)
            .map_err(|e| OAuthError::Server(format!("failed to encode params: {e}")))?
    );

    Ok(AuthorizeRequest {
        auth_url,
        verifier,
        state,
    })
}

#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
fn callback_code(params: &std::collections::HashMap<String, String>) -> Result<String, String> {
    if let Some(error) = params.get("error") {
        let desc = match params.get("error_description") {
            Some(desc) => desc.clone(),
            None => {
                warn!("OAuth callback error omitted error_description: {error}");
                error.clone()
            }
        };
        Err(desc)
    } else if let Some(code) = params.get("code") {
        Ok(code.clone())
    } else {
        Err("no code in callback".to_string())
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
fn verify_callback_state(callback_state: Option<&str>, expected_state: &str) -> Result<(), String> {
    match callback_state {
        Some(state) if state == expected_state => Ok(()),
        Some(_) => Err("state mismatch in callback".to_string()),
        None => Err("missing state in callback".to_string()),
    }
}

/// Open the user's browser, wait for the OAuth callback, and exchange the
/// authorization code for tokens.
///
/// Flow:
/// 1. Generate PKCE verifier + challenge
/// 2. Open browser to `auth_url` with the required parameters
/// 3. Spawn a one-shot HTTP server on `localhost:{redirect_port}`
/// 4. Wait for the callback with the authorization code
/// 5. Exchange the code for tokens at `token_url`
///
/// Native-only: binds a localhost TCP port (`tokio::net`), serves the callback
/// with axum, and opens the system browser (`open`) — none of which exist on
/// wasm. The browser captures the redirect itself via the pure
/// [`build_authorize_url`] + [`exchange_authorize_request`] pair. Also gated on
/// `oauth-providers` (it pulls in `open`).
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
pub async fn authorize(
    config: &OAuthConfig,
    cancel: tokio::sync::watch::Receiver<bool>,
    clock: &dyn crate::clock::Clock,
) -> Result<OAuthTokens, OAuthError> {
    let redirect_uri = format!("http://localhost:{}/callback", config.redirect_port);
    let AuthorizeRequest {
        auth_url,
        verifier,
        state,
    } = build_authorize_url(config, &redirect_uri)?;

    // Channel to receive the authorization code from the callback handler
    let (tx, rx) = tokio::sync::oneshot::channel::<Result<String, String>>();
    let tx = std::sync::Arc::new(tokio::sync::Mutex::new(Some(tx)));

    let tx_for_handler = tx.clone();
    let expected_state = state.clone();
    let app = axum::Router::new().route(
        "/callback",
        axum::routing::get(
            move |axum::extract::Query(params): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >| {
                let tx = tx_for_handler.clone();
                let expected_state = expected_state.clone();
                async move {
                    let mut guard = tx.lock().await;
                    let callback =
                        verify_callback_state(params.get("state").map(String::as_str), &expected_state)
                            .and_then(|()| callback_code(&params));
                    let is_error = callback.is_err();
                    if let Some(sender) = guard.take() {
                        if sender.send(callback).is_err() {
                            warn!("OAuth callback receiver dropped before result delivery");
                        }
                    }
                    let html = if is_error {
                        oauth_callback_html(
                            "Authorization denied",
                            "Authorization was denied. You can close this window and try again in the app.",
                        )
                    } else {
                        oauth_callback_html(
                            "Authorization complete",
                            "You can close this window and return to the app.",
                        )
                    };
                    (
                        [
                            (axum::http::header::CACHE_CONTROL, "no-store"),
                            (axum::http::header::CONNECTION, "close"),
                        ],
                        axum::response::Html(html),
                    )
                }
            },
        ),
    );

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", config.redirect_port))
        .await
        .map_err(|e| OAuthError::Server(format!("failed to bind port: {e}")))?;

    // Spawn the server. The guard aborts the task on drop so future
    // cancellation (parent .await dropped) tears the listener down too.
    let server_guard = AbortOnDrop::new(tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app)
            .with_graceful_shutdown(async {
                tokio::time::sleep(std::time::Duration::from_secs(300)).await;
            })
            .await
        {
            warn!("OAuth callback server exited with error: {e}");
        }
    }));

    // Open the browser
    open::that(&auth_url).map_err(|e| OAuthError::BrowserOpen(format!("{e}")))?;

    info!("Opened browser for OAuth authorization, waiting for callback");

    // Wait for the callback, cancellation, or timeout
    let mut cancel = cancel;
    let result = tokio::select! {
        result = rx => {
            result
                .map_err(|_| OAuthError::Server("callback channel closed".to_string()))
                .and_then(|r| r.map_err(OAuthError::Denied))
        }
        _ = cancel.wait_for(|&v| v) => {
            Err(OAuthError::Denied("cancelled".to_string()))
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(300)) => {
            Err(OAuthError::Timeout)
        }
    };

    // Disarm the abort-on-drop and await the listener task's termination
    // briefly. Bounded timeout because awaiting the abort could otherwise
    // deadlock on a small thread pool with no idle worker. This matters
    // for back-to-back sign-in flows: without the wait, the next bind on
    // the same port can race the not-yet-released listener (no SO_REUSEADDR).
    if let Some(handle) = server_guard.take_handle() {
        handle.abort();
        match tokio::time::timeout(std::time::Duration::from_millis(500), handle).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.is_cancelled() => {}
            Ok(Err(e)) => {
                warn!("OAuth callback server task panicked on shutdown: {e}");
            }
            Err(_) => {
                warn!(
                    "OAuth callback server did not exit within 500ms; \
                     port {} may briefly remain in use",
                    config.redirect_port
                );
            }
        }
    }

    let code = result?;

    info!("Received authorization code, exchanging for tokens");

    // Exchange the code for tokens
    exchange_code(config, &code, &verifier, &redirect_uri, clock).await
}

#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
async fn exchange_code(
    config: &OAuthConfig,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
    clock: &dyn crate::clock::Clock,
) -> Result<OAuthTokens, OAuthError> {
    let mut params = vec![
        ("grant_type", "authorization_code".to_string()),
        ("code", code.to_string()),
        ("redirect_uri", redirect_uri.to_string()),
        ("client_id", config.client_id.clone()),
        ("code_verifier", verifier.to_string()),
    ];
    if let Some(secret) = &config.client_secret {
        params.push(("client_secret", secret.clone()));
    }

    post_token_request(config, params, clock).await
}

/// Verify the callback state from an [`AuthorizeRequest`] and exchange its code
/// for tokens.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
pub async fn exchange_authorize_request(
    config: &OAuthConfig,
    code: &str,
    callback_state: Option<&str>,
    request: &AuthorizeRequest,
    redirect_uri: &str,
    clock: &dyn crate::clock::Clock,
) -> Result<OAuthTokens, OAuthError> {
    request.verify_callback_state(callback_state)?;
    exchange_code(config, code, &request.verifier, redirect_uri, clock).await
}

/// Refresh an expired access token using a refresh token.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
pub async fn refresh(
    config: &OAuthConfig,
    refresh_token: &str,
    clock: &dyn crate::clock::Clock,
) -> Result<OAuthTokens, OAuthError> {
    let mut params = vec![
        ("grant_type", "refresh_token".to_string()),
        ("refresh_token", refresh_token.to_string()),
        ("client_id", config.client_id.clone()),
    ];
    if let Some(secret) = &config.client_secret {
        params.push(("client_secret", secret.clone()));
    }

    let mut tokens = post_token_request(config, params, clock).await?;
    // Provider didn't return a new refresh token (common — many providers
    // only rotate it on the initial exchange). Reuse the existing one so the
    // session can refresh again next cycle.
    if tokens.refresh_token.is_none() {
        tracing::debug!("provider did not return a new refresh_token; reusing existing token");
        tokens.refresh_token = Some(refresh_token.to_string());
    }
    Ok(tokens)
}

/// The OAuth config for a provider that uses OAuth, or an error for providers
/// (S3, CloudKit) that don't.
///
/// Native-only: reads each provider's config off its native-only backend type
/// (`GoogleDriveCloudHome::oauth_config()` etc.); also gated on `oauth-providers`.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
fn oauth_config_for_provider(
    provider: crate::config::CloudProvider,
) -> Result<OAuthConfig, OAuthError> {
    use crate::config::CloudProvider;
    use crate::storage::cloud::{dropbox, google_drive, onedrive};

    match provider {
        CloudProvider::GoogleDrive => Ok(google_drive::GoogleDriveCloudHome::oauth_config()?),
        CloudProvider::Dropbox => Ok(dropbox::DropboxCloudHome::oauth_config()?),
        CloudProvider::OneDrive => Ok(onedrive::OneDriveCloudHome::oauth_config()?),
        other => Err(OAuthError::Denied(format!("{other:?} does not use OAuth"))),
    }
}

/// Run an OAuth authorization flow for the given cloud provider (desktop: opens
/// a browser and captures the redirect on coven's localhost callback server).
///
/// Returns tokens on success. Only Google Drive, Dropbox, and OneDrive support
/// OAuth; other providers return an error.
///
/// Native-only: uses [`authorize`]'s localhost-callback flow and the native-only
/// [`oauth_config_for_provider`]; also gated on `oauth-providers`.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
pub async fn authorize_provider(
    provider: crate::config::CloudProvider,
    cancel: tokio::sync::watch::Receiver<bool>,
    clock: &dyn crate::clock::Clock,
) -> Result<OAuthTokens, OAuthError> {
    authorize(&oauth_config_for_provider(provider)?, cancel, clock).await
}

/// Build an OAuth authorization request for `provider` redirecting to
/// `redirect_uri`, for hosts that capture the redirect themselves (a mobile OS
/// auth session). Pair the returned request, callback `code`, callback `state`,
/// and same `redirect_uri` with [`exchange_code_for_provider`].
///
/// Native-only: depends on [`oauth_config_for_provider`], whose per-provider
/// config lives on the native-only cloud-backend types; also gated on
/// `oauth-providers`.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
pub fn build_authorize_request_for_provider(
    provider: crate::config::CloudProvider,
    redirect_uri: &str,
) -> Result<AuthorizeRequest, OAuthError> {
    build_authorize_url(&oauth_config_for_provider(provider)?, redirect_uri)
}

/// Exchange an authorization `code` captured by the host for `provider`'s
/// tokens. `redirect_uri`, `request`, and `callback_state` must match the
/// originating [`build_authorize_request_for_provider`] call.
///
/// Native-only for the same reason as [`build_authorize_request_for_provider`];
/// also gated on `oauth-providers`.
#[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
pub async fn exchange_code_for_provider(
    provider: crate::config::CloudProvider,
    code: &str,
    callback_state: Option<&str>,
    request: &AuthorizeRequest,
    redirect_uri: &str,
    clock: &dyn crate::clock::Clock,
) -> Result<OAuthTokens, OAuthError> {
    exchange_authorize_request(
        &oauth_config_for_provider(provider)?,
        code,
        callback_state,
        request,
        redirect_uri,
        clock,
    )
    .await
}

#[cfg(all(test, not(target_arch = "wasm32"), feature = "oauth-providers"))]
pub(crate) mod test_support {
    use super::OAuthConfig;
    use axum::{extract::Form, routing::post, Router};
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::{oneshot, Mutex};

    pub(crate) async fn serve_token_response(
        response_body: &'static str,
    ) -> (
        String,
        oneshot::Receiver<HashMap<String, String>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind token server");
        let url = format!(
            "http://{}/token",
            listener.local_addr().expect("local addr")
        );

        let (request_tx, request_rx) = oneshot::channel();
        let request_tx = Arc::new(Mutex::new(Some(request_tx)));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));

        let app = Router::new().route(
            "/token",
            post(move |Form(params): Form<HashMap<String, String>>| {
                let request_tx = request_tx.clone();
                let shutdown_tx = shutdown_tx.clone();
                async move {
                    let request_tx = request_tx
                        .lock()
                        .await
                        .take()
                        .expect("token request sender available");
                    request_tx.send(params).expect("send token request to test");
                    let shutdown_tx = shutdown_tx
                        .lock()
                        .await
                        .take()
                        .expect("token server shutdown sender available");
                    shutdown_tx.send(()).expect("send token server shutdown");
                    ([("content-type", "application/json")], response_body)
                }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    shutdown_rx.await.expect("receive token server shutdown");
                })
                .await
                .expect("serve token response");
        });
        (url, request_rx, server)
    }

    pub(crate) fn oauth_config(token_url: String) -> OAuthConfig {
        OAuthConfig {
            client_id: "client-id".to_string(),
            client_secret: Some("client-secret".to_string()),
            auth_url: "http://auth.example/authorize".to_string(),
            token_url,
            scopes: vec![],
            redirect_port: 19284,
            extra_auth_params: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
    use test_support::{oauth_config, serve_token_response};

    #[test]
    fn pkce_verifier_is_url_safe() {
        let verifier = generate_code_verifier();
        assert!(verifier.len() >= 43);
        assert!(verifier
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn pkce_challenge_is_deterministic() {
        let verifier = "test-verifier-string";
        let c1 = code_challenge(verifier);
        let c2 = code_challenge(verifier);
        assert_eq!(c1, c2);
    }

    #[test]
    fn pkce_challenge_is_base64url() {
        let verifier = generate_code_verifier();
        let challenge = code_challenge(&verifier);
        assert!(challenge
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn oauth_tokens_serialization_roundtrip() {
        let tokens = OAuthTokens {
            access_token: "at_123".to_string(),
            refresh_token: Some("rt_456".to_string()),
            expires_at: Some(1700000000),
        };
        let json = serde_json::to_string(&tokens).unwrap();
        let parsed: OAuthTokens = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.access_token, "at_123");
        assert_eq!(parsed.refresh_token, Some("rt_456".to_string()));
        assert_eq!(parsed.expires_at, Some(1700000000));
    }

    #[test]
    fn debug_redacts_access_and_refresh_tokens() {
        let tokens = OAuthTokens {
            access_token: "access-token-do-not-print".to_string(),
            refresh_token: Some("refresh-token-do-not-print".to_string()),
            expires_at: Some(1700000000),
        };
        let debug = format!("{tokens:?}");

        assert!(debug.contains("<redacted>"), "{debug}");
        // Non-secret expiry stays visible.
        assert!(debug.contains("1700000000"), "{debug}");
        assert!(
            !debug.contains("access-token-do-not-print"),
            "access token leaked: {debug}"
        );
        assert!(
            !debug.contains("refresh-token-do-not-print"),
            "refresh token leaked: {debug}"
        );
        // refresh_token presence is still observable.
        assert!(debug.contains("refresh_token: Some"), "{debug}");
    }

    fn parse_token_response(body: &str) -> TokenResponse {
        serde_json::from_str(body).expect("parse TokenResponse")
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
    fn authorize_url_params(auth_url: &str) -> HashMap<String, String> {
        let (_, query) = auth_url.split_once('?').expect("authorize URL has query");
        serde_urlencoded::from_str(query).expect("parse authorize query")
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
    #[test]
    fn oauth_callback_html_renders_result_text() {
        let html = oauth_callback_html("Authorization denied", "Denied message");

        assert!(html.contains("<h1>Authorization denied</h1>"));
        assert!(html.contains("<p>Denied message</p>"));
        assert!(!html.contains("{{title}}"));
        assert!(!html.contains("{{message}}"));
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
    #[test]
    fn build_authorize_url_includes_matching_random_state() {
        let config = oauth_config("http://token.example/token".to_string());

        let first = build_authorize_url(&config, "http://localhost/callback")
            .expect("build first authorize URL");
        let second = build_authorize_url(&config, "http://localhost/callback")
            .expect("build second authorize URL");

        let first_params = authorize_url_params(&first.auth_url);
        let first_state = first_params
            .get("state")
            .expect("authorize URL includes state");
        assert!(
            first
                .verify_callback_state(Some(first_state.as_str()))
                .is_ok(),
            "AuthorizeRequest verifies the state it put in the URL",
        );
        assert!(
            !first_state.is_empty(),
            "state must be present for callback validation",
        );
        let second_params = authorize_url_params(&second.auth_url);
        let second_state = second_params
            .get("state")
            .expect("second authorize URL includes state");
        assert_ne!(
            first_state, second_state,
            "separate OAuth flows must carry separate state values",
        );
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
    #[test]
    fn callback_rejects_missing_or_wrong_state() {
        let expected_state = "expected-state";
        let valid = HashMap::from([
            ("code".to_string(), "auth-code".to_string()),
            ("state".to_string(), expected_state.to_string()),
        ]);
        let wrong = HashMap::from([
            ("code".to_string(), "auth-code".to_string()),
            ("state".to_string(), "wrong-state".to_string()),
        ]);
        let missing = HashMap::from([("code".to_string(), "auth-code".to_string())]);

        assert_eq!(
            verify_callback_state(valid.get("state").map(String::as_str), expected_state)
                .and_then(|()| callback_code(&valid))
                .as_deref(),
            Ok("auth-code"),
        );
        assert!(
            verify_callback_state(wrong.get("state").map(String::as_str), expected_state).is_err(),
            "wrong state must reject the callback",
        );
        assert!(
            verify_callback_state(missing.get("state").map(String::as_str), expected_state)
                .is_err(),
            "missing state must reject the callback",
        );
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
    #[tokio::test]
    async fn exchange_authorize_request_rejects_wrong_state_before_token_request() {
        let config = oauth_config("http://127.0.0.1:9/token".to_string());
        let request = build_authorize_url(&config, "http://localhost/callback")
            .expect("build authorize request");

        let result = exchange_authorize_request(
            &config,
            "auth-code",
            Some("wrong-state"),
            &request,
            "http://localhost/callback",
            &crate::clock::SystemClock,
        )
        .await;

        assert!(
            matches!(result, Err(OAuthError::Denied(ref msg)) if msg.contains("state mismatch")),
            "expected state rejection before token request, got {result:?}",
        );
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
    #[tokio::test]
    async fn exchange_code_posts_authorization_code_params() {
        let (token_url, request_body, server) = serve_token_response(
            r#"{"access_token":"new-access","refresh_token":"new-refresh","expires_in":3600}"#,
        )
        .await;
        let config = oauth_config(token_url);

        let tokens = exchange_code(
            &config,
            "auth-code",
            "pkce-verifier",
            "http://localhost/callback",
            &crate::clock::SystemClock,
        )
        .await
        .expect("exchange code");

        let body = request_body.await.expect("request body");
        server.await.expect("token server");
        assert_eq!(
            body.get("grant_type").map(String::as_str),
            Some("authorization_code")
        );
        assert_eq!(body.get("code").map(String::as_str), Some("auth-code"));
        assert_eq!(
            body.get("redirect_uri").map(String::as_str),
            Some("http://localhost/callback")
        );
        assert_eq!(body.get("client_id").map(String::as_str), Some("client-id"));
        assert_eq!(
            body.get("code_verifier").map(String::as_str),
            Some("pkce-verifier")
        );
        assert_eq!(
            body.get("client_secret").map(String::as_str),
            Some("client-secret")
        );
        assert_eq!(tokens.access_token, "new-access");
        assert_eq!(tokens.refresh_token.as_deref(), Some("new-refresh"));
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
    #[tokio::test]
    async fn refresh_posts_refresh_token_params_and_reuses_existing_refresh_token() {
        let (token_url, request_body, server) =
            serve_token_response(r#"{"access_token":"refreshed-access","expires_in":3600}"#).await;
        let config = oauth_config(token_url);

        let tokens = refresh(&config, "existing-refresh", &crate::clock::SystemClock)
            .await
            .expect("refresh");

        let body = request_body.await.expect("request body");
        server.await.expect("token server");
        assert_eq!(
            body.get("grant_type").map(String::as_str),
            Some("refresh_token")
        );
        assert_eq!(
            body.get("refresh_token").map(String::as_str),
            Some("existing-refresh")
        );
        assert_eq!(body.get("client_id").map(String::as_str), Some("client-id"));
        assert_eq!(
            body.get("client_secret").map(String::as_str),
            Some("client-secret")
        );
        assert_eq!(tokens.access_token, "refreshed-access");
        assert_eq!(tokens.refresh_token.as_deref(), Some("existing-refresh"));
    }

    #[test]
    fn into_tokens_classifies_invalid_grant_as_reauthorize() {
        let body =
            r#"{"error":"invalid_grant","error_description":"Token has been expired or revoked"}"#;
        let result = parse_token_response(body)
            .into_tokens(reqwest::StatusCode::BAD_REQUEST, &crate::clock::SystemClock);
        match result {
            Err(OAuthError::Reauthorize(detail)) => {
                assert!(
                    detail.contains("expired or revoked"),
                    "expected error_description in detail, got: {detail}",
                );
            }
            other => panic!("expected Reauthorize, got {other:?}"),
        }
    }

    #[test]
    fn into_tokens_classifies_unauthorized_client_as_reauthorize() {
        let body =
            r#"{"error":"unauthorized_client","error_description":"Client has been revoked"}"#;
        let result = parse_token_response(body)
            .into_tokens(reqwest::StatusCode::BAD_REQUEST, &crate::clock::SystemClock);
        assert!(
            matches!(result, Err(OAuthError::Reauthorize(_))),
            "expected Reauthorize, got {result:?}",
        );
    }

    #[test]
    fn into_tokens_leaves_other_provider_errors_as_token_exchange() {
        let body = r#"{"error":"invalid_request","error_description":"missing parameter"}"#;
        let result = parse_token_response(body)
            .into_tokens(reqwest::StatusCode::BAD_REQUEST, &crate::clock::SystemClock);
        match result {
            Err(OAuthError::TokenExchange(msg)) => {
                assert!(
                    msg.contains("missing parameter"),
                    "expected description in message, got: {msg}",
                );
            }
            other => panic!("expected TokenExchange, got {other:?}"),
        }
    }

    #[test]
    fn into_tokens_returns_tokens_on_success() {
        let body = r#"{"access_token":"new_at","refresh_token":"new_rt","expires_in":3600}"#;
        let tokens = parse_token_response(body)
            .into_tokens(reqwest::StatusCode::OK, &crate::clock::SystemClock)
            .expect("into_tokens");
        assert_eq!(tokens.access_token, "new_at");
        assert_eq!(tokens.refresh_token.as_deref(), Some("new_rt"));
        assert!(tokens.expires_at.is_some());
    }

    #[test]
    fn into_tokens_errors_when_success_response_missing_access_token() {
        let body = r#"{}"#;
        let result = parse_token_response(body)
            .into_tokens(reqwest::StatusCode::OK, &crate::clock::SystemClock);
        match result {
            Err(OAuthError::TokenExchange(msg)) => {
                assert!(
                    msg.contains("missing access_token"),
                    "expected access_token error, got: {msg}",
                );
            }
            other => panic!("expected TokenExchange, got {other:?}"),
        }
    }

    #[test]
    fn into_tokens_missing_access_token_error_does_not_include_tokens() {
        let body = r#"{"refresh_token":"refresh-token-that-must-not-be-logged"}"#;
        let result = parse_token_response(body)
            .into_tokens(reqwest::StatusCode::OK, &crate::clock::SystemClock);
        match result {
            Err(OAuthError::TokenExchange(msg)) => {
                assert!(
                    msg.contains("missing access_token"),
                    "expected access_token error, got: {msg}",
                );
                assert!(
                    msg.contains("HTTP 200"),
                    "expected status in error, got: {msg}",
                );
                assert!(
                    !msg.contains("refresh-token-that-must-not-be-logged"),
                    "error included refresh token: {msg}",
                );
            }
            other => panic!("expected TokenExchange, got {other:?}"),
        }
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "oauth-providers"))]
    #[tokio::test]
    async fn parse_failure_error_does_not_include_tokens() {
        let (token_url, _request_body, server) =
            serve_token_response(r#"{"access_token":"access-token-that-must-not-be-logged""#).await;
        let config = oauth_config(token_url);

        let result = exchange_code(
            &config,
            "auth-code",
            "pkce-verifier",
            "http://localhost/callback",
            &crate::clock::SystemClock,
        )
        .await;

        server.await.expect("token server");
        match result {
            Err(OAuthError::TokenExchange(msg)) => {
                assert!(
                    msg.contains("parse token response"),
                    "expected parse error, got: {msg}",
                );
                assert!(
                    msg.contains("HTTP 200"),
                    "expected status in error, got: {msg}",
                );
                assert!(
                    !msg.contains("access-token-that-must-not-be-logged"),
                    "error included access token: {msg}",
                );
            }
            other => panic!("expected TokenExchange, got {other:?}"),
        }
    }
}
