//! OAuth 2.0 helper for consumer cloud provider authentication.
//!
//! Provides PKCE-based authorization code flow with a localhost callback server.
//! Used by Google Drive, Dropbox, and OneDrive cloud home backends.

use std::collections::HashMap;
use std::sync::OnceLock;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::info;

/// OAuth provider configuration.
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
#[derive(Clone, Debug, Default)]
pub struct OAuthClientCreds {
    pub client_id: String,
    /// None for public (PKCE-only) clients.
    pub client_secret: Option<String>,
}

static OAUTH_CLIENT_CREDS: OnceLock<HashMap<String, OAuthClientCreds>> = OnceLock::new();

/// Register the host's OAuth client credentials, keyed by provider name
/// (`"google_drive"`, `"dropbox"`, `"onedrive"`). Call once at startup, before
/// any OAuth flow. Providers absent from the map get empty credentials.
pub fn set_oauth_client_creds(creds: HashMap<String, OAuthClientCreds>) {
    let _ = OAUTH_CLIENT_CREDS.set(creds);
}

/// The credentials registered for a provider, or empty if none were registered.
pub fn oauth_client_creds(provider: &str) -> OAuthClientCreds {
    OAUTH_CLIENT_CREDS
        .get()
        .and_then(|m| m.get(provider).cloned())
        .unwrap_or_default()
}

/// Tokens returned from an OAuth authorization or refresh.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OAuthTokens {
    pub access_token: String,
    pub refresh_token: Option<String>,
    /// Unix timestamp when the access token expires. None if unknown.
    pub expires_at: Option<i64>,
}

#[derive(Error, Debug)]
pub enum OAuthError {
    #[error("failed to open browser: {0}")]
    BrowserOpen(String),
    #[error("callback server error: {0}")]
    Server(String),
    #[error("token exchange error: {0}")]
    TokenExchange(String),
    #[error("authorization denied: {0}")]
    Denied(String),
    #[error("timeout waiting for authorization callback")]
    Timeout,
}

struct AbortOnDrop(Option<tokio::task::JoinHandle<()>>);

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

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(h) = self.0.take() {
            h.abort();
        }
    }
}

/// Token response from the OAuth provider (internal deserialization).
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: Option<i64>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    error_description: Option<String>,
}

/// Generate a random PKCE code verifier (43-128 URL-safe characters).
pub fn generate_code_verifier() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Compute the S256 PKCE code challenge from a verifier.
pub fn code_challenge(verifier: &str) -> String {
    let hash = Sha256::digest(verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(hash)
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
pub async fn authorize(
    config: &OAuthConfig,
    cancel: tokio::sync::watch::Receiver<bool>,
    clock: &dyn crate::clock::Clock,
) -> Result<OAuthTokens, OAuthError> {
    let verifier = generate_code_verifier();
    let challenge = code_challenge(&verifier);
    let redirect_uri = format!("http://localhost:{}/callback", config.redirect_port);

    let mut auth_params = vec![
        ("response_type", "code".to_string()),
        ("client_id", config.client_id.clone()),
        ("redirect_uri", redirect_uri.clone()),
        ("code_challenge", challenge),
        ("code_challenge_method", "S256".to_string()),
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

    // Channel to receive the authorization code from the callback handler
    let (tx, rx) = tokio::sync::oneshot::channel::<Result<String, String>>();
    let tx = std::sync::Arc::new(tokio::sync::Mutex::new(Some(tx)));

    let tx_for_handler = tx.clone();
    let app = axum::Router::new().route(
        "/callback",
        axum::routing::get(
            move |axum::extract::Query(params): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >| {
                let tx = tx_for_handler.clone();
                async move {
                    let mut guard = tx.lock().await;
                    let is_error = params.contains_key("error") || !params.contains_key("code");
                    if let Some(sender) = guard.take() {
                        if let Some(error) = params.get("error") {
                            let desc = params
                                .get("error_description")
                                .cloned()
                                .unwrap_or_else(|| error.clone());
                            let _ = sender.send(Err(desc));
                        } else if let Some(code) = params.get("code") {
                            let _ = sender.send(Ok(code.clone()));
                        } else {
                            let _ = sender.send(Err("no code in callback".to_string()));
                        }
                    }
                    let html = if is_error {
                        include_str!("oauth_success.html")
                            .replace("Authorization complete", "Authorization denied")
                            .replace(
                                "You can close this window and return to bae.",
                                "Authorization was denied. You can close this window and try again in bae.",
                            )
                    } else {
                        include_str!("oauth_success.html").to_string()
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
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                tokio::time::sleep(std::time::Duration::from_secs(300)).await;
            })
            .await
            .ok();
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
        let _ = tokio::time::timeout(std::time::Duration::from_millis(500), handle).await;
    }

    let code = result?;

    info!("Received authorization code, exchanging for tokens");

    // Exchange the code for tokens
    exchange_code(config, &code, &verifier, &redirect_uri, clock).await
}

/// Exchange an authorization code for tokens.
pub async fn exchange_code(
    config: &OAuthConfig,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
    clock: &dyn crate::clock::Clock,
) -> Result<OAuthTokens, OAuthError> {
    let client = reqwest::Client::new();
    let mut params = vec![
        ("grant_type", "authorization_code"),
        ("code", code),
        ("redirect_uri", redirect_uri),
        ("client_id", &config.client_id),
        ("code_verifier", verifier),
    ];

    let secret_ref;
    if let Some(ref secret) = config.client_secret {
        secret_ref = secret.clone();
        params.push(("client_secret", &secret_ref));
    }

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

    let token_resp: TokenResponse = serde_json::from_str(&body)
        .map_err(|e| OAuthError::TokenExchange(format!("parse response: {e} (body: {body})")))?;

    if let Some(error) = token_resp.error {
        let desc = token_resp.error_description.unwrap_or(error);
        return Err(OAuthError::TokenExchange(format!(
            "provider error (HTTP {status}): {desc}"
        )));
    }

    let expires_at = token_resp
        .expires_in
        .map(|secs| clock.now().timestamp() + secs);

    Ok(OAuthTokens {
        access_token: token_resp.access_token,
        refresh_token: token_resp.refresh_token,
        expires_at,
    })
}

/// Refresh an expired access token using a refresh token.
pub async fn refresh(
    config: &OAuthConfig,
    refresh_token: &str,
    clock: &dyn crate::clock::Clock,
) -> Result<OAuthTokens, OAuthError> {
    let client = reqwest::Client::new();
    let mut params = vec![
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
        ("client_id", &config.client_id),
    ];

    let secret_ref;
    if let Some(ref secret) = config.client_secret {
        secret_ref = secret.clone();
        params.push(("client_secret", &secret_ref));
    }

    let resp = client
        .post(&config.token_url)
        .form(&params)
        .send()
        .await
        .map_err(|e| OAuthError::TokenExchange(format!("refresh request failed: {e}")))?;

    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| OAuthError::TokenExchange(format!("read body: {e}")))?;

    let token_resp: TokenResponse = serde_json::from_str(&body)
        .map_err(|e| OAuthError::TokenExchange(format!("parse response: {e} (body: {body})")))?;

    if let Some(error) = token_resp.error {
        let desc = token_resp.error_description.unwrap_or(error);
        return Err(OAuthError::TokenExchange(format!(
            "provider error (HTTP {status}): {desc}"
        )));
    }

    let expires_at = token_resp
        .expires_in
        .map(|secs| clock.now().timestamp() + secs);

    // If the provider doesn't return a new refresh token, keep the old one
    let new_refresh = token_resp
        .refresh_token
        .or_else(|| Some(refresh_token.to_string()));

    Ok(OAuthTokens {
        access_token: token_resp.access_token,
        refresh_token: new_refresh,
        expires_at,
    })
}

/// Run an OAuth authorization flow for the given cloud provider.
///
/// Returns tokens on success. Only Google Drive, Dropbox, and OneDrive
/// support OAuth; other providers return an error.
pub async fn authorize_provider(
    provider: crate::config::CloudProvider,
    cancel: tokio::sync::watch::Receiver<bool>,
    clock: &dyn crate::clock::Clock,
) -> Result<OAuthTokens, OAuthError> {
    use crate::config::CloudProvider;
    use crate::storage::cloud::{dropbox, google_drive, onedrive};

    let config = match provider {
        CloudProvider::GoogleDrive => google_drive::GoogleDriveCloudHome::oauth_config(),
        CloudProvider::Dropbox => dropbox::DropboxCloudHome::oauth_config(),
        CloudProvider::OneDrive => onedrive::OneDriveCloudHome::oauth_config(),
        other => {
            return Err(OAuthError::Denied(format!("{other:?} does not use OAuth")));
        }
    };

    authorize(&config, cancel, clock).await
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
