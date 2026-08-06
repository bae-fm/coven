//! OAuth 2.0 helper for consumer cloud provider authentication.
//!
//! Provides PKCE-based authorization code flow with a localhost callback server.
//! Used by Google Drive, Dropbox, and OneDrive cloud home backends.

#[cfg(feature = "oauth-providers")]
use std::collections::HashMap;

#[cfg(any(test, feature = "oauth-providers"))]
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
#[cfg(feature = "oauth-providers")]
use rand::RngCore;
#[cfg(any(test, feature = "oauth-providers"))]
#[cfg(any(test, feature = "oauth-providers"))]
use serde::Deserialize;
#[cfg(any(test, feature = "oauth-providers"))]
use sha2::{Digest, Sha256};
#[cfg(any(test, feature = "oauth-providers"))]
use thiserror::Error;
// `info`/`warn` are used only by the localhost-callback `authorize`, which is
// gated on `oauth-providers` too.
#[cfg(feature = "oauth-providers")]
use tracing::{info, warn};

/// OAuth provider configuration.
#[cfg(feature = "oauth-providers")]
#[derive(Clone, Debug)]
pub(crate) struct OAuthConfig {
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
/// OAuth application. coven ships no app credentials of its own.
#[cfg(feature = "oauth-providers")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OAuthClientCreds {
    pub client_id: String,
    /// None for public (PKCE-only) clients.
    pub client_secret: Option<String>,
}

/// The consuming app's OAuth clients. Each [`CovenBuilder`](crate::CovenBuilder)
/// retains its own value, so unrelated apps in one process never share
/// credentials.
#[derive(Clone, Debug)]
pub struct OAuthClients {
    #[cfg(feature = "oauth-providers")]
    credentials: HashMap<coven_foundation::config::CloudProvider, OAuthClientCreds>,
    #[cfg(feature = "oauth-providers")]
    client: reqwest::Client,
}

/// An OAuth client set is missing a provider or names a provider that does not
/// use OAuth.
#[cfg(feature = "oauth-providers")]
#[derive(Debug, thiserror::Error)]
pub enum OAuthClientCredsError {
    #[error("no OAuth client credentials configured for provider {0:?}")]
    MissingProvider(coven_foundation::config::CloudProvider),
    #[error("provider {0:?} does not use OAuth")]
    UnsupportedProvider(coven_foundation::config::CloudProvider),
}

impl OAuthClients {
    /// Construct the OAuth clients this app can use.
    #[cfg(feature = "oauth-providers")]
    pub fn new(
        credentials: HashMap<coven_foundation::config::CloudProvider, OAuthClientCreds>,
    ) -> Result<Self, OAuthClientCredsError> {
        if let Some(provider) = credentials.keys().find(|provider| !provider.needs_oauth()) {
            return Err(OAuthClientCredsError::UnsupportedProvider(
                (*provider).clone(),
            ));
        }
        Ok(Self {
            credentials,
            client: reqwest::Client::new(),
        })
    }

    /// No OAuth providers configured. Suitable for apps using only S3,
    /// CloudKit, or local storage.
    pub fn empty() -> Self {
        Self {
            #[cfg(feature = "oauth-providers")]
            credentials: HashMap::new(),
            #[cfg(feature = "oauth-providers")]
            client: reqwest::Client::new(),
        }
    }

    #[cfg(feature = "oauth-providers")]
    fn credentials_for(
        &self,
        provider: &coven_foundation::config::CloudProvider,
    ) -> Result<OAuthClientCreds, OAuthClientCredsError> {
        if !provider.needs_oauth() {
            return Err(OAuthClientCredsError::UnsupportedProvider(provider.clone()));
        }
        self.credentials
            .get(provider)
            .cloned()
            .ok_or_else(|| OAuthClientCredsError::MissingProvider(provider.clone()))
    }

    #[cfg(feature = "oauth-providers")]
    pub(crate) fn config_for(
        &self,
        provider: coven_foundation::config::CloudProvider,
    ) -> Result<OAuthConfig, OAuthClientCredsError> {
        use crate::storage::cloud::{dropbox, google_drive, onedrive};
        use coven_foundation::config::CloudProvider;

        let credentials = self.credentials_for(&provider)?;
        match provider {
            CloudProvider::GoogleDrive => Ok(google_drive::GoogleDriveCloudHome::oauth_config(
                credentials,
            )),
            CloudProvider::Dropbox => Ok(dropbox::DropboxCloudHome::oauth_config(credentials)),
            CloudProvider::OneDrive => Ok(onedrive::OneDriveCloudHome::oauth_config(credentials)),
            provider => Err(OAuthClientCredsError::UnsupportedProvider(provider)),
        }
    }

    #[cfg(all(test, feature = "oauth-providers"))]
    pub(crate) fn for_tests() -> Self {
        Self::new(HashMap::from([
            (
                coven_foundation::config::CloudProvider::GoogleDrive,
                OAuthClientCreds {
                    client_id: "test-client".to_string(),
                    client_secret: None,
                },
            ),
            (
                coven_foundation::config::CloudProvider::Dropbox,
                OAuthClientCreds {
                    client_id: "test-client".to_string(),
                    client_secret: None,
                },
            ),
            (
                coven_foundation::config::CloudProvider::OneDrive,
                OAuthClientCreds {
                    client_id: "test-client".to_string(),
                    client_secret: None,
                },
            ),
        ]))
        .expect("test clients contain only OAuth providers")
    }

    #[cfg(feature = "oauth-providers")]
    pub async fn authorize(
        &self,
        provider: coven_foundation::config::CloudProvider,
        cancel: tokio::sync::watch::Receiver<bool>,
        clock: &dyn coven_foundation::clock::Clock,
    ) -> Result<OAuthTokens, OAuthError> {
        let config = self.config_for(provider)?;
        let client = &self.client;
        let redirect_uri = format!("http://localhost:{}/callback", config.redirect_port);
        let mut entropy = [0_u8; 64];
        rand::rng().fill_bytes(&mut entropy);
        let AuthorizeRequest {
            auth_url,
            verifier,
            state,
        } = AuthorizeRequest::from_entropy(&config, &redirect_uri, entropy)?;

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
        exchange_code(client, &config, &code, &verifier, &redirect_uri, clock).await
    }

    #[cfg(feature = "oauth-providers")]
    /// Build an authorization request for a host-managed redirect flow.
    pub fn build_authorize_request(
        &self,
        provider: coven_foundation::config::CloudProvider,
        redirect_uri: &str,
    ) -> Result<AuthorizeRequest, OAuthError> {
        let mut entropy = [0_u8; 64];
        rand::rng().fill_bytes(&mut entropy);
        AuthorizeRequest::from_entropy(&self.config_for(provider)?, redirect_uri, entropy)
    }

    #[cfg(feature = "oauth-providers")]
    /// Exchange the result of [`Self::build_authorize_request`] for tokens.
    pub async fn exchange_code(
        &self,
        provider: coven_foundation::config::CloudProvider,
        code: &str,
        callback_state: Option<&str>,
        request: &AuthorizeRequest,
        redirect_uri: &str,
        clock: &dyn coven_foundation::clock::Clock,
    ) -> Result<OAuthTokens, OAuthError> {
        request.verify_callback_state(callback_state)?;
        exchange_code(
            &self.client,
            &self.config_for(provider)?,
            code,
            &request.verifier,
            redirect_uri,
            clock,
        )
        .await
    }

    #[cfg(feature = "oauth-providers")]
    /// Authorize Google Drive, prepare this store's folder, and retain the
    /// resulting tokens in the store key service.
    pub async fn sign_in_google_drive(
        &self,
        key_service: &coven_keys::keys::StoreKeys,
        store_name: &str,
        cancel: tokio::sync::watch::Receiver<bool>,
        clock: &dyn coven_foundation::clock::Clock,
    ) -> Result<String, crate::storage::cloud::SetupError> {
        let tokens = self
            .authorize(
                coven_foundation::config::CloudProvider::GoogleDrive,
                cancel,
                clock,
            )
            .await
            .map_err(|error| {
                crate::storage::cloud::SetupError(format!(
                    "Google Drive authorization failed: {error}"
                ))
            })?;
        let folder_name = format!("your-app - {store_name}");
        let search_query = crate::storage::cloud::folder_search_query(&folder_name);
        let search_resp = crate::storage::cloud::supports_all_drives(
            self.client.get("https://www.googleapis.com/drive/v3/files"),
        )
        .bearer_auth(&tokens.access_token)
        .query(&[
            ("q", search_query.as_str()),
            ("fields", "files(id)"),
            ("includeItemsFromAllDrives", "true"),
        ])
        .send()
        .await
        .map_err(|error| {
            crate::storage::cloud::SetupError(format!(
                "Failed to search for existing Google Drive folder: {error}"
            ))
        })?;
        if !search_resp.status().is_success() {
            let status = search_resp.status();
            let body = search_resp.text().await.map_err(|error| {
                crate::storage::cloud::SetupError(format!(
                    "Failed to read Google Drive folder search error (HTTP {status}): {error}"
                ))
            })?;
            return Err(crate::storage::cloud::SetupError(format!(
                "Failed to search for existing Google Drive folder (HTTP {status}): {body}"
            )));
        }
        let search_json: serde_json::Value = search_resp.json().await.map_err(|error| {
            crate::storage::cloud::SetupError(format!(
                "Failed to parse Google Drive search response: {error}"
            ))
        })?;
        let existing_folder_id = search_json["files"][0]["id"].as_str().map(str::to_string);
        let folder_id = if let Some(id) = existing_folder_id {
            id
        } else {
            let create_body = serde_json::json!({
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
            });
            let response = crate::storage::cloud::supports_all_drives(
                self.client
                    .post("https://www.googleapis.com/drive/v3/files"),
            )
            .bearer_auth(&tokens.access_token)
            .json(&create_body)
            .send()
            .await
            .map_err(|error| {
                crate::storage::cloud::SetupError(format!(
                    "Failed to create Google Drive folder: {error}"
                ))
            })?;
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.map_err(|error| {
                    crate::storage::cloud::SetupError(format!(
                        "Failed to read Google Drive folder creation error (HTTP {status}): {error}"
                    ))
                })?;
                return Err(crate::storage::cloud::SetupError(format!(
                    "Failed to create Google Drive folder (HTTP {status}): {body}"
                )));
            }
            let folder: serde_json::Value = response.json().await.map_err(|error| {
                crate::storage::cloud::SetupError(format!(
                    "Failed to parse Google Drive folder response: {error}"
                ))
            })?;
            folder["id"]
                .as_str()
                .ok_or_else(|| {
                    crate::storage::cloud::SetupError(
                        "Google Drive folder response missing 'id'".to_string(),
                    )
                })?
                .to_string()
        };
        key_service
            .set_cloud_home_oauth_tokens(&tokens)
            .map_err(|error| {
                crate::storage::cloud::SetupError(format!("Failed to save OAuth token: {error}"))
            })?;
        info!("Authorized Google Drive; folder ready");
        Ok(folder_id)
    }

    #[cfg(feature = "oauth-providers")]
    /// Authorize Dropbox, prepare this store's folder, and retain the resulting
    /// tokens in the store key service.
    pub async fn sign_in_dropbox(
        &self,
        key_service: &coven_keys::keys::StoreKeys,
        store_name: &str,
        cancel: tokio::sync::watch::Receiver<bool>,
        clock: &dyn coven_foundation::clock::Clock,
    ) -> Result<String, crate::storage::cloud::SetupError> {
        let tokens = self
            .authorize(
                coven_foundation::config::CloudProvider::Dropbox,
                cancel,
                clock,
            )
            .await
            .map_err(|error| {
                crate::storage::cloud::SetupError(format!("Dropbox authorization failed: {error}"))
            })?;
        let folder_path = format!("/Apps/your-app/{store_name}");
        let response = self
            .client
            .post("https://api.dropboxapi.com/2/files/create_folder_v2")
            .bearer_auth(&tokens.access_token)
            .json(&serde_json::json!({
                "path": folder_path,
                "autorename": false,
            }))
            .send()
            .await
            .map_err(|error| {
                crate::storage::cloud::SetupError(format!(
                    "Failed to create Dropbox folder: {error}"
                ))
            })?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.map_err(|error| {
                crate::storage::cloud::SetupError(format!(
                    "Failed to read Dropbox folder creation error (HTTP {status}): {error}"
                ))
            })?;
            if !(status == reqwest::StatusCode::CONFLICT && body.contains("conflict")) {
                return Err(crate::storage::cloud::SetupError(format!(
                    "Failed to create Dropbox folder (HTTP {status}): {body}"
                )));
            }
        }
        key_service
            .set_cloud_home_oauth_tokens(&tokens)
            .map_err(|error| {
                crate::storage::cloud::SetupError(format!("Failed to save OAuth token: {error}"))
            })?;
        info!("Authorized Dropbox; folder ready");
        Ok(folder_path)
    }

    #[cfg(feature = "oauth-providers")]
    /// Authorize OneDrive, prepare this store's folder, and retain the
    /// resulting tokens in the store key service.
    pub async fn sign_in_onedrive(
        &self,
        key_service: &coven_keys::keys::StoreKeys,
        cancel: tokio::sync::watch::Receiver<bool>,
        clock: &dyn coven_foundation::clock::Clock,
    ) -> Result<(String, String), crate::storage::cloud::SetupError> {
        let tokens = self
            .authorize(
                coven_foundation::config::CloudProvider::OneDrive,
                cancel,
                clock,
            )
            .await
            .map_err(|error| {
                crate::storage::cloud::SetupError(format!("OneDrive authorization failed: {error}"))
            })?;
        let drive_response = self
            .client
            .get("https://graph.microsoft.com/v1.0/me/drive")
            .bearer_auth(&tokens.access_token)
            .send()
            .await
            .map_err(|error| {
                crate::storage::cloud::SetupError(format!("Failed to get OneDrive info: {error}"))
            })?;
        if !drive_response.status().is_success() {
            let status = drive_response.status();
            let body = drive_response.text().await.map_err(|error| {
                crate::storage::cloud::SetupError(format!(
                    "Failed to read OneDrive info error (HTTP {status}): {error}"
                ))
            })?;
            return Err(crate::storage::cloud::SetupError(format!(
                "Failed to get OneDrive info (HTTP {status}): {body}"
            )));
        }
        let drive: serde_json::Value = drive_response.json().await.map_err(|error| {
            crate::storage::cloud::SetupError(format!("Failed to parse OneDrive response: {error}"))
        })?;
        let drive_id = drive["id"]
            .as_str()
            .ok_or_else(|| {
                crate::storage::cloud::SetupError(
                    "OneDrive response missing 'id' field".to_string(),
                )
            })?
            .to_string();
        let folder_response = self
            .client
            .post(format!(
                "https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"
            ))
            .bearer_auth(&tokens.access_token)
            .json(&serde_json::json!({
                "name": "your-app",
                "folder": {},
                "@microsoft.graph.conflictBehavior": "useExisting",
            }))
            .send()
            .await
            .map_err(|error| {
                crate::storage::cloud::SetupError(format!(
                    "Failed to create OneDrive folder: {error}"
                ))
            })?;
        if !folder_response.status().is_success() {
            let status = folder_response.status();
            let body = folder_response.text().await.map_err(|error| {
                crate::storage::cloud::SetupError(format!(
                    "Failed to read OneDrive folder creation error (HTTP {status}): {error}"
                ))
            })?;
            return Err(crate::storage::cloud::SetupError(format!(
                "Failed to create OneDrive folder (HTTP {status}): {body}"
            )));
        }
        let folder: serde_json::Value = folder_response.json().await.map_err(|error| {
            crate::storage::cloud::SetupError(format!(
                "Failed to parse OneDrive folder response: {error}"
            ))
        })?;
        let folder_id = folder["id"]
            .as_str()
            .ok_or_else(|| {
                crate::storage::cloud::SetupError(
                    "OneDrive folder response missing 'id' field".to_string(),
                )
            })?
            .to_string();
        key_service
            .set_cloud_home_oauth_tokens(&tokens)
            .map_err(|error| {
                crate::storage::cloud::SetupError(format!("Failed to save OAuth token: {error}"))
            })?;
        info!("Authorized OneDrive; folder ready");
        Ok((drive_id, folder_id))
    }
}

pub use coven_keys::keys::OAuthTokens;

#[cfg(any(test, feature = "oauth-providers"))]
#[derive(Error, Debug)]
pub enum OAuthError {
    #[cfg(feature = "oauth-providers")]
    #[error("failed to open browser: {0}")]
    BrowserOpen(String),
    #[cfg(feature = "oauth-providers")]
    #[error("callback server error: {0}")]
    Server(String),
    #[error("token exchange error: {0}")]
    TokenExchange(String),
    #[cfg(feature = "oauth-providers")]
    #[error("account email fetch error: {0}")]
    AccountFetch(String),
    #[cfg(feature = "oauth-providers")]
    #[error("authorization denied: {0}")]
    Denied(String),
    #[cfg(feature = "oauth-providers")]
    #[error("timeout waiting for authorization callback")]
    Timeout,
    /// The refresh token is no longer accepted (revoked, expired, password
    /// changed, …). Only a fresh OAuth authorization flow recovers — there
    /// is no point retrying the refresh.
    #[error("re-authorization required: {0}")]
    Reauthorize(String),
    #[cfg(feature = "oauth-providers")]
    #[error(transparent)]
    ClientCreds(#[from] OAuthClientCredsError),
}

/// Guards the localhost OAuth-callback server task.
/// [`OAuthClients::authorize`] is the only operation that spawns it; both are
/// gated on `oauth-providers`.
#[cfg(feature = "oauth-providers")]
struct AbortOnDrop(Option<tokio::task::JoinHandle<()>>);

#[cfg(feature = "oauth-providers")]
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

#[cfg(feature = "oauth-providers")]
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
#[cfg(any(test, feature = "oauth-providers"))]
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

#[cfg(any(test, feature = "oauth-providers"))]
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
        clock: &dyn coven_foundation::clock::Clock,
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

#[cfg(feature = "oauth-providers")]
fn oauth_callback_html(title: &str, message: &str) -> String {
    include_str!("oauth_success.html")
        .replace("{{title}}", title)
        .replace("{{message}}", message)
}

#[cfg(feature = "oauth-providers")]
async fn post_token_request(
    client: &reqwest::Client,
    config: &OAuthConfig,
    params: Vec<(&str, String)>,
    clock: &dyn coven_foundation::clock::Clock,
) -> Result<OAuthTokens, OAuthError> {
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

/// Compute the S256 PKCE code challenge from a verifier.
#[cfg(any(test, feature = "oauth-providers"))]
pub(crate) fn code_challenge(verifier: &str) -> String {
    let hash = Sha256::digest(verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(hash)
}

/// An authorization request the host drives itself: the URL to open plus the
/// PKCE verifier and state value coven checks during exchange. For hosts that
/// capture the redirect outside coven's localhost callback server — e.g. a
/// mobile OS auth session (ASWebAuthenticationSession / Custom Tabs)
/// redirecting to a custom URI scheme, where binding a localhost port and
/// `open::that` don't apply.
#[cfg(feature = "oauth-providers")]
#[derive(Clone, Debug)]
pub struct AuthorizeRequest {
    pub auth_url: String,
    verifier: String,
    state: String,
}

#[cfg(feature = "oauth-providers")]
impl AuthorizeRequest {
    fn from_entropy(
        config: &OAuthConfig,
        redirect_uri: &str,
        entropy: [u8; 64],
    ) -> Result<Self, OAuthError> {
        let verifier = URL_SAFE_NO_PAD.encode(&entropy[..32]);
        let state = URL_SAFE_NO_PAD.encode(&entropy[32..]);
        let challenge = code_challenge(&verifier);

        let mut auth_params = vec![
            ("response_type", "code".to_string()),
            ("client_id", config.client_id.clone()),
            ("redirect_uri", redirect_uri.to_string()),
            ("code_challenge", challenge),
            ("code_challenge_method", "S256".to_string()),
            ("state", state.clone()),
        ];

        for (key, value) in &config.extra_auth_params {
            auth_params.push((key.as_str(), value.clone()));
        }

        if !config.scopes.is_empty() {
            auth_params.push(("scope", config.scopes.join(" ")));
        }

        let auth_url = format!(
            "{}?{}",
            config.auth_url,
            serde_urlencoded::to_string(&auth_params)
                .map_err(|error| OAuthError::Server(format!("failed to encode params: {error}")))?
        );

        Ok(Self {
            auth_url,
            verifier,
            state,
        })
    }

    pub fn verify_callback_state(&self, callback_state: Option<&str>) -> Result<(), OAuthError> {
        verify_callback_state(callback_state, &self.state).map_err(OAuthError::Denied)
    }
}

#[cfg(feature = "oauth-providers")]
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

#[cfg(feature = "oauth-providers")]
fn verify_callback_state(callback_state: Option<&str>, expected_state: &str) -> Result<(), String> {
    match callback_state {
        Some(state) if state == expected_state => Ok(()),
        Some(_) => Err("state mismatch in callback".to_string()),
        None => Err("missing state in callback".to_string()),
    }
}

#[cfg(feature = "oauth-providers")]
async fn exchange_code(
    client: &reqwest::Client,
    config: &OAuthConfig,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
    clock: &dyn coven_foundation::clock::Clock,
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

    post_token_request(client, config, params, clock).await
}

/// Refresh an expired access token using a refresh token.
#[cfg(feature = "oauth-providers")]
pub(crate) async fn refresh(
    client: &reqwest::Client,
    config: &OAuthConfig,
    refresh_token: &str,
    clock: &dyn coven_foundation::clock::Clock,
) -> Result<OAuthTokens, OAuthError> {
    let mut params = vec![
        ("grant_type", "refresh_token".to_string()),
        ("refresh_token", refresh_token.to_string()),
        ("client_id", config.client_id.clone()),
    ];
    if let Some(secret) = &config.client_secret {
        params.push(("client_secret", secret.clone()));
    }

    let mut tokens = post_token_request(client, config, params, clock).await?;
    // Provider didn't return a new refresh token (common — many providers
    // only rotate it on the initial exchange). Reuse the existing one so the
    // session can refresh again next cycle.
    if tokens.refresh_token.is_none() {
        tracing::debug!("provider did not return a new refresh_token; reusing existing token");
        tokens.refresh_token = Some(refresh_token.to_string());
    }
    Ok(tokens)
}

#[cfg(all(test, feature = "oauth-providers"))]
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
#[path = "oauth_tests.rs"]
mod tests;
