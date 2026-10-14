use super::http;
use crate::{
    CloudProvider, DropboxAccess, GoogleDriveAccess, OAuthAccess, OAuthTokens, StorageError,
    StorageFailure,
};
use coven_crypto::{SecretBytes, SecretText};
use coven_foundation::clock::ClockRef;
use oauth2::{CsrfToken, PkceCodeChallenge};
use std::sync::Arc;
use std::time::Duration;

/// Sign-in failures distinguish cancellation, invalid redirects and provider refusal.
#[derive(Debug, thiserror::Error)]
pub enum OAuthError {
    /// This provider has no OAuth client id or presenter configured.
    #[error("OAuth provider unavailable: {0:?}")]
    Unavailable(CloudProvider),
    /// Redirect state is missing or different.
    #[error("authorization state mismatch")]
    StateMismatch,
    /// The provider returned access_denied, retaining its checked callback.
    #[error("provider denied sign-in")]
    Denied(#[source] OAuthRedirectError),
    /// The callback omitted its authorization code.
    #[error("authorization callback omitted its code")]
    MissingCode,
    /// The caller cancelled sign-in.
    #[error("sign-in cancelled")]
    Cancelled,
    /// No callback arrived before the deadline.
    #[error("sign-in timed out")]
    Timeout,
    /// The current tokens have expired; refresh and commit them before reuse.
    #[error("provider tokens expired")]
    Expired,
    /// A new provider sign-in is required.
    #[error("{0:?} requires a new sign-in")]
    Reauthorize(CloudProvider),
    /// The redirect URI or callback request is malformed.
    #[error("invalid OAuth redirect")]
    InvalidRedirect,
    /// The provider's expiry cannot be represented.
    #[error("invalid OAuth token expiry")]
    InvalidExpiry,
    /// The browser or local redirect listener failed.
    #[error("OAuth browser or listener: {0}")]
    Io(#[from] std::io::Error),
    /// The app's native sign-in sheet failed.
    #[error("authorization presentation: {0}")]
    Presentation(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Network or provider failure, preserving its classification.
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// The original checked error callback, omitted from diagnostics because it may
/// contain secrets echoed by the provider.
#[derive(Debug, thiserror::Error)]
#[error("provider returned an OAuth error redirect")]
pub struct OAuthRedirectError {
    callback: SecretText,
}

impl OAuthRedirectError {
    /// Inspect the unmodified callback explicitly, including provider extensions.
    pub fn callback(&self) -> &str {
        self.callback.as_str()
    }
}

/// OAuth error codes have the same meaning in token responses and checked
/// authorization redirects. Unrecognized codes leave HTTP status classification
/// to the caller; redirects without a recognized code are provider refusals.
pub(crate) fn classify_error(code: &str) -> Option<StorageFailure> {
    match code {
        "invalid_grant" | "interaction_required" | "consent_required" => {
            Some(StorageFailure::Authentication)
        }
        "invalid_client"
        | "unauthorized_client"
        | "deleted_client"
        | "invalid_scope"
        | "invalid_resource"
        | "invalid_request"
        | "unsupported_grant_type"
        | "unsupported_response_type" => Some(StorageFailure::InvalidConfiguration),
        "server_error" | "temporarily_unavailable" => Some(StorageFailure::Network),
        "access_denied" => Some(StorageFailure::PermissionDenied),
        _ => None,
    }
}

/// Presents sign-in without handling tokens. The platform sheet must be dismissed
/// when its future is dropped. Native cancellation returns `OAuthError::Cancelled`.
#[async_trait::async_trait]
pub trait OAuthPresenter: Send + Sync {
    /// The registered redirect for this provider. No query or fragment is allowed.
    fn redirect_uri(&self, provider: CloudProvider) -> &str;
    /// Open the authorization URL and return the complete provider redirect.
    async fn present(&self, authorization_url: &str) -> Result<SecretText, OAuthError>;
}

struct AuthorizeRequest {
    auth_url: String,
    verifier: SecretText,
    state: SecretText,
    redirect: String,
}

/// The sign-in and refresh service used by coven's composition roots. Apps
/// configure `OAuthClients` and `OAuthPresenter`; only coven receives tokens.
#[derive(Clone)]
pub struct OAuthFlow {
    clients: OAuthClients,
    presenter: Arc<dyn OAuthPresenter>,
}

impl OAuthFlow {
    /// Compose the provider protocol with the app's presentation capability.
    pub fn new(clients: OAuthClients, presenter: Arc<dyn OAuthPresenter>) -> Self {
        Self { clients, presenter }
    }

    /// Exchange a checked redirect. Dropping this future cancels presentation;
    /// no detached task survives it. The caller commits tokens to custody.
    pub async fn authenticate(&self, access: OAuthAccess) -> Result<OAuthTokens, OAuthError> {
        let provider = access.provider();
        let request = self
            .clients
            .build_authorize_request(access, self.presenter.redirect_uri(provider))?;
        let flow = async {
            let redirect = self.presenter.present(&request.auth_url).await?;
            self.clients
                .exchange_redirect(access, &request, redirect)
                .await
        };
        tokio::select! {
            result = flow => result,
            _ = self.clients.clock.sleep(Duration::from_secs(300)) => Err(OAuthError::Timeout),
        }
    }

    /// Refresh for custody; a live provider must use replacements only after commit.
    pub async fn refresh(
        &self,
        client_id: &str,
        access: OAuthAccess,
        refresh_token: &SecretText,
    ) -> Result<OAuthTokens, OAuthError> {
        self.clients.refresh(client_id, access, refresh_token).await
    }

    /// Refuse unavailable registered access without opening UI or contacting a provider.
    pub fn require_access(&self, access: OAuthAccess) -> Result<(), OAuthError> {
        self.clients.require_access(access)
    }

    /// An incremental request must use the same client as the held authorization.
    pub fn require_client(&self, tokens: &OAuthTokens) -> Result<(), OAuthError> {
        let (id, ..) = self.clients.config(tokens.access)?;
        if id != tokens.client_id {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("OAuth client differs from the held authorization")
                .into());
        }
        Ok(())
    }
}

/// The app's OAuth client ids and injected clock (E10).
#[derive(Clone)]
pub struct OAuthClients {
    google: Option<GoogleDriveClient>,
    dropbox: Option<DropboxClients>,
    onedrive: Option<String>,
    clock: ClockRef,
    client: Result<reqwest::Client, Arc<reqwest::Error>>,
    #[cfg(any(test, feature = "test-utils"))]
    token_override: Option<String>,
}

/// One Google Cloud project client and its registered maximum access.
#[derive(Clone)]
pub struct GoogleDriveClient {
    id: String,
    secret: Option<SecretText>,
    registered_access: GoogleDriveAccess,
}

impl GoogleDriveClient {
    /// Register the maximum access this app offers; new stores still request File.
    pub fn new(client_id: String, registered_access: GoogleDriveAccess) -> Self {
        Self {
            id: client_id,
            secret: None,
            registered_access,
        }
    }
}

/// The app's Dropbox registrations, with an explicit access choice when both exist.
#[derive(Clone)]
pub enum DropboxClients {
    /// The only registration permits not-shared stores in its app folder.
    AppFolder {
        /// The App-folder registration's key.
        app_key: String,
    },
    /// The only registration permits shared folder namespaces.
    FullDropbox {
        /// The Full Dropbox registration's key.
        app_key: String,
    },
    /// Creation must choose AppFolder or FullDropbox; neither is a default.
    Both {
        /// The key used for not-shared stores.
        app_folder_key: String,
        /// The key used for shared-capable stores.
        full_dropbox_key: String,
    },
}

impl OAuthClients {
    /// Configure client ids, `None` for providers the app does not offer.
    pub fn new(
        google_drive: Option<GoogleDriveClient>,
        dropbox: Option<DropboxClients>,
        onedrive_client_id: Option<String>,
        clock: ClockRef,
    ) -> Self {
        Self {
            google: google_drive,
            dropbox,
            onedrive: onedrive_client_id,
            clock,
            client: http::client().map_err(Arc::new),
            #[cfg(any(test, feature = "test-utils"))]
            token_override: None,
        }
    }
    /// Supply the Google Desktop client's secret for code exchange and refresh.
    /// Google's mobile clients do not require it. It is never sent to another provider.
    pub fn with_google_client_secret(mut self, secret: SecretText) -> Result<Self, OAuthError> {
        let google = self
            .google
            .as_mut()
            .ok_or(OAuthError::Unavailable(CloudProvider::GoogleDrive))?;
        if secret.as_str().is_empty() {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("empty Google client secret")
                .into());
        }
        google.secret = Some(secret);
        Ok(self)
    }
    fn config(
        &self,
        access: OAuthAccess,
    ) -> Result<(&str, &'static str, &'static str, &'static str), OAuthError> {
        let (id, auth, token) = match access {
            OAuthAccess::GoogleDrive(_) => {
                let client = self
                    .google
                    .as_ref()
                    .ok_or(OAuthError::Unavailable(access.provider()))?;
                (
                    Some(client.id.as_str()),
                    "https://accounts.google.com/o/oauth2/v2/auth",
                    "https://oauth2.googleapis.com/token",
                )
            }
            OAuthAccess::Dropbox(requested) => {
                let client = self
                    .dropbox
                    .as_ref()
                    .ok_or(OAuthError::Unavailable(access.provider()))?;
                let id = match (client, requested) {
                    (DropboxClients::AppFolder { app_key }, DropboxAccess::AppFolder)
                    | (DropboxClients::FullDropbox { app_key }, DropboxAccess::FullDropbox) => {
                        app_key
                    }
                    (DropboxClients::Both { app_folder_key, .. }, DropboxAccess::AppFolder) => {
                        app_folder_key
                    }
                    (
                        DropboxClients::Both {
                            full_dropbox_key, ..
                        },
                        DropboxAccess::FullDropbox,
                    ) => full_dropbox_key,
                    _ => {
                        return Err(StorageError::from(StorageFailure::AccessNotRegistered {
                            required: access,
                        })
                        .into())
                    }
                };
                (
                    Some(id.as_str()),
                    "https://www.dropbox.com/oauth2/authorize",
                    "https://api.dropboxapi.com/oauth2/token",
                )
            }
            OAuthAccess::OneDrive => (
                self.onedrive.as_deref(),
                "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
                "https://login.microsoftonline.com/common/oauth2/v2.0/token",
            ),
        };
        let id = id
            .filter(|id| !id.is_empty())
            .ok_or(OAuthError::Unavailable(access.provider()))?;
        Ok((id, auth, token, access.scope()))
    }
    fn require_access(&self, access: OAuthAccess) -> Result<(), OAuthError> {
        self.config(access)?;
        if access == OAuthAccess::GoogleDrive(GoogleDriveAccess::Full)
            && self
                .google
                .as_ref()
                .expect("configured Google client")
                .registered_access
                != GoogleDriveAccess::Full
        {
            return Err(StorageError::from(StorageFailure::AccessNotRegistered {
                required: access,
            })
            .into());
        }
        Ok(())
    }
    fn build_authorize_request(
        &self,
        access: OAuthAccess,
        redirect_uri: &str,
    ) -> Result<AuthorizeRequest, OAuthError> {
        self.require_access(access)?;
        let (client_id, auth, _, scope) = self.config(access)?;
        let redirect = url::Url::parse(redirect_uri).map_err(|_| OAuthError::InvalidRedirect)?;
        if redirect.fragment().is_some()
            || redirect.query().is_some()
            || !redirect.username().is_empty()
            || redirect.password().is_some()
        {
            return Err(OAuthError::InvalidRedirect);
        }
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let state = CsrfToken::new_random();
        let mut url = url::Url::parse(auth).map_err(|_| OAuthError::InvalidRedirect)?;
        url.query_pairs_mut().extend_pairs([
            ("response_type", "code"),
            ("client_id", client_id),
            ("redirect_uri", redirect_uri),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
            ("state", state.secret()),
            ("scope", scope),
        ]);
        match access.provider() {
            CloudProvider::GoogleDrive => {
                url.query_pairs_mut()
                    .append_pair("access_type", "offline")
                    .append_pair("prompt", "consent");
                if access == OAuthAccess::GoogleDrive(GoogleDriveAccess::Full) {
                    url.query_pairs_mut()
                        .append_pair("include_granted_scopes", "true");
                }
            }
            CloudProvider::Dropbox => {
                url.query_pairs_mut()
                    .append_pair("token_access_type", "offline");
            }
            _ => {}
        }
        Ok(AuthorizeRequest {
            auth_url: url.into(),
            verifier: SecretText::new(verifier.secret().clone()),
            state: SecretText::new(state.secret().clone()),
            redirect: redirect_uri.to_owned(),
        })
    }
    async fn exchange_redirect(
        &self,
        access: OAuthAccess,
        request: &AuthorizeRequest,
        callback: SecretText,
    ) -> Result<OAuthTokens, OAuthError> {
        let code = Self::redirect_code(access.provider(), request, callback)?;
        let (client_id, _, _, _) = self.config(access)?;
        self.tokens(
            access,
            &[
                ("grant_type", "authorization_code"),
                ("code", code.as_str()),
                ("redirect_uri", request.redirect.as_str()),
                ("client_id", client_id),
                ("code_verifier", request.verifier.as_str()),
            ],
        )
        .await
    }
    /// The authorization code of a checked redirect: its origin and path, its
    /// state, and either a code or the provider's error.
    fn redirect_code(
        provider: CloudProvider,
        request: &AuthorizeRequest,
        callback: SecretText,
    ) -> Result<SecretText, OAuthError> {
        let mut redirect =
            url::Url::parse(callback.as_str()).map_err(|_| OAuthError::InvalidRedirect)?;
        let mut fields = std::collections::BTreeMap::new();
        for (key, value) in redirect.query_pairs() {
            if fields
                .insert(key.into_owned(), SecretText::new(value.into_owned()))
                .is_some()
            {
                return Err(OAuthError::InvalidRedirect);
            }
        }
        redirect.set_query(None);
        if redirect
            != url::Url::parse(&request.redirect).map_err(|_| OAuthError::InvalidRedirect)?
        {
            return Err(OAuthError::InvalidRedirect);
        }
        if fields.get("state").map(SecretText::as_str) != Some(request.state.as_str()) {
            return Err(OAuthError::StateMismatch);
        }
        if let Some(error) = fields.get("error") {
            if error.as_str().is_empty() || fields.contains_key("code") {
                return Err(OAuthError::InvalidRedirect);
            }
            let source = OAuthRedirectError { callback };
            if error.as_str() == "access_denied" {
                return Err(OAuthError::Denied(source));
            }
            let failure = match classify_error(error.as_str()) {
                Some(failure) => failure,
                None => StorageFailure::Refused,
            };
            return Err(StorageError::Provider {
                provider,
                failure,
                source: Box::new(source),
            }
            .into());
        }
        fields
            .remove("code")
            .filter(|code| !code.as_str().is_empty())
            .ok_or(OAuthError::MissingCode)
    }
    /// Obtain replacement tokens. The facade commits them to custody before
    /// installing them on its provider session. Omitted refresh tokens retain
    /// the previous token as specified by OAuth.
    async fn refresh(
        &self,
        client_id: &str,
        access: OAuthAccess,
        refresh_token: &SecretText,
    ) -> Result<OAuthTokens, OAuthError> {
        let (id, _, _, _) = self.config(access)?;
        if id != client_id {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("refresh client differs from the held authorization")
                .into());
        }
        let mut replacement = self
            .tokens(
                access,
                &[
                    ("grant_type", "refresh_token"),
                    ("refresh_token", refresh_token.as_str()),
                    ("client_id", id),
                ],
            )
            .await?;
        if replacement.refresh_token.is_none() {
            replacement.refresh_token = Some(refresh_token.clone());
        }
        Ok(replacement)
    }
    async fn tokens(
        &self,
        access: OAuthAccess,
        params: &[(&str, &str)],
    ) -> Result<OAuthTokens, OAuthError> {
        let provider = access.provider();
        let (client_id, _, token, _) = self.config(access)?;
        let mut params = params.to_vec();
        if provider == CloudProvider::GoogleDrive {
            if let Some(secret) = self
                .google
                .as_ref()
                .and_then(|client| client.secret.as_ref())
            {
                params.push(("client_secret", secret.as_str()));
            }
        }
        #[cfg(any(test, feature = "test-utils"))]
        let token = match &self.token_override {
            Some(url) => url.as_str(),
            None => token,
        };
        let client = self
            .client
            .as_ref()
            .map_err(|source| StorageError::Provider {
                provider,
                failure: crate::StorageFailure::Network,
                source: Box::new(source.clone()),
            })?;
        let response = client
            .post(token)
            .form(&params)
            .send()
            .await
            .map_err(|e| http::transport(provider, e))?;
        let status = response.status();
        if !status.is_success() {
            return Err(http::response_error(provider, response).await.into());
        }
        let bytes = SecretBytes::new(
            response
                .bytes()
                .await
                .map_err(|e| http::transport(provider, e))?
                .to_vec(),
        );
        #[derive(serde::Deserialize)]
        struct Tokens {
            access_token: SecretText,
            refresh_token: Option<SecretText>,
            expires_in: Option<u64>,
            token_type: String,
            scope: Option<String>,
        }
        let tokens: Tokens = serde_json::from_slice(bytes.as_bytes())
            .map_err(|error| StorageFailure::Encoding.with_source(error))?;
        if tokens.access_token.as_str().is_empty()
            || !tokens.token_type.eq_ignore_ascii_case("bearer")
        {
            return Err(StorageFailure::Protocol
                .with_source("invalid OAuth token response")
                .into());
        }
        let expires_at = tokens
            .expires_in
            .map(|secs| {
                self.clock
                    .now()
                    .checked_add(Duration::from_secs(secs))
                    .ok_or(OAuthError::InvalidExpiry)
            })
            .transpose()?;
        Ok(OAuthTokens {
            client_id: client_id.to_owned(),
            access: access.granted(tokens.scope.as_deref())?,
            access_token: tokens.access_token,
            refresh_token: tokens.refresh_token,
            expires_at,
        })
    }
    /// Redirect token requests to an HTTP fake while retaining the real protocol.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_token_endpoint(mut self, endpoint: String) -> Self {
        self.token_override = Some(endpoint);
        self
    }
}

#[path = "oauth_admin.rs"]
mod admin;

#[cfg(test)]
#[path = "oauth_tests.rs"]
mod tests;
