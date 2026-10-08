use super::http;
use crate::{CloudProvider, OAuthTokens, StorageError};
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
    /// The provider declined the sign-in.
    #[error("provider denied sign-in")]
    Denied,
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
    pub async fn authenticate(&self, provider: CloudProvider) -> Result<OAuthTokens, OAuthError> {
        let request = self
            .clients
            .build_authorize_request(provider, self.presenter.redirect_uri(provider))?;
        let flow = async {
            let redirect = self.presenter.present(&request.auth_url).await?;
            self.clients
                .exchange_redirect(provider, &request, redirect)
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
        provider: CloudProvider,
        tokens: &OAuthTokens,
    ) -> Result<OAuthTokens, OAuthError> {
        self.clients.refresh(provider, tokens).await
    }
}

/// The app's OAuth client ids and injected clock (E10).
#[derive(Clone)]
pub struct OAuthClients {
    google: Option<String>,
    dropbox: Option<String>,
    onedrive: Option<String>,
    clock: ClockRef,
    client: Result<reqwest::Client, Arc<reqwest::Error>>,
    #[cfg(any(test, feature = "test-utils"))]
    token_override: Option<String>,
}
impl OAuthClients {
    /// Configure client ids, `None` for providers the app does not offer.
    pub fn new(
        google_drive_client_id: Option<String>,
        dropbox_client_id: Option<String>,
        onedrive_client_id: Option<String>,
        clock: ClockRef,
    ) -> Self {
        Self {
            google: google_drive_client_id,
            dropbox: dropbox_client_id,
            onedrive: onedrive_client_id,
            clock,
            client: http::client().map_err(Arc::new),
            #[cfg(any(test, feature = "test-utils"))]
            token_override: None,
        }
    }
    fn config(
        &self,
        provider: CloudProvider,
    ) -> Result<(&str, &'static str, &'static str, &'static str), OAuthError> {
        let (id, auth, token, scope) = match provider {
            CloudProvider::GoogleDrive => (&self.google, "https://accounts.google.com/o/oauth2/v2/auth", "https://oauth2.googleapis.com/token", "https://www.googleapis.com/auth/drive"),
            CloudProvider::Dropbox => (&self.dropbox, "https://www.dropbox.com/oauth2/authorize", "https://api.dropboxapi.com/oauth2/token", "files.content.read files.content.write files.metadata.read sharing.read sharing.write account_info.read"),
            CloudProvider::OneDrive => (&self.onedrive, "https://login.microsoftonline.com/common/oauth2/v2.0/authorize", "https://login.microsoftonline.com/common/oauth2/v2.0/token", "Files.ReadWrite.All offline_access User.Read"),
            _ => return Err(OAuthError::Unavailable(provider)),
        };
        let id = id
            .as_deref()
            .filter(|id| !id.is_empty())
            .ok_or(OAuthError::Unavailable(provider))?;
        Ok((id, auth, token, scope))
    }
    fn build_authorize_request(
        &self,
        provider: CloudProvider,
        redirect_uri: &str,
    ) -> Result<AuthorizeRequest, OAuthError> {
        let (client_id, auth, _, scope) = self.config(provider)?;
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
        match provider {
            CloudProvider::GoogleDrive => {
                url.query_pairs_mut()
                    .append_pair("access_type", "offline")
                    .append_pair("prompt", "consent");
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
        provider: CloudProvider,
        request: &AuthorizeRequest,
        callback: SecretText,
    ) -> Result<OAuthTokens, OAuthError> {
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
        if fields.contains_key("error") {
            return Err(OAuthError::Denied);
        }
        let code = fields
            .get("code")
            .filter(|code| !code.as_str().is_empty())
            .ok_or(OAuthError::MissingCode)?;
        let (client_id, _, _, _) = self.config(provider)?;
        self.tokens(
            provider,
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
    /// Obtain replacement tokens. The facade commits them to custody before
    /// installing them on its provider session. Omitted refresh tokens retain
    /// the previous token as specified by OAuth.
    async fn refresh(
        &self,
        provider: CloudProvider,
        tokens: &OAuthTokens,
    ) -> Result<OAuthTokens, OAuthError> {
        let (id, _, _, _) = self.config(provider)?;
        let refresh = tokens
            .refresh_token
            .as_ref()
            .ok_or(OAuthError::Reauthorize(provider))?;
        let mut replacement = self
            .tokens(
                provider,
                &[
                    ("grant_type", "refresh_token"),
                    ("refresh_token", refresh.as_str()),
                    ("client_id", id),
                ],
            )
            .await?;
        if replacement.refresh_token.is_none() {
            replacement.refresh_token = Some(refresh.clone());
        }
        Ok(replacement)
    }
    async fn tokens(
        &self,
        provider: CloudProvider,
        params: &[(&str, &str)],
    ) -> Result<OAuthTokens, OAuthError> {
        let (_, _, token, _) = self.config(provider)?;
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
            .form(params)
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
        }
        let tokens: Tokens = serde_json::from_slice(bytes.as_bytes())
            .map_err(|error| StorageError::Encoding(Box::new(error)))?;
        if tokens.access_token.as_str().is_empty()
            || !tokens.token_type.eq_ignore_ascii_case("bearer")
        {
            return Err(StorageError::Protocol("invalid OAuth token response").into());
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

#[cfg(test)]
#[path = "oauth_tests.rs"]
mod tests;
