use super::http;
use crate::{CloudProvider, OAuthTokens, StorageError};
use coven_crypto::{SecretBytes, SecretText};
use coven_foundation::clock::ClockRef;
use oauth2::{CsrfToken, PkceCodeChallenge};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

/// Sign-in failures distinguish cancellation, invalid redirects and provider refusal.
#[derive(Debug, thiserror::Error)]
pub enum OAuthError {
    /// The app did not configure this provider's client id.
    #[error("OAuth provider unavailable: {0:?}")]
    Unavailable(CloudProvider),
    /// The request's provider or redirect does not match the exchange.
    #[error("authorization request does not match this exchange")]
    RequestMismatch,
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
    #[error("provider requires a new sign-in")]
    Reauthorize,
    /// The redirect URI or callback request is malformed.
    #[error("invalid OAuth redirect")]
    InvalidRedirect,
    /// The provider's expiry cannot be represented.
    #[error("invalid OAuth token expiry")]
    InvalidExpiry,
    /// The browser or local redirect listener failed.
    #[error("OAuth browser or listener: {0}")]
    Io(#[from] std::io::Error),
    /// Network or provider failure, preserving its classification.
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// A request for an app that handles the redirect itself (§20.10).
pub struct AuthorizeRequest {
    /// The URL the app opens for sign-in.
    pub auth_url: String,
    verifier: SecretText,
    state: SecretText,
    provider: CloudProvider,
    redirect_uri: String,
    client_id: String,
}

/// The app's OAuth client ids and injected clock (§20.10).
#[derive(Clone)]
pub struct OAuthClients {
    google: Option<String>,
    dropbox: Option<String>,
    onedrive: Option<String>,
    clock: ClockRef,
    client: Result<reqwest::Client, Arc<reqwest::Error>>,
    #[cfg(test)]
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
            #[cfg(test)]
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
    /// Build the URL and private proof for an app-managed redirect.
    pub fn build_authorize_request(
        &self,
        provider: CloudProvider,
        redirect_uri: &str,
    ) -> Result<AuthorizeRequest, OAuthError> {
        let (client_id, auth, _, scope) = self.config(provider)?;
        let redirect = url::Url::parse(redirect_uri).map_err(|_| OAuthError::InvalidRedirect)?;
        if redirect.fragment().is_some() {
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
            provider,
            redirect_uri: redirect_uri.into(),
            client_id: client_id.into(),
        })
    }
    /// Check the redirect's state and exchange its code using this client's clock.
    pub async fn exchange_code(
        &self,
        provider: CloudProvider,
        code: &str,
        callback_state: Option<&str>,
        request: &AuthorizeRequest,
        redirect_uri: &str,
    ) -> Result<OAuthTokens, OAuthError> {
        let (client_id, _, _, _) = self.config(provider)?;
        if request.provider != provider
            || request.redirect_uri != redirect_uri
            || request.client_id != client_id
        {
            return Err(OAuthError::RequestMismatch);
        }
        if callback_state != Some(request.state.as_str()) {
            return Err(OAuthError::StateMismatch);
        }
        if code.is_empty() {
            return Err(OAuthError::MissingCode);
        }
        self.tokens(
            provider,
            &[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", redirect_uri),
                ("client_id", client_id),
                ("code_verifier", request.verifier.as_str()),
            ],
        )
        .await
    }
    /// Obtain replacement tokens. The facade commits them to custody before
    /// installing them on its provider session. Omitted refresh tokens retain
    /// the previous token as specified by OAuth.
    pub async fn refresh(
        &self,
        provider: CloudProvider,
        tokens: &OAuthTokens,
    ) -> Result<OAuthTokens, OAuthError> {
        let (id, _, _, _) = self.config(provider)?;
        let refresh = tokens
            .refresh_token
            .as_ref()
            .ok_or(OAuthError::Reauthorize)?;
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
        #[cfg(test)]
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
    /// Run sign-in in the browser and await its local redirect. All listener work
    /// is scoped to this future; cancellation or dropping it closes the socket.
    pub async fn authorize(
        &self,
        provider: CloudProvider,
        cancel: watch::Receiver<bool>,
    ) -> Result<OAuthTokens, OAuthError> {
        if *cancel.borrow() {
            return Err(OAuthError::Cancelled);
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:19284").await?;
        let redirect = "http://localhost:19284/callback";
        let request = self.build_authorize_request(provider, redirect)?;
        open::that(&request.auth_url)?;
        self.receive_redirect(listener, provider, cancel, request, redirect)
            .await
    }
    async fn receive_redirect(
        &self,
        listener: tokio::net::TcpListener,
        provider: CloudProvider,
        mut cancel: watch::Receiver<bool>,
        request: AuthorizeRequest,
        redirect: &str,
    ) -> Result<OAuthTokens, OAuthError> {
        let callback = async {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let (mut socket, _) = listener.accept().await?;
                let mut data = zeroize::Zeroizing::new(Vec::with_capacity(8192));
                while !data.ends_with(b"\r\n\r\n") {
                    if data.len() >= 8192 {
                        return Err(OAuthError::InvalidRedirect);
                    }
                    let byte = socket.read_u8().await?;
                    data.push(byte);
                }
                let line = std::str::from_utf8(&data)
                    .map_err(|_| OAuthError::InvalidRedirect)?
                    .lines()
                    .next()
                    .ok_or(OAuthError::InvalidRedirect)?;
                let target = line
                    .strip_prefix("GET ")
                    .and_then(|s| s.strip_suffix(" HTTP/1.1"))
                    .ok_or(OAuthError::InvalidRedirect)?;
                if !target.starts_with('/') || target.starts_with("//") {
                    return Err(OAuthError::InvalidRedirect);
                }
                let url = url::Url::parse(redirect)
                    .and_then(|base| base.join(target))
                    .map_err(|_| OAuthError::InvalidRedirect)?;
                if url.path() != "/callback" {
                    socket.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await?;
                    continue;
                }
                let mut fields = std::collections::BTreeMap::new();
                for (key, value) in url.query_pairs() {
                    if fields
                        .insert(key.into_owned(), SecretText::new(value.into_owned()))
                        .is_some()
                    {
                        return Err(OAuthError::InvalidRedirect);
                    }
                }
                if fields.get("state").map(SecretText::as_str) != Some(request.state.as_str()) {
                    return Err(OAuthError::StateMismatch);
                }
                if fields.contains_key("error") {
                    return Err(OAuthError::Denied);
                }
                let code = fields.get("code").ok_or(OAuthError::MissingCode)?;
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n").await?;
                return self
                    .exchange_code(
                        provider,
                        code.as_str(),
                        fields.get("state").map(SecretText::as_str),
                        &request,
                        redirect,
                    )
                    .await;
            }
        };
        tokio::select! {
            result = callback => result,
            _ = cancel.wait_for(|value| *value) => Err(OAuthError::Cancelled),
            _ = tokio::time::sleep(Duration::from_secs(300)) => Err(OAuthError::Timeout),
        }
    }
}

#[cfg(test)]
#[path = "oauth_tests.rs"]
mod tests;
