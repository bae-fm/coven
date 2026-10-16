//! Google sign-in for the storage account's admin access (§4): the app's
//! Google client, the installed-app loopback with PKCE, and the
//! `cloud-platform` scope. The access token lives for one operation; no
//! refresh token is requested or kept.
use super::*;

/// The scope managing service accounts, HMAC keys and managed folders.
pub(crate) const CLOUD_PLATFORM: &str = "https://www.googleapis.com/auth/cloud-platform";

/// Google sign-in administers GCS, an S3 location (§4): its redirect, its
/// unavailability and its failures name the S3 provider.
pub(super) const GOOGLE_ADMIN: CloudProvider = CloudProvider::S3;

impl OAuthFlow {
    /// Present Google sign-in for admin access and return its access token.
    /// Dropping this future dismisses the sign-in; it times out after five
    /// minutes like provider sign-in.
    pub async fn google_cloud_admin(&self) -> Result<SecretText, OAuthError> {
        let google = self
            .clients
            .google
            .as_ref()
            .ok_or(OAuthError::Unavailable(GOOGLE_ADMIN))?;
        let redirect_uri = self.presenter.redirect_uri(GOOGLE_ADMIN);
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
        let mut url =
            url::Url::parse("https://accounts.google.com/o/oauth2/v2/auth").expect("static URL");
        url.query_pairs_mut().extend_pairs([
            ("response_type", "code"),
            ("client_id", google.id.as_str()),
            ("redirect_uri", redirect_uri),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
            ("state", state.secret()),
            ("scope", CLOUD_PLATFORM),
            ("access_type", "online"),
            ("prompt", "consent"),
        ]);
        let request = AuthorizeRequest {
            auth_url: url.into(),
            verifier: SecretText::new(verifier.secret().clone()),
            state: SecretText::new(state.secret().clone()),
            redirect: redirect_uri.to_owned(),
        };
        let flow = async {
            let callback = self.presenter.present(&request.auth_url).await?;
            let code = OAuthClients::redirect_code(GOOGLE_ADMIN, &request, callback)?;
            self.clients.admin_token(&request, &code).await
        };
        tokio::select! {
            result = flow => result,
            _ = self.clients.clock.sleep(Duration::from_secs(300)) => Err(OAuthError::Timeout),
        }
    }
}

impl OAuthClients {
    async fn admin_token(
        &self,
        request: &AuthorizeRequest,
        code: &SecretText,
    ) -> Result<SecretText, OAuthError> {
        let provider = GOOGLE_ADMIN;
        let google = self
            .google
            .as_ref()
            .ok_or(OAuthError::Unavailable(provider))?;
        let mut params = vec![
            ("grant_type", "authorization_code"),
            ("code", code.as_str()),
            ("redirect_uri", request.redirect.as_str()),
            ("client_id", google.id.as_str()),
            ("code_verifier", request.verifier.as_str()),
        ];
        if let Some(secret) = &google.secret {
            params.push(("client_secret", secret.as_str()));
        }
        #[allow(unused_mut)]
        let mut token = "https://oauth2.googleapis.com/token";
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(url) = &self.token_override {
            token = url.as_str();
        }
        let client = self
            .client
            .as_ref()
            .map_err(|source| StorageError::Provider {
                provider,
                failure: StorageFailure::Network,
                source: Box::new(source.clone()),
            })?;
        let response = client
            .post(token)
            .form(&params)
            .send()
            .await
            .map_err(|e| http::transport(provider, e))?;
        if !response.status().is_success() {
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
        struct Token {
            access_token: SecretText,
            token_type: String,
            scope: Option<String>,
        }
        let token: Token = serde_json::from_slice(bytes.as_bytes())
            .map_err(|error| StorageFailure::Encoding.with_source(error))?;
        if token.access_token.as_str().is_empty()
            || !token.token_type.eq_ignore_ascii_case("bearer")
        {
            return Err(StorageFailure::Protocol
                .with_source("invalid OAuth token response")
                .into());
        }
        // An omitted scope means the requested one (OAuth 2.0); a narrower
        // grant cannot manage keys.
        if token
            .scope
            .as_deref()
            .is_some_and(|scope| !scope.split_ascii_whitespace().any(|s| s == CLOUD_PLATFORM))
        {
            return Err(StorageFailure::PermissionDenied
                .with_source("Google sign-in did not grant cloud-platform")
                .into());
        }
        Ok(token.access_token)
    }
}

#[cfg(test)]
#[path = "oauth_admin_tests.rs"]
mod tests;
