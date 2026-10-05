use crate::{ByteRange, CloudProvider, OAuthTokens, StorageError, StorageFailure};
use coven_crypto::SecretBytes;
use coven_foundation::clock::ClockRef;
use reqwest::{Method, Response, StatusCode};
use serde_json::Value;
use std::fmt;

/// A device's provider sign-in and HTTP connection, injected into its provider.
pub struct OAuthSession {
    client: reqwest::Client,
    provider: CloudProvider,
    tokens: tokio::sync::RwLock<OAuthTokens>,
    clock: ClockRef,
}
impl OAuthSession {
    /// Build a connection with redirect handling disabled: bearer tokens must not
    /// follow provider-returned URLs to other origins.
    pub fn new(
        provider: CloudProvider,
        tokens: OAuthTokens,
        clock: ClockRef,
    ) -> Result<Self, StorageError> {
        if !matches!(
            provider,
            CloudProvider::GoogleDrive | CloudProvider::Dropbox | CloudProvider::OneDrive
        ) {
            return Err(StorageError::InvalidConfiguration(
                "provider does not use OAuth",
            ));
        }
        Ok(Self {
            client: client().map_err(|e| transport(provider, e))?,
            provider,
            tokens: tokio::sync::RwLock::new(tokens),
            clock,
        })
    }
    /// Install tokens after the facade has committed their refreshed value to custody.
    pub async fn set_tokens(&self, tokens: OAuthTokens) {
        *self.tokens.write().await = tokens;
    }
    pub(crate) fn provider(&self) -> CloudProvider {
        self.provider
    }
    pub(crate) async fn send(
        &self,
        method: Method,
        url: &str,
        headers: &[(&str, String)],
        body: Body,
        authenticated: bool,
    ) -> Result<Response, StorageError> {
        let mut request = self.client.request(method, url);
        if authenticated {
            let tokens = self.tokens.read().await;
            if tokens
                .expires_at
                .is_some_and(|expiry| self.clock.now() >= expiry)
            {
                return Err(StorageError::Provider {
                    provider: self.provider,
                    failure: StorageFailure::Authentication,
                    source: Box::new(super::oauth::OAuthError::Expired),
                });
            }
            request = request.bearer_auth(tokens.access_token.as_str());
        }
        for (name, value) in headers {
            request = request.header(*name, value);
        }
        request = match body {
            Body::Empty => request,
            Body::Json(value) => request.json(&value),
            Body::Bytes(bytes) => request.body(bytes),
        };
        request
            .send()
            .await
            .map_err(|e| transport(self.provider, e))
    }
}
pub(crate) enum Body {
    Empty,
    Json(Value),
    Bytes(Vec<u8>),
}
pub(crate) fn client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(120))
        .build()
}
pub(crate) fn transport(provider: CloudProvider, error: reqwest::Error) -> StorageError {
    let failure = if error.is_builder() {
        StorageFailure::InvalidConfiguration
    } else {
        StorageFailure::Network
    };
    StorageError::Provider {
        provider,
        failure,
        source: Box::new(error.without_url()),
    }
}

/// The original error response. Its body is never printed; providers may echo secrets.
pub struct ProviderResponse {
    status: u16,
    body: SecretBytes,
}
impl ProviderResponse {
    /// The HTTP status returned by the provider.
    pub fn status(&self) -> u16 {
        self.status
    }
    /// Inspect the original provider response explicitly at the app boundary.
    pub fn body(&self) -> &[u8] {
        self.body.as_bytes()
    }
}
impl fmt::Debug for ProviderResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderResponse")
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for ProviderResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "provider returned HTTP {}", self.status)
    }
}
impl std::error::Error for ProviderResponse {}

pub(crate) async fn checked(
    provider: CloudProvider,
    response: Response,
) -> Result<Response, StorageError> {
    if response.status().is_success() {
        return Ok(response);
    }
    Err(response_error(provider, response).await)
}
pub(crate) async fn response_error(provider: CloudProvider, response: Response) -> StorageError {
    let status = response.status().as_u16();
    let body = match response.bytes().await {
        Ok(body) => body.to_vec(),
        Err(error) => return transport(provider, error),
    };
    let failure = classify(provider, status, &body);
    StorageError::Provider {
        provider,
        failure,
        source: Box::new(ProviderResponse {
            status,
            body: SecretBytes::new(body),
        }),
    }
}
pub(crate) fn classify(provider: CloudProvider, status: u16, body: &[u8]) -> StorageFailure {
    let parsed = match serde_json::from_slice::<Value>(body) {
        Ok(value) => Some(value),
        Err(error) => {
            tracing::debug!(%error, "provider error response is not JSON");
            None
        }
    };
    if parsed.as_ref().is_some_and(|value| {
        matches!(
            value["error"].as_str(),
            Some("invalid_grant" | "unauthorized_client" | "invalid_client")
        )
    }) {
        return StorageFailure::Authentication;
    }
    let code = parsed.as_ref().and_then(|value| match provider {
        CloudProvider::GoogleDrive => value["error"]["errors"][0]["reason"].as_str(),
        CloudProvider::Dropbox => value["error_summary"].as_str(),
        CloudProvider::OneDrive => value["error"]["code"].as_str(),
        _ => None,
    });
    if let Some(code) = code {
        match code {
            "storageQuotaExceeded" | "quotaLimitReached" => return StorageFailure::QuotaExceeded,
            "authError" | "InvalidAuthenticationToken" | "invalid_grant" => {
                return StorageFailure::Authentication
            }
            "insufficientFilePermissions" | "accessDenied" => {
                return StorageFailure::PermissionDenied
            }
            "notFound" | "itemNotFound" => return StorageFailure::NotFound,
            "nameAlreadyExists" => return StorageFailure::AlreadyExists,
            "rateLimitExceeded" | "userRateLimitExceeded" | "activityLimitReached" => {
                return StorageFailure::RateLimited
            }
            _ => {}
        }
        if provider == CloudProvider::Dropbox {
            let tags: Vec<_> = code.trim_end_matches('.').split('/').collect();
            if tags.contains(&"not_found") {
                return StorageFailure::NotFound;
            }
            if tags.contains(&"insufficient_space") {
                return StorageFailure::QuotaExceeded;
            }
            if tags.contains(&"conflict") {
                return StorageFailure::AlreadyExists;
            }
            if tags.contains(&"no_permission") {
                return StorageFailure::PermissionDenied;
            }
            if tags.contains(&"expired_access_token") || tags.contains(&"invalid_access_token") {
                return StorageFailure::Authentication;
            }
        }
    }
    match status {
        401 => StorageFailure::Authentication,
        403 => StorageFailure::PermissionDenied,
        404 => StorageFailure::NotFound,
        409 | 412 => StorageFailure::AlreadyExists,
        416 => StorageFailure::InvalidConfiguration,
        429 => StorageFailure::RateLimited,
        507 => StorageFailure::QuotaExceeded,
        408 | 500..=599 => StorageFailure::Network,
        _ => StorageFailure::Refused,
    }
}
pub(crate) async fn json(
    provider: CloudProvider,
    response: Response,
) -> Result<Value, StorageError> {
    let bytes = checked(provider, response)
        .await?
        .bytes()
        .await
        .map_err(|e| transport(provider, e))?;
    Ok(serde_json::from_slice(&bytes)?)
}
pub(crate) fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str, StorageError> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or(StorageError::Protocol("missing string field"))
}
pub(crate) fn array<'a>(value: &'a Value, field: &str) -> Result<&'a Vec<Value>, StorageError> {
    value[field]
        .as_array()
        .ok_or(StorageError::Protocol("missing array field"))
}
pub(crate) fn endpoint(
    base: &str,
    segments: &[&str],
    query: &[(&str, &str)],
) -> Result<String, StorageError> {
    let mut url = url::Url::parse(base)
        .map_err(|_| StorageError::InvalidConfiguration("provider endpoint"))?;
    url.path_segments_mut()
        .map_err(|_| StorageError::InvalidConfiguration("provider endpoint cannot hold paths"))?
        .pop_if_empty()
        .extend(segments);
    if !query.is_empty() {
        url.query_pairs_mut().extend_pairs(query.iter().copied());
    }
    Ok(url.into())
}
pub(crate) fn same_origin(base: &str, target: &str) -> Result<(), StorageError> {
    let base = url::Url::parse(base)
        .map_err(|_| StorageError::InvalidConfiguration("provider endpoint"))?;
    let target =
        url::Url::parse(target).map_err(|_| StorageError::Protocol("invalid provider URL"))?;
    if target.origin() != base.origin()
        || !target.username().is_empty()
        || target.password().is_some()
    {
        return Err(StorageError::Protocol("provider URL crossed origins"));
    }
    Ok(())
}
pub(crate) async fn bytes(
    provider: CloudProvider,
    response: Response,
    range: Option<ByteRange>,
) -> Result<Vec<u8>, StorageError> {
    let response = checked(provider, response).await?;
    if let Some(range) = range {
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(StorageError::Protocol("provider ignored byte range"));
        }
        let header = response
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .ok_or(StorageError::Protocol("missing Content-Range"))?;
        validate_content_range(header, range)?;
    }
    let data = response
        .bytes()
        .await
        .map_err(|e| transport(provider, e))?
        .to_vec();
    if range.is_some_and(|r| r.len() != data.len() as u64) {
        return Err(StorageError::Protocol("short ranged body"));
    }
    Ok(data)
}
pub(crate) fn validate_content_range(header: &str, range: ByteRange) -> Result<(), StorageError> {
    let (bounds, total) = header
        .strip_prefix("bytes ")
        .and_then(|h| h.split_once('/'))
        .ok_or(StorageError::Protocol("invalid Content-Range"))?;
    let total: u64 = total
        .parse()
        .map_err(|_| StorageError::Protocol("invalid Content-Range total"))?;
    if bounds != format!("{}-{}", range.start(), range.end() - 1) || range.end() > total {
        return Err(StorageError::InvalidRange);
    }
    Ok(())
}

pub(crate) async fn verify_published_part(
    storage: &dyn crate::Storage,
    session: &mut crate::UploadSession,
    bytes: &[u8],
) -> Result<(), StorageError> {
    let end = session.end_of_part(bytes.len())?;
    let actual = storage
        .read_range(&session.path, ByteRange::new(session.confirmed, end)?)
        .await?;
    if actual != bytes {
        return Err(StorageError::AlreadyExists);
    }
    session.confirmed = end;
    if end == session.total {
        session.state = crate::session::SessionState::Complete;
    }
    Ok(())
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
