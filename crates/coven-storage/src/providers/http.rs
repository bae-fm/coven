use crate::{ByteRange, CloudProvider, OAuthTokens, StorageError, StorageFailure};
use coven_crypto::SecretBytes;
use coven_foundation::clock::ClockRef;
use reqwest::{Method, Response};
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
            return Err(
                StorageFailure::InvalidConfiguration.with_source("provider does not use OAuth")
            );
        }
        Ok(Self {
            client: client().map_err(|e| transport(provider, e))?,
            provider,
            tokens: tokio::sync::RwLock::new(tokens),
            clock,
        })
    }
    /// Resolve the signed-in account through the provider's own identity API.
    pub async fn account(&self) -> Result<String, StorageError> {
        let (method, url) = match self.provider {
            CloudProvider::GoogleDrive => (
                Method::GET,
                "https://www.googleapis.com/drive/v3/about?fields=user(emailAddress)",
            ),
            CloudProvider::Dropbox => (
                Method::POST,
                "https://api.dropboxapi.com/2/users/get_current_account",
            ),
            CloudProvider::OneDrive => (
                Method::GET,
                "https://graph.microsoft.com/v1.0/me?$select=mail,userPrincipalName",
            ),
            _ => {
                return Err(
                    StorageFailure::InvalidConfiguration.with_source("provider does not use OAuth")
                )
            }
        };
        let value = json(
            self.provider,
            self.send(method, url, &[], Body::Empty, true).await?,
        )
        .await?;
        let account = match self.provider {
            CloudProvider::GoogleDrive => string(&value["user"], "emailAddress")?,
            CloudProvider::Dropbox => string(&value, "email")?,
            CloudProvider::OneDrive => {
                if value["mail"].is_null() {
                    string(&value, "userPrincipalName")?
                } else {
                    string(&value, "mail")?
                }
            }
            _ => unreachable!("checked OAuth provider"),
        };
        if account.is_empty() {
            return Err(StorageFailure::Protocol.with_source("empty signed-in account"));
        }
        Ok(account.to_ascii_lowercase())
    }
    /// Install tokens after the facade has committed their refreshed value to custody.
    pub async fn set_tokens(&self, tokens: OAuthTokens) {
        *self.tokens.write().await = tokens;
    }
    pub(crate) async fn access(&self) -> crate::OAuthAccess {
        self.tokens.read().await.access
    }
    pub(crate) fn provider(&self) -> CloudProvider {
        self.provider
    }
    pub(crate) async fn sleep(&self, duration: std::time::Duration) {
        self.clock.sleep(duration).await;
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
        let access = if authenticated {
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
            Some(tokens.access)
        } else {
            None
        };
        for (name, value) in headers {
            request = request.header(*name, value);
        }
        request = match body {
            Body::Empty => request,
            Body::Json(value) => request.json(&value),
            Body::Bytes(bytes) => request.body(bytes),
        };
        let mut response = request
            .send()
            .await
            .map_err(|e| transport(self.provider, e))?;
        // Token replacement must not relabel a request already in flight.
        if let Some(access) = access {
            response.extensions_mut().insert(access);
        }
        Ok(response)
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
    headers: reqwest::header::HeaderMap,
    body: SecretBytes,
}
impl ProviderResponse {
    /// The HTTP status returned by the provider.
    pub fn status(&self) -> u16 {
        self.status
    }
    /// Original response headers, including Retry-After. Inspect explicitly;
    /// they are redacted from ordinary error formatting along with the body.
    pub fn headers(&self) -> &reqwest::header::HeaderMap {
        &self.headers
    }
    pub(crate) fn into_error(self, provider: CloudProvider) -> StorageError {
        StorageError::Provider {
            provider,
            failure: classify(provider, self.status, self.body()),
            source: Box::new(self),
        }
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

pub(crate) fn invalid_response(
    provider: CloudProvider,
    status: u16,
    headers: reqwest::header::HeaderMap,
    body: Vec<u8>,
) -> StorageError {
    StorageError::Provider {
        provider,
        failure: StorageFailure::Protocol,
        source: Box::new(ProviderResponse {
            status,
            headers,
            body: SecretBytes::new(body),
        }),
    }
}

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
    let headers = response.headers().clone();
    let body = match response.bytes().await {
        Ok(body) => body.to_vec(),
        Err(error) => return transport(provider, error),
    };
    ProviderResponse {
        status,
        headers,
        body: SecretBytes::new(body),
    }
    .into_error(provider)
}
pub(crate) fn container_error(mut error: StorageError) -> StorageError {
    match &mut error {
        StorageError::Provider { failure, .. } if *failure == StorageFailure::NotFound => {
            *failure = StorageFailure::ContainerNotFound;
        }
        _ => {}
    }
    error
}
pub(crate) fn classify(provider: CloudProvider, status: u16, body: &[u8]) -> StorageFailure {
    let parsed = match serde_json::from_slice::<Value>(body) {
        Ok(value) => Some(value),
        Err(error) => {
            tracing::debug!(%error, "provider error response is not JSON");
            None
        }
    };
    if let Some(failure) = parsed
        .as_ref()
        .and_then(|value| value["error"].as_str())
        .and_then(super::oauth::classify_error)
    {
        return failure;
    }
    if provider == CloudProvider::Dropbox {
        if let Some(failure) = parsed.as_ref().and_then(dropbox_failure) {
            return failure;
        }
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
    }
    match status {
        401 => StorageFailure::Authentication,
        403 => StorageFailure::PermissionDenied,
        404 => StorageFailure::NotFound,
        409 if provider == CloudProvider::Dropbox => StorageFailure::Refused,
        409 | 412 => StorageFailure::AlreadyExists,
        416 => StorageFailure::InvalidRange,
        429 => StorageFailure::RateLimited,
        507 => StorageFailure::QuotaExceeded,
        408 | 500..=599 => StorageFailure::Network,
        _ => StorageFailure::Refused,
    }
}
fn dropbox_failure(value: &Value) -> Option<StorageFailure> {
    fn tags<'a>(value: &'a Value, result: &mut Vec<&'a str>) {
        if let Some(object) = value.as_object() {
            if let Some(tag) = object.get(".tag").and_then(Value::as_str) {
                result.push(tag);
            }
            for child in object.values().filter(|child| child.is_object()) {
                tags(child, result);
            }
        }
    }
    let mut names = Vec::new();
    if let Some(summary) = value["error_summary"].as_str() {
        names.extend(summary.trim_end_matches('.').split('/'));
    }
    // Dropbox reports its monthly data-transport quota as this AccessError.
    // Other invalid-account-type cases still describe unavailable permissions.
    let transport_limit = names.starts_with(&["invalid_account_type", "feature"])
        || (value["error"][".tag"].as_str() == Some("invalid_account_type")
            && value["error"]["invalid_account_type"][".tag"].as_str() == Some("feature"));
    tags(&value["error"], &mut names);
    tags(&value["failed"], &mut names);
    // Specific native failures take precedence over HTTP 409's many meanings.
    if names.iter().any(|name| {
        matches!(
            *name,
            "too_many_requests" | "too_many_write_operations" | "rate_limit"
        )
    }) {
        Some(StorageFailure::RateLimited)
    } else if names.contains(&"missing_scope") {
        // Refreshing the same authorization cannot add a required scope.
        Some(StorageFailure::InvalidConfiguration)
    } else if names
        .iter()
        .any(|name| matches!(*name, "expired_access_token" | "invalid_access_token"))
    {
        Some(StorageFailure::Authentication)
    } else if names.iter().any(|name| {
        matches!(
            *name,
            "no_permission"
                | "no_write_permission"
                | "access_denied"
                | "not_a_member"
                | "invalid_member"
                | "user_suspended"
        )
    }) {
        Some(StorageFailure::PermissionDenied)
    } else if transport_limit
        || names.iter().any(|name| {
            matches!(
                *name,
                "insufficient_space"
                    | "team_folder_insufficient_space"
                    | "member_folder_insufficient_space"
                    | "upload_traffic_limit_reached"
            )
        })
    {
        Some(StorageFailure::QuotaExceeded)
    } else if names.contains(&"conflict") {
        Some(StorageFailure::AlreadyExists)
    } else if names.contains(&"invalid_root")
        || names.contains(&"invalid_namespace_id")
        || names.contains(&"invalid_id")
    {
        Some(StorageFailure::ContainerNotFound)
    } else if names.contains(&"not_found") {
        Some(StorageFailure::NotFound)
    } else {
        None
    }
}
pub(crate) async fn json(
    provider: CloudProvider,
    response: Response,
) -> Result<Value, StorageError> {
    Ok(json_response(provider, response).await?.0)
}
pub(crate) async fn json_response(
    provider: CloudProvider,
    response: Response,
) -> Result<(Value, ProviderResponse), StorageError> {
    let response = checked(provider, response).await?;
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    let body = response
        .bytes()
        .await
        .map_err(|error| transport(provider, error))?;
    let value = serde_json::from_slice(&body)
        .map_err(|_| invalid_response(provider, status, headers.clone(), body.to_vec()))?;
    Ok((
        value,
        ProviderResponse {
            status,
            headers,
            body: SecretBytes::new(body.to_vec()),
        },
    ))
}
pub(crate) fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str, StorageError> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or(StorageFailure::Protocol.with_source("missing string field"))
}
pub(crate) fn timestamp(value: &Value, field: &str) -> Result<std::time::SystemTime, StorageError> {
    use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
    let timestamp = DateTime::from_str(string(value, field)?, DateTimeFormat::DateTimeWithOffset)
        .map_err(|error| StorageFailure::Protocol.with_source(error))?;
    timestamp
        .try_into()
        .map_err(|error| StorageFailure::Protocol.with_source(error))
}
pub(crate) fn array<'a>(value: &'a Value, field: &str) -> Result<&'a Vec<Value>, StorageError> {
    value[field]
        .as_array()
        .ok_or(StorageFailure::Protocol.with_source("missing array field"))
}
pub(crate) fn endpoint(
    base: &str,
    segments: &[&str],
    query: &[(&str, &str)],
) -> Result<String, StorageError> {
    let mut url = url::Url::parse(base)
        .map_err(|error| StorageFailure::InvalidConfiguration.with_source(error))?;
    url.path_segments_mut()
        .map_err(|_| {
            StorageFailure::InvalidConfiguration.with_source("provider endpoint cannot hold paths")
        })?
        .pop_if_empty()
        .extend(segments);
    if !query.is_empty() {
        url.query_pairs_mut().extend_pairs(query.iter().copied());
    }
    Ok(url.into())
}
pub(crate) fn same_origin(base: &str, target: &str) -> Result<(), StorageError> {
    let base = url::Url::parse(base)
        .map_err(|error| StorageFailure::InvalidConfiguration.with_source(error))?;
    let target =
        url::Url::parse(target).map_err(|error| StorageFailure::Protocol.with_source(error))?;
    if target.origin() != base.origin()
        || !target.username().is_empty()
        || target.password().is_some()
    {
        return Err(StorageFailure::Protocol.with_source("provider URL crossed origins"));
    }
    Ok(())
}
pub(crate) fn validate_content_range(header: &str, range: ByteRange) -> Result<u64, StorageError> {
    let (bounds, total) = header
        .strip_prefix("bytes ")
        .and_then(|h| h.split_once('/'))
        .ok_or(StorageFailure::Protocol.with_source("invalid Content-Range"))?;
    let total: u64 = total
        .parse()
        .map_err(|error| StorageFailure::Protocol.with_source(error))?;
    if range.start() < total
        && range.end() > total
        && bounds == format!("{}-{}", range.start(), total - 1)
    {
        return Err(StorageFailure::InvalidRange.into());
    }
    if bounds != format!("{}-{}", range.start(), range.end() - 1) || range.end() > total {
        return Err(StorageFailure::Protocol.with_source("Content-Range disagrees with request"));
    }
    Ok(total)
}

pub(crate) async fn verify_published_part(
    storage: &dyn crate::ProviderOps,
    session: &mut crate::UploadSession,
    bytes: &[u8],
    end: u64,
) -> Result<(), StorageError> {
    compare_published_part(storage, session, bytes, end).await?;
    session.confirmed = end;
    if end == session.total {
        session.state = crate::session::SessionState::Complete;
    }
    Ok(())
}

pub(crate) async fn compare_published_part(
    storage: &dyn crate::ProviderOps,
    session: &crate::UploadSession,
    bytes: &[u8],
    end: u64,
) -> Result<(), StorageError> {
    let range = ByteRange::new(session.confirmed, end)?;
    let actual = storage.read_object(&session.path, Some(range)).await?;
    actual.check(&session.path, Some(range))?;
    if actual.metadata.size != session.total || actual.bytes != bytes {
        return Err(StorageFailure::AlreadyExists.into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
