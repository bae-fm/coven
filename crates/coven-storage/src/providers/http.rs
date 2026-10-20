use crate::{
    ByteRange, CloudProvider, HeldTokens, OAuthTokens, StorageError, StorageFailure, TokenCustody,
};
use coven_crypto::SecretBytes;
use coven_foundation::clock::ClockRef;
use reqwest::{Method, Response};
use serde_json::Value;
use std::fmt;
use std::sync::Arc;

/// A device's provider sign-in and HTTP connection, injected into its provider.
/// A token is used until the provider rejects it with HTTP 401; the request
/// then refreshes once through the sign-in's custody and is retried once (§20.2).
pub struct OAuthSession {
    client: super::http_client::HttpClient,
    provider: CloudProvider,
    tokens: HeldTokens,
    clock: ClockRef,
}
impl OAuthSession {
    /// Sign in over `client`, which follows no redirect: bearer tokens must not
    /// follow provider-returned URLs to other origins.
    pub(crate) fn new(
        provider: CloudProvider,
        tokens: OAuthTokens,
        custody: Arc<dyn TokenCustody>,
        clock: ClockRef,
        client: super::http_client::HttpClient,
    ) -> Result<Self, StorageError> {
        if !matches!(provider, CloudProvider::Dropbox | CloudProvider::OneDrive) {
            return Err(
                StorageFailure::InvalidConfiguration.with_source("provider does not use OAuth")
            );
        }
        Ok(Self {
            client,
            provider,
            tokens: HeldTokens::new(tokens, custody),
            clock,
        })
    }
    /// Resolve the signed-in account through the provider's own identity API.
    pub async fn account(&self) -> Result<String, StorageError> {
        let api = match self.provider {
            CloudProvider::Dropbox => "https://api.dropboxapi.com/2",
            CloudProvider::OneDrive => "https://graph.microsoft.com/v1.0",
            _ => unreachable!("checked OAuth provider"),
        };
        self.account_at(api).await
    }
    pub(crate) async fn account_at(&self, api: &str) -> Result<String, StorageError> {
        let (method, url, body) = match self.provider {
            CloudProvider::Dropbox => (
                Method::POST,
                endpoint(api, &["users", "get_current_account"], &[])?,
                Body::Json(Value::Null),
            ),
            CloudProvider::OneDrive => (
                Method::GET,
                endpoint(api, &["me"], &[("$select", "mail,userPrincipalName")])?,
                Body::Empty,
            ),
            _ => unreachable!("checked OAuth provider"),
        };
        let value = json(
            self.provider,
            self.send(method, &url, &[], body, true).await?,
        )
        .await?;
        let account = match self.provider {
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
    /// Install tokens after their owner committed them to custody.
    pub async fn set_tokens(&self, tokens: OAuthTokens) {
        self.tokens.install(tokens).await;
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
        self.send_request(None, method, url, headers, body, authenticated)
            .await
    }
    pub(crate) async fn publish(
        &self,
        publication: &crate::PublicationRequest,
        method: Method,
        url: &str,
        headers: &[(&str, String)],
        body: Body,
        authenticated: bool,
    ) -> Result<Response, StorageError> {
        self.send_request(Some(publication), method, url, headers, body, authenticated)
            .await
    }
    async fn send_request(
        &self,
        publication: Option<&crate::PublicationRequest>,
        method: Method,
        url: &str,
        headers: &[(&str, String)],
        body: Body,
        authenticated: bool,
    ) -> Result<Response, StorageError> {
        let mut request = self.client.request(method, url);
        for (name, value) in headers {
            request = request.header(*name, value);
        }
        request = match body {
            Body::Empty => request,
            Body::Json(value) => request.json(&value),
            Body::Bytes(bytes) => request.body(bytes),
        };
        let request = request.build().map_err(|e| transport(self.provider, e))?;
        if !authenticated {
            return self.execute(publication, request, None).await;
        }
        // Buffered bodies clone without copying, so the one retry resends them.
        let retry = request.try_clone();
        let used = self.tokens.current().await;
        let response = self.execute(publication, request, Some(&used)).await?;
        if response.status() != reqwest::StatusCode::UNAUTHORIZED {
            return Ok(response);
        }
        let rejection = response_error(self.provider, response).await;
        // Refreshing cannot cure a rejection other than the token's, such as
        // Dropbox's missing scope or a suspended account.
        if rejection.failure() != StorageFailure::Authentication {
            return Err(rejection);
        }
        let Some(retry) = retry else {
            return Err(rejection);
        };
        let replacement = self.tokens.rejected(&used).await?;
        self.execute(publication, retry, Some(&replacement)).await
    }
    /// Send one request with `tokens`, labelling the response with the access
    /// they carried: a later replacement must not relabel it.
    async fn execute(
        &self,
        publication: Option<&crate::PublicationRequest>,
        mut request: reqwest::Request,
        tokens: Option<&OAuthTokens>,
    ) -> Result<Response, StorageError> {
        if let Some(tokens) = tokens {
            let mut value = reqwest::header::HeaderValue::try_from(format!(
                "Bearer {}",
                tokens.access_token.as_str()
            ))
            .map_err(|error| StorageFailure::InvalidConfiguration.with_source(error))?;
            value.set_sensitive(true);
            request
                .headers_mut()
                .insert(reqwest::header::AUTHORIZATION, value);
        }
        if let Some(publication) = publication {
            publication.check()?;
        }
        let mut response = self
            .client
            .execute(request)
            .await
            .map_err(|error| error.into_storage(self.provider))?;
        if let Some(tokens) = tokens {
            response.extensions_mut().insert(tokens.access);
        }
        Ok(response)
    }
}
pub(crate) enum Body {
    Empty,
    Json(Value),
    Bytes(Vec<u8>),
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
    /// A response carrying `headers`, as a native adapter or fixture saw it.
    pub(crate) fn new(status: u16, headers: reqwest::header::HeaderMap, body: Vec<u8>) -> Self {
        Self {
            status,
            headers,
            body: SecretBytes::new(body),
        }
    }
    /// A `status` response, asking for `retry_after` seconds of
    /// `Retry-After` when given, classified as `failure`, as the in-memory
    /// provider injects it.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn injected(
        provider: CloudProvider,
        failure: StorageFailure,
        status: u16,
        retry_after: Option<u64>,
    ) -> StorageError {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(seconds) = retry_after {
            headers.insert(reqwest::header::RETRY_AFTER, seconds.into());
        }
        StorageError::Provider {
            provider,
            failure,
            source: Box::new(Self::new(status, headers, Vec::new())),
        }
    }
    /// The provider's requested wait: `Retry-After` as delay-seconds, or as an
    /// HTTP-date less this response's own `Date` (§6.7). Dropbox also
    /// states it in its error body.
    pub(crate) fn retry_after(&self, provider: CloudProvider) -> Option<crate::RetryAfter> {
        use reqwest::header::{DATE, RETRY_AFTER};
        if let Some(value) = self.headers.get(RETRY_AFTER) {
            return Some(retry_after_header(
                value.to_str().unwrap_or_default(),
                self.headers.get(DATE).and_then(|date| date.to_str().ok()),
            ));
        }
        if provider == CloudProvider::Dropbox {
            let value: Value = serde_json::from_slice(self.body()).ok()?;
            return value["error"]["retry_after"]
                .as_u64()
                .map(|seconds| crate::RetryAfter::Wait(std::time::Duration::from_secs(seconds)));
        }
        None
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
/// Interpret HTTP retry timing using the response's clock, for both HTTP clients.
pub(crate) fn retry_after_header(value: &str, date: Option<&str>) -> crate::RetryAfter {
    use crate::RetryAfter;
    use std::time::Duration;
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return RetryAfter::Wait(Duration::from_secs(seconds));
    }
    let parse = |value: &str| {
        use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
        DateTime::from_str(value.trim(), DateTimeFormat::HttpDate)
            .ok()
            .map(|time| time.secs())
    };
    match (parse(value), date.and_then(parse)) {
        (Some(retry), Some(basis)) => RetryAfter::Wait(Duration::from_secs(
            retry.saturating_sub(basis).max(0) as u64,
        )),
        _ => RetryAfter::NoTimeBasis,
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
        source: Box::new(ProviderResponse::new(status, headers, body)),
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
    ProviderResponse::new(status, headers, body).into_error(provider)
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
    let native = parsed.as_ref().and_then(|value| match provider {
        CloudProvider::Dropbox => dropbox_failure(value),
        CloudProvider::OneDrive => onedrive_failure(value),
        _ => None,
    });
    if let Some(failure) = native {
        return failure;
    }
    match status {
        401 => StorageFailure::Authentication,
        403 => StorageFailure::PermissionDenied,
        404 => StorageFailure::NotFound,
        409 if provider == CloudProvider::Dropbox => StorageFailure::Refused,
        409 | 412 => StorageFailure::AlreadyExists,
        416 => StorageFailure::InvalidRange,
        // Another party's lock on the item lasts only while its work runs.
        423 => StorageFailure::Conflict,
        429 => StorageFailure::RateLimited,
        507 => StorageFailure::QuotaExceeded,
        408 | 500..=599 => StorageFailure::Network,
        _ => StorageFailure::Refused,
    }
}
fn onedrive_failure(value: &Value) -> Option<StorageFailure> {
    Some(match value["error"]["code"].as_str()? {
        "quotaLimitReached" => StorageFailure::QuotaExceeded,
        "InvalidAuthenticationToken" => StorageFailure::Authentication,
        "accessDenied" => StorageFailure::PermissionDenied,
        "itemNotFound" => StorageFailure::NotFound,
        "nameAlreadyExists" => StorageFailure::AlreadyExists,
        "activityLimitReached" => StorageFailure::RateLimited,
        "resourceLocked" => StorageFailure::Conflict,
        _ => return None,
    })
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
    } else if names.contains(&"not_found") || names.contains(&"not_folder") {
        // `not_folder`: a file holds the name of a folder the path needs, so
        // nothing exists beneath it.
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
    Ok((value, ProviderResponse::new(status, headers, body.to_vec())))
}
pub(crate) fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str, StorageError> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or(StorageFailure::Protocol.with_source("missing string field"))
}
/// An optional nullable provider field must still be text when present.
pub(crate) fn optional_string<'a>(
    value: &'a Value,
    field: &str,
) -> Result<Option<&'a str>, StorageError> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(StorageFailure::Protocol.with_source("non-string optional field")),
    }
}
pub(crate) fn timestamp(
    value: &Value,
    field: &str,
) -> Result<coven_format::value::StorageTime, StorageError> {
    use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
    let timestamp = DateTime::from_str(string(value, field)?, DateTimeFormat::DateTimeWithOffset)
        .map_err(|error| StorageFailure::Protocol.with_source(error))?;
    storage_time(timestamp)
}
/// A provider's date with every returned digit; a time before the Unix epoch
/// is a protocol error.
pub(crate) fn storage_time(
    time: aws_sdk_s3::primitives::DateTime,
) -> Result<coven_format::value::StorageTime, StorageError> {
    let seconds = u64::try_from(time.secs())
        .map_err(|_| StorageFailure::Protocol.with_source("provider time precedes the epoch"))?;
    coven_format::value::StorageTime::new(seconds, time.subsec_nanos())
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
    let actual = storage.read_object(session.path(), Some(range)).await?;
    actual.check(session.path(), Some(range))?;
    if actual.metadata.size != session.total || actual.bytes != bytes {
        return Err(StorageFailure::AlreadyExists.into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
