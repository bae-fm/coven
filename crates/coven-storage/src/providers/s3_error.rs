use crate::{CloudProvider, StorageError, StorageFailure};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use std::fmt;

pub(super) fn s3_error<E>(error: SdkError<E>) -> StorageError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    // A local send refusal is not a provider network or configuration failure.
    let mut cause: &(dyn std::error::Error + 'static) = &error;
    loop {
        if let Some(refusal) = cause.downcast_ref::<StorageError>() {
            return refusal.failure().with_source(error);
        }
        match cause.source() {
            Some(source) => cause = source,
            None => break,
        }
    }
    let status = error
        .raw_response()
        .map(|response| response.status().as_u16());
    let failure = match &error {
        // The device clock was refused after the retry its offset allowed (§20.7).
        _ if super::clock::clock_refused(error.raw_response()) => StorageFailure::ClockSkew,
        SdkError::ConstructionFailure(_) => StorageFailure::InvalidConfiguration,
        SdkError::ResponseError(_) => StorageFailure::Protocol,
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) => StorageFailure::Network,
        SdkError::ServiceError(service) => {
            s3_service_failure(service.err(), service.raw().status().as_u16())
        }
        _ => StorageFailure::Protocol,
    };
    StorageError::Provider {
        provider: CloudProvider::S3,
        failure,
        source: Box::new(S3ResponseError {
            status,
            code: error.code().map(str::to_owned),
            retry_after: error.raw_response().and_then(|response| {
                response.headers().get("retry-after").map(|value| {
                    crate::providers::http::retry_after_header(
                        value,
                        response.headers().get("date"),
                    )
                })
            }),
            error: Box::new(error),
        }),
    }
}

/// The native HTTP status behind an S3 adapter error, when a response arrived.
pub(super) fn native_status(error: &StorageError) -> Option<u16> {
    match error {
        StorageError::Provider { source, .. } => response_status(source.as_ref()),
        _ => None,
    }
}

/// The status of the response behind an S3 failure's native cause, if one arrived.
pub(crate) fn response_status(source: &(dyn std::error::Error + 'static)) -> Option<u16> {
    source
        .downcast_ref::<S3ResponseError>()
        .and_then(|response| response.status)
}

/// The retry delay retained from the SDK's original response.
pub(crate) fn response_retry_after(
    source: &(dyn std::error::Error + 'static),
) -> Option<crate::RetryAfter> {
    source.downcast_ref::<S3ResponseError>()?.retry_after
}

fn s3_service_failure(
    error: &(impl ProvideErrorMetadata + std::error::Error),
    status: u16,
) -> StorageFailure {
    match error.code() {
        Some(
            "InvalidAccessKeyId"
            | "SignatureDoesNotMatch"
            | "InvalidToken"
            | "ExpiredToken"
            | "InvalidClientTokenId",
        ) => StorageFailure::Authentication,
        Some("AccessDenied" | "AllAccessDisabled" | "AccountProblem") => {
            StorageFailure::PermissionDenied
        }
        Some("RequestTimeTooSkewed") => StorageFailure::ClockSkew,
        Some("NoSuchBucket") => StorageFailure::ContainerNotFound,
        Some("NoSuchKey" | "NotFound") => StorageFailure::NotFound,
        Some("NoSuchUpload") => StorageFailure::SessionExpired,
        Some("PreconditionFailed") => StorageFailure::AlreadyExists,
        Some("ConditionalRequestConflict" | "OperationAborted" | "ConcurrentModification") => {
            StorageFailure::Conflict
        }
        Some("SlowDown" | "Throttling") => StorageFailure::RateLimited,
        Some(
            "PermanentRedirect"
            | "AuthorizationHeaderMalformed"
            | "IncorrectEndpoint"
            | "IllegalLocationConstraintException",
        ) => StorageFailure::RegionMismatch,
        Some("OverQuota" | "QuotaExceeded" | "InsufficientStorage") => {
            StorageFailure::QuotaExceeded
        }
        Some("RequestTimeout") => StorageFailure::Network,
        Some("InvalidObjectState") => StorageFailure::Refused,
        Some("InvalidRange") => StorageFailure::InvalidRange,
        // The SDK also wraps XML decoding failures in ServiceError. A parsed
        // response without an error code (including HEAD) has ErrorMetadata as
        // its source; a decoding failure retains the parser's native error.
        None if error
            .source()
            .is_some_and(|source| !source.is::<aws_sdk_s3::error::ErrorMetadata>()) =>
        {
            StorageFailure::Protocol
        }
        _ => match status {
            401 => StorageFailure::Authentication,
            403 => StorageFailure::PermissionDenied,
            404 => StorageFailure::NotFound,
            412 => StorageFailure::AlreadyExists,
            429 => StorageFailure::RateLimited,
            507 => StorageFailure::QuotaExceeded,
            408 | 500..=599 => StorageFailure::Network,
            _ => StorageFailure::Refused,
        },
    }
}

// An S3 service's reply can echo credentials in its message or body, so a
// failure that got a reply prints only its HTTP status and error code. One that
// got none prints its cause chain, its transport, TLS or DNS failure, with each
// URL cut to its scheme and host: a signed URL's query and the object's path
// never print. The original typed cause stays available through Error::source.
struct S3ResponseError {
    status: Option<u16>,
    code: Option<String>,
    retry_after: Option<crate::RetryAfter>,
    error: Box<dyn std::error::Error + Send + Sync>,
}
impl fmt::Debug for S3ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("S3ResponseError")
            .field(&format_args!("{self}"))
            .finish()
    }
}
impl fmt::Display for S3ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(status) = self.status {
            write!(f, "S3 answered HTTP {status}")?;
            if let Some(code) = &self.code {
                write!(f, " ({code})")?;
            }
            return Ok(());
        }
        f.write_str("S3 request failed")?;
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(self.error.as_ref());
        while let Some(error) = cause {
            write!(f, ": {}", redact_urls(&error.to_string()))?;
            cause = error.source();
        }
        Ok(())
    }
}
/// `text` with each URL cut to its scheme, host and port.
fn redact_urls(text: &str) -> String {
    let mut redacted = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = ["http://", "https://"]
        .iter()
        .filter_map(|scheme| rest.find(scheme))
        .min()
    {
        redacted.push_str(&rest[..start]);
        let end = rest[start..]
            .find(|c: char| c.is_whitespace() || matches!(c, ')' | '"' | '\'' | '>'))
            .map_or(rest.len(), |length| start + length);
        match url::Url::parse(&rest[start..end]) {
            Ok(url) => {
                redacted.push_str(url.scheme());
                redacted.push_str("://");
                redacted.push_str(url.host_str().unwrap_or_default());
                if let Some(port) = url.port() {
                    redacted.push_str(&format!(":{port}"));
                }
                redacted.push_str("/…");
            }
            Err(_) => redacted.push_str("<url>"),
        }
        rest = &rest[end..];
    }
    redacted.push_str(rest);
    redacted
}
impl std::error::Error for S3ResponseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error.as_ref())
    }
}
