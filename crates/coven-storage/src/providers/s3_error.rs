use crate::{CloudProvider, StorageError, StorageFailure};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use std::fmt;

pub(super) fn s3_error<E>(error: SdkError<E>) -> StorageError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let status = error
        .raw_response()
        .map(|response| response.status().as_u16());
    let failure = match &error {
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
            error: Box::new(error),
        }),
    }
}

/// The native HTTP status behind an S3 adapter error, when a response arrived.
pub(super) fn native_status(error: &StorageError) -> Option<u16> {
    match error {
        StorageError::Provider { source, .. } => source
            .downcast_ref::<S3ResponseError>()
            .and_then(|response| response.status),
        _ => None,
    }
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
        Some("NoSuchBucket") => StorageFailure::ContainerNotFound,
        Some("NoSuchKey" | "NotFound") => StorageFailure::NotFound,
        Some("NoSuchUpload") => StorageFailure::SessionExpired,
        Some("PreconditionFailed") => StorageFailure::AlreadyExists,
        Some("ConditionalRequestConflict" | "OperationAborted") => StorageFailure::Conflict,
        Some("SlowDown" | "Throttling") => StorageFailure::RateLimited,
        Some(
            "PermanentRedirect"
            | "AuthorizationHeaderMalformed"
            | "IncorrectEndpoint"
            | "IllegalLocationConstraintException",
        ) => StorageFailure::RegionMismatch,
        Some("OverQuota" | "QuotaExceeded" | "InsufficientStorage" | "XMinioStorageFull") => {
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

// SDK service errors can echo credentials in their message or response body.
// Keep the original typed cause available through Error::source, but do not
// include it in ordinary storage error formatting.
struct S3ResponseError {
    status: Option<u16>,
    error: Box<dyn std::error::Error + Send + Sync>,
}
impl fmt::Debug for S3ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("S3ResponseError")
    }
}
impl fmt::Display for S3ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("S3 request failed")
    }
}
impl std::error::Error for S3ResponseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error.as_ref())
    }
}
