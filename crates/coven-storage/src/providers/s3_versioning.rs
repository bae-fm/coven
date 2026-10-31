//! Bucket versioning is owner-controlled state, not a detected adapter setting.
use super::*;
use aws_sdk_s3::types::BucketVersioningStatus;
use std::sync::Arc;

/// A refused or unimplemented read leaves the state unknown. Transport and
/// malformed-response errors fail detection instead of producing an observation.
pub(super) fn unknown(error: &StorageError) -> bool {
    matches!(error.failure(), StorageFailure::PermissionDenied | StorageFailure::Refused)
        || unimplemented(error)
}

fn unimplemented(error: &StorageError) -> bool {
    // A malformed 501 reply is a protocol error, not an API observation.
    error.failure() == StorageFailure::Network && error::native_status(error) == Some(501)
}

impl S3Storage {
    pub(super) async fn bucket_versioning(&self) -> Result<S3BucketVersioning, StorageError> {
        let result = self.client.get_bucket_versioning().bucket(&self.bucket).send().await;
        match result {
            Ok(reply) => {
                match reply.status() {
                    None => Ok(S3BucketVersioning::Disabled),
                    Some(BucketVersioningStatus::Enabled) => Ok(S3BucketVersioning::Enabled),
                    Some(BucketVersioningStatus::Suspended) => Ok(S3BucketVersioning::Suspended),
                    Some(_) => Err(StorageFailure::Protocol
                        .with_source("unrecognized bucket versioning status")),
                }
            }
            Err(error) => {
                let error = s3_error(error);
                // R2 has no object versioning and no GetBucketVersioning API.
                // Only its authoritative hosted endpoint establishes that fact;
                // selecting its name at an arbitrary endpoint does not.
                let StorageConfig::S3 { endpoint, .. } = &self.config else { unreachable!() };
                let hosted_r2 = endpoint
                    .as_ref()
                    .and_then(profile::CataloguedProfile::hosting)
                    .is_some_and(|profile| profile.name == S3ProfileName::R2);
                if hosted_r2
                    && (error.failure() == StorageFailure::PermissionDenied
                        || unimplemented(&error))
                {
                    tracing::debug!(cause = %error, "R2 has no object-versioning API");
                    Ok(S3BucketVersioning::Unavailable)
                } else {
                    Err(error)
                }
            }
        }
    }
}

/// Whether the selected profile holds certification with versioning enabled,
/// which lets setup proceed when the versioning read is refused.
pub(super) fn certified(
    endpoint: Option<&url::Url>,
    profile: Option<&S3Profile>,
) -> Result<bool, StorageError> {
    Ok(matches!(
        profile::select(endpoint, profile)?,
        profile::Selected::Catalogued(profile::CataloguedProfile {
            versioned_buckets: profile::Certification::Passed { .. },
            ..
        })
    ))
}

pub(crate) fn check(
    endpoint: Option<&url::Url>,
    profile: Option<&S3Profile>,
    versioning: &Result<S3BucketVersioning, Arc<StorageError>>,
) -> Result<(), StorageSetupError> {
    let certified = certified(endpoint, profile)?;
    match versioning {
        Ok(S3BucketVersioning::Disabled | S3BucketVersioning::Unavailable) => Ok(()),
        Ok(state) if !certified => {
            Err(StorageSetupError::S3VersioningUnsupported { state: *state })
        }
        Err(source) if !certified => {
            Err(StorageSetupError::S3VersioningUnknown { source: source.clone() })
        }
        Err(source) => {
            tracing::warn!(cause = %source, "S3 versioning unknown; profile certified with versioning enabled");
            Ok(())
        }
        Ok(_) => Ok(()),
    }
}
