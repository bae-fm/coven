//! Resolve the storage credentials a restore uses before it starts.

use super::*;

/// The credentials for a restore code's `storage`. On S3 the key is the one
/// the code carries, or else the one supplied beside it; a code that carries
/// a key takes no second one.
pub(super) fn restore_connection(
    builder: &CovenBuilder,
    storage: RestoreStorage,
    s3_credentials: Option<S3Credentials>,
) -> Result<(ConnectionCredentials, Option<Arc<SessionSignIn>>), BootstrapError> {
    let RestoreStorage { location, s3_key } = storage;
    let s3_credentials = match (s3_key, s3_credentials) {
        (Some(_), Some(_)) => {
            return Err(SyncError::from(
                StorageFailure::InvalidConfiguration
                    .with_source("the restore code carries its own S3 key"),
            )
            .into())
        }
        (carried, supplied) => carried.or(supplied),
    };
    let credentials = match (location.provider(), s3_credentials) {
        (CloudProvider::S3, Some(keys)) => StorageCredentials::S3(keys),
        (CloudProvider::S3, None) => {
            return Err(SyncError::from(
                StorageFailure::InvalidConfiguration
                    .with_source("S3 restore requires an access key"),
            )
            .into())
        }
        (_, Some(_)) => {
            return Err(SyncError::from(
                StorageFailure::InvalidConfiguration
                    .with_source("S3 credentials cannot restore another provider"),
            )
            .into())
        }
        (CloudProvider::CloudKit, None) => StorageCredentials::CloudKit,
        (_, None) => {
            let (data, held) = held_connection(builder, location)?;
            return Ok((data, Some(held)));
        }
    };
    let data = ConnectionCredentials { location, credentials };
    data.validate().map_err(SyncError::from)?;
    Ok((data, None))
}
