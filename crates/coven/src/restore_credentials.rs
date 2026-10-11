//! Resolve separately supplied storage credentials before restore starts.

use super::*;

pub(super) async fn restore_connection(
    builder: &mut CovenBuilder,
    location: StorageConfig,
    s3_credentials: Option<S3Credentials>,
    cancel: &watch::Receiver<bool>,
) -> Result<ConnectionCredentials, BootstrapError> {
    let credentials = match (location.provider(), s3_credentials) {
        (CloudProvider::S3, Some(keys)) => StorageCredentials::S3(keys),
        (CloudProvider::S3, None) => {
            return Err(SyncError::from(
                StorageFailure::InvalidConfiguration
                    .with_source("S3 restore requires a separately supplied access key"),
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
        (provider, None) => account_credentials(builder, provider, cancel).await?,
    };
    let data = ConnectionCredentials {
        location,
        credentials,
    };
    data.validate().map_err(SyncError::from)?;
    Ok(data)
}
