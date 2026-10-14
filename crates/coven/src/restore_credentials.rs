//! Resolve separately supplied storage credentials before restore starts.

use super::*;

pub(super) fn restore_connection(
    builder: &CovenBuilder,
    location: StorageConfig,
    s3_credentials: Option<S3Credentials>,
) -> Result<(ConnectionCredentials, Option<Arc<SessionSignIn>>), BootstrapError> {
    let mut sign_in = None;
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
        (CloudProvider::CloudKit, None) => StorageCredentials::CloudKit,
        (provider, None) => {
            let held = account_sign_in(builder, provider)?;
            let tokens = held.tokens()?;
            sign_in = Some(held);
            StorageCredentials::OAuth(tokens)
        }
    };
    let data = ConnectionCredentials {
        location,
        credentials,
    };
    data.validate().map_err(SyncError::from)?;
    Ok((data, sign_in))
}
