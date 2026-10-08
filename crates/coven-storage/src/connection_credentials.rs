//! The configured location and device credentials used to construct a provider.

use crate::{CloudProvider, StorageConfig, StorageCredentials, StorageError, StorageFailure};

/// Connection data used between coven crates; OAuth tokens never enter restore codes.
#[derive(Clone)]
pub struct ConnectionCredentials {
    /// The shared store's location.
    pub location: StorageConfig,
    /// Credentials for this member, never persisted in public settings.
    pub credentials: StorageCredentials,
}

impl ConnectionCredentials {
    /// Validate both location and credential kind before encoding or connecting.
    pub fn validate(&self) -> Result<(), StorageError> {
        self.location.validate()?;
        match (self.location.provider(), &self.credentials) {
            (CloudProvider::S3, StorageCredentials::S3(keys))
                if !keys.access_key_id.is_empty()
                    && !keys.secret_access_key.as_str().is_empty() =>
            {
                Ok(())
            }
            (
                CloudProvider::GoogleDrive | CloudProvider::Dropbox | CloudProvider::OneDrive,
                StorageCredentials::OAuth(tokens),
            ) if !tokens.access_token.as_str().is_empty() => Ok(()),
            (CloudProvider::CloudKit, StorageCredentials::CloudKit) => Ok(()),
            _ => Err(StorageFailure::InvalidConfiguration
                .with_source("credentials do not match provider")),
        }
    }
}
