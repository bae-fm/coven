//! Provider-owned restore payload: location and this member's credentials (§12.1).

use crate::{CloudProvider, StorageConfig, StorageCredentials, StorageError};
use coven_crypto::SecretBytes;
use serde::{Deserialize, Serialize};

/// The storage portion of a restore code. Format treats this as secret bytes;
/// storage validates that the credentials belong to the stated provider.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreStorage {
    /// The shared store's location.
    pub location: StorageConfig,
    /// Credentials for this member, never persisted in public settings.
    pub credentials: StorageCredentials,
}

impl RestoreStorage {
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
            _ => Err(StorageError::InvalidConfiguration(
                "credentials do not match provider",
            )),
        }
    }

    /// Encode without leaving unerased copies of credential bytes.
    pub fn encode(&self) -> Result<SecretBytes, StorageError> {
        self.validate()?;
        crate::secret_json::encode(self)
    }

    /// Decode and validate a restore code's storage bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|error| StorageError::Encoding(Box::new(error)))?;
        value.validate()?;
        Ok(value)
    }
}
