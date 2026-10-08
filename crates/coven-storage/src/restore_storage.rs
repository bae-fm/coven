//! Storage information carried by a restore code, without device sign-in tokens.

use crate::{
    CloudProvider, ConnectionCredentials, S3Credentials, StorageConfig, StorageCredentials,
    StorageError,
};
use coven_crypto::SecretBytes;
use serde::{Deserialize, Serialize};

/// A restore code names the location and, only for S3, carries the member's key.
#[derive(Clone, Serialize, Deserialize)]
pub enum RestoreStorage {
    /// The new device signs in to its own provider account.
    Account(StorageConfig),
    /// The member's manually issued S3 key also works on the new device.
    S3 {
        /// The store's bucket and prefix.
        location: StorageConfig,
        /// The member's own S3 key.
        credentials: S3Credentials,
    },
}

impl RestoreStorage {
    /// Strip device-only tokens when preparing a restore code.
    pub fn from_connection(data: &ConnectionCredentials) -> Self {
        match &data.credentials {
            StorageCredentials::S3(credentials) => Self::S3 {
                location: data.location.clone(),
                credentials: credentials.clone(),
            },
            StorageCredentials::OAuth(_) | StorageCredentials::CloudKit => {
                Self::Account(data.location.clone())
            }
        }
    }

    /// The location to connect after obtaining this device's credentials.
    pub fn location(&self) -> &StorageConfig {
        match self {
            Self::Account(location) | Self::S3 { location, .. } => location,
        }
    }

    fn validate(&self) -> Result<(), StorageError> {
        match self {
            Self::Account(location) if location.provider() != CloudProvider::S3 => {
                location.validate()
            }
            Self::S3 {
                location,
                credentials,
            } => ConnectionCredentials {
                location: location.clone(),
                credentials: StorageCredentials::S3(credentials.clone()),
            }
            .validate(),
            _ => Err(StorageError::InvalidConfiguration(
                "S3 restore requires a member key",
            )),
        }
    }

    /// Encode only the information another device needs.
    pub fn encode(&self) -> Result<SecretBytes, StorageError> {
        self.validate()?;
        crate::secret_json::encode(self)
    }

    /// Decode and validate a restore code's provider payload.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|error| StorageError::Encoding(Box::new(error)))?;
        value.validate()?;
        Ok(value)
    }
}

#[cfg(test)]
#[path = "restore_storage_tests.rs"]
mod tests;
