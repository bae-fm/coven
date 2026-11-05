use crate::{StorageConfig, StorageError};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::files::{StoreDir, StoreFile};

/// Read the location recorded in the store directory's settings file, with
/// absence distinct from malformed settings. Credentials never enter this file.
pub fn recorded_config(directory: &StoreDir) -> Result<Option<StorageConfig>, StorageError> {
    let Some(bytes) = directory.owned_file(StoreFile::StorageSettings).read_optional()? else {
        return Ok(None);
    };
    Ok(Some(StorageConfig::decode(&bytes)?))
}

/// Atomically record `config` as the store's location after the facade has
/// established the connection.
pub fn record_config(directory: &StoreDir, config: &StorageConfig) -> Result<(), StorageError> {
    let bytes = config.encode()?;
    directory.owned_file(StoreFile::StorageSettings).replace(bytes.as_bytes())?;
    Ok(())
}

/// Remove the recorded location after the facade successfully removes
/// credentials.
pub fn remove_recorded_config(directory: &StoreDir) -> Result<(), StorageError> {
    Ok(directory.owned_file(StoreFile::StorageSettings).remove()?)
}

impl StorageConfig {
    /// Encode the validated location as the settings file holds it, without
    /// any storage credentials.
    pub(crate) fn encode(&self) -> Result<coven_crypto::SecretBytes, StorageError> {
        self.validate()?;
        crate::secret_json::encode(self)
    }

    /// Decode the settings file's location; unknown fields and credential
    /// payloads fail.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let location: Self = crate::secret_json::decode(bytes)?;
        location.validate()?;
        Ok(location)
    }
}

#[cfg(test)]
#[path = "settings_tests.rs"]
mod tests;
