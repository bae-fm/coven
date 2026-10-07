//! The store's id and name, and this device's id, in one atomic settings file.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::files::atomic_file::{self, FileError};
use crate::id_source::{DeviceId, StoreId};

pub(crate) const SETTINGS_FILE: &str = "settings.json";

/// The settings written when creating, restoring or joining a store (E1).
/// Storage settings belong to coven-storage's own named file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreSettings {
    /// The store's identity, shared by all of its devices.
    pub id: StoreId,
    /// The store's name.
    pub name: String,
    /// This install's identity; a restored copy receives a fresh one (§10).
    pub device_id: DeviceId,
}

/// Missing, damaged or unreadable store settings remain distinct errors.
#[derive(Debug, thiserror::Error)]
pub enum SettingsError {
    /// The store has no settings file.
    #[error("settings are missing for store {0}")]
    Missing(StoreId),
    /// The settings do not contain the declared data.
    #[error("invalid store settings: {0}")]
    Corrupt(#[source] serde_json::Error),
    /// A settings file belongs to a different directory's store.
    #[error("settings identify store {actual}, directory identifies {expected}")]
    WrongStore {
        /// The id naming the directory.
        expected: StoreId,
        /// The id in the settings file.
        actual: StoreId,
    },
    /// The settings path is not an ordinary file, or is a symbolic link.
    #[error("settings are not a regular file for store {0}")]
    NotRegularFile(StoreId),
    /// Reading or writing settings failed at the filesystem boundary.
    #[error("store settings: {0}")]
    File(#[from] FileError),
}

pub(crate) fn read(directory: &Path, id: StoreId) -> Result<StoreSettings, SettingsError> {
    let path = directory.join(SETTINGS_FILE);
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if !metadata.is_file() => return Err(SettingsError::NotRegularFile(id)),
        Ok(_) => {}
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Err(SettingsError::Missing(id))
        }
        Err(source) => return Err(FileError::at("inspect settings file", &path, source).into()),
    }
    let bytes = atomic_file::read_optional(&path)?.ok_or(SettingsError::Missing(id))?;
    let settings: StoreSettings = serde_json::from_slice(&bytes).map_err(SettingsError::Corrupt)?;
    if settings.id != id {
        return Err(SettingsError::WrongStore {
            expected: id,
            actual: settings.id,
        });
    }
    Ok(settings)
}

pub(crate) fn write(directory: &Path, settings: &StoreSettings) -> Result<(), SettingsError> {
    let bytes = serde_json::to_vec(settings).map_err(SettingsError::Corrupt)?;
    atomic_file::replace(&directory.join(SETTINGS_FILE), &bytes)?;
    Ok(())
}

#[cfg(test)]
#[path = "settings_tests.rs"]
mod tests;
