//! A stale or damaged installation becomes a new device in its own directory (§10, §19.2).
//!
//! The durable record is the decision: once written, ordinary opens refuse the
//! store and every retry uses the same fresh id. Turning the directory into an
//! unpublished bootstrap moves the discarded local state aside as a whole.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::{atomic_file, settings, FileError, StoreCreationError};
use crate::id_source::{DeviceId, StoreId};

pub(crate) const RECORD: &str = "device-reset.json";
const ARCHIVE_PREFIX: &str = "discarded-device-";
/// Files describing the store, its location and this installation's custody,
/// which the replacement installation keeps. Everything else is local state.
const KEPT: [&str; 4] = [
    settings::SETTINGS_FILE,
    "storage.json",
    "store-keys.sealed",
    "member-keys.sealed",
];

/// Why this installation is being replaced by a new device (E5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeviceResetReason {
    /// The database's id does not match device-only custody.
    IdentityMismatch,
    /// Storage proves counters beyond those reserved in the local database.
    StorageAhead,
    /// An occupied immutable path holds different bytes.
    SlotMismatch,
    /// The store log has a kept replacement of this device's id.
    Replaced,
    /// The database could not be trusted; no local work was salvaged (§19.2).
    DatabaseDamage,
}

/// A replacement of this installation, not a membership removal (E5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceReset {
    /// The id whose local copy was discarded.
    pub old_device: DeviceId,
    /// The fresh id the replacement registers.
    pub new_device: DeviceId,
    /// Why the old copy cannot continue.
    pub reason: DeviceResetReason,
}

pub(super) fn read(directory: &Path) -> Result<Option<DeviceReset>, FileError> {
    let path = directory.join(RECORD);
    atomic_file::read_optional(&path)?
        .map(|bytes| {
            serde_json::from_slice(&bytes).map_err(|error| {
                FileError::at(
                    "read device reset",
                    &path,
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error),
                )
            })
        })
        .transpose()
}

/// Keep the first decision. A later detection in the same copy, or another
/// reason found before reopening, never changes the chosen fresh id.
pub(super) fn record(directory: &Path, reset: &DeviceReset) -> Result<DeviceReset, FileError> {
    if let Some(recorded) = read(directory)? {
        return Ok(recorded);
    }
    if reset.old_device == reset.new_device {
        return Err(FileError::at(
            "record device reset",
            &directory.join(RECORD),
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "a reset needs a fresh device id",
            ),
        ));
    }
    let bytes = serde_json::to_vec(reset).expect("device reset encodes");
    atomic_file::replace(&directory.join(RECORD), &bytes)?;
    Ok(*reset)
}

/// Under the writer and bootstrap locks, finish turning a recorded reset into
/// an unpublished bootstrap. Each step is idempotent, so an interrupted call
/// resumes: moving local state aside completes before the bootstrap marker is
/// written, and the settings change last. A directory naming the new id has
/// therefore finished the move, and without its marker it is published.
pub(super) fn begin(
    directory: &Path,
    id: StoreId,
) -> Result<DeviceReset, super::BootstrapDirectoryError> {
    let reset = read(directory)?.ok_or_else(|| {
        FileError::at(
            "begin device reset",
            &directory.join(RECORD),
            std::io::Error::new(std::io::ErrorKind::NotFound, "no device reset is recorded"),
        )
    })?;
    let mut current = settings::read(directory, id).map_err(StoreCreationError::Settings)?;
    if current.device_id == reset.new_device {
        return Ok(reset);
    }
    let marker = super::bootstrap::marker(directory);
    if !atomic_file::exists("inspect bootstrap marker", &marker)? {
        let archive = directory.join(format!("{ARCHIVE_PREFIX}{}", reset.old_device.0));
        if !atomic_file::exists("inspect discarded device archive", &archive)? {
            fs::create_dir(&archive).map_err(|source| {
                FileError::at("create discarded device archive", &archive, source)
            })?;
        }
        let entries = fs::read_dir(directory)
            .map_err(|source| FileError::at("list discarded installation", directory, source))?;
        for entry in entries {
            let entry = entry.map_err(|source| {
                FileError::at("list discarded installation", directory, source)
            })?;
            let name = entry.file_name();
            if name.to_str().is_some_and(|name| {
                KEPT.contains(&name) || name == RECORD || name.starts_with(ARCHIVE_PREFIX)
            }) {
                continue;
            }
            let from = entry.path();
            atomic_file::rename_new(&from, &archive.join(&name))
                .map_err(|source| FileError::at("archive discarded installation", &from, source))?;
        }
        #[cfg(unix)]
        for path in [archive.as_path(), directory] {
            atomic_file::sync_directory(path)
                .map_err(|source| FileError::at("sync discarded installation", path, source))?;
        }
        atomic_file::replace(&marker, b"")?;
    }
    for area in [super::directory::APP_FILES, super::directory::CACHE] {
        let path = directory.join(area);
        if !atomic_file::exists("inspect store files directory", &path)? {
            fs::create_dir(&path)
                .map_err(|source| FileError::at("create store files directory", &path, source))?;
            atomic_file::sync_publication(&path)?;
        }
    }
    current.device_id = reset.new_device;
    settings::write(directory, &current).map_err(StoreCreationError::Settings)?;
    Ok(reset)
}

/// Remove the record once the replacement is published; its database keeps the notice.
pub(super) fn finish(directory: &Path) -> Result<(), FileError> {
    atomic_file::remove(&directory.join(RECORD))
}

#[cfg(test)]
#[path = "device_reset_tests.rs"]
mod tests;
