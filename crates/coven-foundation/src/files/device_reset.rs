//! Swapping a loaded replacement into a store's directory (§19.3).
//!
//! `reset_device` and `restore_device` load the new installation in its own
//! directory inside the store's, then move it into place. The durable record
//! names the old and new device ids and the call that began the swap: written
//! before anything moves, it makes ordinary opens refuse the store, naming
//! that call, and every retry of that call finishes the same swap.
//! The bootstrap marker separates the two moves. Without it, the old local
//! state is still moving into its archive; with it, the new installation is
//! moving in. The settings change last. Each step is idempotent, so a retry
//! after any interruption continues where it stopped, in either direction.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::{atomic_file, bootstrap, settings, DirectoryCreationError, FileError, SettingsError};
use crate::id_source::{DeviceId, StoreId};

pub(crate) const RECORD: &str = "device-reset.json";
const ARCHIVE_PREFIX: &str = "discarded-device-";
/// The directory inside a store's directory where its replacement loads.
pub(super) const AREA: &str = ".coven-replacement";
/// Files describing the store's location and this installation's custody,
/// which the replacement keeps in place. Everything else is local state.
const KEPT: [&str; 3] = ["storage.json", "store-keys.sealed", "member-keys.sealed"];

/// A replacement of this installation, not a membership removal (E5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceReset {
    /// The id whose local copy was discarded.
    pub old_device: DeviceId,
    /// The fresh id the replacement registers.
    pub new_device: DeviceId,
}

/// The call that began a replacement, and so the only call that finishes it
/// (§19.3). The other call would load under different keys, or keep or remove
/// a different copy, so it is refused before anything changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplacedBy {
    /// `CovenBuilder::reset_device`, which keeps the old copy for inspection.
    Reset,
    /// `CovenBuilder::restore_device`, which commits the restore code's member
    /// keys and removes the old copy (§13.1).
    Restore,
}

impl std::fmt::Display for ReplacedBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Reset => "reset_device",
            Self::Restore => "restore_device",
        })
    }
}

/// The durable record of a replacement whose swap has begun (§19.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplacementRecord {
    /// The installation being replaced and its fresh successor.
    pub reset: DeviceReset,
    /// The call that began the swap and finishes it.
    pub by: ReplacedBy,
    /// The store's name in the new installation's loaded database. While the
    /// swap is unfinished neither database can be read, so listings show this.
    pub name: String,
}

pub(super) fn read(directory: &Path) -> Result<Option<ReplacementRecord>, FileError> {
    let path = directory.join(RECORD);
    atomic_file::read_optional(&path)?
        .map(|bytes| {
            serde_json::from_slice(&bytes).map_err(|error| {
                FileError::at(
                    "read replacement record",
                    &path,
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error),
                )
            })
        })
        .transpose()
}

/// The record of a replacement that has not finished: one whose marker is
/// still present, or whose settings do not yet name the new device. A record
/// that outlived publication is not returned.
pub(super) fn unfinished(
    directory: &Path,
    id: StoreId,
) -> Result<Option<ReplacementRecord>, SettingsError> {
    let Some(record) = read(directory)? else {
        return Ok(None);
    };
    if !bootstrap::is_pending(directory)?
        && settings::read(directory, id)?.device_id == record.reset.new_device
    {
        return Ok(None);
    }
    Ok(Some(record))
}

/// Keep the first decision: a retried swap never changes the chosen fresh id.
pub(super) fn record(
    directory: &Path,
    record: &ReplacementRecord,
) -> Result<ReplacementRecord, FileError> {
    if let Some(recorded) = read(directory)? {
        return Ok(recorded);
    }
    if record.reset.old_device == record.reset.new_device {
        return Err(FileError::at(
            "record device reset",
            &directory.join(RECORD),
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "a reset needs a fresh device id",
            ),
        ));
    }
    let bytes = serde_json::to_vec(record).expect("replacement record encodes");
    atomic_file::replace(&directory.join(RECORD), &bytes)?;
    Ok(record.clone())
}

/// The directory where the replacement of the store at `directory` loads.
pub(super) fn loading(directory: &Path, id: StoreId) -> PathBuf {
    directory.join(AREA).join(id.to_string())
}

/// The discarded installation's local state, kept for inspection after a reset.
fn archive(directory: &Path, old_device: DeviceId) -> PathBuf {
    directory.join(format!("{ARCHIVE_PREFIX}{}", old_device.0))
}

/// Under the writer, bootstrap and excluded reader locks, move the old local
/// state into its archive and the loaded installation into its place, then
/// name the new device in the settings. Resumes any interrupted swap or undo.
pub(super) fn forward(
    directory: &Path,
    id: StoreId,
    reset: DeviceReset,
) -> Result<(), DirectoryCreationError> {
    let marker = bootstrap::marker(directory);
    if !atomic_file::exists("inspect bootstrap marker", &marker)? {
        let archive = archive(directory, reset.old_device);
        if !atomic_file::exists("inspect discarded device archive", &archive)? {
            fs::create_dir(&archive).map_err(|source| {
                FileError::at("create discarded device archive", &archive, source)
            })?;
        }
        move_local_state(directory, &archive)?;
        atomic_file::replace(&marker, b"")?;
    }
    let loaded = loading(directory, id);
    if atomic_file::exists("inspect loaded replacement", &loaded)? {
        move_local_state(&loaded, directory)?;
    }
    name_device(directory, id, reset.new_device)
}

/// Undo `forward`, step by step in reverse, after a later step failed: the
/// old installation is back in place and its settings name it again.
pub(super) fn backward(
    directory: &Path,
    id: StoreId,
    reset: DeviceReset,
) -> Result<(), DirectoryCreationError> {
    name_device(directory, id, reset.old_device)?;
    let marker = bootstrap::marker(directory);
    if atomic_file::exists("inspect bootstrap marker", &marker)? {
        move_local_state(directory, &loading(directory, id))?;
        atomic_file::remove(&marker)?;
    }
    let archive = archive(directory, reset.old_device);
    if atomic_file::exists("inspect discarded device archive", &archive)? {
        move_local_state(&archive, directory)?;
        fs::remove_dir(&archive)
            .map_err(|source| FileError::at("remove discarded device archive", &archive, source))?;
        atomic_file::sync_publication(&archive)?;
    }
    Ok(atomic_file::remove(&directory.join(RECORD))?)
}

/// Remove a swap's archive: a restore keeps no old copy (§13.1).
pub(super) fn discard(directory: &Path, old_device: DeviceId) -> Result<(), FileError> {
    let archive = archive(directory, old_device);
    if atomic_file::exists("inspect discarded device archive", &archive)? {
        fs::remove_dir_all(&archive)
            .map_err(|source| FileError::at("remove discarded device archive", &archive, source))?;
        atomic_file::sync_publication(&archive)?;
    }
    Ok(())
}

/// Whether an entry stays where it is while installations swap: the custody
/// files the replacement keeps, the settings, which change last, the swap's
/// own record and marker, the replacement's area and earlier archives.
fn stays(name: &str) -> bool {
    KEPT.contains(&name)
        || name == settings::SETTINGS_FILE
        || name == RECORD
        || name == bootstrap::MARKER
        || name == AREA
        || name.starts_with(ARCHIVE_PREFIX)
}

/// Move every local entry of `from` into `to`, then sync both directories.
fn move_local_state(from: &Path, to: &Path) -> Result<(), FileError> {
    let entries = fs::read_dir(from)
        .map_err(|source| FileError::at("list installation files", from, source))?;
    for entry in entries {
        let entry =
            entry.map_err(|source| FileError::at("list installation files", from, source))?;
        let name = entry.file_name();
        if name.to_str().is_some_and(stays) {
            continue;
        }
        let source = entry.path();
        atomic_file::rename_new(&source, &to.join(&name))
            .map_err(|error| FileError::at("move installation file", &source, error))?;
    }
    #[cfg(unix)]
    for path in [from, to] {
        atomic_file::sync_directory(path)
            .map_err(|source| FileError::at("sync installation files", path, source))?;
    }
    Ok(())
}

fn name_device(
    directory: &Path,
    id: StoreId,
    device: DeviceId,
) -> Result<(), DirectoryCreationError> {
    let mut current = settings::read(directory, id)?;
    if current.device_id != device {
        current.device_id = device;
        settings::write(directory, &current)?;
    }
    Ok(())
}

/// The database a swap moved aside, in its discarded-device archive.
pub(super) fn discarded_database(directory: &Path, old_device: DeviceId) -> PathBuf {
    archive(directory, old_device).join(super::directory::DATABASE)
}

/// Remove the record once the replacement is published; its database keeps the notice.
pub(super) fn finish(directory: &Path) -> Result<(), FileError> {
    atomic_file::remove(&directory.join(RECORD))
}
