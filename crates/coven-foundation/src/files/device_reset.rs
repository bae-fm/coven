//! Swapping a loaded replacement into a store's directory (§19.3).
//!
//! `reset_device` and `restore_device` load the new installation in its own
//! directory inside the store's, then move it into place. The durable record
//! names the old and new device ids and the call that began the replacement:
//! written before the fresh device registers, it makes ordinary opens refuse
//! the store, naming that call, and every later call finishes the same
//! replacement (§19.3). It says, too, whether the swap has begun.
//! The bootstrap marker separates the swap's two moves. Without it, the old
//! local state is still moving into its archive; with it, the new installation
//! is moving in. The settings change last. Each step is idempotent, so a retry
//! after any interruption continues forward where it stopped.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::{
    atomic_file, bootstrap, settings, DirectoryCreationError, FileError, SettingsError, StoreFile,
};
use crate::id_source::{DeviceId, StoreId};

pub(crate) const RECORD: &str = "device-reset.json";
const ARCHIVE_PREFIX: &str = "discarded-device-";
/// The directory inside a store's directory where its replacement loads.
pub(super) const AREA: &str = ".coven-replacement";
/// Files describing the store's location and this installation's custody,
/// which the replacement keeps in place. Everything else is local state.
const KEPT: [StoreFile; 3] =
    [StoreFile::StorageSettings, StoreFile::StoreKeys, StoreFile::MemberKeys];

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

/// How far a recorded replacement has come (§19.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplacementPhase {
    /// Recorded before the fresh device registers. The new installation is
    /// in its hidden directory and the old one in the store's place.
    Registering,
    /// The swap has begun: the installations are moving, or have moved.
    Swapping,
}

/// The durable record of a replacement past its point of no return: from
/// just before the fresh device registers until it is published (§19.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplacementRecord {
    /// The installation being replaced and its fresh successor.
    pub reset: DeviceReset,
    /// The call that began the replacement and finishes it.
    pub by: ReplacedBy,
    /// The store's name in the new installation's loaded database. While the
    /// replacement is unfinished the store doesn't open, so listings show this.
    pub name: String,
    /// How far it has come.
    pub phase: ReplacementPhase,
}

pub(super) fn read(directory: &Path) -> Result<Option<ReplacementRecord>, FileError> {
    atomic_file::read_json(&directory.join(RECORD), "read replacement record")
}

/// The record of a replacement that has not finished: one still
/// registering, one whose marker is still present, or one whose settings do
/// not yet name the new device. A record that outlived publication is not
/// returned.
pub(super) fn unfinished(
    directory: &Path,
    id: StoreId,
) -> Result<Option<ReplacementRecord>, SettingsError> {
    let Some(record) = read(directory)? else {
        return Ok(None);
    };
    if record.phase == ReplacementPhase::Swapping
        && !bootstrap::is_pending(directory)?
        && settings::read(directory, id)?.device_id == record.reset.new_device
    {
        return Ok(None);
    }
    Ok(Some(record))
}

/// Keep the first decision: a resumed replacement never changes the chosen
/// fresh id. A record already there is returned as it is.
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

/// Mark the recorded replacement's swap as begun, before anything moves.
pub(super) fn swapping(directory: &Path, recorded: &ReplacementRecord) -> Result<(), FileError> {
    if recorded.phase == ReplacementPhase::Swapping {
        return Ok(());
    }
    let record = ReplacementRecord { phase: ReplacementPhase::Swapping, ..recorded.clone() };
    let bytes = serde_json::to_vec(&record).expect("replacement record encodes");
    atomic_file::replace(&directory.join(RECORD), &bytes)
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
/// name the new device in the settings. Resumes any interrupted swap.
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
    KEPT.iter().any(|file| file.name() == name)
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

/// Remove the record once the replacement is published, its database
/// keeping the notice, or once the facts end a replacement before its swap.
pub(super) fn finish(directory: &Path) -> Result<(), FileError> {
    atomic_file::remove(&directory.join(RECORD))
}
