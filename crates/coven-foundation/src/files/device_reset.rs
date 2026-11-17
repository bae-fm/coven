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
    atomic_file, bootstrap, set_aside, settings, DirectoryCreationError, FileError, SettingsError,
    StoreFile,
};
use crate::id_source::{DeviceId, StoreId};

pub(crate) const RECORD: &str = "device-reset.json";
const ARCHIVE_PREFIX: &str = "discarded-device-";
/// The folder each archive moves into as its removal begins, so the archive
/// leaves the store's directory in one rename while its tree goes (§19.3).
const REMOVING: &str = "removing-discarded-devices";
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

/// What the fresh device's registration does to the old id (§19.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Registration {
    /// The fresh device replaces the old id: nothing else uses it.
    ReplacesOld,
    /// The fresh device joins its member beside the old id, which another
    /// installation still uses and keeps (a copy that found its id in use).
    NewDevice,
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
    /// What the registration does to the old id, kept so a resumed
    /// replacement registers the same entry.
    pub registration: Registration,
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
    directory.join(archive_name(old_device))
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
        // The archive stays for inspection, so what unfinished saves left,
        // sealed keys among it, doesn't go in. The writer lock is held.
        atomic_file::remove_temporaries(directory)?;
        move_local_state(directory, &archive)?;
        atomic_file::replace(&marker, b"")?;
    }
    let loaded = loading(directory, id);
    if atomic_file::exists("inspect loaded replacement", &loaded)? {
        move_local_state(&loaded, directory)?;
    }
    name_device(directory, id, reset.new_device)
}

/// The name of the archive a swap keeps of `old_device`'s installation.
pub(super) fn archive_name(old_device: DeviceId) -> String {
    format!("{ARCHIVE_PREFIX}{}", old_device.0)
}

/// The name of every archive in the store's directory, in order.
pub(super) fn discarded_devices(directory: &Path) -> Result<Vec<String>, FileError> {
    Ok(archives(directory)?
        .iter()
        .map(|archive| {
            archive
                .file_name()
                .and_then(|name| name.to_str())
                .expect("an archive is listed by its UTF-8 name")
                .to_owned()
        })
        .collect())
}

/// The size of everything in the archive `name`.
pub(super) fn discarded_device_size(directory: &Path, name: &str) -> Result<u64, FileError> {
    tree_size(&named_archive(directory, name)?)
}

/// The path of the archive `name`, refusing a name that is not one.
fn named_archive(directory: &Path, name: &str) -> Result<PathBuf, FileError> {
    if !name.starts_with(ARCHIVE_PREFIX) || name.contains(['/', '\\']) || name.contains("..") {
        return Err(FileError::at(
            "name a discarded device archive",
            &directory.join(name),
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "not an archive's name"),
        ));
    }
    Ok(directory.join(name))
}

/// Move the archive `name` into the removal folder, in one rename, so it
/// leaves the store's directory in one step. Returns whether it was there
/// to move.
pub(super) fn retire(directory: &Path, name: &str) -> Result<bool, FileError> {
    set_aside::move_aside(&named_archive(directory, name)?, &directory.join(REMOVING).join(name))
}

/// Move the archive `name` back from the removal folder, as `retire` found it.
pub(super) fn unretire(directory: &Path, name: &str) -> Result<(), FileError> {
    set_aside::move_back(&named_archive(directory, name)?, &directory.join(REMOVING).join(name))
}

/// Delete the removal folder and everything in it, then sync the store's
/// directory. Nothing names what is in it, so a deletion that stops partway
/// leaves the rest for the next call. When the folder is absent, the call
/// does nothing and succeeds.
pub(super) fn remove_retired(directory: &Path) -> Result<(), FileError> {
    set_aside::delete(&directory.join(REMOVING))
}

/// The size of a file, or of everything under a folder. Links are not
/// followed, and what disappears while the walk runs counts for nothing.
pub(super) fn tree_size(path: &Path) -> Result<u64, FileError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(source) => return Err(FileError::at("measure a store file", path, source)),
    };
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() {
        return Ok(0);
    }
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(source) => return Err(FileError::at("list a store folder", path, source)),
    };
    let mut total = 0u64;
    for entry in entries {
        let entry = entry.map_err(|source| FileError::at("list a store folder", path, source))?;
        total = total.saturating_add(tree_size(&entry.path())?);
    }
    Ok(total)
}

/// The archives of earlier resets in the store's directory, by name. Nothing
/// reads one once its reset's notice is in the database (§19.3).
pub(super) fn archives(directory: &Path) -> Result<Vec<PathBuf>, FileError> {
    let entries = fs::read_dir(directory)
        .map_err(|source| FileError::at("list discarded device archives", directory, source))?;
    let mut found = Vec::new();
    for entry in entries {
        let entry = entry
            .map_err(|source| FileError::at("list discarded device archives", directory, source))?;
        if entry.file_name().to_str().is_some_and(|name| name.starts_with(ARCHIVE_PREFIX)) {
            found.push(entry.path());
        }
    }
    found.sort();
    Ok(found)
}

/// Whether an entry stays where it is while installations swap: the custody
/// files the replacement keeps, the settings, which change last, the swap's
/// own record and marker, the replacement's area, earlier archives and the
/// folder of those being removed.
fn stays(name: &str) -> bool {
    KEPT.iter().any(|file| file.name() == name)
        || name == settings::SETTINGS_FILE
        || name == RECORD
        || name == bootstrap::MARKER
        || name == AREA
        || name == REMOVING
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
