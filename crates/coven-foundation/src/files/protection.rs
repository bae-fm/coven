//! The Apple file protection class a layout gives its stores' files (E1).
//!
//! A folder's class is the class of every file and folder made in it
//! afterwards, so a store's directory gets its class before anything is made
//! there. Files that already exist keep the class they were made with, so
//! opening a store with a newly chosen class walks its directory, folders
//! before their contents, and gives every file the class. A record beside the
//! settings names the class the last complete walk gave, and the next open
//! with that class skips the walk: a check of every file on every open would
//! cost tens of microseconds a file.

use std::ffi::c_int;
use std::fs;
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::files::{atomic_file, lock, FileError};

/// `F_GETPROTECTIONCLASS` and `F_SETPROTECTIONCLASS` from the SDK's
/// `sys/fcntl.h`, which the `libc` crate doesn't name.
const F_GETPROTECTIONCLASS: c_int = 63;
const F_SETPROTECTIONCLASS: c_int = 64;

/// The record of the class the last complete walk gave, in the store's
/// directory.
const RECORD: &str = "file-protection.json";

/// Apple's file protection classes: when a file can be read and written,
/// against whether the device is locked (api.md E1). A file's class
/// decides what an open handle can still do once the device locks, such
/// as syncing in the background.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileProtection {
    /// Readable and writable only while the device is unlocked. Shortly
    /// after it locks, even a file already open can't be read or written.
    Complete,
    /// A file open when the device locks stays readable and writable, and
    /// new files can be made while it is locked, but a closed file can't
    /// be opened until the device unlocks.
    CompleteUnlessOpen,
    /// Unreadable from the device's start until the person first unlocks
    /// it, then readable and writable, locked or not, until it restarts.
    /// Apple's default for an app's files.
    CompleteUntilFirstUserAuthentication,
    /// Readable and writable whenever the device is on, even before the
    /// first unlock after a restart.
    None,
}

impl FileProtection {
    /// The class's number for `fcntl`: XNU's `PROTECTION_CLASS_A` to `_D`.
    /// The SDK names none of them; Apple's own SQLite numbers its
    /// `SQLITE_OPEN_FILEPROTECTION_*` flags in the same order.
    fn number(self) -> c_int {
        match self {
            Self::Complete => 1,
            Self::CompleteUnlessOpen => 2,
            Self::CompleteUntilFirstUserAuthentication => 3,
            Self::None => 4,
        }
    }

    /// The class a file or folder has; `Ok(None)` for a folder with no
    /// class of its own, whose new files take the volume's default.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn of(path: &Path) -> io::Result<Option<Self>> {
        match class_number(&fs::File::open(path)?)? {
            0 => Ok(None),
            1 => Ok(Some(Self::Complete)),
            2 => Ok(Some(Self::CompleteUnlessOpen)),
            3 => Ok(Some(Self::CompleteUntilFirstUserAuthentication)),
            4 => Ok(Some(Self::None)),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown protection class {other}"),
            )),
        }
    }
}

/// The class a layout gives its stores' files: the one the app chose, if
/// any.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Protection {
    class: Option<FileProtection>,
}

impl Protection {
    /// A layout whose stores' files take `class`.
    pub(crate) fn new(class: FileProtection) -> Self {
        Self { class: Some(class) }
    }

    /// Give a new, empty store directory the class before anything is
    /// made in it, so every file it ever holds is made with it.
    pub(crate) fn begin(self, directory: &Path) -> Result<(), FileError> {
        let Some(class) = self.class else {
            return Ok(());
        };
        set(directory, class, Entry::Folder)?;
        record(directory, class)
    }

    /// Give every folder and file in an existing store directory the
    /// class, unless the record shows the last complete walk gave it. The
    /// caller holds the store's writer or bootstrap lock, so no other
    /// writer makes files meanwhile.
    pub(crate) fn apply(self, store: &Path) -> Result<(), FileError> {
        let Some(class) = self.class else {
            return Ok(());
        };
        if recorded(store)? == Some(class) {
            return Ok(());
        }
        // A reader in another process may be writing a cache range, whose
        // bytes could land under a name the walk has already passed
        // (Appendix C.3).
        let _cache_writes = lock::cache_writes(store, false)?;
        walk(store, class, false)?;
        // Each changed entry was handed to the drive as it changed; one flush
        // of the drive's cache puts them all on its permanent storage before
        // the record says so, as a commit's files are (Appendix C.3).
        atomic_file::sync_directory(store)
            .map_err(|source| FileError::at("flush drive cache", store, source))?;
        // Only a complete walk is recorded: the next open repeats one that
        // stopped partway, leaving alone the entries it reached.
        record(store, class)
    }
}

#[derive(Clone, Copy)]
enum Entry {
    Folder,
    File,
}

/// Give `folder` the class first, so whatever is made in it from then on
/// takes it, then give it to everything already inside.
fn walk(folder: &Path, class: FileProtection, nested: bool) -> Result<(), FileError> {
    set(folder, class, Entry::Folder)?;
    let entries =
        fs::read_dir(folder).map_err(|source| FileError::at("list store files", folder, source))?;
    let mut holds_record = false;
    for entry in entries {
        let entry = entry.map_err(|source| FileError::at("list store files", folder, source))?;
        let path = entry.path();
        let kind = entry
            .file_type()
            .map_err(|source| FileError::at("inspect store file", &path, source))?;
        if kind.is_dir() {
            walk(&path, class, true)?;
        } else if kind.is_file() {
            set(&path, class, Entry::File)?;
            holds_record |= entry.file_name() == RECORD;
        }
        // Coven makes no links or other kinds of entry in a store.
    }
    // An installation inside the store's directory, one loading to
    // replace it or one a reset archived, keeps its own record. A swap
    // moves the loaded one into the store's place, so it must name the
    // class its files now have.
    if nested && holds_record {
        record(folder, class)?;
    }
    Ok(())
}

/// Give one folder or file the class, unless it has it already, and hand
/// the change to the drive. Apple takes a file's new class only through a
/// descriptor open for writing, and a folder's through one open for reading.
/// An entry removed since the listing needs no class.
///
/// For a few milliseconds after it changes a file's class, Apple refuses to
/// set that file's class again, even to the same one, with `EBUSY`. Leaving
/// alone an entry that has the class keeps a walk repeated after one that
/// stopped partway clear of the entries that one reached; an open that still
/// meets the refusal fails with it.
fn set(path: &Path, class: FileProtection, entry: Entry) -> Result<(), FileError> {
    let opened = match entry {
        Entry::Folder => fs::File::open(path),
        Entry::File => fs::OpenOptions::new().write(true).open(path),
    };
    let file = match opened {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(FileError::at("open store file", path, source)),
    };
    let current = class_number(&file)
        .map_err(|source| FileError::at("read file protection class", path, source))?;
    if current == class.number() {
        return Ok(());
    }
    // SAFETY: `file` is open for the whole call, and this command takes
    // one `int` argument, the class.
    if unsafe { libc::fcntl(file.as_raw_fd(), F_SETPROTECTIONCLASS, class.number()) } == -1 {
        return Err(FileError::at("set file protection class", path, io::Error::last_os_error()));
    }
    atomic_file::sync_file(&file)
        .map_err(|source| FileError::at("sync file protection class", path, source))
}

/// The class number of an open file or folder; 0 for a folder with no class.
fn class_number(file: &fs::File) -> io::Result<c_int> {
    // SAFETY: `file` is open for the whole call, and this command takes no
    // argument beyond the descriptor.
    match unsafe { libc::fcntl(file.as_raw_fd(), F_GETPROTECTIONCLASS) } {
        -1 => Err(io::Error::last_os_error()),
        number => Ok(number),
    }
}

fn recorded(store: &Path) -> Result<Option<FileProtection>, FileError> {
    let path = store.join(RECORD);
    atomic_file::read_optional(&path)?
        .map(|bytes| {
            serde_json::from_slice(&bytes).map_err(|error| {
                FileError::at(
                    "read file protection record",
                    &path,
                    io::Error::new(io::ErrorKind::InvalidData, error),
                )
            })
        })
        .transpose()
}

/// Made in the store's directory after it has the class, so the record
/// has it too.
fn record(store: &Path, class: FileProtection) -> Result<(), FileError> {
    let bytes = serde_json::to_vec(&class).expect("a protection class encodes");
    atomic_file::replace(&store.join(RECORD), &bytes)
}

#[cfg(test)]
#[path = "protection_tests.rs"]
mod tests;
