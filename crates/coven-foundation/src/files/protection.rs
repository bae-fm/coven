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
//!
//! Only a volume mounted with content protection keeps classes (`mount`
//! lists it as `protect`). On one without, such as a disk image, Apple
//! reports every file at `CompleteUntilFirstUserAuthentication` and refuses
//! any other class with `EINVAL`, so a store whose app chose a class is
//! refused there before any file changes.
//!
//! While the device is locked, Apple refuses with `EPERM` to give a file
//! `Complete`, to make a file in a `Complete` folder, and to open a closed
//! `Complete` or `CompleteUnlessOpen` file. Coven reports each of these as
//! `FileError::DeviceLocked`. A file's class reads without opening it, so
//! what the lock refuses is told apart from any other `EPERM` by the class
//! of the file, or of the folder a new file goes in.

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
    /// class of its own, whose new files take the volume's default. It reads
    /// without opening the file, so a locked device answers too.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn of(path: &Path) -> io::Result<Option<Self>> {
        match class_at(path)? {
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

    /// Whether a file made in `folder` can be given a class now: tries to
    /// give a scratch file `Complete`, then removes it. This learns what the
    /// volume and the lock do, not what coven's check of the volume before
    /// giving a store's files a class concludes.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn settable_in(folder: &Path) -> io::Result<ProtectionSupport> {
        let scratch = tempfile::NamedTempFile::new_in(folder)?;
        // SAFETY: the scratch file is open for the whole call, and this
        // command takes one `int` argument, the class.
        let result = unsafe {
            libc::fcntl(
                scratch.as_file().as_raw_fd(),
                F_SETPROTECTIONCLASS,
                Self::Complete.number(),
            )
        };
        match result {
            -1 => match io::Error::last_os_error() {
                error if error.raw_os_error() == Some(libc::EINVAL) => {
                    Ok(ProtectionSupport::KeepsNone)
                }
                error if error.raw_os_error() == Some(libc::EPERM) => Ok(ProtectionSupport::Locked),
                error => Err(error),
            },
            _ => Ok(ProtectionSupport::Keeps),
        }
    }
}

/// What a folder's volume does with protection classes now, as
/// `FileProtection::settable_in` learns it.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtectionSupport {
    /// The volume keeps classes, and a file can be given any of them.
    Keeps,
    /// The volume keeps no classes: every file reads as
    /// `CompleteUntilFirstUserAuthentication`, and a store whose app chose a
    /// class is refused there.
    KeepsNone,
    /// The volume keeps classes, but the device is locked, so a file can't
    /// be given `Complete` until it unlocks.
    Locked,
}

/// The class a layout gives its stores' files: the one the app chose, if
/// any.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Protection {
    class: Option<FileProtection>,
}

impl Protection {
    /// A layout whose stores' files keep the class they have, and whose new
    /// files take their folder's.
    pub(crate) fn none() -> Self {
        Self { class: None }
    }

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
        volume_keeps_classes(directory)?;
        set(directory, class, Entry::Folder)?;
        record(directory, class)
    }

    /// Give every folder and file in an existing store directory the
    /// class, unless the record shows the last complete walk gave it. The
    /// caller holds the store's writer or bootstrap lock, so no other
    /// writer makes files meanwhile. A store moved since onto a volume
    /// that keeps no classes is refused even when the record names the
    /// class.
    pub(crate) fn apply(self, store: &Path) -> Result<(), FileError> {
        let Some(class) = self.class else {
            return Ok(());
        };
        volume_keeps_classes(store)?;
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

/// Refuse a class for the store at `directory` when its volume keeps none.
fn volume_keeps_classes(directory: &Path) -> Result<(), FileError> {
    let folder = fs::File::open(directory)
        .map_err(|source| FileError::at("open store directory", directory, source))?;
    let volume = rustix::fs::fstatfs(&folder)
        .map_err(|source| FileError::at("inspect store volume", directory, source.into()))?;
    if volume.f_flags & libc::MNT_CPROTECT as u32 == 0 {
        return Err(FileError::at(
            "give store files a protection class",
            directory,
            io::Error::new(
                io::ErrorKind::Unsupported,
                "the volume keeps no file protection classes",
            ),
        ));
    }
    Ok(())
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
        Err(source) => {
            return Err(locked_or(path, path, FileError::at("open store file", path, source)))
        }
    };
    let current = class_number(&file)
        .map_err(|source| FileError::at("read file protection class", path, source))?;
    if current == class.number() {
        return Ok(());
    }
    // SAFETY: `file` is open for the whole call, and this command takes
    // one `int` argument, the class.
    if unsafe { libc::fcntl(file.as_raw_fd(), F_SETPROTECTIONCLASS, class.number()) } == -1 {
        let source = io::Error::last_os_error();
        // Only the lock refuses a class with `EPERM`: giving a file
        // `Complete` while the device is locked.
        if source.raw_os_error() == Some(libc::EPERM) {
            return Err(FileError::DeviceLocked { path: path.to_owned() });
        }
        return Err(FileError::at("set file protection class", path, source));
    }
    atomic_file::sync_file(&file)
        .map_err(|source| FileError::at("sync file protection class", path, source))
}

/// `failure`, unless it is the lock's refusal of `path`: an `EPERM` where
/// `protected`, the file itself or the folder a new one goes in, has a class
/// the lock closes, `Complete` or `CompleteUnlessOpen`.
fn locked_or(path: &Path, protected: &Path, failure: FileError) -> FileError {
    let refused = match &failure {
        FileError::Io { source, .. } => source
            .downcast_ref::<io::Error>()
            .is_some_and(|error| error.raw_os_error() == Some(libc::EPERM)),
        _ => false,
    };
    let closes = || {
        class_at(protected).is_ok_and(|class| {
            class == FileProtection::Complete.number()
                || class == FileProtection::CompleteUnlessOpen.number()
        })
    };
    if refused && closes() {
        FileError::DeviceLocked { path: path.to_owned() }
    } else {
        failure
    }
}

/// The attributes `getattrlist` returns for `ATTR_CMN_DATA_PROTECT_FLAGS`:
/// their length, then the class.
#[repr(C)]
struct ClassAttribute {
    length: u32,
    class: u32,
}

/// The class number of the file or folder at `path`, read without opening
/// it, so the lock doesn't refuse it; 0 for a folder with no class.
fn class_at(path: &Path) -> io::Result<c_int> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let mut request = libc::attrlist {
        bitmapcount: libc::ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: libc::ATTR_CMN_DATA_PROTECT_FLAGS,
        volattr: 0,
        dirattr: 0,
        fileattr: 0,
        forkattr: 0,
    };
    let mut answer = ClassAttribute { length: 0, class: 0 };
    // SAFETY: `path` is a NUL-terminated string, `request` asks for one
    // 32-bit attribute, and `answer` holds its length word and the attribute,
    // with its size passed.
    let result = unsafe {
        libc::getattrlist(
            path.as_ptr(),
            std::ptr::from_mut(&mut request).cast(),
            std::ptr::from_mut(&mut answer).cast(),
            std::mem::size_of::<ClassAttribute>(),
            0,
        )
    };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(answer.class as c_int)
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
    atomic_file::read_json(&path, "read file protection record")
        .map_err(|failure| locked_or(&path, &path, failure))
}

/// Made in the store's directory after it has the class, so the record
/// has it too.
fn record(store: &Path, class: FileProtection) -> Result<(), FileError> {
    let bytes = serde_json::to_vec(&class).expect("a protection class encodes");
    let path = store.join(RECORD);
    // The new record is made in the store's directory, of the class just
    // given it.
    atomic_file::replace(&path, &bytes).map_err(|failure| locked_or(&path, store, failure))
}

#[cfg(test)]
#[path = "protection_tests.rs"]
mod tests;
