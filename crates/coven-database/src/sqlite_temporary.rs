//! The folder SQLite keeps temporary tables in, for every store this
//! process opens (§21.2).
//!
//! Temporary tables spill to files there once a connection's cache fills,
//! so a staged snapshot waits on disk. SQLite finds a folder for itself
//! everywhere but Android, where an app names one, such as its cache folder.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// What this process has told SQLite, and whether a connection may be using it.
struct Temporary {
    directory: Option<PathBuf>,
    opened: bool,
}

static TEMPORARY: Mutex<Temporary> = Mutex::new(Temporary {
    directory: None,
    opened: false,
});

/// Why the folder for temporary tables could not be named (E1).
#[derive(Debug, thiserror::Error)]
pub enum TemporaryDirectoryError {
    /// A store's database opened in this process before the folder was named;
    /// SQLite reads it without a lock, so it is named first.
    #[error("the temporary folder is named before any store opens")]
    AfterOpening,
    /// Another folder was named already. Naming the same one again is allowed.
    #[error("the temporary folder is already {}", directory.display())]
    AlreadyNamed {
        /// The folder named first.
        directory: PathBuf,
    },
    /// The path is not an existing folder, or SQLite cannot hold it: it is not
    /// UTF-8, or contains a NUL.
    #[error("{} is not a folder SQLite can keep temporary tables in", directory.display())]
    NotAFolder {
        /// The path given.
        directory: PathBuf,
    },
}

/// Keep SQLite's temporary tables in `directory`, for every store this
/// process opens from now on. Call it before opening any store.
pub fn set_temporary_directory(directory: &Path) -> Result<(), TemporaryDirectoryError> {
    let mut temporary = TEMPORARY.lock().expect("temporary folder lock poisoned");
    if let Some(named) = &temporary.directory {
        if named == directory {
            return Ok(());
        }
        return Err(TemporaryDirectoryError::AlreadyNamed {
            directory: named.clone(),
        });
    }
    if temporary.opened {
        return Err(TemporaryDirectoryError::AfterOpening);
    }
    let unusable = || TemporaryDirectoryError::NotAFolder {
        directory: directory.to_owned(),
    };
    if !coven_foundation::files::is_folder(directory) {
        return Err(unusable());
    }
    let text = directory.to_str().ok_or_else(unusable)?;
    let text = std::ffi::CString::new(text).map_err(|_nul| unusable())?;
    // SAFETY: no connection exists yet, so nothing reads the variable while it
    // changes, and later connections only read it. SQLite requires its value
    // to come from sqlite3_mprintf.
    unsafe {
        let copy = rusqlite::ffi::sqlite3_mprintf(c"%s".as_ptr(), text.as_ptr());
        if copy.is_null() {
            return Err(unusable());
        }
        rusqlite::ffi::sqlite3_temp_directory = copy;
    }
    temporary.directory = Some(directory.to_owned());
    Ok(())
}

/// Every connection opens after this: the folder can no longer change.
pub(crate) fn opening() {
    TEMPORARY
        .lock()
        .expect("temporary folder lock poisoned")
        .opened = true;
}
