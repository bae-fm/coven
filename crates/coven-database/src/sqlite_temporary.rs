//! The folder SQLite keeps temporary tables in, for every store this
//! process opens (§21.2).
//!
//! Temporary tables spill to files there once a connection's cache fills,
//! so a staged snapshot waits on disk. SQLite finds a folder for itself
//! everywhere but Android, where an app names one, such as its cache folder.

use coven_foundation::files::TemporaryFolder;
use coven_foundation::locks::lock_unpoisoned;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// What this process has told SQLite, and whether a connection may be using it.
struct Temporary {
    directory: Option<TemporaryFolder>,
    opened: bool,
}

static TEMPORARY: Mutex<Temporary> = Mutex::new(Temporary { directory: None, opened: false });

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
    let mut temporary = lock_unpoisoned(&TEMPORARY);
    if let Some(named) = &temporary.directory {
        if named.path() == directory {
            return Ok(());
        }
        return Err(TemporaryDirectoryError::AlreadyNamed { directory: named.path().to_owned() });
    }
    if temporary.opened {
        return Err(TemporaryDirectoryError::AfterOpening);
    }
    let unusable = || TemporaryDirectoryError::NotAFolder { directory: directory.to_owned() };
    let folder = TemporaryFolder::at(directory).ok_or_else(unusable)?;
    let text = folder.path().to_str().ok_or_else(unusable)?;
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
    temporary.directory = Some(folder);
    Ok(())
}

/// Every connection opens after this: the folder can no longer change. It
/// refuses a SQLite built to keep temporary tables in memory unless a
/// connection asks for files (`SQLITE_TEMP_STORE=2`) or whatever it asks
/// (`=3`, libsqlite3-sys's Android default): coven's own temporary tables,
/// such as a streamed write's staged rows, would then hold all of it in
/// memory. It refuses one built without `SQLITE_ENABLE_MEMORY_MANAGEMENT`
/// too: each connection would keep its unused pages apart, and releasing
/// memory on one connection would leave every other's (§4).
pub(crate) fn opening() -> Result<(), crate::DbError> {
    // SAFETY: the option is a NUL-terminated string that outlives the call.
    if unsafe { rusqlite::ffi::sqlite3_compileoption_used(c"ENABLE_MEMORY_MANAGEMENT".as_ptr()) }
        == 0
    {
        return Err(crate::DbError::Invariant {
            what: "SQLite keeps every connection's unused pages in one pool (SQLITE_ENABLE_MEMORY_MANAGEMENT)",
        });
    }
    for option in [c"TEMP_STORE=2", c"TEMP_STORE=3"] {
        // SAFETY: the option is a NUL-terminated string that outlives the call.
        if unsafe { rusqlite::ffi::sqlite3_compileoption_used(option.as_ptr()) } != 0 {
            return Err(crate::DbError::Invariant {
                what: "SQLite keeps temporary tables in files unless a connection asks for memory (SQLITE_TEMP_STORE below 2)",
            });
        }
    }
    lock_unpoisoned(&TEMPORARY).opened = true;
    Ok(())
}
