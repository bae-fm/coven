//! SQLite's own handle to the main database file: syncing it (§4), and
//! reading its size for the database's watch after each checkpoint. Both go
//! through SQLite's handle, because closing any other handle to the file would
//! drop every lock SQLite holds on it in this process.

use super::HostSql;
use crate::authorization::HostError;
use crate::authorization::Policy;
use crate::authorization::PolicyError;
use rusqlite::ffi::{sqlite3_file, sqlite3_io_methods};

impl<P: Policy> HostSql<P> {
    /// Sync the database file as this connection's checkpoints do: with
    /// `F_FULLFSYNC` on macOS and iOS when `checkpoint_fullfsync` or
    /// `fullfsync` is on, as SQLite's own checkpoint sync is, and a plain sync
    /// otherwise.
    pub(crate) fn sync_database_file(&self) -> Result<(), PolicyError<P>> {
        let flush = |pragma: &str| -> Result<bool, PolicyError<P>> {
            Ok(self.query_row::<i64, _>(&format!("PRAGMA {pragma}"), [], |r| r.get(0))? != 0)
        };
        let flags = if flush("checkpoint_fullfsync")? || flush("fullfsync")? {
            rusqlite::ffi::SQLITE_SYNC_FULL
        } else {
            rusqlite::ffi::SQLITE_SYNC_NORMAL
        };
        let (file, methods) = self.database_file()?;
        let sync = methods
            .xSync
            .ok_or(crate::Error::Invariant { what: "an open connection's database file syncs" })?;
        // SAFETY: the file is the connection's own, which no one else uses
        // while this owner holds the connection.
        failed(unsafe { sync(file, flags) })
    }

    /// The main database file's size in bytes.
    pub(crate) fn database_file_size(&self) -> Result<u64, PolicyError<P>> {
        let (file, methods) = self.database_file()?;
        let size_of = methods.xFileSize.ok_or(crate::Error::Invariant {
            what: "an open connection's database file has a size",
        })?;
        let mut bytes: i64 = 0;
        // SAFETY: the file is the connection's own, which no one else uses
        // while this owner holds the connection, and xFileSize writes one
        // integer to `bytes`.
        failed(unsafe { size_of(file, &raw mut bytes) })?;
        u64::try_from(bytes).map_err(|_negative| crate::Error::Invariant {
            what: "a database file's size is not negative",
        })
    }

    /// After a checkpoint, send the main database file's size to the
    /// database's watch, which this connection holds when it is a database's
    /// writer or checkpointer. The watch's receivers wake only when the size
    /// changed. This reads the size while it holds the watch's lock, so when
    /// two checkpoints end close together, the watch keeps the size read
    /// later.
    pub(crate) fn send_checkpointed_size(&self) -> Result<(), PolicyError<P>> {
        let Some(sizes) = &self.checkpointed_sizes else {
            return Ok(());
        };
        let mut read = Ok(());
        sizes.send_if_modified(|size| match self.database_file_size() {
            Ok(now) => std::mem::replace(size, now) != now,
            Err(error) => {
                read = Err(error);
                false
            }
        });
        read
    }

    /// SQLite's handle to the main database file, and the methods SQLite set
    /// as it opened the file.
    fn database_file(&self) -> Result<(*mut sqlite3_file, &sqlite3_io_methods), PolicyError<P>> {
        let mut file: *mut sqlite3_file = std::ptr::null_mut();
        // SAFETY: this owner exclusively uses its live connection, and
        // SQLITE_FCNTL_FILE_POINTER writes one pointer to `file`.
        let code = unsafe {
            rusqlite::ffi::sqlite3_file_control(
                self.connection.handle(),
                c"main".as_ptr(),
                rusqlite::ffi::SQLITE_FCNTL_FILE_POINTER,
                (&raw mut file).cast(),
            )
        };
        failed(code)?;
        // SAFETY: SQLite hands out the main database's file, open while the
        // connection is, with the methods SQLite set as it opened the file.
        // Those methods live as long as the process.
        let methods = unsafe { file.as_ref().and_then(|file| file.pMethods.as_ref()) }.ok_or(
            crate::Error::Invariant { what: "an open connection's database file has its methods" },
        )?;
        Ok((file, methods))
    }
}

/// SQLite's result `code` as a result.
fn failed<F: HostError, R: HostError>(code: std::ffi::c_int) -> Result<(), crate::Error<F, R>> {
    if code == rusqlite::ffi::SQLITE_OK {
        return Ok(());
    }
    Err(rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None).into())
}
