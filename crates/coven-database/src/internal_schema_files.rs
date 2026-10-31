//! Coven's own synced table of files (§17.2, Appendix C.1). Each row is one
//! file's record in one audience; app rows reference it by its id. Coven
//! writes it, and its rows travel as row changes of this table, merged like
//! any synced row. App SQL reads it as `coven_files` and never writes it.
//! A file made over another file's bytes names that file in `shares`, so
//! the device holding those bytes finds them for it however the record
//! arrives.
//! The app's local triggers may follow it (§9.4): coven makes, stores and
//! deletes a record with one statement on this table, as this device's own
//! write and as it applies another device's.

/// The table of files, which app SQL reads and the app's triggers may follow.
pub(crate) const FILES: &str = "coven_files";

pub(super) const SCHEMA: &str = "CREATE TABLE coven_files (
        id TEXT PRIMARY KEY NOT NULL,
        audience TEXT NOT NULL,
        size INTEGER NOT NULL,
        hash BLOB,
        coordinates TEXT NOT NULL,
        namespace TEXT NOT NULL,
        shares TEXT
    ) STRICT, WITHOUT ROWID;";

#[cfg(test)]
#[path = "internal_schema_files_tests.rs"]
mod tests;
