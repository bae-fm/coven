//! Coven's own synced table of files (§17.2, Appendix C.1). Each row is one
//! file's record in one audience; app rows reference it by its id. Coven
//! writes it, and its rows travel as row changes of this table, merged like
//! any synced row. App SQL reads it as `coven_files` and never writes it.

/// The table of files, which app SQL reads.
pub(crate) const FILES: &str = "coven_files";

pub(super) const SCHEMA: &str = "CREATE TABLE coven_files (
        id TEXT PRIMARY KEY NOT NULL,
        audience TEXT NOT NULL,
        size INTEGER NOT NULL,
        hash BLOB,
        coordinates TEXT NOT NULL,
        namespace TEXT NOT NULL
    ) STRICT, WITHOUT ROWID;";
