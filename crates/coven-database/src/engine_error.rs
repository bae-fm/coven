//! The error the engine's modules return: SQLite's failures, the handle's,
//! and its host's. `F` is the host's failure type, which a handle the host
//! failed carries as it is, and `R` is the type the host's policy refuses
//! with, which a refused statement carries as it is.

use coven_foundation::task::Bug;
use std::path::PathBuf;

/// A failure of the engine's own that ends its handle: every later call
/// and live query fails with the host's failure built from it.
#[derive(Debug)]
pub(crate) enum EngineFailure<F, R> {
    /// Rolling back a failed transaction failed, so a connection may still
    /// be inside it.
    Rollback,
    /// One of the engine's WAL checkpoints failed with this cause, other than
    /// on a busy or locked database, so checkpointing stopped.
    Checkpoint(EngineError<F, R>),
    /// Checking or gathering the query planner's statistics failed with this
    /// cause, other than on a busy or locked database, so the engine stopped
    /// keeping them.
    Statistics(EngineError<F, R>),
}

/// Why an engine call failed.
#[derive(Debug)]
pub(crate) enum EngineError<F, R> {
    /// SQLite refused or failed a statement, preserving its error.
    Sqlite(rusqlite::Error),
    /// A statement met damage SQLite reports (`SQLITE_CORRUPT`,
    /// `SQLITE_NOTADB`).
    DamagedDatabase,
    /// The host's policy, or one of the host's SQL functions, refused a
    /// statement with this refusal.
    Refused(R),
    /// App SQL tried transaction control, a PRAGMA, ATTACH, loading an
    /// extension, changing a hidden rowid, or changing the schema outside a
    /// migration.
    StatementForbidden {
        /// The refused operation.
        operation: &'static str,
    },
    /// SQLite rolled the owned transaction back itself, so it can't go on.
    TransactionEnded,
    /// The handle has closed the database.
    Closed,
    /// The call's work panicked: a bug, which the caller reports.
    Bug(Bug),
    /// An earlier bug may have left the handle's state wrong, so the handle
    /// failed with it, and every call fails with it until the host reopens
    /// the database.
    HandleFailed(Bug),
    /// The host failed the handle with this failure.
    Host(F),
    /// The engine broke a rule it relies on: a bug, not damage.
    Invariant {
        /// The broken rule.
        what: &'static str,
    },
    /// SQLite kept another journal mode instead of WAL.
    WalUnavailable {
        /// The journal mode SQLite selected.
        mode: String,
    },
    /// A SQLite setting is larger than SQLite can hold.
    SqliteSettingOutOfRange {
        /// The setting's PRAGMA name.
        setting: &'static str,
        /// The largest value SQLite holds for it.
        maximum: u64,
    },
    /// The system refused a thread of the engine's upkeep, so the database
    /// did not open.
    UpkeepThread(std::io::Error),
    /// Opening the database's file at `path` failed.
    #[cfg_attr(
        not(windows),
        expect(
            dead_code,
            reason = "only on Windows can a name in the path mean another file once the engine drops the path's verbatim prefix"
        )
    )]
    Path {
        /// The database file.
        path: PathBuf,
        /// What the file system answered.
        source: std::io::Error,
    },
    /// A transaction failed and rolling it back failed too. The handle fails
    /// with `EngineFailure::Rollback`.
    Rollback {
        /// The operation that required rollback.
        operation: Box<EngineError<F, R>>,
        /// SQLite's rollback failure.
        rollback: rusqlite::Error,
    },
}

impl<F: std::fmt::Display, R: std::fmt::Display> std::fmt::Display for EngineError<F, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(error) => error.fmt(f),
            Self::DamagedDatabase => f.write_str("damaged database"),
            Self::Refused(refusal) => refusal.fmt(f),
            Self::StatementForbidden { operation } => {
                write!(f, "app SQL cannot perform {operation}")
            }
            Self::TransactionEnded => f.write_str("SQLite ended the owned transaction"),
            Self::Closed => f.write_str("the store is closed"),
            Self::Bug(bug) => bug.fmt(f),
            Self::HandleFailed(bug) => {
                write!(f, "the store's handle failed on an earlier bug; reopen the store: {bug}")
            }
            Self::Host(failure) => failure.fmt(f),
            Self::Invariant { what } => write!(f, "coven invariant failed: {what}"),
            Self::WalUnavailable { mode } => {
                write!(f, "SQLite kept journal mode {mode} instead of WAL")
            }
            Self::SqliteSettingOutOfRange { setting, maximum } => {
                write!(f, "SQLite setting {setting} exceeds {maximum}")
            }
            Self::UpkeepThread(error) => write!(f, "an upkeep thread could not start: {error}"),
            Self::Path { path, source } => write!(f, "open database at {path:?}: {source}"),
            Self::Rollback { operation, rollback } => {
                write!(f, "{operation}; rollback failed: {rollback}")
            }
        }
    }
}

impl<F: std::error::Error + 'static, R: std::error::Error + 'static> std::error::Error
    for EngineError<F, R>
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sqlite(error) => error.source(),
            Self::Refused(refusal) => refusal.source(),
            Self::Bug(bug) => bug.source(),
            Self::Host(failure) => failure.source(),
            Self::UpkeepThread(error) => Some(error),
            Self::Path { source, .. } => Some(source),
            Self::Rollback { rollback, .. } => Some(rollback),
            _ => None,
        }
    }
}

impl<F, R> From<rusqlite::Error> for EngineError<F, R>
where
    F: std::error::Error + Send + Sync + 'static,
    R: std::error::Error + Send + Sync + 'static,
{
    fn from(error: rusqlite::Error) -> Self {
        match error {
            // An error the engine raised inside a SQL function or a column
            // conversion comes back as it was raised.
            rusqlite::Error::UserFunctionError(source) => match source.downcast::<Self>() {
                Ok(error) => *error,
                Err(source) => Self::Sqlite(rusqlite::Error::UserFunctionError(source)),
            },
            // SQLite found a page it can't read as what it should hold, or a
            // file that isn't a database.
            rusqlite::Error::SqliteFailure(failure, _)
                if matches!(
                    failure.code,
                    rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase
                ) =>
            {
                Self::DamagedDatabase
            }
            error => Self::Sqlite(error),
        }
    }
}

/// A caller's error for a transaction it runs: built from the engine's
/// errors, and able to keep a failed rollback beside its own cause.
pub(crate) trait RollbackJoin<F, R>: From<EngineError<F, R>> {
    /// Keep `rollback`, SQLite's failure to roll back, beside this error.
    fn with_rollback(self, rollback: rusqlite::Error) -> Self;
}

impl<F, R> RollbackJoin<F, R> for EngineError<F, R> {
    fn with_rollback(self, rollback: rusqlite::Error) -> Self {
        Self::Rollback { operation: Box::new(self), rollback }
    }
}
