//! The error sqlive's calls return: SQLite's failures, the handle's, and its
//! host's. `F` is the host's failure type, which a handle the host
//! failed carries as it is, and `R` is the type the host's policy refuses
//! with, which a refused statement carries as it is.

use panic_as_error::Bug;
use std::path::PathBuf;

/// A failure of sqlive's own that ends its handle: every later call
/// and live query fails with the host's failure built from it.
#[derive(Debug)]
pub enum EngineFailure<F, R> {
    /// Rolling back a failed transaction failed, so a connection may still
    /// be inside it.
    Rollback,
    /// One of sqlive's WAL checkpoints failed with this cause, other than
    /// on a busy or locked database, so checkpointing stopped.
    Checkpoint(Error<F, R>),
    /// Checking or gathering the query planner's statistics failed with this
    /// cause, other than on a busy or locked database, so sqlive stopped
    /// keeping them.
    Statistics(Error<F, R>),
    /// SQLite refused to remove a transaction's hook with this cause, so
    /// what the connection calls next is unknown.
    Hook(Error<F, R>),
    /// Judging a change to the host tables failed with this cause: its read
    /// transaction or one of its lookups. The rows had changed already, and
    /// no write is left to fail, so a live query could miss the change.
    HostJudge(Error<F, R>),
}

/// Why a call failed.
#[derive(Debug)]
pub enum Error<F, R> {
    /// SQLite refused or failed a statement, preserving its error.
    Sqlite(rusqlite::Error),
    /// A statement met damage SQLite reports (`SQLITE_CORRUPT`,
    /// `SQLITE_NOTADB`).
    DamagedDatabase,
    /// The host's policy, or one of the host's SQL functions, refused a
    /// statement with this refusal.
    Refused(R),
    /// App SQL tried transaction control, a PRAGMA, ATTACH, loading an
    /// extension, or changing the schema outside a migration.
    StatementForbidden {
        /// The refused operation.
        operation: &'static str,
    },
    /// A table has no primary key, so no key tells its rows apart and sqlive
    /// can't capture its changes or read it for a live query.
    TableWithoutPrimaryKey {
        /// The table, as SQLite names it.
        table: String,
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
    /// sqlive broke a rule it relies on: a bug, not damage.
    Invariant {
        /// The broken rule.
        what: &'static str,
    },
    /// A compiled statement's scope plan was built against a schema that
    /// shows tables, views or functions otherwise than the open database
    /// does: the plan could narrow by what the database doesn't hold.
    CompiledSchemaDiffers(sqlive_reads::SchemaDiffers),
    /// A compiled statement's scope plan is no plan this sqlive's compiler
    /// writes.
    UndecodablePlan(sqlive_reads::UndecodablePlan),
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
    /// The system refused a thread of sqlive's own, for a database's upkeep
    /// or its host tables' judge, so the database did not open.
    Thread(std::io::Error),
    /// Opening the database's file at `path` failed. Only on Windows can a
    /// name in the path mean another file once sqlive drops the path's
    /// verbatim prefix.
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
        operation: Box<Error<F, R>>,
        /// SQLite's rollback failure.
        rollback: rusqlite::Error,
    },
}

impl<F: std::fmt::Display, R: std::fmt::Display> std::fmt::Display for Error<F, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(error) => error.fmt(f),
            Self::DamagedDatabase => f.write_str("damaged database"),
            Self::Refused(refusal) => refusal.fmt(f),
            Self::StatementForbidden { operation } => {
                write!(f, "app SQL cannot perform {operation}")
            }
            Self::TableWithoutPrimaryKey { table } => {
                write!(f, "table {table} has no primary key, so sqlive can't tell its rows apart")
            }
            Self::TransactionEnded => f.write_str("SQLite ended the owned transaction"),
            Self::Closed => f.write_str("the database is closed"),
            Self::Bug(bug) => bug.fmt(f),
            Self::HandleFailed(bug) => {
                write!(
                    f,
                    "the database's handle failed on an earlier bug; reopen the database: {bug}"
                )
            }
            Self::Host(failure) => failure.fmt(f),
            Self::Invariant { what } => write!(f, "sqlive invariant failed: {what}"),
            Self::CompiledSchemaDiffers(differs) => differs.fmt(f),
            Self::UndecodablePlan(error) => error.fmt(f),
            Self::WalUnavailable { mode } => {
                write!(f, "SQLite kept journal mode {mode} instead of WAL")
            }
            Self::SqliteSettingOutOfRange { setting, maximum } => {
                write!(f, "SQLite setting {setting} exceeds {maximum}")
            }
            Self::Thread(error) => write!(f, "a thread of sqlive's could not start: {error}"),
            Self::Path { path, source } => write!(f, "open database at {path:?}: {source}"),
            Self::Rollback { operation, rollback } => {
                write!(f, "{operation}; rollback failed: {rollback}")
            }
        }
    }
}

impl<F: std::error::Error + 'static, R: std::error::Error + 'static> std::error::Error
    for Error<F, R>
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sqlite(error) => error.source(),
            Self::Refused(refusal) => refusal.source(),
            Self::Bug(bug) => bug.source(),
            Self::Host(failure) => failure.source(),
            Self::Thread(error) => Some(error),
            Self::CompiledSchemaDiffers(differs) => Some(differs),
            Self::UndecodablePlan(error) => Some(error),
            Self::Path { source, .. } => Some(source),
            Self::Rollback { rollback, .. } => Some(rollback),
            _ => None,
        }
    }
}

impl<F, R> From<rusqlite::Error> for Error<F, R>
where
    F: std::error::Error + Send + Sync + 'static,
    R: std::error::Error + Send + Sync + 'static,
{
    fn from(error: rusqlite::Error) -> Self {
        match error {
            // An error sqlive raised inside a SQL function or a column
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

/// A caller's error for a transaction it runs: built from sqlive's
/// errors, and able to keep a failed rollback beside its own cause.
pub trait RollbackJoin<F, R>: From<Error<F, R>> {
    /// Keep `rollback`, SQLite's failure to roll back, beside this error.
    fn with_rollback(self, rollback: rusqlite::Error) -> Self;
}

impl<F, R> RollbackJoin<F, R> for Error<F, R> {
    fn with_rollback(self, rollback: rusqlite::Error) -> Self {
        Self::Rollback { operation: Box::new(self), rollback }
    }
}
