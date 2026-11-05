//! The app's error for opening, reading and writing a store: the database's
//! failures, a sync step's, and the facade's own opening and custody work.

use crate::task::{Bug, CarriesBug, Shutdown, TaskFailed};
use crate::{
    CovenMigrationError, DbError, KeyError, MigrationError, NetworkWatchError, SettingsError,
    SqliteError, StoreId, StoreLockError, SyncError, WriteFailure,
};
use coven_database::OpenError;

/// A store call's result, retaining its typed cause.
pub type CovenResult<T> = Result<T, CovenError>;

/// Failures of opening, reading and writing a store (§4, E1, E3).
#[derive(Debug, thiserror::Error)]
pub enum CovenError {
    /// Reading device identity or an app callback's custody operation failed.
    #[error(transparent)]
    Key(KeyError),
    /// An app callback failed and SQLite also failed to roll it back. Later
    /// calls fail with `DbError::RollbackFailed` until the store is reopened (§4).
    #[error("{operation}; rollback failed: {rollback}")]
    Rollback {
        /// The callback or database failure.
        operation: Box<CovenError>,
        /// SQLite's rollback failure.
        #[source]
        rollback: SqliteError,
    },
    /// File cleanup failed after the callback failed, retaining both.
    #[error("file cleanup failed: {failures:?}; write failed: {write}")]
    FileCleanup {
        /// The callback's original error.
        write: Box<CovenError>,
        /// Each failed byte removal or pending-record update.
        failures: Vec<DbError>,
    },
    /// The database or a write's validation failed.
    #[error(transparent)]
    Database(#[from] DbError),
    /// A sync step failed inside an app's write, which did not commit, or
    /// while the store opened, keeping sync's own error (E1, E3).
    #[error(transparent)]
    Sync(SyncError),
    /// An app migration failed or cannot run on this schema.
    #[error(transparent)]
    Migration(#[from] MigrationError),
    /// Coven's internal schema is unsupported or its migration cannot run.
    #[error(transparent)]
    CovenMigration(#[from] CovenMigrationError),
    /// The directory's settings could not be read.
    #[error(transparent)]
    Settings(#[from] SettingsError),
    /// The writer's lock could not be acquired.
    #[error(transparent)]
    Lock(StoreLockError),
    /// A required builder choice was not supplied (E1).
    #[error("opening requires {field}")]
    MissingConfiguration {
        /// The missing choice.
        field: &'static str,
    },
    /// The keychain holds no entry naming this directory's device; opening
    /// changed nothing (§19.3).
    #[error("store {store}: this device's keychain entry is missing")]
    DeviceKeyMissing {
        /// The store whose device entry is missing.
        store: StoreId,
    },
    /// A connected store's identity custody holds no member keys either,
    /// beside the missing device entry; only restoring with the restore code
    /// continues. Opening changed nothing (§19.3, §13).
    #[error("store {store}: this device's entry and member keys are missing")]
    MemberKeysMissing {
        /// The store whose member keys are missing.
        store: StoreId,
    },
    /// The device is locked, and the protection class the app chose keeps
    /// the store's files closed until it unlocks; opening changed nothing
    /// the lock refused (api.md E1). The app waits for the unlock and opens
    /// again.
    #[error("{}: the device is locked, and the store's files are protected until it unlocks", path.display())]
    DeviceLocked {
        /// The first protected file the open was refused.
        path: std::path::PathBuf,
    },
    /// The call's runtime is shutting down and dropped coven's work for it
    /// on the blocking pool unstarted, such as opening's custody work, so
    /// that work changed nothing (E15). A database call dropped this way is
    /// `DbError::Shutdown`.
    #[error(transparent)]
    Shutdown(#[from] Shutdown),
    /// Coven's work for the call panicked on the blocking pool, such as
    /// opening's custody work or a sync step: a bug, which the caller
    /// reports (E15). A database call that panicked is `DbError::Bug`.
    #[error(transparent)]
    Bug(#[from] Bug),
    /// The platform refused to let coven watch the device's network, such
    /// as an Android app without coven's Android library (api.md E1, E5).
    #[error(transparent)]
    NetworkWatch(#[from] NetworkWatchError),
}

impl CarriesBug for CovenError {
    fn bug(&self) -> Option<&Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::Database(error) => error.bug(),
            Self::Sync(error) => error.bug(),
            Self::Key(error) => error.bug(),
            Self::Lock(error) => error.bug(),
            Self::Rollback { operation, .. } => operation.bug(),
            Self::FileCleanup { write, failures } => {
                write.bug().or_else(|| failures.iter().find_map(DbError::bug))
            }
            Self::Migration(MigrationError::Failed { source, .. })
            | Self::CovenMigration(CovenMigrationError::Failed { source }) => source.bug(),
            _ => None,
        }
    }
}

impl From<KeyError> for CovenError {
    fn from(error: KeyError) -> Self {
        match error {
            KeyError::Bug(bug) => Self::Bug(bug),
            error => Self::Key(error),
        }
    }
}

impl From<StoreLockError> for CovenError {
    fn from(error: StoreLockError) -> Self {
        match error {
            StoreLockError::Bug(bug) => Self::Bug(bug),
            error => Self::Lock(error),
        }
    }
}

impl From<TaskFailed> for CovenError {
    fn from(failed: TaskFailed) -> Self {
        failed.into_error()
    }
}

impl From<SqliteError> for CovenError {
    fn from(error: SqliteError) -> Self {
        Self::Database(error.into())
    }
}

/// Opening the database fails as the app's open call reports it, each
/// cause under its own name.
impl From<OpenError> for CovenError {
    fn from(error: OpenError) -> Self {
        match error {
            OpenError::Database(error) => Self::Database(error),
            OpenError::Migration(error) => Self::Migration(error),
            OpenError::CovenMigration(error) => Self::CovenMigration(error),
            OpenError::Settings(error) => Self::Settings(error),
            OpenError::Lock(error) => Self::Lock(error),
            OpenError::MissingConfiguration { field } => Self::MissingConfiguration { field },
            OpenError::Bug(bug) => Self::Bug(bug),
        }
    }
}

/// A sync step's failure inside an app's write or while the store opens: a
/// database failure as itself, a bug or shutdown as the call's own, and any
/// other as the step's own error (§4, E15).
impl From<SyncError> for CovenError {
    fn from(error: SyncError) -> Self {
        match error {
            SyncError::Database(error) => Self::Database(error),
            SyncError::Shutdown(shutdown) => Self::Shutdown(shutdown),
            SyncError::Bug(bug) => Self::Bug(bug),
            error => Self::Sync(error),
        }
    }
}

/// An app's write failing with `CovenError` keeps a failed rollback or byte
/// removal in the error it wraps: the database's and a sync step's in their
/// own, any other beside it (Appendix C.3).
impl WriteFailure for CovenError {
    fn with_rollback(self, rollback: SqliteError) -> Self {
        match self {
            Self::Database(error) => Self::Database(error.with_rollback(rollback)),
            Self::Sync(error) => Self::Sync(error.with_rollback(rollback)),
            error => Self::Rollback { operation: Box::new(error), rollback },
        }
    }
    fn with_cleanup(write: Self, failures: Vec<DbError>) -> Self {
        match write {
            Self::Database(error) => Self::Database(DbError::with_cleanup(error, failures)),
            Self::Sync(error) => Self::Sync(SyncError::with_cleanup(error, failures)),
            error => Self::FileCleanup { write: Box::new(error), failures },
        }
    }
}

#[cfg(test)]
#[path = "coven_error_tests.rs"]
mod tests;
