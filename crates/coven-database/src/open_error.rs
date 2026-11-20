//! Why opening the database failed: its connections, lock, settings, coven's
//! own schema or the app's migrations (§4, E1, E13).

use crate::DbError;
use coven_foundation::files::{SettingsError, StoreLockError};
use coven_foundation::task::{Bug, CarriesBug};

/// Opening the database failed: its writer or read-only connections, its
/// lock, its settings or its migrations (§4, E1, E13).
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// The database failed.
    #[error(transparent)]
    Database(#[from] DbError),
    /// An app migration failed or cannot run on this schema.
    #[error(transparent)]
    Migration(#[from] MigrationError),
    /// Coven's internal schema is unsupported or its tables cannot be created.
    #[error(transparent)]
    CovenSchema(#[from] CovenSchemaError),
    /// The directory's settings could not be read.
    #[error(transparent)]
    Settings(SettingsError),
    /// The store's lock could not be taken.
    #[error(transparent)]
    Lock(StoreLockError),
    /// A required builder choice was not supplied (E1).
    #[error("opening requires {field}")]
    MissingConfiguration {
        /// The missing choice.
        field: &'static str,
    },
    /// Taking the store's lock panicked: a bug, which the caller reports
    /// (E15). Opening's database work that panicked is `DbError::Bug`.
    #[error(transparent)]
    Bug(#[from] coven_foundation::task::Bug),
}

impl CarriesBug for OpenError {
    fn bug(&self) -> Option<&Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::Database(error) => error.bug(),
            Self::Lock(error) => error.bug(),
            Self::Settings(error) => error.bug(),
            Self::Migration(MigrationError::Failed { source, .. })
            | Self::CovenSchema(CovenSchemaError::Failed { source }) => source.bug(),
            Self::Migration(
                MigrationError::NotContiguous { .. } | MigrationError::SchemaTooNew { .. },
            )
            | Self::CovenSchema(
                CovenSchemaError::ReadOnly | CovenSchemaError::SchemaTooNew { .. },
            )
            | Self::MissingConfiguration { .. } => None,
        }
    }
}

impl From<SettingsError> for OpenError {
    fn from(error: SettingsError) -> Self {
        match error {
            SettingsError::Bug(bug) => Self::Bug(bug),
            error => Self::Settings(error),
        }
    }
}

impl From<StoreLockError> for OpenError {
    fn from(error: StoreLockError) -> Self {
        match error {
            StoreLockError::Bug(bug) => Self::Bug(bug),
            error => Self::Lock(error),
        }
    }
}

impl From<sqlive::rusqlite::Error> for OpenError {
    fn from(error: sqlive::rusqlite::Error) -> Self {
        Self::Database(error.into())
    }
}

/// Coven's local tables cannot be used at this version (Appendix C.1, E1).
#[derive(Debug, thiserror::Error)]
pub enum CovenSchemaError {
    /// The database holds no coven tables, and the read-only connection cannot create them.
    #[error("read-only open cannot create coven's schema")]
    ReadOnly,
    /// The internal schema requires a newer coven.
    #[error("coven schema version {current} is newer than supported version {supported}")]
    SchemaTooNew {
        /// The database's internal schema version.
        current: u32,
        /// The internal schema version this coven supports.
        supported: u32,
    },
    /// Creating the tables failed and its transaction rolled back.
    #[error("creating coven's schema failed: {source}")]
    Failed {
        /// The failure, including any rollback error.
        #[source]
        source: Box<DbError>,
    },
}

/// The application migration run could not complete.
#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    /// The list is not numbered from one without gaps.
    #[error("migration at {position} has version {found}, expected {expected}")]
    NotContiguous {
        /// Zero-based position in the list.
        position: usize,
        /// Declared version.
        found: u32,
        /// Required version.
        expected: u32,
    },
    /// The database requires a newer app.
    #[error("schema version {current} is newer than supported version {supported}")]
    SchemaTooNew {
        /// The database version.
        current: u32,
        /// The last supplied migration.
        supported: u32,
    },
    /// The migration or final validation failed; the whole run rolled back.
    #[error("migration {version} ({name}) failed: {source}")]
    Failed {
        /// Migration being run, or the final migration for validation errors.
        version: u32,
        /// Its name.
        name: &'static str,
        /// The original failure.
        #[source]
        source: Box<DbError>,
    },
}
