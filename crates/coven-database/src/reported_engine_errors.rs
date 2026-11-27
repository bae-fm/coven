//! How coven reports the engine's errors: coven turns each engine error into
//! the `DbError` an app sees.

use crate::store_failure::StoreFailure;
use crate::{DbError, OpenError};

/// Coven turns an engine error into the `DbError` an app sees. An error
/// coven raised inside a SQL function, or a refusal of coven's policy, comes
/// back as coven raised it.
impl From<sqlive::Error<StoreFailure, DbError>> for DbError {
    fn from(error: sqlive::Error<StoreFailure, DbError>) -> Self {
        match error {
            sqlive::Error::Sqlite(error) => Self::from(error),
            sqlive::Error::DamagedDatabase => Self::DamagedDatabase,
            sqlive::Error::Refused(refusal) => refusal,
            sqlive::Error::StatementForbidden { operation } => {
                Self::StatementForbidden { operation }
            }
            sqlive::Error::TableWithoutPrimaryKey { table } => {
                Self::Schema(crate::SchemaError::NoPrimaryKey { table })
            }
            sqlive::Error::TransactionEnded => Self::TransactionEnded,
            sqlive::Error::Closed => Self::StoreClosed,
            sqlive::Error::Bug(bug) => Self::Bug(bug),
            sqlive::Error::HandleFailed(bug) => Self::StoreFailed(bug),
            sqlive::Error::Host(failure) => failure.error(),
            sqlive::Error::Invariant { what } => Self::Invariant { what },
            sqlive::Error::CompiledSchemaDiffers(differs) => Self::CompiledSchemaDiffers(differs),
            sqlive::Error::UndecodablePlan(error) => Self::UndecodablePlan(error),
            sqlive::Error::WalUnavailable { mode } => Self::WalUnavailable { mode },
            sqlive::Error::SqliteSettingOutOfRange { setting, maximum } => {
                Self::SqliteSettingOutOfRange { setting, maximum }
            }
            sqlive::Error::UpkeepThread(error) => Self::UpkeepThread(error),
            sqlive::Error::Path { path, source } => {
                Self::Disk(coven_foundation::files::FileError::Io {
                    operation: "open database",
                    path,
                    source: Box::new(source),
                })
            }
            sqlive::Error::Rollback { operation, rollback } => {
                Self::Rollback { operation: Box::new(Self::from(*operation)), rollback }
            }
        }
    }
}

impl From<sqlive::Error<StoreFailure, DbError>> for OpenError {
    fn from(error: sqlive::Error<StoreFailure, DbError>) -> Self {
        Self::Database(error.into())
    }
}

/// Return the `DbError` that an error raised inside a SQL function or a
/// column conversion carries, whether coven or the engine raised it, and
/// return any other error unchanged.
pub(crate) fn raised(
    source: Box<dyn std::error::Error + Send + Sync>,
) -> Result<DbError, Box<dyn std::error::Error + Send + Sync>> {
    match source.downcast::<DbError>() {
        Ok(error) => Ok(*error),
        Err(source) => source
            .downcast::<sqlive::Error<StoreFailure, DbError>>()
            .map(|error| DbError::from(*error)),
    }
}

#[cfg(test)]
#[path = "reported_engine_errors_tests.rs"]
mod tests;
