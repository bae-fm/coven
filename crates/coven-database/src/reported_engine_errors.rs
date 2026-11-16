//! How coven reports the engine's errors: coven turns each engine error into
//! the `DbError` an app sees.

use crate::engine_error::EngineError;
use crate::store_failure::StoreFailure;
use crate::{DbError, OpenError};

/// Coven turns an engine error into the `DbError` an app sees. An error
/// coven raised inside a SQL function, or a refusal of coven's policy, comes
/// back as coven raised it.
impl From<EngineError<StoreFailure, DbError>> for DbError {
    fn from(error: EngineError<StoreFailure, DbError>) -> Self {
        match error {
            EngineError::Sqlite(error) => Self::from(error),
            EngineError::DamagedDatabase => Self::DamagedDatabase,
            EngineError::Refused(refusal) => refusal,
            EngineError::StatementForbidden { operation } => Self::StatementForbidden { operation },
            EngineError::TransactionEnded => Self::TransactionEnded,
            EngineError::Closed => Self::StoreClosed,
            EngineError::Bug(bug) => Self::Bug(bug),
            EngineError::HandleFailed(bug) => Self::StoreFailed(bug),
            EngineError::Host(failure) => failure.error(),
            EngineError::Invariant { what } => Self::Invariant { what },
            EngineError::WalUnavailable { mode } => Self::WalUnavailable { mode },
            EngineError::SqliteSettingOutOfRange { setting, maximum } => {
                Self::SqliteSettingOutOfRange { setting, maximum }
            }
            EngineError::UpkeepThread(error) => Self::UpkeepThread(error),
            EngineError::Path { path, source } => {
                Self::Disk(coven_foundation::files::FileError::Io {
                    operation: "open database",
                    path,
                    source: Box::new(source),
                })
            }
            EngineError::Rollback { operation, rollback } => {
                Self::Rollback { operation: Box::new(Self::from(*operation)), rollback }
            }
        }
    }
}

impl From<EngineError<StoreFailure, DbError>> for OpenError {
    fn from(error: EngineError<StoreFailure, DbError>) -> Self {
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
            .downcast::<EngineError<StoreFailure, DbError>>()
            .map(|error| DbError::from(*error)),
    }
}
