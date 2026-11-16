//! How coven reports the engine's errors: each as the `DbError` an app
//! sees, through which every error of coven's own converts from them.

use crate::engine_error::EngineError;
use crate::store_failure::{sole, StoreFailure};
use crate::{DbError, OpenError};
use std::sync::Arc;

/// An engine error as the `DbError` an app sees. An error coven raised
/// inside a SQL function, or a refusal of coven's policy, comes back as
/// coven raised it.
impl From<EngineError<StoreFailure>> for DbError {
    fn from(error: EngineError<StoreFailure>) -> Self {
        match error {
            EngineError::Sqlite(error) => Self::from(error),
            EngineError::DamagedDatabase => Self::DamagedDatabase,
            EngineError::Refused(refusal) => match refusal.downcast::<Self>() {
                Ok(error) => *error,
                Err(_) => Self::Invariant { what: "coven's policy refuses only with a DbError" },
            },
            EngineError::StatementForbidden { operation } => Self::StatementForbidden { operation },
            EngineError::TransactionEnded => Self::TransactionEnded,
            EngineError::RollbackFailed => Self::RollbackFailed,
            EngineError::CheckpointFailed(cause) => Self::CheckpointFailed(Arc::new(sole(cause))),
            EngineError::StatisticsFailed(cause) => Self::StatisticsFailed(Arc::new(sole(cause))),
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

impl From<EngineError<StoreFailure>> for OpenError {
    fn from(error: EngineError<StoreFailure>) -> Self {
        Self::Database(error.into())
    }
}

/// The `DbError` an error raised inside a SQL function or a column
/// conversion carries, coven's own or the engine's; any other error comes
/// back as it is.
pub(crate) fn raised(
    source: Box<dyn std::error::Error + Send + Sync>,
) -> Result<DbError, Box<dyn std::error::Error + Send + Sync>> {
    match source.downcast::<DbError>() {
        Ok(error) => Ok(*error),
        Err(source) => {
            source.downcast::<EngineError<StoreFailure>>().map(|error| DbError::from(*error))
        }
    }
}
