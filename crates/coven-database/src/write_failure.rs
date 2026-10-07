//! Preserve the facade's callback failure when a transaction or cleanup also fails.

use crate::{CovenError, DbError};

/// A caller's write error can retain both its original cause and failures that
/// happen while rolling back or removing bytes. Used by facade composition;
/// ordinary database clients use `DbError`.
pub trait WriteFailure: From<DbError> + From<rusqlite::Error> {
    /// Retain a failed rollback alongside the original failure.
    fn with_rollback(self, rollback: rusqlite::Error) -> Self;
    /// Retain the write's outcome and every failed byte-removal operation.
    fn with_cleanup(write: Result<(), Self>, failures: Vec<DbError>) -> Self
    where
        Self: Sized;
}

impl WriteFailure for DbError {
    fn with_rollback(self, rollback: rusqlite::Error) -> Self {
        Self::Rollback {
            operation: Box::new(self),
            rollback,
        }
    }
    fn with_cleanup(write: Result<(), Self>, failures: Vec<DbError>) -> Self {
        Self::FileCleanup {
            write: write.map_err(Box::new),
            failures,
        }
    }
}

impl WriteFailure for CovenError {
    fn with_rollback(self, rollback: rusqlite::Error) -> Self {
        match self {
            Self::Database(error) => Self::Database(error.with_rollback(rollback)),
            error => Self::Rollback {
                operation: Box::new(error),
                rollback,
            },
        }
    }
    fn with_cleanup(write: Result<(), Self>, failures: Vec<DbError>) -> Self {
        match write {
            Ok(()) => Self::Database(DbError::with_cleanup(Ok(()), failures)),
            Err(Self::Database(error)) => {
                Self::Database(DbError::with_cleanup(Err(error), failures))
            }
            Err(error) => Self::FileCleanup {
                write: Err(Box::new(error)),
                failures,
            },
        }
    }
}
