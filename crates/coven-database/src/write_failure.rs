//! Preserve the facade's callback failure when a transaction or cleanup also fails.
//! A committed result never becomes a failure because of cleanup (§2.1, Appendix C.3).

use crate::{CovenError, DbError};

/// A caller's write error can retain both its original cause and failures that
/// happen while rolling back or removing bytes. Used by facade composition;
/// ordinary database clients use `DbError`.
pub trait WriteFailure: From<DbError> + From<rusqlite::Error> {
    /// Retain a failed rollback alongside the original failure.
    fn with_rollback(self, rollback: rusqlite::Error) -> Self;
    /// Retain the failed write's error and every failed byte-removal operation.
    fn with_cleanup(write: Self, failures: Vec<DbError>) -> Self
    where
        Self: Sized;
}

/// The outcome of an operation whose released bytes were then removed. A
/// failed removal stays recorded for the next write or open (Appendix C.3): it
/// joins a failed operation's error, and leaves a committed result as it is.
pub(crate) fn after_cleanup<R, E: WriteFailure>(
    result: Result<R, E>,
    failures: Vec<DbError>,
) -> Result<R, E> {
    if failures.is_empty() {
        return result;
    }
    match result {
        Ok(result) => {
            tracing::error!(
                ?failures,
                "removing released file bytes failed; their records stay for the next write or open"
            );
            Ok(result)
        }
        Err(error) => Err(E::with_cleanup(error, failures)),
    }
}

impl WriteFailure for DbError {
    fn with_rollback(self, rollback: rusqlite::Error) -> Self {
        Self::Rollback {
            operation: Box::new(self),
            rollback,
        }
    }
    fn with_cleanup(write: Self, failures: Vec<DbError>) -> Self {
        Self::FileCleanup {
            write: Err(Box::new(write)),
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
    fn with_cleanup(write: Self, failures: Vec<DbError>) -> Self {
        match write {
            Self::Database(error) => Self::Database(DbError::with_cleanup(error, failures)),
            error => Self::FileCleanup {
                write: Box::new(error),
                failures,
            },
        }
    }
}
