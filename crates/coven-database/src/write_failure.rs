//! Preserve the facade's callback failure when a transaction or cleanup also fails.
//! A committed result never becomes a failure because of cleanup (§2.1, Appendix C.3).

use crate::engine_error::EngineError;
use crate::store_failure::StoreFailure;
use crate::DbError;

/// A caller's write error can retain both its original cause and failures that
/// happen while rolling back or removing bytes, and says which bug it carries,
/// for a write whose caller stopped waiting (E15). A write fails with its
/// closure's own error type, which implements this: `DbError` for ordinary
/// database clients, sync's error for sync's writes, and an app's own type
/// for the app's writes through the facade (E3).
pub trait WriteFailure:
    From<DbError> + From<rusqlite::Error> + coven_foundation::task::CarriesBug
{
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
        Self::Rollback { operation: Box::new(self), rollback }
    }
    fn with_cleanup(write: Self, failures: Vec<DbError>) -> Self {
        Self::FileCleanup { write: Err(Box::new(write)), failures }
    }
}

impl crate::engine_error::RollbackJoin<StoreFailure, DbError> for DbError {
    fn with_rollback(self, rollback: rusqlite::Error) -> Self {
        WriteFailure::with_rollback(self, rollback)
    }
}

/// A caller's error `E` inside an engine call. `E` may be an app's or sync's
/// type, which knows only `DbError`, so the engine's errors reach it as the
/// `DbError` each maps to.
pub(crate) struct Hosted<E>(pub(crate) E);

impl<E> Hosted<E> {
    /// The caller's error.
    pub(crate) fn into_inner(self) -> E {
        self.0
    }
}

/// Run `run`, whose error is the caller's `E`, inside an engine closure.
pub(crate) fn hosted<T, E>(run: impl FnOnce() -> Result<T, E>) -> Result<T, Hosted<E>> {
    run().map_err(Hosted)
}

impl<E: From<DbError>> From<EngineError<StoreFailure, DbError>> for Hosted<E> {
    fn from(error: EngineError<StoreFailure, DbError>) -> Self {
        Self(E::from(DbError::from(error)))
    }
}

impl<E: WriteFailure> crate::engine_error::RollbackJoin<StoreFailure, DbError> for Hosted<E> {
    fn with_rollback(self, rollback: rusqlite::Error) -> Self {
        Self(self.0.with_rollback(rollback))
    }
}
