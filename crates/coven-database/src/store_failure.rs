//! The failures coven reports to its handle as its host, after which the
//! handle's every call fails.

use crate::engine_error::EngineError;
use crate::DbError;
use coven_foundation::clock::WallTimerError;
use std::sync::Arc;

#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum StoreFailure {
    #[error("saving the store's state failed")]
    State(Arc<DbError>),
    #[error("the handle's clock could not wait")]
    WallTimer(WallTimerError),
    /// Rolling back a failed transaction failed, so the writer may still
    /// hold its uncommitted changes.
    #[error("an earlier rollback failed")]
    Rollback,
    /// A WAL checkpoint failed with this cause, other than on a busy or
    /// locked database, and the store's checkpointing stopped.
    #[error("a WAL checkpoint failed")]
    Checkpoint(Arc<DbError>),
    /// Checking or gathering the planner's statistics failed with this
    /// cause, other than on a busy or locked database, and keeping them
    /// stopped.
    #[error("gathering the planner's statistics failed")]
    Statistics(Arc<DbError>),
}

impl StoreFailure {
    /// The error every later call returns.
    pub(crate) fn error(&self) -> DbError {
        match self {
            Self::State(cause) => DbError::StatePersistence(Arc::clone(cause)),
            Self::WallTimer(cause) => DbError::WallTimer(cause.clone()),
            Self::Rollback => DbError::RollbackFailed,
            Self::Checkpoint(cause) => DbError::CheckpointFailed(Arc::clone(cause)),
            Self::Statistics(cause) => DbError::StatisticsFailed(Arc::clone(cause)),
        }
    }
}

/// The engine's error that ended the handle, as coven holds it: each later
/// call's error carries the same cause.
impl From<EngineError<StoreFailure>> for StoreFailure {
    fn from(error: EngineError<StoreFailure>) -> Self {
        match error {
            EngineError::RollbackFailed => Self::Rollback,
            EngineError::CheckpointFailed(cause) => Self::Checkpoint(Arc::new(sole(cause))),
            EngineError::StatisticsFailed(cause) => Self::Statistics(Arc::new(sole(cause))),
            EngineError::Host(failure) => failure,
            // The engine ends a handle only on the three above; any other
            // error keeps its cause as a state failure.
            other => Self::State(Arc::new(DbError::from(other))),
        }
    }
}

/// A checkpoint's or statistics' cause as coven reports it. The engine
/// hands it on as its only holder.
pub(crate) fn sole(cause: Arc<EngineError<StoreFailure>>) -> DbError {
    match Arc::try_unwrap(cause) {
        Ok(cause) => DbError::from(cause),
        Err(_) => DbError::Invariant { what: "a checkpoint's or statistics' cause has one holder" },
    }
}
