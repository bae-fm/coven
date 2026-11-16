//! The failures coven reports to its handle as its host, after which the
//! handle's every call fails.

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
    /// cause, other than on a busy or locked database, and coven stopped
    /// keeping them.
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

/// The engine's failure that ended the handle, as coven holds it: each later
/// call's error carries the same cause.
impl From<sqlive::EngineFailure<StoreFailure, DbError>> for StoreFailure {
    fn from(failure: sqlive::EngineFailure<StoreFailure, DbError>) -> Self {
        match failure {
            sqlive::EngineFailure::Rollback => Self::Rollback,
            sqlive::EngineFailure::Checkpoint(cause) => {
                Self::Checkpoint(Arc::new(DbError::from(cause)))
            }
            sqlive::EngineFailure::Statistics(cause) => {
                Self::Statistics(Arc::new(DbError::from(cause)))
            }
            sqlive::EngineFailure::Hook(cause) => Self::State(Arc::new(DbError::from(cause))),
        }
    }
}

/// The store's handle: the failure state and live queries that the store's
/// databases share, failing with coven's `StoreFailure`.
pub(crate) type StoreHandle = sqlive::Handle<StoreFailure>;
