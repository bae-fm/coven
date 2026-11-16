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
}

impl StoreFailure {
    /// The error every later call returns.
    pub(crate) fn error(&self) -> DbError {
        match self {
            Self::State(cause) => DbError::StatePersistence(Arc::clone(cause)),
            Self::WallTimer(cause) => DbError::WallTimer(cause.clone()),
        }
    }
}
