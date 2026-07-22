use crate::sync::circle::{CircleId, CircleTransitionError};
use crate::sync::store::{StoreError, StoreRegistrationError};
use crate::sync::store_objects::StoreObjectError;

#[derive(Debug, thiserror::Error)]
pub enum CircleOperationError {
    #[error("database: {0}")]
    Database(String),
    #[error("circle protocol state is absent: {0}")]
    MissingState(&'static str),
    #[error("circle protocol state is invalid: {0}")]
    InvalidState(String),
    #[error("circle construction: {0}")]
    Construction(#[from] CircleTransitionError),
    #[error("circle object: {0}")]
    Object(#[from] StoreObjectError),
    #[error("Store publication: {0}")]
    StoreOutbound(#[from] StoreError),
    #[error("Store device registration: {0}")]
    StoreRegistration(#[from] StoreRegistrationError),
    #[error("circles require opaque cloud storage")]
    BrowsableStorage,
    #[error("circle operation journal: {0}")]
    Journal(String),
    #[error("circle operation {circle_id} is blocked: {reason}")]
    Blocked { circle_id: CircleId, reason: String },
    #[error("circle command channel is closed")]
    CommandChannelClosed,
    #[error("circle command ended without a reply")]
    ReplyChannelClosed,
}

impl From<crate::database::DbError> for CircleOperationError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.into_message())
    }
}
