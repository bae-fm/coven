use crate::protocol::objects::StorageError;
use crate::protocol::store_commit::{OwnerPromotionId, OwnerPromotionStaleReason};
use crate::sync::store::StoreError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum OwnerPromotionError {
    #[error("Owner promotion database state: {0}")]
    Database(String),
    #[error("Owner promotion protocol state: {0}")]
    Protocol(String),
    #[error("Owner promotion storage: {0}")]
    Storage(String),
    #[error("Owner promotion request has not activated")]
    RequestNotActivated,
    #[error("Owner promotion {0} is absent")]
    NotFound(OwnerPromotionId),
    #[error("Owner promotion is stale: {0:?}")]
    Stale(Box<OwnerPromotionStaleReason>),
}

impl From<crate::database::DbError> for OwnerPromotionError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.into_message())
    }
}

impl From<StoreError> for OwnerPromotionError {
    fn from(error: StoreError) -> Self {
        Self::Protocol(error.to_string())
    }
}

impl From<StorageError> for OwnerPromotionError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error.to_string())
    }
}
