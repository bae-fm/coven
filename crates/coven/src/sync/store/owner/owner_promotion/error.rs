use crate::sync::store::StoreError;
use coven_protocol::objects::StorageError;
use coven_protocol::store_commit::{OwnerPromotionId, OwnerPromotionStaleReason};

#[derive(Debug, thiserror::Error)]
pub(crate) enum OwnerPromotionError {
    #[error("Owner promotion database state: {0}")]
    Database(#[from] crate::database::DbError),
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

impl From<coven_protocol::objects::StoreObjectError> for OwnerPromotionError {
    fn from(error: coven_protocol::objects::StoreObjectError) -> Self {
        match error {
            coven_protocol::objects::StoreObjectError::Storage(error) => error.into(),
            error => Self::Protocol(error.to_string()),
        }
    }
}

impl From<coven_protocol::prepared_commit::PreparedCommitError> for OwnerPromotionError {
    fn from(error: coven_protocol::prepared_commit::PreparedCommitError) -> Self {
        OwnerPromotionError::Protocol(error.to_string())
    }
}

impl From<coven_protocol::membership_mutation::MembershipPreparationError> for OwnerPromotionError {
    fn from(error: coven_protocol::membership_mutation::MembershipPreparationError) -> Self {
        OwnerPromotionError::Protocol(error.to_string())
    }
}

impl From<coven_protocol::owner_promotion_journal::OwnerPromotionJournalError>
    for OwnerPromotionError
{
    fn from(error: coven_protocol::owner_promotion_journal::OwnerPromotionJournalError) -> Self {
        OwnerPromotionError::Protocol(error.to_string())
    }
}
