use crate::protocol::objects::StorageError;
use crate::protocol::store_commit::{OwnerPromotionId, OwnerPromotionStaleReason};
use crate::sync::store::StoreError;

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

impl From<crate::protocol::objects::StoreObjectError> for OwnerPromotionError {
    fn from(error: crate::protocol::objects::StoreObjectError) -> Self {
        match error {
            crate::protocol::objects::StoreObjectError::Storage(error) => error.into(),
            error => Self::Protocol(error.to_string()),
        }
    }
}

impl From<crate::protocol::prepared_commit::PreparedCommitError> for OwnerPromotionError {
    fn from(error: crate::protocol::prepared_commit::PreparedCommitError) -> Self {
        OwnerPromotionError::Protocol(error.to_string())
    }
}

impl From<crate::protocol::membership_mutation::MembershipPreparationError>
    for OwnerPromotionError
{
    fn from(error: crate::protocol::membership_mutation::MembershipPreparationError) -> Self {
        OwnerPromotionError::Protocol(error.to_string())
    }
}

impl From<crate::protocol::owner_promotion_journal::OwnerPromotionJournalError>
    for OwnerPromotionError
{
    fn from(error: crate::protocol::owner_promotion_journal::OwnerPromotionJournalError) -> Self {
        OwnerPromotionError::Protocol(error.to_string())
    }
}
