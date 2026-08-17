use crate::sync::store::StoreError;
use coven_protocol::objects::StorageError;
use coven_protocol::store_commit::{OwnerPromotionId, OwnerPromotionStaleReason};

#[derive(Debug, thiserror::Error)]
pub enum OwnerPromotionError {
    #[error("Owner promotion database state: {0}")]
    Database(#[from] coven_database::DbError),
    #[error("Owner promotion protocol state: {0}")]
    Protocol(String),
    #[error("Owner promotion storage: {0}")]
    Storage(#[from] StorageError),
    #[error("Owner promotion Store operation: {0}")]
    Store(#[source] Box<StoreError>),
    #[error("Owner promotion writer authorization: {0}")]
    WriterAuthorization(#[source] Box<crate::sync::store::StoreWriterAuthorizationError>),
    #[error("Owner promotion Store pull: {0}")]
    StorePull(#[source] Box<crate::sync::store::StorePullError>),
    #[error("Owner promotion membership operation: {0}")]
    MembershipMutation(#[source] Box<crate::sync::store::MembershipMutationError>),
    #[error("Owner promotion membership chain: {0}")]
    AnchoredChain(#[source] Box<crate::sync::store::AnchoredChainError>),
    #[error("Owner promotion Store object: {0}")]
    StoreObject(#[from] coven_protocol::objects::StoreObjectError),
    #[error("Owner promotion prepared commit: {0}")]
    PreparedCommit(#[from] coven_protocol::prepared_commit::PreparedCommitError),
    #[error("Owner promotion membership preparation: {0}")]
    MembershipPreparation(#[from] coven_protocol::membership_mutation::MembershipPreparationError),
    #[error("Owner promotion journal: {0}")]
    Journal(#[from] coven_protocol::owner_promotion_journal::OwnerPromotionJournalError),
    #[error("Owner promotion Store protocol: {0}")]
    StoreProtocol(#[from] coven_protocol::store_commit::StoreProtocolError),
    #[error("Owner promotion membership protocol: {0}")]
    Membership(#[from] coven_protocol::membership::MembershipError),
    #[error("Owner promotion key: {0}")]
    Key(#[from] coven_keys::keys::KeyError),
    #[error("Owner promotion encryption: {0}")]
    Encryption(#[from] coven_keys::encryption::EncryptionError),
    #[error("Owner promotion request has not activated")]
    RequestNotActivated,
    #[error("Owner promotion requires an encrypted cloud home")]
    EncryptionRequired,
    #[error("Owner promotion {0} is absent")]
    NotFound(OwnerPromotionId),
    #[error("Owner promotion is stale: {0:?}")]
    Stale(Box<OwnerPromotionStaleReason>),
}

impl From<StoreError> for OwnerPromotionError {
    fn from(error: StoreError) -> Self {
        Self::Store(Box::new(error))
    }
}

impl From<crate::sync::store::StoreWriterAuthorizationError> for OwnerPromotionError {
    fn from(error: crate::sync::store::StoreWriterAuthorizationError) -> Self {
        Self::WriterAuthorization(Box::new(error))
    }
}

impl From<crate::sync::store::StorePullError> for OwnerPromotionError {
    fn from(error: crate::sync::store::StorePullError) -> Self {
        Self::StorePull(Box::new(error))
    }
}

impl From<crate::sync::store::MembershipMutationError> for OwnerPromotionError {
    fn from(error: crate::sync::store::MembershipMutationError) -> Self {
        Self::MembershipMutation(Box::new(error))
    }
}

impl From<crate::sync::store::AnchoredChainError> for OwnerPromotionError {
    fn from(error: crate::sync::store::AnchoredChainError) -> Self {
        Self::AnchoredChain(Box::new(error))
    }
}
