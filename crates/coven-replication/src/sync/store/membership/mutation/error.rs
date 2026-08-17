use coven_keys::keys::KeyError;
use coven_protocol::membership::MembershipError;
use coven_protocol::objects::StorageError;
use coven_storage::cloud::CloudHomeError;

#[derive(Debug, thiserror::Error)]
pub enum MembershipMutationError {
    #[error("Bucket error: {0}")]
    Bucket(#[from] StorageError),
    #[error("Key error: {0}")]
    Key(#[from] KeyError),
    #[error("Membership error: {0}")]
    Membership(#[from] MembershipError),
    #[error("Cloud home error: {0}")]
    CloudHome(#[from] CloudHomeError),
    #[error("Crypto error: {0}")]
    Crypto(String),
    #[error("membership mutation encryption: {0}")]
    Encryption(#[from] coven_keys::encryption::EncryptionError),
    #[error("membership mutation wrapped keyring: {0}")]
    WrappedKeyring(#[from] coven_protocol::wrapped_store_key::WrappedKeyringError),
    #[error("membership mutation JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("membership mutation history: {0}")]
    Pull(#[source] Box<crate::sync::store::StorePullError>),
    #[error("membership mutation chain: {0}")]
    AnchoredChain(#[source] Box<crate::sync::store::AnchoredChainError>),
    #[error("membership mutation worker: {0}")]
    Blocking(#[from] coven_foundation::blocking::BlockingTaskError),
    #[error("membership mutation Store operation: {0}")]
    Store(#[source] Box<crate::sync::store::StoreError>),
    #[error("membership mutation remote object: {0}")]
    RemoteObject(#[from] coven_protocol::remote_object::RemoteObjectRecordError),
    #[error("membership mutation Store protocol: {0}")]
    Protocol(#[from] coven_protocol::store_commit::StoreProtocolError),
    #[error("User {0} is not a current member")]
    NotAMember(String),
    #[error("Cannot revoke the last owner of a store")]
    LastOwner,
    #[error("membership mutation database state: {0}")]
    Database(#[from] coven_database::DbError),
    #[error("pending membership mutation does not match this request: {0}")]
    PendingMutation(String),
    #[error("durable membership mutation is invalid: {0}")]
    InvalidDurableMutation(String),
    #[error("durable membership object is invalid: {0}")]
    StoreObject(#[from] coven_protocol::objects::StoreObjectError),
    #[error("membership mutation preparation failed: {0}")]
    Preparation(#[from] coven_protocol::membership_mutation::MembershipPreparationError),
    #[error("membership rotation state failed: {0}")]
    RotationState(#[from] coven_storage::RotationStateError),
    #[error("prepared membership commit is invalid: {0}")]
    PreparedCommit(#[from] coven_protocol::prepared_commit::PreparedCommitError),
    #[error("membership floor is invalid: {0}")]
    MembershipFloor(#[from] coven_protocol::membership::MembershipFloorError),
}

impl From<crate::sync::store::StoreError> for MembershipMutationError {
    fn from(error: crate::sync::store::StoreError) -> Self {
        Self::Store(Box::new(error))
    }
}

impl From<crate::sync::store::StorePullError> for MembershipMutationError {
    fn from(error: crate::sync::store::StorePullError) -> Self {
        Self::Pull(Box::new(error))
    }
}

impl From<crate::sync::store::AnchoredChainError> for MembershipMutationError {
    fn from(error: crate::sync::store::AnchoredChainError) -> Self {
        Self::AnchoredChain(Box::new(error))
    }
}
