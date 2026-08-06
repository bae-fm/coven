use crate::keys::KeyError;
use crate::protocol::membership::MembershipError;
use crate::protocol::objects::StorageError;
use crate::storage::cloud::CloudHomeError;

#[derive(Debug, thiserror::Error)]
pub enum InviteError {
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
    #[error("User {0} is not a current member")]
    NotAMember(String),
    #[error("Cannot revoke the last owner of a store")]
    LastOwner,
    #[error("membership mutation database state: {0}")]
    Database(#[from] crate::database::DbError),
    #[error("pending membership mutation does not match this request: {0}")]
    PendingMutation(String),
    #[error("durable membership mutation is invalid: {0}")]
    InvalidDurableMutation(String),
}

impl From<crate::protocol::objects::StoreObjectError> for InviteError {
    fn from(error: crate::protocol::objects::StoreObjectError) -> Self {
        match error {
            crate::protocol::objects::StoreObjectError::Storage(error) => Self::Bucket(error),
            error => Self::InvalidDurableMutation(error.to_string()),
        }
    }
}

impl From<crate::protocol::membership_mutation::MembershipPreparationError> for InviteError {
    fn from(error: crate::protocol::membership_mutation::MembershipPreparationError) -> Self {
        InviteError::InvalidDurableMutation(error.to_string())
    }
}
