use crate::keys::KeyError;
use crate::storage::cloud::CloudHomeError;
use crate::sync::membership::MembershipError;
use crate::sync::storage::StorageError;

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
    #[error(
        "stale membership head for {author}: committed through seq {committed}, \
         cannot write seq {attempted}"
    )]
    StaleMembershipHead {
        author: String,
        committed: u64,
        attempted: u64,
    },
    #[error("{operation} failed: {original}; rollback failed: {rollback}")]
    Rollback {
        operation: &'static str,
        original: String,
        rollback: String,
    },
    #[error("User {0} is not a current member")]
    NotAMember(String),
    #[error("Cannot revoke the last owner of a store")]
    LastOwner,
    #[error("membership mutation database state: {0}")]
    Database(String),
    #[error("pending membership mutation does not match this request: {0}")]
    PendingMutation(String),
    #[error("durable membership mutation is invalid: {0}")]
    InvalidDurableMutation(String),
}

impl From<crate::database::DbError> for InviteError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.into_message())
    }
}
