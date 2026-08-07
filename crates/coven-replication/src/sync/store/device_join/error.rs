#[derive(Debug, thiserror::Error)]
pub enum DeviceJoinError {
    #[error("device join signature is invalid")]
    InvalidSignature,
    #[error("device join offer does not name one active Store/member/provider authority")]
    OfferMismatch,
    #[error(
        "device provider admission approval differs from its request or activated access grant"
    )]
    ApprovalMismatch,
    #[error("device registration request differs from its offer, approval, or reserved slots")]
    RegistrationRequestMismatch,
    #[error("device join attempt differs from its signed exchange")]
    AttemptMismatch,
    #[error("device join cleanup does not contain the unconditional canonical slot set")]
    CleanupMismatch,
    #[error("device join journal transition is not the declared adjacent transition")]
    NonAdjacentJournalTransition,
    #[error("device join journal has a different durable value for this role and attempt")]
    JournalConflict,
    #[error("device join reserved slots are not pairwise distinct")]
    DuplicateReservedSlot,
    #[error("device join requires an existing Member identity")]
    MemberNotEligible,
    #[error("provider operation failed: {0}")]
    Provider(String),
    #[error("Store device join state: {0}")]
    Store(String),
    #[error("Store device join database state: {0}")]
    Database(#[from] coven_database::DbError),
    #[error("device join requires an activated local Store device")]
    ActiveDeviceRequired,
    #[error("device join requires the active local Owner authority")]
    OwnerAuthorityRequired,
    #[error("device join requires the selected effective provider administrator")]
    ProviderAdministratorRequired,
    #[error("device join requires resolved Store membership")]
    MembershipConflict,
    #[error("device join attempt cut does not include its provider-access activation")]
    ApprovalActivationMissing,
    #[error("device join activation is not materialized in the installed Store database")]
    ActivationNotMaterialized,
    #[error(transparent)]
    Object(#[from] coven_protocol::objects::StoreObjectError),
    #[error(transparent)]
    Registration(#[from] crate::sync::store::StoreRegistrationError),
    #[error(transparent)]
    Pull(#[from] crate::sync::store::pull::StorePullError),
    #[error(transparent)]
    Outbound(#[from] crate::sync::store::StoreError),
    #[error(transparent)]
    Protocol(#[from] coven_protocol::store_commit::StoreProtocolError),
    #[error(transparent)]
    Storage(#[from] coven_protocol::objects::StorageError),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

impl DeviceJoinError {
    /// Classify an exact create-and-readback failure: an object that opened to
    /// other bytes is the caller's `mismatch` verdict; every other failure is
    /// the storage failure itself.
    pub(crate) fn readback(error: coven_protocol::objects::StorageError, mismatch: Self) -> Self {
        match error {
            coven_protocol::objects::StorageError::ReadbackMismatch(_) => mismatch,
            error => Self::Storage(error),
        }
    }
}

impl From<coven_protocol::store_commit::device_join_exchange::DeviceJoinExchangeError>
    for DeviceJoinError
{
    fn from(
        error: coven_protocol::store_commit::device_join_exchange::DeviceJoinExchangeError,
    ) -> Self {
        use coven_protocol::store_commit::device_join_exchange::DeviceJoinExchangeError as E;
        match error {
            E::InvalidSignature => DeviceJoinError::InvalidSignature,
            E::OfferMismatch => DeviceJoinError::OfferMismatch,
            E::ApprovalMismatch => DeviceJoinError::ApprovalMismatch,
            E::RegistrationRequestMismatch => DeviceJoinError::RegistrationRequestMismatch,
            E::AttemptMismatch => DeviceJoinError::AttemptMismatch,
            E::CleanupMismatch => DeviceJoinError::CleanupMismatch,
            E::DuplicateReservedSlot => DeviceJoinError::DuplicateReservedSlot,
            E::Provider(message) => DeviceJoinError::Provider(message),
            E::Storage(error) => DeviceJoinError::Storage(error),
            E::Protocol(error) => DeviceJoinError::Store(error.to_string()),
        }
    }
}

impl From<coven_database::DeviceJoinJournalError> for DeviceJoinError {
    fn from(error: coven_database::DeviceJoinJournalError) -> Self {
        use coven_database::DeviceJoinJournalError as E;
        match error {
            E::NonAdjacentJournalTransition => DeviceJoinError::NonAdjacentJournalTransition,
            E::JournalConflict => DeviceJoinError::JournalConflict,
            E::Serialization(error) => DeviceJoinError::Serialization(error),
            E::Database(error) => DeviceJoinError::Database(error),
        }
    }
}
