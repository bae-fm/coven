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
    #[error("pending join payload hash differs from the exact transferred payload")]
    PendingTransferHashMismatch,
    #[error("device join reserved slots are not pairwise distinct")]
    DuplicateReservedSlot,
    #[error("device join requires an existing Member identity")]
    MemberNotEligible,
    #[error("provider operation failed: {0}")]
    Provider(String),
    #[error("Store device join state: {0}")]
    Store(String),
    #[error("device join requires an activated local Store device")]
    ActiveDeviceRequired,
    #[error("device join requires the active local Owner authority")]
    OwnerAuthorityRequired,
    #[error("device join requires the selected effective provider administrator")]
    ProviderAdministratorRequired,
    #[error("device join requires resolved Store membership")]
    MembershipConflict,
    #[error("device join requires the provider's exact-slot adapter")]
    ExactSlotStorageRequired,
    #[error("device join attempt cut does not include its provider-access activation")]
    ApprovalActivationMissing,
    #[error("device join activation is not materialized in the installed Store database")]
    ActivationNotMaterialized,
    #[error(transparent)]
    Object(#[from] crate::sync::store_objects::StoreObjectError),
    #[error(transparent)]
    Registration(#[from] crate::sync::store::StoreRegistrationError),
    #[error(transparent)]
    Pull(#[from] crate::sync::store::owner::pull::StorePullError),
    #[error(transparent)]
    Outbound(#[from] crate::sync::store::StoreError),
    #[error(transparent)]
    Protocol(#[from] crate::sync::store_commit::StoreProtocolError),
    #[error(transparent)]
    Storage(#[from] crate::sync::storage::StorageError),
    #[error(transparent)]
    Database(#[from] rusqlite::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}
