//! Durable recovery authority: the exact continuation or Owner-recovery state
//! a restore proves before it may rebuild a device.

use serde::{Deserialize, Serialize};

/// The closed authority a restore operation may exercise.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RestoreAuthority {
    /// Continue one exact, already-activated Store device.
    ActivatedContinuation(ActivatedContinuation),
    /// Recover an Owner identity at its exact root-anchored recovery cursor.
    OwnerRecovery(OwnerRecoveryAuthority),
}

/// Exact durable state required to continue an activated Store device.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivatedContinuation {
    pub identity_signing_secret: String,
    pub device_signing_secret: String,
    pub registration: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    pub registration_bytes: Vec<u8>,
    pub registration_prepared: crate::protocol::objects::PreparedExactObject,
    pub initial_ack: crate::protocol::store_commit::StoreAckRef,
    pub initial_ack_bytes: Vec<u8>,
    pub initial_ack_prepared: crate::protocol::objects::PreparedExactObject,
    pub activation: crate::protocol::store_commit::StoreDeviceRegistrationActivation,
    pub latest_ack: crate::protocol::store_commit::StoreAckRef,
    pub latest_snapshot: Option<crate::protocol::store_commit::StoreSnapshotRef>,
    pub latest_position: Option<crate::protocol::store_commit::StoreBatchCommitRef>,
}

/// Exact Owner grant and recovery-stream authority used to create a replacement
/// device when no activated device continuation survives.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerRecoveryAuthority {
    pub owner_identity_secret: String,
    pub owner_grant: crate::protocol::membership::MembershipGrantId,
    pub recovery: crate::protocol::store_commit::OwnerRecoveryCursor,
    pub published_at: String,
}

impl std::fmt::Debug for RestoreAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ActivatedContinuation(value) => f
                .debug_struct("ActivatedContinuation")
                .field("identity_signing_secret", &"<redacted>")
                .field("device_signing_secret", &"<redacted>")
                .field("registration", &value.registration)
                .field("initial_ack", &value.initial_ack)
                .field("activation", &value.activation)
                .field("latest_ack", &value.latest_ack)
                .field("latest_snapshot", &value.latest_snapshot)
                .field("latest_position", &value.latest_position)
                .finish(),
            Self::OwnerRecovery(value) => f
                .debug_struct("OwnerRecovery")
                .field("owner_identity_secret", &"<redacted>")
                .field("owner_grant", &value.owner_grant)
                .field("recovery", &value.recovery)
                .finish(),
        }
    }
}
