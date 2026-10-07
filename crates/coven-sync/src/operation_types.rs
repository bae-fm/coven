//! Application values for operations, members, circles and invitations (Appendix E).

use coven_crypto::{MemberId, SecretText};
use coven_format::store_log::MemberRole;
use coven_foundation::id_source::{CircleId, DeviceId, InviteId};
use std::time::SystemTime;

pub use coven_database::{AccessKeyToDelete, OperationId};
/// An operation failure retains the same typed causes as an ordinary sync call.
pub type OperationError = crate::SyncError;

/// The work retained in an unfinished journal row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationKind {
    /// Run the app migration batch and publish every readable audience.
    MigrateSchema,
    /// Snapshot this device and reset one audience to that state.
    Reset,
    /// Write, upload and retain one audience snapshot.
    WriteSnapshot,
    /// Publish a migrated audience snapshot and its schema raise.
    RaiseSchema,
    /// Publish an audience snapshot in coven's current format and raise it.
    RaiseFormat,
    /// Replace readable audiences and replay waiting writes atomically.
    ReloadSnapshots,
    /// Delete objects released by snapshot coverage.
    Retention,
    /// Remove a member and rotate every affected key.
    RemoveMember,
    /// Create a circle and its first key.
    CreateCircle,
    /// Seal a circle's history to a member.
    AddCircleMember,
    /// Replace a circle's key after removing a member.
    RemoveCircleMember,
    /// Delete a circle's rows and publish its deletion.
    DeleteCircle,
    /// Share storage and settle an invitation.
    Invite,
    /// The owner's device revokes recorded access after removal or a later access entry.
    RevokeAccess,
}

/// Who initiated the retained work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartedBy {
    /// The app method that started it.
    AppCall(String),
    /// Work caused by applying a store-log entry.
    Coven,
}

/// An unfinished operation stopped by a permanent failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockedOperation {
    /// Local journal identity.
    pub id: OperationId,
    /// The work being performed.
    pub kind: OperationKind,
    /// Last committed step.
    pub last_step: u32,
    /// The initiator.
    pub started_by: StartedBy,
    /// The failure presented when no app call is waiting.
    pub failure: String,
}

/// A live member and the devices still belonging to them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberInfo {
    /// Signing identity.
    pub id: MemberId,
    /// Current store role.
    pub role: MemberRole,
    /// Active devices.
    pub devices: Vec<DeviceId>,
    /// Whether this is the local member.
    pub is_self: bool,
}

/// A circle the local member belongs to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Circle {
    /// Circle identity.
    pub id: CircleId,
    /// Current name.
    pub name: String,
}

/// An active store member in a circle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CircleMemberInfo {
    /// Signing identity.
    pub member: MemberId,
    /// Whether this is the local member.
    pub is_self: bool,
}

/// Why a circle call failed.
#[derive(Debug, thiserror::Error)]
pub enum CircleError {
    /// The caller is outside the circle.
    #[error("not a member of circle {0}")]
    NotMember(CircleId),
    /// The circle has been deleted or does not exist.
    #[error("circle {0} is deleted")]
    Deleted(CircleId),
    /// The target is not an active store member.
    #[error("{0} is not a store member")]
    NotStoreMember(MemberId),
    /// A database, storage or cryptographic step failed.
    #[error(transparent)]
    Sync(crate::SyncError),
}

impl From<crate::SyncError> for CircleError {
    fn from(error: crate::SyncError) -> Self {
        match error {
            crate::SyncError::CircleNotMember(id) => Self::NotMember(id),
            crate::SyncError::CircleDeleted(id) => Self::Deleted(id),
            crate::SyncError::NotStoreMember(id) => Self::NotStoreMember(id),
            error => Self::Sync(error),
        }
    }
}

/// How the invited person reaches the store's provider.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum InviteAccess {
    /// Share with the named provider account.
    ProviderAccount {
        /// The account's email address.
        email: String,
    },
    /// Credentials created in the S3 console for this invite.
    S3AccessKey {
        /// Public key identifier.
        access_key_id: String,
        /// The key's secret, carried only in the invitation.
        secret_access_key: SecretText,
    },
}

/// The invitation returned once its storage access has been granted.
pub struct Invite {
    /// The one-time request path's identity.
    pub id: InviteId,
    /// Code the app may show as a QR code. Contains secrets.
    pub code: String,
    /// Role the person will receive on approval.
    pub role: MemberRole,
    /// One day after creation on the inviting device's clock.
    pub expires_at: SystemTime,
}

/// A checked request, presented with the account from its local invitation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinRequest {
    /// Invitation being answered.
    pub invite: InviteId,
    /// The new member's signing key.
    pub member: MemberId,
    /// The device's name, covered by its signature.
    pub device_name: String,
    /// Account invited by the owner, absent on S3.
    pub provider_account_email: Option<String>,
    pub(crate) record: Vec<u8>,
}
