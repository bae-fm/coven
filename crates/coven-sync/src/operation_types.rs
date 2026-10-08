//! Application values for operations, members, circles and invitations (Appendix E).

use coven_crypto::{MemberId, SecretText};
use coven_format::store_log::MemberRole;
use coven_foundation::id_source::{CircleId, DeviceId, InviteId};
use std::time::SystemTime;

pub use coven_database::{AccessKeyToDelete, OperationId};
/// An operation failure retains the same typed causes as an ordinary sync call.
pub type OperationError = crate::SyncError;

/// The app purpose of an unfinished operation; maintenance stays internal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationKind {
    /// Publish the app's migrated schema and its audience snapshots.
    SchemaChange,
    /// Snapshot this device and reset one audience to that state.
    Reset,
    /// Reload snapshots at the app's request, keeping waiting writes.
    ReloadFromSnapshot,
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

/// App work stopped by a permanent failure, awaiting retry or discard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockedOperation {
    /// Local journal identity.
    pub id: OperationId,
    /// The work being performed.
    pub kind: OperationKind,
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
