//! Application values for operations, members, circles and invitations (Appendix E).

use coven_crypto::SecretText;
use coven_format::store_log::MemberRole;
use coven_foundation::id_source::{InviteId, MemberId};
use std::time::SystemTime;

pub use coven_database::{
    AccessKeyToDelete, Circle, CircleMemberInfo, DeviceInfo, DeviceState, MemberInfo, OperationId,
};
/// An operation failure retains the same typed causes as an ordinary sync call.
pub type OperationError = crate::SyncError;

/// The purpose of an unfinished operation, including maintenance.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum OperationKind {
    /// Write and verify a maintenance snapshot.
    WriteSnapshot,
    /// Publish the app's migrated schema and its audience snapshots.
    SchemaChange,
    /// Snapshot this device and reset one audience to that state.
    Reset,
    /// Reload snapshots at the app's request, keeping waiting writes.
    ReloadFromSnapshot,
    /// Reload what sync requires: a changed schema or reset, a returning
    /// circle, joining or recovery.
    RequiredReload,
    /// Publish a fresh audience key and its sealed copies.
    RotateKey,
    /// Create a circle and its first key.
    CreateCircle,
    /// Seal a circle's history to a member.
    AddCircleMember,
    /// Delete a circle's rows and publish its deletion.
    DeleteCircle,
    /// Share storage and settle an invitation.
    Invite,
    /// Align one provider account's grants with members and open invitations.
    RevokeAccess,
    /// An S3 key awaiting deletion with admin access or explicit confirmation (§13).
    DeleteAccessKey,
    /// An admin desktop replacing a member's S3 key (§13).
    ReplaceAccessKey,
    /// Replace a member's signing and sealing pair: this member's own, with a
    /// candidate kept in identity custody, or a prepared pair as an admin (§9).
    ReplaceMemberKeys,
}

impl OperationKind {
    /// Coven's own work, which no app call retries or discards: a failed or
    /// dropped attempt retries under the shared backoff (§18, E5).
    pub(crate) fn coven_owned(self) -> bool {
        matches!(
            self,
            Self::WriteSnapshot
                | Self::RequiredReload
                | Self::RotateKey
                | Self::RevokeAccess
                | Self::DeleteAccessKey
        )
    }
}

/// A member's replaced S3 key (§13): the new key, which the member keeps
/// separately, and what happened to the old one.
#[derive(Debug)]
pub struct AccessKeyReplacement {
    /// The member's new key, delivered to their devices and recorded.
    pub key: coven_storage::S3MemberKey,
    /// The old key's deletion.
    pub old_key: OldKeyDeletion,
}

/// The old key after a replacement was recorded (§13).
#[derive(Debug)]
pub enum OldKeyDeletion {
    /// No deletion remains: coven deleted it with admin access and read it
    /// absent, or its record had already cleared.
    Deleted,
    /// Made in the console: delete it there and confirm (§13).
    Console {
        /// Its pending deletion.
        operation: OperationId,
    },
    /// Deleting it failed; its record stays for a later deletion.
    Failed {
        /// Its pending deletion.
        operation: OperationId,
        /// Why.
        error: crate::SyncError,
    },
}

/// The admin's choice, fixed before the invite is shown (§12.2, §12.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InviteTarget {
    /// A new person: coven generates a fresh member id, added with this role.
    NewMember {
        /// The role the person receives on approval.
        role: MemberRole,
    },
    /// An existing active member who lost every device: approval binds the
    /// requester's new keys to this id, keeping its role, circles and devices.
    ReplaceMemberKeys {
        /// The member whose keys the approval replaces.
        member: MemberId,
    },
}

/// How the invited person reaches the store's provider.
#[derive(Clone)]
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
    /// S3: coven makes the invited person's key with the storage account's
    /// admin access, held by this invite's operation until the key is made (§4).
    S3AdminAccess(std::sync::Arc<dyn coven_storage::AccessKeyAdmin>),
}

/// The invitation returned once its storage access has been granted.
pub struct Invite {
    /// The one-time request path's identity.
    pub id: InviteId,
    /// The stable member id the invite fixed.
    pub member: MemberId,
    /// Code the app may show as a QR code. Contains secrets.
    pub code: String,
    /// One day after creation on the inviting device's clock.
    pub expires_at: SystemTime,
}

/// A checked request, presented with the account from its local invitation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinRequest {
    /// Invitation being answered.
    pub invite: InviteId,
    /// The stable member id the invite fixed and the request repeats.
    pub member: MemberId,
    /// The fresh public pair the request's signature proves it holds;
    /// approval binds exactly this pair to `member` (§12.2).
    pub keys: coven_crypto::MemberPublicKeys,
    /// The device's name, covered by its signature.
    pub device_name: String,
    /// Account invited by the owner, absent on S3.
    pub provider_account_email: Option<String>,
    pub(crate) record: Vec<u8>,
}
