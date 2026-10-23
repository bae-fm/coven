//! Application values for operations, members, circles and invitations (api.md).

use coven_crypto::SecretText;
use coven_format::{admin::MemberRole, codes::InviteDeliveryKind};
use coven_foundation::id_source::{InviteId, MemberId};
use std::time::{Duration, SystemTime};

pub use coven_database::{
    Circle, CircleMemberInfo, DeviceInfo, DeviceState, MemberInfo, OperationId,
};
/// An operation failure retains the same typed causes as an ordinary sync call.
pub type OperationError = crate::SyncError;

/// The purpose of an unfinished operation, including maintenance.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum OperationKind {
    /// Create the store's first slot.
    CreateStore,
    /// Set the store name.
    RenameStore,
    /// Register this installation.
    AddDevice,
    /// Set a member's role.
    SetRole,
    /// Replace provider access.
    SetAccess,
    /// Remove a member.
    RemoveMember,
    /// Remove an installation.
    RemoveDevice,
    /// Rename a circle.
    RenameCircle,
    /// Remove a circle member.
    RemoveCircleMember,
    /// Advance the floor and delete the covered prefix.
    RetainSequence,
    /// Delete unreferenced files after establishing retained-object protection.
    RetainFiles,
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
    /// Replace a member's signing and sealing pair: this member's own, with a
    /// candidate kept in identity custody, or a prepared pair as an admin (§10).
    ReplaceMemberKeys,
}

impl OperationKind {
    /// Coven's own work, which no app call retries or discards: a failed
    /// attempt retries under the shared backoff (§11, E5).
    pub(crate) fn coven_owned(self) -> bool {
        matches!(
            self,
            Self::WriteSnapshot
                | Self::RetainSequence
                | Self::RetainFiles
                | Self::RequiredReload
                | Self::RotateKey
                | Self::RevokeAccess
        )
    }
}

/// The admin's choice, fixed before the invite is shown (§13.2, §13.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InviteTarget {
    /// A new person: coven generates a fresh member id, added with this role.
    NewMember {
        /// The role the person receives on approval.
        role: MemberRole,
        /// The person's name as the admin typed it, a format `name`: the
        /// invite carries it to their device, whose app uses it for their
        /// profile.
        name: String,
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
}

/// How the invite reaches the person, which fixes how it is approved (§13.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InviteDelivery {
    /// Shown as a QR code to the person beside the admin; the inviting device
    /// approves the request on its own. Expires an hour after it is made.
    InPerson,
    /// Sent by message or email; the admin approves after comparing the
    /// confirmation number.
    Sent {
        /// How long the invite stays open.
        lifetime: InviteLifetime,
    },
}

impl InviteDelivery {
    /// The choice without its lifetime, as the code carries it.
    pub(crate) fn kind(self) -> InviteDeliveryKind {
        match self {
            Self::InPerson => InviteDeliveryKind::InPerson,
            Self::Sent { .. } => InviteDeliveryKind::Sent,
        }
    }

    /// How long after it is made the invite expires, by the inviting
    /// device's clock.
    pub(crate) fn lifetime(self) -> Duration {
        match self {
            Self::InPerson => InviteLifetime::Hour.duration(),
            Self::Sent { lifetime } => lifetime.duration(),
        }
    }
}

/// How long a sent invite stays open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InviteLifetime {
    /// One hour.
    Hour,
    /// One day.
    Day,
    /// One week.
    Week,
}

impl InviteLifetime {
    fn duration(self) -> Duration {
        Duration::from_secs(match self {
            Self::Hour => 3_600,
            Self::Day => 86_400,
            Self::Week => 7 * 86_400,
        })
    }
}

/// The invitation returned once its storage access has been granted.
pub struct Invite {
    /// The one-time request path's identity.
    pub id: InviteId,
    /// The stable member id the invite fixed.
    pub member: MemberId,
    /// The new person's name; `None` for a replacement.
    pub invitee: Option<String>,
    /// In person or sent.
    pub delivery: InviteDeliveryKind,
    /// The access the invite grants: the account shared with, or the id of
    /// the S3 key it carries, never its secret (§14.3).
    pub access: coven_format::MemberAccess,
    /// Code the app may show as a QR code. Contains secrets.
    pub code: String,
    /// The delivery's lifetime after creation, on the inviting device's clock.
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
    /// approval binds exactly this pair to `member` (§13.2).
    pub keys: coven_crypto::MemberPublicKeys,
    /// The device's name, covered by its signature.
    pub device_name: String,
    /// Account invited by the owner, absent on S3.
    pub provider_account_email: Option<String>,
    /// Computed from the invite id and `keys`; the requester's device shows
    /// the same number (format.md D11).
    pub confirmation: coven_crypto::ConfirmationNumber,
    pub(crate) record: Vec<u8>,
}
