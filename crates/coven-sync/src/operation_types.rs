//! Application values for operations, members, circles and invitations (api.md).

use coven_crypto::SecretText;
use coven_format::admin::MemberRole;
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
    /// Record how the store's first admin reaches storage, which fixes how
    /// every member does (§10.3).
    SetUpStorage,
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
    /// Send an older audience key to a current member whose sealing key has
    /// no copy of it.
    SendKeyCopy,
    /// Create a circle and its first key.
    CreateCircle,
    /// Seal a circle's history to a member.
    AddCircleMember,
    /// Publish a circle's deletion; its rows are taken out from that slot
    /// (§15.9).
    DeleteCircle,
    /// Share storage and settle an invitation.
    Invite,
    /// Look for a pairing's request, put its handover or marker in the
    /// request's place (§13.2).
    Pairing,
    /// Align one provider account's grants with members and open invitations.
    RevokeAccess,
    /// Replace a member's signing and sealing pair: this member's own, with a
    /// candidate kept in identity custody, or a prepared pair as an admin (§10).
    ReplaceMemberKeys,
    /// Set up storage on this device: check the provider, then create the
    /// store or reconnect to it (§11.6, §20.8).
    StorageSetup,
}

impl OperationKind {
    /// The purpose of the operation that claims `change`.
    pub(crate) fn of(change: &coven_format::admin::StoreChange) -> Self {
        use coven_format::admin::StoreChange;
        use OperationKind as K;
        match change {
            StoreChange::CreateStore { .. } => K::CreateStore,
            StoreChange::SetUpStorage { .. } => K::SetUpStorage,
            StoreChange::RenameStore { .. } => K::RenameStore,
            StoreChange::AdmitMember { .. }
            | StoreChange::RecoverMember { .. }
            | StoreChange::AddInviteShare { .. }
            | StoreChange::EndInviteShare { .. } => K::Invite,
            StoreChange::RemoveMember { .. } => K::RemoveMember,
            StoreChange::ChangeRole { .. } => K::SetRole,
            StoreChange::SetAccess { .. } => K::SetAccess,
            StoreChange::ReplaceMemberKeys { .. } => K::ReplaceMemberKeys,
            StoreChange::AddDevice { .. } => K::AddDevice,
            StoreChange::RemoveDevice { .. } => K::RemoveDevice,
            StoreChange::CreateCircle { .. } => K::CreateCircle,
            StoreChange::RenameCircle { .. } => K::RenameCircle,
            StoreChange::DeleteCircle { .. } => K::DeleteCircle,
            StoreChange::AddCircleMember { .. } => K::AddCircleMember,
            StoreChange::RemoveCircleMember { .. } => K::RemoveCircleMember,
            StoreChange::RotateKey { .. } => K::RotateKey,
            StoreChange::RaiseSchema { .. } => K::SchemaChange,
            StoreChange::Reset { .. } => K::Reset,
        }
    }

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
                | Self::SendKeyCopy
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
    /// The S3 key this device connects with, which coven reads from its
    /// custody, so the secret never reaches the app (§14.3).
    HeldS3AccessKey,
}

/// How long an invite stays open after it is made, by the inviting
/// device's clock (§13.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InviteLifetime {
    /// Seven days.
    SevenDays,
    /// Thirty days.
    ThirtyDays,
}

impl InviteLifetime {
    pub(crate) fn duration(self) -> Duration {
        Duration::from_secs(match self {
            Self::SevenDays => 7 * 86_400,
            Self::ThirtyDays => 30 * 86_400,
        })
    }
}

/// One of this device's open invites (§13.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Invite {
    /// The one-time request path's identity.
    pub id: InviteId,
    /// The invite's operation, which names its pending record in
    /// `coven_pending` while its check or its grant waits or is refused.
    pub operation: coven_database::OperationId,
    /// The stable member id the invite fixed.
    pub member: MemberId,
    /// Whether the invite adds a new person or recovers a member.
    pub kind: coven_format::codes::InviteKind,
    /// The access the invite grants: the account shared with, or the id of
    /// the S3 key it carries, never its secret (§14.3).
    pub access: coven_format::MemberAccess,
    /// The invite's lifetime after creation, on the inviting device's clock.
    pub expires_at: SystemTime,
    /// Where the invite is: granting its access, waiting for its request or
    /// the app's decision, or approving its request.
    pub stage: InviteStage,
}

/// Where one of this device's open invites is (§13.2).
#[derive(Clone, PartialEq, Eq)]
pub enum InviteStage {
    /// The app's write made the invite. A pass checks it against the
    /// history it catches up with and grants its storage access, and then
    /// it shows its code. A check that refuses it, or a failed grant, is
    /// its pending record under its operation; cancelling ends it.
    Granting,
    /// The code is shown, and the request, once one has arrived and been
    /// checked, waits for the app to approve or decline it.
    Waiting {
        /// Code the app may show as a QR code, again whenever the admin
        /// asks. Contains secrets.
        code: String,
        /// The checked request, once one has arrived.
        request: Option<JoinRequest>,
    },
    /// Approval of this request has begun, and finishes without the app:
    /// approving, declining or cancelling the invite fail with
    /// `InvitationChanged`. The approval is the app's own, and
    /// `coven_operations` lists its binding until the app acknowledges the
    /// outcome.
    Approving {
        /// The code first shown, which the joining device scanned.
        /// Contains secrets.
        code: String,
        /// The request being approved.
        request: JoinRequest,
        /// The operation that claims the request's copies and binds its
        /// pair.
        binding: coven_database::OperationId,
    },
}

impl std::fmt::Debug for InviteStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Granting => f.write_str("Granting"),
            Self::Waiting { request, .. } => f
                .debug_struct("Waiting")
                .field("code", &"[REDACTED]")
                .field("request", request)
                .finish(),
            Self::Approving { request, binding, .. } => f
                .debug_struct("Approving")
                .field("code", &"[REDACTED]")
                .field("request", request)
                .field("binding", binding)
                .finish(),
        }
    }
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
    /// Who asks: a new person with the name they typed, or a recovery. It
    /// matches the invite's kind (§13.2).
    pub joiner: crate::Joiner,
    /// Account invited by the owner, absent on S3.
    pub provider_account_email: Option<String>,
    /// Computed from the invite id and `keys`; the requester's device shows
    /// the same number (format.md D11).
    pub confirmation: coven_crypto::ConfirmationNumber,
    pub(crate) record: Vec<u8>,
}
