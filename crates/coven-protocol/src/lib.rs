//! Protocol: Coven's deterministic model of replicated state — signed values,
//! their parsing, and their validation.
//!
//! Everything here is a pure function of its inputs. No SQLite, no storage
//! provider, no network, no clock reads of its own: a commit, a membership
//! entry, a circle operation, or a provider proof either parses and validates
//! or it does not, identically on every device. That is what lets two devices
//! that never met agree on what happened.
//!
//! `write` lives here because a write's identity and publication status are
//! stated in protocol terms — a `WriteId` names a host transaction, and a
//! `PublishedPosition` names the commit that made it visible to peers.

pub mod audience_package;
pub mod blob;
pub mod causal_grants;
pub mod circle;
pub mod circle_activation;
#[cfg(any(test, feature = "test-utils"))]
pub mod circle_activation_test_fixtures;
pub mod circle_control;
pub mod circle_journal;
pub mod circle_roster;
#[cfg(any(test, feature = "test-utils"))]
pub mod circle_test_fixtures;
pub mod device_exclusion_journal;
pub mod hlc;
pub mod membership;
pub mod membership_mutation;
pub mod objects;
pub mod owner_promotion_journal;
pub mod prepared_commit;
pub mod provider;
pub mod reclaim;
pub mod recovery;
pub mod remote_object;
pub mod store_commit;
pub mod store_creation;
pub mod synced_schema;
pub mod wrapped_store_key;
pub mod write;

pub use objects::{
    AwsPrincipal, CloudKitEnvironment, GoogleDriveCorpus, ProviderDeviceBinding,
    ProviderPrincipalId, ResolvedProviderBinding, S3EndpointBinding, StoreProviderBinding,
};

pub use circle::{
    Audience, Circle, CircleCloseParticipant, CircleCloseSettlement, CircleCloseStatus,
    CircleControlCoord, CircleEpochCloseId, CircleId, CircleInfo, CircleMemberInfo,
    CircleOperationBlock, CircleOperationId, CircleOperationInfo, CircleOperationKind,
    CircleOperationState, CircleRole, CircleState,
};
pub use membership::{
    MemberInfo, MemberRole, MembershipConflictChoice, MembershipConflictInfo, MembershipCoord,
};
pub use provider::{
    CloudKitAcceptedShare, CrossPrincipalProbeReceipt, ExactSlotProbeReceipt,
    ProviderAccessLocator, ProviderAccessWithdrawal, ProviderAdminChange, ProviderAdminGrantId,
    ProviderAdminGrantRecord, ProviderAdminMembershipChange, ProviderAdminState,
    ProviderCapabilityProof, ProviderProbeId, StoreMemberProviderAccessGrantRef,
};
pub use store_commit::{
    CommitFrontier, DeviceJoinAttemptId, ObjectHash, StoreBatchCommitRef, StoreCommitCoord,
    StoreCommitOrder, StoreDeviceId,
};
