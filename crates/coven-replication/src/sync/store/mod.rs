use coven_database::StoreDatabase;
use coven_foundation::store_dir::StoreDir;
use coven_keys::keys::UserKeypair;

use super::cycle::SyncCycleFailure;
use coven_protocol::store_commit::StoreProtocolRoot;
use coven_storage::BlobPathScheme;
use coven_storage::CloudSyncObjectStorage;

pub(crate) mod acknowledgements;
pub(crate) mod authorization;
#[doc(hidden)]
pub mod blob;
pub(crate) mod circles;
pub(crate) mod commit_publication;
mod commit_verification;
pub(crate) mod device_exclusion;
pub(crate) mod device_join;
mod error;
mod founder_creation;
mod host_write;
mod membership;
mod merge_conflict;
mod reclaim;
use commit_publication::operation::commit_plan;
pub(crate) mod owner_role_promotion;
mod package_preparation;
pub(super) mod protocol_root;
pub(crate) mod pull;
mod registration_object;
pub(crate) mod restore;
pub(crate) mod snapshots;
use registration_object::prepare_registration_object;

pub use acknowledgements::StoreAckError;
pub use blob::{BlobCacheError, BlobStream};
pub use circles::CircleOperationError;
#[cfg(test)]
pub(crate) use commit_publication::operation::commit_plan::StoreOperationBatch;
pub use commit_publication::operation::commit_plan::StoreOperationCommitPlan;
pub use commit_verification::merge_history::{
    MergeHistorySuccessorEvidence, MergeOutboundAuthorization, PreparedMergeHistorySuccessor,
    VerifiedMergeMembershipPrefix,
};
pub use coven_protocol::circle_journal::CircleTransitionHistory;
#[cfg(any(test, feature = "test-utils"))]
pub use device_exclusion::StoreDeviceExclusionOperationInfo;
pub use device_exclusion::{StoreDeviceExclusionError, StoreDeviceExclusionResult};
pub use device_join::transport::StoreDeviceJoinTransport;
pub use device_join::transport::{
    DeviceJoinApproval, DeviceJoinApprovalPolicy, DeviceJoinDriveOutcome, DeviceJoinOfferBundle,
    DeviceJoinTransportError, DeviceJoinTransportKind, DeviceJoinTransportParams,
    DeviceJoinTransportTiming,
};
pub use device_join::transport::{DeviceJoinRoles, DeviceJoinStep, DeviceJoinTransport};
pub(crate) use device_join::JoiningStore;
pub use device_join::{
    DeviceJoinAbandonment, DeviceJoinAction, DeviceJoinActivation, DeviceJoinCancellation,
    DeviceJoinCleanupActivation, DeviceJoinCleanupReceipt, DeviceJoinError,
    DeviceJoinJournalDatabase, DeviceJoinJournalRecord, DeviceJoinOffer, DeviceJoinProducer,
    DeviceJoinProducerWriteRevocation, DeviceJoinReadiness, DeviceJoinRole, DeviceJoinStatus,
    DeviceJoinWriteRevocationExecutor, DeviceProviderAccessAdministrator,
    DeviceProviderAccessRequest, DeviceProviderAdmission, DeviceProviderAdmissionApproval,
    DeviceProviderAdmissionCompletion, DeviceProviderReadiness, DeviceRegistrationRequest,
    JoinedStore, JoinerJoinClosure, JoinerJoinTerminal, ProviderAdminJoinClosure,
    ProviderAdminJoinTerminal, ProviderReadyDeviceBootstrap, ProviderWriteAuthorityRef,
    ProvisionalDeviceBootstrap,
};
pub use device_join::{PendingDeviceJoinAuthority, PendingDeviceJoinObservation};
pub use error::StoreError;
pub(crate) use error::StorePreparationError;
pub use host_write::HostWriteBlobStaging;
pub use membership::AnchoredChainError;
pub use membership::InviteError;
pub use membership::MemberInvitation;
pub(crate) use membership::MembershipOpsError;
pub use merge_conflict::{ExcludedCandidateHeadObservation, MergeCandidateAbandonment};
pub use owner_role_promotion::OwnerPromotionError;
pub use reclaim::StoreReclaimError;
pub use reclaim::StoreReclaimResult;

pub use authorization::StoreInitializationError;
pub use authorization::StoreRegistrationError;
pub use authorization::{
    HistoryConstructionAuthority, Store, StoreKeyrings, StoreRestoreMembership,
};
pub use circles::CirclePackageReadError;
pub use circles::StoreCircleCommands;
pub use commit_publication::{AuthorizedWriterOperation, StoreWriterAuthorizationError};
pub use founder_creation::{
    FounderObjectDeleteError, FounderPublicationRollback, FounderRollbackError,
};
#[cfg(test)]
pub(crate) use pull::HeldStoreCoordinate;
pub(crate) use pull::{HeldStorePosition, VerifiedStoreDeviceHead};
pub use pull::{LoadedCirclePackage, Readiness};
pub use pull::{PullError, StorePullError, StorePullResult};
pub(crate) use restore::RestoringStore;
pub(crate) use snapshots::SnapshotCut;
#[doc(hidden)]
pub use snapshots::{
    PreparedSnapshotBootstrap, SnapshotBlobReconcile, SnapshotBlobReconcileError, SnapshotError,
    SnapshotSpoolCleanupError,
};
