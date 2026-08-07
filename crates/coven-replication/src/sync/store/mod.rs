use coven_database::StoreDatabase;
use coven_foundation::store_dir::StoreDir;
use coven_keys::keys::UserKeypair;

use super::cycle::SyncCycleFailure;
use coven_protocol::store_commit::{CommitFrontier, StoreProtocolRoot};
use coven_storage::SyncStorage;
use coven_storage::{BlobPathScheme, CloudCipherAccess};

pub(crate) mod acknowledgements;
#[doc(hidden)]
pub mod blob;
mod circle_controls;
pub(crate) mod device_exclusion;
mod error;
mod founder_creation;
mod host_write;
mod membership;
mod merge_conflict;
pub(crate) mod owner;
mod reclaim;
use owner::operations;
pub(crate) mod owner_role_promotion;
mod package_preparation;
#[cfg(not(any(test, feature = "test-utils")))]
mod protocol_root;
#[cfg(any(test, feature = "test-utils"))]
pub(super) mod protocol_root;
mod registration_object;
pub(crate) mod restore;
pub(crate) mod snapshots;
use registration_object::prepare_registration_object;

pub use acknowledgements::StoreAckError;
pub use blob::{BlobCacheError, BlobStream};
pub use circle_controls::CircleOperationError;
pub use circle_controls::CircleTransitionHistory;
#[cfg(any(test, feature = "test-utils"))]
pub use device_exclusion::StoreDeviceExclusionOperationInfo;
pub use device_exclusion::{StoreDeviceExclusionError, StoreDeviceExclusionResult};
pub use error::StoreError;
pub(crate) use error::StorePreparationError;
pub use host_write::HostWriteBlobStaging;
pub use membership::AnchoredChainError;
pub use membership::InviteError;
pub(crate) use membership::MembershipOpsError;
pub use merge_conflict::{ExcludedCandidateHeadObservation, MergeCandidateAbandonment};
pub(crate) use owner::device_join::JoiningStore;
pub use owner::device_join::{
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
pub use owner::device_join::{PendingDeviceJoinAuthority, PendingDeviceJoinObservation};
pub use owner::device_join_transport::{
    DeviceJoinApproval, DeviceJoinApprovalPolicy, DeviceJoinDriveOutcome, DeviceJoinOfferBundle,
    DeviceJoinTransportError, DeviceJoinTransportKind, DeviceJoinTransportParams,
    DeviceJoinTransportTiming,
};
pub use owner::device_join_transport::{DeviceJoinRoles, DeviceJoinStep, DeviceJoinTransport};
#[cfg(test)]
pub(crate) use owner::history::prepare_merge_abandonment_history_summary_for_test as prepare_merge_abandonment_history_summary;
#[cfg(test)]
pub(crate) use owner::operations::StoreOperationBatch;
pub use owner::operations::StoreOperationCommitPlan;
pub use owner_role_promotion::OwnerPromotionError;
pub use reclaim::StoreReclaimError;
pub use reclaim::StoreReclaimResult;

#[cfg(test)]
pub(crate) use owner::pull::HeldStoreCoordinate;
pub(crate) use owner::pull::{HeldStorePosition, VerifiedStoreDeviceHead};
pub use owner::pull::{LoadedCirclePackage, Readiness};
pub use owner::pull::{PullError, StorePullError, StorePullResult};
pub(crate) use owner::StoreInitializationError;
pub use owner::StoreRegistrationError;
pub use owner::{
    AuthorizedWriterOperation, StoreCircleCommands, StoreDeviceJoinTransport,
    StoreWriterAuthorizationError,
};
pub use owner::{
    CirclePackageReadError, MergeHistorySuccessorEvidence, MergeOutboundAuthorization,
    PreparedMergeHistorySuccessor, VerifiedMergeMembershipPrefix,
};
pub use owner::{HistoryConstructionAuthority, Store, StoreKeyrings, StoreRestoreMembership};
pub(crate) use restore::RestoringStore;
pub(crate) use snapshots::SnapshotCut;
#[doc(hidden)]
pub use snapshots::{PreparedSnapshotBootstrap, SnapshotBlobReconcile, SnapshotError};
