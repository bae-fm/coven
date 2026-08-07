use coven_database::StoreDatabase;
use coven_foundation::store_dir::StoreDir;
use coven_keys::keys::UserKeypair;

use super::cycle::SyncCycleFailure;
use coven_protocol::store_commit::StoreProtocolRoot;
use coven_storage::SyncStorage;
use coven_storage::{BlobPathScheme, CloudCipherAccess};

pub(crate) mod acknowledgements;
#[doc(hidden)]
pub mod blob;
mod circle_controls;
mod commit_verification;
pub(crate) mod device_exclusion;
pub(crate) mod device_join;
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
#[cfg(test)]
pub(crate) use commit_verification::merge_history::prepare_merge_abandonment_history_summary;
pub use commit_verification::merge_history::{
    MergeHistorySuccessorEvidence, MergeOutboundAuthorization, PreparedMergeHistorySuccessor,
    VerifiedMergeMembershipPrefix,
};
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
pub(crate) use membership::MembershipOpsError;
pub use merge_conflict::{ExcludedCandidateHeadObservation, MergeCandidateAbandonment};
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
pub use owner::CirclePackageReadError;
pub(crate) use owner::StoreInitializationError;
pub use owner::StoreRegistrationError;
pub use owner::{AuthorizedWriterOperation, StoreCircleCommands, StoreWriterAuthorizationError};
pub use owner::{HistoryConstructionAuthority, Store, StoreKeyrings, StoreRestoreMembership};
pub(crate) use restore::RestoringStore;
pub(crate) use snapshots::SnapshotCut;
#[doc(hidden)]
pub use snapshots::{PreparedSnapshotBootstrap, SnapshotBlobReconcile, SnapshotError};
