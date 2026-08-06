use coven_database::StoreDatabase;
use coven_foundation::store_dir::StoreDir;
use coven_keys::keys::UserKeypair;

use super::cycle::SyncCycleFailure;
use coven_protocol::store_commit::{CommitFrontier, StoreProtocolRoot};
use coven_storage::SyncStorage;
use coven_storage::{BlobPathScheme, CloudCipherAccess};

#[doc(hidden)]
pub mod blob;
mod circle_controls;
mod error;
mod host_write;
mod membership;
pub(crate) mod owner;
use owner::operations;
mod package_preparation;
#[cfg(not(any(test, feature = "test-utils")))]
mod protocol_root;
#[cfg(any(test, feature = "test-utils"))]
pub(super) mod protocol_root;
mod registration_object;
use registration_object::prepare_registration_object;

pub use blob::{BlobCacheError, BlobStream};
pub use circle_controls::CircleOperationError;
pub use circle_controls::CircleTransitionHistory;
pub use error::StoreError;
pub(crate) use error::StorePreparationError;
pub use host_write::HostWriteBlobStaging;
pub use membership::AnchoredChainError;
pub use membership::InviteError;
pub(crate) use membership::MembershipOpsError;
#[cfg(any(test, feature = "test-utils"))]
pub use owner::device_exclusion::StoreDeviceExclusionOperationInfo;
pub use owner::device_exclusion::{StoreDeviceExclusionError, StoreDeviceExclusionResult};
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
pub use owner::history::{ExcludedCandidateHeadObservation, MergeCandidateAbandonment};
#[cfg(test)]
pub(crate) use owner::operations::StoreOperationBatch;
pub use owner::operations::StoreOperationCommitPlan;
pub use owner::owner_promotion::OwnerPromotionError;

#[cfg(test)]
pub(crate) use owner::pull::HeldStoreCoordinate;
pub(crate) use owner::pull::{HeldStorePosition, VerifiedStoreDeviceHead};
pub use owner::pull::{LoadedCirclePackage, Readiness};
pub use owner::pull::{PullError, StorePullError, StorePullResult};
pub use owner::reclaim::StoreReclaimError;
pub use owner::reclaim::StoreReclaimResult;
pub(crate) use owner::snapshot::SnapshotCut;
#[doc(hidden)]
pub use owner::snapshot::{PreparedSnapshotBootstrap, SnapshotBlobReconcile, SnapshotError};
pub(crate) use owner::RestoringStore;
pub use owner::StoreAckError;
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
