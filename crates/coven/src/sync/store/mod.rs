use crate::database::StoreDatabase;
use crate::keys::UserKeypair;
use crate::store_dir::StoreDir;

use super::cycle::SyncCycleFailure;
use crate::protocol::store_commit::{CommitFrontier, StoreProtocolRoot};
use crate::storage::SyncStorage;
use crate::storage::{BlobPathScheme, CloudCipherAccess};

#[doc(hidden)]
pub(crate) mod blob;
mod circle_controls;
mod error;
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
pub(crate) use circle_controls::CircleOperationError;
#[cfg(test)]
pub(crate) use circle_controls::CircleTransitionHistory;
pub(crate) use error::StoreError;
pub(crate) use error::StorePreparationError;
#[cfg(test)]
pub(crate) use membership::AnchoredChainError;
pub(crate) use membership::{InviteError, MembershipOpsError};
#[cfg(test)]
pub(crate) use owner::device_exclusion::{
    StoreDeviceExclusionError, StoreDeviceExclusionOperationInfo, StoreDeviceExclusionResult,
};
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
pub(crate) use owner::device_join::{
    JoiningStore, PendingDeviceJoinAuthority, PendingDeviceJoinObservation,
};
pub use owner::device_join_transport::{
    DeviceJoinApproval, DeviceJoinApprovalPolicy, DeviceJoinDriveOutcome, DeviceJoinOfferBundle,
    DeviceJoinTransportError, DeviceJoinTransportKind, DeviceJoinTransportParams,
    DeviceJoinTransportTiming,
};
pub(crate) use owner::device_join_transport::{
    DeviceJoinRoles, DeviceJoinStep, DeviceJoinTransport,
};
#[cfg(test)]
pub(crate) use owner::history::prepare_merge_abandonment_history_summary_for_test as prepare_merge_abandonment_history_summary;
#[cfg(test)]
pub(crate) use owner::history::{ExcludedCandidateHeadObservation, MergeCandidateAbandonment};
#[cfg(test)]
pub(crate) use owner::operations::{StoreOperationBatch, StoreOperationCommitPlan};
#[cfg(test)]
pub(crate) use owner::owner_promotion::OwnerPromotionError;

#[cfg(test)]
pub(crate) use owner::pull::{HeldStoreCoordinate, LoadedCirclePackage, Readiness};
pub(crate) use owner::pull::{
    HeldStorePosition, PullError, StorePullError, StorePullResult, VerifiedStoreDeviceHead,
};
pub(crate) use owner::reclaim::StoreReclaimError;
#[cfg(test)]
pub(crate) use owner::reclaim::StoreReclaimResult;
pub(crate) use owner::snapshot::SnapshotCut;
#[doc(hidden)]
pub(crate) use owner::snapshot::{PreparedSnapshotBootstrap, SnapshotBlobReconcile, SnapshotError};
#[cfg(test)]
pub(crate) use owner::StoreAckError;
pub(crate) use owner::{
    AuthorizedWriterOperation, HistoryConstructionAuthority, HostWriteBlobStaging, Store,
    StoreInitializationError, StoreKeyrings,
};
#[cfg(test)]
pub(crate) use owner::{
    CirclePackageReadError, MergeHistorySuccessorEvidence, MergeOutboundAuthorization,
    PreparedMergeHistorySuccessor, StoreRestoreMembership, VerifiedMergeMembershipPrefix,
};
pub(crate) use owner::{RestoringStore, StoreRegistrationError};
