#[cfg(test)]
mod blob_content_hash_tests;
// Shared backoff math: the sync loop and the blob engine's per-upload wait
// (`crate::blob::retry`) both count attempts in multiples of one base interval,
// so the formula is `pub(crate)`.
pub(crate) mod backoff;
pub mod cycle;
pub mod store;
// Exercises the register clock through `Database::hlc()`.
#[cfg(test)]
mod cycle_tests;
mod error;
#[cfg(test)]
mod exact_founder_graph_tests;
#[cfg(test)]
mod hlc_register_tests;
pub(crate) mod loop_policy;
#[cfg(test)]
mod pull_tests;
#[cfg(test)]
mod refresh_tests;
#[cfg(test)]
mod scoped_write_routing_tests;
pub(crate) mod status;
#[cfg(test)]
mod store_history_checkpoint_tests;
pub mod sync_loop;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_helpers;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_owner_graph;

#[cfg(test)]
mod tests;
pub use error::SyncError;
pub use loop_policy::{SyncLoopAlerts, SyncLoopSuccess};
pub use status::DeviceActivity;
pub use store::MemberAdmission;
pub use store::Store;
pub use store::{
    AdmittingDeviceJoinProgress, DeviceJoinAbandonment, DeviceJoinAction, DeviceJoinActivation,
    DeviceJoinApproval, DeviceJoinApprovalPolicy, DeviceJoinDriveOutcome, DeviceJoinError,
    DeviceJoinJournalDatabase, DeviceJoinJournalRecord, DeviceJoinOffer, DeviceJoinOfferBundle,
    DeviceJoinReadiness, DeviceJoinRole, DeviceJoinStatus, DeviceJoinTransportError,
    DeviceJoinTransportKind, DeviceJoinTransportParams, DeviceJoinTransportTiming,
    DeviceProviderAccessAdministrator, DeviceProviderAccessRequest,
    DeviceProviderAdmissionApproval, DeviceProviderAdmissionCompletion, DeviceProviderReadiness,
    DeviceRegistrationRequest, JoinedStore, JoiningDeviceJoinProgress,
    JoiningDeviceJoinProgressObserver, ProviderReadyDeviceBootstrap, ProvisionalDeviceBootstrap,
    SamePrincipalDeviceJoin,
};
pub use store::{
    BlobCacheError, BlobStream, EagerCacheFillError, EagerCacheFillProgress, EagerCacheFillStatus,
};
pub use sync_loop::{
    BlockedOperation, BlockedOperationId, RetryStuckReclaimError, SyncLoopFailure, SyncLoopStatus,
};
