#[cfg(test)]
mod blob_content_hash_tests;
// Shared backoff math: the sync loop and blob engine's per-upload wait
// (`crate::blob::upload`) both count attempts in multiples of one base interval,
// so the formula is `pub(crate)`.
pub(crate) mod backoff;
pub(crate) mod cycle;
pub(crate) mod store;
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
pub(crate) mod sync_loop;
#[cfg(test)]
pub(crate) mod test_helpers;
#[cfg(test)]
pub(crate) mod test_owner_graph;

#[cfg(test)]
mod tests;
pub use error::SyncError;
pub use loop_policy::{SyncLoopAlerts, SyncLoopSuccess};
pub use status::DeviceActivity;
pub(crate) use store::Store;
pub use store::{BlobCacheError, BlobStream};
pub use store::{
    DeviceJoinAbandonment, DeviceJoinAction, DeviceJoinActivation, DeviceJoinApproval,
    DeviceJoinApprovalPolicy, DeviceJoinCancellation, DeviceJoinCleanupActivation,
    DeviceJoinCleanupReceipt, DeviceJoinDriveOutcome, DeviceJoinError, DeviceJoinJournalDatabase,
    DeviceJoinJournalRecord, DeviceJoinOffer, DeviceJoinOfferBundle, DeviceJoinProducer,
    DeviceJoinProducerWriteRevocation, DeviceJoinReadiness, DeviceJoinRole, DeviceJoinStatus,
    DeviceJoinTransportError, DeviceJoinTransportKind, DeviceJoinTransportParams,
    DeviceJoinTransportTiming, DeviceJoinWriteRevocationExecutor,
    DeviceProviderAccessAdministrator, DeviceProviderAccessRequest, DeviceProviderAdmission,
    DeviceProviderAdmissionApproval, DeviceProviderAdmissionCompletion, DeviceProviderReadiness,
    DeviceRegistrationRequest, JoinedStore, JoinerJoinClosure, JoinerJoinTerminal,
    ProviderAdminJoinClosure, ProviderAdminJoinTerminal, ProviderReadyDeviceBootstrap,
    ProviderWriteAuthorityRef, ProvisionalDeviceBootstrap,
};
pub use sync_loop::SyncLoopStatus;
