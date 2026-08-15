//! End-to-end encrypted, multi-writer SQLite sync over host-selected storage.
//!
//! The host owns its SQLite schema and domain. Coven owns the connection,
//! captures host writes, stores protocol state, and synchronizes rows and blobs.
//! Hosts use the crate-root API; implementation modules remain private.
//!
//! ```compile_fail
//! let _ = coven::store_sync::StoreSync::connect;
//! ```

pub(crate) mod circles;
mod cloud_home_setup;
pub(crate) mod coven;
mod handle;
mod live_query;
#[cfg(test)]
mod live_query_tests;
mod read_handle;
pub(crate) mod store_blobs;
pub(crate) mod store_circles;
pub(crate) mod store_cloud_storage;
pub(crate) mod store_joining;
pub(crate) mod store_membership;
pub(crate) mod store_recovery;
pub(crate) mod store_rows;
pub(crate) mod store_security;
pub(crate) mod store_sync;

#[cfg(test)]
mod blob_facade_tests;
#[cfg(test)]
mod store_key_ownership_tests;

pub use coven_database::rusqlite;

pub use circles::{CircleError, Circles};
pub use cloud_home_setup::{
    CloudHomeRollbackError, CloudHomeSetupError, CloudHomeSetupFailure, CloudHomeUnlockError,
    ConnectedCloudHome,
};
pub use coven::{Coven, CovenBuilder, CovenConfig, CovenError, CovenResult};
pub use coven_database::{
    BlobFileFailure, BlobFileFailures, DbError, ExternalBlob, MakeRemoteProgress, QueuedDelete,
    QueuedUpload, SqlContext, SqlReadContext, WriteBatch,
};
pub use coven_database::{Migration, MigrationContext, MigrationError, MigrationStep};
pub use coven_domain::joining::{
    abandon_join_request, decode_invite_code_info, decode_join_request, generate_join_request,
    BootstrapError, DeviceJoinClient, InviteCodeInfo, JoinCodeError, JoinRequestCode,
};
pub use coven_domain::joining::{
    close_scanned_invite_join, join_with_scanned_invite, DeviceJoinInvite,
    DeviceJoinTransportOutcome,
};
pub use coven_domain::restoration::{
    decode_restore_code_info, restore_from_cloud, restore_from_code, ActivatedContinuation,
    OwnerRecoveryAuthority, RestoreAuthority, RestoreCodeError, RestoreCodeInfo, RestoreSource,
};
pub use coven_foundation::atomic_file::{write_atomic, WriteError};
pub use coven_foundation::changeset::{ChangeOp, RowChange};
#[cfg(any(test, feature = "test-utils"))]
pub use coven_foundation::clock::FixedClock;
pub use coven_foundation::clock::{Clock, ClockRef, SystemClock};
pub use coven_foundation::config::{
    CloudHomeConfig, CloudProvider, Config, ConfigError, ExactUploadVerification, HomeStorage,
};
#[cfg(any(test, feature = "test-utils"))]
pub use coven_foundation::id_provider::SequentialIdProvider;
pub use coven_foundation::id_provider::{IdProvider, IdRef, UuidProvider};
pub use coven_foundation::store_dir::{StoreDir, StoreLayout};
pub use coven_keys::custody::{KeyCustody, Passphrase};
#[cfg(any(test, feature = "test-utils"))]
pub use coven_keys::encryption::EncryptionService;
pub use coven_keys::encryption::{
    EncryptionError, KeyFingerprint, MasterKeyring, SealError, CHUNK_SIZE,
};
pub use coven_keys::identity_custody::IdentityCustody;
#[cfg(any(test, feature = "test-utils"))]
pub use coven_keys::keys::test_keyring::install_for_service as install_test_keyring_service;
#[cfg(all(
    any(test, feature = "test-utils"),
    any(target_os = "macos", target_os = "ios")
))]
pub use coven_keys::keys::{apple_keyring_entry_facts_for_test, AppleKeyringEntryFacts};
pub use coven_keys::keys::{
    keyring_service, set_keyring_service, CloudHomeCredentials, DeviceIdentityCustody,
    IdentityError, KeyError, MasterKeyCustody, MasterKeyError, StoreKeys, UserKeypair,
};
pub use coven_protocol::blob::{
    content_hash, BlobRef, BlobReplacement, BlobScope, BlobTransitionObserver, CacheFill,
    Provenance, RowBlobAuthority, RowBlobRef,
};
pub use coven_protocol::hlc::Timestamp;
pub use coven_protocol::objects::{
    ExactObjectRef, ObjectSlot, PhysicalObjectLocator, StorageError,
};
pub use coven_protocol::synced_schema::{BlobDecl, RowIdentity, SyncedTable};
pub use coven_protocol::write::{
    AffectedRow, PendingWrite, PublishedPosition, WriteBlock, WriteId, WriteReceipt,
    WriteResolution, WriteRetractionWitness, WriteStatus,
};
pub use coven_protocol::{
    Audience, Circle, CircleCloseParticipant, CircleCloseSettlement, CircleCloseStatus,
    CircleControlCoord, CircleEpochCloseId, CircleId, CircleMemberInfo, CircleOperationBlock,
    CircleOperationId, CircleOperationInfo, CircleOperationKind, CircleOperationState, CircleRole,
    CircleState, CommitFrontier, CrossPrincipalProbeReceipt, DeviceJoinAttemptId,
    DeviceJoinAttemptRef, ExactSlotProbeReceipt, MemberInfo, MemberRole, MembershipConflictChoice,
    MembershipConflictInfo, ObjectHash, ProviderAccessLocator, ProviderAccessWithdrawal,
    ProviderAdminGrantId, ProviderAdminGrantRecord, ProviderCapabilityProof, StoreBatchCommitRef,
    StoreCommitCoord, StoreDeviceId,
};
pub use coven_protocol::{
    AwsPrincipal, CloudKitEnvironment, GoogleDriveCorpus, ProviderDeviceBinding,
    ProviderPrincipalId, ResolvedProviderBinding, S3EndpointBinding, StoreProviderBinding,
};
pub use coven_replication::blob::{
    DrainOutcome, UploadFailure, UploadFailureCause, UploadFailures,
};
pub use coven_replication::blob::{MakeLocalError, MakeRemoteError};
pub use coven_replication::sync::{
    BlobCacheError, BlobStream, DeviceActivity, DeviceJoinAbandonment, DeviceJoinAction,
    DeviceJoinActivation, DeviceJoinApproval, DeviceJoinApprovalPolicy, DeviceJoinCancellation,
    DeviceJoinCleanupActivation, DeviceJoinCleanupReceipt, DeviceJoinDriveOutcome, DeviceJoinError,
    DeviceJoinJournalDatabase, DeviceJoinJournalRecord, DeviceJoinOffer, DeviceJoinOfferBundle,
    DeviceJoinProducer, DeviceJoinProducerWriteRevocation, DeviceJoinReadiness, DeviceJoinRole,
    DeviceJoinStatus, DeviceJoinTransportError, DeviceJoinTransportKind, DeviceJoinTransportParams,
    DeviceJoinTransportTiming, DeviceJoinWriteRevocationExecutor,
    DeviceProviderAccessAdministrator, DeviceProviderAccessRequest, DeviceProviderAdmission,
    DeviceProviderAdmissionApproval, DeviceProviderAdmissionCompletion, DeviceProviderReadiness,
    DeviceRegistrationRequest, JoinedStore, JoinerJoinClosure, JoinerJoinTerminal,
    ProviderAdminJoinClosure, ProviderAdminJoinTerminal, ProviderReadyDeviceBootstrap,
    ProviderWriteAuthorityRef, ProvisionalDeviceBootstrap, SyncError, SyncLoopAlerts,
    SyncLoopStatus, SyncLoopSuccess,
};
#[cfg(feature = "oauth-providers")]
pub use coven_storage::fetch_account_email;
#[cfg(feature = "oauth-providers")]
pub use coven_storage::oauth::OAuthError;
#[cfg(feature = "oauth-providers")]
pub use coven_storage::oauth::{AuthorizeRequest, OAuthClientCreds, OAuthClientCredsError};
pub use coven_storage::oauth::{OAuthClients, OAuthTokens};
#[cfg(feature = "test-utils")]
pub use coven_storage::CloudCipher;
#[cfg(any(test, feature = "test-utils"))]
pub use coven_storage::InMemoryCloudHome;
pub use coven_storage::{
    write_cloud_object_stream, BlobBody, BoxPartSink, CloudAccessOutcome, CloudAccessState,
    CloudFileReadError, CloudHome, CloudHomeError, CloudHomeJoinInfo, CloudKitAcceptedShareRecord,
    CloudKitAtomicCreateBatch, CloudKitOps, CloudKitProviderIdentity, CloudKitRecordCreate,
    CloudKitRecordVersion, CloudKitScope, CloudKitShare, CloudKitShareAcceptance,
    CloudKitSharePermission, CloudObjectStream, CloudObjectVersion, CloudVersionedObject,
    ExactCloudHome, ExactCreateOutcome, ExactSlotStorage, ExactUpload, ExactUploadSource, PartSink,
    S3CloudHome, UploadProgress,
};
pub use handle::CovenHandle;
pub use live_query::{
    LiveQuery, LiveQueryClosed, LiveQueryRequests, LiveQueryRevision, ReconfigurableLiveQuery,
    ReconfigurableLiveQueryCause, ReconfigurableLiveQueryEvent,
};
pub use read_handle::CovenReadHandle;
pub use store_security::CloudHomeKeyState;
