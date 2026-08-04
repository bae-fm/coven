//! End-to-end encrypted, multi-writer SQLite sync over host-selected storage.
//!
//! The host owns its SQLite schema and domain. Coven owns the connection,
//! captures host writes, stores protocol state, and synchronizes rows and blobs.
//! Hosts use the crate-root API; implementation modules remain private.
//!
//! ```compile_fail
//! let _ = coven::store_sync::StoreSync::connect;
//! ```

pub(crate) mod atomic_file;
pub(crate) mod blob;
pub(crate) mod changeset;
pub(crate) mod circles;
pub(crate) mod clock;
mod code_envelope;
pub(crate) mod config;
pub(crate) mod coven;
pub(crate) mod custody;
pub(crate) mod database;
pub(crate) mod encryption;
pub(crate) mod envelope;
mod handle;
pub(crate) mod id_provider;
pub(crate) mod identity_custody;
pub(crate) mod join_code;
pub(crate) mod joining;
pub(crate) mod keyring_backend;
pub(crate) mod keys;
pub(crate) mod local_file;
pub(crate) mod oauth;
pub(crate) mod object_hash;
pub(crate) mod protocol;
mod read_handle;
mod read_store_rows;
pub(crate) mod restoration;
pub(crate) mod storage;
pub(crate) mod store_blobs;
pub(crate) mod store_circles;
pub(crate) mod store_cloud_storage;
pub(crate) mod store_dir;
pub(crate) mod store_joining;
pub(crate) mod store_membership;
pub(crate) mod store_recovery;
pub(crate) mod store_rows;
pub(crate) mod store_security;
pub(crate) mod store_sync;
pub(crate) mod sync;
mod write;

#[cfg(test)]
mod blob_facade_tests;

pub use database::rusqlite;

pub use atomic_file::{write_atomic, WriteError};
pub use blob::{MakeLocalError, MakeRemoteError};
pub use changeset::{ChangeOp, RowChange};
pub use circles::{CircleError, Circles};
pub use clock::{Clock, ClockRef, SystemClock};
#[cfg(any(test, feature = "test-utils"))]
pub use clock::{FixedClock, SteppingClock};
pub use config::{
    CloudHomeConfig, CloudProvider, Config, ConfigError, CustomS3ExactSlots, HomeStorage,
};
pub use coven::{Coven, CovenBuilder, CovenConfig, CovenError, CovenResult};
pub use custody::{rewrap_passphrase_custody, KeyCustody, Passphrase};
#[cfg(feature = "invariant-tests")]
#[doc(hidden)]
pub use database::exercise_exact_outbound_blob_graph;
pub use database::{
    DbError, ExternalBlob, MakeRemoteProgress, QueuedDelete, QueuedUpload, SqlContext,
    SqlReadContext, WriteBatch,
};
pub use database::{Migration, MigrationContext, MigrationError, MigrationStep};
#[cfg(any(test, feature = "test-utils"))]
pub use encryption::EncryptionService;
pub use encryption::{EncryptionError, KeyFingerprint, MasterKeyring, SealError, CHUNK_SIZE};
pub use handle::CovenHandle;
#[cfg(any(test, feature = "test-utils"))]
pub use id_provider::SequentialIdProvider;
pub use id_provider::{IdProvider, IdRef, UuidProvider};
pub use identity_custody::{rewrap_passphrase_identity_custody, IdentityCustody};
pub use joining::{
    abandon_join_request, decode_invite_code_info, decode_join_request, generate_join_request,
    BootstrapError, DeviceJoinClient, InviteCodeInfo, JoinCodeError, JoinRequestCode,
};
pub use joining::{
    close_scanned_invite_join, join_with_scanned_invite, DeviceJoinInvite,
    DeviceJoinTransportOutcome,
};
#[cfg(any(test, feature = "test-utils"))]
pub use joining::{
    close_scanned_invite_join_over_test_home, join_with_scanned_invite_over_test_home,
};
#[cfg(all(
    any(test, feature = "test-utils"),
    any(target_os = "macos", target_os = "ios")
))]
pub use keys::{apple_keyring_entry_facts_for_test, AppleKeyringEntryFacts};
pub use keys::{
    keyring_service, set_keyring_service, CloudHomeCredentials, DeviceIdentityCustody,
    IdentityError, KeyError, MasterKeyCustody, MasterKeyError, StoreKeys, UserKeypair,
};
#[cfg(any(test, feature = "oauth-providers"))]
pub use oauth::OAuthError;
#[cfg(feature = "oauth-providers")]
pub use oauth::{AuthorizeRequest, OAuthClientCreds, OAuthClientCredsError};
pub use oauth::{OAuthClients, OAuthTokens};
pub use protocol::blob::{
    content_hash, BlobRef, BlobReplacement, BlobScope, BlobTransitionObserver, CacheFill,
    ContentHasher, DrainOutcome, Provenance, RowBlobAuthority, RowBlobRef, UploadFailure,
    UploadFailureCause, UploadFailures,
};
pub use protocol::hlc::Timestamp;
pub use protocol::objects::{ObjectSlot, PhysicalObjectLocator, StorageError};
pub use protocol::synced_schema::{BlobDecl, RowIdentity, SyncedTable};
pub use protocol::{
    Audience, Circle, CircleCloseParticipant, CircleCloseSettlement, CircleCloseStatus,
    CircleControlCoord, CircleEpochCloseId, CircleId, CircleInfo, CircleMemberInfo,
    CircleOperationBlock, CircleOperationId, CircleOperationInfo, CircleOperationKind,
    CircleOperationState, CircleRole, CircleState, CloudKitAcceptedShare, CommitFrontier,
    CrossPrincipalProbeReceipt, DeviceJoinAttemptId, DeviceJoinAttemptRef, ExactSlotProbeReceipt,
    MemberInfo, MemberRole, MembershipConflictChoice, MembershipConflictInfo, MembershipCoord,
    ObjectHash, ProviderAccessLocator, ProviderAccessWithdrawal, ProviderAdminChange,
    ProviderAdminGrantId, ProviderAdminGrantRecord, ProviderAdminMembershipChange,
    ProviderAdminState, ProviderCapabilityProof, ProviderProbeId, StoreBatchCommitRef,
    StoreCommitCoord, StoreCommitOrder, StoreDeviceId,
};
pub use protocol::{
    AwsPrincipal, CloudKitEnvironment, GoogleDriveCorpus, ProviderDeviceBinding,
    ProviderPrincipalId, ResolvedProviderBinding, S3EndpointBinding, StoreProviderBinding,
};
pub use read_handle::CovenReadHandle;
pub use restoration::{
    decode_restore_code_info, restore_from_cloud, restore_from_code, ActivatedContinuation,
    OwnerRecoveryAuthority, RestoreAuthority, RestoreCode, RestoreCodeError, RestoreCodeInfo,
    RestoreSource,
};
#[cfg(feature = "oauth-providers")]
pub use storage::fetch_account_email;
#[cfg(feature = "test-utils")]
pub use storage::CloudCipher;
#[cfg(any(test, feature = "test-utils"))]
pub use storage::InMemoryCloudHome;
pub use storage::{
    write_cloud_object_stream, BlobBody, BoxPartSink, CloudAccessOutcome, CloudAccessState,
    CloudFileReadError, CloudHome, CloudHomeError, CloudHomeJoinInfo, CloudKitAcceptedShareRecord,
    CloudKitAtomicCreateBatch, CloudKitOps, CloudKitProviderIdentity, CloudKitRecordCreate,
    CloudKitRecordVersion, CloudKitScope, CloudKitShare, CloudKitShareAcceptance,
    CloudKitSharePermission, CloudObjectStream, CloudObjectVersion, CloudVersionedObject,
    ExactSlotStorage, PartSink, S3CloudHome, UploadProgress,
};
pub use store_dir::{StoreDir, StoreLayout};
pub use sync::{
    BlobCacheError, BlobStream, DeviceActivity, DeviceJoinAbandonment, DeviceJoinAction,
    DeviceJoinActivation, DeviceJoinApproval, DeviceJoinApprovalPolicy, DeviceJoinCancellation,
    DeviceJoinCleanupActivation, DeviceJoinCleanupProgress, DeviceJoinCleanupReceipt,
    DeviceJoinDriveOutcome, DeviceJoinError, DeviceJoinJournalDatabase, DeviceJoinJournalRecord,
    DeviceJoinOffer, DeviceJoinOfferBundle, DeviceJoinProducer, DeviceJoinProducerWriteRevocation,
    DeviceJoinReadiness, DeviceJoinRole, DeviceJoinStatus, DeviceJoinTransportError,
    DeviceJoinTransportKind, DeviceJoinTransportParams, DeviceJoinTransportTiming,
    DeviceJoinWriteRevocationExecutor, DeviceProviderAccessAdministrator,
    DeviceProviderAccessRequest, DeviceProviderAdmission, DeviceProviderAdmissionApproval,
    DeviceProviderAdmissionCompletion, DeviceProviderReadiness, DeviceRegistrationRequest, Hlc,
    JoinedStore, JoinerJoinClosure, JoinerJoinTerminal, ProviderAdminJoinClosure,
    ProviderAdminJoinTerminal, ProviderReadyDeviceBootstrap, ProviderWriteAuthorityRef,
    ProvisionalDeviceBootstrap, SyncError, SyncLoopAlerts, SyncLoopStatus, SyncLoopSuccess,
};
pub use write::{
    AffectedRow, PendingWrite, PublishedPosition, WriteBlock, WriteId, WriteReceipt,
    WriteResolution, WriteRetractionWitness, WriteStatus,
};
