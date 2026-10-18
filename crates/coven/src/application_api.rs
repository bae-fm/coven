//! The application surface re-exported at the crate root. Owner-only operations
//! stay in their defining crates.

pub use crate::builder::CovenBuilder;
pub use crate::builder::{
    join_with_invite, restore_from_code, restore_from_keychain, BootstrapError, BootstrapStatus,
};
pub use crate::coven::Coven;
pub use crate::error::{
    ReadOnlyOpenError, StoreCreationError, StoreDeletionError, StoreKeyUnlockError,
};
pub use crate::handle::{CovenHandle, IfUnsent};
pub use crate::read_handle::CovenReadHandle;
pub use crate::saved_restore::{saved_restore_codes, SavedRestoreCode};
pub use crate::store_layout::{StoreLayout, StoreLayoutError};
pub use coven_crypto::custody::{
    set_keyring_service, IdentityCustody, IdentityError, KeyCustody, KeyError, KeychainError,
    MemberKeyCustody, Passphrase, StoreKeyCustody,
};
pub use coven_crypto::{
    CircleKey, CryptoError, MaterialError, MemberIdentity, MemberKeys, MemberPublicKeys,
    SealingKey, SecretBytes, SecretText, SigningKey, StoreKey, StoreKeyring,
};
pub use coven_database::StoreIdentity as StoreInfo;
pub use coven_database::{
    named_params, params, prepare_user_file, types, CacheFill, ChangeOp, ChangeSource,
    ColumnChange, CovenError, CovenMigrationError, CovenResult, DbError, FileDecl, FileRef,
    FileSource, LiveQuery, LiveQueryCause, LiveQueryClosed, LiveQueryRequests, LiveQueryRevision,
    Lost, LostCell, LostValue, Migration, MigrationContext, MigrationError, OldWrites, Params,
    PreparedUserFile, Provenance, Read, ReconfigurableLiveQuery, ReconfigurableLiveQueryEvent,
    RemovalRule, Replacement, Row, RowAuthorizationFailure, RowChange, RowIdentity, RowKey,
    SchemaError, SnapshotError, SqlContext, SqlReadContext, SyncedTable, ToSql, UnsentWork,
    UserFile, WriteBatch, WriteId, WriteWait,
};
pub use coven_sync::{decode_code_info, CodeError, CodeInfo, CodeKind, CodeMetadata};
pub use coven_sync::{
    DrainOutcome, EagerCacheFillStatus, FileRangeStream, FileReadError, FileStatus, FileStream,
    PinProgress, QueuedUpload, TransferLimits, UploadPhase, UploadQueue,
};
/// An admin entry is identified by the slot containing it (§10).
pub type EntryId = Slot;
pub use crate::circles::Circles;
pub use crate::storage::GuidedAccessKey;
pub use crate::storage::{ConnectedStorage, StorageSetup, StoreKeyState};
#[cfg(any(test, feature = "test-utils"))]
pub use crate::test_utils::TestCoven;
pub use coven_format::admin::{MemberRole, SnapshotId};
pub use coven_format::chunks::CHUNK_SIZE;
pub use coven_format::value::StorageTime;
pub use coven_format::{IamPolicy, S3Principal};
pub use coven_foundation::clock::{Clock, ClockRef, SystemClock};
pub use coven_foundation::files::FileError as DiskError;
#[cfg(feature = "test-utils")]
pub use coven_foundation::files::{assert_no_open_files_under, open_files_under};
pub use coven_foundation::files::{
    write_atomic, BootstrapDirectoryError, DeviceReset, DirectoryCreationError, FileError,
    FileName, FileNameError, SettingsError, StoreDir, StoreLockError,
};
pub use coven_foundation::id_source::MemberId;
pub use coven_foundation::id_source::{
    CircleId, DeviceId, FileId, IdSource, IdSourceRef, InviteId, KeyId, StoreId, UuidIds,
};
pub use coven_foundation::uuid_text::UuidText;
#[cfg(any(test, feature = "test-utils"))]
pub use coven_foundation::{clock::FixedClock, id_source::SequentialIds};
pub use coven_merge::{Audience, MergeError, Slot};
#[cfg(not(any(target_os = "ios", target_os = "android")))]
pub use coven_storage::providers::DesktopDeviceSignIn;
#[cfg(not(any(target_os = "ios", target_os = "android")))]
pub use coven_storage::providers::DesktopOAuthPresenter;
#[cfg(any(test, feature = "test-utils"))]
pub use coven_storage::providers::StorageConnector;
pub use coven_storage::providers::{AdminAccessSource, DeviceSignInPresenter};
pub use coven_storage::providers::{
    DropboxClients, OAuthClients, OAuthError, OAuthPresenter, OAuthRedirectError, ProviderResponse,
};
pub use coven_storage::{
    AccessKeyAdmin, AdminCredential, ListedKey, MintScope, S3MemberKey, StoreListing,
    UnkeyedPrincipal,
};
pub use coven_storage::{
    ByteRange, ChecksumValue, CloudProvider, DropboxAccess, DropboxLocation, OAuthAccess,
    ObjectChecksum, ObjectPath, ObjectPrefix, OwnBucketReason, PathError, S3BucketVersioning,
    S3Checksums, S3CopyCondition, S3CopyPublication, S3Credentials, S3Detection, S3Listing,
    S3MultipartTimestamp, S3Probe, S3ProbeResult, S3Profile, S3ProfileName, S3ResponseHeaders,
    S3Revision, S3Settings, S3Signing, S3SinglePut, StorageConfig, StorageError, StorageFailure,
    StoredObject,
};
pub use coven_storage::{ProviderSignOut, RetainedAccess, RetainedAccessReason};
pub use coven_storage::{StorageCheck, StorageSetupError};
pub use coven_sync::{
    AccessKeyReplacement, AccessKeyToDelete, Circle, CircleMemberInfo, DamagedObject,
    DeleteAccessKeyError, DeviceIdEvidence, DeviceInfo, DeviceState, EntryTarget,
    FileMissingReason, FileSourceFailure, Invite, InviteAccess, InviteTarget, JoinRequest,
    LocalFailure, MemberInfo, OldKeyDeletion, OperationError, OperationId, OperationKind,
    OperationRejection, PendingReason, PendingRecord, PendingSubject, Prerequisite,
    ProviderAccessAction, Refusal, RequiredUpdate, Retry, SyncError, SyncFailure, SyncStatus,
    WriteCutoff,
};

#[cfg(any(test, doctest))]
#[path = "application_api_tests.rs"]
mod tests;
