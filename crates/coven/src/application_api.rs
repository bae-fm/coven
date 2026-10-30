//! The application surface re-exported at the crate root. Owner-only operations
//! stay in their defining crates.

pub use crate::builder::CovenBuilder;
pub use crate::builder::{
    join_with_invite, restore_from_code, restore_from_keychain, BootstrapError, BootstrapStatus,
    JoinControl,
};
pub use crate::circles::Circles;
pub use crate::coven::Coven;
#[cfg(feature = "dev-tools")]
pub use crate::dev_tools::empty_s3_prefix;
pub use crate::error::{
    ReadOnlyOpenError, StoreCreationError, StoreDeletionError, StoreKeyUnlockError,
};
pub use crate::handle::{CovenHandle, IfUnsent};
pub use crate::read_handle::CovenReadHandle;
pub use crate::saved_restore::{
    forget_saved_restore_code, saved_restore_codes, SavedRestoreCode, SavedRestoreEntry,
};
pub use crate::storage::GuidedAccessKey;
pub use crate::storage::{ConnectedStorage, StorageSetup, StoreKeyState};
pub use crate::store_layout::{StoreInfo, StoreLayout, StoreLayoutError};
#[cfg(any(test, feature = "test-utils"))]
pub use crate::test_utils::TestCoven;
pub use coven_crypto::custody::{
    set_keyring_service, IdentityCustody, IdentityError, KeyCustody, KeyError, KeychainError,
    MemberKeyCustody, Passphrase, StoreKeyCustody,
};
pub use coven_crypto::{
    CircleKey, ConfirmationNumber, CryptoError, MaterialError, MemberIdentity, MemberKeys,
    MemberPublicKeys, SealingKey, SecretBytes, SecretText, SigningKey, StoreKey, StoreKeyring,
};
pub use coven_database::{
    named_params, observe_user_file, params, prepare_user_file, types, BulkSqlContext, ChangeOp,
    ChangeSource, ColumnChange, CovenError, CovenMigrationError, CovenResult, DbError, Determinism,
    FileColumn, FileLocation, FilePlace, FileRef, FileSource, FunctionArguments, LiveQuery,
    LiveQueryCause, LiveQueryClosed, LiveQueryRequests, LiveQueryRevision, Lost, LostCell,
    LostValue, Migration, MigrationContext, MigrationError, MoveFile, MoveId, Namespace,
    ObservedUserFile, OldWrites, Params, Place, PreparedUserFile, Provenance, Read,
    ReconfigurableLiveQuery, ReconfigurableLiveQueryEvent, RemovalRule, Replacement, Row,
    RowAuthorizationFailure, RowChange, RowIdentity, RowKey, SchemaError, SnapshotError,
    SqlContext, SqlRead, SqlReadContext, SqliteError, SqliteResult, SyncedTable,
    TemporaryDirectoryError, ToSql, TransferChange, TransferProgress, TransferWatch, UnsentWork,
    UserFile, UserSource, WriteBatch, WriteId, WriteWait,
};
pub use coven_database::{
    AnalysisLimit, AutoVacuum, CacheSize, CacheSpill, CheckpointMode, IncrementalVacuum,
    JournalSizeLimit, PageSize, SecureDelete, SqliteSettings, SqliteSettingsInEffect, TempStore,
    WalAutocheckpoint, WalCheckpoint,
};
pub use coven_format::admin::{MemberRole, SnapshotId};
pub use coven_format::chunks::CHUNK_SIZE;
pub use coven_format::codes::InviteDeliveryKind;
pub use coven_format::value::StorageTime;
pub use coven_format::MemberAccess;
pub use coven_foundation::clock::{Clock, ClockRef, SystemClock};
pub use coven_foundation::files::FileError as DiskError;
#[cfg(feature = "test-utils")]
pub use coven_foundation::files::{assert_no_open_files_under, open_files_under};
pub use coven_foundation::files::{
    write_atomic, BootstrapDirectoryError, CopyPathError, DeviceReset, DirectoryCreationError,
    FileError, FileLock, FileName, FileNameError, LockMode, RelativePath, RelativePathError,
    ReplacedBy, SettingsError, StoreDir, StoreLockError,
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
pub use coven_storage::providers::DesktopOAuthPresenter;
#[cfg(any(test, feature = "test-utils"))]
pub use coven_storage::providers::StorageConnector;
pub use coven_storage::providers::{
    DropboxClients, OAuthClients, OAuthError, OAuthPresenter, OAuthRedirectError, ProviderResponse,
    StorageFolder,
};
#[cfg(feature = "dev-tools")]
pub use coven_storage::providers::{EmptiedS3Prefix, EmptyS3PrefixError};
#[cfg(any(test, feature = "test-utils"))]
pub use coven_storage::test_utils::{
    BodyFailure, Faults, MemoryProvider, MemoryRequest, MemoryStorage, MemoryStorageBuilder,
};
#[cfg(any(test, feature = "test-utils"))]
pub use coven_storage::Storage;
pub use coven_storage::{
    ByteRange, ChecksumValue, CloudProvider, DropboxAccess, DropboxLocation, OAuthAccess,
    ObjectChecksum, ObjectPath, ObjectPrefix, OwnBucketReason, PathError, S3BucketVersioning,
    S3Checksums, S3CopyCondition, S3CopyPublication, S3Credentials, S3Detection, S3Listing,
    S3MultipartTimestamp, S3Prefix, S3PrefixError, S3Probe, S3ProbeResult, S3Profile,
    S3ProfileName, S3ResponseHeaders, S3Revision, S3Settings, S3Signing, S3SinglePut,
    StorageConfig, StorageError, StorageFailure, StoredObject,
};
pub use coven_storage::{ProviderSignOut, RetainedAccess, RetainedAccessReason};
pub use coven_storage::{StorageCheck, StorageSetupError};
pub use coven_sync::{
    decode_code_info, CodeError, CodeInfo, CodeKind, CodeMetadata, InviteInfo, UnreadableSavedCode,
};
pub use coven_sync::{
    Circle, CircleMemberInfo, DamagedObject, DeviceIdEvidence, DeviceInfo, DeviceState,
    EntryTarget, FileMissingReason, FileSourceFailure, Invite, InviteAccess, InviteDelivery,
    InviteLifetime, InviteTarget, JoinRequest, LocalFailure, MemberInfo, NextCheck, OperationError,
    OperationId, OperationKind, OperationRejection, PassFailure, PendingCauses, PendingReason,
    PendingRecord, PendingSubject, Prerequisite, ProviderAccessAction, Refusal, RequiredUpdate,
    Retry, RetryAnswer, RetryEffect, RetryOutcome, SyncError, SyncFailure, SyncOftenGuard,
    SyncStatus, WriteCutoff,
};
pub use coven_sync::{
    DeviceOnlyError, DrainOutcome, FileRangeStream, FileReadError, FileStatus, FileStream,
    PinProgress, QueuedUpload, TransferLimits, UploadPhase, UploadProgress, UploadQueue,
};

#[cfg(any(test, doctest))]
#[path = "application_api_tests.rs"]
mod tests;
