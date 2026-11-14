//! The application surface re-exported at the crate root. Owner-only operations
//! stay in their defining crates.

pub use crate::builder::CovenBuilder;
pub use crate::builder::{
    join_with_invite, join_with_pairing, restore_from_code, restore_from_saved_code,
    BootstrapError, BootstrapStatus, JoinControl,
};
pub use crate::coven::{
    ClosedSyncStatus, Coven, ListedStore, StoreInfo, StoreListError, Unfinished,
};
pub use crate::coven_error::{CovenError, CovenResult};
pub use crate::coven_write::CovenWrite;
#[cfg(feature = "dev-tools")]
pub use crate::dev_tools::empty_s3_bucket;
pub use crate::error::{
    ReadOnlyOpenError, StorageSetupError, StoreCreationError, StoreDeletionError,
    StoreKeyUnlockError,
};
pub use crate::handle::{Circles, CovenHandle, IfUnsent};
pub use crate::member_write::MemberWrite;
pub use crate::read_handle::CovenReadHandle;
pub use crate::saved_restore::{SavedRestoreCode, SavedRestoreEntry};
pub use crate::storage::GuidedAccessKey;
pub use crate::storage::{
    ConnectedStorage, SetupCancellation, StorageSetup, StorageSetupStep, StoreKeyState,
};
pub use coven_crypto::custody::{
    Custody, CustodyChoice, IdentityError, KeyError, KeyMaterial, KeychainError, Passphrase,
};
pub use coven_crypto::{
    CircleKey, ConfirmationNumber, CryptoError, MaterialError, MemberIdentity, MemberKeys,
    MemberPublicKeys, SealingKey, SecretBytes, SecretText, SigningKey, StoreKey, StoreKeyring,
};
#[cfg(any(test, feature = "test-utils"))]
pub use coven_database::test_utils::{
    followed_tables_sql, path_text_sql, StatementLog, WorkCounter, WorkCounts,
};
#[cfg(any(test, feature = "test-utils"))]
pub use coven_database::LiveQueryRuns;
pub use coven_database::{
    named_params, params, prepare_user_file, sqlite_error_is_disk, types, unix_seconds_and_nanos,
    Binding, BulkSqlContext, ChangeOp, ChangeSource, ColumnChange, CovenMigrationError, DbError,
    Determinism, FileColumn, FileLocation, FilePlace, FileRef, FileSource, FunctionArguments,
    Joining, JoiningWrite, LiveQuery, LiveQueryCause, LiveQueryClosed, LiveQueryRequests,
    LiveQueryRevision, LoadStep, Lost, LostCell, LostValue, Migration, MigrationContext,
    MigrationError, Mode, MoveFile, MoveId, Namespace, OldWrites, OpenStep, Params, Place,
    PreparedDeviceOnly, PreparedPin, PreparedUserFile, Provenance, Read, ReadOnly,
    ReconfigurableLiveQuery, ReconfigurableLiveQueryEvent, RemovalRule, Replacement, Row,
    RowAuthorizationFailure, RowChange, RowIdentity, RowKey, SchemaError, SnapshotError, SqlRead,
    SqlReadContext, SqliteError, SqliteResult, StagedFiles, SyncedTable, TemporaryDirectoryError,
    ToSql, TransferChange, TransferEnd, TransferProgress, TransferWatch, UnsentWork, UserFile,
    WriteBatch, WriteFailure, WriteId, WriteWait, Writer, BOUND_FILE_LIMIT,
};

pub use coven_database::{
    AnalysisLimit, AutoVacuum, CacheSize, CacheSpill, CheckpointMode, IncrementalVacuum,
    JournalSizeLimit, PageSize, SecureDelete, SqliteSettings, SqliteSettingsInEffect, TempStore,
    WalAutocheckpoint, WalCheckpoint,
};
pub use coven_database::{LocalBytes, LocalBytesWatch, NamespaceBytes};
pub use coven_format::admin::{MemberRole, SnapshotId};
pub use coven_format::chunks::CHUNK_SIZE;
pub use coven_format::value::StorageTime;
pub use coven_format::MemberAccess;
pub use coven_foundation::clock::{Clock, ClockRef, SystemClock, WallTimerError};
#[cfg(any(test, feature = "test-utils"))]
pub use coven_foundation::files::FileBudgetWatch;
#[cfg(target_vendor = "apple")]
pub use coven_foundation::files::FileProtection;
#[cfg(all(target_vendor = "apple", any(test, feature = "test-utils")))]
pub use coven_foundation::files::ProtectionSupport;
#[cfg(any(test, feature = "test-utils"))]
pub use coven_foundation::files::{assert_no_open_files_under, open_files_under};
pub use coven_foundation::files::{
    path_bytes, path_from_bytes, path_text, write_atomic, BootstrapDirectoryError, CopyPathError,
    DeviceReset, DirectoryCreationError, FileBudget, FileError, FileLease, FileLock, FileName,
    FileNameError, LeasePlace, LeaseTooLarge, LeasedFile, LockMode, PathBytesError, RelativePath,
    RelativePathError, ReplacedBy, SettingsError, StoreDir, StoreLockError,
};
pub use coven_foundation::id_source::MemberId;
pub use coven_foundation::id_source::{
    CircleId, DeviceId, FileId, IdSource, IdSourceRef, InviteId, KeyId, ObjectId, StoreId, UuidIds,
};
pub use coven_foundation::network_watch::NetworkWatchError;
#[cfg(any(test, feature = "test-utils"))]
pub use coven_foundation::network_watch::TestNetwork;
pub use coven_foundation::progress::Progress;
pub use coven_foundation::sharing::SharingRefusal;
pub use coven_foundation::uuid_text::UuidText;
#[cfg(any(test, feature = "test-utils"))]
pub use coven_foundation::{
    clock::{FixedClock, PollingClock},
    id_source::SequentialIds,
};
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
pub use coven_storage::providers::{EmptiedS3Bucket, EmptyS3BucketError};
#[cfg(any(test, feature = "test-utils"))]
pub use coven_storage::test_utils::{
    BodyFailure, Faults, MemoryProvider, MemoryRequest, MemoryStorage, MemoryStorageBuilder,
};
#[cfg(any(test, feature = "test-utils"))]
pub use coven_storage::Storage;
pub use coven_storage::{
    ByteRange, ChecksumValue, CloudProvider, DropboxAccess, DropboxLocation, FolderLabel,
    OAuthAccess, ObjectChecksum, ObjectPath, ObjectPrefix, PathError, S3BucketVersioning,
    S3Checksums, S3CopyCondition, S3CopyPublication, S3Credentials, S3Detection, S3Listing,
    S3MultipartTimestamp, S3Probe, S3ProbeResult, S3Profile, S3ProfileName, S3ResponseHeaders,
    S3Revision, S3Settings, S3Signing, S3SinglePut, StorageConfig, StorageError, StorageFailure,
    StoredObject,
};
pub use coven_storage::{ProviderSignOut, RetainedAccess, RetainedAccessReason};
pub use coven_storage::{StorageCheck, StorageCheckError};
pub use coven_sync::{
    decode_code_info, CodeError, CodeInfo, CodeKind, CodeMetadata, InviteAdmits, InviteInfo,
    Joiner, PairingInfo, UnreadableSavedCode,
};
pub use coven_sync::{AddDevice, AddDeviceEnd, AddDeviceId, AddDeviceRequest, AddDeviceStatus};
pub use coven_sync::{
    Circle, CircleMemberInfo, CutOffRefusal, DamagedObject, DeviceIdEvidence, DeviceInfo,
    DeviceState, DeviceStop, EntryTarget, FileMissingReason, FileSourceFailure, Invite,
    InviteAccess, InviteLifetime, InviteStage, JoinRequest, LastPass, LocalFailure, MemberInfo,
    OperationError, OperationId, OperationKind, OperationOutcome, OperationRejection, PassFailure,
    PendingReason, PendingRecord, PendingSubject, Prerequisite, ProviderAccessAction, Refusal,
    Removal, RequiredUpdate, ResetReason, Retry, RetryAnswer, RetryEffect, RetryOutcome,
    StatusFailure, SyncActivity, SyncCause, SyncError, SyncFailure, SyncOftenGuard, SyncState,
    SyncStatus, SyncWait, UndoCause, WriteCutoff,
};
pub use coven_sync::{
    DeviceOnlyError, DeviceOnlyStep, DrainOutcome, FileRangeStream, FileReadError, FileStatus,
    FileStream, FilesMode, PinProgress, QueuedUpload, ReadProgress, TransferLimits, UploadPhase,
    UploadProgress, UploadQueue, UploadSettings, UploadWait,
};

#[cfg(any(test, doctest))]
#[path = "application_api_tests.rs"]
mod tests;
