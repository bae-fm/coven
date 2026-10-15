//! The application API for coven stores (Appendix E).
//!
//! Declare tables and migrations, create a store directory, then open it with
//! [`Coven::builder`]. SQL, files, keys and their capabilities remain with the
//! crates that own them. Opening resumes unfinished operations and committed
//! file work without starting a sync loop; an empty journal needs no unlocked keys.

mod builder;
mod coven;
mod error;
mod handle;
mod read_handle;
#[cfg(any(test, feature = "test-utils"))]
mod test_utils;

pub use builder::CovenBuilder;
pub use builder::{join_with_invite, restore_from_code, restore_from_keychain, BootstrapError};
pub use coven::Coven;
pub use coven_sync::{decode_code_info, CodeError, CodeInfo, CodeKind};
pub use coven_sync::{
    DrainOutcome, EagerCacheFillStatus, FileRangeStream, FileReadError, FileStatus, FileStream,
    PinProgress, QueuedUpload, TransferLimits, UploadPhase, UploadQueue,
};
pub use error::{ReadOnlyOpenError, StoreCreationError, StoreDeletionError, StoreKeyUnlockError};
pub use handle::CovenHandle;
pub use read_handle::CovenReadHandle;

pub use coven_crypto::custody::{
    set_keyring_service, IdentityCustody, IdentityError, KeyCustody, KeyError, KeychainError,
    MemberKeyCustody, Passphrase, StoreKeyCustody,
};
pub use coven_crypto::{
    CircleKey, CryptoError, MaterialError, MemberIdentity, MemberKeys, SecretBytes, SecretText,
    StoreKey, StoreKeyring,
};
pub use coven_database::{
    named_params, params, prepare_user_file, types, CacheFill, ChangeOp, ChangeSource,
    ColumnChange, CovenError, CovenMigrationError, CovenResult, DbError, FileDecl, FileRef,
    FileSource, LiveQuery, LiveQueryCause, LiveQueryClosed, LiveQueryRequests, LiveQueryRevision,
    Lost, LostCell, LostValue, Migration, MigrationChange, MigrationContext, MigrationError,
    MigrationOutcome, Params, PreparedUserFile, Provenance, Read, ReconfigurableLiveQuery,
    ReconfigurableLiveQueryEvent, RemovalRule, Replacement, Row, RowChange, RowIdentity, RowKey,
    SchemaError, SqlContext, SqlReadContext, SyncedTable, ToSql, UserFile, WriteBatch, WriteId,
};
pub use coven_database::{DropReason, EntryTarget};
pub use coven_database::{LogObject, LogRefusal};
pub use coven_format::pending::RefusalCode;
pub use coven_format::value::EntryId;
pub use coven_format::{IamPolicy, MemberAccess, S3Principal};
pub use coven_foundation::clock::{Clock, ClockRef, SystemClock};
pub use coven_foundation::files::FileError as DiskError;
pub use coven_foundation::files::{
    BootstrapDirectoryError, DeviceReset, DeviceResetReason, FileError, FileName, FileNameError,
    SettingsError, StoreDir, StoreInfo, StoreLayout, StoreLayoutError, StoreLockError,
};
pub use coven_foundation::id_source::MemberId;
pub use coven_foundation::id_source::{
    CircleId, DeviceId, FileId, IdSource, IdSourceRef, InviteId, KeyId, StoreId, UuidIds,
};
pub use coven_merge::{Audience, MergeError};
#[cfg(not(any(target_os = "ios", target_os = "android")))]
pub use coven_storage::providers::DesktopDeviceSignIn;
pub use coven_storage::providers::{AdminAccessSource, DeviceSignInPresenter};
pub use coven_storage::providers::{
    DropboxClients, GoogleDriveClient, OAuthClients, OAuthError, OAuthPresenter,
    OAuthRedirectError, ProviderResponse,
};
pub use coven_storage::{AccessKeyAdmin, AdminCredential, MintScope, S3MemberKey};
pub use coven_storage::{
    ByteRange, ChecksumValue, CloudProvider, DropboxAccess, DropboxLocation, GoogleDriveAccess,
    OAuthAccess, ObjectChecksum, ObjectPath, ObjectPrefix, OwnBucketReason, S3BucketVersioning,
    S3Checksums, S3CopyCondition, S3Credentials, S3Detection, S3Listing, S3Multipart,
    S3MultipartTimestamp, S3Observation, S3Probe, S3Profile, S3ProfileName, S3ResponseHeaders,
    S3Revision, S3Settings, S3Signing, S3SinglePut, StorageConfig, StorageError, StorageFailure,
    StoredObject,
};
pub use storage::GuidedAccessKey;

#[cfg(any(test, feature = "test-utils"))]
pub use coven_foundation::{clock::FixedClock, id_source::SequentialIds};
#[cfg(any(test, feature = "test-utils"))]
pub use test_utils::TestCoven;

mod circles;
pub use circles::Circles;
pub use coven_format::store_log::{MemberRole, SnapshotId};
pub use coven_storage::{ProviderSignOut, RetainedAccess, RetainedAccessReason};
pub use coven_sync::{
    AccessKeyReplacement, AccessKeyToDelete, Circle, CircleMemberInfo, DamagedObject, DeviceInfo,
    DeviceState, FileMissingReason, FileSourceFailure, Invite, InviteAccess, InviteTarget,
    JoinRequest, LocalFailure, MemberInfo, OldKeyDeletion, OperationError, OperationId,
    OperationKind, PendingReason, PendingRecord, PendingSubject, Prerequisite,
    ProviderAccessAction, Refusal, RequiredUpdate, Retry, SyncError, SyncFailure, SyncStatus,
};

#[cfg(not(any(target_os = "ios", target_os = "android")))]
pub use coven_storage::providers::DesktopOAuthPresenter;

mod authentication;
mod storage;
#[cfg(any(test, feature = "test-utils"))]
pub use coven_storage::providers::StorageConnector;
pub use coven_storage::{StorageCheck, StorageSetupError};
pub use storage::{ConnectedStorage, StoreKeyState};

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
