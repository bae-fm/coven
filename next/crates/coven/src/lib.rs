//! The application API for coven stores (§20).
//!
//! Declare tables and migrations, create a store directory, then open it with
//! [`Coven::builder`]. SQL, files, keys and their capabilities remain with the
//! crates that own them. Opening resumes unfinished operations and committed
//! file work without starting a sync loop; an empty journal needs no unlocked keys.

mod bootstrap;
mod builder;
mod connection;
mod coven;
mod error;
mod handle;
mod read_handle;
#[cfg(any(test, feature = "test-utils"))]
mod test_utils;

pub use bootstrap::{join_with_invite, restore_from_code, restore_from_keychain, BootstrapError};
pub use builder::CovenBuilder;
pub use coven::Coven;
pub use coven_sync::{decode_code_info, CodeError, CodeInfo, CodeKind};
pub use coven_sync::{
    DrainOutcome, EagerCacheFillStatus, FileRangeStream, FileReadError, FileStream, PinProgress,
    QueuedUpload, RecordedUploadFailure, RowsPinnedLiveQuery, UploadFailure, UploadFailures,
    UploadPhase, UploadQueue, UploadsLiveQuery,
};
pub use error::{RecoveryError, StoreCreationError, StoreDeletionError};
pub use handle::CovenHandle;
pub use read_handle::CovenReadHandle;

pub use coven_crypto::custody::{
    set_keyring_service, IdentityCustody, IdentityError, KeyCustody, KeyError, KeychainError,
    MemberKeyCustody, Passphrase, SecretNameError, StoreKeyCustody,
};
pub use coven_crypto::{
    CircleKey, CryptoError, MaterialError, MemberId, MemberKeys, SealError, SecretBytes,
    SecretText, StoreKey, StoreKeyring,
};
pub use coven_database::{
    named_params, params, prepare_user_file, types, CacheFill, ChangeOp, ColumnChange, CovenError,
    CovenMigrationError, CovenMigrationPolicy, CovenResult, DbError, FileDecl, FileLocation,
    FileRef, FileSource, LiveQuery, LiveQueryCause, LiveQueryClosed, LiveQueryRequests,
    LiveQueryRevision, Lost, LostCell, LostValue, Migration, MigrationChange, MigrationContext,
    MigrationError, MigrationOutcome, Params, PreparedUserFile, Provenance, Read,
    ReconfigurableLiveQuery, ReconfigurableLiveQueryEvent, RemovalRule, Replacement, Row,
    RowChange, RowIdentity, RowKey, SchemaError, SqlContext, SqlReadContext, SyncedTable, ToSql,
    Uploads, UserFile, WriteBatch, WriteId,
};
pub use coven_format::value::EntryId;
pub use coven_format::MemberAccess;
pub use coven_foundation::clock::{Clock, ClockRef, SystemClock};
pub use coven_foundation::files::FileError as DiskError;
pub use coven_foundation::files::{
    BootstrapDirectoryError, FileError, SettingsError, StoreDir, StoreInfo, StoreLayout,
    StoreLayoutError, StoreLockError,
};
pub use coven_foundation::id_source::{
    CircleId, DeviceId, FileId, IdSource, IdSourceRef, InviteId, KeyId, StoreId, UuidIds,
};
pub use coven_merge::{Audience, MergeError};
pub use coven_storage::providers::{
    AuthorizeRequest, CloudKitOps, CloudKitUpload, CloudKitUploadStatus, OAuthClients, OAuthError,
};
pub use coven_storage::{
    ByteRange, CloudProvider, OAuthTokens, ObjectPath, ObjectPrefix, StorageConfig, StorageError,
    StorageFailure,
};

#[cfg(any(test, feature = "test-utils"))]
pub use coven_foundation::{clock::FixedClock, id_source::SequentialIds};
#[cfg(any(test, feature = "test-utils"))]
pub use test_utils::TestCoven;

mod circles;
pub use circles::Circles;
pub use coven_format::store_log::MemberRole;
pub use coven_storage::{MemberRemoval, ProviderSignOut, RetainedAccess, RetainedAccessReason};
pub use coven_sync::{
    AccessKeyToDelete, BlockedOperation, Circle, CircleError, CircleMemberInfo, Disagreement,
    DroppedEntry, Invite, InviteAccess, JoinRequest, MemberInfo, OperationError, OperationId,
    OperationKind, StartedBy, StoreLogChange, SyncError, SyncFailure, SyncReport,
};
