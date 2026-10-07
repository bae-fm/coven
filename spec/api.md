## Appendix E. API

- The API is what apps call, listed as Rust declarations with their doc
  comments. What one coven crate offers another is documented in its code,
  not here.
- Long parameter lists are abbreviated: `/* … */` marks parameters left out,
  and the comment beside it names them.
- Calls on `handle` are on the `CovenHandle` that opening a store returns.
- Every call that reaches storage or the database is `async`.
- A row is named by its table and its *key*: the values of its primary key
  columns, in order ([§8.5](coven.md#85-keys-and-uniqueness)).

```rust
// External types used by the declarations below.
use std::{collections::HashMap, future::Future, num::{NonZeroU64, NonZeroUsize},
          path::{Path, PathBuf}, pin::Pin, sync::Arc, time::SystemTime};
use async_trait::async_trait;
use rusqlite::{Params, ToSql};
use tokio::{io::AsyncRead, sync::watch};
use url::Url;
use uuid::Uuid;

/// A row's primary key: one value per key column, in the order the table
/// declares them (§8.5).
pub struct RowKey(/* private */);

impl From<&str> for RowKey { /* a one-column text key */ }
impl<A: Into<rusqlite::types::Value>, B: Into<rusqlite::types::Value>> From<(A, B)> for RowKey { /* a two-column key */ }
impl From<Vec<rusqlite::types::Value>> for RowKey { /* any key width */ }

/// One install of the app, by its 64-bit device id (§10).
pub struct DeviceId(/* private */);

/// A member, by the public half of their Ed25519 key pair (§11.1).
pub struct MemberId(/* private */);

/// One write: the device that made it and its number in that device's log (§6).
pub struct WriteId {
    pub device: DeviceId,
    pub number: u64,
}

/// One store log entry: the device that wrote it and its number in that device's store log (§9).
pub struct EntryId {
    /// The device that wrote the entry.
    pub device: DeviceId,
    /// Its entry number, starting at one.
    pub number: u64,
}

/// Where a row goes: the store, or one circle (§14).
pub enum Audience {
    Store,
    Circle(CircleId),
}
```

### E1 Opening

- A store lives in one directory on the device, its `StoreDir`, which holds
  the database, coven's copies of files, the cache, and the store's
  settings: its id and name, this device's id, and its storage settings.
- Creating, restoring or joining a store makes the directory and writes
  the settings, so the app never handles a device id ([§10](coven.md#10-device-identity)).
- A `StoreLayout` says where an app's stores live on disk.
- Store lock files live beside each store directory. Deletion holds the writer
  and reader locks while removing the directory, then releases and removes the
  lock files. Creation and lock-file removal are serialized by the layout.
- Opening a store needs its declared tables ([E2](#e2-declaring-synced-tables))
  and its migrations ([E13](#e13-migrations)).
- *Key custody* is where this device keeps the store keys and circle keys
  it has opened ([§11](coven.md#11-keys)): every key it has used, so it reads
  writes made under older ones.
- *Identity custody* is where this device keeps its member's two key pairs
  ([§11.1](coven.md#111-cryptography)).
- The app's `CloudKitOps` maps paths to stable record names in the configured
  container, owner and zone; all bytes it receives are encrypted.
- The bridge creates objects once and replaces posted positions atomically.
  Large uploads use bounded CKAssets, keep their ids and parts across
  restarts, and publish a record only after all parts are stored.
- Bridge failures keep their native cause in `StorageError::Provider`,
  classified with `CloudProvider::CloudKit` and a `StorageFailure`.

```rust
/// A store's UUID, independent of its name and location (E1).
pub struct StoreId(pub Uuid);

/// A circle's UUID, independent of its name and key (§14.3).
pub struct CircleId(pub Uuid);

/// The wall clock used for timestamps (§7.2).
pub trait Clock: Send + Sync {
    /// The wall clock's current time.
    fn now(&self) -> SystemTime;
}

/// A shared clock supplied when opening a store (§20.2).
pub type ClockRef = Arc<dyn Clock>;

/// The system wall clock used unless the app supplies another (E1).
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime;
}

/// The source of fresh ids, supplied when creating or opening a store (§20.2).
pub trait IdSource: Send + Sync {
    /// A fresh UUID; distinct calls must yield distinct ids.
    fn new_id(&self) -> Uuid;
    /// The default implementation derives a fresh 64-bit device id from this source (§10).
    fn new_device_id(&self) -> DeviceId;
}

/// A shared source of ids (§20.2).
pub type IdSourceRef = Arc<dyn IdSource>;

/// Random UUIDv4 ids, independent of the clock (E1).
pub struct UuidIds;

impl IdSource for UuidIds {
    fn new_id(&self) -> Uuid;
}

/// One store's directory; its path and store id are private (E1).
pub struct StoreDir { /* private fields */ }

/// The app directory under which each store has its own directory (E1).
pub struct StoreLayout { /* private fields */ }

/// A store on this device (E1).
pub struct StoreInfo {
    /// The store's identity.
    pub id: StoreId,
    /// The store's name.
    pub name: String,
}

/// The opening choices for stores under one layout (E1).
pub struct CovenBuilder { /* private fields */ }

/// A shared handle to the open store and its running work (§20.2).
pub struct CovenHandle { /* private fields */ }

/// A handle that only reads an already open store (§5, E1).
pub struct CovenReadHandle { /* private fields */ }

/// A table's name, key, audience, file and shared-trigger declarations (E2).
pub struct SyncedTable { /* private fields */ }

/// A numbered database migration and its optional waiting-write conversion (§17.1).
pub struct Migration { /* private fields */ }

/// A passphrase owned by custody and erased when dropped (E1).
pub struct Passphrase(/* private */);

impl Passphrase {
    /// Takes ownership of the passphrase without exposing it again.
    pub fn new(secret: String) -> Self;
}

/// The member's Ed25519 and X25519 pairs, erased when dropped (§11.1).
pub struct MemberKeys { /* private fields */ }

/// Every opened store and circle key, including older keys (§11, §14.3).
pub struct StoreKeyring { /* private fields */ }

/// The app's provider clients and shared clock, kept private (E10).
pub struct OAuthClients { /* private fields */ }

/// A validated encrypted-object path in the store (§4).
pub struct ObjectPath { /* private fields */ }

impl ObjectPath {
    /// A device's create-once write record (§6).
    pub fn device_log(device: DeviceId, number: NonZeroU64) -> Self;
    /// A device's create-once store log entry (§9).
    pub fn store_log(device: DeviceId, number: NonZeroU64) -> Self;
    /// A snapshot written by a device (§15).
    pub fn snapshot(audience: Audience, device: DeviceId, number: NonZeroU64) -> Self;
    /// A device's posted positions (§6).
    pub fn positions(device: DeviceId) -> Self;
    /// A sealed store key for a member (§11).
    pub fn store_key(key: KeyId, member: &MemberId) -> Self;
    /// A sealed circle key for a member (§14.3).
    pub fn circle_key(circle: CircleId, key: KeyId, member: &MemberId) -> Self;
    /// An uploaded file's encrypted bytes, under its random id (§16.2).
    pub fn file(device: DeviceId, id: FileId) -> Self;
    /// An encrypted join request under its invite id (§12.2).
    pub fn join_request(invite: InviteId) -> Self;
    /// Parses a listed or recorded path, refusing paths outside the store's layout.
    pub fn parse(value: &str) -> Result<Self, StorageError>;
    /// The path bound into the object's encryption.
    pub fn as_str(&self) -> &str;
    /// Whether this is a posted-positions path, the only kind that may be replaced.
    pub fn is_replaceable(&self) -> bool;
    /// The device named by a log, snapshot, file or positions path.
    pub fn device(&self) -> Option<DeviceId>;
}

/// A prefix of the validated object layout (§4).
pub struct ObjectPrefix { /* private fields */ }

impl ObjectPrefix {
    /// Every object in the store's location.
    pub fn all() -> Self;
    /// Every write of a device.
    pub fn device_log(device: DeviceId) -> Self;
    /// Every device's writes, to find devices not yet known (§6).
    pub fn device_logs() -> Self;
    /// Every store log entry of a device.
    pub fn store_log(device: DeviceId) -> Self;
    /// Every device's store log entries (§6, §9).
    pub fn store_logs() -> Self;
    /// Every snapshot.
    pub fn snapshots() -> Self;
    /// Every stored file.
    pub fn files() -> Self;
    /// Every waiting join request.
    pub fn join_requests() -> Self;
    /// Every sealed key.
    pub fn keys() -> Self;
    /// Every device's posted positions.
    pub fn positions() -> Self;
    /// The prefix supplied to the provider.
    pub fn as_str(&self) -> &str;
    /// Whether a validated path is under this prefix.
    pub fn contains(&self, path: &ObjectPath) -> bool;
}

/// A nonempty byte range, including its start and excluding its end (§16.3).
pub struct ByteRange { /* private fields */ }

impl ByteRange {
    /// Validates that the start is before the end.
    pub fn new(start: u64, end: u64) -> Result<Self, StorageError>;
    /// The first byte included.
    pub fn start(self) -> u64;
    /// The first byte excluded; a read refuses an end beyond the object.
    pub fn end(self) -> u64;
    /// The number of requested bytes.
    pub fn len(self) -> u64;
    /// Always false for a validated range.
    pub fn is_empty(self) -> bool;
}

/// A complete object returned by the app's CloudKit list call.
pub struct StoredObject {
    /// Validated path relative to the store's location.
    pub path: ObjectPath,
    /// Complete encrypted length in bytes.
    pub size: u64,
    /// Server publication time of the complete object, not the uploading device's clock.
    pub stored_at: SystemTime,
}

/// A durable upload prepared by the app's CloudKit bridge (§16.5).
pub struct CloudKitUpload {
    /// The bridge's recorded session capability, erased on drop.
    pub id: SecretText,
    /// The maximum part size the bridge accepts.
    pub part_size: usize,
}

/// The bridge's confirmed upload state (§16.5).
pub enum CloudKitUploadStatus {
    /// Bytes stored before publishing the complete object.
    Uploading {
        /// The contiguous stored prefix, in bytes.
        confirmed: u64,
    },
    /// This session's object has been published in the zone.
    Complete,
}

/// Native CloudKit calls implemented by the app (§4, E1).
#[async_trait]
pub trait CloudKitOps: Send + Sync {
    /// Whether the signed-in Apple account owns this store's shared zone.
    async fn is_owner(&self, location: &StorageConfig) -> Result<bool, StorageError>;
    /// Maximum encrypted body in one native call; nonzero, set by the asset representation.
    fn single_request_limit(&self) -> u64;
    /// Creates complete encrypted bytes using the server's create-only policy.
    async fn create(&self, location: &StorageConfig, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError>;
    /// Replaces a complete posted-positions object atomically (§6).
    async fn replace(&self, location: &StorageConfig, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError>;
    /// Reads the whole object or only the asset parts covering the range (§16.3).
    async fn read(&self, location: &StorageConfig, path: &ObjectPath, range: Option<ByteRange>) -> Result<Vec<u8>, StorageError>;
    /// Lists complete objects with encrypted size and server publication time,
    /// following every native query cursor; pending assets are not listed.
    async fn list(&self, location: &StorageConfig, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError>;
    /// Deletes an object and its parts; an already absent object succeeds (§18).
    async fn delete(&self, location: &StorageConfig, path: &ObjectPath) -> Result<(), StorageError>;
    /// Saves read/write CKShare participation and returns its native share URL.
    async fn grant_access(&self, location: &StorageConfig, email: &str) -> Result<SecretText, StorageError>;
    /// Removes only this account, preserving the owner and grants reaching others.
    async fn revoke_access(&self, location: &StorageConfig, email: &str) -> Result<MemberRemoval, StorageError>;
    /// Fetches share metadata, verifies container/owner/zone against location, then
    /// accepts as the signed-in recipient. Repeating acceptance succeeds.
    async fn accept_share(&self, location: &StorageConfig, url: &SecretText) -> Result<(), StorageError>;
    /// Prepares a durable upload without publishing its destination (§16.5).
    async fn begin_upload(&self, location: &StorageConfig, path: &ObjectPath, total: u64) -> Result<CloudKitUpload, StorageError>;
    /// Returns confirmed progress, including parts whose replies were lost.
    async fn upload_status(&self, location: &StorageConfig, id: &SecretText) -> Result<CloudKitUploadStatus, StorageError>;
    /// Stores a part at its byte offset; retrying identical bytes is idempotent.
    async fn upload_part(&self, location: &StorageConfig, id: &SecretText, offset: u64, bytes: &[u8]) -> Result<(), StorageError>;
    /// Publishes all parts atomically; a retry succeeds without replacing another object.
    async fn finish_upload(&self, location: &StorageConfig, id: &SecretText) -> Result<(), StorageError>;
    /// Discards pending parts without deleting a published object.
    async fn abort_upload(&self, location: &StorageConfig, id: &SecretText) -> Result<(), StorageError>;
}

/// A database or store call's result, retaining its typed cause (§20.3).
pub type CovenResult<T> = Result<T, CovenError>;

/// Failures of opening, reading and writing a store (§5, E1, E3).
pub enum CovenError {
    /// App data could not be sealed with the selected store key.
    Seal(SealError),
    /// Reading device identity or an app callback's custody operation failed.
    Key(KeyError),
    /// An app callback failed and SQLite also failed to roll it back.
    Rollback { operation: Box<CovenError>, rollback: rusqlite::Error },
    /// Cleanup also failed after an app callback failed.
    FileCleanup { write: Result<(), Box<CovenError>>, failures: Vec<DbError> },
    /// The database or a write's validation failed.
    Database(DbError),
    /// An app migration failed or cannot run on this schema.
    Migration(MigrationError),
    /// Coven's tables need a migration this open cannot run.
    CovenMigration(CovenMigrationError),
    /// The directory's settings could not be read.
    Settings(SettingsError),
    /// The writer's lock could not be acquired.
    Lock(StoreLockError),
    /// A required builder choice was not supplied (E1).
    MissingConfiguration { field: &'static str },
    /// Reloading from storage failed (§19.2).
    Sync(SyncError),
}

/// A failed database call or a write the database refuses (§5, §8, §14, §16).
pub enum DbError {
    /// This handle or a clone has closed the store (E1).
    StoreClosed,
    /// SQLite refused or failed a statement, preserving its error.
    Sqlite(rusqlite::Error),
    /// SQLite's integrity check or decoding stored facts found damage (§19.1).
    DamagedDatabase,
    /// A synced table declaration or schema is invalid.
    Schema(SchemaError),
    /// App SQL read, changed or defined one of coven's internal objects (§5).
    InternalTable { table: String },
    /// App SQL tried transaction control, a PRAGMA, ATTACH or loading an
    /// extension (§5, E13).
    StatementForbidden { operation: &'static str },
    /// SQLite rolled a migration's transaction back itself, so it can't go on (E13).
    TransactionEnded,
    /// SQLite kept another journal mode instead of WAL (§5).
    WalUnavailable { mode: String },
    /// The wall clock reads past the last time a timestamp holds, or no
    /// later timestamp is left (§7.2).
    ClockOutOfRange,
    /// A value or row exceeds the format (§5), or a write's plaintext or sealed
    /// bytes cannot fit in a SQLite value and its queue row (§6).
    TooLarge { field: &'static str, actual: u64, maximum: u64 },
    /// A local trigger wrote a synced table or a shared trigger a local one (§8.7).
    TriggerTarget { trigger: String, table: String },
    /// A reference points at a row outside the source row's audience (§14.5).
    ReferenceAudience { table: String, key: RowKey, column: String },
    /// A write targets a circle whose deletion has been applied (§14.7).
    DeletedCircle(CircleId),
    /// A write puts a row in a circle this member isn't in, or in no circle
    /// the store log has (§14.5, §14.6).
    NotInCircle(CircleId),
    /// An inserted row's independent key holds no UUID (§8.5).
    KeyNotUuid { table: String, key: RowKey },
    /// A downloaded write fails the merge's checks, such as a timestamp no
    /// later than a write it had read; it is never applied (§19.1).
    InvalidWrite { write: WriteId, error: MergeError },
    /// A waiting record or its conversion violates the write format (E13).
    WriteFormat(coven_format::Error),
    /// A conversion changed or introduced a reference whose generation it cannot know (E13).
    MigrationReference { table: String, column: String },
    /// A converted row names the same column twice.
    MigrationDuplicateColumn { table: String, column: String },
    /// A converted column has old/new values incompatible with its operation.
    MigrationColumnOperation { table: String, column: String, op: ChangeOp, has_old: bool, has_new: bool },
    /// A converted column is absent from the target schema.
    MigrationColumnMissing { table: String, column: String },
    /// A converted table has no primary index.
    MigrationPrimaryKeyMissing { table: String },
    /// A converted key has the wrong number of values.
    MigrationKeyArity { table: String, expected: usize, actual: usize },
    /// A converted row or reference names an absent table.
    MigrationTableMissing { table: String },
    /// Distinct original references became the same converted reference.
    MigrationReferenceCollision { table: String, reference: coven_merge::ForeignKey },
    /// A waiting write is not older than the migration converting it.
    MigrationWriteVersion { write: coven_merge::WriteId, schema_version: u32, migration_version: u32 },
    /// A waiting upload disappeared while its conversion was being recorded.
    MigrationUploadMissing { write: coven_merge::WriteId },
    /// A supplied store-log entry cannot be encoded as a checked entry (§9).
    InvalidStoreLogEntry { entry: EntryId, error: coven_format::Error },
    /// A replay result does not cover exactly the stored entries and the incoming entry.
    StoreLogEntriesChanged,
    /// An already applied entry was supplied again with different bytes or author-view check.
    StoreLogEntryChanged(EntryId),
    /// A write changes a file declared write-once (E2).
    FileWriteOnce { table: String, key: RowKey },
    /// A file reference no longer names the row's file (§16.3).
    FileRefChanged { table: String, key: RowKey },
    /// The row's declared size differs from the prepared file (§16).
    FileSizeMismatch { expected: u64, actual: u64 },
    /// The user's file is no longer at the recorded path (§16.1).
    UserFileMissing { path: PathBuf },
    /// The user's file changed during preparation or since it was prepared (E3).
    UserFileChanged { path: PathBuf },
    /// App SQL tried to assign a hash or where-column, including NULL (§16.1).
    FileColumnWrite { table: String, column: String },
    /// A batch supplies the same namespace and file id twice.
    FileBatchDuplicate { namespace: String, id: String },
    /// A batch names no app-provided file declaration.
    FileNamespaceNotAppProvided { namespace: String },
    /// The id source reused the physical name of a kept file.
    FileNameReused { name: FileName },
    /// No row in the namespace names a supplied file.
    FileUnreferenced { namespace: String, id: String },
    /// A file operation targets a table that is not synced.
    FileTableNotSynced { table: String },
    /// A synced table has no declared file columns.
    FileNotDeclared { table: String },
    /// Registration or clearing requires a user-provided declaration.
    FileTableNotUserProvided { table: String },
    /// A file lookup has the wrong number of primary-key values.
    FileKeyArity { table: String, expected: usize, actual: usize },
    /// A file lookup supplies NULL for a primary-key column.
    FileKeyNull { table: String, column: String },
    /// An attached file has no id.
    FileIdMissing { column: String },
    /// The size column is not a nonnegative integer byte count.
    FileSizeInvalid { column: String, value: rusqlite::types::Value },
    /// A trigger removed a row while coven filled its file columns.
    FileRowRemoved { table: String, key: RowKey },
    /// A row no longer names the file attached during this write.
    FileAttachmentChanged { table: String, key: RowKey },
    /// Hash and location are neither both NULL nor both set.
    FileHashLocationMismatch { table: String, key: RowKey },
    /// A write changes an existing file without supplying its bytes.
    FileBytesRequired { table: String, key: RowKey },
    /// The requested row is absent or carries no file.
    FileAbsent { table: String, key: RowKey },
    /// Reading or keeping file bytes failed (E3).
    Disk(DiskError),
    /// Retaining the store while staging file bytes failed.
    Lock(StoreLockError),
    /// Removing owned bytes or their pending records failed. A committed write
    /// stays committed; an unsuccessful write retains its original error (§16.6).
    FileCleanup { write: Result<(), Box<DbError>>, failures: Vec<DbError> },
    /// A transaction failed and rolling it back failed too.
    Rollback { operation: Box<DbError>, rollback: rusqlite::Error },
    /// Closing failed for these connections, after every one was tried (E1).
    Closing { failures: Vec<DbError> },
}

/// A schema rule checked on open and after migrating (§8, §14.1).
pub enum SchemaError {
    /// SET DEFAULT is on a synced table or references one (§8.4).
    /// `key` names the referencing columns and referenced table and columns as SQL.
    SetDefault { table: String, key: String },
    /// A declared synced table isn't in the database (E2).
    MissingTable { table: String },
    /// Two declarations name one table (E2).
    DuplicateTable { table: String },
    /// A table declares both `audience_column` and `audience_from` (E2).
    TwoAudiences { table: String },
    /// A declared file column isn't in the table (E2).
    FileColumn { table: String, column: String },
    /// A hash or where-column cannot represent a row without a file (§16.1).
    FileColumnNotNullable { table: String, column: String },
    /// SET NULL would change one file column alone (§16.1).
    FileForeignKeyAction { table: String, column: String },
    /// A trigger declared shared isn't on the table (§8.7).
    MissingTrigger { table: String, trigger: String },
    /// A synced table has no primary key (§8.5).
    NoPrimaryKey { table: String },
    /// A primary key column allows NULL (§8.5).
    NullableKey { table: String, column: String },
    /// SET NULL would put NULL in a NOT NULL column (§8.4).
    ImpossibleAction { table: String, column: String },
    /// A local table's foreign key could stop coven deleting a synced row (§8.4).
    LocalChildAction { table: String, column: String },
    /// SQLite chooses the primary key itself (§8.5).
    GeneratedPrimaryKey { table: String },
    /// An independent key has no text column to hold its UUID (§8.5).
    IndependentKeyNotUuid { table: String },
    /// Declared key columns do not match the table's primary key (E2).
    KeyColumns { table: String },
    /// SET NULL acts on a primary-key column (§8.4).
    PrimaryKeyAction { table: String, column: String },
    /// The audience root column is nullable or is not text (§14, E2).
    AudienceColumn { table: String, column: String },
    /// The audience foreign key spans more than one column (§14.1).
    AudienceForeignKeyColumns { table: String },
    /// The audience foreign key does not point into a synced table (§14.1).
    AudienceForeignKeyTarget { table: String, column: String },
    /// The audience foreign key uses SET NULL (§14.1).
    AudienceForeignKeyAction { table: String, column: String },
    /// Following audience foreign keys forms a loop (§14.1).
    AudienceCycle { table: String },
    /// A unique constraint or shared key spans audiences (§14.1).
    AudienceConstraint { table: String, constraint: String },
    /// A shared trigger lacks WHEN NOT coven_applying() (§8.7).
    SharedTriggerGuard { table: String, trigger: String },
}

/// Coven's local tables cannot be used at this version (§17.2, E1).
pub enum CovenMigrationError {
    /// Opening would need to migrate, but this open refuses it.
    Pending,
    /// A migration failed and its transaction rolled back.
    Failed { source: Box<DbError> },
}

/// A filesystem failure, including whether bytes already changed (E1, §20.3).
pub enum FileError {
    /// The operation failed without replacing its target.
    Io { operation: &'static str, path: PathBuf, source: Box<dyn std::error::Error + Send + Sync> },
    /// Replacement is visible, but syncing its directory failed.
    AfterReplace { path: PathBuf, source: Box<dyn std::error::Error + Send + Sync> },
    /// Removal is visible, but syncing its directory failed.
    AfterRemove { path: PathBuf, source: Box<dyn std::error::Error + Send + Sync> },
    /// Removing an unpublished temporary file failed too.
    Cleanup { operation: Box<FileError>, cleanup: Box<dyn std::error::Error + Send + Sync> },
}

/// File reads and custody report the same filesystem failures (§16, E1).
pub type DiskError = FileError;

/// The store's directory settings could not be read or written (E1).
pub enum SettingsError {
    /// No settings file exists.
    Missing(StoreId),
    /// The settings do not contain the declared data.
    Corrupt(Box<dyn std::error::Error + Send + Sync>),
    /// Settings name a different store from the directory.
    WrongStore { expected: StoreId, actual: StoreId },
    /// The settings path is not a regular file.
    NotRegularFile(StoreId),
    /// The filesystem operation failed.
    File(FileError),
}

/// The requested writer, reader or deletion lock could not be taken (E1).
pub enum StoreLockError {
    /// Another handle or process holds the lock.
    AlreadyOpen(StoreId),
    /// An explicit recovery has not published its replacement database.
    RecoveryPending(StoreId),
    /// An unfinished installation must be resumed with restore or join.
    BootstrapPending(StoreId),
    /// A supplied lock protects a different directory.
    WrongDirectory(StoreId),
    /// Opening or locking the lock file failed.
    File(FileError),
}

/// Explicit damaged-database recovery failed (§19.2).
pub enum RecoveryError {
    /// Opening SQLite, custody or the local directory failed.
    Local(CovenError),
    /// Authenticating and loading storage failed.
    Sync(SyncError),
    /// Recovery requires unlocked store keys before moving database files.
    NoStoreKeys,
}

/// Listing the app's stores failed (E1).
pub enum StoreLayoutError {
    /// Listing directories or reading settings failed.
    File(FileError),
}

/// Creating a store failed, with publication and rollback made explicit (E1).
pub enum StoreCreationError {
    /// Keeping the device-only identity failed before publication.
    Initialization { id: StoreId, source: KeyError },
    /// Removing that identity after an unpublished failure also failed.
    InitializationCleanup { operation: Box<StoreCreationError>, cleanup: KeyError },
    /// A directory or file already occupies this store id.
    AlreadyExists(StoreId),
    /// A file operation failed before publication.
    File(FileError),
    /// Writing the store's settings failed before publication.
    Settings(SettingsError),
    /// The store is visible, but syncing its parent directory failed.
    Published { id: StoreId, source: std::io::Error },
    /// Removing the unpublished directory failed too.
    Rollback { operation: Box<StoreCreationError>, cleanup: std::io::Error },
}

/// Deleting the local store stopped at a step the caller may retry (E1).
pub enum StoreDeletionError {
    /// The store is open or its lock could not be taken.
    Lock(StoreLockError),
    /// Removing a keychain entry failed.
    Key(KeyError),
    /// Removing the store directory failed.
    File(FileError),
}

/// Unlocking, keeping or forgetting keys or host secrets failed (E1, E11).
pub enum KeyError {
    /// The device-only installation id is malformed.
    DeviceIdEncoding,
    /// This handle's custody has closed.
    StoreClosed,
    /// The custody file operation failed.
    File(FileError),
    /// A cryptographic service was unavailable or stored bytes failed validation.
    Crypto(CryptoError),
    /// The opened key material was malformed.
    Material(MaterialError),
    /// The passphrase was wrong or the custody file was altered.
    PassphraseAuthentication,
    /// The passphrase file's header was malformed or unsupported.
    PassphraseHeader,
    /// The stored passphrase settings exceed accepted resource bounds.
    PassphraseParameters,
    /// The device lacks resources needed to unlock or keep keys.
    Unavailable(Box<dyn std::error::Error + Send + Sync>),
    /// The OS credential store refused the call.
    Keychain(KeychainError),
    /// The keyring service was not registered at startup.
    ServiceNotRegistered,
    /// A different service name was already registered.
    ServiceAlreadyRegistered,
    /// The service name is empty or contains NUL.
    InvalidServiceName,
    /// This platform has no native credential store.
    UnsupportedKeyringPlatform,
    /// The host secret name cannot name an app entry.
    SecretName(SecretNameError),
    /// A stored host secret is not UTF-8.
    HostSecretEncoding,
}

/// Secret bytes crossing custody or code boundaries, erased when dropped (§11, §12).
pub struct SecretBytes { /* private fields */ }

impl SecretBytes {
    /// Takes ownership of bytes, including their allocation capacity.
    pub fn new(bytes: Vec<u8>) -> Self;
    /// Borrows bytes for custody or a restore code.
    pub fn as_bytes(&self) -> &[u8];
}

/// Secret text, erased when dropped and redacted in diagnostics (§11, E10).
pub struct SecretText { /* private fields */ }

impl SecretText {
    /// Takes ownership of text, including its allocation capacity.
    pub fn new(text: String) -> Self;
    /// Borrows text for a provider request or key custody.
    pub fn as_str(&self) -> &str;
}

/// A native keychain cause whose diagnostics do not expose secret bytes (§11).
pub struct KeychainError(/* private */);

/// A host secret name is invalid (E11).
pub enum SecretNameError {
    /// The name is empty.
    Empty,
    /// The name contains a colon.
    Separator,
    /// The name is reserved for coven.
    Reserved,
    /// Native credential APIs cannot represent NUL in a name.
    Nul,
}

/// A cryptographic operation failed (§11.1).
pub enum CryptoError {
    /// The device cannot provide a cryptographic service needed by this call.
    Unavailable(Box<dyn std::error::Error + Send + Sync>),
    /// The ciphertext, key, path, index or associated data did not authenticate.
    Authentication,
    /// A sealed value was truncated or had invalid framing.
    Malformed,
    /// The stored object has an unrecognized kind.
    UnknownKind(u8),
    /// The stored object uses a format version this reader does not support.
    UnsupportedVersion(u16),
    /// The X25519 public key has low order.
    WeakSealingKey,
    /// The Ed25519 public key is invalid or weak.
    InvalidMemberId,
    /// The member's signature did not verify.
    Signature,
    /// Authenticated key material had the wrong shape.
    Material(MaterialError),
}

/// A key or encoded secret has an invalid representation, or isn't held (§11).
pub enum MaterialError {
    /// The encoded secret or sealed value is malformed.
    Encoding,
    /// A different key is already held under this store key id.
    StoreKeyConflict(KeyId),
    /// A different key is already held under this circle and key id.
    CircleKeyConflict { circle: CircleId, key: KeyId },
    /// The keyring does not hold this store key.
    UnknownStoreKey(KeyId),
    /// The keyring does not hold this circle key.
    UnknownCircleKey { circle: CircleId, key: KeyId },
}

/// A store or circle key's random id, named by the store log entry that
/// brings the key in (§11).
pub struct KeyId(pub Uuid);

/// Registers the OS keychain service every key and secret is stored under.
/// Called once at startup, before any store opens.
pub fn set_keyring_service(name: impl Into<String>) -> Result<(), KeyError>;

impl StoreDir {
    /// The store identified by this directory.
    pub fn id(&self) -> StoreId;
}

impl StoreLayout {
    /// The stores under `app_dir`, one directory each.
    pub fn new(app_dir: PathBuf) -> Self;

    /// The stores on this device, by id and name.
    pub async fn stores(&self) -> Result<Vec<StoreInfo>, StoreLayoutError>;

    /// The directory of the store with `id`.
    pub fn store_dir(&self, id: &StoreId) -> StoreDir;
}

pub struct Coven;

impl Coven {
    /// Makes a new store on this device, named `name`: its directory, its id
    /// and this device's id, both from `ids`. Storage is set up after opening
    /// (E5).
    pub async fn create_store(
        layout: &StoreLayout,
        name: &str,
        ids: IdSourceRef,
    ) -> Result<StoreDir, StoreCreationError>;

    /// Collects the choices for opening, restoring or joining a store in this layout.
    pub fn builder(layout: StoreLayout) -> CovenBuilder;

    /// Deletes a closed store from this device: every keychain entry coven
    /// holds for it, including the named host secrets, then its directory.
    /// Refused while a writer, read-only handle, file stream or outstanding
    /// file I/O retains its store lock; storage is untouched. Retrying finishes
    /// a deletion that failed partway.
    pub async fn delete_store(
        store_dir: &StoreDir,
        host_secret_names: &[&str],
    ) -> Result<(), StoreDeletionError>;
}

impl CovenBuilder {
    /// The tables that sync (E2). Required.
    pub fn synced_tables(self, tables: Vec<SyncedTable>) -> Self;

    /// The app's schema migrations, numbered from 1 with no gaps (E13).
    /// Required.
    pub fn migrations(self, migrations: Vec<Migration>) -> Self;

    /// Whether opening may migrate coven's own tables to this version of
    /// coven (§17.2). Required by `open`.
    pub fn coven_migration_policy(self, policy: CovenMigrationPolicy) -> Self;

    /// The wall clock that timestamps use (§7.2). Defaults to the system clock.
    pub fn clock(self, clock: ClockRef) -> Self;

    /// The source of new ids (§20.2). Defaults to `UuidIds`, random UUIDs.
    pub fn id_source(self, ids: IdSourceRef) -> Self;

    /// An already connected provider capability shared by operations and files
    /// at the composition root. The adapter owns its settings and credentials.
    pub fn storage(self, storage: Arc<dyn coven_storage::Storage>) -> Self;

    /// The app's own OAuth clients for Google Drive, Dropbox and OneDrive.
    /// Coven ships none.
    pub fn oauth_clients(self, clients: OAuthClients) -> Self;

    /// The app's CloudKit calls, on Apple platforms. Every iCloud operation
    /// goes through them.
    pub fn apply_cloudkit_ops(self, ops: Option<Arc<dyn CloudKitOps>>) -> Self;

    /// How many file uploads run at once. Defaults to one.
    pub fn max_concurrent_uploads(self, n: NonZeroUsize) -> Self;

    /// How many file downloads a pin runs at once (E8). Defaults to one.
    pub fn max_concurrent_downloads(self, n: NonZeroUsize) -> Self;

    /// Where this device keeps the store keys: the OS keychain by default, a
    /// file sealed with a passphrase, memory for this session only, or the
    /// app's own `StoreKeyCustody`.
    pub fn key_custody(self, custody: KeyCustody) -> Self;

    /// Where this device keeps its member's keys, with the same four choices
    /// and the app's own `MemberKeyCustody` as the last.
    pub fn identity_custody(self, custody: IdentityCustody) -> Self;

    /// Opens the store for reading and writing, taking the store's lock.
    /// Opening runs migrations and resumes unfinished operations and committed
    /// file work. An empty journal needs no keys; resumed steps read keys when
    /// needed. Local database calls need no unlocked key. Opening does not start
    /// the sync loop; `start_sync` starts it. A store with storage set up
    /// opens as `Stopped`; otherwise its status is `Disconnected`.
    pub async fn open(self, store: StoreId) -> CovenResult<CovenHandle>;

    /// Opens a store whose database is damaged (§19.2): moves the damaged
    /// file aside, loads the latest snapshot, and queues the waiting writes it
    /// can still read from the old file, then resumes unfinished operations.
    /// It needs storage and the store key; without either it fails, leaving
    /// the damaged file where it was.
    pub async fn open_reloading(self, store: StoreId) -> Result<CovenHandle, RecoveryError>;

    /// Opens the store for reading only, alongside a handle that has it open,
    /// for example from another process. Its shared lock prevents deletion
    /// while its read connections and local cache connection remain open.
    /// It runs no migration and refuses a database whose schema is newer than
    /// its migrations or whose coven tables need migrating.
    pub async fn open_read_only(self, store: StoreId) -> CovenResult<CovenReadHandle>;
}

pub enum KeyCustody {
    Keyring,
    Passphrase(Passphrase),
    /// Starts empty; opening keys keeps them only for this handle’s session.
    InMemory,
    Custom(Arc<dyn StoreKeyCustody>),
}

pub enum IdentityCustody {
    Keyring,
    Passphrase(Passphrase),
    /// Starts empty; initialization, restore or join fills this session’s custody.
    InMemory,
    Custom(Arc<dyn MemberKeyCustody>),
}

pub enum CovenMigrationPolicy {
    /// Migrate coven's tables on open.
    ApplyPending,
    /// Fail to open with `CovenMigrationError::Pending` instead.
    RefusePending,
}

impl CovenHandle {
    /// Closes the store: stops syncing, operation and file work, closes every
    /// connection and releases the writer lock. Open file streams and outstanding file I/O
    /// retain their shared deletion guards. Later database calls on any clone
    /// fail with `DbError::StoreClosed`; custody calls fail with `KeyError::StoreClosed`.
    /// Reports connection-close failures; cancellation does not stop closing.
    pub async fn close(&self) -> Result<(), DbError>;
}
```

Example:

```rust
coven::set_keyring_service("com.example.notes")?;

let layout = StoreLayout::new(app_dir);
let store_dir = Coven::create_store(&layout, "Household", Arc::new(UuidIds)).await?;

let handle = Coven::builder(layout.clone())
    .synced_tables(tables())                        // E2
    .migrations(migrations())                       // E13
    .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
    .open(store_dir.id())
    .await?;
```

### E2 Declaring synced tables

- Table names in declarations match SQLite identifiers without regard to ASCII
  case. Opening records SQLite's spelling, which identifies the table in writes,
  snapshots and file retention.
- Each synced table declares its kind of key ([§8.5](coven.md#85-keys-and-uniqueness)),
  how its rows get their audience ([§14](coven.md#14-audiences)), and whether its rows
  carry a file ([§16](coven.md#16-files)).
- A table declares at most one of `audience_column` and `audience_from`;
  opening refuses both with `SchemaError::TwoAudiences`. A table that
  declares neither is in the store.

```rust
pub enum RowIdentity {
    /// Each new row gets a UUID, version 4 or 7, in canonical lowercase form.
    IndependentUuid,
    /// The app derives the key from what makes the row unique, so equal keys
    /// are one row on every device.
    SharedKey,
}

impl SyncedTable {
    /// Declares a synced table and its kind of key.
    pub fn new(name: impl Into<String>, identity: RowIdentity) -> Self;

    /// The columns of its primary key, in order, matching the table's
    /// PRIMARY KEY. Without this call the key is the one column `id`.
    pub fn key_columns<I, S>(self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>;

    /// Makes the table a root: its text `column` holds each row's audience,
    /// `store` or a circle's id, and is never NULL (§14).
    pub fn audience_column(self, column: impl Into<String>) -> Self;

    /// A descendant: each row takes the audience of the row its
    /// `foreign_key` column points at (§14.1).
    pub fn audience_from(self, foreign_key: impl Into<String>) -> Self;

    /// The table's rows carry a file, declared by `declaration`.
    pub fn carries_files(self, declaration: FileDecl) -> Self;

    /// Declares the trigger `name` on this table as shared (§8.7); every
    /// trigger not declared shared is local. Opening refuses a shared trigger
    /// without `WHEN NOT coven_applying()`, naming it.
    pub fn shared_trigger(self, name: impl Into<String>) -> Self;
}

pub enum Provenance {
    /// The user's own file at a path on their device; coven records it and
    /// never copies, changes or deletes it (§16.1).
    UserProvided,
    /// Bytes the app hands to coven, which keeps and owns them.
    AppProvided,
}

pub enum CacheFill {
    /// Devices download an uploaded file as soon as its row arrives.
    CacheEager,
    /// Devices download it on first read.
    CacheLazy,
}

/// One table's file columns, namespace, kind and cache choice (§16, E2).
pub struct FileDecl { /* private fields */ }

impl FileDecl {
    /// Declares the file a table's rows carry: its namespace, which groups
    /// files in the cache, each with its own budget (E8), its kind, and when
    /// devices download it. Every attached file is queued for upload as the
    /// attaching write commits, including while storage is disconnected.
    pub fn new(
        namespace: impl Into<String>,
        provenance: Provenance,
        fill: CacheFill,
    ) -> Self;

    /// The column naming the file. Defaults to `id`.
    pub fn with_id_column(self, column: impl Into<String>) -> Self;

    /// The column holding the file's size in bytes. Defaults to `size`.
    pub fn with_size_column(self, column: impl Into<String>) -> Self;

    /// The column holding the file's content hash, the SHA-256 of its bytes,
    /// which coven fills in (§16.2). Must allow NULL; app SQL cannot assign it.
    /// Defaults to `hash`.
    pub fn with_hash_column(self, column: impl Into<String>) -> Self;

    /// The column holding where the file is, which coven fills in:
    /// `uploaded` with the file's id and key, or the id of the device that
    /// attached it, while waiting to upload (§16.1, §16.2). Read it through
    /// `FileRef::location`.
    /// Must allow NULL; app SQL cannot assign it. Defaults to `location`.
    pub fn with_location_column(self, column: impl Into<String>) -> Self;

    /// Refuses a write that points an existing row at a different file.
    pub fn write_once(self) -> Self;
}
```

Example:

```rust
fn tables() -> Vec<SyncedTable> {
    vec![
        // A root: each note is the store's or a circle's.
        SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
        // Descendants of notes. Each attachment carries the user's own file.
        SyncedTable::new("attachments", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .carries_files(FileDecl::new("attachments", Provenance::UserProvided, CacheFill::CacheLazy)),
        // A thumbnail the app makes, in the note's audience.
        SyncedTable::new("thumbnails", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .carries_files(FileDecl::new("thumbnails", Provenance::AppProvided, CacheFill::CacheEager)),
        // In the store, with keys from the tag's name.
        SyncedTable::new("tags", RowIdentity::SharedKey),
        // A shared key over two columns, which includes note_id (§14.1).
        SyncedTable::new("note_tags", RowIdentity::SharedKey)
            .key_columns(["note_id", "tag_id"])
            .audience_from("note_id"),
    ]
}
```

### E3 Writing

- A write runs the app's SQL in one transaction ([§5](coven.md#5-local-database)).
- The closure returns the write's result; an error rolls the whole write
  back, files included.

```rust
/// App-provided files staged for one write; a failure discards them (E3).
pub struct WriteBatch { /* private fields */ }

/// A transaction's SQL access, borrowing its connection and write's file state (§5, E3).
pub struct SqlContext<'connection, 'write> { /* private fields */ }

/// A user's file checked before a write; callers cannot change its recorded facts (E3).
pub struct PreparedUserFile { /* private fields */ }

/// The facts recorded for a user's original file (§16.1).
pub struct UserFile {
    /// The user's path, which coven never changes or deletes.
    pub path: PathBuf,
    /// Its size in bytes.
    pub size: u64,
    /// Its recorded modification time.
    pub modified_at: SystemTime,
}

/// A row's file at the time it was read; its captured facts are private (§16.3).
pub struct FileRef { /* private fields */ }

impl CovenHandle {
    /// Runs one write.
    pub async fn write<F, R>(&self, sql: F) -> CovenResult<R>
    where
        F: FnOnce(SqlContext<'_, '_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static;

    /// Runs one write that also hands coven app-provided files. `build` adds
    /// the files, then `sql` runs the write that refers to them. Streams are
    /// read asynchronously before taking the transaction's writer; close waits
    /// for staging and its cleanup, including cancellation.
    pub async fn write_with_files<F, S, R>(&self, build: F, sql: S) -> CovenResult<R>
    where
        F: FnOnce(&mut WriteBatch) -> CovenResult<()> + Send + 'static,
        S: FnOnce(SqlContext<'_, '_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static;
}

impl WriteBatch {
    /// Hands coven an app-provided file's bytes, kept under `namespace` and
    /// `id`. `bytes` is a byte buffer, or a stream read once, so a large file
    /// never has to fit in memory. The write must give a row of a table
    /// declared under `namespace` this id in its id column; a staged file
    /// no row names, or a row naming an id neither staged nor kept, fails
    /// the write. Coven fills that row's size, hash and where-columns.
    pub fn put_file(
        &mut self,
        namespace: impl Into<String>,
        id: impl Into<String>,
        bytes: impl Into<FileSource>,
    );
}

pub enum FileSource {
    Bytes(Vec<u8>),
    Stream(Pin<Box<dyn AsyncRead + Send>>),
}

impl SqlContext<'_, '_> {
    /// Ordinary SQL inside the write.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize>;
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()>;
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>;
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>;

    /// Runs the app's INSERT of a row that carries a user-provided file, and
    /// records the prepared file on it.
    pub fn insert_user_file(
        &self,
        table: &str,
        key: impl Into<RowKey>,
        prepared: PreparedUserFile,
        insert_sql: &str,
        params: &[(&str, &dyn ToSql)],
    ) -> Result<(), DbError>;

    /// Records a prepared user-provided file on a row the write already has.
    /// Fails if the row's size column disagrees with the file, or the file
    /// changed since it was prepared.
    pub fn register_user_file(
        &self,
        table: &str,
        key: impl Into<RowKey>,
        prepared: PreparedUserFile,
    ) -> Result<(), DbError>;

    /// Forgets the user-provided file recorded on a row and clears its hash
    /// and where-columns to NULL. All four file columns go in the write
    /// (§16.1). The original is untouched.
    pub fn clear_user_file(&self, table: &str, key: impl Into<RowKey>) -> Result<(), DbError>;

    /// Checks that a file reference taken earlier still names the row's
    /// current file; the write fails if it doesn't. Called before changing
    /// or deleting the row.
    pub fn validate_file_ref(&self, reference: &FileRef) -> Result<(), DbError>;
}

/// Reads a user's file once, before the write, recording its size, whole-file
/// hash and plaintext chunk hashes for uploads. `progress` receives the bytes
/// read so far. Fails if the file changes while it is read.
pub async fn prepare_user_file(
    path: &Path,
    progress: impl Fn(u64) + Send + Sync,
) -> Result<PreparedUserFile, DbError>;
```

Example:

```rust
let note_id = Uuid::now_v7().to_string();
let attachment_id = Uuid::now_v7().to_string();
let size = std::fs::metadata(&path)?.len() as i64;
let prepared = prepare_user_file(&path, |read| show_progress(read)).await?;

handle
    .write(move |sql| {
        sql.execute(
            "INSERT INTO notes (id, title, audience) VALUES (?1, ?2, 'store')",
            (&note_id, "Paint colors"),
        )?;
        sql.execute(
            "INSERT INTO attachments (id, note_id, title, size) VALUES (?1, ?2, ?3, ?4)",
            (&attachment_id, &note_id, "Swatches", size),
        )?;
        sql.register_user_file("attachments", attachment_id.as_str(), prepared)?;
        Ok(())
    })
    .await?;
```

### E4 Reading

- Reads run on several read-only connections at once ([§5](coven.md#5-local-database)).
- A *live query* runs once, then again whenever a write commits that changes
  rows it read.

```rust
/// SQL access to one read-only database snapshot (§5, E4).
pub struct SqlReadContext<'connection> { /* private fields */ }

/// An awaitable read that keeps its store borrow until it finishes (E4).
pub struct Read<'a, F> { /* private fields */ }

impl<F, R> Future for Read<'_, F>
where
    F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
    R: Send + 'static,
{
    type Output = CovenResult<R>;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output>;
}

/// A subscription's results and lifetime; dropping it ends the query (E4).
pub struct LiveQuery<T> { /* private fields */ }

/// A subscription whose request may be replaced while it runs (E4).
pub struct ReconfigurableLiveQuery<Q, T> { /* private fields */ }

/// Shared access to replacing a live query's request (E4).
pub struct LiveQueryRequests<Q> { /* private fields */ }

/// A request revision assigned by one live query (E4).
pub struct LiveQueryRevision(pub u64);

/// The query was dropped before its request could be replaced (E4).
pub struct LiveQueryClosed;

/// A result together with the request that produced it (E4).
pub struct ReconfigurableLiveQueryEvent<Q, T> {
    /// The request used by this run.
    pub request: Q,
    /// The revision assigned to that request.
    pub revision: LiveQueryRevision,
    /// A new request, changed rows, or both.
    pub cause: LiveQueryCause,
    /// The query's result, including failures that do not end the query.
    pub result: CovenResult<T>,
}

/// Why a reconfigurable live query ran; its first run answers the initial request (E4).
pub enum LiveQueryCause {
    /// A new request, including the initial one.
    Request,
    /// A write changed rows the query read.
    Write,
    /// A new request and a relevant write arrived before this run.
    RequestAndWrite,
}

/// An open file with checked identity, header and range-reading state (§16.3).
/// Retaining a stream prevents store deletion, including after the store closes.
pub struct FileStream { /* private fields */ }

/// App data could not be sealed or opened with its store key (§11, E11).
pub enum SealError {
    /// No applied store-log entry identifies the key for new app data (§11).
    NoCurrentStoreKey,
    /// This device has no store keys in custody.
    NoStoreKeys,
    /// The keyring lacks the named key or the encoded material is invalid.
    Key(MaterialError),
    /// The cipher refused the bytes or their associated data.
    Crypto(CryptoError),
    /// Lazy unlocking from custody failed (E1).
    Custody(KeyError),
}

impl CovenHandle {
    /// A read of one consistent snapshot, run when awaited. Attach `process`
    /// to work on the result after the connection is released.
    pub fn read<F, R>(&self, read: F) -> Read<'_, F>
    where
        F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static;

    /// A live query. Coven records the tables, columns and keys the query
    /// reads, and reruns it only for writes that touch them.
    pub fn subscribe<F, R>(&self, query: F) -> LiveQuery<R>
    where
        F: Fn(SqlReadContext<'_>) -> CovenResult<R> + Send + Sync + 'static,
        R: Send + 'static;

    /// A live query whose request, such as a page or a search term, can be
    /// replaced without starting a new subscription.
    pub fn subscribe_reconfigurable<Q, F, R>(
        &self,
        initial_request: Q,
        query: F,
    ) -> ReconfigurableLiveQuery<Q, R>
    where
        Q: Clone + PartialEq + Send + Sync + 'static,
        F: Fn(&Q, SqlReadContext<'_>) -> CovenResult<R> + Send + Sync + 'static,
        R: Send + 'static;

    /// Every lost value and removed row, as `_coven_lost` holds them (§8).
    /// References show what was written, even if their parents were deleted.
    pub async fn lost_values(&self) -> CovenResult<Vec<LostValue>>;

    /// Dismisses lost values the app has dealt with, in a write, so every
    /// device drops them from `_coven_lost`; a removed row is deleted for
    /// good, and never comes back (§8).
    pub async fn dismiss_lost_values(&self, values: &[LostValue]) -> CovenResult<()>;

    /// The same, as a live query.
    pub fn subscribe_lost_values(&self) -> LiveQuery<Vec<LostValue>>;
}

impl SqlReadContext<'_> {
    /// Ordinary SQL reads. A read context can't write.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>;
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>;
}

impl<'a, F, R> Read<'a, F> {
    /// Runs `process` on the read's result on a separate worker, after the
    /// read has released its connection.
    pub async fn process<P, T>(self, process: P) -> CovenResult<T>
    where
        P: FnOnce(R) -> CovenResult<T> + Send + 'static;
}

impl<T: Clone + PartialEq> LiveQuery<T> {
    /// The first result at once; after that, waits for a write that changes
    /// the result and returns the new one. An error is a result too, and
    /// doesn't end the query.
    pub async fn next(&mut self) -> CovenResult<T>;

    /// Runs `process` on each result after the connection is released.
    pub fn process<P, U>(self, process: P) -> LiveQuery<U>
    where
        P: Fn(T) -> CovenResult<U> + Send + Sync + 'static;
}

impl<Q, T: Clone + PartialEq> ReconfigurableLiveQuery<Q, T> {
    /// A handle that replaces the request.
    pub fn requests(&self) -> LiveQueryRequests<Q>;

    /// The next result, with the request and revision that produced it, and
    /// whether a new request, a write, or both caused it.
    pub async fn next(&mut self) -> ReconfigurableLiveQueryEvent<Q, T>;
}

impl<Q: Clone + PartialEq> LiveQueryRequests<Q> {
    /// Replaces the request, returning the revision whose result will answer
    /// it. Fails once the query is dropped.
    pub fn set(&self, request: Q) -> Result<LiveQueryRevision, LiveQueryClosed>;
}

/// One `_coven_lost` row (§8).
pub struct LostValue {
    pub table: String,
    pub key: RowKey,
    /// One cell's value, or a whole removed row's values.
    pub lost: Lost,
    /// What replaced it: a write that hadn't read it, the removal rules, or
    /// a breaking change or reset the write hadn't read.
    pub replaced_by: Replacement,
    /* private identity for dismissing this loss, including its audience */
}

pub enum Lost {
    Cell(LostCell),
    /// Each of the removed row's columns, with the write that set it.
    Row(Vec<LostCell>),
}

pub struct LostCell {
    pub column: String,
    /// The value as written, without foreign-key null substitution.
    pub value: rusqlite::types::Value,
    /// The write that set the value.
    pub set_by: WriteId,
}

pub enum Replacement {
    Write(WriteId),
    /// Removal rules took the row out: every one that holds (§8.4, §8.5,
    /// §8.6, §14).
    Rules(Vec<RemovalRule>),
    /// A breaking schema change the write hadn't read, named by the
    /// schema version it raised the store to (§17.1).
    SchemaChange { version: u32 },
    /// A reset the write hadn't read (§19.3).
    Reset(EntryId),
}

pub enum RemovalRule {
    ForeignKey { columns: Vec<String>, parent: String, parent_columns: Vec<String> },
    Check { constraint: String },
    /// The row is in a deleted circle (§14.7).
    DeletedCircle,
    /// The same key is present in another audience, whose row is shown
    /// (§14.2).
    OtherAudience,
    Unique { terms: Vec<String>, partial: Option<String> },
}

/// A handle that only reads, opened with `open_read_only`.
impl CovenReadHandle {
    /// Closes every connection and drops unlocked key material on all clones.
    /// Reports connection-close failures; cancellation does not stop closing.
    pub async fn close(&self) -> Result<(), DbError>;

    pub fn read<F, R>(&self, read: F) -> Read<'_, F>;
    pub async fn file_ref(&self, table: &str, key: impl Into<RowKey>) -> Result<FileRef, DbError>;
    pub async fn user_file(&self, table: &str, key: impl Into<RowKey>) -> Result<Option<UserFile>, DbError>;
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>, FileReadError>;
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError>;
    pub async fn is_pinned(&self, files: &[FileRef]) -> Result<bool, FileReadError>;
    pub fn open_app_data(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError>;
}
```

- A read handle's file reads fill the cache like any other, which changes no
  synced state.

Example:

```rust
let titles: Vec<String> = handle
    .read(|sql| Ok(sql.query("SELECT title FROM notes", [], |row| row.get(0))?))
    .process(|mut titles| {
        titles.sort();
        Ok(titles)
    })
    .await?;

let mut notes = handle.subscribe(|sql| {
    Ok(sql.query(
        "SELECT id, title, audience FROM notes ORDER BY title",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
    )?)
});
while let Ok(rows) = notes.next().await {
    show_notes(rows);
}

let mut lost = handle.subscribe_lost_values();
while let Ok(values) = lost.next().await {
    offer_to_restore(values);
}
```

### E5 Storage and sync

- *Setting up storage* creates the store at a location on a provider, the
  first time it connects, and connects this device to it.
  - At a location that already holds this store, such as after
    `disconnect_storage`, setup reconnects to it; one that holds another
    store, or anything that isn't a coven store, fails with
    `LocationOccupied`.
  - Creating uploads the store's first entry and its key sealed to this
    member; waiting writes then go up through sync like any others.
- Setup commits the storage credentials, keys, location and restore code only
  once the connection is ready. Failure preserves their previous values and
  the previous connection. The fixed first-entry or access-update attempt stays
  reserved for an identical retry: its number, plaintext, sealing key ids and
  prerequisite sealed-key bytes cannot change (§6, §18).
- Setup takes the first device's name, as joining and restore do (§10, §12).
  Reconnecting an existing device keeps its registered name. Changed provider
  access is published as `Set access` before committing the new credentials.
- A started connection runs one complete sync at a time, immediately after a
  local write or `sync_now`, and every 30 seconds while idle. Calls arriving
  during a sync request another pass; stopping and closing finish the active
  pass first.
  - A pass applies the store log and keys, resumes operations and required
    reloads, then reloads snapshots if they cover missing logs (§15).
  - It uploads waiting writes, resumes operations waiting for those uploads,
    downloads writes and completes any reload they require.
  - File uploads and eager downloads follow. Writes authored by file uploads
    are then uploaded before snapshot writing, retention and posted positions.
  - Retention uses previously confirmed posted positions; the new position is
    published last, after every preceding step has completed. File transfer
    failures remain visible through their file status and operation reports.
  - A fingerprint disagreement is reported without an automatic reload (§19.1).
- A device that isn't connected still reads and writes
  ([§3](coven.md#3-guarantees)); its writes wait in `_coven_uploads`.
- `start_sync` builds the provider client if absent, reading credentials from
  custody and refreshing expired tokens, then starts the loop. Starting an
  already running loop, or a store with no storage set up, does nothing.
- `stop_sync` finishes the active pass and file transfers, then drops the keys
  sync unlocked and all workers' references to the provider client. Credentials
  and the storage location remain available for the next `start_sync`.
- `disconnect_storage` also removes this device's storage credentials. It leaves
  storage's contents untouched; syncing requires storage setup again.

```rust
/// The provider holding a store (§4).
pub enum CloudProvider {
    /// S3, including compatible providers.
    S3,
    /// Google Drive.
    GoogleDrive,
    /// Dropbox.
    Dropbox,
    /// OneDrive.
    OneDrive,
    /// iCloud through the app's CloudKit calls.
    CloudKit,
}

/// A store's location, with credentials kept separately (§4, E5).
pub enum StorageConfig {
    /// An existing S3 bucket and the prefix reserved for this store.
    S3 {
        /// The bucket's name.
        bucket: String,
        /// The signing region.
        region: String,
        /// A compatible provider's endpoint, or None for AWS's regional endpoint.
        endpoint: Option<Url>,
        /// The store's prefix, without leading or trailing slashes.
        prefix: String,
    },
    /// A store folder in Google Drive.
    GoogleDrive { folder_id: String },
    /// A Dropbox shared folder namespace, independent of each member's mount path.
    Dropbox { namespace_id: String },
    /// A store folder in a OneDrive drive.
    OneDrive { drive_id: String, folder_id: String },
    /// The CloudKit zone reached by the app's bridge.
    CloudKit {
        /// The app's CloudKit container.
        container: String,
        /// The owner's CloudKit record name.
        owner: String,
        /// The custom zone containing the store.
        zone: String,
    },
}

impl StorageConfig {
    /// The provider of this location.
    pub fn provider(&self) -> CloudProvider;
    /// Refuses missing or invalid location information before making a request.
    pub fn validate(&self) -> Result<(), StorageError>;
}

/// Limits for concurrent file transfers (E1, E5).
pub struct TransferLimits {
    /// Maximum file uploads running at once.
    pub uploads: NonZeroUsize,
    /// Maximum file downloads a pin runs at once.
    pub downloads: NonZeroUsize,
}

/// A storage failure the app can act on, classified by `failure()` (§20.3).
pub enum StorageFailure {
    /// No route to the provider, a timeout, or an interrupted response.
    Network,
    /// Credentials were refused or must be refreshed.
    Authentication,
    /// The account is signed in but cannot perform this operation.
    PermissionDenied,
    /// The requested object is absent.
    NotFound,
    /// A create-once path is occupied.
    AlreadyExists,
    /// The bucket, folder or zone is absent.
    ContainerNotFound,
    /// The S3 endpoint or signing region is wrong.
    RegionMismatch,
    /// The provider has no space left.
    QuotaExceeded,
    /// The provider requests a later retry.
    RateLimited,
    /// The request's configuration is invalid.
    InvalidConfiguration,
    /// The provider refused the request for another reason.
    Refused,
    /// The response or recorded session is malformed.
    Protocol,
}

/// A storage error that preserves its typed cause (§4, E5).
pub enum StorageError {
    /// A classified provider failure with its original cause.
    Provider {
        /// The provider that failed.
        provider: CloudProvider,
        /// The failure the app can act on.
        failure: StorageFailure,
        /// The original transport, SDK or bridge error.
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The location settings are invalid.
    InvalidConfiguration(&'static str),
    /// An object path is outside the store's layout.
    InvalidPath,
    /// A range is empty, reversed or beyond the object's end.
    InvalidRange,
    /// An object does not exist.
    NotFound,
    /// The path already holds an object; creation never replaces it.
    AlreadyExists,
    /// Only the account holding the store may change its sharing.
    NotStoreOwner,
    /// Dropbox cannot upgrade a pending viewer until its account id is available.
    AccountIdUnavailable,
    /// The invite belongs to another provider location.
    InvitationMismatch,
    /// The upload session belongs to another provider or location.
    SessionMismatch,
    /// The provider no longer retains the recorded upload.
    SessionExpired,
    /// A part disagrees with the session's offset, size or alignment.
    InvalidPart,
    /// A posted-positions replacement exceeds the provider's single-request limit.
    SingleRequestTooLarge { size: u64, limit: u64 },
    /// A response violates the provider's protocol.
    Protocol(&'static str),
    /// Parsing recorded data failed.
    Encoding(Box<dyn std::error::Error + Send + Sync>),
    /// Persisting provider settings failed.
    File(FileError),
    /// Cleanup failed too; both causes are retained.
    Cleanup { operation: Box<StorageError>, cleanup: Box<StorageError> },
}

impl StorageError {
    /// The failure the app can act on.
    pub fn failure(&self) -> StorageFailure;
    /// True only for network interruptions and provider throttling.
    pub fn retryable(&self) -> bool;
}

/// An HTTP provider's original response, retained as a StorageError cause.
/// Bodies and headers are omitted from Display and Debug because providers
/// can echo credentials. This also retains Dropbox's asynchronous job failures
/// and malformed OneDrive progress responses, even when HTTP succeeded.
pub struct ProviderResponse { /* private */ }

impl ProviderResponse {
    /// The native HTTP status.
    pub fn status(&self) -> u16;
    /// Inspect native headers explicitly, including Retry-After.
    pub fn headers(&self) -> &reqwest::header::HeaderMap;
    /// Inspect the unmodified response body explicitly.
    pub fn body(&self) -> &[u8];
}

/// Storage setup failed before committing credentials and keys (E5).
pub enum StorageSetupError {
    /// Another store already occupies the location.
    LocationOccupied,
    /// The provider refused or failed setup.
    Storage(StorageError),
    /// Sign-in failed or was cancelled.
    OAuth(OAuthError),
    /// This device does not hold its member's keys.
    MemberKeysMissing,
    /// Keeping credentials or keys failed.
    SecureStorage(KeyError),
    /// Local setup failed, with its cause.
    Internal(Box<dyn std::error::Error + Send + Sync>),
}

/// The setup failure the app presents, classified by `failure()` (E5).
pub enum StorageSetupFailure {
    /// Sign-in did not complete or the credentials were rejected.
    Authentication,
    /// The account lacks access.
    PermissionDenied,
    /// The bucket, folder or zone is absent.
    ContainerNotFound,
    /// The S3 bucket is in a different region.
    RegionMismatch,
    /// The provider's quota is exhausted.
    QuotaExceeded,
    /// A provider or sign-in setting is invalid.
    InvalidConfiguration,
    /// Another store already occupies this location.
    LocationOccupied,
    /// Storage cannot be reached.
    Network,
    /// This device lacks its member's keys.
    MemberKeysMissing,
    /// Key or credential custody failed.
    SecureStorage,
    /// A setup step failed internally.
    Internal,
}

/// Opening the member's sealed store key failed (§11, E5).
pub enum StoreKeyUnlockError {
    /// No storage is connected.
    NoStorage,
    /// Identity custody has no member keys.
    MemberKeysMissing,
    /// Reading the sealed key failed.
    Storage(StorageError),
    /// The sealed key could not be opened or checked.
    Crypto(CryptoError),
    /// Unlocking or keeping keys in custody failed.
    SecureStorage(KeyError),
}

/// Sync or a store-log change failed (§9, §13, §17, E5).
pub enum SyncError {
    /// The proposed entry violates its byte format.
    Format(coven_format::Error),
    /// A required key is absent from custody, or its material conflicts.
    Key(coven_crypto::MaterialError),
    /// This install has no member keys in custody.
    MissingMemberKeys,
    /// A required sealed key has not arrived; retry after acquiring it.
    KeyUnavailable(KeyId),
    /// A key introduction reused an immutable key identity.
    KeyAlreadyUsed(KeyId),
    /// Replay already rejects the proposed change in the author's applied view.
    Rejected(DropReason),
    /// An object required by the change failed its checks.
    Damaged(DamagedObject),
    /// The device must stop syncing: removed, location taken, or update required.
    Stopped(SyncFailure),
    /// No storage is connected for a call that requires it.
    NoStorage,
    /// The provider refused or failed the call.
    Storage(StorageError),
    /// The local database failed.
    Database(DbError),
    /// Reading or removing credentials or keys failed.
    SecureStorage(KeyError),
    /// Opening the store key failed.
    Unlock(StoreKeyUnlockError),
    /// Making or checking signed or encrypted bytes failed.
    Crypto(CryptoError),
    /// The request or invitation is absent, expired, or no longer matches.
    InvitationChanged,
    /// The provider retained grants requiring the owner's action (E9).
    AccessRemains(Vec<RetainedAccess>),
    /// The member's role does not permit this entry (§9).
    PermissionDenied,
    /// Removing or demoting the member would leave no admin (§9).
    LastAdmin,
    /// The member's provider account holds the store, so they can't be
    /// removed (§4, §13).
    StoreOwner,
    /// The store needs a newer schema or format (§17).
    UpdateRequired,
    /// A restore code could not be decoded (E9).
    Code(CodeError),
    /// A create-store entry or credential update names another store (E9).
    WrongStore { expected: StoreId, actual: StoreId },
    /// A credential update's code names another member (E9).
    WrongMember { expected: MemberId, actual: MemberId },
    /// Circle membership is required by this operation.
    CircleNotMember(CircleId),
    /// The circle has been deleted or does not exist.
    CircleDeleted(CircleId),
    /// The target is not an active store member.
    NotStoreMember(MemberId),
    /// The requested journal row is not a blocked operation.
    NotBlocked(OperationId),
    /// Decoding persisted operation data failed, retaining its cause.
    OperationData(serde_json::Error),
}

/// How far this device has applied one device's writes (§6, E5).
pub struct DeviceActivity {
    /// The authoring device.
    pub device: DeviceId,
    /// The last applied write number; zero means none.
    pub applied_through: u64,
}

/// An object that failed a check when read (§19.1).
pub struct DamagedObject {
    /// The object's path in storage.
    pub path: String,
    /// Which check failed, retaining its cause.
    pub failure: ObjectCheckFailure,
}

/// The checks whose failure makes a stored object damaged (§19.1).
pub enum ObjectCheckFailure {
    /// The object would not decrypt or authenticate.
    Decryption(CryptoError),
    /// Its member signature did not verify.
    Signature(CryptoError),
    /// Its bytes could not be parsed.
    Parse(Arc<dyn std::error::Error + Send + Sync>),
}

/// Two devices differ after applying the same history (§19.1).
pub struct Disagreement {
    /// Both devices in id order; neither is presumed correct.
    pub devices: [DeviceId; 2],
    /// The audience compared.
    pub audience: Audience,
    /// The last applied write of each log included in the comparison.
    pub positions: Vec<WriteId>,
    /// The common store-log positions.
    pub store_log: Vec<EntryId>,
    /// The common app schema version.
    pub schema_version: u32,
}

/// This member's store log entry that was dropped during replay (§9).
pub struct DroppedEntry {
    /// The dropped entry.
    pub entry: EntryId,
    /// What it would have done.
    pub change: StoreLogChange,
    /// Why the replay dropped it.
    pub reason: DropReason,
}

/// Why a store log entry was dropped (§9).
pub enum DropReason {
    /// A conflicting concurrent entry beat it.
    BeatenBy(EntryId),
    /// At its place in the replay, the member, device or circle it changes
    /// no longer existed.
    TargetGone,
    /// Applying it would have left the store without an admin.
    NoAdminLeft,
    /// Its author's role didn't allow it, in the member list they had read.
    NotAllowed,
    /// A removal's replaced circle keys didn't name exactly the circles the
    /// removed member shared with others, in its author's view (§13).
    WrongCircleKeys,
}

pub use coven_format::store_log::SnapshotId;

/// What a dropped store log entry would have changed, for its author to see (§9).
pub enum StoreLogChange {
    /// Creates the store, names its first admin and registers the writing device.
    CreateStore { store: StoreId, name: String, admin: MemberId, device_name: String },
    /// Adds a member with this role.
    AddMember { member: MemberId, role: MemberRole },
    /// Removes the member and their devices (§13).
    RemoveMember { member: MemberId },
    /// Sets the member's role.
    SetMemberRole { member: MemberId, role: MemberRole },
    /// Records the entry author’s current storage access (§9).
    SetAccess { access: MemberAccess },
    /// Adds a device belonging to the entry's author (§10).
    AddDevice { device: DeviceId },
    /// Removes a device.
    RemoveDevice { device: DeviceId },
    /// Makes a named circle with the entry's author as its first member (E12).
    CreateCircle { circle: CircleId, name: String },
    /// Renames a circle (E12).
    RenameCircle { circle: CircleId, name: String },
    /// Deletes a circle (§14.7).
    DeleteCircle { circle: CircleId },
    /// Adds a store member to a circle (§14.3).
    AddCircleMember { circle: CircleId, member: MemberId },
    /// Removes a circle member and replaces its key (§14.6).
    RemoveCircleMember { circle: CircleId, member: MemberId },
    /// Raises the schema version of the snapshot's audience (§17.1).
    SchemaChange { version: u32, snapshot: SnapshotId },
    /// Raises the format version of the snapshot's audience (§17.2).
    FormatChange { version: u16, snapshot: SnapshotId },
    /// Resets the snapshot's audience to that snapshot (§19.3).
    Reset { snapshot: SnapshotId },
}

impl CovenHandle {
    /// Sets up storage on S3 with this member's access key (§4).
    pub async fn setup_s3_storage(
        &self,
        storage: StorageConfig,
        device_name: &str,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<ConnectedStorage, StorageSetupError>;

    /// Sets up storage on Google Drive, Dropbox or OneDrive, running the
    /// provider's sign-in with the builder's OAuth clients. `cancel` stops
    /// the sign-in.
    pub async fn setup_oauth_storage(
        &self,
        storage: StorageConfig,
        device_name: &str,
        cancel: watch::Receiver<bool>,
    ) -> Result<ConnectedStorage, StorageSetupError>;

    /// Sets up storage on iCloud, through the builder's CloudKit calls.
    pub async fn setup_cloudkit_storage(
        &self,
        storage: StorageConfig,
        device_name: &str,
    ) -> Result<ConnectedStorage, StorageSetupError>;

    /// Checks that the storage `storage` describes can be reached and used,
    /// without connecting to it.
    pub async fn probe_storage(&self, storage: &StorageConfig) -> Result<(), SyncError>;

    /// Opens the current store key from its copy sealed to this member in
    /// storage (§11), keeps it in key custody, and connects, without
    /// starting to sync.
    pub async fn unlock_store_key(&self) -> Result<ConnectedStorage, StoreKeyUnlockError>;

    /// Whether key custody holds the store key: `Available` or `Locked`.
    pub fn store_key_state(&self) -> Result<StoreKeyState, KeyError>;

    /// Finishes the active pass, removes this device's storage credentials and
    /// drops the provider client. If removing credentials fails, the connection
    /// and loop state stay. Storage's contents are untouched.
    pub async fn disconnect_storage(&self) -> Result<(), SyncError>;

    /// Starts syncing, building the provider client if absent from the configured
    /// location and custody credentials, refreshing tokens as needed. Reads keys
    /// from custody. Does nothing if already running or no storage is set up.
    pub async fn start_sync(&self) -> Result<(), SyncError>;

    /// Finishes the active pass and file transfers, then drops unlocked keys and
    /// the provider client. Keeps credentials and location for the next start.
    /// Completion publishes `Stopped`, or `Disconnected` if no storage is set up;
    /// a failure to release storage publishes `Failed`.
    pub fn stop_sync(&self);

    /// Syncs now instead of at the next idle tick. While idle, coven syncs
    /// every 30 seconds, and at once after a local write.
    pub fn sync_now(&self);

    /// The sync status, live. The first value is the current status.
    pub fn subscribe_sync_status(&self) -> watch::Receiver<SyncStatus>;

    /// How many uploads and downloads run at once.
    pub fn transfer_limits(&self) -> TransferLimits;

    /// Changes them while the store is open. Transfers already running keep
    /// the limit they started under.
    pub fn set_transfer_limits(&self, limits: TransferLimits);
}

pub enum SyncStatus {
    /// No storage is set up on this device.
    Disconnected,
    /// Storage is set up and syncing is stopped; no provider client is required.
    Stopped,
    /// Storage hasn't been reached since connecting.
    Offline,
    /// The initial sync is queued or a sync is running.
    Syncing,
    /// The last sync finished.
    Synced(SyncReport),
    /// The last sync failed as a whole.
    Failed { error: SyncFailure },
}

/// The store-log step fills `damaged_objects` and `dropped_entries`.
/// The complete sync assembles the other results from their respective steps.
pub struct SyncReport {
    pub finished_at: SystemTime,
    /// How far this device has applied each other device's log.
    pub devices: Vec<DeviceActivity>,
    /// Writes this device holds back, the writes they wait for, and since
    /// when (§19.1).
    pub waiting: Vec<WaitingWrite>,
    /// Objects that failed their check when read (§19.1).
    pub damaged_objects: Vec<DamagedObject>,
    /// Devices whose fingerprints differ from this device's at the same
    /// positions (§19.1).
    pub disagreements: Vec<Disagreement>,
    /// Operations that failed for good (§18).
    pub blocked_operations: Vec<BlockedOperation>,
    /// This member's store log entries dropped during replay, with their reasons (§9).
    pub dropped_entries: Vec<DroppedEntry>,
    /// S3 keys an admin must delete in the provider's console, until the
    /// admin confirms each is gone (§13).
    pub access_keys_to_delete: Vec<AccessKeyToDelete>,
    /// The rows the sync's writes changed, as a hint for refreshing views
    /// that aren't live queries. Not a complete list.
    pub row_changes: Option<Vec<RowChange>>,
}

/// An S3 key whose member no longer has access, or whose invite ended (§13).
pub struct AccessKeyToDelete {
    pub access_key_id: String,
    pub member: Option<MemberId>,
}

// These notices live in the device-local _coven_access_keys_to_delete table,
// outside the operation journal. confirm_access_key_deleted marks the key as
// confirmed; reports omit it, and later records cannot make it pending again.

pub struct WaitingWrite {
    pub write: WriteId,
    pub waiting_for: Vec<WriteId>,
    pub since: SystemTime,
}

/// Storage this device has set up, with whether it holds the store key.
pub struct ConnectedStorage {
    pub storage: StorageConfig,
    pub key_state: StoreKeyState,
}

pub enum StoreKeyState {
    Available,
    Locked,
}

impl StorageSetupError {
    /// The setup failure the app presents, with its cause retained in this error.
    pub fn failure(&self) -> StorageSetupFailure;
}

pub enum SyncFailure {
    /// The store's schema or format version is newer than this app (§17).
    UpdateRequired,
    /// This device, or its member, was removed from the store (§10).
    Removed,
    /// Another store was set up in this location at the same moment, first;
    /// set this one up somewhere else (§4).
    LocationTaken,
    /// Storage refused or failed a request.
    Storage(Arc<StorageError>),
    /// Anything else, with its cause.
    Other(Arc<dyn std::error::Error + Send + Sync>),
}
```

Example:

```rust
match handle.setup_s3_storage(storage, device_name, access_key_id, SecretText::new(secret_access_key)).await {
    Ok(connected) => remember(connected.storage),
    Err(error) => return show_setup_failure(error.failure()),
}

let mut status = handle.subscribe_sync_status();
loop {
    match &*status.borrow_and_update() {
        SyncStatus::Synced(report) if !report.waiting.is_empty() => show_waiting(&report.waiting),
        SyncStatus::Failed { error } => show_sync_error(error),
        other => show_status(other),
    }
    if status.changed().await.is_err() {
        break;
    }
}
```

### E6 Operations and recovery

- Every unfinished operation is a row in `_coven_operations`
  ([§18](coven.md#18-operations)).
- A failed step goes to the app call that started its operation while
  that call waits; otherwise it is reported in the sync status.

```rust
/// The local integer primary key of one unfinished operation (§18).
pub struct OperationId(pub i64);

/// The work represented by an unfinished operation (§18.1, §19.3).
pub enum OperationKind {
    /// Remove a member and replace the store key.
    RemoveMember,
    /// Remove a circle member and replace its key.
    RemoveCircleMember,
    /// Make a circle and publish its first sealed key.
    CreateCircle,
    /// Seal the circle's history and add a member.
    AddCircleMember,
    /// Delete the circle's rows and publish its deletion.
    DeleteCircle,
    /// An owner's device takes back access after applying a kept removal.
    RevokeAccess,
    /// Migrate the schema, snapshot it and raise the version.
    SchemaChange,
    /// Migrate the format, snapshot it and raise the version.
    FormatChange,
    /// Replace this device's synced data from a snapshot.
    ReloadFromSnapshot,
    /// Write a snapshot and delete covered logs and unused files.
    Snapshot,
    /// Grant access, approve or decline a join, and settle the invite.
    Invite,
    /// Snapshot and reset an audience (§19.3).
    Reset,
}

/// Who initiated an operation (§18).
pub enum StartedBy {
    /// The app call, named as in _coven_operations.started_by.
    AppCall(String),
    /// Coven's own running work.
    Coven,
}

/// Operation calls retain the same typed causes as sync calls (§18).
pub type OperationError = SyncError;

impl CovenHandle {
    /// Runs a failed operation again from the step after its last completed
    /// one. An operation whose cause still stands fails again.
    pub async fn retry_blocked_operation(&self, operation: OperationId) -> Result<(), OperationError>;

    /// Abandons a failed operation and deletes its row. Steps already done
    /// stay done; each kind's steps are ordered so other devices never see a
    /// half-done operation (§18).
    pub async fn discard_blocked_operation(&self, operation: OperationId) -> Result<(), OperationError>;

    /// Reloads this device from the latest snapshot, keeping its waiting
    /// writes, as an operation (§19.2).
    pub async fn reload_from_snapshot(&self) -> Result<(), OperationError>;

    /// Resets the store from this device's copy, as an admin (§19.3): writes
    /// a snapshot, then records the reset in the store log. Every other
    /// device reloads from that snapshot.
    pub async fn reset_store(&self) -> Result<(), SyncError>;
}

/// One `_coven_operations` row whose step failed for good.
pub struct BlockedOperation {
    pub id: OperationId,
    /// Such as removing a member, or reloading from a snapshot.
    pub kind: OperationKind,
    pub last_step: u32,
    /// The app call that started it, or coven.
    pub started_by: StartedBy,
    pub failure: String,
}
```

### E7 Moves and uploads

- A row moves to another audience by an ordinary write that changes its
  root's audience column, or points a descendant at a parent in another
  audience ([§14.2](coven.md#142-moving-rows)).
  - The write commits at once on this device; its moved rows' uploaded
    files stay where they are ([§16.1](coven.md#161-kinds-and-where-files-are)).
- Files enter the upload queue as the attaching write commits, including
  with no storage connected. Once stored, a file's where-column changes from
  the attaching device's id to `uploaded` ([§16.1](coven.md#161-kinds-and-where-files-are)).
  Pinning and the cache keep uploaded files on a device without changing
  that column.
- Upload attempts read the source again, checking its size and whole-file
  content hash before transfer, and the first-read hash of each plaintext
  chunk before encryption. A changed user original reports `UserFileChanged`;
  a changed app-provided copy reports `Integrity`. Retries retain the same
  file id, key, chunk size and chunk hashes, without an encrypted local copy.

```rust
/// Live upload-queue results, ending when the store closes (E7).
pub struct UploadsLiveQuery { /* private fields */ }

/// A file upload attempt failed (§16.5, E7).
pub enum UploadFailure {
    /// Reading or checking the source file failed.
    File(FileReadError),
    /// The provider refused or failed the upload.
    Storage(StorageError),
    /// Creating the independent file key failed.
    Crypto(CryptoError),
    /// A prior process recorded this failure category.
    Recorded(RecordedUploadFailure),
}

/// Actionable categories that can be retained across a process restart.
pub enum RecordedUploadFailure {
    NoStorage,
    File,
    Local,
    Crypto,
    Storage(StorageFailure),
}

/// The failed files and their causes from one upload drain (E7).
pub type UploadFailures = Vec<(FileRef, Arc<UploadFailure>)>;

impl CovenHandle {
    /// A live query over the upload queue: every file waiting to upload, with
    /// its progress. The first result is the current state.
    pub fn subscribe_uploads(&self) -> UploadsLiveQuery;

    /// Retries every waiting upload now, instead of after its retry delay,
    /// which starts at 1 second and doubles to at most 5 minutes.
    pub async fn retry_uploads_now(&self) -> Result<DrainOutcome, SyncError>;

    /// Pauses uploads, or resumes them. A paused upload keeps its place,
    /// including a provider upload session in progress.
    pub fn set_uploads_paused(&self, paused: bool);
}

impl UploadsLiveQuery {
    /// The current state at once, then the next state each time it changes.
    pub async fn next(&mut self) -> Result<UploadQueue, DbError>;
}

pub struct UploadQueue {
    pub paused: bool,
    /// Oldest first.
    pub files: Vec<QueuedUpload>,
}

pub struct QueuedUpload {
    pub file: FileRef,
    pub phase: UploadPhase,
    /// Failed attempts so far.
    pub attempts: u64,
    pub last_failure: Option<Arc<UploadFailure>>,
    pub queued_at: SystemTime,
    pub last_attempt_at: Option<SystemTime>,
}

pub enum UploadPhase {
    /// Not started, or waiting for its retry delay.
    Waiting,
    /// Checking the source before transfer: bytes checked of its size.
    Preparing { bytes_read: u64, bytes_total: u64 },
    /// Verifying chunks, encrypting and sending them: encrypted bytes the
    /// provider has received of the total.
    Uploading { bytes_sent: u64, bytes_total: u64 },
    /// Stored; the write that refers to it can upload (§16.5).
    Stored,
}

pub enum DrainOutcome {
    Drained { uploaded: usize, failures: UploadFailures },
    QueueEmpty,
    AllInBackoff,
    Paused,
}
```

Example:

```rust
let mut uploads = handle.subscribe_uploads();
loop {
    let state = uploads.next().await?;
    for upload in &state.files {
        if let UploadPhase::Uploading { bytes_sent, bytes_total } = upload.phase {
            show_progress(upload.file.key(), bytes_sent, bytes_total);
        }
    }
    if state.files.is_empty() {
        break; // every queued file is stored
    }
}
```

### E8 Files and the cache

- A *file reference*, `FileRef`, names one row's file as of that row's
  current version, so a later change to the row can't redirect a read.
- Coven reads a file from wherever it is: the user's original, coven's own
  copy, the cache, or storage ([§16](coven.md#16-files)).

```rust
/// Progress while keeping files whole on this device (§16.4, E8).
pub struct PinProgress {
    /// Files whose bytes have all been kept.
    pub files_completed: u64,
    /// Files requested, including those already present.
    pub files_total: u64,
    /// Bytes downloaded by this call so far.
    pub bytes_downloaded: u64,
    /// Bytes this call needs to download, excluding bytes already cached.
    pub bytes_total: u64,
}

/// A live query of whether each requested row's file is pinned (E8).
pub struct RowsPinnedLiveQuery { /* private fields */ }

impl RowsPinnedLiveQuery {
    /// Replaces the table and keys whose pin state is watched.
    pub fn set_rows(&self, table: &str, keys: Vec<RowKey>) -> Result<(), LiveQueryClosed>;
    /// The current answers, in key order, then each change (E8).
    pub async fn next(&mut self) -> Result<Vec<Option<bool>>, FileReadError>;
}

/// Downloads of files declared CacheEager (§16.4, E8).
pub enum EagerCacheFillStatus {
    /// No files are waiting to download.
    Idle,
    /// Files are being fetched into the cache.
    Downloading(PinProgress),
    /// The app stopped these downloads.
    Cancelled(PinProgress),
    /// A download failed, with the progress reached before it failed.
    Failed { progress: PinProgress, error: Arc<FileReadError> },
}

impl CovenHandle {
    /// The file a row carries, as of its current file version. Its four file
    /// columns, audience and version are read in one committed state. A row
    /// without a file returns `DbError::FileAbsent`; malformed stored
    /// file facts return `DbError::DamagedDatabase`.
    pub async fn file_ref(&self, table: &str, key: impl Into<RowKey>) -> Result<FileRef, DbError>;

    /// Reads a whole file, checking it against its row.
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>, FileReadError>;

    /// Opens a file for reading ranges (§16.3). Opening checks the file
    /// against its row once; keep the stream for as long as the file is read.
    /// Its shared store lock prevents deletion until it and its I/O finish.
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError>;

    /// Makes sure a file's bytes are on this device: an uploaded file is
    /// downloaded into the cache, and one waiting to upload here is checked.
    pub async fn ensure_file_on_device(&self, file: &FileRef) -> Result<(), FileReadError>;

    /// The path, size and modification time coven recorded for a row's
    /// user-provided file, or `None` when the row has none.
    pub async fn user_file(&self, table: &str, key: impl Into<RowKey>) -> Result<Option<UserFile>, DbError>;

    /// Keeps uploaded files whole on this device regardless of the cache
    /// budget, downloading what is missing. `on_progress` is called before
    /// the first download, as bytes arrive, and as each file is kept.
    pub async fn pin(
        &self,
        files: &[FileRef],
        on_progress: &(dyn Fn(PinProgress) + Send + Sync),
    ) -> Result<(), FileReadError>;

    /// Stops keeping files; they stay in the cache until the budget evicts them.
    pub async fn unpin(&self, files: &[FileRef]) -> Result<(), FileReadError>;

    /// Whether every file in `files` is pinned. An empty set is pinned.
    pub async fn is_pinned(&self, files: &[FileRef]) -> Result<bool, FileReadError>;

    /// Whether each row's file is pinned, one answer per key in order, or
    /// `None` for a key with no row carrying a file. A file not yet uploaded
    /// reads as not pinned.
    pub async fn rows_pinned(&self, table: &str, keys: Vec<RowKey>) -> Result<Vec<Option<bool>>, FileReadError>;

    /// The same answers, live. `set_rows` changes which rows it watches.
    pub fn subscribe_rows_pinned(&self, table: &str, keys: Vec<RowKey>) -> RowsPinnedLiveQuery;

    /// Removes an uploaded file's copies from the cache, pinned or not. Never
    /// touches a file waiting to upload, or storage; a later read
    /// downloads it again.
    pub async fn evict_file(&self, file: &FileRef) -> Result<(), FileReadError>;

    /// The cache budget for one namespace, in bytes; each namespace evicts
    /// on its own. A namespace with no budget set evicts nothing.
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError>;
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<Option<u64>, DbError>;

    /// Progress of downloading every file declared to download as soon as
    /// its row arrives that this device doesn't have yet, such as after
    /// loading a snapshot to join or recover (§16, §19.2).
    pub fn subscribe_eager_cache_fill_status(&self) -> watch::Receiver<EagerCacheFillStatus>;

    /// Stops those downloads without stopping sync.
    pub fn cancel_eager_cache_fill(&self);
}

impl FileRef {
    pub fn table(&self) -> &str;
    pub fn key(&self) -> &RowKey;
    /// The column naming the file.
    pub fn column(&self) -> &str;
    /// The file's size in bytes.
    pub fn plaintext_size(&self) -> u64;
    /// The row's audience, whose encrypted writes carry the file's key (§16.1).
    pub fn audience(&self) -> Audience;
    /// Where the file is (§16.1).
    pub fn location(&self) -> FileLocation;
}

pub enum FileLocation {
    /// The where-column holds `uploaded <device id> <file id> <key in lowercase hex>`
    /// (Appendix D12); the reference retains both privately.
    Uploaded,
    /// Waiting to upload on the device that attached it.
    OnDevice(DeviceId),
}

impl FileStream {
    /// The file's whole size in bytes.
    pub fn plaintext_size(&self) -> u64;

    /// Reads `len` bytes at `offset`. A range past the end is an error, never
    /// a short read.
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, FileReadError>;

    /// The same range as bounded plaintext buffers, without allocating its
    /// whole length. Authentication precedes each yielded buffer; a complete
    /// sequential read checks the row's content hash before the final buffer.
    pub fn read_range(&self, offset: u64, len: u64) -> Result<FileRangeStream<'_>, FileReadError>;
}

impl FileRangeStream<'_> {
    pub async fn next(&mut self) -> Result<Option<Vec<u8>>, FileReadError>;
}

pub enum FileReadError {
    /// The range needs chunks that aren't cached, and storage can't be reached.
    Offline { id: String },
    /// An uploaded file was read with no storage connected.
    NoStorage,
    /// The file is only on another device, which the app can name.
    OnOtherDevice { id: String, device: DeviceId },
    /// A user-provided file is gone from its recorded path.
    UserFileMissing { id: String, path: PathBuf },
    /// A user-provided file's size, modification time or checked content no
    /// longer matches what coven recorded.
    UserFileChanged { id: String, path: PathBuf },
    /// A chunk, or a copy on this device, failed its check.
    Integrity { id: String },
    /// The range lies outside the file.
    RangeOutOfBounds { id: String, offset: u64, end: u64, size: u64 },
    /// Storage refused or failed the request.
    Storage(StorageError),
    /// The database failed, with its cause.
    Database(DbError),
    /// The disk failed, with its cause.
    Disk(DiskError),
    /// The store cannot be retained while opening its file.
    Lock(StoreLockError),
}
```

Example:

```rust
let recording = handle.file_ref("attachments", attachment_id.as_str()).await?;
let stream = handle.open_file_stream(&recording).await?;

// A voice memo: read its header, then seek to where listening resumes.
let header = stream.read_at(0, 64 * 1024).await?;
let resume_at = position_for(&header, saved_seconds);
match stream.read_at(resume_at, 256 * 1024).await {
    Ok(bytes) => play(bytes),
    Err(FileReadError::Offline { .. }) => show_not_downloaded(),
    Err(error) => return Err(error.into()),
}
```

### E9 Members and devices

- Only admins add and remove members, and change roles; each member removes
  their own devices, and admins any device ([§9](coven.md#9-members-and-roles)).
- Removing a member is an operation ([§18.1](coven.md#181-operations)).
- Removing an account can leave access through a parent, a grant reaching other
  accounts, an unidentified recipient, or the owner. `MemberRemoval::AccessRemains`
  returns these grants and their reasons for the app to present to the owner.
  The revocation remains a blocked operation until the app retries after the
  owner changes those grants, or discards the operation to acknowledge them.
  This also preserves the result when no app call is waiting, including a
  revocation initiated by applying another device's removal.

```rust
impl CovenHandle {
    /// The members and their devices, as this device's store log has them.
    pub async fn get_members(&self) -> Result<Vec<MemberInfo>, SyncError>;

    /// On S3: switches this member to the access key they made in the
    /// provider's console, records the new key's id in the store log (§9),
    /// and returns their new restore code, for their other devices to scan
    /// and for them to write down. They delete the old key in the console
    /// (§13). Requires connected storage. If publication fails, the call
    /// returns the failure and retains the new credentials: the remote entry
    /// may already exist. Retry with the same key to finish publication.
    /// Member removal lists every recorded key until confirmed deleted,
    /// including this key if replay drops its set-access entry (§13).
    pub async fn replace_access_key(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<String, SyncError>;

    /// On a device that already has the store open: takes the storage
    /// credentials from a new restore code of this member's, and keeps
    /// everything else. A disconnected handle keeps them for its next
    /// connection; a connected handle also replaces its provider's credentials.
    pub async fn update_credentials(&self, code: &str) -> Result<(), SyncError>;

    /// Changes a member's role, as an admin (§9).
    pub async fn set_member_role(&self, member: &MemberId, role: MemberRole) -> Result<(), SyncError>;

    /// Records that the admin deleted an S3 key in the provider's console,
    /// so the sync status stops asking for it (§13).
    pub async fn confirm_access_key_deleted(&self, access_key_id: &str) -> Result<(), SyncError>;

    /// Removes a member and all their devices, rotates the store key, and
    /// revokes all their recorded storage access (§13). The result describes
    /// their current replayed access, with retained grants from any recorded
    /// account. On S3, access_keys_to_delete lists every recorded key until
    /// confirmed deleted, including keys in dropped entries.
    pub async fn remove_member(&self, member: &MemberId) -> Result<MemberRemoval, SyncError>;

    /// Removes a device. The provider can't cut off one device alone, so the
    /// result says how its member signs out of the provider and signs in
    /// again on the devices they keep (§13).
    pub async fn remove_device(&self, device: DeviceId) -> Result<ProviderSignOut, SyncError>;
}

/// Revoked sharing or remaining owner actions (§13).
pub enum MemberRemoval {
    /// This non-owner admin's removal is kept; an owner's device revokes
    /// sharing when it applies the removal.
    PendingOwner,
    /// Another active member or an open invite uses the same provider account.
    /// Its sharing must remain until that use ends.
    AccountInUse { account: String },
    /// The provider no longer shares with the account.
    Revoked,
    /// Exclusive grants were removed; these grants remain for owner action.
    AccessRemains { shares: Vec<RetainedAccess> },
    /// The admin deletes the key with this public id in the provider's console.
    DeleteAccessKey { access_key_id: String },
}

/// A grant that revocation left for the owner to inspect.
pub struct RetainedAccess {
    /// Native permission, membership or group id, scoped to this store.
    pub provider_id: String,
    /// Why this grant cannot revoke only the requested account.
    pub reason: RetainedAccessReason,
}

pub enum RetainedAccessReason {
    /// The grant also reaches other accounts, including public or group access.
    OtherAccounts,
    /// The grant comes from a parent location.
    Inherited,
    /// The provider did not identify the recipient sufficiently for safe removal.
    UnidentifiedAccount,
    /// The grant belongs to the store's owner.
    StoreOwner,
}

pub struct MemberInfo {
    pub id: MemberId,
    pub role: MemberRole,
    pub devices: Vec<DeviceId>,
    /// Whether this is the member using this device.
    pub is_self: bool,
}

pub enum MemberRole {
    Admin,
    Member,
}

/// How a removed device's member cuts off its provider access (§13).
pub enum ProviderSignOut {
    /// Remove the app's access from Google Drive, Dropbox or OneDrive, then sign in again.
    RemoveAppAccess { provider: CloudProvider },
    /// Remove the device from the Apple account.
    RemoveFromAppleAccount,
    /// Enter a new S3 key, update retained devices and the written restore code, then delete the old key.
    ReplaceAccessKey,
}
```

### E10 Joining and restore

- A person's new device opens the store from their restore code: scanned
  as a QR code, typed in, or read from iCloud Keychain
  ([§12.1](coven.md#121-a-persons-new-device)).
- An admin adds a person with an invite, and approves their join request
  ([§12.2](coven.md#122-adding-a-person)).
- Each call takes the app's layout-scoped `CovenBuilder`, plus its code,
  device name, OAuth tokens, status callback and cancellation receiver, and
  returns the open `CovenHandle`. There is no second app-side open.
  - The builder supplies tables, migrations, custody, clock, id source,
    provider clients and file-transfer limits once. Session-only `InMemory`
    custody lives with the returned handle.
  - An unfinished store is created at its permanent path with a durable
    `.coven-bootstrap` marker. Listings hide it and ordinary opens refuse it.
    Bootstrap holds its own directory capability and opens the database once;
    publication removes the marker without moving open database or custody files.
  - Keys and credentials enter final custody only after the store loads.
    Custody or publication failures roll back final custody; rollback failures
    retain both causes. Errors after publication carry the open handle,
    including its session-only keys, and retain committed custody.
  - Cancellation closes the loading database and removes the unpublished store
    without committing final keys or credentials. Dropping a waiting future
    retains encrypted work for explicit retry. There is no cancellable await
    between the final cancellation check, custody commitment, publication and
    returning the handle.
  - Restore and joining register the supplied device name. Every installation
    receives its own device id; resuming a pending join keeps that id.
  - The singular keychain call returns an ambiguity error if several stores
    are discoverable; it never chooses one arbitrarily.
  - Loading uses snapshots and logs through their owners. The file-transfer
    queue starts with the returned handle and uses the builder's limits.

```rust
/// A one-time invite's UUID, also naming its join-request object (§12.2).
pub struct InviteId(pub Uuid);

/// An uploaded file's random id, its name in storage (§16.2).
pub struct FileId(pub Uuid);

/// What the app may show before using a scanned or typed code (E10).
pub struct CodeInfo {
    /// Whether this is the person's restore code or an invite.
    pub kind: CodeKind,
    /// The store named by the code.
    pub store_id: StoreId,
    /// The store's name.
    pub store_name: String,
    /// The provider holding the store.
    pub cloud_provider: CloudProvider,
    /// Whether this provider requires sign-in before using the code.
    pub needs_oauth: bool,
}

/// The two codes used to open a store on a new device (§12).
pub enum CodeKind {
    /// Holds the person's member keys and storage credentials.
    Restore,
    /// Holds an invite id and secret; approval is still required.
    Invite,
}

/// A scanned or typed code cannot be used for this call (§12, E10).
pub enum CodeError {
    /// The code does not contain valid restore or invite data.
    Invalid,
    /// The call needs the other kind of code.
    WrongKind { expected: CodeKind, actual: CodeKind },
}

/// Opening a store on a new device failed (§12, E10).
pub enum BootstrapError {
    /// The person cancelled the call.
    Cancelled,
    /// The code cannot be used.
    Code(CodeError),
    /// Preparing, publishing or removing the unpublished directory failed.
    Directory(BootstrapDirectoryError),
    /// Loading or opening the store failed.
    Store(CovenError),
    /// Provider sign-in failed.
    OAuth(OAuthError),
    /// Reading or keeping credentials or identity keys failed.
    SecureStorage(KeyError),
    /// Provider admission, signed membership or snapshot loading failed.
    Sync(SyncError),
    /// The app must select a code explicitly when several stores are available.
    MultipleStores(Vec<StoreId>),
    /// Cleanup failed too; neither failure is hidden.
    Cleanup { operation: Box<BootstrapError>, cleanup: Box<BootstrapError> },
    /// Publication took effect, but durability or cleanup failed. The handle
    /// retains the loaded store and its session-only keys.
    Published { handle: CovenHandle, source: Box<BootstrapError> },
}

/// Provider sign-in tokens, held as secrets rather than printed (E10).
pub struct OAuthTokens {
    /// The token authorizing provider requests.
    pub access_token: SecretText,
    /// A renewal token, if the provider supplied one.
    pub refresh_token: Option<SecretText>,
    /// Expiry calculated with the injected clock, or None for a non-expiring token.
    pub expires_at: Option<SystemTime>,
}

/// A browser request plus private state retained to check its redirect (E10).
pub struct AuthorizeRequest {
    /// The URL the app opens for sign-in.
    pub auth_url: String,
    /* private fields */
}

/// Provider sign-in could not finish (E5, E10).
pub enum OAuthError {
    /// This provider is not an OAuth provider or the app supplied no client id.
    Unavailable(CloudProvider),
    /// The request's provider, redirect or client id does not match the exchange.
    RequestMismatch,
    /// The redirect state is missing or different.
    StateMismatch,
    /// The provider declined sign-in.
    Denied,
    /// The callback omitted its authorization code.
    MissingCode,
    /// The person cancelled sign-in.
    Cancelled,
    /// No callback arrived before the deadline.
    Timeout,
    /// The current tokens have expired; refresh and commit them before reuse.
    Expired,
    /// A new provider sign-in is required.
    Reauthorize,
    /// The redirect URI or callback request is malformed.
    InvalidRedirect,
    /// The provider's expiry cannot be represented.
    InvalidExpiry,
    /// The browser or local redirect listener failed.
    Io(std::io::Error),
    /// The provider refused sign-in or token exchange, or could not be reached.
    Storage(StorageError),
}

impl CovenHandle {
    /// This member's restore code: their member key, the store's id and
    /// name, and storage credentials. The app shows it as a QR code, blurred
    /// until the person taps it, and asks them to write it down at setup.
    pub async fn restore_code(&self) -> Result<String, SyncError>;

    /// Starts adding a person, as an admin, and returns the invite to show as
    /// a QR code. Expires after a day.
    pub async fn create_invite(&self, role: MemberRole, access: InviteAccess) -> Result<Invite, SyncError>;

    /// Join requests waiting for approval, live. The first value is the
    /// current list.
    pub fn subscribe_join_requests(&self) -> watch::Receiver<Vec<JoinRequest>>;

    /// Adds the person who sent `request` as a member with the invite's role,
    /// and seals the store key to them.
    pub async fn approve_join_request(&self, request: &JoinRequest) -> Result<(), SyncError>;

    /// Declines the request, and takes back the storage access its invite
    /// granted; on S3 the admin deletes the key in the provider's console.
    pub async fn decline_join_request(&self, request: &JoinRequest) -> Result<(), SyncError>;

    /// Cancels an invite before anyone joins with it, taking back the
    /// storage access it granted; on S3 the admin deletes the key in the
    /// provider's console.
    pub async fn cancel_invite(&self, invite: &InviteId) -> Result<(), SyncError>;
}

/// How the new person reaches storage (§12.2).
pub enum InviteAccess {
    /// Google Drive, Dropbox, OneDrive or iCloud: the store is shared with
    /// this account.
    ProviderAccount { email: String },
    /// S3: an access key the admin made for them in the provider's console.
    S3AccessKey { access_key_id: String, secret_access_key: SecretText },
}

pub struct Invite {
    pub id: InviteId,
    /// The code to show as a QR code.
    pub code: String,
    pub role: MemberRole,
    pub expires_at: SystemTime,
}

pub struct JoinRequest {
    pub invite: InviteId,
    /// The new member's public key.
    pub member: MemberId,
    /// The name the new device gave itself, for the admin to check.
    pub device_name: String,
    pub provider_account_email: Option<String>,
}

/// What a restore code or invite is for, before using it: the store's id and
/// name, its provider, and whether the provider needs a sign-in first.
pub fn decode_code_info(code: &str) -> Result<CodeInfo, CodeError>;

/// Opens the store on a new device from the person's restore code, scanned
/// or typed. `oauth_tokens` is the provider sign-in, when `needs_oauth`.
pub async fn restore_from_code(
    builder: CovenBuilder,
    code: &str,
    device_name: &str,
    oauth_tokens: Option<OAuthTokens>,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<CovenHandle, BootstrapError>;

/// Opens the store on a new Apple device from the iCloud Keychain item,
/// which holds what a restore code holds (§12.1). `None` when the keychain
/// holds no store.
pub async fn restore_from_keychain(
    builder: CovenBuilder,
    device_name: &str,
    oauth_tokens: Option<OAuthTokens>,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<CovenHandle>, BootstrapError>;

/// Completes the provider's recipient acceptance using the encrypted invite
/// code before sending the join request. CloudKit acceptance runs through the
/// app's CloudKitOps calls. If access is removed while joining, the provider's
/// permission failure reaches the app and an admin can invite again. Keys and
/// credentials are committed only after bootstrap succeeds.
///
/// On the new person's device: makes their member keys, writes a join
/// request to storage, and waits for the admin to approve it, then loads
/// the store. Picks up where it left off after a restart. Returns `None`
/// when the request is declined or the invite expires.
pub async fn join_with_invite(
    builder: CovenBuilder,
    code: &str,
    device_name: &str,
    oauth_tokens: Option<OAuthTokens>,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<CovenHandle>, BootstrapError>;

impl OAuthClients {
    /// Sets client ids (None for providers the app does not offer) and the sign-in clock.
    pub fn new(
        google_drive_client_id: Option<String>,
        dropbox_client_id: Option<String>,
        onedrive_client_id: Option<String>,
        clock: ClockRef,
    ) -> Self;

    /// Runs browser sign-in with a local redirect; cancellation or dropping it closes the listener.
    pub async fn authorize(
        &self,
        provider: CloudProvider,
        cancel: watch::Receiver<bool>,
    ) -> Result<OAuthTokens, OAuthError>;

    /// Builds the URL and private proof for an app that handles its own redirect.
    pub fn build_authorize_request(&self, provider: CloudProvider, redirect_uri: &str) -> Result<AuthorizeRequest, OAuthError>;
    /// Checks the redirect's state and exchanges its code using this client's clock.
    pub async fn exchange_code(
        &self,
        provider: CloudProvider,
        code: &str,
        callback_state: Option<&str>,
        request: &AuthorizeRequest,
        redirect_uri: &str,
    ) -> Result<OAuthTokens, OAuthError>;

    /// Gets replacement tokens, retaining the old refresh token when the provider omits it.
    pub async fn refresh(&self, provider: CloudProvider, tokens: &OAuthTokens) -> Result<OAuthTokens, OAuthError>;
}
```

- Refreshed tokens are committed to key custody before the provider session
  uses them.

Example, when the app handles the sign-in redirect:

```rust
let request = oauth_clients.build_authorize_request(provider, redirect_uri)?;
open_sign_in(&request.auth_url);
let (code, callback_state) = receive_sign_in_redirect().await?;
let tokens = oauth_clients
    .exchange_code(provider, &code, callback_state.as_deref(), &request, redirect_uri)
    .await?;
```

Example, adding Ana's laptop. On her phone:

```rust
let code = handle.restore_code().await?;
show_blurred_qr_code(&code);
```

On the laptop:

```rust
let info = decode_code_info(&scanned)?;
let tokens = if info.needs_oauth {
    Some(oauth_clients.authorize(info.cloud_provider, cancel_rx.clone()).await?)
} else {
    None
};
let builder = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
    .oauth_clients(oauth_clients.clone())
    .apply_cloudkit_ops(cloudkit_ops.clone())
    .clock(clock.clone())
    .id_source(ids.clone());
let handle = restore_from_code(
    builder,
    &scanned,
    "Ana’s laptop",
    tokens,
    |step| show_step(step),
    &cancel_rx,
)
.await?;
```

Example, Ana adding Carol. On Ana's phone:

```rust
let invite = handle
    .create_invite(
        MemberRole::Member,
        InviteAccess::ProviderAccount { email: "carol@example.com".into() },
    )
    .await?;
show_qr_code(&invite.code);

let mut requests = handle.subscribe_join_requests();
loop {
    for request in requests.borrow_and_update().clone() {
        if request.invite == invite.id {
            if confirm(&request.device_name).await {
                handle.approve_join_request(&request).await?;
            } else {
                handle.decline_join_request(&request).await?;
            }
        }
    }
    requests.changed().await?;
}
```

On Carol's phone:

```rust
let tokens = oauth_clients.authorize(CloudProvider::GoogleDrive, cancel_rx.clone()).await?;
let builder = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
    .oauth_clients(oauth_clients.clone());
match join_with_invite(
    builder,
    &scanned,
    "Carol's phone",
    Some(tokens),
    |step| show_step(step),
    &cancel_rx,
)
.await?
{
    Some(handle) => show_store(handle),
    None => show_declined(),
}
```

### E11 Keys and secrets

```rust
/// Initializing this device's member identity failed (E11).
pub enum IdentityError {
    /// Custody already holds member keys.
    AlreadyInitialized,
    /// Making the two key pairs failed.
    Crypto(CryptoError),
    /// Reading or persisting identity custody failed.
    Custody(KeyError),
}

impl MemberKeys {
    /// Encodes both private seeds for custody or a restore code (§12.1).
    pub fn to_secret_bytes(&self) -> SecretBytes;
    /// Restores both pairs from custody or a restore code.
    pub fn from_secret_bytes(bytes: &[u8]) -> Result<Self, MaterialError>;
}

impl StoreKeyring {
    /// Encodes every opened store and circle key for custody (§11).
    pub fn to_secret_bytes(&self) -> SecretBytes;
    /// Reads custody bytes, rejecting malformed, duplicate or empty keyrings.
    pub fn from_secret_bytes(bytes: &[u8]) -> Result<Self, MaterialError>;
}

impl CovenHandle {
    /// Makes this member's two key pairs and puts them in identity custody,
    /// for the person creating a store. Fails if custody already holds keys.
    /// Joining and restoring put the keys there themselves.
    pub fn initialize_identity(&self) -> Result<MemberId, IdentityError>;

    /// Removes the store keys from key custody and drops any connection that
    /// holds them unlocked. If custody can't remove them, the connection
    /// stays.
    pub async fn forget_store_keys(&self) -> Result<(), KeyError>;

    /// Keeps an app secret, such as an API token, in the same keychain and
    /// under the same access policy as coven's keys. Names can't be empty,
    /// contain `:`, or match one of coven's own entries.
    pub fn set_host_secret(&self, name: &str, value: &str) -> Result<(), KeyError>;

    /// The secret, or `None` if it was never set.
    pub fn host_secret(&self, name: &str) -> Result<Option<String>, KeyError>;

    /// Deletes the secret; succeeds if it was never set.
    pub fn delete_host_secret(&self, name: &str) -> Result<(), KeyError>;

    /// Encrypts the app's own data with the current store key, for the app to
    /// keep in its rows, since the local database is not encrypted. `aad`
    /// binds it to its place, such as the row's key. Reads the key id from the
    /// committed store log, then unlocks its bytes from custody. Without a
    /// selected key, returns `CovenError::Seal(SealError::NoCurrentStoreKey)`.
    /// Database reads and custody work run off the async executor.
    pub async fn seal_app_data(&self, plaintext: &[u8], aad: &[u8]) -> CovenResult<Vec<u8>>;

    /// Decrypts what `seal_app_data` made, with the store key it names, so it
    /// still opens after the key is replaced. Fails with a different `aad`.
    pub fn open_app_data(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError>;
}

/// The app's own store for the store keys and circle keys this device holds.
pub trait StoreKeyCustody: Send + Sync {
    /// The keys, or `None` when this device has never held any.
    fn unlock(&self) -> Result<Option<StoreKeyring>, KeyError>;
    /// Keeps `keyring`, replacing what was kept.
    fn persist(&self, keyring: &StoreKeyring) -> Result<(), KeyError>;
    fn forget(&self) -> Result<(), KeyError>;
}

/// The app's own store for this member's keys.
pub trait MemberKeyCustody: Send + Sync {
    fn unlock(&self) -> Result<Option<MemberKeys>, KeyError>;
    fn persist(&self, keys: &MemberKeys) -> Result<(), KeyError>;
    fn forget(&self) -> Result<(), KeyError>;
}
```

### E12 Circles

- A circle's members add and remove its members ([§14.3](coven.md#143-circles)).
- Circle calls return `SyncError` ([E5](#e5-storage-and-sync)), as member calls
  do ([E9](#e9-members-and-devices)). `CircleNotMember`, `CircleDeleted` and
  `NotStoreMember` retain the circle or member id the app can act on.
- Removing someone from a circle is an operation that replaces the circle's
  key ([§18.1](coven.md#181-operations)); its failure is retried or abandoned with
  the calls of [E6](#e6-operations-and-recovery).

```rust
/// Circle calls borrowing the open store that owns their work (§14, E12).
pub struct Circles<'a> { /* private fields */ }

impl CovenHandle {
    pub fn circles(&self) -> Circles<'_>;
}

impl Circles<'_> {
    /// Makes a circle with this member in it.
    pub async fn create(&self, name: &str) -> Result<CircleId, SyncError>;

    /// Renames a circle. Its key, members and rows don't change.
    pub async fn rename(&self, circle: CircleId, name: &str) -> Result<(), SyncError>;

    /// Deletes a circle: deletes each of its rows this device has, and
    /// removes the circle in the store log (§14.7).
    pub async fn delete(&self, circle: CircleId) -> Result<(), SyncError>;

    /// Adds a store member to the circle. They get its current and earlier
    /// keys, so they read its history.
    pub async fn add_member(&self, circle: CircleId, member: &MemberId) -> Result<(), SyncError>;

    /// Removes someone from the circle and replaces its key (§14.6).
    pub async fn remove_member(&self, circle: CircleId, member: &MemberId) -> Result<(), SyncError>;

    /// The circles this member is in.
    pub async fn list(&self) -> Result<Vec<Circle>, SyncError>;

    /// A circle's members who are still in the store.
    pub async fn members(&self, circle: CircleId) -> Result<Vec<CircleMemberInfo>, SyncError>;

    /// Resets a circle's rows from this device's copy (§19.3).
    pub async fn reset(&self, circle: CircleId) -> Result<(), SyncError>;
}

pub struct Circle {
    pub id: CircleId,
    pub name: String,
}

pub struct CircleMemberInfo {
    pub member: MemberId,
    pub is_self: bool,
}
```

### E13 Migrations

- The app's schema is a list of migrations numbered from 1 with no gaps.
- Opening runs every migration above the database's version in one
  transaction, so a failure leaves the schema as it was.
- A migration has two parts ([§17.1](coven.md#171-host-application)): the first
  changes the database, and the optional second changes writes made in the
  older version that still wait in `_coven_uploads`.
  - Each breaking migration converts them in version order, keeping the
    device, write number, timestamp and causal positions. Tried uploads keep
    their exact bytes. Additions run no conversion.
  - Conversion changes only the waiting records. Their effects are already
    in the device's merge records, carried through the migration by §17.1.
- Coven decides whether a migration is an addition or a breaking change by
  comparing the schema before and after it. Statements that change rows of
  synced tables also make it breaking (§17.1).
- A converted change keeps the references its columns carried, renamed
  with them. Changing a reference's value, or adding a column that is a
  reference, fails the migration: the device can't know which generation
  of the parent the old write meant.

```rust
impl Migration {
    /// A migration whose first part is SQL.
    pub fn sql(version: u32, name: &'static str, sql: &'static str) -> Self;

    /// A migration whose first part is code, for rebuilds and backfills SQL
    /// alone can't express.
    pub fn run<F>(version: u32, name: &'static str, f: F) -> Self
    where
        F: Fn(&MigrationContext<'_>) -> Result<(), DbError> + Send + Sync + 'static;

    /// Its second part: changes each row change of a waiting write to fit the
    /// new schema. Without it, a breaking change's waiting writes upload
    /// marked lost.
    pub fn writes<F>(self, f: F) -> Self
    where
        F: Fn(&mut RowChange) -> Result<(), DbError> + Send + Sync + 'static;
}

/// SQL access inside the transaction applying a migration (§17.1, E13).
pub struct MigrationContext<'connection> { /* private fields */ }

/// What one row change does (§5).
pub enum ChangeOp {
    /// Inserts the row.
    Insert,
    /// Changes columns of the row.
    Update,
    /// Deletes the row.
    Delete,
}

impl MigrationContext<'_> {
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize>;
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()>;
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>;
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>;
}

/// One row change of a write record (§5).
pub struct RowChange {
    pub table: String,
    pub key: RowKey,
    pub op: ChangeOp,
    pub columns: Vec<ColumnChange>,
}

impl RowChange {
    /// Renames one column of the change.
    pub fn rename_column(&mut self, from: &str, to: &str);
}

/// One column of a row change. A column that holds a reference also keeps,
/// privately, which generation of its parent the write named (§8.4).
pub struct ColumnChange {
    pub name: String,
    pub old: Option<rusqlite::types::Value>,
    pub new: Option<rusqlite::types::Value>,
    /* private: the references the new value carries */
}

impl ColumnChange {
    /// A column the conversion adds; it carries no references.
    pub fn new(name: impl Into<String>, old: Option<rusqlite::types::Value>, new: Option<rusqlite::types::Value>) -> Self;
}

pub enum MigrationError {
    /// The versions aren't 1 to N in order.
    NotContiguous { position: usize, found: u32, expected: u32 },
    /// The database's schema is newer than this app; the app must update.
    SchemaTooNew { current: u32, supported: u32 },
    /// A migration failed, and the transaction rolled back.
    Failed { version: u32, name: &'static str, source: Box<DbError> },
}
```

Example:

```rust
fn migrations() -> Vec<Migration> {
    vec![
        Migration::sql(1, "initial", include_str!("migrations/001_initial.sql")),
        Migration::sql(2, "rename_attachment_title", "ALTER TABLE attachments RENAME COLUMN title TO name")
            .writes(|change| {
                if change.table == "attachments" {
                    change.rename_column("title", "name");
                }
                Ok(())
            }),
    ]
}
```
