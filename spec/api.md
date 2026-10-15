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
use std::{collections::HashMap, future::Future, num::NonZeroUsize,
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

/// A member's random stable UUID, independent of their keys (§9).
pub struct MemberId(pub Uuid);

/// A validated Ed25519 public key; invalid and weak points are refused.
pub struct SigningKey(/* private */);

/// An X25519 public key (§11.1).
pub struct SealingKey(/* private */);

/// The public pair from D2, re-exported without duplicating its type:
/// `signing: SigningKey`, `sealing: SealingKey`.
pub use coven_format::MemberKeys as MemberPublicKeys;

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
  the settings, so the app never chooses a device id ([§10](coven.md#10-device-identity)).
- Provider access is chosen when storage is first attached (E5), not when the
  device-local directory is created. Google starts with `drive.file`.
  Dropbox's required `DropboxLocation` selects not-shared (`AppFolder`) or
  shared-capable (`SharedFolder`); it has no default. With one registered
  Dropbox key only that key's location kind is available; with both the app
  supplies the user's or its own explicit choice. The choice survives reopening
  and restore and cannot be changed by reconnecting.
- A `StoreLayout` says where an app's stores live on disk.
- Store lock files live beside each store directory. Deletion holds the writer
  and reader locks while removing the directory, then releases and removes the
  lock files. Creation and lock-file removal are serialized by the layout.
- Directory-entry existence checks do not follow symlinks. Only not-found
  means absent; other failures retain their operation, path and OS cause.
- Opening a store needs its declared tables ([E2](#e2-declaring-synced-tables))
  and its migrations ([E13](#e13-migrations)).
- Writable opens always migrate coven's own local tables in place
  ([§17.2](coven.md#172-covens-schema)). Read-only opens refuse tables that
  need migrating; any open refuses an internal schema newer than it supports.
- *Key custody* is where this device keeps the store keys and circle keys
  it has opened ([§11](coven.md#11-keys)): every key it has used, so it reads
  writes made under older ones.
- *Identity custody* keeps the stable member id and held private pairs,
  including historical credentials and a pending replacement candidate
  ([§11.1](coven.md#111-cryptography)).
- CloudKit is a built-in provider on Apple platforms (§4). The app supplies
  the container identifier and its iCloud entitlement; coven-storage calls
  Apple's CloudKit framework through `objc2-cloud-kit`. Native failures retain
  their cause in `StorageError::Provider`. On other platforms, setup returns
  `StorageFailure::ProviderUnavailable`.

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

/// A numbered database migration and its optional received-write conversion (§17.1).
pub struct Migration { /* private fields */ }

/// A passphrase owned by custody and erased when dropped (E1).
pub struct Passphrase(/* private */);

impl Passphrase {
    /// Takes ownership of the passphrase without exposing it again.
    pub fn new(secret: String) -> Self;
}

/// The member's Ed25519 and X25519 pairs, erased when dropped (§11.1).
pub struct MemberKeys { /* private fields */ }

/// Stable member id and held private pairs, including historical credentials
/// and a durable replacement candidate. Replay selects current authority (§11).
/// Private material is erased when dropped.
pub struct MemberIdentity { /* private fields */ }

/// Every opened store and circle key, including older keys (§11, §14.3).
pub struct StoreKeyring { /* private fields */ }

/// The app's provider client ids, kept private (E10).
pub struct OAuthClients { /* private fields */ }

/// A validated encrypted-object path, beginning with its store id (§4).
pub struct ObjectPath { /* private fields */ }

impl ObjectPath {
    /// Parses a listed or recorded path, refusing paths outside the store's layout.
    pub fn parse(value: &str) -> Result<Self, StorageError>;
    /// The path bound into the object's encryption.
    pub fn as_str(&self) -> &str;
    /// Whether this is a posted-positions or clock path, the replaceable kinds.
    pub fn is_replaceable(&self) -> bool;
}

/// A prefix of the validated object layout (§4).
pub struct ObjectPrefix { /* private fields */ }

impl ObjectPrefix {
    /// The prefix supplied to the provider.
    pub fn as_str(&self) -> &str;
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

/// A provider time: Unix seconds and a nanosecond fraction (D2). It keeps
/// every digit the provider returned on every platform, which `SystemTime`
/// cannot: Windows counts it in 100 ns intervals (§4). Ordered by time.
pub struct StorageTime(/* seconds: u64, nanos: u32 */);

impl StorageTime {
    pub fn seconds(self) -> u64;
    pub fn nanos(self) -> u32;
}

/// Metadata of a complete object returned by a provider read, listing or status (§4).
pub struct StoredObject {
    /// Validated path relative to the store's location.
    pub path: ObjectPath,
    /// Complete encrypted length in bytes.
    pub size: u64,
    /// Server publication time of the complete object, not the uploading device's clock.
    /// Immutable objects retain their first stored time across retries. Replaced
    /// positions and clock objects carry their replacement time (§9).
    pub stored_at: StorageTime,
    /// Timestamp resolution; S3 uses its whole-second HEAD/GET representation (§4).
    pub time_resolution: Duration,
    /// Native object id, or the complete S3 key.
    pub object_id: String,
    /// Opaque identity that changes when replaced encrypted bytes change (§4).
    /// It is not a checksum or proof of byte equality.
    pub revision: String,
    /// Native checksum, including its algorithm, encoding and scope.
    pub checksum: Option<ObjectChecksum>,
}

/// A checksum of all encrypted bytes or a multipart composite.
pub enum ObjectChecksum {
    /// Compare using the named algorithm over the complete encrypted object.
    FullObject(ChecksumValue),
    /// A digest derived from part digests; not a complete-object checksum.
    Composite(ChecksumValue),
}

/// Native checksum algorithm and encoding; never inferred from an ETag.
pub enum ChecksumValue {
    Md5Hex(String),
    Sha1Hex(String),
    DropboxContentHash(String),
    QuickXorBase64(String),
    Sha256Base64(String),
    Crc64NvmeBase64(String),
    Crc32cBase64(String),
    Crc32Base64(String),
    Sha1Base64(String),
}

/// Encrypted response bytes with bounded buffering (§4).
pub struct StorageStream { /* private fields */ }

impl StorageStream {
    /// Metadata of the complete published object accompanying this logical read,
    /// including its storage time (§9). Any native lookup is charged under §3.1.
    pub fn metadata(&self) -> &StoredObject;
    /// The next bounded buffer, EOF, or the original typed storage failure.
    /// A native asset request is charged separately from this logical stream.
    pub async fn next(&mut self) -> Result<Option<Vec<u8>>, StorageError>;
}

/// A database or store call's result, retaining its typed cause (§20.3).
pub type CovenResult<T> = Result<T, CovenError>;

/// Failures of opening, reading and writing a store (§5, E1, E3).
pub enum CovenError {
    /// Reading device identity or an app callback's custody operation failed.
    Key(KeyError),
    /// An app callback failed and SQLite also failed to roll it back. Later
    /// calls fail with `DbError::RollbackFailed` until the store is reopened (§5).
    Rollback { operation: Box<CovenError>, rollback: rusqlite::Error },
    /// Cleanup also failed after an app callback failed.
    FileCleanup { write: Result<(), Box<CovenError>>, failures: Vec<DbError> },
    /// The database or a write's validation failed.
    Database(DbError),
    /// An app migration failed or cannot run on this schema.
    Migration(MigrationError),
    /// Coven's internal schema is unsupported or its migration cannot run.
    CovenMigration(CovenMigrationError),
    /// The directory's settings could not be read.
    Settings(SettingsError),
    /// The writer's lock could not be acquired.
    Lock(StoreLockError),
    /// A required builder choice was not supplied (E1).
    MissingConfiguration { field: &'static str },
}

/// A failed database call or a write the database refuses (§5, §8, §14, §16).
pub enum DbError {
    /// This handle or a clone has closed the store (E1).
    StoreClosed,
    /// SQLite refused or failed a statement, preserving its error.
    Sqlite(rusqlite::Error),
    /// SQLite's integrity check or decoding stored facts found damage (§19.1).
    DamagedDatabase,
    /// Coven's own code broke a rule it relies on, such as a caller's contract
    /// or a step its state machine cannot reach: a bug, never stored damage.
    /// Sync reports it as `Failed(LocalFailure::Database)` (§19.1).
    Invariant { what: &'static str },
    /// Directory settings disagree with the store's creation entry when it
    /// is prepared, applied or read at opening (§9); these are the id and
    /// name in that entry.
    StoreSettingsMismatch { store: StoreId, name: String },
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
    /// A statement or shared trigger directly changed another member's row (§8.8).
    /// The transaction, including its upload record, did not commit.
    NotOwner { table: String, key: RowKey, owner: MemberId },
    /// A local trigger wrote a synced table or a shared trigger a local one (§8.7).
    TriggerTarget { trigger: String, table: String },
    /// A reference points at a row outside the source row's audience (§14.5).
    ReferenceAudience { table: String, key: RowKey, column: String },
    /// A write targets a circle whose deletion has been applied (§14.7).
    DeletedCircle(CircleId),
    /// A write puts a row in a circle this member isn't in, or in no circle
    /// the store log has (§14.5, §14.6).
    NotInCircle(CircleId),
    /// A required reset, schema or skipped-history reload has not committed.
    AudienceReloading(Audience),
    /// This device's last store-log entry is fixed but unpublished; it must be
    /// published before this device makes another (§9).
    StoreLogUploadPending(EntryId),
    /// This device has used every store-log entry number (§9).
    StoreLogNumberExhausted,
    /// This device has reserved every key-copy number (§11).
    KeyCopyNumberExhausted,
    /// Self-replacement has paused app writes while queued objects settle
    /// and the new binding and custody are installed (§10, E9).
    MemberKeysChanging,
    /// An inserted row's independent key holds no UUID (§8.5).
    KeyNotUuid { table: String, key: RowKey },
    /// A downloaded write fails the merge's checks, such as a timestamp no
    /// later than a write it had read; it is never applied (§19.1).
    InvalidWrite { write: WriteId, error: MergeError },
    /// A received record or its conversion violates the write format (E13).
    WriteFormat(coven_format::Error),
    /// Snapshots or their gap writes could not be checked or loaded; the
    /// database is unchanged (§15, §19.2).
    Snapshot(SnapshotError),
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
    /// The attaching write could not create its file's random key (§16.1).
    FileKey(CryptoError),
    /// Retaining the store while staging file bytes failed.
    Lock(StoreLockError),
    /// Removing owned bytes or their pending records failed. A committed write
    /// stays committed; an unsuccessful write retains its original error (§16.6).
    FileCleanup { write: Result<(), Box<DbError>>, failures: Vec<DbError> },
    /// A transaction failed and rolling it back failed too. Later calls fail
    /// with `RollbackFailed` until the store is reopened (§5).
    Rollback { operation: Box<DbError>, rollback: rusqlite::Error },
    /// An earlier call's rollback failed, so a connection may still be inside
    /// that transaction, holding its uncommitted changes or its old snapshot;
    /// this handle, read-only or not, refuses calls until reopened (§5).
    RollbackFailed,
    /// Sync found this copy stale, replaced or holding another copy's slot.
    /// Every later call and live query of this handle fails; reopening
    /// requires `reset_device` (§10).
    DeviceReset(DeviceReset),
    /// Saving pending state failed. The call that lost the update and every
    /// later call and live query fail with this same cause until the store is
    /// reopened (E5).
    PendingPersistence(Arc<DbError>),
    /// A pending record or its subject could not be encoded, or a stored one
    /// could not be decoded as the requested record type (E5).
    PendingEncoding(serde_json::Error),
    /// Sync's part of a database transaction failed, keeping sync's own typed
    /// cause, and the transaction did not commit: a streamed transfer's
    /// producer or final authentication check, or the schema publication
    /// operation a migration commits with (§17.1, E13).
    SyncStream(Box<dyn std::error::Error + Send + Sync>),
    /// Closing failed for these connections, after every one was tried (E1).
    Closing { failures: Vec<DbError> },
}

/// Why snapshots or their gap writes could not be checked or loaded (§15, §19.2).
pub enum SnapshotError {
    /// A downloaded gap write failed while the reload was applying it.
    Write { write: WriteId, error: Box<DbError> },
    /// Reading the plaintext supplied by sync failed.
    Read(std::io::Error),
    /// A frame or its merge state failed the format's checks.
    Format(coven_format::Error),
    /// The snapshot needs a different app schema (§17).
    Schema { snapshot: u32, database: u32 },
    /// These store-log entries must apply first; snapshots don't carry them.
    StoreLog { missing: Vec<EntryId> },
    /// These gap writes or causal predecessors were not supplied.
    MissingWrites { missing: Vec<WriteId> },
    /// A supplied write cannot apply yet.
    WriteWaiting(WriteWait),
    /// Individually valid records disagree with one another or the schema.
    Inconsistent(&'static str),
}

/// What a downloaded write waits for before it can apply (§6, §19.1).
pub enum WriteWait {
    /// Store-log entries the write's author had applied.
    StoreLog(Vec<EntryId>),
    /// Writes that must apply first, including the author's preceding write.
    Writes(Vec<WriteId>),
    /// The app must support this schema version.
    SchemaVersion(u32),
    /// This device's breaking migration awaits publication and reload.
    SchemaPublication(u32),
    /// The audience's reload has not committed its replacement data.
    Reload(Audience),
}

/// A schema rule checked on open and after migrating (§8, §14.1).
pub enum SchemaError {
    /// A synced foreign key does not reference the complete parent primary key (§8.4).
    /// `key` names the referencing columns and referenced table and columns as SQL.
    ForeignKeyNotPrimaryKey { table: String, key: String },
    /// A synced foreign key is one generated column (§8.4).
    /// `key` names the referencing column and referenced table and columns as SQL.
    GeneratedForeignKey { table: String, key: String },
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
    /// A local table's foreign key could stop coven updating a synced value (§8.4).
    LocalChildUpdate { table: String, column: String },
    /// SQLite chooses the primary key itself (§8.5).
    GeneratedPrimaryKey { table: String },
    /// A synced key, unique or foreign-key column, or a unique index term, uses
    /// a collation other than BINARY (§8.5, D3). `column` names the index for
    /// an expression term.
    KeyCollation { table: String, column: String, collation: String },
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
    /// The read-only connection cannot migrate coven's tables.
    ReadOnly,
    /// The internal schema requires a newer coven.
    SchemaTooNew { current: u32, supported: u32 },
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
    /// Another live instance in this process, or another process, holds the
    /// lock: opening or bootstrapping returns this error immediately. If this
    /// process's instance is closing, opening or bootstrapping the same store
    /// waits until its last blocking work finishes and its lock handles close,
    /// then tries to take the lock. The wait uses release notifications, with
    /// no timeout or polling, keyed by canonical directory and store id.
    AlreadyOpen(StoreId),
    /// An unfinished installation must be resumed with restore, join or reset_device.
    BootstrapPending(StoreId),
    /// A supplied lock protects a different directory.
    WrongDirectory(StoreId),
    /// Opening or locking the lock file failed.
    File(FileError),
}

/// Opening a read-only store or its saved file-storage connection failed (E1).
pub enum ReadOnlyOpenError {
    /// The database or store directory could not be opened.
    Local(CovenError),
    /// Saved settings, credential custody or provider construction failed.
    Storage(SyncError),
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
    /// The stored key or secret material was malformed.
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
    InvalidSigningKey,
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
    /// holds for it, including every saved host secret, then its directory.
    /// Deletes all secrets in coven's saved names list before deleting the list
    /// and its other entries. Needs no app-supplied names and does not open the database.
    /// Refused while a writer, read-only handle, file stream or outstanding
    /// file I/O retains its store lock; storage is untouched. Retrying finishes
    /// a deletion that failed partway.
    pub async fn delete_store(store_dir: &StoreDir) -> Result<(), StoreDeletionError>;
}

impl CovenBuilder {
    /// The tables that sync (E2). Required.
    pub fn synced_tables(self, tables: Vec<SyncedTable>) -> Self;

    /// The app's schema migrations, numbered from 1 with no gaps (E13).
    /// Required.
    pub fn migrations(self, migrations: Vec<Migration>) -> Self;

    /// The wall clock that timestamps use (§7.2). Defaults to the system clock.
    pub fn clock(self, clock: ClockRef) -> Self;

    /// The source of new ids (§20.2). Defaults to `UuidIds`, random UUIDs.
    pub fn id_source(self, ids: IdSourceRef) -> Self;

    /// The app's own OAuth clients for Google Drive, Dropbox and OneDrive.
    /// Coven ships none.
    pub fn oauth_clients(self, clients: OAuthClients) -> Self;

    /// The app's sign-in presenter. Desktop defaults to `DesktopOAuthPresenter`;
    /// iOS and Android require a presenter using the platform's sign-in sheet.
    pub fn oauth_presenter(self, presenter: Arc<dyn OAuthPresenter>) -> Self;

    /// Sign in and commit tokens to coven's session custody for setup, restore
    /// or join. Dropping the future cancels sign-in; failure retains the prior
    /// sign-in. Success returns no tokens to the app.
    pub async fn authenticate(&mut self, access: OAuthAccess) -> Result<(), OAuthError>;

    /// How many file uploads run at once. Defaults to one.
    pub fn max_concurrent_uploads(self, n: NonZeroUsize) -> Self;

    /// How many downloads run at once, including log fetch-ahead and file
    /// ranges (E5, E8). Defaults to one.
    pub fn max_concurrent_downloads(self, n: NonZeroUsize) -> Self;

    /// Where this device keeps the store keys: the OS keychain by default, a
    /// file sealed with a passphrase, memory for this session only, or the
    /// app's own `StoreKeyCustody`.
    pub fn key_custody(self, custody: KeyCustody) -> Self;

    /// Where this device keeps its member's keys, with the same four choices
    /// and the app's own `MemberKeyCustody` as the last.
    pub fn identity_custody(self, custody: IdentityCustody) -> Self;

    /// Opens the store for reading and writing, taking the store's lock.
    /// Opening always migrates coven's tables before the app's schema, then
    /// unlocks each configured key custody once for this handle's session,
    /// then resumes unfinished operations and committed file work. Empty
    /// custody is allowed; malformed or inaccessible custody fails opening.
    /// Local database calls need no store key. Opening does not start
    /// the sync loop; `start_sync` starts it. A store with storage set up
    /// opens as `Stopped`; otherwise its status is `Disconnected`.
    pub async fn open(self, store: StoreId) -> CovenResult<CovenHandle>;

    /// Replaces a damaged local database with a new device loaded from storage
    /// (§19.2), or finishes a stale copy's recorded reset (§10), which `open`
    /// refuses with `BootstrapPending`. Uses saved settings and custody, with
    /// no data salvage.
    /// Checks storage and keys before moving the old directory aside.
    /// Uses the existing bootstrap state and errors; an interrupted call
    /// resumes with the same fresh id. The returned handle holds a device-reset
    /// notice that unsent edits may have been lost (E5).
    pub async fn reset_device(self, store: StoreId, device_name: &str) -> Result<CovenHandle, BootstrapError>;

    /// Opens the store for reading only, alongside a handle that has it open,
    /// for example from another process. Its shared lock prevents deletion
    /// while its read connections and local cache connection remain open.
    /// It runs no migration and refuses a database whose schema is newer than
    /// its migrations or whose coven tables need migrating.
    /// File reads use saved storage settings and custody credentials when present.
    pub async fn open_read_only(self, store: StoreId) -> Result<CovenReadHandle, ReadOnlyOpenError>;
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

impl CovenHandle {
    /// Closes the store: stops syncing, operation and file work, closes every
    /// connection, erases the unlocked custody sessions and releases the writer
    /// lock. Open file streams and outstanding file I/O
    /// retain their shared deletion guards. Later database calls on any clone
    /// fail with `DbError::StoreClosed`; custody calls fail with `KeyError::StoreClosed`.
    /// Reports every failure of closing workers and connections, one alone as
    /// itself and several chained as `SyncError::Cleanup`; cancellation does
    /// not stop closing.
    pub async fn close(&self) -> Result<(), SyncError>;
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
    .open(store_dir.id())
    .await?;
```

Example, choosing a not-shared Dropbox store when both app keys are registered:

```rust
handle.authenticate(OAuthAccess::Dropbox(DropboxAccess::AppFolder)).await?;
handle.setup_oauth_storage(
    StorageConfig::Dropbox { location: DropboxLocation::AppFolder },
    "Ana's phone",
).await?;
// A shared choice uses DropboxAccess::FullDropbox and
// DropboxLocation::SharedFolder { namespace_id } instead.
```

### E2 Declaring synced tables

- Table names in declarations match SQLite identifiers without regard to ASCII
  case. Opening records SQLite's spelling, which identifies the table in writes,
  snapshots and file retention.
- Each synced table declares its kind of key ([§8.5](coven.md#85-keys-and-uniqueness)),
  how its rows get their audience ([§14](coven.md#14-audiences)), and whether its rows
  carry a file ([§16](coven.md#16-files)). It may restrict direct writes to
  each row's original creator with `owner_only()`
  ([§8.8](coven.md#88-owner-tables)).
- Owner tables use the same foreign-key actions, unique constraints, CHECKs
  and shared triggers as other synced tables. Opening and schema raises
  enforce the ordinary schema rules, including SET DEFAULT's refusal; no
  owner-column or owner-specific RESTRICT requirement is added.
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

    /// Only each row's original creating member may directly update or delete
    /// it. Shared-trigger targets count as direct writes; exact foreign-key
    /// consequences are verified separately (§8.8). No app owner column is needed.
    pub fn owner_only(self) -> Self;

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

    /// The column holding the fixed path, uploader and file key chosen by
    /// the attaching write (§16.1, D12). `FileRef::path` reads its path;
    /// `file_status` checks availability without changing this column.
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
    /// Runs one write. A statement or shared trigger targeting another owner's
    /// row refuses the transaction with DbError::NotOwner (§8.8). Authorized
    /// foreign-key actions may change that row under the declared schema.
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
- Each table-backed built-in read uses one pre-built internal query for
  its one-shot call and `subscribe_x() -> LiveQuery<T>` counterpart:
  - `pending`, `lost_values`, `uploads`, `device_reset` and
    `eager_cache_fill_status` on the handle;
  - `pin_progress`, with the same fixed file references in both calls;
  - `members` on the handle, and `list` and `members` on `Circles`;
  - `rows_pinned` on the handle, with the same table and keys in both calls.
- These reads work without storage and use the app's normal read and live-query
  machinery. Subscriptions observe committed changes to the same query inputs.
  No app-facing channel mirrors database state in memory. Upload progress is
  the persisted session's provider-confirmed bytes; eager and pin progress
  come from committed cache coverage. Only non-database state such as sync
  status and join requests uses its own stream (E5, E10). Internal caches
  rebuilt from the database are not additional app-facing state.
- E.g. Ana's app reads `members()` to draw its first list, or subscribes with
  `subscribe_members()` to receive that same list and later committed changes.
- Subscriptions show current results, including replay changes. Their callback
  histories need not match between devices (§9).
- A loss with `pending_entries` can disappear on replay. Offer a removed row
  elsewhere with the same key (§8); copying to a new key can produce two rows.

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
    /// good, and never comes back (§8). Deleting an owned removed row requires
    /// its owner's permission (§8.8); dismissing a frozen record changes no row.
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
    /// What replaced it: a write that hadn't read it, removal rules, a
    /// breaking change, dropped data sources, or failed row authorization.
    pub replaced_by: Replacement,
    /// Old-shape values kept independently of the current schema (§17.1).
    /// Their table or columns may no longer exist; dismissal still works.
    pub frozen: bool,
    /// Entries whose outcome can still change this loss (§9), sorted by id.
    /// Empty when no non-final entry affects it. This is computed locally;
    /// it is not part of the loss's fingerprint or snapshot identity.
    /// Storage-time finality can change this list without changing the loss;
    /// that committed change also notifies subscribe_lost_values().
    pub pending_entries: Vec<EntryId>,
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
    /// A breaking change excluded the write or deleted a hidden row through
    /// its generation. Frozen values retain their original setters (§17.1).
    SchemaChange { version: u32 },
    /// This original row change used dropped data (§8, §10). Sources name
    /// generation/setter changes, additional direct-execution sources, then
    /// verified foreign-key causes (§8.8),
    /// in authored role/execution order, with repeats. Row identity distinguishes
    /// several causes in the same write. Empty deletes retain their write id.
    DataDependency { write: WriteId, sources: Vec<ChangeSource> },
    /// A member's row change lacked permission; other changes may apply (§8.8).
    NotAuthorized { write: WriteId, reason: RowAuthorizationFailure },
}

/// An original row change or direct execution target used as data (§8, §8.8).
pub struct ChangeSource {
    pub write: WriteId,
    pub table: String,
    pub key: RowKey,
    pub audience: Audience,
}

pub enum RowAuthorizationFailure {
    /// A direct target belongs to this member, or a foreign target had no
    /// valid foreign-key derivation from an authorized direct change.
    NotOwner { owner: MemberId },
    /// The proposed derived payload differs from the reconstructed schema effect.
    InvalidForeignKeyEffect,
}

pub enum RemovalRule {
    ForeignKey { columns: Vec<String>, parent: String, parent_columns: Vec<String> },
    Check { constraint: String },
    /// The circle is deleted by this kept entry (§14.7). This describes
    /// the deletion, not a conflicting row edit; the row returns if it drops.
    DeletedCircle { entry: EntryId },
    /// The same key is present in another audience, whose row is shown
    /// (§14.2).
    OtherAudience,
    Unique { terms: Vec<String>, partial: Option<String> },
}

/// A handle that only reads, opened with `open_read_only`.
impl CovenReadHandle {
    /// Closes read and cache connections on all clones and releases their store lock.
    /// Reports connection-close failures; cancellation does not stop closing.
    pub async fn close(&self) -> Result<(), DbError>;

    pub fn read<F, R>(&self, read: F) -> Read<'_, F>;
    pub async fn file_ref(&self, table: &str, key: impl Into<RowKey>) -> Result<FileRef, DbError>;
    pub async fn user_file(&self, table: &str, key: impl Into<RowKey>) -> Result<Option<UserFile>, DbError>;
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>, FileReadError>;
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError>;
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
  - Paths start with this store's id. Other stores can share the location,
    except on an S3 service whose member keys reach a whole bucket (below).
  - After `disconnect_storage`, setup reconnects at the recorded location.
    It refuses a different location with `LocationChangeUnsupported`;
    setup does not relocate a store already used by other devices.
  - Creating uploads the store's first entry and its key sealed to this
    member; waiting writes then go up through sync like any others.
- The app calls `authenticate(access)` on the handle before OAuth setup.
  Coven owns the request, checked redirect and code exchange. The builder's
  presenter opens the page and returns the redirect or cancellation; it receives
  no tokens. Sign-in is held in coven's session custody until setup succeeds.
  Replacing a sign-in does not change an existing storage connection; setup
  verifies the chosen account and location before attaching it.
- Every setup, including reconnection, reaches the candidate provider with the
  newly supplied credentials and checks it before reserving or publishing a
  store entry or committing local state. A fresh sealed test object must be
  created once, refuse a second create at that path, return the original bytes
  through whole and interior ranged reads, appear with its size in a listing,
  and disappear after deletion. Cleanup runs even after a lost create reply;
  a path already occupied before the check is left untouched. A failed check
  returns `StorageSetupError::ProviderCheck`, naming the operation and its
  classified cause; the error retains a cleanup failure too if both fail.
- An S3 location's behaviors are `S3Settings` (§4). Its `S3Profile` names a
  catalogued service or carries custom settings; with no profile, the
  endpoint selects (§4): no endpoint means AWS S3; `storage.googleapis.com`
  means GCS XML, using generation conditions and conditional copy to publish
  multipart uploads. Hosts ending in `.r2.cloudflarestorage.com` select R2,
  using conditional PUT and multipart completion and ignoring checksums
  without stated scope. The self-hosted RustFS, SeaweedFS, Ceph RGW and
  Versity Gateway profiles are selected only by name; SeaweedFS publishes
  every object by conditional copy (§4). Setup refuses SeaweedFS before any
  request with `StorageSetupError::S3ProfileRefused { profile: SeaweedFs }`:
  its suite 5 certificate did not reproduce (§4). A name or custom profile contradicting a
  catalogued host, a name other than AWS or any custom profile without an
  endpoint, or plain `http` to a catalogued host is `InvalidConfiguration`;
  other endpoints may use `http`.
- Where an S3 service's member keys reach a whole bucket (R2, and every
  custom profile, detected or chosen), any member's key reaches every store
  in the bucket, so each store has its own bucket from creation (§4). A
  non-empty base prefix fails validation, before any request, with
  `StorageFailure::OwnBucketRequired { reason: OwnBucketReason::Prefix }`.
  A store's first setup lists the bucket's root before detection writes and
  refuses a bucket holding anything outside the store's folder with
  `OwnBucketRequired { reason: OwnBucketReason::Occupied }`, wrapped in
  `StorageSetupError::Storage`. Reconnecting checks no objects outside the
  store's folder. AWS, GCS, RustFS, SeaweedFS, Ceph RGW and Versity Gateway
  keys can be limited to the store's prefix, so their stores share buckets and prefixes unchecked.
- An endpoint matching no catalogued host and naming no profile has no AWS
  fallback: setup detects its settings and returns and records the location
  with them as a custom profile. Reconnecting with that same location and no
  profile reuses the recorded settings.
- `detect_s3_settings` runs the same detection for an app that offers
  advanced settings: it probes the bucket with the supplied key, writing only
  fresh objects under the store's `.coven-check` prefix and deleting them,
  also on failure. Its `S3Detection` holds the detected settings and one
  observation per probe; the app may prefill and override them, and
  `S3Detection::check` tells whether an override agrees with what was observed.
- Every S3 setup runs detection and checks the effective settings, detected,
  named or custom, against its observations before the provider check:
  `CreateOnce`, `MultipartCreateOnce`, `PublicationTime` and `Behavior` name the
  first failure. An override that contradicts an observation is refused, never
  accepted. A service that no setting makes safe is refused too: a server that
  accepts writes to an occupied name, e.g. Garage, fails `CreateOnce`. Every
  connection is accepted only after all setup checks pass, including those
  using certified profiles. An endpoint that overwrites on
  create fails `CreateOnce`; setup preserves the prior connection.
- Detection also reads bucket versioning through `GetBucketVersioning`,
  including GCS's XML equivalent. Before the behavior checks, setup refuses
  `Enabled` or `Suspended` with `S3VersioningUnsupported` unless the selected
  service has separate certification with versioning enabled. Sequential
  create-once probes cannot establish this: versioned Ceph RGW accepted both
  concurrent writers while readers saw only one writer's bytes. Custom and
  detected settings never support versioned buckets. A refused/unimplemented
  read retains its cause in detection; uncertified profiles refuse it with
  `S3VersioningUnknown`. A certified profile may proceed with a warning. Other
  read failures fail detection. Member policies need `s3:GetBucketVersioning`
  on the bucket (GCS: `storage.buckets.get`). Hosted R2 has no versioning
  feature; its documented, refused API is recorded as `Unavailable`. Naming
  R2 at another endpoint does not establish that exception. No setup changes
  bucket versioning or commits credentials after a versioning refusal.
- Setup commits the storage credentials, keys, location and restore code only
  once the connection is ready. Failure preserves their previous values and
  the previous connection. The fixed first-entry or access-update attempt stays
  reserved for an identical retry: its number, plaintext, sealing key ids and
  prerequisite sealed-key bytes cannot change (§6, §18).
- Setup takes the first device's name, as joining and restore do (§10, §12).
  Reconnecting an existing device keeps its registered name. Changed provider
  access is published as `Set access` before committing the new credentials.
- A started connection runs one complete sync at a time, immediately after a
  local write or `sync_now`, and every 30 seconds while idle, subject to
  connection backoff and provider cooldowns. Stopping and closing finish the
  active pass first.
  - [One sync pass](sync-pass.md) specifies every listing, read trigger,
    send and cache lifetime in order; [§3.1](coven.md#31-io-bounds) states
    the request and local-work bounds. List only store-log device folders
    and positions on an idle pass. GET each undrained known writer's next
    store-log, write and key-copy number; fetch ahead on a hit within the
    transfer limit, stopping at the first miss. No pass lists pages of log history.
  - New or long-absent devices list snapshots by audience (§15); snapshots
    never use next-number discovery. The clock object's storage time and
    the last completed catch-up's storage time measure absence. Due retention
    can refresh snapshot coverage; idle passes do not list snapshots or files.
  - Before any upload, the pass uses its completed observations, checked own
    positions and snapshots, received store log and device custody to check
    identity, counters, removal and replacement (§10). Other senders use
    the same serialized check. They do not make independent duplicate scans.
    Every send checks current signing credentials; an already signed object
    must also have the current signer, with §10's bootstrap exceptions.
    Every send, including retries and file parts, also checks that the last
    completed first phase started less than five minutes ago, monotonically
    with sleep counted. Otherwise it waits for the next pass (§10).
  - The pass borrows the keys unlocked at open for the handle's session.
    Its observations and retained checked bytes serve operations, reloads,
    writes, file transfers, snapshots, retention and agreement together.
    File uploads run in their own worker (§16.5), sharing those observations,
    identity checks, transfer limits and backoff. A pass does not await them.
  - Own positions are posted only when their complete publishable contents
    change, or the post is absent. Upload completion creates no app write.
  - Calls or app writes arriving during sync retain one wake if this pass
    has not serviced them. Its own queue, operation and cache commits do
    not trigger an immediate second pass. Waiting work uses the shared
    backoff rules; a completion notification alone is not new evidence.
  - Every subject that cannot advance has its first unmet condition in `pending()`.
    This includes missing prerequisites, key copies, damaged objects, dropped
    entries, fingerprint disagreements, pending operations and file failures.
  - Independent work continues. Per-object and maintenance pending records do not
    fail sync status; a failed required reload prevents positions advancing
    past that reload, and is recorded under its audience and operation.
  - Posted positions never advance over unfinished work. A device can replace
    its previous post with updated pending records while its positions wait.
    It omits fingerprints unless they describe exactly the posted positions.
  - A finished pass publishes its completion time; `Synced` can coexist with
    pending records. Live queries report committed changes (E4).
  - Reset and coven-update retry rules are in §19.1. Neither a disagreement
    nor a peer's silence triggers recovery on its own.
- `Offline` describes the last failed attempt, not the connection's history.
  `Failed` means storage answered with an error that prevented the whole pass.
  - Keep whether the provider answered; `StorageFailure::Network` alone
    cannot decide this. A server error response is not an offline device.
  - A denied store listing can fail the pass. A refused write, missing key,
    unpublished schema, failed file or retention wait stops its subject
    and lets independent work continue; it does not set `Failed`.
  - Removal records a `Connection` block with `Removed`, and stops the loop
    for good. Local failures also name their pending subject and preserve
    their typed cause for a waiting caller.
  - If the database cannot save a pending record, stop the loop and fail database
    calls and live queries with that cause, as `DbError::PendingPersistence`,
    until the store is reopened.
    Never expose an apparently empty pending list after losing its update.
- E.g. Ana syncs successfully, then loses Wi-Fi. The next attempt is
  `Offline`. A later provider error denying the store listing is `Failed`.
  One bad photo while the rest syncs leaves `Synced` with a pending file record.
- A device that isn't connected still reads and writes
  ([§3](coven.md#3-guarantees)); its writes wait in `_coven_uploads`.
- `start_sync` builds the provider client if absent, reading credentials from
  custody, then starts the loop. Tokens refresh on provider rejection (§4).
  Starting an already running loop, or a store with no storage set up, does nothing.
- `stop_sync` finishes the active pass and file transfers, then drops all
  workers' references to the provider client. The open handle keeps its session
  keys until close or explicit forgetting. Credentials
  and the storage location remain available for the next `start_sync`.
- `disconnect_storage` also removes this device's storage credentials. It leaves
  storage's contents untouched; syncing requires storage setup again.

Storage failures retain the same `StorageFailure` through setup, sync status,
uploads, reads and operations. Their `StorageError` carries the native cause
where one exists; setup adds the failed check or its distinct local failure
without reclassifying storage failures. Missing member keys are always
`StorageFailure::MemberKeysMissing`. Pending records retain this same
classification when the native cause cannot survive closing the app.

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
    /// iCloud through coven-storage's native Apple-platform adapter.
    CloudKit,
}

/// A store's location, with credentials kept separately (§4, E5).
pub enum StorageConfig {
    /// An existing S3 bucket and a base prefix. Where the service's member
    /// keys can be limited to a prefix, the base prefix may hold several
    /// stores; where they reach a whole bucket (R2, and every custom or
    /// detected profile), the store has the bucket to itself (§4).
    S3 {
        /// The bucket's name.
        bucket: String,
        /// The signing region.
        region: String,
        /// None is AWS's own endpoint. Catalogued hosts require https (§4).
        endpoint: Option<Url>,
        /// A catalogued service's name or custom settings; either must agree
        /// with a catalogued endpoint host. None selects by host:
        /// storage.googleapis.com is GCS XML, hosts ending in
        /// .r2.cloudflarestorage.com are R2, no endpoint or .amazonaws.com is
        /// AWS, and setup detects any other endpoint's settings, recording
        /// them as Custom. All run the setup checks.
        profile: Option<S3Profile>,
        /// The base prefix, without leading or trailing slashes; paths add
        /// the store id. Empty where member keys reach a whole bucket;
        /// otherwise `OwnBucketRequired { reason: Prefix }`.
        prefix: String,
    },
    /// A Google Drive folder; paths within it start with the store id.
    GoogleDrive { folder_id: String },
    /// Required creation choice: an App folder or a shared-capable namespace.
    Dropbox { location: DropboxLocation },
    /// A OneDrive folder; paths within it start with the store id.
    OneDrive { drive_id: String, folder_id: String },
    /// The CloudKit custom zone reached by the built-in adapter.
    CloudKit {
        /// The app's CloudKit container.
        container: String,
        /// The owner's CloudKit record name.
        owner: String,
        /// The custom zone containing the store.
        zone: String,
    },
}

/// An S3 location's behaviors: a catalogued service's, or its own (§4).
pub enum S3Profile {
    /// The catalogued service's settings.
    Named(S3ProfileName),
    /// Settings detected at setup or chosen by the app; restore codes carry them.
    Custom(S3Settings),
}

/// Every S3 behavior the adapter varies, one value each (§4).
pub struct S3Settings {
    pub signing: S3Signing,
    /// The condition a create-once single PUT sends.
    pub single_put: S3SinglePut,
    /// Which creates publish by a conditional copy of a staged temporary.
    pub copy_publication: S3CopyPublication,
    /// What the native multipart `Last-Modified` dates.
    pub multipart_timestamp: S3MultipartTimestamp,
    pub checksums: S3Checksums,
    pub listing: S3Listing,
    /// Where a replaceable object's revision comes from.
    pub revision: S3Revision,
    pub response_headers: S3ResponseHeaders,
    /// Whether HEAD and GET request checksum mode.
    pub request_checksums: bool,
}

pub enum S3Signing { AwsSigV4, GoogleHmac }
/// `If-None-Match: *` or `x-goog-if-generation-match: 0`.
pub enum S3SinglePut { IfNoneMatch, GenerationMatch }
pub enum S3CopyPublication {
    /// None: single PUTs and multipart completions (`If-None-Match: *` on
    /// CompleteMultipartUpload) carry the create-once condition.
    Never,
    /// Multipart uploads go to a reserved temporary, then a copy with this
    /// destination condition; single PUTs carry the create-once condition.
    Multipart(S3CopyCondition),
    /// Every create, single-request or multipart, is staged at a reserved
    /// temporary and published by a copy with this destination condition.
    Every(S3CopyCondition),
}
/// `If-None-Match: *` (AWS), `x-goog-if-generation-match: 0` (GCS) or
/// `cf-copy-destination-if-none-match: *` (R2).
pub enum S3CopyCondition { IfNoneMatch, GenerationMatch, CloudflareIfNoneMatch }
pub enum S3MultipartTimestamp { Completion, Start }
pub enum S3Checksums {
    /// Returned checksums state their scope.
    ScopeStated,
    /// Checksums without a stated scope are discarded.
    UnstatedIgnored,
    /// None are returned; any returned checksum is a protocol error.
    Absent,
}
pub enum S3Listing { V2, V1 }
/// ETag uses native versions or `coven-revision` metadata for replacements.
pub enum S3Revision { ETag, Generation }
pub enum S3ResponseHeaders { S3, Google }

/// What detection observed at one location (§4).
pub struct S3Detection {
    /// Bucket versioning, or a refused/unimplemented read with its native cause.
    pub versioning: Result<S3BucketVersioning, Arc<StorageError>>,
    /// Settings derived from the observations; prefill advanced settings with them.
    pub settings: S3Settings,
    /// One observation per probe, in the order run.
    pub observations: Vec<S3Observation>,
}

/// Bucket state read through the XML API; a failed read is not Disabled.
pub enum S3BucketVersioning {
    Disabled,
    Enabled,
    Suspended,
    /// Hosted R2 has no object-versioning feature.
    Unavailable,
}

impl S3Detection {
    /// Whether this probe's behavior held; an unrun probe did not.
    pub fn held(&self, probe: S3Probe) -> bool;
    /// Refuses settings that contradict the observations, naming the first
    /// failed check as setup would: CreateOnce, MultipartCreateOnce,
    /// PublicationTime, then Behavior.
    pub fn check(&self, settings: &S3Settings) -> Result<(), StorageSetupError>;
    /// The location setup records, after checking versioning support and its effective settings:
    /// an uncatalogued endpoint naming no profile gets these settings as its
    /// custom profile; any other location is kept as given.
    pub fn resolve(&self, location: StorageConfig) -> Result<StorageConfig, StorageSetupError>;
}

pub struct S3Observation {
    pub probe: S3Probe,
    /// Whether the behavior held, as described on each probe.
    pub held: bool,
    /// Native outcome; names no key, endpoint or object.
    pub detail: String,
}

pub enum S3Probe {
    /// A read-only listing is accepted with this signer.
    Signing(S3Signing),
    /// The condition refuses an occupied name, leaving its bytes and time.
    SinglePut(S3SinglePut),
    /// An unfinished multipart upload is absent from HEAD, GET and listing.
    PartialUploadHidden,
    /// Conditional completion refuses an occupied name, leaving bytes, ETag and time.
    ConditionalCompletion,
    /// Native multipart `Last-Modified` dates completion; otherwise it dates start.
    CompletionTimestamp,
    /// A copy with this condition refuses an occupied name, leaving bytes and time.
    CopyPublication(S3CopyCondition),
    /// A copy is dated later than its source.
    CopyTimestamp,
    /// This protocol lists every probe object; V2 must also agree with V1.
    Listing(S3Listing),
    /// HEAD with checksum mode is accepted.
    ChecksumMode,
    /// A checksum arrives without checksum mode.
    PassiveChecksum,
    /// Some probe returned a checksum.
    ChecksumReturned,
    /// Every returned checksum stated its scope.
    ChecksumScopeStated,
    /// GCS's `x-goog-generation` arrived.
    GoogleGeneration,
    /// GCS's `x-goog-hash` or generation arrived.
    GoogleHeaders,
    /// A zero-entry listing at a missing object's key is accepted; status
    /// uses it to tell a missing bucket from a missing object.
    BucketList,
}

/// An S3 service whose catalogued profile a location names (§4).
/// Self-hosted services have arbitrary endpoints, so no hostname identifies them.
pub enum S3ProfileName {
    /// Amazon S3.
    Aws,
    /// Google Cloud Storage through its XML API.
    Gcs,
    /// Cloudflare R2.
    R2,
    /// RustFS, self-hosted.
    RustFs,
    /// SeaweedFS's S3 gateway, self-hosted.
    SeaweedFs,
    /// Ceph RADOS Gateway, self-hosted.
    CephRgw,
    /// Versity Gateway, self-hosted.
    VersityGw,
}

/// Dropbox's location fixes its access level at cloud-store creation (§4).
pub enum DropboxLocation {
    /// Not-shared: paths are relative to the registered app's folder.
    AppFolder,
    /// Shared-capable: an existing shared namespace, independent of mount paths.
    SharedFolder { namespace_id: String },
}

impl StorageConfig {
    /// The provider of this location.
    pub fn provider(&self) -> CloudProvider;
    /// Initial sign-in for this location; shared Drive invites require Full (E10).
    pub fn oauth_access(&self) -> Option<OAuthAccess>;
    /// Refuses missing or invalid location information before making a request.
    pub fn validate(&self) -> Result<(), StorageError>;
}

/// Limits for concurrent transfers (E1, E5).
pub struct TransferLimits {
    /// Maximum file uploads running at once.
    pub uploads: NonZeroUsize,
    /// Maximum concurrent downloads, shared by log fetch-ahead and file reads.
    pub downloads: NonZeroUsize,
}

/// The single storage failure classification used by every app-facing error (§20.3).
pub enum StorageFailure {
    /// This platform cannot use the selected built-in provider.
    ProviderUnavailable,
    /// The app did not register the access needed by this operation.
    AccessNotRegistered { required: OAuthAccess },
    /// The sign-in lacks the access needed by this operation.
    AccessNotGranted { required: OAuthAccess },
    /// A folder is outside the Drive File grant's view; Full is needed to inspect it.
    /// This does not establish that the folder has been deleted.
    GoogleDriveFolderNotVisible,
    /// Sharing an App-folder store requires relocation, outside this operation.
    DropboxAppFolderNotShareable,
    /// The incremental sign-in selected another account; the store keeps its prior credentials.
    AccountMismatch,
    /// No completed membership and key-copy catch-up started less than five
    /// minutes ago; the send waits for the next pass (§10).
    CatchUpRequired,
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
    /// A concurrent mutation defeated a conditional request; retry fixed bytes.
    /// Invalidated multipart publication reports SessionExpired instead (§4).
    Conflict,
    /// The bucket, folder or zone is absent.
    ContainerNotFound,
    /// The S3 endpoint or signing region is wrong.
    RegionMismatch,
    /// The S3 service's member keys reach a whole bucket, so a store there
    /// needs a bucket of its own (§4).
    OwnBucketRequired { reason: OwnBucketReason },
    /// A member key coven made could list outside its store's prefix (§4).
    KeyBeyondStore,
    /// Setting up a store that never had storage found another device's
    /// creation marker: another installation of the store claimed this
    /// location first (§10, D10). Restore or join it instead.
    StoreExists,
    /// S3 rejected the device clock after one retry with the learned offset (§4).
    ClockSkew,
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
    /// Complete uploaded bytes disagree with the local checksum (§15).
    Integrity,
    /// An object path is outside the store's layout.
    InvalidPath,
    /// A range is empty, reversed or beyond the object's end.
    InvalidRange,
    /// Only the account holding the store may change its sharing.
    NotStoreOwner,
    /// Dropbox cannot upgrade a pending viewer without the recipient's account id.
    AccountIdUnavailable,
    /// The invite belongs to another provider location.
    InvitationMismatch,
    /// The session belongs to another provider or location.
    SessionMismatch,
    /// The recorded upload expired or a publication conflict invalidated it.
    SessionExpired,
    /// A part disagrees with the session's offset, size or alignment.
    InvalidPart,
    /// A posted-positions replacement exceeds this provider's single-request limit.
    SingleRequestTooLarge {
        /// Encrypted body length in bytes.
        size: u64,
        /// The adapter's single-request limit in bytes.
        limit: u64,
    },
    /// An S3 object published by `CopyObject` exceeds its single-copy limit,
    /// so it cannot be published atomically on this profile (§4).
    CopyTooLarge {
        /// Encrypted body length in bytes.
        size: u64,
        /// The single-copy limit in bytes.
        limit: u64,
    },
    /// Parsing recorded provider data failed.
    Encoding,
    /// Local file access or persisting provider settings or upload progress failed.
    File,
    /// This installation has no member keys in custody.
    MemberKeysMissing,
}

/// Why an S3 location cannot hold a store that needs its own bucket (§4).
pub enum OwnBucketReason {
    /// The location names a base prefix; the store occupies the bucket's root.
    Prefix,
    /// Creating the store found objects outside its own folder in the bucket.
    Occupied,
}

impl StorageFailure {
    /// Retain a diagnostic or native cause alongside this classification.
    pub fn with_source(self, source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> StorageError;
}

/// A storage error preserves its classification and typed cause (§4, E5).
pub enum StorageError {
    /// A failure with no underlying native error; also constructed by `failure.into()`.
    Failure(StorageFailure),
    /// A failure with its diagnostic or original local cause.
    Caused { failure: StorageFailure, source: Box<dyn std::error::Error + Send + Sync> },
    /// A provider failure with its original transport, SDK or framework cause.
    Provider {
        provider: CloudProvider,
        failure: StorageFailure,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Cleanup failed too; both causes and their classifications are retained.
    Cleanup { operation: Box<StorageError>, cleanup: Box<StorageError> },
}

impl StorageError {
    /// The stored classification; for Cleanup, the original operation's failure.
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
    /// The named catalogued S3 profile failed certification's concurrent
    /// create-once races; setup refuses it before any request (§4).
    S3ProfileRefused { profile: S3ProfileName },
    /// The profile has no certification with bucket versioning enabled.
    S3VersioningUnsupported { state: S3BucketVersioning },
    /// The bucket state is unknown and the profile has no versioning certification.
    S3VersioningUnknown { source: Arc<StorageError> },
    /// A provider check failed; cleanup failures retain both causes.
    ProviderCheck { check: StorageCheck, source: StorageError },
    /// An existing store cannot be relocated by setting up another location.
    LocationChangeUnsupported,
    /// The provider refused or failed setup.
    Storage(StorageError),
    /// Sign-in failed or was cancelled.
    OAuth(OAuthError),
    /// Keeping credentials or keys failed.
    SecureStorage(KeyError),
    /// Local setup failed, with its cause.
    Internal(Box<dyn std::error::Error + Send + Sync>),
}

/// The provider operation checked before setup commits anything (E5).
pub enum StorageCheck {
    /// Create a fresh sealed object.
    Create,
    /// Refuse a second create at the same path.
    CreateOnce,
    /// Read the original bytes in full.
    Read,
    /// Read exactly an interior byte range.
    ReadRange,
    /// Run the scoped query and check returned probe metadata; fresh objects may be omitted.
    List,
    /// Delete the object and confirm it is absent.
    Delete,
    /// S3: multipart publication refuses an occupied name with its bytes and
    /// time unchanged, and conditional completion hides unfinished uploads.
    MultipartCreateOnce,
    /// S3: multipart objects are dated by complete publication, as recorded.
    PublicationTime,
    /// S3: another setting agrees with the service's observed behavior.
    Behavior,
}

/// Opening the member's sealed store key failed (§11, E5).
pub enum StoreKeyUnlockError {
    /// No storage is connected.
    NoStorage,
    /// Reading the sealed key failed.
    Storage(StorageError),
    /// The sealed key could not be opened or checked.
    Crypto(CryptoError),
    /// Unlocking or keeping keys in custody failed.
    SecureStorage(KeyError),
}

/// Sync or a store-log change failed (§9, §13, §17, E5).
pub enum SyncError {
    /// This call requires work that cannot yet advance (§19.1).
    Pending(Box<PendingRecord>),
    /// The proposed entry violates its byte format.
    Format(coven_format::Error),
    /// A required key is absent from custody, or its material conflicts.
    Key(coven_crypto::MaterialError),
    /// A required sealed key has not arrived; retry after acquiring it.
    KeyUnavailable(KeyId),
    /// A key introduction reused an immutable key identity.
    KeyAlreadyUsed(KeyId),
    /// Replay already rejects the proposed change in the author's applied view.
    Rejected(DropReason),
    /// An object required by the change failed its checks.
    Damaged(DamagedObject),
    /// This device or its member was removed; syncing stops for good (§10).
    Removed,
    /// This copy cannot keep its device id. It sends nothing more; the reset
    /// is recorded and `reset_device` finishes it (§10).
    DeviceReset(DeviceResetReason),
    /// Held signing credentials are no longer current for this stable id.
    /// Sending stops; restoring with current keys registers a new device.
    MemberKeysReplaced { member: MemberId },
    /// No storage is connected for a call that requires it.
    NoStorage,
    /// The provider refused or failed the call.
    Storage(StorageError),
    /// The local database failed.
    Database(DbError),
    /// A local invariant or resource failed: the database refusing sync's
    /// own requests (`Database`), local files, directories or the store lock
    /// (`Disk`), or the operation worker stopping on its journal
    /// (`Operation`). The public category is also recorded as
    /// PendingReason::Failed; the initiating call retains the native cause.
    /// Callers failed by one cause, such as a stopped worker's waiters, share it.
    Failed { failure: LocalFailure, source: Arc<dyn std::error::Error + Send + Sync> },
    /// A later step, such as a rollback or closing another owner, failed
    /// too; neither failure is hidden.
    Cleanup { operation: Box<SyncError>, cleanup: Box<SyncError> },
    /// Reading or removing credentials or keys failed.
    SecureStorage(KeyError),
    /// Making or checking signed or encrypted bytes failed.
    Crypto(CryptoError),
    /// The request or invitation is absent, expired, or no longer matches.
    InvitationChanged,
    /// The member's provider account holds the store, so they can't be
    /// removed (§4, §13).
    StoreOwner,
    /// This call needs a newer app schema or coven format (§17).
    UpdateRequired(RequiredUpdate),
    /// A create-store entry names another store (§4).
    WrongStore { expected: StoreId, actual: StoreId },
    /// The requested journal row is not failed work eligible for this call:
    /// retry accepts provider work; discard accepts only app-owned work.
    NotPending(OperationId),
    /// This device publishes no store-log entry or snapshot until this
    /// retained reload finishes (§18).
    ReloadPending(OperationId),
    /// Decoding persisted operation data failed, retaining its cause.
    OperationData(serde_json::Error),
    /// Deleting an S3 key with admin access did not establish its absence;
    /// the key stays pending (§13).
    AccessKeyNotDeleted(DeleteAccessKeyError),
}

/// Why coven's deletion of an S3 key did not clear its record (§13).
pub enum DeleteAccessKeyError {
    /// The provider refused or failed the deletion.
    DeleteFailed(StorageError),
    /// No provider read followed the deletion.
    ObservationMissing,
    /// The read concerned another key.
    WrongKey,
    /// The read still found the key.
    StillPresent,
    /// The read failed; a failed read is never absence.
    ObservationFailed(StorageError),
}

/// An object that failed a check when read (§19.1).
pub struct DamagedObject {
    /// The object's path in storage.
    pub path: String,
    /// Which check failed, retaining its cause.
    pub failure: Refusal,
}

/// One audience snapshot, identified by its storage path (§15).
pub struct SnapshotId {
    pub audience: Audience,
    pub device: DeviceId,
    pub number: u64,
}

/// One thing coven cannot currently apply or deliver (§19.1).
pub struct PendingRecord {
    pub subject: PendingSubject,
    /// The first unmet condition for this subject, kept as a typed value.
    pub reason: PendingReason,
    /// This device for a local observation; otherwise the signed report's author.
    pub reported_by: DeviceId,
}

/// Stable identities; a record is keyed by subject and reporting device.
pub enum PendingSubject {
    Write(WriteId),
    Entry(EntryId),
    KeyCopy { audience: Audience, key: KeyId, member: MemberId, sealing: SealingKey },
    /// A key-copy object whose clear prefix does not decode: it names no
    /// audience, key or recipient, so its path identifies it (§19.1).
    KeyCopyPath { path: ObjectPath },
    Snapshot(SnapshotId),
    /// Retention waits for this active reader's position, or storage age (§15).
    Positions(DeviceId),
    File { device: DeviceId, file: FileId },
    Operation { id: OperationId, kind: OperationKind },
    Retention { path: ObjectPath },
    Audience(Audience),
    Agreement { audience: Audience, peer: DeviceId },
    JoinRequest(InviteId),
    Connection,
}

/// A condition coven has not yet observed (§19.1).
pub enum Prerequisite {
    /// A completed membership and key-copy catch-up fresh enough to send
    /// (§10), or one reaching 30 days before a snapshot's covered write (§15).
    CatchUp,
    Object(ObjectPath),
    DeviceRegistration(DeviceId),
    SchemaPublication { audience: Audience, version: u32 },
    Reload(Audience),
    Positions(DeviceId),
    /// Physical cleanup cannot release this entry's retained inputs (§9), or
    /// a snapshot waits to be prepared until it is final (§15).
    EntryFinality(EntryId),
}

/// One vocabulary for failed object checks in calls and pending records.
/// Every check that produces a native error retains it as an optional cause,
/// using its narrowest native type. Variants whose checks never produce a
/// native error stay unit; direct verdicts on other variants use cause: None.
/// Immediate calls retain those causes. Persisted and peer reports retain
/// the same variant without a native cause; equality and D8 encode the
/// variant only, never the process-local error object.
pub enum Refusal {
    Decryption { cause: Option<Arc<CryptoError>> },
    Signature { cause: Option<Arc<CryptoError>> },
    Parse { cause: Option<Arc<dyn std::error::Error + Send + Sync>> },
    InvalidWrite { cause: Option<Arc<DbError>> },
    NotAuthorized,
    InvalidCausality { cause: Option<Arc<MergeError>> },
    /// Identity checks can fail in the format, merge or key-material layer.
    WrongIdentity { cause: Option<Arc<dyn std::error::Error + Send + Sync>> },
    /// Authenticated file plaintext disagrees with the row's content hash.
    ContentHash,
}

pub enum RequiredUpdate {
    AppSchema { version: u32 },
    CovenFormat { version: u16 },
}

/// A source the attaching device cannot deliver. Local paths never travel to peers.
pub enum FileSourceFailure {
    Missing,
    Changed,
    Integrity,
}

/// Durable local categories; the failing call also retains its native typed cause.
pub enum LocalFailure {
    Database,
    Disk,
    KeyCustody,
    Crypto,
    Operation,
}

pub enum ProviderAccessAction {
    Grant,
    Revoke,
}

/// Reasons describe the current block, including work waiting without an error.
pub enum PendingReason {
    Missing { path: ObjectPath },
    Waits(Prerequisite),
    KeyUnavailable { audience: Audience, key: KeyId },
    UpdateRequired(RequiredUpdate),
    NoStorage,
    Storage { provider: CloudProvider, failure: StorageFailure },
    Refused(Refusal),
    /// Positions can be replaced, unlike an immutable refused object.
    InvalidPositions(Refusal),
    Dropped(DropReason),
    ProviderPending { action: ProviderAccessAction, account: String },
    PendingOwner { account: String },
    AccountInUse { account: String },
    AccessRemains { account: String, shares: Vec<RetainedAccess> },
    DeleteAccessKey(AccessKeyToDelete),
    FileUnavailable(FileMissingReason),
    Disagrees,
    Failed(LocalFailure),
    Paused,
    /// Provider work needs the storage account's admin access again; coven
    /// holds it for one operation only (§4).
    AdminAccessRequired,
    Removed,
    /// Sending needs this stable member's current credentials (§10).
    MemberKeysReplaced { member: MemberId },
}

pub enum Retry {
    Automatic,
    AfterUpdate,
    AppAction,
    Never,
}

impl PendingRecord {
    /// Automatic: missing objects, prerequisites, keys, pending provider/owner
    /// work, shared-account waits, invalid replaceable positions, and storage
    /// Network/Conflict/RateLimited/NotFound/SessionExpired.
    /// AfterUpdate: Refused and UpdateRequired.
    /// Never: a dropped entry or write, or this device's removal.
    /// An operation whose attempt replay dropped is AppAction: an explicit
    /// retry catches up and validates again, and a cause that still stands
    /// drops it again (§18).
    /// AppAction: every other reason, including source files and disagreements.
    /// A replay change can clear a replay-dependent reason without retrying
    /// its subject. It cannot undo an entry's LandedTooLate, but can undo a
    /// write's retirement-dependent LandedTooLate (§10).
    pub fn retry(&self) -> Retry;
}

/// The absent target named by a rejected store-log change (§9).
pub enum EntryTarget {
    Member(MemberId),
    Device(DeviceId),
    Circle(CircleId),
}

/// Why an entry or a write was dropped (§9, §10).
pub enum DropReason {
    /// Storage published this entry more than 30 days after an entry it had
    /// not read (§9). Shown as “landed too late”, including on its author's
    /// device. Permanent across replay and updates; the entry's position passes.
    /// For a write, publication exceeded a kept device retirement's or member-key
    /// replacement's 30-day deadline (§10). Its position passes, but dropping
    /// that deciding entry can restore it.
    LandedTooLate,
    /// A conflicting concurrent entry beat it.
    BeatenBy(EntryId),
    /// At its place in the replay, the member, device or circle it changes
    /// no longer existed.
    TargetGone(EntryTarget),
    /// Applying it would have left the store without an admin.
    NoAdminLeft,
    /// The proposed member pair reuses an established signing or sealing key.
    KeysAlreadyUsed,
    /// Its author's role didn't allow it, in the member list they had read.
    NotAllowed,
}

impl CovenHandle {
    /// Sets up storage on S3 with this member's access key (§4), after the
    /// safety check. `ConnectedStorage::storage` is the recorded location,
    /// carrying detected settings when the endpoint needed them.
    pub async fn setup_s3_storage(
        &self,
        storage: StorageConfig,
        device_name: &str,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<ConnectedStorage, StorageSetupError>;

    /// Probes the S3 service at this location with this member's key (§4),
    /// writing only fresh objects under the store's `.coven-check` prefix and
    /// deleting them, also on failure. Commits nothing.
    pub async fn detect_s3_settings(
        &self,
        storage: StorageConfig,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<S3Detection, StorageSetupError>;

    /// Runs sign-in through the builder's presenter and commits the tokens to
    /// session custody. Setup attaches this sign-in and persists credentials.
    /// Dropping the future cancels presentation without replacing held tokens.
    pub async fn authenticate(&self, access: OAuthAccess) -> Result<(), StorageSetupError>;

    /// Sets up Google Drive, Dropbox or OneDrive using coven's held sign-in.
    /// Without a new sign-in, reconnects using this store's custody credentials.
    /// A provider 401 refreshes once and retries once. Never opens sign-in UI.
    pub async fn setup_oauth_storage(
        &self,
        storage: StorageConfig,
        device_name: &str,
    ) -> Result<ConnectedStorage, StorageSetupError>;

    /// Sets up iCloud through the built-in adapter; other platforms return ProviderUnavailable.
    /// Coven obtains the current owner and creates the store's custom zone.
    pub async fn setup_cloudkit_storage(
        &self,
        container: &str,
        device_name: &str,
    ) -> Result<ConnectedStorage, StorageSetupError>;

    /// Opens store keys from their copies sealed to this member in storage
    /// (§11), keeps them in custody, and connects without starting sync.
    /// Uses the member-key session already held at open; it does not unlock
    /// custody again. Key selection follows §11; other waits remain in pending().
    pub async fn unlock_store_key(&self) -> Result<ConnectedStorage, StoreKeyUnlockError>;

    /// Whether the held session has the store key: `Available` or `Locked`.
    /// Reads no custody and derives no passphrase key.
    pub fn store_key_state(&self) -> Result<StoreKeyState, KeyError>;

    /// Finishes the active pass, removes this device's storage credentials and
    /// drops the provider client. If removing credentials fails, the connection
    /// and loop state stay. Storage's contents are untouched.
    pub async fn disconnect_storage(&self) -> Result<(), SyncError>;

    /// Starts syncing, building the provider client if absent from the configured
    /// location and custody credentials, refreshing once on rejection. Borrows
    /// keys already unlocked for the handle's session. Does nothing if already running or no storage is set up.
    pub async fn start_sync(&self) -> Result<(), SyncError>;

    /// Finishes the active pass and file transfers, then drops the provider
    /// client. Keeps session keys, credentials and location for the next start.
    /// Completion publishes `Stopped`, or `Disconnected` if no storage is set up.
    /// A release failure is returned and recorded as a pending connection record.
    pub async fn stop_sync(&self) -> Result<(), SyncError>;

    /// Syncs now instead of at the next idle tick. While idle, coven syncs
    /// every 30 seconds, and at once after a local write. Requests coalesce
    /// while a pass or connection backoff is active; provider cooldowns hold.
    pub fn sync_now(&self);

    /// The sync status, live. The first value is the current status.
    pub fn subscribe_sync_status(&self) -> watch::Receiver<SyncStatus>;

    /// The latest completed replacement stored in this installation, or None.
    /// It survives reopening; unsent edits were discarded, or may be lost
    /// for a damaged database (§10, §19.2).
    pub async fn device_reset(&self) -> CovenResult<Option<DeviceReset>>;

    /// The same pre-built query, including a replacement completed during open.
    pub fn subscribe_device_reset(&self) -> LiveQuery<Option<DeviceReset>>;

    /// Current local blocks and relevant authenticated peer reports, including
    /// while disconnected. Sorted by subject, then reporting device (§19.1).
    pub async fn pending(&self) -> CovenResult<Vec<PendingRecord>>;

    /// The same internal query as pending(), on the app's live-query mechanism.
    pub fn subscribe_pending(&self) -> LiveQuery<Vec<PendingRecord>>;

    /// How many uploads and downloads run at once.
    pub fn transfer_limits(&self) -> TransferLimits;

    /// Changes them while the store is open. Transfers already running keep
    /// the limit they started under.
    pub fn set_transfer_limits(&self, limits: TransferLimits);
}

/// A completed replacement of this installation, not a membership removal.
pub struct DeviceReset {
    pub old_device: DeviceId,
    pub new_device: DeviceId,
    pub reason: DeviceResetReason,
}

pub enum DeviceResetReason {
    /// The database's id does not match device-only custody.
    IdentityMismatch,
    /// Storage proves counters beyond those reserved in the local database.
    StorageAhead,
    /// An occupied immutable path holds different bytes.
    SlotMismatch,
    /// The store log has a kept replacement of this device's id.
    Replaced,
    /// The database could not be trusted; no local work was salvaged (§19.2).
    DatabaseDamage,
}

pub enum SyncStatus {
    /// No storage is set up on this device.
    Disconnected,
    /// Storage is set up and syncing is stopped; no provider client is required.
    Stopped,
    /// The last attempt could not reach storage, regardless of earlier success.
    /// The request preventing the pass received no usable provider response.
    Offline { error: Arc<StorageError> },
    /// The initial sync is queued or a sync is running.
    Syncing,
    /// The last pass finished; individual subjects may still be pending.
    Synced { finished_at: SystemTime },
    /// Storage answered with an error preventing the whole pass.
    /// Per-object and maintenance failures belong only in pending().
    Failed { error: Arc<StorageError> },
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
```

Example:

```rust
// Optional: an advanced-settings screen prefilled from detection.
let detection = handle
    .detect_s3_settings(storage.clone(), access_key_id.clone(), SecretText::new(secret_access_key.clone()))
    .await?;
let settings = edit_advanced_settings(detection.settings, &detection.observations);
detection.check(&settings)?;
let StorageConfig::S3 { profile, .. } = &mut storage else { unreachable!() };
*profile = Some(S3Profile::Custom(settings));

match handle.setup_s3_storage(storage, device_name, access_key_id, SecretText::new(secret_access_key)).await {
    Ok(connected) => remember(connected.storage),
    Err(error) => return show_setup_error(error),
}

let mut status = handle.subscribe_sync_status();
loop {
    match &*status.borrow_and_update() {
        SyncStatus::Failed { error } => show_sync_error(error),
        other => show_status(other),
    }
    if status.changed().await.is_err() {
        break;
    }
}
```

### E6 Operations and recovery

- Administrative calls first catch up on reachable storage (§9). No storage
  returns `SyncError::NoStorage`; a failed read returns its typed error.
  Neither starts an operation or reserves an entry. Already-started work
  retains its progress across network failures.
- A tried entry publishes its fixed bytes only while §10's current-credential
  gate permits it. If it lands too late, its
  `Entry` subject reports `Dropped(LandedTooLate)`; publication settles but
  the action has not succeeded. When replay drops an attempt's entry and
  work remains, starting over catches up and validates again, then resumes
  at the first step with a new entry number and, for a rotation, a fresh key.
  The old copies and entry stay in storage, and a late entry's pending
  record remains (§18, §19.1;
  [checked history: `restart_changes_key`](proofs/queue.md#checked-histories-and-limits)).
  Member-key replacement instead reports its rejection without creating a
  new entry to override the winner; replacing again needs an explicit call
  or approved invite. Retrying publication keeps the same pair and bytes.
- Every unfinished operation is a row in `_coven_operations`
  ([§18](coven.md#18-operations)).
- A failed step returns its typed error to a waiting caller and records its
  first unmet condition in `pending()` (E5). Pending work appears there too.
- Automatic snapshot writing, retention and reloads use the same list.
  Retention is derived each pass and has a path subject, with no operation
  row. Automatic operation records cannot be discarded through app-work calls.
- App work includes keeping a reset or schema change and an app-requested
  reload. Provider access work is recorded by replay; the app may retry a
  failed request but cannot discard the current access intention.
  Discarding an invite takes back what it granted, as cancelling does, and
  returns once that is done; an invite with a reserved approval entry
  publishes it instead (§13).
  Single-entry calls return their errors directly; a reserved entry still
  has its own pending subject.
- A record stores a typed reason, not an error string. The immediate call
  retains the native cause; reopening retains the reason and subject.

```rust
/// The local integer primary key of one unfinished operation (§18).
pub struct OperationId(pub i64);

/// The app purpose of an unfinished operation (§18.1, §19.3).
pub enum OperationKind {
    /// Make a circle and publish its first sealed key.
    CreateCircle,
    /// Seal the circle's history and add a member.
    AddCircleMember,
    /// An owner's device takes back access under the current replay.
    RevokeAccess,
    /// An owner's device restores access when replay returns a member.
    GrantAccess,
    /// Migrate the schema, snapshot it and raise the version.
    SchemaChange,
    /// Reload an audience, requested by the app or required by sync.
    ReloadFromSnapshot,
    /// Replace an audience key exposed to an excluded member or credential (§11).
    RotateKey,
    /// Publish a member's replacement pair and finish required custody (§11, E9).
    ReplaceMemberKeys,
    /// Write and verify a snapshot for retention.
    WriteSnapshot,
    /// Grant access, approve or decline a join, and settle the invite.
    Invite,
    /// An S3 key awaiting deletion with admin access or explicit
    /// confirmation (§13). Its record names the key.
    DeleteAccessKey,
    /// An admin desktop replacing a member's S3 key (§13).
    ReplaceAccessKey,
    /// Reseal this device's key copy the provider damaged under a new
    /// number and record the replacement in the store log (§11).
    ReplaceKeyCopy,
    /// Snapshot and reset an audience (§19.3).
    Reset,
}

/// Operation calls retain the same typed causes as sync calls (§18).
pub type OperationError = SyncError;

impl CovenHandle {
    /// Runs failed app-owned or provider access work again from the step
    /// after its last completed one. A cause that still stands fails again.
    pub async fn retry_pending_operation(&self, operation: OperationId) -> Result<(), OperationError>;

    /// Abandons a failed operation and deletes its row. Completed steps stay
    /// completed; reserved objects keep their bytes and §10's sending gates.
    /// Returns Pending with the operation's unmet condition while a reserved
    /// replacement needs settlement or its current binding needs custody. Before
    /// reservation, releasing a self-replacement pause requires held current keys.
    /// Other devices may already see stored copies or a complete snapshot;
    /// §18's publication order and reader checks still apply (queue proof:
    /// copies_visible_before_finish, snapshot_visible_before_finish).
    pub async fn discard_pending_operation(&self, operation: OperationId) -> Result<(), OperationError>;

    /// Reloads this device from the latest snapshot, keeping its waiting
    /// writes, as an operation (§19.2).
    pub async fn reload_from_snapshot(&self) -> Result<(), OperationError>;

    /// Resets the store from this device's copy, as an admin (§19.3): writes
    /// a snapshot, then records the reset in the store log. Every device,
    /// including this one, reloads from it. Pre-reset writes outside it are
    /// ignored without loss records; queued writes still upload to fill the log.
    pub async fn reset_store(&self) -> Result<(), SyncError>;
}
```

### E7 Moves and uploads

- A row moves to another audience by an ordinary write that changes its
  root's audience column, or points a descendant at a parent in another
  audience ([§14.2](coven.md#142-moving-rows)).
  - The write commits at once on this device; its moved rows' file
    references stay fixed ([§16.1](coven.md#161-kinds-and-where-files-are)).
- Files enter the upload queue as the attaching write commits, including
  with no storage connected. That write fixes the path and key. Upload
  completion changes no row or `FileRef`; `file_status` derives availability
  ([§16.1](coven.md#161-kinds-and-where-files-are)). Pinning and caching
  change no file reference either.
- Upload attempts read the source again, checking its size and whole-file
  content hash before transfer, and the first-read hash of each plaintext
  chunk before encryption. A changed user original reports `UserFileChanged`;
  a changed app-provided copy reports `Integrity`. Retries retain the same
  file id, key and chunk hashes, without an encrypted local copy.

```rust
impl CovenHandle {
    /// Queue, pause setting and provider-confirmed session progress in one
    /// committed database snapshot, available while disconnected.
    pub async fn uploads(&self) -> CovenResult<UploadQueue>;

    /// The same pre-built query, through the app's live-query mechanism.
    pub fn subscribe_uploads(&self) -> LiveQuery<UploadQueue>;

    /// Retries waiting uploads now; individual failures stay in pending().
    /// Overrides ordinary backoff or retries a repaired source, but never
    /// bypasses a provider Retry-After cooldown.
    /// Automatic delays start at 1 second and double to at most 5 minutes
    /// with persisted deadline T and wait W. Restart waits max(0, min(T-now, W));
    /// a live provider cooldown is never discarded (§16.5).
    pub async fn retry_uploads_now(&self) -> Result<DrainOutcome, SyncError>;

    /// Pauses uploads, or resumes them. A paused upload keeps its place,
    /// including a provider upload session in progress.
    pub async fn set_uploads_paused(&self, paused: bool) -> CovenResult<()>;
}

pub struct UploadQueue {
    pub paused: bool,
    /// Oldest first.
    pub files: Vec<QueuedUpload>,
}

pub struct QueuedUpload {
    pub file: FileRef,
    pub phase: UploadPhase,
    pub queued_at: SystemTime,
}

pub enum UploadPhase {
    /// Not started, or waiting for its retry delay.
    Waiting,
    /// Encrypted bytes confirmed by the provider and persisted in the
    /// resumable session. Sending bytes alone does not advance this value.
    Uploading { bytes_confirmed: u64, bytes_total: u64 },
}

pub enum DrainOutcome {
    Drained { uploaded: usize },
    QueueEmpty,
    Paused,
}
```

Example:

```rust
let mut uploads = handle.subscribe_uploads();
loop {
    let state = uploads.next().await?;
    for upload in &state.files {
        if let UploadPhase::Uploading { bytes_confirmed, bytes_total } = upload.phase {
            show_progress(upload.file.key(), bytes_confirmed, bytes_total);
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

Ana's photo cannot be downloaded while her voice note continues arriving.
The photo's reason is in `pending()`; eager-fill status still describes the
files and bytes completed. There is no competing failure state in that progress.

```rust
/// Progress while keeping files whole on this device (§16.4, E8).
pub struct PinProgress {
    /// Files whose bytes have all been kept.
    pub files_completed: u64,
    /// Files requested, including those already present.
    pub files_total: u64,
    /// Verified bytes present in the committed cache for the requested files.
    pub bytes_cached: u64,
    /// Total plaintext size of the requested files, including cached bytes.
    pub bytes_total: u64,
}

/// Progress of files declared CacheEager (§16.4, E8).
/// Failures appear only in pending(); independent downloads keep progressing.
pub enum EagerCacheFillStatus {
    /// No files are waiting to download.
    Idle,
    /// Files are being fetched into the cache.
    Downloading(PinProgress),
    /// The app stopped these downloads.
    Cancelled(PinProgress),
}

impl CovenHandle {
    /// The file a row carries, as of its current file version. Its four file
    /// columns, audience and version are read in one committed state. A row
    /// without a file returns `DbError::FileAbsent`; malformed stored
    /// file facts return `DbError::DamagedDatabase`.
    pub async fn file_ref(&self, table: &str, key: impl Into<RowKey>) -> Result<FileRef, DbError>;

    /// Calls single-object status, then checks the uploader's current state and its signed
    /// undeliverable-file report (§16.1). Checks the reference against its row.
    /// Network failures and denied access are errors, not Missing; an object
    /// at the fixed path of any size but the file's complete one is `Integrity`.
    pub async fn file_status(&self, file: &FileRef) -> Result<FileStatus, FileReadError>;

    /// Reads a whole file, checking it against its row.
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>, FileReadError>;

    /// Opens a file for reading ranges (§16.3). Opening checks the file
    /// against its row once; keep the stream for as long as the file is read.
    /// Its shared store lock prevents deletion until it and its I/O finish.
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError>;

    /// The path, size and modification time coven recorded for a row's
    /// user-provided file, or `None` when the row has none.
    pub async fn user_file(&self, table: &str, key: impl Into<RowKey>) -> Result<Option<UserFile>, DbError>;

    /// Keeps uploaded files whole on this device regardless of the cache
    /// budget, downloading what is missing before the call returns.
    /// Progress is read from pin_progress().
    pub async fn pin(&self, files: &[FileRef]) -> Result<(), FileReadError>;

    /// Verified cache coverage for these fixed references, in one snapshot.
    pub async fn pin_progress(&self, files: Vec<FileRef>) -> CovenResult<PinProgress>;

    /// The same pre-built cache query with the same fixed references.
    pub fn subscribe_pin_progress(&self, files: Vec<FileRef>) -> LiveQuery<PinProgress>;

    /// Stops keeping files; they stay in the cache until the budget evicts them.
    pub async fn unpin(&self, files: &[FileRef]) -> Result<(), FileReadError>;

    /// Whether each row's file is pinned, one answer per key in order, or
    /// `None` for a key with no row carrying a file. A file not yet uploaded
    /// reads as not pinned.
    pub async fn rows_pinned(&self, table: &str, keys: Vec<RowKey>) -> CovenResult<Vec<Option<bool>>>;

    /// The same internal query, live. To watch different keys, drop this query
    /// and subscribe with the new keys; there is no separate pin-query type.
    pub fn subscribe_rows_pinned(&self, table: &str, keys: Vec<RowKey>) -> LiveQuery<Vec<Option<bool>>>;

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
    pub async fn eager_cache_fill_status(&self) -> CovenResult<EagerCacheFillStatus>;

    /// The same query over cache reservations, cancellation and verified ranges.
    pub fn subscribe_eager_cache_fill_status(&self) -> LiveQuery<EagerCacheFillStatus>;

    /// Records cancellation of the current eager requests without stopping sync.
    pub async fn cancel_eager_cache_fill(&self) -> CovenResult<()>;
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
    /// The fixed destination chosen by the attaching write, even before upload.
    pub fn path(&self) -> ObjectPath;
    pub fn id(&self) -> FileId;
    pub fn uploader(&self) -> DeviceId;
}

/// Current remote availability; never persisted in the app row (§16.1).
pub enum FileStatus {
    Available,
    /// Confirmed absent; this active device has not reported it undeliverable.
    Uploading { device: DeviceId },
    /// Confirmed absent, with a reason the uploader cannot supply it.
    Missing { device: DeviceId, reason: FileMissingReason },
}

pub enum FileMissingReason {
    DeviceRemoved,
    DeviceReplaced,
    Source(FileSourceFailure),
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
    /// Remote status or uncached bytes were requested with no storage connected.
    NoStorage,
    /// Storage confirms absence; the active uploader has not reported failure.
    Uploading { id: String, device: DeviceId },
    /// Storage confirms absence and the uploader cannot supply it (§16.1).
    Missing { id: String, device: DeviceId, reason: FileMissingReason },
    /// A user-provided file is gone from its recorded path.
    UserFileMissing { id: String, path: PathBuf },
    /// A user-provided file's size, modification time or checked content no
    /// longer matches what coven recorded.
    UserFileChanged { id: String, path: PathBuf },
    /// A chunk, a copy on this device, or the object at the fixed path failed
    /// its check; a stored object of the wrong size is damage, not absence (§16.1).
    Integrity { id: String },
    /// The range lies outside the file.
    RangeOutOfBounds { id: String, offset: u64, end: u64, size: u64 },
    /// Storage failed, including Network when required chunks aren't cached.
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
    Err(FileReadError::Storage(error)) if error.failure() == StorageFailure::Network => show_not_downloaded(),
    Err(error) => return Err(error.into()),
}
```

### E9 Members and devices

- Only admins add and remove members, and change roles; each member removes
  their own devices, and admins any device ([§9](coven.md#9-members-and-roles)).
- Administrative calls require an online store-log catch-up (§9, E6).
- A member records their own new storage access; an admin may also record an
  S3 replacement for another member. Authority uses the author's role in the
  entry's recorded view, independently of who holds provider-admin access.
  `ProviderSignOut::ReplaceAccessKey` directs the person to the admin desktop
  flow in §13, `replace_member_access_key`: create, deliver to remaining
  devices, record for that member, then delete the old key. Each remaining
  device installs the delivered key on its next sync. Without admin access,
  `replace_access_key` records a console-made key instead. §4 lists the
  operation-scoped admin-access sources.
- A member can replace their own signing/sealing pair; any admin can publish
  a prepared pair for an existing member with `replace_member_keys_for`.
  Replacement invites provide the approval/bootstrap flow (E10), subject to
  their provider-access requirements. Another
  admin's concurrent replacement wins over self-replacement, even when self
  is an admin. Member removal wins. A rejected replacement returns its replay
  reason, never restarts to overrule the winner. Keys must both be fresh.
- Replacement preserves the id, role, circle memberships and device ids.
  Historical public bindings remain available for signature checks. Old
  sealing credentials expose their audience keys (§11); storage access is
  a separate operation. Neither a restore code nor a key copy proves approval.
- Removing a member publishes one entry. It returns `()` once replay keeps
  it; applying entries is the single path that records provider access work.
- The pending list describes unresolved access work. Each owner's device
  serializes grant and revoke requests, and performs the opposite request
  when replay reverses the intention (§4, §13).
  Pending and failed work appears in `pending()`, with the operation id for
  retry. Derived provider work cannot be discarded while its current
  intention remains unmet: disappearance from this list must not hide a
  retained grant. A replay change can replace or remove an account-sharing
  intention; S3 deletion records follow the rule below.
- Each distinct superseded or removed-member S3 key has a `DeleteAccessKey`
  record, including keys recorded by an admin and keys in dropped entries.
  The old key becomes pending when its replacement is recorded. A record
  clears only after coven deletes that exact key with admin access and a
  subsequent provider read confirms that id absent, or after an explicit
  `confirm_access_key_deleted`. Missing, failed or wrong-key reads retain it;
  a delete reply, elapsed time or replay reversal never suffices. Both paths
  retain confirmation by key id, so later entries and retries cannot restore
  the notice. Failed commits preserve pending work and report the failure.
- Removing an account can leave access through a parent, a grant reaching other
  accounts, an unidentified recipient, or the owner. `PendingReason::AccessRemains`
  retains these grants and their reasons for the app to present to the owner;
  a waiting call receives that record as `SyncError::Pending`. Each account
  whose grants remain is its own `RevokeAccess` record, so a member whose
  recorded accounts kept grants lists every such account.
  The revocation remains in `pending()` until a retry confirms the owner
  removed those grants, or replay no longer requires that revocation.
  This also preserves the result when no app call is waiting, including a
  revocation initiated by applying another device's removal.

```rust
impl CovenHandle {
    /// The members and their devices, as this device's store log has them.
    pub async fn members(&self) -> CovenResult<Vec<MemberInfo>>;

    /// The same internal query, live (E4).
    pub fn subscribe_members(&self) -> LiveQuery<Vec<MemberInfo>>;

    /// Replaces this member's signing and sealing keys (§9, §11).
    /// Pauses local app writes and confirms this installation's queued writes
    /// and entries first; attempted app writes return DbError::MemberKeysChanging.
    /// Keeps a fresh private pair durably before sending
    /// prerequisite key copies and the entry signed by the current pair.
    /// Failures retain both pairs and recorded progress for idempotent retry;
    /// no local writes resume under an uncommitted binding or failed custody.
    /// Returns once the binding is kept and custody and restore code refreshed.
    /// The app asks the person to keep the new restore code. Other devices
    /// holding old credentials report MemberKeysReplaced and need current keys.
    pub async fn replace_member_keys(&self) -> Result<(), SyncError>;

    /// Publishes a prepared public pair for an active member under §9's
    /// authority rules. The target retains its private pair before this call;
    /// this call receives no private keys and changes no local private custody.
    /// Confirms the initial and held historical audience-key copies before
    /// publishing the replacement; returns once replay keeps it.
    /// Any admin may replace another member, independently of provider ownership.
    /// A self target is still self-replacement, even for an admin; callers
    /// replacing this installation's own pair use replace_member_keys instead.
    /// Rejection is reported without restarting against the winner. Provider
    /// access and installing the prepared private pair are separate work.
    pub async fn replace_member_keys_for(
        &self,
        member: &MemberId,
        keys: MemberPublicKeys,
    ) -> Result<(), SyncError>;

    /// On S3: switches this member to a key supplied through §4's admin-access
    /// or console path and records the new key's id in the store log (§9).
    /// The restore code does not change. They keep the key separately,
    /// update their other devices with update_credentials, and the old key
    /// becomes pending deletion under §13. Requires connected storage.
    /// If publication fails, the call returns the failure and retains the new credentials:
    /// the remote entry may already exist. Retry with the same key to finish publication.
    /// Member removal lists every recorded key until confirmed deleted,
    /// including this key if replay drops its set-access entry (§13).
    pub async fn replace_access_key(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError>;

    /// On a device that already has the store open: takes this member's S3
    /// key directly. The member identity, location and restore code do not
    /// change. A disconnected handle keeps the credentials for its next
    /// connection; a connected handle also replaces its provider's credentials.
    pub async fn update_credentials(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError>;

    /// Changes a member's role, as an admin (§9).
    pub async fn set_member_role(&self, member: &MemberId, role: MemberRole) -> Result<(), SyncError>;

    /// Explicitly confirms that the admin deleted this S3 key, including
    /// through guided console steps. Clears only this id's DeleteAccessKey
    /// record and retains its confirmation (§13). Coven's automatic path
    /// instead requires its admin deletion followed by a provider read
    /// confirming this exact id absent; it never infers this confirmation.
    pub async fn confirm_access_key_deleted(&self, access_key_id: &str) -> Result<(), SyncError>;

    /// Obtains the storage account's admin access from the source the person
    /// chose, held in memory by the operations it is passed to and never
    /// stored, logged or put in the store log or invites (§4). S3 only.
    pub async fn admin_access(&self, source: AdminAccessSource) -> Result<Arc<dyn AccessKeyAdmin>, SyncError>;

    /// The bucket, the store's prefix and the filled member policy for making
    /// a key in the provider's console (§4).
    pub async fn guided_access_key(&self) -> Result<GuidedAccessKey, SyncError>;

    /// Deletes the key a DeleteAccessKey record names with admin access, then
    /// reads the provider for that exact key. Only its absence clears the
    /// record; failures return AccessKeyNotDeleted and the record stays (§13).
    pub async fn delete_access_key(&self, operation: OperationId, admin: Arc<dyn AccessKeyAdmin>) -> Result<(), SyncError>;

    /// Resumes work pending with AdminAccessRequired, holding this admin access
    /// for that operation only (§4).
    pub async fn retry_with_admin_access(&self, operation: OperationId, admin: Arc<dyn AccessKeyAdmin>) -> Result<(), OperationError>;

    /// Replaces a member's S3 key with admin access (§13): makes the key and
    /// checks it from the member's side, delivers it sealed to the member's
    /// devices (D9), records it for them, then deletes the old key and reads
    /// it gone. Their own key, or another member's as an admin; any other
    /// caller is refused before admin access is used. Returns the new key for
    /// the person to keep, and the old key's outcome. A dropped entry returns
    /// its refusal, with the made key pending deletion beside the old one.
    /// After a restart the operation waits with AdminAccessRequired.
    /// Discarding it leaves no key unrecorded (§13).
    pub async fn replace_member_access_key(&self, member: &MemberId, admin: Arc<dyn AccessKeyAdmin>) -> Result<AccessKeyReplacement, SyncError>;

    /// Publishes the removal of this member and all their devices (§13).
    /// Returns once the entry is kept. Provider work appears only in pending(),
    /// including owner waits, retained grants and S3 keys until confirmed deleted.
    pub async fn remove_member(&self, member: &MemberId) -> Result<(), SyncError>;

    /// Removes a device. The provider can't cut off one device alone, so the
    /// result says how its member signs out of the provider and signs in
    /// again on the devices they keep (§13).
    pub async fn remove_device(&self, device: DeviceId) -> Result<ProviderSignOut, SyncError>;
}

/// A member's replaced S3 key: the new key, which the member keeps
/// separately, and what happened to the old one (§13).
pub struct AccessKeyReplacement {
    pub key: S3MemberKey,
    pub old_key: OldKeyDeletion,
}

pub enum OldKeyDeletion {
    /// No deletion remains: coven deleted it and read it absent, or its
    /// record had already cleared.
    Deleted,
    /// Made in the console: delete it there, then confirm_access_key_deleted.
    Console { operation: OperationId },
    /// Deleting failed; the record stays for delete_access_key.
    Failed { operation: OperationId, error: SyncError },
}

/// An S3 key superseded, recorded for a removed member, or from an ended invite (§13).
pub struct AccessKeyToDelete {
    pub access_key_id: String,
    /// Who holds the key; deleting it with admin access uses these handles.
    pub principal: S3Principal,
    /// The member it was recorded for; none for an invite that ended unanswered.
    pub member: Option<MemberId>,
}

/// The non-secret handles deleting an S3 key needs (D6).
pub enum S3Principal {
    /// Made by hand in the provider's console; only the console deletes it.
    Console,
    /// AWS IAM, Ceph RGW accounts, SeaweedFS's embedded IAM and Versity
    /// Gateway's standalone IAM.
    IamUser { user: String, policy: IamPolicy },
    /// GCS: the HMAC key's service account and its managed folder.
    GcsServiceAccount { project: String, email: String, managed_folder: String },
    /// Cloudflare: the account holding the API token whose id is the key id.
    R2Token { account: String },
    /// RustFS: the parent user of the service account whose access key is the key id.
    RustFsServiceAccount { parent: String },
}

pub enum IamPolicy {
    Inline { name: String },
    Managed { arn: String },
}

/// A service's key administration with the storage account's admin access,
/// for one operation (§4). Coven's minters implement it per catalogued profile.
#[async_trait]
pub trait AccessKeyAdmin: Send + Sync {
    /// Create the principal and a key limited to the scope's store, tolerating
    /// what an earlier attempt with this scope made.
    async fn mint(&self, scope: &MintScope) -> Result<S3MemberKey, StorageError>;
    /// The member-side check before delivery (§4).
    async fn probe(&self, scope: &MintScope, key: &S3Credentials) -> Result<(), StorageError>;
    /// Remove whatever an attempt with this scope made.
    async fn roll_back(&self, scope: &MintScope) -> Result<(), StorageError>;
    /// Delete the key and the principal holding it; absence succeeds.
    async fn revoke(&self, access_key_id: &str, principal: &S3Principal) -> Result<(), StorageError>;
    /// Read whether this exact key remains; a failed read is never absence.
    async fn key_exists(&self, access_key_id: &str, principal: &S3Principal) -> Result<bool, StorageError>;
}

/// Where admin access comes from (§4). The CLI and browser sources exist on
/// desktops only; coven runs or reads nothing before the person chooses one.
pub enum AdminAccessSource {
    AwsCli { profile: Option<String> },
    GcloudCli { account: Option<String> },
    GoogleSignIn,
    AwsIdentityCenter {
        start_url: String,
        region: String,
        account_id: String,
        role_name: String,
        presenter: Arc<dyn DeviceSignInPresenter>,
    },
    Pasted(AdminCredential),
}

/// Admin access for one operation; formatting never shows a secret.
pub enum AdminCredential {
    AccessKey {
        access_key_id: String,
        secret_access_key: SecretText,
        session_token: Option<SecretText>,
        /// Versity Gateway's standalone IAM service, which listens apart
        /// from the S3 endpoint; None for every other service (§4).
        iam_endpoint: Option<Url>,
    },
    BearerToken(SecretText),
}

/// Shows a device sign-in page and code. `DesktopDeviceSignIn` opens the browser.
#[async_trait]
pub trait DeviceSignInPresenter: Send + Sync {
    async fn show(&self, verification_url: &str, user_code: &str) -> Result<(), StorageError>;
}

/// Guided console steps for a key made by hand (§4).
pub struct GuidedAccessKey {
    pub bucket: String,
    pub store_prefix: String,
    pub policy: String,
}

/// What a minted key reaches; the principal name is fixed by the journal.
pub struct MintScope { pub store: StoreId, pub name: String, pub member: String }

/// A member's S3 key and who holds it.
pub struct S3MemberKey { pub credentials: S3Credentials, pub principal: S3Principal }

// Each DeleteAccessKey record belongs to its own DeleteAccessKey operation,
// whose id the app passes to confirm or delete with admin access. Retry and
// discard refuse it: only the two clearing events end it.

// Pending notices are DeleteAccessKey records in _coven_pending. Only verified
// admin deletion or explicit confirm_access_key_deleted clears one. Confirmation
// is retained separately by key id, so later entries and retries cannot restore it.

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
    pub keys: MemberPublicKeys,
    pub role: MemberRole,
    pub devices: Vec<DeviceInfo>,
    /// Whether this is the member using this device.
    pub is_self: bool,
}

pub struct DeviceInfo {
    pub id: DeviceId,
    pub name: String,
    pub state: DeviceState,
}

pub enum DeviceState {
    Active,
    Removed,
    /// A kept add-device entry replaces this id. Its older objects are
    /// judged by their recorded reads, with no upper log number (§10).
    Replaced,
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
    /// An admin desktop with the storage account's admin access creates a new
    /// S3 key, delivers it to the member's remaining devices, records it for
    /// that member, then deletes the old key (§13, replace_member_access_key). The old key is pending
    /// DeleteAccessKey work from recording until verified deletion or explicit
    /// confirmation. Keep the new member key separately; the restore code does
    /// not change. Admin access is held for one operation and never stored (§4).
    ReplaceAccessKey,
}
```

### E10 Joining and restore

- A person's new device opens the store from their restore code: scanned
  as a QR code, typed in, or read from iCloud Keychain
  ([§12.1](coven.md#121-a-persons-new-device)).
- The restore code contains only the stable member id and current private
  pair, store id and name, and
  storage location. On S3, the person keeps their access key separately,
  such as in a password manager, and supplies it on the new device. Google
  Drive, Dropbox and OneDrive likewise require the person's provider sign-in.
- A QR scan from one of the person's own devices may supply both the code
  and the S3 key, because the person controls the showing device. The key
  accompanies the code; it is never part of the restore-code encoding.
  iCloud Keychain may also hold the S3 key separately alongside the code.
- An admin adds a person with an invite, and approves their join request
  ([§12.2](coven.md#122-adding-a-person)).
- Before `create_invite` makes any provider change, coven checks the registered
  access. A Drive app registered only for `drive.file` fails with
  `StorageFailure::AccessNotRegistered { required: OAuthAccess::GoogleDrive(GoogleDriveAccess::Full) }`.
  An App-folder Dropbox store fails with `DropboxAppFolderNotShareable`:
  sharing it requires store relocation, outside this operation.
- On Drive, the first invite whose device still has narrow access presents
  incremental authorization through the configured presenter, on the same client
  with `include_granted_scopes=true`. Coven checks the granted scopes and the
  signed-in account, commits the replacement credentials, then grants the
  recipient access. A later invite reuses the held full grant. Consent denial,
  cancellation, an insufficient grant or a different account fails before sharing
  and preserves the previous credentials. Existing file ids and location stay
  unchanged. Joining requires full Drive access too. A restored device initially
  requests `drive.file`; if Drive reports the folder is shared, the adapter returns
  `AccessNotGranted` requiring full access before listing its contents. A 404 for
  the folder under `drive.file` instead returns `GoogleDriveFolderNotVisible`,
  retaining the native error: another person's shared folder is outside that
  grant's view, so this does not establish that the folder was deleted. The app
  requests full access and retries restore to inspect the folder. It must not
  treat an incomplete view of other members' files as an empty store.
- A replacement invite fixes an existing active id before the requester
  generates keys. Approval binds the exact signed request's new pair after
  publishing the initial and available historical audience-key copies. The
  join waits for its approving writer's kept matching id-and-pair binding
  before registration; an equal-payload winner does not rescue a rejected approval.
  A losing replacement fails with its replay reason even if copies exist.
  Restore checks the supplied pair against the current binding and reports
  `SyncError::MemberKeysReplaced` for an old code; it never rebinds old keys.
- Each call takes the app's layout-scoped `CovenBuilder`, plus its code,
  device name, status callback and cancellation receiver, and
  returns the open `CovenHandle`. Restore also accepts the separately
  supplied S3 credentials. There is no second app-side open.
  - The builder supplies tables, migrations, custody, clock, id source,
    provider clients, sign-in presenter and file-transfer limits once. The app
    calls `builder.authenticate(access)` first when `oauth_access` is present;
    bootstrap uses that held sign-in and never opens UI. Session-only `InMemory`
    custody lives with the returned handle. OAuth sign-in tokens move from
    session custody into the device keychain on successful publication.
  - Every call migrates coven's own local tables in place before applying
    the app's migrations, as a writable open does ([§17.2](coven.md#172-covens-schema)).
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
    /// Stable member id fixed by either kind of code.
    pub member: MemberId,
    /// The provider holding the store.
    pub cloud_provider: CloudProvider,
    /// Sign-in request for this code: Drive File for a personal restore,
    /// Drive Full for an invite, or the recorded Dropbox access level.
    pub oauth_access: Option<OAuthAccess>,
}

/// The two codes used to open a store on a new device (§12).
pub enum CodeKind {
    /// Holds only the person's member id and private keys, store id/name and location.
    Restore,
    /// Holds an invite id, chosen member id, secret, initial key id, inviting writer and its
    /// key-copy number at invite time; approval is required (§12.2).
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

/// Opens sign-in and returns the provider's complete redirect URL. Coven holds
/// the expected state, PKCE verifier and resulting tokens (E5, E10).
#[async_trait]
pub trait OAuthPresenter: Send + Sync {
    /// The registered redirect for this provider, without query or fragment.
    fn redirect_uri(&self, provider: CloudProvider) -> &str;
    /// Opens the page and returns its redirect. Native cancellation returns
    /// `OAuthError::Cancelled`; dropping this future must dismiss the sheet.
    async fn present(&self, authorization_url: &str) -> Result<SecretText, OAuthError>;
}

/// The desktop presenter: opens the system browser and listens locally at
/// `http://localhost:19284/callback`. Not shipped on iOS or Android.
pub struct DesktopOAuthPresenter;

/// Provider sign-in could not finish (E5, E10).
pub enum OAuthError {
    /// This provider has no OAuth client id or presenter configured.
    Unavailable(CloudProvider),
    /// The redirect state is missing or different.
    StateMismatch,
    /// The provider returned access_denied, retaining its checked callback.
    Denied(OAuthRedirectError),
    /// The callback omitted its authorization code.
    MissingCode,
    /// The person cancelled sign-in.
    Cancelled,
    /// No callback arrived before the deadline.
    Timeout,
    /// A new provider sign-in is required.
    Reauthorize(CloudProvider),
    /// The redirect URI or callback request is malformed.
    InvalidRedirect,
    /// The browser or local redirect listener failed.
    Io(std::io::Error),
    /// The app's native sign-in sheet failed.
    Presentation(Box<dyn std::error::Error + Send + Sync>),
    /// The provider refused sign-in or token exchange, or could not be reached.
    Storage(StorageError),
}

/// An original checked OAuth error callback, retained as the cause of Denied
/// or StorageError::Provider. Display and Debug omit the callback because the
/// provider may echo secrets in any field.
pub struct OAuthRedirectError { /* private */ }

impl OAuthRedirectError {
    /// Inspect the unmodified callback explicitly, including provider extensions.
    pub fn callback(&self) -> &str;
}

impl CovenHandle {
    /// This member's restore code: their stable id and current private pair, the store's id and
    /// name, and storage location only. It never carries storage credentials.
    /// Selects the pair current in checked replay; absent current credentials
    /// return MemberKeysReplaced rather than exporting a retired pair.
    /// The app shows it as a QR code, blurred until the person taps it,
    /// and asks them to write it down at setup.
    pub async fn restore_code(&self) -> Result<String, SyncError>;

    /// Fixes a fresh member id or an existing active replacement target, as
    /// an admin. The invite expires after a day. Replacement preserves role,
    /// circle memberships and device ids; changed provider access is separate.
    pub async fn create_invite(&self, target: InviteTarget, access: InviteAccess) -> Result<Invite, SyncError>;

    /// Join requests waiting for approval, live. The first value is the
    /// current list.
    pub fn subscribe_join_requests(&self) -> watch::Receiver<Vec<JoinRequest>>;

    /// Checks the invite's member id and the exact request's fresh key pair.
    /// Confirms initial/historical key copies, then adds the new member with
    /// the invite's role or replaces the existing member's keys. A rejected
    /// replacement is reported, not automatically retried with a new entry.
    pub async fn approve_join_request(&self, request: &JoinRequest) -> Result<(), SyncError>;

    /// Declines the request, and takes back the storage access its invite
    /// granted; on S3 the key stays pending until deleted and cleared under E9.
    pub async fn decline_join_request(&self, request: &JoinRequest) -> Result<(), SyncError>;

    /// Cancels an invite before anyone joins with it, taking back the
    /// storage access it granted; on S3 the key stays pending until deleted
    /// and cleared under E9.
    pub async fn cancel_invite(&self, invite: &InviteId) -> Result<(), SyncError>;
}

/// The admin's choice, fixed before sharing the invite (§12.2, §12.3).
pub enum InviteTarget {
    NewMember { role: MemberRole },
    ReplaceMemberKeys { member: MemberId },
}

/// How the invited person reaches storage (§12.2).
pub enum InviteAccess {
    /// Google Drive, Dropbox, OneDrive or iCloud: the store is shared with
    /// this account.
    ProviderAccount { email: String },
    /// S3: a member key made in the provider's console (§4).
    S3AccessKey { access_key_id: String, secret_access_key: SecretText },
    /// S3: coven makes the person's key with this admin access, held by the
    /// invite's operation until the key passes its member-side check (§4).
    S3AdminAccess(Arc<dyn AccessKeyAdmin>),
}

pub struct Invite {
    pub id: InviteId,
    pub member: MemberId,
    /// The code to show as a QR code.
    pub code: String,
    pub expires_at: SystemTime,
}

pub struct JoinRequest {
    pub invite: InviteId,
    /// The stable id fixed by the invite, never chosen by the requester.
    pub member: MemberId,
    pub keys: MemberPublicKeys,
    /// The name the new device gave itself, for the admin to check.
    pub device_name: String,
    pub provider_account_email: Option<String>,
}

/// What a restore code or invite is for: the store and member ids, store
/// name, provider, and whether the provider needs a sign-in first.
pub fn decode_code_info(code: &str) -> Result<CodeInfo, CodeError>;

/// Bootstrap progress, using the same records before an open handle exists.
pub enum BootstrapStatus {
    Connecting,
    WaitingForApproval,
    Loading,
    Pending(Vec<PendingRecord>),
}

/// The member's S3 access key, supplied separately from a restore code.
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: SecretText,
}

/// Opens the store on a new device from the person's restore code, scanned
/// or typed. S3 requires Some(s3_credentials), entered separately or received
/// alongside the code from one of the person's own devices. Other providers
/// require None; the builder holds its own sign-in when `oauth_access` is present.
/// Missing S3 credentials or credentials for another provider fail before
/// bootstrap with BootstrapError::Sync carrying
/// StorageFailure::InvalidConfiguration.
pub async fn restore_from_code(
    builder: CovenBuilder,
    code: &str,
    s3_credentials: Option<S3Credentials>,
    device_name: &str,
    on_status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<CovenHandle, BootstrapError>;

/// Opens the store on a new Apple device from its iCloud Keychain restore
/// code (§12.1). On S3, uses the supplied credentials when Some; None reads
/// the S3 key held separately alongside the code. An absent key fails with
/// BootstrapError::Sync carrying StorageFailure::InvalidConfiguration;
/// a keychain read failure is returned. Other providers require None and
/// their own sign-in.
/// Returns None when the keychain holds no store.
pub async fn restore_from_keychain(
    builder: CovenBuilder,
    s3_credentials: Option<S3Credentials>,
    device_name: &str,
    on_status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<CovenHandle>, BootstrapError>;

/// Completes the provider's recipient acceptance using the encrypted invite
/// code before sending the join request. CloudKit acceptance uses coven-storage's
/// native adapter. If access is removed while joining, the provider's
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
    on_status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<CovenHandle>, BootstrapError>;

impl OAuthClients {
    /// Registers the access the app offers; None disables a provider.
    pub fn new(
        google_drive: Option<GoogleDriveClient>,
        dropbox: Option<DropboxClients>,
        onedrive_client_id: Option<String>,
        clock: ClockRef,
    ) -> Self;
    /// Supplies a Google Desktop client's secret, which Google requires for code
    /// exchange and refresh; mobile clients have none. It is sent only to Google's
    /// token endpoint. Refused when no Google client id is set or the secret is empty.
    pub fn with_google_client_secret(self, secret: SecretText) -> Result<Self, OAuthError>;
}

/// Google's registered maximum, or the access requested by one sign-in.
pub enum GoogleDriveAccess { File, Full }

/// The Google client and its registered access; the client id stays private.
pub struct GoogleDriveClient { /* private */ }
impl GoogleDriveClient {
    pub fn new(client_id: String, registered_access: GoogleDriveAccess) -> Self;
}

/// Dropbox access is fixed by the app registration, not an incremental scope.
pub enum DropboxAccess { AppFolder, FullDropbox }

/// One or both of the app's Dropbox registrations. There is no empty variant.
pub enum DropboxClients {
    AppFolder { app_key: String },
    FullDropbox { app_key: String },
    Both { app_folder_key: String, full_dropbox_key: String },
}

/// Access for this request. New Google stores request File; shared stores need Full.
/// Dropbox callers supply the creation choice explicitly, using the only
/// registered access when one key is configured.
pub enum OAuthAccess {
    GoogleDrive(GoogleDriveAccess),
    Dropbox(DropboxAccess),
    OneDrive,
}
```

- Coven validates the complete callback URL and its state before exchanging the
  code. Missing, duplicate or mismatched callback fields fail sign-in. The
  sign-in deadline is five minutes on a monotonic timer. Dropping the future
  releases the presenter.
- Tokens have no local expiry deadline. Setup, bootstrap, operations, file
  transfers and sync use a token until the provider rejects it with HTTP 401.
- A rejected request refreshes once, commits the replacement to custody,
  then retries once. A failed refresh or second rejection reaches the caller.
- OAuth error codes distinguish the required action in token responses and
  checked redirects: invalid grants and interaction or consent requirements
  are `Authentication`; invalid client, scope, resource or request configuration is
  `InvalidConfiguration`; `server_error` and `temporarily_unavailable` are
  retryable `Network` failures. Only an `access_denied` redirect is `Denied`;
  native cancellation stays `Cancelled`. Other redirect errors are `Refused`.
  Token failures retain the original `ProviderResponse`; redirect failures
  retain the original `OAuthRedirectError`, including denial. Neither prints
  provider-supplied details.
- Refresh and S3 key replacement leave restore codes untouched. Restore
  codes carry the storage location, never S3 keys or OAuth tokens.
- Google and Dropbox token custody retain the client id and granted access. Refresh uses that
  registered client, including the chosen Dropbox key, and preserves granted
  access only when the provider omits a scope field. A returned scope field is
  authoritative: a missing required scope is `AccessNotGranted`, never a
  successful upgrade.

Example, a Google store's first invitation. The configured Google client has
registered full access, but setup still signs in with `drive.file`:

```rust
handle.authenticate(OAuthAccess::GoogleDrive(GoogleDriveAccess::File)).await?;
handle.setup_oauth_storage(StorageConfig::GoogleDrive { folder_id }, "Ana's phone").await?;
// create_invite presents incremental full-Drive consent before sharing.
let invite = handle.create_invite(
    InviteTarget::NewMember { role: MemberRole::Member },
    InviteAccess::ProviderAccount { email: "carol@example.com".into() },
).await?;
```

Example, adding Ana's laptop. On her phone:

```rust
let code = handle.restore_code().await?;
show_blurred_qr_code(&code);
```

On the laptop:

```rust
let info = decode_code_info(&scanned)?;
let mut builder = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .oauth_clients(oauth_clients.clone())
    .clock(clock.clone())
    .id_source(ids.clone());
if let Some(access) = info.oauth_access {
    builder.authenticate(access).await?;
}
let s3_credentials = if info.cloud_provider == CloudProvider::S3 {
    Some(ask_for_s3_access_key().await?)
} else {
    None
};
let handle = restore_from_code(
    builder,
    &scanned,
    s3_credentials,
    "Ana’s laptop",
    |step| show_step(step),
    &cancel_rx,
)
.await?;
```

Example, Ana adding Carol. On Ana's phone:

```rust
let invite = handle
    .create_invite(
        InviteTarget::NewMember { role: MemberRole::Member },
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
let mut builder = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .oauth_clients(oauth_clients.clone())
    .oauth_presenter(platform_sign_in_sheet);
builder.authenticate(OAuthAccess::GoogleDrive(GoogleDriveAccess::Full)).await?;
match join_with_invite(
    builder,
    &scanned,
    "Carol's phone",
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
    /// The public pair bound to a stable member id by the store log.
    pub fn public_keys(&self) -> MemberPublicKeys;
    /// Encodes both private seeds for custody or a restore code (§12.1).
    pub fn to_secret_bytes(&self) -> SecretBytes;
    /// Restores both pairs from custody or a restore code.
    pub fn from_secret_bytes(bytes: &[u8]) -> Result<Self, MaterialError>;
}

impl MemberIdentity {
    pub fn member(&self) -> MemberId;
    /// Encodes the id and every held private pair, including pending and
    /// historical credentials. This custody encoding is not a restore code.
    pub fn to_secret_bytes(&self) -> SecretBytes;
    pub fn from_secret_bytes(bytes: &[u8]) -> Result<Self, MaterialError>;
}

impl StoreKeyring {
    /// Encodes every opened store and circle key for custody (§11).
    pub fn to_secret_bytes(&self) -> SecretBytes;
    /// Reads custody bytes, rejecting malformed, duplicate or empty keyrings.
    pub fn from_secret_bytes(bytes: &[u8]) -> Result<Self, MaterialError>;
}

impl CovenHandle {
    /// Generates an independent member UUID and two key pairs in identity custody,
    /// for the person creating a store. Fails if custody already holds keys.
    /// Joining and restoring put the keys there themselves.
    pub fn initialize_identity(&self) -> Result<MemberId, IdentityError>;

    /// Finishes work using store keys, removes them from custody, erases
    /// them from the held session and drops the provider connection.
    /// A failed custody removal retains the previous session and connection.
    pub async fn forget_store_keys(&self) -> Result<(), KeyError>;

    /// Keeps an app secret, such as an API token, in the same keychain and
    /// under the same access policy as coven's keys. Coven records the name in a
    /// separate keychain list before writing the value to its own entry, keeping
    /// the platform's per-secret size limit. Names are arbitrary strings, encoded
    /// into native account names without colliding with coven's entries.
    /// The list always includes every saved secret, even if the database is damaged.
    /// Failure or a crash between writes may leave an extra name; retrying is safe.
    pub fn set_host_secret(&self, name: &str, value: &str) -> Result<(), KeyError>;

    /// The secret, or `None` if it was never set.
    pub fn host_secret(&self, name: &str) -> Result<Option<String>, KeyError>;

    /// Deletes the secret before removing its name from the list; succeeds if absent.
    /// Failure or a crash between these steps may leave an extra name; retrying is safe.
    pub fn delete_host_secret(&self, name: &str) -> Result<(), KeyError>;
}
```

- A custody owner unlocks once at open and retains the capability needed to
  persist through that session. Custom custody obeys the same lifetime:
  `persist` must reuse its unlocked derivation, and `close` erases it.
  Ana adding a historical key saves the changed keyring without another
  passphrase derivation. An empty session can acquire keys during setup.

```rust
/// The app's own store for the store keys and circle keys this device holds.
pub trait StoreKeyCustody: Send + Sync {
    /// Called once at open; None when this device has never held keys.
    /// Retains the unlocked persistence capability until close.
    fn unlock(&self) -> Result<Option<StoreKeyring>, KeyError>;
    /// Keeps `keyring`, replacing what was kept.
    fn persist(&self, keyring: &StoreKeyring) -> Result<(), KeyError>;
    fn forget(&self) -> Result<(), KeyError>;
    /// Erases held memory after users finish; performs no persistence or IO.
    /// Fallible saves complete through persist before dependent work starts.
    fn close(&self);
}

/// The app's own store for the stable id and held member credentials.
pub trait MemberKeyCustody: Send + Sync {
    /// Called once at open; persistence reuses the held session capability.
    fn unlock(&self) -> Result<Option<MemberIdentity>, KeyError>;
    fn persist(&self, identity: &MemberIdentity) -> Result<(), KeyError>;
    fn forget(&self) -> Result<(), KeyError>;
    /// Erases held memory after users finish; performs no persistence or IO.
    /// Fallible saves complete through persist before dependent work starts.
    fn close(&self);
}
```

### E12 Circles

- A circle's members add and remove its members ([§14.3](coven.md#143-circles)).
  Circle changes first catch up on reachable storage (§9, E6).
- Circle-changing calls return `SyncError` ([E5](#e5-storage-and-sync)), as member calls
  do ([E9](#e9-members-and-devices)). Pre-validation uses replay's rules:
  `Rejected(NotAllowed)` for authority, `Rejected(NoAdminLeft)` for the
  last-admin rule, and `Rejected(TargetGone(target))` for an absent member,
  device or circle. The target retains its typed id. `StoreOwner` remains
  a separate provider constraint.
- Removing someone from a circle publishes one entry and returns once it is
  kept. Rotation follows in the sync pass; it is not part of that call (§11).

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

    /// Deletes a circle with one store-log entry. The deletion rule hides
    /// its rows while the entry is kept; there is no row-delete write (§14.7).
    pub async fn delete(&self, circle: CircleId) -> Result<(), SyncError>;

    /// Adds a store member to the circle. They get its current and earlier
    /// keys, so they read its history. Rejoining devices reload skipped parts,
    /// including when replay returns a deleted circle (§14.4).
    pub async fn add_member(&self, circle: CircleId, member: &MemberId) -> Result<(), SyncError>;

    /// Removes someone with one entry; remaining members rotate exposed keys (§14.6).
    pub async fn remove_member(&self, circle: CircleId, member: &MemberId) -> Result<(), SyncError>;

    /// The circles this member is in.
    pub async fn list(&self) -> CovenResult<Vec<Circle>>;

    /// The same circle-list query, live (E4).
    pub fn subscribe_list(&self) -> LiveQuery<Vec<Circle>>;

    /// A circle's members who are still in the store.
    pub async fn members(&self, circle: CircleId) -> CovenResult<Vec<CircleMemberInfo>>;

    /// The same query for this circle's members, live (E4).
    pub fn subscribe_members(&self, circle: CircleId) -> LiveQuery<Vec<CircleMemberInfo>>;

    /// Resets a circle from this copy; only the snapshot and writes that
    /// read the reset count in that circle, on this device too (§19.3).
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

- Coven migrates its own local tables automatically before the app's
  migrations; each internal migration is atomic ([§17.2](coven.md#172-covens-schema)).
- The app's schema is a list of migrations numbered from 1 with no gaps.
- Opening runs every migration above the database's version in one
  transaction, so a failure leaves the schema as it was.
- A migration has two parts (§17.1): the first changes the database; the
  optional second converts eligible row changes of received late ordinary
  writes using the winning raise's conversion, in version and causal order
  ([`receive_matches_evaluation`](proofs/queue/CovenQueue/Conversion.lean)).
  - Local migration preserves every queued original record, including
    untried writes; tried uploads also retain their format and sealing keys
    ([`conversions_preserve_uploads`, `conversions_preserve_attempt`](proofs/queue/CovenQueue/Conversion.lean)).
  - Key-replacement window drops use the same data rule (§10).
  - Before invoking a callback, coven determines candidate schema admission
    and resolves each change's original generation source and replaced
    setters under §8. A dependent change never reaches the callback: retain
    its original write/row identity, generation, operation, names, scalar
    values and failed source identities as a frozen loss. A delete keeps
    its identity even without scalars. Recorded reads only govern readiness
    ([`dependent_change_not_converted`, `converted_change_matches_receiver`](proofs/queue/CovenQueue/Conversion.lean)).
  - One received interpretation, including one audience part, can contain
    converted changes and frozen originals. Conversion is independent of
    attempt status and preserves device, number, timestamp, causal positions,
    row identity, generation, operation and original source identities.
    Renamed tables and columns retain their sources under the new names;
    frozen changes keep their old names and original cause
    ([`conversion_preserves_identity`, `convertChange_metadata`](proofs/queue/CovenQueue/Conversion.lean);
    reference and rename validation remain implementation obligations).
  - With no converter, keep each original change as a schema loss naming
    the breaking version and pass the position. No callback changes any
    upload record. Already frozen values and causes survive later raises
    ([`missing_converter_preserves_values`, `conversions_preserve_frozen`, `lost_never_reconverted`](proofs/queue/CovenQueue/Conversion.lean)).
  - The author reloading after a competing raise uses the same per-change
    rule as peers. New writes use the atomically reloaded state; an implicit
    own read of an excluded position creates no data dependence. Source
    evidence and verdicts survive snapshots and loss dismissal (§8).
  - E.g. Ben's attempted 9 arrives after Ana's raise: its eligible changes
    convert on receipt. If 9's note change was already excluded, 10's edit
    of it stays frozen while its unrelated playlist rename can convert;
    later changes keep that same distinction (§17.1;
    [`mixed_write`, `partial_write_chain`](proofs/queue/CovenQueue/DataExamples.lean)).
  - A failed callback aborts the receive or reload transaction, preserving
    the preceding committed values, losses, source verdicts and positions.
    The upload queue remains unchanged on success and failure alike.
- Coven decides whether a migration is an addition or a breaking change by
  comparing the schema before and after it. Statements that change rows of
  synced tables also make it breaking (§17.1).
- A converted change keeps the references its columns carried, renamed
  with them. Changing a reference's value, or adding a column that is a
  reference, fails conversion and leaves application pending without a
  partial commit: the device can't know which generation of the parent the
  old write meant.

```rust
impl Migration {
    /// A migration whose first part is SQL.
    pub fn sql(version: u32, name: &'static str, sql: &'static str) -> Self;

    /// A migration whose first part is code, for rebuilds and backfills SQL
    /// alone can't express.
    pub fn run<F>(version: u32, name: &'static str, f: F) -> Self
    where
        F: Fn(&MigrationContext<'_>) -> Result<(), DbError> + Send + Sync + 'static;

    /// Its second part: interprets eligible received old-schema row changes
    /// in the new schema, independently of upload attempt status (§17.1).
    /// Dependent changes never reach this callback; they remain frozen
    /// originals alongside the write's converted changes. Without a converter,
    /// retain each original change as a schema loss. It never rewrites the
    /// original upload record. An error aborts the receive or reload transaction.
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
    /* private: original write/row/generation and authenticated data sources */
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
    /* private: written references and original setter-source identity */
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
