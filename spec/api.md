# API

- The API is what apps call, listed as Rust declarations with their doc
  comments. What one coven crate offers another is documented in its code,
  not here.
- The declarations include every public root item, inherent method, field and enum variant. Private
  fields are marked `/* private */`; `cfg` attributes show platform and feature availability. Types
  from dependencies keep their qualified names. Standard trait implementations retain their Rust
  meanings; conversions and operational traits are shown where callers use them. Feature gates
  belong to the defining crate; dependency-only gates below name that crate.
- Calls on `handle` are on the `CovenHandle` that opening a store returns.
- App-facing calls reaching storage or the database are `async`;
  transaction callbacks (`SqlContext`, `MigrationContext`) are synchronous.
- A row is named by its table and its *key*: the values of its primary key
  columns, in order ([§9.2](coven.md#92-keys-and-unique-values)).

Coven re-exports the rusqlite types used in its application signatures, so the app
can name SQL callback results and errors without declaring a second dependency.
`coven::SqliteError` and `coven::SqliteResult` name rusqlite's `Error` and `Result`.
The root also exports `Params`, `Row`, `ToSql`, `types`, `params!` and
`named_params!`. Raw connections and other rusqlite helpers are outside these
re-exports.

```rust
// External types used by the declarations below.
use std::{collections::HashMap, future::Future, num::{NonZeroU64, NonZeroUsize},
          path::{Path, PathBuf}, pin::Pin, sync::Arc, time::{Duration, SystemTime}};
use async_trait::async_trait;
// These SQL items are also exported from coven at its root.
pub use coven_database::{
    named_params, params, types, Params, Row, SqliteError, SqliteResult, ToSql,
};
use tokio::{io::AsyncRead, sync::watch};
use url::Url;
use uuid::Uuid;

/// Primary-key values in declaration order (§9.2).
pub struct RowKey(/* private */);

impl RowKey {
    /// The SQL values identifying the row.
    pub fn values(&self) -> &[rusqlite::types::Value];
}

impl From<&str> for RowKey { /* one text key */ }
impl From<UuidText> for RowKey { /* one UUID text key */ }
impl<A: Into<rusqlite::types::Value>, B: Into<rusqlite::types::Value>> From<(A, B)> for RowKey { /* a two-column key */ }
impl From<Vec<rusqlite::types::Value>> for RowKey { /* any key width */ }

/// One install of the app, by its 64-bit device id (§10). ToSql/FromSql
/// bind/read its eight big-endian bytes, as `coven_devices` holds it (E4).
pub struct DeviceId(pub u64);

/// A member's random stable UUID, independent of their keys (§10).
/// ToSql/FromSql bind/read canonical text, as `coven_members` holds it (E4).
pub struct MemberId(pub Uuid);

/// A validated Ed25519 public key; invalid and weak points are refused.
pub struct SigningKey(/* private */);

impl SigningKey {
    /// Validate a public signing key, refusing invalid and weak Ed25519 points.
    pub fn from_bytes(bytes: [u8; 32]) -> Result<Self, CryptoError>;

    /// The validated Ed25519 public key's 32 bytes.
    pub fn as_bytes(&self) -> &[u8; 32];

    /// Verify a detached signature against this public key (§12).
    pub fn verify(&self, message: &[u8], signature: &coven_crypto::Signature) -> Result<(), CryptoError>;

    /// Verify an object's signature after hashing all bytes before it (Appendix D.3).
    /// The domain and path are authenticated along with the SHA-256 digest.
    /// Panics if the storage path is empty.
    pub fn verify_object(
        &self,
        path: &str,
        digest: &coven_crypto::ObjectDigest,
        signature: &coven_crypto::Signature,
    ) -> Result<(), CryptoError>;

    /// Verify a snapshot prefix independently of its encrypted data (D9).
    /// `prefix` is exactly kind, version and routing fields, without signatures.
    /// Panics if the storage path is empty.
    pub fn verify_prefix(
        &self,
        path: &str,
        prefix: &[u8],
        signature: &coven_crypto::Signature,
    ) -> Result<(), CryptoError>;

    /// Verify a write's header signature without its part chunks (D9): the
    /// exact cleartext prefix, the complete sealed header chunk and the part
    /// roots. Snapshots carry these to authenticate another member's leaves.
    /// Panics if the storage path is empty.
    pub fn verify_write_header(
        &self,
        path: &str,
        prefix: &[u8],
        header: &[u8],
        roots: &[[u8; 32]],
        signature: &coven_crypto::Signature,
    ) -> Result<(), CryptoError>;
}

/// An X25519 public key (Appendix D).
pub struct SealingKey(/* private */);

impl SealingKey {
    /// Read a public sealing key. Low-order keys are refused when sealing.
    pub fn from_bytes(bytes: [u8; 32]) -> Self;

    /// The public bytes carried with the member's join request (§13.2).
    pub fn as_bytes(&self) -> &[u8; 32];
}

/// The public pair from D2, re-exported without duplicating its type:
/// `signing: SigningKey`, `sealing: SealingKey`.
pub use coven_crypto::MemberPublicKeys;

/// One write: its device and local write number, independent of its slot (§4).
/// Claim numbers separately count every slot kind and remain internal to sync.
pub struct WriteId {
    pub device: DeviceId,
    pub number: u64,
}

/// A positive slot number in the store's one sequence (§5).
pub struct Slot(pub u64);

/// Row audience; ToSql/FromSql bind/read canonical text (§15).
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
  the settings, so the app never chooses a device id ([§10](coven.md#10-members-roles-and-devices)).
- A `create_store` error carries no open handle. If the store becomes visible
  but syncing its directory fails, coven closes it and waits for release before
  returning `StoreCreationError::PublishedButNotSynced { store, source }`.
  The complete store stays on disk; the app can call `open(store)` with its
  usual custody choices. Keeping the error cannot keep the store locked.
- Provider access is chosen when storage is first attached (E5), not when the
  device-local directory is created. Dropbox's required `DropboxLocation`
  selects not-shared (`AppFolder`) or shared-capable (`SharedFolder`); it has
  no default. With one registered Dropbox key only that key's location kind is
  available; with both the app supplies the user's or its own explicit choice.
  The choice survives reopening and restore and cannot be changed by
  reconnecting.
- A `StoreLayout` says where an app's stores live on disk.
- `Coven::storage_config` reads a closed store's saved location without opening
  its database or custody. The handle's `storage_config` reads the same committed
  `StorageConfig`, including the member's account label, so an app can show
  "Dropbox: ana@example.com" without keeping a second copy. Neither returns
  credentials or contacts storage. Before setup they return
  `None`; disconnect retains the location, and unreadable settings remain an error.
- Store lock files live beside each store directory: the writer, reader and
  bootstrap locks. Deletion holds all three while removing the directory, then
  releases and removes every lock file, so a deleted store leaves nothing named
  for it. Holding the bootstrap lock keeps out a running restore, join or
  replacement: after its swap, a running `reset_device` or `restore_device`
  holds only that lock between steps, and deletion must not remove files under
  it. Lock acquisition, creation and lock-file removal are serialized by the
  layout, so no one can lock a lock file between its release and its removal.
- Deleting a store with storage set up loses whatever hasn't been uploaded.
  So before Ana deletes the Household store from her phone, the app shows
  her `unsent_work()`: say, 3 writes and 1 file still waiting. These are the
  same counts `delete_store(IfUnsent::Refuse)` refuses with, read the same
  way, so the warning and the refusal can't disagree, and the app needn't
  attempt a deletion to learn them.
- Directory-entry existence checks do not follow symlinks. Only not-found
  means absent; other failures retain their operation, path and OS cause.
- Opening a store needs its declared tables ([E2](#e2-declaring-synced-tables))
  and its migrations ([E13](#e13-migrations)).
- Writable opens always migrate coven's own local tables in place ([Appendix
  C.1](coven.md#c1-covens-tables-in-the-apps-database)). Read-only opens refuse tables that need
  migrating; any open refuses an internal schema newer than it supports.
- *SQLite settings* tune the store's connections ([§4](coven.md#4-writing-locally)).
  `CovenBuilder::sqlite_settings` takes a `SqliteSettings`, whose fields are
  typed values, never PRAGMA text; a field left `None` keeps SQLite's default,
  except `checkpoint_fullfsync`, which is on for macOS and iOS
  ([§3](coven.md#3-what-coven-assumes)). Coven applies them to the writer and
  readers of a writable handle, and to every connection of a read-only one, as
  each opens and before any statement of its own. Connections coven opens to
  its own files, such as `inputs.db` beside the database, keep SQLite's
  defaults, with the same `checkpoint_fullfsync` on macOS and iOS.
- `sqlite_settings()` on either handle reads back the settings SQLite has in
  effect, which every connection shares. SQLite lowers `mmap_size` and
  `threads` to the limits it was built with, and the readback shows the lowered
  value. A value larger than SQLite can hold, such as a busy timeout past
  `i32::MAX` milliseconds, fails the open with
  `DbError::SqliteSettingOutOfRange`.
- The page size and auto-vacuum mode apply only when the store's database is
  created, by whichever call creates it: creating, joining, restoring or
  resetting. A store keeps the ones it was created with; reopening it with
  others changes nothing, and the readback shows the store's own.
- `checkpoint(mode)` checkpoints the store's WAL on the writer, between writes,
  in one of SQLite's modes: `Passive`, `Full`, `Restart` or `Truncate`. It
  returns what SQLite reports: whether a reader or another writer kept it
  from finishing within the busy timeout, the frames in the WAL, and the
  frames copied into the database. After a `Restart` that wasn't busy, the
  next write starts the WAL file over from its start, so the file stops
  growing. A store created with `AutoVacuum::Incremental` gives back free
  pages only through `incremental_vacuum`.

Every SQLite setting, and who chooses it. Settings marked *app* are fields of
`SqliteSettings`; the rest have no field.

| Setting | Chosen by | Why, and when it applies |
|---|---|---|
| `busy_timeout` | app | how long a connection waits for a lock; unset keeps the 5 seconds rusqlite sets at open; RESTART and TRUNCATE checkpoints wait this long for readers |
| `cache_size`, `cache_spill` | app | memory each connection uses, and when a transaction writes changed pages early |
| `mmap_size` | app | reads through memory-mapped I/O; SQLite caps it |
| `temp_store` | app | where temporary tables live; set before coven creates its temporary triggers, since changing it drops temporary objects; on Android, coven's SQLite is built to keep them in memory |
| `threads` | app | helper threads for sorting; SQLite caps it |
| `journal_size_limit`, `wal_autocheckpoint` | app | how large the WAL file stays once a checkpoint lets it start over, and when commits checkpoint it; with automatic checkpoints off, the WAL grows until the app checkpoints |
| `automatic_index`, `analysis_limit` | app | query planning; `analysis_limit` bounds every `ANALYZE`, and coven's own reads at most 2,000 rows of each index whatever it is |
| `cell_size_check` | app | extra damage checks as pages are read |
| `secure_delete` | app | overwrites deleted content |
| `fullfsync` | app | on macOS and iOS, a commit also flushes the drive's own cache; unset keeps SQLite's default, off, so a power loss can take the latest commits |
| `checkpoint_fullfsync` | app | on macOS and iOS, a checkpoint also flushes the drive's own cache; unset is on there, so a power loss can't damage the database file once the WAL starts over |
| `page_size` | app, at creation | fixed once the file has its first page, and WAL mode never changes it |
| `auto_vacuum` | app, at creation | turning it on or off later needs a `VACUUM`, which coven never runs |
| `journal_mode` | coven: WAL | readers keep their snapshots while a write commits (§4) |
| `synchronous` | coven: FULL | a committed write survives a power loss (§4) |
| `locking_mode` | coven: NORMAL | an exclusive lock would shut out the readers and other processes |
| `foreign_keys` | coven: ON | the rules of §9.1 |
| `recursive_triggers` | coven: ON | every device runs the same triggers for the same SQL (§9.4) |
| `trusted_schema` | coven: OFF | triggers and views call only functions marked safe for them |
| `application_id`, `user_version` | coven | the version of coven's tables, and of the app's migrations (E13) |
| `schema_version`, `writable_schema` | coven: untouched | writing them would corrupt the schema or get around coven's checks of it |
| `query_only` | coven: off | the writer commits coven's own changes; readers open read-only |
| `defer_foreign_keys` | coven: off | it lasts one transaction, and every transaction is coven's |
| `ignore_check_constraints` | coven: off | a row failing a CHECK after a merge is taken out on every device (§9.3) |
| `legacy_alter_table` | coven: off | it changes what a rename does to triggers and views, so one migration would leave different schemas on different devices |
| `case_sensitive_like` | coven: off | it changes what `LIKE` returns, so the same trigger or CHECK could decide differently on two devices |
| `encoding` | coven: UTF-8 | key comparison compares stored bytes, which differ between encodings (A.10) |
| `max_page_count` | coven: unlimited | other members' writes would fill it, and this device could no longer apply slots |
| `lock_proxy_file` | coven: none | it changes how other processes lock the file |
| `reverse_unordered_selects` | coven: off | a testing aid that reorders results without `ORDER BY` |
| `read_uncommitted` | no effect | it affects only shared-cache connections, which coven never opens |
| `soft_heap_limit`, `hard_heap_limit`, `temp_store_directory`, `data_store_directory` | the process | they set every connection in the process, not one store's |
| `count_changes`, `default_cache_size`, `empty_result_callbacks`, `full_column_names`, `short_column_names` | nobody | deprecated; they do nothing |
| `parser_trace`, `sql_trace`, `vdbe_*`, `activate_extensions` | nobody | they exist only in SQLite's debugging or encryption builds |
| `wal_checkpoint`, `incremental_vacuum` | app, as calls | `checkpoint` and `incremental_vacuum` on the handle |
| `integrity_check`, `quick_check` | coven, as calls | opening checks integrity (§19.3) |
| `optimize` | coven, as calls | the writer gathers the planner's statistics at a writable open, after migrating and before the readers open, and at close (§4); app writes can't run `ANALYZE` |
| `foreign_key_check`, `shrink_memory` | nobody | calls coven doesn't offer |
| `collation_list`, `compile_options`, `database_list`, `data_version`, `foreign_key_list`, `freelist_count`, `function_list`, `index_info`, `index_list`, `index_xinfo`, `lock_status`, `module_list`, `page_count`, `pragma_list`, `stats`, `table_info`, `table_list`, `table_xinfo` | nobody | reports, not settings |
| `sqlite3_limit` limits | coven: SQLite's | a lower limit on one device would refuse values, rows and statements other devices and coven itself write |
| `sqlite3_db_config` options | coven | double-quoted string literals off; the rest at SQLite's defaults, not offered |
- *Collations* are the app's own orders for text, which its SQL names with
  `COLLATE` ([§4](coven.md#4-writing-locally)). `CovenBuilder::collation(name,
  compare)` adds one; `compare` takes two strings and returns their
  `Ordering`. Coven registers every collation on each connection it opens to the
  store's database, before any app SQL runs there: the writer, the readers, every
  connection of a read-only handle, and the connection a reset uses to check the
  database it replaces. So every builder that opens the store, a widget's
  read-only one included, registers the same collations.
- An order is part of the store's schema. `compare` must be deterministic, the
  same on every device and in every app version that opens the store, and a
  total order. A changed order leaves the store's indexes out of order, and the
  next open fails its integrity check with `DbError::DamagedDatabase`. To change
  one, the app registers a new name, migrates to it, and keeps registering the
  old one.
- SQLite compares collation names without regard to case. A name that is one of
  SQLite's own, `BINARY`, `NOCASE` or `RTRIM`, fails the open with
  `DbError::ReservedCollation`, and a name given twice with
  `DbError::DuplicateCollation`. SQLite refuses any statement that needs a
  collation the app didn't register, so a store with an index in one fails to
  open. A panic in `compare` ends the process: SQLite has no way for a
  comparison to fail.
- *Functions* are the app's own scalar functions, which its SQL calls by name
  ([§4](coven.md#4-writing-locally)). `CovenBuilder::function(name, arguments,
  determinism, compute)` adds one taking exactly `arguments` arguments;
  `compute` reads them from its `FunctionArguments` and returns a value that
  converts into `types::Value`, or an error that fails the statement calling
  it. `FunctionArguments` holds only the arguments: the connection running the
  statement stays coven's. Coven registers every function on the same connections as the
  collations. Every app function is marked safe for triggers and views.
- `Determinism::Deterministic` lets SQLite use the function in an index, a
  partial index's condition, a CHECK and a generated column, and to call it
  once for repeated arguments; `Determinism::Nondeterministic` refuses it
  there. A deterministic function the schema uses is part of the store's
  schema, like an order: the same arguments give the same result on every device
  and in every app version. A changed result leaves an index on it wrong, and
  the next open fails with `DbError::DamagedDatabase`.
- SQLite compares function names without regard to case. A name SQLite or coven
  already has, or one starting with `coven_` or `_coven_`, fails the open with
  `DbError::ReservedFunction`, and a name given twice with
  `DbError::DuplicateFunction`. A deterministic function takes at least one
  argument; one that takes none is a constant, and fails the open with
  `DbError::ConstantFunction`. `regexp` is the app's to define: SQLite has
  none, and `x REGEXP y` calls `regexp(y, x)`. SQLite refuses any statement
  that calls a function the app didn't register, so a store with an index on
  one fails to open. A panic in `compute` ends the process.
- *Key custody* is where this device keeps the store keys and circle keys it has opened
  ([§12.2](coven.md#122-exposure-and-which-key-new-data-uses)): every key it has used, so it reads
  writes made under older ones.
- *Identity custody* keeps the stable member id and held private pairs,
  including historical credentials and a pending replacement candidate
  ([Appendix D](coven.md#appendix-d-cryptography-and-keys-reference)).
- CloudKit is a built-in provider on Apple platforms (§20.2). The app supplies
  the container identifier and its iCloud entitlement; coven-storage calls
  Apple's CloudKit framework through `objc2-cloud-kit`. Native failures retain
  their cause in `StorageError::Provider`. On other platforms, setup returns
  `StorageFailure::ProviderUnavailable`.
- Dropbox, OneDrive and S3 are reached over HTTPS through clients built one
  way, which check each server's certificate with the platform's own verifier
  (`rustls-platform-verifier`). So coven trusts what the device trusts, the
  same for every provider ([Appendix F.1](coven.md#f1-crates)). A verifier that
  cannot start is `StorageFailure::InvalidConfiguration`.
- On Android the verifier asks the JVM, so the app initializes it before any
  storage call: it passes its `Context` to
  `rustls_platform_verifier::android::init_with_env` and packages the
  verifier's Android classes. Coven cannot tell beforehand that this was
  skipped; the first HTTPS connection then panics inside the verifier.

```rust
/// A store's UUID, independent of its name and location (E1).
pub struct StoreId(pub Uuid);

/// Circle UUID; ToSql/FromSql bind/read canonical text (§15.5).
pub struct CircleId(pub Uuid);

/// ToSql/FromSql for canonical UUID text; bare rusqlite UUIDs bind as BLOBs.
pub struct UuidText(pub Uuid);

/// The wall clock used for timestamps (§7.2).
pub trait Clock: Send + Sync {
    /// The wall clock's current time.
    fn now(&self) -> SystemTime;
    /// Monotonic elapsed time, including system sleep; readings never decrease.
    /// Only differences between readings of this clock are meaningful.
    fn elapsed(&self) -> Duration;
    /// Controlled clocks override this to wait for their supplied time.
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

/// A shared clock supplied when opening a store (Appendix F.2).
pub type ClockRef = Arc<dyn Clock>;

/// The system wall clock used unless the app supplies another (E1).
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime;
    fn elapsed(&self) -> Duration;
}

/// An installation with an isolated in-memory keychain for application tests.
#[cfg(feature = "test-utils")]
pub struct TestCoven { /* private */ }

#[cfg(feature = "test-utils")]
impl TestCoven {
    /// Make an installation with an empty device-only and synced keychain.
    pub fn new() -> Self;

    /// Another device of the same person: its own empty device-only keychain,
    /// sharing this one's synced entries as iCloud Keychain would (§13.1).
    pub fn another_device(&self) -> Self;

    /// Create through the production builder, then close for a reopening fixture.
    pub async fn create_closed_store(
        &self,
        layout: &StoreLayout,
        name: &str,
        ids: IdSourceRef,
    ) -> Result<StoreDir, StoreCreationError>;

    /// Build the production graph with this installation's keychain.
    pub fn builder(&self, layout: StoreLayout) -> CovenBuilder;

    /// Discard through the same locked directory and keychain path as `Coven`.
    pub async fn discard_store(&self, directory: &StoreDir) -> Result<(), StoreDeletionError>;

    /// List this installation's saved codes through the production discovery path (E10).
    pub async fn saved_restore_codes(&self) -> Result<Vec<SavedRestoreEntry>, BootstrapError>;

    /// Forget one store's saved code and S3 key through the production path (E10).
    pub async fn forget_saved_restore_code(&self, store: StoreId) -> Result<(), BootstrapError>;

    /// Seed the simulated OS custody as if this installation had opened keys.
    pub fn keep_store_keys(
        &self,
        directory: &StoreDir,
        keys: &StoreKeyring,
    ) -> Result<(), KeyError>;

    /// Refuse the next keychain operation before changing any stored value.
    pub fn fail_next_keychain_operation(&self);
}

/// Application storage fixtures, available with coven's test-utils feature.
/// MemoryStorage implements StorageConnector: pass Arc<MemoryStorage> to the builder.
/// Its builder configures the provider location, clock, transfers and listing pages.
/// Clones retain objects; for_device gives another connection to those objects.
/// Faults and BodyFailure inject request and response-body failures, and
/// MemoryRequest records the attempted calls. Storage supplies reads and listings.
#[cfg(feature = "test-utils")]
pub use coven_storage::test_utils::{
    BodyFailure, Faults, MemoryProvider, MemoryRequest, MemoryStorage,
    MemoryStorageBuilder,
};
#[cfg(feature = "test-utils")]
pub use coven_storage::Storage;

/// A controlled clock; forward settings advance elapsed time and wake sleepers.
#[cfg(feature = "test-utils")]
pub struct FixedClock(/* private */);

#[cfg(feature = "test-utils")]
impl FixedClock {
    /// A clock initially set to `now`.
    pub fn new(now: SystemTime) -> Self;

    /// Set the time every subsequent read returns, including a backward jump.
    pub fn set(&self, now: SystemTime);
}

/// Deterministic test ids, starting at one.
#[cfg(feature = "test-utils")]
pub struct SequentialIds(/* private */);

#[cfg(feature = "test-utils")]
impl SequentialIds {
    /// A sequence whose first UUID's payload and first device id are one.
    pub fn new() -> Self;
}

#[cfg(feature = "test-utils")]
impl Clock for FixedClock {
    fn now(&self) -> SystemTime;
    fn elapsed(&self) -> Duration;
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

#[cfg(feature = "test-utils")]
impl IdSource for SequentialIds {
    fn new_id(&self) -> Uuid;
}

#[cfg(feature = "test-utils")]
/// Constructs provider connections without committing settings or credentials.
/// Tests replace the connection boundary with in-memory storage.
#[async_trait]
pub trait StorageConnector: Send + Sync {
    /// Read the signed-in account's display label before recording a member's location.
    async fn account_label(
        &self,
        _auth: ProviderAuth,
        _cooldown: Arc<dyn Cooldown>,
    ) -> Result<String, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }

    /// Read the signed-in OneDrive root.
    async fn onedrive_root(
        &self,
        _auth: ProviderAuth,
        _cooldown: Arc<dyn Cooldown>,
    ) -> Result<StorageFolder, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }

    /// Read all immediate OneDrive child folders.
    async fn onedrive_folders(
        &self,
        _parent: &StorageConfig,
        _auth: ProviderAuth,
        _cooldown: Arc<dyn Cooldown>,
    ) -> Result<Vec<StorageFolder>, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }

    /// Create a OneDrive folder without renaming or replacing an occupied name.
    async fn create_onedrive_folder(
        &self,
        _parent: &StorageConfig,
        _name: &str,
        _auth: ProviderAuth,
        _cooldown: Arc<dyn Cooldown>,
    ) -> Result<StorageFolder, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }

    /// Read all shared Dropbox namespaces.
    async fn dropbox_shared_folders(
        &self,
        _auth: ProviderAuth,
        _cooldown: Arc<dyn Cooldown>,
    ) -> Result<Vec<StorageFolder>, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }

    /// Create the owner's CloudKit location from the entitled container.
    /// A connector without native CloudKit support refuses this capability.
    async fn prepare_cloudkit(
        &self,
        _container: &str,
        _store: StoreId,
    ) -> Result<StorageConfig, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }
    /// Validate settings and wrap the selected native provider once. The caller
    /// verifies the connection before publication. Every request waits for
    /// the store's shared `cooldown`; a rejected OAuth token is refreshed in
    /// the sign-in's custody (§20.2).
    async fn connect(
        &self,
        config: StorageConfig,
        auth: coven_storage::ProviderAuth,
        publications: Arc<dyn coven_storage::PublicationGate>,
        cooldown: Arc<dyn coven_storage::Cooldown>,
        transfers: Arc<coven_storage::TransferBudget>,
    ) -> Result<Arc<coven_storage::StorageConnection>, StorageError>;
    /// Probe an S3 location's service (§20.7), writing only fresh objects under
    /// the store's `.coven-check` prefix and deleting them, also on failure.
    /// A connector without native S3 refuses this capability.
    async fn detect_s3(
        &self,
        _config: StorageConfig,
        _credentials: S3Credentials,
        _store: StoreId,
    ) -> Result<S3Detection, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }
    /// Before a store's first setup at an S3 location whose member keys reach
    /// the whole bucket, refuse a bucket holding anything outside the store's
    /// folder or the `coven-bucket` marker (§20.7). Reads only.
    /// A connector without native S3 refuses this capability.
    async fn check_new_s3_store(
        &self,
        _config: StorageConfig,
        _credentials: S3Credentials,
        _store: StoreId,
    ) -> Result<(), StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }
}

/// Test-injected ids; production uses OS randomness (Appendix F.2).
pub trait IdSource: Send + Sync {
    /// A fresh UUID; distinct calls must yield distinct ids.
    fn new_id(&self) -> Uuid;
    /// The default implementation derives a fresh 64-bit device id from this source (§10.4).
    fn new_device_id(&self) -> DeviceId;
}

/// A shared source of ids (Appendix F.2).
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
    /// The current name from this device's committed database state. While
    /// a replacement is unfinished, the name its new installation loaded.
    pub name: String,
    /// The call that must finish a replacement of this device's installation
    /// before the store opens (§19.3); `None` when no replacement is unfinished.
    pub unfinished_replacement: Option<ReplacedBy>,
}

/// The call that began a replacement, and the only call that finishes it (§19.3).
pub enum ReplacedBy {
    /// `CovenBuilder::reset_device`, which keeps the old copy for inspection.
    Reset,
    /// `CovenBuilder::restore_device`, which commits the restore code's member
    /// keys and removes the old copy (§13.1).
    Restore,
}

/// The opening choices for stores under one layout (E1).
pub struct CovenBuilder { /* private fields */ }

/// A shared handle to the open store and its running work (Appendix F.4).
pub struct CovenHandle { /* private fields */ }

/// A handle that only reads an already open store (§4, E1).
pub struct CovenReadHandle { /* private fields */ }

/// A table's name, key, audience, file and shared-trigger declarations (E2).
pub struct SyncedTable { /* private fields */ }

/// A numbered database migration and its optional received-write conversion (§18.3).
pub struct Migration { /* private fields */ }

/// A passphrase owned by custody and erased when dropped (E1).
pub struct Passphrase(/* private */);

impl Passphrase {
    /// Takes ownership of the passphrase without exposing it again.
    pub fn new(secret: String) -> Self;
}

/// The member's Ed25519 and X25519 pairs, erased when dropped (Appendix D).
pub struct MemberKeys { /* private fields */ }

/// Stable member id and held private pairs, including historical credentials
/// and a durable replacement candidate. Applied slots select current authority (§12.3).
/// Private material is erased when dropped.
pub struct MemberIdentity { /* private fields */ }

/// A store's identified 32-byte key (§12), erased on drop.
/// Cloning supplies an independent unlocked snapshot to in-memory custody.
pub struct StoreKey { /* private */ }

impl StoreKey {
    /// Make random key material for an identity supplied by the id source.
    pub fn generate(id: KeyId) -> Result<Self, CryptoError>;

    /// Import a store key opened from custody or a member's sealed copy (§12.1).
    pub fn from_bytes(id: KeyId, bytes: [u8; 32]) -> Self;

    /// The random identity named by the entry introducing this key (§12.1).
    pub fn id(&self) -> KeyId;

    /// SHA-256 of the exact key bytes, committed inside its introducing entry (§12.1).
    pub fn hash(&self) -> [u8; 32];

    /// Check the hash committed by an authorized introduction before accepting this key.
    pub fn check_hash(&self, expected: &[u8; 32]) -> Result<(), MaterialError>;

    /// Derive separate encryption and authorship keys.
    pub fn derive(&self) -> coven_crypto::DerivedKeys;
}

/// One circle's identified 32-byte key, introduced by creation or rotation (§12.1).
/// Cloning supplies an independent unlocked snapshot to in-memory custody.
pub struct CircleKey { /* private */ }

impl CircleKey {
    /// Make a circle key using the operating system's randomness.
    pub fn generate(circle: CircleId, id: KeyId) -> Result<Self, CryptoError>;

    /// Import a circle key opened from custody or a member's sealed copy.
    pub fn from_bytes(circle: CircleId, id: KeyId, bytes: [u8; 32]) -> Self;

    /// The circle whose rows this key protects; file keys travel inside those rows.
    pub fn circle(&self) -> CircleId;

    /// The random identity named by the entry introducing this key.
    pub fn id(&self) -> KeyId;

    /// SHA-256 of the exact key bytes, committed inside its introducing entry (§12.1).
    pub fn hash(&self) -> [u8; 32];

    /// Check the hash committed by an authorized introduction before accepting this key.
    pub fn check_hash(&self, expected: &[u8; 32]) -> Result<(), MaterialError>;

    /// Derive separate encryption and authorship keys.
    pub fn derive(&self) -> coven_crypto::DerivedKeys;
}

/// Every opened store and circle key, including older keys (§12.2, §15.5).
pub struct StoreKeyring { /* private fields */ }

/// The app's provider client ids, kept private (E10).
pub struct OAuthClients { /* private fields */ }

/// A validated encrypted-object path, beginning with its store id (§20.3).
pub struct ObjectPath { /* private fields */ }

impl ObjectPath {
    /// Parses a listed or recorded path, refusing paths outside the store's layout.
    pub fn parse(value: &str) -> Result<Self, PathError>;
    /// The path bound into the object's encryption.
    pub fn as_str(&self) -> &str;
    /// Whether this is the floor path, which permits conditional replacement.
    pub fn is_replaceable(&self) -> bool;

    /// One create-once slot in the store's shared sequence (§5).
    pub fn slot(store: StoreId, number: NonZeroU64) -> Self;

    /// The positive number of a sequence slot.
    pub fn slot_number(&self) -> Option<Slot>;

    /// The store's conditionally replaced floor (§16.4).
    pub fn floor(store: StoreId) -> Self;

    /// The identity carried by a snapshot path.
    pub fn snapshot_id(&self) -> Option<SnapshotId>;

    /// A snapshot written by a device (§16).
    pub fn snapshot(
        store: StoreId,
        audience: Audience,
        device: DeviceId,
        number: NonZeroU64,
    ) -> Self;

    /// Encrypted file bytes named by a random UUID (§17.3).
    pub fn file(store: StoreId, device: DeviceId, name: FileId) -> Self;

    /// An encrypted join request under its invite id (§13.2).
    pub fn join_request(store: StoreId, invite: InviteId) -> Self;

    /// The invite named by a join request's path.
    pub fn join_request_invite(&self) -> Option<InviteId>;

    /// The store whose objects this path names.
    pub fn store(&self) -> StoreId;

    /// The device named by a snapshot or uploaded file path.
    pub fn device(&self) -> Option<DeviceId>;

    /// The fixed id of a stored file's path.
    pub fn file_id(&self) -> Option<FileId>;

    /// The canonical path components, for providers with folder objects.
    pub fn components(&self) -> Vec<&str>;

    /// The last path component.
    pub fn file_name(&self) -> &str;

    /// The provider path with a leading slash.
    pub fn absolute(&self) -> String;
}

/// A path outside the canonical object layout.
pub struct PathError;

/// A prefix of the validated object layout (§20.3).
pub struct ObjectPrefix { /* private fields */ }

impl ObjectPrefix {
    /// The prefix supplied to the provider.
    pub fn as_str(&self) -> &str;

    /// All objects belonging to this store.
    pub fn all(store: StoreId) -> Self;

    /// Every stored file.
    pub fn files(store: StoreId) -> Self;

    /// The files one device uploaded (§17.3).
    pub fn device_files(store: StoreId, device: DeviceId) -> Self;

    /// Every waiting join request.
    pub fn join_requests(store: StoreId) -> Self;

    /// The store whose objects this prefix selects.
    pub fn store(&self) -> StoreId;

    /// Whether a validated path is under this prefix.
    pub fn contains(&self, path: &ObjectPath) -> bool;
}

/// A nonempty byte range, including its start and excluding its end (§17.5).
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
/// cannot: Windows counts it in 100 ns intervals (§20.5). Ordered by time.
pub struct StorageTime(/* seconds: u64, nanos: u32 */);

impl StorageTime {
    /// The Unix epoch, the earliest storage time.
    pub const UNIX_EPOCH: Self;

    /// Validates the nanosecond fraction.
    pub fn new(seconds: u64, nanos: u32) -> Result<Self, coven_format::Error>;
    pub fn seconds(self) -> u64;
    pub fn nanos(self) -> u32;

    /// The exact provider time, refusing one before the Unix epoch.
    pub fn from_system_time(time: std::time::SystemTime) -> Result<Self, coven_format::Error>;

    /// The time `since` the Unix epoch.
    pub fn since_epoch(since: std::time::Duration) -> Self;

    /// The exact time elapsed since the Unix epoch.
    pub fn elapsed_since_epoch(self) -> std::time::Duration;

    /// This time plus `duration`, or `None` past the representable range.
    pub fn checked_add(self, duration: std::time::Duration) -> Option<Self>;

    /// This time minus `duration`, or `None` before the Unix epoch.
    pub fn checked_sub(self, duration: std::time::Duration) -> Option<Self>;

    /// The exact time from `earlier` to this time, or `None` if `earlier` is later.
    pub fn duration_since(self, earlier: Self) -> Option<std::time::Duration>;
}

/// Metadata of a complete object returned by a provider read, listing or status (§20.5).
pub struct StoredObject {
    /// Validated path relative to the store's location.
    pub path: ObjectPath,
    /// Complete encrypted length in bytes.
    pub size: u64,
    /// Server publication time of the complete object, not the uploading device's clock.
    /// Immutable objects retain their first stored time across retries. Replaced
    /// floor objects carry their replacement time (§20.5).
    pub stored_at: StorageTime,
    /// Timestamp resolution; S3 uses its whole-second HEAD/GET representation (§20.5).
    pub time_resolution: Duration,
    /// Native object id, or the complete S3 key.
    pub object_id: String,
    /// Opaque identity that changes when replaced encrypted bytes change (§20.1).
    /// It is not a checksum or proof of byte equality.
    pub revision: String,
    /// Opaque native replacement condition from this same observation. It can
    /// differ from the revision identity, such as an S3 ETag and version id.
    pub replace_token: String,
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
    Sha1Hex(String),
    DropboxContentHash(String),
    QuickXorBase64(String),
    Sha256Base64(String),
    Crc64NvmeBase64(String),
    Crc32cBase64(String),
    Crc32Base64(String),
    Sha1Base64(String),
}

/// A database or store call's result, retaining its typed cause (Appendix F.5).
pub type CovenResult<T> = Result<T, CovenError>;

/// Failures of opening, reading and writing a store (§4, E1, E3).
pub enum CovenError {
    /// Reading device identity or an app callback's custody operation failed.
    Key(KeyError),
    /// An app callback failed and SQLite also failed to roll it back. Later
    /// calls fail with `DbError::RollbackFailed` until the store is reopened (§4).
    Rollback { operation: Box<CovenError>, rollback: rusqlite::Error },
    /// Cleanup also failed after an app callback failed.
    FileCleanup { write: Box<CovenError>, failures: Vec<DbError> },
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
    /// The keychain has no entry for this device; nothing changed (§19.3).
    DeviceKeyMissing { store: StoreId },
    /// A connected store's member keys are missing too; only restore continues.
    MemberKeysMissing { store: StoreId },
}

/// Invalid input to the pure merge; no partially changed state is returned.
pub enum MergeError {
    /// Milliseconds do not fit in the timestamp's 48 bits.
    MillisecondsOutOfRange(u64),
    /// The final millisecond and counter have both been used.
    TimestampExhausted,
    /// Advancing a generation would exceed its representation.
    GenerationExhausted,
    /// An insert was not made at an even generation, or an edit/delete at an odd one.
    GenerationParity(u64),
    /// No write the author had read moved this row to the specified generation.
    GenerationNotSeen {
        /// The row, including its audience.
        row: coven_merge::RowId,
        /// The generation the change was made at.
        generation: u64,
    },
    /// Causality has not brought the receiving row to this generation.
    GenerationAhead {
        /// The receiving row.
        row: coven_merge::RowId,
    },
    /// A required write is missing from the applied set.
    MissingWrite(WriteId),
    /// A write occurs twice in the input or has already been applied.
    DuplicateWrite(WriteId),
    /// Two different writes have equal timestamps.
    DuplicateTimestamp(WriteId, WriteId),
    /// A timestamp is not larger than that of a write its author had read.
    CausalTimestamp(WriteId),
    /// A write's recorded past omits a predecessor of a write it had read.
    CausalClosure(WriteId),
    /// The timestamp's device is different from the write's device.
    TimestampDevice(WriteId),
    /// A write's row changes do not include the row passed to `apply`.
    MissingChange(coven_merge::RowId),
    /// A parent generation must name an existing incarnation, an odd generation.
    ParentGeneration(u64),
    /// A reference reaches an audience some readers of the child cannot read.
    ReferenceAudience(coven_merge::RowId),
    /// The removal view listed a row more than once.
    // Requires coven-merge/test-utils; coven/test-utils does not enable it.
    #[cfg(feature = "test-utils")]
    DuplicateRow(coven_merge::RowId),
    /// Stored generations must be contiguous from one through the current one.
    GenerationGap(u64),
    /// Each generation must be stamped after the preceding generation.
    GenerationTimestamp(u64),
    /// A deleted row cannot retain winning cells.
    DeletedRowHasCells,
    /// A cell's setter cannot precede the start of its incarnation.
    CellBeforeIncarnation(String),
    /// A lost record has an invalid incarnation or canonical replacer.
    InvalidLostValue(coven_merge::LostKey),
}

/// A failed database call or a write the database refuses (§4, §9, §15, §17).
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
    /// The directory's store id disagrees with the creation entry when it
    /// is prepared, applied or read at opening (§10). Names are not compared.
    StoreSettingsMismatch { store: StoreId },
    /// A synced table declaration or schema is invalid.
    Schema(SchemaError),
    /// App SQL read, changed or defined one of coven's internal objects (§4).
    InternalTable { table: String },
    /// App SQL tried transaction control, a PRAGMA, ATTACH or loading an
    /// extension (§4, E13).
    StatementForbidden { operation: &'static str },
    /// SQLite rolled a migration's transaction back itself, so it can't go on (E13).
    TransactionEnded,
    /// SQLite kept another journal mode instead of WAL (§4).
    WalUnavailable { mode: String },
    /// An app's SQLite setting is larger than SQLite can hold (E1).
    SqliteSettingOutOfRange { setting: &'static str, maximum: u64 },
    /// An app collation has the name of one of SQLite's own (E1).
    ReservedCollation { name: String },
    /// The app gave two collations one name, ignoring ASCII case (E1).
    DuplicateCollation { name: String },
    /// An app function has the name of one of SQLite's or coven's own, or
    /// coven's prefix `coven_` or `_coven_` (E1).
    ReservedFunction { name: String },
    /// The app gave two functions one name, ignoring ASCII case (E1).
    DuplicateFunction { name: String },
    /// An app function is deterministic and takes no arguments (E1).
    ConstantFunction { name: String },
    /// The wall clock reads past the last time a timestamp holds, or no
    /// later timestamp is left (§7.2).
    ClockOutOfRange,
    /// A value or row exceeds the format (§4), or a write's plaintext or sealed
    /// bytes cannot fit in a SQLite value and its queue row (§6.3).
    TooLarge { field: &'static str, actual: u64, maximum: u64 },
    /// The write directly changed another member's row (§9.6). The
    /// transaction, including its upload record, did not commit.
    NotOwner { table: String, key: RowKey, owner: MemberId },
    /// The write's foreign-key actions on a row are not the schema's exact
    /// consequence of its direct changes (§9.6). Nothing committed.
    InvalidForeignKeyEffect { table: String, key: RowKey },
    /// A local trigger wrote a synced table or a shared trigger a local one (§9.4).
    TriggerTarget { trigger: String, table: String },
    /// A reference points at a row outside the source row's audience (§15.3).
    ReferenceAudience { table: String, key: RowKey, column: String },
    /// A write targets a circle whose deletion has been applied (§15.9).
    DeletedCircle(CircleId),
    /// A write puts a row in a circle this member isn't in, or a circle
    /// absent from this device's applied membership (§15.3, §15.8).
    NotInCircle(CircleId),
    /// A reset, schema raise or newly readable circle is still loading (§16.5).
    AudienceReloading(Audience),
    /// This device has exhausted its local write identities (§4).
    WriteNumberExhausted,

    /// Self-replacement has paused app writes while queued objects settle
    /// and the new binding and custody are installed (§12.3, E9).
    MemberKeysChanging,
    /// An inserted row's independent key holds no UUID (§9.2).
    KeyNotUuid { table: String, key: RowKey },
    /// A root row's audience cell is neither `store` nor a circle id in
    /// canonical lowercase form (§15).
    InvalidAudience { table: String, key: RowKey },
    /// A downloaded write fails the merge's checks, such as a timestamp no
    /// later than a write it had read; it is never applied (§19.1).
    InvalidWrite { write: WriteId, error: MergeError },
    /// A received record or its conversion violates the write format (E13).
    WriteFormat(coven_format::Error),
    /// Snapshots or their gap writes could not be checked or loaded; the
    /// database is unchanged (§16.5, §19.2).
    Snapshot(SnapshotError),
    /// The declared migration kind disagrees with its runtime classification (E13).
    MigrationKindMismatch { version: u32 },
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
    /// A file reference no longer names the row's file (§17.5).
    FileRefChanged { table: String, key: RowKey },
    /// The row's declared size differs from the prepared file (§17).
    FileSizeMismatch { expected: u64, actual: u64 },
    /// The user's file is no longer at the recorded path (§17.1).
    UserFileMissing { path: PathBuf },
    /// The user's file changed during preparation or since it was prepared (E3).
    UserFileChanged { path: PathBuf },
    /// App SQL tried to assign a hash or where-column, including NULL (§17.2).
    FileColumnWrite { table: String, column: String },
    /// A batch supplies the same namespace and file id twice.
    FileBatchDuplicate { namespace: String, id: String },
    /// A batch names no app-provided file declaration.
    FileNamespaceNotAppProvided { namespace: String },
    /// No synced table ever declared this file namespace (E8).
    UnknownNamespace { namespace: String },
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
    /// The attaching write could not create its file's random key (§17.2).
    FileKey(CryptoError),
    /// Retaining the store while staging file bytes failed.
    Lock(StoreLockError),
    /// Removing owned bytes or their records failed: on opening (`Ok`), or
    /// after a failed write, whose error it retains (Appendix C.3).
    FileCleanup { write: Result<(), Box<DbError>>, failures: Vec<DbError> },
    /// A transaction failed and rolling it back failed too. Later calls fail
    /// with `RollbackFailed` until the store is reopened (§4).
    Rollback { operation: Box<DbError>, rollback: rusqlite::Error },
    /// An earlier call's rollback failed, so a connection may still be inside
    /// that transaction, holding its uncommitted changes or its old snapshot;
    /// this handle, read-only or not, refuses calls until reopened (§4).
    RollbackFailed,
    /// Saving pending state failed. The call that lost the update and every
    /// later call and live query fail with this same cause until the store is
    /// reopened (E5).
    PendingPersistence(Arc<DbError>),
    /// A pending record or its subject could not be encoded, or a stored one
    /// could not be decoded as the requested record type (E5).
    PendingEncoding(serde_json::Error),
    /// Sync's part of a database transaction failed, keeping sync's own typed
    /// cause, and the transaction did not commit: a streamed transfer's
    /// producer or final authentication check, or the schema raise
    /// operation a migration commits with (§18.3, E13).
    SyncStream(Box<dyn std::error::Error + Send + Sync>),
    /// Closing failed for these connections, after every one was tried (E1).
    Closing { failures: Vec<DbError> },

    /// Another claim must settle before this device reserves a new number.
    ClaimPending,

    /// No current claim exists for this operation.
    NoClaim,

    /// Uncertain publication cannot be abandoned or resealed; a landed writes
    /// batch cannot finish until its consumed state has committed.
    ClaimUnsettled,

    /// The installation has exhausted its claim-number representation.
    ClaimNumberExhausted,

    /// The supplied floor evidence does not cover the attempted slot.
    InvalidClaimCoverage,

    /// Slot application must extend the previously committed prefix.
    SequenceGap {
        /// Previously committed coverage.
        applied: u64,
        /// The received slot.
        received: coven_merge::Slot,
    },

    /// A slot's readable original awaits an external prerequisite.
    WriteWaiting(WriteWait),

    /// Protected remote input fills the configured disk budget. No input or
    /// budget change was committed; increase the budget or finish pending work.
    InputBudgetExceeded {
        /// Bytes required after evicting every eligible range.
        required: u64,
        /// Configured encrypted-input limit.
        maximum: u64,
    },

    /// Upload attempts must follow this device's queue order (§6.3).
    UploadNotOldest {
        /// The write whose upload was requested.
        write: coven_merge::WriteId,
    },

    /// An upload cannot succeed before its sealing keys have been fixed (§6.3).
    UploadNotAttempted {
        /// The write whose success was reported.
        write: coven_merge::WriteId,
    },

    /// Original row-change evidence is unavailable. Receipt waits without
    /// consuming the write; local authoring rolls back instead of guessing.
    MissingWriteSources(Vec<coven_format::write_sources::ChangeId>),

    /// An operation no longer has the state expected by its caller.
    OperationChanged(OperationId),

    /// The prepared raise no longer names the current counted prefix.
    CaptureChanged,
}

/// Why snapshots or their gap writes could not be checked or loaded (§16.5, §19.2).
pub enum SnapshotError {
    /// Reading or checking a selected snapshot's plaintext failed.
    Body { snapshot: SnapshotId, error: Box<DbError> },
    /// Reading the plaintext supplied by sync failed.
    Read(std::io::Error),
    /// A frame or its merge state failed the format's checks.
    Format(coven_format::Error),
    /// The snapshot needs a different app schema (§18).
    Schema { snapshot: u32, database: u32 },
    /// Authenticating a snapshot's original write evidence needs this key.
    KeyUnavailable { audience: Audience, key: KeyId },
    /// Individually valid records disagree with one another or the schema.
    Inconsistent(&'static str),
}

/// What a downloaded write waits for before it can apply (§5.4, §19.1).
pub enum WriteWait {
    /// The original's authored sequence prefix has not been applied.
    Slot(u64),
    /// These original row changes are unavailable even if their slots were consumed.
    Sources(Vec<coven_format::write_sources::ChangeId>),
    /// The app must support this schema version.
    SchemaVersion(u32),
    /// This device's breaking migration awaits its raise and reload.
    SchemaPublication(u32),
    /// The audience's reload has not committed its replacement data.
    Reload(Audience),
}

/// A schema rule checked on open and after migrating (§9, §15.1).
pub enum SchemaError {
    /// Tables sharing a file namespace declare different cache budgets (E8).
    CacheBudgetDisagrees { namespace: String },
    /// A synced foreign key does not reference the complete parent primary key (§9.1).
    /// `key` names the referencing columns and referenced table and columns as SQL.
    ForeignKeyNotPrimaryKey { table: String, key: String },
    /// SET DEFAULT is on a synced table or references one (§9.1).
    /// `key` names the referencing columns and referenced table and columns as SQL.
    SetDefault { table: String, key: String },
    /// A declared synced table isn't in the database (E2).
    MissingTable { table: String },
    /// Two declarations name one table (E2).
    DuplicateTable { table: String },
    /// A table's `owner_only` declaration changed without a breaking
    /// migration (§9.6, §18.1).
    OwnerOnlyChanged { table: String },
    /// A table declares both `audience_column` and `audience_from` (E2).
    TwoAudiences { table: String },
    /// A declared file column isn't in the table (E2).
    FileColumn { table: String, column: String },
    /// A hash or where-column cannot represent a row without a file (§17.2).
    FileColumnNotNullable { table: String, column: String },
    /// SET NULL would change one file column alone (§17.2).
    FileForeignKeyAction { table: String, column: String },
    /// A trigger declared shared isn't on the table (§9.4).
    MissingTrigger { table: String, trigger: String },
    /// A synced table has no primary key (§9.2).
    NoPrimaryKey { table: String },
    /// A primary key column allows NULL (§9.2).
    NullableKey { table: String, column: String },
    /// A foreign-key action would assign a generated column (§9.1).
    GeneratedForeignKeyAction { table: String, column: String },
    /// SET NULL would put NULL in a NOT NULL column (§9.1).
    ImpossibleAction { table: String, column: String },
    /// A local table's foreign key could stop coven deleting a synced row (§9.1).
    LocalChildAction { table: String, column: String },
    /// A local table's foreign key could stop coven updating a synced value (§9.1).
    LocalChildUpdate { table: String, column: String },
    /// SQLite chooses the primary key itself (§9.2).
    GeneratedPrimaryKey { table: String },
    /// A synced key, unique or foreign-key column, or a unique index term, uses
    /// a collation other than BINARY (§9.2, D3). `column` names the index for
    /// an expression term.
    KeyCollation { table: String, column: String, collation: String },
    /// An independent key has no text column to hold its UUID (§9.2).
    IndependentKeyNotUuid { table: String },
    /// Declared key columns do not match the table's primary key (E2).
    KeyColumns { table: String },
    /// SET NULL acts on a primary-key column (§9.1).
    PrimaryKeyAction { table: String, column: String },
    /// The audience root column is nullable or is not text (§15, E2).
    AudienceColumn { table: String, column: String },
    /// The audience foreign key spans more than one column (§15.1).
    AudienceForeignKeyColumns { table: String },
    /// The audience foreign key does not point into a synced table (§15.1).
    AudienceForeignKeyTarget { table: String, column: String },
    /// The audience foreign key uses SET NULL (§15.1).
    AudienceForeignKeyAction { table: String, column: String },
    /// Following audience foreign keys forms a loop (§15.1).
    AudienceCycle { table: String },
    /// A unique constraint or shared key spans audiences (§15.2).
    AudienceConstraint { table: String, constraint: String },
    /// A shared trigger lacks WHEN NOT coven_applying() (§9.4).
    SharedTriggerGuard { table: String, trigger: String },
}

/// Coven's local tables cannot be used at this version (Appendix C.1, E1).
pub enum CovenMigrationError {
    /// The read-only connection cannot migrate coven's tables.
    ReadOnly,
    /// The internal schema requires a newer coven.
    SchemaTooNew { current: u32, supported: u32 },
    /// A migration failed and its transaction rolled back.
    Failed { source: Box<DbError> },
}

/// A filesystem failure, including whether bytes already changed (E1, Appendix F.5).
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

impl FileError {
    /// Whether readers already see the replacement despite a durability error.
    pub fn installed_new_bytes(&self) -> bool;
}

/// File reads and custody report the same filesystem failures (§17, E1).
pub use coven_foundation::files::FileError as DiskError;

/// Replace an app-owned file using coven's durable replacement operation.
/// Writes and syncs a temporary sibling, replaces the target, then syncs its
/// directory. The parent must exist; concurrent readers see whole versions.
/// A pre-replacement failure leaves the target unchanged. AfterReplace means
/// the new bytes are visible but directory syncing failed.
/// Windows requires FileRenameInfoEx and a filesystem supporting POSIX replacement;
/// unsupported systems fail with the OS cause, without a weaker rename fallback.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), FileError>;

/// Whether other holders may share a `FileLock`.
pub enum LockMode {
    /// Coexists with other shared holders and excludes exclusive ones.
    Shared,
    /// Excludes every other holder.
    Exclusive,
}

/// An OS lock on an open file, released when dropped. Separate opens of one
/// file exclude each other, within a process and across processes. Unix
/// systems, Android included, take `flock`; Windows takes `LockFileEx`.
/// std's `File::lock` family is not used: on Android it returns `Unsupported`.
pub struct FileLock { /* private */ }

impl FileLock {
    /// Wait for the lock on `file`.
    pub fn lock(file: File, mode: LockMode) -> std::io::Result<FileLock>;
    /// Lock `file` unless another holder excludes it (`TryLockError::WouldBlock`).
    pub fn try_lock(file: File, mode: LockMode) -> Result<FileLock, std::fs::TryLockError>;
}

/// Every file under `dir` this process has open, once per descriptor or handle.
/// Excludes directories and, on Windows, handles opened only to read attributes.
/// Panics if the directory cannot be resolved or enumeration fails. Files closed
/// during enumeration may disappear from the result.
#[cfg(feature = "test-utils")]
pub fn open_files_under(dir: &Path) -> Vec<PathBuf>;

/// Panics naming the open files under `dir`, or if enumeration fails.
#[cfg(feature = "test-utils")]
pub fn assert_no_open_files_under(dir: &Path);

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
    /// An unfinished first installation must be resumed by the call that
    /// began it: restore or join.
    BootstrapPending(StoreId),
    /// A replacement of this device's installation swapped in but did not
    /// finish. Opening, and the other of `reset_device` and `restore_device`,
    /// are refused before anything changes; only `by` finishes it, or
    /// `Coven::discard_store` deletes the store from this device (§19.3).
    ReplacementUnfinished { store: StoreId, by: ReplacedBy },
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
    /// Reading this store's current name failed; no older name is substituted.
    Store { store: StoreId, source: CovenError },
}

/// A store creation failure, with publication and rollback failures explicit.
pub enum DirectoryCreationError {
    /// A directory or file already occupies this store's id. It is untouched.
    AlreadyExists(StoreId),
    /// An operation failed before publication.
    File(FileError),
    /// Writing the new device's settings failed before publication.
    Settings(SettingsError),
    /// The entire store is visible, but syncing its parent failed.
    Published {
        /// The store that is already visible.
        id: StoreId,
        /// The durability error.
        source: std::io::Error,
    },
    /// Removing the unpublished directory also failed.
    Rollback {
        /// The original creation failure.
        operation: Box<DirectoryCreationError>,
        /// The error removing the directory.
        cleanup: std::io::Error,
    },
}

/// Creating a store failed, with publication and rollback made explicit (E1).
pub enum StoreCreationError {
    /// Preparing, publishing or removing the hidden directory failed.
    Directory(BootstrapDirectoryError),
    /// Opening the database or custody failed.
    Store(CovenError),
    /// Creating or persisting the member identity failed.
    Identity(IdentityError),
    /// Closing connections or workers failed.
    Sync(SyncError),
    /// Closing or rollback also failed; both causes remain available.
    Cleanup { operation: Box<StoreCreationError>, cleanup: Box<StoreCreationError> },
    /// Complete store on disk, closed before returning; directory sync failed.
    PublishedButNotSynced {
        /// The published store; reopen it with `CovenBuilder::open`.
        store: StoreId,
        /// The directory-sync failure, retaining any close failure as `Cleanup`.
        source: Box<StoreCreationError>,
    },
}

/// Deleting the local store stopped at a step the caller may retry (E1).
pub enum StoreDeletionError {
    /// The store is open or its lock could not be taken.
    Lock(StoreLockError),
    /// Writes or files wait to upload, as `unsent_work` counts them;
    /// nothing was deleted.
    Unsent(UnsentWork),
    /// Reading the queue or publishing this device's removal failed, deleting
    /// nothing; or closing failed after the removal.
    Sync(SyncError),
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
    /// Native NoStorageAccess, or Apple's errSecInteractionNotAllowed (-25308):
    /// a locked session, which unlocking clears; retains the native cause.
    KeychainTemporarilyUnavailable(KeychainError),
    /// Apple's errSecMissingEntitlement (-34018): the app lacks its
    /// keychain-access-groups entitlement; retains the native cause.
    MissingKeychainEntitlement(KeychainError),
    /// The keyring service was not registered at startup.
    ServiceNotRegistered,
    /// A different service name was already registered.
    ServiceAlreadyRegistered,
    /// The service name is empty or contains NUL.
    InvalidServiceName,
    /// This platform has no native credential store.
    UnsupportedKeyringPlatform,

    /// Synced entries require an Apple keychain.
    Unsupported,
    /// The person left the store: their synced entries were deleted, and
    /// this installation refuses to write them again (§14.1).
    Left,

    /// A synced restore-code account's id part is not a store id as coven
    /// writes it.
    RestoreCodeStoreId,
}

/// Secret bytes crossing custody or code boundaries, erased when dropped (§12.4, §13).
pub struct SecretBytes { /* private fields */ }

impl SecretBytes {
    /// Takes ownership of bytes, including their allocation capacity.
    pub fn new(bytes: Vec<u8>) -> Self;
    /// Borrows bytes for custody or a restore code.
    pub fn as_bytes(&self) -> &[u8];

    /// Allocation capacity for tests checking that secret encoders never grow it.
    #[cfg(feature = "test-utils")]
    pub fn capacity(&self) -> usize;
}

/// Secret text, erased when dropped and redacted in diagnostics (§12.4, E10).
pub struct SecretText { /* private fields */ }

impl SecretText {
    /// Takes ownership of text, including its allocation capacity.
    pub fn new(text: String) -> Self;
    /// Borrows text for a provider request or key custody.
    pub fn as_str(&self) -> &str;
}

/// A native keychain cause whose diagnostics do not expose secret bytes (§12.4).
pub struct KeychainError(/* private */);

/// A cryptographic operation failed (Appendix D).
pub enum CryptoError {
    /// The device cannot provide a cryptographic service needed by this call.
    Unavailable(Box<dyn std::error::Error + Send + Sync>),
    /// The ciphertext, key, path, index or associated data did not authenticate.
    Authentication,
    /// A sealed value was truncated or had invalid framing.
    Malformed,
    /// An opened key's audience or identity disagrees with its routing prefix.
    KeyIdentity,
    /// The X25519 public key has low order.
    WeakSealingKey,
    /// The Ed25519 public key is invalid or weak.
    InvalidSigningKey,
    /// The member's signature did not verify.
    Signature,
    /// Authenticated key material had the wrong shape.
    Material(MaterialError),
}

/// A key or encoded secret has an invalid representation, or isn't held (§12.4).
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

/// A store or circle key's random id, named by the admin slot that
/// brings the key in (§12.1).
pub struct KeyId(pub Uuid);

/// Registers the OS keychain service every key and secret is stored under.
/// Called once at startup, before any store opens.
pub fn set_keyring_service(name: impl Into<String>) -> Result<(), KeyError>;

impl StoreDir {
    /// The store identified by this directory.
    pub fn id(&self) -> StoreId;
}

impl StoreLayout {
    /// The stores under `app_dir`, one directory each. The app creates
    /// `app_dir`; coven creates directories only inside it and never opens a
    /// directory above it, which Android refuses.
    pub fn new(app_dir: PathBuf) -> Self;

    /// The stores on this device, ordered by id, with their committed names.
    /// Reads names from their databases; unreadable names return StoreLayoutError::Store.
    /// A store whose replacement is unfinished is listed with the call that
    /// finishes it; an unfinished first installation is not listed.
    pub async fn stores(&self) -> Result<Vec<StoreInfo>, StoreLayoutError>;

    /// The directory of the store with `id`.
    pub fn store_dir(&self, id: &StoreId) -> StoreDir;

    /// Publish a directory fixture without a database or member custody.
    #[cfg(feature = "test-utils")]
    pub fn create_store_dir(
        &self,
        id: StoreId,
        ids: &dyn IdSource,
    ) -> Result<StoreDir, DirectoryCreationError>;
}

pub struct Coven;

impl Coven {
    /// Read the recorded location without opening the store or its custody (E1).
    pub async fn storage_config(store_dir: &StoreDir) -> Result<Option<StorageConfig>, StorageError>;

    /// Configure creating, opening, restoring or joining a store.
    pub fn builder(layout: StoreLayout) -> CovenBuilder;

    /// Deletes a closed store without opening it: this device's own keychain
    /// entries for it, then its directory. The person's restore code and S3
    /// key in iCloud Keychain stay for their other devices (§13.1). Unsent
    /// work is lost; membership stays until this device is removed. For a
    /// store that cannot open, or to finish `CovenHandle::delete_store`. A
    /// store whose `reset_device` or `restore_device` stopped after its swap
    /// is deleted whole: both installations, the replacement's directory and
    /// record, and this device's entries, whichever device they name by then.
    /// Refused while a writer, read-only handle, file stream, file I/O or a
    /// running replacement holds its lock; storage is untouched. Retrying
    /// finishes a deletion that failed partway.
    pub async fn discard_store(store_dir: &StoreDir) -> Result<(), StoreDeletionError>;
}

impl CovenBuilder {
    /// The tables that sync (E2). Required.
    pub fn synced_tables(self, tables: Vec<SyncedTable>) -> Self;

    /// The app's schema migrations, numbered from 1 with no gaps (E13).
    /// Required.
    pub fn migrations(self, migrations: Vec<Migration>) -> Self;

    /// The wall clock that timestamps use (§7.2). Defaults to the system clock.
    pub fn clock(self, clock: ClockRef) -> Self;

    /// Replace OS-random ids in tests (Appendix F.2).
    #[cfg(feature = "test-utils")]
    pub fn id_source(self, ids: IdSourceRef) -> Self;

    /// The app's own OAuth clients for Dropbox and OneDrive. Coven ships none.
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

    /// How many downloads run at once, including sequence fetch-ahead and file
    /// ranges (E5, E8). Defaults to one.
    pub fn max_concurrent_downloads(self, n: NonZeroUsize) -> Self;

    /// Where this device keeps the store keys: the OS keychain by default, a
    /// file sealed with a passphrase, memory for this session only, or the
    /// app's own `StoreKeyCustody`.
    pub fn key_custody(self, custody: KeyCustody) -> Self;

    /// Where this device keeps its member's keys, with the same four choices
    /// and the app's own `MemberKeyCustody` as the last.
    pub fn identity_custody(self, custody: IdentityCustody) -> Self;

    /// SQLite settings for every connection a handle opens to the store's
    /// database. Unset ones keep SQLite's defaults; the page size and
    /// auto-vacuum mode apply only when the database is created.
    pub fn sqlite_settings(self, settings: SqliteSettings) -> Self;

    /// A collation the app's SQL names with `COLLATE name`, ordering text by
    /// `compare` on every connection a handle opens to the store's database.
    /// `compare` must be deterministic, the same on every device and app
    /// version, and a total order.
    pub fn collation<F>(self, name: impl Into<String>, compare: F) -> Self
    where
        F: Fn(&str, &str) -> Ordering + Send + Sync + 'static;

    /// A scalar function the app's SQL calls as `name(...)` with exactly
    /// `arguments` arguments, on every connection a handle opens to the
    /// store's database. `compute` returns the result, or an error that fails
    /// the statement calling it. A deterministic function the schema uses must
    /// give the same result for the same arguments on every device and app
    /// version.
    pub fn function<F, T>(
        self,
        name: impl Into<String>,
        arguments: u8,
        determinism: Determinism,
        compute: F,
    ) -> Self
    where
        F: Fn(&FunctionArguments<'_>) -> SqliteResult<T> + Send + Sync + 'static,
        T: Into<types::Value>;

    /// Creates an open store and its member in the chosen custody before
    /// publication; cancellation does not interrupt creation or rollback.
    /// Errors carry no handle; a published store is closed before returning.
    pub async fn create_store(self, name: &str) -> Result<CovenHandle, StoreCreationError>;

    /// Opens the store for reading and writing, taking the store's lock.
    /// Opening always migrates coven's tables before the app's schema, then
    /// unlocks each configured key custody once for this handle's session,
    /// then resumes unfinished operations and committed file work. Empty
    /// custody is allowed; malformed or inaccessible custody fails opening.
    /// Local database calls need no store key. Opening does not start
    /// the sync loop; `start_sync` starts it. A store with storage set up
    /// opens as `Stopped`; otherwise its status is `Disconnected`.
    pub async fn open(self, store: StoreId) -> CovenResult<CovenHandle>;

    /// Replaces this copy with a new device loaded from storage when the app
    /// chooses: a damaged database (§19.3), a missing device entry, or another
    /// copy using this id (§19.3). Uses saved settings and custody; no salvage.
    /// Checks storage and keys, then loads beside the old copy, which moves
    /// aside only once loading is complete. Saved member keys that history
    /// has replaced fail with MemberKeysReplaced and change nothing. Uses the
    /// existing bootstrap errors; an interrupted call resumes with the same
    /// fresh id. A swap restore_device began is refused with
    /// ReplacementUnfinished, changing nothing. The returned handle holds a
    /// device-reset notice that unsent edits may have been lost (E5).
    pub async fn reset_device(self, store: StoreId, device_name: &str) -> Result<CovenHandle, BootstrapError>;

    /// Opens the store for reading only, alongside a handle that has it open,
    /// for example from another process. Its shared lock prevents deletion
    /// while its read connections and local cache connection remain open.
    /// It runs no migration and refuses a database whose schema is newer than
    /// its migrations or whose coven tables need migrating.
    /// File reads use saved storage settings and custody credentials when present.
    pub async fn open_read_only(self, store: StoreId) -> Result<CovenReadHandle, ReadOnlyOpenError>;

    /// Replace provider construction in tests while exercising setup, credential
    /// custody, reconnect, restore and recovery through the production graph.
    /// `coven_storage::test_utils::MemoryStorage` implements this connector.
    #[cfg(feature = "test-utils")]
    pub fn storage_connector(mut self, connector: Arc<dyn StorageConnector>) -> Self;
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
    /// Creation, restore or join fills this session-only custody.
    InMemory,
    Custom(Arc<dyn MemberKeyCustody>),
}

impl CovenHandle {
    /// The stable id, including before storage setup.
    pub fn store_id(&self) -> StoreId;

    /// This installation's device id, available before setup and retained after close.
    pub fn device_id(&self) -> DeviceId;

    /// The same committed location as Coven::storage_config; refused after close.
    pub async fn storage_config(&self) -> Result<Option<StorageConfig>, SyncError>;

    /// Closes the store: stops syncing, operation and file work, closes every
    /// connection, erases the unlocked custody sessions and releases the writer
    /// lock. Open file streams and outstanding file I/O
    /// retain their shared deletion guards. Later database calls on any clone
    /// fail with `DbError::StoreClosed`; custody calls fail with `KeyError::StoreClosed`.
    /// Reports every failure of closing workers and connections, one alone as
    /// itself and several chained as `SyncError::Cleanup`; cancellation does
    /// not stop closing.
    pub async fn close(&self) -> Result<(), SyncError>;

    /// Deletes this store from this device. With storage set up, it refuses
    /// unsent work unless discarding it, then publishes this device's removal
    /// (§14.1); either failure deletes nothing. Then closes and deletes as
    /// `Coven::discard_store`, which finishes a deletion failing after that.
    /// Only this device leaves: the person's restore code and S3 key stay.
    pub async fn delete_store(&self, unsent: IfUnsent) -> Result<(), StoreDeletionError>;

    /// The writes and files this device committed and has not uploaded, from
    /// one committed snapshot: what deleting the store would lose. With storage
    /// set up, `delete_store(IfUnsent::Refuse)` refuses with these counts.
    /// Available while disconnected.
    pub async fn unsent_work(&self) -> CovenResult<UnsentWork>;

    /// The same pre-built query, live (E4).
    pub fn subscribe_unsent_work(&self) -> LiveQuery<UnsentWork>;

    /// The SQLite settings the store's connections have in effect.
    pub async fn sqlite_settings(&self) -> Result<SqliteSettingsInEffect, DbError>;

    /// Checkpoints the store's WAL between writes, returning what SQLite
    /// reports. `Restart` and `Truncate` wait up to the busy timeout for
    /// reads still using the WAL, and report `busy` if they outlast it.
    pub async fn checkpoint(&self, mode: CheckpointMode) -> Result<WalCheckpoint, DbError>;

    /// Gives back free pages of a store created with incremental auto-vacuum,
    /// between writes. On any other store it does nothing.
    pub async fn incremental_vacuum(&self, pages: IncrementalVacuum) -> Result<(), DbError>;

    /// Read committed plaintext records still awaiting upload in application tests.
    #[cfg(feature = "test-utils")]
    pub async fn test_queued_writes(
        &self,
    ) -> Result<Vec<coven_format::write::WriteRecord>, DbError>;
}

pub enum IfUnsent { Refuse, Discard }

/// Committed writes and attached files not yet uploaded.
pub struct UnsentWork { pub writes: u64, pub files: u64 }

impl UnsentWork {
    /// Whether anything waits to upload.
    pub fn is_empty(&self) -> bool;
}

/// SQLite settings for the store's connections; `None` keeps SQLite's default,
/// except `checkpoint_fullfsync`, on for macOS and iOS. Each field's PRAGMA is
/// its name. `Default` leaves every one unset.
#[derive(Default)]
pub struct SqliteSettings {
    /// At most `i32::MAX` milliseconds.
    pub busy_timeout: Option<Duration>,
    pub cache_size: Option<CacheSize>,
    pub cache_spill: Option<CacheSpill>,
    /// At most `i64::MAX`; SQLite lowers it to its build's limit.
    pub mmap_size: Option<u64>,
    pub temp_store: Option<TempStore>,
    /// At most `i32::MAX`; SQLite lowers it to its build's limit.
    pub threads: Option<u32>,
    pub journal_size_limit: Option<JournalSizeLimit>,
    pub wal_autocheckpoint: Option<WalAutocheckpoint>,
    pub automatic_index: Option<bool>,
    pub cell_size_check: Option<bool>,
    pub secure_delete: Option<SecureDelete>,
    pub fullfsync: Option<bool>,
    pub checkpoint_fullfsync: Option<bool>,
    pub analysis_limit: Option<AnalysisLimit>,
    /// Applies only when the database is created.
    pub page_size: Option<PageSize>,
    /// Applies only when the database is created.
    pub auto_vacuum: Option<AutoVacuum>,
}

/// The same settings as SQLite reports them, every one known.
pub struct SqliteSettingsInEffect {
    pub busy_timeout: Duration,
    pub cache_size: CacheSize,
    /// When on, never below the cache's own page count.
    pub cache_spill: CacheSpill,
    pub mmap_size: u64,
    pub temp_store: TempStore,
    pub threads: u32,
    pub journal_size_limit: JournalSizeLimit,
    pub wal_autocheckpoint: WalAutocheckpoint,
    pub automatic_index: bool,
    pub cell_size_check: bool,
    pub secure_delete: SecureDelete,
    pub fullfsync: bool,
    pub checkpoint_fullfsync: bool,
    pub analysis_limit: AnalysisLimit,
    pub page_size: PageSize,
    pub auto_vacuum: AutoVacuum,
}

/// Counts at most `i32::MAX` unless noted.
pub enum CacheSize { Pages(u32), Kibibytes(u32) }
pub enum CacheSpill { Off, Pages(NonZeroU32) }
pub enum TempStore { Default, File, Memory }
/// Bytes at most `i64::MAX`.
pub enum JournalSizeLimit { Unlimited, Bytes(u64) }
pub enum WalAutocheckpoint { Off, Pages(NonZeroU32) }
pub enum SecureDelete { Off, On, Fast }
pub enum AnalysisLimit { Unlimited, Rows(NonZeroU32) }
pub enum PageSize {
    Bytes512, Bytes1024, Bytes2048, Bytes4096,
    Bytes8192, Bytes16384, Bytes32768, Bytes65536,
}
pub enum AutoVacuum { Off, Full, Incremental }

pub enum CheckpointMode { Passive, Full, Restart, Truncate }

/// What a checkpoint did, as SQLite reports it.
pub struct WalCheckpoint {
    /// `Full`, `Restart` or `Truncate` couldn't finish within the busy timeout.
    pub busy: bool,
    /// Frames in the WAL.
    pub log_frames: u32,
    /// Frames copied into the database, this time or before.
    pub checkpointed_frames: u32,
}

pub enum IncrementalVacuum { All, Pages(NonZeroU32) }

/// Whether an app function's result depends on its arguments alone; only a
/// deterministic one can appear where a stored value depends on it.
pub enum Determinism { Deterministic, Nondeterministic }

/// The arguments SQLite passes one call of an app function.
pub struct FunctionArguments<'a>(/* private */);

impl FunctionArguments<'_> {
    /// Argument `index`, counted from zero, converted as a row's column is.
    /// An index past the function's arguments panics.
    pub fn get<T: types::FromSql>(&self, index: usize) -> SqliteResult<T>;
    /// Argument `index`, counted from zero, as SQLite holds it.
    pub fn get_raw(&self, index: usize) -> types::ValueRef<'_>;
}
```

Example:

```rust
coven::set_keyring_service("com.example.notes")?;

let layout = StoreLayout::new(app_dir);
let handle = Coven::builder(layout.clone())
    .synced_tables(tables())                        // E2
    .migrations(migrations())                       // E13
    .create_store("Household")
    .await?;
```

Example, choosing a not-shared Dropbox store when both app keys are registered:

```rust
handle.authenticate(OAuthAccess::Dropbox(DropboxAccess::AppFolder)).await?;
handle.setup_oauth_storage(
    StorageConfig::Dropbox { location: DropboxLocation::AppFolder, account_label: None },
    "Ana's phone",
).await?;
// A shared choice uses DropboxAccess::FullDropbox and
// DropboxLocation::SharedFolder { namespace_id } instead.
```

### E2 Declaring synced tables

- Table names in declarations match SQLite identifiers without regard to ASCII
  case. Opening records SQLite's spelling, which identifies the table in writes,
  snapshots and file retention.
- Declaration getters let app tests inspect builder choices without opening a store.
  They borrow the declarations as supplied, preserving spelling and order; they do
  not report the opened schema or current cache budgets. Getters whose names would
  collide with a builder method use `get_`; boolean getters use `is_`.
- Each synced table declares its kind of key ([§9.2](coven.md#92-keys-and-unique-values)),
  how its rows get their audience ([§15](coven.md#15-circles)), and whether its rows
  carry a file ([§17](coven.md#17-files)). It may restrict direct writes to
  each row's original creator with `owner_only()`
  ([§9.6](coven.md#96-owner-tables)).
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
    /// App-derived keys: equal keys are one row everywhere.
    SharedKey,
}

impl SyncedTable {
    /// The table name exactly as supplied to the declaration.
    pub fn name(&self) -> &str;

    /// The declared kind of row identity.
    pub fn identity(&self) -> &RowIdentity;

    /// The declared primary-key columns, in order; defaults to `id`.
    pub fn get_key_columns(&self) -> &[String];

    /// Whether direct writes are restricted to each row's original creator.
    pub fn is_owner_only(&self) -> bool;

    /// The root audience column, including when inheritance was also declared.
    /// Opening refuses a declaration with both audience sources.
    pub fn get_audience_column(&self) -> Option<&str>;

    /// The audience foreign key, including when a root column was also declared.
    /// Opening refuses a declaration with both audience sources.
    pub fn get_audience_from(&self) -> Option<&str>;

    /// The file declaration supplied to `carries_files`, if any.
    pub fn files(&self) -> Option<&FileDecl>;

    /// The shared trigger names, in declaration order, including repetitions.
    pub fn shared_triggers(&self) -> &[String];

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
    /// consequences are verified separately (§9.6). No app owner column is needed.
    pub fn owner_only(self) -> Self;

    /// Root audience column: bind Audience or CircleId; NULL and
    /// noncanonical text are refused (§15).
    pub fn audience_column(self, column: impl Into<String>) -> Self;

    /// A descendant: each row takes the audience of the row its
    /// `foreign_key` column points at (§15.1).
    pub fn audience_from(self, foreign_key: impl Into<String>) -> Self;

    /// The table's rows carry a file, declared by `declaration`.
    pub fn carries_files(self, declaration: FileDecl) -> Self;

    /// Declares the trigger `name` on this table as shared (§9.4); every
    /// trigger not declared shared is local. Opening refuses a shared trigger
    /// without `WHEN NOT coven_applying()`, naming it.
    pub fn shared_trigger(self, name: impl Into<String>) -> Self;
}

pub enum Provenance {
    /// The user's own file at a path on their device; coven records it and
    /// never copies, changes or deletes it (§17.1).
    UserProvided,
    /// Bytes the app hands to coven, which keeps and owns them.
    AppProvided,
}

/// One table's file columns, namespace and kind (§17, E2).
pub struct FileDecl { /* private fields */ }

impl FileDecl {
    /// The cache namespace exactly as declared.
    pub fn namespace(&self) -> &str;

    /// The initial cache budget in bytes, as declared, not its current setting.
    pub fn cache_budget(&self) -> u64;

    /// Whether the declared file belongs to the user or is supplied by the app.
    pub fn provenance(&self) -> &Provenance;

    /// The declared file identity column; defaults to `id`.
    pub fn id_column(&self) -> &str;

    /// The declared byte-count column; defaults to `size`.
    pub fn size_column(&self) -> &str;

    /// The declared content-hash column; defaults to `hash`.
    pub fn hash_column(&self) -> &str;

    /// The declared location column; defaults to `location`.
    pub fn location_column(&self) -> &str;

    /// Whether the declaration forbids replacing an existing file.
    pub fn is_write_once(&self) -> bool;

    /// Declares the file a table's rows carry: its namespace, which groups
    /// files in the cache, its cache budget in bytes from first declaration
    /// (E8), and its kind. Every attached file is queued for upload as the
    /// attaching write commits, even disconnected; other devices cache it
    /// when they read it.
    pub fn new(namespace: impl Into<String>, cache_budget: u64, provenance: Provenance) -> Self;

    /// The column naming the file. Defaults to `id`.
    pub fn with_id_column(self, column: impl Into<String>) -> Self;

    /// The column holding the file's size in bytes. Defaults to `size`.
    pub fn with_size_column(self, column: impl Into<String>) -> Self;

    /// The column holding the file's content hash, the SHA-256 of its bytes,
    /// which coven fills in when the file's upload completes (§17.2, §17.3).
    /// Must allow NULL; app SQL cannot assign it. Defaults to `hash`.
    pub fn with_hash_column(self, column: impl Into<String>) -> Self;

    /// The column holding the fixed path, uploader and file key chosen by
    /// the attaching write, which coven fills in when the file's upload
    /// completes (§17.2, D12). `file_status` checks availability without
    /// changing this column.
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
            .carries_files(FileDecl::new("attachments", 2 << 30, Provenance::UserProvided)),
        // A thumbnail the app makes, in the note's audience.
        SyncedTable::new("thumbnails", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .carries_files(FileDecl::new("thumbnails", 256 << 20, Provenance::AppProvided)),
        // In the store, with keys from the tag's name.
        SyncedTable::new("tags", RowIdentity::SharedKey),
        // A shared key over two columns, which includes note_id (§15.2).
        SyncedTable::new("note_tags", RowIdentity::SharedKey)
            .key_columns(["note_id", "tag_id"])
            .audience_from("note_id"),
    ]
}
```

### E3 Writing

- App SQL runs in one transaction; errors roll back rows and files
  ([§4](coven.md#4-writing-locally)).
- Ordinary reload queues app writes with their original authored view. Required audience reloads
  return `AudienceReloading`; self-replacement returns `MemberKeysChanging`. Retry after
  installation to author against its state ([§12.3](coven.md#123-replacing-a-members-keys),
  [§16.5](coven.md#165-coming-back-after-a-long-absence)).

```rust
/// App-provided files staged for one write; a failure discards them (E3).
pub struct WriteBatch { /* private fields */ }

/// A transaction's SQL access, borrowing its connection and write's file state (§4, E3).
pub struct SqlContext<'connection, 'write> { /* private fields */ }

/// A user's file checked before a write; callers cannot change its recorded facts (E3).
pub struct PreparedUserFile { /* private fields */ }

/// The facts recorded for a user's original file (§17.1).
pub struct UserFile {
    /// The user's path, which coven never changes or deletes.
    pub path: PathBuf,
    /// Its size in bytes.
    pub size: u64,
    /// Its recorded modification time.
    pub modified_at: SystemTime,
}

/// A row's file at the time it was read; its captured facts are private (§17.5).
pub struct FileRef { /* private fields */ }

impl CovenHandle {
    /// Runs one write. A statement or shared trigger targeting another owner's
    /// row refuses the transaction with DbError::NotOwner (§9.6). Authorized
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
    /// the write. The write records the row's pending attachment; the write
    /// that completes the upload fills its size, hash and where-columns
    /// (§17.2).
    pub fn put_file(
        &mut self,
        namespace: impl Into<String>,
        id: impl Into<String>,
        bytes: impl Into<FileSource>,
    );
}

/// A portable single filename: ASCII letters, digits, `-`, `_` and interior
/// dots, up to 255 bytes. Windows device names are refused on every platform.
pub struct FileName(/* private */);

impl FileName {
    /// The validated filename, for recording an owned file in local metadata.
    pub fn as_str(&self) -> &str;

    /// Validate a single filename before it is joined to an owned directory.
    pub fn new(name: impl Into<String>) -> Result<Self, FileNameError>;
}

/// A name that could escape its directory or alias a Windows device.
pub enum FileNameError {
    /// The name is empty or exceeds a filesystem component's length.
    Length,
    /// The name contains a forbidden byte, starts with a dot or ends with one.
    Character,
    /// The name names a Windows device, even with an extension.
    Reserved,
}

pub enum FileSource {
    Bytes(Vec<u8>),
    Stream(Pin<Box<dyn AsyncRead + Send>>),
}

impl SqlContext<'_, '_> {
    /// Ordinary SQL inside the write.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize>;
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()>;
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>;
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>;

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

    /// Records a prepared user-provided file on a row the write already has,
    /// as the row's pending attachment until its upload completes (§17.2).
    /// Fails if the row's size column disagrees with the file. The row's
    /// four file columns keep their values from before the write until then.
    pub fn register_user_file(
        &self,
        table: &str,
        key: impl Into<RowKey>,
        prepared: PreparedUserFile,
    ) -> Result<(), DbError>;

    /// Forgets the user-provided file recorded on a row and clears its hash
    /// and where-columns to NULL. All four file columns go in the write
    /// (§17.1). It also abandons the row's pending attachment, if any
    /// (§17.2). The original is untouched.
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
let note_id = UuidText(Uuid::now_v7());
let attachment_id = UuidText(Uuid::now_v7());
let size = std::fs::metadata(&path)?.len() as i64;
let prepared = prepare_user_file(&path, |read| show_progress(read)).await?;

handle
    .write(move |sql| {
        sql.execute(
            "INSERT INTO notes (id, title, audience) VALUES (?1, ?2, ?3)",
            (note_id, "Paint colors", Audience::Store),
        )?;
        sql.execute(
            "INSERT INTO attachments (id, note_id, title, size) VALUES (?1, ?2, ?3, ?4)",
            (&attachment_id, &note_id, "Swatches", size),
        )?;
        sql.register_user_file("attachments", attachment_id, prepared)?;
        Ok(())
    })
    .await?;
```

### E4 Reading

- Reads run on several read-only connections at once ([§4](coven.md#4-writing-locally)).
- A *live query* runs once, then again whenever a write commits that changes
  rows it read. Its errors are results and don't end it, except the one saying
  the store closed or its handle fails every call: after that one, `next()`
  returns `None`, and never repeats it.
- Table-backed built-in reads work offline. Each `subscribe_x()` counterpart reports the same
  committed data and later changes as its corresponding one-shot read.
- Upload progress counts provider-confirmed parts; file progress counts verified bytes
  committed to the cache. Neither reports bytes merely received or sent.
- Sync status and open invites have their own streams (E5, E10).
- E.g. Ana's app reads `members()` to draw its first list, or subscribes with
  `subscribe_members()` to receive that same list and later committed changes.
- Subscriptions show current results, as slots apply. Their callback
  histories need not match between devices (§10).
- Offer a removed row elsewhere with the same key (§8.4); copying to a new key
  can produce two rows. Published owner and unique claims follow slot order.
- *Coven's state in SQL.* App SQL reads five views of coven's own tables,
  in reads, writes and migrations alike ([§4](coven.md#4-writing-locally)). They
  are coven's to define and the app's to rely on: the columns below are stable.
  A view is true in every transaction that changes the tables it reads, and a
  live query that reads one reruns when coven changes them. The app can't
  write a view, or name an object of its own after one.
- A row is named by `table_name` and `key`, the row's key as coven stores it
  (format.md D3). `coven_key(v1, v2, ...)` gives the same bytes for the app's
  own key columns, in key order, so a join reads
  `u.table_name='photos' AND u.key=coven_key(p.id)`. It is deterministic, and an
  index on an expression can hold it. It takes one value or more, up to the
  1,000 an SQL call can pass; `coven_key()` with none fails the statement.
- Whether a file is being sent now, and its bytes on the way, last only as long
  as the process; they stay on `subscribe_uploads()` (E7). So does a pending
  record's cause, which `subscribe_pending_causes()` gives by record (E5).

| View | Column | Holds |
|---|---|---|
| `coven_uploads`: each file waiting in this device's upload queue, as `uploads()` lists it | `table_name`, `key` | the row whose file it is |
| | `position` | its place in the queue, smallest first: the order uploads start in |
| | `paused` | 1 while the app has paused uploads (`set_uploads_paused`) |
| | `retry_at` | when its retry wait ends, in milliseconds since the Unix epoch; NULL with none |
| | `pending` | its pending record, read as `PendingRecord`; NULL with none |
| `coven_pins`: each row whose file this device keeps pinned (E8) | `table_name`, `key` | the row |
| | `kept` | 1 once the whole file is on this device; 0 while its pin waits for a download |
| `coven_pending`: every pending record (§19.1) | `record` | the record, read as `PendingRecord`, without the cause only this process holds: `subscribe_pending_causes()` has it (E5) |
| | `table_name`, `key` | for a file's record, a row on this device whose file it is; NULL otherwise |
| `coven_members`: every member this device's applied slots name, former ones included | `member` | the member id as canonical UUID text, read as `MemberId` |
| | `role` | `admin` or `member` |
| | `removed` | 1 for a former member |
| | `is_self` | 1 for the member using this device |
| `coven_devices`: every device a member registered | `device` | the device id's eight big-endian bytes, read as `DeviceId` |
| | `member` | its member, as in `coven_members` |
| | `name` | its name from its registration |
| | `state` | `active`, `removed` or `replaced` |
| | `is_self` | 1 for this installation |

```rust
/// Reads the `pending` and `record` columns of coven's views.
impl types::FromSql for PendingRecord;
```

Example:

```rust
// The albums still uploading come first, one live query over the app's
// tables and coven's queue.
let albums = handle.subscribe(|sql| {
    Ok(sql.query(
        "SELECT a.title FROM albums AS a
         ORDER BY EXISTS(SELECT 1 FROM photos AS p JOIN coven_uploads AS u
                         ON u.table_name='photos' AND u.key=coven_key(p.id)
                         WHERE p.album_id=a.id) DESC, a.title",
        [],
        |row| row.get::<_, String>(0),
    )?)
});
```

```rust
// The photos that can't upload, each with its error: the records from the
// view, their causes from this process.
let mut failing = handle.subscribe(|sql| {
    Ok(sql.query(
        "SELECT e.record, p.title FROM coven_pending AS e
         JOIN photos AS p ON e.table_name='photos' AND e.key=coven_key(p.id)",
        [],
        |row| Ok((row.get::<_, PendingRecord>(0)?, row.get::<_, String>(1)?)),
    )?)
});
let mut causes = handle.subscribe_pending_causes();
let mut rows = Vec::new();
loop {
    tokio::select! {
        Some(result) = failing.next() => rows = result?,
        Ok(()) = causes.changed() => {}
        else => break,
    }
    let held = causes.borrow_and_update();
    show_failures(rows.iter().map(|(record, title)| (title, held.cause(record))));
}
```

```rust
/// SQL access to one read-only database snapshot (§4, E4).
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

/// An open file with checked identity, header and range-reading state (§17.5).
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

    /// Every lost value and removed row, as `_coven_lost` holds them (§8.2, §8.4).
    /// References show what was written, even if their parents were deleted.
    pub async fn lost_values(&self) -> CovenResult<Vec<LostValue>>;

    /// Dismisses lost values the app has dealt with, in a write, so every
    /// device drops them from `_coven_lost`; a removed row is deleted for
    /// good, and never comes back (§8.4). Deleting an owned removed row requires
    /// its owner's permission (§9.6); dismissing a frozen record changes no row.
    pub async fn dismiss_lost_values(&self, values: &[LostValue]) -> CovenResult<()>;

    /// The same, as a live query.
    pub fn subscribe_lost_values(&self) -> LiveQuery<Vec<LostValue>>;
}

impl SqlReadContext<'_> {
    /// Ordinary SQL reads. A read context can't write.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>;
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>;
}

/// The reads both `SqlReadContext` and `SqlContext` offer, so code that reads
/// the same way inside a write and outside one is written once. Inside a
/// write it sees the write's own changes.
pub trait SqlRead {
    fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>;
    fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>;
}
impl SqlRead for SqlReadContext<'_> {}
impl SqlRead for SqlContext<'_, '_> {}

impl<'a, F, R> Read<'a, F>
where
    F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
    R: Send + 'static,
{
    /// Runs `process` on the read's result on a separate worker, after the
    /// read has released its connection. The closure and result cross workers,
    /// so both must be Send and own any borrowed data.
    pub async fn process<P, T>(self, process: P) -> CovenResult<T>
    where
        P: FnOnce(R) -> CovenResult<T> + Send + 'static,
        T: Send + 'static;
}

impl<T: Send + 'static> LiveQuery<T> {
    /// The first result at once; after that, waits for a write that changes
    /// the result and returns the new one. An error is a result too, and
    /// doesn't end the query, except the one saying the store closed or the
    /// handle fails every call (`RollbackFailed`, `PendingPersistence`):
    /// after it, `None`, so a loop over the query ends.
    pub async fn next(&mut self) -> Option<CovenResult<T>>
    where
        T: Clone + PartialEq;

    /// Runs `process` on each result after the connection is released.
    pub fn process<P, U>(self, process: P) -> LiveQuery<U>
    where
        T: Clone + PartialEq,
        P: Fn(T) -> CovenResult<U> + Send + Sync + 'static,
        U: Send + 'static;

    /// Also runs again whenever `local` changes: state the app keeps beside
    /// the database, which its `process` step reads. Such a run is
    /// delivered only when its result changed.
    pub fn rerun_on(self, local: tokio::sync::watch::Receiver<()>) -> Self;
}

impl<Q, T> ReconfigurableLiveQuery<Q, T>
where
    Q: Clone + PartialEq + Send + Sync + 'static,
    T: Send + 'static,
{
    /// Process each successful value on a worker after releasing its connection.
    /// Request, revision and cause stay on the event; errors remain results.
    /// Equality compares processed values. Existing request handles still work.
    pub fn process<P, U>(self, process: P) -> ReconfigurableLiveQuery<Q, U>
    where
        T: Clone + PartialEq,
        P: Fn(T) -> CovenResult<U> + Send + Sync + 'static,
        U: Send + 'static;

    /// Also runs again whenever `local` changes, as `LiveQuery::rerun_on`.
    pub fn rerun_on(self, local: tokio::sync::watch::Receiver<()>) -> Self;

    /// A handle that replaces the request.
    pub fn requests(&self) -> LiveQueryRequests<Q>;

    /// The next result, with the request and revision that produced it, and
    /// whether a new request, a write, or both caused it; `None` after the
    /// result saying the store closed or the handle failed, as for
    /// `LiveQuery::next`.
    pub async fn next(&mut self) -> Option<ReconfigurableLiveQueryEvent<Q, T>>
    where
        T: Clone + PartialEq;
}

impl<Q: Clone + PartialEq> LiveQueryRequests<Q> {
    /// Replaces the request, returning the revision whose result will answer
    /// it. Fails once the query is dropped.
    pub fn set(&self, request: Q) -> Result<LiveQueryRevision, LiveQueryClosed>;
}

/// One `_coven_lost` row (Appendix B.3).
pub struct LostValue {
    pub table: String,
    pub key: RowKey,
    /// One cell's value, or a whole removed row's values.
    pub lost: Lost,
    /// What replaced it: a write that hadn't read it, removal rules, a
    /// breaking change, dropped data sources, or failed row authorization.
    pub replaced_by: Replacement,
    /// Old-shape values kept independently of the current schema (§18.7).
    /// Their table or columns may no longer exist; dismissal still works.
    pub frozen: bool,

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
    /// Removal rules took the row out: every one that holds (§9.1, §9.2,
    /// §9.3, §15).
    Rules(Vec<RemovalRule>),
    /// A breaking change excluded the write or deleted a hidden row through
    /// its generation. Frozen values retain their original setters (§18.7).
    SchemaChange { version: u32 },
    /// This original row change used dropped data (§9.5, §14.1). Sources name
    /// generation/setter changes, additional direct-execution sources, then
    /// verified foreign-key causes (§9.6),
    /// in authored role/execution order, with repeats. Row identity distinguishes
    /// several causes in the same write. Empty deletes retain their write id.
    DataDependency { write: WriteId, sources: Vec<ChangeSource> },
    /// A member's row change lacked permission; other changes may apply (§9.6).
    NotAuthorized { write: WriteId, reason: RowAuthorizationFailure },
}

/// An original row change or direct execution target used as data (§9.5, §9.6).
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
    /// The circle is deleted by this entry (§15.9). This describes
    /// the deletion, not a conflicting row edit; deletion is final.
    DeletedCircle { entry: Slot },
    /// The same key is present in another audience, whose row is shown
    /// (§15.4).
    OtherAudience,
    Unique { terms: Vec<String>, partial: Option<String> },
}

/// A handle that only reads, opened with `open_read_only`.
impl CovenReadHandle {
    /// The current name from this device's committed state (E9).
    pub async fn store_name(&self) -> CovenResult<String>;

    /// The SQLite settings this handle's connections have in effect (E1).
    pub async fn sqlite_settings(&self) -> Result<SqliteSettingsInEffect, DbError>;

    /// Closes read and cache connections on all clones and releases their store lock.
    /// Reports connection-close failures; cancellation does not stop closing.
    pub async fn close(&self) -> Result<(), DbError>;

    pub fn read<F, R>(&self, read: F) -> Read<'_, F>
    where
        F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static;
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
while let Some(Ok(rows)) = notes.next().await {
    show_notes(rows);
}

let mut lost = handle.subscribe_lost_values();
while let Some(Ok(values)) = lost.next().await {
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
  - Creating claims slot 1, then the creator’s key-copy slot (§10.3). Waiting
    writes then claim a writes slot through ordinary sync.
- The app calls `authenticate(access)` on the handle before OAuth setup.
  Coven owns the request, checked redirect and code exchange. The builder's
  presenter opens the page and returns the redirect or cancellation; it receives
  no tokens. Sign-in is held in coven's session custody until setup succeeds.
  Replacing a sign-in does not change an existing storage connection; setup
  verifies the chosen account and location before attaching it.
- Every setup, including reconnection, reaches the candidate provider with the
  newly supplied credentials and checks it before preparing or claiming a
  store entry or committing local state. A fresh sealed test object must be
  created once, refuse a second create at that path, return the original bytes
  through whole and interior ranged reads, and disappear after deletion.
  Listings may lag; check the probe's metadata when it appears.
  Cleanup runs even after a lost create reply;
  a path already occupied before the check is left untouched. A failed check
  returns `StorageSetupError::ProviderCheck`, naming the operation and its
  classified cause; the error retains a cleanup failure too if both fail.
  It also checks conditional replacement for the floor: a matching revision
  succeeds and a stale revision changes nothing.
- An S3 location's base prefix is an `S3Prefix`, parsed from what the person
  typed when the app builds the location (§20.3). Slashes at its ends are
  dropped, so `/music/stores/`, `music/stores/` and `music/stores` are one
  location, and a prefix of only slashes is the bucket's root, as an empty one
  is. Two slashes in a row, a folder name with a character other than an ASCII
  letter or digit, `-` or `_`, or a folder name over 255 bytes is refused with
  an `S3PrefixError` naming the folder at fault, before any location exists.
  A recorded location decodes only the canonical spelling; a recorded `dev/` is
  `StorageFailure::Encoding`.
- An S3 location's behaviors are `S3Settings` (§20.7). Its `S3Profile` names a
  known service or carries custom settings; with no profile, the
  endpoint selects (Appendix E.3): no endpoint means AWS S3; `storage.googleapis.com`
  means GCS XML, using generation conditions and conditional copy to publish
  multipart uploads. Hosts ending in `.r2.cloudflarestorage.com` select R2,
  using conditional PUT and multipart completion and ignoring checksums
  without stated scope. The self-hosted RustFS, SeaweedFS, Ceph RGW and
  Versity Gateway profiles are selected only by name; SeaweedFS publishes
  every object by conditional copy (§20.6). A name or custom profile contradicting a
  known host, a name other than AWS or any custom profile without an
  endpoint, or plain `http` to a known host is `InvalidConfiguration`;
  other endpoints may use `http`.
- Where an S3 service's member keys reach a whole bucket (R2, and every
  custom profile, detected or chosen), any member's key reaches every store
  in the bucket, so each store has its own bucket from creation (§20.7). A
  non-empty base prefix fails validation, before any request, with
  `StorageFailure::OwnBucketRequired { reason: OwnBucketReason::Prefix }`.
  A store's first setup lists the bucket's root before detection writes and
  permits only that store's folder and the root `coven-bucket` marker. Setup
  later checks that the marker names the same store (§20.7). Any other object causes
  `OwnBucketRequired { reason: OwnBucketReason::Occupied }`, wrapped in
  `StorageSetupError::Storage`. Reconnecting, restoring and joining read that
  exact marker to verify bucket ownership (§20.7); they do not list outside
  the store's folder. AWS, GCS, RustFS, SeaweedFS, Ceph RGW and Versity Gateway
  keys can be limited to the store's prefix, so their stores share buckets and prefixes unchecked.
- An endpoint matching no known host and naming no profile has no AWS
  fallback: setup detects its settings and returns and records the location
  with them as a custom profile. Reconnecting with that same location and no
  profile reuses the recorded settings.
- `detect_s3_settings` runs the same detection for an app that offers
  advanced settings: it probes the bucket with the supplied key, writing only
  fresh objects under the store's `.coven-check` prefix and deleting them,
  also on failure. Its `S3Detection` holds the detected settings and one
  result per probe; the app may prefill and override them, and
  `S3Detection::check` tells whether an override agrees with what was observed.
- Every S3 setup runs detection and checks the effective settings, detected,
  named or custom, against its results before the provider check:
  `CreateOnce`, `MultipartCreateOnce`, `PublicationTime` and `Behavior` name the
  first failure. An override that contradicts a result is refused, never
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
- Setup returns the restore code with the committed connection. Credentials,
  keys, location and code commit once the connection is ready; failure retains
  their previous values and connection. An uncertain claim retains its
  destination and exact bytes. Confirmed contention reads forward and
  revalidates; invalidated work undoes its journaled artifacts (§6.3, §11).
- Setup takes the first device's name, as joining and restore do (§10.3, §13).
  Reconnecting an existing device keeps its registered name. Changed provider
  access is published as `Set access` before committing the new credentials.
- A started connection runs one complete sync at a time, immediately after a
  local write or `sync_now`, and every 30 seconds while idle, subject to
  connection backoff and provider cooldowns. Stopping and closing finish the
  active pass first.
  - `sync_now` never shortens a backoff. `network_changed` ends every backoff
    whose last attempt got no provider response: the connection's, which status
    shows as `Offline`, and those of coven's operations, queued uploads and
    carried pins' downloads, whose pending records say `answered: false`. Call it when
    the platform reports that the network became available or changed, and
    when the app returns to the foreground or the device wakes. It also requests
    a pass, so the app need not call both. A backoff after a provider's answer,
    such as a server error, and a provider cooldown hold: a new network changes
    neither ([§6.7](coven.md#67-waiting-and-retrying)).
  - Each backoff ends durably, keeping its failure count: reopening after the
    report arms none. Reports during a pass coalesce into one more pass, and
    reports during transfers are acted on after them. If the running pass or
    transfer fails for want of a network, its wait ends too: the failure may
    have come before the change. Reports while the connection is stopped apply
    when it starts.
  - `retry_now` is the person asking to retry everything now, such as by
    pulling to refresh. `sync_now` leaves every backoff in place; `retry_now`
    ends every ordinary one, answered or not: the connection's, operations',
    uploads' and downloads'. It requests a pass, travels and coalesces
    as a network report does, and keeps each wait's failure count, so a retry
    that fails again waits the next longer delay. Provider cooldowns,
    failures that never retry, and waits for new credentials or a new
    connection hold ([§6.7](coven.md#67-waiting-and-retrying)).
  - `retry_now` returns a `RetryAnswer`, a future that resolves when the
    first pass after the request ends, so a pull-to-refresh spinner can stay
    until then: `Synced`, or `Failed` with the pass's failure. A request made
    while a pass runs waits for the next pass, which it runs; requests made
    together share one. A pass held by a provider cooldown answers
    `CoolingDown` with when it ends, and one held by a failure only new
    credentials, an update or a reset can clear answers `Failed` with that
    failure, both at once. Stopped sync, or a closed handle, answers
    `NotSyncing`, and stopping answers every waiting request so. The retry
    takes effect when called: dropping the future leaves it in place. The
    answer is about the pass; uploads and downloads retry on their own and
    report in `pending()`.
  - [A sync pass (§6)](coven.md#6-a-sync-pass) gives the read, apply, claim and effects order.
    An idle pass reads the next slot and the floor: two requests.
  - A missing slot below the floor reloads its snapshots. Returning devices retain eligible queued
    writes ([§16.5](coven.md#165-coming-back-after-a-long-absence)).
  - `Blocked` names a slot that cannot apply and its cause.
    [§5.4's stuck-device rule](coven.md#54-a-stuck-device-stops) applies;
    local reads and eligible app writes continue.
  - File transfers run outside the pass. Their failures appear in `pending()`
    without blocking sequence application.
  - `Synced` means the pass reached its read end. Files, provider effects or
    retention may still be pending, but an unapplied discovered slot prevents this status.
  - Each pending record names this device's first unmet condition for that subject. Cut-off writes
    record `WritesRefused`; [§19.1](coven.md#191-what-the-app-sees) specifies retries.

- `Offline` describes the last failed attempt, not the connection's history.
  `Failed`: storage refused the whole pass, or coven's own operation failed;
  one that never clears stops sync until a reset.
  - Keep whether the provider answered; `StorageFailure::Network` alone
    cannot decide this. A server error response is not an offline device.
  - A denied required read can fail the pass. A slot’s unavailable key, schema
    or damaged bytes gives `Blocked`. A local failure applying it, such as a
    locked keychain, gives `Failed` and retries under backoff (§19.1). A failed
    file or retention wait stays with its subject.
  - Removal records a `Connection` block with `Removed`, and stops the loop
    for good. Another copy using this device's id fails the pass with
    `DeviceIdInUse` and stops the loop, recording nothing (§19.3). Local
    failures also name their pending subject and preserve their typed cause
    for a waiting caller.
  - If the database cannot save a pending record, stop the loop and fail database
    calls and live queries with that cause, as `DbError::PendingPersistence`,
    until the store is reopened.
    Never expose an apparently empty pending list after losing its update.
- E.g. Ana syncs successfully, then loses Wi-Fi. The next attempt is
  `Offline`. A later provider error denying the required read is `Failed`.
  One bad photo while the rest syncs leaves `Synced` with a pending file record.
- A device that isn't connected still reads and writes
  ([§2.1](coven.md#21-local-writes)); its writes wait in `_coven_uploads`.
- `start_sync` starts the loop; its first pass builds the provider client from
  custody credentials, failing and backing off like any pass. Tokens refresh
  on provider rejection (§20.2).
- `stop_sync` returns immediately, releasing providers after the active pass
  and transfers. Keys, credentials and location remain. Status reports completion
  or `Failed` with its cause; no pending connection record is written.
- `disconnect_storage` also removes this device's storage credentials. It leaves
  storage's contents untouched; syncing requires storage setup again.

Storage failures retain the same `StorageFailure` through setup, sync status,
uploads, reads and operations. Their `StorageError` carries the native cause
where one exists; setup adds the failed check or its distinct local failure
without reclassifying storage failures. Missing member keys are always
`StorageFailure::MemberKeysMissing`. Pending records retain this same
classification, and whether the provider answered, when the native cause cannot
survive closing the app.

Choosing a folder uses the handle's pending `authenticate` sign-in, so the app never
needs tokens. `onedrive_root` starts at the person's default drive; `onedrive_folders`
reads its immediate child folders across every page. Shared folders use the target
drive and item ids. `create_onedrive_folder` returns
the newly created folder, or the provider's error if its name is occupied. Dropbox
selection requires Full Dropbox access and returns shared-folder namespace ids.

The returned `storage` is passed unchanged to `setup_oauth_storage`; selecting or
creating a folder does not commit settings or credentials. These requests use the
same token refresh and provider cooldown as setup. A failed page fails the listing.

Setup replaces `account_label` with the signed-in member's label before committing
the location and restore code, so personal and work accounts can be told apart.
Dropbox uses the account email; OneDrive uses mail when present, otherwise the
principal name. Joining and restoring record the member's own label, replacing
any label in the code. These labels never select a folder, grant access or change
an upload's destination. S3 and CloudKit have no account label.

```rust
/// A provider's current folder name and the location storage setup accepts.
pub struct StorageFolder {
    pub name: String,
    pub storage: StorageConfig,
}

impl CovenHandle {
    /// Read the signed-in person's default OneDrive root and its actual item id.
    pub async fn onedrive_root(&self) -> Result<StorageFolder, StorageSetupError>;

    /// Read all immediate child folders, without reading file contents.
    pub async fn onedrive_folders(
        &self, parent: &StorageConfig,
    ) -> Result<Vec<StorageFolder>, StorageSetupError>;

    /// Create this exact name; an occupied name is never renamed or replaced.
    pub async fn create_onedrive_folder(
        &self, parent: &StorageConfig, name: &str,
    ) -> Result<StorageFolder, StorageSetupError>;

    /// Shared namespaces accessible through the pending Full Dropbox sign-in.
    pub async fn dropbox_shared_folders(&self) -> Result<Vec<StorageFolder>, StorageSetupError>;
}
```

```rust
/// The provider holding a store (§20.2).
pub enum CloudProvider {
    /// S3, including compatible providers.
    S3,
    /// Dropbox.
    Dropbox,
    /// OneDrive.
    OneDrive,
    /// iCloud through coven-storage's native Apple-platform adapter.
    CloudKit,
}

impl CloudProvider {
    /// Instructions for cutting off a removed device's provider access (§14.2).
    /// This depends on the provider kind, not on a connected account or request.
    /// None on S3, where a member's devices share one key.
    pub fn sign_out(self) -> Option<ProviderSignOut>;
}

/// A store's location, with credentials kept separately (§20.3, E5).
pub enum StorageConfig {
    /// An existing S3 bucket and a base prefix. Where the service's member
    /// keys can be limited to a prefix, the base prefix may hold several
    /// stores; where they reach a whole bucket (R2, and every custom or
    /// detected profile), the store has the bucket to itself (§20.7).
    S3 {
        /// The bucket's name.
        bucket: String,
        /// The signing region.
        region: String,
        /// None is AWS's own endpoint. Known hosts require https (Appendix E.3).
        endpoint: Option<Url>,
        /// A known service's name or custom settings; either must agree
        /// with a known endpoint host. None selects by host:
        /// storage.googleapis.com is GCS XML, hosts ending in
        /// .r2.cloudflarestorage.com are R2, no endpoint or .amazonaws.com is
        /// AWS, and setup detects any other endpoint's settings, recording
        /// them as Custom. All run the setup checks.
        profile: Option<S3Profile>,
        /// The base prefix, parsed where the location is built; paths add
        /// the store id. Empty where member keys reach a whole bucket;
        /// otherwise `OwnBucketRequired { reason: Prefix }`.
        prefix: S3Prefix,
    },
    /// Required creation choice: an App folder or a shared-capable namespace.
    Dropbox {
        location: DropboxLocation,
        /// Display only; None before setup. Coven reads this member's account at setup.
        account_label: Option<String>,
    },
    /// A OneDrive folder; paths within it start with the store id.
    OneDrive {
        drive_id: String,
        folder_id: String,
        /// Display only; None before setup. Coven reads this member's account at setup.
        account_label: Option<String>,
    },
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

/// An S3 location's base prefix in its one spelling: folder names joined by
/// single slashes, with no slash at either end. The default is the bucket's
/// root. Serialized as that string; decoding refuses any other spelling.
pub struct S3Prefix { /* private fields */ }

impl S3Prefix {
    /// Parse a prefix as a person typed it, dropping slashes at either end;
    /// only slashes is the bucket's root (§20.3).
    pub fn parse(typed: &str) -> Result<Self, S3PrefixError>;
    /// The canonical spelling.
    pub fn as_str(&self) -> &str;
    /// Whether this is the bucket's root.
    pub fn is_empty(&self) -> bool;
    /// What every key under this prefix starts with: empty at the bucket's
    /// root, otherwise the prefix and a slash.
    pub fn key_prefix(&self) -> String;
    /// The S3 key of an object in the layout.
    pub fn key(&self, path: &ObjectPath) -> String;
    /// The S3 key prefix that selects these objects.
    pub fn key_prefix_of(&self, objects: &ObjectPrefix) -> String;
}

/// Why a typed base prefix was refused (§20.3).
pub enum S3PrefixError {
    /// Two slashes in a row, as in `music//bae`, leave a folder with no name.
    EmptySegment,
    /// A folder name holds a character other than an ASCII letter or digit,
    /// `-` or `_`: the folder as typed, and its first such character.
    InvalidCharacter { segment: String, character: char },
    /// A folder name is longer than 255 bytes.
    SegmentTooLong { segment: String },
}

/// An S3 location's behaviors: a known service's, or its own (§20.7).
pub enum S3Profile {
    /// The known service's settings.
    Named(S3ProfileName),
    /// Settings detected at setup or chosen by the app; restore codes carry them.
    Custom(S3Settings),
}

/// Every S3 behavior the adapter varies, one value each (§20.7).
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

/// What detection observed at one location (§20.7).
pub struct S3Detection {
    /// Bucket versioning, or a refused/unimplemented read with its native cause.
    pub versioning: Result<S3BucketVersioning, Arc<StorageError>>,
    /// Settings derived from the results; prefill advanced settings with them.
    pub settings: S3Settings,
    /// One result per probe, in the order run.
    pub results: Vec<S3ProbeResult>,
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
    /// Refuses settings that contradict the results, naming the first
    /// failed check as setup would: CreateOnce, MultipartCreateOnce,
    /// PublicationTime, then Behavior.
    pub fn check(&self, settings: &S3Settings) -> Result<(), StorageSetupError>;
    /// The location setup records, after checking versioning support and its effective settings:
    /// an unrecognized endpoint naming no profile gets these settings as its
    /// custom profile; any other location is kept as given.
    pub fn resolve(&self, location: StorageConfig) -> Result<StorageConfig, StorageSetupError>;
}

pub struct S3ProbeResult {
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

/// An S3 service whose known profile a location names (§20.7).
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

/// Dropbox's location fixes its access level at cloud-store creation (§20.2).
pub enum DropboxLocation {
    /// Not-shared: paths are relative to the registered app's folder.
    AppFolder,
    /// Shared-capable: an existing shared namespace, independent of mount paths.
    SharedFolder { namespace_id: String },
}

impl DropboxLocation {
    /// The app registration required to reach this location.
    pub fn access(&self) -> DropboxAccess;
}

impl StorageConfig {
    /// Compare provider location and settings, ignoring display-only account labels.
    pub fn same_location(&self, other: &Self) -> bool;

    /// The provider of this location.
    pub fn provider(&self) -> CloudProvider;
    /// The sign-in this location requires (E10).
    pub fn oauth_access(&self) -> Option<OAuthAccess>;
    /// Refuses missing or invalid location information before making a request.
    pub fn validate(&self) -> Result<(), StorageError>;

    /// Encode the validated location, without any storage credentials.
    pub fn encode(&self) -> Result<SecretBytes, StorageError>;

    /// Decode only a provider location; unknown fields and credential payloads fail.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError>;
}

/// Limits for concurrent transfers (E1, E5).
pub struct TransferLimits {
    /// File uploads running at once. While uploads can run, the upload
    /// worker keeps this many in flight, starting the next as each ends.
    pub uploads: NonZeroUsize,
    /// Maximum concurrent downloads, shared by sequence fetch-ahead and file reads.
    pub downloads: NonZeroUsize,
}

/// The single storage failure classification used by every app-facing error (Appendix F.5).
pub enum StorageFailure {
    /// This platform cannot use the selected built-in provider.
    ProviderUnavailable,
    /// The app did not register the access needed by this operation.
    AccessNotRegistered { required: OAuthAccess },
    /// The sign-in lacks the access needed by this operation.
    AccessNotGranted { required: OAuthAccess },
    /// Sharing an App-folder store requires relocation, outside this operation.
    DropboxAppFolderNotShareable,
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
    /// Invalidated multipart publication reports SessionExpired instead (Appendix E.10).
    Conflict,
    /// The bucket, folder or zone is absent.
    ContainerNotFound,
    /// The S3 endpoint or signing region is wrong.
    RegionMismatch,
    /// The S3 service's member keys reach a whole bucket, so a store there
    /// needs a bucket of its own (§20.7).
    OwnBucketRequired { reason: OwnBucketReason },
    /// Setting up a store that never had storage found another device's
    /// slot 1: another installation claimed this location first (§10.3, D10).
    /// Restore or join it instead.
    StoreExists,
    /// S3 rejected the device clock after one retry with the learned offset (§20.7).
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
    /// An object path is outside the store's layout.
    InvalidPath,
    /// A range is empty, reversed or beyond the object's end.
    InvalidRange,
    /// Only the account holding the store may change its sharing.
    NotStoreOwner,
    /// The invited account is the one holding the store (§13.2).
    InviteeIsStoreOwner,
    /// Dropbox cannot upgrade a pending viewer without the recipient's account id.
    AccountIdUnavailable,
    /// The invite belongs to another provider location.
    InvitationMismatch,
    /// The session belongs to another provider or location.
    SessionMismatch,
    /// A lost multipart-initiation reply left no attributable native session id.
    /// Retain its cleanup journal; never abort another claimant’s session.
    SessionIdentityUnknown,
    /// The recorded upload expired or a publication conflict invalidated it.
    SessionExpired,
    /// A part disagrees with the session's offset, size or alignment.
    InvalidPart,
    /// The request exceeds this provider's single-request limit.
    SingleRequestTooLarge {
        /// Encrypted body length in bytes.
        size: u64,
        /// The adapter's single-request limit in bytes.
        limit: u64,
    },
    /// An S3 object published by `CopyObject` exceeds its single-copy limit,
    /// so it cannot be published atomically on this profile (§20.6).
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

    /// The installation must complete membership and key-copy catch-up before sending.
    CatchUpRequired,
}

/// Why an S3 location cannot hold a store that needs its own bucket (§20.7).
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

/// A storage error preserves its classification and typed cause (§20, E5).
pub enum StorageError {
    /// A failure with no underlying native error; also constructed by `failure.into()`.
    Failure(StorageFailure),
    /// A failure with its diagnostic or original local cause.
    Caused { failure: StorageFailure, source: Box<dyn std::error::Error + Send + Sync> },
    /// A provider failure with its original transport, SDK or framework cause.
    /// Its text names that cause safely. An answered request shows its HTTP
    /// status and the error codes the reply states from the provider's fixed
    /// vocabulary, such as S3's `AccessDenied`, Dropbox's `path/not_found`,
    /// Graph's `itemNotFound` or an OAuth `invalid_grant`; a CloudKit failure
    /// shows the domain and code of its NSError, of the errors beneath it and
    /// of each partial-failure item. It never shows a reply's message or
    /// description, which can echo credentials or names. A request to any
    /// provider that got no reply shows every layer of its transport, TLS or
    /// DNS failure, such as "client error (Connect): dns error: …" below
    /// "error sending request", with each URL cut to its scheme and host, so a
    /// signed query, a token or an object path never prints.
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

    /// Whether the provider answered the request behind this failure, even
    /// with an error (E5). `Network` alone cannot say: a server's error
    /// response is an answer; a refused connection or a timeout is not.
    pub fn answered(&self) -> bool;

    /// Whether the provider answered this request with a refusal, so it
    /// stored nothing. A timeout, or a lost, malformed or server-failure
    /// reply, leaves the outcome unknown. The clients resend a request only
    /// after a refusal (a rejected token, a skewed clock), so no earlier
    /// attempt of it stored anything either.
    pub fn refused(&self) -> bool;
}

/// An HTTP provider's original response, retained as a StorageError cause.
/// Display and Debug show the status and the body's fixed-vocabulary error
/// codes (an OAuth `error` and Microsoft's `error_codes`, Dropbox's tags,
/// Graph's `code` and inner codes, Google's `status` and `reason`s,
/// Cloudflare's numeric codes), never the body's other text or the headers,
/// because providers can echo credentials. This also retains Dropbox's asynchronous job failures
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
    /// This device or its member was removed from the store (§14.1).
    Removed,
    /// The store needs a newer app schema or coven format (§18).
    UpdateRequired(RequiredUpdate),
    /// Local setup failed, with its cause.
    Internal(Box<dyn std::error::Error + Send + Sync>),
}

/// The provider operation checked before setup commits anything (E5).
pub enum StorageCheck {
    /// Create a fresh sealed object.
    Create,
    /// Refuse a second create at the same path.
    CreateOnce,
    /// Replace at a matching revision and refuse a stale revision (§16.6).
    ConditionalReplace,
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

/// Opening the member's sealed store key failed (§12.1, E5).
pub enum StoreKeyUnlockError {
    /// No storage is connected.
    NoStorage,
    /// Reading the sealed key failed.
    Storage(StorageError),
    /// The sealed key could not be opened or checked.
    Crypto(CryptoError),
    /// Unlocking or keeping keys in custody failed.
    SecureStorage(KeyError),
    /// This device or its member was removed from the store (§14.1).
    Removed,
    /// The store needs a newer app schema or coven format (§18).
    UpdateRequired(RequiredUpdate),

    /// Sequence validation or local work failed, preserving its cause.
    Other(Box<dyn std::error::Error + Send + Sync>),
}

/// Sync or an administrative operation failed (§10, §14, §18, E5).
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
    /// The latest applied prefix no longer permits the operation; its journal
    /// undoes owned artifacts before completion (§11).
    Rejected(OperationRejection),
    /// The sequence has no next slot.
    SequenceExhausted,
    /// An object required by the change failed its checks.
    Damaged(DamagedObject),
    /// This device or its member was removed; syncing stops for good (§14.1).
    Removed,
    /// Another copy uses this device's id; this one sends nothing more.
    /// Nothing is recorded; `reset_device` is the app's option (§19.3).
    DeviceIdInUse(DeviceIdEvidence),
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
    /// removed (§20.9, §14.2).
    StoreOwner,
    /// This call needs a newer app schema or coven format (§18).
    UpdateRequired(RequiredUpdate),
    /// A create-store entry names another store (§10.3).
    WrongStore { expected: StoreId, actual: StoreId },
    /// The requested journal row has no retryable work for this call.
    NotPending(OperationId),

    /// Decoding persisted operation data failed, retaining its cause.
    OperationData(serde_json::Error),

    /// Administrative and snapshot publication wait for the retained reload (§11).
    ReloadPending(OperationId),
    /// Deleting stored files waits for this unfinished snapshot, whose
    /// journal still owns references no row holds (§17.8).
    SnapshotPending(OperationId),
}

impl SyncError {
    /// The durable reason for a failed request, without losing its typed cause here.
    pub fn pending_reason(&self, provider: coven_storage::CloudProvider) -> PendingReason;

    /// The local category and one shared cause, for a status and a waiting
    /// caller failed by the same error.
    pub fn into_shared(
        self,
    ) -> (
        LocalFailure,
        Arc<dyn std::error::Error + Send + Sync>,
    );
}

/// What showed that another copy uses this device's id (§19.3).
pub enum DeviceIdEvidence {
    /// A slot signed by this device has a path its journal never attempted.
    UnattemptedSlot { slot: Slot },
    /// An earlier slot replaced this device's id.
    Replaced,
}

/// An object that failed a check when read (§19.1).
pub struct DamagedObject {
    /// The object's path in storage.
    pub path: String,
    /// Which check failed, retaining its cause.
    pub failure: Refusal,
}

/// One audience snapshot, identified by its storage path (§16).
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
    /// The error behind `reason`, with every cause below it, while this
    /// process holds it: an upload, download or read no caller awaited, or
    /// an attempt of an operation coven drives itself. Never persisted, so a
    /// record read after a restart has none, and none when the failure went
    /// back to an app call. Equality ignores it. A record's text names its
    /// subject, its reason and this error.
    pub cause: Option<Arc<dyn std::error::Error + Send + Sync>>,
}

/// The errors this process holds behind pending records (§19.1).
pub struct PendingCauses { /* private fields */ }

impl PendingCauses {
    /// The error behind `record`, with every cause below it, while this
    /// process holds it for exactly the reason the record states.
    pub fn cause(&self, record: &PendingRecord) -> Option<Arc<dyn std::error::Error + Send + Sync>>;

    /// `records`, each with its cause as `cause` finds it.
    pub fn attach(&self, records: Vec<PendingRecord>) -> Vec<PendingRecord>;
}

/// Stable identities; this device keeps one record per subject.
pub enum PendingSubject {
    Write(WriteId),
    Slot(Slot),
    KeyCopy { audience: Audience, key: KeyId, member: MemberId, sealing: SealingKey },

    Snapshot(SnapshotId),
    File { device: DeviceId, file: FileId },
    Operation { id: OperationId, kind: OperationKind },
    Retention { path: ObjectPath },
    Audience(Audience),
    JoinRequest(InviteId),
    Connection,
}

/// A condition coven has not yet observed (§19.1).
pub enum Prerequisite {
    /// A completed read and apply through the current sequence end (§11.1).
    SequenceCatchUp,

    Object(ObjectPath),
    DeviceRegistration(DeviceId),
    SchemaRaise { audience: Audience, version: u32 },
    Reload(Audience),
    /// A snapshot needed to load history below the floor (§16.4).
    Snapshot(SnapshotId),

}

/// One vocabulary for failed object checks in calls and pending records.
/// Every check that produces a native error retains it as an optional cause,
/// using its narrowest native type. Variants whose checks never produce a
/// native error stay unit; direct verdicts on other variants use cause: None.
/// Immediate calls retain native causes. Local pending records preserve
/// the variant without the process-local error object.
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

/// A source this device cannot deliver; only its local app receives this failure.
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
    /// `answered`: whether the provider answered the failed request, even
    /// with an error. A `Network` failure nobody answered is `Offline` (E5).
    Storage { provider: CloudProvider, failure: StorageFailure, answered: bool },
    Refused(Refusal),
    /// Consumed without effects; a permanent lower-slot cut-off (§14.1).
    WritesRefused { cutoff: Slot, cause: WriteCutoff },
    ProviderPending { action: ProviderAccessAction, account: String },
    PendingOwner { account: String },
    AccountInUse { account: String },
    AccessRemains { account: String, shares: Vec<RetainedAccess> },
    FileUnavailable(FileMissingReason),
    Failed(LocalFailure),
    Rejected(OperationRejection),
    /// Both causes stay visible; the cleanup cause determines retry (§11.3).
    Cleanup { operation: Box<PendingReason>, cleanup: Box<PendingReason> },
    Paused,
    Removed,
    /// Sending needs this stable member's current credentials (§14.1).
    MemberKeysReplaced { member: MemberId },
}

pub enum Retry {
    Automatic,
    AfterUpdate,
    AppAction,
    Never,
}

impl PendingRecord {
    /// The record of `subject`'s first unmet condition, with no cause held.
    pub fn new(subject: PendingSubject, reason: PendingReason) -> Self;

    /// Automatic: retryable reads, keys, snapshots, provider work and
    /// unfinished operation/undo work, subject to shared backoff.
    /// Storage Network/Conflict/RateLimited/NotFound/SessionExpired retry
    /// automatically. So do local database, disk or custody failures, such
    /// as a locked keychain, except in an operation the app started: its call
    /// returns them and the app calls again (§19.1). Files and coven-owned
    /// operations also retry storage QuotaExceeded/File.
    /// AfterUpdate: refused immutable objects and unsupported formats/schema.
    /// Never: WritesRefused or this device's removal/replacement.
    /// AppAction: failures a person clears, such as required credentials;
    /// retried on new credentials or connection.
    /// Other permanent file or coven-owned operation failures are Never.
    /// An uploader's missing, changed or mismatching source retries automatically.
    /// A later slot cannot reverse a cut-off or bypass a blocked slot.
    pub fn retry(&self) -> Retry;
}

/// The absent target named by a rejected administrative change (§10).
pub enum EntryTarget {
    Member(MemberId),
    Device(DeviceId),
    Circle(CircleId),
}

/// Why validation refused an administrative operation (§10, §11).
pub enum OperationRejection {
    TargetGone(EntryTarget),
    NoAdminLeft,
    KeysAlreadyUsed,
    /// The expected member binding changed while this call was preparing.
    MemberKeysChanged(MemberId),
    NotAllowed,

    /// The provider account owns the store and cannot be removed.
    StoreOwner,
}

/// The lower-slot event that permanently excluded a writes slot (§14.1).
pub enum WriteCutoff {
    DeviceRemoved,
    MemberRemoved,
    MemberKeysReplaced,
}

impl CovenHandle {
    /// Checks S3 and returns its committed location, key state and restore code.
    /// The location includes any detected settings (§20.7).
    pub async fn setup_s3_storage(
        &self,
        storage: StorageConfig,
        device_name: &str,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<StorageSetup, StorageSetupError>;

    /// Probes the S3 service at this location with this member's key (§20.7),
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

    /// Sets up Dropbox or OneDrive using coven's held sign-in.
    /// Without a new sign-in, reconnects using this store's custody credentials.
    /// A provider 401 refreshes once and retries once. Never opens sign-in UI.
    pub async fn setup_oauth_storage(
        &self,
        storage: StorageConfig,
        device_name: &str,
    ) -> Result<StorageSetup, StorageSetupError>;

    /// Sets up iCloud through the built-in adapter; other platforms return ProviderUnavailable.
    /// Coven obtains the current owner and creates the store's custom zone.
    pub async fn setup_cloudkit_storage(
        &self,
        container: &str,
        device_name: &str,
    ) -> Result<StorageSetup, StorageSetupError>;

    /// Opens store keys from their copies sealed to this member in storage
    /// (§12.1), keeps them in custody, and connects without starting sync.
    /// Uses the member-key session already held at open; it does not unlock
    /// custody again. Key selection follows §12.2; other waits remain in pending().
    pub async fn unlock_store_key(&self) -> Result<ConnectedStorage, StoreKeyUnlockError>;

    /// Whether the held session has the store key: `Available` or `Locked`.
    /// Reads no custody and derives no passphrase key.
    pub fn store_key_state(&self) -> Result<StoreKeyState, KeyError>;

    /// Finishes the active pass, removes this device's storage credentials and
    /// drops the provider client. If removing credentials fails, the connection
    /// and loop state stay. Storage's contents are untouched.
    pub async fn disconnect_storage(&self) -> Result<(), SyncError>;

    /// Starts syncing with the keys unlocked for this session; the first pass
    /// connects. Does nothing if already running or no storage is set up.
    pub async fn start_sync(&self) -> Result<(), SyncError>;

    /// Queues stopping after the active pass and transfers, retaining keys,
    /// credentials and location. Completion publishes Stopped or Disconnected;
    /// release failures publish Failed with their cause, without a pending record.
    pub fn stop_sync(&self);

    /// Syncs now instead of at the next idle tick. While idle, coven syncs
    /// every 30 seconds, and at once after a local write. Requests coalesce
    /// while a pass or connection backoff is active; provider cooldowns hold.
    pub fn sync_now(&self);

    /// The device's network changed: it became available, or moved to
    /// another network. Ends every backoff whose last attempt got no provider
    /// response: the connection's (`Offline`), operations', uploads' and
    /// downloads'. Syncs now, as `sync_now` does. Backoffs after a
    /// provider's answer, and provider cooldowns, hold. Reports coalesce as
    /// `sync_now` requests do (§6.7).
    pub fn network_changed(&self);

    /// The person asked to retry everything now. Ends every ordinary
    /// backoff, answered or not: the connection's, operations', uploads'
    /// and downloads'; each keeps its failure count. Syncs now, as
    /// `sync_now` does. Provider cooldowns and failures that never retry
    /// hold. Requests coalesce with network reports (§6.7). Resolves when
    /// the first pass after the request ends; a pass held by a cooldown or
    /// by a failure only the person can clear answers at once.
    pub fn retry_now(&self) -> RetryAnswer;

    /// The sync status, live. The first value is the current status.
    pub fn subscribe_sync_status(&self) -> watch::Receiver<SyncStatus>;

    /// The latest completed replacement stored in this installation, or None.
    /// It survives reopening; unsent edits were discarded, or may be lost
    /// for a damaged database (§19.3).
    pub async fn device_reset(&self) -> CovenResult<Option<DeviceReset>>;

    /// The same pre-built query, including a replacement completed during open.
    pub fn subscribe_device_reset(&self) -> LiveQuery<Option<DeviceReset>>;

    /// This device's pending work, sorted by subject, including offline (§19.1).
    pub async fn pending(&self) -> CovenResult<Vec<PendingRecord>>;

    /// The same internal query as pending(), on the app's live-query
    /// mechanism. Each result carries the causes held as it is delivered;
    /// a cause that changes while its record stays comes on
    /// `subscribe_pending_causes()`.
    pub fn subscribe_pending(&self) -> LiveQuery<Vec<PendingRecord>>;

    /// The errors behind pending records while this process holds them, by
    /// record: what `coven_pending` leaves out (E4). The first value is the
    /// current one; another follows whenever the cause some record shows
    /// appears, changes or ends.
    pub fn subscribe_pending_causes(&self) -> watch::Receiver<PendingCauses>;

    /// How many uploads and downloads run at once.
    pub fn transfer_limits(&self) -> TransferLimits;

    /// Changes them while the store is open. The next upload to start sees
    /// the new upload limit; transfers already running continue.
    pub fn set_transfer_limits(&self, limits: TransferLimits);
}

/// A completed replacement of this installation, not a membership removal.
pub struct DeviceReset {
    pub old_device: DeviceId,
    pub new_device: DeviceId,
}

/// A sync step stopped as a whole (E5).
pub enum SyncFailure {
    /// A schema or stored object format is newer than this implementation.
    UpdateRequired(RequiredUpdate),
    /// This device or its member has been removed.
    Removed,
    /// Another copy uses this device's id; this copy sends nothing more (§19.3).
    DeviceIdInUse(DeviceIdEvidence),
    /// The provider refused or failed a request.
    Storage(Arc<StorageError>),
    /// The pass finished, but this work of coven's own failed its last
    /// attempt; it retries under backoff (E5). The record carries the error
    /// behind it while this process holds it, and the failure's text names
    /// the subject, the reason and that error.
    Operation(Box<PendingRecord>),
    /// Any other failure, retaining its typed cause.
    Other(Arc<dyn std::error::Error + Send + Sync>),
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
    /// The pass reached its observed end; other subjects may still be pending.
    Synced { finished_at: SystemTime },
    /// This slot invokes §5.4's stuck-device rule.
    /// `error` is the error that blocked the slot, with every cause below
    /// it, while this process holds it; a device that opens already blocked
    /// has none, since only the reason is saved. Coven logs it when the
    /// blocked slot or its reason changes.
    Blocked {
        slot: Slot,
        reason: PendingReason,
        error: Option<Arc<dyn std::error::Error + Send + Sync>>,
    },
    /// Storage refused the whole pass, or coven's own operation failed.
    Failed { error: SyncFailure },
}

/// Why a pass, or the connection attempt that begins one, failed: the
/// status it leaves.
pub enum PassFailure {
    Offline { error: Arc<StorageError> },
    Blocked {
        slot: Slot,
        reason: PendingReason,
        error: Option<Arc<dyn std::error::Error + Send + Sync>>,
    },
    Failed { error: SyncFailure },
}

/// What the first sync pass after the person's retry came to.
pub enum RetryOutcome {
    /// The pass reached its read end.
    Synced { finished_at: SystemTime },
    /// The pass failed, or a failure only new credentials, an update or a
    /// reset can clear holds every pass.
    Failed(PassFailure),
    /// A provider cooldown holds every pass until then, by the store's clock.
    /// The pass runs once it ends, with no further request.
    CoolingDown { until: SystemTime },
    /// Sync is stopped or the handle closed, so no pass follows the request.
    NotSyncing,
}

/// Resolves when the first sync pass after a retry request ends.
/// Dropping it leaves the retry in place.
pub struct RetryAnswer { /* private */ }

impl Future for RetryAnswer {
    type Output = RetryOutcome;
}

/// Storage this device has set up, with whether it holds the store key.
pub struct ConnectedStorage {
    pub storage: StorageConfig,
    pub key_state: StoreKeyState,
}

/// Setup returns the committed connection and code, including on reconnection.
pub struct StorageSetup {
    pub connection: ConnectedStorage,
    pub restore_code: SecretText,
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
let settings = edit_advanced_settings(detection.settings, &detection.results);
detection.check(&settings)?;
let StorageConfig::S3 { profile, .. } = &mut storage else { unreachable!() };
*profile = Some(S3Profile::Custom(settings));

match handle.setup_s3_storage(storage, device_name, access_key_id, SecretText::new(secret_access_key)).await {
    Ok(setup) => {
        remember(setup.connection.storage);
        show_blurred_qr_code(setup.restore_code.as_str());
    }
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

- Every administrative call follows [§11](coven.md#11-admin-operations): validate,
  journal, do prerequisite work, then claim. Contention reads forward and validates again.
- On reopening or retrying, settle the recorded path and bytes before another
  attempt ([§5.1](coven.md#51-claiming-a-slot)). Each device has one claim counter
  across writes, admin entries, key copies and announcements, and settles its
  current claim before starting another. If the floor has passed the slot,
  compare its recorded claim number c with the snapshot's last included claim L:
  L below c means absent; equal with the recorded digest means landed; above, or
  equal with another digest, means another copy of this device id;
  the resulting app or admin state is not evidence that a claim landed.
- If validation fails, undo the operation's artifacts and return `Rejected`.
  Failed undo reports both causes and keeps the journal for retry.
- A claimed entry is final. An already-satisfied request needs no extra entry.
  Neither dropping the call nor resetting data erases cleanup obligations.
- Snapshot creation, reload and retention use journals too (§16).
  Calls retain native errors; local pending records retain the subject and category.

```rust
/// The local integer primary key of one unfinished operation (§11).
pub struct OperationId(pub i64);

/// The app purpose of an unfinished operation (§11.5, §19.4).
pub enum OperationKind {
    CreateStore,
    RenameStore,
    AddDevice,
    SetRole,
    SetAccess,
    RemoveMember,
    RemoveDevice,
    RenameCircle,
    RemoveCircleMember,
    /// Conditionally advance the floor, then delete slots below it (§16.6).
    RetainSequence,
    /// Delete stored files nothing retained refers to (§17.8).
    RetainFiles,
    /// Make a circle and publish its first sealed key.
    CreateCircle,
    /// Seal the circle's history and add a member.
    AddCircleMember,
    /// Delete a circle's rows and publish its deletion.
    DeleteCircle,
    /// An owner’s device grants or takes back one account’s required access.
    RevokeAccess,
    /// Migrate the schema, snapshot it and raise the version.
    SchemaChange,
    /// Reload at the app's request.
    ReloadFromSnapshot,
    /// A reload sync requires.
    RequiredReload,
    /// Replace an audience key exposed to an excluded member or credential (§12.2).
    RotateKey,
    /// Publish a member's replacement pair and finish required custody (§12.3, E9).
    ReplaceMemberKeys,
    /// Upload a snapshot, then claim its announcement (§16.2).
    WriteSnapshot,
    /// Grant access, approve or decline a join, and settle the invite.
    Invite,
    /// Snapshot and reset an audience (§19.4).
    Reset,
}

/// Operation calls retain the same typed causes as sync calls (§11).
pub type OperationError = SyncError;

impl CovenHandle {
    /// Makes an operation’s retry or undo due now, from its recorded progress.
    /// It cannot resurrect an invalidated action or bypass provider cooldown.
    pub async fn retry_pending_operation(&self, operation: OperationId) -> Result<(), OperationError>;

    /// Reloads this device from the latest snapshot, keeping its waiting
    /// writes, as an operation (§19.2). It cannot bypass §5.4's stuck-device rule.
    pub async fn reload_from_snapshot(&self) -> Result<(), OperationError>;

    /// Resets the store from this device's copy, as an admin (§19.4): writes
    /// a snapshot, then announces it and claims the reset slot. Every device,
    /// including this one, reloads from it. Pre-reset writes outside it are
    /// ignored without loss records; queued writes retain their original pasts when claimed.
    pub async fn reset_store(&self) -> Result<(), SyncError>;

    /// Deletes removed devices' stored files that nothing refers to (§17.8):
    /// lists again the file folder of every removed device this device is
    /// responsible for, then deletes each file found that no retained row,
    /// write or snapshot names. Safe at any time; a removed device may keep
    /// uploading until its provider access ends. Returns once the listing and
    /// deletions finish, or with the failure that stopped them, which the
    /// RetainFiles operation also records. `SnapshotPending` while this
    /// device's own snapshot is still being published.
    pub async fn clean_up_removed_files(&self) -> Result<(), SyncError>;
}
```

### E7 Moves and uploads

- A row moves to another audience by an ordinary write that changes its
  root's audience column, or points a descendant at a parent in another
  audience ([§15.4](coven.md#154-moving-rows)).
  - The write commits at once on this device; its moved rows' file
    references stay fixed ([§17.2](coven.md#172-attaching-a-file)).
- Files enter the upload queue as the attaching write commits, including
  with no storage connected. That write fixes the path and key and records
  the row's pending attachment; the row's file columns stay as they were.
  Upload completion writes them as a new local write, or abandons the
  attachment if this device has seen a newer change to the row's file
  ([§17.2](coven.md#172-attaching-a-file)). A `FileRef` taken from the
  pending attachment stays valid through completion. Pinning and caching
  change no file reference.
- Upload attempts read the source again, checking its size and whole-file
  content hash before transfer, and the first-read hash of each plaintext
  chunk before encryption. A changed user original reports `UserFileChanged`;
  a changed app-provided copy reports `Integrity`. Retries retain the same
  file id, key and chunk hashes, without an encrypted local copy.

```rust
impl CovenHandle {
    /// Queue, pause setting and provider-confirmed session progress in one
    /// committed database snapshot, available while disconnected, with the
    /// files this device is sending now.
    pub async fn uploads(&self) -> CovenResult<UploadQueue>;

    /// The same pre-built query, through the app's live-query mechanism. It
    /// answers again as an upload starts or ends, too.
    pub fn subscribe_uploads(&self) -> LiveQuery<UploadQueue>;

    /// Optional early retry of recoverable uploads; coven retries on its own.
    /// It joins the uploads in progress, covers every queued file that can
    /// retry whatever its delay, and returns once each one's attempt ended.
    /// Provider cooldowns still hold; failures stay in pending() (§6.7).
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
    /// Whether this device is sending the file now. Kept in memory only:
    /// a reopened store shows every file waiting. What it waits for is in
    /// `coven_uploads` (E4).
    pub phase: UploadPhase,
    /// Bytes the provider confirmed in the file's resumable session; `None`
    /// while it has none, as a file sent in one request never does.
    pub progress: Option<UploadProgress>,
    pub queued_at: SystemTime,
}

pub enum UploadPhase {
    /// Not being sent: not started, paused, waiting for a free upload slot
    /// or for its retry delay.
    Waiting,
    /// An attempt is sending it now, in one request or in parts.
    Uploading,
}

/// Encrypted bytes confirmed by the provider and persisted in the resumable
/// session. Sending bytes alone does not advance it; a paused upload keeps it.
pub struct UploadProgress {
    pub bytes_confirmed: u64,
    pub bytes_total: u64,
}

/// What `retry_uploads_now` did.
pub enum DrainOutcome {
    /// Every file it covered was attempted; this many were stored.
    Drained { uploaded: usize },
    QueueEmpty,
    Paused,
}
```

Example:

```rust
let mut uploads = handle.subscribe_uploads();
while let Some(state) = uploads.next().await {
    let state = state?;
    for upload in &state.files {
        let sending = upload.phase == UploadPhase::Uploading;
        match upload.progress {
            Some(UploadProgress { bytes_confirmed, bytes_total }) => {
                show_progress(upload.file.key(), sending, bytes_confirmed, bytes_total)
            }
            None => show_state(upload.file.key(), sending),
        }
    }
    if state.files.is_empty() {
        break; // every queued file is stored
    }
}
```

### E8 Files and the cache

- A *file reference*, `FileRef`, names one row's file as of that row's
  current version, so a later change to the row can't redirect a read. On the
  attaching device, a row's pending attachment is its file until the upload
  completes ([§17.2](coven.md#172-attaching-a-file)).
- Coven reads a file from wherever it is: the user's original, coven's own
  copy, the cache, or storage ([§17](coven.md#17-files)).
- An abandoned attachment stays listed until the app dismisses it.
- A device caches a file when it reads it; nothing downloads a file because
  its row arrived. Pinning keeps a file whole on the device
  ([§17.6](coven.md#176-cache-and-pinning)).

Ana pins an album and one song cannot be downloaded. The song's reason is in
`pending()`; pin progress still counts the files and bytes kept. There is no
competing failure state in that progress.

```rust
/// Plaintext bytes in a full file, write-part or snapshot chunk: 64 KiB.
/// Re-exported from coven-format so app buffers use the same chunk size.
pub const CHUNK_SIZE: usize = 64 * 1024;

/// Progress while keeping files whole on this device (§17.6, E8).
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

impl CovenHandle {
    /// The file a row carries, as of its current file version: on the
    /// attaching device, the row's current pending attachment, otherwise its
    /// four file columns. They, its audience and version are read in one
    /// committed state. A row without a file returns `DbError::FileAbsent`;
    /// malformed stored file facts return `DbError::DamagedDatabase`.
    pub async fn file_ref(&self, table: &str, key: impl Into<RowKey>) -> Result<FileRef, DbError>;

    /// Reads the fixed path's metadata (§17.4); this device's own pending
    /// attachment is judged locally, with no storage request. Checks the
    /// reference against its row. Network failures and denied access are
    /// errors, not Missing; an object at the fixed path of any size but the
    /// file's complete one is `Integrity`.
    pub async fn file_status(&self, file: &FileRef) -> Result<FileStatus, FileReadError>;

    /// Reads a whole file, checking it against its row.
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>, FileReadError>;

    /// Opens a file for reading ranges (§17.5). Opening checks the file
    /// against its row once; keep the stream for as long as the file is read.
    /// Its shared store lock prevents deletion until it and its I/O finish.
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError>;

    /// The path, size and modification time coven recorded for a row's
    /// user-provided file, or `None` when the row has none.
    pub async fn user_file(&self, table: &str, key: impl Into<RowKey>) -> Result<Option<UserFile>, DbError>;

    /// Keeps files whole on this device regardless of the cache budget,
    /// downloading what is missing before the call returns; a file still
    /// queued here keeps its pin through its upload. Progress is read from
    /// pin_progress().
    pub async fn pin(&self, files: &[FileRef]) -> Result<(), FileReadError>;

    /// Verified cache coverage for these fixed references, in one snapshot.
    pub async fn pin_progress(&self, files: Vec<FileRef>) -> CovenResult<PinProgress>;

    /// The same pre-built cache query with the same fixed references.
    pub fn subscribe_pin_progress(&self, files: Vec<FileRef>) -> LiveQuery<PinProgress>;

    /// Stops keeping files; they stay in the cache until the budget evicts them.
    pub async fn unpin(&self, files: &[FileRef]) -> Result<(), FileReadError>;

    /// Whether each row's file is pinned, one answer per key in order, or
    /// `None` for a key with no row carrying a file.
    pub async fn rows_pinned(&self, table: &str, keys: Vec<RowKey>) -> CovenResult<Vec<Option<bool>>>;

    /// The same internal query, live. To watch different keys, drop this query
    /// and subscribe with the new keys; there is no separate pin-query type.
    pub fn subscribe_rows_pinned(&self, table: &str, keys: Vec<RowKey>) -> LiveQuery<Vec<Option<bool>>>;

    /// Removes an uploaded file's copies from the cache, pinned or not. Never
    /// touches a file waiting to upload, or storage; a later read
    /// downloads it again.
    pub async fn evict_file(&self, file: &FileRef) -> Result<(), FileReadError>;

    /// A declared namespace's cache budget, in bytes; each evicts on its own.
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError>;
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<u64, DbError>;

    /// This device's abandoned attachments, oldest first, read offline too
    /// (§17.2). Each stays until the app dismisses it.
    pub async fn abandoned_attachments(&self) -> CovenResult<Vec<AbandonedAttachment>>;

    /// The same pre-built query, live.
    pub fn subscribe_abandoned_attachments(&self) -> LiveQuery<Vec<AbandonedAttachment>>;

    /// Forgets these records. Records already gone are ignored.
    pub async fn dismiss_abandoned_attachments(&self, records: &[AbandonedAttachment]) -> CovenResult<()>;
}

/// A file this device attached whose row took a newer change before its
/// upload completed, so the row never named it (§17.2). Its stored bytes,
/// if any, are deleted under §17.8.
pub struct AbandonedAttachment {
    pub table: String,
    pub key: RowKey,
    /// The file's fixed id; its uploader is this device.
    pub file: FileId,
    pub reason: AbandonReason,
}

pub enum AbandonReason {
    /// The row now names another file, or another file was attached here.
    Replaced,
    /// The row's file was cleared or its file columns changed.
    Cleared,
    /// The row was deleted, moved to another audience or taken out.
    RowGone,
}

impl FileRef {
    pub fn table(&self) -> &str;
    pub fn key(&self) -> &RowKey;
    /// The column naming the file.
    pub fn column(&self) -> &str;
    /// The file's size in bytes.
    pub fn plaintext_size(&self) -> u64;
    /// The row's audience, whose encrypted writes carry the file's key (§17.2).
    pub fn audience(&self) -> Audience;
    pub fn id(&self) -> FileId;
    pub fn uploader(&self) -> DeviceId;

    /// The cache namespace declared for this file.
    pub fn namespace(&self) -> &str;

    /// The row's content hash, for checking a complete download.
    pub fn content_hash(&self) -> coven_crypto::ContentHash;
}

/// Current availability; never persisted in the app row (§17.4).
pub enum FileStatus {
    Available,
    /// This device's own pending attachment, still queued here. No other
    /// device has its reference yet.
    Uploading,
    /// Unreadable from storage, with the reason.
    Missing { device: DeviceId, reason: FileMissingReason },
}

pub enum FileMissingReason {
    /// Storage confirms nothing at the path a row names: the provider lost
    /// it. Nothing brings it back on its own; the app can attach it again.
    Lost,
    /// This device's pending attachment, whose source failed; its upload
    /// retries (§17.7).
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

/// A borrowed stream of bounded authenticated plaintext buffers.
pub struct FileRangeStream<'a> { /* private */ }

impl FileRangeStream<'_> {
    pub async fn next(&mut self) -> Result<Option<Vec<u8>>, FileReadError>;
}

pub enum FileReadError {
    /// This file uses a newer coven format (Appendix C.2).
    UpdateRequired(RequiredUpdate),
    /// Remote status or uncached bytes were requested with no storage connected.
    NoStorage,
    /// Storage confirms nothing at the path a row names: the provider lost
    /// it (§17.4). Its pending reason is `FileUnavailable(Lost)`.
    Missing { id: String, device: DeviceId },
    /// A user-provided file is gone from its recorded path.
    UserFileMissing { id: String, path: PathBuf },
    /// A user-provided file's size, modification time or checked content no
    /// longer matches what coven recorded.
    UserFileChanged { id: String, path: PathBuf },
    /// A chunk, a copy on this device, or the object at the fixed path failed
    /// its check; a stored object of the wrong size is damage, not absence (§17.4).
    /// `cause` is the authentication failure when a chunk didn't open; a size,
    /// layout or hash disagreement has none.
    Integrity { id: String, cause: Option<Arc<CryptoError>> },
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
let recording = handle.file_ref("attachments", attachment_id).await?;
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

- Only admins rename the store, add and remove members, and change roles. Each
  member removes their own devices, and admins any device
  ([§10](coven.md#10-members-roles-and-devices)).
- The handle reads the current store name from its committed database state;
  `subscribe_store_name` publishes that query after commits (E4). Before
  storage is set up, this is the name chosen at local creation. No directory
  setting retains a second name that a rename could leave behind.
- Administrative calls require an online sequence catch-up (§10, E6).
- A member records their own new storage access; an admin may also record an
  S3 replacement for another member. Authority uses the author's role in the
  state immediately preceding its slot.
  To replace their S3 key, the person makes a new one in the provider's
  console (§14.3): `replace_access_key` installs it on one device and records
  it. Each of the member's other devices needs the new key from the person,
  through `update_credentials`.
- A member can replace their own signing/sealing pair; any admin can publish
  a prepared pair for an existing member with `replace_member_keys_for`.
  Replacement invites provide approval/bootstrap (E10), subject to provider
  access. The expected old binding must still be current at the claim; an
  earlier replacement or removal fails the call and undoes its artifacts.
  Replacing again needs a new call. Both keys must be fresh.

- Replacement preserves the id, role, circle memberships and device ids.
  Historical public bindings remain available for signature checks. Old
  sealing credentials expose their audience keys (§12.2); storage access is
  a separate operation. Neither a restore code nor a key copy proves approval.
- Removing a member publishes one entry. It returns `()` once its slot commits; applying entries is
  the single path that records provider access work.
- The pending list describes unresolved access work. Each owner's device
  serializes grant and revoke requests, and performs the opposite request
  when a later slot changes the required access (§20.9, §14.2).
  Pending and failed work appears in `pending()`, with the operation id for
  retry. Derived provider work cannot be discarded while its current
  intention remains unmet: disappearance from this list must not hide a
  retained grant. A later slot can replace or remove an account-sharing
  intention.
- A superseded or removed member's S3 key, and an ended invite's, leave no
  record: coven forgets them, and the person deletes them in the provider's
  console (§14.3).
- Removing an account can leave access through a parent, a grant reaching other
  accounts, an unidentified recipient, or the owner. `PendingReason::AccessRemains`
  retains these grants and their reasons for the app to present to the owner;
  a waiting call receives that record as `SyncError::Pending`. Each account
  whose grants remain is its own `RevokeAccess` record, so a member whose
  recorded accounts kept grants lists every such account.
  The revocation remains in `pending()` until a retry confirms the owner
  removed those grants, or later slots no longer require that revocation.
  This also preserves the result when no app call is waiting, including a
  revocation initiated by applying another device's removal.

```rust
impl CovenHandle {
    /// The current name from this device's committed state (§10).
    pub async fn store_name(&self) -> CovenResult<String>;

    /// The same internal query, live (E4).
    pub fn subscribe_store_name(&self) -> LiveQuery<String>;

    /// Sets the name as an admin in the state before its slot (§10, §11).
    /// Returns once the slot commits, or without a slot if already satisfied.
    pub async fn rename_store(&self, name: &str) -> Result<(), SyncError>;

    /// The members and their devices at this device's applied slot.
    pub async fn members(&self) -> CovenResult<Vec<MemberInfo>>;

    /// The same internal query, live (E4).
    pub fn subscribe_members(&self) -> LiveQuery<Vec<MemberInfo>>;

    /// Replaces this member's pair and returns its restore code (§10, §12.3).
    /// Refuses app writes with MemberKeysChanging while confirming writes
    /// made under the old binding, then keeps the candidate before claiming its copies
    /// and binding. Success follows the claimed binding, custody and synced-code
    /// refresh. Failure retains both pairs and progress; other devices holding
    /// the old pair report MemberKeysReplaced.
    pub async fn replace_member_keys(&self) -> Result<SecretText, SyncError>;

    /// Publishes a prepared public pair for an active member under §10's
    /// authority rules. The target retains its private pair before this call;
    /// this call receives no private keys and changes no local private custody.
    /// Confirms copies of every store key and the held circle keys before
    /// publishing the replacement; returns once its slot commits.
    /// Any admin may replace another member, independently of provider ownership.
    /// A self target is still self-replacement, even for an admin; callers
    /// replacing this installation's own pair use replace_member_keys instead.
    /// Invalidation undoes this call’s artifacts. Provider
    /// access and installing the prepared private pair are separate work.
    pub async fn replace_member_keys_for(
        &self,
        member: &MemberId,
        keys: MemberPublicKeys,
    ) -> Result<(), SyncError>;

    /// On S3: switches this member to a key supplied through §20.10's admin-access
    /// or console path and records the new key's id in an admin entry (§10).
    /// The restore code does not change. They keep the key separately,
    /// update their other devices with update_credentials, and delete the
    /// old key in the provider's console; coven forgets it (§14.3).
    /// Requires connected storage.
    /// An uncertain claim retains the new credentials and attempted bytes.
    /// Retry with the same key to settle it before completing or undoing (§11).
    /// An invalidated operation settles any uncertain claim before undo (§11).
    /// Once this member's removal applies, returns Removed and saves nothing.
    pub async fn replace_access_key(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError>;

    /// On a device that already has the store open: takes this member's S3
    /// key directly. The member identity, location and restore code do not
    /// change. A disconnected handle keeps the credentials for its next
    /// connection; a connected handle also replaces its provider's credentials.
    /// Once this member's removal applies, returns Removed and saves nothing:
    /// the key would return to the synced keychain the removal emptied (§14.1).
    pub async fn update_credentials(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError>;

    /// Changes a member's role, as an admin (§10).
    pub async fn set_member_role(&self, member: &MemberId, role: MemberRole) -> Result<(), SyncError>;

    /// The bucket, the store's prefix and the filled member policy for making
    /// a key in the provider's console (§20.10).
    pub async fn guided_access_key(&self) -> Result<GuidedAccessKey, SyncError>;

    /// Publishes the removal of this member and all their devices (§14.1).
    /// Returns once the slot commits. Provider work appears only in pending(),
    /// including owner waits and retained grants. Coven forgets the member's
    /// S3 keys; the person deletes them in the provider's console (§14.3).
    /// An admin removing themselves leaves: this device deletes their restore
    /// code and S3 key from iCloud Keychain before applying the slot, then
    /// stops. A failed deletion is returned; calling again finishes.
    pub async fn remove_member(&self, member: &MemberId) -> Result<(), SyncError>;

    /// Removes a device. The provider can't cut off one device alone, so the
    /// result says how its member signs out of the provider and signs in
    /// again on the devices they keep (§14.2). None on S3: the member's
    /// devices share one key, so nothing at the provider changes.
    pub async fn remove_device(&self, device: DeviceId) -> Result<Option<ProviderSignOut>, SyncError>;
}

/// Guided console steps for a key made by hand (§20.10).
pub struct GuidedAccessKey {
    pub bucket: String,
    pub store_prefix: String,
    pub policy: String,
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

/// How a member reaches storage (format.md D6).
pub enum MemberAccess {
    /// The provider account shared with the member.
    ProviderAccount(String),
    /// The id of the S3 key the person made for the member in the console.
    S3AccessKey { access_key_id: String },
}

pub struct MemberInfo {
    pub id: MemberId,
    pub keys: MemberPublicKeys,
    pub role: MemberRole,
    /// The member's provider account, or the id of their S3 key; never a
    /// secret (§14.3).
    pub access: MemberAccess,
    pub devices: Vec<DeviceInfo>,
    /// Whether this is the member using this device.
    pub is_self: bool,
}

pub struct DeviceInfo {
    pub id: DeviceId,
    pub name: String,
    pub state: DeviceState,
    /// This installation's device id matches, even when removed or replaced.
    pub is_self: bool,
}

pub enum DeviceState {
    Active,
    Removed,
    /// An earlier add-device slot replaced this id. Later writes are refused (§14.1).
    Replaced,
}

pub enum MemberRole {
    Admin,
    Member,
}

/// How a removed device's member cuts off its provider access (§14.2).
pub enum ProviderSignOut {
    /// Remove the app's access from Dropbox or OneDrive, then sign in again.
    RemoveAppAccess { provider: CloudProvider },
    /// Remove the device from the Apple account.
    RemoveFromAppleAccount,
}
```

### E10 Joining and restore

- A person's new device opens the store from their restore code: scanned
  as a QR code, typed in, or read from iCloud Keychain
  ([§13.1](coven.md#131-a-persons-new-device-the-restore-code)).
- The restore code contains only the stable member id and current private
  pair, store id and storage location. On S3, the person keeps their access key
  separately, such as in a password manager, and supplies it on the new device. Dropbox
  and OneDrive likewise require the person's provider sign-in.
- A restore code carries no name. Before connecting, a device restoring from
  a typed or scanned code may show the id, provider and account or location,
  never a store name; it learns the name only after loading. An invite carries
  the current name copied when it was made, the new person's name, and whether it
  was shown in person or sent; it expires at the time fixed when it was made
  (§13.2).
  `decode_code_info` exposes the same location as a saved Keychain entry, so a
  typed or scanned code can show which account it is for before connecting.
- Coven's iCloud Keychain entry saves the code with the store's name: the last
  name one of the person's devices saw, kept with the slot that set it. Setup,
  joining, restoring, resetting and member-key replacement save it; a device
  rewrites only the name before applying a rename, and when opening finds its
  own name newer than the entry's. An older name never replaces a newer one, a
  rewrite keeps the entry's code, and an absent entry stays absent. A failed
  rewrite fails the sync pass or the opening call
  ([§13.1](coven.md#131-a-persons-new-device-the-restore-code)).
- When that rewrite finds the entry unreadable (another format, damaged,
  another store's code, or copies in more than one keychain group), the device
  replaces it with one copy holding its own current code and the store's
  current name. Opening and applying the rename then succeed. A device that
  holds no current code (keys replaced, no storage location, or the member
  removed) leaves the entry as it is. A refused keychain read still fails the
  call.
- A QR scan from one of the person's own devices may supply both the code
  and the S3 key, because the person controls the showing device. The key
  accompanies the code; it is never part of the restore-code encoding.
  iCloud Keychain may also hold the S3 key separately alongside the code.
- `s3_credentials_for_transfer()` gives the showing device's app the S3 key
  this device holds now, read from the same custody its connection uses.
  The app shows it only in the restore code's QR code, to the person's own
  new device, behind the same tap that reveals the code. After a key
  replacement it returns the new key. Other providers return `None`.
- `saved_restore_codes()` lists the registered service's saved codes so the
  app can offer a choice before opening a store. A readable entry exposes the
  store's saved name and id, provider location, including the member's account
  label, and separate S3-key presence, with no secrets.
  The app passes its selection to `restore_from_keychain`; no local store or
  provider connection is needed to list it.
- An entry coven can't restore from is listed too, as unreadable, with the
  store id its keychain account names and the reason: saved in another format
  or damaged, holding another store's code, or held in more than one of the
  app's keychain groups. The app offers to forget it. Otherwise one such entry
  would hide every other choice, and the person could never remove it.
- Only a failure to read the keychain itself fails the whole list: the search,
  an entry's bytes, or an S3 key's presence. A locked keychain says nothing
  about any entry, and offering to delete one then could cost the person a good
  code. A restore-code account whose id part is not a store id as coven writes
  it fails the list with `KeyError::RestoreCodeStoreId`: coven never writes one,
  and there is no store id to forget it by.
- Both synced entries belong to the person. Deleting the store from one device
  keeps them; a device deletes them when it applies the person's removal from
  the store, before the slot applies (§14.1).
- `forget_saved_restore_code(store)` deletes one store's synced restore code
  and S3 key, removing them from all the person's Apple devices. It changes no
  store, storage or device's copy, needs no open store or provider connection,
  and succeeds when the entries are already absent. It deletes an unreadable
  entry the same way, by its store id, in every keychain group that holds one.
- A device that still holds the store under member keys another device
  replaced restores in place with `CovenBuilder::restore_device`
  ([§13.1](coven.md#131-a-persons-new-device-the-restore-code)). The code must
  name that store, or the call fails with `CodeError::WrongStore`. The new
  device loads beside the old copy, which stays openable until the load
  completes; a failure before the swap, or a failed key commit after it,
  leaves the old copy and every keychain entry as they were. Retrying the
  same call finishes an interrupted restore. Until then, opening fails with
  `StoreLockError::ReplacementUnfinished { by: Restore }`, `StoreLayout::stores`
  lists the store in that state, and `reset_device` is refused with that error,
  changing nothing ([§19.3](coven.md#193-another-copy-of-the-same-device)).
  `Coven::discard_store` can delete the store from the device instead, both
  copies with it. The person's restore code and S3 key stay.
- An admin adds a person with an invite
  ([§13.2](coven.md#132-adding-a-person-invites)). An in-person invite's request
  is approved by the inviting device as soon as it is read and checked. A sent
  invite's request waits for the admin's decision, with its confirmation number.
- Only the inviting device lists its open invites, each with its code to show
  again and the request once one has arrived. It looks for the request on the
  shared backoff; `retry_now` ends that wait (E5).
- The joining device keeps the invite code in its own keychain, never synced,
  until the join ends. On Dropbox and OneDrive it keeps the join's provider
  sign-in beside the code, in the store's device credentials, so a restart
  needs nothing from the person on any provider: an S3 invite's code carries
  its key, and CloudKit uses the device's Apple account.
  `CovenBuilder::unfinished_joins` lists the joins this device has under way,
  each as `decode_code_info` reads its code, and `resume_join` continues one
  with neither the code nor a new sign-in. Once the request may have been
  sent, a join can't be cancelled; it ends only by approval, decline or expiry.
- The kept sign-in refreshes where it is kept: a rejected token's one refresh
  is committed to the keychain before any connection uses it, and the joined
  store keeps the refreshed tokens. Otherwise a restart after a refresh would
  resume with a token the provider has already rejected.
- A sign-in the builder holds when a join is resumed replaces the kept one,
  once it is shown to grant the location's access; one that doesn't is
  refused and leaves the kept one. With no sign-in kept or held, or a kept
  one the provider refuses even after its one refresh, the call fails with
  `OAuthError::Reauthorize` and the join stays unfinished. The app then signs
  in with the access the listed join names in `oauth_access`, and resumes.
- A join that ends without a store removes its kept sign-in, then its code,
  then its installation. A failure part way leaves a join that still lists
  and that the next call ends; a sign-in is never left without its code.
- While it waits, the join's status gives the next check's time and, for a sent
  invite, the confirmation number. `JoinControl::check_now` checks at once,
  ending the join's wait; a provider cooldown still holds.
- Before `create_invite` makes any provider change, an App-folder Dropbox
  store fails with `DropboxAppFolderNotShareable`: sharing it requires store
  relocation, outside this operation.
- Inviting the account that holds the store fails with `InviteeIsStoreOwner`.
  CloudKit identifies this case from the saved share: it saves the share, then
  refuses when the returned participants contain no added invitee. The share
  may have changed, but this refusal retains no unwanted participant.
- A replacement invite fixes an existing active id before the requester
  generates keys. Approval binds the exact signed request's new pair after
  publishing copies of every store key and the held circle keys. The
  join requires its approving device’s claimed matching id-and-pair binding
  before registration. If the expected binding changed first, approval undoes
  its owned artifacts and fails; available key bytes alone are not admission.
  Restore checks the supplied pair against the current binding and reports
  `SyncError::MemberKeysReplaced` when authenticated history shows an old
  code. If the current snapshot cannot be opened, report the unavailable
  key instead of inferring membership. Restore never rebinds old keys.
- Each call takes the app's layout-scoped `CovenBuilder`, plus its code,
  device name, status callback and cancellation receiver, and
  returns the open `CovenHandle`. Restore also accepts the separately
  supplied S3 credentials. There is no second app-side open.
  - The builder supplies tables, migrations, custody, clock,
    provider clients, sign-in presenter and file-transfer limits once. Production
    ids use OS randomness; only tests replace their source (E1). The app
    calls `builder.authenticate(access)` first when `oauth_access` is present,
    except to resume a join, which asks for it only when needed (above);
    bootstrap uses that held sign-in and never opens UI. Session-only `InMemory`
    custody lives with the returned handle. A restore's OAuth sign-in tokens
    move from session custody into the device keychain on successful
    publication; a join's move there as soon as it has saved its code.
  - Every call migrates coven's own local tables in place before applying the app's migrations, as a
    writable open does ([Appendix C.1](coven.md#c1-covens-tables-in-the-apps-database)).
  - An unfinished store is created at its permanent path with a durable
    `.coven-bootstrap` marker. Listings hide it and ordinary opens refuse it.
    Bootstrap holds its own directory capability and opens the database once;
    publication removes the marker without moving open database or custody files.
  - Keys and credentials enter final custody only after the store loads, except
    a join's kept provider sign-in. Custody or publication failures roll back
    final custody, restoring exactly the entries the commit changed, so a join
    keeps its sign-in; rollback failures retain both causes. Errors after
    publication carry the open handle, including its session-only keys, and
    retain committed custody.
  - Cancellation closes the loading database and removes the unpublished store,
    with a join's kept sign-in and code, without committing final keys or
    credentials. Dropping a waiting future
    retains encrypted work for explicit retry. There is no cancellable await
    between the final cancellation check, custody commitment, publication and
    returning the handle.
  - Restore and joining register the supplied device name. Every installation
    receives its own device id; resuming a pending join keeps that id.
  - Loading uses snapshots and logs through their owners. The file-transfer
    queue starts with the returned handle and uses the builder's limits.

```rust
/// A one-time invite's UUID, also naming its join-request object (§13.2).
pub struct InviteId(pub Uuid);

/// An uploaded file's random id, its name in storage (§17.3).
pub struct FileId(pub Uuid);

/// What the app may show before using a scanned or typed code (E10).
pub enum CodeInfo {
    /// A restore code reveals no name before loading.
    Restore(CodeMetadata),
    /// An invite carries the store's name captured when the admin made it,
    /// the new person's name (None for a replacement) and how it is approved.
    Invite(InviteInfo),
}

impl CodeInfo {
    /// The store, member and provider facts shared by either kind.
    pub fn metadata(&self) -> &CodeMetadata;
}

/// What an invite code tells before connecting: as typed or scanned, and as a
/// join under way saved it.
pub struct InviteInfo {
    /// Includes the sign-in the join needs.
    pub metadata: CodeMetadata,
    pub store_name: String,
    pub invitee: Option<String>,
    pub delivery: InviteDeliveryKind,
}

/// Facts that either code carries, without secrets or a store name.
pub struct CodeMetadata {
    /// The store named by the code.
    pub store_id: StoreId,
    /// Stable member id fixed by either kind of code.
    pub member: MemberId,
    /// The provider holding the store.
    pub cloud_provider: CloudProvider,
    /// The code's provider location and account label, without credentials or share tokens.
    pub location: StorageConfig,
    /// Sign-in request for this code: the location's Dropbox access level or
    /// OneDrive's scopes; None for S3 and CloudKit.
    pub oauth_access: Option<OAuthAccess>,
}

/// The two codes used to open a store on a new device (§13).
pub enum CodeKind {
    /// Holds only the person's member id and private keys, store id and location.
    Restore,
    /// Holds an invite id, chosen member id, secret, initial key id, inviting writer and its
    /// applied slot at invite time; approval is required (§13.2).
    Invite,
}

/// A scanned or typed code cannot be used for this call (§13, E10).
pub enum CodeError {
    /// The code does not contain valid restore or invite data.
    Invalid,
    /// The call needs the other kind of code.
    WrongKind { expected: CodeKind, actual: CodeKind },
    /// A valid restore code names another store than the one being restored.
    WrongStore { expected: StoreId, actual: StoreId },
}

/// Filesystem failures while reserving, publishing or cancelling a new device.
pub enum BootstrapDirectoryError {
    /// A store already exists or preparing its directory failed.
    Create(DirectoryCreationError),
    /// Another bootstrap or an open database prevents this operation.
    Lock(StoreLockError),
    /// A filesystem operation failed.
    File(FileError),
}

/// Opening a store on a new device failed (§13, E10).
pub enum BootstrapError {
    /// The person cancelled the call.
    Cancelled,
    /// Cancelling a join after its request may have been sent. The join stays
    /// unfinished and resumable; it ends only by approval, decline or expiry.
    JoinRequestSent,
    /// `resume_join` found no join under way for this store on this device:
    /// none was started here, or it has ended.
    NoUnfinishedJoin(StoreId),
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
#[cfg(not(any(target_os = "ios", target_os = "android")))]
pub struct DesktopOAuthPresenter;

#[cfg(not(any(target_os = "ios", target_os = "android")))]
#[async_trait]
impl OAuthPresenter for DesktopOAuthPresenter {
    fn redirect_uri(&self, provider: CloudProvider) -> &str;
    async fn present(&self, authorization_url: &str) -> Result<SecretText, OAuthError>;
}

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
/// or StorageError::Provider. Display and Debug show only its OAuth `error`
/// code; the rest of the callback is omitted because the provider may echo
/// secrets in any field.
pub struct OAuthRedirectError { /* private */ }

impl OAuthRedirectError {
    /// Inspect the unmodified callback explicitly, including provider extensions.
    pub fn callback(&self) -> &str;
}

impl CovenHandle {
    /// Shows this member's code again: stable id, current private pair, store
    /// id and location; no name or storage credentials. The pair is the one current
    /// in the applied prefix; missing current keys report MemberKeysReplaced,
    /// and a retired pair is never exported.
    pub async fn restore_code(&self) -> Result<String, SyncError>;

    /// The S3 key this device holds now, for the app to show beside the
    /// restore code, never inside it, to the person's own new device (§13.1).
    /// A replaced key returns its replacement. None on other providers or
    /// when this device holds no key. A locked keychain fails with
    /// SyncError::SecureStorage(KeyError::KeychainTemporarilyUnavailable).
    pub async fn s3_credentials_for_transfer(&self) -> Result<Option<S3Credentials>, SyncError>;

    /// Fixes a fresh member id or an existing active replacement target, as
    /// an admin. An in-person invite expires an hour after it is made; a sent
    /// one after the lifetime chosen. Replacement preserves role, circle
    /// memberships and device ids; changed provider access is separate.
    pub async fn create_invite(
        &self,
        target: InviteTarget,
        access: InviteAccess,
        delivery: InviteDelivery,
    ) -> Result<Invite, SyncError>;

    /// This device's invites that haven't ended, live, in the order they were
    /// made, each with its request once one has arrived. An invite is listed
    /// once its access is granted and its code can be shown, and the code it
    /// lists is the one first shown. The first value is the current list.
    /// Other devices' invites are never listed: only the inviting device can
    /// act on one.
    pub fn subscribe_invites(&self) -> watch::Receiver<Vec<Invite>>;

    /// For a sent invite: checks the invite's member id and the exact
    /// request's fresh key pair. An in-person invite's request is approved
    /// without this call, and calling it fails with InvitationChanged.
    /// Confirms initial/historical key copies, then adds the new member with
    /// the invite's role or replaces the existing member's keys. A rejected
    /// replacement is reported, not automatically retried with a new entry.
    pub async fn approve_join_request(&self, request: &JoinRequest) -> Result<(), SyncError>;

    /// Declines the request, and takes back the storage access its invite
    /// granted; on S3 the key stays pending until deleted and cleared under E9.
    pub async fn decline_join_request(&self, request: &JoinRequest) -> Result<(), SyncError>;

    /// Cancels an invite before anyone joins with it, taking back the
    /// storage access it granted; on S3 the key stays pending until deleted
    /// and cleared under E9. A waiting request is deleted with it, and the
    /// requester's join ends as declined. Once approval has begun, including an
    /// in-person invite's own approval, fails with InvitationChanged.
    pub async fn cancel_invite(&self, invite: &InviteId) -> Result<(), SyncError>;
}

/// The admin's choice, fixed before sharing the invite (§13.2, §13.3).
pub enum InviteTarget {
    /// The name is the one the admin typed; the invite carries it to the new
    /// person's device, and the app uses it for their profile. It follows the
    /// format's `name` rule (format.md D2): an empty one fails the call with
    /// `SyncError::Format` before any provider change.
    NewMember { role: MemberRole, name: String },
    ReplaceMemberKeys { member: MemberId },
}

/// How the invited person reaches storage (§13.2).
pub enum InviteAccess {
    /// Dropbox, OneDrive or iCloud: the store is shared with
    /// this account.
    ProviderAccount { email: String },
    /// S3: a member key made in the provider's console (§20.10).
    S3AccessKey { access_key_id: String, secret_access_key: SecretText },
}

/// How the invite reaches the person, which fixes how it is approved (§13.2).
pub enum InviteDelivery {
    /// Shown as a QR code to the person beside the admin; the inviting device
    /// approves the request on its own. Expires after an hour.
    InPerson,
    /// Sent by message or email; the admin approves after comparing the
    /// confirmation number.
    Sent { lifetime: InviteLifetime },
}

/// The same choice without its lifetime, as the code carries it.
pub enum InviteDeliveryKind {
    InPerson,
    Sent,
}

/// How long a sent invite stays open.
pub enum InviteLifetime {
    Hour,
    Day,
    Week,
}

/// One of this device's open invites.
pub struct Invite {
    pub id: InviteId,
    pub member: MemberId,
    /// The new person's name; None for a replacement.
    pub invitee: Option<String>,
    pub delivery: InviteDeliveryKind,
    /// The access the invite grants: the account shared with, or the id of
    /// the S3 key it carries; never a secret (§14.3).
    pub access: MemberAccess,
    /// The code to show as a QR code, again whenever the admin asks.
    pub code: String,
    pub expires_at: SystemTime,
    /// The checked request, once one has arrived. For an in-person invite it
    /// is being approved.
    pub request: Option<JoinRequest>,
}

pub struct JoinRequest {
    pub invite: InviteId,
    /// The stable id fixed by the invite, never chosen by the requester.
    pub member: MemberId,
    pub keys: MemberPublicKeys,
    /// The name the new device gave itself, for the admin to check.
    pub device_name: String,
    pub provider_account_email: Option<String>,
    /// Computed from the invite id and `keys`; the requester's phone shows the
    /// same number (format.md D11).
    pub confirmation: ConfirmationNumber,
    /* private authenticated request record; construct only by receiving a checked request */
}

/// Six digits naming a request's keys, compared by the admin and the
/// requester before a sent invite is approved (§13.2). Displays as `041 778`.
pub struct ConfirmationNumber(pub u32);

/// The store and member ids, provider location and account, and sign-in requirement. Only an
/// invite returns a store name, captured when the invite was made.
pub fn decode_code_info(code: &str) -> Result<CodeInfo, CodeError>;

/// Bootstrap progress, using the same records before an open handle exists.
pub enum BootstrapStatus {
    Connecting,
    /// The request is sent. `confirmation` is set for a sent invite. A new
    /// status is reported each time the next check changes: `Checking` while
    /// a check runs, then when the next one is due.
    WaitingForApproval {
        confirmation: Option<ConfirmationNumber>,
        next_check: NextCheck,
    },
    Loading,
    Pending(Vec<PendingRecord>),
}

/// When a waiting join checks for approval next.
pub enum NextCheck {
    /// Now: a check is running.
    Checking,
    /// At this time, on the join's backoff; `check_now` ends the wait.
    At(SystemTime),
    /// The provider asked coven to slow down until this time; `check_now`
    /// can't end it.
    CoolingDown(SystemTime),
}

/// The member's S3 access key, supplied separately from a restore code.
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: SecretText,
}

impl S3Credentials {
    /// Encode the separately held or transferred S3 key, never a restore code.
    pub fn encode(&self) -> Result<SecretBytes, StorageError>;

    /// Read a key supplied separately from a restore code.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError>;
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

impl CovenBuilder {
    /// Restores this device's installation of `store` from the person's
    /// current restore code, for a device whose member keys were replaced
    /// (§13.1). The code must name `store`; its pair is checked against
    /// history before the store or keychain changes, so an old code fails
    /// with MemberKeysReplaced. The new device loads beside the old copy and
    /// swaps in only once loaded; then the pair is committed, the old copy
    /// removed and the synced restore code rewritten. A failure before the
    /// swap, or a failed key commit after it, leaves the old copy and every
    /// keychain entry unchanged; retrying the call finishes an interrupted
    /// one. A swap reset_device began is refused with ReplacementUnfinished,
    /// changing nothing. When custody holds the code's pair and applied
    /// history binds it, the store just opens. S3 credentials as for
    /// `restore_from_code`. The handle holds the device-reset notice (E5).
    pub async fn restore_device(
        self,
        store: StoreId,
        code: &str,
        s3_credentials: Option<S3Credentials>,
        device_name: &str,
        on_status: impl Fn(BootstrapStatus),
        cancel: &watch::Receiver<bool>,
    ) -> Result<CovenHandle, BootstrapError>;
}

/// A saved restore choice, containing no secrets (§13.1).
pub struct SavedRestoreCode {
    /// The store's name as one of the person's devices last saw it: a label
    /// only, never used to find the store, which a rename made since no
    /// device of theirs had it open leaves unchanged.
    pub name: String,
    pub store_id: StoreId,
    /// Provider and account or location as the code holds them, without credentials.
    /// provider() identifies the provider; oauth_access() names its sign-in requirement.
    pub location: StorageConfig,
    /// Whether a separate S3 key was present when this entry was listed.
    pub has_s3_credentials: bool,
}

/// One store's saved iCloud Keychain entry, as listing found it (§13.1).
pub enum SavedRestoreEntry {
    /// An entry the app can pass to `restore_from_keychain`.
    Readable(SavedRestoreCode),
    /// An entry coven can't restore from. The app offers to forget it.
    Unreadable {
        /// The store its keychain account names.
        store_id: StoreId,
        reason: UnreadableSavedCode,
    },
}

impl SavedRestoreEntry {
    /// The store this entry is saved under.
    pub fn store_id(&self) -> StoreId;
}

/// Why a saved entry can't be restored from (§13.1).
pub enum UnreadableSavedCode {
    /// Not a saved restore code coven reads: saved in another format, such as
    /// a bare restore code from before entries carried the store's name, or
    /// damaged. Includes a code whose storage location doesn't decode.
    Format,
    /// The code of another store than the one its account names.
    OtherStore,
    /// More than one of the app's keychain groups holds an entry for the store,
    /// so reading it by store id could give either.
    Ambiguous,
}

/// The registered service's saved iCloud Keychain entries, one per store,
/// ordered by store id; empty when none are saved. An entry coven can't
/// restore from is listed as `Unreadable`, never omitted, and never fails the
/// call. A keychain read failure (the search, an entry's bytes or an S3 key's
/// presence) fails the whole call, as does a restore-code account with no
/// store id, `KeyError::RestoreCodeStoreId`.
/// Native non-Apple platforms return BootstrapError::SecureStorage(KeyError::Unsupported).
pub async fn saved_restore_codes() -> Result<Vec<SavedRestoreEntry>, BootstrapError>;

/// Deletes the store's synced S3 key, then its restore code, from iCloud
/// Keychain, and so from every one of the person's Apple devices (§13.1).
/// Readable and unreadable entries alike; every keychain group's copy goes.
/// Touches no store, storage or device's copy, and needs no open store or
/// provider connection. Absent entries succeed; after a failure part way the
/// store is still listed and calling again finishes. A later join, restore,
/// reset or member-key replacement saves the entry again.
/// Native non-Apple platforms return BootstrapError::SecureStorage(KeyError::Unsupported).
pub async fn forget_saved_restore_code(store: StoreId) -> Result<(), BootstrapError>;

/// Opens the selected store on a new Apple device from its current iCloud
/// Keychain restore code (§13.1). On S3, uses supplied credentials when Some; None reads
/// the S3 key held separately alongside the code. An absent key fails with
/// BootstrapError::Sync carrying StorageFailure::InvalidConfiguration;
/// a keychain read failure is returned. Other providers require None and
/// their own sign-in.
/// Reads by store id, including a code or key refreshed since listing.
/// Returns None when the selected store's code is no longer saved.
pub async fn restore_from_keychain(
    builder: CovenBuilder,
    saved: &SavedRestoreCode,
    s3_credentials: Option<S3Credentials>,
    device_name: &str,
    on_status: impl Fn(BootstrapStatus),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<CovenHandle>, BootstrapError>;

/// Completes the provider's recipient acceptance using the encrypted invite
/// code before sending the join request. CloudKit acceptance uses coven-storage's
/// native adapter. If access is removed while joining, the provider's
/// permission failure reaches the app and an admin can invite again. Keys and
/// credentials are committed only after bootstrap succeeds, except a Dropbox
/// or OneDrive sign-in, kept on the device beside the code until the join ends.
///
/// On the new person's device: makes their member keys, keeps the code in
/// this device's keychain, writes a join request to storage, and waits for
/// approval, then loads the store. Returns `None` when the request is declined
/// or the invite expires. Dropping the call keeps the unfinished join for
/// `resume_join`. Cancelling through `control` once the request may have been
/// sent ends the call with `BootstrapError::JoinRequestSent`, keeping the
/// unfinished join, as dropping does.
pub async fn join_with_invite(
    builder: CovenBuilder,
    code: &str,
    device_name: &str,
    on_status: impl Fn(BootstrapStatus),
    control: &JoinControl,
) -> Result<Option<CovenHandle>, BootstrapError>;

/// The app's hold on a running join: cancel before the request is sent, or
/// check for approval at once.
pub struct JoinControl { /* private */ }

impl JoinControl {
    pub fn new() -> Self;
    pub fn cancel(&self);
    /// Ends the join's wait so the next check runs now. The wait keeps its
    /// failure count, as `retry_now` does for a handle's waits, so a check
    /// that finds the request still waiting waits the next longer delay. A
    /// provider cooldown still holds, and the status says when it ends.
    pub fn check_now(&self);
}

impl CovenBuilder {
    /// The joins under way on this device (§13.2), ordered by store id, each
    /// as `decode_code_info` reads its saved code. A saved code coven can't
    /// read fails the call; it is never skipped.
    pub async fn unfinished_joins(&self) -> Result<Vec<InviteInfo>, BootstrapError>;

    /// Continues an unfinished join from its saved code and device name, as
    /// `join_with_invite` does. On Dropbox and OneDrive it connects through the
    /// sign-in the join kept, or a sign-in this builder holds, which replaces
    /// the kept one once it grants the location's access. With neither, or a
    /// kept one the provider refuses even after its one refresh, it fails with
    /// `OAuthError::Reauthorize` and the join stays unfinished. Returns `None`
    /// when the request was declined or the invite expired, after deleting the
    /// kept sign-in and the saved code. A store with no saved code fails with
    /// `BootstrapError::NoUnfinishedJoin`.
    pub async fn resume_join(
        self,
        store: StoreId,
        on_status: impl Fn(BootstrapStatus),
        control: &JoinControl,
    ) -> Result<Option<CovenHandle>, BootstrapError>;
}

impl OAuthClients {
    /// Registers the access the app offers; None disables a sign-in.
    pub fn new(
        dropbox: Option<DropboxClients>,
        onedrive_client_id: Option<String>,
        clock: ClockRef,
    ) -> Self;

    /// Redirect token requests to an HTTP fake while retaining the real protocol.
    // Enabled by coven/test-utils.
    #[cfg(feature = "test-utils")]
    pub fn with_token_endpoint(mut self, endpoint: String) -> Self;
}

/// Dropbox access is fixed by the app registration, not an incremental scope.
pub enum DropboxAccess { AppFolder, FullDropbox }

/// One or both of the app's Dropbox registrations. There is no empty variant.
pub enum DropboxClients {
    AppFolder { app_key: String },
    FullDropbox { app_key: String },
    Both { app_folder_key: String, full_dropbox_key: String },
}

/// Access for this request. Dropbox callers supply the creation choice
/// explicitly, using the only registered access when one key is configured.
pub enum OAuthAccess {
    Dropbox(DropboxAccess),
    OneDrive,
}

impl OAuthAccess {
    /// The provider receiving this authorization request.
    pub fn provider(self) -> CloudProvider;
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
  retain the original `OAuthRedirectError`, including denial. Each prints
  only the status and the OAuth error code, never a description or other
  provider-supplied detail.
- Refresh and S3 key replacement leave restore codes untouched. Restore
  codes carry the storage location, never S3 keys or OAuth tokens.
- Token custody retains the client id and granted access. Refresh uses that
  registered client, including the chosen Dropbox key, and preserves granted
  access only when the provider omits a scope field. A returned scope field is
  authoritative: a missing required scope is `AccessNotGranted`, never a
  successful upgrade. Microsoft's scope field lists only the Graph
  permissions (`Files.ReadWrite.All`, `User.Read`, bare or under
  `https://graph.microsoft.com/`); it grants the requested `offline_access`
  by issuing a refresh token, so OneDrive tokens holding none are
  `AccessNotGranted`. A refresh reply without a new refresh token keeps the
  one it used.

Example, adding Ana's laptop. On her phone:

```rust
let code = handle.restore_code().await?;
// On S3 the key rides beside the code, as its own value.
let s3_key = match handle.s3_credentials_for_transfer().await? {
    Some(key) => Some(key.encode()?),
    None => None,
};
show_blurred_qr_code(&code, s3_key.as_ref());
```

On the laptop:

```rust
let info = decode_code_info(&scanned)?;
let metadata = info.metadata();
show_provider_location(&metadata.location);
let mut builder = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .oauth_clients(oauth_clients.clone())
    .clock(clock.clone());
if let Some(access) = metadata.oauth_access {
    builder.authenticate(access).await?;
}
// `scanned_key` is the encoded key the phone showed beside the code, if any.
let s3_credentials = match (metadata.cloud_provider, scanned_key) {
    (CloudProvider::S3, Some(bytes)) => Some(S3Credentials::decode(&bytes)?),
    (CloudProvider::S3, None) => Some(ask_for_s3_access_key().await?),
    _ => None,
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

If the laptop uses iCloud Keychain instead, Ana chooses from the saved entries:

```rust
let saved = saved_restore_codes().await?;
let selected = match choose_saved_store(&saved) {
    SavedRestoreEntry::Readable(selected) => selected,
    // An entry coven can't read is offered only for deletion.
    SavedRestoreEntry::Unreadable { store_id, .. } => {
        forget_saved_restore_code(*store_id).await?;
        return Ok(());
    }
};
let mut builder = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .oauth_clients(oauth_clients.clone());
if let Some(access) = selected.location.oauth_access() {
    builder.authenticate(access).await?;
}
let s3_credentials = if selected.location.provider() == CloudProvider::S3
    && !selected.has_s3_credentials
{
    Some(ask_for_s3_access_key().await?)
} else {
    None
};
match restore_from_keychain(
    builder, selected, s3_credentials, "Ana's laptop", |step| show_step(step), &cancel_rx,
).await? {
    Some(handle) => show_store(handle),
    None => show_saved_code_removed(),
}
```

If Carol's laptop stopped with `MemberKeysReplaced`, it restores in place from the
code her phone shows, keeping its copy until the new one is ready:

```rust
let handle = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .restore_device(
        store,
        &scanned,
        s3_credentials,
        "Carol's laptop",
        |step| show_step(step),
        &cancel_rx,
    )
    .await?;
```

Example, Ana adding Carol. On Ana's phone:

```rust
let invite = handle
    .create_invite(
        InviteTarget::NewMember { role: MemberRole::Member, name: "Carol".into() },
        InviteAccess::ProviderAccount { email: "carol@example.com".into() },
        InviteDelivery::Sent { lifetime: InviteLifetime::Day },
    )
    .await?;
send_to_carol(&invite.code);

let mut invites = handle.subscribe_invites();
loop {
    let open = invites.borrow_and_update().clone();
    if let Some(request) = open.iter().find(|i| i.id == invite.id).and_then(|i| i.request.clone()) {
        // Ana asks Carol for the number her phone shows.
        if confirm(&request.confirmation).await {
            handle.approve_join_request(&request).await?;
        } else {
            handle.decline_join_request(&request).await?;
        }
        break;
    }
    invites.changed().await?;
}
```

On Carol's phone:

```rust
let mut builder = Coven::builder(layout.clone())
    .synced_tables(tables())
    .migrations(migrations())
    .oauth_clients(oauth_clients.clone())
    .oauth_presenter(platform_sign_in_sheet);
builder.authenticate(OAuthAccess::Dropbox(DropboxAccess::FullDropbox)).await?;
// The waiting screen shows Carol's number and a countdown to the next check;
// its Check now button calls control.check_now().
let control = JoinControl::new();
match join_with_invite(
    builder,
    &scanned,
    "Carol's phone",
    |step| show_step(step),
    &control,
)
.await?
{
    Some(handle) => show_store(handle),
    None => show_declined(),
}
```

When Carol's app next starts after closing while she waited:

```rust
// Each call consumes its builder, so each gets one configured as above.
let new_builder = || {
    Coven::builder(layout.clone())
        .synced_tables(tables())
        .migrations(migrations())
        .oauth_clients(oauth_clients.clone())
        .oauth_presenter(platform_sign_in_sheet.clone())
};
// A join under way: show its waiting screen and continue it with the sign-in
// it kept. Only when that is gone or refused does Carol sign in again.
if let Some(join) = new_builder().unfinished_joins().await?.into_iter().next() {
    let store = join.metadata.store_id;
    let resumed = match new_builder().resume_join(store, |step| show_step(step), &control).await {
        Err(BootstrapError::OAuth(OAuthError::Reauthorize(_))) => {
            let mut builder = new_builder();
            builder.authenticate(join.metadata.oauth_access.expect("a sign-in join")).await?;
            builder.resume_join(store, |step| show_step(step), &control).await
        }
        resumed => resumed,
    };
    match resumed? {
        Some(handle) => show_store(handle),
        None => show_declined(),
    }
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
    /// The public pair bound to a stable member id by admin entries.
    pub fn public_keys(&self) -> MemberPublicKeys;
    /// Encodes both private seeds for custody or a restore code (§13.1).
    pub fn to_secret_bytes(&self) -> SecretBytes;
    /// Restores both pairs from custody or a restore code.
    pub fn from_secret_bytes(bytes: &[u8]) -> Result<Self, MaterialError>;

    /// Make both member key pairs using the operating system's randomness.
    pub fn generate() -> Result<Self, CryptoError>;

    /// Sign bytes with the member's Ed25519 pair (§12).
    pub fn sign(&self, bytes: &[u8]) -> coven_crypto::Signature;

    /// Sign the digest of every object byte before the signature (Appendix D.3).
    /// Panics if the storage path is empty.
    pub fn sign_object(&self, path: &str, digest: &coven_crypto::ObjectDigest) -> coven_crypto::Signature;

    /// Sign a snapshot's exact kind, version and routing fields (D9).
    /// The prefix domain and path are bound separately from whole-object signing.
    /// Panics if the storage path is empty.
    pub fn sign_prefix(&self, path: &str, prefix: &[u8]) -> coven_crypto::Signature;

    /// Sign a write's cleartext prefix, sealed header chunk and part roots
    /// (D9), separately from the whole-object signature.
    /// Panics if the storage path is empty.
    pub fn sign_write_header(
        &self,
        path: &str,
        prefix: &[u8],
        header: &[u8],
        roots: &[[u8; 32]],
    ) -> coven_crypto::Signature;

    /// Open a store-key box, authenticating the exact path and opaque clear prefix.
    /// Format checks the recipient and opened identity; the caller checks the
    /// introduction commitment before accepting the candidate key.
    /// Panics if the storage path is empty.
    pub fn open_store_key(
        &self,
        path: &str,
        prefix: &[u8],
        sealed: &[u8],
    ) -> Result<StoreKey, CryptoError>;

    /// Open a circle-key box, authenticating the exact path and opaque clear prefix.
    /// Format checks the recipient and opened identity; the caller checks the
    /// introduction commitment before accepting the candidate key.
    /// Panics if the storage path is empty.
    pub fn open_circle_key(
        &self,
        path: &str,
        prefix: &[u8],
        sealed: &[u8],
    ) -> Result<CircleKey, CryptoError>;
}

impl MemberIdentity {
    pub fn member(&self) -> MemberId;
    /// Encodes the id and every held private pair, including pending and
    /// historical credentials. This custody encoding is not a restore code.
    pub fn to_secret_bytes(&self) -> SecretBytes;
    pub fn from_secret_bytes(bytes: &[u8]) -> Result<Self, MaterialError>;

    /// Bind a private pair to the stable id its admin entry names.
    pub fn new(member: MemberId, keys: MemberKeys) -> Self;

    /// Make both key pairs for an independently generated id (§10).
    pub fn generate(member: MemberId) -> Result<Self, CryptoError>;

    /// The active private pair: the one this device signs with and a
    /// restore code exports.
    pub fn keys(&self) -> &MemberKeys;

    /// A self-replacement's fresh pair, kept before its binding is published.
    pub fn candidate(&self) -> Option<&MemberKeys>;

    /// The held private pair whose sealing key is `sealing`. A copy names the
    /// exact credential it is sealed to, and opens only with that one, never
    /// with whichever pair is latest (§12.1).
    pub fn credential(&self, sealing: &SealingKey) -> Option<&MemberKeys>;

    /// Whether some held pair is exactly `keys`.
    pub fn holds(&self, keys: &MemberPublicKeys) -> bool;

    /// This identity keeping `candidate` for a self-replacement. A candidate
    /// already held stays held as a retired credential: its copies may exist.
    pub fn with_candidate(&self, candidate: MemberKeys) -> Self;

    /// This identity with the held pair `current` active, as replay's current
    /// binding selects it (§12.3). The previously active pair and any other
    /// candidate stay held as retired credentials. `None` when no held pair
    /// is `current`.
    pub fn activate(&self, current: &MemberPublicKeys) -> Option<Self>;

    /// This identity with its candidate retired: a replacement that never
    /// became current leaves its copies, so its pair stays held (§12.4).
    pub fn retire_candidate(&self) -> Self;

    /// This identity without its candidate: a self-replacement cancelled
    /// before reserving anything never sent a copy to it.
    pub fn without_candidate(&self) -> Self;
}

impl StoreKeyring {
    /// Encodes every opened store and circle key for custody (§12.4).
    pub fn to_secret_bytes(&self) -> SecretBytes;
    /// Reads custody bytes, rejecting malformed, duplicate or empty keyrings.
    pub fn from_secret_bytes(bytes: &[u8]) -> Result<Self, MaterialError>;

    /// Start with one opened store key. A keyring always holds a store key.
    pub fn new(key: StoreKey) -> Self;

    /// Keep an opened store key; a different key at the same id is an error.
    pub fn insert_store_key(&mut self, key: StoreKey) -> Result<(), MaterialError>;

    /// Keep an opened circle key, retaining its earlier keys too (§15.5).
    pub fn insert_circle_key(&mut self, key: CircleKey) -> Result<(), MaterialError>;

    /// An opened store key by identity, so older writes still open (§12.2).
    pub fn store_key(&self, key: KeyId) -> Result<&StoreKey, MaterialError>;

    /// All held store-key identities, including keys preceding a member's join.
    pub fn store_key_ids(&self) -> impl Iterator<Item = KeyId> + '_;

    /// All held keys for a circle, including concurrent and historical rotations.
    pub fn circle_key_ids(&self, circle: CircleId) -> impl Iterator<Item = KeyId> + '_;

    /// An opened circle key by its circle and identity (§15.5).
    pub fn circle_key(&self, circle: CircleId, key: KeyId) -> Result<&CircleKey, MaterialError>;
}

impl CovenHandle {
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

- A custody owner unlocks once at creation or open and retains the capability needed to
  persist through that session. Custom custody obeys the same lifetime:
  `persist` must reuse its unlocked derivation, and `close` erases it.
  Ana adding a historical key saves the changed keyring without another
  passphrase derivation. An empty session can acquire keys during setup.

```rust
/// The app's own store for the store keys and circle keys this device holds.
pub trait StoreKeyCustody: Send + Sync {
    /// Called once at creation or open; None when this device has never held keys.
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
    /// Called once at creation or open; persistence reuses the held session capability.
    fn unlock(&self) -> Result<Option<MemberIdentity>, KeyError>;
    fn persist(&self, identity: &MemberIdentity) -> Result<(), KeyError>;
    fn forget(&self) -> Result<(), KeyError>;
    /// Erases held memory after users finish; performs no persistence or IO.
    /// Fallible saves complete through persist before dependent work starts.
    fn close(&self);
}
```

### E12 Circles

- A circle's members add and remove its members ([§15.5](coven.md#155-running-a-circle)).
  Circle changes first catch up on reachable storage (§10, E6).
- Circle-changing calls return `SyncError` ([E5](#e5-storage-and-sync)), as member calls
  do ([E9](#e9-members-and-devices)). Validation uses the latest applied prefix:
  `Rejected(NotAllowed)` for authority, `Rejected(NoAdminLeft)` for the
  last-admin rule, and `Rejected(TargetGone(target))` for an absent member,
  device or circle. The target retains its typed id. `StoreOwner` remains
  a separate provider constraint.
- Removing someone from a circle publishes one entry and returns once it is
  claimed. Rotation follows in the sync pass; it is not part of that call (§12.2).

```rust
/// Circle calls borrowing the open store that owns their work (§15, E12).
pub struct Circles<'a> { /* private fields */ }

impl CovenHandle {
    pub fn circles(&self) -> Circles<'_>;
}

impl Circles<'_> {
    /// Makes a circle with this member in it.
    pub async fn create(&self, name: &str) -> Result<CircleId, SyncError>;

    /// Renames a circle. Its key, members and rows don't change.
    pub async fn rename(&self, circle: CircleId, name: &str) -> Result<(), SyncError>;

    /// Deletes a circle with one admin slot. The deletion rule hides
    /// its rows permanently; there is no row-delete write (§15.9).
    pub async fn delete(&self, circle: CircleId) -> Result<(), SyncError>;

    /// Adds a store member to the circle. They get its current and earlier
    /// keys, so they read its history. Rejoining devices reload skipped parts,
    /// using the snapshot and retained suffix (§15.7).
    pub async fn add_member(&self, circle: CircleId, member: &MemberId) -> Result<(), SyncError>;

    /// Removes someone with one entry; remaining members rotate exposed keys (§15.8).
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
    /// read the reset count in that circle, on this device too (§19.4).
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

- Coven migrates its own local tables automatically before the app's migrations; each internal
  migration is atomic ([Appendix C.1](coven.md#c1-covens-tables-in-the-apps-database)).
- The app's schema is a list of migrations numbered from 1 with no gaps.
- Opening runs every migration above the database's version in one
  transaction, so a failure leaves the schema as it was.
- Constructors declare the kind: `addition` or `breaking` for SQL,
  `addition_with` or `breaking_with` for code. Breaking constructors require
  `OldWrites::Convert(f)` or `OldWrites::Drop`; there is no implicit choice.
  `Convert` handles eligible received late ordinary changes in version and
  causal order (§18.6;
  [`receive_matches_evaluation`](proofs/queue/CovenQueue/Conversion.lean)).
  - Local migration preserves every queued original record, including untried writes; tried uploads
    also retain their format and sealing keys ([`conversions_preserve_uploads`,
    `conversions_preserve_attempt`](proofs/queue/CovenQueue/Conversion.lean)).
  - Member-key slot cut-offs use the same data rule (§14.1).
  - Before invoking a callback, coven determines candidate schema admission and resolves each
    change's original generation source and replaced setters under §9.5. A dependent change never
    reaches the callback: retain its original write/row identity, generation, operation, names,
    scalar values and failed source identities as a frozen loss. A delete keeps its identity even
    without scalars. Recorded reads only govern readiness ([`dependent_change_not_converted`,
    `converted_change_matches_receiver`](proofs/queue/CovenQueue/Conversion.lean)).
  - One received interpretation, including one audience part, can contain converted changes and
    frozen originals. Conversion is independent of attempt status and preserves device, number,
    timestamp, authored read-through slot, row identity, generation, operation and original source
    identities. Renamed tables and columns retain their sources under the new names; frozen changes
    keep their old names and original cause ([`conversion_preserves_identity`,
    `convertChange_metadata`](proofs/queue/CovenQueue/Conversion.lean); reference and rename
    validation remain implementation obligations).
  - With `Drop`, keep each original change as a schema loss naming the breaking version and consume
    the write. No callback changes any upload record. Already frozen values and causes survive later
    raises ([`explicit_drop_preserves_values`, `conversions_preserve_frozen`,
    `lost_never_reconverted`](proofs/queue/CovenQueue/Conversion.lean)).
  - The author reloading after a competing raise uses the same per-change
    rule as peers. New writes use the atomically reloaded state; an implicit
    own read of an excluded original write creates no data dependence. Only
    the change's recorded data sources establish that dependence (§9.5). Source
    evidence and verdicts survive snapshots and loss dismissal (§9.5).
  - A failed callback aborts the receive or reload transaction, preserving
    the preceding committed values, losses, source verdicts and applied slot.
    The upload queue remains unchanged on success and failure alike.
- Coven still classifies every migration (§18.1). Either kind mismatch returns
  `DbError::MigrationKindMismatch { version }` and rolls back the whole batch;
  local-only changes accept `addition`. Reopening checks historical declarations
  against committed classifications before running new migrations.
- A converted change keeps the references its columns carried, renamed
  with them. Changing a reference's value, or adding a column that is a
  reference, fails conversion and leaves application pending without a
  partial commit: the device can't know which generation of the parent the
  old write meant.

```rust
pub enum OldWrites {
    /// Convert eligible changes; never rewrite their original upload records.
    Convert(std::sync::Arc<dyn Fn(&mut RowChange) -> Result<(), DbError> + Send + Sync>),
    /// Retain original changes as frozen schema losses, including their values.
    Drop,
}

impl Migration {
    pub fn addition(version: u32, name: &'static str, sql: &'static str) -> Self;
    pub fn addition_with<F>(version: u32, name: &'static str, f: F) -> Self
    where
        F: Fn(&MigrationContext<'_>) -> Result<(), DbError> + Send + Sync + 'static;

    pub fn breaking(version: u32, name: &'static str, sql: &'static str, old_writes: OldWrites) -> Self;
    pub fn breaking_with<F>(version: u32, name: &'static str, f: F, old_writes: OldWrites) -> Self
    where
        F: Fn(&MigrationContext<'_>) -> Result<(), DbError> + Send + Sync + 'static;
}

/// SQL access inside the transaction applying a migration (§18.3, E13).
pub struct MigrationContext<'connection> { /* private fields */ }

/// What one row change does (§4).
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
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>;
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>;
}

/// One row change of a write record (§4).
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
/// privately, which generation of its parent the write named (§9.1).
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
        Migration::addition(1, "initial", include_str!("migrations/001_initial.sql")),
        Migration::breaking(2, "rename_attachment_title", "ALTER TABLE attachments RENAME COLUMN title TO name",
            OldWrites::Convert(std::sync::Arc::new(|change| {
                if change.table == "attachments" {
                    change.rename_column("title", "name");
                }
                Ok(())
            }))),
    ]
}
```

### E14 Development tools

- A development tool is a call an app's own development builds use around
  coven, never part of the sync protocol. It opens no store, and it checks and
  records nothing about any store. An app enables these calls with coven's
  `dev-tools` feature; a release build leaves the feature off, so they are
  absent from it.
- Ana works on her app against a test bucket at base prefix `dev`. Each time
  she resets her development library she wants it to start on an empty
  remote. Before creating the new store she calls `empty_s3_prefix` with the
  location and key the store will connect with. It deletes everything under
  `dev/` that earlier stores left there, and returns how many objects and
  unfinished uploads went.
- `empty_s3_prefix` deletes every object whose key starts with the base prefix
  and a slash, whatever wrote it. That includes other stores, upload
  temporaries, detection probes and things coven never wrote. A neighbouring
  prefix such as `dev-old` stays. It also aborts every unfinished multipart
  upload under the prefix.
- It lists everything before removing anything, so the listing's page markers
  stay valid, and then lists again. Something still under the prefix means a
  device kept writing there while it ran, and the call fails with
  `StorageFailure::Conflict`.
- A location with no base prefix is refused with
  `EmptyS3PrefixError::WholeBucket` before any request. Emptying it would
  empty the whole bucket. A prefix typed as only slashes is that location too:
  it parses to the bucket's root (§20.3).
- The location is validated as setup validates it (E5). A location that is not
  S3, an empty key, or an uncatalogued endpoint with no profile is refused
  with `EmptyS3PrefixError::Storage`. Listing, aborting and deleting failures
  keep their provider cause there too.

Edge cases:

- The first failure stops the call. What it removed before then stays
  removed. Removal is idempotent: an object already gone counts as deleted,
  and an upload already finished or aborted counts as aborted. So calling
  again finishes the job.
- In a versioned bucket, deleting an object leaves its earlier versions and
  a delete marker. The call leaves those: a member key may not delete
  versions. Only the bucket's own lifecycle or an admin removes them.
- It needs only what a member key already may do under the prefix (§20.10):
  list objects and uploads, delete objects and abort uploads.

```rust
/// What emptying an S3 prefix removed (E14).
#[cfg(feature = "dev-tools")]
pub struct EmptiedS3Prefix {
    /// Objects deleted.
    pub objects: usize,
    /// Unfinished multipart uploads aborted.
    pub uploads: usize,
}

/// Why an S3 prefix was not emptied (E14).
#[cfg(feature = "dev-tools")]
pub enum EmptyS3PrefixError {
    /// The location has no base prefix, so emptying it would empty the whole
    /// bucket. Nothing was sent.
    WholeBucket,
    /// The location or key was refused, or a request failed. Whatever was
    /// removed before the failure stays removed; emptying again finishes.
    Storage(StorageError),
}

/// Delete every object and abort every unfinished upload under an S3
/// location's base prefix, whatever wrote them, and return how many of each
/// went (E14). A location with no base prefix is refused before any request
/// with `EmptyS3PrefixError::WholeBucket`. The first failure stops it;
/// calling again finishes. Retained versions in a versioned bucket stay.
#[cfg(feature = "dev-tools")]
pub async fn empty_s3_prefix(
    location: StorageConfig,
    credentials: S3Credentials,
) -> Result<EmptiedS3Prefix, EmptyS3PrefixError>;
```
