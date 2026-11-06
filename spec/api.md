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
  transaction callbacks (`CovenWrite`, `MigrationContext`) are synchronous.
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
use std::{collections::{BTreeMap, HashMap}, future::Future, num::{NonZeroU64, NonZeroUsize},
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
/// Reads the `key` column of coven's views and tables, coven's encoding
/// of a row's key (format.md D3), as for a row that is gone; any other value
/// fails as `DamagedDatabase`.
impl types::FromSql for RowKey;

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
  the database, coven's copies of files, the cache, the store's settings
  (its id and this device's id), and its storage settings, which
  coven-storage keeps in a file of their own. The store's name is not a setting: a rename is synced,
  so the name is the database's committed state, which listing the stores reads without opening the
  store (`StoreInfo::name`, E9).
- Creating, restoring or joining a store makes the directory and writes
  the settings, so the app never chooses a device id ([§10](coven.md#10-members-roles-and-devices)).
- A `create_store` error carries no open handle. If the store becomes visible
  but syncing its directory fails, coven closes it and waits for release before
  returning `StoreCreationError::PublishedButNotSynced { store, source }`.
  The complete store stays on disk; the app can call `open(store)` with its
  usual custody choices. Keeping the error cannot keep the store locked.
- Creation writes the member identity and the device id before it
  publishes the store. A failure before publication removes what it
  wrote of them, and a panic does too, as `StoreCreationError::Bug` (E15).
  It removes only what it wrote: an identity the custody already held is
  refused before anything is written, so no failure takes it away. A panic while
  publishing leaves unknown whether the store became visible, so coven
  reads its marker: a store already published keeps both and returns
  `PublishedButNotSynced` with the bug as its source. Without that check, a
  published store would lose its identity and could never open.
- Provider access is chosen when storage is first attached (E5), not when the
  device-local directory is created. Dropbox's required `DropboxLocation`
  selects not-shared (`AppFolder`) or shared-capable (`SharedFolder`); it has
  no default. With one registered Dropbox key only that key's location kind is
  available; with both the app supplies the user's or its own explicit choice.
  The choice survives reopening and restore and cannot be changed by
  reconnecting.
- A `Coven` is this app's stores on this device: the app directory they live
  under, the OS keychain service their keys and secrets are kept under, and the
  device's network. The app builds one at startup, holds it while it runs, and
  makes every builder, listing and deletion from it. `Coven::new` names the
  keychain service and opens the OS keychain, so an invalid name or a
  keychain coven can't reach fails at startup rather than at the first store; an
  application test builds its installation with `Coven::in_memory` instead.
  `Coven::new` also takes the app's bug sink, its crash reporter, which hears
  every bug in work nobody waits for, in each store and in the `Coven`'s own
  calls (E15). There is no default: an app that names no reporter would lose
  those bugs to a log.
- `Coven::storage_config` reads a closed store's saved location without opening
  its database or custody. The handle's `storage_config` reads the same committed
  `StorageConfig`, including the member's account label, so an app can show
  "Dropbox: ana@example.com" without keeping a second copy. Neither returns
  credentials or contacts storage. Before setup they return
  `None`; once set up, the location stays, and unreadable settings remain an error.
- Store lock files live beside each store directory: the writer, reader and
  bootstrap locks. Deletion holds all three. It first renames the directory to
  its deletion name, which hides the store and records the deletion, then
  deletes the device's entries for the store, removes the directory, and
  releases and removes every lock file, so a deleted store leaves nothing named
  for it. What a cut deletion leaves, the renamed directory or the lock files
  alone, would refuse the store's id to creation, restore and join; listing the
  stores, and a restore or join of the store, finish it first (§11.6). Holding the bootstrap lock
  keeps out a running restore, join or replacement: after its swap, a running `reset_device`,
  `restore_device` or `resume_replacement` holds only that lock between steps, and deletion must not
  remove files under it. Lock acquisition, creation and lock-file removal are serialized by the
  layout, so no one can lock a lock file between its release and its removal.
- Deleting a store with storage set up loses whatever hasn't been uploaded.
  So before Ana deletes the Household store from her phone, the app shows
  her `unsent_work()`: say, 3 writes and 1 file still waiting. These are the
  same counts `delete_store(IfUnsent::Refuse)` refuses with, read the same
  way, so the warning and the refusal can't disagree, and the app needn't
  attempt a deletion to learn them.
- Directory-entry existence checks do not follow symlinks. Only not-found
  means absent; other failures retain their operation, path and OS cause.
- *File protection* is Apple's class for a file: whether it can be read and
  written while the device is locked ([§3](coven.md#3-what-coven-assumes)). On
  Apple platforms, `CovenBuilder::file_protection` takes a `FileProtection`, one
  of Apple's four classes; neither exists elsewhere, where files have no class.
  Unset, coven changes no file's class, and a new file takes its folder's: the
  app's default, unless the app gave the folder another.
- Creating, restoring or joining a store gives its directory the class before
  anything is made there, so each file is made with it. A writable open gives
  it to every folder and file already in the store's directory, folders first,
  unless the directory's `file-protection.json` says the last complete walk
  gave this class. It does so before opening the database, whose WAL and
  shared-memory files are then made with it. Resuming a restore or join walks
  the files it already loaded the same way, and `reset_device` and
  `restore_device` walk the installation they replace before making the new
  one. A read-only open changes no class. Files outside the store's directory
  keep their own folder's class: the store's lock files beside it, and
  SQLite's temporary files.
- Coven bundles stock SQLite, which has none of the `SQLITE_OPEN_FILEPROTECTION_*`
  flags Apple's own SQLite takes, so the folder's class is how the database and
  its WAL and shared-memory files get theirs.
- Opening reads this device's keychain entries, which are readable only while
  the device is unlocked ([Appendix D.6](coven.md#d6-where-secrets-live)), so a
  store opens only then, whatever the class. The class decides what an open
  handle can still do once the device locks, such as sync in the background:

| Class | Once the device locks |
|---|---|
| `Complete` | shortly after, no file of the store can be read or written; the handle's reads, writes and sync fail until the device unlocks |
| `CompleteUnlessOpen` | the files the handle holds open, its database and its WAL and shared-memory files, stay readable and writable, and new files can be made, so a pass can apply and send writes; a step that opens a closed file, such as reading, caching or uploading a file, fails until the device unlocks |
| `CompleteUntilFirstUserAuthentication` | every file of the store stays readable and writable, once the person has unlocked the device since it started; Apple's default for an app's files |
| `None` | every file of the store stays readable and writable, even before the first unlock after the device starts, though a store opens only while it is unlocked |

- Whatever the class, a step that reads or writes this device's keychain
  entries while the device is locked, such as saving a provider sign-in it
  refreshed, fails with `KeychainTemporarilyUnavailable`.
- While the device is locked, Apple refuses to give a file `Complete`, to make
  a file in a `Complete` folder, and to open a closed `Complete` or
  `CompleteUnlessOpen` file. Creating, restoring, joining, opening or replacing
  a store that needs one of these fails with `DeviceLocked`, naming the file:
  `CovenError::DeviceLocked`, `StoreCreationError::DeviceLocked` or
  `BootstrapError::DeviceLocked`, from `FileError::DeviceLocked`. The app waits
  for the device to unlock and calls again. Coven tells the lock's refusal from
  any other `EPERM` by the class of the file, or of the folder a new one goes
  in, which it reads without opening the file.
- Choosing no class after choosing one doesn't undo it: the files keep the
  class they were last given until an open chooses another. A file the walk
  can't open for writing fails the call with its `FileError`, inside
  `DbError::Disk` for an open, and so does Apple's refusal, for a few
  milliseconds after it changes a file's class, to set that file's class again
  (`ResourceBusy`). Nothing records a walk until it completes; the next call
  finishes it, leaving alone the files that already have the class. macOS takes `None` but keeps the
  file at `CompleteUntilFirstUserAuthentication`.
- Only a volume mounted with content protection keeps classes (`mount` lists
  it as `protect`). On one without, such as a disk image, Apple reports every
  file at `CompleteUntilFirstUserAuthentication` and refuses any other class.
  There a store whose app chose a class is refused before any of its files
  change: creating, restoring or joining it fails and leaves no store, and a
  writable open, a resumed restore or join and a replacement fail, even when
  `file-protection.json` names the class. The `FileError` names the store's
  directory and has an `Unsupported` cause, inside `DbError::Disk` for an
  open. Without a class, a store opens on any volume.
- Opening a store needs its declared tables ([E2](#e2-declaring-synced-tables))
  and its migrations ([E13](#e13-migrations)).
- Writable opens always migrate coven's own local tables in place ([Appendix
  C.1](coven.md#c1-covens-tables-in-the-apps-database)). Read-only opens refuse tables that need
  migrating; any open refuses an internal schema newer than it supports.
- *Progress*: a long operation reports how far it has come, so the app can draw
  a bar instead of a bare spinner ([§4](coven.md#4-writing-locally)). Each
  report is a `Progress`: the step the operation is on, and how many of the
  step's units are done out of its total, which is known as the step begins.
  Each step names its unit. Steps arrive in the order the operation runs
  them; within a step, `done` never goes backwards and never passes `total`,
  and `total` never shrinks. It grows only when work the step had no way to
  count turns up, as slots that land while a restore reads them. An operation
  that starts over, as a restore does when the floor moves past what it read,
  begins its steps again. A step's start and end are always reported, and the
  reports between them come at most four times a second.
- `CovenBuilder::open` calls its `on_progress` with each `Progress<OpenStep>`
  as the open runs: `Rebuilding`, counted in rebuilds, then `Migrating`,
  counted in migrations. It is called on the task awaiting the open, so it
  needn't be `Send`. An app that redraws more slowly than coven reports sees
  the newest report, never an older one after it; a step that starts and ends
  between two redraws may go unseen. An open with nothing to rebuild or
  migrate calls it never. A read-only open runs neither step.
- *Temporary tables* spill to files once a connection's cache fills. SQLite
  finds a folder for them everywhere but Android, where the app names one, such
  as its cache folder, with `Coven::set_temporary_directory` before the first
  store opens. Without one there, a connection whose temporary tables outgrow
  its cache fails with SQLite's `SQLITE_IOERR_GETTEMPPATH`. Coven builds SQLite to keep
  temporary tables in files unless a connection asks for memory, SQLite's own
  default, on Android too. It refuses to open with a SQLite built to keep them
  in memory unless a connection asks for files (`SQLITE_TEMP_STORE=2`), or
  whatever it asks (`=3`, libsqlite3-sys's Android default): a streamed write's
  staged rows, in coven's temporary tables, would then sit in memory whole.
  Coven also keeps a statement's journal in memory up to 124 KiB
  (`SQLITE_STMTJRNL_SPILL`, [§21.2](coven.md#212-what-else-stays-bounded)). It
  refuses to open with a SQLite built without `SQLITE_ENABLE_MEMORY_MANAGEMENT`,
  which libsqlite3-sys no longer sets itself: releasing memory would then free
  only the releasing connection's pages ([§4](coven.md#4-writing-locally)). The
  app's workspace sets coven's `LIBSQLITE3_FLAGS`, `SQLITE_DEFAULT_MEMSTATUS=0
  -USQLITE_TEMP_STORE -DSQLITE_TEMP_STORE=1 -DSQLITE_STMTJRNL_SPILL=126976
  -DSQLITE_ENABLE_MEMORY_MANAGEMENT`.
- *SQLite settings* tune the store's connections ([§4](coven.md#4-writing-locally)).
  `CovenBuilder::sqlite_settings` takes a `SqliteSettings`, whose fields are
  typed values, never PRAGMA text; a field left `None` keeps SQLite's default,
  except `checkpoint_fullfsync`, which is on for macOS and iOS
  ([§3](coven.md#3-what-coven-assumes)). Coven applies them to the writer and
  readers of a writable handle, and to every connection of a read-only one, as
  each opens and before any statement of its own. Connections coven opens to
  its own files, such as `inputs.db` beside the database, keep SQLite's
  defaults, with the same `checkpoint_fullfsync` on macOS and iOS and coven's
  own checkpoints and `journal_size_limit`.
- Coven checkpoints the WAL itself, off the commit path
  ([§4](coven.md#4-writing-locally)): the writer's own `wal_autocheckpoint` is
  always 0, and a thread of the store's own, with its own connection,
  checkpoints once the WAL holds `wal_autocheckpoint` pages not yet copied,
  1,000 when unset. It waits up to 100 ms for readers, while coven's writes
  wait. `WalAutocheckpoint::Off`
  leaves checkpoints to the app. A checkpoint that finds the database busy or
  locked, or that SQLite couldn't start because another connection was
  checkpointing or committing, is tried again at the next commit; any other
  failure fails the handle with `DbError::CheckpointFailed` until the store is
  reopened. The thread starts as the store opens and closing stops and joins
  it, so no runtime's shutdown ends it; a checkpoint that panics ends the
  process.
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
  in one of SQLite's modes: `Noop`, which only reads the frame counts,
  `Passive`, `Full`, `Restart` or `Truncate`. It
  returns what SQLite reports: `NotStarted` when SQLite read nothing of the
  WAL, because another connection was checkpointing it, or was committing to
  it as SQLite read where it ends; it gives up on that at once, without the
  busy timeout. Otherwise `Ran`: whether a reader or another writer kept it
  from finishing within the busy timeout, the frames in the WAL, and the
  frames copied into the database. After a `Restart` that wasn't busy, the
  next write starts the WAL file over from its start, so the file stops
  growing. A store created with `AutoVacuum::Incremental` gives back free
  pages only through `incremental_vacuum`.
- `release_memory()` frees the cached pages no statement is using, as SQLite's
  `sqlite3_db_release_memory` does ([§4](coven.md#4-writing-locally)). A host calls it when the OS
  warns of low memory and when the app goes to the background: on Android from `onTrimMemory`, whose
  `TRIM_MEMORY_UI_HIDDEN` and `TRIM_MEMORY_BACKGROUND` levels report the app leaving the screen and
  waiting in the background; on iOS on `didReceiveMemoryWarningNotification` and
  `didEnterBackgroundNotification`. A desktop app needn't. SQLite, as coven builds it, keeps every
  connection's unused pages in one pool for the process, which one release empties: so the call
  releases once, on an idle reader, and frees the pages of every connection, and of every store the
  process has open. It is safe at any time and never waits for a write. Later reads load what they
  need again.

Every SQLite setting, and who chooses it. Settings marked *app* are fields of
`SqliteSettings`; the rest have no field.

| Setting | Chosen by | Why, and when it applies |
|---|---|---|
| `busy_timeout` | app | how long a connection waits for a lock; unset keeps the 5 seconds rusqlite sets at open; RESTART and TRUNCATE checkpoints wait this long for readers |
| `cache_size`, `cache_spill` | app | memory each connection uses, and when a transaction writes changed pages early |
| `mmap_size` | app | reads through memory-mapped I/O; SQLite caps it |
| `temp_store` | app | where temporary tables live; set before coven creates its temporary triggers, since changing it drops temporary objects. Coven's own `inputs.db` connection keeps them in files (§21.2) |
| `threads` | app | helper threads for sorting; SQLite caps it |
| `journal_size_limit`, `wal_autocheckpoint` | app | how large the WAL file stays once a checkpoint lets it start over, unset is coven's 4 MiB; and how many pages not yet copied the WAL holds before coven checkpoints it, off the commit path, unset is 1,000; with coven's checkpoints off, the WAL grows until the app checkpoints. The writer's own `wal_autocheckpoint` is always 0, and `sqlite_settings()` reads coven's from the readers. A read-only handle's cache connection, the only one of its connections that commits, checkpoints as it commits at this size |
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
| `schema_version`, `writable_schema` | coven: untouched, and refused by defensive mode | writing them would corrupt the schema or get around coven's checks of it |
| `query_only` | coven: off | the writer commits coven's own changes; readers open read-only |
| `defer_foreign_keys` | coven: off | it lasts one transaction, and every transaction is coven's |
| `ignore_check_constraints` | coven: off | a row failing a CHECK after a merge is taken out on every device (§9.3) |
| `legacy_alter_table` | coven: off | it changes what a rename does to triggers and views, so one migration would leave different schemas on different devices |
| `case_sensitive_like` | coven: off | it changes what `LIKE` returns, so the same trigger or CHECK could decide differently on two devices |
| `encoding` | coven: UTF-8 | key comparison compares stored bytes, which differ between encodings (§9.2) |
| `max_page_count` | coven: unlimited | other members' writes would fill it, and this device could no longer apply slots |
| `lock_proxy_file` | coven: none | it changes how other processes lock the file |
| `reverse_unordered_selects` | coven: off | a testing aid that reorders results without `ORDER BY` |
| `read_uncommitted` | no effect | it affects only shared-cache connections, which coven never opens |
| `soft_heap_limit`, `hard_heap_limit`, `data_store_directory` | the process | they set every connection in the process, not one store's |
| `temp_store_directory` | app, for the process | `Coven::set_temporary_directory`, before the first store opens; an Android app names one, since SQLite finds no folder there |
| `count_changes`, `default_cache_size`, `empty_result_callbacks`, `full_column_names`, `short_column_names` | nobody | deprecated, though the bundled SQLite still runs them: `count_changes` makes a write return a row count, the column-name pragmas rename result columns, `empty_result_callbacks` changes `sqlite3_exec`'s callbacks, and `default_cache_size` writes a cache size into the file's header |
| `parser_trace`, `sql_trace`, `vdbe_*`, `activate_extensions` | nobody | they exist only in SQLite's debugging or encryption builds |
| `wal_checkpoint`, `incremental_vacuum`, `shrink_memory` | app, as calls | `checkpoint`, `incremental_vacuum` and `release_memory` on the handle |
| `optimize` | coven, as calls | coven gathers the planner's statistics at a writable open, after migrating and before the readers open, and at close, and keeps none for a table under 100 rows (§4); app writes can't run `ANALYZE` |
| `foreign_key_check` | nobody | a call coven doesn't offer |
| `integrity_check`, `quick_check` | nobody | they read every row and index; a statement that meets damage fails with `DbError::DamagedDatabase` instead (§19.3) |
| `collation_list`, `compile_options`, `database_list`, `data_version`, `foreign_key_list`, `freelist_count`, `function_list`, `index_info`, `index_list`, `index_xinfo`, `lock_status`, `module_list`, `page_count`, `pragma_list`, `stats`, `table_info`, `table_list`, `table_xinfo` | nobody | reports, not settings |
| `sqlite3_limit` limits | coven: SQLite's | a lower limit on one device would refuse values, rows and statements other devices and coven itself write |
| `sqlite3_db_config` options | coven | double-quoted string literals off; defensive mode on, so SQL that could damage the database file fails, such as writing a virtual table's own tables directly; the query planner's stability guarantee on, so the plan of every statement, coven's and the app's, never depends on the values bound to it: otherwise each use of a cached statement whose plan read a bound value parses and plans it again, once per row of a write's commit, and a `LIKE ?` gives up the index range it would take from a bound prefix; the rest at SQLite's defaults, not offered |
- *Collations* are the app's own orders for text, which its SQL names with
  `COLLATE` ([§4](coven.md#4-writing-locally)). `CovenBuilder::collation(name,
  version, compare)` adds one; `compare` takes two strings and returns their
  `Ordering`. Coven registers every collation on each connection it opens to the
  store's database, before any app SQL runs there: the writer, the readers, and
  every connection of a read-only handle. So every builder that opens the store,
  a widget's read-only one included, registers the same collations.
- An order is part of the store's schema. At one `version`, `compare` must be
  deterministic, the same on every device and in every app version that
  registers that version, and a total order. Every change to the order takes a
  new version. A writable open whose version differs from the one the store's
  indexes were built with, or that has none recorded, rebuilds every index in
  that order and records the version, in one transaction before the app's
  migrations; one whose versions match rebuilds nothing.
- A version can't change while a CHECK or generated column of any table, or a
  unique index of a synced table, uses the collation, by naming it or reading a
  column declared in it: the writable open fails with
  `DbError::VersionChangeRefused` and changes nothing. To change such an order,
  the app registers a new name, migrates to it, and keeps registering the old
  one at its version. A unique index of a local table that the new order
  makes hold two equal values fails the rebuild with SQLite's constraint error,
  and the open with it. A read-only open, which can't rebuild, fails with
  `DbError::VersionNotRebuilt` when it registers another version than the
  store's indexes were built with.
- SQLite compares collation names without regard to case. A name that is one of
  SQLite's own, `BINARY`, `NOCASE` or `RTRIM`, fails the open with
  `DbError::ReservedCollation`, and a name given twice with
  `DbError::DuplicateCollation`. SQLite refuses any statement that needs a
  collation the app didn't register, including the statistics a writable open
  gathers for a table with an index in it. A panic in `compare` ends the
  process: SQLite has no way for a comparison to fail.
- *Functions* are the app's own scalar functions, which its SQL calls by name
  ([§4](coven.md#4-writing-locally)). `CovenBuilder::function(name, arguments,
  determinism, compute)` adds one taking exactly `arguments` arguments;
  `compute` reads them from its `FunctionArguments` and returns a value that
  converts into `types::Value`, or an error that fails the statement calling
  it. `FunctionArguments` holds only the arguments: the connection running the
  statement stays coven's. Coven registers every function on the same connections as the
  collations. Every app function is marked safe for triggers and views.
- `Determinism::Deterministic { version }` lets SQLite use the function in an
  index, a partial index's condition, a CHECK and a generated column, and to
  call it once for repeated arguments; `Determinism::Nondeterministic` refuses
  it there, and has no version. A deterministic function the schema uses is
  part of the store's schema, like an order: at one version, the same arguments
  give the same result on every device and in every app version. Every change
  to its results takes a new version, which rebuilds exactly the indexes that
  call it, in their terms or condition, and is refused or required as for a
  collation. A call counts as SQLite resolves it: `x REGEXP y` calls
  `regexp`.
- A writable open whose SQLite is another version than the one that last
  opened the store for writing rebuilds every expression index, coven's own
  included, and records the version, in the transaction that rebuilds for
  the app's versions, before its migrations
  ([§4](coven.md#4-writing-locally)). SQLite 3.53 writes more digits when it
  turns a number into text, so an index on `CAST(rating AS TEXT)` that 3.52
  built misses rows. A new store records the SQLite that created it, and a
  read-only open doesn't compare it.
- SQLite compares function names without regard to case. A name SQLite or coven
  already has, or one starting with `coven_` or `_coven_`, fails the open with
  `DbError::ReservedFunction`, and a name given twice with
  `DbError::DuplicateFunction`. A deterministic function takes at least one
  argument; one that takes none is a constant, and fails the open with
  `DbError::ConstantFunction`. `regexp` is the app's to define: SQLite has
  none, and `x REGEXP y` calls `regexp(y, x)`. SQLite refuses any statement
  that calls a function the app didn't register, such as a write to a table
  with an index on it. A panic in `compute` ends the process.
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
- On Android the verifier asks the JVM. Coven starts it itself, once per
  process, before it builds the HTTPS client, from the JVM and the `Context` the
  app gives `ndk-context`, as the network watch and Block Store use them; the
  app packages the verifier's Android classes and calls nothing. A verifier
  that can't start fails the storage call with
  `StorageFailure::InvalidConfiguration`, its JNI cause kept, where an
  uninitialized verifier would have panicked at the first HTTPS connection.
- On Android, coven keeps the person's saved restore codes in Google's Block
  Store ([§13.1](coven.md#131-a-persons-new-device-the-restore-code)), calling
  Google Play services through JNI with the context the app gives `ndk-context`
  for the keystore. The app adds
  `com.google.android.gms:play-services-auth-blockstore` to its dependencies,
  and keeps its classes if it shrinks code. Without them, building the
  keychain fails with `KeyError::Unavailable` naming the missing class. On a
  phone without Google Play services, coven keeps no saved entries. Each call
  waits on Google's `Tasks.await`, which Android refuses on the main thread, so
  such a call made there fails with `KeyError::Keychain`.
- On Android, coven watches the device's network through
  `ConnectivityManager` (E5). A callback the platform calls must be a Java
  class, which JNI can't define, so coven ships one, compiled, as an Android
  library: a Maven repository in coven-foundation's `android/maven` folder, with
  `fm.coven:coven-network` in it. The app adds that folder as a repository,
  found through `cargo metadata` as the verifier's is, and the library to its
  dependencies; its manifest asks for `ACCESS_NETWORK_STATE`, which merges
  into the app's. Coven loads the class through the app's class loader, with
  the context the app gives `ndk-context`, and registers its native methods
  itself, so the app writes no JNI. Without the library, opening fails with
  `CovenError::NetworkWatch` naming the missing class. The library is
  committed, and `scripts/build-android-library.py` rebuilds it from the Java
  source beside it after that source changes; the build gives the same bytes
  for the same source.
- On Apple platforms coven links Network.framework; on Windows it uses
  `Windows.Networking.Connectivity`; on Linux it talks to NetworkManager over
  the system bus, and works without it (E5).

```rust
/// A store's UUID, independent of its name and location (E1).
pub struct StoreId(pub Uuid);

/// Circle UUID; ToSql/FromSql bind/read canonical text, as `coven_circles`
/// holds it (§15.5, E4).
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
    /// End once `duration` of elapsed time has passed: a deadline work
    /// races, such as a sign-in's wait for its callback or a stalled
    /// transfer's, or an interval, such as the idle time between passes.
    /// Controlled clocks override this to wait for their supplied time.
    fn deadline(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(duration))
    }
    /// Wait out `wait` before trying again: a backoff, a provider's
    /// cooldown, or the spacing between requests coven chose. It passes as
    /// a deadline of the same length does, which is all an app's clock
    /// needs; a test's clock may move its time to the wait's end as coven
    /// arms it, since no work races it.
    fn wait_out(&self, wait: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.deadline(wait)
    }
    /// Wait until the wall clock reads `at` or later, by running or by being
    /// set past it; a time already reached ends the wait at once, and a
    /// clock set back moves the wait's end with it. The clock itself says
    /// when it reaches `at`: nothing reads it again on a timer. A clock that
    /// can't say fails the wait (§13.2). Required: every clock answers it.
    fn sleep_until(
        &self,
        at: SystemTime,
    ) -> Pin<Box<dyn Future<Output = Result<(), WallTimerError>> + Send>>;
}

/// A clock couldn't wait for a wall-clock time (§13.2): the system refused
/// its timer, or the clock has none. Each failure keeps its cause as the
/// error's source.
#[derive(Clone, Debug)]
pub enum WallTimerError {
    /// The clock has no timer for a wall-clock time: a platform without one,
    /// or a clock that can't say when its reading reaches it.
    NoTimer,
    /// The time is past what the system's wall timer holds; `source` is the
    /// failed conversion where the timer takes the time as another integer.
    OutOfRange { source: Option<TryFromIntError> },
    /// The system couldn't create its timer source.
    TimerSource,
    /// A call that arms or waits on the system's timer failed.
    System { call: &'static str, source: Arc<std::io::Error> },
    /// An app's own clock failed to wait; `clock` builds it.
    Clock(Arc<dyn Error + Send + Sync>),
}

impl WallTimerError {
    pub fn clock(failure: impl Error + Send + Sync + 'static) -> Self;
}

/// A shared clock supplied when opening a store (Appendix F.2).
pub type ClockRef = Arc<dyn Clock>;

/// The system wall clock used unless the app supplies another (E1).
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime;
    fn elapsed(&self) -> Duration;
    /// The system's own timer for an absolute wall-clock time, which the
    /// system moves when its clock is set: a timerfd on `CLOCK_REALTIME`
    /// with `TFD_TIMER_ABSTIME` on Linux and Android, a libdispatch timer
    /// started at a `dispatch_walltime` on Apple platforms, and a waitable
    /// timer with an absolute due time on Windows. Elsewhere, as on the
    /// BSDs, the wait fails. A time at or before the epoch has passed and
    /// arms no timer.
    fn sleep_until(
        &self,
        at: SystemTime,
    ) -> Pin<Box<dyn Future<Output = Result<(), WallTimerError>> + Send>>;
}

/// Installations for application tests, built in place of `Coven::new`: an
/// isolated in-memory keychain and a network the test drives. The network
/// starts usable and unmetered; each installation has its own.
#[cfg(feature = "test-utils")]
impl Coven {
    /// An installation with its stores under `app_dir`, no device-only
    /// entries and none of the person's entries. Its bug sink is a
    /// `HeardBugs` no test can take from, so any bug coven reports to it
    /// fails the test.
    pub fn in_memory(app_dir: PathBuf) -> Self;

    /// `in_memory`, whose bugs in work nobody waits for go to `bugs`, for a
    /// test that hears them (E15).
    pub fn in_memory_reporting_to(app_dir: PathBuf, bugs: coven::task::BugSinkRef) -> Self;

    /// An installation on an Android phone: the person's entries behave as
    /// Block Store keeps them, this phone's own, with its limits (§13.1).
    pub fn with_block_store(app_dir: PathBuf) -> Self;

    /// Another device of the same person, its stores under `app_dir`: its own
    /// empty device-only keychain. It shares the person's entries with this
    /// one as iCloud Keychain would, or with Block Store has its own, empty
    /// (§13.1). It shares this one's bug sink, as do the next two.
    pub fn another_device(&self, app_dir: PathBuf) -> Self;

    /// A new phone set up from this one by direct transfer, its stores under
    /// `app_dir`: a copy of its Block Store entries. Requires `with_block_store`.
    pub fn set_up_new_device(&self, app_dir: PathBuf) -> Self;

    /// A new phone set up from this phone's last cloud backup, its stores
    /// under `app_dir`: the Block Store entries it holds. Requires
    /// `with_block_store`.
    pub fn restore_new_device_from_cloud(&self, app_dir: PathBuf) -> Self;

    /// Google's periodic cloud backup of this phone's Block Store: the entries
    /// stored while end-to-end encryption was available.
    pub fn back_up_to_cloud(&self);

    /// Whether this phone's cloud backup would be end-to-end encrypted: it has
    /// a screen lock. A new installation starts with one.
    pub fn set_end_to_end_encryption(&self, available: bool);

    /// Create through the production builder, then close for a reopening fixture.
    pub async fn create_closed_store(
        &self,
        name: &str,
        ids: IdSourceRef,
    ) -> Result<StoreDir, StoreCreationError>;

    /// Publish a directory fixture without a database or member custody.
    pub fn create_store_dir(
        &self,
        id: StoreId,
        ids: &dyn IdSource,
    ) -> Result<StoreDir, DirectoryCreationError>;

    /// The device moves to another usable network: a network report (E5).
    pub fn join_network(&self, metered: bool);

    /// The device loses its network: no report.
    pub fn lose_network(&self);

    /// The platform fails to say what the device's network is: coven can't
    /// tell, and calls it metered, until the test says again.
    pub fn network_cannot_tell(&self);

    /// The device's network turns metered or unmetered: no report.
    pub fn set_network_metered(&self, metered: bool);

    /// Seed the simulated OS custody as if this installation had opened keys.
    pub fn keep_store_keys(
        &self,
        directory: &StoreDir,
        keys: &StoreKeyring,
    ) -> Result<(), KeyError>;

    /// Delete this installation's storage credentials for one store, as a
    /// keychain entry deleted by hand: the location stays recorded, and the
    /// store opens with no storage set up on this device (Appendix F.4).
    pub fn lose_storage_credentials(&self, store: StoreId) -> Result<(), KeyError>;

    /// Refuse the next keychain operation before changing any stored value.
    pub fn fail_next_keychain_operation(&self);
}

/// Application storage fixtures, available with coven's test-utils feature.
/// MemoryStorage implements StorageConnector: pass Arc<MemoryStorage> to the builder.
/// Its builder configures the provider location, clock, transfers and listing pages.
/// Clones retain objects; for_device gives another connection to those objects.
/// Faults and BodyFailure inject request and response-body failures, and
/// MemoryRequest records the attempted calls. Storage supplies reads and listings.
/// hold_creations, gate_creations, gate_parts and gate_reads take the test's
/// barrier or semaphore, which holds each create, part or read until the test
/// releases it.
/// The hold_next_* calls and hold_body_at stop one request, or one body at
/// an offset, tell the test it arrived, and wait for the test to resume it.
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

    /// How many deadlines and waits have begun on this clock so far.
    pub fn sleeps_begun(&self) -> u64;

    /// Wait until a deadline or wait begins after the first `seen`, and
    /// return how many have begun and the latest one's end by `elapsed`, so
    /// a test can set the clock to exactly the wait the code armed.
    pub fn sleep_begun_after(&self, seen: u64)
        -> impl Future<Output = (u64, Duration)> + Send + 'static;
}

/// A `FixedClock` whose backoffs pass as the code arms them: `wait_out` moves
/// the fixed clock to the wait's end. Deadlines and idle intervals end only
/// when the test moves the clock, as does every wait once held.
#[cfg(feature = "test-utils")]
pub struct PollingClock(/* private */);

#[cfg(feature = "test-utils")]
impl PollingClock {
    /// A polling clock over `clock`, which it moves.
    pub fn new(clock: Arc<FixedClock>) -> Self;

    /// End later waits by the test's moves of the fixed clock alone.
    pub fn hold_waits(&self);

    /// Pass later waits as the code arms them again.
    pub fn release_waits(&self);

    /// Whether waits are held.
    pub fn holds_waits(&self) -> bool;

    /// Each backoff, cooldown or spacing the code armed, in order.
    pub fn waited(&self) -> Vec<Duration>;
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
    /// Ends when a setting advances elapsed time past it; `wait_out` is a
    /// deadline here too.
    fn deadline(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
    /// Ends when a setting reaches `at`; settings before it, forward or
    /// back, leave it waiting.
    fn sleep_until(
        &self,
        at: SystemTime,
    ) -> Pin<Box<dyn Future<Output = Result<(), WallTimerError>> + Send>>;
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
    /// the sign-in's custody (§20.2). The connection writes for `device`. A
    /// setup call makes a connection of its own beside it for its requests
    /// (§20.8).
    async fn connect(
        &self,
        config: StorageConfig,
        auth: coven_storage::ProviderAuth,
        device: DeviceId,
        publications: Arc<dyn coven_storage::PublicationGate>,
        cooldown: Arc<dyn coven_storage::Cooldown>,
        transfers: Arc<coven_storage::TransferBudget>,
    ) -> Result<Arc<coven_storage::StorageConnection>, StorageError>;
    /// Probe an S3 location's service (§20.7), writing only fresh objects under
    /// the store's `.coven-check/<run>/` prefix for `runs.run` and deleting
    /// them, also on failure, then removing what each of `runs.earlier`
    /// left there. The caller recorded `runs.run` before calling; the answer
    /// names the runs whose probes and uploads are confirmed gone.
    /// A connector without native S3 refuses this capability. A rate-limited
    /// probe is sent again while setup's `waits` allow (§20.8).
    async fn detect_s3(
        &self,
        _config: StorageConfig,
        _credentials: S3Credentials,
        _store: StoreId,
        runs: &S3DetectionRuns,
        _waits: &SetupWaits,
    ) -> S3DetectionRun {
        S3DetectionRun::unwritten(runs, Err(StorageFailure::ProviderUnavailable.into()))
    }
    /// Before a store's first setup at a location it needs to itself (a
    /// Dropbox shared folder, a OneDrive folder, or an S3 bucket), refuse a
    /// location whose root holds anything outside the store's folder or the
    /// `coven-location` marker (§20.3).
    /// Reads only. A connector without that provider refuses this capability.
    /// A rate-limited listing is sent again while setup's `waits` allow.
    async fn check_new_store(
        &self,
        _config: StorageConfig,
        _auth: ProviderAuth,
        _store: StoreId,
        _waits: &SetupWaits,
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

/// This app's stores on this device: the app directory each has its own
/// directory under, the OS keychain service their keys and secrets are kept
/// under, and the device's network (E1).
#[derive(Clone)]
pub struct Coven { /* private fields */ }

/// A store on this device (E1).
pub struct StoreInfo {
    /// The store's identity.
    pub id: StoreId,
    /// The current name from this device's committed database state. While
    /// a replacement is unfinished, the name its new installation loaded.
    pub name: String,
    /// The work that must finish before the store opens; `None` when none is.
    pub unfinished: Option<Unfinished>,
}

/// Work on a store that must finish before it opens (E1).
pub enum Unfinished {
    /// A replacement of this device's installation, which the call it names
    /// finishes, or `CovenBuilder::resume_replacement` (§19.3).
    Replacement(ReplacedBy),
    /// This device is deleting the store and leaving it, its removal
    /// journaled or about to be: `CovenBuilder::resume_deletion` finishes it
    /// (§11.6).
    Deletion,
}

/// The call that began a replacement, and the only call that finishes it (§19.3).
pub enum ReplacedBy {
    /// `CovenBuilder::reset_device`, which keeps the old copy for inspection.
    Reset,
    /// `CovenBuilder::restore_device`, which commits the restore code's member
    /// keys and removes the old copy (§13.1).
    Restore,
}

/// The opening choices for one store of a `Coven` (E1).
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

    /// A stored object: encrypted file bytes named by a random id (§17.5).
    pub fn file(store: StoreId, device: DeviceId, object: ObjectId) -> Self;

    /// An encrypted join request under its invite id (§13.2).
    pub fn join_request(store: StoreId, invite: InviteId) -> Self;

    /// The invite named by a join request's path.
    pub fn join_request_invite(&self) -> Option<InviteId>;

    /// The store whose objects this path names.
    pub fn store(&self) -> StoreId;

    /// The device named by a snapshot or uploaded file path.
    pub fn device(&self) -> Option<DeviceId>;

    /// The stored object's id, the last part of a stored file's path.
    pub fn object_id(&self) -> Option<ObjectId>;

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

    /// The folder this prefix names, without its closing slash, as a
    /// provider with folders names it.
    pub fn folder(&self) -> &str;

    /// All objects belonging to this store.
    pub fn all(store: StoreId) -> Self;

    /// Every stored file.
    pub fn files(store: StoreId) -> Self;

    /// The files one device uploaded (§17.5).
    pub fn device_files(store: StoreId, device: DeviceId) -> Self;

    /// Every waiting join request.
    pub fn join_requests(store: StoreId) -> Self;

    /// The store whose objects this prefix selects.
    pub fn store(&self) -> StoreId;

    /// Whether a validated path is under this prefix.
    pub fn contains(&self, path: &ObjectPath) -> bool;
}

/// A nonempty byte range, including its start and excluding its end (§17.7).
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

/// A provider time: Unix seconds and a nanosecond fraction. It keeps
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
    /// A sync step failed inside an app's write, which did not commit, or
    /// while the store opened, keeping sync's own error (E1, E3).
    Sync(SyncError),
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
    /// The call's runtime is shutting down and dropped coven's work for it
    /// unstarted, such as opening's custody work; a dropped database call is
    /// `DbError::Shutdown` (E15).
    Shutdown(Shutdown),
    /// Coven's work for the call panicked, such as opening's custody work or
    /// a sync step: a bug, which the caller reports; a database call that
    /// panicked is `DbError::Bug` (E15).
    Bug(Bug),
    /// The platform refused to let coven watch the device's network, such as
    /// an Android app without coven's Android library (E1, E5).
    NetworkWatch(NetworkWatchError),
    /// The device is locked, and the class the app chose keeps the store's
    /// files closed until it unlocks; the app waits for the unlock and opens
    /// again (E1).
    DeviceLocked { path: PathBuf },
}

/// Why coven can't watch the device's network (E5). Each failure keeps the
/// platform's own error as its source.
#[derive(Clone, Debug)]
pub enum NetworkWatchError {
    /// The platform's path monitor couldn't be created.
    MonitorUnavailable,
    /// The platform's path monitor ended before it described the network.
    MonitorEnded,
    /// A call to the platform's network interface failed.
    Platform { call: &'static str, source: Arc<dyn Error + Send + Sync> },
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
    /// The call's runtime is shutting down and dropped its work (E15): a
    /// call changed nothing (`NotRun`); closing may have stopped partway
    /// (`Unknown`).
    Shutdown(Shutdown),
    /// The call's work panicked: a bug in coven or in the app's callback,
    /// which the caller reports (E15). A write that panicked rolled back,
    /// and the bytes it released or staged are removed as after any failed
    /// write. One that panicked after it committed, telling its live
    /// queries, kept its changes and failed the handle with the bug. A bug
    /// a drop in the call's work found is this call's too.
    Bug(Bug),
    /// An earlier bug may have left this handle's state wrong, so the
    /// handle failed with it, and every call and live query fails with it
    /// until the store is reopened (E15). The call whose work hit the bug
    /// returned it as `Bug`, or the bug sink heard it, so the app shows this
    /// and doesn't report it again.
    StoreFailed(Bug),
    /// SQLite refused or failed a statement, preserving its error.
    Sqlite(rusqlite::Error),
    /// A statement met damage SQLite reports (`SQLITE_CORRUPT`,
    /// `SQLITE_NOTADB`), decoding stored facts found damage (§19.3), or a
    /// snapshot's staged records differ from a second staging's (§19.1).
    DamagedDatabase,
    /// Coven's own code broke a rule it relies on, such as a caller's contract
    /// or a step its state machine cannot reach: a bug, never stored damage.
    /// Sync reports it as `Failed(LocalFailure::Bug)` (§19.1).
    Invariant { what: &'static str },
    /// The directory's store id disagrees with the creation entry when it
    /// is prepared, applied or read at opening (§10). Names are not compared.
    StoreSettingsMismatch { store: StoreId },
    /// A synced table declaration or schema is invalid.
    Schema(SchemaError),
    /// A write was given files staged, or a pin prepared, in another open
    /// store, or in this one before it closed: the copies they name aren't
    /// reserved here (§17.3, §17.8).
    PreparedElsewhere,
    /// A write made a file device-only and also removed its last reference,
    /// so the copy kept as its original would name no file (§17.3). Nothing
    /// committed.
    DeviceOnlyUnreferenced { file: FileId },
    /// A pin prepared over a whole copy already kept found that copy gone
    /// when its write committed it, evicted since: nothing was pinned, and
    /// preparing again downloads it (§17.8).
    PinChanged { file: FileId },
    /// One of coven's steps in this write failed, and the write's closure
    /// went on to return success: the write rolls back, since a failed step
    /// may have made only part of its change (§4).
    StepFailed,
    /// One of the app's statements in this write failed, and the write's
    /// closure went on to return success: the write rolls back, since SQLite
    /// may have kept part of the statement's change or undone rows coven saw
    /// it change (§4).
    StatementFailed,
    /// App SQL read, changed or defined one of coven's internal objects (§4).
    InternalTable { table: String },
    /// App SQL tried transaction control, a PRAGMA, ATTACH, loading an
    /// extension, changing a hidden rowid, or changing the schema outside a
    /// migration (§4, E13).
    StatementForbidden { operation: &'static str },
    /// SQLite rolled the owned transaction back itself, so it can't go on (E3, E13).
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
    /// A writable open found a new version of an app collation or function
    /// that a CHECK, a generated column or a synced table's unique index of
    /// `table` uses; it changed nothing (§4, E1).
    VersionChangeRefused { name: String, table: String },
    /// A read-only open registers another version of an app collation or
    /// function than the store's indexes were built with; only a writable
    /// open rebuilds them (§4, E1).
    VersionNotRebuilt { name: String, built: u32, registered: u32 },
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
    /// A local trigger wrote a synced table or a shared trigger a local one,
    /// including a trigger on `coven_files`, `coven_pins`,
    /// `coven_pin_requests`, `coven_moves` or `coven_upload_queue` and those
    /// it fires (§9.4).
    TriggerTarget { trigger: String, table: String },
    /// A bulk write's statement, or a trigger or foreign-key action it would
    /// run, writes a synced table (§4, E3). Nothing committed.
    SyncedTableInBulkWrite { table: String },
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
    /// A write dismissed a removed row, deleting it for good, and also
    /// changed that row or one its delete cascades to (§8.4).
    DismissedRowChanged { table: String, key: RowKey },
    /// A write changed a write-once file reference that named a file (§17.2).
    FileWriteOnce { table: String, key: RowKey },
    /// The user's file is no longer at the recorded path (§17.1).
    UserFileMissing { path: PathBuf },
    /// The user's file changed during preparation or since it was prepared (E3).
    UserFileChanged { path: PathBuf },
    /// No namespace of this name was declared when the store opened (E1).
    UndeclaredNamespace { namespace: String },
    /// A file was staged into a namespace of the other kind: app bytes into a
    /// user namespace, or a user's original into an app one (§17.1); or a
    /// bound reference names a user's file (§17.4).
    NamespaceKind { namespace: String, kind: Provenance },
    /// The id source reused the physical name of a kept file.
    FileNameReused { name: FileName },
    /// A file the write staged is referenced by none of its rows (§17.2).
    FileUnreferenced { namespace: String, file: FileId },
    /// A reference names no file this device has a record of in its row's
    /// audience (§17.2).
    FileUnknown { table: String, column: String, file: String },
    /// This device has no record of the file (§17.2): one to move, to make
    /// device-only, or to stage a new file over.
    FileAbsent { file: FileId },
    /// The file is on another device only, and only that device has its
    /// bytes: one to move to the cloud (§17.3), or one a write makes a new
    /// file over (§17.2).
    NotThisDevice { file: FileId },
    /// The file to move is in the cloud already (§17.3).
    NotDeviceOnlyHere { file: FileId },
    /// The file to move is in a move already, moving or failed, which `id`
    /// names: a file is in one move at a time, and the app cancels a failed
    /// move to move its files again (§17.3).
    FileInMove { file: FileId, id: MoveId },
    /// A bound reference names a file larger than its limit (§17.4).
    BoundFileTooLarge { table: String, column: String, size: u64, limit: u64 },
    /// A bound reference names a file on one device only that no bound
    /// upload stores (§17.4).
    DeviceOnly { file: FileId },
    /// This device never made the move (§17.3).
    MoveUnknown { id: MoveId },
    /// The move finished: its files are in the cloud (§17.3).
    MoveFinished { id: MoveId },
    /// The move is a bound file's: its upload holds a write until the file
    /// is stored, so it can't be cancelled (§17.4).
    MoveBound { id: MoveId },
    /// The file to make device-only is on one device only already, so
    /// storage holds no bytes of it to download (§17.3).
    NotInCloud { file: FileId },
    /// No record here names the stored bytes a file reference was taken
    /// over any more: the record went, or a switch took it off them, while
    /// a pin downloaded them. The cache keeps none of them (§17.8).
    NoLongerStored { file: FileId },
    /// Another call making files device-only is writing a copy at this path
    /// (§17.3).
    CopyPathTaken { path: PathBuf },
    /// Reading or keeping file bytes failed (E3).
    Disk(FileError),
    /// The staging write could not create its file's random key (§17.3).
    FileKey(CryptoError),
    /// Retaining the store while staging file bytes failed.
    Lock(StoreLockError),
    /// Removing owned bytes or their records failed: on opening (`Ok`), or
    /// after a failed write, whose error it retains (Appendix C.3).
    FileCleanup { write: Result<(), Box<DbError>>, failures: Vec<DbError> },
    /// A snapshot load or file check failed, and so did removing the records
    /// it staged, which its failure ends (§16.2); both failures are kept,
    /// and sync judges the load's or check's own as it would alone (§19.1).
    StagingCleanup { operation: Box<DbError>, cleanup: Box<DbError> },
    /// A transaction failed and rolling it back failed too. Later calls fail
    /// with `RollbackFailed` until the store is reopened (§4).
    Rollback { operation: Box<DbError>, rollback: rusqlite::Error },
    /// An earlier call's rollback failed, so a connection may still be inside
    /// that transaction, holding its uncommitted changes or its old snapshot;
    /// this handle, read-only or not, refuses calls until reopened (§4).
    RollbackFailed,
    /// Saving pending state or the sync status failed. The call that lost
    /// the update and every later call and live query fail with this same
    /// cause until the store is reopened (E5).
    StatePersistence(Arc<DbError>),
    /// One of coven's own WAL checkpoints failed with this cause, other than
    /// on a busy or locked database, so the store's checkpointing stopped.
    /// Rather than stay open while its WAL grows without bound, this handle
    /// and every live query fail with this same cause until reopened (§4).
    CheckpointFailed(Arc<DbError>),
    /// The handle's clock could not wait for an open invite's expiry, so
    /// the invite would outlive its lifetime. Rather than stay open
    /// without writing expiries, this handle and every live query fail with
    /// this same cause until reopened, and opening writes each expiry
    /// already past (§13.2).
    WallTimer(WallTimerError),
    /// The system refused the thread that checkpoints the store's WAL, so
    /// the store did not open (§4).
    CheckpointerThread(std::io::Error),
    /// Reading a claim's sealed bytes to retain them failed, and the claim's
    /// transaction did not commit (§11).
    ClaimRead(std::io::Error),
    /// A pending record, its subject, the sync status or an operation's data
    /// could not be encoded, or a stored pending record could not be decoded
    /// as the requested record type (E5).
    StateEncoding(serde_json::Error),
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

    /// Original row-change evidence is unavailable. Receipt waits without
    /// consuming the write; local authoring rolls back instead of guessing.
    MissingWriteSources(Vec<coven_format::write_sources::ChangeId>),

    /// An operation no longer has the state expected by its caller.
    OperationChanged(OperationId),

    /// The operation a write acknowledges is still running (§11.4).
    OperationRunning(OperationId),

    /// `coven_operations` lists no such operation: no app write journaled
    /// it, or a write acknowledged its outcome already (§11.4).
    OperationUnknown(OperationId),

    /// The prepared raise no longer names the current counted prefix.
    CaptureChanged,
}

/// Why snapshots or their gap writes could not be checked or loaded (§16.5, §19.2).
pub enum SnapshotError {
    /// Reading or checking a selected snapshot's plaintext failed. `staged`
    /// is the digest of its staged records as the failed read left them,
    /// which sync compares with a second staging's to tell damage on this
    /// device from the snapshot's own (§19.1).
    Body { snapshot: SnapshotId, error: Box<DbError>, staged: Option<StagedDigest> },
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
    /// Another member's write the snapshot carries does not authenticate:
    /// its header does not open with the key its prefix names, or its
    /// signature does not verify (D9).
    Unauthentic(CryptoError),
    /// Another member's write the snapshot carries names a device whose
    /// member, at the write's read-through, could not write every part it
    /// has (D9).
    Unauthorized,
}

/// A digest of one snapshot staging's records as its tables hold them. Two
/// stagings of the same snapshot hold the same records, so they differ only
/// where this device's disk changed one (§19.1).
pub struct StagedDigest(/* private */);

/// What a downloaded write waits for before it can apply (§5.4, §19.1).
pub enum WriteWait {
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
    /// Two namespace declarations share a name (E1).
    DuplicateNamespace { namespace: String },
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
    /// A column is declared a file reference twice (E2).
    FileColumnRepeated { table: String, column: String },
    /// A file reference column is generated, so coven can't write it (E2).
    FileColumnGenerated { table: String, column: String },
    /// An SQL foreign key references `coven_files`, which only declared
    /// file columns reference (§17.2, E2). `key` names the referencing
    /// columns and referenced table and columns as SQL.
    FilesForeignKey { table: String, key: String },
    /// A file reference column is also an SQL foreign key (E2).
    FileColumnForeignKey { table: String, column: String },
    /// A synced table's file reference columns changed without a breaking
    /// migration to raise the schema (§18.1).
    FileColumnsChanged { table: String },
    /// A trigger declared shared isn't on the table (§9.4).
    MissingTrigger { table: String, trigger: String },
    /// A synced table has no primary key (§9.2).
    NoPrimaryKey { table: String },
    /// A primary key column of a synced or device-local table allows NULL
    /// (§9.2).
    NullableKey { table: String, column: String },
    /// A foreign key includes a generated column (§9.1).
    GeneratedForeignKey { table: String, column: String },
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
    /// The audience root column is nullable, is not text, or is generated
    /// (§15, E2).
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
    /// A trigger on `coven_files`, `coven_pins`, `coven_pin_requests`,
    /// `coven_moves`, `coven_upload_queue`, `coven_sync_status` or
    /// `coven_retry_effect` runs before its change (§9.4).
    TriggerTiming { trigger: String, table: String },
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
    /// The device is locked, and the file's protection class keeps it from
    /// being made, opened or given its class until it unlocks (E1).
    DeviceLocked { path: PathBuf },
    /// The operation's runtime is shutting down and dropped it unstarted (E15).
    Shutdown(Shutdown),
    /// The operation panicked on the blocking pool: a bug (E15).
    Bug(Bug),
}

impl FileError {
    /// Whether readers already see the replacement despite a durability error.
    pub fn installed_new_bytes(&self) -> bool;
}

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

/// Copy the tree at `from` into `to`, making `to` and every folder below
/// it, as restoring a backup of an installation does. Panics on failure.
#[cfg(feature = "test-utils")]
pub fn copy_tree(from: &Path, to: &Path);

/// Copy the tree as `copy_tree` does, with only the files whose name
/// `keep` accepts.
#[cfg(feature = "test-utils")]
pub fn copy_tree_keeping(from: &Path, to: &Path, keep: &dyn Fn(&str) -> bool);

/// Told once file work reaches `path`: about to open it, or finding its
/// lock held and about to wait for it. A test that stalls the file, as a
/// FIFO in its place does, then knows the work is held there.
#[cfg(feature = "test-utils")]
pub fn when_reached(path: &Path) -> tokio::sync::oneshot::Receiver<()>;

/// The lock file a store's cache range writes share and its opening takes
/// alone (Appendix C.3), for a test to wait on with `when_reached`.
#[cfg(feature = "test-utils")]
pub fn cache_writes_lock(store: &Path) -> PathBuf;

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
    /// Reading or writing the settings panicked: a bug (E15).
    Bug(Bug),
}

/// The requested writer, reader or deletion lock could not be taken (E1).
pub enum StoreLockError {
    /// Another live instance in this process, or another process, holds the
    /// lock: opening, bootstrapping or discarding returns this error
    /// immediately. If this process's instance is closing, opening,
    /// bootstrapping or discarding the same store waits until its last
    /// blocking work finishes and its lock handles close, then tries to take
    /// the lock. Discarding waits so for a closing read-only handle too;
    /// opening and bootstrapping share the store with readers and don't wait
    /// on one. The wait uses release notifications, with
    /// no timeout or polling, keyed by canonical directory and store id.
    AlreadyOpen(StoreId),
    /// An unfinished first installation must be resumed by the call that
    /// began it: opening refuses it, and so does a restore on a join's
    /// installation or a join on a recorded restore's, changing nothing.
    BootstrapPending(StoreId),
    /// A replacement of this device's installation recorded itself, just
    /// before its fresh device registered, and did not finish. Opening, read
    /// only too, and the other of `reset_device` and `restore_device`, are
    /// refused before anything changes; only `by`, or
    /// `CovenBuilder::resume_replacement`, finishes it, or
    /// `Coven::discard_store` deletes the store from this device (§19.3).
    ReplacementUnfinished { store: StoreId, by: ReplacedBy },
    /// This device is deleting the store and leaving it: its deletion
    /// recorded itself, just before it journaled the device's removal, and
    /// did not finish. Opening, read only too, and replacing the store are
    /// refused before anything changes; `CovenBuilder::resume_deletion`
    /// finishes it, or `Coven::discard_store` deletes the store from this
    /// device alone (§11.6).
    DeletionUnfinished(StoreId),
    /// A supplied lock protects a different directory.
    WrongDirectory(StoreId),
    /// Opening or locking the lock file failed, or did not run because its
    /// runtime is shutting down (`FileError::Shutdown`).
    File(FileError),
    /// Taking the lock panicked on the blocking pool: a bug (E15).
    Bug(Bug),
}

/// Opening a read-only store or its saved file-storage connection failed (E1).
pub enum ReadOnlyOpenError {
    /// The database or store directory could not be opened.
    Local(CovenError),
    /// Saved settings, credential custody or provider construction failed.
    Storage(SyncError),
    /// Opening panicked: a bug (E15).
    Bug(Bug),
}

/// A store on this device as the listing found it (E1).
pub enum ListedStore {
    /// Its name and state read.
    Readable(StoreInfo),
    /// Its replacement record or name can't be read, for `reason`. The app
    /// shows it as damaged and can offer `discard_store` for it.
    Unreadable { id: StoreId, reason: CovenError },
}

impl ListedStore {
    /// The store this entry lists.
    pub fn id(&self) -> StoreId;
}

/// Listing the app's stores failed (E1). A store that can't be read is not a
/// failure: it is listed as `ListedStore::Unreadable`.
pub enum StoreListError {
    /// Listing directories or reading settings failed.
    File(FileError),
    /// The runtime is shutting down and dropped the listing's work (E15).
    Shutdown(Shutdown),
    /// Listing the stores panicked on the blocking pool: a bug (E15).
    Bug(Bug),
    /// Finishing the recorded deletion of `store` failed. The store is not
    /// listed; the next listing finishes its deletion (§11.6).
    Deletion { store: StoreId, source: StoreDeletionError },
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
    /// The device is locked, and the class the app chose keeps the new
    /// store's files from being made until it unlocks. No store was made;
    /// the app waits for the unlock and creates it again (E1).
    DeviceLocked { path: PathBuf },
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
    /// Creation's runtime is shutting down and dropped its work (E15). With
    /// `Unknown` it may have stopped before or after publishing the store;
    /// listing the stores shows which.
    Shutdown(Shutdown),
    /// Creation panicked: a bug (E15). As with `Shutdown`, listing the
    /// stores shows whether it published the store.
    Bug(Bug),
}

/// The folder for temporary tables could not be named (E1).
pub enum TemporaryDirectoryError {
    /// A store's database already opened in this process.
    AfterOpening,
    /// Another folder was named already.
    AlreadyNamed { directory: PathBuf },
    /// Not an existing folder, or not UTF-8, or it contains a NUL.
    NotAFolder { directory: PathBuf },
}

/// Deleting the local store stopped at a step the caller may retry (E1).
pub enum StoreDeletionError {
    /// The store is open or its lock could not be taken.
    Lock(StoreLockError),
    /// Writes or files wait to upload, as `unsent_work` counts them;
    /// nothing was deleted.
    Unsent(UnsentWork),
    /// Reading the queue, connecting or journaling this device's removal
    /// failed before the removal was journaled: nothing was deleted, nothing
    /// in storage changed, and the store is as it was.
    Sync(SyncError),
    /// A location is recorded and this device holds no credentials for it,
    /// so it can't publish its removal: the person signs in, then deletes
    /// again. Before the removal was journaled nothing changed; after, the
    /// deletion stays recorded.
    SignInRequired(CloudProvider),
    /// This device's removal is journaled, and the deletion failed before
    /// it applied. The handle is closed, the store lists as being deleted
    /// and refuses opens, and `CovenBuilder::resume_deletion` finishes it.
    Unfinished(SyncError),
    /// This device's removal applied, or no storage was set up, and closing
    /// the handle failed, deleting nothing. `CovenBuilder::resume_deletion`
    /// finishes a recorded deletion, and `Coven::discard_store` either.
    Closing(SyncError),
    /// Opening the store to finish its recorded deletion failed.
    Open(CovenError),
    /// `resume_deletion` found no deletion of the store recorded here.
    NoUnfinishedDeletion(StoreId),
    /// Removing a keychain entry failed.
    Key(KeyError),
    /// Removing the store directory failed.
    File(FileError),
    /// The deletion's runtime is shutting down and dropped it unstarted;
    /// nothing was deleted (E15).
    Shutdown(Shutdown),
    /// The deletion panicked: a bug (E15). Retrying finishes a deletion
    /// that stopped partway.
    Bug(Bug),
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
    /// The service name is empty or contains NUL.
    InvalidServiceName,

    /// The person's entries need iCloud Keychain or Block Store: Apple
    /// platforms, or an Android phone with Google Play services.
    Unsupported,
    /// Block Store already holds its 16 entries for the app (§13.1).
    SavedEntriesFull,
    /// A saved code doesn't fit in a Block Store entry, even with the store's
    /// name shortened (§13.1).
    SavedEntryTooLarge,
    /// The person left the store: their saved entries were deleted, and
    /// this installation refuses to write them again (§14.1).
    Left,

    /// A saved restore-code account's id part is not a store id as coven
    /// writes it.
    RestoreCodeStoreId,
    /// The call's runtime is shutting down and dropped it partway (E15).
    Shutdown(Shutdown),
    /// The call's custody work panicked: a bug (E15). What it changed in
    /// custody is unknown.
    Bug(Bug),
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

/// The two places coven keeps file bytes inside a store (E1).
pub enum FileArea {
    /// Coven's own copies of app-provided files (§17.1).
    AppProvided,
    /// The cache of uploaded files and chunks (§17.8).
    Cache,
}

impl StoreDir {
    /// The store identified by this directory.
    pub fn id(&self) -> StoreId;
    /// The folder of the store's file `area`, for a test that looks at the
    /// files coven keeps there.
    #[cfg(feature = "test-utils")]
    pub fn area_path(&self, area: FileArea) -> PathBuf;
}

impl Coven {
    /// The stores under `app_dir`, their keys and secrets in the OS keychain
    /// under the service `keyring_service`, watching the platform's network.
    /// The app creates `app_dir`; coven creates directories only inside it
    /// and never opens a directory above it, which Android refuses. No key is
    /// read here. A bug in work nobody waits for, in any store or in this
    /// `Coven`'s own calls, goes to `bugs`, the app's crash reporter (E15).
    /// An empty service name, or one containing NUL, fails with
    /// `KeyError::InvalidServiceName`; an OS keychain coven can't reach
    /// fails with its `KeyError`.
    pub fn new(
        app_dir: PathBuf,
        keyring_service: &str,
        bugs: coven::task::BugSinkRef,
    ) -> Result<Self, KeyError>;

    /// The stores on this device, ordered by id, with their committed names.
    /// Reads names from their databases. A store whose replacement record or
    /// name can't be read is listed as `Unreadable` with why; no older name is
    /// substituted.
    /// A store whose replacement or deletion is unfinished is listed with the
    /// work that must finish; an unfinished first installation is not listed.
    /// First it finishes every deletion from the device that a cut call left
    /// under the directory's deletion name and no call holds; a failure there
    /// is `StoreListError::Deletion` (§11.6).
    pub async fn stores(&self) -> Result<Vec<ListedStore>, StoreListError>;

    /// The directory of the store with `id`.
    pub fn store_dir(&self, id: &StoreId) -> StoreDir;

    /// Configure creating, opening, restoring or joining a store.
    pub fn builder(&self) -> CovenBuilder;

    /// Read the recorded location without opening the store or its custody (E1).
    pub async fn storage_config(store_dir: &StoreDir) -> Result<Option<StorageConfig>, SyncError>;

    /// Keep SQLite's temporary tables in `directory`, such as the app's cache
    /// folder, for every store this process opens. Called before the first
    /// store opens; naming the same folder again is allowed (E1, §21.2).
    pub fn set_temporary_directory(directory: &Path) -> Result<(), TemporaryDirectoryError>;

    /// Deletes a closed store without opening it: it renames the store's
    /// directory to its deletion name, which hides the store and records the
    /// deletion, then deletes this device's own keychain entries for it, then
    /// the directory and its lock files. The person's saved restore code and
    /// S3 key stay for their next devices (§13.1). Unsent
    /// work is lost; membership stays until this device is removed. For a
    /// store that cannot open, or to finish `CovenHandle::delete_store`. A
    /// store whose `reset_device` or `restore_device` stopped past its record
    /// is deleted whole: both installations, the replacement's directory and
    /// record, and this device's entries, whichever device they name by then.
    /// Refused while a writer, read-only handle, file stream, file I/O or a
    /// running replacement holds its lock; storage is untouched. A handle in
    /// this process that is closing, or whose last clone was dropped, is
    /// waited for first, as opening waits for it. A deletion that stopped
    /// part way, a crash included, is finished by retrying, by the next
    /// `stores()`, or by a restore or join of the store (§11.6).
    pub async fn discard_store(&self, store_dir: &StoreDir) -> Result<(), StoreDeletionError>;
}

impl CovenBuilder {
    /// The tables that sync (E2). Required.
    pub fn synced_tables(self, tables: Vec<SyncedTable>) -> Self;

    /// The file namespaces: each kind of file, with its kind and the cache
    /// budget it starts with (§17.1, §17.8). Staging a file into an
    /// undeclared namespace fails with `UndeclaredNamespace`; declaring one
    /// twice fails opening with `SchemaError::DuplicateNamespace`.
    pub fn namespaces(self, namespaces: Vec<Namespace>) -> Self;

    /// The app's schema migrations, numbered from 1 with no gaps (E13).
    /// Required.
    pub fn migrations(self, migrations: Vec<Migration>) -> Self;

    /// The wall clock that timestamps use (§7.2). Defaults to the system clock.
    pub fn clock(self, clock: ClockRef) -> Self;

    /// Replace OS-random ids in tests (Appendix F.2).
    #[cfg(feature = "test-utils")]
    pub fn id_source(self, ids: IdSourceRef) -> Self;

    /// Replace the platform's network with one the test drives (E5).
    #[cfg(feature = "test-utils")]
    pub fn network(self, network: TestNetwork) -> Self;

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

    /// How many file downloads run at once, pins' and file ranges' (E5, E8).
    /// Defaults to one. Reading the sequence doesn't count against it.
    pub fn max_concurrent_downloads(self, n: NonZeroUsize) -> Self;

    /// Where this device keeps the store keys: the OS keychain by default, a
    /// file sealed with a passphrase, memory for this session only, or the
    /// app's own `Custody<StoreKeyring>`.
    pub fn key_custody(self, custody: CustodyChoice<StoreKeyring>) -> Self;

    /// Where this device keeps its member's keys, with the same four choices
    /// and the app's own `Custody<MemberIdentity>` as the last.
    pub fn identity_custody(self, custody: CustodyChoice<MemberIdentity>) -> Self;

    /// SQLite settings for every connection a handle opens to the store's
    /// database. Unset ones keep SQLite's defaults; the page size and
    /// auto-vacuum mode apply only when the database is created.
    pub fn sqlite_settings(self, settings: SqliteSettings) -> Self;

    /// Apple's protection class for the store's files. Creating, restoring,
    /// joining or replacing a store gives each new installation's directory
    /// the class before anything is made there. A writable open, a resumed
    /// restore or join, and a replacement give it to every file already
    /// there, unless they were last given this class. On a volume that keeps
    /// no classes each of them fails before any file changes. Unset, coven
    /// leaves every class as it is, and new files take their folder's.
    #[cfg(target_vendor = "apple")]
    pub fn file_protection(self, protection: FileProtection) -> Self;

    /// A collation the app's SQL names with `COLLATE name`, ordering text by
    /// `compare` at `version` on every connection a handle opens to the
    /// store's database. At one version `compare` must be deterministic, the
    /// same on every device and app version, and a total order; every change
    /// to the order takes a new version, and a writable open then rebuilds
    /// the indexes in it.
    pub fn collation<F>(self, name: impl Into<String>, version: u32, compare: F) -> Self
    where
        F: Fn(&str, &str) -> Ordering + Send + Sync + 'static;

    /// A scalar function the app's SQL calls as `name(...)` with exactly
    /// `arguments` arguments, on every connection a handle opens to the
    /// store's database. `compute` returns the result, or an error that fails
    /// the statement calling it. A deterministic function the schema uses must,
    /// at its version, give the same result for the same arguments on every
    /// device and app version; every change to its results takes a new
    /// version, and a writable open then rebuilds the indexes that call it.
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
    /// the sync loop; `start_sync` starts it. Opening records whether
    /// storage is set up, so the sync state is ready once it returns (E5).
    /// A store whose replacement or deletion is unfinished is refused with
    /// `StoreLockError::ReplacementUnfinished` or `DeletionUnfinished`,
    /// changing nothing (§19.3, §11.6).
    /// `on_progress` hears each step the open runs, on this task (§4).
    pub async fn open(
        self,
        store: StoreId,
        on_progress: impl Fn(Progress<OpenStep>),
    ) -> CovenResult<CovenHandle>;

    /// Replaces this copy with a new device loaded from storage when the app
    /// chooses: a damaged database (§19.3), a missing device entry, or another
    /// copy using this id (§19.3). Uses saved settings and custody; no salvage.
    /// Checks storage and keys, then loads beside the old copy, which moves
    /// aside only once loading is complete. Saved member keys that history
    /// has replaced fail with MemberKeysReplaced and change nothing. Just
    /// before the fresh device registers, the reset records itself and keeps
    /// the device name on the device; from then on the store refuses opens
    /// with ReplacementUnfinished and the reset only goes forward, finished
    /// by this call or `resume_replacement`. Uses the existing bootstrap
    /// errors; an interrupted call resumes with the same fresh id. A
    /// replacement restore_device began is refused with
    /// ReplacementUnfinished, changing nothing, and a store this device is
    /// deleting with DeletionUnfinished. The returned handle holds a
    /// device-reset notice that unsent edits may have been lost (E5).
    /// `on_status` hears how far the new device has come, as `BootstrapStatus`
    /// reports it for the other bootstrap calls (E10).
    pub async fn reset_device(
        self,
        store: StoreId,
        device_name: &str,
        on_status: impl Fn(BootstrapStatus),
    ) -> Result<CovenHandle, BootstrapError>;

    /// Opens the store for reading only, alongside a handle that has it open,
    /// for example from another process. Its shared lock prevents deletion
    /// while its read connections and local cache connection remain open.
    /// It runs no migration and refuses a database whose schema is newer than
    /// its migrations, whose coven tables need migrating, or whose indexes
    /// were built with other versions of the app's collations and functions.
    /// File reads use saved storage settings and custody credentials when present.
    /// A store whose replacement or deletion is unfinished is refused, as
    /// `open` refuses it.
    pub async fn open_read_only(self, store: StoreId) -> Result<CovenReadHandle, ReadOnlyOpenError>;

    /// Finishes this device's deletion of `store` that `delete_store` began
    /// and recorded: it opens the store as `open` does, then goes on as the
    /// deletion would, with the choice for unsent work it recorded. Before
    /// the device's removal is journaled, a refusal or failure removes the
    /// record and leaves the store as it was; from the journal on, a failure
    /// is `StoreDeletionError::Unfinished` and the deletion stays recorded
    /// (§11.6). The call runs to its end even if the app drops its future. A
    /// store with no deletion recorded fails with
    /// `StoreDeletionError::NoUnfinishedDeletion`.
    pub async fn resume_deletion(self, store: StoreId) -> Result<(), StoreDeletionError>;

    /// Replace provider construction in tests while exercising setup, credential
    /// custody, reconnect, restore and recovery through the production graph.
    /// `coven_storage::test_utils::MemoryStorage` implements this connector.
    #[cfg(feature = "test-utils")]
    pub fn storage_connector(mut self, connector: Arc<dyn StorageConnector>) -> Self;
}

/// How far a long operation has come: the step it is on, and how many of that
/// step's units are done out of its total (E1). Steps arrive in the order the
/// operation runs them, and within a step `done` only grows and never passes
/// `total`, which never shrinks.
pub struct Progress<S> {
    pub step: S,
    pub done: u64,
    /// Known as the step begins; grows only when work the step had no way
    /// to count turns up.
    pub total: u64,
}

/// A step of opening a store for writing (§4), in the order they run. A step
/// runs only when it has work to do.
pub enum OpenStep {
    /// Rebuilding the indexes a new version of SQLite, or of one of the app's
    /// collations or functions, changed. Counted in rebuilds: one for each
    /// index, and one for all the indexes in a collation.
    Rebuilding,
    /// Running the app's migrations the store hasn't run yet. Counted in
    /// migrations.
    Migrating,
}

/// A kind of file, with its cache budget, declared when the store opens
/// (§17.1, §17.8).
pub struct Namespace { /* private fields */ }

impl Namespace {
    /// A namespace of this kind. The budget applies when the namespace is
    /// first declared; `set_cache_budget` changes it after (E8).
    pub fn new(name: impl Into<String>, kind: Provenance, cache_budget: u64) -> Self;

    /// The name exactly as declared.
    pub fn name(&self) -> &str;

    /// Whose bytes its files are.
    pub fn kind(&self) -> &Provenance;

    /// The initial cache budget in bytes, as declared, not its current setting.
    pub fn cache_budget(&self) -> u64;
}

/// Where this device keeps `T`: the store keys (`StoreKeyring`) or the
/// member's identity (`MemberIdentity`).
pub enum CustodyChoice<T: KeyMaterial> {
    Keyring,
    Passphrase(Passphrase),
    /// Starts empty; holds only this handle's session: what opening,
    /// creation, restore or join puts there.
    InMemory,
    Custom(Arc<dyn Custody<T>>),
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

    /// Deletes this store from this device and leaves it. With storage set
    /// up, a location recorded without credentials is refused as
    /// `SignInRequired`, and unsent work unless discarding it as `Unsent`;
    /// both change nothing. Then the deletion records itself in the store's
    /// directory, with `unsent`, and publishes this device's removal
    /// (§14.1), waiting until it applies. A failure before the removal is
    /// journaled removes the record and is `Sync`, the store as it was. From
    /// the journal on it only goes forward: a failure closes the handle and
    /// is `Unfinished`, and the store lists as being deleted
    /// (`Unfinished::Deletion`), refuses opens with `DeletionUnfinished` and
    /// is finished by `CovenBuilder::resume_deletion` (§11.6). Once the
    /// removal applied, or with no storage set up, it closes, a failure
    /// there being `Closing`, and deletes as `Coven::discard_store`. A device
    /// already stopped for good publishes nothing. The call runs to its end
    /// even if the app drops its future. Only this device leaves: the
    /// person's restore code and S3 key stay.
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
    /// `NotStarted` when another connection was checkpointing or committing.
    pub async fn checkpoint(&self, mode: CheckpointMode) -> Result<WalCheckpoint, DbError>;

    /// Gives back free pages of a store created with incremental auto-vacuum,
    /// between writes. On any other store it does nothing.
    pub async fn incremental_vacuum(&self, pages: IncrementalVacuum) -> Result<(), DbError>;

    /// Frees every connection's cached pages that no statement is using, for
    /// when the OS warns of low memory or the app goes to the background.
    /// One release on an idle reader frees them all, since SQLite keeps them
    /// in one pool; safe at any time, and never waits for a write.
    pub async fn release_memory(&self) -> Result<(), DbError>;

    /// Read committed plaintext records still awaiting upload in application tests.
    #[cfg(feature = "test-utils")]
    pub async fn test_queued_writes(
        &self,
    ) -> Result<Vec<coven_format::write::WriteRecord>, DbError>;
}

pub enum IfUnsent { Refuse, Discard }

/// Committed writes and staged files not yet uploaded.
pub struct UnsentWork { pub writes: u64, pub files: u64 }

impl UnsentWork {
    /// Whether anything waits to upload.
    pub fn is_empty(&self) -> bool;
}

/// Apple's file protection classes: when a file can be read and written,
/// against whether the device is locked (E1).
#[cfg(target_vendor = "apple")]
pub enum FileProtection {
    /// Readable and writable only while the device is unlocked.
    Complete,
    /// A file open when the device locks stays readable and writable, and new
    /// files can be made; a closed file opens only once it unlocks.
    CompleteUnlessOpen,
    /// Readable and writable from the first unlock after the device starts,
    /// locked or not. Apple's default for an app's files.
    CompleteUntilFirstUserAuthentication,
    /// Readable and writable whenever the device is on.
    None,
}

#[cfg(target_vendor = "apple")]
impl FileProtection {
    /// The class a file or folder has, read without opening it, so a locked
    /// device answers too; `Ok(None)` for a folder with no class of its own.
    #[cfg(feature = "test-utils")]
    pub fn of(path: &Path) -> std::io::Result<Option<Self>>;

    /// Whether a file made in `folder` can be given a class now, learned by
    /// giving a scratch file `Complete`.
    #[cfg(feature = "test-utils")]
    pub fn settable_in(folder: &Path) -> std::io::Result<ProtectionSupport>;
}

/// What a folder's volume does with protection classes now.
#[cfg(all(target_vendor = "apple", feature = "test-utils"))]
pub enum ProtectionSupport {
    /// The volume keeps classes, and a file can be given any of them.
    Keeps,
    /// The volume keeps no classes; a store whose app chose one is refused.
    KeepsNone,
    /// The volume keeps classes, but the device is locked, so a file can't be
    /// given `Complete` until it unlocks.
    Locked,
}

/// SQLite settings for the store's connections; `None` keeps SQLite's default,
/// except `checkpoint_fullfsync`, on for macOS and iOS, and
/// `journal_size_limit`, 4 MiB. `wal_autocheckpoint` is when coven checkpoints,
/// off the commit path. Each field's PRAGMA is
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

pub enum CheckpointMode { Noop, Passive, Full, Restart, Truncate }

/// What a checkpoint did, as SQLite reports it.
pub enum WalCheckpoint {
    /// SQLite read nothing of the WAL: another connection was checkpointing
    /// it, or committing to it as SQLite read where it ends.
    NotStarted,
    /// SQLite read the WAL and copied what it could.
    Ran {
        /// `Full`, `Restart` or `Truncate` couldn't finish within the busy
        /// timeout.
        busy: bool,
        /// Frames in the WAL.
        log_frames: u32,
        /// Frames copied into the database, this time or before.
        checkpointed_frames: u32,
    },
}

pub enum IncrementalVacuum { All, Pages(NonZeroU32) }

/// Whether an app function's result depends on its arguments alone; only a
/// deterministic one can appear where a stored value depends on it, and it
/// carries the version of its results (§4).
pub enum Determinism { Deterministic { version: u32 }, Nondeterministic }

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
let coven = Coven::new(app_dir, "com.example.notes", Arc::new(CrashReports))?; // E15
let handle = coven.builder()
    .synced_tables(tables())                        // E2
    .namespaces(vec![
        Namespace::new("attachments", Provenance::UserProvided, 2 << 30),
        Namespace::new("thumbnails", Provenance::AppProvided, 256 << 20),
    ])
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
    |progress| show_setup_step(progress),
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
  how its rows get their audience ([§15](coven.md#15-circles)), and which of its
  columns reference a file ([§17](coven.md#17-files)). It may restrict direct writes to
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

    /// The declared file reference columns, in declaration order.
    pub fn file_columns(&self) -> &[FileColumn];

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

    /// Declares a column holding a file's id, or NULL (§17.2). A table may
    /// declare several; each references its own file. Opening checks the
    /// column against the table's schema: it must exist, be declared once,
    /// not be generated, and be no SQL foreign key, since coven writes it
    /// when its row moves to another audience. No table may hold an SQL
    /// foreign key into `coven_files` (`FilesForeignKey`).
    pub fn file_column(self, column: FileColumn) -> Self;

    /// Declares the trigger `name` on this table as shared (§9.4); every
    /// trigger not declared shared is local. Opening refuses a shared trigger
    /// without `WHEN NOT coven_applying()`, naming it.
    pub fn shared_trigger(self, name: impl Into<String>) -> Self;
}

/// A namespace's kind: whose bytes its files are (§17.1).
pub enum Provenance {
    /// The user's own file at a path on their device; coven records it and
    /// never copies, changes or deletes it (§17.1).
    UserProvided,
    /// Bytes the app hands to coven, which keeps and owns them.
    AppProvided,
}

/// When a write setting a file reference goes out (§17.4).
pub enum Binding {
    /// Once its file is stored, together with it: the write and every later
    /// write of this device wait for the upload. Its files are app files of
    /// at most `limit` bytes.
    Bound { limit: u64 },
    /// At once; the file is on its uploader only until it is stored.
    Trailing,
}

/// The size limit of a bound reference that declares none: 1.5 MiB (§17.4).
pub const BOUND_FILE_LIMIT: u64 = 3 << 19;

/// A column holding a file's id, or NULL (§17.2). It references the file
/// as a foreign key references its parent, in the row's own audience.
pub struct FileColumn { /* private fields */ }

impl FileColumn {
    /// A trailing file reference held in `column`.
    pub fn new(column: impl Into<String>) -> Self;

    /// Bound, naming files of at most `BOUND_FILE_LIMIT` bytes (§17.4).
    pub fn bound(self) -> Self;

    /// Bound, naming files of at most `limit` bytes (§17.4).
    pub fn bound_with_limit(self, limit: u64) -> Self;

    /// Refuse a write that changes the reference once it names a file
    /// (`FileWriteOnce`).
    pub fn write_once(self) -> Self;

    /// The column exactly as declared.
    pub fn column(&self) -> &str;

    /// When a write setting the reference goes out.
    pub fn binding(&self) -> &Binding;

    /// Whether a reference, once it names a file, can't change.
    pub fn is_write_once(&self) -> bool;
}
```

Which columns are file references, and whether each is write-once, changes
only with a breaking migration
([§18.1](coven.md#181-telling-an-addition-from-a-breaking-change)): every
device must read the same cells as references. Opening with changed file
columns and no migration fails with `SchemaError::FileColumnsChanged`. A
reference's binding and size limit change freely: they decide only when this
device sends its own writes.

A bound reference holds the write that sets it, and every later write of this
device, until its file is stored, so other devices get the row and the stored
file together ([§17.4](coven.md#174-bound-and-trailing-references)). It names
only app files within its limit: a larger file fails the write with
`BoundFileTooLarge`, a user's file with `NamespaceKind`, and a file on one
device only that no bound upload stores, such as one staged with
`Place::DeviceOnly`, with `DeviceOnly`. Keep bound files small: every later
write waits for them.

Example:

```rust
fn tables() -> Vec<SyncedTable> {
    vec![
        // A root: each note is the store's or a circle's.
        SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
        // Descendants of notes. Each attachment references the user's own
        // file, in the `attachments` namespace (E1), and never changes it.
        SyncedTable::new("attachments", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .file_column(FileColumn::new("file").write_once()),
        // A thumbnail the app makes, in the note's audience. It is bound:
        // the thumbnail row reaches other devices with its stored file.
        SyncedTable::new("thumbnails", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .file_column(FileColumn::new("file").bound()),
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
- Coven's own state changes are steps on the same `CovenWrite`, beside the
  app's SQL: each runs where it is called, the write's later SQL sees it, and
  it commits or rolls back with the app's statements. A failed step fails the
  write even if the closure goes on and returns success (`StepFailed`). A
  step's refusal passed on with `?` keeps its own error: the write fails
  with `CovenError::Sync`, holding the step's `SyncError`, such as
  `SyncError::NotPending` for retrying an operation that isn't pending
  ([§4](coven.md#4-writing-locally)).
- A statement SQLite fails fails the write too, even if the closure drops
  the error and returns success (`StatementFailed`): SQLite may have kept
  part of its change, or undone rows coven saw it change. A closure that
  returns its own error keeps it. `QueryReturnedNoRows`, and the closure's
  own mapping errors, fail nothing ([§4](coven.md#4-writing-locally)).
- A write fails with its closure's own error type. Ana's music app has a
  `LibraryError` of its own; its closures return one, and `write` gives
  back the app's own value, never one wrapped in coven's error that the app
  must take apart. An app with no error type of its own uses `CovenError`.
  The type implements `WriteFailure`: it takes coven's `DbError` and
  SQLite's error with `From`, says which bug it carries (E15), and keeps a
  failed rollback or byte removal beside the write's own failure, as
  `CovenError`'s `Rollback` and `FileCleanup` do. Without them, a failed
  rollback would be lost behind the app's error, while every later call
  fails with `RollbackFailed` because of it
  ([§4](coven.md#4-writing-locally)).
- A bulk write runs SQL on device-local tables alone, as plain SQLite: no
  row change is recorded, a table empties in one step, and after commit each
  live query that read a table it changed runs again, whatever rows it read
  ([§4](coven.md#4-writing-locally)).
- Ordinary reload queues app writes with their original authored view. Required audience reloads
  return `AudienceReloading`; self-replacement returns `MemberKeysChanging`. Retry after
  installation to author against its state ([§12.3](coven.md#123-replacing-a-members-keys),
  [§16.5](coven.md#165-coming-back-after-a-long-absence)).

```rust
/// The app bytes one staging writes to coven's copies, each file given a
/// new id; a failure discards them (E3). It borrows the store's id source
/// while the app stages.
pub struct WriteBatch<'a> { /* private fields */ }

/// App bytes staged before the write that puts them in rows, with what the
/// staging's batch returned (§17.3, E3). It holds nothing close waits for:
/// dropped without a write, or after the store closes, its copies are
/// removed by the next write or open.
pub struct StagedFiles<T> { /* private fields */ }

/// One write's SQL and coven's steps, in one transaction (§4, E3).
pub struct CovenWrite<'write> { /* private fields */ }

/// A bulk write's SQL access, borrowing its connection (§4, E3).
pub struct BulkSqlContext<'connection> { /* private fields */ }

/// An error a write's closure fails with, which coven's own failures and
/// SQLite's convert into, and which keeps a failed rollback or byte removal
/// beside the write's own failure (§4, E3). `CovenError`, `DbError` and
/// `SyncError` implement it; an app implements it for its own error type.
pub trait WriteFailure: From<DbError> + From<rusqlite::Error> + coven::task::CarriesBug {
    /// The write failed with `self`, and SQLite then failed to roll it back.
    fn with_rollback(self, rollback: rusqlite::Error) -> Self;
    /// The write failed with `write`, and removing the bytes it released
    /// failed with each of `failures`.
    fn with_cleanup(write: Self, failures: Vec<DbError>) -> Self
    where
        Self: Sized;
}

/// A user's original read whole once before a write: its path, size and
/// modification time, and the content hash and chunk hashes of the bytes the
/// read found, which coven records and checks the original against;
/// callers cannot change them (E3).
pub struct PreparedUserFile { /* private fields */ }

impl PreparedUserFile {
    /// Its size in bytes when it was read, which coven records and checks
    /// the original against.
    pub fn size(&self) -> u64;
    /// Its modification time when it was read: the exact value coven
    /// records and checks the original against, at the precision the
    /// operating system gives (§17.3).
    pub fn modified_at(&self) -> SystemTime;
}

/// Where a staged file is to be, app bytes or a user's original (§17.3).
pub enum Place {
    /// On this device only, with no upload.
    DeviceOnly,
    /// Uploaded; on this device only until its upload is stored.
    Cloud,
    /// Uploaded, as `Cloud`, and kept whole here too once stored: the write
    /// switching it to the cloud pins it on this device, which makes its
    /// cache copy from coven's copy of app bytes, and downloads a user's
    /// original's from storage, never reading the original (§17.8).
    CloudKeptHere,
}

/// The facts recorded for a user's original file (§17.1).
pub struct UserFile {
    /// The user's path, which coven never changes or deletes.
    pub path: PathBuf,
    /// Its size in bytes.
    pub size: u64,
    /// Its recorded modification time.
    pub modified_at: SystemTime,
}

/// A file's record as this device read it (§17.2, E8).
pub struct FileRef { /* private fields */ }

impl CovenHandle {
    /// Runs one write: the app's SQL and coven's steps, in one transaction.
    /// A statement or shared trigger targeting another owner's row refuses
    /// the transaction with DbError::NotOwner (§9.6). Authorized foreign-key
    /// actions may change that row under the declared schema.
    pub async fn write<F, R, E>(&self, write: F) -> Result<R, E>
    where
        F: FnOnce(CovenWrite<'_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: WriteFailure + Send + 'static;

    /// Runs one bulk write: SQL that changes device-local tables alone, as
    /// plain SQLite with no pre-update hook, so emptying a table takes one
    /// step (§4). A statement that would write a synced table, itself or
    /// through a trigger or foreign-key action, fails the write with
    /// `SyncedTableInBulkWrite`; one that would write coven's tables with
    /// `InternalTable`. Triggers still run. After commit, every live query
    /// that read a table the write's statements may change runs again once,
    /// whatever rows it read: a small change that should rerun only the live
    /// queries reading its rows belongs in `write`. Failures, the write lock,
    /// a closed store and a failed handle are as for `write`.
    pub async fn write_bulk<F, R, E>(&self, sql: F) -> Result<R, E>
    where
        F: FnOnce(BulkSqlContext<'_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: WriteFailure + Send + 'static;

    /// Stages app bytes for a write that puts them in rows (§17.3): `stage`
    /// stages each file and returns what that write needs, such as the ids
    /// staging gave them, and the write takes it with `put_staged`. The bytes
    /// are streamed to coven's own copies, synced and flushed before this
    /// returns; close waits for staging and its cleanup, including
    /// cancellation, but not for the staged files it returns. An undeclared
    /// namespace fails with `UndeclaredNamespace`, a user one with
    /// `NamespaceKind`; a failed copy or flush removes what it wrote.
    pub async fn stage_files<F, T, E>(&self, stage: F) -> Result<StagedFiles<T>, E>
    where
        F: FnOnce(&mut WriteBatch<'_>) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: WriteFailure + Send + 'static;
}

impl WriteBatch<'_> {
    /// Stages app bytes as a new file in an app namespace, kept in coven's
    /// own copy, and returns its id for a row to reference. `bytes` is a
    /// byte buffer, or a stream read once, so a large file never has to fit
    /// in memory. The write that takes it makes the file's record, on this
    /// device only; for `Place::Cloud` it also queues the upload, which
    /// switches the record to the cloud when it completes (§17.3).
    pub fn put_file(&mut self, namespace: &str, bytes: impl Into<FileSource>, place: Place)
        -> FileId;
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

/// One move's identity on this device, never reused (§17.3). ToSql/FromSql
/// bind/read it as an SQL integer, as `coven_uploads.move` and
/// `coven_pending.move` hold it (E4).
pub struct MoveId(pub i64);

/// A file of this device, on it only, to move to the cloud (§17.3).
pub enum MoveFile {
    /// A user's original: its upload checks the bytes it sends against the
    /// size, modification time and chunk hashes its staging recorded.
    User(FileId),
    /// As `User`, and kept whole here too once stored (§17.8).
    UserKeptHere(FileId),
    /// An app file, uploaded from coven's copy.
    App(FileId),
    /// As `App`, and kept whole here too once stored, its cache copy made
    /// from coven's copy (§17.8).
    AppKeptHere(FileId),
}

impl CovenWrite<'_> {
    /// Puts files `stage_files` staged into this write and returns what its
    /// batch returned, such as their ids, for this write's rows to reference
    /// (§17.3). The write makes each file's record, in the audience of the
    /// rows that name it, and queues its upload for the cloud. Files another
    /// open store staged, or this one before it closed, fail with
    /// `PreparedElsewhere`.
    pub fn put_staged<T>(&self, staged: StagedFiles<T>) -> Result<T, DbError>;

    /// Stages a user's original, read whole once already, as a new file in a
    /// user namespace and returns its id for a row to reference: on this
    /// device only, or for the cloud, with its upload queued. Coven records
    /// the original's path, size, modification time and the hashes its read
    /// found, which its upload checks, now or when the file moves to the
    /// cloud; it never changes the original, nor reads it here (§17.1,
    /// §17.3). An undeclared namespace fails with `UndeclaredNamespace`, an
    /// app one with `NamespaceKind`.
    pub fn put_user_file(&self, namespace: &str, original: PreparedUserFile, place: Place)
        -> Result<FileId, DbError>;

    /// Stages a new file over an existing file's bytes, for a row in any
    /// audience, and returns its id for that row to reference. The write
    /// makes it in the audience of the rows that reference it, with
    /// `from`'s namespace, size, place and key; nothing is uploaded again. A
    /// file on this device only gives a new file on this device only, over
    /// the same original or coven's copy. The write fails with `FileAbsent`
    /// if this device has no record of `from`, and with `NotThisDevice` if
    /// `from` is on another device only (§17.2).
    pub fn existing_file(&self, from: FileId) -> FileId;

    /// Records a move of these files to the cloud (§17.3). Each must be on
    /// this device only (`NotThisDevice`, `NotDeviceOnlyHere`, `FileAbsent`)
    /// and in no move, a failed one included (`FileInMove`, naming the move). The write reads no file: a user's
    /// original uploads with what its staging recorded. The move survives
    /// restarts; once every file is stored, one write switches them all to
    /// the cloud, changing no app row. A missing original waits; one changed
    /// since its staging, even keeping its size and modification time, fails
    /// the move before a changed byte is sent, and the move's record says
    /// which until the app cancels it. `coven_moves` lists the move's files
    /// with its state from this write on, a failed move's too, until a
    /// switch or a cancel ends it (E4).
    pub fn move_to_cloud(&self, files: Vec<MoveFile>) -> Result<MoveId, DbError>;

    /// Cancels a move, failed or still moving: every file stays on this
    /// device only, and its record and its list in `coven_moves` end. A write staging files for the
    /// cloud returns no move id; `coven_uploads.move` names the move of
    /// each file still uploading (E4). A cancelled move cancels again
    /// without effect; a finished one fails with `MoveFinished`, a bound
    /// file's with `MoveBound` (§17.4), and an id this device never gave a
    /// move with `MoveUnknown`.
    pub fn cancel_move(&self, id: MoveId) -> Result<(), DbError>;

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
}

impl BulkSqlContext<'_> {
    /// Ordinary SQL inside the bulk write, on device-local tables; it reads
    /// any table app SQL reads.
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

/// Reads a user's file once, before the write, recording its size, whole-file
/// hash and plaintext chunk hashes, which its upload checks, whether it is
/// staged for the cloud or on this device only and moved later. `progress`
/// receives the bytes read so far. Fails if the file changes while it is
/// read. Its path may be any the operating system opens, valid Unicode or
/// not (§17.1).
pub async fn prepare_user_file(
    path: &Path,
    progress: impl Fn(u64) + Send + Sync,
) -> Result<PreparedUserFile, DbError>;

/// The text that shows `path`, as `coven_path_text` shows its recorded bytes
/// (§17.3): valid UTF-8 as it is, each other byte as `\x` and two uppercase
/// hex digits, and a backslash already in a name as it is. For a name the
/// app syncs or shows; it can't be turned back into the path.
pub fn path_text(path: &Path) -> String;

/// The path's bytes as coven records them in `coven_user_files.path`
/// (§17.3): on Unix the path's own bytes, on Windows its UTF-16 in WTF-8. A
/// path that is valid Unicode is its UTF-8 on both. An app that keeps paths
/// of its own keeps them as these bytes, so it opens a file by them again
/// and matches them against the paths coven records.
pub fn path_bytes(path: &Path) -> Vec<u8>;

/// The path that `path_bytes` gave `bytes`. Bytes it never gives on this
/// operating system fail: on Windows, bytes that aren't WTF-8, such as a
/// Latin-1 name recorded on Linux. Unix takes any bytes.
pub fn path_from_bytes(bytes: Vec<u8>) -> Result<PathBuf, PathBytesError>;

/// Bytes that are no path on this operating system.
pub struct PathBytesError;

/// A time as `coven_user_files` lists a modification time, in
/// `modified_seconds` and `modified_nanos` (E4): whole seconds since the Unix
/// epoch, rounded down, and the nanoseconds past that second, from 0 through
/// 999,999,999 (§17.3). Half a second before the epoch is (-1, 500,000,000).
/// An app compares a file's time on disk with the view's through it.
pub fn unix_seconds_and_nanos(time: SystemTime) -> (i64, u32);
```

Example:

```rust
let note_id = UuidText(Uuid::now_v7());
let attachment_id = UuidText(Uuid::now_v7());
let prepared = prepare_user_file(&path, |read| show_progress(read)).await?;

let file = handle
    .write(move |sql| {
        let file = sql.put_user_file("attachments", prepared, Place::Cloud)?;
        sql.execute(
            "INSERT INTO notes (id, title, audience) VALUES (?1, ?2, ?3)",
            (&note_id, "Paint colors", Audience::Store),
        )?;
        sql.execute(
            "INSERT INTO attachments (id, note_id, title, file) VALUES (?1, ?2, ?3, ?4)",
            (&attachment_id, &note_id, "Swatches", file),
        )?;
        Ok::<_, CovenError>(file)
    })
    .await?;
```

App bytes are staged first, outside the write, which puts them in its rows:

```rust
let staged = handle
    .stage_files(move |batch| Ok::<_, CovenError>(batch.put_file("covers", cover_bytes, Place::Cloud)))
    .await?;
let cover = handle
    .write(move |sql| {
        let cover = sql.put_staged(staged)?;
        sql.execute("UPDATE notes SET cover = ?1 WHERE id = ?2", (cover, &note_id))?;
        Ok::<_, CovenError>(cover)
    })
    .await?;
```

Staging on this device only, then moving to the cloud as one move:

```rust
let prepared = prepare_user_file(&path, |read| show_progress(read)).await?;
let file = handle
    .write(move |sql| {
        let file = sql.put_user_file("attachments", prepared, Place::DeviceOnly)?;
        sql.execute(
            "INSERT INTO attachments (id, note_id, title, file) VALUES (?1, ?2, ?3, ?4)",
            (&attachment_id, &note_id, "Swatches", file),
        )?;
        Ok::<_, CovenError>(file)
    })
    .await?;
// Later, when the person moves it to the cloud: nothing is read again.
let moved = handle
    .write(move |sql| Ok::<_, CovenError>(sql.move_to_cloud(vec![MoveFile::User(file)])?))
    .await?;
```

### E4 Reading

- Reads run on several read-only connections at once ([§4](coven.md#4-writing-locally)).
- A one-shot read, as `read`, records nothing of what it reads: nothing runs
  it again. It and a live query's run alike run only the app's statements;
  the writes that change a joined table run coven's lookups
  ([§4](coven.md#4-writing-locally)).
- A *live query* runs once, then again whenever a write commits that changes
  rows it read. Its errors are results and don't end it, except the one saying
  the store closed or its handle fails every call, and one that carries a bug,
  as a run that panicked does (E15): after that one, `next()` returns `None`,
  and never repeats it.
- A read and a live query fail with their closure's own error type, as a
  write does (E3): Ana's music app's closures return its `LibraryError`, and
  its live query of albums delivers one. The type takes coven's `DbError`
  with `From` and says which bug it carries (E15). `process` fails with the
  same type. An app with no error type of its own uses `CovenError`, and
  names it as a live query's: `LiveQuery<Vec<String>, CovenError>`.
- Each screen is one live query over the local database, its SQL returning the
  rows the screen draws in the order it draws them, with their counts
  ([§4.1](coven.md#41-showing-data)). Writes go to the database and live
  queries carry them to every screen; the app keeps no second copy of rows to
  sort, filter or count. A value computed across tables that a screen sorts or
  filters by lives in a local table that local triggers keep in the same write.
  Only values with no table, such as progress, stay in memory, each on its own
  stream.
- The closure passed to `read` or `subscribe` runs SQL while it holds its read
  snapshot and a reader connection; `process` maps the result to the app's types
  after the connection is released. A rerun after a write that returns the same
  result isn't delivered.
- Table-backed built-in reads work offline. Each `subscribe_x()` counterpart reports the same
  committed data and later changes as its corresponding one-shot read.
- Upload progress counts provider-confirmed parts; file progress counts verified bytes
  committed to the cache. Neither reports bytes merely received or sent. How
  far a transfer in flight has got, bytes handed over or received included,
  is on the transfer watches (E7, E8): memory only, keyed by file, with no
  table or live query.
- This device's open invites are the view `coven_invites` (E10), what the
  sync state is computed from and what the person's retry would do the tables
  `coven_sync_status` and `coven_retry_effect` (E5), which a live query
  follows like any other.
- E.g. Ana's app reads `members()` to draw its first list, or subscribes with
  `subscribe_members()` to receive that same list and later committed changes.
- Subscriptions show current results, as slots apply. Their callback
  histories need not match between devices (§10).
- Offer a removed row elsewhere with the same key (§8.4); copying to a new key
  can produce two rows. Published owner and unique claims follow slot order.
- *Coven's state in SQL.* App SQL reads eleven views of coven's own tables and
  seven tables, `coven_files`, `coven_pins`, `coven_pin_requests`,
  `coven_moves`, `coven_upload_queue`, `coven_sync_status` and
  `coven_retry_effect`, in reads,
  writes and migrations alike
  ([§4](coven.md#4-writing-locally)). They are coven's to define and the app's
  to rely on: the columns below are stable. Each is true in every transaction
  that changes the tables behind it, and a live query that reads one reruns
  when coven changes them. The app can't write them, or name an object of its
  own after one.
- The app's migrations may declare local triggers on `coven_files`,
  `coven_pins`, `coven_pin_requests`, `coven_moves`, `coven_upload_queue`,
  `coven_sync_status` and `coven_retry_effect`, AFTER their
  changes, writing only
  local tables and reading only what app SQL reads
  ([§9.4](coven.md#94-triggers)). A trigger on `coven_files` sees each record
  made, switched to its stored bytes (`UPDATE OF coordinates`) and deleted, on
  this device and as another device's write applies; it is how the app learns
  that a file uploaded elsewhere is stored. Making or dropping such a trigger
  changes only this device, so its migration is an addition
  ([§18.1](coven.md#181-telling-an-addition-from-a-breaking-change)). Count or
  group the queue by what such a trigger keeps: `coven_uploads` joins what
  each file waits for into every row it reads.
- The file views and tables name files, not rows, by the file's id as
  canonical UUID text, read as `FileId`, the id the app's file reference
  columns hold ([§17.2](coven.md#172-files-and-references)). So a join reads
  `u.file=p.file`, and a file two rows reference is listed once.
- `coven_file_device(coordinates)` gives the device a file is on, as
  `coven_devices.device` holds it, for a file on one device only, and NULL for
  a stored one. `coven_key(v1, v2, ...)` gives a row's key as coven stores it
  (format.md D3), for the app's own key columns in key order.
  `coven_path_text(path)` gives the text that shows an original's path from
  the bytes `coven_user_files.path` holds, as `path_text` gives it (E3); NULL
  gives NULL, and a value that isn't bytes fails the statement. All three are
  deterministic, so an index on an expression can hold them. `coven_key` takes
  one value or more, up to the 1,000 an SQL call can pass; `coven_key()` with
  none fails the statement.
- Whether a file is being sent now lasts only as long as the process, and is
  never stored. `coven_uploads.sending` and `coven_sending` read it from the
  upload worker's memory, and a live query that reads either runs again each
  time an upload starts or ends. So does whether a stored file is being
  downloaded to keep whole, which `coven_downloading` reads, again as each
  download starts or ends. Only these views reach it: calling
  `_coven_sending`, `_coven_sending_objects` or `_coven_downloading_objects`
  directly, or in the app's own view or trigger, fails with
  `StatementForbidden`. A read-only handle transfers nothing, and shows every
  file waiting and none downloading.
- The device's network is the same kind of fact. `coven_network`'s `usable`
  and `metered`, and `coven_uploads.metered`, read it from the store's watch
  of the network (E5), and a live query that reads one runs again as the
  network changes. The function behind them, `_coven_network`, refuses a
  direct call in the same way. A read-only handle watches nothing: it shows
  `usable` NULL and nothing metered, with the setting as stored.
- Count what is being sent through `coven_sending`, which holds one row per
  upload in flight and finds each by index. Counting `sending` over
  `coven_uploads` reads every queued file.
- A pending record's cause is the text of the error the running process met
  for it, written with the record. Opening clears every cause: one an earlier
  run wrote names a failure nothing has met since (E5).
- *A read's and a lookup's cost, in tests.* With the test-utils feature,
  `test_track_work` counts SQLite's steps as SQLite counts them for each
  statement: the app's statements in `query` and `query_row`, and apart,
  coven's lookups judging whether a write changed rows a live query joins
  ([§4](coven.md#4-writing-locally)). A test that runs the same read, or the
  same write beside the same live query, over a small table and a large one
  fails when its steps grow with the table. The app's statements count in
  writes and migrations too.

| View | Column | Holds |
|---|---|---|
| `coven_files`: each file this device has a record of, the files its rows and lost values reference; a table coven writes ([§17.2](coven.md#172-files-and-references)), which local triggers can follow | `id` | the file's id as canonical UUID text, read as `FileId` |
| | `audience` | the store or the circle the record is in |
| | `size` | its size in bytes |
| | `hash` | its content hash, 32 bytes; NULL until it is stored |
| | `coordinates` | where it is (format.md D12): stored at a location with its key, or on one device only; read the device with `coven_file_device` |
| | `namespace` | its namespace, which fixes its kind |
| `coven_user_files`: each user original this device records, while its file's record exists here ([§17.3](coven.md#173-staging-a-file)) | `file` | the file's id, as in `coven_files` |
| | `path` | the original's path as the operating system's bytes, which coven never changes: on Unix its own, on Windows its UTF-16 in WTF-8, as `path_bytes` gives them; show it with `coven_path_text`, and turn it back into a path with `path_from_bytes` |
| | `size` | its size in bytes, recorded when it was staged |
| | `modified_seconds` | its modification time when it was staged, exactly as coven records it and checks the original against, in two parts: whole seconds since the Unix epoch, rounded down, so negative before 1970 |
| | `modified_nanos` | the nanoseconds past `modified_seconds`, from 0 through 999,999,999: half a second before the epoch is -1 and 500,000,000. `unix_seconds_and_nanos` gives a time on disk as the same pair (E3) |
| `coven_uploads`: each file waiting in this device's upload queue, as `uploads()` lists it | `file` | the file's id; this device uploads it |
| | `position` | its place in the queue, smallest first: the order uploads start in |
| | `paused` | 1 while the app has paused uploads (`set_uploads_paused`) and the upload is trailing; 0 for a bound upload, which the pause doesn't hold ([§17.4](coven.md#174-bound-and-trailing-references)) |
| | `metered` | 1 while the network is metered, the app moves files only on unmetered networks, and the upload is trailing; 0 for a bound upload, which the setting doesn't hold ([§17.9](coven.md#179-uploads)); follows the network, which isn't stored |
| | `retry_at` | when its retry wait ends, in milliseconds since the Unix epoch; NULL with none |
| | `pending` | its pending record, read as `PendingRecord`; NULL with none |
| | `bytes_confirmed` | the encrypted bytes the provider confirmed in its resumable session, as `UploadProgress` gives them (E7); NULL with no session, as for a file sent in one request |
| | `bytes_total` | the whole encrypted size its session sends; NULL with no session |
| | `sending` | 1 while this process sends it; never stored, so 0 after reopening and on a read-only handle |
| | `move` | the move its upload belongs to, read as `MoveId`, for `cancel_move`: the id `move_to_cloud` returned, or the move of the write that staged it ([§17.3](coven.md#173-staging-a-file)); files sharing one upload share it. NULL for a bound file's upload, whose move can't be cancelled ([§17.4](coven.md#174-bound-and-trailing-references)) |
| `coven_sending`: each queued file this process is sending now, one per upload in flight | `file` | the file's id, as in `coven_uploads` |
| | `position` | its place in the queue, as in `coven_uploads` |
| `coven_downloading`: each stored file this process is downloading now to keep whole, for a pin (E8); never stored | `file` | the file's id |
| `coven_upload_pause`: whether uploads are paused, in one row, with an empty queue too | `paused` | 1 while the app has paused uploads (`set_uploads_paused`); persisted across reopening |
| `coven_network`: the device's network as coven last heard from the platform, and the setting that holds transfers on a metered one, in one row (E5, E7) | `usable` | 1 while the device has a usable network, 0 while it has none, NULL where coven can't tell; never stored, read from the platform as the store opens |
| | `metered` | 1 while that network is metered, and while coven can't tell after the platform failed to answer since opening; 0 with none, and where the platform never tells; never stored |
| | `unmetered_only` | 1 while the app moves files only on unmetered networks (`set_unmetered_only`); persisted across reopening |
| `coven_upload_queue`: each file waiting in this device's upload queue, as `coven_uploads` lists it; a table, which local triggers can follow | `file` | the file's id, as in `coven_uploads` |
| | `position` | its place in the queue, as in `coven_uploads` |
| `coven_pins`: each file this device keeps pinned, whole on this device (E8); a table, which local triggers can follow | `file` | the file's id |
| `coven_pin_requests`: each file a switch to the cloud asked this device to keep whole, whose pin coven has not yet made (E8); a table, which local triggers can follow: a row appears as the switching write records the request and goes as the request ends, whether by the pin made, the app's unpin or the file gone | `file` | the file's id; the copy the request keeps and its wait are coven's, and `pin_requests` reads them |
| `coven_moves`: each file of each move to the cloud that is still moving or has failed ([§17.3](coven.md#173-staging-a-file)), one row per file, since a file is in at most one move; a table, which local triggers can follow: a move's rows appear in the write that records it, a file's as it joins a moving move as a new file over a moving file's bytes, their state changes in the transaction that fails the move, a file's row goes with its record, and the rest go in the write that cancels or switches the move | `move` | the move, read as `MoveId`: the id `move_to_cloud` returned, or the move of the write that staged the file; a bound file's move is listed too, though `cancel_move` refuses it with `MoveBound` ([§17.4](coven.md#174-bound-and-trailing-references)) |
| | `state` | `moving` while its uploads are queued, `failed` once a changed original failed it: its files stay on this device only, none waits for an upload, and the move's pending record says which original until the app cancels it |
| | `file` | the file's id; a file a failed move lists stays in it, and `move_to_cloud` refuses it with `FileInMove`, naming the move, until the app cancels the move |
| `coven_sync_status`: what this installation's sync state is computed from, in one row from the store's creation on, with no storage set up too (E5); a table, which local triggers can follow; not readable on a read-only handle; read the state with `sync_status` | `storage` | `none` with no location recorded, `signed_out` with a location whose credentials this device doesn't hold, `connected` with both |
| | `provider` | the recorded location's provider by its `CloudProvider` name: `S3`, `Dropbox`, `OneDrive` or `CloudKit`; NULL with none |
| | `last_pass` | how the last pass ended, read as `LastPass`; NULL until a pass ends on this device, which counts as synced, since setup, joining and restoring read the store to its end |
| | `moving` | 1 while a running pass has read a slot it hadn't, or claimed one; opening writes 0 |
| | `last_synced` | when a pass last reached the end of the sequence, or setup, joining or restoring finished, in milliseconds since the Unix epoch by the store's clock; never earlier than before; NULL until then |
| | `starts` | how many times sync has started in this installation, the app's starts and the one setup makes alike; each start adds one in the step that starts it, so a trigger hears a start that holds for the reason the last one did, which changes no other column; 0 until sync first starts |
| `coven_retry_effect`: what the person's retry would do if made now, in one row from the store's creation on (E5); a table, which local triggers can follow; not readable on a read-only handle | `effect` | `pass`, `cooling_down`, `failed` or `not_syncing`, computed from the columns below, read as `RetryEffect` with `RetryEffect::read` |
| | `syncing` | 1 while a started connection will plan |
| | `held` | 1 while a failure only new credentials, an update or a reset can clear holds every pass |
| | `cooling_until` | while syncing, when the provider cooldown holding every pass ends, in milliseconds since the Unix epoch by the store's clock, rounded up; NULL with no cooldown, and while stopped |
| `coven_pending`: every pending record (§19.1) | `record` | the record, read as `PendingRecord`, without its cause |
| | `file` | for a record about stored bytes or an upload, each file over them on this device: the files the upload stores, or the stored files; NULL otherwise |
| | `position` | its place in the order the records arose, smallest first; a record whose condition changes keeps its place |
| | `cause` | the text of the error behind the record, written with it, when this run met that error and no app call received it; NULL otherwise (E5) |
| | `move` | for a record about a move, such as its original found missing, the move, read as `MoveId` and joined to `coven_uploads.move`; NULL otherwise |
| `coven_invites`: this device's invites that haven't ended, each from the write that makes it until approval, decline, cancel or expiry ends it, each a write, or a pass refuses its request (E10) | `invite` | the invite with its stage, read with `operation` by `Invite::list` |
| | `operation` | the invite's operation, which names its pending record in `coven_pending`, read as `i64` for `OperationId`; smallest first is the order the invites were made |
| `coven_operations`: each operation an app's write journaled (§11.4), from that write's commit, and each storage setup, from its journal (§11.6), until a write acknowledges it, in the order they were journaled (E6) | `operation` | its id, read as `i64` for `OperationId` |
| | `kind` | what it does, read as `OperationKind` |
| | `outcome` | NULL while it runs; then how it ended, read as `OperationOutcome` |
| `coven_members`: every member this device's applied slots name, former ones included | `member` | the member id as canonical UUID text, read as `MemberId` |
| | `role` | `admin` or `member` |
| | `removed` | 1 for a former member |
| | `is_self` | 1 for the member using this device |
| | `signing`, `sealing` | the member's current public keys, 32 bytes each, read as `[u8; 32]` for `SigningKey::from_bytes` and `SealingKey::from_bytes` |
| | `account`, `access_key_id` | how they reach storage (§14.3): their provider account, or the id of their S3 key, never its secret; one is NULL, as `MemberAccess` holds one or the other |
| `coven_devices`: every device a member registered | `device` | the device id's eight big-endian bytes, read as `DeviceId` |
| | `member` | its member, as in `coven_members` |
| | `name` | its name from its registration |
| | `state` | `active`, `removed` or `replaced` |
| | `is_self` | 1 for this installation |
| `coven_circles`: each circle this device's member is in, as `Circles::list()` lists them; a deleted circle isn't listed (§15) | `circle` | the circle id as canonical UUID text, read as `CircleId` |
| | `name` | its current name |
| `coven_circle_members`: each circle's members who remain in the store, as `Circles::members()` lists them; none for a deleted circle | `circle` | the circle, as in `coven_circles` |
| | `member` | the member, as in `coven_members` |
| | `is_self` | 1 for the member using this device |

```rust
/// Reads the `pending` and `record` columns of coven's views.
impl types::FromSql for PendingRecord;
/// Reads `coven_sync_status.last_pass`.
impl types::FromSql for LastPass;
/// Reads `coven_operations.kind`.
impl types::FromSql for OperationKind;
/// Reads `coven_operations.outcome` once it is not NULL.
impl types::FromSql for OperationOutcome;
/// Reads a file id column: the app's file references and coven's file views.
impl types::FromSql for FileId;
/// Reads `coven_uploads.move`, `coven_pending.move` and
/// `coven_moves.move`, the move `cancel_move` takes.
impl types::FromSql for MoveId;
```

Example:

```rust
// The albums still uploading come first, one live query over the app's
// tables and coven's queue.
let albums: LiveQuery<_, CovenError> = handle.subscribe(|sql| {
    Ok(sql.query(
        "SELECT a.title FROM albums AS a
         ORDER BY EXISTS(SELECT 1 FROM photos AS p JOIN coven_uploads AS u
                         ON u.file=p.file
                         WHERE p.album_id=a.id) DESC, a.title",
        [],
        |row| row.get::<_, String>(0),
    )?)
});
```

```rust
// How many of an album's photos are going out now: a lookup per file being
// sent, run again as each upload starts or ends.
let mut sending: LiveQuery<_, CovenError> = handle.subscribe(move |sql| {
    Ok(sql.query_row(
        "SELECT count(*) FROM coven_sending AS s JOIN photos AS p ON s.file=p.file
         WHERE p.album_id=?1",
        [&album],
        |row| row.get::<_, i64>(0),
    )?)
});
```

```rust
// The photos that can't upload, each with its error.
let mut failing: LiveQuery<_, CovenError> = handle.subscribe(|sql| {
    Ok(sql.query(
        "SELECT e.record, e.cause, p.title FROM coven_pending AS e
         JOIN photos AS p ON e.file=p.file",
        [],
        |row| {
            Ok((
                row.get::<_, PendingRecord>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
            ))
        },
    )?)
});
while let Some(rows) = failing.next().await {
    show_failures(rows?);
}
```

```rust
/// SQL access to one read-only database snapshot (§4, E4).
pub struct SqlReadContext<'connection> { /* private fields */ }

/// Which handle a read or a file stream comes from: the store's one
/// writer, or a read-only handle (E1). Sealed: these two are the only modes.
pub trait Mode: Clone + Send + Sync + 'static {}
/// The store's writer: `CovenHandle`'s reads and streams.
pub enum Writer {}
/// A read-only handle: `CovenReadHandle`'s reads and streams.
pub enum ReadOnly {}
impl Mode for Writer {}
impl Mode for ReadOnly {}

/// An awaitable read that keeps its store borrow until it finishes (E4).
pub struct Read<'a, F, M: Mode = Writer> { /* private fields */ }

impl<F, R, E, M: Mode> Future for Read<'_, F, M>
where
    F: FnOnce(SqlReadContext<'_>) -> Result<R, E> + Send + 'static,
    R: Send + 'static,
    E: From<DbError> + coven::task::CarriesBug + Send + 'static,
{
    type Output = Result<R, E>;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output>;
}

/// A subscription's results and lifetime; dropping it ends the query (E4).
/// Its results fail with `E`, the query closure's own error type.
pub struct LiveQuery<T, E> { /* private fields */ }

/// A subscription whose request may be replaced while it runs (E4), its
/// results failing with `E` as a `LiveQuery`'s do.
pub struct ReconfigurableLiveQuery<Q, T, E> { /* private fields */ }

/// Shared access to replacing a live query's request (E4).
pub struct LiveQueryRequests<Q> { /* private fields */ }

/// How often a live query had run when asked, for tests and benchmarks that
/// count the runs writes caused and those that found nothing new (E4).
#[cfg(feature = "test-utils")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LiveQueryRuns { /* private fields */ }

/// A request revision assigned by one live query (E4).
pub struct LiveQueryRevision(pub u64);

/// The query was dropped before its request could be replaced (E4).
pub struct LiveQueryClosed;

/// A result together with the request that produced it (E4).
pub struct ReconfigurableLiveQueryEvent<Q, T, E> {
    /// The request used by this run.
    pub request: Q,
    /// The revision assigned to that request.
    pub revision: LiveQueryRevision,
    /// A new request, changed rows, or both.
    pub cause: LiveQueryCause,
    /// The query's result, including failures that do not end the query.
    pub result: Result<T, E>,
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

/// An open file with checked identity, header and range-reading state (§17.7).
/// Retaining a stream prevents store deletion, including after the store closes.
/// A read-only handle's stream (`FileStream<ReadOnly>`) reads the same way
/// and records nothing: a failed read goes to its caller only.
pub struct FileStream<M: FilesMode = Writer> { /* private fields */ }

/// What a mode adds to its file owner (sealed with `Mode`): the writer's
/// upload settings, and nothing for a read-only handle, which runs no upload.
pub trait FilesMode: Mode {
    /// The mode's upload state, beside what every file read uses.
    type Uploads: Send + Sync + 'static;
}
impl FilesMode for Writer {
    type Uploads = UploadSettings;
}
impl FilesMode for ReadOnly {
    type Uploads = ();
}
/// The writer's copies of the upload settings (§17.4, §17.9); the app changes
/// them through `CovenHandle`, never through this type.
pub struct UploadSettings { /* private fields */ }

impl CovenHandle {
    /// A read of one consistent snapshot, run when awaited. Attach `process`
    /// to work on the result after the connection is released. It fails
    /// with the closure's own error type.
    pub fn read<F, R, E>(&self, read: F) -> Read<'_, F>
    where
        F: FnOnce(SqlReadContext<'_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: From<DbError> + coven::task::CarriesBug + Send + 'static;

    /// A live query. Coven records the tables and columns the query reads,
    /// and the rows its conditions allow, and reruns it only for writes that
    /// touch them. Its results fail with the closure's own error type.
    pub fn subscribe<F, R, E>(&self, query: F) -> LiveQuery<R, E>
    where
        F: Fn(SqlReadContext<'_>) -> Result<R, E> + Send + Sync + 'static,
        R: Send + 'static,
        E: From<DbError> + coven::task::CarriesBug + Send + 'static;

    /// A live query whose request, such as a page or a search term, can be
    /// replaced without starting a new subscription.
    pub fn subscribe_reconfigurable<Q, F, R, E>(
        &self,
        initial_request: Q,
        query: F,
    ) -> ReconfigurableLiveQuery<Q, R, E>
    where
        Q: Clone + PartialEq + Send + Sync + 'static,
        F: Fn(&Q, SqlReadContext<'_>) -> Result<R, E> + Send + Sync + 'static,
        R: Send + 'static,
        E: From<DbError> + coven::task::CarriesBug + Send + 'static;

    /// Every lost value and removed row, as `_coven_lost` holds them (§8.2, §8.4).
    /// References show what was written, even if their parents were deleted.
    pub async fn lost_values(&self) -> CovenResult<Vec<LostValue>>;

    /// `CovenWrite::dismiss_lost_values`, in a write of its own.
    pub async fn dismiss_lost_values(&self, values: &[LostValue]) -> CovenResult<()>;

    /// The lost values, as a live query.
    pub fn subscribe_lost_values(&self) -> LiveQuery<Vec<LostValue>>;
}

impl CovenWrite<'_> {
    /// Dismisses lost values the app has dealt with, as a step of this write
    /// (§4, §8.4), so every device drops them from `_coven_lost`; a removed
    /// row is deleted for good, with its cascade, and never comes back. The
    /// dismissals follow the app's own changes in the write's record, so a
    /// value restored and its loss dismissed in one write go together. A
    /// write that also changes a row its dismissals delete fails with
    /// `DismissedRowChanged`. Deleting an owned removed row requires its
    /// owner's permission (§9.6); dismissing a frozen record changes no row.
    /// A value no longer lost as it was is left alone.
    pub fn dismiss_lost_values(&self, values: &[LostValue]) -> Result<(), DbError>;
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

/// The reads `SqlReadContext`, `CovenWrite` and `BulkSqlContext` offer, so
/// code that reads the same way inside a write and outside one is written
/// once. Inside a write it sees the write's own changes.
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
impl SqlRead for CovenWrite<'_> {}
impl SqlRead for BulkSqlContext<'_> {}

impl<'a, F, R, E, M: Mode> Read<'a, F, M>
where
    F: FnOnce(SqlReadContext<'_>) -> Result<R, E> + Send + 'static,
    R: Send + 'static,
    E: From<DbError> + coven::task::CarriesBug + Send + 'static,
{
    /// Runs `process` on the read's result on a separate worker, after the
    /// read has released its connection. The closure and result cross workers,
    /// so both must be Send and own any borrowed data.
    pub async fn process<P, T>(self, process: P) -> Result<T, E>
    where
        P: FnOnce(R) -> Result<T, E> + Send + 'static,
        T: Send + 'static;
}

impl<T, E> LiveQuery<T, E>
where
    T: Send + 'static,
    E: From<DbError> + coven::task::CarriesBug + Send + 'static,
{
    /// The first result at once; after that, waits for a write that changes
    /// the result and returns the new one. An error is a result too, and
    /// doesn't end the query, except the one saying the store closed, the
    /// handle fails every call (`RollbackFailed`, `StatePersistence`,
    /// `CheckpointFailed`, `WallTimer`, or `StoreFailed` for state a bug
    /// may have left wrong), or one that carries a bug, as a run that panicked
    /// does (`DbError::Bug`, E15): after it, `None`, so a loop over the
    /// query ends.
    pub async fn next(&mut self) -> Option<Result<T, E>>
    where
        T: Clone + PartialEq;

    /// Runs `process` on each result after the connection is released.
    pub fn process<P, U>(self, process: P) -> LiveQuery<U, E>
    where
        T: Clone + PartialEq,
        P: Fn(T) -> Result<U, E> + Send + Sync + 'static,
        U: Send + 'static;

    /// How often this query has run so far, its processed form's runs too.
    #[cfg(feature = "test-utils")]
    pub fn runs(&self) -> LiveQueryRuns;

    /// Whether a commit since this idle query's last result changes what it
    /// reads, so `next` runs it again. A commit marks the queries it changes
    /// as it commits, so this answers at once.
    #[cfg(feature = "test-utils")]
    pub fn is_marked_for_rerun(&self) -> bool;
}

impl<Q, T, E> ReconfigurableLiveQuery<Q, T, E>
where
    Q: Clone + PartialEq + Send + Sync + 'static,
    T: Send + 'static,
    E: From<DbError> + coven::task::CarriesBug + Send + 'static,
{
    /// Process each successful value on a worker after releasing its connection.
    /// Request, revision and cause stay on the event; errors remain results.
    /// Equality compares processed values. Existing request handles still work.
    pub fn process<P, U>(self, process: P) -> ReconfigurableLiveQuery<Q, U, E>
    where
        T: Clone + PartialEq,
        P: Fn(T) -> Result<U, E> + Send + Sync + 'static,
        U: Send + 'static;

    /// A handle that replaces the request.
    pub fn requests(&self) -> LiveQueryRequests<Q>;

    /// How often this query has run so far, its processed form's runs too.
    #[cfg(feature = "test-utils")]
    pub fn runs(&self) -> LiveQueryRuns;

    /// Whether a commit since this idle query's last result changes what it
    /// reads, so `next` runs it again, answered at once.
    #[cfg(feature = "test-utils")]
    pub fn is_marked_for_rerun(&self) -> bool;

    /// The next result, with the request and revision that produced it, and
    /// whether a new request, a write, or both caused it; `None` after the
    /// result saying the store closed or the handle failed, or one carrying
    /// a bug, as for `LiveQuery::next`.
    pub async fn next(&mut self) -> Option<ReconfigurableLiveQueryEvent<Q, T, E>>
    where
        T: Clone + PartialEq;
}

impl<Q: Clone + PartialEq> LiveQueryRequests<Q> {
    /// Replaces the request, returning the revision whose result will answer
    /// it. Fails once the query is dropped.
    pub fn set(&self, request: Q) -> Result<LiveQueryRevision, LiveQueryClosed>;
}

#[cfg(feature = "test-utils")]
impl LiveQueryRuns {
    /// The runs that have finished.
    pub fn runs(&self) -> u64;
    /// Of those, the runs a write caused, with no new request.
    pub fn writes(&self) -> u64;
    /// Of those, the runs whose result equaled the one before, which the
    /// query didn't return.
    pub fn equal(&self) -> u64;
}

/// Counts the work a store's connections run; clones count into the same
/// counts, and no other store's.
#[cfg(feature = "test-utils")]
#[derive(Clone)]
pub struct WorkCounter { /* private fields */ }

#[cfg(feature = "test-utils")]
impl WorkCounter {
    /// A counter nothing has counted on yet, for a test to give a store.
    pub fn new() -> Self;
    /// The work counted since the last take, starting the next count.
    pub fn take(&self) -> WorkCounts;
}

/// The work a store's connections ran.
#[cfg(feature = "test-utils")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkCounts {
    /// Times a call took the writer, read-only work included.
    pub writer_holds: usize,
    /// Transactions committed, unchanged ones included.
    pub commits: usize,
    /// Snapshot bodies parsed, to load, verify or check a file reference.
    pub snapshot_reads: usize,
    /// Rows whose published state an applied slot kept or put back beside
    /// this device's unsent writes (§6.2).
    pub published_rows: usize,
    /// Reads of what a transaction can't change: the schema version,
    /// SQLite's schema cookie, and synced columns' and foreign keys' ids.
    pub constant_reads: usize,
    /// SQLite steps the app's statements ran in `query` and `query_row`, in
    /// reads, live queries' runs, writes and migrations.
    pub app_read_steps: usize,
    /// SQLite steps the app's statements ran in `execute` and
    /// `execute_batch`, triggers' included, in writes and migrations.
    pub app_write_steps: usize,
    /// Steps of coven's lookups judging whether a write changed rows a
    /// live query joins (§4).
    pub lookup_steps: usize,
}

#[cfg(feature = "test-utils")]
impl CovenHandle {
    /// Counts the work the store's connections run into `counter`, from the
    /// next call on. Install it between calls, before the one to measure.
    pub async fn test_track_work(&self, counter: WorkCounter) -> Result<(), DbError>;
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

    /// Closes read and cache connections on all clones, releases their
    /// storage client and their store lock, while the app still holds the
    /// handle. From its first poll every clone refuses calls with
    /// `StoreClosed`, so none runs after closing begins; a call running then
    /// answers `StoreClosed` too, and discarding the store in this process
    /// waits for the close to release the readers' lock. Reports
    /// connection-close failures; cancellation does not stop closing, which
    /// runs as its own task (E15).
    pub async fn close(&self) -> Result<(), DbError>;

    pub fn read<F, R, E>(&self, read: F) -> Read<'_, F, ReadOnly>
    where
        F: FnOnce(SqlReadContext<'_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: From<DbError> + coven::task::CarriesBug + Send + 'static;
    /// The file a row references, by the id the row holds, as
    /// `CovenHandle::file_ref` reads it (E8).
    pub async fn file_ref(&self, file: FileId) -> Result<FileRef, DbError>;
    /// The path, size and modification time coven recorded for a file's
    /// user original on this device, or `None` when it has none.
    pub async fn user_file(&self, file: FileId) -> Result<Option<UserFile>, DbError>;
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>, FileReadError>;
    pub async fn open_file_stream(
        &self,
        file: &FileRef,
    ) -> Result<FileStream<ReadOnly>, FileReadError>;
}
```

- A read handle's file reads fill the cache like any other, which changes no
  synced state. It keeps no pending record of a file and sends nothing, so
  it has no upload or download in flight.

Example:

```rust
// The order is the query's; process only builds the app's types.
let titles: Vec<Title> = handle
    .read(|sql| Ok::<_, CovenError>(sql.query("SELECT title FROM notes ORDER BY title", [], |row| row.get(0))?))
    .process(|titles: Vec<String>| Ok(titles.into_iter().map(Title::new).collect()))
    .await?;

let mut notes: LiveQuery<_, CovenError> = handle.subscribe(|sql| {
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
  - Paths start with this store's id. Other stores can share a CloudKit zone
    or the Dropbox App folder. A Dropbox shared folder, a OneDrive folder and
    an S3 bucket hold one store each (below).
  - Once a location is recorded, setup reconnects at it, such as after this
    device's credentials were lost. It refuses a different location with
    `LocationChangeUnsupported`; setup does not relocate a store already used by other devices. The
    member whose account holds the store reconnects with that account; another account fails with
    `StoreOwner` (§20.9).
  - Creating claims slot 1, then the creator’s key-copy slot (§10.3). Waiting
    writes then claim a writes slot through ordinary sync.
  - Setup can take a while: it may wait out a minute of rate limits, and a
    reconnect loads what other devices published. So it reports its steps to
    its `on_progress` as `Progress<StorageSetupStep>`
    ([E1](#e1-opening)), on the task awaiting it, and the app shows them
    instead of a bare spinner. Creating a store that needs its location to
    itself first lists the location's root (`CheckingLocation`). S3 then
    detects settings (`DetectingSettings`). iCloud prepares a new store's
    zone (`PreparingZone`); Dropbox and OneDrive read the account's name
    (`ReadingAccount`). Every setup then connects and checks the provider
    (`Connecting`). Creating claims slot 1 (`CreatingStore`). Reconnecting
    loads with loading's own steps and counts (`Loading`, §13.4), then
    publishes changed access (`RecordingAccess`). A step other than `Loading`
    is one unit, done when it ends.
  - Setup is an operation with one point of no return
    ([§11.6](coven.md#116-every-call-that-writes-to-storage-in-steps),
    [§20.8](coven.md#208-checking-a-provider-before-use)). Before its first
    write to storage it journals itself, listed in `coven_operations` as
    `StorageSetup`, and keeps the credentials it connects with in a device
    entry of its own. `cancel_storage_setup` from the setup call until
    `CreatingStore` or `RecordingAccess` begins answers `Undoing`: setup undoes
    what it wrote and fails with `Cancelled`, its outcome `Cancelled`. A cancel
    that comes before setup has journaled itself is kept: setup journals, then
    undoes itself at once. From those steps on it
    answers `Finishing`, and setup finishes; the app stops offering Cancel when
    either step begins. Dropping the call leaves setup running to its end.
  - Only finishing commits the location, sign-in and restore code on the
    device; the outcome is `Landed` at slot 1 for a creation, `Landed` at the
    `Set access` slot or `Satisfied` for a reconnect. A failure before the point
    of no return undoes setup, outcome `Undone(Failed)`; one after it is
    returned while setup stays running, and setup starts the sync loop, whose
    connection finishes it. A claim refused after it, slot 1 or the location
    marker another store holds, or a refused `Set access`, undoes setup:
    `Undone(Failed)` or `Rejected`.
  - Opening a store undoes a setup a crash cut before its point of no return,
    outcome `Undone(Interrupted)`, and leaves one past it running: the first
    connection attempt after `start_sync` connects with setup's own
    credentials, settles its claim and finishes it. Another setup meanwhile
    fails with `SetupRunning`.
- The app calls `authenticate(access)` on the handle before OAuth setup.
  Coven owns the request, checked redirect and code exchange. The builder's
  presenter opens the page and returns the redirect or cancellation; it receives
  no tokens. Sign-in is held in coven's session custody until setup succeeds,
  and from setup's journal on also in setup's own device entry (§11.6).
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
  returns `StorageSetupError::Check(StorageCheckError::ProviderCheck)`, naming the operation and its
  classified cause; the error retains a cleanup failure too if both fail.
  It also checks conditional replacement for the floor: a matching revision
  succeeds and a stale revision changes nothing. A request refused as
  `RateLimited` during setup is sent again after its requested wait or the
  ordinary delay, until the setup call has waited a minute in all (§20.8).
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
- A Dropbox shared folder, a OneDrive folder and an S3 bucket hold one store
  each from creation (§20.3): any member reaches everything in them, so a
  member of one store could delete another store's objects there. On S3 coven
  can't tell how far a member's key reaches, so it takes every key to reach
  the whole bucket (§20.7). A store's first setup lists the location's root
  before any write and permits only that store's folder and the root
  `coven-location` marker. Setup later checks that the marker names the same
  store (§20.3). Any other object causes `StorageFailure::OwnLocationRequired`,
  wrapped in `StorageSetupError::Storage`. Reconnecting, restoring and joining
  read that exact marker to verify the location is the store's (§20.3); they
  do not list outside the store's folder. A Dropbox App folder, which can't be
  shared, has no marker and no check.
- An endpoint matching no known host and naming no profile has no AWS
  fallback: setup detects its settings and returns and records the location
  with them as a custom profile. Reconnecting with that same location and no
  profile reuses the recorded settings.
- `detect_s3_settings` runs the same detection for an app that offers
  advanced settings: it probes the bucket with the supplied key, writing only
  fresh objects under the store's `.coven-check` folder and deleting them,
  also on failure. Each detection records its run before its first probe, and
  removes what earlier recorded runs at that bucket left, so a
  detection a crash cut leaves no upload billing for good. Its `S3Detection`
  holds the detected settings and one
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
  - `sync_often(interval)` makes idle passes come at most `interval` apart
    until the `SyncOftenGuard` it returns drops, such as while a screen shows
    a restore code and waits for the new device to register. With several
    guards held, the shortest interval wins; once the last drops, idle passes
    return to every 30 seconds. The next idle pass comes `interval` after the
    last pass ended, so taking a guard when that time has passed runs one at
    once. Connection backoff and provider cooldowns still hold every pass.
    Coven sets no lower bound: each idle pass costs its requests, two for a
    quiet one. The request outlives stopping and starting sync, and runs no
    pass while sync is stopped ([§6.7](coven.md#67-waiting-and-retrying)).
  - `sync_now` never shortens a backoff. Coven watches the device's network
    itself, from opening to closing, and needs no call from the app
    ([§6.7](coven.md#67-waiting-and-retrying)). When the device gains a
    usable network, or moves to another, coven makes a network report: it
    ends every backoff whose last attempt got no provider response, the
    connection's, which the sync state shows as `Offline`, and those of
    coven's operations and queued uploads, whose pending records say
    `answered: false`, and it requests a pass. A backoff after a provider's
    answer, such as a server error, and a provider cooldown hold: a new
    network changes neither. A store that opens on a usable network reports
    it, so a backoff saved while the device had none ends at once. Losing
    the network, or a network turning metered or unmetered, makes no
    report.
  - Each backoff ends durably, keeping its failure count: reopening after the
    report arms none. Reports during a pass coalesce into one more pass, and
    reports during transfers are acted on after them. If the running pass or
    transfer fails for want of a network, its wait ends too: the failure may
    have come before the change. Reports while the connection is stopped apply
    when it starts.
  - What each platform says, and so what coven hears:

    | Platform | Coven asks | A usable network | Metered |
    |---|---|---|---|
    | macOS, iOS | Network.framework's path monitor | the path is satisfied; another set of interfaces is another network | the path is expensive (cellular, a phone's hotspot) or constrained (Low Data Mode) |
    | Android | `ConnectivityManager`'s default-network callback, through coven's Android library (E1) | the default network is validated and not blocked for the app, as Doze and App Standby block it; another default network is another network. Before Android 10 a blocked network isn't reported, so the end of Doze counts as the network coming back | the network has neither `NOT_METERED` nor `TEMPORARILY_NOT_METERED` |
    | Windows | `NetworkInformation`'s internet connection profile, and its `NetworkStatusChanged` event | the profile has internet access; another profile is another network | the profile's cost is fixed or variable, roaming, or over its data limit |
    | Linux | NetworkManager, on the system bus | its connectivity is full, or, where it checks none, it is connected globally; another primary connection is another network | its `Metered` says yes, or guesses yes |
    | Linux without NetworkManager as the store opens, or without a system bus | nothing, and the log says so | never reported | never |

    Where coven asks nothing, it makes no report, and no network is metered,
    so the setting to move files only on unmetered networks holds nothing
    there (E7). A watch the platform refuses fails opening with
    `CovenError::NetworkWatch` (E1), since a store that can't watch would
    move files on a metered network the person asked it to keep off; on
    Android that is most often an app that didn't package coven's Android
    library. Windows and Linux read the network again at each change; a
    read that fails after opening logs an error and says coven can't tell,
    so `coven_network.usable` is NULL, until the next read succeeds. On
    Linux that includes NetworkManager stopping, until it starts again, and
    the system bus closing, for as long as the store stays open.
    Meanwhile coven calls the network metered, since it may be:
    `coven_network.metered` says 1, and the setting holds what it holds on
    a metered network, with `SyncWait::Metered` (E7).
    A panic in coven's reading of what the platform says can't unwind into
    the platform's callback, which would end the process. It goes to the
    app's bug sink (E15), and coven can't tell, as after a failed read,
    until the platform says again. On Linux, where coven follows
    NetworkManager on a thread of its own, that thread ends with it, as
    when the system bus closes.
  - `retry_now` is the person asking to retry everything now, such as by
    pulling to refresh. `sync_now` leaves every backoff in place; `retry_now`
    ends every ordinary one, answered or not: the connection's, operations',
    uploads' and downloads'. It requests a pass, travels and coalesces
    as a network report does, and keeps each wait's failure count, so a retry
    that fails again waits the next longer delay. Provider cooldowns,
    failures that never retry, and waits for new credentials or a new
    connection hold ([§6.7](coven.md#67-waiting-and-retrying)).
  - `retry_now` returns, once the request is in place, a `RetryAnswer`: a
    future that resolves when the first pass after the request ends, so a
    pull-to-refresh spinner can stay until then: `Synced`, or `Failed` with
    the pass's failure. A request made while a pass runs waits for the next
    pass, which it runs; requests made together share one. While a provider
    cooldown holds every pass it resolves at once, `CoolingDown` with when the
    cooldown ends; while a failure only new credentials, an update or a reset
    can clear holds them, `Failed` with that failure; while sync is stopped,
    or on a closed handle, `NotSyncing`. A request already waiting when a
    cooldown or such a failure begins hears so when the connection's owner
    next plans, and stopping answers every waiting request `NotSyncing`.
    Dropping the future leaves the retry in place. The answer is about the
    pass; uploads retry on their own, downloads when the app reads or pins
    again, and uploads and pins report in `pending()`.
  - What `retry_now` would do if called now is the one row of
    `coven_retry_effect` (E4), which the app follows with a live query, as
    `RetryEffect::read` reads it, or with a local trigger. `pass`: a pass runs
    and its end answers the retry. `cooling_down` with `cooling_until`,
    `failed` and `not_syncing`: the retry gets that answer at once, with no
    pass. The row holds what decides it, whether sync is started, whether such
    a failure holds every pass, and the cooldown's end, and computes `effect`
    from them. Each change writes the row in the step that makes it, on the
    store's writer, and the retry's answer is decided there from the same row,
    so an app that offers Retry Now only while the effect is `pass` offers it
    only when pressing it runs a pass; the sync state says why it is not. A
    cooldown's end is fixed by the store's clock when it is recorded and kept
    to the millisecond, rounded up, so the row and the answer name one
    instant. Its end is a write too: the connection's owner wakes when the
    cooldown ends and writes it, and any earlier step that finds it over
    writes it first, so the app polls nothing. The owner takes that wake
    before it writes the row, so a cooldown the write still names that ends
    right after wakes it too. While sync is stopped the row
    keeps no cooldown, since no owner would write its end; a held failure
    stays across stopping and starting, as the connection's retry state does.
    Opening writes `not_syncing` with nothing held
    ([§6.7](coven.md#67-waiting-and-retrying)).
  - A write of the row that fails fails the handle, as a pending record does
    (below), and the requests waiting then are answered `Failed` with that
    failure, the `DbError` every later call reports, inside
    `SyncFailure::Other`. `retry_now` and `stop_sync` return it.
  - [A sync pass (§6)](coven.md#6-a-sync-pass) gives the read, apply, claim and effects order.
    An idle pass reads the next slot and the floor: two requests.
  - A missing slot below the floor reloads its snapshots. Returning devices retain eligible queued
    writes ([§16.5](coven.md#165-coming-back-after-a-long-absence)).
  - `Blocked` names a slot that cannot apply and its cause.
    [§5.4's stuck-device rule](coven.md#54-a-stuck-device-stops) applies;
    local reads and eligible app writes continue.
  - File transfers run outside the pass. Their failures appear in `pending()`
    without blocking sequence application.
  - A synced pass reached its read end. Files, provider effects or
    retention may still be pending, but an unapplied discovered slot prevents it.
  - Each pending record names this device's first unmet condition for that subject. Cut-off writes
    record `WritesRefused`; [§19.1](coven.md#191-what-the-app-sees) specifies retries.

- The sync state is what a status screen shows: one of the states of
  [§19.1](coven.md#191-what-the-app-sees), its cause where it has one, and
  when sync last finished. `sync_status()` reads it, and
  `subscribe_sync_status()` follows it: its first value is the
  current state, read at once from what opening wrote, so the app shows no
  placeholder before it. Coven names the states; the app picks the words.
  - The state is computed in the read from the rows of `coven_sync_status`
    and `coven_retry_effect` (E4), the send queue, the upload queue with its
    pause and retry waits, `coven_network`, the files being sent or
    downloaded and the pending records, so the live query runs again
    whenever one of them changes: a write committed while offline makes it offline with unsent
    changes in that transaction. With several candidates, the first in
    §19.1's order shows; Appendix H.3 of coven.md lists which state each
    fact names.
  - Synced means nothing waits, data or files: a pass held back at a slot
    that can't apply yet or by a provider's cooldown waits to sync, with what
    holds it (`SyncWait`); while a file waits in the upload queue the state
    is syncing as files move, waiting to sync while none does, with what the
    first waiting file waits for, or failed when its source failed, naming
    it.
  - Syncing shows only while a pass has read or claimed a slot, or a file is
    moving. A pass that finds nothing writes nothing the state reads until it
    ends, and then only the last synced time when it ends synced again, so
    an idle store's state doesn't flicker every 30 seconds.
  - The cause is the last pass, as `LastPass` keeps it, or the first pending
    record, in the order they arose, that names the state, with the cause
    `coven_pending` keeps beside it. Signed out with no credentials has none.
  - The last synced time is kept across every state and every reopen, and
    never goes backward.
- The row holds what the loop knows. Opening writes whether storage is set
  up there, and that no pass is moving, before the handle exists, and keeps
  the last pass and the last synced time. Setup, joining and restoring write
  storage set up and their finish as the last synced time. The loop writes
  that the pass moved a slot as it first does, and how the pass ended as it
  ends, before its retries hear the answer. A row that can't be written fails
  the handle, as a pending record does (below), and the loop ends. Each error
  is kept as its text, with its typed category beside it (`StatusFailure`),
  so a state read after its process ended still says why. The row is there
  from the store's creation on, saying no storage is set up until the first
  open writes over it, so a read always finds one.
- The row, like `coven_retry_effect`'s, describes the sync of the handle that
  holds the store's writer lock. A read-only handle can't tell whether the
  writer that last wrote it still runs: one that ended mid-pass left its move
  behind, and no write marks its end. So app SQL on a read-only handle may
  not read either row, directly or through the app's own view:
  `StatementForbidden`; and `CovenReadHandle` has no `sync_status`. The
  app's trigger on them still lets a read-only handle open, since it never
  runs there.
- An offline pass describes the last failed attempt, not the connection's
  history. A failed pass: storage refused the whole pass, or coven's own
  operation failed; one that never clears stops sync until a reset.
  - Keep whether the provider answered; `StorageFailure::Network` alone
    cannot decide this. A server error response is not an offline device.
  - A denied required read can fail the pass. A slot’s unavailable key, schema
    or damaged bytes blocks it. A local failure applying it, such as a
    locked keychain, fails it and retries under backoff (§19.1). A failed
    file or retention wait stays with its subject.
  - Removal records a `Connection` block with `Removed`, and stops the loop
    for good. Another copy using this device's id fails the pass with
    `DeviceIdInUse` and stops the loop, recording no pending record (§19.3).
    Local failures also name their pending subject and preserve their typed
    cause for a waiting caller.
  - If the database cannot save a pending record or the sync status row, stop
    the loop and fail database calls and live queries with that cause, as
    `DbError::StatePersistence`, until the store is reopened.
    Never expose an apparently empty pending list after losing its update.
- E.g. Ana syncs successfully, then loses Wi-Fi. The next attempt is offline,
  and her state too. A later provider error denying the required read fails
  the pass: no access to storage. One bad photo while the rest syncs leaves
  the last pass synced; its upload's pending record makes the state failed,
  with that record as the cause. Paused uploads with photos queued make it
  `WaitingToSync(SyncWait::Paused)`; photos queued on cellular while the
  app moves files only on unmetered networks make it
  `WaitingToSync(SyncWait::Metered)`.
- A device that isn't connected still reads and writes
  ([§2.1](coven.md#21-local-writes)); its writes wait in `_coven_uploads`.
- `start_sync` starts the loop; its first pass builds the provider client from
  custody credentials, failing and backing off like any pass. Tokens refresh
  on provider rejection (§20.2).
- `stop_sync` first writes `coven_retry_effect` as not running and answers
  the waiting retries; then the active pass and transfers finish, the
  providers are released and the location recorded, and it returns. Once it
  has, no pass or upload runs until sync starts again. It holds the store's
  call lock throughout, so a `start_sync` asked meanwhile starts after it, and it
  finishes even if the app drops its future. Keys, credentials and location
  remain. Stopping changes nothing the sync state reads; a failure releasing
  the providers is recorded as the last pass's failure, with no pending
  connection record, and returned.

Storage failures retain the same `StorageFailure` through setup, sync state,
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
an upload's destination. S3 has no account label, and coven records none for
CloudKit ([§13.1](coven.md#131-a-persons-new-device-the-restore-code)).

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
    /// Whether a person signs in to it in a browser (OAuth): Dropbox and
    /// OneDrive.
    pub fn signs_in_with_oauth(self) -> bool;
}

/// A store's location, with credentials kept separately (§20.3, E5).
pub enum StorageConfig {
    /// An existing S3 bucket, which the store has to itself: coven takes
    /// every member key to reach the whole bucket (§20.3, §20.7). Paths
    /// start at the bucket's root.
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
    },
    /// Required creation choice: an App folder or a shared-capable namespace,
    /// which holds this store alone (§20.3).
    Dropbox {
        location: DropboxLocation,
        /// Display only; None before setup. Coven reads this member's account at setup.
        account_label: Option<String>,
    },
    /// A OneDrive folder holding this store alone (§20.3); paths within it
    /// start with the store id.
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

/// The detection runs one detection cleans up after (§20.7): its own, which
/// the caller recorded before detection writes its first probe, and the
/// earlier runs at the same bucket whose probes and uploads no cleanup has
/// confirmed gone.
pub struct S3DetectionRuns {
    /// This detection's run: its probes go under `.coven-check/<run>/`.
    pub run: Uuid,
    /// The earlier runs whose leftovers it removes after its own.
    pub earlier: Vec<Uuid>,
}

/// What one detection did (§20.7).
pub struct S3DetectionRun {
    /// What it observed, or why it failed; a failed cleanup fails it.
    pub detection: Result<S3Detection, StorageError>,
    /// The runs whose probes and uploads it confirmed gone: its own when it
    /// wrote nothing or its cleanup succeeded, and each earlier run whose
    /// leftovers it removed.
    pub cleaned: Vec<Uuid>,
}

impl S3DetectionRun {
    /// A detection that wrote nothing: its own run left nothing, and it
    /// removed no earlier run's leftovers.
    pub fn unwritten(runs: &S3DetectionRuns, detection: Result<S3Detection, StorageError>) -> Self;
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
    pub fn check(&self, settings: &S3Settings) -> Result<(), StorageCheckError>;
    /// The location setup records, after checking versioning support and its effective settings:
    /// an unrecognized endpoint naming no profile gets these settings as its
    /// custom profile; any other location is kept as given.
    pub fn resolve(&self, location: StorageConfig) -> Result<StorageConfig, StorageCheckError>;
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
    /// Whether both are S3 locations naming the same bucket, at one endpoint
    /// and region, whatever profile reaches it.
    pub fn same_s3_bucket(&self, other: &Self) -> bool;

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
    /// File downloads running at once, pins' and file ranges'. Reading the
    /// sequence doesn't count against it: its 16 reads ahead bound it
    /// ([§21.1](coven.md#211-counting-the-requests-of-a-pass)).
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
    /// No route to the provider, a timeout, an interrupted response, or a
    /// server failure the provider answered with (§6.7).
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
    /// Every member reaches the whole location (a Dropbox shared folder, a
    /// OneDrive folder, or an S3 bucket), so a store there needs the location
    /// to itself (§20.3). Creating the store found objects outside its own
    /// folder in the location, or the location marker names another store;
    /// reconnecting, restoring or joining found the marker missing,
    /// malformed or naming another store.
    OwnLocationRequired,
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
    /// Two complete reads of one immutable object returned different bytes:
    /// a fault on the way to the device changed one of them (§19.1).
    Corrupted,
    /// The response or recorded session is malformed.
    Protocol,
    /// An object path is outside the store's layout.
    InvalidPath,
    /// A range is empty, reversed or beyond the object's end.
    InvalidRange,
    /// Coven changes sharing only from the account holding the store. That is
    /// coven's rule, not every provider's limit (§13.2).
    NotStoreOwner,
    /// The invited account is the one holding the store (§13.2).
    InviteeIsStoreOwner,
    /// Dropbox cannot upgrade a pending viewer without the recipient's account id.
    AccountIdUnavailable,
    /// The invite belongs to another provider location.
    InvitationMismatch,
    /// The session belongs to another provider or location.
    SessionMismatch,
    /// An S3 session at a shared slot or floor path may have started an
    /// upload whose native id it never recorded, and nothing tells that
    /// upload from another claimant's (Appendix E.10).
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
    /// so this profile refuses it (§20.6 says why).
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
    /// Coven's work for the request panicked: a bug (E15). `StorageError::Bug`
    /// carries it. Like a local failure's `Bug`, it retries under backoff and
    /// shows failed meanwhile (§19.1).
    Bug,

    /// The installation must complete membership and key-copy catch-up before sending.
    CatchUpRequired,
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
    /// Coven's work for the request panicked: a bug (E15). Its failure is
    /// `StorageFailure::Bug`.
    Bug(Bug),
}

impl StorageError {
    /// The stored classification; for Cleanup, the original operation's failure.
    pub fn failure(&self) -> StorageFailure;
    /// The failure when the request alone failed; none for Cleanup, which
    /// leaves state behind that handling the failure itself doesn't know of.
    pub fn own_failure(&self) -> Option<StorageFailure>;
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

/// A provider or S3 settings check refused the location (E5).
pub enum StorageCheckError {
    /// The profile has no certification with bucket versioning enabled.
    S3VersioningUnsupported { state: S3BucketVersioning },
    /// The bucket state is unknown and the profile has no versioning certification.
    S3VersioningUnknown { source: Arc<StorageError> },
    /// A provider check failed; cleanup failures retain both causes.
    ProviderCheck { check: StorageCheck, source: StorageError },
    /// A classified storage failure.
    Storage(StorageError),
}

/// Storage setup failed before committing credentials and keys (E5).
pub enum StorageSetupError {
    /// A provider or S3 settings check refused the location. A check's
    /// storage failure is `Storage`, not `Check(Storage(..))`.
    Check(StorageCheckError),
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
    /// This member's account holds the store, and setup signed in with
    /// another account. Their access stays on the account that holds it:
    /// the person signs in with that one (§10.1, §20.9).
    StoreOwner,
    /// The store needs a newer app schema or coven format (§18).
    UpdateRequired(RequiredUpdate),
    /// The app cancelled setup before its point of no return, and setup
    /// undid what it wrote (§11.6).
    Cancelled,
    /// Another setup call is running on this store, or a setup past its
    /// point of no return is still finishing; starting sync finishes that
    /// one (§11.6).
    SetupRunning,
    /// Local setup failed, with its cause.
    Internal(Box<dyn std::error::Error + Send + Sync>),
    /// Setup's runtime is shutting down and dropped it partway, so how much
    /// of it was saved is unknown (E15).
    Shutdown(Shutdown),
    /// Setup panicked: a bug (E15). How much of it was saved is unknown.
    Bug(Bug),
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
    /// Unlocking's runtime is shutting down and dropped it partway, so
    /// whether it kept the keys it opened is unknown (E15).
    Shutdown(Shutdown),
    /// Unlocking panicked: a bug (E15). Whether it kept the keys it opened
    /// is unknown.
    Bug(Bug),
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
    /// removed and their access can't move to another account (§10.1, §20.9,
    /// §14.2): OneDrive and CloudKit can't hand a store to another account,
    /// and coven doesn't use Dropbox's ownership transfer (bae-fm/coven#237).
    StoreOwner,
    /// This call needs a newer app schema or coven format (§18).
    UpdateRequired(RequiredUpdate),
    /// A code can't be used for this call, as an invite code naming another
    /// store than the join's directory. Bootstrap reports it as
    /// `BootstrapError::Code`.
    Code(CodeError),
    /// The requested journal row has no retryable work for this call.
    NotPending(OperationId),

    /// Decoding persisted operation data failed, retaining its cause.
    OperationData(serde_json::Error),

    /// Administrative and snapshot publication wait for the retained reload (§11).
    ReloadPending(OperationId),
    /// Deleting stored files waits for this unfinished snapshot, whose
    /// journal still owns references no row holds (§17.10).
    SnapshotPending(OperationId),
    /// The call's runtime is shutting down and dropped its work (E15): a
    /// database call it made changed nothing; a call that runs as its own
    /// task, as closing and replacing credentials do, may have stopped
    /// partway. Its category is `LocalFailure::Shutdown`.
    Shutdown(Shutdown),
    /// The call's work panicked: a bug in coven, which the caller reports
    /// (E15). Its category is `LocalFailure::Bug`.
    Bug(Bug),
    /// The worker this call waits on hit a bug: the sync loop, a
    /// connection attempt or the upload worker panicked and ended, the sync
    /// loop ended on a bug in its own work, or the upload worker's run
    /// serving this call failed on it. The bug sink
    /// heard the bug as it happened, so the app shows this and doesn't
    /// report it again (E15). Its category is `LocalFailure::Bug`.
    WorkerFailed(Bug),
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
    /// The text of the error behind `reason`, naming every cause below it:
    /// an upload, download or read no caller awaited, an attempt of an
    /// operation coven drives itself, or what blocked a slot.
    /// `coven_pending.cause` keeps it, written with the record. Only the run
    /// that met the error writes one: opening clears every cause, and a
    /// failure that went back to an app call leaves none. Equality ignores
    /// it. A record's text names its subject, its reason and this error.
    pub cause: Option<String>,
}

/// Stable identities; this device keeps one record per subject.
pub enum PendingSubject {
    Write(WriteId),
    Slot(Slot),
    KeyCopy { audience: Audience, key: KeyId, member: MemberId, sealing: SealingKey },

    Snapshot(SnapshotId),
    /// This device's upload of a stored object, and its reads of its staged
    /// original while the upload waits.
    Upload { device: DeviceId, object: ObjectId },
    /// This device's downloads of a stored object, for the app's pins; a
    /// read's failure goes to its caller and leaves no record (§17.7).
    Download { device: DeviceId, object: ObjectId },
    /// A pin coven keeps for a file kept here, which it downloads itself
    /// (§17.8).
    Keep { file: FileId },
    /// A move of this device's files to the cloud: the files one write staged
    /// for the cloud, or the files the app moved (§17.3).
    Move { id: MoveId },
    Operation { id: OperationId, kind: OperationKind },
    Retention { path: ObjectPath },
    Audience(Audience),
    Connection,
}

/// A condition coven has not yet observed (§19.1).
pub enum Prerequisite {
    /// A completed read and apply through the current sequence end (§11.1).
    SequenceCatchUp,
    /// This audience must reload.
    Reload(Audience),
    /// This audience must be raised to this schema version: a queued write
    /// of this device's at that version has a part in it, and every device
    /// refuses that part until the raise (§18.5).
    Raise { audience: Audience, version: u32 },
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
    /// The work's runtime is shutting down; the next process runs it again,
    /// so it clears by itself (E15).
    Shutdown,
    /// Coven's own code broke its rule or panicked: a bug. The work retries
    /// under backoff, as a database failure does, and fails again until the
    /// bug is fixed (E15).
    Bug,
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
    AccountInUse { account: String },
    AccessRemains { account: String, shares: Vec<RetainedAccess> },
    FileUnavailable(FileMissingReason),
    /// A move's original is missing, and the move waits for it, or changed,
    /// and the move failed (§17.3).
    Original { path: PathBuf, failure: FileSourceFailure },
    Failed(LocalFailure),
    /// Never a slot's: a slot isn't this device's own request, so a
    /// rejection there blocks it as `Failed(Operation)`.
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
    /// The record of `subject`'s first unmet condition, with no cause.
    pub fn new(subject: PendingSubject, reason: PendingReason) -> Self;

    /// Automatic: retryable reads, keys, snapshots, provider work and
    /// unfinished operation/undo work, subject to shared backoff.
    /// Storage Network/Conflict/RateLimited/NotFound/SessionExpired retry
    /// automatically. So do local database, disk or custody failures, such
    /// as a locked keychain, except in an operation the app started: its call
    /// returns them and the app calls again (§19.1). Uploads and coven-owned
    /// operations also retry storage QuotaExceeded/File.
    /// AfterUpdate: refused immutable objects, which every build of coven
    /// other than the one that refused them rechecks once, and unsupported
    /// formats/schema, retried once coven or the app supports them (§19.1).
    /// Never: WritesRefused or this device's removal/replacement.
    /// AppAction: failures a person clears, such as required credentials;
    /// retried on new credentials or connection.
    /// Other permanent file or coven-owned operation failures are Never.
    /// A move's missing original, and coven's copy failing its check, retry
    /// automatically. A changed original fails its move, which is AppAction:
    /// the app cancels the move (§17.3).
    /// Only a pin's failed download is recorded, and its call returns the
    /// failure: one that would otherwise retry on its own, or clear when a
    /// person acts, is AppAction, retried by the app's next pin or read
    /// (§17.7). A pin coven keeps (`Keep`) is coven's own work: its failures
    /// retry under the shared backoff, a person's clear on new credentials or
    /// connection, and a permanent one never (§17.8).
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
    /// The author's device, membership or role does not permit it; for a
    /// snapshot, also this device's raise of its audience still waiting
    /// (§16.2).
    NotAllowed,

    /// The provider account owns the store: its member can't be removed, and
    /// their access can't move to another account (§10.1): OneDrive and
    /// CloudKit can't hand a store to another account, and coven doesn't use
    /// Dropbox's ownership transfer (bae-fm/coven#237).
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
    /// `on_progress` is called on this task with each step (E5).
    pub async fn setup_s3_storage(
        &self,
        storage: StorageConfig,
        device_name: &str,
        access_key_id: String,
        secret_access_key: SecretText,
        on_progress: impl Fn(Progress<StorageSetupStep>),
    ) -> Result<StorageSetup, StorageSetupError>;

    /// Probes the S3 service at this location with this member's key (§20.7),
    /// writing only fresh objects under the store's `.coven-check` folder and
    /// deleting them, also on failure, then removing what earlier recorded
    /// detection runs at that bucket left. Commits only its run's
    /// record, kept until its probes and uploads are confirmed gone.
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
    /// `on_progress` is called on this task with each step (E5).
    pub async fn setup_oauth_storage(
        &self,
        storage: StorageConfig,
        device_name: &str,
        on_progress: impl Fn(Progress<StorageSetupStep>),
    ) -> Result<StorageSetup, StorageSetupError>;

    /// Sets up iCloud through the built-in adapter; other platforms return ProviderUnavailable.
    /// Coven obtains the current owner and creates the store's custom zone.
    /// `on_progress` is called on this task with each step (E5).
    pub async fn setup_cloudkit_storage(
        &self,
        container: &str,
        device_name: &str,
        on_progress: impl Fn(Progress<StorageSetupStep>),
    ) -> Result<StorageSetup, StorageSetupError>;

    /// Cancels the storage setup running on this store (§11.6), from the
    /// moment its call starts: before its point of no return it undoes what
    /// it wrote and fails with `Cancelled`; from its `CreatingStore` or
    /// `RecordingAccess` step on, or once a crash cut it there, it finishes.
    /// Takes no lock.
    pub fn cancel_storage_setup(&self) -> SetupCancellation;

    /// Opens store keys from their copies sealed to this member in storage
    /// (§12.1), keeps them in custody, and connects without starting sync.
    /// Uses the member-key session already held at open; it does not unlock
    /// custody again. Key selection follows §12.2; other waits remain in pending().
    pub async fn unlock_store_key(&self) -> Result<ConnectedStorage, StoreKeyUnlockError>;

    /// Whether the held session has the store key: `Available` or `Locked`.
    /// Reads no custody and derives no passphrase key.
    pub fn store_key_state(&self) -> Result<StoreKeyState, KeyError>;

    /// Starts syncing with the keys unlocked for this session; the first pass
    /// connects. Does nothing if already running or no storage is set up.
    /// Once the sync loop has ended on its own, it fails with why:
    /// `SyncError::WorkerFailed` for a panic or a bug in the loop's own
    /// work, and otherwise the failure that ended it (E15).
    pub async fn start_sync(&self) -> Result<(), SyncError>;

    /// Stops syncing, retaining keys, credentials and location: once
    /// `coven_retry_effect` says sync isn't running and the waiting retries
    /// are answered `NotSyncing`, the active pass and transfers finish, the
    /// providers are released, and it returns. A `start_sync` asked
    /// meanwhile starts after it; dropping the future leaves it to finish.
    /// A release failure is recorded as the last pass's failure, without a
    /// pending record, and returned. A failed write of either row has failed
    /// the handle; stopping goes on and the failure is returned.
    pub async fn stop_sync(&self) -> Result<(), SyncError>;

    /// Syncs now instead of at the next idle tick. While idle, coven syncs
    /// every 30 seconds, and at once after a local write. Requests coalesce
    /// while a pass or connection backoff is active; provider cooldowns hold.
    pub fn sync_now(&self);

    /// The person asked to retry everything now. Ends every ordinary
    /// backoff, answered or not: the connection's, operations' and
    /// uploads'; each keeps its failure count. Syncs now, as
    /// `sync_now` does. Provider cooldowns and failures that never retry
    /// hold. Requests coalesce with network reports (§6.7). The answer
    /// resolves when the first pass after the request ends, or at once as
    /// `coven_retry_effect` says when no pass runs for it. A failed write of
    /// that row has failed the handle, and is returned.
    pub async fn retry_now(&self) -> Result<RetryAnswer, SyncError>;

    /// Idle passes come at most `interval` apart, instead of every 30
    /// seconds, until the guard drops. The shortest interval held wins.
    /// Connection backoff and provider cooldowns hold (§6.7).
    pub fn sync_often(&self, interval: Duration) -> SyncOftenGuard;

    /// The latest completed replacement stored in this installation, or None.
    /// It survives reopening; unsent edits were discarded, or may be lost
    /// for a damaged database (§19.3).
    pub async fn device_reset(&self) -> CovenResult<Option<DeviceReset>>;

    /// The same pre-built query, including a replacement completed during open.
    pub fn subscribe_device_reset(&self) -> LiveQuery<Option<DeviceReset>>;

    /// This device's pending work, in the order the records arose, as
    /// `coven_pending` lists them by position, including offline, each with
    /// the cause the view keeps beside it (§19.1).
    pub async fn pending(&self) -> CovenResult<Vec<PendingRecord>>;

    /// The sync state and when sync last finished, computed from
    /// `coven_sync_status`, `coven_retry_effect`, the send queue, the upload
    /// queue, `coven_network`, the files moving and the pending records in
    /// one committed
    /// snapshot (§19.1). `CovenReadHandle` has none.
    pub async fn sync_status(&self) -> CovenResult<SyncStatus>;

    /// The same pre-built query, live (E4): its first value is the current
    /// state.
    pub fn subscribe_sync_status(&self) -> LiveQuery<SyncStatus>;

    /// How many file uploads and file downloads run at once.
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
    /// attempt; it retries under backoff (E5). The record carries the cause
    /// it keeps, and the failure's text names the subject, the reason and
    /// that error.
    Operation(Box<PendingRecord>),
    /// Any other failure, retaining its typed cause.
    Other(Arc<dyn std::error::Error + Send + Sync>),
}

/// What a status screen shows (E5, §19.1): the sync state and when sync
/// last finished, as `sync_status()` reads it and `subscribe_sync_status()`
/// follows it.
pub struct SyncStatus {
    pub state: SyncState,
    /// When a pass last reached the end of the sequence, by the store's
    /// clock; kept across every state and never earlier than before.
    pub last_synced: Option<SystemTime>,
}

/// The sync state (§19.1). With several candidates, the first of `Removed`,
/// `NeedsReset`, `UpdateNeeded`, `SignedOut`, `NoAccess`, `FolderMissing`,
/// `ClockWrong`, `StorageFull`, `OfflineWithUnsentChanges`, `Offline`,
/// `Failed`, `Syncing`, `WaitingToSync`, `Synced` shows; `NotSynced` comes
/// before them all.
pub enum SyncState {
    /// No storage location is recorded on this device.
    NotSynced,
    /// A running pass has read or claimed a slot, or a file is being sent
    /// or downloaded.
    Syncing,
    /// Nothing moves, and the last pass was held back, at a slot that can't
    /// apply yet or by a provider's cooldown, or a file waits in the upload
    /// queue: what the pass waits for, else what the first such file waits
    /// for.
    WaitingToSync(SyncWait),
    /// The last pass reached the end of the sequence, every file this device
    /// uploads is stored, and nothing below holds: nothing waits.
    Synced,
    /// The last pass got no provider response.
    Offline(SyncCause),
    /// Offline, and the send queue holds a write or the upload queue a file.
    OfflineWithUnsentChanges(SyncCause),
    /// A location is recorded, but this device holds no credentials for it
    /// (no cause), or storage refused them.
    SignedOut { provider: CloudProvider, cause: Option<SyncCause> },
    /// Signed in, but the account isn't allowed.
    NoAccess(SyncCause),
    StorageFull(SyncCause),
    FolderMissing(SyncCause),
    ClockWrong(SyncCause),
    /// A newer app or coven is needed.
    UpdateNeeded(SyncCause),
    /// This device or its member was removed.
    Removed(SyncCause),
    /// Another copy uses this device's id, or its member keys were replaced.
    NeedsReset(SyncCause),
    /// Any other failure, or a file that can't be uploaded or read, whose
    /// pending record is the cause.
    Failed(SyncCause),
}

/// What waiting to sync waits for, as `WaitingToSync` names it: what holds
/// every transfer, the pass's and each file's alike, first; then, for a
/// file waiting in the upload queue, what holds that file (§19.1).
pub enum SyncWait {
    /// Sync isn't running (`start_sync`, `stop_sync`).
    NotSyncing,
    /// The provider asked every request to wait until then (§6.7).
    CoolingDown { until: SystemTime },
    /// A slot can't apply yet; nothing goes out or comes in until it does
    /// (§5.4).
    Blocked,
    /// The app paused uploads, and the file's reference trails
    /// (`set_uploads_paused`, §17.4).
    Paused,
    /// The network is metered, or may be, since the platform stopped
    /// answering after opening (E5); the app moves files only on unmetered
    /// networks (`set_unmetered_only`, §17.9), and the file's reference
    /// trails.
    Metered,
    /// Its last attempt failed, and it retries then, or at the person's
    /// retry (§6.7); `coven_uploads.pending` says why.
    Retrying { at: SystemTime },
    /// Its turn hasn't come: a file's, or the next pass's once a cooldown
    /// that held the last one is over.
    Queued,
}

/// A network a test drives in place of the platform's (E1 `network`, §6.7):
/// clones drive the same network.
#[cfg(feature = "test-utils")]
pub struct TestNetwork { /* private fields */ }

#[cfg(feature = "test-utils")]
impl TestNetwork {
    /// The network as the platform would describe it as the store opens.
    pub fn new(usable: bool, metered: bool) -> Self;
    /// The device moves to another usable network: a network report.
    pub fn join(&self, metered: bool);
    /// The device loses its network: no report.
    pub fn lose(&self);
    /// The same network turns metered or unmetered: no report.
    pub fn set_metered(&self, metered: bool);
}

/// What names a state: the last pass, or the first pending record, in the
/// order they arose, that names it, with the cause `coven_pending` keeps.
pub enum SyncCause {
    Pass(LastPass),
    Pending(PendingRecord),
}

/// How the last pass ended, as `coven_sync_status.last_pass` holds it (E4).
/// Each error is kept as its text, which names every cause below it with
/// secrets and URLs already cut.
pub enum LastPass {
    /// The pass reached its observed end; other subjects may still be pending.
    Synced,
    /// The request preventing the pass received no usable provider response.
    Offline { failure: StorageFailure, error: String },
    /// This slot invokes §5.4's stuck-device rule.
    /// `error` is the text of the error that blocked the slot: the cause
    /// the slot's pending record keeps. A device that opens already blocked
    /// has none until a pass of its own meets the error. Coven logs it when
    /// the blocked slot or its reason changes.
    Blocked { slot: Slot, reason: PendingReason, error: Option<String> },
    /// Storage refused the whole pass, or coven's own operation failed.
    Failed { failure: StatusFailure, error: String },
}

/// What stopped a pass as a whole, as `LastPass` keeps it: a `SyncFailure`
/// without its error objects, whose text it holds beside it.
pub enum StatusFailure {
    UpdateRequired(RequiredUpdate),
    Removed,
    DeviceIdInUse(DeviceIdEvidence),
    Storage(StorageFailure),
    Operation(PendingRecord),
    Other,
}

impl LastPass {
    /// How a pass that failed as a whole ended.
    pub fn failed(failure: impl Into<SyncFailure>) -> Self;
}

/// Why a pass, or the connection attempt that begins one, failed: how it
/// ended (`LastPass`).
pub enum PassFailure {
    Offline { error: Arc<StorageError> },
    /// `error` is the cause the slot's pending record keeps.
    Blocked { slot: Slot, reason: PendingReason, error: Option<String> },
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

/// What the person's retry would do if made now, as `coven_retry_effect`
/// holds it.
pub enum RetryEffect {
    /// A pass runs for the retry, and its end answers it.
    Pass,
    /// A provider cooldown holds every pass until then, by the store's
    /// clock: the retry is answered `CoolingDown` at once, and the pass runs
    /// once the cooldown ends.
    CoolingDown { until: SystemTime },
    /// A failure only new credentials, an update or a reset can clear holds
    /// every pass: the retry is answered `Failed` with it at once. The
    /// sync state shows the failure.
    Failed,
    /// Sync is stopped or the handle closed: the retry is answered
    /// `NotSyncing` at once.
    NotSyncing,
}

impl RetryEffect {
    /// Reads `coven_retry_effect`: `handle.subscribe(RetryEffect::read)`.
    pub fn read(sql: SqlReadContext<'_>) -> Result<RetryEffect, DbError>;
}

/// Holds idle passes at most its interval apart; dropping it ends the request.
pub struct SyncOftenGuard { /* private */ }

/// Storage this device has set up, with whether it holds the store key.
pub struct ConnectedStorage {
    pub storage: StorageConfig,
    pub key_state: StoreKeyState,
}

/// What cancelling storage setup did (§11.6).
pub enum SetupCancellation {
    /// Before its point of no return: setup undoes what it wrote and fails
    /// with `StorageSetupError::Cancelled`.
    Undoing,
    /// Past its point of no return: setup finishes.
    Finishing,
    /// No setup is running.
    NoSetup,
}

/// Setup returns the committed connection and code, including on reconnection.
pub struct StorageSetup {
    pub connection: ConnectedStorage,
    pub restore_code: SecretText,
}

/// A step of setting up storage, in the order they run (E5). A step other
/// than `Loading` is one unit, done when it ends.
pub enum StorageSetupStep {
    /// Creating a store in a location it needs to itself: a Dropbox shared
    /// folder, a OneDrive folder, or an S3 bucket.
    CheckingLocation,
    /// S3: detecting settings and checking them against the service (§20.8).
    DetectingSettings,
    /// iCloud, creating a store: reading the account and saving its zone.
    PreparingZone,
    /// Dropbox or OneDrive: reading the signed-in account's name.
    ReadingAccount,
    /// Connecting and checking the provider.
    Connecting,
    /// Creating: past the point of no return, claiming the location where it
    /// holds one store, then slot 1. Cancel no longer undoes setup.
    CreatingStore,
    /// Reconnecting: loading, with loading's steps and counts (§13.4).
    Loading(LoadStep),
    /// Reconnecting with changed access: past the point of no return,
    /// publishing `Set access`. Cancel no longer undoes setup.
    RecordingAccess,
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

let secret = SecretText::new(secret_access_key);
match handle.setup_s3_storage(storage, device_name, access_key_id, secret, show_setup_step).await {
    Ok(setup) => {
        remember(setup.connection.storage);
        show_blurred_qr_code(setup.restore_code.as_str());
    }
    Err(error) => return show_setup_error(error),
}

// The sync state, as a live query follows it; the first value is the
// current state.
let mut status = handle.subscribe_sync_status();
while let Some(result) = status.next().await {
    let SyncStatus { state, last_synced } = result?;
    show_sync_line(&state, last_synced);
}
```

### E6 Operations and recovery

- Every administrative operation follows [§11](coven.md#11-admin-operations):
  journal, catch up and validate, do prerequisite work, then claim. Contention
  reads forward and validates again. The admin changes an app makes in a write
  (E9, E12) are steps that check against the slots this device has applied and
  journal, with or without storage; a pass runs them. Each returns its
  `OperationId`, or `None` when the applied state already has what it asks.
- `coven_operations` (E4) lists each operation an app's write journaled, from
  that write's commit, and each storage setup, from its journal on (E5): its
  outcome is NULL while it runs, then `Landed`, `Satisfied` or `Rejected`, or
  for a setup `Cancelled` or `Undone`, written in the transaction that ends
  its journal row, so no outcome is lost to a dropped call or a restart. Applying a slot
  that stops this device for good, its removal, its replacement by a reset or
  a replacement of its member's keys it doesn't hold, ends every one still
  running, in that transaction, as `Stopped` with that stop
  ([§14.1](coven.md#141-the-cut-off)). It stays listed
  until a write acknowledges it with `acknowledge_operation`, so an app row
  that names the operation changes in that same write
  ([§11.4](coven.md#114-crashes-and-waiting)).
- On reopening or retrying, settle the recorded path and bytes before another
  attempt ([§5.1](coven.md#51-claiming-a-slot)). Each device has one claim counter
  across writes, admin entries, key copies and announcements, and settles its
  current claim before starting another. If the floor has passed the slot,
  compare its recorded claim number c with the snapshot's last included claim L:
  L below c means absent; equal with the recorded digest means landed; above, or
  equal with another digest, means another copy of this device id;
  the resulting app or admin state is not evidence that a claim landed.
- If validation fails, undo the operation's artifacts and end `Rejected`: a
  call waiting for the operation returns it, and `coven_operations` lists it
  for one an app's write journaled. Failed undo reports both causes and keeps
  the journal for retry.
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
    /// Delete stored files nothing retained refers to (§17.10).
    RetainFiles,
    /// Make a circle and publish its first sealed key.
    CreateCircle,
    /// Seal the circle's history and add a member.
    AddCircleMember,
    /// Publish a circle's deletion; its rows are taken out from that slot
    /// (§15.9).
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
    /// Record the invite's share and grant it, approve or decline a join,
    /// and settle the invite, recording its share's end (§13.2).
    Invite,
    /// Snapshot and reset an audience (§19.4).
    Reset,
    /// Set up storage on this device: check the provider, then create the
    /// store or reconnect to it (§11.6, §20.8).
    StorageSetup,
}

/// Operation calls retain the same typed causes as sync calls (§11).
pub type OperationError = SyncError;

/// How an operation an app's write journaled ended, as
/// `coven_operations.outcome` holds it until a write acknowledges it (§11.4).
pub enum OperationOutcome {
    /// Its entry landed at this slot, which this device has applied.
    Landed(Slot),
    /// The applied state already had what it asked for, so it claimed no slot.
    Satisfied,
    /// The caught-up state refused it; its preparation was undone (§11.2, §11.3).
    Rejected(OperationRejection),
    /// This device stopped for good before it ended, so it never will here.
    /// Nothing refused it: another device may still make the same change,
    /// and for a copy of this installation, the other copy may land it
    /// (§14.1, §19.3).
    Stopped(DeviceStop),
    /// The app cancelled it before its point of no return, and what it had
    /// written was undone (§11.6). Only storage setup ends this way.
    Cancelled,
    /// It was undone before its point of no return, or refused after it
    /// before anything it claimed landed (§11.6). Only storage setup ends
    /// this way.
    Undone(UndoCause),
}

/// Why an operation was undone (§11.6).
pub enum UndoCause {
    /// The app or device stopped before its point of no return, and opening
    /// the store undid it.
    Interrupted,
    /// A step failed, with this error's text; the call that ran it returned
    /// the error itself.
    Failed { error: String },
}

/// Why a device stopped for good (§14.1, §19.3).
pub enum DeviceStop {
    /// The device, or its member, was removed.
    Removed,
    /// A reset registered another installation in this device's place.
    Replaced,
    /// History shows another copy of this installation using its device id.
    Copied,
    /// Its member's keys were replaced with a pair it doesn't hold.
    MemberKeysReplaced,
}

impl CovenWrite<'_> {
    /// Acknowledges a finished operation's outcome, as a step of this write,
    /// so it leaves `coven_operations` (§11.4). An app row naming the
    /// operation changes in the same write. One still running fails with
    /// `OperationRunning`; one not listed with `OperationUnknown`.
    pub fn acknowledge_operation(&self, operation: OperationId) -> Result<(), DbError>;

    /// Makes an operation's retry or undo due now, from its recorded
    /// progress, as a step of this write (§4, §11.4): its pending record and
    /// retry wait end in the write, with or without storage. It fails with
    /// `NotPending` for an operation that has finished or never existed, and
    /// with `Pending` and the record for one whose record never retries, or
    /// only after an update not installed yet. It can't bypass a provider
    /// cooldown. Once the write commits, a pass runs the operation as soon as
    /// storage is connected.
    pub fn retry_pending_operation(&self, operation: OperationId) -> Result<(), OperationError>;
}

impl CovenHandle {
    /// The same retry, in a write of its own.
    pub async fn retry_pending_operation(&self, operation: OperationId) -> Result<(), OperationError>;

    /// Waits for an operation an app's write journaled to end and returns its
    /// outcome, as `coven_operations` lists it; the outcome stays listed until
    /// a write acknowledges it. It waits for storage too: a pass runs the
    /// operation once storage is connected. One not listed fails with
    /// `OperationUnknown`.
    pub async fn operation_outcome(&self, operation: OperationId) -> Result<OperationOutcome, SyncError>;

    /// Reloads this device from the latest snapshot, keeping its waiting
    /// writes, as an operation (§19.2). It cannot bypass §5.4's stuck-device rule.
    /// A reload left unfinished until retention deletes what it selected ends
    /// with `NotFound`; the next call selects afresh (§16.5).
    pub async fn reload_from_snapshot(&self) -> Result<(), OperationError>;

    /// Resets the store from this device's copy, as an admin (§19.4): writes
    /// a snapshot, then announces it and claims the reset slot. Every device,
    /// including this one, reloads from it. Pre-reset writes outside it are
    /// ignored without loss records; queued writes retain their original pasts when claimed.
    pub async fn reset_store(&self) -> Result<(), SyncError>;

    /// Deletes the stored files of devices gone for good (§14.1) that nothing
    /// refers to (§17.10): for every such device this device is responsible
    /// for, ends the uploads it left and deletes the snapshot bodies of its
    /// that no announcement names (§16.6), lists its file folder again, then
    /// deletes each file found that no retained row, write or snapshot
    /// names, once every other device not
    /// gone for good has published a write that read past the listing; later
    /// passes delete the rest as those writes arrive. Safe at any time; a
    /// gone device may keep uploading until its provider access ends. Returns once the listing and
    /// deletions finish, or with the failure that stopped them, which the
    /// RetainFiles operation also records. `SnapshotPending` while this
    /// device's own snapshot is still being published.
    pub async fn clean_up_gone_devices_files(&self) -> Result<(), SyncError>;
}
```

### E7 Moves and uploads

- A row moves to another audience by an ordinary write that changes its
  root's audience column, or points a descendant at a parent in another
  audience ([§15.4](coven.md#154-moving-rows)).
  - The write commits at once on this device. Each moved row gets a new file
    in its new audience for each file it references, over the same stored
    bytes and key, and the write puts the new file's id in the row
    ([§17.2](coven.md#172-files-and-references)). A row naming a file on
    another device only fails the write with `NotThisDevice`.
- Files enter the upload queue as the staging write commits, including with
  no storage connected. That write fixes the path and key, makes the file's
  record on this device only, and the rows reference the file at once.
  Upload completion switches the record to the stored location as a new local
  write, or switches nothing if the record was deleted meanwhile
  ([§17.3](coven.md#173-staging-a-file)). A `FileRef` taken while the file
  uploaded names the same file afterwards. Pinning and caching change no
  file reference.
- A write setting a bound reference waits in the queue, with every later
  write, until its file is stored; the completion then goes out with them in
  one slot ([§17.4](coven.md#174-bound-and-trailing-references)). Bound
  uploads run in a lane of their own, up to the upload limit beside the
  trailing ones, and the pause holds only trailing uploads.
- `set_unmetered_only(true)`, a step of the app's write or a write of its
  own, moves files only on unmetered networks: while the network is metered
  no trailing upload starts, one in flight stops at its next part, keeping
  its session, and pins' downloads wait before their next request to storage
  (E8). Bound uploads, reads and `prepare_device_only` run on. The setting
  persists and is this device's alone, never synced; `coven_network` shows
  it beside the network, and the sync state's `SyncWait::Metered` names it
  ([§17.9](coven.md#179-uploads)).
- Upload attempts open the source again, checking its size, and an
  original's modification time, before transfer, and the first-read hash of
  each plaintext chunk before encryption. A changed user original reports
  `UserFileChanged`; a changed app-provided copy reports `Integrity`. Retries retain the same
  file id, key and chunk hashes, without an encrypted local copy.

```rust
impl CovenHandle {
    /// Queue, pause setting and provider-confirmed session progress in one
    /// committed database snapshot, available while disconnected, with the
    /// files this device is sending now.
    pub async fn uploads(&self) -> CovenResult<UploadQueue>;

    /// Optional early retry of recoverable uploads; coven retries on its own.
    /// It joins the uploads in progress, covers every queued file that can
    /// retry whatever its delay, and returns once each one's attempt ended.
    /// While uploads are paused it tries only bound ones and returns
    /// `Paused`, and while the setting holds trailing uploads on a metered
    /// network it does the same and returns `Metered`. While a
    /// self-replacement pauses writes it does the same and fails with
    /// `MemberKeysChanging` (§12.3, §17.4). Each answer means a trailing
    /// upload it covers was held; covering only bound ones, it returns
    /// `Drained`. What it covers comes from a read of the queue after it
    /// was asked, so a bound upload queued or waiting by then is always
    /// tried, even when asked as a held run ends. While sync is stopped
    /// it tries none and returns `NotSyncing`. Provider cooldowns still hold; failures stay in
    /// pending() (§6.7). Once the upload worker has ended other than with
    /// the store, by a panic or a failure of the handle, no run follows, so
    /// it fails with why, `SyncError::WorkerFailed` for a panic. The run
    /// serving it that fails answers it with that failure, and with
    /// `WorkerFailed` when the failure is a bug, which the sink hears (E15).
    pub async fn retry_uploads_now(&self) -> Result<DrainOutcome, SyncError>;

    /// `CovenWrite::set_uploads_paused`, in a write of its own.
    pub async fn set_uploads_paused(&self, paused: bool) -> CovenResult<()>;

    /// `CovenWrite::set_unmetered_only`, in a write of its own.
    pub async fn set_unmetered_only(&self, unmetered_only: bool) -> CovenResult<()>;

    /// How far each file this process is sending has got, by file: every
    /// one being sent first, then each change as its bytes go, until the
    /// store closes. The files are the ones `coven_sending` lists (E4).
    pub fn subscribe_upload_transfers(&self) -> TransferWatch;
}

impl CovenWrite<'_> {
    /// Pauses trailing uploads, or resumes them, as a step of this write
    /// (§4, §17.9). A paused upload keeps its place, including a provider
    /// upload session in progress, and stops between confirmed parts; the
    /// setting persists. Bound uploads run on, since writes wait for them
    /// (§17.4). A retry asked after the write returns is never answered
    /// from the setting it replaced.
    pub fn set_uploads_paused(&self, paused: bool) -> Result<(), DbError>;

    /// Moves files only on unmetered networks, or on any, as a step of this
    /// write (§4, §17.9). While the network is metered, trailing uploads
    /// start no more and stop between confirmed parts, and pins' downloads
    /// wait before their next request to storage; bound uploads and reads
    /// run on. The setting persists, on this device only. A retry asked
    /// after the write returns is never answered from the setting it
    /// replaced.
    pub fn set_unmetered_only(&self, unmetered_only: bool) -> Result<(), DbError>;
}

/// How far one file's transfer has got: the bytes of its encrypted object
/// handed to the connection or received, in order from its start, out of
/// the object's whole size. Known from the transfer's start; never stored.
/// A part counts from where it starts in the object; a resumed session
/// starts where the provider confirmed it; a download counts the chunks it
/// already had as it copies them in.
pub struct TransferProgress {
    /// The object's bytes moved so far, from its start.
    pub bytes_transferred: u64,
    /// The object's whole encrypted size.
    pub bytes_total: u64,
}

/// Hears how far each file in flight has got. Changes that arrive faster
/// than the reader reads are heard together: the next read names each file
/// that differs from what the reader last heard.
pub struct TransferWatch { /* private */ }

impl TransferWatch {
    /// Every file in flight as one `Reset` on the first read; after that,
    /// wait for a file's progress to change and name each file it changed
    /// for. `None` once the store has closed.
    pub async fn next(&mut self) -> Option<Vec<TransferChange>>;
}

/// A change to one file's progress, or every file's.
pub enum TransferChange {
    /// Every file in flight, as a reader first hears them; a file not
    /// listed has nothing in flight.
    Reset(BTreeMap<FileId, TransferProgress>),
    /// The file started, or its progress moved.
    Updated { file: FileId, progress: TransferProgress },
    /// The file's transfer ended, whether it finished or not.
    Removed { file: FileId },
}

pub struct UploadQueue {
    pub paused: bool,
    /// Oldest first.
    pub files: Vec<QueuedUpload>,
}

pub struct QueuedUpload {
    /// The file its upload switches to the cloud.
    pub file: FileId,
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
    /// or for its retry delay, or stored, waiting for the rest of its move
    /// (§17.3).
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
    /// Every file it covered was attempted; this many were stored. An upload
    /// whose move failed or was cancelled while it was sent stores no file
    /// and doesn't count (§17.3).
    Drained { uploaded: usize },
    QueueEmpty,
    /// Uploads were paused: no trailing upload was tried, and each bound
    /// one it covered was (§17.4).
    Paused,
    /// The network is metered and the app moves files only on unmetered
    /// networks: no trailing upload was tried, and each bound one it covered
    /// was (§17.9).
    Metered,
    /// Sync is stopped: no upload was tried (§17.4).
    NotSyncing,
}
```

Example:

```rust
// The outbox as a live query over coven's views (E4): each queued photo,
// whether it is being sent, and the bytes the provider confirmed.
let mut outbox: LiveQuery<_, CovenError> = handle.subscribe(|sql| {
    Ok(sql.query(
        "SELECT p.title, u.sending, u.bytes_confirmed, u.bytes_total
         FROM coven_uploads AS u JOIN photos AS p ON u.file=p.file
         ORDER BY u.position",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?,
                  row.get::<_, Option<i64>>(2)?, row.get::<_, Option<i64>>(3)?)),
    )?)
});
while let Some(rows) = outbox.next().await {
    let rows = rows?;
    show_outbox(&rows);
    if rows.is_empty() {
        break; // every queued photo is stored
    }
}
```

```rust
// Each photo's bar as its bytes go, beside the outbox: the watch keeps
// progress by file, the id the photo's row holds.
let mut bars = BTreeMap::new();
let mut watch = handle.subscribe_upload_transfers();
while let Some(changes) = watch.next().await {
    for change in changes {
        match change {
            TransferChange::Reset(files) => bars = files,
            TransferChange::Updated { file, progress } => { bars.insert(file, progress); }
            TransferChange::Removed { file } => { bars.remove(&file); }
        }
    }
    draw_bars(&bars);
}
```

### E8 Files and the cache

- A *file reference*, `FileRef`, is a file's record as this device read it,
  taken by the file's id, the id a row's file reference column holds
  ([§17.2](coven.md#172-files-and-references)). A file never changes its
  bytes, so a reference taken while the file uploaded reads it from storage
  once it is stored; reads, status and pins use the record as it now stands.
- Coven reads a file from wherever it is: the user's original, coven's own
  copy, the cache, or storage ([§17](coven.md#17-files)). A file on another
  device only can't be read here: reads, downloads and pins fail with
  `FileReadError::DeviceOnly`, and its status is `DeviceOnly`. A file a bound
  reference names reaches other devices already stored, with the row naming
  it, so only its uploader sees it on one device only
  ([§17.4](coven.md#174-bound-and-trailing-references)).
- A device caches a file when it reads it; nothing downloads a file because
  its record arrived. Pinning keeps a stored file whole on the device
  ([§17.8](coven.md#178-cache-and-pinning)): `prepare_pin` downloads, and a
  write's `pin` step keeps what it prepared. While a prepare downloads a file,
  `coven_downloading` lists it (E4).
- `prepare_device_only` brings cloud files back to this device, into a folder
  the person picks, reporting each file's bytes and the call's as they
  download, and a write's `make_device_only` step switches them all to
  device-only here ([§17.3](coven.md#173-staging-a-file)). A failure or a
  cancel before that write removes everything the call made.

Ana pins an album and one song cannot be downloaded. The song's reason is in
`pending()`; pin progress still counts the files and bytes kept. There is no
competing failure state in that progress. While the album's other songs
download, `coven_downloading` names the ones in flight, and the download watch
how far each has got: a song's bytes count only once it is kept whole, so pin
progress alone would sit still until then.

```rust
/// Plaintext bytes in a full file, write-part or snapshot chunk: 64 KiB.
/// Re-exported from coven-format so app buffers use the same chunk size.
pub const CHUNK_SIZE: usize = 64 * 1024;

/// Stored files ready to be kept whole in one write (§17.8, E8): each
/// file's whole copy, written and checked under a recorded name, or a
/// checked whole copy already kept. It holds nothing close waits for:
/// committed by no write, its copies are removed by the next write or open.
pub struct PreparedPin { /* private fields */ }

impl CovenWrite<'_> {
    /// Keeps a prepared pin's files whole on this device regardless of the
    /// cache budget, as a step of this write (§4, §17.8): each whole copy
    /// becomes the file's pinned cache entry, and each copy already kept is
    /// pinned. A pin prepared in another open store, or in this one before
    /// it closed, fails with `PreparedElsewhere`; a file whose record left
    /// its stored bytes since with `NoLongerStored`, keeping nothing; and a
    /// copy already kept that the budget evicted since with `PinChanged`,
    /// which preparing again downloads.
    pub fn pin(&self, prepared: PreparedPin) -> Result<(), DbError>;

    /// Stops keeping files, as a step of this write (§4, §17.8): each file's
    /// pin ends and its namespace is evicted down to its budget in the
    /// write, and the evicted bytes go once it has committed. A file on one
    /// device only, or one this device has no record of, has no pin to
    /// release. A file kept here whose pin coven has not yet made has its
    /// pin request ended too: `coven_pin_requests` loses it in this write,
    /// and coven never makes that pin.
    pub fn unpin(&self, files: &[FileId]) -> Result<(), DbError>;

    /// Removes a stored file's copies from the cache, pinned or not, as a
    /// step of this write (§4, §17.8); the bytes go once it has committed.
    /// Never touches a file waiting to upload, or storage; a later read
    /// downloads it again. A file on one device only, or one this device
    /// has no record of, has none.
    pub fn evict_file(&self, file: FileId) -> Result<(), DbError>;

    /// Sets a declared namespace's cache budget, in bytes, as a step of this
    /// write (§4, §17.8), and evicts its unpinned files and chunks, least
    /// recently read first, down to it; the evicted bytes go once the write
    /// has committed. An undeclared namespace fails with
    /// `UndeclaredNamespace`.
    pub fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError>;
}

/// Progress while keeping files whole on this device (§17.8, E8).
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
    /// The file a row references, by the id the row holds: its size,
    /// namespace, audience and place, read in one committed state. An id this
    /// device has no record of returns `DbError::FileAbsent`; a malformed
    /// record returns `DbError::DamagedDatabase`.
    pub async fn file_ref(&self, file: FileId) -> Result<FileRef, DbError>;

    /// The file's status as its record now stands (§17.6): one this device
    /// uploads is judged locally, and one on a device only with no upload,
    /// this one or another, from its record, both with no storage request; a
    /// stored one by reading its fixed path's metadata. Network failures and denied access are errors,
    /// not Missing; an object at the fixed path of any size but the file's
    /// complete one is `Integrity`.
    pub async fn file_status(&self, file: &FileRef) -> Result<FileStatus, FileReadError>;

    /// Reads a whole file. A stored file is checked against its content hash.
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>, FileReadError>;

    /// Opens a file for reading ranges (§17.7), as its record stands when it
    /// opens; keep the stream for as long as the file is read. A file on this
    /// device only reads from its bytes here, uploading or not; a file only on
    /// another device fails with `FileReadError::DeviceOnly`. Its shared store
    /// lock prevents deletion until it and its I/O finish.
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError>;

    /// The path, size and modification time coven recorded for a file's
    /// user original on this device, or `None` when it has none.
    pub async fn user_file(&self, file: FileId) -> Result<Option<UserFile>, DbError>;

    /// Downloads cloud files to the paths named under `folder`, for a write
    /// to switch them all to device-only on this device with
    /// `CovenWrite::make_device_only`, each copy becoming the original this
    /// device records (§17.3). Each download checks the content hash, and
    /// each copy is written under a temporary name in its final folder and
    /// renamed once synced. The member must be in each file's audience and
    /// may change every row referencing it (`NotInCircle` for a member who
    /// left the file's circle, `NotOwner`); a file of a circle the member was
    /// never in has no record here (`FileAbsent`). Only cloud files of user
    /// namespaces qualify (`NotInCloud`, `NamespaceKind`). Each name is
    /// written as one rule, the same on every platform, makes it valid on
    /// any volume. A path through a symbolic link below the folder, where
    /// something is already, or naming what another path of the call names
    /// once written, case and Unicode form aside, is refused before anything
    /// downloads. `on_progress` is called on this task with each file's
    /// download, at most a few times a second (`DeviceOnlyStep`). Until it
    /// returns, `cancel` stops the call; a failure, a panic in a download
    /// among them, or a cancel removes every copy and folder it made before
    /// it returns. A dropped call's copies go with the store's next write.
    pub async fn prepare_device_only(
        &self,
        folder: &Path,
        files: Vec<(FileId, RelativePath)>,
        cancel: &watch::Receiver<bool>,
        on_progress: impl Fn(Progress<DeviceOnlyStep>),
    ) -> Result<PreparedDeviceOnly, DeviceOnlyError>;

    /// `prepare_device_only`, then `CovenWrite::make_device_only` in a write
    /// of its own. A write that fails removes the copies.
    pub async fn make_device_only(
        &self,
        folder: &Path,
        files: Vec<(FileId, RelativePath)>,
        cancel: &watch::Receiver<bool>,
        on_progress: impl Fn(Progress<DeviceOnlyStep>),
    ) -> Result<(), DeviceOnlyError>;

    /// Prepares keeping stored files whole on this device regardless of the
    /// cache budget (§17.8): downloads what each lacks, up to the download
    /// limit at once, into a whole copy checked against its content hash,
    /// or notes a checked whole copy already kept, and pins nothing. A write
    /// keeps them with `CovenWrite::pin`. A file only on one device, this
    /// one included while it uploads, is refused with
    /// `FileReadError::DeviceOnly`, and one whose record goes, or leaves its
    /// stored bytes, while it downloads keeps nothing. A failed download is
    /// the stored bytes' pending record; one that succeeds clears it. The
    /// bytes of each download in flight are on
    /// subscribe_download_transfers(). A file staged or moved to be kept
    /// here needs no call: the write switching it to the cloud pins it, and
    /// coven makes that pin itself. While the network is metered and the
    /// app moves files only on unmetered networks (E7), it waits before each
    /// request to storage until that ends; dropping it cancels.
    pub async fn prepare_pin(&self, files: &[FileRef]) -> Result<PreparedPin, FileReadError>;

    /// `prepare_pin`, then `CovenWrite::pin` in a write of its own.
    pub async fn pin(&self, files: &[FileRef]) -> Result<(), FileReadError>;

    /// Verified cache coverage for these fixed references, in one snapshot.
    pub async fn pin_progress(&self, files: Vec<FileRef>) -> CovenResult<PinProgress>;

    /// The same pre-built cache query with the same fixed references.
    pub fn subscribe_pin_progress(&self, files: Vec<FileRef>) -> LiveQuery<PinProgress>;

    /// How far each file this process is downloading to keep whole has got,
    /// by file, for a pin: every one being downloaded first,
    /// then each change as its bytes arrive, until the store closes. The
    /// files are the ones `coven_downloading` lists (E4); the watch is the
    /// one E7 describes.
    pub fn subscribe_download_transfers(&self) -> TransferWatch;

    /// `CovenWrite::unpin`, in a write of its own.
    pub async fn unpin(&self, files: &[FileId]) -> CovenResult<()>;

    /// Whether each file is pinned, one answer per file in order, or `None`
    /// for a file this device has no record of. The answer is whether
    /// `coven_pins` lists it (E4).
    pub async fn files_pinned(&self, files: &[FileId]) -> CovenResult<Vec<Option<bool>>>;

    /// The same internal query, live. To watch different files, drop this
    /// query and subscribe with the new files; there is no separate
    /// pin-query type.
    pub fn subscribe_files_pinned(&self, files: &[FileId]) -> LiveQuery<Vec<Option<bool>>>;

    /// `CovenWrite::evict_file`, in a write of its own.
    pub async fn evict_file(&self, file: FileId) -> CovenResult<()>;

    /// `CovenWrite::set_cache_budget`, in a write of its own.
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> CovenResult<()>;
    /// A declared namespace's cache budget, in bytes; each evicts on its own.
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<u64, DbError>;
}

impl FileRef {
    /// The file's id, which rows hold.
    pub fn id(&self) -> FileId;
    /// The file's size in bytes.
    pub fn size(&self) -> u64;
    /// Its namespace, which fixes its kind.
    pub fn namespace(&self) -> &str;
    /// The audience its record is in, whose encrypted writes carry its key.
    pub fn audience(&self) -> Audience;
    /// Where it is: stored, or on one device only.
    pub fn place(&self) -> FilePlace<'_>;
}

/// Where a file is (§17.2): in storage, or only on one device.
pub enum FilePlace<'a> {
    /// Stored at its location, with its content hash.
    Cloud { location: &'a FileLocation, uploader: DeviceId, content_hash: &'a ContentHash },
    /// Only on one device, which may be this one: while a file uploads, it
    /// is on its uploader only.
    DeviceOnly { device: DeviceId },
}

/// A path below a folder the person picked, as the names the app gives,
/// outermost first: an album's name, then a track's. Each name is one name
/// on every platform, whatever it holds, `/` and `\` included. Coven writes
/// each through one rule that makes it valid on any volume (§17.3), so
/// joined to the folder the path stays inside it. Shown, its names are
/// joined by `/`.
pub struct RelativePath(/* private */);

impl RelativePath {
    pub fn new<S: Into<String>>(names: impl IntoIterator<Item = S>) -> Result<Self, RelativePathError>;
    /// Its names as the app gave them, outermost first.
    pub fn names(&self) -> impl Iterator<Item = &str>;
    /// The path coven writes for it below a folder: each name made valid by
    /// the one rule (§17.3), joined by the platform's separator, for a file
    /// the app writes itself under a name it stores.
    pub fn local_path(&self) -> PathBuf;
}

pub enum RelativePathError {
    /// The path has no names, or a name is empty.
    Empty,
}

/// Why a copy can't go where the app named it (§17.3).
pub enum CopyPathError {
    /// The folder isn't an absolute path to a folder, or a path below it
    /// passes through something that isn't a folder.
    NotAFolder(PathBuf),
    /// A path below the folder passes through a symbolic link.
    Link(PathBuf),
    /// Something is at a file's path already.
    Exists(PathBuf),
    /// Two paths of the call are one path once each name is made valid
    /// (§17.3), or differ only in case or Unicode form, which the default
    /// macOS and Windows volumes hold as one name; refused on every volume.
    /// Each is the path as the app named it, up to the name that clashes.
    Clash { path: RelativePath, other: RelativePath },
    /// The folder's path isn't valid Unicode.
    NotUnicode(PathBuf),
    /// Inspecting a path failed.
    File(FileError),
    /// Inspecting the paths panicked on the blocking pool: a bug (E15).
    Bug(Bug),
}

/// Cloud files downloaded to copies in the person's folder, ready for one
/// write to make them device-only here (§17.3, E8). It holds nothing close
/// waits for. Committed by no write, its copies go with the store's next
/// write or open once it drops.
pub struct PreparedDeviceOnly { /* private fields */ }

impl PreparedDeviceOnly {
    /// Removes the copies, and the folders made for them, now. A failed
    /// removal keeps its record, and the next open removes it.
    pub async fn abandon(self) -> Result<(), DbError>;
}

/// A step of preparing files to be made device-only (§17.3), which a host
/// draws a bar for: one step for each file downloaded, in order. Done and
/// total count the file's bytes; done plus `before`, out of `all`, is how
/// far the whole call has come.
pub enum DeviceOnlyStep {
    /// Downloading one file to its copy.
    Downloading { file: FileId, before: u64, all: u64 },
}

impl CovenWrite<'_> {
    /// Makes a prepared call's files device-only on this device, each
    /// switched to its copy, which becomes the original this device records,
    /// as a step of this write (§4, §17.3). Each must still be one this
    /// device may make device-only, and still stored where its copy came
    /// from (`NotInCloud`); copies prepared in another open store, or in
    /// this one before it closed, fail with `PreparedElsewhere`. A write
    /// that also removes such a file's last reference fails with
    /// `DeviceOnlyUnreferenced`. A write that fails removes the copies.
    pub fn make_device_only(&self, prepared: PreparedDeviceOnly) -> Result<(), DbError>;
}

/// Why no file of a call was made device-only (§17.3). Whatever the call
/// made is removed before it returns, or, if that failed too, at the next
/// open.
pub enum DeviceOnlyError {
    /// The call names one file twice.
    RepeatedFile { file: FileId },
    /// A copy can't go where the app named it.
    Path(CopyPathError),
    /// This device may not make a file device-only, or the write failed:
    /// `FileAbsent`, `NotInCloud`, `NamespaceKind`, `NotInCircle`,
    /// `NotOwner`, `CopyPathTaken`, or the database failing.
    Database(DbError),
    /// Downloading a file failed.
    Download { file: FileId, error: FileReadError },
    /// Writing a folder or a copy failed.
    Disk(FileError),
    /// The app cancelled the call before its files switched.
    Cancelled,
    /// Removing what the call made failed too; the next open removes it.
    Cleanup { error: Box<DeviceOnlyError>, cleanup: DbError },
    /// The call's work panicked: a bug (E15).
    Bug(Bug),
}

/// A stored file's fixed location: the uploader and the stored object's id,
/// with the file's key, which diagnostics never show.
pub struct FileLocation { /* private fields */ }

impl FileLocation {
    /// The device that uploaded the bytes, which owns deleting them.
    pub fn uploader(&self) -> DeviceId;
    /// The stored object's id, the last part of its path.
    pub fn object(&self) -> ObjectId;
}

/// Current availability; never persisted in the file's record (§17.6).
pub enum FileStatus {
    /// Storage holds the complete file at its fixed path.
    Available,
    /// This device is uploading it; until then it is on this device only.
    Uploading,
    /// It is on one device only, with no upload: another device, which may
    /// be gone, and then nothing here can read it, or this one, which reads
    /// it from its bytes here. Nothing waits for it.
    DeviceOnly { device: DeviceId },
    /// Unreadable from storage, with the reason.
    Missing { device: DeviceId, reason: FileMissingReason },
}

pub enum FileMissingReason {
    /// Storage confirms nothing at the path a file's record names: the
    /// provider lost it. Nothing brings it back on its own; the app can stage
    /// it again.
    Lost,
    /// A file this device uploads, on it only until then, whose source
    /// failed: its original is missing, or coven's copy fails its check; its
    /// upload retries (§17.3, §17.9). A changed original fails its move.
    Source(FileSourceFailure),
}

impl<M: FilesMode> FileStream<M> {
    /// The file's whole size in bytes.
    pub fn plaintext_size(&self) -> u64;

    /// Reads `len` bytes at `offset`. A range past the end is an error, never
    /// a short read.
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, FileReadError>;

    /// `read_at`, telling `progress` how many of the range's bytes are here
    /// each time that grows while the read fetches from storage. A read the
    /// cache or this device's copy serves alone tells it nothing (§17.7).
    pub async fn read_at_reporting(
        &self,
        offset: u64,
        len: u64,
        progress: &ReadProgress,
    ) -> Result<Vec<u8>, FileReadError>;

    /// The same range as bounded plaintext buffers, without allocating its
    /// whole length. Authentication precedes each yielded buffer; a complete
    /// sequential read of a stored file checks the record's content hash
    /// before the final buffer. A file this device uploads is read as it is
    /// on disk (§17.7).
    pub fn read_range(
        &self,
        offset: u64,
        len: u64,
    ) -> Result<FileRangeStream<'_, M>, FileReadError>;
}

/// Where a read tells how many of its range's bytes are here.
pub struct ReadProgress { /* private */ }

impl ReadProgress {
    /// Tell `report` each new count of the range's bytes in hand.
    pub fn new(report: impl Fn(u64) + Send + Sync + 'static) -> Self;
}

/// A borrowed stream of bounded authenticated plaintext buffers.
pub struct FileRangeStream<'a, M: FilesMode = Writer> { /* private */ }

impl<M: FilesMode> FileRangeStream<'_, M> {
    pub async fn next(&mut self) -> Result<Option<Vec<u8>>, FileReadError>;
}

pub enum FileReadError {
    /// This file uses a newer coven format (Appendix C.2).
    UpdateRequired(RequiredUpdate),
    /// Remote status or uncached bytes were requested with no storage connected.
    NoStorage,
    /// Storage confirms nothing at the path the file's record names: the
    /// provider lost it (§17.6). Its pending reason is `FileUnavailable(Lost)`.
    Missing { id: String, device: DeviceId },
    /// The file is only on another device, or on this one with no source to
    /// read: nothing here can read it, and nothing waits for it (§17.2).
    DeviceOnly { file: FileId, device: DeviceId },
    /// A user-provided file is gone from its recorded path.
    UserFileMissing { id: String, path: PathBuf },
    /// A user-provided file's size, modification time or checked content no
    /// longer matches what coven recorded.
    UserFileChanged { id: String, path: PathBuf },
    /// A chunk, a copy on this device, or the object at the fixed path failed
    /// its check; a stored object of the wrong size is damage, not absence (§17.6).
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
    Disk(FileError),
    /// The store cannot be retained while opening its file.
    Lock(StoreLockError),
    /// Recording what a read or an upload settled failed in sync's own work,
    /// as a retry wait the clock can't hold, keeping its cause; a failed read
    /// whose failure couldn't be recorded keeps both (§17.9).
    Sync(Box<SyncError>),
    /// The read's work panicked: a bug (E15). A record of it fails as
    /// `LocalFailure::Bug`.
    Bug(Bug),
}
```

Making an album local again, into the folder the person picked:

```rust
let (_stop, cancel) = watch::channel(false);
let files = tracks
    .iter()
    .map(|track| Ok((track.file, RelativePath::new([album.as_str(), track.name.as_str()])?)))
    .collect::<Result<Vec<_>, RelativePathError>>()?;
let prepared = match handle
    .prepare_device_only(&music_folder, files, &cancel, |progress| show_bar(&album, progress))
    .await
{
    Ok(prepared) => prepared,
    Err(DeviceOnlyError::Path(CopyPathError::Exists(path))) => return ask_to_move(&path),
    Err(error) => return Err(error.into()),
};
let local = album.id.clone();
handle
    .write(move |sql| {
        sql.make_device_only(prepared)?;
        sql.execute("UPDATE albums SET local_here = 1 WHERE id = ?1", [&local])?;
        Ok::<_, CovenError>(())
    })
    .await?;
```

Example:

```rust
let recording = handle.file_ref(file).await?;
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
- Administrative changes are steps of an app's write, checked against the
  slots this device has applied and journaled offline too; a pass catches up,
  validates them again and claims their slots (§11.1, E6). Each handle call
  below is the same step in a write of its own.
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
- Removing a member publishes one entry. Applying entries is the single path
  that records provider access work.
- The pending list describes unresolved access work, on the owner's devices
  only: no other device records it, since it could neither do the work nor
  see it done. Each owner's device serializes grant and revoke requests, and performs the opposite
  request when a later slot changes the required access (§20.9, §14.2). Pending and failed work
  appears in `pending()`, with the operation id for retry. Derived provider work cannot be discarded
  while its current intention remains unmet: disappearance from this list must not hide a retained
  grant. A later slot can replace or remove an account-sharing intention.
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

    /// Renames the store, in a write of its own (`CovenWrite::rename_store`).
    pub async fn rename_store(&self, name: &str) -> Result<Option<OperationId>, SyncError>;

    /// The members and their devices at this device's applied slot, in
    /// member-id order, each member's devices in device-id order.
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

    /// Publishes a prepared public pair for another member, in a write of its
    /// own (`CovenWrite::replace_member_keys_for`).
    pub async fn replace_member_keys_for(
        &self,
        member: &MemberId,
        keys: MemberPublicKeys,
    ) -> Result<Option<OperationId>, SyncError>;

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
    /// the key would return to the saved entries the removal emptied (§14.1).
    pub async fn update_credentials(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError>;

    /// Changes a member's role, in a write of its own (`CovenWrite::set_member_role`).
    pub async fn set_member_role(
        &self,
        member: &MemberId,
        role: MemberRole,
    ) -> Result<Option<OperationId>, SyncError>;

    /// The store's bucket and the filled member policy for making a key in
    /// the provider's console (§20.10).
    pub async fn guided_access_key(&self) -> Result<GuidedAccessKey, SyncError>;

    /// Removes a member, in a write of its own (`CovenWrite::remove_member`).
    pub async fn remove_member(&self, member: &MemberId) -> Result<Option<OperationId>, SyncError>;

    /// Removes a device, in a write of its own (`CovenWrite::remove_device`).
    pub async fn remove_device(
        &self,
        device: DeviceId,
    ) -> Result<(Option<OperationId>, Option<ProviderSignOut>), SyncError>;
}

impl CovenWrite<'_> {
    /// Renames the store, as an admin (§10). `None` if it already has the name.
    pub fn rename_store(&self, name: &str) -> Result<Option<OperationId>, SyncError>;

    /// Changes a member's role, as an admin (§10).
    pub fn set_member_role(
        &self,
        member: &MemberId,
        role: MemberRole,
    ) -> Result<Option<OperationId>, SyncError>;

    /// Removes this member and all their devices (§14.1). Provider work
    /// appears only in `coven_pending` on the devices of the member whose
    /// account holds the store, including retained grants. Coven forgets the member's S3 keys; the person deletes them in
    /// the provider's console (§14.3). An admin removing themselves leaves:
    /// this device deletes their saved restore code and S3 key before
    /// applying the slot, then stops, and the operation ends landed. On
    /// Android that is this phone's Block Store only (§13.1). A failed
    /// deletion fails the pass, which sync tries again.
    pub fn remove_member(&self, member: &MemberId) -> Result<Option<OperationId>, SyncError>;

    /// Removes a device: their own, or any as an admin. The provider can't
    /// cut off one device alone, so it also says how the device's member
    /// signs out of the provider and signs in again on the devices they keep,
    /// from the recorded location (§14.2). None on S3: the member's devices
    /// share one key, so nothing at the provider changes. With no recorded
    /// location it fails with `NoStorage`.
    pub fn remove_device(
        &self,
        device: DeviceId,
    ) -> Result<(Option<OperationId>, Option<ProviderSignOut>), SyncError>;

    /// Publishes a prepared public pair for another active member under
    /// §10's authority rules, expecting the pair this device's applied state
    /// binds to them. The target retains its private pair; this step
    /// receives no private keys and changes no local private custody. Its
    /// operation confirms copies of every store key and the held circle keys
    /// before publishing the replacement. Any admin may replace another
    /// member, independently of provider ownership. A self target fails with
    /// `Rejected(NotAllowed)`, even for an admin: that is
    /// `replace_member_keys`. Invalidation undoes the operation's artifacts.
    /// Provider access and installing the prepared private pair are separate
    /// work.
    pub fn replace_member_keys_for(
        &self,
        member: &MemberId,
        keys: MemberPublicKeys,
    ) -> Result<Option<OperationId>, SyncError>;
}

/// Guided console steps for a key made by hand (§20.10).
pub struct GuidedAccessKey {
    pub bucket: String,
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
  as a QR code, typed in, or read from iCloud Keychain on Apple or Block Store
  on Android ([§13.1](coven.md#131-a-persons-new-device-the-restore-code)).
- The restore code contains only the stable member id and current private
  pair, store id and storage location. On S3, the person keeps their access key
  separately, such as in a password manager, and supplies it on the new device,
  unless the code their own device showed holds it. Dropbox and OneDrive
  likewise require the person's provider sign-in.
- A restore code carries no name. Before connecting, a device restoring from
  a typed or scanned code may show the id, provider and account or location,
  never a store name; it learns the name only after loading. An invite carries
  the current name copied when it was made, the new person's name, and whether it
  was shown in person or sent; it expires at the time fixed when it was made
  (§13.2).
  `decode_code_info` exposes the same location as a saved entry, so a
  typed or scanned code can show which account it is for before connecting.
- Coven's saved entry, in iCloud Keychain or Block Store, saves the code with
  the store's name: the last
  name one of the person's devices saw, kept with the slot that set it. Setup,
  joining, restoring, resetting and member-key replacement save it; a device
  rewrites only the name before applying a rename, and when opening finds its
  own name newer than the entry's. An older name never replaces a newer one, a
  rewrite keeps the entry's code, and an absent entry stays absent. A failed
  rewrite fails the sync pass or the opening call
  ([§13.1](coven.md#131-a-persons-new-device-the-restore-code)). On Android,
  opening also writes the phone's saved entries again, unchanged, so Block
  Store's choice of a cloud copy follows the phone's current screen lock; a
  failed write fails the opening call.
- When that rewrite finds the entry unreadable (another format, damaged,
  another store's code, or copies in more than one keychain group), the device
  replaces it with one copy holding its own current code and the store's
  current name. Opening and applying the rename then succeed. A device that
  holds no current code (keys replaced, no storage location, or the member
  removed) leaves the entry as it is. A refused keychain read still fails the
  call.
- A QR scan from one of the person's own devices may supply the S3 key inside
  the code, because the person controls the showing device. The code to write
  down, the one `restore_code()` shows, and the saved copy never hold one; the
  S3 key may be saved in its own entry beside the code.
- `restore_code_for_transfer()` gives the showing device's app the restore code
  with the S3 key this device holds now, read from the same custody its
  connection uses. The app shows it only as a QR code, to the person's own new
  device, behind the same tap that reveals the code. After a key replacement it
  holds the new key. On other providers, or with no key, it is `restore_code()`'s.
- `decode_code_info` reports `carries_s3_key`, so the new device asks for a key
  only when the code holds none. Restoring with a code that holds a key and a
  key supplied beside it fails with `InvalidConfiguration` before bootstrap.
- `Coven::saved_restore_codes` lists the saved codes of the installation's
  keychain service so the
  app can offer a choice before opening a store. A readable entry exposes the
  store's saved name and id, provider location, including the member's account
  label, and separate S3-key presence, with no secrets.
  The app passes its selection to `restore_from_saved_code`; no local store or
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
- Both saved entries belong to the person. Deleting the store from one device
  keeps them; a device deletes them when it applies the person's removal from
  the store, before the slot applies (§14.1).
- `Coven::forget_saved_restore_code(store)` deletes one store's saved restore code
  and S3 key, removing them from all the person's Apple devices, or on Android
  from this phone, leaving other phones' own. It changes no
  store, storage or device's copy, needs no open store or provider connection,
  and succeeds when the entries are already absent. It deletes an unreadable
  entry the same way, by its store id, in every keychain group that holds one.
- A restore records itself on the device before it registers the device:
  the code it restores with, holding the S3 key on S3, and the device's
  name, in a device-only keychain entry, then on Dropbox and OneDrive its
  sign-in, in the store's device credentials, where it refreshes from then
  on. From the record on, the restore only goes forward
  ([§13.1](coven.md#131-a-persons-new-device-the-restore-code)).
  `CovenBuilder::unfinished_restores` lists the restores under way, and
  `resume_restore` finishes one with neither the code nor a new sign-in.
  Cancelling once the record is kept waits for the registration to end,
  then fails with `BootstrapError::RestoreRegistered`, the restore still
  listed. A cancel, a registration refused, the member or device removed,
  or the member's keys replaced, while the history the installation applied
  names the device as gone for good, or doesn't name it and no registration
  of it is journaled, ends
  the restore instead: it deletes the kept sign-in, the record and the
  installation, in that order.
- A device that still holds the store under member keys another device
  replaced restores in place with `CovenBuilder::restore_device`
  ([§13.1](coven.md#131-a-persons-new-device-the-restore-code)). The code must
  name that store, or the call fails with `CodeError::WrongStore`. The new
  device loads beside the old copy, which stays openable until the restore
  records itself, just before the new device registers; a failure before
  the record leaves the old copy as it was. From the record on it only goes
  forward: a failure leaves it for the next call, a cancel fails with
  `BootstrapError::RestoreRegistered`, and retrying the same call, or
  `CovenBuilder::resume_replacement` with nothing from the app, finishes it.
  Until then, opening fails with
  `StoreLockError::ReplacementUnfinished { by: Restore }`, `Coven::stores`
  lists the store in that state, and `reset_device` is refused with that error,
  changing nothing ([§19.3](coven.md#193-another-copy-of-the-same-device)).
  A store this device is deleting refuses the restore with
  `StoreLockError::DeletionUnfinished`, changing nothing.
  Only the facts end it before the swap: its registration refused, or the
  new device gone for good, removes the record, the kept entry and the new
  installation, and the old copy is the store again.
  `Coven::discard_store` can delete the store from the device instead, both
  copies with it. The person's restore code and S3 key stay.
- An admin adds a person with an invite
  ([§13.2](coven.md#132-adding-a-person-invites)). An in-person invite's request
  is approved by the inviting device as soon as it is read and checked. A sent
  invite's request waits for the admin's decision, with its confirmation number.
- Making an invite is a step of the app's write (`CovenWrite::create_invite`),
  with or without storage: it returns the invite's id, the member id it fixes
  and its operation, and the invite is listed from the write's commit as
  `InviteStage::Granting`. A pass checks it again against the history it
  catches up with, grants its access and fixes its code. A check that
  refuses it, such as the admin's role taken away meanwhile, and a failed
  grant are the invite's pending record under its operation in
  `coven_pending`, not an error of any call; the app shows it beside the
  listed invite and cancels the invite to end it.
- Only the inviting device lists its open invites, in `coven_invites` (E4),
  each with its stage: granting, waiting with its code to show again and the
  request once one has arrived, or approving. The code it lists is the one
  first shown. Other devices' invites are never listed: only the inviting
  device can act on one. It looks for the request on the shared backoff;
  `retry_now` ends that wait (E5). Applying a slot that stops the device for
  good ends every invite it made, in that transaction
  ([§14.1](coven.md#141-the-cut-off)).
- A request that fails the inviting device's checks, its signature, the
  member id it names or the freshness of its keys, ends its invite in the
  pass that read it, as a decline does: the request never changes and holds
  the invite's one path, so no retry could pass it. The invite leaves
  `coven_invites` with no pending record, and the requester's join ends as
  declined ([§13.2](coven.md#132-adding-a-person-invites)).
- History refusing the binding of an in-person invite's request, which the
  inviting device approved on its own, ends the invite as a decline does, in
  the pass that ran the binding. The device logs the refusal, and the pass
  goes on: no call made that approval, and a pass failed with it would leave
  the connection a record that never retries, holding every later pass
  ([§13.2](coven.md#132-adding-a-person-invites)).
- An invite ends in a write: approval, decline and cancel each write it, and
  so does expiry. Approving, declining and cancelling are steps of the app's
  write (`CovenWrite::approve_join_request`, `CovenWrite::decline_join_request`,
  `CovenWrite::cancel_invite`), with or without storage, so an app row about
  the decision commits with it. The
  handle wakes at the earliest open invite's expiry, by the inviting
  device's clock, and writes it, with storage disconnected, offline or with
  sync stopped too, so the invite leaves `coven_invites` without a pass;
  opening writes each expiry already past before the handle exists. Deleting
  the ended invite's request and taking back its access is then its own
  work, which a pass does once storage is connected. A failed expiry write
  fails the handle with its cause, which every call and live query reports.
- The joining device keeps the invite code in its own keychain, never synced,
  until the join ends. On Dropbox and OneDrive it keeps the join's provider
  sign-in beside the code, in the store's device credentials, so a restart
  needs nothing from the person on any provider: an S3 invite's code carries
  its key, and CloudKit uses the device's Apple account.
  `CovenBuilder::unfinished_joins` lists the joins this device has under way,
  each as `decode_code_info` reads its code, and `resume_join` continues one
  with neither the code nor a new sign-in. Once the request may have been
  sent, a join can't be cancelled; it ends only by approval, decline or expiry.
- A join sends no first request once the invite's `expires_at` has passed by
  the joining device's clock: it ends as an expired one does. An ended
  invite's marker at the request's path (format.md D8) ends a join that
  finds it, sending or checking, as declined, so a request sent after its
  invite ended is never stored to wait for no one.
- The other devices end the requests of an inviting device gone for good
  ([§14.1](coven.md#141-the-cut-off)): an active admin's device that applies
  the stop creates the gone device's marker beside its requests (format.md
  D8), then deletes the requests. A join that finds the marker before
  sending sends nothing; one that finds it beside its waiting request
  deletes the request. Either ends as declined, and the marker is never
  deleted.
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
  Before registering, restore checks the signed-in account or S3 key: the
  member whose account holds the store fails with `SyncError::StoreOwner`
  under another account. After registering, it records a changed account or
  key as the member's access, as setup does.
- Each call takes a `CovenBuilder` from the app's `Coven`, plus its code,
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
  - The status callback is called on the task awaiting the call, with each
    phase as it starts: `Opening`, the new installation's open steps;
    `Connecting`; a join's `WaitingForApproval` or `Pending`; then `Loading`,
    with each of loading's steps as a `Progress<LoadStep>` (E1,
    [§13.4](coven.md#134-edge-cases-installing-on-the-device)). A check's
    scan for a join's copies is part of its wait and reports no `Loading`.
    A join loads in the approval check in which its keys first open in that
    call, reporting that scan first, so its `Loading` can come while approval
    is still being checked. A phase never comes back: once a join's keys
    open, it reports no `WaitingForApproval` or `Pending` again, in that call
    or any call that resumes it, and checks on its waits for the entry
    approving it with its status at loading's last step. It records the
    opened keys with its saved request before it reports `Loading`. A join
    resumed part way through loading reports the slots it has left. An app
    that redraws more slowly sees the newest report, as `CovenBuilder::open`'s
    caller does.
  - An unfinished store is created at its permanent path with a durable
    `.coven-bootstrap` marker. Listings hide it and ordinary opens refuse it.
    Bootstrap holds its own directory capability and opens the database once;
    publication removes the marker without moving open database or custody files.
    The bootstrap lock is the call's alone: the directories it gives out open
    the unfinished store only while it runs, and once it publishes, cancels or
    is dropped they don't, even when an owner still holds one; dropping it
    releases the lock at once.
  - Keys and credentials enter final custody only after the store loads, except
    a join's kept provider sign-in. The commit runs past the call's point of
    no return and only goes forward: a custody or publication failure, a
    panic in a custody step (`BootstrapError::Bug`, E15) or a crash keeps
    what it wrote, the installation stays listed, and the call that resumes
    it writes everything again before publishing. Ending an unpublished
    installation without its store deletes every device-only entry of the
    store, the provider credentials before the saved code or record (§13.4).
    Errors after publication carry the open handle, including its
    session-only keys, and retain committed custody.
  - A panic while publishing leaves unknown whether the marker went, so
    coven reads it: a store already published keeps its custody, with the
    bug as the error after publication, and one still unpublished fails with
    the bug, its custody kept for the call that resumes it.
  - Cancellation closes the loading database and removes the unpublished store,
    with a join's kept sign-in and code, without committing final keys or
    credentials. Dropping a waiting future
    retains encrypted work for explicit retry. There is no cancellable await
    between the final cancellation check, custody commitment, publication and
    returning the handle.
  - Restore and joining register the supplied device name. Every installation
    receives its own device id; resuming a pending join keeps that id.
  - Each loading step logs what it took at info level, so a slow restore shows
    where its time went: opening the database (migrating and optimizing),
    connecting to storage, reading the member's store keys from their copies,
    downloading each snapshot, reading the history in the floor's store
    snapshot, installing snapshots with the slots after them, reading and
    applying slots, registering the device, and committing custody. Each line
    names its time and the slots, rows or bytes it handled. The bootstrap's own
    lines also name the provider requests the step sent, by kind: how many, the
    body bytes and the time spent waiting on the provider. The same lines log
    whenever a sync pass loads snapshots or applies slots.
  - Loading uses snapshots and logs through their owners. The file-transfer
    queue starts with the returned handle and uses the builder's limits.

```rust
/// A one-time invite's UUID, also naming its join-request object (§13.2).
pub struct InviteId(pub Uuid);

/// A file's random id, as `coven_files` and the app's file reference
/// columns hold it (§17.2). ToSql/FromSql bind/read canonical text.
pub struct FileId(pub Uuid);

/// A stored object's random id, the last part of its path
/// `<store>/files/<uploader>/<object>` (§17.5): never a file's id, since
/// files over the same bytes share one object. ToSql/FromSql bind/read its
/// 16 bytes.
pub struct ObjectId(pub Uuid);

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
    /// When the invite ends, by the inviting device's clock, to the second:
    /// the joining device sends no first request after it, so the app can
    /// say until when it may be used (§13.2).
    pub expires_at: SystemTime,
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
    /// Whether the code holds an S3 key, so the device asks for none: every S3
    /// invite does, and a restore code the person's own device showed.
    pub carries_s3_key: bool,
}

/// The two codes used to open a store on a new device (§13).
pub enum CodeKind {
    /// Holds only the person's member id and private keys, store id and
    /// location, and on S3 perhaps their key.
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
    /// A valid code names another store than the one it is used for: a
    /// restore code for a store being restored in place, or an invite code
    /// saved for another store's join.
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
    /// The work panicked on the blocking pool: a bug (E15).
    Bug(Bug),
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
    /// Cancelling a restore once its device may be registered. The restore
    /// stays unfinished and listed; it only finishes (§13.1).
    RestoreRegistered,
    /// `resume_restore` found no restore under way for this store on this
    /// device: none recorded itself here, or it has ended.
    NoUnfinishedRestore(StoreId),
    /// `resume_replacement` found no replacement of this device's
    /// installation of the store past its record.
    NoUnfinishedReplacement(StoreId),
    /// The code cannot be used.
    Code(CodeError),
    /// Preparing, publishing or removing the unpublished directory failed.
    Directory(BootstrapDirectoryError),
    /// Loading or opening the store failed.
    Store(CovenError),
    /// The device is locked, and the class the app chose keeps the
    /// installation's files closed until it unlocks. A join's unfinished
    /// installation stays, and a recorded restore's; a restore before its
    /// record removes its own. The app waits for the unlock and calls again
    /// (E1).
    DeviceLocked { path: PathBuf },
    /// Provider sign-in failed.
    OAuth(OAuthError),
    /// Reading or keeping credentials or identity keys failed.
    SecureStorage(KeyError),
    /// Provider admission, signed membership or snapshot loading failed. A
    /// code sync refuses (`SyncError::Code`) is reported as `Code` instead.
    Sync(SyncError),
    /// Cleanup failed too; neither failure is hidden.
    Cleanup { operation: Box<BootstrapError>, cleanup: Box<BootstrapError> },
    /// Publication took effect, but durability or cleanup failed. The handle
    /// retains the loaded store and its session-only keys.
    Published { handle: CovenHandle, source: Box<BootstrapError> },
    /// The saved restore codes' keychain work did not run: its runtime is
    /// shutting down (E15).
    Shutdown(Shutdown),
    /// The call's work panicked: a bug (E15).
    Bug(Bug),
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
    /// and a retired pair is never exported. A credential change in flight,
    /// such as `replace_member_keys`, finishes first, and the code is the one
    /// it leaves (Appendix F.4).
    pub async fn restore_code(&self) -> Result<String, SyncError>;

    /// `restore_code()`'s code holding the S3 key this device holds now, for
    /// the app to show as a QR code to the person's own new device (§13.1). A
    /// replaced key gives its replacement. Without a key, on other providers or
    /// when this device holds none, it is `restore_code()`'s code. A locked
    /// keychain fails with
    /// SyncError::SecureStorage(KeyError::KeychainTemporarilyUnavailable).
    pub async fn restore_code_for_transfer(&self) -> Result<String, SyncError>;

    /// Makes an invite, in a write of its own (`CovenWrite::create_invite`).
    pub async fn create_invite(
        &self,
        target: InviteTarget,
        access: InviteAccess,
        delivery: InviteDelivery,
    ) -> Result<(InviteId, MemberId, OperationId), SyncError>;

    /// Approves a request, in a write of its own
    /// (`CovenWrite::approve_join_request`).
    pub async fn approve_join_request(
        &self,
        request: &JoinRequest,
    ) -> Result<OperationId, SyncError>;

    /// Declines a request, in a write of its own
    /// (`CovenWrite::decline_join_request`).
    pub async fn decline_join_request(&self, request: &JoinRequest) -> Result<(), SyncError>;

    /// Cancels an invite, in a write of its own (`CovenWrite::cancel_invite`).
    pub async fn cancel_invite(&self, invite: &InviteId) -> Result<(), SyncError>;
}

impl CovenWrite<'_> {
    /// Fixes a fresh member id or an existing active replacement target, as
    /// an admin, and returns the invite's id, that member id and the
    /// invite's operation. The write checks the invite against the applied
    /// state, offline too: one made by a device that isn't a registered
    /// admin fails with `Rejected(NotAllowed)`, a replacement of a member who
    /// isn't active with `Rejected(TargetGone)`. A pass then checks it again,
    /// grants its access and fixes its code, which `coven_invites` shows. An
    /// in-person invite expires an hour after it is made; a sent one after
    /// the lifetime chosen. Replacement preserves role, circle memberships
    /// and device ids; changed provider access is separate.
    pub fn create_invite(
        &self,
        target: InviteTarget,
        access: InviteAccess,
        delivery: InviteDelivery,
    ) -> Result<(InviteId, MemberId, OperationId), SyncError>;

    /// For a sent invite: approves exactly this checked request, and returns
    /// the operation that binds its pair, listed in `coven_operations` until
    /// the app acknowledges its outcome (E6). The write journals it, offline
    /// too, without checking it against the applied state: a pass checks it
    /// after catching up, and a refusal, such as this admin's demotion, is
    /// the operation's outcome. A pass confirms initial and historical
    /// key copies, then adds the new member with the invite's role or
    /// replaces the existing member's keys. Approval, once begun, finishes. A
    /// rejection is the operation's outcome and ends the invite as a decline
    /// does; it is never retried with a new entry. An in-person invite's
    /// request is approved without this call, and calling it fails with
    /// InvitationChanged, as do a request the invite no longer shows and an
    /// invite whose lifetime has ended.
    pub fn approve_join_request(&self, request: &JoinRequest) -> Result<OperationId, SyncError>;

    /// Declines an open invite's checked request. The write ends the invite,
    /// which leaves `coven_invites` with it; once it commits, a pass deletes
    /// the request, so the requester's join ends as declined, and takes back
    /// the storage access the invite granted. On S3 the key stays pending
    /// until deleted and cleared under E9. A request the invite no longer
    /// shows, an invite whose approval has begun, and one whose lifetime has
    /// ended fail with InvitationChanged; an ended one leaves the list when
    /// coven writes its expiry.
    pub fn decline_join_request(&self, request: &JoinRequest) -> Result<(), SyncError>;

    /// Cancels an invite before anyone joins with it. The write ends the
    /// invite, which leaves `coven_invites` with it; once it commits, a pass
    /// deletes a waiting request, so the requester's join ends as declined,
    /// and takes back the storage access the invite granted. On S3 the key
    /// stays pending until deleted and cleared under E9. An invite whose
    /// approval has begun, including an in-person invite's own, and one
    /// whose lifetime has ended fail with InvitationChanged.
    pub fn cancel_invite(&self, invite: &InviteId) -> Result<(), SyncError>;
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

/// One of this device's open invites, as `coven_invites` lists it (E4).
pub struct Invite {
    pub id: InviteId,
    /// Names the invite's pending record in `coven_pending` while its check
    /// or its grant waits or is refused.
    pub operation: OperationId,
    pub member: MemberId,
    /// The new person's name; None for a replacement.
    pub invitee: Option<String>,
    pub delivery: InviteDeliveryKind,
    /// The access the invite grants: the account shared with, or the id of
    /// the S3 key it carries; never a secret (§14.3).
    pub access: MemberAccess,
    pub expires_at: SystemTime,
    /// Granting its access, waiting for its request or the app's decision,
    /// or approving.
    pub stage: InviteStage,
}

/// Where an open invite is (§13.2). The app offers approving and declining
/// only for a request that is `Waiting`.
pub enum InviteStage {
    /// Made by the app's write: a pass checks it again and grants its access,
    /// and then it shows its code. A refusal or failed grant is its pending
    /// record; cancelling ends it.
    Granting,
    /// The code to show as a QR code, again whenever the admin asks, and the
    /// checked request once one has arrived, waiting for the app's decision.
    Waiting { code: String, request: Option<JoinRequest> },
    /// Approval of `request` has begun and finishes without the app;
    /// approving, declining or cancelling fail with InvitationChanged.
    /// `binding` is the operation that binds the request's pair: for a sent
    /// invite, the one `approve_join_request` returned, which
    /// `coven_operations` lists until its outcome is acknowledged; an
    /// in-person invite's approval is the inviting device's own.
    Approving { code: String, request: JoinRequest, binding: OperationId },
}

impl Invite {
    /// Reads `coven_invites`, in the order the invites were made, for a
    /// read or a live query: `handle.subscribe(Invite::list)`.
    pub fn list(sql: SqlReadContext<'_>) -> Result<Vec<Invite>, DbError>;
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
    /// Opening a database, with the open's steps as `CovenBuilder::open`
    /// reports them: the new installation's, whose first open runs every
    /// migration, before connecting; or, when `restore_device` finds the
    /// code's keys already installed, the store's own.
    Opening(Progress<OpenStep>),
    /// Connecting to storage, accepting provider access and sending a join
    /// request.
    Connecting,
    /// The request is sent. `confirmation` is set for a sent invite. A new
    /// status is reported each time the next check changes: `Checking` while
    /// a check runs, then when the next one is due.
    WaitingForApproval {
        confirmation: Option<ConfirmationNumber>,
        next_check: NextCheck,
    },
    /// Loading the store from storage, with loading's steps (§13.4). A
    /// waiting join's scan for its keys is part of its wait; the check in
    /// which its keys open reports that scan, then loads. Registering follows
    /// the last.
    Loading(Progress<LoadStep>),
    /// Registering this device once the store is loaded: a restore's check of
    /// the access it reaches storage with, the registration, whose claim first
    /// reads the slots added since loading, and the recorded access that
    /// follows (§13.1). No count: the claim's reads aren't known until made.
    Registering,
    /// Keeping the member's keys and the device's credentials in custody and
    /// publishing the store, the last work before the handle opens. A
    /// replacement's swap and reopening, reported as `Opening`, come first;
    /// `restore_device` also removes the installation it replaced, on the
    /// blocking pool, with no count.
    Committing,
    Pending(Vec<PendingRecord>),
}

/// A step of loading a store from storage (§13.4), in the order they run. A
/// step runs only when it has work to do.
pub enum LoadStep {
    /// Reading slots for a copy of this member's keys, when the floor holds
    /// none: from the floor's first kept slot, slot 1 without a floor, or a
    /// join's invite, to the end. Counted in slots; the total is the end
    /// found once the first slot is read, and grows if slots land meanwhile.
    FindingKeys,
    /// Streaming one of the floor's snapshots, staging its rows as they
    /// arrive: the store's first, then each circle's the member reads.
    /// Counted in the snapshot's bytes.
    Streaming(Audience),
    /// Installing the staged snapshots' rows. Counted in tables, once in each
    /// of installing's six passes over each snapshot's tables.
    Installing,
    /// Applying, in the same transaction, the slots from the snapshots'
    /// coverage to the load's target. Counted in slots.
    InstallingSlots,
    /// Reading the slots after the target. Counted in slots; the total is the
    /// end found before reading, and grows if slots land meanwhile.
    Reading,
    /// Applying the slots read, one transaction each. Counted in slots.
    Applying,
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

/// The member's S3 access key: entered on a new device, or held in the
/// restore code the person's own device showed.
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: SecretText,
}

impl S3Credentials {
    /// Encode the key as it is saved beside a saved restore code.
    pub fn encode(&self) -> Result<SecretBytes, StorageError>;

    /// Read a key saved beside a saved restore code.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError>;

    /// Refuse a key whose id or secret is empty (`InvalidConfiguration`):
    /// S3 needs both.
    pub fn validate(&self) -> Result<(), StorageError>;
}

/// Opens the store on a new device from the person's restore code, scanned
/// or typed. S3 requires a key: the one the code holds, when the person's own
/// device showed it, or else Some(s3_credentials) entered separately. Other
/// providers require None; the builder holds its own sign-in when
/// `oauth_access` is present. A missing S3 key, a second key beside one the
/// code holds, or credentials for another provider fail before bootstrap with
/// BootstrapError::Sync carrying StorageFailure::InvalidConfiguration.
/// Before it registers the device, the restore records itself on the device,
/// and from then on only goes forward: dropping or failing the call, or a
/// crash, leaves it listed for `CovenBuilder::resume_restore`, and cancelling
/// waits for the registration, then ends the call with
/// `BootstrapError::RestoreRegistered`. A cancel, a registration refused, a
/// member removed or its keys replaced, while nothing in storage names the
/// device as active
/// ends the restore, removing everything it kept (§13.1). Before the record,
/// a cancel or a failure removes the installation it loaded into. A store whose
/// unfinished installation holds a join's saved code is refused with
/// `StoreLockError::BootstrapPending` before anything changes (§13.4).
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
    /// with MemberKeysReplaced. The new device loads beside the old copy.
    /// Just before it registers, the restore records itself, keeping the
    /// code, with the S3 key inside it on S3, and the device name on the
    /// device, and on Dropbox and OneDrive the sign-in in the store's device
    /// credentials; from then on the store refuses opens with
    /// ReplacementUnfinished and the restore only goes forward. Once
    /// registered it swaps in; then the pair is committed, the old copy
    /// removed and the saved restore code rewritten. A failure before the
    /// record leaves the old copy unchanged; past it, a failure, or a failed
    /// key commit after the swap, leaves the restore for the next call, and
    /// cancelling waits for the registration, then fails with
    /// RestoreRegistered. Retrying the call, or `resume_replacement`,
    /// finishes it. A replacement reset_device began is refused with
    /// ReplacementUnfinished, changing nothing. When custody holds the code's
    /// pair and applied history binds it, the store just opens, reporting the
    /// open's steps as `BootstrapStatus::Opening`. S3 credentials as for
    /// `restore_from_code`. `prepare` is the app's step on the new
    /// installation, one bulk write of its device-local tables
    /// (`CovenHandle::write_bulk`, E3), run after loading and before the
    /// record: what it writes takes the old copy's place with the new
    /// installation or not at all. A failed step fails the call before the
    /// record; a call that finds the restore recorded or the pair installed
    /// doesn't run it. The handle holds the device-reset notice (E5).
    pub async fn restore_device(
        self,
        store: StoreId,
        code: &str,
        s3_credentials: Option<S3Credentials>,
        device_name: &str,
        prepare: impl FnOnce(BulkSqlContext<'_>) -> CovenResult<()> + Send + 'static,
        on_status: impl Fn(BootstrapStatus),
        cancel: &watch::Receiver<bool>,
    ) -> Result<CovenHandle, BootstrapError>;

    /// Finishes this device's replacement of `store` past its record, as the
    /// call that began it would, `reset_device` or `restore_device`, from what
    /// it kept on the device: with neither the code nor a new sign-in
    /// (§19.3). On Dropbox and OneDrive it connects through the kept sign-in,
    /// or a sign-in this builder holds once it grants the location's access;
    /// with neither, or a kept one the provider refuses even after its one
    /// refresh, it fails with `OAuthError::Reauthorize` and the replacement
    /// stays unfinished. A store with no replacement past its record fails
    /// with `BootstrapError::NoUnfinishedReplacement`.
    pub async fn resume_replacement(
        self,
        store: StoreId,
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

/// One store's saved entry, as listing found it (§13.1).
pub enum SavedRestoreEntry {
    /// An entry the app can pass to `restore_from_saved_code`.
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

impl Coven {
    /// The saved entries of this installation's keychain service, in iCloud
    /// Keychain on Apple and in this phone's Block Store on Android, one per store,
    /// ordered by store id; empty when none are saved. An entry coven can't
    /// restore from is listed as `Unreadable`, never omitted, and never fails the
    /// call. A keychain read failure (the search, an entry's bytes or an S3 key's
    /// presence) fails the whole call, as does a restore-code account with no
    /// store id, `KeyError::RestoreCodeStoreId`.
    /// Platforms with neither iCloud Keychain nor Block Store return BootstrapError::SecureStorage(KeyError::Unsupported).
    pub async fn saved_restore_codes(&self) -> Result<Vec<SavedRestoreEntry>, BootstrapError>;

    /// Deletes the store's saved S3 key, then its restore code: from iCloud
    /// Keychain, and so from every one of the person's Apple devices, or from this
    /// phone's Block Store, leaving other phones' own (§13.1).
    /// Readable and unreadable entries alike; every keychain group's copy goes.
    /// Touches no store, storage or device's copy, and needs no open store or
    /// provider connection. Absent entries succeed; after a failure part way the
    /// store is still listed and calling again finishes. A later join, restore,
    /// reset or member-key replacement saves the entry again.
    /// Platforms with neither iCloud Keychain nor Block Store return BootstrapError::SecureStorage(KeyError::Unsupported).
    pub async fn forget_saved_restore_code(&self, store: StoreId) -> Result<(), BootstrapError>;
}

/// Opens the selected store on a new device from its current saved restore
/// code, in iCloud Keychain or Block Store (§13.1). On S3, uses supplied credentials when Some; None reads
/// the S3 key held separately alongside the code. An absent key fails with
/// BootstrapError::Sync carrying StorageFailure::InvalidConfiguration;
/// a keychain read failure is returned. Other providers require None and
/// their own sign-in.
/// Reads by store id, including a code or key refreshed since listing.
/// Returns None when the selected store's code is no longer saved.
pub async fn restore_from_saved_code(
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
/// or the invite expires, including when it expired before the request was
/// sent, which then isn't. Dropping the call keeps the unfinished join for
/// `resume_join`. Cancelling through `control` once the request may have been
/// sent ends the call with `BootstrapError::JoinRequestSent`, keeping the
/// unfinished join, as dropping does. A store whose unfinished installation
/// holds a restore's record is refused with `StoreLockError::BootstrapPending`
/// before anything changes (§13.4).
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
    /// read fails the call; it is never skipped. An unfinished installation
    /// that keeps neither a saved code nor a restore's record, and that no
    /// call holds, is deleted: a crash left it before its call wrote to
    /// storage (§13.4).
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

    /// The restores under way on this device (§13.1), ordered by store id,
    /// each as `decode_code_info` reads the code it recorded: restores that
    /// recorded themselves before registering the device and have not ended.
    /// A record coven can't read fails the call; it is never skipped. An
    /// unfinished installation that keeps neither a record nor a join's saved
    /// code, and that no call holds, is deleted, as `unfinished_joins` does
    /// (§13.4).
    pub async fn unfinished_restores(&self) -> Result<Vec<CodeMetadata>, BootstrapError>;

    /// Continues an unfinished restore from its record, as `restore_from_code`
    /// does: the code, with an S3 key in it on S3, and the device name. On
    /// Dropbox and OneDrive it connects through the sign-in the restore kept,
    /// or a sign-in this builder holds, which replaces the kept one once it
    /// grants the location's access. With neither, or a kept one the provider
    /// refuses even after its one refresh, it fails with
    /// `OAuthError::Reauthorize` and the restore stays unfinished. Cancelling
    /// ends it as `restore_from_code`'s cancel does. A store with no record
    /// fails with `BootstrapError::NoUnfinishedRestore`.
    pub async fn resume_restore(
        self,
        store: StoreId,
        on_status: impl Fn(BootstrapStatus),
        cancel: &watch::Receiver<bool>,
    ) -> Result<CovenHandle, BootstrapError>;
}

impl OAuthClients {
    /// Registers the access the app offers; None disables a sign-in. Coven
    /// sends the token requests with the builder's clock.
    pub fn new(dropbox: Option<DropboxClients>, onedrive_client_id: Option<String>) -> Self;

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
// On S3 the code holds the key the phone uses.
let code = handle.restore_code_for_transfer().await?;
show_blurred_qr_code(&code);
```

On the laptop:

```rust
let info = decode_code_info(&scanned)?;
let metadata = info.metadata();
show_provider_location(&metadata.location);
let mut builder = coven.builder()
    .synced_tables(tables())
    .migrations(migrations())
    .oauth_clients(oauth_clients.clone())
    .clock(clock.clone());
if let Some(access) = metadata.oauth_access {
    builder.authenticate(access).await?;
}
// A code the phone showed holds the key; a typed one needs it entered.
let s3_credentials = match metadata.cloud_provider {
    CloudProvider::S3 if !metadata.carries_s3_key => Some(ask_for_s3_access_key().await?),
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

If the laptop has her saved entries instead, from iCloud Keychain, Ana chooses
from them:

```rust
let saved = coven.saved_restore_codes().await?;
let selected = match choose_saved_store(&saved) {
    SavedRestoreEntry::Readable(selected) => selected,
    // An entry coven can't read is offered only for deletion.
    SavedRestoreEntry::Unreadable { store_id, .. } => {
        coven.forget_saved_restore_code(*store_id).await?;
        return Ok(());
    }
};
let mut builder = coven.builder()
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
match restore_from_saved_code(
    builder, selected, s3_credentials, "Ana's laptop", |step| show_step(step), &cancel_rx,
).await? {
    Some(handle) => show_store(handle),
    None => show_saved_code_removed(),
}
```

If Carol's laptop stopped with `MemberKeysReplaced`, it restores in place from the
code her phone shows, keeping its copy until the new one is ready. Her theme,
read from the old copy before the call, goes into the new one in the same swap:

```rust
let handle = coven.builder()
    .synced_tables(tables())
    .migrations(migrations())
    .restore_device(
        store,
        &scanned,
        s3_credentials,
        "Carol's laptop",
        move |sql| {
            sql.execute("INSERT INTO preferences VALUES('theme',?1)", [theme])?;
            Ok(())
        },
        |step| show_step(step),
        &cancel_rx,
    )
    .await?;
```

Example, Ana adding Carol. On Ana's phone:

```rust
let (invite, _, _) = handle
    .create_invite(
        InviteTarget::NewMember { role: MemberRole::Member, name: "Carol".into() },
        InviteAccess::ProviderAccount { email: "carol@example.com".into() },
        InviteDelivery::Sent { lifetime: InviteLifetime::Day },
    )
    .await?;

let mut invites = handle.subscribe(Invite::list);
let mut sent = false;
while let Some(open) = invites.next().await {
    let found = open?.into_iter().find(|i| i.id == invite).map(|i| i.stage);
    match found {
        // A pass has granted its access and fixed its code.
        Some(InviteStage::Waiting { code, request: None }) if !sent => {
            send_to_carol(&code);
            sent = true;
        }
        // Only a request waiting for Ana is hers to decide.
        Some(InviteStage::Waiting { request: Some(request), .. }) => {
            // Ana asks Carol for the number her phone shows.
            if confirm(&request.confirmation).await {
                handle.approve_join_request(&request).await?;
            } else {
                handle.decline_join_request(&request).await?;
            }
            break;
        }
        _ => {}
    }
}
```

On Carol's phone:

```rust
let mut builder = coven.builder()
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
    coven.builder()
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
    /// Initializing the identity panicked: a bug (E15).
    Bug(Bug),
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
    /// A bug in the call's work, such as a panic in the app's custody, is
    /// `KeyError::Bug` (E15).
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
/// What custody keeps: the store keys or the member's identity. Sealed:
/// coven alone says how each is named and encoded.
pub trait KeyMaterial: Clone + Send + Sync + 'static { /* sealed */ }
impl KeyMaterial for StoreKeyring {}
impl KeyMaterial for MemberIdentity {}

/// The app's own store for key material this device holds.
pub trait Custody<T: KeyMaterial>: Send + Sync {
    /// Called once at creation or open; None when this device has never held `T`.
    /// Retains the unlocked persistence capability until close.
    fn unlock(&self) -> Result<Option<T>, KeyError>;
    /// Keeps `material`, replacing what was kept.
    fn persist(&self, material: &T) -> Result<(), KeyError>;
    /// Removes what was kept; succeeds if nothing was kept.
    fn forget(&self) -> Result<(), KeyError>;
    /// Erases held memory after users finish; performs no persistence or IO.
    /// Fallible saves complete through persist before dependent work starts.
    fn close(&self);
}
```

### E12 Circles

- A circle's members add and remove its members ([§15.5](coven.md#155-running-a-circle)).
  Making, renaming, deleting and changing a circle's members are steps of an
  app's write, journaled offline too (E6). A new circle takes rows once its
  slot applies.
- Circle-changing calls return `SyncError` ([E5](#e5-storage-and-sync)), as member calls
  do ([E9](#e9-members-and-devices)). Validation uses the latest applied prefix:
  `Rejected(NotAllowed)` for authority, `Rejected(NoAdminLeft)` for the
  last-admin rule, and `Rejected(TargetGone(target))` for a removed or
  deleted member, device or circle. One the applied slots don't name yet is
  journaled, and the pass that reads on answers `TargetGone` as the
  operation's outcome if it is still absent ([§11.1](coven.md#111-the-steps)).
  The target retains its typed id. `StoreOwner` remains
  a separate provider constraint.
- Removing someone from a circle publishes one entry. Rotation follows in a
  later sync pass; it is not part of that operation (§12.2).

```rust
/// Circle calls borrowing the open store that owns their work (§15, E12).
pub struct Circles<'a> { /* private fields */ }

impl CovenHandle {
    pub fn circles(&self) -> Circles<'_>;
}

impl Circles<'_> {
    /// Makes a circle, in a write of its own (`CovenWrite::create_circle`).
    pub async fn create(&self, name: &str) -> Result<(CircleId, OperationId), SyncError>;

    /// Renames a circle, in a write of its own (`CovenWrite::rename_circle`).
    pub async fn rename(&self, circle: CircleId, name: &str) -> Result<Option<OperationId>, SyncError>;

    /// Deletes a circle, in a write of its own (`CovenWrite::delete_circle`).
    pub async fn delete(&self, circle: CircleId) -> Result<Option<OperationId>, SyncError>;

    /// Adds a store member, in a write of its own (`CovenWrite::add_circle_member`).
    pub async fn add_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<Option<OperationId>, SyncError>;

    /// Removes a member, in a write of its own (`CovenWrite::remove_circle_member`).
    pub async fn remove_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<Option<OperationId>, SyncError>;

    /// The circles this member is in, in circle-id order.
    pub async fn list(&self) -> CovenResult<Vec<Circle>>;

    /// The same circle-list query, live (E4).
    pub fn subscribe_list(&self) -> LiveQuery<Vec<Circle>>;

    /// A circle's members who are still in the store, in member-id order.
    pub async fn members(&self, circle: CircleId) -> CovenResult<Vec<CircleMemberInfo>>;

    /// The same query for this circle's members, live (E4).
    pub fn subscribe_members(&self, circle: CircleId) -> LiveQuery<Vec<CircleMemberInfo>>;

    /// Resets a circle from this copy; only the snapshot and writes that
    /// read the reset count in that circle, on this device too (§19.4).
    pub async fn reset(&self, circle: CircleId) -> Result<(), SyncError>;
}

impl CovenWrite<'_> {
    /// Makes a circle with this member as its first member and returns its id
    /// with the operation's. Any active member may. Its first key, the copy
    /// that hands it to this member and its introduction follow in a pass;
    /// rows go into it once its slot applies (§15.5). A name that isn't a
    /// valid name fails with `Format`.
    pub fn create_circle(&self, name: &str) -> Result<(CircleId, OperationId), SyncError>;

    /// Renames a circle. Its key, members and rows don't change.
    pub fn rename_circle(&self, circle: CircleId, name: &str) -> Result<Option<OperationId>, SyncError>;

    /// Deletes a circle with one admin slot. The deletion rule hides
    /// its rows permanently; there is no row-delete write (§15.9).
    pub fn delete_circle(&self, circle: CircleId) -> Result<Option<OperationId>, SyncError>;

    /// Adds a store member to the circle. They get its current and earlier
    /// keys, so they read its history. Rejoining devices reload skipped parts,
    /// using the snapshot and retained suffix (§15.7).
    pub fn add_circle_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<Option<OperationId>, SyncError>;

    /// Removes someone with one entry; remaining members rotate exposed keys
    /// in a later pass (§15.8).
    pub fn remove_circle_member(
        &self,
        circle: CircleId,
        member: &MemberId,
    ) -> Result<Option<OperationId>, SyncError>;
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
- Ana works on her app against a test bucket, `ana-dev`. Each time she resets
  her development library she wants it to start on an empty remote: a new
  store refuses a bucket holding anything else (§20.3). Before creating the
  new store she calls `empty_s3_bucket` with the location and key the store
  will connect with. It deletes everything the earlier stores left in
  `ana-dev`, their location marker included, and returns how many objects
  and unfinished uploads went.
- `empty_s3_bucket` first lists the bucket's root with delimiter `/`. Any
  object there other than the location marker `coven-location`, or any folder
  not named by a store id, refuses the call with
  `EmptyS3BucketError::OtherFiles` before anything is removed: the bucket
  holds files no store wrote, which emptying would destroy. Ana pointing the
  tool at her photo bucket by mistake gets that refusal.
- Otherwise it deletes every object in the bucket, whatever wrote it: every
  store's objects, upload temporaries, detection probes and the marker. It
  also aborts every unfinished multipart upload.
- It lists everything before removing anything, so the listing's page markers
  stay valid, and then lists again. Something still in the bucket means a
  device kept writing there while it ran, and the call fails with
  `StorageFailure::Conflict`.
- The location is validated as setup validates it (E5). A location that is not
  S3, an empty key, or an uncatalogued endpoint with no profile is refused
  with `EmptyS3BucketError::Storage`. Listing, aborting and deleting failures
  keep their provider cause there too.

Edge cases:

- The first failure stops the call. What it removed before then stays
  removed. Removal is idempotent: an object already gone counts as deleted,
  and an upload already finished or aborted counts as aborted. So calling
  again finishes the job, and a marker already deleted leaves the root
  holding only store folders, which the listing admits.
- In a versioned bucket, deleting an object leaves its earlier versions and
  a delete marker. The call leaves those: a member key may not delete
  versions. Only the bucket's own lifecycle or an admin removes them.
- It needs only what a member key already may do in the bucket (§20.10):
  list objects and uploads, delete objects and abort uploads.

```rust
/// What emptying an S3 bucket removed (E14).
#[cfg(feature = "dev-tools")]
pub struct EmptiedS3Bucket {
    /// Objects deleted.
    pub objects: usize,
    /// Unfinished multipart uploads aborted.
    pub uploads: usize,
}

/// Why an S3 bucket was not emptied (E14).
#[cfg(feature = "dev-tools")]
pub enum EmptyS3BucketError {
    /// The bucket's root holds something no store wrote: an object other
    /// than the location marker, or a folder not named by a store id.
    /// Nothing was removed.
    OtherFiles,
    /// The location or key was refused, or a request failed. Whatever was
    /// removed before the failure stays removed; emptying again finishes.
    Storage(StorageError),
}

/// Delete every object and abort every unfinished upload in an S3
/// location's bucket, whatever wrote them, and return how many of each went
/// (E14). A bucket whose root holds anything but stores' folders and the
/// location marker is refused with `EmptyS3BucketError::OtherFiles` before
/// anything is removed. The first failure stops it; calling again finishes.
/// Retained versions in a versioned bucket stay.
#[cfg(feature = "dev-tools")]
pub async fn empty_s3_bucket(
    location: StorageConfig,
    credentials: S3Credentials,
) -> Result<EmptiedS3Bucket, EmptyS3BucketError>;
```

### E15 Background work, panics and shutdown

- Work an app runs apart from the caller that waits for it, on tokio's
  blocking pool or as its own task, loses its panic. tokio keeps a task's
  panic in its `JoinHandle`, and a dropped handle discards it. Re-raising it
  at the join with `std::panic::resume_unwind` skips every cleanup the caller
  wrote for `Err`.
- `coven::task` turns such a panic into a `TaskFailed` value at the nearest
  join, so the caller's `Err` path runs. The caller reports a `Panicked`
  wherever it reports its own bugs; work nobody waits for reports to the
  app's bug sink instead, as the end of this section describes. A
  `Shutdown` is not a bug. The runtime dropped the work because the process
  is ending it.
- Ana's music app reads an album's tags on the blocking pool with
  `blocking("read the tags", ...)` while it imports the album. The tag reader
  panics on a damaged file with "tag table is empty". The call returns
  `Err(TaskFailed::Panicked(Bug { operation: "read the tags", message: "tag
  table is empty" }))`. The import removes its staged files, as on any other error,
  and the app reports the bug. Had the panic been re-raised, it would have
  unwound through the import and left the staged files behind.
- `blocking` runs a closure on the blocking pool. `joined` waits for a task
  already spawned. `caught` runs a future inline and catches a panic in any
  of its polls: one message of a long-lived loop fails with its `Bug`, and
  the loop serves the next. `caught_sync` does the same for a synchronous
  call, such as a callback foreign code calls. Unwinding out of an
  `extern "C"` function ends the process, so such a callback catches the
  panic and answers with its own failure value.

A runtime that shuts down drops the work it has not finished, and coven's own
calls run work the same way: database calls and file work on the blocking
pool, and calls that must finish even when their caller stops waiting, such
as closing a store, as their own task. A runtime shutting down is not a bug,
so such a call ends with a `Shutdown` in its error instead of a panic.

- Ana's music app keeps a live query of its albums. A test of the app opens
  the store on its own runtime and ends while the query is still reading.
  The query's next read goes to a blocking pool that runs nothing more, and
  its `next` returns `Err(CovenError::Database(DbError::Shutdown(Shutdown {
  operation: "live query", outcome: ShutdownOutcome::NotRun })))`. Had coven
  panicked there, the test process would have aborted.
- `Shutdown.outcome` says how far the work got, so whether a write took
  effect. tokio can't stop a closure on the blocking pool once it has
  started, so the pool only drops a closure it never ran: `NotRun`, and
  nothing the call would change changed. Every database call, coven's file
  work and the custody work that opening, deleting and the saved restore
  codes do run this way.
- A task the runtime drops stops at whichever wait it is at: `Unknown`, and
  what the call would change may be changed in part. Closing a store or a
  read-only handle, `create_store`, storage setup, unlocking store keys,
  `forget_store_keys`, `replace_member_keys`,
  `replace_access_key` and `update_credentials` run as their own task. Their
  caller reads the state again before relying on it: whether the store is
  listed, or what `storage_config` and the key state say.
- The error that carries the `Shutdown` is the call's own:
  `DbError::Shutdown` for a database call, `FileError::Shutdown` for file
  work, wherever a disk failure would appear, and a `Shutdown` variant of
  `CovenError`, `KeyError`, `SyncError`, `StorageSetupError`,
  `StoreKeyUnlockError`, `StoreCreationError`, `StoreDeletionError` and
  `BootstrapError` for the calls above. `SyncError` takes a database call's
  `Shutdown` as its own and reports it as `LocalFailure::Shutdown`, which
  clears by itself: the next process runs the work again, and a slot whose
  application it stopped is not blocked (§19.1).

Coven's own calls turn a panic in their work into a value the same way. A
panic is a bug, in coven or in the app's callback, and the call's error
carries it as `Bug`: `DbError::Bug` for a database call, `FileError::Bug`
for file work, and a `Bug` variant of each error that has a `Shutdown` one.
`SyncError` reports it as `LocalFailure::Bug`.

- Ana's app writes a note with `write`, and its closure panics on a note
  with no title: "note 43 has no title". The write's transaction rolls back,
  the bytes it staged are removed as after any failed write, and `write`
  returns `Err(DbError::Bug(Bug { operation: "write", message: "note 43 has
  no title" }))`. The next write runs on the same writer. Had the panic
  unwound, it would have ended the app's task that called `write`, skipping
  the cleanup the app does on an error.
- A file removal that fails after such a write joins the bug as
  `DbError::FileCleanup`, as it joins any failed write's error, so neither
  failure is lost.
- CloudKit calls each operation's callbacks on its own queues, where a
  panic unwinding out would end the process. Each callback catches its
  panic, and the first panic in any of an operation's callbacks, such as a
  record's progress or the operation's completion, answers the request with
  `StorageError::Bug`. The completion still runs its own work, such as
  removing the operation's staged asset files.
- A panic as an HTTPS request hands its body to the connection, such as
  in the progress reader that hears the bytes go, fails that request with
  `StorageError::Bug`. Unwound into the connection, it would end the
  exchange with what reads as a network failure, which the caller retries.
- A panic in one of coven's own SQL functions, such as `coven_key`, fails
  the statement that called it and the app's call with `DbError::Bug`, its
  message kept. rusqlite alone reports such a panic as an error that says
  only "unwinding panic". A function changes nothing, so the handle goes
  on.
- Setting up storage and forgetting the store keys hold the running
  connection's passes while they work. A panic in that work fails the call
  with the bug and lets the passes run again, as a failure does. Unwound,
  it would leave sync held until the store closes.
- A panic in one of `prepare_device_only`'s downloads fails the call with
  `DeviceOnlyError::Bug`, and the call removes every copy and folder it
  made before it returns, as after any failure. Unwound, it would leave
  them on disk until the store's next write.

An error that keeps another failure takes the bug that failure carries as
its own `Bug`, never under a disk, lock, storage or custody label, so the
call's own error says it was a bug.

- File and lock work: `StoreLockError::Bug` for taking a store's lock,
  `StoreListError::Bug` for listing the stores, `KeyError::Bug` for
  custody's file work, `LocalFileError::Bug` for reading a file on this
  device, and `StorageError::Bug`, whose failure is `StorageFailure::Bug`,
  for a provider request's local work.
- Each error that keeps one of those, or another call's error, does the
  same: `DbError`, `IdentityError`, `SettingsError`, `CovenError`,
  `SyncError`, `StorageSetupError`, `FileReadError`, `CopyPathError`,
  `DeviceOnlyError`, `BootstrapDirectoryError`, `BootstrapError`,
  `StoreCreationError`, `StoreDeletionError`, `StoreKeyUnlockError` and
  `ReadOnlyOpenError`.
- `CovenError` keeps a database call's bug as
  `CovenError::Database(DbError::Bug)`, as it keeps the call's other
  failures, and a sync step's bug as `CovenError::Bug`.

Work nobody waits for has no caller to return its bug to. A store's
long-lived workers run that way: its sync loop, a connection attempt, the
upload worker, the invite expiry, the network follower and a read-ahead. So
does work a call runs as its own task once its caller stops waiting, such as
closing a store. Such a bug goes to the app's bug sink, which the app passes
to `Coven::new`, usually its crash reporter; every store and call of that
`Coven` reports there. The sink has no default. An app that wants its bugs
only logged passes `LoggedBugs`, which logs each at error. Without the sink a
worker's panic would end it unseen, and the work it served would wait for it
forever.

- Ben pulls to refresh the Household store on his phone, and the sync loop's
  pass panics reading slot 412 with "a chunk runs past the slot's end". The
  app's bug sink hears `Bug { operation: "the sync loop", message: "a chunk
  runs past the slot's end" }`. The last pass is recorded failed with the
  bug, so the sync status shows it, and sync stops. Ben's retry is answered
  `Failed` with `SyncError::WorkerFailed` carrying the bug, and a later
  `start_sync` fails with the same `WorkerFailed`. So does a command the
  loop had taken or not yet taken as it panicked, such as a `stop_sync`
  closing it: the loop catches its own panic while it still holds them,
  records the bug, and only then answers them. The sink heard the bug
  already, so Ben's app shows these and reports neither.
- Each worker's owner ends the work it served, so nothing waits on a worker
  that is gone. The sync loop and a connection attempt do as above, and
  answer every waiting retry with `WorkerFailed`, those the panicking pass
  took among them. The upload worker answers each waiting
  `retry_uploads_now` with `WorkerFailed`, and each later one too, as it
  does with the failure when a failed handle ends it. The invite expiry, the
  network follower and a read-ahead only report: the next open writes the
  expiries that passed, the network shows as last heard until the store
  closes, and the next read fetches what the read-ahead would have.
- A worker that doesn't end can still fail on a bug. Ben's app asks
  `retry_uploads_now`, and a database call in the upload run serving it
  panics. The worker sends the bug to the sink and goes on; the retry, and
  every other one that run serves, is answered `WorkerFailed` with the
  bug. The retries waited on the worker's work, so the bug is the
  worker's to report, once.
- A call's `Bug` is always a bug its own work hit, and its caller is the
  one that reports it. A bug coven has already reported comes back to later
  calls under a variant naming the work it failed: `SyncError::WorkerFailed`
  for a worker's, and `DbError::StoreFailed` for a handle's, below. Were
  both `Bug`, an app would report one bug once for each call that repeats
  it, or guess from their text which ones are the same.
- A failure such a worker records, answers a retry with, or ends on may
  carry a bug: a database call in the loop's pass that panicked returns
  `DbError::Bug`, and the loop records it as the pass's failure. No caller
  hears of it, so that bug goes to the sink too, once. So does a bug in any
  other database call the sync loop or a connection attempt makes on its
  own: writing what a retry would do, beginning a pass, writing the last
  pass, or saving the connection's wait. Such a write answers the retries
  waiting then with its failure, and the worker stops as on any failure
  of its own write. A sync loop that stops so answers every later command
  with why: `WorkerFailed` for a bug, and otherwise its write's failure,
  which has failed the handle. A loop answers `DbError::StoreClosed` only
  once its store's closing ended it, and `SyncError::NoStorage` once its
  connection was released. So a pass's failure, in the sync status or a retry's
  answer, is never the app's to report. `CarriesBug::bug` finds the bug an
  error carries, itself or through the failure it keeps, and none in
  `WorkerFailed` or `StoreFailed`.
- A live query whose run panics returns its `DbError::Bug` once, then
  `None`, as for a closed store. What the run read is unknown, so no commit
  could tell whether to run it again.
- The calls that run as their own task, above, run with `spawn_completing`,
  as do stopping sync, `rotate_key` and an administrative operation's
  submission. A caller still waiting gets the bug in the call's error. Once
  none is, the bug goes to the sink, as does a bug the call's error carries.
- Every database call runs on the blocking pool with `blocking_completing`:
  a read, a write, a live query's run and its processing, staging files,
  opening and closing a store, reading a store's name for its listing and
  a reset's discarded database, and reading a file on this device for its
  upload. Ana's app drops a write's future while
  the write runs, and its closure panics with "note 43 has no title". The
  write rolls back and returns `DbError::Bug` to no one, so the sink hears
  `Bug { operation: "write", message: "note 43 has no title" }`.
- Coven's file work runs the same way: listing the stores and their
  recorded deletions, taking a store's lock, each write and sync of a
  staged or cache file, flushing a commit's batch of files, the folder work
  of `prepare_device_only`'s copies, and removing a replaced installation.
  A caller still waiting gets `FileError::Bug`, or the error that keeps it;
  once none is, the bug goes to the sink.
- So does the keychain and custody work of coven's other calls: listing
  the stores, `discard_store`, `storage_config`, the saved restore codes,
  opening a store's custody and owners, creating a store's custody,
  reading a read-only open's saved credentials, closing custody, recording,
  finishing and forgetting a deletion, and undoing a cut storage setup.
  Ana's app asks for `saved_restore_codes` and drops the call when its
  screen closes, and the keychain read panics. The sink hears `Bug {
  operation: "list saved restore codes", .. }`. Coven runs none of its own
  work with `blocking`.

A panic can also leave state half-changed: a lock held as it unwinds is
poisoned, and code that SQLite calls can't fail at all. What coven does
depends on whose state it is. State of one open store that a panic may have
left wrong fails that store's handle with the bug. The store's call whose
work hit it returns it as `DbError::Bug`; one hit outside the store's calls
goes to the sink. Every later call and live query fails with
`DbError::StoreFailed`, carrying the same bug, until the store reopens, as
for the other failures that fail a handle ([§4](coven.md#4-writing-locally)). The bug has
reached its one reporter, so the app shows `StoreFailed` and doesn't report
it. State the whole process shares ends the process. Elsewhere a panic leaves
nothing half-changed, by how the state changes.

- Dan's app writes a note, and a bug in coven's SQL authorizer panics while
  it judges the `INSERT`. SQLite's authorizer can't fail, and what the
  authorizer was recording may be half-changed, so it refuses that statement
  and every later one on its connection. The write fails with
  `DbError::Bug(Bug { operation: "authorize a statement", .. })`, which
  Dan's app reports. The handle fails with the same bug: Dan's next read
  fails with `DbError::StoreFailed` carrying it, which his app only shows,
  and the app reopens the store.
- A hook SQLite calls as a write's transaction changes a row, which
  records the row for sync and for live queries, can't fail. A panic in it,
  which rusqlite drops, and a read of the row's values that fails are kept
  by the hook, and the transaction rolls back with `DbError::Bug` or the
  read's error rather than committing what the hook missed. The next write
  goes on. Only a virtual generated column, which keeps no value, reads as
  absent.
- A panic while the handle's live-query state is held may have left a live
  query untold of a commit. The panic reaches whatever ran it, as any panic
  does. The next use of that state fails the handle with a bug naming it,
  `Bug { operation: "the store's live queries", .. }`, and each live query
  returns `StoreFailed` with it as its last result, as every later call
  does. A panic after a write commits, as it tells its live queries, fails
  the handle at once, with that panic's own bug. The write's changes stay
  committed, the write returns the bug as `DbError::Bug`, and every later
  call returns `StoreFailed`.
- The registry of the store locks this process holds is shared by every
  store, and its changes check its rules as they go. A panic partway ends
  the process: no one store's handle covers it. A lock's drop that finds
  the registry breaking a rule ends the process the same way, without a
  panic.
- Every other lock coven takes guards state that each change leaves whole
  before anything in it can panic, such as a set it adds a name to or an
  option it takes, so a poisoned lock is used as it is.

Edge cases:

- After a panic, `caught` drops the future at once, inside a catch of its
  own. A drop that panics on state the first panic left behind is logged at
  error and does not unwind into the caller.
- A panic's payload whose own drop panics is dropped the same way, by every
  helper. So none of them unwinds into its caller.
- A payload that is not text, thrown with `std::panic::panic_any`, gives the
  message "a panic whose payload is not text".
- tokio reports an aborted task and a task its runtime dropped the same way.
  `joined` gives `Shutdown` with `ShutdownOutcome::Unknown` for both;
  `blocking` gives `ShutdownOutcome::NotRun`.
- A store's background workers, such as the upload worker, that the runtime
  dropped have stopped, which is all closing waits for: closing goes on.
- Dropping the future `blocking` or `joined` returns does not stop work
  already spawned: it runs to its end, and its result or panic is dropped.
  Dropping a `spawn_completing` or `blocking_completing` future does not
  stop its work either, and its panic goes to the bug sink, as does a bug
  its error carries. Work that already answered, its answer unread when
  the future drops, reports that answer's bug as the future drops.
- A bug sink whose `report` panics is logged at error; its panic goes no
  further, since nothing above it would catch it.
- Coven's own values never panic as they drop: one dropped during an
  unwind that panicked again would end the process. What a drop finds
  wrong fails the store's handle instead: a write transaction whose hooks
  SQLite won't let go of, a staged or cache file's name released that no
  holder reserved, or a transfer whose file lost its entry in flight. A bug
  a drop finds goes where a panic's does: to the store's call the drop runs
  in, as `DbError::Bug`, or, outside the store's calls, as for a transfer
  dropped on the upload worker's task, to the sink.
- A store's call whose work hits a second bug returns the first, and the
  sink hears the second, so neither is lost. The handle keeps the first
  bug that failed it.

```rust
// These items are in `coven::task`, not at coven's root.

/// Why work apart from its caller gave no result (E15).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TaskFailed {
    /// The work panicked: a bug, which the caller reports.
    Panicked(Bug),
    /// The runtime it ran on is shutting down and dropped it before it
    /// finished. Not a bug: the process is ending the work.
    Shutdown(Shutdown),
}

/// Work that panicked: a bug in it, not a failure its caller can act on
/// (E15). Shown as "{operation} panicked: {message}".
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub struct Bug {
    /// What the work was doing, as its caller named it.
    pub operation: &'static str,
    /// The text the panic carried (`panic_message`).
    pub message: String,
}

/// Work its runtime dropped as it shut down, before the work finished (E15).
/// Not a bug: the process is ending the work. Shown as "{operation} did not
/// run: its runtime is shutting down", or "{operation} may have run in part:
/// ..." for `Unknown`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub struct Shutdown {
    /// What the work was doing, as its caller named it.
    pub operation: &'static str,
    /// How far the work got, so whether anything it would change changed.
    pub outcome: ShutdownOutcome,
}

/// How far work its runtime dropped got (E15).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShutdownOutcome {
    /// It never started, so nothing it would change changed: a closure the
    /// blocking pool dropped.
    NotRun,
    /// It may have stopped at any of its waits, after some of its steps: a
    /// task the runtime dropped partway. The caller reads what it would
    /// change before relying on it.
    Unknown,
}

impl TaskFailed {
    /// The failure of `operation` whose panic carried `payload`.
    pub fn panicked(operation: &'static str, payload: &(dyn std::any::Any + Send)) -> Self;

    /// The failure a joined task's `error` stands for: `Panicked` for a
    /// panic, `Shutdown` with `ShutdownOutcome::Unknown` for a cancelled task.
    pub fn of_join(operation: &'static str, error: tokio::task::JoinError) -> Self;

    /// The failure a join of a closure on the blocking pool stands for:
    /// `Shutdown` with `ShutdownOutcome::NotRun` for one the pool dropped.
    pub fn of_blocking_join(operation: &'static str, error: tokio::task::JoinError) -> Self;

    /// This failure as the error of the call whose work it ended: its `Bug`
    /// for a panic, its `Shutdown` for the runtime dropping the work.
    pub fn into_error<E: From<Bug> + From<Shutdown>>(self) -> E;

    /// What the failed work was doing, as its caller named it.
    pub fn operation(&self) -> &'static str;

    /// Whether the work panicked, rather than being dropped by its runtime.
    pub fn is_panic(&self) -> bool;
}

/// The text a panic carried: what `panic!` and `expect` were given, or a
/// placeholder for a payload that is not text.
pub fn panic_message(payload: &(dyn std::any::Any + Send)) -> String;

/// Run `work` on the blocking pool. A panic in it, or the runtime dropping it
/// unstarted (`ShutdownOutcome::NotRun`), comes back as `TaskFailed`, never
/// as an unwind.
pub async fn blocking<T: Send + 'static>(
    operation: &'static str,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, TaskFailed>;

/// Wait for a task already spawned. Its panic, or the runtime cancelling it,
/// comes back as `TaskFailed`.
pub async fn joined<T: Send + 'static>(
    operation: &'static str,
    task: tokio::task::JoinHandle<T>,
) -> Result<T, TaskFailed>;

/// Run `work` inline, catching a panic in any of its polls. Run inline, the
/// work can't be dropped by its runtime, so a panic is its only failure.
/// After a panic `work` is dropped at once, inside a catch of its own.
pub async fn caught<T>(operation: &'static str, work: impl Future<Output = T>) -> Result<T, Bug>;

/// `caught` for a synchronous call: an FFI callback, a dedicated thread's
/// body, a `Drop`.
pub fn caught_sync<T>(operation: &'static str, work: impl FnOnce() -> T) -> Result<T, Bug>;

/// Where a bug in work nobody waits for goes: a store's long-lived workers
/// and work whose caller stopped waiting for it. The app passes its own
/// crash reporter to `Coven::new`.
pub trait BugSink: Send + Sync {
    /// Report `bug`, which no caller hears of otherwise.
    fn report(&self, bug: &Bug);
}

/// A shared `BugSink`.
pub type BugSinkRef = std::sync::Arc<dyn BugSink>;

/// Logs each bug at error: the sink of an app that wants its bugs only
/// logged.
pub struct LoggedBugs;

/// Keeps every bug it hears of and logs each at error: the sink a test
/// gives coven. The test takes each bug it expects; a bug left untaken when
/// the sink drops fails the test, so a bug no test expected never passes
/// unseen.
#[cfg(feature = "test-utils")]
pub struct HeardBugs { /* private */ }

#[cfg(feature = "test-utils")]
impl HeardBugs {
    /// A sink that has heard nothing yet, to share with coven.
    pub fn shared() -> std::sync::Arc<Self>;

    /// Every bug heard since the last take, in the order heard.
    pub fn take(&self) -> Vec<Bug>;

    /// Wait for the first bug not yet taken, and take it: for a test whose
    /// bug reaches the sink after the call it made returned.
    pub async fn next(&self) -> Bug;
}

/// An error that may carry a bug: work its call ran panicked, and the
/// call's error says so. Implemented by `Bug`, `TaskFailed`, every coven
/// error with a `Bug` variant, `std::io::Error`, which carries the bug it was
/// made from, and `Infallible`.
pub trait CarriesBug {
    /// The bug this error carries, itself or through the failure it keeps:
    /// one for the caller to report. An earlier bug that an error repeats,
    /// `DbError::StoreFailed`'s or `SyncError::WorkerFailed`'s, was reported
    /// already, so this finds none in it.
    fn bug(&self) -> Option<&Bug>;
}

/// Hand `bug` to `bugs`. A sink that panics too is logged, and its panic
/// goes no further.
pub fn report(bugs: &dyn BugSink, bug: &Bug);

/// Hand the bug `error` carries, if it carries one, to `bugs`. Whether it
/// carried one, so the work logs an error that carried none.
pub fn report_carried(bugs: &dyn BugSink, error: &impl CarriesBug) -> bool;

/// `error`, once the bug it carries, if any, has gone to `bugs`.
pub fn reported<E: CarriesBug>(bugs: &dyn BugSink, error: E) -> E;

/// Run `work` as its own task that nobody waits for: a worker its owner
/// stops. A panic in it goes to `bugs`, then `ended` runs with the bug, so
/// the owner ends what the work served in a way its readers see. A panic in
/// `ended` goes to `bugs` too, and no further.
pub fn spawn_owned<E, F>(
    operation: &'static str,
    bugs: BugSinkRef,
    work: impl Future<Output = ()> + Send + 'static,
    ended: E,
) -> OwnedTask
where
    E: FnOnce(Bug) -> F + Send + 'static,
    F: Future<Output = ()> + Send;

/// A task `spawn_owned` started. Dropping it leaves the task running.
pub struct OwnedTask { /* private */ }

impl OwnedTask {
    /// End the work at its next wait. Its `ended` does not run.
    pub fn abort(&self);
    /// Whether the work has ended.
    pub fn is_finished(&self) -> bool;
    /// Wait until the work has ended: finished, panicked and reported, or
    /// dropped by its runtime as it shuts down.
    pub async fn ended(self);
}

/// Run `work` as its own task, so it finishes even when its caller stops
/// waiting. Its panic comes back as `TaskFailed::Panicked` to a caller still
/// waiting. Once none is, the panic goes to `bugs`, as does a bug its error
/// carries.
pub fn spawn_completing<T: Send + 'static, E: CarriesBug + Send + 'static>(
    operation: &'static str,
    bugs: BugSinkRef,
    work: impl Future<Output = Result<T, E>> + Send + 'static,
) -> CompletingTask<Result<T, E>>;

/// Run `work` on the blocking pool, where it runs to its end even when its
/// caller stops waiting. Its panic comes back as `TaskFailed::Panicked` to a
/// caller still waiting, and the pool dropping it unstarted as `Shutdown`
/// with `ShutdownOutcome::NotRun`. Once no caller waits, the panic goes to
/// `bugs`, as does a bug its error carries.
pub fn blocking_completing<T: Send + 'static, E: CarriesBug + Send + 'static>(
    operation: &'static str,
    bugs: BugSinkRef,
    work: impl FnOnce() -> Result<T, E> + Send + 'static,
) -> CompletingTask<Result<T, E>>;

/// The result of work `spawn_completing` or `blocking_completing` runs, a
/// future of `Result<T, TaskFailed>`. Dropping it leaves the work running;
/// the work's bug, or the bug of an answer it gave that no one read, goes to
/// the sink.
pub struct CompletingTask<T> { /* private */ }
```
