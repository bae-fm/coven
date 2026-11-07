//! Typed opening, authorization and migration failures; the schema rules
//! are `schema_error`'s.

use coven_foundation::files::{SettingsError, StoreLockError};
use coven_foundation::task::{Bug, CarriesBug};

impl CarriesBug for DbError {
    fn bug(&self) -> Option<&Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::Disk(error) => error.bug(),
            Self::Lock(error) => error.bug(),
            Self::StatePersistence(cause)
            | Self::CheckpointFailed(cause)
            | Self::StatisticsFailed(cause) => cause.bug(),
            Self::Rollback { operation, .. } => operation.bug(),
            Self::FileCleanup { write, failures } => write
                .as_ref()
                .err()
                .and_then(|write| write.bug())
                .or_else(|| failures.iter().find_map(DbError::bug)),
            Self::StagingCleanup { operation, cleanup } => {
                operation.bug().or_else(|| cleanup.bug())
            }
            Self::Closing { failures } => failures.iter().find_map(DbError::bug),
            // An earlier bug, which reached its one reporter as it happened.
            Self::StoreFailed(_) => None,
            _ => None,
        }
    }
}

/// A failed database call or a write the database refuses (§4, §9, §15, §17).
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// Another claim must settle before this device reserves a new number.
    #[error("a claim is already pending")]
    ClaimPending,
    /// No current claim exists for this operation.
    #[error("there is no current claim")]
    NoClaim,
    /// A request to a provider account is between its target being fixed and
    /// its reply, so a revocation queued now could precede the grants it
    /// must follow (§20.9). The call is made again once the request ends.
    #[error("a request to a provider account is running")]
    ProviderRequestRunning,
    /// Uncertain publication cannot be abandoned or resealed; a landed writes
    /// batch cannot finish until its consumed state has committed.
    #[error("the current claim has not settled or its writes have not applied")]
    ClaimUnsettled,
    /// The installation has exhausted its claim-number representation.
    #[error("claim numbers exhausted")]
    ClaimNumberExhausted,
    /// This installation has exhausted its positive original-write identities.
    #[error("write numbers exhausted")]
    WriteNumberExhausted,
    /// The supplied floor evidence does not cover the attempted slot.
    #[error("floor evidence does not cover the claim")]
    InvalidClaimCoverage,
    /// Slot application must extend the previously committed prefix.
    #[error("slot {received:?} does not follow applied slot {applied}")]
    SequenceGap {
        /// Previously committed coverage.
        applied: u64,
        /// The received slot.
        received: coven_merge::Slot,
    },
    /// A slot's readable original awaits an external prerequisite.
    #[error("write awaits {0:?}")]
    WriteWaiting(crate::WriteWait),
    /// A pending record, its subject, the sync status or an operation's
    /// data could not be encoded, or a stored pending record could not be
    /// decoded as the requested record type (E5).
    #[error("invalid sync state data: {0}")]
    StateEncoding(#[source] serde_json::Error),
    /// Saving pending state or the sync status failed. The call that lost
    /// the update and every later call and live query fail with this same
    /// cause until reopened (E5).
    #[error("saving sync state failed: {0}")]
    StatePersistence(#[source] std::sync::Arc<DbError>),
    /// An earlier call's rollback failed, so a connection may still be inside
    /// that transaction, holding its uncommitted changes or its old snapshot;
    /// this handle, read-only or not, refuses calls until reopened (§4).
    #[error("an earlier rollback failed; reopen the store")]
    RollbackFailed,
    /// One of coven's own WAL checkpoints failed with this cause, other than
    /// on a busy or locked database, so the store's checkpointing stopped.
    /// Rather than stay open while its WAL grows without bound, this handle
    /// and every live query fail with this same cause until reopened (§4).
    #[error("a WAL checkpoint failed: {0}")]
    CheckpointFailed(#[source] std::sync::Arc<DbError>),
    /// Checking or gathering the query planner's statistics while the store
    /// stays open failed with this cause, other than on a busy or locked
    /// database, so keeping them stopped. Rather than stay open while the
    /// planner plans with statistics that no longer hold, this handle and
    /// every live query fail with this same cause until reopened (§4).
    #[error("gathering the planner's statistics failed: {0}")]
    StatisticsFailed(#[source] std::sync::Arc<DbError>),
    /// The handle's clock could not wait for an open invite's expiry, so
    /// the invite would outlive its lifetime. Rather than stay open
    /// without writing expiries, this handle and every live query fail with
    /// this same cause until reopened, and opening writes each expiry
    /// already past (§13.2).
    #[error(transparent)]
    WallTimer(coven_foundation::clock::WallTimerError),
    /// The system refused a thread of coven's upkeep, the one that
    /// checkpoints a WAL or the one that keeps the planner's statistics, so
    /// the store did not open (§4).
    #[error("an upkeep thread could not start: {0}")]
    UpkeepThread(#[source] std::io::Error),
    /// Reading a claim's sealed bytes to retain them failed, and the claim's
    /// transaction did not commit (§11).
    #[error("reading a claim's bytes failed: {0}")]
    ClaimRead(#[source] std::io::Error),
    /// This handle or a clone has closed the store (E1).
    #[error("store is closed")]
    StoreClosed,
    /// The call's runtime is shutting down and dropped its work (E15). A
    /// call the blocking pool dropped unstarted changed nothing
    /// (`ShutdownOutcome::NotRun`); closing, which runs as its own task, may
    /// have closed some connections (`ShutdownOutcome::Unknown`).
    #[error(transparent)]
    Shutdown(#[from] coven_foundation::task::Shutdown),
    /// The call's work panicked: a bug in coven or in the app's callback,
    /// which the caller reports (E15). A write that panicked rolled back,
    /// and the bytes it released or staged are removed as after any failed
    /// write.
    #[error(transparent)]
    Bug(#[from] coven_foundation::task::Bug),
    /// An earlier bug may have left this handle's state wrong, so the
    /// handle failed with it, and every call and live query fails with it
    /// until the store is reopened (E15). The call whose work hit the bug
    /// returned it as `Bug`, or the store's bug sink heard it, so the app
    /// shows this and doesn't report it again.
    #[error("the store's handle failed on an earlier bug; reopen the store: {0}")]
    StoreFailed(coven_foundation::task::Bug),
    /// SQLite refused or failed a statement, preserving its error.
    #[error(transparent)]
    Sqlite(rusqlite::Error),
    /// A statement met damage SQLite reports (`SQLITE_CORRUPT`,
    /// `SQLITE_NOTADB`), decoding stored facts found damage (§19.3), or a
    /// snapshot's staged records differ from a second staging's (§19.1).
    #[error("damaged database")]
    DamagedDatabase,
    /// Coven's own code broke a rule it relies on, such as a caller's
    /// contract or a step its state machine cannot reach: a bug, not damage.
    /// Sync reports it as `Failed(LocalFailure::Bug)` (§19.1).
    #[error("coven invariant failed: {what}")]
    Invariant {
        /// The broken rule.
        what: &'static str,
    },
    /// Directory settings disagree with a store's creation entry (§10).
    #[error("store settings disagree with creation of {store}")]
    StoreSettingsMismatch {
        /// The id in the creation entry.
        store: coven_foundation::id_source::StoreId,
    },
    /// A synced table declaration or schema is invalid.
    #[error(transparent)]
    Schema(#[from] crate::SchemaError),
    /// A write was given files staged, or a pin prepared, in another open
    /// store, or in this one before it closed: the copies they name aren't
    /// reserved here (§17.3, §17.8).
    #[error("these files were not prepared in this open store")]
    PreparedElsewhere,
    /// A write made a file device-only and also removed its last reference,
    /// so the copy kept as its original would name no file (§17.3). Nothing
    /// committed.
    #[error("file {file} made device-only has no reference left in the same write")]
    DeviceOnlyUnreferenced {
        /// The file.
        file: coven_foundation::id_source::FileId,
    },
    /// A pin prepared over a whole copy already kept found that copy gone
    /// when its write committed it, evicted since: nothing was pinned, and
    /// preparing again downloads it. A prepare whose partial download the
    /// app's unpin ended meanwhile fails with it too (§17.8).
    #[error("the kept copy of file {file} changed since its pin was prepared")]
    PinChanged {
        /// The file.
        file: coven_foundation::id_source::FileId,
    },
    /// One of coven's steps in this write failed, and the write's closure
    /// went on to return success: the write rolls back, since a failed step
    /// may have made only part of its change (§4).
    #[error("a step of this write failed")]
    StepFailed,
    /// One of the app's statements in this write failed, and the write's
    /// closure went on to return success: the write rolls back, since SQLite
    /// may have kept part of the statement's change or undone rows coven saw
    /// it change (§4).
    #[error("a statement of this write failed")]
    StatementFailed,
    /// App SQL read, changed or defined one of coven's internal objects (§4).
    #[error("app SQL cannot access internal object {table}")]
    InternalTable {
        /// The reserved object.
        table: String,
    },
    /// A local trigger wrote a synced table or a shared trigger a local one (§9.4).
    #[error("trigger {trigger} cannot write table {table}")]
    TriggerTarget {
        /// The trigger doing the write.
        trigger: String,
        /// The target table.
        table: String,
    },
    /// A bulk write's statement, or a trigger or foreign-key action it would
    /// run, writes a synced table (§4, E3). Nothing committed.
    #[error("a bulk write cannot write synced table {table}")]
    SyncedTableInBulkWrite {
        /// The synced table.
        table: String,
    },
    /// SQLite rolled the owned transaction back itself, so it can't go on (E3, E13).
    #[error("SQLite ended the owned transaction")]
    TransactionEnded,
    /// App SQL tried transaction control, a PRAGMA, ATTACH, loading an
    /// extension, changing a hidden rowid, or changing the schema outside a
    /// migration (§4, E13).
    #[error("app SQL cannot perform {operation}")]
    StatementForbidden {
        /// The refused operation.
        operation: &'static str,
    },
    /// SQLite kept another journal mode instead of WAL (§4).
    #[error("SQLite kept journal mode {mode} instead of WAL")]
    WalUnavailable {
        /// The journal mode SQLite selected.
        mode: String,
    },
    /// An app's SQLite setting is larger than SQLite can hold (api.md E1).
    #[error("SQLite setting {setting} exceeds {maximum}")]
    SqliteSettingOutOfRange {
        /// The setting's PRAGMA name.
        setting: &'static str,
        /// The largest value SQLite holds for it.
        maximum: u64,
    },
    /// An app collation has the name of one of SQLite's own (api.md E1).
    #[error("collation {name} is SQLite's own")]
    ReservedCollation {
        /// The name as the app gave it.
        name: String,
    },
    /// The app gave two collations one name, which SQLite compares without
    /// regard to ASCII case (api.md E1).
    #[error("collation {name} is registered twice")]
    DuplicateCollation {
        /// The second registration's name.
        name: String,
    },
    /// An app function has the name of one SQLite or coven already has, or
    /// coven's reserved prefix (api.md E1).
    #[error("function {name} is SQLite's or coven's own")]
    ReservedFunction {
        /// The name as the app gave it.
        name: String,
    },
    /// The app gave two functions one name, which SQLite compares without
    /// regard to ASCII case (api.md E1).
    #[error("function {name} is registered twice")]
    DuplicateFunction {
        /// The second registration's name.
        name: String,
    },
    /// An app function is deterministic and takes no arguments, which SQLite
    /// evaluates while planning in a way coven can't receive (api.md E1).
    #[error("function {name} is deterministic and takes no arguments")]
    ConstantFunction {
        /// The function's name.
        name: String,
    },
    /// A writable open found a new version of an app collation or function
    /// that a CHECK or generated column uses. Coven rebuilds only indexes,
    /// so the open changed nothing (§4).
    #[error("{name} has a new version, and a CHECK or generated column of {table} uses it")]
    VersionChangeRefused {
        /// The collation's or function's name, ASCII lowercase.
        name: String,
        /// The table whose CHECK or generated column uses it.
        table: String,
    },
    /// A read-only open found the store's indexes built with another
    /// version of an app collation or function than the one registered.
    /// Only a writable open rebuilds them (§4).
    #[error("the store's indexes use version {built} of {name}, not {registered}")]
    VersionNotRebuilt {
        /// The collation's or function's name, ASCII lowercase.
        name: String,
        /// The version the store's indexes were built with.
        built: u32,
        /// The version this open registered.
        registered: u32,
    },
    /// The wall clock or next timestamp exceeds its representation (§7.2).
    #[error("clock is outside the timestamp range")]
    ClockOutOfRange,
    /// Protected remote input fills the configured disk budget. No input or
    /// budget change was committed; increase the budget or finish pending work.
    #[error("retained input requires {required} bytes within a {maximum} byte budget")]
    InputBudgetExceeded {
        /// Bytes required after evicting every eligible range.
        required: u64,
        /// Configured encrypted-input limit.
        maximum: u64,
    },
    /// A value or row exceeds the format (format.md D1, D2, D5), or a write's
    /// plaintext and sealing key ids cannot fit in its SQLite queue row (§6.3).
    #[error("{field} length {actual} exceeds {maximum}")]
    TooLarge {
        /// The bounded field or collection.
        field: &'static str,
        /// Its actual length.
        actual: u64,
        /// Its maximum length.
        maximum: u64,
    },
    /// A reference reaches an audience the source row's readers cannot read (§15.3).
    #[error("reference {table}.{column} at {key:?} reaches another audience")]
    ReferenceAudience {
        /// The referencing table.
        table: String,
        /// Its row's primary key.
        key: crate::RowKey,
        /// The referencing column.
        column: String,
    },
    /// A write targets a circle whose deletion has been applied (§15.9).
    #[error("circle {0} has been deleted")]
    DeletedCircle(coven_foundation::id_source::CircleId),
    /// A local write targets a circle this member is not in or the store does not have (§15).
    #[error("not a member of circle {0}")]
    NotInCircle(coven_foundation::id_source::CircleId),
    /// This audience still exposes its previous data while its selected boundary reloads (§16.5).
    #[error("audience {0:?} is reloading")]
    AudienceReloading(coven_merge::Audience),
    /// The write directly changed another member's row of an `owner_only()`
    /// table: a statement, a shared trigger, or coven copying or dismissing
    /// it for the app (§9.6). The transaction, including its upload record,
    /// did not commit.
    #[error("{table} row {key:?} belongs to member {owner:?}")]
    NotOwner {
        /// The owned row's table.
        table: String,
        /// Its primary key, in declared key order.
        key: crate::RowKey,
        /// The member who originally created it.
        owner: coven_foundation::id_source::MemberId,
    },
    /// The write's foreign-key actions on this row are not the schema's exact
    /// consequence of its direct changes, so every receiver would refuse
    /// them (§9.6). The transaction, including its upload record, did not
    /// commit.
    #[error("{table} row {key:?} is not an exact foreign-key effect")]
    InvalidForeignKeyEffect {
        /// The row's table.
        table: String,
        /// Its primary key, in declared key order.
        key: crate::RowKey,
    },
    /// An inserted row's independent key holds no UUID (§9.2).
    #[error("independent key {key:?} of {table} does not contain a canonical lowercase UUIDv4 or UUIDv7")]
    KeyNotUuid {
        /// The inserted row's table.
        table: String,
        /// Its primary key, in declared key order.
        key: crate::RowKey,
    },
    /// A root row's audience cell is neither `store` nor a circle id in
    /// canonical lowercase form (§15).
    #[error("audience cell of {table} at {key:?} is neither `store` nor a canonical lowercase circle id")]
    InvalidAudience {
        /// The written row's table.
        table: String,
        /// Its primary key, in declared key order.
        key: crate::RowKey,
    },
    /// A downloaded write fails the merge's checks and is never applied (§19.1).
    #[error("invalid write {write:?}: {error}")]
    InvalidWrite {
        /// The refused write's device and write number.
        write: coven_merge::WriteId,
        /// The merge rule it broke.
        #[source]
        error: coven_merge::MergeError,
    },
    /// Original row-change evidence is unavailable. Receipt waits without
    /// consuming the position; local authoring rolls back instead of guessing.
    #[error("missing original write sources: {0:?}")]
    MissingWriteSources(Vec<coven_format::write_sources::ChangeId>),
    /// A waiting record or its conversion violates the write format (E13).
    #[error(transparent)]
    WriteFormat(coven_format::Error),
    /// A migration's declared kind disagrees with its runtime classification.
    #[error("migration {version} declared the wrong kind")]
    MigrationKindMismatch {
        /// The refused migration's version.
        version: u32,
    },
    /// A conversion changed or introduced a reference whose generation it cannot know (E13).
    #[error("conversion cannot change reference {table}.{column}")]
    MigrationReference {
        /// The referencing table in the converted change.
        table: String,
        /// The referencing column in the converted change.
        column: String,
    },
    /// A converted row names the same column twice.
    #[error("migration conversion repeats {table}.{column}")]
    MigrationDuplicateColumn {
        /// The converted table.
        table: String,
        /// The repeated column.
        column: String,
    },
    /// A converted column has old/new values incompatible with its operation.
    #[error("migration conversion of {table}.{column} does not fit {op:?}")]
    MigrationColumnOperation {
        /// The converted table.
        table: String,
        /// The converted column.
        column: String,
        /// The requested operation.
        op: crate::ChangeOp,
        /// Whether an old value was supplied.
        has_old: bool,
        /// Whether a new value was supplied.
        has_new: bool,
    },
    /// A converted column is absent from the target schema.
    #[error("converted column {table}.{column} is absent from the schema")]
    MigrationColumnMissing {
        /// The converted table.
        table: String,
        /// The absent column.
        column: String,
    },
    /// A converted table has no primary index.
    #[error("converted table {table} has no primary index")]
    MigrationPrimaryKeyMissing {
        /// The converted table.
        table: String,
    },
    /// A converted key has the wrong number of values.
    #[error("converted key of {table} has {actual} values, expected {expected}")]
    MigrationKeyArity {
        /// The converted table.
        table: String,
        /// The primary index width.
        expected: usize,
        /// The supplied key width.
        actual: usize,
    },
    /// A converted row or reference names an absent table.
    #[error("converted table {table} is absent from the schema")]
    MigrationTableMissing {
        /// The absent table.
        table: String,
    },
    /// Distinct original references became the same converted reference.
    #[error("converted references of {table} collide at {reference:?}")]
    MigrationReferenceCollision {
        /// The original referencing table.
        table: String,
        /// The repeated converted reference.
        reference: coven_merge::ForeignKey,
    },
    /// A write dismissed a removed row, deleting it for good, and also
    /// changed that row or one its delete cascades to (§8.4).
    #[error("a write dismissing {table} at {key:?} also changes it")]
    DismissedRowChanged {
        /// The declared table.
        table: String,
        /// The row's key.
        key: crate::RowKey,
    },
    /// A write changed a write-once file reference that named a file (§17.2).
    #[error("file reference on {table} at {key:?} cannot change")]
    FileWriteOnce {
        /// The declared table.
        table: String,
        /// The row's key.
        key: crate::RowKey,
    },
    /// The user's original disappeared.
    #[error("user file is missing: {}", path.display())]
    UserFileMissing {
        /// The original path.
        path: std::path::PathBuf,
    },
    /// The user's original changed during preparation or since it was prepared.
    #[error("user file changed: {}", path.display())]
    UserFileChanged {
        /// The original path.
        path: std::path::PathBuf,
    },
    /// No namespace of this name was declared when the store opened (E1).
    #[error("file namespace {namespace} is not declared")]
    UndeclaredNamespace {
        /// The supplied namespace.
        namespace: String,
    },
    /// A file was staged into a namespace of the other kind: app bytes into a
    /// user namespace, or a user's original into an app one (§17.1); or a
    /// bound reference names a user's file (§17.4).
    #[error("file namespace {namespace} holds {kind:?} files")]
    NamespaceKind {
        /// The supplied namespace.
        namespace: String,
        /// The kind the namespace was declared with.
        kind: crate::Provenance,
    },
    /// The id source reused the physical name of a kept file.
    #[error("file name {name:?} is already kept by this device")]
    FileNameReused {
        /// The refused physical file name.
        name: coven_foundation::files::FileName,
    },
    /// A file the write staged is referenced by none of its rows (§17.2).
    #[error("no row references staged file {file} in {namespace}")]
    FileUnreferenced {
        /// The staged file's namespace.
        namespace: String,
        /// The staged file's id.
        file: coven_foundation::id_source::FileId,
    },
    /// A reference names no file this device has a record of in its row's
    /// audience (§17.2).
    #[error("{table}.{column} names {file}, which is no file of its row's audience")]
    FileUnknown {
        /// The referencing table.
        table: String,
        /// The reference column.
        column: String,
        /// The value the column holds.
        file: String,
    },
    /// This device has no record of the file (§17.2): one to move, to make
    /// device-only, or to stage a new file over.
    #[error("no file {file} here")]
    FileAbsent {
        /// The requested file.
        file: coven_foundation::id_source::FileId,
    },
    /// The file is on another device only, and only that device has its
    /// bytes: one to move to the cloud (§17.3), or one a write makes a new
    /// file over (§17.2).
    #[error("file {file} is on another device")]
    NotThisDevice {
        /// The file.
        file: coven_foundation::id_source::FileId,
    },
    /// The file to move is in the cloud already (§17.3).
    #[error("file {file} is not on this device only")]
    NotDeviceOnlyHere {
        /// The file.
        file: coven_foundation::id_source::FileId,
    },
    /// The file to move is in a move already, moving or failed: a file is in
    /// one move at a time, and the app cancels a failed one to move its files
    /// again (§17.3).
    #[error("file {file} is in move {id:?}")]
    FileInMove {
        /// The file.
        file: coven_foundation::id_source::FileId,
        /// The move that holds it.
        id: crate::MoveId,
    },
    /// A bound reference names a file larger than its limit (§17.4).
    #[error("{table}.{column} names a file of {size} bytes, over its limit of {limit}")]
    BoundFileTooLarge {
        /// The referencing table.
        table: String,
        /// The bound reference column.
        column: String,
        /// The file's size in bytes.
        size: u64,
        /// The column's limit in bytes.
        limit: u64,
    },
    /// A bound reference names a file on one device only that no bound
    /// upload stores (§17.4).
    #[error("file {file} is on one device only, with no bound upload")]
    DeviceOnly {
        /// The file.
        file: coven_foundation::id_source::FileId,
    },
    /// This device never made the move (§17.3).
    #[error("no move {id:?} here")]
    MoveUnknown {
        /// The move.
        id: crate::MoveId,
    },
    /// The move finished: its files are in the cloud (§17.3).
    #[error("move {id:?} finished")]
    MoveFinished {
        /// The move.
        id: crate::MoveId,
    },
    /// The move is a bound file's: its upload holds a write until the file
    /// is stored, so it can't be cancelled (§17.4).
    #[error("move {id:?} stores a bound file")]
    MoveBound {
        /// The move.
        id: crate::MoveId,
    },
    /// The file to make device-only is on one device only already, so
    /// storage holds no bytes of it to download (§17.3).
    #[error("file {file} is not in the cloud")]
    NotInCloud {
        /// The file.
        file: coven_foundation::id_source::FileId,
    },
    /// No record here names the stored bytes a file reference was taken
    /// over any more: the record went, or a switch took it off them, as
    /// making the file device-only does. The cache keeps bytes only while a
    /// record names them, so a pin fetching them keeps nothing (§17.8).
    #[error("file {file} is no longer stored where its reference names")]
    NoLongerStored {
        /// The file.
        file: coven_foundation::id_source::FileId,
    },
    /// Another call making files device-only is writing a copy at this path
    /// (§17.3).
    #[error("a copy is being written at {}", path.display())]
    CopyPathTaken {
        /// The copy's path.
        path: std::path::PathBuf,
    },
    /// Reading or keeping file bytes failed.
    #[error(transparent)]
    Disk(coven_foundation::files::FileError),
    /// A write queueing a file's upload, as staging for the cloud or a move
    /// does, could not create the file's random key (§17.3).
    #[error("creating a file key failed: {0}")]
    FileKey(#[source] coven_crypto::CryptoError),
    /// Retaining the store while staging file bytes failed.
    #[error(transparent)]
    Lock(StoreLockError),
    /// File cleanup failed, retaining every failure: on opening (`Ok`), or
    /// after a failed write, whose error it keeps (Appendix C.3).
    #[error("file cleanup failed: {failures:?}; write outcome: {write:?}")]
    FileCleanup {
        /// `Ok` on opening; otherwise the failed write's error.
        write: Result<(), Box<DbError>>,
        /// Each failed byte removal or pending-record update.
        failures: Vec<DbError>,
    },
    /// A snapshot load or file check failed, and so did removing the records
    /// it staged, which its failure ends (§16.2); both failures are kept,
    /// and sync judges the load's or check's own as it would alone (§19.1).
    #[error("removing staged snapshot records failed: {cleanup}; after: {operation}")]
    StagingCleanup {
        /// The load's or check's own failure.
        operation: Box<DbError>,
        /// Removing the staged records' tables.
        cleanup: Box<DbError>,
    },
    /// An operation no longer has the state expected by its caller.
    #[error("operation {0:?} changed or is absent")]
    OperationChanged(crate::OperationId),
    /// The operation the app acknowledges is still running: it has no
    /// outcome yet (§11.4).
    #[error("operation {0:?} is still running")]
    OperationRunning(crate::OperationId),
    /// `coven_operations` lists no operation with this id: the app's writes
    /// never journaled it, or its outcome was acknowledged already (§11.4).
    #[error("no operation {0:?} is listed")]
    OperationUnknown(crate::OperationId),
    /// Self-replacement has paused app writes while queued objects settle
    /// and the new binding and custody are installed (§12.3, E9).
    #[error("member keys are changing; app writes are paused")]
    MemberKeysChanging,
    /// The prepared raise no longer names the current counted prefix.
    #[error("raise capture changed before snapshot creation")]
    CaptureChanged,
    /// An ordinary snapshot of an audience whose raise this device captured
    /// and has not published: it would carry a schema version the history
    /// doesn't hold yet (§16.2).
    #[error("this device's raise of the audience has not landed")]
    RaiseWaiting,
    /// Loading plaintext supplied by sync failed without changing the database.
    #[error(transparent)]
    Snapshot(#[from] crate::SnapshotError),
    /// Closing failed for these connections, after every one was tried (E1).
    #[error("closing database connections failed: {failures:?}")]
    Closing {
        /// Failures in connection-close order.
        failures: Vec<DbError>,
    },
    /// A transaction failed and rolling it back failed too. Later calls fail
    /// with `RollbackFailed` until the store is reopened (§4).
    #[error("{operation}; rollback failed: {rollback}")]
    Rollback {
        /// The operation that required rollback.
        operation: Box<DbError>,
        /// SQLite's rollback failure.
        #[source]
        rollback: rusqlite::Error,
    },
}

impl DbError {
    /// Whether the disk under the store caused this failure: its files
    /// could not be read or kept, or SQLite found the disk full, failing,
    /// read-only or damaged. A failure of any other kind, and one that
    /// names no disk error, is not the disk's.
    pub fn is_disk(&self) -> bool {
        match self {
            Self::Disk(_) => true,
            Self::Sqlite(error) => sqlite_error_is_disk(error),
            Self::Rollback { operation, .. } => operation.is_disk(),
            _ => false,
        }
    }
}

/// Whether SQLite failed because of the disk under it: full, failing, gone
/// read-only, or damaged.
pub fn sqlite_error_is_disk(error: &rusqlite::Error) -> bool {
    matches!(
        error.sqlite_error_code(),
        Some(
            rusqlite::ErrorCode::DiskFull
                | rusqlite::ErrorCode::SystemIoFailure
                | rusqlite::ErrorCode::CannotOpen
                | rusqlite::ErrorCode::ReadOnly
                | rusqlite::ErrorCode::DatabaseCorrupt
                | rusqlite::ErrorCode::NotADatabase
        )
    )
}

impl From<coven_foundation::task::TaskFailed> for DbError {
    fn from(failed: coven_foundation::task::TaskFailed) -> Self {
        failed.into_error()
    }
}

impl From<coven_foundation::files::FileError> for DbError {
    fn from(error: coven_foundation::files::FileError) -> Self {
        match error {
            coven_foundation::files::FileError::Bug(bug) => Self::Bug(bug),
            error => Self::Disk(error),
        }
    }
}

impl From<StoreLockError> for DbError {
    fn from(error: StoreLockError) -> Self {
        match error {
            StoreLockError::Bug(bug) => Self::Bug(bug),
            error => Self::Lock(error),
        }
    }
}

impl From<coven_foundation::files::WriterError> for DbError {
    fn from(error: coven_foundation::files::WriterError) -> Self {
        match error {
            coven_foundation::files::WriterError::Lock(error) => error.into(),
            coven_foundation::files::WriterError::File(error) => error.into(),
        }
    }
}

impl From<coven_format::Error> for DbError {
    fn from(error: coven_format::Error) -> Self {
        match error {
            coven_format::Error::Limit { field, actual, maximum } => {
                Self::TooLarge { field, actual: actual as u64, maximum: maximum as u64 }
            }
            error => Self::WriteFormat(error),
        }
    }
}

impl From<rusqlite::Error> for DbError {
    fn from(error: rusqlite::Error) -> Self {
        match error {
            rusqlite::Error::UserFunctionError(source) => match source.downcast::<Self>() {
                Ok(error) => *error,
                Err(source) => Self::Sqlite(rusqlite::Error::UserFunctionError(source)),
            },
            // A column coven reads into one of its types, such as a `RowKey`,
            // fails with what that type found.
            rusqlite::Error::FromSqlConversionFailure(index, kind, source) => {
                match source.downcast::<Self>() {
                    Ok(error) => *error,
                    Err(source) => {
                        Self::Sqlite(rusqlite::Error::FromSqlConversionFailure(index, kind, source))
                    }
                }
            }
            // SQLite found a page it can't read as what it should hold, or a
            // file that isn't a database: the damage, wherever a statement
            // met it, is the store's, not the statement's (§19.3).
            rusqlite::Error::SqliteFailure(failure, _)
                if matches!(
                    failure.code,
                    rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase
                ) =>
            {
                Self::DamagedDatabase
            }
            error => Self::Sqlite(error),
        }
    }
}

/// Opening the database failed: its writer or read-only connections, its
/// lock, its settings or its migrations (§4, E1, E13).
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// The database failed.
    #[error(transparent)]
    Database(#[from] DbError),
    /// An app migration failed or cannot run on this schema.
    #[error(transparent)]
    Migration(#[from] MigrationError),
    /// Coven's internal schema is unsupported or its migration cannot run.
    #[error(transparent)]
    CovenMigration(#[from] CovenMigrationError),
    /// The directory's settings could not be read.
    #[error(transparent)]
    Settings(SettingsError),
    /// The store's lock could not be taken.
    #[error(transparent)]
    Lock(StoreLockError),
    /// A required builder choice was not supplied (E1).
    #[error("opening requires {field}")]
    MissingConfiguration {
        /// The missing choice.
        field: &'static str,
    },
    /// Taking the store's lock panicked: a bug, which the caller reports
    /// (E15). Opening's database work that panicked is `DbError::Bug`.
    #[error(transparent)]
    Bug(#[from] coven_foundation::task::Bug),
}

impl CarriesBug for OpenError {
    fn bug(&self) -> Option<&Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::Database(error) => error.bug(),
            Self::Lock(error) => error.bug(),
            Self::Settings(error) => error.bug(),
            Self::Migration(MigrationError::Failed { source, .. })
            | Self::CovenMigration(CovenMigrationError::Failed { source }) => source.bug(),
            Self::Migration(
                MigrationError::NotContiguous { .. } | MigrationError::SchemaTooNew { .. },
            )
            | Self::CovenMigration(
                CovenMigrationError::ReadOnly | CovenMigrationError::SchemaTooNew { .. },
            )
            | Self::MissingConfiguration { .. } => None,
        }
    }
}

impl From<SettingsError> for OpenError {
    fn from(error: SettingsError) -> Self {
        match error {
            SettingsError::Bug(bug) => Self::Bug(bug),
            error => Self::Settings(error),
        }
    }
}

impl From<StoreLockError> for OpenError {
    fn from(error: StoreLockError) -> Self {
        match error {
            StoreLockError::Bug(bug) => Self::Bug(bug),
            error => Self::Lock(error),
        }
    }
}

impl From<rusqlite::Error> for OpenError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error.into())
    }
}

/// Coven's local tables cannot be used at this version (Appendix C.1, E1).
#[derive(Debug, thiserror::Error)]
pub enum CovenMigrationError {
    /// The read-only connection cannot migrate coven's tables.
    #[error("read-only open cannot migrate coven's schema")]
    ReadOnly,
    /// The internal schema requires a newer coven.
    #[error("coven schema version {current} is newer than supported version {supported}")]
    SchemaTooNew {
        /// The database's internal schema version.
        current: u32,
        /// The internal schema version this coven supports.
        supported: u32,
    },
    /// A migration failed and its transaction rolled back.
    #[error("coven migration failed: {source}")]
    Failed {
        /// The failure, including any rollback error.
        #[source]
        source: Box<DbError>,
    },
}

/// The application migration run could not complete.
#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    /// The list is not numbered from one without gaps.
    #[error("migration at {position} has version {found}, expected {expected}")]
    NotContiguous {
        /// Zero-based position in the list.
        position: usize,
        /// Declared version.
        found: u32,
        /// Required version.
        expected: u32,
    },
    /// The database requires a newer app.
    #[error("schema version {current} is newer than supported version {supported}")]
    SchemaTooNew {
        /// The database version.
        current: u32,
        /// The last supplied migration.
        supported: u32,
    },
    /// The migration or final validation failed; the whole run rolled back.
    #[error("migration {version} ({name}) failed: {source}")]
    Failed {
        /// Migration being run, or the final migration for validation errors.
        version: u32,
        /// Its name.
        name: &'static str,
        /// The original failure.
        #[source]
        source: Box<DbError>,
    },
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
