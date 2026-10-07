//! Typed opening, schema, authorization and migration failures.

use coven_foundation::files::{SettingsError, StoreLockError};

/// The result of opening or using this database.
pub type CovenResult<T> = Result<T, CovenError>;

/// Failures of opening, reading and writing a store (§5, §20.1, §20.3).
#[derive(Debug, thiserror::Error)]
pub enum CovenError {
    /// App data could not be sealed with the selected store key.
    #[error(transparent)]
    Seal(#[from] coven_crypto::SealError),
    /// Reading device identity or an app callback's custody operation failed.
    #[error(transparent)]
    Key(#[from] coven_crypto::custody::KeyError),
    /// An app callback failed and SQLite also failed to roll it back.
    #[error("{operation}; rollback failed: {rollback}")]
    Rollback {
        /// The callback or database failure.
        operation: Box<CovenError>,
        /// SQLite's rollback failure.
        #[source]
        rollback: rusqlite::Error,
    },
    /// File cleanup failed, retaining the callback's outcome and every failure.
    #[error("file cleanup failed: {failures:?}; write outcome: {write:?}")]
    FileCleanup {
        /// Ok if the transaction committed; otherwise its original error.
        write: Result<(), Box<CovenError>>,
        /// Each failed byte removal or pending-record update.
        failures: Vec<DbError>,
    },
    /// The database or a write's validation failed.
    #[error(transparent)]
    Database(#[from] DbError),
    /// An app migration failed or cannot run on this schema.
    #[error(transparent)]
    Migration(#[from] MigrationError),
    /// Coven's tables need a migration this open cannot run.
    #[error(transparent)]
    CovenMigration(#[from] CovenMigrationError),
    /// The directory's settings could not be read.
    #[error(transparent)]
    Settings(#[from] SettingsError),
    /// The writer's lock could not be acquired.
    #[error(transparent)]
    Lock(#[from] StoreLockError),
    /// A required builder choice was not supplied (§20.1).
    #[error("opening requires {field}")]
    MissingConfiguration {
        /// The missing choice.
        field: &'static str,
    },
}

impl From<rusqlite::Error> for CovenError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error.into())
    }
}

/// A failed database call or a write the database refuses (§5, §8, §14, §16).
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// This handle or a clone has closed the store (§20.1).
    #[error("store is closed")]
    StoreClosed,
    /// SQLite refused or failed a statement, preserving its error.
    #[error(transparent)]
    Sqlite(rusqlite::Error),
    /// SQLite's integrity check or decoding stored facts found damage (§19.1).
    #[error("damaged database")]
    DamagedDatabase,
    /// A synced table declaration or schema is invalid.
    #[error(transparent)]
    Schema(#[from] SchemaError),
    /// App SQL read, changed or defined one of coven's internal objects (§5).
    #[error("app SQL cannot access internal object {table}")]
    InternalTable {
        /// The reserved object.
        table: String,
    },
    /// A local trigger wrote a synced table or a shared trigger a local one (§8.7).
    #[error("trigger {trigger} cannot write table {table}")]
    TriggerTarget {
        /// The trigger doing the write.
        trigger: String,
        /// The target table.
        table: String,
    },
    /// SQLite rolled the owned transaction back itself, so it can't go on (§20.3, §20.13).
    #[error("SQLite ended the owned transaction")]
    TransactionEnded,
    /// App SQL tried transaction control, a PRAGMA, ATTACH, loading an
    /// extension, changing a hidden rowid, or changing the schema outside a
    /// migration (§5, §20.13).
    #[error("app SQL cannot perform {operation}")]
    StatementForbidden {
        /// The refused operation.
        operation: &'static str,
    },
    /// SQLite kept another journal mode instead of WAL (§5).
    #[error("SQLite kept journal mode {mode} instead of WAL")]
    WalUnavailable {
        /// The journal mode SQLite selected.
        mode: String,
    },
    /// The wall clock or next timestamp exceeds its representation (§7.2).
    #[error("clock is outside the timestamp range")]
    ClockOutOfRange,
    /// A value or row exceeds the format (§5), or a write's plaintext or sealed
    /// bytes cannot fit in a SQLite value and its queue row (§6).
    #[error("{field} length {actual} exceeds {maximum}")]
    TooLarge {
        /// The bounded field or collection.
        field: &'static str,
        /// Its actual length.
        actual: u64,
        /// Its maximum length.
        maximum: u64,
    },
    /// Upload attempts must follow this device's queue order (§6).
    #[error("write {write:?} is not the oldest waiting upload")]
    UploadNotOldest {
        /// The write whose upload was requested.
        write: coven_merge::WriteId,
    },
    /// An upload cannot succeed before its bytes have been fixed (§6).
    #[error("write {write:?} has no kept sealed bytes")]
    UploadNotSealed {
        /// The write whose success was reported.
        write: coven_merge::WriteId,
    },
    /// Sync supplied bytes whose length differs from the write's sealed layout.
    #[error("sealed write {write:?} has length {actual}, expected {expected}")]
    UploadLength {
        /// The write being sealed.
        write: coven_merge::WriteId,
        /// Bytes supplied by sync.
        actual: u64,
        /// Bytes required by the format.
        expected: u64,
    },
    /// A reference reaches an audience the source row's readers cannot read (§14.5).
    #[error("reference {table}.{column} at {key:?} reaches another audience")]
    ReferenceAudience {
        /// The referencing table.
        table: String,
        /// Its row's primary key.
        key: crate::RowKey,
        /// The referencing column.
        column: String,
    },
    /// A write targets a circle whose deletion has been applied (§14.7).
    #[error("circle {0} has been deleted")]
    DeletedCircle(coven_foundation::id_source::CircleId),
    /// An inserted row's independent key holds no UUID (§8.5).
    #[error("independent key {key:?} of {table} does not contain a canonical lowercase UUIDv4 or UUIDv7")]
    KeyNotUuid {
        /// The inserted row's table.
        table: String,
        /// Its primary key, in declared key order.
        key: crate::RowKey,
    },
    /// A downloaded write fails the merge's checks and is never applied (§19.1).
    #[error("invalid write {write:?}: {error}")]
    InvalidWrite {
        /// The refused write's device and log number.
        write: coven_merge::WriteId,
        /// The merge rule it broke.
        #[source]
        error: coven_merge::MergeError,
    },
    /// A waiting record or its conversion violates the write format (§20.13).
    #[error(transparent)]
    WriteFormat(coven_format::Error),
    /// A conversion changed or introduced a reference whose generation it cannot know (§20.13).
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
    /// A waiting write is not older than the migration converting it.
    #[error("waiting write {write:?} has schema {schema_version}, not older than migration {migration_version}")]
    MigrationWriteVersion {
        /// The waiting write.
        write: coven_merge::WriteId,
        /// The recorded schema version.
        schema_version: u32,
        /// The migration being applied.
        migration_version: u32,
    },
    /// A waiting upload disappeared while its conversion was being recorded.
    #[error("waiting upload {write:?} disappeared during migration")]
    MigrationUploadMissing {
        /// The missing waiting write.
        write: coven_merge::WriteId,
    },
    /// A write tried to replace a file declared write-once (§16).
    #[error("file on {table} at {key:?} cannot be replaced")]
    FileWriteOnce {
        /// The declared table.
        table: String,
        /// The row's key.
        key: crate::RowKey,
    },
    /// A reference no longer names the row's current file (§16.3).
    #[error("file reference changed for {table} at {key:?}")]
    FileRefChanged {
        /// The referenced table.
        table: String,
        /// The referenced primary key.
        key: crate::RowKey,
    },
    /// The supplied bytes disagree with the row's size (§16).
    #[error("file size is {actual}, expected {expected}")]
    FileSizeMismatch {
        /// The row's declared size.
        expected: u64,
        /// The bytes' measured size.
        actual: u64,
    },
    /// The user's original disappeared.
    #[error("user file is missing: {}", path.display())]
    UserFileMissing {
        /// The original path.
        path: std::path::PathBuf,
    },
    /// The user's original changed after it was observed.
    #[error("user file changed: {}", path.display())]
    UserFileChanged {
        /// The original path.
        path: std::path::PathBuf,
    },
    /// App SQL named a column only coven writes.
    #[error("app SQL cannot write file column {table}.{column}")]
    FileColumnWrite {
        /// The declared table.
        table: String,
        /// The managed column.
        column: String,
    },
    /// A batch supplies the same namespace and file id twice.
    #[error("batch repeats file {namespace}/{id}")]
    FileBatchDuplicate {
        /// The declared namespace.
        namespace: String,
        /// The repeated file id.
        id: String,
    },
    /// A batch names no app-provided file declaration.
    #[error("namespace {namespace} is not app-provided")]
    FileNamespaceNotAppProvided {
        /// The supplied namespace.
        namespace: String,
    },
    /// The id source reused the physical name of a kept file.
    #[error("file name {name:?} is already kept by this device")]
    FileNameReused {
        /// The refused physical file name.
        name: coven_foundation::files::FileName,
    },
    /// No row in the namespace names a supplied file.
    #[error("no row names file {namespace}/{id}")]
    FileUnreferenced {
        /// The supplied namespace.
        namespace: String,
        /// The unreferenced file id.
        id: String,
    },
    /// A file operation targets a table that is not synced.
    #[error("file table {table} is not synced")]
    FileTableNotSynced {
        /// The supplied table.
        table: String,
    },
    /// A synced table has no declared file columns.
    #[error("table {table} does not declare a file")]
    FileNotDeclared {
        /// The supplied table.
        table: String,
    },
    /// Registration or clearing requires a user-provided declaration.
    #[error("table {table} does not declare user-provided files")]
    FileTableNotUserProvided {
        /// The supplied table.
        table: String,
    },
    /// A file lookup has the wrong number of primary-key values.
    #[error("file key of {table} has {actual} values, expected {expected}")]
    FileKeyArity {
        /// The declared table.
        table: String,
        /// The declared key width.
        expected: usize,
        /// The supplied key width.
        actual: usize,
    },
    /// A file lookup supplies NULL for a primary-key column.
    #[error("file key {table}.{column} cannot be NULL")]
    FileKeyNull {
        /// The declared table.
        table: String,
        /// The key column supplied as NULL.
        column: String,
    },
    /// An attached file has no id.
    #[error("attached file column {column} has no id")]
    FileIdMissing {
        /// The declared id column.
        column: String,
    },
    /// The size column is not a nonnegative integer byte count.
    #[error("file size column {column} is not a nonnegative integer: {value:?}")]
    FileSizeInvalid {
        /// The declared size column.
        column: String,
        /// The refused SQL value.
        value: rusqlite::types::Value,
    },
    /// A trigger removed a row while coven filled its file columns.
    #[error("row {table} at {key:?} was removed while attaching its file")]
    FileRowRemoved {
        /// The declared table.
        table: String,
        /// The removed row key.
        key: crate::RowKey,
    },
    /// A row no longer names the file attached during this write.
    #[error("row {table} at {key:?} no longer names its attached file")]
    FileAttachmentChanged {
        /// The declared table.
        table: String,
        /// The changed row key.
        key: crate::RowKey,
    },
    /// Hash and location are neither both NULL nor both set.
    #[error("file hash and location disagree on {table} at {key:?}")]
    FileHashLocationMismatch {
        /// The declared table.
        table: String,
        /// The inconsistent row key.
        key: crate::RowKey,
    },
    /// A write changes an existing file without supplying its bytes.
    #[error("changing the file on {table} at {key:?} requires its bytes")]
    FileBytesRequired {
        /// The declared table.
        table: String,
        /// The changed row key.
        key: crate::RowKey,
    },
    /// The requested row is absent or carries no file.
    #[error("row {table} at {key:?} has no file")]
    FileAbsent {
        /// The requested table.
        table: String,
        /// The requested row key.
        key: crate::RowKey,
    },
    /// Reading or keeping file bytes failed.
    #[error(transparent)]
    Disk(#[from] coven_foundation::files::FileError),
    /// File cleanup failed, retaining the write's outcome and every failure.
    #[error("file cleanup failed: {failures:?}; write outcome: {write:?}")]
    FileCleanup {
        /// Ok if the transaction committed; otherwise its original error.
        write: Result<(), Box<DbError>>,
        /// Each failed byte removal or pending-record update.
        failures: Vec<DbError>,
    },
    /// The supplied checked entry does not have a valid format encoding.
    #[error("invalid store-log entry {entry:?}: {error}")]
    InvalidStoreLogEntry {
        /// The refused entry's position.
        entry: crate::EntryId,
        /// The format check that failed.
        #[source]
        error: coven_format::Error,
    },
    /// Replaying a stale or different set cannot replace the committed result.
    #[error("store-log replay must cover exactly the applied entries and the incoming entry")]
    StoreLogEntriesChanged,
    /// An applied entry's immutable bytes or author-view check differ on repetition.
    #[error("store-log entry {0:?} has different bytes or author-view check")]
    StoreLogEntryChanged(crate::EntryId),
    /// A fixed entry must be published before another is made.
    #[error("store-log entry {0:?} is waiting for publication")]
    StoreLogUploadPending(crate::EntryId),
    /// This device has used every store-log number.
    #[error("store-log entry numbers exhausted")]
    StoreLogNumberExhausted,
    /// Loading plaintext supplied by sync failed without changing the database.
    #[error(transparent)]
    Snapshot(#[from] crate::SnapshotError),
    /// Closing failed for these connections, after every one was tried (§20.1).
    #[error("closing database connections failed: {failures:?}")]
    Closing {
        /// Failures in connection-close order.
        failures: Vec<DbError>,
    },
    /// A transaction failed and rolling it back failed too.
    #[error("{operation}; rollback failed: {rollback}")]
    Rollback {
        /// The operation that required rollback.
        operation: Box<DbError>,
        /// SQLite's rollback failure.
        #[source]
        rollback: rusqlite::Error,
    },
}

impl From<coven_format::Error> for DbError {
    fn from(error: coven_format::Error) -> Self {
        match error {
            coven_format::Error::Limit {
                field,
                actual,
                maximum,
            } => Self::TooLarge {
                field,
                actual: actual as u64,
                maximum: maximum as u64,
            },
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
            error => Self::Sqlite(error),
        }
    }
}

/// A schema rule checked on open and after migrating (§8, §14.1).
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SchemaError {
    /// A local foreign key could prevent deletion of a synced row (§8.4).
    #[error("local reference {table}.{column} must use CASCADE or nullable SET NULL")]
    LocalChildAction {
        /// The local child table.
        table: String,
        /// The referencing column.
        column: String,
    },
    /// A declared synced table isn't in the database (§20.2).
    #[error("declared synced table {table} is missing")]
    MissingTable {
        /// The declared table.
        table: String,
    },
    /// Two declarations name one table (§20.2).
    #[error("synced table {table} is declared twice")]
    DuplicateTable {
        /// The repeated table.
        table: String,
    },
    /// A table declares both `audience_column` and `audience_from` (§20.2).
    #[error("synced table {table} declares two audience sources")]
    TwoAudiences {
        /// The declared table.
        table: String,
    },
    /// Hash and location must represent a row without a file (§16.1).
    #[error("file column {table}.{column} must allow NULL")]
    FileColumnNotNullable {
        /// The declared table.
        table: String,
        /// The non-null managed column.
        column: String,
    },
    /// SET NULL or SET DEFAULT would change one file column alone (§16.1).
    #[error("file column {table}.{column} cannot use SET NULL or SET DEFAULT")]
    FileForeignKeyAction {
        /// The declared table.
        table: String,
        /// The referencing file column.
        column: String,
    },
    /// A declared file column isn't in the table (§20.2).
    #[error("file column {table}.{column} is missing")]
    FileColumn {
        /// The declared table.
        table: String,
        /// The missing column.
        column: String,
    },
    /// A trigger declared shared isn't on the table (§8.7).
    #[error("shared trigger {trigger} is missing from {table}")]
    MissingTrigger {
        /// The declared table.
        table: String,
        /// The missing trigger.
        trigger: String,
    },
    /// A synced table has no primary key (§8.5).
    #[error("synced table {table} has no primary key")]
    NoPrimaryKey {
        /// The declared table.
        table: String,
    },
    /// A primary-key column permits NULL (§8.5).
    #[error("primary-key column {table}.{column} allows NULL")]
    NullableKey {
        /// The synced table.
        table: String,
        /// The nullable key column.
        column: String,
    },
    /// SQLite chooses the primary key itself (§8.5).
    #[error("SQLite generates the primary key of {table}")]
    GeneratedPrimaryKey {
        /// The declared table.
        table: String,
    },
    /// An independent key has no text-affinity column to hold its UUID (§8.5).
    #[error("independent key of {table} has no text-affinity column to hold its UUID")]
    IndependentKeyNotUuid {
        /// The declared table.
        table: String,
    },
    /// Declared key columns do not match the table's primary key (§20.2).
    #[error("declared key columns differ from the primary key of {table}")]
    KeyColumns {
        /// The declared table.
        table: String,
    },
    /// SET NULL or SET DEFAULT would put NULL in a NOT NULL column (§8.4).
    #[error("foreign key action would put NULL in non-null column {table}.{column}")]
    ImpossibleAction {
        /// The synced or local table.
        table: String,
        /// The non-null referencing column.
        column: String,
    },
    /// SET NULL or SET DEFAULT acts on a primary-key column (§8.4).
    #[error("foreign key action changes key column {table}.{column}")]
    PrimaryKeyAction {
        /// The declared table.
        table: String,
        /// The primary-key column.
        column: String,
    },
    /// The audience root column is nullable or is not text (§14, §20.2).
    #[error("audience column {table}.{column} must be non-null text")]
    AudienceColumn {
        /// The declared table.
        table: String,
        /// The audience column.
        column: String,
    },
    /// The audience foreign key spans more than one column (§14.1).
    #[error("audience foreign key of {table} must have one column")]
    AudienceForeignKeyColumns {
        /// The declared table.
        table: String,
    },
    /// The audience foreign key does not point into a synced table (§14.1).
    #[error("audience foreign key {table}.{column} must point into a synced table")]
    AudienceForeignKeyTarget {
        /// The declared table.
        table: String,
        /// The audience foreign-key column.
        column: String,
    },
    /// The audience foreign key uses SET NULL or SET DEFAULT (§14.1).
    #[error("audience foreign key {table}.{column} cannot use SET NULL or SET DEFAULT")]
    AudienceForeignKeyAction {
        /// The declared table.
        table: String,
        /// The audience foreign-key column.
        column: String,
    },
    /// Following audience foreign keys forms a loop (§14.1).
    #[error("audience inheritance from {table} forms a cycle")]
    AudienceCycle {
        /// The table from which the cycle was reached.
        table: String,
    },
    /// A unique constraint or shared key spans audiences (§14.1).
    #[error("constraint {constraint} of {table} spans audiences")]
    AudienceConstraint {
        /// The declared table.
        table: String,
        /// The primary key or unique index.
        constraint: String,
    },
    /// A shared trigger lacks WHEN NOT coven_applying() (§8.7).
    #[error("shared trigger {trigger} on {table} requires WHEN NOT coven_applying()")]
    SharedTriggerGuard {
        /// The declared table.
        table: String,
        /// The shared trigger.
        trigger: String,
    },
}

/// Coven's local tables cannot be used at this version (§17.2, §20.1).
#[derive(Debug, thiserror::Error)]
pub enum CovenMigrationError {
    /// Opening would need to migrate, but this open refuses it.
    #[error("coven's schema requires migration")]
    Pending,
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
