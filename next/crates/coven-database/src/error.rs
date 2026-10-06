//! Typed opening, schema, authorization and migration failures.

use coven_foundation::files::{SettingsError, StoreLockError};

/// The result of opening or using this database.
pub type CovenResult<T> = Result<T, CovenError>;

/// Failures of opening, reading and writing a store (§5, §20.1, §20.3).
#[derive(Debug, thiserror::Error)]
pub enum CovenError {
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

/// A failed database call or a write the database refuses (§5, §8, §14, §16).
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// This handle or a clone has closed the store (§20.1).
    #[error("store is closed")]
    StoreClosed,
    /// SQLite refused or failed a statement, preserving its error.
    #[error(transparent)]
    Sqlite(rusqlite::Error),
    /// SQLite's opening integrity check found damage (§19.1).
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
    /// extension, or changing the schema outside a migration (§5, §20.13).
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
    /// A value or write exceeds the format's bounds (§5).
    #[error("{field} length {actual} exceeds {maximum}")]
    TooLarge {
        /// The bounded field or collection.
        field: &'static str,
        /// Its actual length.
        actual: usize,
        /// Its maximum length.
        maximum: usize,
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
    /// An independent key does not contain a UUID (§8.5).
    #[error("independent key of {table} does not contain a canonical UUIDv4 or UUIDv7")]
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
