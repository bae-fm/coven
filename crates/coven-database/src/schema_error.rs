//! The schema rules checked when a store opens and after it migrates.

/// A schema rule checked on open and after migrating (§9, §15.1).
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SchemaError {
    /// Two namespace declarations share a name (E1).
    #[error("file namespace {namespace} is declared twice")]
    DuplicateNamespace {
        /// The namespace.
        namespace: String,
    },
    /// A synced foreign key does not reference the complete parent primary key (§9.1).
    #[error("foreign key {table}: {key} must reference the parent's primary key")]
    ForeignKeyNotPrimaryKey {
        /// The referencing synced table.
        table: String,
        /// The foreign key's columns and referenced table and columns, as SQL.
        key: String,
    },
    /// SET DEFAULT is on a synced table or references one (§9.1).
    #[error("foreign key {table}: {key} cannot use SET DEFAULT")]
    SetDefault {
        /// The referencing table.
        table: String,
        /// The foreign key's columns and referenced table and columns, as SQL.
        key: String,
    },
    /// A local foreign key could prevent deletion of a synced row (§9.1).
    #[error("local reference {table}.{column} must use CASCADE or nullable SET NULL")]
    LocalChildAction {
        /// The local child table.
        table: String,
        /// The referencing column.
        column: String,
    },
    /// A local foreign key could stop coven updating a synced value in place (§9.1).
    #[error("local reference {table}.{column} must use ON UPDATE CASCADE or SET NULL")]
    LocalChildUpdate {
        /// The local child table.
        table: String,
        /// The referencing column.
        column: String,
    },
    /// A declared synced table isn't in the database (E2).
    #[error("declared synced table {table} is missing")]
    MissingTable {
        /// The declared table.
        table: String,
    },
    /// Two declarations name one table (E2).
    #[error("synced table {table} is declared twice")]
    DuplicateTable {
        /// The repeated table.
        table: String,
    },
    /// A synced table's `owner_only` declaration changed without a breaking
    /// migration to raise the schema (§18.3).
    #[error("synced table {table} changed its owner_only declaration without a migration")]
    OwnerOnlyChanged {
        /// The table whose declaration changed.
        table: String,
    },
    /// A table declares both `audience_column` and `audience_from` (E2).
    #[error("synced table {table} declares two audience sources")]
    TwoAudiences {
        /// The declared table.
        table: String,
    },
    /// A synced table's file reference columns changed without a breaking
    /// migration to raise the schema (§18.1).
    #[error("synced table {table} changed its file columns without a migration")]
    FileColumnsChanged {
        /// The table whose declaration changed.
        table: String,
    },
    /// A column is declared a file reference twice (E2).
    #[error("file column {table}.{column} is declared twice")]
    FileColumnRepeated {
        /// The declared table.
        table: String,
        /// The repeated column.
        column: String,
    },
    /// A file reference column is generated, so coven can't write it (E2).
    #[error("file column {table}.{column} is generated")]
    FileColumnGenerated {
        /// The declared table.
        table: String,
        /// The generated column.
        column: String,
    },
    /// An SQL foreign key references coven's files table, which only
    /// declared file columns reference (§17.2, E2). `key` names the
    /// referencing columns and referenced table and columns as SQL.
    #[error("{table}'s foreign key {key} references coven_files; declare a file column instead")]
    FilesForeignKey {
        /// The table holding the key.
        table: String,
        /// The key, as SQL.
        key: String,
    },
    /// A file reference column is also an SQL foreign key (E2).
    #[error("file column {table}.{column} is also a foreign key")]
    FileColumnForeignKey {
        /// The declared table.
        table: String,
        /// The column.
        column: String,
    },
    /// A declared file column isn't in the table (E2).
    #[error("file column {table}.{column} is missing")]
    FileColumn {
        /// The declared table.
        table: String,
        /// The missing column.
        column: String,
    },
    /// A trigger declared shared isn't on the table (§9.4).
    #[error("shared trigger {trigger} is missing from {table}")]
    MissingTrigger {
        /// The declared table.
        table: String,
        /// The missing trigger.
        trigger: String,
    },
    /// A synced table has no primary key (§9.2).
    #[error("synced table {table} has no primary key")]
    NoPrimaryKey {
        /// The declared table.
        table: String,
    },
    /// A primary-key column permits NULL (§9.2).
    #[error("primary-key column {table}.{column} allows NULL")]
    NullableKey {
        /// The synced table.
        table: String,
        /// The nullable key column.
        column: String,
    },
    /// A synced key column compares text by a collation other than BINARY (§9.2, D3).
    #[error(
        "key column {table}.{column} uses collation {collation}; synced keys compare text by bytes"
    )]
    KeyCollation {
        /// The table holding the key, unique constraint or foreign key.
        table: String,
        /// The column, or for an expression term the unique index naming it.
        column: String,
        /// The column's or index term's collation.
        collation: String,
    },
    /// SQLite chooses the primary key itself (§9.2).
    #[error("SQLite generates the primary key of {table}")]
    GeneratedPrimaryKey {
        /// The declared table.
        table: String,
    },
    /// An independent key has no text-affinity column to hold its UUID (§9.2).
    #[error("independent key of {table} has no text-affinity column to hold its UUID")]
    IndependentKeyNotUuid {
        /// The declared table.
        table: String,
    },
    /// Declared key columns do not match the table's primary key (E2).
    #[error("declared key columns differ from the primary key of {table}")]
    KeyColumns {
        /// The declared table.
        table: String,
    },
    /// A foreign key includes a generated column (§9.1).
    #[error("foreign key includes generated column {table}.{column}")]
    GeneratedForeignKey {
        /// The synced or local table.
        table: String,
        /// The generated referencing column.
        column: String,
    },
    /// SET NULL would put NULL in a NOT NULL column (§9.1).
    #[error("foreign key action would put NULL in non-null column {table}.{column}")]
    ImpossibleAction {
        /// The synced or local table.
        table: String,
        /// The non-null referencing column.
        column: String,
    },
    /// SET NULL acts on a primary-key column (§9.1).
    #[error("foreign key action changes key column {table}.{column}")]
    PrimaryKeyAction {
        /// The declared table.
        table: String,
        /// The primary-key column.
        column: String,
    },
    /// The audience root column is nullable or is not text (§15, E2).
    #[error("audience column {table}.{column} must be non-null text")]
    AudienceColumn {
        /// The declared table.
        table: String,
        /// The audience column.
        column: String,
    },
    /// The audience foreign key spans more than one column (§15.1).
    #[error("audience foreign key of {table} must have one column")]
    AudienceForeignKeyColumns {
        /// The declared table.
        table: String,
    },
    /// The audience foreign key does not point into a synced table (§15.1).
    #[error("audience foreign key {table}.{column} must point into a synced table")]
    AudienceForeignKeyTarget {
        /// The declared table.
        table: String,
        /// The audience foreign-key column.
        column: String,
    },
    /// The audience foreign key uses SET NULL (§15.1).
    #[error("audience foreign key {table}.{column} cannot use SET NULL")]
    AudienceForeignKeyAction {
        /// The declared table.
        table: String,
        /// The audience foreign-key column.
        column: String,
    },
    /// Following audience foreign keys forms a loop (§15.1).
    #[error("audience inheritance from {table} forms a cycle")]
    AudienceCycle {
        /// The table from which the cycle was reached.
        table: String,
    },
    /// A unique constraint or shared key spans audiences (§15.2).
    #[error("constraint {constraint} of {table} spans audiences")]
    AudienceConstraint {
        /// The declared table.
        table: String,
        /// The primary key or unique index.
        constraint: String,
    },
    /// A shared trigger lacks WHEN NOT coven_applying() (§9.4).
    #[error("shared trigger {trigger} on {table} requires WHEN NOT coven_applying()")]
    SharedTriggerGuard {
        /// The declared table.
        table: String,
        /// The shared trigger.
        trigger: String,
    },
    /// A trigger on `coven_files`, `coven_pins` or `coven_upload_queue` runs
    /// before the change, where it could skip or refuse coven's own (§9.4).
    #[error("trigger {trigger} on {table} must run AFTER the change")]
    TriggerTiming {
        /// The trigger.
        trigger: String,
        /// The table of coven's it follows.
        table: String,
    },
}
