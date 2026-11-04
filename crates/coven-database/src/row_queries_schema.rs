//! The definitions the synced schema registers: the tables whose rows have
//! owners, and each table's columns, unique constraints and foreign keys by
//! stable id.

macro_rules! coven_schema_definitions_tables {
    ($visit:ident) => {
        $visit!(
            _coven_owner_tables,
            "
            CREATE TABLE _coven_owner_tables (
                table_name TEXT PRIMARY KEY NOT NULL
            ) STRICT, WITHOUT ROWID;
        "
        );
        // A synced column, or a column a breaking migration dropped, which
        // has a row only while a frozen lost value names it (§18.7): the
        // deletion or move of the last one deletes it.
        $visit!(
            _coven_columns,
            "
            CREATE TABLE _coven_columns (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                synced INTEGER NOT NULL CHECK(synced IN (0,1)),
                UNIQUE(table_name, column_name)
            ) STRICT;
        "
        );
        $visit!(
            _coven_constraints,
            "
            CREATE TABLE _coven_constraints (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                identity BLOB NOT NULL,
                UNIQUE(table_name, identity)
            ) STRICT;
        "
        );
        $visit!(
            _coven_foreign_keys,
            "
            CREATE TABLE _coven_foreign_keys (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                identity BLOB NOT NULL,
                UNIQUE(table_name, identity)
            ) STRICT;
        "
        );
    };
}

/// The deletion or move of the last frozen lost value naming a dropped
/// column deletes the column's row (§18.7). Created after the losses' table
/// the triggers are on.
pub(crate) const COLUMN_RELEASE: &str = "
    CREATE TRIGGER _coven_lost_column_deleted AFTER DELETE ON _coven_lost
    WHEN OLD.column_id IS NOT NULL BEGIN
        DELETE FROM _coven_columns WHERE id=OLD.column_id AND synced=0
            AND NOT EXISTS(SELECT 1 FROM _coven_lost WHERE column_id=OLD.column_id);
    END;
    CREATE TRIGGER _coven_lost_column_moved AFTER UPDATE OF column_id ON _coven_lost
    WHEN OLD.column_id IS NOT NULL AND OLD.column_id IS NOT NEW.column_id BEGIN
        DELETE FROM _coven_columns WHERE id=OLD.column_id AND synced=0
            AND NOT EXISTS(SELECT 1 FROM _coven_lost WHERE column_id=OLD.column_id);
    END;
";
