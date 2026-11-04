//! The definitions the synced schema registers: the tables whose rows have
//! owners, and each table's unique constraints and foreign keys by stable id.

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
