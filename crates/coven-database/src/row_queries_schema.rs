//! The synced tables whose rows have owners.

macro_rules! coven_owner_tables_tables {
    ($visit:ident) => {
        $visit!(
            _coven_owner_tables,
            "
            CREATE TABLE _coven_owner_tables (
                table_name TEXT PRIMARY KEY NOT NULL
            ) STRICT, WITHOUT ROWID;
        "
        );
    };
}
