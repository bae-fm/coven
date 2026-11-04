//! Each row's selected original creation (§9.6), kept apart from merge state
//! and dismissible losses.

macro_rules! coven_ownership_tables {
    ($visit:ident) => {
        $visit!(
            _coven_creations,
            "
            CREATE TABLE _coven_creations (
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL,
                write_id BLOB NOT NULL CHECK(length(write_id)=16),
                creator BLOB NOT NULL,
                PRIMARY KEY(table_name,key,audience)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_creations_audience ON _coven_creations(audience,table_name,key);
        "
        );
        coven_authorship_tables!($visit);
        coven_schema_definitions_tables!($visit);
    };
}
