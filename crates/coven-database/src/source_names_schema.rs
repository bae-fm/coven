//! The schema identities that validate original sources across renames.

macro_rules! coven_source_names_tables {
    ($visit:ident) => {
        $visit!(_coven_table_origins, "
            CREATE TABLE _coven_table_origins (
                version INTEGER NOT NULL,
                name TEXT NOT NULL,
                origin_version INTEGER NOT NULL,
                origin_name TEXT NOT NULL,
                PRIMARY KEY(version,name)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_table_origin ON _coven_table_origins(version,origin_version,origin_name);
        ");
        $visit!(_coven_column_origins, "
            CREATE TABLE _coven_column_origins (
                version INTEGER NOT NULL,
                table_name TEXT NOT NULL,
                name TEXT NOT NULL,
                origin_version INTEGER NOT NULL,
                origin_table TEXT NOT NULL,
                origin_name TEXT NOT NULL,
                PRIMARY KEY(version,table_name,name)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_column_origin ON _coven_column_origins(version,table_name,origin_version,origin_table,origin_name);
        ");
    };
}
