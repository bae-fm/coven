//! The schema identities a migration keeps to convert received values.

macro_rules! coven_conversion_schemas_tables {
    ($visit:ident) => {
        $visit!(
            _coven_conversion_schemas,
            "
            CREATE TABLE _coven_conversion_schemas (
                version INTEGER PRIMARY KEY,
                before_schema BLOB NOT NULL,
                after_schema BLOB NOT NULL,
                tables BLOB NOT NULL,
                columns BLOB NOT NULL
            ) STRICT;
        "
        );
    };
}
