//! The local captured prefix a raise publishes.

macro_rules! coven_migration_prefix_tables {
    ($visit:ident) => {
        $visit!(
            _coven_migration_prefix,
            "
            CREATE TABLE _coven_migration_prefix (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                device BLOB NOT NULL CHECK(length(device)=8),
                through BLOB NOT NULL CHECK(length(through)=8),
                version INTEGER NOT NULL
            ) STRICT;
        "
        );
    };
}
