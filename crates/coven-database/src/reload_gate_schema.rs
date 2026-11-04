//! The audiences whose replacement data has not committed.

macro_rules! coven_data_reloads_tables {
    ($visit:ident) => {
        $visit!(
            _coven_data_reloads,
            "
            CREATE TABLE _coven_data_reloads (
                audience TEXT PRIMARY KEY NOT NULL,
                subject BLOB NOT NULL
            ) STRICT, WITHOUT ROWID;
        "
        );
    };
}
