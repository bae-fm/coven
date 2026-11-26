//! Each provider account's intended and confirmed access, and its unfinished
//! request (§14.2).

macro_rules! coven_provider_access_tables {
    ($visit:ident) => {
        $visit!(
            _coven_provider_access,
            "
            CREATE TABLE _coven_provider_access (
                account TEXT PRIMARY KEY NOT NULL,
                intended INTEGER NOT NULL CHECK(intended IN (0,1)),
                granted INTEGER CHECK(granted IN (0,1)),
                invitation BLOB,
                operation INTEGER UNIQUE REFERENCES _coven_operations(id),
                target INTEGER CHECK(target IN (0,1)),
                CHECK(target IS NULL OR operation IS NOT NULL)
            ) STRICT, WITHOUT ROWID;
        "
        );
    };
}
