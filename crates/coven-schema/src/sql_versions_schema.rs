//! The versions of the app's collations and functions, and of SQLite, the
//! store's indexes were built with.

macro_rules! coven_SQL_versions_tables {
    ($visit:ident) => {
        // The version of each app collation and deterministic function the
        // store's indexes were last built with (§4), under the name in ASCII
        // lowercase, as SQLite compares names; and of the SQLite library that
        // last opened the store for writing, as kind 'library' and name
        // 'sqlite'.
        $visit!(
            _coven_sql_versions,
            "
            CREATE TABLE _coven_sql_versions (
                kind TEXT NOT NULL CHECK(kind IN ('collation','function','library')),
                name TEXT NOT NULL CHECK(name=lower(name)),
                version INTEGER NOT NULL CHECK(version BETWEEN 0 AND 4294967295),
                PRIMARY KEY(kind,name)
            ) STRICT, WITHOUT ROWID;
        "
        );
    };
}
