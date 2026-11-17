//! The bytes the store's own files take on this device, apart from the
//! cache, kept as tables the app's SQL reads and sums with
//! `coven_cache_bytes` (§17.8).

macro_rules! coven_local_bytes_tables {
    ($visit:ident) => {
        // The store's own files by part, each row holding the part's size in
        // bytes: `database` is the store's database file and `inputs` the
        // input database's (§4), each as its last checkpoint left it,
        // written after each checkpoint that changes it and as the store
        // opens. Neither counts its WAL, which a checkpoint lets start over.
        $visit!(
            coven_library_bytes,
            "
            CREATE TABLE coven_library_bytes (
                part TEXT PRIMARY KEY NOT NULL CHECK(part IN ('database','inputs')),
                bytes INTEGER NOT NULL CHECK(bytes>=0)
            ) STRICT, WITHOUT ROWID;
            INSERT INTO coven_library_bytes(part,bytes) VALUES('database',0),('inputs',0);
        "
        );
    };
}
