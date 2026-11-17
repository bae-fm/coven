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
        // `copies` is coven's copies of app bytes that a row names, which
        // the triggers below keep in the statement that counts or releases a
        // copy. `rebuild` is there while a rebuild's folder is (§18.3): the
        // sizes of the rebuild's store's own files together.
        $visit!(
            coven_library_bytes,
            "
            CREATE TABLE coven_library_bytes (
                part TEXT PRIMARY KEY NOT NULL CHECK(part IN ('database','inputs','copies','rebuild')),
                bytes INTEGER NOT NULL CHECK(bytes>=0)
            ) STRICT, WITHOUT ROWID;
            INSERT INTO coven_library_bytes(part,bytes)
                VALUES('database',0),('inputs',0),('copies',0);
            CREATE TRIGGER _coven_copy_counted AFTER INSERT ON _coven_copy_sizes BEGIN
                UPDATE coven_library_bytes SET bytes=bytes+NEW.size WHERE part='copies';
            END;
            CREATE TRIGGER _coven_copy_released AFTER DELETE ON _coven_copy_sizes BEGIN
                UPDATE coven_library_bytes SET bytes=bytes-OLD.size WHERE part='copies';
            END;
        "
        );
        // Each archive a reset kept in the store's directory (§19.3), by
        // name, with the size of everything in it: written as the store
        // opens after the swap that kept the archive, and deleted in the
        // step that moves the archive out of the directory to remove it.
        $visit!(
            coven_archive_bytes,
            "
            CREATE TABLE coven_archive_bytes (
                archive TEXT PRIMARY KEY NOT NULL,
                bytes INTEGER NOT NULL CHECK(bytes>=0)
            ) STRICT, WITHOUT ROWID;
        "
        );
    };
}
