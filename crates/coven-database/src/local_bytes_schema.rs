//! The bytes the store's own files take on this device, apart from the
//! cache, kept as tables the app's SQL reads and sums with
//! `coven_cache_bytes` (§17.8).

macro_rules! coven_local_bytes_tables {
    ($visit:ident) => {
        // The store's own files by part, each row holding the part's size in
        // bytes: `database` is the store's database file and `inputs` the
        // input database's (§4), each as its last checkpoint left it: coven
        // writes each row as the store opens and after each checkpoint that
        // changes it. Neither row counts its WAL, which a checkpoint lets
        // start over.
        // `copies` is coven's copies of app bytes that a row names: the
        // triggers below add or subtract a copy's size in the statement that
        // counts or releases the copy. coven keeps a `rebuild` row while a
        // rebuild's folder exists (§18.3), and the row holds the total size
        // of the rebuild's store's own files.
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
        // name, with the size of everything in it: coven writes the row as
        // the store opens after the swap that kept the archive, and deletes
        // it in the step that moves the archive out of the directory to
        // delete it.
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
