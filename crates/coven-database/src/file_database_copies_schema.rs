//! The folders device-only copies are written into (§17.3).

macro_rules! coven_copy_folders_tables {
    ($visit:ident) => {
        // The folders a call makes on each copy's path, recorded before it
        // makes them. `made` is set once its own mkdir made the folder: one
        // that was there already is someone else's. A made folder goes when
        // empty with the last copy it holds.
        $visit!(
            _coven_copy_folders,
            "
            CREATE TABLE _coven_copy_folders (
                folder TEXT NOT NULL,
                copy TEXT NOT NULL REFERENCES _coven_copies(path) ON DELETE CASCADE,
                made INTEGER NOT NULL DEFAULT 0 CHECK(made IN (0,1)),
                PRIMARY KEY(folder,copy)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_copy_folders_copy ON _coven_copy_folders(copy);
        "
        );
    };
}
