//! The copies coven writes into folders the person picked to make cloud files
//! device-only, and the folders it makes for them (§17.3).

macro_rules! coven_device_only_copies_tables {
    ($visit:ident) => {
        // Each copy coven writes into a folder the person picked, to make a
        // cloud file device-only (§17.3), recorded before any of its bytes
        // exist: its path, the temporary name its bytes are written under,
        // and where it stands. `writing` and `placed` copies belong to a call
        // that hasn't switched its files; `kept` ones are originals this
        // device records; a `lost` one's switch lost to another device's, and
        // it goes if it is as coven left it (Appendix C.3). A copy's path is
        // valid Unicode, and the original recorded at it holds that text's
        // UTF-8 bytes on every platform, so the two compare through a CAST.
        $visit!(
            _coven_copies,
            "
            CREATE TABLE _coven_copies (
                path TEXT PRIMARY KEY NOT NULL,
                temporary TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL CHECK(state IN ('writing','placed','kept','lost')),
                size INTEGER CHECK(size>=0),
                modified_at BLOB CHECK(modified_at IS NULL OR length(modified_at)=13),
                CHECK((state='writing')=(size IS NULL)),
                CHECK((size IS NULL)=(modified_at IS NULL))
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_copies_state ON _coven_copies(state);
            -- A kept copy is forgotten with the last original recorded at
            -- its path: the file stays, the person's (§17.3).
            CREATE TRIGGER _coven_copy_forgotten AFTER DELETE ON _coven_user_files
            WHEN NOT EXISTS(SELECT 1 FROM _coven_user_files WHERE path=OLD.path)
            BEGIN
                DELETE FROM _coven_copies WHERE path=CAST(OLD.path AS TEXT) AND state='kept';
            END;
        "
        );
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
