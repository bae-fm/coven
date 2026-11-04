//! The user's originals this device has, with the chunk hashes of each while
//! its file may still move to the cloud (§17.3).

macro_rules! coven_user_originals_tables {
    ($visit:ident) => {
        // The user's originals this device has, by file, while their
        // records exist here: the path, as the operating system's bytes
        // (`path_bytes`), size, modification time and content hash recorded
        // when each was staged, or placed as a copy (§17.3).
        $visit!(
            _coven_user_files,
            "
            CREATE TABLE _coven_user_files (
                file TEXT PRIMARY KEY NOT NULL,
                path BLOB NOT NULL,
                size INTEGER NOT NULL CHECK(size>=0),
                modified_at BLOB NOT NULL CHECK(length(modified_at)=13),
                hash BLOB NOT NULL CHECK(length(hash)=32)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_user_files_path ON _coven_user_files(path);
        "
        );
        // The plaintext chunk hashes of each original recorded above, from
        // the same read as its content hash, while its file may still move
        // to the cloud: a move's upload checks the bytes it sends against
        // them. They go when the file's record switches to stored bytes, or
        // with the original's row (§17.3).
        $visit!(
            _coven_user_chunks,
            "
            CREATE TABLE _coven_user_chunks (
                file TEXT NOT NULL REFERENCES _coven_user_files(file) ON DELETE CASCADE,
                chunk INTEGER NOT NULL CHECK(chunk>=0),
                hash BLOB NOT NULL CHECK(length(hash)=32),
                PRIMARY KEY(file,chunk)
            ) STRICT, WITHOUT ROWID;
        "
        );
    };
}
