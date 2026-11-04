//! Each queued file's chunk hashes (§17.9).

macro_rules! coven_file_chunks_tables {
    ($visit:ident) => {
        $visit!(
            _coven_file_chunks,
            "
            CREATE TABLE _coven_file_chunks (
                device BLOB NOT NULL,
                file BLOB NOT NULL,
                chunk INTEGER NOT NULL CHECK(chunk>=0),
                hash BLOB NOT NULL CHECK(length(hash)=32),
                PRIMARY KEY(device,file,chunk),
                FOREIGN KEY(device,file) REFERENCES _coven_file_uploads(device,file)
                    ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
        "
        );
    };
}
