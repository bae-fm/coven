//! The format and keys each upload's first attempt fixed.

macro_rules! coven_upload_keys_tables {
    ($visit:ident) => {
        // What changes about a queued write after it is queued lives beside
        // its record, never in its row: SQLite rewrites a row's whole record
        // on any update, so setting a field next to the write's plaintext
        // would write all of it again.
        $visit!(_coven_upload_keys, "
            CREATE TABLE _coven_upload_keys (
                device BLOB NOT NULL,
                number BLOB NOT NULL,
                sealing_keys BLOB NOT NULL,
                PRIMARY KEY(device, number),
                FOREIGN KEY(device,number) REFERENCES _coven_uploads(device,number) ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
        ");
    };
}
