//! The files each queued write references, kept until it is sent (§17.10).

macro_rules! coven_queued_file_references_tables {
    ($visit:ident) => {
        $visit!(_coven_queued_file_refs, "
            CREATE TABLE _coven_queued_file_refs (
                device BLOB NOT NULL CHECK(length(device)=8),
                file BLOB NOT NULL CHECK(length(file)=16),
                writer BLOB NOT NULL CHECK(length(writer)=8),
                number BLOB NOT NULL CHECK(length(number)=8),
                PRIMARY KEY(device,file,writer,number),
                FOREIGN KEY(writer,number) REFERENCES _coven_uploads(device,number) ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_queued_file_refs_write ON _coven_queued_file_refs(writer,number);
        ");
    };
}
