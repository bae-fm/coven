//! What protects each file (§17.2, §17.10): the files each synced row and lost
//! value references, the stored bytes each file record names, the files left
//! without a reference until this device's own write deletes their records,
//! and the files each queued write references, kept until it is sent.

macro_rules! coven_file_protection_tables {
    ($visit:ident) => {
        $visit!(_coven_row_file_refs, "
            CREATE TABLE _coven_row_file_refs (
                device BLOB NOT NULL CHECK(length(device)=8),
                object BLOB NOT NULL CHECK(length(object)=16),
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                PRIMARY KEY(device,object,table_name,key)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_row_file_refs_row ON _coven_row_file_refs(table_name,key);
        ");
        // Each stored file's record as the cache keys its bytes, kept with
        // the record (§17.8): what coven's pins and transfer views join a
        // file by. Records over the same stored bytes share an object path.
        $visit!(_coven_stored_files, "
            CREATE TABLE _coven_stored_files (
                file TEXT PRIMARY KEY NOT NULL,
                namespace TEXT NOT NULL,
                device BLOB NOT NULL CHECK(length(device)=8),
                object BLOB NOT NULL CHECK(length(object)=16),
                object_path TEXT NOT NULL,
                hash BLOB NOT NULL CHECK(length(hash)=32)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_stored_files_object ON _coven_stored_files(device,object);
            CREATE INDEX _coven_stored_files_object_path ON _coven_stored_files(namespace,object_path);
        ");
        // Every reference this device has to a file (§17.2): a synced row's
        // merged cell, in any audience and whether or not removal rules took
        // the row out, or a lost value. A file goes with its last one.
        $visit!(_coven_file_references, "
            CREATE TABLE _coven_file_references (
                file TEXT NOT NULL,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                column_name TEXT NOT NULL,
                PRIMARY KEY(file,table_name,key,column_name)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_file_references_row ON _coven_file_references(table_name,key);
        ");
        // Files that writes from other devices left without a reference here
        // (§17.2), until this device's own write deletes their records.
        $visit!(_coven_unreferenced_files, "
            CREATE TABLE _coven_unreferenced_files (
                file TEXT PRIMARY KEY NOT NULL
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_queued_file_refs, "
            CREATE TABLE _coven_queued_file_refs (
                device BLOB NOT NULL CHECK(length(device)=8),
                object BLOB NOT NULL CHECK(length(object)=16),
                writer BLOB NOT NULL CHECK(length(writer)=8),
                number BLOB NOT NULL CHECK(length(number)=8),
                PRIMARY KEY(device,object,writer,number),
                FOREIGN KEY(writer,number) REFERENCES _coven_uploads(device,number) ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_queued_file_refs_write ON _coven_queued_file_refs(writer,number);
        ");
    };
}
