//! The uploads this device queues for its files, with each one's chunk hashes
//! and the files it switches to the cloud; coven's copies of app bytes on this
//! device only, which an upload sends; and the app's upload pause and its
//! unmetered-only setting (§17.3, §17.9).

macro_rules! coven_file_uploads_tables {
    ($visit:ident) => {
        // Coven's copy of each app file on this device only (§17.3), by
        // file, with the content hash of its bytes. Files over the same bytes
        // share one copy, and its chunk hashes, which a move's upload checks.
        $visit!(_coven_device_files, "
            CREATE TABLE _coven_device_files (
                file TEXT PRIMARY KEY NOT NULL,
                path TEXT NOT NULL,
                hash BLOB NOT NULL CHECK(length(hash)=32)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_device_files_path ON _coven_device_files(path);
            CREATE TRIGGER _coven_device_file_unremoved BEFORE INSERT ON _coven_device_files BEGIN
                SELECT RAISE(ABORT,'a file recorded for removal is named by a row')
                WHERE EXISTS(SELECT 1 FROM _coven_file_removals WHERE path=NEW.path);
            END;
        ");
        $visit!(_coven_copy_chunks, "
            CREATE TABLE _coven_copy_chunks (
                path TEXT NOT NULL,
                chunk INTEGER NOT NULL CHECK(chunk>=0),
                hash BLOB NOT NULL CHECK(length(hash)=32),
                PRIMARY KEY(path,chunk)
            ) STRICT, WITHOUT ROWID;
        ");
        // One stored object this device uploads, with the location, size,
        // content hash and source its staging fixed (§17.9): the user's
        // original, or coven's copy of app bytes. A bound upload holds the
        // number of the first write it holds back, set as it is queued: the
        // write that staged its file, or the first write another queued
        // bound upload holds. That write and every later one wait while a
        // file waits for the upload (§17.4).
        $visit!(_coven_file_uploads, "
            CREATE TABLE _coven_file_uploads (
                id INTEGER PRIMARY KEY,
                location TEXT NOT NULL UNIQUE,
                device BLOB NOT NULL CHECK(length(device)=8),
                file BLOB NOT NULL CHECK(length(file)=16),
                size INTEGER NOT NULL CHECK(size>=0),
                hash BLOB NOT NULL CHECK(length(hash)=32),
                original BLOB,
                modified_at BLOB CHECK(modified_at IS NULL OR length(modified_at)=13),
                copy TEXT,
                move INTEGER NOT NULL REFERENCES _coven_moves(id),
                stored INTEGER NOT NULL DEFAULT 0 CHECK(stored IN (0,1)),
                holds BLOB CHECK(holds IS NULL OR (length(holds)=8 AND holds>x'0000000000000000')),
                queued_at BLOB NOT NULL,
                session BLOB,
                bytes_confirmed INTEGER CHECK(bytes_confirmed>=0),
                bytes_total INTEGER CHECK(bytes_total>=bytes_confirmed),
                UNIQUE(device,file),
                CHECK((original IS NULL)=(modified_at IS NULL)),
                CHECK((original IS NULL)!=(copy IS NULL)),
                CHECK((session IS NULL)=(bytes_confirmed IS NULL)
                    AND (session IS NULL)=(bytes_total IS NULL))
            ) STRICT;
            CREATE INDEX _coven_file_uploads_copy ON _coven_file_uploads(copy) WHERE copy IS NOT NULL;
            CREATE INDEX _coven_file_uploads_move ON _coven_file_uploads(move);
            CREATE INDEX _coven_file_uploads_holds ON _coven_file_uploads(holds) WHERE holds IS NOT NULL;
            CREATE TRIGGER _coven_upload_copy_unremoved BEFORE INSERT ON _coven_file_uploads
            WHEN NEW.copy IS NOT NULL BEGIN
                SELECT RAISE(ABORT,'a file recorded for removal is named by a row')
                WHERE EXISTS(SELECT 1 FROM _coven_file_removals WHERE path=NEW.copy);
            END;
        ");
        // The files an upload's completion switches to the cloud: the one its
        // write staged, and each a move gave the moved rows (§17.2).
        $visit!(_coven_upload_files, "
            CREATE TABLE _coven_upload_files (
                file TEXT PRIMARY KEY NOT NULL,
                upload INTEGER NOT NULL REFERENCES _coven_file_uploads(id),
                keep INTEGER NOT NULL DEFAULT 0 CHECK(keep IN (0,1))
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_upload_files_upload ON _coven_upload_files(upload);
        ");
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
        // Present while the app has paused uploads (E7).
        $visit!(
            _coven_upload_pause,
            "
            CREATE TABLE _coven_upload_pause (
                paused INTEGER PRIMARY KEY CHECK(paused=1)
            ) STRICT;
        "
        );
        // Present while the app moves files only on unmetered networks (E7).
        $visit!(
            _coven_unmetered_only,
            "
            CREATE TABLE _coven_unmetered_only (
                unmetered_only INTEGER PRIMARY KEY CHECK(unmetered_only=1)
            ) STRICT;
        "
        );
    };
}
