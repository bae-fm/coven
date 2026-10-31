//! Coven's local schema. `application_id` identifies these tables independently
//! of the application's `user_version`.
//!
//! Unsigned merge counters are big-endian eight-byte blobs. SQLite's signed
//! integer range must not truncate device ids, generations or write numbers.
//! Present displayed values live in app tables; a written value that reads
//! differently (a NULL-substituted reference, a generated value recomputed from
//! merged inputs) is kept by cell. Metadata and lost values use coven-format's snapshot
//! field encodings inside blobs.
//! Loss targets name their row directly: a write excluded by a schema change
//! (§18.6) need not have an accepted generation in `_coven_rows`.
//! An upload's `record` is one plaintext value: the write header frame followed
//! by its audience parts' row-frame streams. Its first attempt fixes the format
//! and key ids in `sealing_keys`, encoded as the D9 write prefix. Each attempted
//! slot keeps its exact ciphertext in the claim journal before storage is called.
//! `_coven_uploads` retains its rowid for queue order and incremental BLOB reads.
//! Cell keys start with the row; column-wide cell edits occur only in migrations.
//! Original-change admission is NULL until judged here, then this device's
//! `ChangeVerdict`. Consuming a slot does not establish admission.
//! Write-id indexes serve SQLite's incoming foreign-key checks on INSERT with
//! RETURNING, which EXPLAIN QUERY PLAN includes alongside explicit lookups.

pub(crate) const VERSION: u32 = 1;
pub(crate) const CACHE_HEADER: i64 = -1;
pub(crate) const CACHE_COMPLETE_FILE: i64 = -2;
// D2 timestamps contain six clock bytes, two counter bytes, then the device id.
pub(crate) const WRITE_DEVICE_SQL: &str = "substr(timestamp,9,8)";

/// App objects cannot use either reserved namespace (Appendix C.1).
pub(crate) fn reserved_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.starts_with("_coven_") || name.starts_with("coven_")
}

// This is also the ownership checker's source of reserved table names.
#[macro_use]
#[path = "internal_schema_ownership.rs"]
mod ownership;
#[macro_use]
#[path = "internal_schema_sequence.rs"]
mod sequence;

macro_rules! coven_tables {
    ($visit:ident) => {
        $visit!(_coven_pending, "
            CREATE TABLE _coven_pending (
                id INTEGER PRIMARY KEY,
                subject BLOB NOT NULL UNIQUE,
                value BLOB NOT NULL,
                operation INTEGER REFERENCES _coven_operations(id),
                object BLOB CHECK(object IS NULL OR length(object)=9),
                work TEXT CHECK(work IN ('upload','download')),
                device BLOB CHECK(device IS NULL OR length(device)=8),
                file BLOB CHECK(file IS NULL OR length(file)=16),
                coven_version TEXT NOT NULL,
                move INTEGER REFERENCES _coven_moves(id),
                pin TEXT REFERENCES _coven_pin_requests(file) ON DELETE CASCADE,
                CHECK((device IS NULL)=(file IS NULL)),
                CHECK((work IS NULL)=(device IS NULL))
            ) STRICT;
            CREATE INDEX _coven_pending_object ON _coven_pending(object);
            CREATE INDEX _coven_pending_file ON _coven_pending(device,file) WHERE device IS NOT NULL;
            CREATE UNIQUE INDEX _coven_pending_operation ON _coven_pending(operation) WHERE operation IS NOT NULL;
            CREATE UNIQUE INDEX _coven_pending_move ON _coven_pending(move) WHERE move IS NOT NULL;
            CREATE UNIQUE INDEX _coven_pending_pin ON _coven_pending(pin) WHERE pin IS NOT NULL;
        ");
        $visit!(_coven_data_reloads, "
            CREATE TABLE _coven_data_reloads (
                audience TEXT PRIMARY KEY NOT NULL,
                subject BLOB NOT NULL
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_row_file_refs, "
            CREATE TABLE _coven_row_file_refs (
                device BLOB NOT NULL CHECK(length(device)=8),
                file BLOB NOT NULL CHECK(length(file)=16),
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                PRIMARY KEY(device,file,table_name,key)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_row_file_refs_row ON _coven_row_file_refs(table_name,key);
        ");
        // Each stored file's record as the cache keys its bytes, kept with
        // the record (§17.8): what coven's pins and transfer views join a
        // file by. Records over the same stored bytes share a cache id.
        $visit!(_coven_stored_files, "
            CREATE TABLE _coven_stored_files (
                file TEXT PRIMARY KEY NOT NULL,
                namespace TEXT NOT NULL,
                device BLOB NOT NULL CHECK(length(device)=8),
                object BLOB NOT NULL CHECK(length(object)=16),
                cache_id TEXT NOT NULL,
                hash BLOB NOT NULL CHECK(length(hash)=32)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_stored_files_object ON _coven_stored_files(device,object);
            CREATE INDEX _coven_stored_files_cache ON _coven_stored_files(namespace,cache_id);
            -- A download's record ends with the last file over its bytes,
            -- in the transaction that removes it (§19.1).
            CREATE TRIGGER _coven_stored_files_removed AFTER DELETE ON _coven_stored_files
            WHEN NOT EXISTS(SELECT 1 FROM _coven_stored_files AS s WHERE s.device=OLD.device AND s.object=OLD.object)
            BEGIN
                DELETE FROM _coven_pending WHERE work='download' AND device=OLD.device AND file=OLD.object;
            END;
            CREATE TRIGGER _coven_stored_files_moved AFTER UPDATE OF device,object ON _coven_stored_files
            WHEN NOT EXISTS(SELECT 1 FROM _coven_stored_files AS s WHERE s.device=OLD.device AND s.object=OLD.object)
            BEGIN
                DELETE FROM _coven_pending WHERE work='download' AND device=OLD.device AND file=OLD.object;
            END;
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
        // The app's file reference columns as last registered, which a later
        // open or migration compares against (§18.1).
        $visit!(_coven_file_columns, "
            CREATE TABLE _coven_file_columns (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                write_once INTEGER NOT NULL CHECK(write_once IN (0,1)),
                PRIMARY KEY(table_name,column_name)
            ) STRICT, WITHOUT ROWID;
        ");
        // Files that writes from other devices left without a reference here
        // (§17.2), until this device's own write deletes their records.
        $visit!(_coven_unreferenced_files, "
            CREATE TABLE _coven_unreferenced_files (
                file TEXT PRIMARY KEY NOT NULL
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_device_reset, "
            CREATE TABLE _coven_device_reset (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                old_device BLOB NOT NULL CHECK(length(old_device)=8),
                new_device BLOB NOT NULL CHECK(length(new_device)=8)
            ) STRICT;
        ");
        // The version of each app collation and deterministic function the
        // store's indexes were last built with (§4), under the name in ASCII
        // lowercase, as SQLite compares names.
        $visit!(_coven_sql_versions, "
            CREATE TABLE _coven_sql_versions (
                kind TEXT NOT NULL CHECK(kind IN ('collation','function')),
                name TEXT NOT NULL CHECK(name=lower(name)),
                version INTEGER NOT NULL CHECK(version BETWEEN 0 AND 4294967295),
                PRIMARY KEY(kind,name)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_snapshot_reservation, "
            CREATE TABLE _coven_snapshot_reservation (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                number INTEGER NOT NULL CHECK(number>=0)
            ) STRICT;
            INSERT INTO _coven_snapshot_reservation VALUES(1,0);
        ");
        $visit!(_coven_members, "
            CREATE TABLE _coven_members (
                member BLOB PRIMARY KEY NOT NULL CHECK(length(member)=16),
                signing BLOB NOT NULL CHECK(length(signing)=32),
                sealing BLOB NOT NULL CHECK(length(sealing)=32),
                access BLOB NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('admin','member')),
                removed INTEGER NOT NULL CHECK(removed IN (0,1))
            ) STRICT, WITHOUT ROWID;
        ");
        // Every public key pair bound by a counted administrative slot (§10).
        $visit!(_coven_bound_keys, "
            CREATE TABLE _coven_bound_keys (
                signing BLOB NOT NULL CHECK(length(signing)=32),
                sealing BLOB NOT NULL CHECK(length(sealing)=32),
                PRIMARY KEY(signing,sealing)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_devices, "
            CREATE TABLE _coven_devices (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8),
                member BLOB NOT NULL REFERENCES _coven_members(member),
                name TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('active','removed','replaced'))
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_devices_member ON _coven_devices(member);
        ");
        $visit!(_coven_circles, "
            CREATE TABLE _coven_circles (
                circle TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_circle_deletions, "
            CREATE TABLE _coven_circle_deletions (
                circle TEXT PRIMARY KEY NOT NULL,
                slot BLOB NOT NULL CHECK(length(slot)=8 AND slot>x'0000000000000000')
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_circle_members, "
            CREATE TABLE _coven_circle_members (
                circle TEXT NOT NULL REFERENCES _coven_circles(circle),
                member BLOB NOT NULL REFERENCES _coven_members(member),
                PRIMARY KEY(circle,member)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_circle_members_member ON _coven_circle_members(member);
        ");
        $visit!(_coven_boundaries, "
            CREATE TABLE _coven_boundaries (
                slot BLOB NOT NULL CHECK(length(slot)=8 AND slot>x'0000000000000000'),
                kind TEXT NOT NULL CHECK(kind IN ('schema','reset')),
                version INTEGER,
                audience TEXT NOT NULL,
                snapshot_device BLOB NOT NULL CHECK(length(snapshot_device)=8),
                snapshot_number BLOB NOT NULL CHECK(length(snapshot_number)=8 AND snapshot_number>x'0000000000000000'),
                included BLOB,
                applied INTEGER NOT NULL DEFAULT 0 CHECK(applied IN (0,1)),
                CHECK((kind='schema' AND version IS NOT NULL AND version>0)
                   OR (kind='reset' AND version IS NULL)),
                CHECK(applied=0 OR included IS NOT NULL),
                PRIMARY KEY(slot)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_boundaries_applied ON _coven_boundaries(applied,kind);
        ");
        $visit!(_coven_snapshot_schema, "
            CREATE TABLE _coven_snapshot_schema (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                minimum INTEGER NOT NULL CHECK(minimum>=0),
                publication INTEGER NOT NULL CHECK(publication>=0)
            ) STRICT;
            INSERT INTO _coven_snapshot_schema VALUES(1,0,0);
        ");
        $visit!(_coven_raise_captures, "
            CREATE TABLE _coven_raise_captures (
                audience TEXT PRIMARY KEY NOT NULL,
                version INTEGER NOT NULL,
                through BLOB NOT NULL CHECK(length(through)=8),
                covers BLOB NOT NULL CHECK(length(covers)=8)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_writes, "
            CREATE TABLE _coven_writes (
                id INTEGER PRIMARY KEY,
                timestamp BLOB NOT NULL UNIQUE CHECK(length(timestamp) = 16),
                number BLOB NOT NULL CHECK(length(number) = 8 AND number > x'0000000000000000'),
                read_through BLOB NOT NULL CHECK(length(read_through)=8),
                slot BLOB CHECK(slot IS NULL OR length(slot)=8)
            ) STRICT;
            CREATE UNIQUE INDEX _coven_write_identity ON _coven_writes({WRITE_DEVICE_SQL}, number);
            CREATE INDEX _coven_write_slot ON _coven_writes(slot,timestamp);
        ");
        $visit!(_coven_write_headers, "
            CREATE TABLE _coven_write_headers (
                device BLOB NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8 AND number>x'0000000000000000'),
                header BLOB NOT NULL,
                PRIMARY KEY(device,number)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_authored_parts, "
            CREATE TABLE _coven_authored_parts (
                write_id BLOB NOT NULL CHECK(length(write_id)=16),
                audience TEXT NOT NULL,
                header BLOB NOT NULL,
                complete INTEGER NOT NULL CHECK(complete IN (0,1)),
                PRIMARY KEY(write_id,audience)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_authored_changes, "
            CREATE TABLE _coven_authored_changes (
                write_id BLOB NOT NULL,
                audience TEXT NOT NULL,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                record BLOB NOT NULL,
                admission BLOB,
                PRIMARY KEY(write_id,audience,table_name,key),
                FOREIGN KEY(write_id,audience) REFERENCES _coven_authored_parts(write_id,audience)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_authored_changes_audience ON _coven_authored_changes(audience,write_id,table_name,key);
        ");
        coven_ownership_tables!($visit);
        coven_sequence_tables!($visit);
        $visit!(_coven_conversion_schemas, "
            CREATE TABLE _coven_conversion_schemas (
                version INTEGER PRIMARY KEY,
                before_schema BLOB NOT NULL,
                after_schema BLOB NOT NULL,
                tables BLOB NOT NULL,
                columns BLOB NOT NULL
            ) STRICT;
        ");
        $visit!(_coven_table_origins, "
            CREATE TABLE _coven_table_origins (
                version INTEGER NOT NULL,
                name TEXT NOT NULL,
                origin_version INTEGER NOT NULL,
                origin_name TEXT NOT NULL,
                PRIMARY KEY(version,name)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_table_origin ON _coven_table_origins(version,origin_version,origin_name);
        ");
        $visit!(_coven_column_origins, "
            CREATE TABLE _coven_column_origins (
                version INTEGER NOT NULL,
                table_name TEXT NOT NULL,
                name TEXT NOT NULL,
                origin_version INTEGER NOT NULL,
                origin_table TEXT NOT NULL,
                origin_name TEXT NOT NULL,
                PRIMARY KEY(version,table_name,name)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_column_origin ON _coven_column_origins(version,table_name,origin_version,origin_table,origin_name);
        ");
        $visit!(_coven_columns, "
            CREATE TABLE _coven_columns (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                UNIQUE(table_name, column_name)
            ) STRICT;
        ");
        $visit!(_coven_rows, "
            CREATE TABLE _coven_rows (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL,
                generation BLOB NOT NULL CHECK(length(generation) = 8 AND generation > x'0000000000000000'),
                write_id INTEGER NOT NULL REFERENCES _coven_writes(id),
                UNIQUE(table_name, key, audience, generation)
            ) STRICT;
            CREATE INDEX _coven_rows_audience ON _coven_rows(audience,table_name,key,generation);
            CREATE INDEX _coven_rows_write ON _coven_rows(write_id);
        ");
        $visit!(_coven_cells, "
            CREATE TABLE _coven_cells (
                column_id INTEGER NOT NULL REFERENCES _coven_columns(id),
                row_id INTEGER NOT NULL REFERENCES _coven_rows(id),
                write_id INTEGER NOT NULL REFERENCES _coven_writes(id),
                PRIMARY KEY(row_id, column_id)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_cells_write ON _coven_cells(write_id);
        ");
        $visit!(_coven_foreign_keys, "
            CREATE TABLE _coven_foreign_keys (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                identity BLOB NOT NULL,
                UNIQUE(table_name, identity)
            ) STRICT;
        ");
        $visit!(_coven_written_values, "
            CREATE TABLE _coven_written_values (
                column_id INTEGER NOT NULL,
                row_id INTEGER NOT NULL,
                value ANY,
                PRIMARY KEY(row_id,column_id),
                FOREIGN KEY(column_id,row_id) REFERENCES _coven_cells(column_id,row_id) ON DELETE CASCADE ON UPDATE CASCADE
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_references, "
            CREATE TABLE _coven_references (
                row_id INTEGER NOT NULL REFERENCES _coven_rows(id),
                column_id INTEGER NOT NULL REFERENCES _coven_columns(id),
                foreign_key_id INTEGER NOT NULL REFERENCES _coven_foreign_keys(id),
                parent_table TEXT NOT NULL,
                parent_key BLOB NOT NULL,
                parent_audience TEXT NOT NULL,
                parent_generation BLOB NOT NULL CHECK(length(parent_generation)=8),
                PRIMARY KEY(row_id, column_id, foreign_key_id)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_references_parent ON _coven_references(parent_table,parent_key,parent_audience,foreign_key_id,row_id);
            CREATE INDEX _coven_references_column ON _coven_references(column_id,row_id);
            CREATE INDEX _coven_references_key ON _coven_references(foreign_key_id,row_id);
        ");
        $visit!(_coven_constraints, "
            CREATE TABLE _coven_constraints (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                identity BLOB NOT NULL,
                UNIQUE(table_name, identity)
            ) STRICT;
        ");
        $visit!(_coven_claims, "
            CREATE TABLE _coven_claims (
                row_id INTEGER NOT NULL REFERENCES _coven_rows(id),
                constraint_id INTEGER NOT NULL REFERENCES _coven_constraints(id),
                audience TEXT NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY(row_id, constraint_id)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_claims_value ON _coven_claims(constraint_id,audience,value,row_id);
        ");
        $visit!(_coven_lost, "
            CREATE TABLE _coven_lost (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL,
                generation BLOB NOT NULL CHECK(length(generation) = 8),
                column_id INTEGER REFERENCES _coven_columns(id),
                value BLOB NOT NULL,
                set_by BLOB NOT NULL,
                frozen INTEGER NOT NULL DEFAULT 0 CHECK(frozen IN (0,1)),
                replacement_kind TEXT NOT NULL CHECK(replacement_kind IN ('write', 'rules', 'excluded', 'dependency', 'unauthorized')),
                replaced_by BLOB NOT NULL,
                CHECK((replacement_kind='write') = (column_id IS NOT NULL)),
                CHECK(replacement_kind NOT IN ('excluded','dependency','unauthorized') OR frozen=1)
            ) STRICT;
            CREATE INDEX _coven_lost_row ON _coven_lost(audience,table_name,key,generation,column_id);
            CREATE INDEX _coven_lost_app_key ON _coven_lost(table_name,key);
            CREATE UNIQUE INDEX _coven_lost_live_cell ON _coven_lost(table_name,key,audience,column_id,set_by) WHERE frozen=0 AND replacement_kind='write';
            CREATE INDEX _coven_lost_removed ON _coven_lost(table_name,key,audience) WHERE frozen=0 AND replacement_kind='rules';
            CREATE INDEX _coven_lost_column ON _coven_lost(column_id);
        ");
        $visit!(_coven_uploads, "
            CREATE TABLE _coven_uploads (
                device BLOB NOT NULL CHECK(length(device) = 8),
                number BLOB NOT NULL CHECK(length(number) = 8 AND number > x'0000000000000000'),
                record BLOB NOT NULL,
                sealing_keys BLOB,
                claim BLOB CHECK(claim IS NULL OR length(claim)=8),
                PRIMARY KEY(device, number)
            ) STRICT;
        ");
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
        // The user's originals this device has, by file, while their
        // records exist here: the path, as the operating system's bytes
        // (`path_bytes`), size and modification time recorded when each was
        // staged (§17.3).
        $visit!(_coven_user_files, "
            CREATE TABLE _coven_user_files (
                file TEXT PRIMARY KEY NOT NULL,
                path BLOB NOT NULL,
                size INTEGER NOT NULL CHECK(size>=0),
                modified_at BLOB NOT NULL CHECK(length(modified_at)=13)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_user_files_path ON _coven_user_files(path);
        ");
        // Each copy coven writes into a folder the person picked, to make a
        // cloud file device-only (§17.3), recorded before any of its bytes
        // exist: its path, the temporary name its bytes are written under,
        // and where it stands. `writing` and `placed` copies belong to a call
        // that hasn't switched its files; `kept` ones are originals this
        // device records; a `lost` one's switch lost to another device's, and
        // it goes if it is as coven left it (Appendix C.3). A copy's path is
        // valid Unicode, and the original recorded at it holds that text's
        // UTF-8 bytes on every platform, so the two compare through a CAST.
        $visit!(_coven_copies, "
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
        ");
        // The folders a call makes on each copy's path, recorded before it
        // makes them. `made` is set once its own mkdir made the folder: one
        // that was there already is someone else's. A made folder goes when
        // empty with the last copy it holds.
        $visit!(_coven_copy_folders, "
            CREATE TABLE _coven_copy_folders (
                folder TEXT NOT NULL,
                copy TEXT NOT NULL REFERENCES _coven_copies(path) ON DELETE CASCADE,
                made INTEGER NOT NULL DEFAULT 0 CHECK(made IN (0,1)),
                PRIMARY KEY(folder,copy)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_copy_folders_copy ON _coven_copy_folders(copy);
        ");
        $visit!(_coven_file_removals, "
            CREATE TABLE _coven_file_removals (
                path TEXT NOT NULL,
                area TEXT NOT NULL DEFAULT 'files' CHECK(area IN ('files','cache')),
                PRIMARY KEY(path,area)
            ) STRICT, WITHOUT ROWID;
        ");
        // Cache ranges whose bytes are being written with no writer held;
        // opening makes the abandoned ones removals (Appendix C.3).
        $visit!(_coven_cache_writes, "
            CREATE TABLE _coven_cache_writes (path TEXT NOT NULL PRIMARY KEY) STRICT, WITHOUT ROWID;
        ");
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
        ");
        $visit!(_coven_copy_chunks, "
            CREATE TABLE _coven_copy_chunks (
                path TEXT NOT NULL,
                chunk INTEGER NOT NULL CHECK(chunk>=0),
                hash BLOB NOT NULL CHECK(length(hash)=32),
                PRIMARY KEY(path,chunk)
            ) STRICT, WITHOUT ROWID;
        ");
        // Each file a switch to the cloud asked this device to keep whole,
        // which coven downloads itself until it is (§17.8): from coven's
        // copy of an app file, which the request keeps until then, or from
        // storage. A user's original is never read for the cache.
        $visit!(_coven_pin_requests, "
            CREATE TABLE _coven_pin_requests (
                file TEXT PRIMARY KEY NOT NULL,
                copy TEXT
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_pin_requests_copy ON _coven_pin_requests(copy) WHERE copy IS NOT NULL;
        ");
        // Each move of files of this device to the cloud (§17.3): the files
        // one write staged for the cloud, or the files the app moved. Every
        // upload belongs to one; a move switches its files together once all
        // its uploads are stored, or fails or is cancelled whole. A move
        // keeps its row once it ends, so it is never mistaken for another.
        $visit!(_coven_moves, "
            CREATE TABLE _coven_moves (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state TEXT NOT NULL CHECK(state IN ('moving','failed','cancelled','finished'))
            ) STRICT;
            CREATE INDEX _coven_moves_moving ON _coven_moves(id) WHERE state='moving';
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
        // Present while the app has paused uploads (E7).
        $visit!(_coven_upload_pause, "
            CREATE TABLE _coven_upload_pause (
                paused INTEGER PRIMARY KEY CHECK(paused=1)
            ) STRICT;
        ");
        $visit!(_coven_file_chunks, "
            CREATE TABLE _coven_file_chunks (
                device BLOB NOT NULL,
                file BLOB NOT NULL,
                chunk INTEGER NOT NULL CHECK(chunk>=0),
                hash BLOB NOT NULL CHECK(length(hash)=32),
                PRIMARY KEY(device,file,chunk),
                FOREIGN KEY(device,file) REFERENCES _coven_file_uploads(device,file)
                    ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
        ");
        // Fixed file paths awaiting a storage-deletion decision (§17.10): the
        // transaction ending a path's last local protection adds it, so
        // retention never lists files/. `since` is the applied slot when it
        // last lost its protection: every other device must have published a
        // write that read through it before the path goes. Applying a slot
        // records it as applied before its changes reach these tables, so a
        // path a slot leaves unnamed counts from that slot.
        $visit!(_coven_file_candidates, "
            CREATE TABLE _coven_file_candidates (
                device BLOB NOT NULL CHECK(length(device)=8),
                file BLOB NOT NULL CHECK(length(file)=16),
                since BLOB NOT NULL CHECK(length(since)=8),
                PRIMARY KEY(device,file)
            ) STRICT, WITHOUT ROWID;
            CREATE VIEW _coven_file_protection(device,file) AS
                SELECT device,file FROM _coven_row_file_refs UNION ALL SELECT device,file FROM _coven_published_file_refs
                UNION ALL SELECT device,file FROM _coven_queued_file_refs UNION ALL SELECT device,file FROM _coven_file_uploads;
            CREATE TRIGGER _coven_row_file_ref_released AFTER DELETE ON _coven_row_file_refs BEGIN
                INSERT INTO _coven_file_candidates SELECT OLD.device,OLD.file,(SELECT applied FROM _coven_sequence WHERE singleton=1) WHERE NOT EXISTS(SELECT 1 FROM _coven_file_protection WHERE device=OLD.device AND file=OLD.file) ON CONFLICT DO UPDATE SET since=max(since,excluded.since);
            END;
            CREATE TRIGGER _coven_published_file_ref_released AFTER DELETE ON _coven_published_file_refs BEGIN
                INSERT INTO _coven_file_candidates SELECT OLD.device,OLD.file,(SELECT applied FROM _coven_sequence WHERE singleton=1) WHERE NOT EXISTS(SELECT 1 FROM _coven_file_protection WHERE device=OLD.device AND file=OLD.file) ON CONFLICT DO UPDATE SET since=max(since,excluded.since);
            END;
            CREATE TRIGGER _coven_queued_file_ref_released AFTER DELETE ON _coven_queued_file_refs BEGIN
                INSERT INTO _coven_file_candidates SELECT OLD.device,OLD.file,(SELECT applied FROM _coven_sequence WHERE singleton=1) WHERE NOT EXISTS(SELECT 1 FROM _coven_file_protection WHERE device=OLD.device AND file=OLD.file) ON CONFLICT DO UPDATE SET since=max(since,excluded.since);
            END;
            CREATE TRIGGER _coven_file_upload_released AFTER DELETE ON _coven_file_uploads BEGIN
                INSERT INTO _coven_file_candidates SELECT OLD.device,OLD.file,(SELECT applied FROM _coven_sequence WHERE singleton=1) WHERE NOT EXISTS(SELECT 1 FROM _coven_file_protection WHERE device=OLD.device AND file=OLD.file) ON CONFLICT DO UPDATE SET since=max(since,excluded.since);
            END;
        ");
        // A removed uploader's file folder this device listed when its
        // removal applied, and that counted removal. A later removal of the
        // same device's member lists it again; the paths found are
        // candidates (§17.10).
        $visit!(_coven_file_scans, "
            CREATE TABLE _coven_file_scans (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8),
                slot BLOB NOT NULL CHECK(length(slot)=8)
            ) STRICT, WITHOUT ROWID;
        ");
        // Recorded bytes are removed once the writer is free, with no check
        // of their own: its triggers record a removal only for a path no row
        // names, and let no row name a path recorded for removal (Appendix C.3).
        $visit!(_coven_cache, "
            CREATE TABLE _coven_cache (
                namespace TEXT NOT NULL,
                file_id TEXT NOT NULL,
                chunk INTEGER NOT NULL CHECK(chunk>={CACHE_COMPLETE_FILE}),
                chunks INTEGER NOT NULL DEFAULT 1 CHECK(chunks>=1) CHECK(chunk>=0 OR chunks=1),
                path TEXT NOT NULL UNIQUE,
                size INTEGER NOT NULL CHECK(size>=0),
                last_read INTEGER NOT NULL,
                pinned INTEGER NOT NULL CHECK(pinned IN (0,1)) CHECK(pinned=0 OR chunk={CACHE_COMPLETE_FILE}),
                checked_hash BLOB CHECK((chunk={CACHE_COMPLETE_FILE})=(checked_hash IS NOT NULL)) CHECK(checked_hash IS NULL OR length(checked_hash)=32),
                PRIMARY KEY(namespace,file_id,chunk)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_cache_lru ON _coven_cache(namespace,pinned,last_read);
            CREATE INDEX _coven_cache_recency ON _coven_cache(last_read);
            CREATE TRIGGER _coven_file_removal_unnamed BEFORE INSERT ON _coven_file_removals BEGIN
                SELECT RAISE(ABORT,'a file recorded for removal is named by a row')
                WHERE EXISTS(SELECT 1 FROM _coven_device_files WHERE path=NEW.path)
                    OR EXISTS(SELECT 1 FROM _coven_file_uploads WHERE copy=NEW.path)
                    OR EXISTS(SELECT 1 FROM _coven_pin_requests WHERE copy=NEW.path)
                    OR EXISTS(SELECT 1 FROM _coven_cache WHERE path=NEW.path);
            END;
            CREATE TRIGGER _coven_device_file_unremoved BEFORE INSERT ON _coven_device_files BEGIN
                SELECT RAISE(ABORT,'a file recorded for removal is named by a row')
                WHERE EXISTS(SELECT 1 FROM _coven_file_removals WHERE path=NEW.path);
            END;
            CREATE TRIGGER _coven_upload_copy_unremoved BEFORE INSERT ON _coven_file_uploads
            WHEN NEW.copy IS NOT NULL BEGIN
                SELECT RAISE(ABORT,'a file recorded for removal is named by a row')
                WHERE EXISTS(SELECT 1 FROM _coven_file_removals WHERE path=NEW.copy);
            END;
            CREATE TRIGGER _coven_cache_unremoved BEFORE INSERT ON _coven_cache BEGIN
                SELECT RAISE(ABORT,'a file recorded for removal is named by a row')
                WHERE EXISTS(SELECT 1 FROM _coven_file_removals WHERE path=NEW.path);
            END;
        ");
        // Each declared namespace's budget and the cached bytes the cache
        // records hold, maintained with them so a budget check needs no sum.
        // Cache records live only in declared namespaces (FileRef validation).
        $visit!(_coven_cache_budgets, "
            CREATE TABLE _coven_cache_budgets (
                namespace TEXT PRIMARY KEY NOT NULL,
                bytes BLOB NOT NULL CHECK(length(bytes)=8),
                total INTEGER NOT NULL DEFAULT 0 CHECK(total>=0)
            ) STRICT, WITHOUT ROWID;
            CREATE TRIGGER _coven_cache_added AFTER INSERT ON _coven_cache BEGIN
                UPDATE _coven_cache_budgets SET total=total+NEW.size WHERE namespace=NEW.namespace;
            END;
            CREATE TRIGGER _coven_cache_removed AFTER DELETE ON _coven_cache BEGIN
                UPDATE _coven_cache_budgets SET total=total-OLD.size WHERE namespace=OLD.namespace;
            END;
        ");
        $visit!(_coven_provider_access, "
            CREATE TABLE _coven_provider_access (
                account TEXT PRIMARY KEY NOT NULL,
                intended INTEGER NOT NULL CHECK(intended IN (0,1)),
                granted INTEGER CHECK(granted IN (0,1)),
                invitation BLOB,
                operation INTEGER UNIQUE REFERENCES _coven_operations(id),
                target INTEGER CHECK(target IN (0,1)),
                CHECK(target IS NULL OR operation IS NOT NULL)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_operations, "
            CREATE TABLE _coven_operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                last_step INTEGER NOT NULL,
                data BLOB NOT NULL,
                started_by TEXT NOT NULL
            );
            CREATE INDEX _coven_operations_kind ON _coven_operations(kind);
        ");
        // The one operation pausing app writes while it replaces this
        // member's keys (§12.3). Its row goes with the operation's.
        $visit!(_coven_write_pause, "
            CREATE TABLE _coven_write_pause (
                paused INTEGER PRIMARY KEY CHECK(paused=1),
                operation INTEGER NOT NULL UNIQUE
                    REFERENCES _coven_operations(id) ON DELETE CASCADE
            ) STRICT;
        ");
        // One persisted retry wait, T and W, per waiting operation, queued
        // upload, joiner poll or provider cooldown (§6.7). Work rows take
        // their waits with them.
        $visit!(_coven_retries, "
            CREATE TABLE _coven_retries (
                id INTEGER PRIMARY KEY,
                scope TEXT UNIQUE CHECK(scope IN ('join','sync','provider')),
                operation INTEGER UNIQUE REFERENCES _coven_operations(id) ON DELETE CASCADE,
                upload INTEGER UNIQUE REFERENCES _coven_file_uploads(id) ON DELETE CASCADE,
                pin TEXT UNIQUE REFERENCES _coven_pin_requests(file) ON DELETE CASCADE,
                failures INTEGER NOT NULL CHECK(failures>=0),
                not_before BLOB NOT NULL CHECK(length(not_before)=13),
                wait INTEGER NOT NULL CHECK(wait>=0),
                CHECK((scope IS NOT NULL)+(operation IS NOT NULL)+(upload IS NOT NULL)
                    +(pin IS NOT NULL)=1)
            ) STRICT;
        ");
    };
}

pub(crate) fn initial_schema() -> String {
    let mut sql = String::new();
    macro_rules! append {
        (_coven_writes, $sql:literal) => {
            sql.push_str(&format!($sql));
        };
        (_coven_cache, $sql:literal) => {
            sql.push_str(&format!($sql));
        };
        ($table:ident, $sql:literal) => {
            sql.push_str($sql);
        };
    }
    coven_tables!(append);
    sql.push_str(files::SCHEMA);
    sql.push_str(pins::SCHEMA);
    sql.push_str(upload_queue::SCHEMA);
    sql.push_str(views::SCHEMA);
    sql
}

/// Canonical lowercase hyphenated text of a 16-byte UUID, as `UuidText`
/// binds one.
macro_rules! uuid_text {
    ($blob:literal) => {
        concat!(
            "lower(substr(hex(",
            $blob,
            "),1,8)||'-'||substr(hex(",
            $blob,
            "),9,4)||'-'||",
            "substr(hex(",
            $blob,
            "),13,4)||'-'||substr(hex(",
            $blob,
            "),17,4)||'-'||",
            "substr(hex(",
            $blob,
            "),21))"
        )
    };
}

#[path = "internal_schema_files.rs"]
mod files;
pub(crate) use files::FILES;

#[path = "internal_schema_pins.rs"]
mod pins;
pub(crate) use pins::PINS;

#[path = "internal_schema_upload_queue.rs"]
mod upload_queue;
pub(crate) use upload_queue::UPLOAD_QUEUE;

/// Coven's tables the app's local triggers may follow (§9.4): coven changes
/// their rows in the statements that change what they say, its files table
/// as each record is made, stored or deleted here or merged from another
/// device, and the app's AFTER triggers on them write only local tables.
pub(crate) const FOLLOWED: [&str; 3] = [FILES, PINS, UPLOAD_QUEUE];

/// Whether `name` is one of the tables the app's triggers may follow.
pub(crate) fn followed(name: &str) -> bool {
    FOLLOWED
        .iter()
        .any(|table| table.eq_ignore_ascii_case(name))
}

#[path = "internal_schema_views.rs"]
mod views;
pub(crate) use views::app_readable;

#[path = "internal_schema_input.rs"]
mod input;
pub(crate) use input::initial_input_schema;

#[cfg(test)]
#[path = "internal_schema_tests.rs"]
mod tests;
