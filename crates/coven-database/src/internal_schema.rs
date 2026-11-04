//! Coven's local schema. `application_id` identifies these tables independently
//! of the application's `user_version`.
//!
//! Unsigned merge counters are big-endian eight-byte blobs. SQLite's signed
//! integer range must not truncate device ids, generations or write numbers.
//! Present displayed values live in app tables; a written value that reads
//! differently (a NULL-substituted reference) is kept by cell. Generated
//! columns have no cells: SQLite computes them. Metadata and lost values use
//! coven-format's snapshot field encodings inside blobs.
//! Loss targets name their row directly: a write excluded by a schema change
//! (§18.6) need not have an accepted generation in `_coven_rows`.
//! An upload's `record` is one plaintext value: the write header frame followed
//! by its audience parts' row-frame streams. Its first attempt fixes the format
//! and key ids in `_coven_upload_keys`, encoded as the D9 write prefix. Each
//! attempted slot keeps its exact ciphertext in the claim journal before storage
//! is called. `_coven_uploads` retains its rowid for queue order and incremental
//! BLOB reads.
//! Cell keys start with the row; column-wide cell edits occur only in migrations.
//! An original change has no admission row until judged here, then this
//! device's `ChangeVerdict`. Consuming a slot does not establish admission.
//! Write-id indexes serve SQLite's incoming foreign-key checks on INSERT with
//! RETURNING, which EXPLAIN QUERY PLAN includes alongside explicit lookups.

pub(crate) const VERSION: u32 = 1;
pub(crate) const CACHE_HEADER: i64 = -1;
pub(crate) const CACHE_COMPLETE_FILE: i64 = -2;
// D2 timestamps contain six clock bytes, two counter bytes, then the device id.
pub(crate) const WRITE_DEVICE_SQL: &str = "substr(timestamp,9,8)";
/// Whether removal rules took out the merge row `r` names, through its
/// `r.table_name`, `r.key` and `r.audience`: an active rules loss holds its
/// values. An active one is always at the row's current generation
/// (format.md D7.2), so the predicate needs no generation.
pub(crate) const RULES_REMOVED_SQL: &str = "(EXISTS(SELECT 1 FROM _coven_lost AS removed
    WHERE removed.table_name=r.table_name AND removed.key=r.key AND removed.audience=r.audience
      AND removed.frozen=0 AND removed.replacement_kind='rules'))";
/// Whether the files-area path `?1` is named: by a file on this device
/// only, or as the copy of a queued upload or a pin request (Appendix C.3).
pub(crate) const FILE_NAMED_SQL: &str = "(EXISTS(SELECT 1 FROM _coven_device_files WHERE path=?1)
    OR EXISTS(SELECT 1 FROM _coven_file_uploads WHERE copy=?1)
    OR EXISTS(SELECT 1 FROM _coven_pin_requests WHERE copy=?1))";
/// Whether the cache-area path `?1` is in use: by an entry, a range being
/// written, or bytes recorded for removal (Appendix C.3).
pub(crate) const CACHE_NAME_USED_SQL: &str = "(EXISTS(SELECT 1 FROM _coven_cache WHERE path=?1)
    OR EXISTS(SELECT 1 FROM _coven_cache_writes WHERE path=?1)
    OR EXISTS(SELECT 1 FROM _coven_file_removals WHERE path=?1 AND area='cache'))";

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
// Each domain's tables, beside the domain that writes them.
#[macro_use]
#[path = "authorship_schema.rs"]
mod authorship_schema;
#[macro_use]
#[path = "captured_changes_schema.rs"]
mod captured_changes_schema;
#[macro_use]
#[path = "claim_schema.rs"]
mod claim_schema;
#[macro_use]
#[path = "database_file_retention_schema.rs"]
mod database_file_retention_schema;
#[macro_use]
#[path = "database_operations_schema.rs"]
mod database_operations_schema;
#[macro_use]
#[path = "device_reset_schema.rs"]
mod device_reset_schema;
#[macro_use]
#[path = "file_database_copies_schema.rs"]
mod file_database_copies_schema;
#[macro_use]
#[path = "file_moves_schema.rs"]
mod file_moves_schema;
#[macro_use]
#[path = "file_pins_schema.rs"]
mod file_pins_schema;
#[macro_use]
#[path = "file_protection_schema.rs"]
mod file_protection_schema;
#[macro_use]
#[path = "file_queue_schema.rs"]
mod file_queue_schema;
#[macro_use]
#[path = "file_table_schema.rs"]
mod file_table_schema;
#[macro_use]
#[path = "file_write_uploads_schema.rs"]
mod file_write_uploads_schema;
#[macro_use]
#[path = "migration_receive_schema.rs"]
mod migration_receive_schema;
#[macro_use]
#[path = "object_removals_schema.rs"]
mod object_removals_schema;
#[macro_use]
#[path = "operation_schema.rs"]
mod operation_schema;
#[macro_use]
#[path = "published_schema.rs"]
mod published_schema;
#[macro_use]
#[path = "raise_capture_schema.rs"]
mod raise_capture_schema;
#[macro_use]
#[path = "reload_gate_schema.rs"]
mod reload_gate_schema;
#[macro_use]
#[path = "retry_schema.rs"]
mod retry_schema;
#[macro_use]
#[path = "row_queries_schema.rs"]
mod row_queries_schema;
#[macro_use]
#[path = "sequence_apply_schema.rs"]
mod sequence_apply_schema;
#[macro_use]
#[path = "sequence_schema.rs"]
mod sequence_schema;
#[macro_use]
#[path = "source_names_schema.rs"]
mod source_names_schema;
#[macro_use]
#[path = "sql_versions_schema.rs"]
mod sql_versions_schema;
#[macro_use]
#[path = "store_state_schema.rs"]
mod store_state_schema;
#[macro_use]
#[path = "upload_schema.rs"]
mod upload_schema;
#[macro_use]
#[path = "write_boundary_schema.rs"]
mod write_boundary_schema;
#[macro_use]
#[path = "write_commit_schema.rs"]
mod write_commit_schema;
#[macro_use]
#[path = "write_headers_schema.rs"]
mod write_headers_schema;
#[macro_use]
#[path = "written_values_schema.rs"]
mod written_values_schema;

macro_rules! coven_tables {
    ($visit:ident) => {
        // A record's cause is the text of the error behind it, written with
        // the record by the run that met the error. Opening clears every
        // cause: one from an earlier run names a failure nothing has met
        // since (§19.1).
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
                cause TEXT,
                CHECK((device IS NULL)=(file IS NULL)),
                CHECK((work IS NULL)=(device IS NULL))
            ) STRICT;
            CREATE INDEX _coven_pending_object ON _coven_pending(object);
            CREATE INDEX _coven_pending_file ON _coven_pending(device,file) WHERE device IS NOT NULL;
            CREATE UNIQUE INDEX _coven_pending_operation ON _coven_pending(operation) WHERE operation IS NOT NULL;
            CREATE UNIQUE INDEX _coven_pending_move ON _coven_pending(move) WHERE move IS NOT NULL;
            CREATE UNIQUE INDEX _coven_pending_pin ON _coven_pending(pin) WHERE pin IS NOT NULL;
        ");
        coven_data_reloads_tables!($visit);
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
        coven_file_columns_tables!($visit);
        // Files that writes from other devices left without a reference here
        // (§17.2), until this device's own write deletes their records.
        $visit!(_coven_unreferenced_files, "
            CREATE TABLE _coven_unreferenced_files (
                file TEXT PRIMARY KEY NOT NULL
            ) STRICT, WITHOUT ROWID;
        ");
        coven_device_reset_tables!($visit);
        coven_SQL_versions_tables!($visit);
        coven_store_state_tables!($visit);
        coven_write_boundaries_tables!($visit);
        coven_raises_tables!($visit);
        coven_writes_tables!($visit);
        coven_write_headers_tables!($visit);
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
                PRIMARY KEY(write_id,audience,table_name,key),
                FOREIGN KEY(write_id,audience) REFERENCES _coven_authored_parts(write_id,audience)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_authored_changes_audience ON _coven_authored_changes(audience,write_id,table_name,key);
        ");
        // This device's verdict on an original change, beside its record for
        // the reason `_coven_upload_keys` is beside an upload's.
        $visit!(_coven_authored_admissions, "
            CREATE TABLE _coven_authored_admissions (
                write_id BLOB NOT NULL,
                audience TEXT NOT NULL,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                admission BLOB NOT NULL,
                PRIMARY KEY(write_id,audience,table_name,key),
                FOREIGN KEY(write_id,audience,table_name,key) REFERENCES _coven_authored_changes(write_id,audience,table_name,key) ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
        ");
        coven_ownership_tables!($visit);
        coven_published_tables!($visit);
        coven_sequence_tables!($visit);
        coven_claim_tables!($visit);
        coven_conversion_schemas_tables!($visit);
        coven_source_names_tables!($visit);
        // A synced column, or a column a breaking migration dropped, which
        // has a row only while a frozen lost value names it (§18.7): the
        // deletion or move of the last one deletes it.
        $visit!(_coven_columns, "
            CREATE TABLE _coven_columns (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                synced INTEGER NOT NULL CHECK(synced IN (0,1)),
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
        coven_written_values_tables!($visit);
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
        $visit!(
            _coven_constraints,
            "
            CREATE TABLE _coven_constraints (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                identity BLOB NOT NULL,
                UNIQUE(table_name, identity)
            ) STRICT;
        "
        );
        $visit!(
            _coven_foreign_keys,
            "
            CREATE TABLE _coven_foreign_keys (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                identity BLOB NOT NULL,
                UNIQUE(table_name, identity)
            ) STRICT;
        "
        );
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
            CREATE TRIGGER _coven_lost_column_deleted AFTER DELETE ON _coven_lost
            WHEN OLD.column_id IS NOT NULL BEGIN
                DELETE FROM _coven_columns WHERE id=OLD.column_id AND synced=0
                    AND NOT EXISTS(SELECT 1 FROM _coven_lost WHERE column_id=OLD.column_id);
            END;
            CREATE TRIGGER _coven_lost_column_moved AFTER UPDATE OF column_id ON _coven_lost
            WHEN OLD.column_id IS NOT NULL AND OLD.column_id IS NOT NEW.column_id BEGIN
                DELETE FROM _coven_columns WHERE id=OLD.column_id AND synced=0
                    AND NOT EXISTS(SELECT 1 FROM _coven_lost WHERE column_id=OLD.column_id);
            END;
        ");
        $visit!(_coven_uploads, "
            CREATE TABLE _coven_uploads (
                device BLOB NOT NULL CHECK(length(device) = 8),
                number BLOB NOT NULL CHECK(length(number) = 8 AND number > x'0000000000000000'),
                record BLOB NOT NULL,
                PRIMARY KEY(device, number)
            ) STRICT;
        ");
        coven_upload_keys_tables!($visit);
        coven_queued_file_references_tables!($visit);
        // The user's originals this device has, by file, while their
        // records exist here: the path, as the operating system's bytes
        // (`path_bytes`), size, modification time and content hash recorded
        // when each was staged, or placed as a copy (§17.3).
        $visit!(_coven_user_files, "
            CREATE TABLE _coven_user_files (
                file TEXT PRIMARY KEY NOT NULL,
                path BLOB NOT NULL,
                size INTEGER NOT NULL CHECK(size>=0),
                modified_at BLOB NOT NULL CHECK(length(modified_at)=13),
                hash BLOB NOT NULL CHECK(length(hash)=32)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_user_files_path ON _coven_user_files(path);
        ");
        // The plaintext chunk hashes of each original recorded above, from
        // the same read as its content hash, while its file may still move
        // to the cloud: a move's upload checks the bytes it sends against
        // them. They go when the file's record switches to stored bytes, or
        // with the original's row (§17.3).
        $visit!(_coven_user_chunks, "
            CREATE TABLE _coven_user_chunks (
                file TEXT NOT NULL REFERENCES _coven_user_files(file) ON DELETE CASCADE,
                chunk INTEGER NOT NULL CHECK(chunk>=0),
                hash BLOB NOT NULL CHECK(length(hash)=32),
                PRIMARY KEY(file,chunk)
            ) STRICT, WITHOUT ROWID;
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
        coven_copy_folders_tables!($visit);
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
        coven_pin_requests_tables!($visit);
        coven_file_moves_tables!($visit);
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
        coven_upload_settings_tables!($visit);
        coven_file_chunks_tables!($visit);
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
        coven_operation_state_tables!($visit);
        coven_object_removals_tables!($visit);
        coven_ended_requests_tables!($visit);
        // The operations the app's writes journaled, each with its kind as
        // coven-sync names it and, once it has finished, its outcome as
        // coven-sync encodes it, until the app acknowledges it
        // (`coven_operations`, §11.4). The journal row of an app operation
        // without its outcome can't be deleted: the outcome is written
        // first, in the same transaction.
        $visit!(_coven_app_operations, "
            CREATE TABLE _coven_app_operations (
                operation INTEGER PRIMARY KEY,
                kind TEXT NOT NULL,
                outcome BLOB
            ) STRICT;
            CREATE TRIGGER _coven_app_operation_ends BEFORE DELETE ON _coven_operations
            WHEN EXISTS(SELECT 1 FROM _coven_app_operations
                WHERE operation=OLD.id AND outcome IS NULL)
            BEGIN
                SELECT RAISE(ABORT,'an app operation ends with its outcome');
            END;
        ");
        coven_retries_tables!($visit);
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
    sql.push_str(&pins::schema());
    sql.push_str(upload_queue::SCHEMA);
    sql.push_str(sync_status::SCHEMA);
    sql.push_str(retry_effect::SCHEMA);
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

#[path = "internal_schema_sync_status.rs"]
pub(crate) mod sync_status;
pub(crate) use sync_status::SYNC_STATUS;

#[path = "internal_schema_retry_effect.rs"]
pub(crate) mod retry_effect;
pub(crate) use retry_effect::RETRY_EFFECT;

/// Coven's tables the app's local triggers may follow (§9.4): coven changes
/// their rows in the statements that change what they say, its files table
/// as each record is made, stored or deleted here or merged from another
/// device, its sync status and retry effect as they change, and the app's
/// AFTER triggers on them write only local tables.
pub(crate) const FOLLOWED: [&str; 5] = [FILES, PINS, UPLOAD_QUEUE, SYNC_STATUS, RETRY_EFFECT];

/// Coven's tables that describe this process's own sync, which only the
/// handle holding the store's writer lock may read.
pub(crate) const WRITER_STATE: [&str; 2] = [SYNC_STATUS, RETRY_EFFECT];

/// Whether `name` is one of the tables the app's triggers may follow.
pub(crate) fn followed(name: &str) -> bool {
    FOLLOWED.iter().any(|table| table.eq_ignore_ascii_case(name))
}

#[path = "internal_schema_views.rs"]
mod views;
pub(crate) use views::app_readable;

#[path = "input_database_schema.rs"]
mod input;
pub(crate) use input::initial_input_schema;

#[cfg(test)]
#[path = "internal_schema_tests.rs"]
mod tests;
