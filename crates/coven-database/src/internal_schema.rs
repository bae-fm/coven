//! Coven's local schema. `application_id` versions these tables independently
//! of the application's `user_version`. Each internal migration is atomic.
//!
//! Unsigned merge counters are big-endian eight-byte blobs. SQLite's signed
//! integer range must not truncate device ids, generations or write numbers.
//! Present displayed values live in app tables; differing written references
//! are kept by cell. Metadata and lost values use coven-format's snapshot
//! field encodings inside blobs.
//! Loss targets name their row directly: a write excluded by a schema change
//! or reset need not have an accepted generation in `_coven_rows`.
//! An upload's `record` is one plaintext value: the write header frame followed
//! by its audience parts' row-frame streams. Its first attempt fixes the format
//! and key ids in `sealing_keys`, encoded as the D9 write prefix. A queued store-log
//! entry keeps its format in `record`'s frame prefix. No ciphertext is kept for either.
//! `_coven_uploads` retains its rowid for queue order and incremental BLOB reads.
//! Cell keys start with the row; column-wide cell edits occur only in migrations.
//! Original-change admission is NULL until judged, an empty blob when admitted,
//! or the encoded exclusion cause. Consumed positions do not establish admission.
//! Write-id indexes serve SQLite's incoming foreign-key checks on INSERT with
//! RETURNING, which EXPLAIN QUERY PLAN includes alongside explicit lookups.

pub(crate) const VERSION: u32 = 1;
pub(crate) const CACHE_HEADER: i64 = -1;
pub(crate) const CACHE_COMPLETE_FILE: i64 = -2;
// D2 timestamps contain six clock bytes, two counter bytes, then the device id.
pub(crate) const WRITE_DEVICE_SQL: &str = "substr(timestamp,9,8)";

/// App objects cannot use either reserved namespace (§17.2).
pub(crate) fn reserved_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.starts_with("_coven_") || name.starts_with("coven_")
}

// This is also the ownership checker's source of reserved table names.
macro_rules! coven_tables {
    ($visit:ident) => {
        $visit!(_coven_pending, "
            CREATE TABLE _coven_pending (
                subject BLOB NOT NULL,
                reporter BLOB NOT NULL CHECK(length(reporter)=8),
                value BLOB NOT NULL,
                operation INTEGER REFERENCES _coven_operations(id),
                object BLOB CHECK(object IS NULL OR length(object)=17),
                coven_version TEXT NOT NULL,
                suppressed INTEGER NOT NULL DEFAULT 0 CHECK(suppressed IN (0,1)),
                PRIMARY KEY(subject,reporter)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_pending_object ON _coven_pending(object,reporter);
            CREATE UNIQUE INDEX _coven_pending_operation ON _coven_pending(operation) WHERE operation IS NOT NULL;
        ");
        $visit!(_coven_peer_positions, "
            CREATE TABLE _coven_peer_positions (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8),
                observation BLOB NOT NULL
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_positions_post, "
            CREATE TABLE _coven_positions_post (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                state BLOB NOT NULL
            ) STRICT;
        ");
        $visit!(_coven_pending_resets, "
            CREATE TABLE _coven_pending_resets (
                subject BLOB NOT NULL,
                reporter BLOB NOT NULL,
                entry_device BLOB NOT NULL,
                entry_number BLOB NOT NULL,
                PRIMARY KEY(subject,reporter,entry_device,entry_number),
                FOREIGN KEY(subject,reporter) REFERENCES _coven_pending(subject,reporter) ON DELETE CASCADE,
                FOREIGN KEY(entry_device,entry_number) REFERENCES _coven_boundaries(entry_device,entry_number)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_pending_resets_entry ON _coven_pending_resets(entry_device,entry_number);
        ");
        $visit!(_coven_data_reloads, "
            CREATE TABLE _coven_data_reloads (
                audience TEXT PRIMARY KEY NOT NULL,
                subject BLOB NOT NULL,
                update_required INTEGER NOT NULL CHECK(update_required IN (0,1))
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_data_inputs, "
            CREATE TABLE _coven_data_inputs (
                id INTEGER PRIMARY KEY,
                audience TEXT NOT NULL,
                kind TEXT NOT NULL CHECK(kind IN ('local','snapshot','empty')),
                device BLOB,
                number BLOB,
                version INTEGER NOT NULL CHECK(version>=0),
                positions BLOB NOT NULL,
                CHECK((kind='snapshot' AND device IS NOT NULL AND length(device)=8
                     AND number IS NOT NULL AND length(number)=8 AND number>x'0000000000000000')
                   OR (kind!='snapshot' AND device IS NULL AND number IS NULL))
            ) STRICT;
            CREATE INDEX _coven_data_inputs_audience ON _coven_data_inputs(audience,kind,device,number);
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
        $visit!(_coven_input_file_refs, "
            CREATE TABLE _coven_input_file_refs (
                device BLOB NOT NULL CHECK(length(device)=8),
                file BLOB NOT NULL CHECK(length(file)=16),
                input INTEGER NOT NULL REFERENCES _coven_data_inputs(id) ON DELETE CASCADE,
                PRIMARY KEY(device,file,input)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_input_file_refs_input ON _coven_input_file_refs(input);
        ");
        $visit!(_coven_data_origins, "
            CREATE TABLE _coven_data_origins (
                audience TEXT PRIMARY KEY NOT NULL,
                input INTEGER NOT NULL REFERENCES _coven_data_inputs(id),
                basis BLOB NOT NULL CHECK(length(basis)%16=0)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_data_origins_input ON _coven_data_origins(input);
        ");
        $visit!(_coven_data_rows, "
            CREATE TABLE _coven_data_rows (
                input INTEGER NOT NULL REFERENCES _coven_data_inputs(id) ON DELETE CASCADE,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                record BLOB NOT NULL,
                PRIMARY KEY(input,table_name,key)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_data_losses, "
            CREATE TABLE _coven_data_losses (
                input INTEGER NOT NULL REFERENCES _coven_data_inputs(id) ON DELETE CASCADE,
                ordinal INTEGER NOT NULL,
                record BLOB NOT NULL,
                PRIMARY KEY(input,ordinal)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_write_inputs, "
            CREATE TABLE _coven_write_inputs (
                audience TEXT NOT NULL,
                timestamp BLOB NOT NULL CHECK(length(timestamp)=16),
                header BLOB NOT NULL,
                PRIMARY KEY(audience,timestamp)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_write_input_frames, "
            CREATE TABLE _coven_write_input_frames (
                audience TEXT NOT NULL,
                timestamp BLOB NOT NULL,
                ordinal INTEGER NOT NULL CHECK(ordinal>=0),
                frame BLOB NOT NULL,
                PRIMARY KEY(audience,timestamp,ordinal),
                FOREIGN KEY(audience,timestamp) REFERENCES _coven_write_inputs(audience,timestamp) ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_local_effects, "
            CREATE TABLE _coven_local_effects (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL
            ) STRICT;
            CREATE INDEX _coven_local_effects_parent ON _coven_local_effects(table_name,key,audience);
        ");
        $visit!(_coven_local_effect_causes, "
            CREATE TABLE _coven_local_effect_causes (
                effect INTEGER NOT NULL REFERENCES _coven_local_effects(id) ON DELETE CASCADE,
                entry_device BLOB NOT NULL,
                entry_number BLOB NOT NULL,
                PRIMARY KEY(effect,entry_device,entry_number),
                FOREIGN KEY(entry_device,entry_number) REFERENCES _coven_store_log(device,number)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_local_effect_rows, "
            CREATE TABLE _coven_local_effect_rows (
                effect INTEGER NOT NULL REFERENCES _coven_local_effects(id) ON DELETE CASCADE,
                ordinal INTEGER NOT NULL CHECK(ordinal>=0),
                table_name TEXT NOT NULL,
                kind TEXT NOT NULL CHECK(kind IN ('row','null')),
                rowid INTEGER,
                columns BLOB NOT NULL,
                wait_table TEXT,
                wait_values BLOB,
                CHECK((wait_table IS NULL) = (wait_values IS NULL)),
                PRIMARY KEY(effect,ordinal)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_local_effect_rows_wait ON _coven_local_effect_rows(wait_table) WHERE wait_table IS NOT NULL;
        ");
        $visit!(_coven_store_log, "
            CREATE TABLE _coven_store_log (
                device BLOB NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8 AND number>x'0000000000000000'),
                record BLOB NOT NULL,
                timestamp BLOB NOT NULL CHECK(length(timestamp)=16),
                author_view BLOB NOT NULL,
                stored_at BLOB NOT NULL CHECK(length(stored_at)=13),
                outcome TEXT NOT NULL CHECK(outcome IN ('kept','late','beaten','target-member','target-device','target-circle','admin','authority')),
                beaten_device BLOB CHECK(length(beaten_device)=8),
                beaten_number BLOB CHECK(length(beaten_number)=8),
                target BLOB,
                CHECK((author_view IN (x'0400',x'0401'))=(outcome='late')),
                CHECK((outcome='target-member' AND target IS NOT NULL AND length(target)=32)
                   OR (outcome='target-device' AND target IS NOT NULL AND length(target)=8)
                   OR (outcome='target-circle' AND target IS NOT NULL AND length(target)=16)
                   OR (outcome NOT IN ('target-member','target-device','target-circle') AND target IS NULL)),
                PRIMARY KEY(device,number),
                FOREIGN KEY(beaten_device,beaten_number) REFERENCES _coven_store_log(device,number),
                CHECK((outcome='beaten' AND beaten_device IS NOT NULL AND beaten_number IS NOT NULL)
                   OR (outcome!='beaten' AND beaten_device IS NULL AND beaten_number IS NULL))
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_store_log_timestamp ON _coven_store_log(timestamp);
        ");
        $visit!(_coven_finality, "
            CREATE TABLE _coven_finality (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                horizon BLOB CHECK(horizon IS NULL OR length(horizon)=12)
            ) STRICT;
            INSERT INTO _coven_finality VALUES(1,NULL);
        ");
        $visit!(_coven_device_reset, "
            CREATE TABLE _coven_device_reset (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                old_device BLOB NOT NULL CHECK(length(old_device)=8),
                new_device BLOB NOT NULL CHECK(length(new_device)=8),
                reason TEXT NOT NULL CHECK(reason IN ('identity-mismatch','storage-ahead','slot-mismatch','replaced','database-damage'))
            ) STRICT;
        ");
        $visit!(_coven_catch_up, "
            CREATE TABLE _coven_catch_up (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                storage_time BLOB CHECK(storage_time IS NULL OR length(storage_time)=12)
            ) STRICT;
            INSERT INTO _coven_catch_up VALUES(1,NULL);
        ");
        $visit!(_coven_log_inputs, "
            CREATE TABLE _coven_log_inputs (
                kind TEXT NOT NULL CHECK(kind IN ('entry','key')),
                writer BLOB NOT NULL CHECK(length(writer)=8),
                number BLOB NOT NULL CHECK(length(number)=8 AND number>x'0000000000000000'),
                observation BLOB NOT NULL,
                bytes BLOB NOT NULL,
                PRIMARY KEY(kind,writer,number)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_log_downloads, "
            CREATE TABLE _coven_log_downloads (
                kind TEXT NOT NULL CHECK(kind IN ('entry','key')),
                writer BLOB NOT NULL CHECK(length(writer)=8),
                number BLOB NOT NULL CHECK(length(number)=8),
                PRIMARY KEY(kind,writer)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_store_log_uploads, "
            CREATE TABLE _coven_store_log_uploads (
                device BLOB NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8 AND number>x'0000000000000000'),
                record BLOB NOT NULL,
                timestamp BLOB NOT NULL CHECK(length(timestamp)=16),
                sealing_key BLOB NOT NULL CHECK(length(sealing_key)=16),
                PRIMARY KEY(device,number)
            ) STRICT, WITHOUT ROWID;
            CREATE UNIQUE INDEX _coven_one_store_log_upload ON _coven_store_log_uploads ((1));
        ");
        $visit!(_coven_key_publications, "
            CREATE TABLE _coven_key_publications (
                writer BLOB PRIMARY KEY NOT NULL CHECK(length(writer)=8),
                number BLOB NOT NULL CHECK(length(number)=8 AND number>x'0000000000000000')
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_key_uploads, "
            CREATE TABLE _coven_key_uploads (
                writer BLOB NOT NULL CHECK(length(writer)=8),
                number BLOB NOT NULL CHECK(length(number)=8 AND number>x'0000000000000000'),
                bytes BLOB NOT NULL,
                before_device BLOB CHECK(length(before_device)=8),
                before_number BLOB CHECK(length(before_number)=8 AND before_number>x'0000000000000000'),
                PRIMARY KEY(writer,number),
                CHECK((before_device IS NULL)=(before_number IS NULL)),
                FOREIGN KEY(before_device,before_number) REFERENCES _coven_store_log_uploads(device,number)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_key_uploads_before ON _coven_key_uploads(before_device,before_number) WHERE before_device IS NOT NULL;
        ");
        $visit!(_coven_members, "
            CREATE TABLE _coven_members (
                member BLOB PRIMARY KEY NOT NULL CHECK(length(member)=32),
                sealing BLOB NOT NULL CHECK(length(sealing)=32),
                access BLOB NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('admin','member')),
                removed INTEGER NOT NULL CHECK(removed IN (0,1))
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
                entry_device BLOB NOT NULL CHECK(length(entry_device)=8),
                entry_number BLOB NOT NULL CHECK(length(entry_number)=8 AND entry_number>x'0000000000000000'),
                FOREIGN KEY(entry_device,entry_number) REFERENCES _coven_store_log(device,number)
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
                entry_device BLOB NOT NULL CHECK(length(entry_device)=8),
                entry_number BLOB NOT NULL CHECK(length(entry_number)=8 AND entry_number>x'0000000000000000'),
                kind TEXT NOT NULL CHECK(kind IN ('schema','reset')),
                version INTEGER,
                audience TEXT NOT NULL,
                snapshot_device BLOB NOT NULL CHECK(length(snapshot_device)=8),
                snapshot_number BLOB NOT NULL CHECK(length(snapshot_number)=8 AND snapshot_number>x'0000000000000000'),
                included BLOB,
                input INTEGER REFERENCES _coven_data_inputs(id),
                applied INTEGER NOT NULL DEFAULT 0 CHECK(applied IN (0,1)),
                CHECK((kind='schema' AND version IS NOT NULL AND version>0)
                   OR (kind='reset' AND version IS NULL)),
                CHECK(applied=0 OR included IS NOT NULL),
                PRIMARY KEY(entry_device,entry_number),
                FOREIGN KEY(entry_device,entry_number) REFERENCES _coven_store_log(device,number)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_boundaries_applied ON _coven_boundaries(applied,kind);
            CREATE INDEX _coven_boundaries_input ON _coven_boundaries(input);
            CREATE INDEX _coven_boundaries_audience ON _coven_boundaries(audience,applied);
        ");
        $visit!(_coven_snapshot_schema, "
            CREATE TABLE _coven_snapshot_schema (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                minimum INTEGER NOT NULL CHECK(minimum>=0),
                publication INTEGER NOT NULL CHECK(publication>=minimum)
            ) STRICT;
            INSERT INTO _coven_snapshot_schema VALUES(1,0,0);
        ");
        $visit!(_coven_loaded_audiences, "
            CREATE TABLE _coven_loaded_audiences (
                audience TEXT PRIMARY KEY NOT NULL
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_fingerprint_leaves, "
            CREATE TABLE _coven_fingerprint_leaves (
                audience TEXT NOT NULL,
                key BLOB NOT NULL CHECK(length(key)=32),
                hash BLOB NOT NULL CHECK(length(hash)=32),
                merge_row BLOB CHECK(merge_row IS NULL OR length(merge_row)=32),
                PRIMARY KEY(audience,key)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_fingerprint_merge ON _coven_fingerprint_leaves(audience,merge_row);
        ");
        $visit!(_coven_fingerprint_sums, "
            CREATE TABLE _coven_fingerprint_sums (
                audience TEXT PRIMARY KEY NOT NULL,
                sum BLOB NOT NULL CHECK(length(sum)=32)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_writes, "
            CREATE TABLE _coven_writes (
                id INTEGER PRIMARY KEY,
                timestamp BLOB NOT NULL UNIQUE CHECK(length(timestamp) = 16),
                number BLOB NOT NULL CHECK(length(number) = 8 AND number > x'0000000000000000'),
                had_read BLOB NOT NULL
            ) STRICT;
            CREATE UNIQUE INDEX _coven_write_position ON _coven_writes({WRITE_DEVICE_SQL}, number);
        ");
        $visit!(_coven_write_headers, "
            CREATE TABLE _coven_write_headers (
                device BLOB NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8 AND number>x'0000000000000000'),
                header BLOB NOT NULL,
                PRIMARY KEY(device,number)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_positions, "
            CREATE TABLE _coven_positions (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8 AND number>x'0000000000000000')
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
        ");
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
        $visit!(_coven_reference_values, "
            CREATE TABLE _coven_reference_values (
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
                retired INTEGER NOT NULL DEFAULT 0 CHECK(retired IN (0,1)),
                replacement_kind TEXT NOT NULL CHECK(replacement_kind IN ('write', 'rules', 'excluded')),
                replaced_by BLOB NOT NULL,
                CHECK((replacement_kind='write') = (column_id IS NOT NULL)),
                CHECK(replacement_kind!='excluded' OR retired=1)
            ) STRICT;
            CREATE INDEX _coven_lost_row ON _coven_lost(audience,table_name,key,generation,column_id);
            CREATE INDEX _coven_lost_app_key ON _coven_lost(table_name,key);
            CREATE UNIQUE INDEX _coven_lost_live_cell ON _coven_lost(table_name,key,audience,column_id,set_by) WHERE retired=0 AND replacement_kind='write';
            CREATE INDEX _coven_lost_removed ON _coven_lost(table_name,key,audience) WHERE retired=0 AND replacement_kind='rules';
            CREATE INDEX _coven_lost_column ON _coven_lost(column_id);
        ");
        $visit!(_coven_uploads, "
            CREATE TABLE _coven_uploads (
                device BLOB NOT NULL CHECK(length(device) = 8),
                number BLOB NOT NULL CHECK(length(number) = 8 AND number > x'0000000000000000'),
                record BLOB NOT NULL,
                sealing_keys BLOB,
                interpretation BLOB,
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
        $visit!(_coven_write_upload_sessions, "
            CREATE TABLE _coven_write_upload_sessions (
                device BLOB NOT NULL CHECK(length(device) = 8),
                number BLOB NOT NULL CHECK(length(number) = 8),
                session BLOB NOT NULL,
                PRIMARY KEY(device, number),
                FOREIGN KEY(device,number) REFERENCES _coven_uploads(device,number) ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_user_files, "
            CREATE TABLE _coven_user_files (
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                column_name TEXT NOT NULL,
                identity BLOB NOT NULL,
                path BLOB NOT NULL,
                size BLOB NOT NULL CHECK(length(size)=8),
                modified_at BLOB NOT NULL CHECK(length(modified_at)=13),
                PRIMARY KEY(table_name,key,column_name)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_file_removals, "
            CREATE TABLE _coven_file_removals (
                path TEXT NOT NULL,
                area TEXT NOT NULL DEFAULT 'files' CHECK(area IN ('files','cache')),
                PRIMARY KEY(path,area)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_device_files, "
            CREATE TABLE _coven_device_files (
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                column_name TEXT NOT NULL,
                identity BLOB NOT NULL,
                path TEXT NOT NULL,
                PRIMARY KEY(table_name,key,column_name)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_device_files_path ON _coven_device_files(path);
        ");
        // Each S3 key named for deletion: the open deletion operation, which
        // carries its pending notice, or NULL once deletion was confirmed (§13).
        $visit!(_coven_access_keys, "
            CREATE TABLE _coven_access_keys (
                access_key_id TEXT PRIMARY KEY NOT NULL,
                operation INTEGER UNIQUE
            ) WITHOUT ROWID;
        ");
        $visit!(_coven_file_uploads, "
            CREATE TABLE _coven_file_uploads (
                id INTEGER PRIMARY KEY,
                reference BLOB NOT NULL UNIQUE,
                device BLOB NOT NULL CHECK(length(device)=8),
                file BLOB NOT NULL CHECK(length(file)=16),
                queued_at BLOB NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
                last_attempt_at BLOB,
                failure BLOB,
                session BLOB,
                UNIQUE(device,file)
            ) STRICT;
        ");
        $visit!(_coven_file_chunks, "
            CREATE TABLE _coven_file_chunks (
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                column_name TEXT NOT NULL,
                identity BLOB NOT NULL,
                chunk INTEGER NOT NULL CHECK(chunk>=0),
                hash BLOB NOT NULL CHECK(length(hash)=32),
                PRIMARY KEY(table_name,key,column_name,chunk)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_file_upload_chunks, "
            CREATE TABLE _coven_file_upload_chunks (
                upload INTEGER NOT NULL REFERENCES _coven_file_uploads(id) ON DELETE CASCADE,
                chunk INTEGER NOT NULL CHECK(chunk>=0),
                hash BLOB NOT NULL CHECK(length(hash)=32),
                PRIMARY KEY(upload,chunk)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_cache, "
            CREATE TABLE _coven_cache (
                namespace TEXT NOT NULL,
                file_id TEXT NOT NULL,
                chunk INTEGER NOT NULL CHECK(chunk>={CACHE_COMPLETE_FILE}),
                path TEXT NOT NULL UNIQUE,
                size INTEGER NOT NULL CHECK(size>=0),
                last_read INTEGER NOT NULL,
                pinned INTEGER NOT NULL CHECK(pinned IN (0,1)) CHECK(pinned=0 OR chunk={CACHE_COMPLETE_FILE}),
                checked_hash BLOB CHECK((chunk={CACHE_COMPLETE_FILE})=(checked_hash IS NOT NULL)) CHECK(checked_hash IS NULL OR length(checked_hash)=32),
                PRIMARY KEY(namespace,file_id,chunk)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_cache_lru ON _coven_cache(namespace,pinned,last_read);
            CREATE INDEX _coven_cache_recency ON _coven_cache(last_read);
        ");
        $visit!(_coven_cache_budgets, "
            CREATE TABLE _coven_cache_budgets (
                namespace TEXT PRIMARY KEY NOT NULL,
                bytes BLOB NOT NULL CHECK(length(bytes)=8)
            ) STRICT, WITHOUT ROWID;
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
    // Source hashes have the same lifetime as their local file facts. Both
    // deletion and replacement remove them in that transaction, including
    // changes made while applying remote writes or restoring a snapshot.
    for source in ["user", "device"] {
        for (event, suffix) in [("DELETE", "delete"), ("UPDATE OF identity", "replace")] {
            sql.push_str(&format!(
                "CREATE TRIGGER _coven_{source}_file_chunks_{suffix}
                 AFTER {event} ON _coven_{source}_files BEGIN
                 DELETE FROM _coven_file_chunks
                 WHERE table_name=OLD.table_name AND key=OLD.key
                   AND column_name=OLD.column_name AND identity=OLD.identity;
                 END;"
            ));
        }
    }
    sql
}

// Independently committed remote input, owned by the same store lifetime.
macro_rules! coven_input_tables {
    ($visit:ident) => {
        $visit!(
            _coven_input_objects,
            "
    CREATE TABLE _coven_input_objects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        path TEXT NOT NULL UNIQUE,
        identity BLOB NOT NULL,
        observation BLOB NOT NULL,
        released INTEGER NOT NULL DEFAULT 0 CHECK(released IN (0,1)),
        received INTEGER NOT NULL DEFAULT 0 CHECK(received IN (0,1)),
        metadata BLOB,
        files_complete INTEGER CHECK(files_complete IN (0,1))
    ) STRICT;
        "
        );
        $visit!(
            _coven_input_ranges,
            "
    CREATE TABLE _coven_input_ranges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        object INTEGER NOT NULL REFERENCES _coven_input_objects(id) ON DELETE CASCADE,
        start INTEGER NOT NULL CHECK(start>=0),
        end INTEGER NOT NULL CHECK(end>start),
        bytes BLOB NOT NULL CHECK(length(bytes)=end-start),
        UNIQUE(object,start)
    ) STRICT;
        "
        );
        $visit!(
            _coven_input_files,
            "
    CREATE TABLE _coven_input_files (
        object INTEGER NOT NULL REFERENCES _coven_input_objects(id) ON DELETE CASCADE,
        device BLOB NOT NULL CHECK(length(device)=8),
        file BLOB NOT NULL CHECK(length(file)=16),
        PRIMARY KEY(object,device,file)
    ) STRICT, WITHOUT ROWID;
        "
        );
        $visit!(
            _coven_input_budget,
            "
    CREATE TABLE _coven_input_budget (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        maximum INTEGER NOT NULL CHECK(maximum>=0),
        retained INTEGER NOT NULL CHECK(retained>=0),
        evicted INTEGER NOT NULL CHECK(evicted>=0)
    ) STRICT;
    INSERT INTO _coven_input_budget VALUES(1,67108864,0,0);
    CREATE TRIGGER _coven_input_range_added AFTER INSERT ON _coven_input_ranges BEGIN
        UPDATE _coven_input_budget SET retained=retained+length(NEW.bytes) WHERE singleton=1;
    END;
    CREATE TRIGGER _coven_input_range_removed AFTER DELETE ON _coven_input_ranges BEGIN
        UPDATE _coven_input_budget SET retained=retained-length(OLD.bytes) WHERE singleton=1;
    END;
    PRAGMA application_id=1;
        "
        );
    };
}

pub(crate) fn initial_input_schema() -> String {
    let mut sql = String::new();
    macro_rules! append {
        ($table:ident, $sql:literal) => {
            sql.push_str($sql);
        };
    }
    coven_input_tables!(append);
    sql
}

#[cfg(test)]
#[path = "internal_schema_tests.rs"]
mod tests;
