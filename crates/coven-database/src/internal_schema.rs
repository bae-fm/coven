//! Coven's local schema. `application_id` versions these tables independently
//! of the application's `user_version`. Each internal migration is atomic.
//!
//! Unsigned merge counters are big-endian eight-byte blobs. SQLite's signed
//! integer range must not truncate device ids, generations or write numbers.
//! Present displayed values live in app tables; a written value that reads
//! differently (a NULL-substituted reference, a generated value recomputed from
//! merged inputs) is kept by cell. Metadata and lost values use coven-format's snapshot
//! field encodings inside blobs.
//! Loss targets name their row directly: a write excluded by a schema change
//! or reset need not have an accepted generation in `_coven_rows`.
//! An upload's `record` is one plaintext value: the write header frame followed
//! by its audience parts' row-frame streams. Its first attempt fixes the format
//! and key ids in `sealing_keys`, encoded as the D9 write prefix. A queued store-log
//! entry keeps its format in `record`'s frame prefix. No ciphertext is kept for either.
//! `_coven_uploads` retains its rowid for queue order and incremental BLOB reads.
//! Cell keys start with the row; column-wide cell edits occur only in migrations.
//! Original-change admission is NULL until judged here, then this device's
//! `ChangeVerdict`. Consumed positions do not establish admission.
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
#[macro_use]
#[path = "internal_schema_ownership.rs"]
mod ownership;

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
                stored_at BLOB NOT NULL CHECK(length(stored_at)=12),
                outcome TEXT NOT NULL CHECK(outcome IN ('kept','late','beaten','target-member','target-device','target-circle','admin','authority','keys')),
                beaten_device BLOB CHECK(length(beaten_device)=8),
                beaten_number BLOB CHECK(length(beaten_number)=8),
                target BLOB,
                retires_device BLOB CHECK(length(retires_device)=8),
                retires_member BLOB CHECK(length(retires_member)=16),
                keys_member BLOB CHECK(length(keys_member)=16),
                keys_signing BLOB CHECK(length(keys_signing)=32),
                registers_device BLOB CHECK(length(registers_device)=8),
                CHECK(retires_device IS NULL OR retires_member IS NULL),
                CHECK((keys_member IS NULL)=(keys_signing IS NULL)),
                CHECK(keys_member IS NULL OR (retires_device IS NULL AND retires_member IS NULL)),
                CHECK((author_view IN (x'0400',x'0401'))=(outcome='late')),
                CHECK((outcome='target-member' AND target IS NOT NULL AND length(target)=16)
                   OR (outcome='target-device' AND target IS NOT NULL AND length(target)=8)
                   OR (outcome='target-circle' AND target IS NOT NULL AND length(target)=16)
                   OR (outcome NOT IN ('target-member','target-device','target-circle') AND target IS NULL)),
                PRIMARY KEY(device,number),
                FOREIGN KEY(beaten_device,beaten_number) REFERENCES _coven_store_log(device,number),
                CHECK((outcome='beaten' AND beaten_device IS NOT NULL AND beaten_number IS NOT NULL)
                   OR (outcome!='beaten' AND beaten_device IS NULL AND beaten_number IS NULL))
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_store_log_timestamp ON _coven_store_log(timestamp);
            CREATE INDEX _coven_store_log_retires_device ON _coven_store_log(retires_device) WHERE retires_device IS NOT NULL;
            CREATE INDEX _coven_store_log_retires_member ON _coven_store_log(retires_member) WHERE retires_member IS NOT NULL;
            CREATE INDEX _coven_store_log_registers_device ON _coven_store_log(registers_device) WHERE registers_device IS NOT NULL;
            CREATE INDEX _coven_store_log_keys_member ON _coven_store_log(keys_member) WHERE keys_member IS NOT NULL;
        ");
        // A received write's first complete publication time, the signing key
        // that authenticated it in its recorded view, and its window verdict,
        // kept while a retirement it depends on could still change.
        $visit!(_coven_write_landings, "
            CREATE TABLE _coven_write_landings (
                device BLOB NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8 AND number>x'0000000000000000'),
                stored_at BLOB NOT NULL CHECK(length(stored_at)=12),
                header BLOB NOT NULL,
                signer BLOB NOT NULL CHECK(length(signer)=32),
                late INTEGER NOT NULL CHECK(late IN (0,1)),
                PRIMARY KEY(device,number)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_write_landing_parts, "
            CREATE TABLE _coven_write_landing_parts (
                device BLOB NOT NULL,
                number BLOB NOT NULL,
                audience TEXT NOT NULL,
                PRIMARY KEY(device,number,audience),
                FOREIGN KEY(device,number) REFERENCES _coven_write_landings(device,number) ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_finality, "
            CREATE TABLE _coven_finality (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                horizon BLOB CHECK(horizon IS NULL OR length(horizon)=12)
            ) STRICT;
            INSERT INTO _coven_finality VALUES(1,NULL);
        ");
        // §6's drain evidence: the qualifying storage time F of the finality
        // observation that found a kept retiring entry final, then the mark of a
        // completed drain. Both go when replay stops keeping that entry.
        $visit!(_coven_retirement_finality, "
            CREATE TABLE _coven_retirement_finality (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8),
                entry_device BLOB NOT NULL CHECK(length(entry_device)=8),
                entry_number BLOB NOT NULL CHECK(length(entry_number)=8),
                final_at BLOB NOT NULL CHECK(length(final_at)=12),
                UNIQUE(device,entry_device,entry_number),
                FOREIGN KEY(entry_device,entry_number) REFERENCES _coven_store_log(device,number)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_retirement_finality_entry ON _coven_retirement_finality(entry_device,entry_number);
        ");
        $visit!(_coven_drained_devices, "
            CREATE TABLE _coven_drained_devices (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8),
                entry_device BLOB NOT NULL,
                entry_number BLOB NOT NULL,
                FOREIGN KEY(device,entry_device,entry_number)
                    REFERENCES _coven_retirement_finality(device,entry_device,entry_number) ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_device_reset, "
            CREATE TABLE _coven_device_reset (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                old_device BLOB NOT NULL CHECK(length(old_device)=8),
                new_device BLOB NOT NULL CHECK(length(new_device)=8),
                reason TEXT NOT NULL CHECK(reason IN ('identity-mismatch','storage-ahead','slot-mismatch','replaced','database-damage'))
            ) STRICT;
        ");
        // §15's historical coverage: per audience and writer, the earliest
        // landing of a checked counting snapshot reaching each position. It
        // outlives the snapshots themselves (snapshot_witnesses.rs).
        $visit!(_coven_snapshot_witnesses, "
            CREATE TABLE _coven_snapshot_witnesses (
                audience TEXT NOT NULL,
                device BLOB NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8 AND number>x'0000000000000000'),
                landed BLOB NOT NULL CHECK(length(landed)=12),
                PRIMARY KEY(audience,device,number)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_snapshot_reservation, "
            CREATE TABLE _coven_snapshot_reservation (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                number INTEGER NOT NULL CHECK(number>=0)
            ) STRICT;
            INSERT INTO _coven_snapshot_reservation VALUES(1,0);
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
                member BLOB PRIMARY KEY NOT NULL CHECK(length(member)=16),
                signing BLOB NOT NULL CHECK(length(signing)=32),
                sealing BLOB NOT NULL CHECK(length(sealing)=32),
                access BLOB NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('admin','member')),
                removed INTEGER NOT NULL CHECK(removed IN (0,1))
            ) STRICT, WITHOUT ROWID;
        ");
        // Every pair replay's kept creation, additions and member-key
        // replacements bound (§9). Replay rewrites it with the member tables.
        // Freshness is judged in each entry's recorded view, so concurrent
        // kept bindings can still share a key: neither key is unique here.
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
                publication INTEGER NOT NULL CHECK(publication>=0)
            ) STRICT;
            INSERT INTO _coven_snapshot_schema VALUES(1,0,0);
        ");
        $visit!(_coven_raise_captures, "
            CREATE TABLE _coven_raise_captures (
                audience TEXT PRIMARY KEY NOT NULL,
                version INTEGER NOT NULL CHECK(version>0),
                through BLOB NOT NULL CHECK(length(through)=8 AND through>x'0000000000000000'),
                schema_version INTEGER NOT NULL,
                writes BLOB NOT NULL,
                store_log BLOB NOT NULL,
                counts BLOB NOT NULL CHECK(length(counts)=64)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_raise_capture_frames, "
            CREATE TABLE _coven_raise_capture_frames (
                audience TEXT NOT NULL REFERENCES _coven_raise_captures(audience) ON DELETE CASCADE,
                sequence INTEGER NOT NULL CHECK(sequence>0),
                frame BLOB NOT NULL,
                PRIMARY KEY(audience,sequence)
            ) STRICT, WITHOUT ROWID;
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
            CREATE INDEX _coven_authored_changes_audience ON _coven_authored_changes(audience,write_id,table_name,key);
        ");
        coven_ownership_tables!($visit);
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
        // Cache ranges whose bytes are being written with no writer held;
        // opening makes the abandoned ones removals (§16.6).
        $visit!(_coven_cache_writes, "
            CREATE TABLE _coven_cache_writes (path TEXT NOT NULL PRIMARY KEY) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_device_files, "
            CREATE TABLE _coven_device_files (
                device BLOB NOT NULL,
                file BLOB NOT NULL,
                path TEXT NOT NULL,
                PRIMARY KEY(device,file),
                FOREIGN KEY(device,file) REFERENCES _coven_file_uploads(device,file)
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
                original BLOB,
                modified_at BLOB CHECK(modified_at IS NULL OR length(modified_at)=13),
                queued_at BLOB NOT NULL,
                session BLOB,
                UNIQUE(device,file),
                CHECK((original IS NULL)=(modified_at IS NULL))
            ) STRICT;
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
        // Fixed file paths awaiting a storage-deletion decision (§16.5): the
        // transaction ending a path's last local protection adds it, so
        // retention never lists files/.
        $visit!(_coven_file_candidates, "
            CREATE TABLE _coven_file_candidates (
                device BLOB NOT NULL CHECK(length(device)=8),
                file BLOB NOT NULL CHECK(length(file)=16),
                PRIMARY KEY(device,file)
            ) STRICT, WITHOUT ROWID;
            CREATE VIEW _coven_file_protection(device,file) AS
                SELECT device,file FROM _coven_row_file_refs UNION ALL SELECT device,file FROM _coven_input_file_refs
                UNION ALL SELECT device,file FROM _coven_queued_file_refs UNION ALL SELECT device,file FROM _coven_file_uploads;
            CREATE TRIGGER _coven_row_file_ref_released AFTER DELETE ON _coven_row_file_refs BEGIN
                INSERT INTO _coven_file_candidates SELECT OLD.device,OLD.file WHERE NOT EXISTS(SELECT 1 FROM _coven_file_protection WHERE device=OLD.device AND file=OLD.file) ON CONFLICT DO NOTHING;
            END;
            CREATE TRIGGER _coven_input_file_ref_released AFTER DELETE ON _coven_input_file_refs BEGIN
                INSERT INTO _coven_file_candidates SELECT OLD.device,OLD.file WHERE NOT EXISTS(SELECT 1 FROM _coven_file_protection WHERE device=OLD.device AND file=OLD.file) ON CONFLICT DO NOTHING;
            END;
            CREATE TRIGGER _coven_queued_file_ref_released AFTER DELETE ON _coven_queued_file_refs BEGIN
                INSERT INTO _coven_file_candidates SELECT OLD.device,OLD.file WHERE NOT EXISTS(SELECT 1 FROM _coven_file_protection WHERE device=OLD.device AND file=OLD.file) ON CONFLICT DO NOTHING;
            END;
            CREATE TRIGGER _coven_file_upload_released AFTER DELETE ON _coven_file_uploads BEGIN
                INSERT INTO _coven_file_candidates SELECT OLD.device,OLD.file WHERE NOT EXISTS(SELECT 1 FROM _coven_file_protection WHERE device=OLD.device AND file=OLD.file) ON CONFLICT DO NOTHING;
            END;
        ");
        // A removed uploader's file folder, listed once after its access was
        // confirmed taken away, and the kept removal that justified it. Replay
        // deletes it once that removal is no longer kept, and the transaction
        // that records any of its access restored deletes it too: a later
        // cut-off scans again (§16.5). The paths found are candidates.
        $visit!(_coven_file_scans, "
            CREATE TABLE _coven_file_scans (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8),
                entry_device BLOB NOT NULL CHECK(length(entry_device)=8),
                entry_number BLOB NOT NULL CHECK(length(entry_number)=8),
                FOREIGN KEY(entry_device,entry_number) REFERENCES _coven_store_log(device,number)
            ) STRICT, WITHOUT ROWID;
        ");
        // The provider accounts and S3 keys whose confirmed removal a scan
        // followed (§13).
        $visit!(_coven_file_scan_access, "
            CREATE TABLE _coven_file_scan_access (
                device BLOB NOT NULL REFERENCES _coven_file_scans(device) ON DELETE CASCADE,
                kind TEXT NOT NULL CHECK(kind IN ('account','key')),
                name TEXT NOT NULL,
                PRIMARY KEY(device,kind,name)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX _coven_file_scan_access_name ON _coven_file_scan_access(kind,name);
            CREATE TRIGGER _coven_file_scan_key_restored AFTER UPDATE ON _coven_access_keys
            WHEN NEW.operation IS NOT NULL BEGIN
                DELETE FROM _coven_file_scans WHERE device IN (SELECT device FROM _coven_file_scan_access WHERE kind='key' AND name=NEW.access_key_id);
            END;
            CREATE TRIGGER _coven_file_scan_key_forgotten AFTER DELETE ON _coven_access_keys BEGIN
                DELETE FROM _coven_file_scans WHERE device IN (SELECT device FROM _coven_file_scan_access WHERE kind='key' AND name=OLD.access_key_id);
            END;
        ");
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
        ");
        // Eager-fill reservations: the requested fixed references, kept
        // until the whole set is cached, and the set's cancellation (§16.4).
        $visit!(_coven_eager_requests, "
            CREATE TABLE _coven_eager_requests (
                namespace TEXT NOT NULL,
                file_id TEXT NOT NULL,
                reference BLOB NOT NULL,
                PRIMARY KEY(namespace,file_id)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_eager_cancel, "
            CREATE TABLE _coven_eager_cancel (
                cancelled INTEGER PRIMARY KEY CHECK(cancelled=1)
            ) STRICT;
        ");
        // Each namespace's budget, if set, and the cached bytes the cache
        // records hold, maintained with them so a budget check needs no sum.
        $visit!(_coven_cache_budgets, "
            CREATE TABLE _coven_cache_budgets (
                namespace TEXT PRIMARY KEY NOT NULL,
                bytes BLOB CHECK(bytes IS NULL OR length(bytes)=8),
                total INTEGER NOT NULL DEFAULT 0 CHECK(total>=0)
            ) STRICT, WITHOUT ROWID;
            CREATE TRIGGER _coven_cache_added AFTER INSERT ON _coven_cache BEGIN
                INSERT INTO _coven_cache_budgets(namespace,total) VALUES(NEW.namespace,NEW.size)
                ON CONFLICT(namespace) DO UPDATE SET total=total+NEW.size;
            END;
            CREATE TRIGGER _coven_cache_removed AFTER DELETE ON _coven_cache BEGIN
                UPDATE _coven_cache_budgets SET total=total-OLD.size WHERE namespace=OLD.namespace;
            END;
        ");
        // Eager rows' fixed file identities as last noted, so only a changed
        // reference becomes eager work, and that work until it is settled.
        $visit!(_coven_eager_rows, "
            CREATE TABLE _coven_eager_rows (
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                file TEXT NOT NULL,
                PRIMARY KEY(table_name,key)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(_coven_eager_work, "
            CREATE TABLE _coven_eager_work (
                namespace TEXT NOT NULL,
                file_id TEXT NOT NULL,
                reference BLOB NOT NULL,
                PRIMARY KEY(namespace,file_id)
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
            -- Access no longer confirmed taken away ends the cut-off a file
            -- folder scan followed, in the transaction that records it (§16.5).
            CREATE TRIGGER _coven_file_scan_account_restored AFTER UPDATE ON _coven_provider_access
            WHEN NOT (NEW.intended=0 AND NEW.granted IS 0 AND NEW.operation IS NULL) BEGIN
                DELETE FROM _coven_file_scans WHERE device IN (SELECT device FROM _coven_file_scan_access WHERE kind='account' AND name=NEW.account);
            END;
            CREATE TRIGGER _coven_file_scan_account_forgotten AFTER DELETE ON _coven_provider_access BEGIN
                DELETE FROM _coven_file_scans WHERE device IN (SELECT device FROM _coven_file_scan_access WHERE kind='account' AND name=OLD.account);
            END;
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
        // member's keys (§10). Its row goes with the operation's.
        $visit!(_coven_write_pause, "
            CREATE TABLE _coven_write_pause (
                paused INTEGER PRIMARY KEY CHECK(paused=1),
                operation INTEGER NOT NULL UNIQUE
                    REFERENCES _coven_operations(id) ON DELETE CASCADE
            ) STRICT;
        ");
        // One persisted retry wait, T and W, per waiting operation, queued
        // upload, eager file download, joiner poll or provider cooldown
        // (§16.5). Work rows take their waits with them; a download's work
        // is its file's pending record.
        $visit!(_coven_retries, "
            CREATE TABLE _coven_retries (
                id INTEGER PRIMARY KEY,
                scope TEXT UNIQUE CHECK(scope IN ('join','provider')),
                operation INTEGER UNIQUE REFERENCES _coven_operations(id) ON DELETE CASCADE,
                upload INTEGER UNIQUE REFERENCES _coven_file_uploads(id) ON DELETE CASCADE,
                download BLOB UNIQUE CHECK(download IS NULL OR length(download)=24),
                pending_subject BLOB,
                pending_reporter BLOB,
                failures INTEGER NOT NULL CHECK(failures>=0),
                not_before BLOB NOT NULL CHECK(length(not_before)=13),
                wait INTEGER NOT NULL CHECK(wait>=0),
                UNIQUE(pending_subject,pending_reporter),
                FOREIGN KEY(pending_subject,pending_reporter)
                    REFERENCES _coven_pending(subject,reporter) ON DELETE CASCADE,
                CHECK((download IS NULL)=(pending_subject IS NULL)
                    AND (download IS NULL)=(pending_reporter IS NULL)),
                CHECK((scope IS NOT NULL)+(operation IS NOT NULL)+(upload IS NOT NULL)
                    +(download IS NOT NULL)=1)
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
    sql
}

#[path = "internal_schema_input.rs"]
mod input;
pub(crate) use input::initial_input_schema;

#[cfg(test)]
#[path = "internal_schema_tests.rs"]
mod tests;
