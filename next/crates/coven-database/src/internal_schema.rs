//! Coven's local schema. `application_id` versions these tables independently
//! of the application's `user_version`. Each internal migration is atomic.
//!
//! Unsigned merge counters are big-endian eight-byte blobs. SQLite's signed
//! integer range must not truncate device ids, generations or write numbers.
//! Present displayed values live in app tables; differing written references
//! are kept by cell. Metadata and lost values use coven-format's snapshot
//! field encodings inside blobs.
//! Loss targets name their row directly: a write excluded by a schema change
//! or reset need not have an accepted generation in `coven_rows`.
//! An upload's `record` is one plaintext value: the write header frame followed
//! by its audience parts' row-frame streams. Its first attempt fixes
//! `sealed_bytes` in a separate `coven_upload_seals` row, so both values fit.

pub(crate) const VERSION: u32 = 1;

// This is also the ownership checker's source of reserved table names.
macro_rules! coven_tables {
    ($visit:ident) => {
        $visit!(coven_store_log, "
            CREATE TABLE coven_store_log (
                device BLOB NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8 AND number>x'0000000000000000'),
                record BLOB NOT NULL,
                author_view BLOB NOT NULL,
                outcome TEXT NOT NULL CHECK(outcome IN ('kept','beaten','target','admin','authority','keys')),
                beaten_device BLOB CHECK(length(beaten_device)=8),
                beaten_number BLOB CHECK(length(beaten_number)=8),
                PRIMARY KEY(device,number),
                FOREIGN KEY(beaten_device,beaten_number) REFERENCES coven_store_log(device,number),
                CHECK((outcome='beaten' AND beaten_device IS NOT NULL AND beaten_number IS NOT NULL)
                   OR (outcome!='beaten' AND beaten_device IS NULL AND beaten_number IS NULL))
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_store_log_uploads, "
            CREATE TABLE coven_store_log_uploads (
                device BLOB NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8 AND number>x'0000000000000000'),
                record BLOB NOT NULL,
                sealed_bytes BLOB NOT NULL,
                PRIMARY KEY(device,number)
            ) STRICT, WITHOUT ROWID;
            CREATE UNIQUE INDEX coven_one_store_log_upload ON coven_store_log_uploads ((1));
        ");
        $visit!(coven_store_log_key_uploads, "
            CREATE TABLE coven_store_log_key_uploads (
                device BLOB NOT NULL,
                number BLOB NOT NULL,
                path TEXT NOT NULL,
                bytes BLOB NOT NULL,
                PRIMARY KEY(device,number,path),
                FOREIGN KEY(device,number) REFERENCES coven_store_log_uploads(device,number) ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_key_uploads, "
            CREATE TABLE coven_key_uploads (
                path TEXT PRIMARY KEY NOT NULL,
                bytes BLOB NOT NULL
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_members, "
            CREATE TABLE coven_members (
                member BLOB PRIMARY KEY NOT NULL CHECK(length(member)=32),
                sealing BLOB NOT NULL CHECK(length(sealing)=32),
                access BLOB NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('admin','member')),
                removed INTEGER NOT NULL CHECK(removed IN (0,1))
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_devices, "
            CREATE TABLE coven_devices (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8),
                member BLOB NOT NULL REFERENCES coven_members(member),
                name TEXT NOT NULL,
                removed INTEGER NOT NULL CHECK(removed IN (0,1))
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_circles, "
            CREATE TABLE coven_circles (
                circle TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                key BLOB NOT NULL CHECK(length(key)=16),
                deleted INTEGER NOT NULL CHECK(deleted IN (0,1))
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_circle_members, "
            CREATE TABLE coven_circle_members (
                circle TEXT NOT NULL REFERENCES coven_circles(circle),
                member BLOB NOT NULL REFERENCES coven_members(member),
                PRIMARY KEY(circle,member)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_store, "
            CREATE TABLE coven_store (
                id TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                key BLOB NOT NULL CHECK(length(key)=16)
            ) STRICT, WITHOUT ROWID;
            CREATE UNIQUE INDEX coven_one_store ON coven_store ((1));
        ");
        $visit!(coven_versions, "
            CREATE TABLE coven_versions (
                audience TEXT NOT NULL,
                kind TEXT NOT NULL CHECK(kind IN ('schema','format')),
                version INTEGER NOT NULL CHECK(version>0),
                snapshot_device BLOB NOT NULL CHECK(length(snapshot_device)=8),
                snapshot_number BLOB NOT NULL CHECK(length(snapshot_number)=8 AND snapshot_number>x'0000000000000000'),
                entry_device BLOB NOT NULL CHECK(length(entry_device)=8),
                entry_number BLOB NOT NULL CHECK(length(entry_number)=8),
                PRIMARY KEY(audience,kind),
                FOREIGN KEY(entry_device,entry_number) REFERENCES coven_store_log(device,number)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_resets, "
            CREATE TABLE coven_resets (
                audience TEXT PRIMARY KEY NOT NULL,
                snapshot_device BLOB NOT NULL CHECK(length(snapshot_device)=8),
                snapshot_number BLOB NOT NULL CHECK(length(snapshot_number)=8 AND snapshot_number>x'0000000000000000')
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_applied_boundaries, "
            CREATE TABLE coven_applied_boundaries (
                id INTEGER PRIMARY KEY,
                cause BLOB NOT NULL,
                audience TEXT NOT NULL,
                included BLOB NOT NULL,
                UNIQUE(cause,audience)
            ) STRICT;
        ");
        $visit!(coven_snapshot_schema, "
            CREATE TABLE coven_snapshot_schema (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                minimum INTEGER NOT NULL CHECK(minimum>=0),
                publication INTEGER NOT NULL CHECK(publication>=minimum)
            ) STRICT;
            INSERT INTO coven_snapshot_schema VALUES(1,0,0);
        ");
        $visit!(coven_loaded_audiences, "
            CREATE TABLE coven_loaded_audiences (
                audience TEXT PRIMARY KEY NOT NULL
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_excluded_writes, "
            CREATE TABLE coven_excluded_writes (
                id INTEGER PRIMARY KEY,
                audience TEXT NOT NULL,
                device BLOB NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8),
                header BLOB NOT NULL,
                cause BLOB NOT NULL,
                UNIQUE(audience,device,number)
            ) STRICT;
        ");
        $visit!(coven_excluded_rows, "
            CREATE TABLE coven_excluded_rows (
                write_id INTEGER NOT NULL REFERENCES coven_excluded_writes(id) ON DELETE CASCADE,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                record BLOB NOT NULL,
                PRIMARY KEY(write_id,table_name,key)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_fingerprint_leaves, "
            CREATE TABLE coven_fingerprint_leaves (
                audience TEXT NOT NULL,
                key BLOB NOT NULL CHECK(length(key)=32),
                hash BLOB NOT NULL CHECK(length(hash)=32),
                PRIMARY KEY(audience,key)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_fingerprint_sums, "
            CREATE TABLE coven_fingerprint_sums (
                audience TEXT PRIMARY KEY NOT NULL,
                sum BLOB NOT NULL CHECK(length(sum)=32)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_writes, "
            CREATE TABLE coven_writes (
                id INTEGER PRIMARY KEY,
                timestamp BLOB NOT NULL UNIQUE CHECK(length(timestamp) = 16),
                number BLOB NOT NULL CHECK(length(number) = 8 AND number > x'0000000000000000'),
                had_read BLOB NOT NULL
            ) STRICT;
            CREATE UNIQUE INDEX coven_write_position ON coven_writes(substr(timestamp, 9, 8), number);
        ");
        $visit!(coven_positions, "
            CREATE TABLE coven_positions (
                device BLOB PRIMARY KEY NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8 AND number>x'0000000000000000')
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_columns, "
            CREATE TABLE coven_columns (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                UNIQUE(table_name, column_name)
            ) STRICT;
        ");
        $visit!(coven_rows, "
            CREATE TABLE coven_rows (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL,
                generation BLOB NOT NULL CHECK(length(generation) = 8 AND generation > x'0000000000000000'),
                write_id INTEGER NOT NULL REFERENCES coven_writes(id),
                UNIQUE(table_name, key, audience, generation)
            ) STRICT;
            CREATE INDEX coven_rows_audience ON coven_rows(audience,table_name,key,generation);
        ");
        $visit!(coven_cells, "
            CREATE TABLE coven_cells (
                column_id INTEGER NOT NULL REFERENCES coven_columns(id),
                row_id INTEGER NOT NULL REFERENCES coven_rows(id),
                write_id INTEGER NOT NULL REFERENCES coven_writes(id),
                PRIMARY KEY(column_id, row_id)
            ) STRICT;
            CREATE INDEX coven_cells_row ON coven_cells(row_id,column_id);
        ");
        $visit!(coven_foreign_keys, "
            CREATE TABLE coven_foreign_keys (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                identity BLOB NOT NULL,
                UNIQUE(table_name, identity)
            ) STRICT;
        ");
        $visit!(coven_reference_values, "
            CREATE TABLE coven_reference_values (
                column_id INTEGER NOT NULL,
                row_id INTEGER NOT NULL,
                value ANY,
                PRIMARY KEY(column_id,row_id),
                FOREIGN KEY(column_id,row_id) REFERENCES coven_cells(column_id,row_id) ON DELETE CASCADE ON UPDATE CASCADE
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_references, "
            CREATE TABLE coven_references (
                row_id INTEGER NOT NULL REFERENCES coven_rows(id),
                column_id INTEGER NOT NULL REFERENCES coven_columns(id),
                foreign_key_id INTEGER NOT NULL REFERENCES coven_foreign_keys(id),
                parent_table TEXT NOT NULL,
                parent_key BLOB NOT NULL,
                parent_audience TEXT NOT NULL,
                parent_generation BLOB NOT NULL CHECK(length(parent_generation)=8),
                PRIMARY KEY(row_id, column_id, foreign_key_id)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX coven_references_parent ON coven_references(parent_table,parent_key,parent_audience,foreign_key_id,row_id);
            CREATE INDEX coven_references_column ON coven_references(column_id,row_id);
            CREATE INDEX coven_references_key ON coven_references(foreign_key_id,row_id);
        ");
        $visit!(coven_constraints, "
            CREATE TABLE coven_constraints (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                identity BLOB NOT NULL,
                UNIQUE(table_name, identity)
            ) STRICT;
        ");
        $visit!(coven_claims, "
            CREATE TABLE coven_claims (
                row_id INTEGER NOT NULL REFERENCES coven_rows(id),
                constraint_id INTEGER NOT NULL REFERENCES coven_constraints(id),
                audience TEXT NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY(row_id, constraint_id)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX coven_claims_value ON coven_claims(constraint_id,audience,value,row_id);
        ");
        $visit!(coven_lost, "
            CREATE TABLE coven_lost (
                id INTEGER PRIMARY KEY,
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                audience TEXT NOT NULL,
                generation BLOB NOT NULL CHECK(length(generation) = 8),
                column_id INTEGER REFERENCES coven_columns(id),
                value BLOB NOT NULL,
                set_by BLOB NOT NULL,
                retired INTEGER NOT NULL DEFAULT 0 CHECK(retired IN (0,1)),
                read_value BLOB,
                replacement_kind TEXT NOT NULL CHECK(replacement_kind IN ('write', 'rules', 'excluded')),
                replaced_by BLOB NOT NULL,
                CHECK((replacement_kind != 'write' OR column_id IS NOT NULL) AND (replacement_kind != 'rules' OR column_id IS NULL))
            ) STRICT;
            CREATE INDEX coven_lost_row ON coven_lost(table_name,key,audience,generation,column_id);
            CREATE INDEX coven_lost_removed ON coven_lost(table_name,key,audience) WHERE retired=0 AND replacement_kind='rules';
            CREATE INDEX coven_lost_column ON coven_lost(column_id);
        ");
        $visit!(coven_lost_references, "
            CREATE TABLE coven_lost_references (
                loss_id INTEGER NOT NULL REFERENCES coven_lost(id) ON DELETE CASCADE,
                foreign_key_id INTEGER NOT NULL REFERENCES coven_foreign_keys(id),
                parent_table TEXT NOT NULL,
                parent_key BLOB NOT NULL,
                parent_audience TEXT NOT NULL,
                PRIMARY KEY(loss_id,foreign_key_id)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX coven_lost_references_key ON coven_lost_references(foreign_key_id,loss_id);
            CREATE INDEX coven_lost_references_parent ON coven_lost_references(parent_table,parent_key,parent_audience,loss_id);
        ");
        $visit!(coven_uploads, "
            CREATE TABLE coven_uploads (
                device BLOB NOT NULL CHECK(length(device) = 8),
                number BLOB NOT NULL CHECK(length(number) = 8 AND number > x'0000000000000000'),
                record BLOB NOT NULL,
                PRIMARY KEY(device, number)
            ) STRICT;
        ");
        $visit!(coven_upload_seals, "
            CREATE TABLE coven_upload_seals (
                device BLOB NOT NULL CHECK(length(device) = 8),
                number BLOB NOT NULL CHECK(length(number) = 8),
                sealed_bytes BLOB NOT NULL,
                PRIMARY KEY(device, number),
                FOREIGN KEY(device,number) REFERENCES coven_uploads(device,number) ON DELETE CASCADE
            ) STRICT;
        ");
        $visit!(coven_write_upload_sessions, "
            CREATE TABLE coven_write_upload_sessions (
                device BLOB NOT NULL CHECK(length(device) = 8),
                number BLOB NOT NULL CHECK(length(number) = 8),
                session BLOB NOT NULL,
                PRIMARY KEY(device, number),
                FOREIGN KEY(device,number) REFERENCES coven_upload_seals(device,number) ON DELETE CASCADE
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_waiting_writes, "
            CREATE TABLE coven_waiting_writes (
                device BLOB NOT NULL CHECK(length(device)=8),
                number BLOB NOT NULL CHECK(length(number)=8),
                since BLOB NOT NULL CHECK(length(since)=12),
                PRIMARY KEY(device,number)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_user_files, "
            CREATE TABLE coven_user_files (
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
        $visit!(coven_file_removals, "
            CREATE TABLE coven_file_removals (
                path TEXT NOT NULL,
                area TEXT NOT NULL DEFAULT 'files' CHECK(area IN ('files','cache','user')),
                destination BLOB,
                reference BLOB,
                operation INTEGER REFERENCES coven_operations(id) ON DELETE SET NULL,
                CHECK((area='user')=(destination IS NOT NULL)),
                CHECK((area='user')=(reference IS NOT NULL)),
                PRIMARY KEY(area,path)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_device_files, "
            CREATE TABLE coven_device_files (
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                column_name TEXT NOT NULL,
                identity BLOB NOT NULL,
                path TEXT NOT NULL,
                PRIMARY KEY(table_name,key,column_name)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX coven_device_files_path ON coven_device_files(path);
        ");
        $visit!(coven_access_keys_to_delete, "
            CREATE TABLE coven_access_keys_to_delete (
                access_key_id TEXT PRIMARY KEY NOT NULL,
                member BLOB
            );
        ");
        $visit!(coven_file_uploads, "
            CREATE TABLE coven_file_uploads (
                id INTEGER PRIMARY KEY,
                reference BLOB NOT NULL UNIQUE,
                queued_at BLOB NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
                last_attempt_at BLOB,
                failure BLOB,
                path TEXT,
                fixed BLOB,
                session BLOB,
                stored INTEGER NOT NULL DEFAULT 0 CHECK(stored IN (0,1)),
                unused INTEGER NOT NULL DEFAULT 0 CHECK(unused IN (0,1)),
                CHECK((path IS NULL)=(fixed IS NULL)),
                CHECK(session IS NULL OR fixed IS NOT NULL),
                CHECK(stored=0 OR fixed IS NOT NULL),
                CHECK(unused=0 OR stored=1)
            ) STRICT;
        ");
        $visit!(coven_cache, "
            CREATE TABLE coven_cache (
                namespace TEXT NOT NULL,
                file_id TEXT NOT NULL,
                chunk INTEGER NOT NULL CHECK(chunk>=-2),
                path TEXT NOT NULL UNIQUE,
                size INTEGER NOT NULL CHECK(size>=0),
                last_read INTEGER NOT NULL,
                pinned INTEGER NOT NULL CHECK(pinned IN (0,1)) CHECK(pinned=0 OR chunk=-2),
                checked_hash BLOB CHECK((chunk=-2)=(checked_hash IS NOT NULL)) CHECK(checked_hash IS NULL OR length(checked_hash)=32),
                PRIMARY KEY(namespace,file_id,chunk)
            ) STRICT, WITHOUT ROWID;
            CREATE INDEX coven_cache_lru ON coven_cache(namespace,pinned,last_read);
        ");
        $visit!(coven_cache_budgets, "
            CREATE TABLE coven_cache_budgets (
                namespace TEXT PRIMARY KEY NOT NULL,
                bytes BLOB NOT NULL CHECK(length(bytes)=8)
            ) STRICT, WITHOUT ROWID;
        ");
        $visit!(coven_operations, "
            CREATE TABLE coven_operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                last_step INTEGER NOT NULL,
                data BLOB NOT NULL,
                started_by TEXT NOT NULL,
                failure TEXT
            );
        ");
    };
}

pub(crate) fn initial_schema() -> String {
    let mut sql = String::new();
    macro_rules! append {
        ($table:ident, $sql:literal) => {
            sql.push_str($sql);
        };
    }
    coven_tables!(append);
    sql
}

#[cfg(test)]
#[path = "internal_schema_tests.rs"]
mod tests;
