//! Coven's local schema. `application_id` versions these tables independently
//! of the application's `user_version`. Each internal migration is atomic.
//!
//! Unsigned merge counters are big-endian eight-byte blobs. SQLite's signed
//! integer range must not truncate device ids, generations or write numbers.
//! Present values live only in app tables. Metadata and lost values use
//! coven-format's snapshot field encodings inside blobs.
//! Loss targets name their row directly: a write excluded by a schema change
//! or reset need not have an accepted generation in `coven_rows`.
//! An upload's `sealed_bytes` is NULL until its first attempt fixes its bytes.

pub(crate) const VERSION: u32 = 1;

// This is also the ownership checker's source of reserved table names.
macro_rules! coven_tables {
    ($visit:ident) => {
        $visit!(coven_applied_boundaries, "
            CREATE TABLE coven_applied_boundaries (
                id INTEGER PRIMARY KEY,
                cause BLOB NOT NULL UNIQUE,
                audience TEXT,
                included BLOB NOT NULL
            ) STRICT;
        ");
        $visit!(coven_deleted_circles, "
            CREATE TABLE coven_deleted_circles (
                circle TEXT PRIMARY KEY NOT NULL
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
                replacement_kind TEXT NOT NULL CHECK(replacement_kind IN ('write', 'rules', 'excluded')),
                replaced_by BLOB NOT NULL,
                CHECK((replacement_kind != 'write' OR column_id IS NOT NULL) AND (replacement_kind != 'rules' OR column_id IS NULL))
            ) STRICT;
            CREATE INDEX coven_lost_row ON coven_lost(table_name,key,audience,generation,column_id);
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
            CREATE INDEX coven_lost_references_parent ON coven_lost_references(parent_table,parent_key,parent_audience,loss_id);
        ");
        $visit!(coven_uploads, "
            CREATE TABLE coven_uploads (
                device BLOB NOT NULL CHECK(length(device) = 8),
                number BLOB NOT NULL CHECK(length(number) = 8 AND number > x'0000000000000000'),
                record BLOB NOT NULL,
                sealed_bytes BLOB,
                PRIMARY KEY(device, number)
            ) STRICT;
        ");
        $visit!(coven_operations, "
            CREATE TABLE coven_operations (
                id INTEGER PRIMARY KEY,
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
