//! Coven's local schema. `application_id` versions these tables independently
//! of the application's `user_version`. Each internal migration is atomic.
//!
//! Unsigned merge counters are big-endian eight-byte blobs. SQLite's signed
//! integer range must not truncate device ids, generations or write numbers.
//! Present values live only in app tables. Metadata and lost values use
//! coven-format's snapshot field encodings inside blobs.
//! Loss targets name their row directly: a write excluded by a schema change
//! or reset need not have an accepted generation in `coven_rows`.

pub(crate) const VERSION: u32 = 1;

// This is also the ownership checker's source of reserved table names.
macro_rules! coven_tables {
    ($visit:ident) => {
        $visit!(coven_writes, "
            CREATE TABLE coven_writes (
                id INTEGER PRIMARY KEY,
                timestamp BLOB NOT NULL UNIQUE CHECK(length(timestamp) = 16),
                number BLOB NOT NULL CHECK(length(number) = 8 AND number > x'0000000000000000'),
                had_read BLOB NOT NULL
            ) STRICT;
            CREATE UNIQUE INDEX coven_write_position ON coven_writes(substr(timestamp, 9, 8), number);
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
        ");
        $visit!(coven_cells, "
            CREATE TABLE coven_cells (
                column_id INTEGER NOT NULL REFERENCES coven_columns(id),
                row_id INTEGER NOT NULL REFERENCES coven_rows(id),
                write_id INTEGER NOT NULL REFERENCES coven_writes(id),
                parents BLOB NOT NULL,
                PRIMARY KEY(column_id, row_id)
            ) STRICT;
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
                replaced_by BLOB NOT NULL
            ) STRICT;
        ");
        $visit!(coven_uploads, "
            CREATE TABLE coven_uploads (
                device BLOB NOT NULL CHECK(length(device) = 8),
                number BLOB NOT NULL CHECK(length(number) = 8 AND number > x'0000000000000000'),
                record BLOB NOT NULL,
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
