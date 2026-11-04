//! Checked entry boundaries and their coverage.

macro_rules! coven_write_boundaries_tables {
    ($visit:ident) => {
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
    };
}
