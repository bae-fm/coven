//! Each write this device has a record of.

macro_rules! coven_writes_tables {
    ($visit:ident) => {
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
    };
}
