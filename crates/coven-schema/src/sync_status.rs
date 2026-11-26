//! The SQL of the table "coven_sync_status", whose rows coven-database reads and writes
//! (`internal_schema_sync_status.rs`).

/// The sync status table, which the app's triggers may follow.
pub const SYNC_STATUS: &str = "coven_sync_status";

/// The table and its one row, as every store holds them from its creation.
pub(crate) const TABLE: &str = "CREATE TABLE coven_sync_status (
        singleton INTEGER PRIMARY KEY NOT NULL CHECK(singleton=1),
        storage TEXT NOT NULL CHECK(storage IN ('none','signed_out','connected')),
        provider TEXT,
        location TEXT,
        last_pass BLOB,
        moved TEXT CHECK(moved IN ('read','claimed')),
        last_synced INTEGER CHECK(last_synced>=0),
        starts INTEGER NOT NULL CHECK(starts>=0),
        halted INTEGER NOT NULL CHECK(halted IN (0,1)),
        CHECK((storage='none')=(provider IS NULL)),
        CHECK((storage='none')=(location IS NULL))
    ) STRICT;
    INSERT INTO coven_sync_status(singleton,storage,provider,location,last_pass,moved,last_synced,starts,halted)
        VALUES(1,'none',NULL,NULL,NULL,NULL,NULL,0,0);";
