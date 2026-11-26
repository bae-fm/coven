//! The SQL of the table "coven_retry_effect", whose rows coven-database reads and writes
//! (`internal_schema_retry_effect.rs`).

/// The retry effect table, which the app's triggers may follow.
pub const RETRY_EFFECT: &str = "coven_retry_effect";

/// The table and its one row, as every store holds them from its creation.
pub(crate) const TABLE: &str = "CREATE TABLE coven_retry_effect (
        singleton INTEGER PRIMARY KEY NOT NULL CHECK(singleton=1),
        syncing INTEGER NOT NULL CHECK(syncing IN (0,1)),
        held INTEGER NOT NULL CHECK(held IN (0,1)),
        cooling_until INTEGER,
        effect TEXT NOT NULL GENERATED ALWAYS AS (CASE
            WHEN syncing=0 THEN 'not_syncing'
            WHEN held=1 THEN 'failed'
            WHEN cooling_until IS NOT NULL THEN 'cooling_down'
            ELSE 'pass' END) STORED,
        CHECK(syncing=1 OR cooling_until IS NULL)
    ) STRICT;
    INSERT INTO coven_retry_effect(singleton,syncing,held,cooling_until) VALUES(1,0,0,NULL);";
