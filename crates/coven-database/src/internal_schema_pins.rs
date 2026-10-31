//! The files this device keeps pinned (§17.8, api.md E4), kept as a table so
//! the app's local triggers can follow them (§9.4).
//!
//! A file is pinned while the cache holds a pinned, checked whole copy of its
//! stored bytes. `_coven_pin_source` says which files are, and coven's
//! triggers on the cache and the stored files bring each file they change up
//! to date in the same statement: a file's entry is inserted when its pin
//! begins and deleted when the pin ends, and a file whose answer stayed the
//! same is left alone. Records over the same stored bytes share one copy, so
//! each is pinned with it.

/// The table of pinned files, which the app's triggers may follow.
pub(crate) const PINS: &str = "coven_pins";

/// Bring the files `$files` names, a SELECT of file ids, up to date with
/// `_coven_pin_source`, which finds them by index. A file whose answer is
/// unchanged is left alone.
macro_rules! refresh {
    ($($files:tt)+) => {
        concat!(
            "DELETE FROM coven_pins WHERE file IN (",
            $($files)+,
            ") AND NOT EXISTS(SELECT 1 FROM _coven_pin_source AS s WHERE s.file=coven_pins.file);
             INSERT INTO coven_pins(file) SELECT file FROM _coven_pin_source WHERE file IN (",
            $($files)+,
            ") ON CONFLICT(file) DO NOTHING;"
        )
    };
}

/// The files whose stored bytes a cache entry holds.
macro_rules! named_by {
    ($row:literal) => {
        concat!(
            "SELECT f.file FROM _coven_stored_files AS f WHERE f.namespace=",
            $row,
            ".namespace AND f.cache_id=",
            $row,
            ".file_id"
        )
    };
}

pub(super) const SCHEMA: &str = concat!(
    "CREATE VIEW _coven_pin_source(file) AS
        SELECT f.file FROM _coven_stored_files AS f
        JOIN _coven_cache AS c ON c.namespace=f.namespace
            AND c.file_id=f.cache_id AND c.chunk=-2
            AND c.pinned=1 AND c.checked_hash=f.hash;
    CREATE TABLE coven_pins (
        file TEXT PRIMARY KEY NOT NULL
    ) STRICT, WITHOUT ROWID;
    CREATE TRIGGER _coven_pins_stored_file_added AFTER INSERT ON _coven_stored_files BEGIN ",
    refresh!("SELECT NEW.file"),
    " END;
    CREATE TRIGGER _coven_pins_stored_file_changed AFTER UPDATE ON _coven_stored_files BEGIN ",
    refresh!("SELECT OLD.file"),
    refresh!("SELECT NEW.file"),
    " END;
    CREATE TRIGGER _coven_pins_stored_file_removed AFTER DELETE ON _coven_stored_files BEGIN ",
    refresh!("SELECT OLD.file"),
    " END;
    CREATE TRIGGER _coven_pins_cached AFTER INSERT ON _coven_cache WHEN NEW.chunk=-2 BEGIN ",
    refresh!(named_by!("NEW")),
    " END;
    CREATE TRIGGER _coven_pins_cache_changed AFTER UPDATE OF namespace,file_id,chunk,pinned,checked_hash
        ON _coven_cache WHEN OLD.chunk=-2 OR NEW.chunk=-2 BEGIN ",
    refresh!(named_by!("OLD")),
    refresh!(named_by!("NEW")),
    " END;
    CREATE TRIGGER _coven_pins_cache_removed AFTER DELETE ON _coven_cache WHEN OLD.chunk=-2 BEGIN ",
    refresh!(named_by!("OLD")),
    " END;",
);

#[cfg(test)]
#[path = "internal_schema_pins_tests.rs"]
mod tests;
