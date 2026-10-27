//! The rows whose file this device keeps pinned (§17.6, api.md E4), kept as
//! a table so the app's local triggers can follow them (§9.4).
//!
//! `_coven_pin_source` says which rows are pinned, from the queue, the
//! cache, the pin requests and each row's file. `coven_pins` holds its
//! answer, and coven's triggers on those tables bring each row they change
//! up to date in the same statement. A row's entry is inserted when its pin
//! begins, its `kept` updated when it changes, and deleted when the pin ends;
//! a row whose answer stayed the same is left alone. Within one of coven's
//! transactions a pin can end and begin again, as retiring an upload ends its
//! queued pin before the stored file's begins.

/// The table of pinned rows, the one of coven's objects the app's triggers
/// may follow.
pub(crate) const PINS: &str = "coven_pins";

/// Bring the rows `$rows` names, a SELECT of a table name and key, up to
/// date with `_coven_pin_source`, whose branches each find them by index.
/// A row whose answer is unchanged is left alone.
macro_rules! refresh {
    ($($rows:tt)+) => {
        concat!(
            "DELETE FROM coven_pins WHERE (table_name,key) IN (",
            $($rows)+,
            ") AND NOT EXISTS(SELECT 1 FROM _coven_pin_source AS s
                 WHERE s.table_name=coven_pins.table_name AND s.key=coven_pins.key);
             INSERT INTO coven_pins(table_name,key,kept)
                 SELECT table_name,key,kept FROM _coven_pin_source WHERE (table_name,key) IN (",
            $($rows)+,
            ") ON CONFLICT(table_name,key) DO UPDATE SET kept=excluded.kept
                 WHERE kept<>excluded.kept;"
        )
    };
}

/// The rows a cache entry's or pin request's file is the file of.
macro_rules! named_by {
    ($row:literal) => {
        concat!(
            "SELECT f.table_name,f.key FROM _coven_row_files AS f
             WHERE f.namespace=",
            $row,
            ".namespace AND f.cache_id=",
            $row,
            ".file_id"
        )
    };
}

pub(super) const SCHEMA: &str = concat!(
    // A pinned file still queued here, which is whole here, or a stored file
    // with a pinned checked whole copy, or a pin waiting for one.
    "CREATE VIEW _coven_pin_source(table_name,key,kept) AS
        SELECT p.table_name,p.key,1
        FROM _coven_pending_attachments AS p
        JOIN _coven_file_uploads AS u ON u.id=p.upload
        WHERE u.pinned=1
        UNION ALL
        SELECT f.table_name,f.key,c.file_id IS NOT NULL
        FROM _coven_row_files AS f
        LEFT JOIN _coven_pending_attachments AS p ON p.table_name=f.table_name AND p.key=f.key
        LEFT JOIN _coven_cache AS c ON c.namespace=f.namespace
            AND c.file_id=f.cache_id AND c.chunk=-2
            AND c.pinned=1 AND c.checked_hash=f.hash
        LEFT JOIN _coven_pin_requests AS q ON q.namespace=f.namespace
            AND q.file_id=f.cache_id
        WHERE p.upload IS NULL AND (c.file_id IS NOT NULL OR q.file_id IS NOT NULL);
    CREATE TABLE coven_pins (
        table_name TEXT NOT NULL,
        key BLOB NOT NULL,
        kept INTEGER NOT NULL CHECK(kept IN (0,1)),
        PRIMARY KEY(table_name,key)
    ) STRICT, WITHOUT ROWID;
    CREATE TRIGGER _coven_pins_attached AFTER INSERT ON _coven_pending_attachments BEGIN ",
    refresh!("SELECT NEW.table_name,NEW.key"),
    " END;
    CREATE TRIGGER _coven_pins_attachment_changed AFTER UPDATE ON _coven_pending_attachments BEGIN ",
    refresh!("SELECT OLD.table_name,OLD.key"),
    refresh!("SELECT NEW.table_name,NEW.key"),
    " END;
    CREATE TRIGGER _coven_pins_attachment_ended AFTER DELETE ON _coven_pending_attachments BEGIN ",
    refresh!("SELECT OLD.table_name,OLD.key"),
    " END;
    CREATE TRIGGER _coven_pins_upload_queued AFTER INSERT ON _coven_file_uploads BEGIN ",
    refresh!("SELECT table_name,key FROM _coven_pending_attachments WHERE upload=NEW.id"),
    " END;
    CREATE TRIGGER _coven_pins_upload_changed AFTER UPDATE OF id,pinned ON _coven_file_uploads BEGIN ",
    refresh!("SELECT table_name,key FROM _coven_pending_attachments WHERE upload=OLD.id"),
    refresh!("SELECT table_name,key FROM _coven_pending_attachments WHERE upload=NEW.id"),
    " END;
    CREATE TRIGGER _coven_pins_upload_ended AFTER DELETE ON _coven_file_uploads BEGIN ",
    refresh!("SELECT table_name,key FROM _coven_pending_attachments WHERE upload=OLD.id"),
    " END;
    CREATE TRIGGER _coven_pins_row_file_added AFTER INSERT ON _coven_row_files BEGIN ",
    refresh!("SELECT NEW.table_name,NEW.key"),
    " END;
    CREATE TRIGGER _coven_pins_row_file_changed AFTER UPDATE ON _coven_row_files BEGIN ",
    refresh!("SELECT OLD.table_name,OLD.key"),
    refresh!("SELECT NEW.table_name,NEW.key"),
    " END;
    CREATE TRIGGER _coven_pins_row_file_removed AFTER DELETE ON _coven_row_files BEGIN ",
    refresh!("SELECT OLD.table_name,OLD.key"),
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
    " END;
    CREATE TRIGGER _coven_pins_requested AFTER INSERT ON _coven_pin_requests BEGIN ",
    refresh!(named_by!("NEW")),
    " END;
    CREATE TRIGGER _coven_pins_request_changed AFTER UPDATE OF namespace,file_id ON _coven_pin_requests BEGIN ",
    refresh!(named_by!("OLD")),
    refresh!(named_by!("NEW")),
    " END;
    CREATE TRIGGER _coven_pins_request_ended AFTER DELETE ON _coven_pin_requests BEGIN ",
    refresh!(named_by!("OLD")),
    " END;",
);

#[cfg(test)]
#[path = "internal_schema_pins_tests.rs"]
mod tests;
