//! This installation's sync status (§6, api.md E5), kept as a table so the
//! app's live queries and local triggers can follow it (§9.4), as they
//! follow `coven_pins`.
//!
//! It has one row from the store's creation on, which says no storage is set
//! up until the first open writes over it, so a read always finds a status.
//! Each change writes the row whole in the step that changes the status:
//! opening writes `Stopped` or `Disconnected`, and the running loop each status
//! after. `state` names the status; `status` holds it with its details, which
//! sync encodes as JSON and the app reads as `SyncStatus`.
//!
//! Only the handle that holds the store's writer lock reads it: a read-only
//! handle can't tell whether that writer still runs, so app SQL there may not
//! read the table (`authorization.rs`).

/// The sync status table, which the app's triggers may follow.
pub(crate) const SYNC_STATUS: &str = "coven_sync_status";

pub(super) const SCHEMA: &str = "CREATE TABLE coven_sync_status (
        singleton INTEGER PRIMARY KEY NOT NULL CHECK(singleton=1),
        state TEXT NOT NULL CHECK(state IN ('disconnected','stopped','offline','syncing',
            'synced','blocked','failed')),
        status BLOB NOT NULL
    ) STRICT;
    INSERT INTO coven_sync_status VALUES(1,'disconnected',CAST('\"Disconnected\"' AS BLOB));";

/// Write the status whole, leaving the row alone when it already says this.
pub(crate) fn save(
    db: &crate::sqlite::DatabaseConnection,
    state: &str,
    status: &[u8],
) -> Result<(), crate::DbError> {
    db.internal_execute(
        "UPDATE coven_sync_status SET state=?1,status=?2
         WHERE singleton=1 AND (state IS NOT ?1 OR status IS NOT ?2)",
        rusqlite::params![state, status],
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "internal_schema_sync_status_tests.rs"]
mod tests;
