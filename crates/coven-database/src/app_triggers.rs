//! The app's triggers that coven's own statements fire: those on the tables
//! they may follow, `coven_files`, `coven_pins`, `coven_pin_requests`,
//! `coven_moves`, `coven_upload_queue`, `coven_sync_status` and
//! `coven_retry_effect`, and those on the app's tables as coven applies writes
//! (§9.4). The authorizer judges what each writes, reads and calls as it would
//! in an app statement, and a refusal names the trigger.

use super::DatabaseConnection;
use crate::DbError;

/// Each change coven makes to the sync status row and the retry effect:
/// rewriting the one row the store's creation made, as opening and the sync
/// loop do. The retry effect's generated `effect` changes with the columns
/// it is computed from.
const STATUS_CHANGES: [&str; 2] = [
    "UPDATE coven_sync_status SET storage=storage,provider=provider,last_pass=last_pass,moved=moved,last_synced=last_synced,starts=starts,halted=halted",
    "UPDATE coven_retry_effect SET syncing=syncing,held=held,cooling_until=cooling_until",
];

/// Each change coven makes to a file's record: making it, switching where
/// its file is, and deleting it.
const FILE_CHANGES: [&str; 3] = [
    "INSERT INTO coven_files(id,audience,size,hash,coordinates,namespace) VALUES('','',0,NULL,'','')",
    "UPDATE coven_files SET coordinates=coordinates,hash=hash",
    "DELETE FROM coven_files",
];

/// Each change coven's own triggers make to the tables they keep for the app:
/// its pins, pin requests, moves and upload queue.
const KEPT_CHANGES: [&str; 12] = [
    "INSERT INTO coven_pins(file) VALUES('')",
    "UPDATE coven_pins SET file=file",
    "DELETE FROM coven_pins",
    "INSERT INTO coven_pin_requests(file) VALUES('')",
    "UPDATE coven_pin_requests SET file=file",
    "DELETE FROM coven_pin_requests",
    "INSERT INTO coven_moves(move,state,file) VALUES(0,'moving','')",
    "UPDATE coven_moves SET state=state",
    "DELETE FROM coven_moves",
    "INSERT INTO coven_upload_queue(file,position) VALUES('',0)",
    "UPDATE coven_upload_queue SET file=file",
    "DELETE FROM coven_upload_queue",
];

impl DatabaseConnection {
    /// A statement as prepared, or what the authorizer refused in the app's
    /// triggers it compiles.
    pub(super) fn trigger_judged<T>(&self, prepared: rusqlite::Result<T>) -> Result<T, DbError> {
        Ok(self.authorization.app_result(prepared)?)
    }

    /// Refuse the app's triggers on the tables they may follow that do what
    /// app SQL may not: write coven's tables, read them other than through
    /// its views, or call what only those views may, and that write synced
    /// tables, since a pin, a move, a queued file, the sync status, the retry
    /// effect and what a trigger keeps of a file's record are this device's
    /// alone.
    /// Coven changes these tables in its own statements, and as it applies
    /// merged rows of `coven_files`; the authorizer judges the app's
    /// triggers alike in both. Preparing each change, without running it,
    /// compiles every trigger it would fire, and those they fire in turn.
    pub(crate) fn check_followed_triggers(&self) -> Result<(), DbError> {
        let _scope = self.authorization.internal();
        for change in FILE_CHANGES.iter().chain(&KEPT_CHANGES).chain(&STATUS_CHANGES) {
            self.trigger_judged(self.connection.prepare(change))?;
        }
        Ok(())
    }
}
