//! The app's triggers that coven's own statements fire: those on the tables
//! they may follow, `coven_files`, `coven_pins` and `coven_upload_queue`,
//! and those on the app's tables as coven applies writes
//! (§9.4). The authorizer judges what each writes, reads and calls as it
//! would in an app statement, and a refusal names the trigger.

use super::DatabaseConnection;
use crate::DbError;

/// Each change coven makes to a file's record: making it, switching where
/// its file is, and deleting it.
const FILE_CHANGES: [&str; 3] = [
    "INSERT INTO coven_files(id,audience,size,hash,coordinates,namespace) VALUES('','',0,NULL,'','')",
    "UPDATE coven_files SET coordinates=coordinates,hash=hash",
    "DELETE FROM coven_files",
];

/// Each change coven's own triggers make to its pins and its upload queue.
const QUEUE_AND_PIN_CHANGES: [&str; 6] = [
    "INSERT INTO coven_pins(file) VALUES('')",
    "UPDATE coven_pins SET file=file",
    "DELETE FROM coven_pins",
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
    /// tables, since a pin, a queued file and what a trigger keeps of a
    /// file's record are this device's alone. Coven changes these tables in
    /// its own statements, and as it applies merged rows of `coven_files`;
    /// the authorizer judges the app's triggers alike in both. Preparing each
    /// change, without running it, compiles every trigger it would fire, and
    /// those they fire in turn.
    pub(crate) fn check_followed_triggers(&self) -> Result<(), DbError> {
        let _scope = self.authorization.internal();
        for change in FILE_CHANGES.iter().chain(&QUEUE_AND_PIN_CHANGES) {
            self.trigger_judged(self.connection.prepare(change))?;
        }
        Ok(())
    }
}
