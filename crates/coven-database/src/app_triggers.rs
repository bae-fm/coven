//! The app's triggers that coven's own statements fire: those on
//! `coven_pins` and `coven_upload_queue`, and those on the app's tables as
//! coven applies writes
//! (§9.4). The authorizer judges what each writes as it would in an app
//! statement, and a refusal names the trigger.

use super::DatabaseConnection;
use crate::DbError;

impl DatabaseConnection {
    /// A statement as prepared, or what the authorizer refused in the app's
    /// triggers it compiles.
    pub(super) fn trigger_judged<T>(&self, prepared: rusqlite::Result<T>) -> Result<T, DbError> {
        Ok(self.authorization.app_result(prepared)?)
    }

    /// Refuse the app's triggers on `coven_pins` and `coven_upload_queue`
    /// that write what they may not: coven's tables, or synced tables, since
    /// a pin and a queued file are this device's alone. Preparing each
    /// change, without running it, compiles every trigger it would fire, and
    /// those they fire in turn.
    pub(crate) fn check_followed_triggers(&self) -> Result<(), DbError> {
        let _scope = self.authorization.internal();
        for change in [
            "INSERT INTO coven_pins(file) VALUES('')",
            "UPDATE coven_pins SET file=file",
            "DELETE FROM coven_pins",
            "INSERT INTO coven_upload_queue(file,position) VALUES('',0)",
            "UPDATE coven_upload_queue SET file=file",
            "DELETE FROM coven_upload_queue",
        ] {
            self.trigger_judged(self.connection.prepare(change))?;
        }
        Ok(())
    }
}
