//! The app's triggers that coven's own statements fire: those on
//! `coven_pins`, and those on the app's tables as coven applies writes
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

    /// Refuse the app's triggers on `coven_pins` that write what they may
    /// not: coven's tables, or synced tables, since a pin is this device's
    /// alone. Preparing each change, without running it, compiles every
    /// trigger it would fire, and those they fire in turn.
    pub(crate) fn check_pin_triggers(&self) -> Result<(), DbError> {
        let _scope = self.authorization.internal();
        for change in [
            "INSERT INTO coven_pins(table_name,key,kept) VALUES('',x'',0)",
            "UPDATE coven_pins SET kept=kept",
            "DELETE FROM coven_pins",
        ] {
            self.trigger_judged(self.connection.prepare(change))?;
        }
        Ok(())
    }
}
