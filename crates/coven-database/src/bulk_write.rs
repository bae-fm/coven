//! A bulk write: the app's SQL on device-local tables alone, run as plain
//! SQLite, with no pre-update hook watching it (§4).
//!
//! A local write's hook records each row operation, which also keeps SQLite
//! from emptying a table in one step. A bulk write changes no synced table,
//! so nothing it does reaches a write record; live queries learn only which
//! tables it changed, from what the authorizer saw as each statement was
//! prepared, triggers and foreign-key actions included.

use super::DatabaseConnection;

impl DatabaseConnection {
    pub(crate) fn bulk_write<F, R, E>(&self, sql: F) -> Result<R, E>
    where
        E: crate::WriteFailure,
        F: FnOnce(crate::BulkSqlContext<'_>) -> Result<R, E>,
    {
        #[cfg(test)]
        let _profile = self.profile_statements();
        self.write_transaction(false, || {
            let scope = self.authorization.bulk();
            let result = sql(crate::BulkSqlContext::new(self))?;
            self.require_transaction()?;
            let changed = scope.changed();
            drop(scope);
            let changes = crate::change_capture::table_changes(self, changed)?;
            Ok(((), result, changes))
        })
    }
}

#[cfg(test)]
#[path = "bulk_write_tests.rs"]
mod tests;
