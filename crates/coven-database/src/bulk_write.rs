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
    pub(crate) fn bulk_write<F, R, E>(&mut self, sql: F) -> Result<R, E>
    where
        E: crate::WriteFailure,
        F: FnOnce(crate::BulkSqlContext<'_>) -> Result<R, E>,
    {
        self.with_profile(|db| db.bulk_transaction(sql))
    }

    fn bulk_transaction<F, R, E>(&mut self, sql: F) -> Result<R, E>
    where
        E: crate::WriteFailure,
        F: FnOnce(crate::BulkSqlContext<'_>) -> Result<R, E>,
    {
        let transaction = super::write_transaction::WriteTransaction::begin(self, false)?;
        let outcome = transaction.within(|database| {
            let scope = database.authorization.bulk();
            let result = sql(crate::BulkSqlContext::new(database))?;
            database.require_transaction()?;
            let changed = scope.changed();
            drop(scope);
            let changes = crate::change_capture::table_changes(database, changed)?;
            Ok((result, changes))
        });
        transaction.commit(outcome)
    }
}

#[cfg(test)]
#[path = "bulk_write_tests.rs"]
mod tests;
