//! A bulk write: the app's SQL on device-local tables alone, run as plain
//! SQLite, with no pre-update hook watching it (§4).
//!
//! A local write's hook records each row operation, which also keeps SQLite
//! from emptying a table in one step. The policy refuses any statement in a
//! bulk write that writes a table whose changes the host records, so no write
//! record holds anything a bulk write does. For live queries, sqlive knows
//! only which tables a bulk write changed, from what the authorizer saw as
//! SQLite prepared each statement, triggers and foreign-key actions
//! included.

use super::HostSql;
use crate::authorization::Policy;

impl<P: Policy> HostSql<P> {
    /// Run `sql`, the app's bulk write, in a write transaction with no
    /// pre-update hook. The policy judges its statements in the bulk scope,
    /// and live queries learn only which tables it changed.
    pub fn bulk_write<F, R, E>(&mut self, sql: F) -> Result<R, E>
    where
        E: crate::RollbackJoin<P::Failure, P::Refusal>,
        F: FnOnce(&Self) -> Result<R, E>,
    {
        self.with_profile(|db| db.bulk_transaction(sql))
    }

    fn bulk_transaction<F, R, E>(&mut self, sql: F) -> Result<R, E>
    where
        E: crate::RollbackJoin<P::Failure, P::Refusal>,
        F: FnOnce(&Self) -> Result<R, E>,
    {
        let transaction = super::write_transaction::WriteTransaction::begin(self, false)?;
        let outcome = transaction.within(|database| {
            let scope = database.authorization.bulk();
            let result = sql(database)?;
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
