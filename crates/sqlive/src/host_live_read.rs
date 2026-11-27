//! The host's own statements in a live query's read, which narrow as app
//! statements do (SPEC.md section 4).

use super::HostSql;
use crate::authorization::{Policy, PolicyError};
use rusqlite::{Params, Row};

impl<P: Policy> HostSql<P> {
    /// Every row of the host's own SELECT `sql` with `params`, through `map`,
    /// as a read of a live query: the statement may read tables the app's SQL
    /// cannot, and the query runs again only for the writes that change rows
    /// within the scope the statement's plan gives this run, as for an app
    /// statement (`app_query`). Outside a live query's read it records
    /// nothing, as `query` does.
    pub fn host_live_read<T, Q: Params>(
        &self,
        sql: &str,
        params: Q,
        mut map: impl FnMut(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<Vec<T>, PolicyError<P>> {
        let _scope = self.authorization.internal();
        self.authorization.begin_read_statement();
        let mut values = Vec::new();
        self.with_statement(sql, |statement| {
            // Only a recording read keeps the plan, which a connection's
            // cached statement never learns.
            let schema = if self.authorization.capturing_reads() {
                Some(self.app_schema_cookie().map_err(crate::Error::from)?)
            } else {
                None
            };
            let plan = self.live_scope_plan(sql, statement, schema)?;
            self.run_rows(
                crate::Recorded::No,
                crate::authorization::Owner::Host,
                statement,
                plan,
                params,
                |row| {
                    values.push(map(row)?);
                    Ok(true)
                },
            )
            .map_err(crate::Error::from)
        })?;
        Ok(values)
    }
}

#[cfg(test)]
#[path = "host_live_read_tests.rs"]
mod tests;
