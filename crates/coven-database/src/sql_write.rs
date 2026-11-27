//! The writes every app SQL context that writes offers.

use crate::{BulkSqlContext, CommitSql, JoiningWrite, SqlRead, Writer};
use sqlive::rusqlite::{Params, Result, Row};
use sqlive_compose::{Write, WriteReturning};

/// SQL writes through any of a write's contexts, so code that writes is
/// written once for each of them. Each context reads as the writer does.
pub trait SqlWrite: SqlRead<Handle = Writer> {
    /// Execute one app statement with parameters.
    fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize>;

    /// Runs a write the app's build compiled (`write!`) that returns no
    /// rows, and returns how many rows it changed.
    fn run<Q: Write>(&self, statement: &Q) -> Result<usize> {
        self.execute(Q::SQL, statement.params().as_slice())
    }

    /// Runs a compiled write with a RETURNING clause, and returns every row
    /// the clause gives.
    fn run_returning<Q: WriteReturning>(&self, statement: &Q) -> Result<Vec<Q::Row>> {
        self.query(Q::SQL, statement.params().as_slice(), |row: &Row<'_>| Q::read(row))
    }
}

impl SqlWrite for BulkSqlContext<'_> {
    fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        BulkSqlContext::execute(self, sql, params)
    }
}

impl SqlWrite for CommitSql<'_> {
    fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        CommitSql::execute(self, sql, params)
    }
}

impl SqlWrite for JoiningWrite<'_> {
    fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        JoiningWrite::execute(self, sql, params)
    }
}
