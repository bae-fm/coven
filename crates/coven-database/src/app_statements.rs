//! The app's statements, kept prepared per connection like coven's own.
//!
//! SQLite compiles into each statement every trigger it can fire, and the
//! views those read, so preparing an app write can cost far more than running
//! it. The authorizer judges a statement and records what it reads only as
//! SQLite prepares it, so a statement is prepared afresh the first time in
//! each authorization context, and what it read is kept beside it: a later
//! use from the cache is judged and recorded exactly as that first one was.

use super::DatabaseConnection;
use crate::authorization::AppContext;
use crate::observation::ColumnSet;
use rusqlite::{CachedStatement, Statement};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;

/// How many app statements' reads a connection keeps, in every context
/// together: more than an app runs, but bounded for SQL built with values in
/// its text.
const RECORDED: usize = 512;

#[derive(Default)]
pub(super) struct AppStatements {
    /// The schema the kept statements were prepared against.
    schema: Cell<Option<i64>>,
    /// What each statement read when it was prepared in each context.
    reads: RefCell<HashMap<AppContext, HashMap<String, ColumnSet>>>,
    /// Statements prepared afresh, for tests of the cache.
    #[cfg(test)]
    pub(super) fresh: Cell<usize>,
}

impl DatabaseConnection {
    /// One app statement, from the cache when this connection prepared it
    /// before in the same context and schema. A connection opening, or
    /// migrating, prepares each one afresh: its statements change the schema.
    pub(super) fn app_statement(&self, sql: &str) -> rusqlite::Result<AppStatement<'_>> {
        let context = match self.authorization.app_context() {
            Some(context) if self.handle.is_some() => context,
            _ => {
                #[cfg(test)]
                self.app_statements
                    .fresh
                    .set(self.app_statements.fresh.get() + 1);
                return Ok(AppStatement::Fresh(self.connection.prepare(sql)?));
            }
        };
        let schema = {
            let _scope = self.authorization.internal();
            self.connection
                .prepare_cached("PRAGMA main.schema_version")?
                .query_row([], |r| r.get::<_, i64>(0))?
        };
        if self.app_statements.schema.get() != Some(schema) {
            // A statement prepared against another schema is no longer
            // judged as it would be now.
            self.connection.flush_prepared_statement_cache();
            self.app_statements.reads.borrow_mut().clear();
            self.app_statements.schema.set(Some(schema));
        }
        let recorded = self
            .app_statements
            .reads
            .borrow()
            .get(&context)
            .and_then(|statements| statements.get(sql).cloned());
        if let Some(reads) = recorded {
            self.authorization.replay_reads(&reads);
            return Ok(AppStatement::Cached(self.connection.prepare_cached(sql)?));
        }
        #[cfg(test)]
        self.app_statements
            .fresh
            .set(self.app_statements.fresh.get() + 1);
        // Judged and recorded in this context. The cache may hold the same SQL
        // from another context, which this preparation stands in for.
        drop(self.connection.prepare(sql)?);
        let reads = self.authorization.statement_reads();
        let mut recorded = self.app_statements.reads.borrow_mut();
        if recorded.values().map(HashMap::len).sum::<usize>() >= RECORDED {
            recorded.clear();
        }
        recorded
            .entry(context)
            .or_default()
            .insert(sql.to_owned(), reads);
        drop(recorded);
        Ok(AppStatement::Cached(self.connection.prepare_cached(sql)?))
    }
}

/// An app statement, kept prepared or prepared for this use alone.
pub(super) enum AppStatement<'connection> {
    Cached(CachedStatement<'connection>),
    Fresh(Statement<'connection>),
}

impl<'connection> std::ops::Deref for AppStatement<'connection> {
    type Target = Statement<'connection>;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Cached(statement) => statement,
            Self::Fresh(statement) => statement,
        }
    }
}

impl std::ops::DerefMut for AppStatement<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Cached(statement) => statement,
            Self::Fresh(statement) => statement,
        }
    }
}

#[cfg(test)]
#[path = "app_statements_tests.rs"]
mod tests;
