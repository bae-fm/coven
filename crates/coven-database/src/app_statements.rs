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
use crate::read_capture::ReadFrame;
use crate::scope_plan::ScopePlan;
use rusqlite::{CachedStatement, Statement};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::Arc;

/// How many app statements' reads a connection keeps, in every context
/// together: more than an app runs, but bounded for SQL built with values in
/// its text.
const RECORDED: usize = 512;

#[derive(Default)]
pub(super) struct AppStatements {
    /// The schema the kept statements were prepared against.
    schema: Cell<Option<i64>>,
    /// What each statement read when it was prepared in each context.
    reads: RefCell<HashMap<AppContext, HashMap<String, ReadFrame>>>,
    /// What each kept statement lets a live query narrow.
    plans: RefCell<HashMap<String, Arc<ScopePlan>>>,
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
        let schema = self.app_schema_cookie()?;
        if self.app_statements.schema.get() != Some(schema) {
            // A statement prepared against another schema is no longer
            // judged as it would be now.
            self.connection.flush_prepared_statement_cache();
            self.app_statements.reads.borrow_mut().clear();
            self.app_statements.plans.borrow_mut().clear();
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

    /// What `statement` lets the live query reading it narrow; `None`
    /// outside a live query's read.
    pub(super) fn live_scope_plan(
        &self,
        sql: &str,
        statement: &AppStatement<'_>,
    ) -> rusqlite::Result<Option<Arc<crate::scope_plan::ScopePlan>>> {
        if !self.authorization.capturing_reads() {
            return Ok(None);
        }
        self.scope_plan(sql, statement).map(Some)
    }

    /// End the app statement begun last: each table it read in the scope
    /// `plan` finds for this run, or whole. When finding them fails, every
    /// table stays whole and the read fails with it.
    pub(super) fn record_app_statement(
        &self,
        plan: Option<&crate::scope_plan::ScopePlan>,
        parameters: &[crate::sql_value::SqlValue],
    ) -> rusqlite::Result<()> {
        let scopes = match plan {
            Some(plan) => plan.scopes(parameters, &mut |sql, values| {
                self.authorization.judged_unrecorded(|| {
                    let mut statement = self.connection.prepare(sql)?;
                    // Coven's SQL names the parameters it uses by number, so
                    // only those up to the highest it names are bound.
                    for index in 1..=statement.parameter_count() {
                        statement.raw_bind_parameter(index, &values[index - 1])?;
                    }
                    self.counting_steps(&mut statement, ReadSteps::Narrowing, |statement| {
                        let mut rows = statement.raw_query();
                        let mut found = Vec::new();
                        while let Some(row) = rows.next()? {
                            found.push(row.get::<_, crate::sql_value::SqlValue>(0)?);
                        }
                        Ok(found)
                    })
                })
            }),
            None => Ok(crate::row_scope::Scopes::default()),
        };
        self.authorization.record_statement(
            scopes
                .as_ref()
                .unwrap_or(&crate::row_scope::Scopes::default()),
        );
        scopes.map(drop)
    }

    /// Run `run` on `statement`, counting the SQLite virtual machine steps it
    /// ran as `steps` (WorkCounts).
    pub(super) fn counting_steps<T>(
        &self,
        statement: &mut Statement<'_>,
        steps: ReadSteps,
        run: impl FnOnce(&mut Statement<'_>) -> T,
    ) -> T {
        #[cfg(any(test, feature = "test-utils"))]
        let before = statement.get_status(rusqlite::StatementStatus::VmStep);
        let result = run(statement);
        // SQLite counts a statement's steps from its preparation, so a kept
        // statement's count includes its earlier runs.
        #[cfg(any(test, feature = "test-utils"))]
        {
            let ran = statement
                .get_status(rusqlite::StatementStatus::VmStep)
                .wrapping_sub(before) as u32 as usize;
            self.record_work(|work| match steps {
                ReadSteps::App => work.app_read_steps += ran,
                ReadSteps::Narrowing => work.narrowing_steps += ran,
            });
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        let _ = steps;
        result
    }

    /// What `statement`, prepared from `sql`, lets a live query narrow, kept
    /// beside a kept statement while the schema stays.
    pub(super) fn scope_plan(
        &self,
        sql: &str,
        statement: &AppStatement<'_>,
    ) -> rusqlite::Result<Arc<ScopePlan>> {
        let kept = matches!(statement, AppStatement::Cached(_));
        if kept {
            if let Some(plan) = self.app_statements.plans.borrow().get(sql) {
                return Ok(Arc::clone(plan));
            }
        }
        let plan = Arc::new(
            self.authorization
                .unrecorded(|| ScopePlan::build(&self.connection, sql, statement))?,
        );
        if kept {
            let mut plans = self.app_statements.plans.borrow_mut();
            if plans.len() >= RECORDED {
                plans.clear();
            }
            plans.insert(sql.to_owned(), Arc::clone(&plan));
        }
        Ok(plan)
    }
}

/// Whose statement's steps a read ran (WorkCounts).
#[derive(Clone, Copy)]
pub(super) enum ReadSteps {
    /// The app's own statement.
    App,
    /// Coven's statement narrowing a live query's run (§4).
    Narrowing,
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
