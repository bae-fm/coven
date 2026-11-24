//! The app's statements, kept prepared per connection like the host's own.
//!
//! SQLite compiles into each statement every trigger it can fire, and the
//! views those read, so preparing an app write can cost far more than running
//! it. The authorizer judges a statement and records what it reads only as
//! SQLite prepares it, so a statement is prepared afresh the first time in
//! each authorization context, and what it read is kept beside it: a later
//! use from the cache is judged and recorded exactly as that first one was.
//!
//! What a kept statement lets a live query narrow depends on its SQL and the
//! schema alone, so a database's connections share one map of those plans:
//! once one reader built a statement's plan, the others use it.

use super::HostSql;
use crate::authorization::AppContext;
use crate::authorization::Policy;
use crate::{ReadFrame, ScopePlan};
use panic_as_error::locks::lock_unpoisoned;
use rusqlite::{CachedStatement, Statement};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// How many app statements' reads a connection keeps, in every context
/// together, and how many statements' plans a database keeps: more than an
/// app runs, but bounded for SQL built with values in its text.
const RECORDED: usize = 512;

/// What each kept statement lets a live query narrow, by its SQL, for the
/// one schema the plans were built against. A database's connections share
/// one; a connection opened outside a database keeps its own.
pub(crate) struct ScopePlans(Mutex<KeptPlans>);

struct KeptPlans {
    schema: Option<i64>,
    plans: HashMap<String, Arc<ScopePlan>>,
}

impl ScopePlans {
    /// A map that holds no plan yet.
    pub(crate) fn new() -> Self {
        Self(Mutex::new(KeptPlans { schema: None, plans: HashMap::new() }))
    }

    /// The plan of `sql` built against `schema`, if one was kept.
    fn get(&self, schema: i64, sql: &str) -> Option<Arc<ScopePlan>> {
        let kept = lock_unpoisoned(&self.0);
        kept.plans.get(sql).filter(|_| kept.schema == Some(schema)).cloned()
    }

    /// Keep `plan`, `sql`'s plan built against `schema`. Plans built against
    /// another schema are dropped first, and all of them once the map holds
    /// [`RECORDED`] statements. A connection still reading an older schema
    /// drops the newer plans too, and the next run on the newer schema builds
    /// them again.
    fn keep(&self, schema: i64, sql: &str, plan: &Arc<ScopePlan>) {
        let mut kept = lock_unpoisoned(&self.0);
        if kept.schema != Some(schema) {
            kept.plans.clear();
            kept.schema = Some(schema);
        }
        if kept.plans.len() >= RECORDED {
            kept.plans.clear();
        }
        kept.plans.insert(sql.to_owned(), Arc::clone(plan));
    }
}

pub(super) struct AppStatements {
    /// The schema the kept statements were prepared against.
    schema: Cell<Option<i64>>,
    /// What each statement read when it was prepared in each context.
    reads: RefCell<HashMap<AppContext, HashMap<String, ReadFrame>>>,
    /// What each kept statement lets a live query narrow, shared with the
    /// database's other connections.
    plans: Arc<ScopePlans>,
    /// Statements prepared afresh, for tests of the cache.
    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fresh: Cell<usize>,
}

impl AppStatements {
    /// No statement kept yet, with the plans of `plans`.
    pub(super) fn new(plans: Arc<ScopePlans>) -> Self {
        Self {
            schema: Cell::new(None),
            reads: RefCell::new(HashMap::new()),
            plans,
            #[cfg(any(test, feature = "test-utils"))]
            fresh: Cell::new(0),
        }
    }
}

impl<P: Policy> HostSql<P> {
    /// One app statement, from the cache when this connection prepared it
    /// before in the same context and schema. A connection opening, or
    /// migrating, prepares each one afresh: its statements change the schema.
    pub(super) fn app_statement(&self, sql: &str) -> rusqlite::Result<AppStatement<'_>> {
        let context = match self.authorization.app_context() {
            Some(context) if self.handle.is_some() => context,
            _ => {
                #[cfg(any(test, feature = "test-utils"))]
                self.app_statements.fresh.set(self.app_statements.fresh.get() + 1);
                return Ok(AppStatement::Fresh(self.connection.prepare(sql)?));
            }
        };
        let schema = self.app_schema_cookie()?;
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
            return Ok(AppStatement::Cached {
                statement: self.connection.prepare_cached(sql)?,
                schema,
            });
        }
        #[cfg(any(test, feature = "test-utils"))]
        self.app_statements.fresh.set(self.app_statements.fresh.get() + 1);
        // Judged and recorded in this context. The cache may hold the same SQL
        // from another context, which this preparation stands in for.
        drop(self.connection.prepare(sql)?);
        let reads = self.authorization.statement_reads();
        let mut recorded = self.app_statements.reads.borrow_mut();
        if recorded.values().map(HashMap::len).sum::<usize>() >= RECORDED {
            recorded.clear();
        }
        recorded.entry(context).or_default().insert(sql.to_owned(), reads);
        drop(recorded);
        Ok(AppStatement::Cached { statement: self.connection.prepare_cached(sql)?, schema })
    }

    /// What `statement` lets the live query reading it narrow; `None`
    /// outside a live query's read.
    pub(super) fn live_scope_plan(
        &self,
        sql: &str,
        statement: &AppStatement<'_>,
    ) -> rusqlite::Result<Option<Arc<crate::ScopePlan>>> {
        if !self.authorization.capturing_reads() {
            return Ok(None);
        }
        self.scope_plan(sql, statement).map(Some)
    }

    /// End the app statement begun last: each table it read in the scope
    /// `plan` gives it for this run, which returned `last` as the ORDER BY's
    /// values of the row the plan's `last_row` names, or whole.
    pub(super) fn record_app_statement(
        &self,
        plan: Option<&crate::ScopePlan>,
        parameters: &[crate::SqlValue],
        last: Option<&[crate::SqlValue]>,
    ) {
        self.authorization.record_statement(&match plan {
            Some(plan) => plan.scopes(parameters, last),
            None => crate::Scopes::default(),
        });
    }

    /// Run `run` on `statement`, counting the SQLite virtual machine steps it
    /// ran as `steps` (WorkCounts).
    pub(super) fn counting_steps<T>(
        &self,
        statement: &mut Statement<'_>,
        steps: StepsOf,
        run: impl FnOnce(&mut Statement<'_>) -> T,
    ) -> T {
        #[cfg(any(test, feature = "test-utils"))]
        let before = statement.get_status(rusqlite::StatementStatus::VmStep);
        let result = run(statement);
        // SQLite counts a statement's steps from its preparation, so a kept
        // statement's count includes its earlier runs.
        #[cfg(any(test, feature = "test-utils"))]
        {
            let ran = statement.get_status(rusqlite::StatementStatus::VmStep).wrapping_sub(before)
                as u32 as usize;
            self.record_work(|work| match steps {
                StepsOf::AppRead => {
                    work.app_read_steps += ran;
                    work.app_read_statements += 1;
                }
                StepsOf::AppWrite => work.app_write_steps += ran,
                StepsOf::Lookup => work.lookup_steps += ran,
            });
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        let _ = steps;
        result
    }

    /// What `statement`, prepared from `sql`, lets a live query narrow, kept
    /// for the database beside a kept statement while the schema stays.
    pub(super) fn scope_plan(
        &self,
        sql: &str,
        statement: &AppStatement<'_>,
    ) -> rusqlite::Result<Arc<ScopePlan>> {
        let AppStatement::Cached { schema, .. } = statement else {
            return self.build_scope_plan(sql, statement);
        };
        if let Some(plan) = self.app_statements.plans.get(*schema, sql) {
            return Ok(plan);
        }
        // Built outside the map's lock, so the other connections' runs don't
        // wait for it; two first runs at once each build it.
        let plan = self.build_scope_plan(sql, statement)?;
        self.app_statements.plans.keep(*schema, sql, &plan);
        Ok(plan)
    }

    fn build_scope_plan(
        &self,
        sql: &str,
        statement: &AppStatement<'_>,
    ) -> rusqlite::Result<Arc<ScopePlan>> {
        #[cfg(any(test, feature = "test-utils"))]
        self.record_work(|work| work.scope_plans_built += 1);
        Ok(Arc::new(self.authorization.unrecorded(|| {
            ScopePlan::build(&self.connection, sql, statement, self.authorization.whole_views())
        })?))
    }
}

/// Whose statement's steps a connection ran (WorkCounts).
#[derive(Clone, Copy)]
pub(super) enum StepsOf {
    /// The app's own statement that returns rows.
    AppRead,
    /// The app's own statement run for its changes.
    AppWrite,
    /// sqlive's lookup judging whether a write reruns a live query (§4).
    Lookup,
}

/// An app statement, kept prepared or prepared for this use alone.
pub(super) enum AppStatement<'connection> {
    /// Kept prepared, against the schema `schema`.
    Cached {
        statement: CachedStatement<'connection>,
        schema: i64,
    },
    Fresh(Statement<'connection>),
}

impl<'connection> std::ops::Deref for AppStatement<'connection> {
    type Target = Statement<'connection>;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Cached { statement, .. } => statement,
            Self::Fresh(statement) => statement,
        }
    }
}

impl std::ops::DerefMut for AppStatement<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Cached { statement, .. } => statement,
            Self::Fresh(statement) => statement,
        }
    }
}

#[cfg(test)]
#[path = "app_statements_tests.rs"]
mod tests;
