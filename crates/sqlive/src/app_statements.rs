//! The app's statements, kept prepared per connection like the host's own.
//!
//! SQLite compiles into each statement every trigger it can fire, and the
//! views those read, so preparing an app write can cost far more than running
//! it. The authorizer judges a statement and records what it reads only as
//! SQLite prepares it, so a statement is prepared afresh the first time in
//! each authorization context, and what it read is kept beside it: a later
//! use from the cache is judged and recorded exactly as that first one was.
//!
//! What a kept statement lets a live query narrow depends on its SQL, the
//! schema, and the SQL functions and whole views its connection was set up
//! with. A database sets each of its connections up alike, so they share one
//! map of those plans, and one of them builds each plan for all.

use super::HostSql;
use crate::authorization::AppContext;
use crate::authorization::Policy;
use crate::read_report::ReadReport;
use crate::scope_plan::ScopePlanSize;
use crate::{ReadFrame, ScopePlan};
use panic_as_error::locks::{lock_unpoisoned, wait_unpoisoned};
use rusqlite::{CachedStatement, Statement};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Condvar, Mutex};

/// How many app statements' reads a connection keeps, in every context
/// together, and how many statements' plans a database keeps: more than an
/// app runs, but bounded for SQL built with values in its text.
const RECORDED: usize = 512;

/// What each kept statement lets a live query narrow, by its SQL and the
/// schema it was built against. A database's connections share one; a
/// connection opened outside a database keeps its own.
pub(crate) struct ScopePlans {
    kept: Mutex<KeptPlans>,
    /// Woken as each build ends, for the callers waiting for its plan.
    ended: Condvar,
    /// What a test runs as the next caller waits.
    #[cfg(test)]
    next_wait: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

/// Each statement's plan by its SQL, under the schema cookie it was built
/// against.
struct KeptPlans(BTreeMap<i64, HashMap<String, Kept>>);

enum Kept {
    /// A connection builds the plan now.
    Building,
    Built(Arc<ScopePlan>),
}

impl KeptPlans {
    /// Say that `sql`'s plan against `schema` is being built. The first plan
    /// of a schema drops those of every earlier one, whose last readers are
    /// ending, and leaves a later schema's kept. Once the map holds
    /// [`RECORDED`] statements, every built plan goes.
    fn begin(&mut self, schema: i64, sql: &str) {
        if !self.0.contains_key(&schema) {
            self.0 = self.0.split_off(&schema);
        }
        if self.0.values().map(HashMap::len).sum::<usize>() >= RECORDED {
            for plans in self.0.values_mut() {
                plans.retain(|_, kept| matches!(kept, Kept::Building));
            }
        }
        self.0.entry(schema).or_default().insert(sql.to_owned(), Kept::Building);
    }

    /// End a build of `sql`'s plan against `schema`, keeping what it built,
    /// unless a later schema dropped that one meanwhile.
    fn end(&mut self, schema: i64, sql: &str, built: Option<Arc<ScopePlan>>) {
        let Some(plans) = self.0.get_mut(&schema) else { return };
        match built {
            Some(plan) => {
                plans.insert(sql.to_owned(), Kept::Built(plan));
            }
            None => {
                plans.remove(sql);
            }
        }
    }
}

impl ScopePlans {
    /// A map that holds no plan yet.
    pub(crate) fn new() -> Self {
        Self {
            kept: Mutex::new(KeptPlans(BTreeMap::new())),
            ended: Condvar::new(),
            #[cfg(test)]
            next_wait: Mutex::new(None),
        }
    }

    /// The plan of `sql` against `schema`, which `build` builds when none is
    /// kept. One caller builds a plan at a time: the others wait for it, and
    /// one of them builds it when that build fails or panics. Builds of
    /// different plans run beside each other.
    fn plan(
        &self,
        schema: i64,
        sql: &str,
        build: impl FnOnce() -> rusqlite::Result<Arc<ScopePlan>>,
    ) -> rusqlite::Result<Arc<ScopePlan>> {
        let mut kept = lock_unpoisoned(&self.kept);
        loop {
            match kept.0.get(&schema).and_then(|plans| plans.get(sql)) {
                Some(Kept::Built(plan)) => return Ok(Arc::clone(plan)),
                Some(Kept::Building) => {
                    #[cfg(test)]
                    if let Some(run) = lock_unpoisoned(&self.next_wait).take() {
                        run();
                    }
                    kept = wait_unpoisoned(&self.ended, kept);
                }
                None => break,
            }
        }
        kept.begin(schema, sql);
        drop(kept);
        let mut building = PlanBuild { plans: self, schema, sql, built: None };
        let plan = build()?;
        building.built = Some(Arc::clone(&plan));
        Ok(plan)
    }

    /// Run `run` as the next caller waits for a plan being built.
    #[cfg(test)]
    fn on_next_wait(&self, run: impl FnOnce() + Send + 'static) {
        assert!(lock_unpoisoned(&self.next_wait).replace(Box::new(run)).is_none());
    }
}

/// One plan's build, which ends as it drops, whether it built the plan,
/// failed or panicked, and wakes the callers waiting for it.
struct PlanBuild<'a> {
    plans: &'a ScopePlans,
    schema: i64,
    sql: &'a str,
    built: Option<Arc<ScopePlan>>,
}

impl Drop for PlanBuild<'_> {
    fn drop(&mut self) {
        lock_unpoisoned(&self.plans.kept).end(self.schema, self.sql, self.built.take());
        self.plans.ended.notify_all();
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
        statement: &Statement<'_>,
        schema: Option<i64>,
    ) -> rusqlite::Result<Option<Arc<crate::ScopePlan>>> {
        if !self.authorization.capturing_reads() {
            return Ok(None);
        }
        self.scope_plan(sql, statement, schema).map(Some)
    }

    /// End the statement begun last, the host's own when `host`: each table
    /// it read in the scope `plan` gives it for this run, which returned `last` as the ORDER BY's
    /// values of the row the plan's `last_row` names, or whole.
    pub(super) fn record_app_statement(
        &self,
        plan: Option<&crate::ScopePlan>,
        host: bool,
        parameters: &[crate::SqlValue],
        last: Option<&[crate::SqlValue]>,
    ) {
        let mut scopes = match plan {
            Some(plan) => plan.scopes(parameters, last),
            None => crate::Scopes::default(),
        };
        scopes.host = host;
        self.authorization.record_statement(&scopes);
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

    /// What `statement`, prepared from `sql`, lets a live query narrow. When
    /// `schema` names the schema the statement was prepared against, the plan
    /// is kept for the database while the schema stays; the plan depends on
    /// the SQL and the schema alone, so an app statement and a host statement
    /// of the same text share it.
    pub(super) fn scope_plan(
        &self,
        sql: &str,
        statement: &Statement<'_>,
        schema: Option<i64>,
    ) -> rusqlite::Result<Arc<ScopePlan>> {
        let Some(schema) = schema else {
            return self.build_scope_plan(sql, statement);
        };
        self.app_statements.plans.plan(schema, sql, || self.build_scope_plan(sql, statement))
    }

    fn build_scope_plan(
        &self,
        sql: &str,
        statement: &Statement<'_>,
    ) -> rusqlite::Result<Arc<ScopePlan>> {
        let plan = self.authorization.unrecorded(|| {
            ScopePlan::build(&self.connection, sql, statement, self.authorization.whole_views())
        })?;
        let ScopePlanSize { sources, cores, edges } = plan.size;
        tracing::debug!(sources, cores, edges, "sqlive built a scope plan");
        if let Some(whole) = ReadReport::of(&plan).whole() {
            tracing::debug!(sql, %whole, "a live query's statement reads tables or views whole");
        }
        #[cfg(any(test, feature = "test-utils"))]
        self.record_work(|work| {
            work.scope_plans_built += 1;
            work.scope_plan_size += plan.size;
        });
        Ok(Arc::new(plan))
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

impl AppStatement<'_> {
    /// The schema a kept statement was prepared against.
    pub(super) fn schema(&self) -> Option<i64> {
        match self {
            Self::Cached { schema, .. } => Some(*schema),
            Self::Fresh(_) => None,
        }
    }
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
