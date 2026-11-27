//! The statements each connection keeps prepared, the app's and the host's.
//!
//! SQLite compiles into each statement every trigger it can fire, and the
//! views those read, so preparing an app write can cost far more than running
//! it. The authorizer judges a statement and learns what it reads only as
//! SQLite prepares it, so the connection keeps that judgment beside the
//! prepared statement: a later run from the cache, in a one-shot read, a
//! live query's run or a write that the judgment allows, records the same
//! reads. The app's statements and the host's are judged by different rules,
//! so each keeps judgments of its own.
//!
//! What a kept statement lets a live query narrow depends on its SQL, the
//! schema, and the SQL functions and whole views its connection was set up
//! with. A database sets each of its connections up alike, so they share one
//! map of those plans, and one of them builds each plan for all. The plans of
//! one schema look their names up in one read of it (`MainSchema`), which the
//! map keeps beside them. A compiled statement brings the plan its compiler
//! built, which the map keeps once a connection has checked it against that
//! read.

use super::HostSql;
use crate::authorization::{Judgment, Policy, PolicyError};
use crate::MainSchema;
use crate::ReadReport;
use crate::ScopePlanSize;
use crate::{CompiledPlan, CompiledStatement, ScopePlan};
use panic_as_error::locks::{lock_unpoisoned, wait_unpoisoned};
use rusqlite::{CachedStatement, Statement};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

/// How many judgments a connection keeps, and how many statements' plans a
/// database keeps: more than an app and its host run, but bounded for SQL
/// built with values in its text.
const RECORDED: usize = 512;

/// What each kept statement lets a live query narrow, by its SQL and the
/// schema it was built against. A database's connections share one; a
/// connection opened outside a database keeps its own.
pub(crate) struct ScopePlans {
    kept: Mutex<KeptPlans>,
    /// Held by the one caller reading a schema, apart from `kept`, so that
    /// the callers of plans already built don't wait for the read.
    reading: Mutex<()>,
    /// Woken as each build ends, for the callers waiting for its plan.
    ended: Condvar,
    /// What a test runs as the next caller waits.
    #[cfg(test)]
    next_wait: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

/// What the map keeps for each schema cookie.
struct KeptPlans(BTreeMap<i64, SchemaPlans>);

/// What the map keeps for one schema.
#[derive(Default)]
struct SchemaPlans {
    /// The schema, once a build has read it.
    schema: Option<Arc<MainSchema>>,
    /// Each statement's plan, by its SQL.
    plans: HashMap<String, Kept>,
    /// The plans of the compiled statements, each checked against the
    /// schema, by the address of the plan's text: two builds of one SQL
    /// against two schemas give two plans, and equal texts at two addresses
    /// give equal plans. No bound keeps them: an app compiles a fixed set of
    /// statements.
    compiled: HashMap<usize, Arc<ScopePlan>>,
}

enum Kept {
    /// A connection builds the plan now.
    Building,
    Built(Arc<ScopePlan>),
}

impl KeptPlans {
    /// Make way for the first plan of `schema`, when the map keeps none:
    /// drop what every earlier schema kept, whose last readers are ending,
    /// and leave a later schema's kept.
    fn enter(&mut self, schema: i64) {
        if !self.0.contains_key(&schema) {
            self.0 = self.0.split_off(&schema);
        }
    }

    /// Say that `sql`'s plan against `schema` is being built, after `enter`.
    /// Once the map holds [`RECORDED`] statements, every built plan goes.
    fn begin(&mut self, schema: i64, sql: &str) {
        self.enter(schema);
        if self.0.values().map(|of| of.plans.len()).sum::<usize>() >= RECORDED {
            for of in self.0.values_mut() {
                of.plans.retain(|_, kept| matches!(kept, Kept::Building));
            }
        }
        self.0.entry(schema).or_default().plans.insert(sql.to_owned(), Kept::Building);
    }

    /// End a build of `sql`'s plan against `schema`, keeping what it built,
    /// unless a later schema dropped that one meanwhile.
    fn end(&mut self, schema: i64, sql: &str, built: Option<Arc<ScopePlan>>) {
        let Some(SchemaPlans { plans, .. }) = self.0.get_mut(&schema) else { return };
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
            reading: Mutex::new(()),
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
            match kept.0.get(&schema).and_then(|of| of.plans.get(sql)) {
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

    /// The schema `cookie` counts, for a plan being built against it, which
    /// `read` reads when the map keeps none. A caller waits while another
    /// reads, and reads itself when that read fails or panics. A read that
    /// ends after a later schema dropped its entry is kept for no other build.
    fn schema(
        &self,
        cookie: i64,
        read: impl FnOnce() -> rusqlite::Result<MainSchema>,
    ) -> rusqlite::Result<Arc<MainSchema>> {
        let _reading = lock_unpoisoned(&self.reading);
        let kept = lock_unpoisoned(&self.kept).0.get(&cookie).and_then(|of| of.schema.clone());
        if let Some(schema) = kept {
            return Ok(schema);
        }
        let schema = Arc::new(read()?);
        if let Some(of) = lock_unpoisoned(&self.kept).0.get_mut(&cookie) {
            of.schema = Some(Arc::clone(&schema));
        }
        Ok(schema)
    }

    /// The compiled plan whose text is `text`, which `check` decodes and
    /// checks against the schema `cookie` counts when no connection has yet,
    /// with the schema `read` reads when the map keeps none (`schema`). Two
    /// connections that check one plan at once each do: a check compares a
    /// few objects of a schema read once, where a build reads the
    /// statement's SQL.
    fn compiled(
        &self,
        cookie: i64,
        text: &'static str,
        check: impl FnOnce(&MainSchema) -> rusqlite::Result<Arc<ScopePlan>>,
        read: impl FnOnce() -> rusqlite::Result<MainSchema>,
    ) -> rusqlite::Result<Arc<ScopePlan>> {
        let address = text.as_ptr() as usize;
        let checked = {
            let mut kept = lock_unpoisoned(&self.kept);
            kept.enter(cookie);
            kept.0.entry(cookie).or_default().compiled.get(&address).cloned()
        };
        if let Some(plan) = checked {
            return Ok(plan);
        }
        let schema = self.schema(cookie, read)?;
        let plan = check(&schema)?;
        if let Some(of) = lock_unpoisoned(&self.kept).0.get_mut(&cookie) {
            of.compiled.insert(address, Arc::clone(&plan));
        }
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
    /// What the authorizer learned of each statement as SQLite prepared it,
    /// by whom it judged it as and the statement's SQL.
    judgments: RefCell<HashMap<JudgedAs, HashMap<String, Judgment>>>,
    /// What each kept statement lets a live query narrow, shared with the
    /// database's other connections.
    plans: Arc<ScopePlans>,
    /// App statements SQLite prepared, for tests of the cache.
    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fresh: Cell<usize>,
    /// The host's statements SQLite prepared to be kept, for tests of the
    /// cache.
    #[cfg(any(test, feature = "test-utils"))]
    pub(super) host_fresh: Cell<usize>,
}

impl AppStatements {
    /// No statement kept yet, with the plans of `plans`.
    pub(super) fn new(plans: Arc<ScopePlans>) -> Self {
        Self {
            schema: Cell::new(None),
            judgments: RefCell::new(HashMap::new()),
            plans,
            #[cfg(any(test, feature = "test-utils"))]
            fresh: Cell::new(0),
            #[cfg(any(test, feature = "test-utils"))]
            host_fresh: Cell::new(0),
        }
    }
}

impl<P: Policy> HostSql<P> {
    /// One app statement, from the cache when this connection prepared it
    /// before against the same schema and its judgment allows it here. A
    /// connection opening, or migrating, prepares each one afresh: its
    /// statements change the schema.
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
        self.keep_for_schema(schema);
        let judged_as = JudgedAs::App { policy: context.policy };
        let statement = self.kept_statement(judged_as, sql, |judgment| judgment.allows(context))?;
        Ok(AppStatement::Cached { statement, schema })
    }

    /// The host's own statement `sql`, from the cache when this connection
    /// prepared it before as the host's, and, in a live query's run, against
    /// the same schema. A run records what it reads either way.
    pub(super) fn host_statement(&self, sql: &str) -> Result<CachedStatement<'_>, PolicyError<P>> {
        // Outside a live query's run nothing uses what a statement reads, so
        // only the first one needs a schema to keep its judgment for: a later
        // run that finds another drops it.
        if self.authorization.capturing_reads() || self.app_statements.schema.get().is_none() {
            self.keep_for_schema(self.app_schema_cookie()?);
        }
        let statement = self.kept_statement(JudgedAs::Host, sql, |_| true);
        Ok(self.authorization.app_result(statement)?)
    }

    /// Drop every kept statement and judgment unless they are of `schema`: a
    /// statement prepared against another schema is no longer judged as it
    /// would be now.
    fn keep_for_schema(&self, schema: i64) {
        if self.app_statements.schema.get() != Some(schema) {
            self.connection.flush_prepared_statement_cache();
            self.app_statements.judgments.borrow_mut().clear();
            self.app_statements.schema.set(Some(schema));
        }
    }

    /// The cached statement `sql`, judged as `judged_as`: SQLite prepares it
    /// unless the cache holds it with a judgment that `allows`, whose reads
    /// are then recorded as preparing it would record them.
    fn kept_statement(
        &self,
        judged_as: JudgedAs,
        sql: &str,
        allows: impl Fn(&Judgment) -> bool,
    ) -> rusqlite::Result<CachedStatement<'_>> {
        let prepare = || self.authorization.judging(|| self.connection.prepare_cached(sql));
        let (statement, judgment) = prepare();
        let mut statement = statement?;
        let judgment = match judgment {
            Some(judgment) => judgment,
            None => {
                let judgments = self.app_statements.judgments.borrow();
                let kept = judgments.get(&judged_as).and_then(|of| of.get(sql));
                if let Some(kept) = kept.filter(|kept| allows(kept)) {
                    self.authorization.replay_reads(&kept.reads);
                    return Ok(statement);
                }
                drop(judgments);
                // The cache holds a statement SQLite prepared to be judged
                // another way, or one whose judgment is gone.
                statement.discard();
                let (prepared, judgment) = prepare();
                statement = prepared?;
                // SQLite prepared this one, so it asked the authorizer about
                // nothing, as for `VACUUM`: sqlive can't judge it, or know
                // what it reads.
                judgment.ok_or_else(|| {
                    let refused: PolicyError<P> = crate::Error::StatementForbidden {
                        operation: "a statement SQLite asks nothing about",
                    };
                    rusqlite::Error::UserFunctionError(Box::new(refused))
                })?
            }
        };
        #[cfg(any(test, feature = "test-utils"))]
        {
            let count = match judged_as {
                JudgedAs::App { .. } => &self.app_statements.fresh,
                JudgedAs::Host => &self.app_statements.host_fresh,
            };
            count.set(count.get() + 1);
        }
        let mut judgments = self.app_statements.judgments.borrow_mut();
        if judgments.values().map(HashMap::len).sum::<usize>() >= RECORDED {
            judgments.clear();
        }
        judgments.entry(judged_as).or_default().insert(sql.to_owned(), judgment);
        Ok(statement)
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

    /// End the statement begun last: each table it read in the scope `plan`
    /// gives it for this run, which returned `last` as the ORDER BY's values
    /// of the row the plan's `last_row` names, or whole.
    pub(super) fn record_app_statement(
        &self,
        plan: Option<&crate::ScopePlan>,
        parameters: &[crate::SqlValue],
        last: Option<&[crate::SqlValue]>,
    ) {
        let scopes = match plan {
            Some(plan) => plan.scopes(parameters, last),
            None => crate::Scopes::default(),
        };
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
        let Some(cookie) = schema else {
            return self.build_scope_plan(None, sql, statement);
        };
        self.app_statements
            .plans
            .plan(cookie, sql, || self.build_scope_plan(Some(cookie), sql, statement))
    }

    /// The plan `statement` brings, decoded and checked against this
    /// connection's schema, which `cookie` counts: kept for the database
    /// while the schema stays, and checked on each use with no cookie. A
    /// schema that shows a table, view or function the plan read otherwise
    /// than the schema the compiler built it against fails the statement
    /// with `CompiledSchemaDiffers`.
    pub(super) fn compiled_plan(
        &self,
        statement: CompiledStatement,
        cookie: Option<i64>,
    ) -> rusqlite::Result<Arc<ScopePlan>> {
        let check = |schema: &MainSchema| {
            let failed =
                |error: PolicyError<P>| rusqlite::Error::UserFunctionError(Box::new(error));
            let started = Instant::now();
            let plan = CompiledPlan::decode(statement.plan)
                .map_err(|error| failed(crate::Error::UndecodablePlan(error)))?;
            let whole = self.authorization.whole_views();
            let checked =
                self.authorization.unrecorded(|| plan.check(&self.connection, schema, whole))?;
            let checked_in = started.elapsed();
            tracing::debug!(?checked_in, "sqlive checked a compiled scope plan");
            match checked {
                Ok(()) => Ok(Arc::new(plan.into_plan())),
                Err(differs) => Err(failed(crate::Error::CompiledSchemaDiffers(differs))),
            }
        };
        let read = || self.authorization.unrecorded(|| MainSchema::read(&self.connection));
        match cookie {
            Some(cookie) => self.app_statements.plans.compiled(cookie, statement.plan, check, read),
            None => check(&read()?),
        }
    }

    /// Build `statement`'s plan against the schema `cookie` counts, or, with
    /// no cookie to keep a read under, against a read of its own.
    fn build_scope_plan(
        &self,
        cookie: Option<i64>,
        sql: &str,
        statement: &Statement<'_>,
    ) -> rusqlite::Result<Arc<ScopePlan>> {
        let started = Instant::now();
        let plan = self.authorization.unrecorded(|| {
            let read = || MainSchema::read(&self.connection);
            let schema = match cookie {
                Some(cookie) => self.app_statements.plans.schema(cookie, read)?,
                None => Arc::new(read()?),
            };
            let whole = self.authorization.whole_views();
            ScopePlan::build(&self.connection, &schema, sql, statement, whole)
        })?;
        let built_in = started.elapsed();
        let ScopePlanSize { sources, cores, edges } = plan.size;
        tracing::debug!(sources, cores, edges, ?built_in, "sqlive built a scope plan");
        if let Some(whole) = ReadReport::of(&plan).whole() {
            tracing::debug!(sql, %whole, "a live query's statement reads tables or views whole");
        }
        #[cfg(any(test, feature = "test-utils"))]
        self.record_work(|work| {
            work.scope_plans_built += 1;
            work.scope_plan_size += plan.size;
            work.scope_plan_time += built_in;
        });
        Ok(Arc::new(plan))
    }
}

/// Whom the authorizer judged a kept statement as.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum JudgedAs {
    /// The host's own statement.
    Host,
    /// The app's, by the policy `Policy::statement_key` names.
    App { policy: u64 },
}

/// Whose statement's steps a connection ran (WorkCounts).
#[derive(Clone, Copy)]
pub(super) enum StepsOf {
    /// The app's own statement that returns rows.
    AppRead,
    /// The app's own statement run for its changes.
    AppWrite,
    /// sqlive's lookup judging whether a write reruns a live query
    /// (SPEC.md section 7).
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
