//! The authorizer: the rules the engine enforces on every statement, then
//! the policy its host supplies.

use crate::store_failure::StoreFailure;
use crate::DbError;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, OnceLock};

use coven_foundation::locks::lock_unpoisoned;
use coven_foundation::task::Bug;

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

use sqlive::{ReadCapture, ReadFrame, ReadSet, Scopes, WholeViews};

/// Whose statement SQLite is preparing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Owner {
    /// The app's statement, or an object of the app's it runs.
    App,
    /// The host's own statement, and the host's own objects it runs.
    Host,
    /// A trigger or view of the app's that a host statement runs.
    AppTriggerOfHost,
}

/// The scopes the statement is prepared in that the policy asks about.
#[derive(Clone, Copy, Debug)]
pub(crate) struct StatementScope {
    /// Whether it is prepared in a bulk write.
    pub(crate) bulk: bool,
}

/// One action SQLite reports, as the authorizer asks the policy about it.
pub(crate) struct PolicyAction<'a> {
    pub(crate) action: AuthAction<'a>,
    pub(crate) database: Option<&'a str>,
    pub(crate) accessor: Option<&'a str>,
    pub(crate) owner: Owner,
    pub(crate) scope: StatementScope,
}

/// What the host adds to the authorizer's rules.
pub(crate) trait Policy: Send + Sync + 'static {
    /// What the policy refuses a statement with.
    type Refusal: std::error::Error + Send + Sync + 'static;
    /// Judge an action the authorizer's rules allowed; `Err` refuses the
    /// statement with it. A host statement's actions come here too, so the
    /// policy sees every action allowed.
    fn judge(&self, action: &PolicyAction<'_>) -> Result<(), Self::Refusal>;
    /// Judge an app statement's text before it is prepared.
    fn check_sql(&self, tokens: &[sqlive::sql::Token]) -> Result<(), Self::Refusal>;
    /// A new app call begins: forget what the last statement left.
    fn begin_app_call(&self);
    /// Whether `name`, a trigger or view, is the host's own object.
    fn host_object(&self, name: &str) -> bool;
    /// What the policy adds to the app statement cache's key.
    fn statement_key(&self) -> u64;
    /// A part of the host's work the policy judges differently, which the
    /// host runs through `DatabaseConnection::within`.
    type Scope;
    /// Work in `scope` begins.
    fn enter(&self, scope: &Self::Scope);
    /// Work in `scope` ended, on any path out of it, a panic's included.
    fn leave(&self, scope: &Self::Scope);
    /// Read again what the policy keeps of the connection's schema: all of
    /// it, or what changed since it last read. The connection calls it with
    /// `Refresh::Whole` as it opens and with `Refresh::Changed` before each
    /// app statement, and the host through `refresh_policy`.
    fn refresh(
        &self,
        sql: &crate::sqlite::DatabaseConnection,
        extent: Refresh,
    ) -> rusqlite::Result<()>;
}

/// How much of the schema the policy reads again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Refresh {
    /// All of it, as the connection opens.
    Whole,
    /// What the statements since it last read changed.
    Changed,
}

/// The state an app statement is judged in as it is prepared: the same SQL
/// can be allowed in one and refused in another.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AppContext {
    writing: bool,
    reading: bool,
    /// Whether it records what it reads for a live query: a statement first
    /// prepared in a one-shot read recorded nothing to replay.
    capturing: bool,
    /// The policy's part of the cache key, so statements the policy judges
    /// differently get different entries.
    policy: u64,
}

/// One call of a SQL function registered through a connection's setup.
pub(crate) struct FunctionCall<'a> {
    context: &'a rusqlite::functions::Context<'a>,
    owner: Owner,
    state: &'a Mutex<AuthorizationState>,
}

impl<'a> FunctionCall<'a> {
    /// The function's arguments.
    pub(crate) fn context(&self) -> &'a rusqlite::functions::Context<'a> {
        self.context
    }

    /// Whose statement runs the function: the host's own, or the app's.
    pub(crate) fn owner(&self) -> Owner {
        self.owner
    }

    /// Refuse the statement running the function with `refusal`, in place
    /// of the message the function's own failure carries.
    pub(crate) fn deny(&self, refusal: DbError) {
        lock_unpoisoned(self.state).denied = Some(sqlive::Error::Refused(refusal));
    }
}

/// Every change to the state but the authorizer's own is whole before it
/// can panic, and the authorizer catches its own panic (`callback`), so the
/// lock is taken as it is.
pub(crate) struct SqlAuthorization<P> {
    state: Arc<Mutex<AuthorizationState>>,
    /// The handle the connection serves, once it serves one, which a panic
    /// in the authorizer fails.
    handle: Arc<OnceLock<crate::observation::CommitObserver>>,
    /// The views and tables a live query reads whole.
    whole: Arc<WholeViews>,
    /// What the host adds to the rules. The authorizer asks it after its own
    /// rules allow an action.
    policy: Arc<P>,
}

struct AuthorizationState {
    internal: bool,
    writing: bool,
    stepping: bool,
    observed: Option<StatementReads>,
    /// The tables of `main` a bulk write's statements, and the triggers and
    /// foreign-key actions SQLite compiles into them, may change, lowercase;
    /// `None` outside a bulk write.
    bulk: Option<BTreeSet<String>>,
    /// Whether app SQL runs in a read, which may only read.
    reading: bool,
    /// What a live query's statements read, while one of its runs reads.
    reads: Option<ReadCapture>,
    altering: bool,
    denied: Option<sqlive::Error<StoreFailure, DbError>>,
    /// The authorizer panicked with this: the state may be half-changed, so
    /// it refuses every later statement with it (E15).
    broken: Option<Bug>,
    /// Panic in the next judgment, as a bug in the authorizer would.
    #[cfg(test)]
    panic_next: bool,
    /// Panic in the next call of one of the host's SQL functions, as a bug in
    /// it would.
    #[cfg(test)]
    panic_next_function: bool,
}

impl<P: Policy<Refusal = DbError>> SqlAuthorization<P> {
    /// An authorizer that asks `policy` about each action its own rules
    /// allow.
    pub(crate) fn new(whole: Arc<WholeViews>, policy: Arc<P>) -> Self {
        Self {
            state: Arc::new(Mutex::new(AuthorizationState {
                internal: false,
                writing: false,
                stepping: false,
                observed: None,
                bulk: None,
                reading: false,
                reads: None,
                altering: false,
                denied: None,
                broken: None,
                #[cfg(test)]
                panic_next: false,
                #[cfg(test)]
                panic_next_function: false,
            })),
            handle: Arc::new(OnceLock::new()),
            whole,
            policy,
        }
    }

    /// The views and tables a live query reads whole.
    pub(crate) fn whole_views(&self) -> &WholeViews {
        &self.whole
    }

    /// Panic in the next judgment, as a bug in the authorizer would.
    #[cfg(test)]
    pub(crate) fn panic_next(&self) {
        lock_unpoisoned(&self.state).panic_next = true;
    }

    /// Panic in the next call of one of the host's SQL functions.
    #[cfg(test)]
    pub(crate) fn panic_next_function(&self) {
        lock_unpoisoned(&self.state).panic_next_function = true;
    }

    /// What wraps the host's SQL functions as the connection registers them.
    pub(crate) fn functions(&self) -> HostFunctions {
        HostFunctions { state: Arc::clone(&self.state) }
    }

    /// The connection now serves `handle`, which a panic in its authorizer
    /// fails.
    pub(crate) fn serve(&self, handle: crate::observation::CommitObserver) {
        if self.handle.set(handle).is_err() {
            unreachable!("a connection serves one handle");
        }
    }

    pub(crate) fn callback(&self) -> impl FnMut(AuthContext<'_>) -> Authorization + Send + 'static {
        let state = Arc::clone(&self.state);
        let handle = Arc::clone(&self.handle);
        let policy = Arc::clone(&self.policy);
        move |context| {
            let mut state = lock_unpoisoned(&state);
            if let Some(bug) = &state.broken {
                state.denied = Some(sqlive::Error::HandleFailed(bug.clone()));
                return Authorization::Deny;
            }
            // SQLite's authorizer can't fail, and rusqlite turns its panic
            // into an error that names no bug, so the authorizer keeps it:
            // what it was changing may be half-changed, so the handle fails
            // and this connection refuses every later statement, repeating
            // the bug its first refusal returned (E15).
            match coven_foundation::task::caught_sync("authorize a statement", || {
                state.judge(context, &*policy)
            }) {
                Ok(authorization) => authorization,
                Err(bug) => {
                    if let Some(handle) = handle.get() {
                        handle.fail_with_bug(bug.clone());
                    }
                    state.denied = Some(sqlive::Error::Bug(bug.clone()));
                    state.broken = Some(bug);
                    Authorization::Deny
                }
            }
        }
    }

    pub(crate) fn internal(&self) -> InternalSql<'_> {
        let mut state = lock_unpoisoned(&self.state);
        let previous = state.internal;
        state.internal = true;
        InternalSql { state: &self.state, previous }
    }

    pub(crate) fn observe_reads(&self) -> ObservedReads<'_> {
        let mut state = lock_unpoisoned(&self.state);
        assert!(state.observed.is_none(), "read observation cannot nest");
        state.observed = Some(StatementReads::default());
        ObservedReads(&self.state)
    }

    pub(crate) fn write(&self) -> WriteSql<'_> {
        let mut state = lock_unpoisoned(&self.state);
        assert!(!state.writing, "write SQL scope cannot nest");
        state.writing = true;
        WriteSql(&self.state)
    }

    /// A bulk write's SQL: a write's, refused any change to a synced table,
    /// recording instead each table of `main` its statements change.
    pub(crate) fn bulk(&self) -> BulkSql<'_> {
        let mut state = lock_unpoisoned(&self.state);
        assert!(!state.writing, "write SQL scope cannot nest");
        state.writing = true;
        state.bulk = Some(BTreeSet::new());
        BulkSql(&self.state)
    }

    pub(crate) fn step(&self) -> StepSql<'_> {
        let mut state = lock_unpoisoned(&self.state);
        assert!(!state.stepping, "SQL step scope cannot nest");
        state.stepping = true;
        StepSql(&self.state)
    }

    pub(crate) fn begin_app_call(&self) {
        let mut state = lock_unpoisoned(&self.state);
        state.denied = None;
        state.altering = false;
        drop(state);
        self.policy.begin_app_call();
    }

    /// A one-shot read's SQL: it may only read, and records nothing of
    /// what it reads, since nothing runs it again.
    pub(crate) fn reading(&self) -> ReadingSql<'_> {
        let mut state = lock_unpoisoned(&self.state);
        assert!(!state.reading, "read scopes cannot nest");
        state.reading = true;
        ReadingSql(&self.state)
    }

    /// A live query's run's SQL: it may only read, and records what it
    /// reads, so a commit that changes it runs the query again (§4).
    pub(crate) fn live_reading(&self) -> ReadingSql<'_> {
        let reading = self.reading();
        lock_unpoisoned(&self.state).reads = Some(ReadCapture::new(Arc::clone(&self.whole)));
        reading
    }

    pub(crate) fn reads(&self) -> ReadSet {
        let mut state = lock_unpoisoned(&self.state);
        state.reads.as_mut().expect("active read scope").reads()
    }

    /// Whether the authorizer is recording what statements read or change.
    /// It learns that only while a statement is prepared, so such a
    /// statement must not come from the cache.
    pub(crate) fn recording(&self) -> bool {
        let state = lock_unpoisoned(&self.state);
        state.reads.is_some() || state.observed.is_some()
    }

    /// What decides how the authorizer judges an app statement it prepares,
    /// or `None` while preparing has effects beyond judging and recording
    /// reads, as in the host's own SQL, a migration's or a bulk write's: then
    /// each statement is prepared afresh. A bulk write's statement is also
    /// compiled for a connection with no pre-update hook, so SQLite may empty
    /// a table in one step there; kept, it would empty one so under a write's
    /// hook too, unseen.
    pub(crate) fn app_context(&self) -> Option<AppContext> {
        let state = lock_unpoisoned(&self.state);
        if state.internal || state.observed.is_some() || state.bulk.is_some() || state.altering {
            return None;
        }
        Some(AppContext {
            writing: state.writing,
            reading: state.reading,
            capturing: state.reads.is_some(),
            policy: self.policy.statement_key(),
        })
    }

    /// The reads the statement being prepared recorded, so a later use of
    /// it, prepared already, records them too.
    pub(crate) fn statement_reads(&self) -> ReadFrame {
        lock_unpoisoned(&self.state)
            .reads
            .as_mut()
            .map(|reads| reads.statement())
            .unwrap_or_default()
    }

    /// Record the reads an already prepared statement made when it was
    /// prepared.
    pub(crate) fn replay_reads(&self, recorded: &ReadFrame) {
        if let Some(reads) = &mut lock_unpoisoned(&self.state).reads {
            reads.replay(recorded);
        }
    }

    pub(crate) fn begin_read_statement(&self) {
        if let Some(reads) = &mut lock_unpoisoned(&self.state).reads {
            reads.begin();
        }
    }

    /// Whether the statements prepared now record what they read for a
    /// live query.
    pub(crate) fn capturing_reads(&self) -> bool {
        lock_unpoisoned(&self.state).reads.is_some()
    }

    /// Run the host's own SQL for the statement being read, as its metadata,
    /// without recording its reads for the statement.
    pub(crate) fn unrecorded<T>(&self, run: impl FnOnce() -> T) -> T {
        let reads = lock_unpoisoned(&self.state).reads.take();
        let result = {
            let _internal = self.internal();
            run()
        };
        lock_unpoisoned(&self.state).reads = reads;
        result
    }

    /// Run SQL of the host's own for the statement being read, judged as app
    /// SQL is, without recording its reads for the statement: it reads only
    /// what the statement read.
    pub(crate) fn judged_unrecorded<T>(
        &self,
        run: impl FnOnce() -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let reads = lock_unpoisoned(&self.state).reads.take();
        self.begin_app_call();
        let result = self.app_result(run());
        lock_unpoisoned(&self.state).reads = reads;
        result
    }

    /// End the statement begun last: each table it read in the scope
    /// `scopes` gives it, or whole when `scopes` gives none.
    pub(crate) fn record_statement(&self, scopes: &Scopes) {
        let mut state = lock_unpoisoned(&self.state);
        let Some(reads) = &mut state.reads else {
            return;
        };
        reads.end(scopes);
    }

    pub(crate) fn check_sql(&self, sql: &str) -> rusqlite::Result<()> {
        // FTS prepares PRAGMA data_version itself. Permit that internal read,
        // while keeping direct app PRAGMAs outside the SQL capability.
        let tokens = sqlive::sql::tokens(sql)?;
        if tokens.first().is_some_and(|token| token.word("pragma")) {
            let error: sqlive::Error<StoreFailure, DbError> =
                sqlive::Error::StatementForbidden { operation: "PRAGMA" };
            return Err(rusqlite::Error::UserFunctionError(Box::new(error)));
        }
        self.policy.check_sql(&tokens).map_err(|refusal| {
            rusqlite::Error::UserFunctionError(Box::new(
                sqlive::Error::<StoreFailure, DbError>::Refused(refusal),
            ))
        })
    }

    pub(crate) fn app_result<T>(&self, result: rusqlite::Result<T>) -> rusqlite::Result<T> {
        let denied = lock_unpoisoned(&self.state).denied.take();
        match denied {
            Some(error) => Err(rusqlite::Error::UserFunctionError(Box::new(error))),
            None => result,
        }
    }
}

/// Wraps the host's SQL functions, sharing the authorizer's state: the
/// function learns whose statement runs it, and its refusal or panic fails
/// that statement.
pub(crate) struct HostFunctions {
    state: Arc<Mutex<AuthorizationState>>,
}

impl HostFunctions {
    /// `function`, one of the host's SQL functions named `name`, as the
    /// connection registers it. rusqlite turns a function's panic into an
    /// error that names no bug, so the function keeps it: the statement
    /// fails with the bug's text, and the app's call with `sqlive::Error::Bug`
    /// (E15). A function changes nothing, so the connection goes on.
    pub(crate) fn function<T: rusqlite::types::ToSql>(
        &self,
        name: &'static str,
        function: impl Fn(&FunctionCall<'_>) -> rusqlite::Result<T> + Send + 'static,
    ) -> impl Fn(&mut rusqlite::functions::Context<'_>) -> rusqlite::Result<T> + Send + 'static
    {
        let state = Arc::clone(&self.state);
        move |context| {
            let called = coven_foundation::task::caught_sync(name, || {
                #[cfg(test)]
                assert!(
                    !std::mem::take(&mut lock_unpoisoned(&state).panic_next_function),
                    "{name} lost its argument"
                );
                let owner = if lock_unpoisoned(&state).internal { Owner::Host } else { Owner::App };
                function(&FunctionCall { context: &*context, owner, state: &state })
            });
            called.unwrap_or_else(|bug| {
                lock_unpoisoned(&state).denied = Some(sqlive::Error::Bug(bug.clone()));
                let error: sqlive::Error<StoreFailure, DbError> = sqlive::Error::Bug(bug);
                Err(rusqlite::Error::UserFunctionError(Box::new(error)))
            })
        }
    }
}

pub(crate) struct InternalSql<'a> {
    state: &'a Mutex<AuthorizationState>,
    previous: bool,
}

/// What the statements prepared while observing read: each column, in the
/// order first read, and each function they call, ASCII lowercase.
#[derive(Clone, Default)]
pub(crate) struct StatementReads {
    pub(crate) columns: Vec<String>,
    pub(crate) functions: BTreeSet<String>,
}

pub(crate) struct ObservedReads<'a>(&'a Mutex<AuthorizationState>);

impl ObservedReads<'_> {
    pub(crate) fn reads(&self) -> StatementReads {
        lock_unpoisoned(self.0).observed.as_ref().expect("observing reads").clone()
    }
}

impl Drop for ObservedReads<'_> {
    fn drop(&mut self) {
        lock_unpoisoned(self.0).observed = None;
    }
}

pub(crate) struct WriteSql<'a>(&'a Mutex<AuthorizationState>);

impl Drop for WriteSql<'_> {
    fn drop(&mut self) {
        lock_unpoisoned(self.0).writing = false;
    }
}

/// A bulk write's SQL scope; it ends with the write's SQL.
pub(crate) struct BulkSql<'a>(&'a Mutex<AuthorizationState>);

impl BulkSql<'_> {
    /// The tables of `main` the write's statements changed so far, as live
    /// queries name them.
    pub(crate) fn changed(&self) -> BTreeSet<String> {
        lock_unpoisoned(self.0).bulk.clone().expect("a bulk write records the tables it changes")
    }
}

impl Drop for BulkSql<'_> {
    fn drop(&mut self) {
        let mut state = lock_unpoisoned(self.0);
        state.writing = false;
        state.bulk = None;
    }
}

pub(crate) struct StepSql<'a>(&'a Mutex<AuthorizationState>);

impl Drop for StepSql<'_> {
    fn drop(&mut self) {
        lock_unpoisoned(self.0).stepping = false;
    }
}

pub(crate) struct ReadingSql<'a>(&'a Mutex<AuthorizationState>);

impl Drop for ReadingSql<'_> {
    fn drop(&mut self) {
        let mut state = lock_unpoisoned(self.0);
        state.reading = false;
        state.reads = None;
    }
}

impl Drop for InternalSql<'_> {
    fn drop(&mut self) {
        lock_unpoisoned(self.state).internal = self.previous;
    }
}

impl AuthorizationState {
    /// Whose statement `accessor`, the innermost trigger or view being
    /// prepared, if any, belongs to.
    fn owner(&self, accessor: Option<&str>, policy: &impl Policy) -> Owner {
        match accessor {
            _ if !self.internal => Owner::App,
            Some(name) if !policy.host_object(name) => Owner::AppTriggerOfHost,
            _ => Owner::Host,
        }
    }

    /// Judge the statement being prepared, `context`, recording what it
    /// reads and changes.
    fn judge(
        &mut self,
        context: AuthContext<'_>,
        policy: &impl Policy<Refusal = DbError>,
    ) -> Authorization {
        #[cfg(test)]
        assert!(!std::mem::take(&mut self.panic_next), "the authorizer's state is past repair");
        if let (
            AuthAction::Insert { table_name }
            | AuthAction::Update { table_name, .. }
            | AuthAction::Delete { table_name },
            Some(tables),
            Some("main"),
        ) = (context.action, &mut self.bulk, context.database_name)
        {
            tables.insert(table_name.to_ascii_lowercase());
        }
        if let AuthAction::Read { table_name, column_name } = context.action {
            if let Some(reads) = &mut self.reads {
                reads.read(table_name, column_name, context.accessor);
            }
            if let Some(observed) = &mut self.observed {
                if !column_name.is_empty() && !observed.columns.iter().any(|c| c == column_name) {
                    observed.columns.push(column_name.into());
                }
            }
        }
        if let AuthAction::Function { function_name } = context.action {
            if let Some(observed) = &mut self.observed {
                observed.functions.insert(function_name.to_ascii_lowercase());
            }
        }
        let owner = self.owner(context.accessor, policy);
        let judged = match owner {
            Owner::Host => Ok(()),
            Owner::App | Owner::AppTriggerOfHost => self.refusal(context),
        }
        .and_then(|()| {
            policy
                .judge(&PolicyAction {
                    action: context.action,
                    database: context.database_name,
                    accessor: context.accessor,
                    owner,
                    scope: StatementScope { bulk: self.bulk.is_some() },
                })
                .map_err(sqlive::Error::Refused)
        });
        match judged {
            Ok(()) => Authorization::Allow,
            Err(error) => {
                self.denied = Some(error);
                Authorization::Deny
            }
        }
    }

    /// The error that refuses `context` by the authorizer's own rules, if
    /// any.
    fn refusal(
        &mut self,
        context: AuthContext<'_>,
    ) -> Result<(), sqlive::Error<StoreFailure, DbError>> {
        use AuthAction::*;
        // FTS5 reads the database's data version as it prepares a statement
        // over its table, in a read or in a write that changes its index, as
        // a local trigger keeping a search index does. Reading it changes
        // nothing; app SQL can't name it, since a PRAGMA statement and the
        // pragma_ table functions stay refused.
        if matches!(context.action, Pragma { pragma_name: "data_version", pragma_value: None }) {
            return Ok(());
        }
        if self.reading
            && !matches!(context.action, Read { .. } | Select | Function { .. } | Recursive)
        {
            return Err(sqlive::Error::StatementForbidden {
                operation: "writing through a read context",
            });
        }
        if self.writing
            && matches!(
                context.action,
                CreateTable { .. }
                    | CreateTempTable { .. }
                    | DropTable { .. }
                    | DropTempTable { .. }
                    | CreateVtable { .. }
                    | DropVtable { .. }
                    | AlterTable { .. }
                    | CreateIndex { .. }
                    | CreateTempIndex { .. }
                    | DropIndex { .. }
                    | DropTempIndex { .. }
                    | CreateTrigger { .. }
                    | CreateTempTrigger { .. }
                    | DropTrigger { .. }
                    | DropTempTrigger { .. }
                    | CreateView { .. }
                    | CreateTempView { .. }
                    | DropView { .. }
                    | DropTempView { .. }
            )
        {
            return Err(sqlive::Error::StatementForbidden {
                operation: "schema changes in a write",
            });
        }
        if matches!(context.action, AlterTable { .. }) {
            self.altering = true;
        }
        // SQLite compiles this read-only check as part of ALTER ADD COLUMN.
        // The permission lasts for this statement, never the following one.
        let internal_check = self.altering;
        let forbidden = match context.action {
            // The session extension initializes its table metadata lazily from
            // its pre-update hook. App SQL was already authorized at prepare;
            // no app row-mapping callback runs with this permission enabled.
            Pragma { pragma_name: "table_xinfo", pragma_value: Some(_) }
                if self.stepping && context.database_name == Some("main") =>
            {
                None
            }
            Pragma { pragma_name: "quick_check", .. } if internal_check => None,
            Read { table_name: "pragma_quick_check", .. } if internal_check => None,
            // Statistics aren't data: a write's capture would record them,
            // and the host gathers them itself.
            Analyze { .. } if self.writing => Some("ANALYZE in a write"),
            Transaction { .. } | Savepoint { .. } => Some("transaction control"),
            Attach { .. } | Detach { .. } => Some("attaching or detaching databases"),
            Pragma { .. } => Some("PRAGMA"),
            Function { function_name } if function_name.eq_ignore_ascii_case("load_extension") => {
                Some("loading an extension")
            }
            Read { table_name, .. } if table_name.to_ascii_lowercase().starts_with("pragma_") => {
                Some("PRAGMA")
            }
            _ => None,
        };
        match forbidden {
            Some(operation) => Err(sqlive::Error::StatementForbidden { operation }),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
#[path = "authorization_tests.rs"]
pub(crate) mod tests;
