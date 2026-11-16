//! The authorizer: the rules the engine enforces on every statement, then
//! the policy its host supplies.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, OnceLock};

use caught::locks::lock_unpoisoned;
use caught::Bug;

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

use crate::{ReadCapture, ReadFrame, ReadSet, Scopes, WholeViews};

/// Whose statement SQLite is preparing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Owner {
    /// The app's statement, or an object of the app's it runs.
    App,
    /// The host's own statement, and the host's own objects it runs.
    Host,
    /// A trigger or view of the app's that a host statement runs.
    AppTriggerOfHost,
}

/// The scopes the statement is prepared in that the policy asks about.
#[derive(Clone, Copy, Debug)]
pub struct StatementScope {
    /// Whether it is prepared in a bulk write.
    pub bulk: bool,
}

/// One action SQLite reports, as the authorizer asks the policy about it.
pub struct PolicyAction<'a> {
    /// What the statement does, as SQLite reports it.
    pub action: AuthAction<'a>,
    /// The schema the action touches, `main`, `temp` or an attached one.
    pub database: Option<&'a str>,
    /// The trigger or view whose code the action runs in, if any.
    pub accessor: Option<&'a str>,
    /// Whose statement it is: the host's own or the app's.
    pub owner: Owner,
    /// The scope the statement runs in.
    pub scope: StatementScope,
}

/// What the engine's error asks of the host's failure and refusal types:
/// errors that cross threads, which the engine finds again inside SQLite's.
pub trait HostError: std::error::Error + Send + Sync + 'static {}

impl<T: std::error::Error + Send + Sync + 'static> HostError for T {}

/// What the host adds to the authorizer's rules.
pub trait Policy: Sized + Send + Sync + 'static {
    /// What the policy refuses a statement with.
    type Refusal: HostError;
    /// What the host's handle fails with: the host's own failures, and
    /// sqlive's, which the host converts into this type through `From`.
    type Failure: HostError + Clone + From<crate::EngineFailure<Self::Failure, Self::Refusal>>;
    /// Judge an action the authorizer's rules allowed; `Err` refuses the
    /// statement with it. A host statement's actions come here too, so the
    /// policy sees every action allowed.
    fn judge(&self, action: &PolicyAction<'_>) -> Result<(), Self::Refusal>;
    /// Judge an app statement's text before it is prepared.
    fn check_sql(&self, tokens: &[crate::sql::Token]) -> Result<(), Self::Refusal>;
    /// A new app call begins: forget what the last statement left.
    fn begin_app_call(&self);
    /// Whether `name`, a trigger or view, is the host's own object.
    fn host_object(&self, name: &str) -> bool;
    /// What the policy adds to the app statement cache's key.
    fn statement_key(&self) -> u64;
    /// A part of the host's work the policy judges differently, which the
    /// host runs through `HostSql::within`.
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
        sql: &crate::connection::HostSql<Self>,
        extent: Refresh,
    ) -> rusqlite::Result<()>;
}

/// The error a connection judged by `P` returns.
pub type PolicyError<P> = crate::Error<<P as Policy>::Failure, <P as Policy>::Refusal>;

/// How much of the schema the policy reads again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refresh {
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
pub struct FunctionCall<'a, P: Policy> {
    context: &'a rusqlite::functions::Context<'a>,
    owner: Owner,
    state: &'a Mutex<AuthorizationState<P>>,
}

impl<'a, P: Policy> FunctionCall<'a, P> {
    /// The function's arguments.
    pub fn context(&self) -> &'a rusqlite::functions::Context<'a> {
        self.context
    }

    /// Whose statement runs the function: the host's own, or the app's.
    pub fn owner(&self) -> Owner {
        self.owner
    }

    /// Refuse the statement running the function with `refusal`, in place
    /// of the message the function's own failure carries.
    pub fn deny(&self, refusal: P::Refusal) {
        lock_unpoisoned(self.state).denied = Some(crate::Error::Refused(refusal));
    }
}

/// Every change to the state but the authorizer's own is whole before it
/// can panic, and the authorizer catches its own panic (`callback`), so the
/// lock is taken as it is.
pub(crate) struct SqlAuthorization<P: Policy> {
    state: Arc<Mutex<AuthorizationState<P>>>,
    /// The handle the connection serves, once it serves one, which a panic
    /// in the authorizer fails.
    handle: Arc<OnceLock<crate::Handle<P::Failure>>>,
    /// The views and tables a live query reads whole.
    whole: Arc<WholeViews>,
    /// What the host adds to the rules. The authorizer asks it after its own
    /// rules allow an action.
    policy: Arc<P>,
}

struct AuthorizationState<P: Policy> {
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
    denied: Option<PolicyError<P>>,
    /// The authorizer panicked with this: the state may be half-changed, so
    /// it refuses every later statement with it (E15).
    broken: Option<Bug>,
    /// Panic in the next judgment, as a bug in the authorizer would.
    #[cfg(any(test, feature = "test-utils"))]
    panic_next: bool,
    /// Panic in the next call of one of the host's SQL functions, as a bug in
    /// it would.
    #[cfg(any(test, feature = "test-utils"))]
    panic_next_function: bool,
}

impl<P: Policy> SqlAuthorization<P> {
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
                #[cfg(any(test, feature = "test-utils"))]
                panic_next: false,
                #[cfg(any(test, feature = "test-utils"))]
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
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn panic_next(&self) {
        lock_unpoisoned(&self.state).panic_next = true;
    }

    /// Panic in the next call of one of the host's SQL functions.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn panic_next_function(&self) {
        lock_unpoisoned(&self.state).panic_next_function = true;
    }

    /// What wraps the host's SQL functions as the connection registers them.
    pub(crate) fn functions(&self) -> HostFunctions<P> {
        HostFunctions { state: Arc::clone(&self.state) }
    }

    /// The connection now serves `handle`, which a panic in its authorizer
    /// fails.
    pub(crate) fn serve(&self, handle: crate::Handle<P::Failure>) {
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
                state.denied = Some(crate::Error::HandleFailed(bug.clone()));
                return Authorization::Deny;
            }
            // SQLite's authorizer can't fail, and rusqlite turns its panic
            // into an error that names no bug, so the authorizer keeps it:
            // what it was changing may be half-changed, so the handle fails
            // and this connection refuses every later statement, repeating
            // the bug its first refusal returned (E15).
            match caught::caught_sync("authorize a statement", || state.judge(context, &*policy)) {
                Ok(authorization) => authorization,
                Err(bug) => {
                    if let Some(handle) = handle.get() {
                        handle.fail_with_bug(bug.clone());
                    }
                    state.denied = Some(crate::Error::Bug(bug.clone()));
                    state.broken = Some(bug);
                    Authorization::Deny
                }
            }
        }
    }

    pub(crate) fn internal(&self) -> InternalSql<'_, P> {
        let mut state = lock_unpoisoned(&self.state);
        let previous = state.internal;
        state.internal = true;
        InternalSql { state: &self.state, previous }
    }

    pub(crate) fn observe_reads(&self) -> ObservedReads<'_, P> {
        let mut state = lock_unpoisoned(&self.state);
        assert!(state.observed.is_none(), "read observation cannot nest");
        state.observed = Some(StatementReads::default());
        ObservedReads(&self.state)
    }

    pub(crate) fn write(&self) -> WriteSql<'_, P> {
        let mut state = lock_unpoisoned(&self.state);
        assert!(!state.writing, "write SQL scope cannot nest");
        state.writing = true;
        WriteSql(&self.state)
    }

    /// A bulk write's SQL: a write's, judged by the policy in the bulk scope,
    /// recording instead each table of `main` its statements change.
    pub(crate) fn bulk(&self) -> BulkSql<'_, P> {
        let mut state = lock_unpoisoned(&self.state);
        assert!(!state.writing, "write SQL scope cannot nest");
        state.writing = true;
        state.bulk = Some(BTreeSet::new());
        BulkSql(&self.state)
    }

    pub(crate) fn step(&self) -> StepSql<'_, P> {
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
    pub(crate) fn reading(&self) -> ReadingSql<'_, P> {
        let mut state = lock_unpoisoned(&self.state);
        assert!(!state.reading, "read scopes cannot nest");
        state.reading = true;
        ReadingSql(&self.state)
    }

    /// A live query's run's SQL: it may only read, and records what it
    /// reads, so a commit that changes it runs the query again (§4).
    pub(crate) fn live_reading(&self) -> ReadingSql<'_, P> {
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
        let tokens = crate::sql::tokens(sql)?;
        if tokens.first().is_some_and(|token| token.word("pragma")) {
            let error: PolicyError<P> = crate::Error::StatementForbidden { operation: "PRAGMA" };
            return Err(rusqlite::Error::UserFunctionError(Box::new(error)));
        }
        self.policy.check_sql(&tokens).map_err(|refusal| {
            rusqlite::Error::UserFunctionError(Box::new(
                crate::Error::<P::Failure, P::Refusal>::Refused(refusal),
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
pub(crate) struct HostFunctions<P: Policy> {
    state: Arc<Mutex<AuthorizationState<P>>>,
}

impl<P: Policy> HostFunctions<P> {
    /// `function`, one of the host's SQL functions named `name`, as the
    /// connection registers it. rusqlite turns a function's panic into an
    /// error that names no bug, so the function keeps it: the statement
    /// fails with the bug's text, and the app's call with `crate::Error::Bug`
    /// (E15). A function changes nothing, so the connection goes on.
    pub(crate) fn function<T: rusqlite::types::ToSql>(
        &self,
        name: &'static str,
        function: impl Fn(&FunctionCall<'_, P>) -> rusqlite::Result<T> + Send + 'static,
    ) -> impl Fn(&mut rusqlite::functions::Context<'_>) -> rusqlite::Result<T> + Send + 'static
    {
        let state = Arc::clone(&self.state);
        move |context| {
            let called = caught::caught_sync(name, || {
                #[cfg(any(test, feature = "test-utils"))]
                assert!(
                    !std::mem::take(&mut lock_unpoisoned(&state).panic_next_function),
                    "{name} lost its argument"
                );
                let owner = if lock_unpoisoned(&state).internal { Owner::Host } else { Owner::App };
                function(&FunctionCall { context: &*context, owner, state: &state })
            });
            called.unwrap_or_else(|bug| {
                lock_unpoisoned(&state).denied = Some(crate::Error::Bug(bug.clone()));
                let error: PolicyError<P> = crate::Error::Bug(bug);
                Err(rusqlite::Error::UserFunctionError(Box::new(error)))
            })
        }
    }
}

pub(crate) struct InternalSql<'a, P: Policy> {
    state: &'a Mutex<AuthorizationState<P>>,
    previous: bool,
}

/// What the statements prepared while observing read: each column, in the
/// order first read, and each function they call, ASCII lowercase.
#[derive(Clone, Default)]
pub struct StatementReads {
    /// Each column read, by its name alone, in the order first read.
    pub columns: Vec<String>,
    /// Each function called, ASCII lowercase.
    pub functions: BTreeSet<String>,
}

pub(crate) struct ObservedReads<'a, P: Policy>(&'a Mutex<AuthorizationState<P>>);

impl<P: Policy> ObservedReads<'_, P> {
    pub(crate) fn reads(&self) -> StatementReads {
        lock_unpoisoned(self.0).observed.as_ref().expect("observing reads").clone()
    }
}

impl<P: Policy> Drop for ObservedReads<'_, P> {
    fn drop(&mut self) {
        lock_unpoisoned(self.0).observed = None;
    }
}

/// The scope the app's write SQL runs in, from its making until it drops.
pub struct WriteSql<'a, P: Policy>(&'a Mutex<AuthorizationState<P>>);

impl<P: Policy> Drop for WriteSql<'_, P> {
    fn drop(&mut self) {
        lock_unpoisoned(self.0).writing = false;
    }
}

/// A bulk write's SQL scope; it ends with the write's SQL.
pub(crate) struct BulkSql<'a, P: Policy>(&'a Mutex<AuthorizationState<P>>);

impl<P: Policy> BulkSql<'_, P> {
    /// The tables of `main` the write's statements changed so far, as live
    /// queries name them.
    pub(crate) fn changed(&self) -> BTreeSet<String> {
        lock_unpoisoned(self.0).bulk.clone().expect("a bulk write records the tables it changes")
    }
}

impl<P: Policy> Drop for BulkSql<'_, P> {
    fn drop(&mut self) {
        let mut state = lock_unpoisoned(self.0);
        state.writing = false;
        state.bulk = None;
    }
}

pub(crate) struct StepSql<'a, P: Policy>(&'a Mutex<AuthorizationState<P>>);

impl<P: Policy> Drop for StepSql<'_, P> {
    fn drop(&mut self) {
        lock_unpoisoned(self.0).stepping = false;
    }
}

pub(crate) struct ReadingSql<'a, P: Policy>(&'a Mutex<AuthorizationState<P>>);

impl<P: Policy> Drop for ReadingSql<'_, P> {
    fn drop(&mut self) {
        let mut state = lock_unpoisoned(self.0);
        state.reading = false;
        state.reads = None;
    }
}

impl<P: Policy> Drop for InternalSql<'_, P> {
    fn drop(&mut self) {
        lock_unpoisoned(self.state).internal = self.previous;
    }
}

impl<P: Policy> AuthorizationState<P> {
    /// Whose statement `accessor`, the innermost trigger or view being
    /// prepared, if any, belongs to.
    fn owner(&self, accessor: Option<&str>, policy: &P) -> Owner {
        match accessor {
            _ if !self.internal => Owner::App,
            Some(name) if !policy.host_object(name) => Owner::AppTriggerOfHost,
            _ => Owner::Host,
        }
    }

    /// Judge the statement being prepared, `context`, recording what it
    /// reads and changes.
    fn judge(&mut self, context: AuthContext<'_>, policy: &P) -> Authorization {
        #[cfg(any(test, feature = "test-utils"))]
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
                .map_err(crate::Error::Refused)
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
    fn refusal(&mut self, context: AuthContext<'_>) -> Result<(), PolicyError<P>> {
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
            return Err(crate::Error::StatementForbidden {
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
            return Err(crate::Error::StatementForbidden {
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
            Some(operation) => Err(crate::Error::StatementForbidden { operation }),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
#[path = "authorization_tests.rs"]
pub(crate) mod tests;
