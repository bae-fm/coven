//! The authorizer's app boundary and prepare-time trigger target rules.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, OnceLock};

use coven_foundation::locks::lock_unpoisoned;
use coven_foundation::task::Bug;

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

use crate::observation::ReadSet;
use crate::read_capture::{ReadCapture, ReadFrame};
use crate::row_scope::Scopes;
use crate::{DbError, SyncedTable};

/// The state an app statement is judged in as it is prepared: the same SQL
/// can be allowed in one and refused in another.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AppContext {
    writing: bool,
    applying: bool,
    reading: bool,
    /// Whether it records what it reads for a live query: a statement first
    /// prepared in a one-shot read recorded nothing to replay.
    capturing: bool,
}

/// Every change to the state but the authorizer's own is whole before it
/// can panic, and the authorizer catches its own panic (`callback`), so the
/// lock is taken as it is.
pub(crate) struct SqlAuthorization {
    state: Arc<Mutex<AuthorizationState>>,
    /// The handle the connection serves, once it serves one, which a panic
    /// in the authorizer fails.
    handle: Arc<OnceLock<crate::observation::CommitObserver>>,
}

impl ChangedTables {
    fn include(&mut self, change: Option<TableChange>) {
        match change {
            None => {}
            Some(TableChange::All) => *self = Self::All,
            Some(TableChange::Named(table)) => match self {
                Self::None => *self = Self::Named(BTreeSet::from([table])),
                Self::Named(tables) => {
                    tables.insert(table);
                }
                Self::All => {}
            },
        }
    }
}

enum TableChange {
    Named((String, String)),
    All,
}

/// How a statement being prepared changes which app tables exist. Coven's
/// internal staging tables can't be named by app SQL, and SQLite's own,
/// such as the statistics ANALYZE makes, hold no app rows: their creation
/// does not change which app rowids need protection.
fn changed_table(context: &AuthContext<'_>) -> Option<TableChange> {
    let named = |schema: &str, table: &str| {
        TableChange::Named((schema.to_ascii_lowercase(), table.to_ascii_lowercase()))
    };
    let (table_name, change) = match context.action {
        AuthAction::CreateTable { table_name } | AuthAction::DropTable { table_name } => {
            (table_name, named(context.database_name.unwrap_or("main"), table_name))
        }
        AuthAction::CreateTempTable { table_name } | AuthAction::DropTempTable { table_name } => {
            (table_name, named("temp", table_name))
        }
        // A virtual table makes shadow tables the authorizer never sees,
        // and a renamed table's new name never reaches it either.
        AuthAction::CreateVtable { table_name, .. }
        | AuthAction::DropVtable { table_name, .. }
        | AuthAction::AlterTable { table_name, .. } => (table_name, TableChange::All),
        _ => return None,
    };
    (!crate::internal_schema::reserved_name(table_name) && !table_name.starts_with("sqlite_"))
        .then_some(change)
}

/// The tables whose hidden rowids may have changed since coven last read
/// them: the ones statements created or dropped, by schema and name in
/// lowercase, or every table after one that altered a table, since a
/// renamed table's new name never reaches the authorizer.
#[derive(Debug, Default)]
pub(crate) enum ChangedTables {
    #[default]
    None,
    Named(BTreeSet<(String, String)>),
    All,
}

struct AuthorizationState {
    internal: bool,
    writing: bool,
    stepping: bool,
    applying: bool,
    observed: Option<StatementReads>,
    mutations: Option<BTreeSet<String>>,
    /// The tables of `main` a bulk write's statements, and the triggers and
    /// foreign-key actions SQLite compiles into them, may change, lowercase;
    /// `None` outside a bulk write.
    bulk: Option<BTreeSet<String>>,
    /// Whether app SQL runs in a read, which may only read.
    reading: bool,
    /// What a live query's statements read, while one of its runs reads.
    reads: Option<ReadCapture>,
    altering: bool,
    denied: Option<DbError>,
    /// The authorizer panicked with this: the state may be half-changed, so
    /// it refuses every later statement with it (E15).
    broken: Option<Bug>,
    /// Panic in the next judgment, as a bug in the authorizer would.
    #[cfg(test)]
    panic_next: bool,
    /// Panic in the next call of one of coven's SQL functions, as a bug in
    /// it would.
    #[cfg(test)]
    panic_next_function: bool,
    synced: BTreeSet<String>,
    shared: BTreeSet<String>,
    hidden_rowids: BTreeSet<(String, String)>,
    changed_tables: ChangedTables,
    /// Coven's tables the app statement being prepared reads through one of
    /// coven's views.
    through_views: BTreeSet<String>,
    /// The tables, lowercase, the app statement being prepared drops: their
    /// rowid guards go with them (`rowid_policy.rs`).
    dropping: BTreeSet<String>,
    /// Whether app SQL may read `coven_sync_status` and
    /// `coven_retry_effect`: only on the handle that holds the writer lock,
    /// whose own sync their rows describe. A read-only handle can't tell
    /// whether the writer that last wrote them still runs.
    writer_state: bool,
}

impl SqlAuthorization {
    /// The authorizer of a connection of the handle that holds the store's
    /// writer lock.
    pub(crate) fn new(tables: &[SyncedTable]) -> Self {
        Self::judging(tables, true)
    }

    /// The authorizer of a read-only handle's connection, which may not read
    /// the sync status or the retry effect (`internal_schema_sync_status.rs`,
    /// `internal_schema_retry_effect.rs`).
    pub(crate) fn read_only(tables: &[SyncedTable]) -> Self {
        Self::judging(tables, false)
    }

    fn judging(tables: &[SyncedTable], writer_state: bool) -> Self {
        Self {
            state: Arc::new(Mutex::new(AuthorizationState {
                internal: false,
                writing: false,
                stepping: false,
                applying: false,
                observed: None,
                mutations: None,
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
                synced: tables.iter().map(|t| t.name.to_ascii_lowercase()).collect(),
                shared: tables
                    .iter()
                    .flat_map(|t| t.shared_triggers.iter().map(|s| s.to_ascii_lowercase()))
                    .collect(),
                hidden_rowids: BTreeSet::new(),
                changed_tables: ChangedTables::None,
                through_views: BTreeSet::new(),
                dropping: BTreeSet::new(),
                writer_state,
            })),
            handle: Arc::new(OnceLock::new()),
        }
    }

    /// Panic in the next judgment, as a bug in the authorizer would.
    #[cfg(test)]
    pub(crate) fn panic_next(&self) {
        lock_unpoisoned(&self.state).panic_next = true;
    }

    /// Panic in the next call of one of coven's SQL functions.
    #[cfg(test)]
    pub(crate) fn panic_next_function(&self) {
        lock_unpoisoned(&self.state).panic_next_function = true;
    }

    /// `function`, one of coven's SQL functions named `name`, as the
    /// connection registers it. rusqlite turns a function's panic into an
    /// error that names no bug, so the function keeps it: the statement
    /// fails with the bug's text, and the app's call with `DbError::Bug`
    /// (E15). A function changes nothing, so the connection goes on.
    pub(crate) fn function<T>(
        &self,
        name: &'static str,
        function: impl Fn(&rusqlite::functions::Context<'_>) -> rusqlite::Result<T> + Send + 'static,
    ) -> impl Fn(&rusqlite::functions::Context<'_>) -> rusqlite::Result<T> + Send + 'static {
        let state = Arc::clone(&self.state);
        move |context| {
            let called = coven_foundation::task::caught_sync(name, || {
                #[cfg(test)]
                assert!(
                    !std::mem::take(&mut lock_unpoisoned(&state).panic_next_function),
                    "{name} lost its argument"
                );
                function(context)
            });
            called.unwrap_or_else(|bug| {
                lock_unpoisoned(&state).denied = Some(DbError::Bug(bug.clone()));
                Err(rusqlite::Error::UserFunctionError(Box::new(DbError::Bug(bug))))
            })
        }
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
        move |context| {
            let mut state = lock_unpoisoned(&state);
            if let Some(bug) = &state.broken {
                state.denied = Some(DbError::Bug(bug.clone()));
                return Authorization::Deny;
            }
            // SQLite's authorizer can't fail, and rusqlite turns its panic
            // into an error that names no bug, so the authorizer keeps it:
            // what it was changing may be half-changed, so the handle fails
            // and this connection refuses every later statement (E15).
            match coven_foundation::task::caught_sync("authorize a statement", || {
                state.judge(context)
            }) {
                Ok(authorization) => authorization,
                Err(bug) => {
                    if let Some(handle) = handle.get() {
                        handle.fail(crate::observation::HandleFailure::Bug(bug.clone()));
                    }
                    state.denied = Some(DbError::Bug(bug.clone()));
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
        InternalSql { authorization: self, previous }
    }

    pub(crate) fn applying_function(
        &self,
    ) -> impl Fn(&rusqlite::functions::Context<'_>) -> rusqlite::Result<bool> + Send + 'static {
        let state = Arc::clone(&self.state);
        move |_| Ok(lock_unpoisoned(&state).applying)
    }

    pub(crate) fn applying(&self) -> ApplyingSql<'_> {
        let mut state = lock_unpoisoned(&self.state);
        assert!(!state.applying, "applying SQL scope cannot nest");
        state.applying = true;
        ApplyingSql(self)
    }

    pub(crate) fn rowid_changed_function(
        &self,
    ) -> impl Fn(&rusqlite::functions::Context<'_>) -> rusqlite::Result<bool> + Send + 'static {
        let state = Arc::clone(&self.state);
        move |_| {
            let mut state = lock_unpoisoned(&state);
            if state.internal {
                return Ok(true);
            }
            state.denied =
                Some(DbError::StatementForbidden { operation: "changing a hidden rowid" });
            Err(rusqlite::Error::UserFunctionError("changing a hidden rowid".into()))
        }
    }

    pub(crate) fn observe_reads(&self) -> ObservedReads<'_> {
        let mut state = lock_unpoisoned(&self.state);
        assert!(state.observed.is_none(), "read observation cannot nest");
        state.observed = Some(StatementReads::default());
        ObservedReads(self)
    }

    pub(crate) fn write(&self) -> WriteSql<'_> {
        let mut state = lock_unpoisoned(&self.state);
        assert!(!state.writing, "write SQL scope cannot nest");
        state.writing = true;
        WriteSql(self)
    }

    /// A bulk write's SQL: a write's, refused any change to a synced table,
    /// recording instead each table of `main` its statements change.
    pub(crate) fn bulk(&self) -> BulkSql<'_> {
        let mut state = lock_unpoisoned(&self.state);
        assert!(!state.writing, "write SQL scope cannot nest");
        state.writing = true;
        state.bulk = Some(BTreeSet::new());
        BulkSql(self)
    }

    pub(crate) fn step(&self) -> StepSql<'_> {
        let mut state = lock_unpoisoned(&self.state);
        assert!(!state.stepping, "SQL step scope cannot nest");
        state.stepping = true;
        StepSql(self)
    }

    pub(crate) fn begin_app_call(&self) {
        let mut state = lock_unpoisoned(&self.state);
        state.denied = None;
        state.altering = false;
        state.through_views.clear();
        state.dropping.clear();
    }

    /// The tables whose hidden rowids may have changed since this was last
    /// asked, which the caller now reads again.
    pub(crate) fn take_changed_tables(&self) -> ChangedTables {
        std::mem::take(&mut lock_unpoisoned(&self.state).changed_tables)
    }

    /// Of the tables `read`, or every table when `None`, those whose hidden
    /// rowid is `unambiguous`: app SQL can't change it.
    pub(crate) fn set_hidden_rowids(
        &self,
        read: Option<&BTreeSet<(String, String)>>,
        unambiguous: BTreeSet<(String, String)>,
    ) {
        let mut state = lock_unpoisoned(&self.state);
        match read {
            Some(read) => {
                state.hidden_rowids.retain(|table| !read.contains(table));
                state.hidden_rowids.extend(unambiguous);
            }
            None => {
                state.hidden_rowids = unambiguous;
                state.changed_tables = ChangedTables::None;
            }
        }
    }

    /// A one-shot read's SQL: it may only read, and records nothing of
    /// what it reads, since nothing runs it again.
    pub(crate) fn reading(&self) -> ReadingSql<'_> {
        let mut state = lock_unpoisoned(&self.state);
        assert!(!state.reading, "read scopes cannot nest");
        state.reading = true;
        ReadingSql(self)
    }

    /// A live query's run's SQL: it may only read, and records what it
    /// reads, so a commit that changes it runs the query again (§4).
    pub(crate) fn live_reading(&self) -> ReadingSql<'_> {
        let reading = self.reading();
        lock_unpoisoned(&self.state).reads = Some(ReadCapture::new());
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
        state.reads.is_some() || state.observed.is_some() || state.mutations.is_some()
    }

    /// What decides how the authorizer judges an app statement it prepares,
    /// or `None` while preparing has effects beyond judging and recording
    /// reads, as in coven's own SQL, a migration's or a bulk write's: then
    /// each statement is prepared afresh. A bulk write's statement is also
    /// compiled for a connection with no pre-update hook, so SQLite may empty
    /// a table in one step there; kept, it would empty one so under a write's
    /// hook too, unseen.
    pub(crate) fn app_context(&self) -> Option<AppContext> {
        let state = lock_unpoisoned(&self.state);
        if state.internal
            || state.observed.is_some()
            || state.mutations.is_some()
            || state.bulk.is_some()
            || state.altering
        {
            return None;
        }
        Some(AppContext {
            writing: state.writing,
            applying: state.applying,
            reading: state.reading,
            capturing: state.reads.is_some(),
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

    /// Run coven's own SQL for the statement being read, as its metadata,
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

    /// Run SQL of coven's own for the statement being read, judged as app
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
            return Err(rusqlite::Error::UserFunctionError(Box::new(
                DbError::StatementForbidden { operation: "PRAGMA" },
            )));
        }

        // SQLITE_ALTER_TABLE reports the old name, not the rename destination.
        // Check the destination token too; comments and string contents are not SQL.
        for part in tokens.windows(3) {
            if part[0].word("rename") && part[1].word("to") {
                let name = match &part[2] {
                    crate::sql::Token::Word(name)
                    | crate::sql::Token::Quoted(name)
                    | crate::sql::Token::String(name) => name,
                    _ => continue,
                };
                if crate::internal_schema::reserved_name(name) {
                    return Err(rusqlite::Error::UserFunctionError(Box::new(
                        DbError::InternalTable { table: name.clone() },
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn mutations<T>(
        &self,
        prepare: impl FnOnce() -> rusqlite::Result<T>,
    ) -> rusqlite::Result<BTreeSet<String>> {
        lock_unpoisoned(&self.state).mutations = Some(BTreeSet::new());
        let result = prepare();
        let mutations = lock_unpoisoned(&self.state).mutations.take().expect("mutation capture");
        result?;
        Ok(mutations)
    }

    pub(crate) fn app_result<T>(&self, result: rusqlite::Result<T>) -> rusqlite::Result<T> {
        let denied = lock_unpoisoned(&self.state).denied.take();
        match denied {
            Some(error) => Err(rusqlite::Error::UserFunctionError(Box::new(error))),
            None => result,
        }
    }
}

pub(crate) struct InternalSql<'a> {
    authorization: &'a SqlAuthorization,
    previous: bool,
}

pub(crate) struct ApplyingSql<'a>(&'a SqlAuthorization);

impl Drop for ApplyingSql<'_> {
    fn drop(&mut self) {
        lock_unpoisoned(&self.0.state).applying = false;
    }
}

/// What the statements prepared while observing read: each column, in the
/// order first read, and each function they call, ASCII lowercase.
#[derive(Clone, Default)]
pub(crate) struct StatementReads {
    pub(crate) columns: Vec<String>,
    pub(crate) functions: BTreeSet<String>,
}

pub(crate) struct ObservedReads<'a>(&'a SqlAuthorization);

impl ObservedReads<'_> {
    pub(crate) fn reads(&self) -> StatementReads {
        lock_unpoisoned(&self.0.state).observed.as_ref().expect("observing reads").clone()
    }
}

impl Drop for ObservedReads<'_> {
    fn drop(&mut self) {
        lock_unpoisoned(&self.0.state).observed = None;
    }
}

pub(crate) struct WriteSql<'a>(&'a SqlAuthorization);

impl Drop for WriteSql<'_> {
    fn drop(&mut self) {
        lock_unpoisoned(&self.0.state).writing = false;
    }
}

/// A bulk write's SQL scope; it ends with the write's SQL.
pub(crate) struct BulkSql<'a>(&'a SqlAuthorization);

impl BulkSql<'_> {
    /// The tables of `main` the write's statements changed so far, as live
    /// queries name them.
    pub(crate) fn changed(&self) -> BTreeSet<String> {
        lock_unpoisoned(&self.0.state)
            .bulk
            .clone()
            .expect("a bulk write records the tables it changes")
    }
}

impl Drop for BulkSql<'_> {
    fn drop(&mut self) {
        let mut state = lock_unpoisoned(&self.0.state);
        state.writing = false;
        state.bulk = None;
    }
}

pub(crate) struct StepSql<'a>(&'a SqlAuthorization);

impl Drop for StepSql<'_> {
    fn drop(&mut self) {
        lock_unpoisoned(&self.0.state).stepping = false;
    }
}

pub(crate) struct ReadingSql<'a>(&'a SqlAuthorization);

impl Drop for ReadingSql<'_> {
    fn drop(&mut self) {
        let mut state = lock_unpoisoned(&self.0.state);
        state.reading = false;
        state.reads = None;
    }
}

impl Drop for InternalSql<'_> {
    fn drop(&mut self) {
        lock_unpoisoned(&self.authorization.state).internal = self.previous;
    }
}

impl AuthorizationState {
    /// Judge the statement being prepared, `context`, recording what it
    /// reads and changes.
    fn judge(&mut self, context: AuthContext<'_>) -> Authorization {
        #[cfg(test)]
        assert!(!std::mem::take(&mut self.panic_next), "the authorizer's state is past repair");
        if context.database_name == Some("main") {
            let changed = match context.action {
                AuthAction::Insert { table_name }
                | AuthAction::Update { table_name, .. }
                | AuthAction::Delete { table_name }
                | AuthAction::CreateIndex { table_name, .. }
                | AuthAction::DropIndex { table_name, .. } => Some(table_name),
                // The app's trigger on a table it may follow is its own
                // local object: making or dropping one changes no row of
                // coven's table and nothing that syncs, so a migration
                // doing it captures no synced state (§18.1).
                AuthAction::CreateTrigger { table_name, .. }
                | AuthAction::DropTrigger { table_name, .. } => {
                    (!crate::internal_schema::followed(table_name)).then_some(table_name)
                }
                _ => None,
            };
            if let (Some(table_name), Some(tables)) = (changed, &mut self.mutations) {
                tables.insert(table_name.to_ascii_lowercase());
            }
            if let (
                AuthAction::Insert { table_name }
                | AuthAction::Update { table_name, .. }
                | AuthAction::Delete { table_name },
                Some(tables),
            ) = (context.action, &mut self.bulk)
            {
                tables.insert(table_name.to_ascii_lowercase());
            }
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
        let changed = changed_table(&context);
        if self.internal {
            if let Some(error) = self.app_trigger_refusal(context) {
                self.denied = Some(error);
                return Authorization::Deny;
            }
            self.changed_tables.include(changed);
            return Authorization::Allow;
        }
        match self.refusal(context) {
            Some(error) => {
                self.denied = Some(error);
                Authorization::Deny
            }
            None => {
                self.changed_tables.include(changed);
                Authorization::Allow
            }
        }
    }

    /// Coven's own statements fire the app's triggers too: those on the
    /// tables they may follow, and those on the app's tables as coven writes
    /// them. Such a trigger, and any view of the app's it reads, is judged as
    /// in an app statement: what it writes, reads and calls (§9.4). Coven's
    /// own statement around it, and coven's triggers and views, are not.
    fn app_trigger_refusal(&mut self, context: AuthContext<'_>) -> Option<DbError> {
        context.accessor.filter(|name| !crate::internal_schema::reserved_name(name))?;
        self.refusal(context)
    }

    fn refusal(&mut self, context: AuthContext<'_>) -> Option<DbError> {
        use AuthAction::*;
        let reserved = |name: &str| {
            crate::internal_schema::reserved_name(name)
                .then(|| DbError::InternalTable { table: name.into() })
        };
        if let DropTable { table_name } | DropTempTable { table_name } = context.action {
            self.dropping.insert(table_name.to_ascii_lowercase());
        }
        // Materializing merged rows writes coven's files table as it writes
        // every synced table; nothing else written there comes from app SQL.
        if self.applying
            && context.accessor.is_none()
            && matches!(
                context.action,
                Insert { table_name } | Update { table_name, .. } | Delete { table_name }
                    if crate::file_table::is_files(table_name)
            )
        {
            return None;
        }
        // The status and retry rows say what their writer last saw, which a
        // read-only handle can't tell is still so: a writer that ended
        // mid-pass left its move behind (§19.1). The app's
        // triggers, judged as coven compiles them, never run on a read-only
        // handle, which changes no row.
        if let Read { table_name, .. } = context.action {
            if !self.writer_state
                && !self.internal
                && crate::internal_schema::WRITER_STATE
                    .iter()
                    .any(|table| table_name.eq_ignore_ascii_case(table))
            {
                return Some(DbError::StatementForbidden {
                    operation: "reading the writer's sync state through a read-only handle",
                });
            }
        }
        let object = match context.action {
            // Coven's views and its pins table are the app's read-only
            // window on coven's state: their own columns, each column the
            // views' joins read, and a table this statement reached through
            // a view that it reads no column of.
            // SQLite names no view for that last read, so a statement that
            // names such a table beside the view could count its rows.
            Read { table_name, .. } if crate::internal_schema::app_readable(table_name) => None,
            Read { table_name, .. }
                if context.accessor.is_some_and(crate::internal_schema::app_readable) =>
            {
                self.through_views.insert(table_name.to_ascii_lowercase());
                None
            }
            Read { table_name, column_name: "" }
                if context.accessor.is_none()
                    && self.through_views.contains(&table_name.to_ascii_lowercase()) =>
            {
                None
            }
            Read { table_name, .. }
            | Insert { table_name }
            | Update { table_name, .. }
            | Delete { table_name }
            | CreateTable { table_name }
            | CreateTempTable { table_name }
            | DropTable { table_name }
            | DropTempTable { table_name }
            | CreateVtable { table_name, .. }
            | DropVtable { table_name, .. }
            | AlterTable { table_name, .. }
            | Analyze { table_name } => Some(table_name),
            CreateIndex { index_name, table_name }
            | CreateTempIndex { index_name, table_name }
            | DropIndex { index_name, table_name }
            | DropTempIndex { index_name, table_name } => {
                if let Some(error) = reserved(index_name) {
                    return Some(error);
                }
                Some(table_name)
            }
            // The app's triggers may follow coven_files, coven_pins,
            // coven_upload_queue, coven_sync_status and coven_retry_effect,
            // the tables coven changes row by row (§9.4).
            CreateTrigger { trigger_name, table_name }
            | DropTrigger { trigger_name, table_name }
                if crate::internal_schema::followed(table_name) =>
            {
                return reserved(trigger_name);
            }
            // SQLite drops a table's triggers with it, coven's rowid guard
            // among them; the app can't drop a guard alone.
            DropTempTrigger { trigger_name, table_name }
                if crate::sqlite::rowid_guard(trigger_name)
                    && self.dropping.contains(&table_name.to_ascii_lowercase()) =>
            {
                Some(table_name)
            }
            CreateTrigger { trigger_name, table_name }
            | CreateTempTrigger { trigger_name, table_name }
            | DropTrigger { trigger_name, table_name }
            | DropTempTrigger { trigger_name, table_name } => {
                if let Some(error) = reserved(trigger_name) {
                    return Some(error);
                }
                Some(table_name)
            }
            Reindex { index_name } => {
                // SQLite names constraint indexes sqlite_autoindex_<table>_<n>.
                // Check the table without its ordinal: app tables named coven
                // or _coven do not have a reserved prefix.
                if index_name
                    .to_ascii_lowercase()
                    .strip_prefix("sqlite_autoindex_")
                    .and_then(|name| name.rsplit_once('_'))
                    .is_some_and(|(table, _)| crate::internal_schema::reserved_name(table))
                {
                    return Some(DbError::InternalTable { table: index_name.into() });
                }
                Some(index_name)
            }
            CreateView { view_name }
            | CreateTempView { view_name }
            | DropView { view_name }
            | DropTempView { view_name } => Some(view_name),
            _ => None,
        };
        if let Some(error) = object.and_then(reserved) {
            return Some(error);
        }
        // FTS5 reads the database's data version as it prepares a statement
        // over its table, in a read or in a write that changes its index, as
        // a local trigger keeping a search index does. Reading it changes
        // nothing; app SQL can't name it, since a PRAGMA statement and the
        // pragma_ table functions stay refused.
        if matches!(context.action, Pragma { pragma_name: "data_version", pragma_value: None }) {
            return None;
        }
        if self.reading
            && !matches!(context.action, Read { .. } | Select | Function { .. } | Recursive)
        {
            return Some(DbError::StatementForbidden {
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
            return Some(DbError::StatementForbidden { operation: "schema changes in a write" });
        }
        if matches!(context.action, AlterTable { .. }) {
            self.altering = true;
        }
        // SQLite compiles this read-only check as part of ALTER ADD COLUMN.
        // The permission lasts for this statement, never the following one.
        let internal_check = self.altering;
        let forbidden = match context.action {
            Update {
                table_name,
                column_name,
            } if column_name.eq_ignore_ascii_case("rowid")
                && context.database_name.is_some_and(|schema| {
                    self.hidden_rowids
                        .contains(&(schema.to_ascii_lowercase(), table_name.to_ascii_lowercase()))
                }) =>
            {
                Some("changing a hidden rowid")
            }
            // The session extension initializes its table metadata lazily from
            // its pre-update hook. App SQL was already authorized at prepare;
            // no app row-mapping callback runs with this permission enabled.
            Pragma {
                pragma_name: "table_xinfo",
                pragma_value: Some(_),
            } if self.stepping && context.database_name == Some("main") => None,
            Pragma {
                pragma_name: "quick_check",
                ..
            } if internal_check => None,
            Read {
                table_name: "pragma_quick_check",
                ..
            } if internal_check => None,
            CreateTempTrigger { trigger_name, .. }
                if self.shared.contains(&trigger_name.to_ascii_lowercase()) =>
            {
                Some("shadowing a shared trigger")
            }
            // Statistics aren't data: a write's capture would record them,
            // and coven gathers them itself at open and close (§4).
            Analyze { .. } if self.writing => Some("ANALYZE in a write"),
            Transaction { .. } | Savepoint { .. } => Some("transaction control"),
            Attach { .. } | Detach { .. } => Some("attaching or detaching databases"),
            Pragma { .. } => Some("PRAGMA"),
            Function { function_name } if function_name.eq_ignore_ascii_case("load_extension") => {
                Some("loading an extension")
            }
            // A live query learns it read what is being transferred from the
            // views' columns it reads; called anywhere else, nothing would
            // run it again as a transfer starts or ends.
            Function { function_name }
                if crate::transfers_in_flight::FUNCTIONS
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(function_name))
                    && !context
                        .accessor
                        .is_some_and(crate::internal_schema::app_readable) =>
            {
                Some("reading what is being transferred outside coven_uploads, coven_sending and coven_downloading")
            }
            // The same holds for the network, which changes with no write.
            Function { function_name }
                if crate::network_facts::FUNCTIONS
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(function_name))
                    && !context
                        .accessor
                        .is_some_and(crate::internal_schema::app_readable) =>
            {
                Some("reading the network outside coven_network and coven_uploads")
            }
            Read { table_name, .. } if table_name.to_ascii_lowercase().starts_with("pragma_") => {
                Some("PRAGMA")
            }
            _ => None,
        };
        if let Some(operation) = forbidden {
            return Some(DbError::StatementForbidden { operation });
        }
        if let (
            Some(trigger),
            Insert { table_name } | Update { table_name, .. } | Delete { table_name },
        ) = (context.accessor, context.action)
        {
            let shared = self.shared.contains(&trigger.to_ascii_lowercase());
            let synced = context.database_name == Some("main")
                && self.synced.contains(&table_name.to_ascii_lowercase());
            if shared != synced {
                return Some(DbError::TriggerTarget {
                    trigger: trigger.into(),
                    table: table_name.into(),
                });
            }
        }
        // A bulk write records no row change, so nothing it does can reach a
        // write record: the app's own statement, a trigger it fires, or a
        // foreign-key action.
        if let (
            Some(_),
            Insert { table_name } | Update { table_name, .. } | Delete { table_name },
        ) = (&self.bulk, context.action)
        {
            if context.database_name == Some("main")
                && self.synced.contains(&table_name.to_ascii_lowercase())
            {
                return Some(DbError::SyncedTableInBulkWrite { table: table_name.into() });
            }
        }
        None
    }
}

#[cfg(test)]
#[path = "authorization_tests.rs"]
pub(crate) mod tests;
