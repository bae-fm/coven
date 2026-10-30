//! The authorizer's app boundary and prepare-time trigger target rules.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

use crate::key_scope::KeyScope;
use crate::observation::{ColumnSet, ReadSet, TableRead};
use crate::{DbError, SyncedTable};

struct ReadCapture {
    current: ColumnSet,
    completed: ReadSet,
}

impl ReadCapture {
    fn finish(&mut self, keys: KeyScope) {
        let mut tables: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (table, column) in std::mem::take(&mut self.current) {
            tables.entry(table).or_default().insert(column);
        }
        self.completed
            .extend(tables.into_iter().map(|(table, columns)| TableRead {
                table,
                columns,
                keys: keys.clone(),
                inserted_only: false,
            }));
    }
}

/// The state an app statement is judged in as it is prepared: the same SQL
/// can be allowed in one and refused in another.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AppContext {
    writing: bool,
    applying: bool,
    reading: bool,
}

pub(crate) struct SqlAuthorization {
    state: Arc<Mutex<AuthorizationState>>,
}

struct AuthorizationState {
    internal: bool,
    writing: bool,
    stepping: bool,
    applying: bool,
    observed: Option<StatementReads>,
    mutations: Option<BTreeSet<String>>,
    reads: Option<ReadCapture>,
    altering: bool,
    denied: Option<DbError>,
    synced: BTreeSet<String>,
    shared: BTreeSet<String>,
    hidden_rowids: BTreeSet<(String, String)>,
    tables_changed: bool,
    /// Coven's tables the app statement being prepared reads through one of
    /// coven's views.
    through_views: BTreeSet<String>,
}

impl SqlAuthorization {
    pub(crate) fn new(tables: &[SyncedTable]) -> Self {
        Self {
            state: Arc::new(Mutex::new(AuthorizationState {
                internal: false,
                writing: false,
                stepping: false,
                applying: false,
                observed: None,
                mutations: None,
                reads: None,
                altering: false,
                denied: None,
                synced: tables.iter().map(|t| t.name.to_ascii_lowercase()).collect(),
                shared: tables
                    .iter()
                    .flat_map(|t| t.shared_triggers.iter().map(|s| s.to_ascii_lowercase()))
                    .collect(),
                hidden_rowids: BTreeSet::new(),
                tables_changed: false,
                through_views: BTreeSet::new(),
            })),
        }
    }

    pub(crate) fn callback(&self) -> impl FnMut(AuthContext<'_>) -> Authorization + Send + 'static {
        let state = Arc::clone(&self.state);
        move |context| {
            let mut state = state.lock().expect("SQL authorization lock poisoned");
            if context.database_name == Some("main") {
                if let AuthAction::Insert { table_name }
                | AuthAction::Update { table_name, .. }
                | AuthAction::Delete { table_name }
                | AuthAction::CreateIndex { table_name, .. }
                | AuthAction::DropIndex { table_name, .. }
                | AuthAction::CreateTrigger { table_name, .. }
                | AuthAction::DropTrigger { table_name, .. } = context.action
                {
                    if let Some(tables) = &mut state.mutations {
                        tables.insert(table_name.to_ascii_lowercase());
                    }
                }
            }
            if let AuthAction::Read {
                table_name,
                column_name,
            } = context.action
            {
                if let Some(reads) = &mut state.reads {
                    reads.current.insert((
                        table_name.to_ascii_lowercase(),
                        column_name.to_ascii_lowercase(),
                    ));
                    // A read through one of coven's views reads the view
                    // itself too, even when the statement reads none of the
                    // view's columns, as a count of its rows or an EXISTS
                    // doesn't: a view can change with no change to its
                    // tables, as `coven_sending` does (§4).
                    if let Some(view) = context
                        .accessor
                        .filter(|name| crate::internal_schema::app_readable(name))
                    {
                        reads
                            .current
                            .insert((view.to_ascii_lowercase(), String::new()));
                    }
                }
                if let Some(observed) = &mut state.observed {
                    if !column_name.is_empty() && !observed.columns.iter().any(|c| c == column_name)
                    {
                        observed.columns.push(column_name.into());
                    }
                }
            }
            if let AuthAction::Function { function_name } = context.action {
                if let Some(observed) = &mut state.observed {
                    observed
                        .functions
                        .insert(function_name.to_ascii_lowercase());
                }
            }
            // Internal staging tables cannot be named by app SQL, and SQLite's
            // own, such as the statistics ANALYZE makes, hold no app rows.
            // Their creation does not change which app rowids need protection.
            let changes_tables = match context.action {
                AuthAction::CreateTable { table_name }
                | AuthAction::CreateTempTable { table_name }
                | AuthAction::DropTable { table_name }
                | AuthAction::DropTempTable { table_name }
                | AuthAction::CreateVtable { table_name, .. }
                | AuthAction::DropVtable { table_name, .. }
                | AuthAction::AlterTable { table_name, .. } => {
                    !crate::internal_schema::reserved_name(table_name)
                        && !table_name.starts_with("sqlite_")
                }
                _ => false,
            };
            if state.internal {
                if let Some(error) = state.app_trigger_refusal(context) {
                    state.denied = Some(error);
                    return Authorization::Deny;
                }
                state.tables_changed |= changes_tables;
                return Authorization::Allow;
            }
            match state.refusal(context) {
                Some(error) => {
                    state.denied = Some(error);
                    Authorization::Deny
                }
                None => {
                    state.tables_changed |= changes_tables;
                    Authorization::Allow
                }
            }
        }
    }

    pub(crate) fn internal(&self) -> InternalSql<'_> {
        let mut state = self.state.lock().expect("SQL authorization lock poisoned");
        let previous = state.internal;
        state.internal = true;
        InternalSql {
            authorization: self,
            previous,
        }
    }

    pub(crate) fn applying_function(
        &self,
    ) -> impl Fn(&rusqlite::functions::Context<'_>) -> rusqlite::Result<bool> + Send + 'static {
        let state = Arc::clone(&self.state);
        move |_| {
            Ok(state
                .lock()
                .expect("SQL authorization lock poisoned")
                .applying)
        }
    }

    pub(crate) fn applying(&self) -> ApplyingSql<'_> {
        let mut state = self.state.lock().expect("SQL authorization lock poisoned");
        assert!(!state.applying, "applying SQL scope cannot nest");
        state.applying = true;
        ApplyingSql(self)
    }

    pub(crate) fn rowid_changed_function(
        &self,
    ) -> impl Fn(&rusqlite::functions::Context<'_>) -> rusqlite::Result<bool> + Send + 'static {
        let state = Arc::clone(&self.state);
        move |_| {
            let mut state = state.lock().expect("SQL authorization lock poisoned");
            if state.internal {
                return Ok(true);
            }
            state.denied = Some(DbError::StatementForbidden {
                operation: "changing a hidden rowid",
            });
            Err(rusqlite::Error::UserFunctionError(
                "changing a hidden rowid".into(),
            ))
        }
    }

    pub(crate) fn observe_reads(&self) -> ObservedReads<'_> {
        let mut state = self.state.lock().expect("SQL authorization lock poisoned");
        assert!(state.observed.is_none(), "read observation cannot nest");
        state.observed = Some(StatementReads::default());
        ObservedReads(self)
    }

    pub(crate) fn write(&self) -> WriteSql<'_> {
        let mut state = self.state.lock().expect("SQL authorization lock poisoned");
        assert!(!state.writing, "write SQL scope cannot nest");
        state.writing = true;
        WriteSql(self)
    }

    pub(crate) fn step(&self) -> StepSql<'_> {
        let mut state = self.state.lock().expect("SQL authorization lock poisoned");
        assert!(!state.stepping, "SQL step scope cannot nest");
        state.stepping = true;
        StepSql(self)
    }

    pub(crate) fn begin_app_call(&self) {
        let mut state = self.state.lock().expect("SQL authorization lock poisoned");
        state.denied = None;
        state.altering = false;
        state.through_views.clear();
    }

    pub(crate) fn tables_changed(&self) -> bool {
        self.state
            .lock()
            .expect("SQL authorization lock poisoned")
            .tables_changed
    }

    pub(crate) fn set_hidden_rowids(&self, tables: BTreeSet<(String, String)>) {
        let mut state = self.state.lock().expect("SQL authorization lock poisoned");
        state.hidden_rowids = tables;
        state.tables_changed = false;
    }

    pub(crate) fn reading(&self) -> ReadingSql<'_> {
        let mut state = self.state.lock().expect("SQL authorization lock poisoned");
        assert!(state.reads.is_none(), "read scopes cannot nest");
        state.reads = Some(ReadCapture {
            current: ColumnSet::new(),
            completed: ReadSet::new(),
        });
        ReadingSql(self)
    }

    pub(crate) fn reads(&self) -> ReadSet {
        let mut state = self.state.lock().expect("SQL authorization lock poisoned");
        let reads = state.reads.as_mut().expect("active read scope");
        reads.finish(KeyScope::All);
        reads.completed.clone()
    }

    /// Whether the authorizer is recording what statements read or change.
    /// It learns that only while a statement is prepared, so such a
    /// statement must not come from the cache.
    pub(crate) fn recording(&self) -> bool {
        let state = self.state.lock().expect("SQL authorization lock poisoned");
        state.reads.is_some() || state.observed.is_some() || state.mutations.is_some()
    }

    /// What decides how the authorizer judges an app statement it prepares,
    /// or `None` while preparing has effects beyond judging and recording
    /// reads, as in coven's own SQL or a migration's: then each statement is
    /// prepared afresh.
    pub(crate) fn app_context(&self) -> Option<AppContext> {
        let state = self.state.lock().expect("SQL authorization lock poisoned");
        if state.internal || state.observed.is_some() || state.mutations.is_some() || state.altering
        {
            return None;
        }
        Some(AppContext {
            writing: state.writing,
            applying: state.applying,
            reading: state.reads.is_some(),
        })
    }

    /// The reads the statement being prepared recorded, so a later use of
    /// it, prepared already, records them too.
    pub(crate) fn statement_reads(&self) -> ColumnSet {
        self.state
            .lock()
            .expect("SQL authorization lock poisoned")
            .reads
            .as_ref()
            .map(|reads| reads.current.clone())
            .unwrap_or_default()
    }

    /// Record the reads an already prepared statement made when it was
    /// prepared.
    pub(crate) fn replay_reads(&self, recorded: &ColumnSet) {
        if let Some(reads) = &mut self
            .state
            .lock()
            .expect("SQL authorization lock poisoned")
            .reads
        {
            reads.current.extend(recorded.iter().cloned());
        }
    }

    pub(crate) fn begin_read_statement(&self) {
        if let Some(reads) = &mut self
            .state
            .lock()
            .expect("SQL authorization lock poisoned")
            .reads
        {
            // Also preserves dependencies when a row-mapping closure nests SQL.
            reads.finish(KeyScope::All);
        }
    }

    pub(crate) fn record_statement(
        &self,
        connection: &rusqlite::Connection,
        statement: Option<(
            &str,
            &rusqlite::Statement<'_>,
            &[crate::sql_value::SqlValue],
        )>,
    ) -> rusqlite::Result<()> {
        let reads = self
            .state
            .lock()
            .expect("SQL authorization lock poisoned")
            .reads
            .take();
        let Some(mut reads) = reads else {
            return Ok(());
        };
        let _internal = self.internal();
        // Metadata and literal evaluation are ours, not dependencies of the app
        // query. Restore capture on either success or an SQLite error.
        let result = (|| {
            let tables: BTreeSet<_> = reads.current.iter().map(|(table, _)| table).collect();
            let keys = match (statement, tables.len()) {
                (Some((sql, statement, parameters)), 1) => KeyScope::for_statement(
                    connection,
                    sql,
                    tables.first().expect("one table"),
                    statement,
                    parameters,
                )?,
                _ => KeyScope::All,
            };
            reads.finish(keys);
            Ok(())
        })();
        self.state
            .lock()
            .expect("SQL authorization lock poisoned")
            .reads = Some(reads);
        result
    }

    pub(crate) fn check_sql(&self, sql: &str) -> rusqlite::Result<()> {
        // FTS prepares PRAGMA data_version itself. Permit that internal read,
        // while keeping direct app PRAGMAs outside the SQL capability.
        let tokens = crate::sql::tokens(sql)?;
        if tokens.first().is_some_and(|token| token.word("pragma")) {
            return Err(rusqlite::Error::UserFunctionError(Box::new(
                DbError::StatementForbidden {
                    operation: "PRAGMA",
                },
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
                        DbError::InternalTable {
                            table: name.clone(),
                        },
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
        self.state
            .lock()
            .expect("SQL authorization lock poisoned")
            .mutations = Some(BTreeSet::new());
        let result = prepare();
        let mutations = self
            .state
            .lock()
            .expect("SQL authorization lock poisoned")
            .mutations
            .take()
            .expect("mutation capture");
        result?;
        Ok(mutations)
    }

    pub(crate) fn app_result<T>(&self, result: rusqlite::Result<T>) -> rusqlite::Result<T> {
        let denied = self
            .state
            .lock()
            .expect("SQL authorization lock poisoned")
            .denied
            .take();
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
        self.0
            .state
            .lock()
            .expect("SQL authorization lock poisoned")
            .applying = false;
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
        self.0
            .state
            .lock()
            .expect("SQL authorization lock poisoned")
            .observed
            .as_ref()
            .expect("observing reads")
            .clone()
    }
}

impl Drop for ObservedReads<'_> {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .expect("SQL authorization lock poisoned")
            .observed = None;
    }
}

pub(crate) struct WriteSql<'a>(&'a SqlAuthorization);

impl Drop for WriteSql<'_> {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .expect("SQL authorization lock poisoned")
            .writing = false;
    }
}

pub(crate) struct StepSql<'a>(&'a SqlAuthorization);

impl Drop for StepSql<'_> {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .expect("SQL authorization lock poisoned")
            .stepping = false;
    }
}

pub(crate) struct ReadingSql<'a>(&'a SqlAuthorization);

impl Drop for ReadingSql<'_> {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .expect("SQL authorization lock poisoned")
            .reads = None;
    }
}

impl Drop for InternalSql<'_> {
    fn drop(&mut self) {
        self.authorization
            .state
            .lock()
            .expect("SQL authorization lock poisoned")
            .internal = self.previous;
    }
}

impl AuthorizationState {
    /// Coven's own statements fire the app's triggers too: those on
    /// `coven_pins`, and those on the app's tables as coven applies writes.
    /// What such a trigger writes is judged as in an app statement: never
    /// coven's tables, and only the kind of table it may write (§9.4).
    fn app_trigger_refusal(&self, context: AuthContext<'_>) -> Option<DbError> {
        let trigger = context
            .accessor
            .filter(|name| !crate::internal_schema::reserved_name(name))?;
        let (AuthAction::Insert { table_name }
        | AuthAction::Update { table_name, .. }
        | AuthAction::Delete { table_name }) = context.action
        else {
            return None;
        };
        if crate::internal_schema::reserved_name(table_name) {
            return Some(DbError::InternalTable {
                table: table_name.into(),
            });
        }
        let shared = self.shared.contains(&trigger.to_ascii_lowercase());
        let synced = context.database_name == Some("main")
            && self.synced.contains(&table_name.to_ascii_lowercase());
        (shared != synced).then(|| DbError::TriggerTarget {
            trigger: trigger.into(),
            table: table_name.into(),
        })
    }

    fn refusal(&mut self, context: AuthContext<'_>) -> Option<DbError> {
        use AuthAction::*;
        // Coven's triggers watching explicit NULL assignments stay installed,
        // so they record in every app statement that assigns one: a write
        // reads what it recorded, and the next one clears the rest.
        if context.database_name == Some("temp")
            && matches!(context.action, Insert { table_name } if table_name == crate::sqlite::reference_assignments::TABLE)
            && context.accessor.is_some_and(|name| {
                name.starts_with(crate::sqlite::reference_assignments::TRIGGER_PREFIX)
            })
        {
            return None;
        }
        let reserved = |name: &str| {
            crate::internal_schema::reserved_name(name)
                .then(|| DbError::InternalTable { table: name.into() })
        };
        // Materializing merged rows writes coven's files table as it writes
        // every synced table; nothing else written there comes from app SQL.
        if self.applying
            && context.accessor.is_none()
            && matches!(
                context.action,
                Insert { table_name } | Update { table_name, .. } | Delete { table_name }
                    if table_name.eq_ignore_ascii_case(crate::internal_schema::FILES)
            )
        {
            return None;
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
                if context
                    .accessor
                    .is_some_and(crate::internal_schema::app_readable) =>
            {
                self.through_views.insert(table_name.to_ascii_lowercase());
                None
            }
            Read {
                table_name,
                column_name: "",
            } if context.accessor.is_none()
                && self
                    .through_views
                    .contains(&table_name.to_ascii_lowercase()) =>
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
            CreateIndex {
                index_name,
                table_name,
            }
            | CreateTempIndex {
                index_name,
                table_name,
            }
            | DropIndex {
                index_name,
                table_name,
            }
            | DropTempIndex {
                index_name,
                table_name,
            } => {
                if let Some(error) = reserved(index_name) {
                    return Some(error);
                }
                Some(table_name)
            }
            // The app's triggers may follow coven_pins and
            // coven_upload_queue, the tables coven changes row by row for
            // them (§9.4).
            CreateTrigger {
                trigger_name,
                table_name,
            }
            | DropTrigger {
                trigger_name,
                table_name,
            } if crate::internal_schema::followed(table_name) => {
                return reserved(trigger_name);
            }
            CreateTrigger {
                trigger_name,
                table_name,
            }
            | CreateTempTrigger {
                trigger_name,
                table_name,
            }
            | DropTrigger {
                trigger_name,
                table_name,
            }
            | DropTempTrigger {
                trigger_name,
                table_name,
            } => {
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
                    return Some(DbError::InternalTable {
                        table: index_name.into(),
                    });
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
        if matches!(
            context.action,
            Pragma {
                pragma_name: "data_version",
                pragma_value: None
            }
        ) {
            return None;
        }
        if self.reads.is_some()
            && !matches!(
                context.action,
                Read { .. } | Select | Function { .. } | Recursive
            )
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
            return Some(DbError::StatementForbidden {
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
        None
    }
}

#[cfg(test)]
#[path = "authorization_tests.rs"]
mod tests;
