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
            }));
    }
}

pub(crate) struct SqlAuthorization {
    state: Arc<Mutex<AuthorizationState>>,
}

struct AuthorizationState {
    internal: bool,
    writing: bool,
    stepping: bool,
    applying: bool,
    columns: Option<Vec<String>>,
    mutations: Option<BTreeSet<String>>,
    reads: Option<ReadCapture>,
    #[cfg(test)]
    integrity_checks: usize,
    altering: bool,
    denied: Option<DbError>,
    synced: BTreeSet<String>,
    shared: BTreeSet<String>,
    hidden_rowids: BTreeSet<(String, String)>,
    tables_changed: bool,
}

impl SqlAuthorization {
    pub(crate) fn new(tables: &[SyncedTable]) -> Self {
        Self {
            state: Arc::new(Mutex::new(AuthorizationState {
                internal: false,
                writing: false,
                stepping: false,
                applying: false,
                columns: None,
                mutations: None,
                reads: None,
                #[cfg(test)]
                integrity_checks: 0,
                altering: false,
                denied: None,
                synced: tables.iter().map(|t| t.name.to_ascii_lowercase()).collect(),
                shared: tables
                    .iter()
                    .flat_map(|t| t.shared_triggers.iter().map(|s| s.to_ascii_lowercase()))
                    .collect(),
                hidden_rowids: BTreeSet::new(),
                tables_changed: false,
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
                }
                if let Some(reads) = &mut state.columns {
                    if !column_name.is_empty() && !reads.iter().any(|c| c == column_name) {
                        reads.push(column_name.into());
                    }
                }
            }
            #[cfg(test)]
            if matches!(
                context.action,
                AuthAction::Pragma {
                    pragma_name: "integrity_check",
                    ..
                }
            ) {
                state.integrity_checks += 1;
            }
            let changes_tables = matches!(
                context.action,
                AuthAction::CreateTable { .. }
                    | AuthAction::CreateTempTable { .. }
                    | AuthAction::DropTable { .. }
                    | AuthAction::DropTempTable { .. }
                    | AuthAction::CreateVtable { .. }
                    | AuthAction::DropVtable { .. }
                    | AuthAction::AlterTable { .. }
            );
            if state.internal {
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

    pub(crate) fn observe_reads(&self) -> ReadColumns<'_> {
        let mut state = self.state.lock().expect("SQL authorization lock poisoned");
        assert!(state.columns.is_none(), "column observation cannot nest");
        state.columns = Some(Vec::new());
        ReadColumns(self)
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
        if crate::sql::tokens(sql)
            .first()
            .is_some_and(|token| token.word("pragma"))
        {
            return Err(rusqlite::Error::UserFunctionError(Box::new(
                DbError::StatementForbidden {
                    operation: "PRAGMA",
                },
            )));
        }

        // SQLITE_ALTER_TABLE reports the old name, not the rename destination.
        // Check the destination token too; comments and string contents are not SQL.
        for part in crate::sql::tokens(sql).windows(3) {
            if part[0].word("rename") && part[1].word("to") {
                let name = match &part[2] {
                    crate::sql::Token::Word(name)
                    | crate::sql::Token::Quoted(name)
                    | crate::sql::Token::String(name) => name,
                    _ => continue,
                };
                if name.to_ascii_lowercase().starts_with("coven_") {
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

pub(crate) struct ReadColumns<'a>(&'a SqlAuthorization);

impl ReadColumns<'_> {
    pub(crate) fn columns(&self) -> Vec<String> {
        self.0
            .state
            .lock()
            .expect("SQL authorization lock poisoned")
            .columns
            .as_ref()
            .expect("observing reads")
            .clone()
    }
}

impl Drop for ReadColumns<'_> {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .expect("SQL authorization lock poisoned")
            .columns = None;
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
    fn refusal(&mut self, context: AuthContext<'_>) -> Option<DbError> {
        use AuthAction::*;
        let reserved = |name: &str| {
            name.to_ascii_lowercase()
                .starts_with("coven_")
                .then(|| DbError::InternalTable { table: name.into() })
        };
        let object = match context.action {
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
                // Those indexes belong to the reserved table too.
                if index_name
                    .to_ascii_lowercase()
                    .starts_with("sqlite_autoindex_coven_")
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
        if self.reads.is_some()
            && matches!(
                context.action,
                Pragma {
                    pragma_name: "data_version",
                    pragma_value: None
                }
            )
        {
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
