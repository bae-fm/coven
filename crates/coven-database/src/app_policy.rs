//! What app SQL may not do to coven's own tables, triggers and functions,
//! judged after the authorizer's own rules allow it.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use coven_foundation::locks::lock_unpoisoned;

use sqlive::rusqlite::hooks::AuthAction;

use crate::coven_functions::Views;
use crate::sqlite::StoreSql;
use crate::{DbError, SyncedTable};
use sqlive::{Owner, Policy, PolicyAction, Refresh};

/// Coven's rules for the statements a connection prepares. A judgment
/// changes several fields of the state together, so `judge` holds the
/// state's lock for the whole judgment.
#[derive(Clone)]
pub(crate) struct CovenPolicy {
    state: Arc<Mutex<PolicyState>>,
}

struct PolicyState {
    applying: bool,
    mutations: Option<BTreeSet<String>>,
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

/// The tables whose hidden rowids may have changed since coven last read
/// them: the ones statements created or dropped, by schema and name in
/// lowercase, or every table after one that altered a table, since a
/// renamed table's new name never reaches the authorizer.
#[derive(Debug)]
pub(crate) enum ChangedTables {
    None,
    Named(BTreeSet<(String, String)>),
    All,
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

/// How a statement being prepared changes which app tables exist. App SQL
/// cannot name coven's internal staging tables, and SQLite's own tables,
/// such as the statistics ANALYZE makes, hold no app rows: creating them
/// changes no app rowid that needs protection.
fn changed_table(action: &PolicyAction<'_>) -> Option<TableChange> {
    let named = |schema: &str, table: &str| {
        TableChange::Named((schema.to_ascii_lowercase(), table.to_ascii_lowercase()))
    };
    let (table_name, change) = match action.action {
        AuthAction::CreateTable { table_name } | AuthAction::DropTable { table_name } => {
            (table_name, named(action.database.unwrap_or("main"), table_name))
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

impl CovenPolicy {
    /// The policy of a connection of the handle that holds the store's
    /// writer lock.
    pub(crate) fn new(tables: &[SyncedTable]) -> Self {
        Self::judging(tables, true)
    }

    /// The policy of a read-only handle's connection, which may not read the
    /// sync status or the retry effect (`internal_schema_sync_status.rs`,
    /// `internal_schema_retry_effect.rs`).
    pub(crate) fn read_only(tables: &[SyncedTable]) -> Self {
        Self::judging(tables, false)
    }

    fn judging(tables: &[SyncedTable], writer_state: bool) -> Self {
        Self {
            state: Arc::new(Mutex::new(PolicyState {
                applying: false,
                mutations: None,
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
        }
    }

    /// Whether materialized SQL is running, as `coven_applying()` says.
    pub(crate) fn applying_function(
        &self,
    ) -> impl Fn(&sqlive::rusqlite::functions::Context<'_>) -> sqlive::rusqlite::Result<bool>
           + Send
           + 'static {
        let state = Arc::clone(&self.state);
        move |_| Ok(lock_unpoisoned(&state).applying)
    }

    /// The tables whose hidden rowids may have changed since a caller last
    /// called `take_changed_tables`. The caller reads them again.
    fn take_changed_tables(&self) -> ChangedTables {
        std::mem::replace(&mut lock_unpoisoned(&self.state).changed_tables, ChangedTables::None)
    }

    /// Of the tables `read`, or every table when `None`, those whose hidden
    /// rowid is `unambiguous`: app SQL can't change it.
    fn set_hidden_rowids(
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
}

/// The parts of coven's own work its policy judges differently.
pub(crate) enum CovenScope {
    /// Materialized SQL runs: `coven_applying()` returns true, so the shared
    /// triggers skip their work, and coven's files table may be written.
    Applying,
    /// The statements prepared record each table of `main` they change, as
    /// live queries name it; leaving the scope puts the tables here.
    Mutations(Arc<Mutex<BTreeSet<String>>>),
}

impl Policy for CovenPolicy {
    type Refusal = DbError;
    type Failure = crate::store_failure::StoreFailure;

    fn judge(&self, action: &PolicyAction<'_>) -> Result<(), DbError> {
        lock_unpoisoned(&self.state).judge(action)
    }

    fn check_sql(&self, tokens: &[sqlive::sql::Token]) -> Result<(), DbError> {
        // SQLITE_ALTER_TABLE reports the old name, not the rename destination.
        // Check the destination token too; comments and string contents are not SQL.
        for part in tokens.windows(3) {
            if part[0].word("rename") && part[1].word("to") {
                let name = match &part[2] {
                    sqlive::sql::Token::Word(name)
                    | sqlive::sql::Token::Quoted(name)
                    | sqlive::sql::Token::String(name) => name,
                    _ => continue,
                };
                if crate::internal_schema::reserved_name(name) {
                    return Err(DbError::InternalTable { table: name.clone() });
                }
            }
        }
        Ok(())
    }

    fn begin_app_call(&self) {
        let mut state = lock_unpoisoned(&self.state);
        state.through_views.clear();
        state.dropping.clear();
    }

    fn host_object(&self, name: &str) -> bool {
        crate::internal_schema::reserved_name(name)
    }

    fn statement_key(&self) -> u64 {
        u64::from(lock_unpoisoned(&self.state).applying)
    }

    type Scope = CovenScope;

    fn enter(&self, scope: &CovenScope) {
        let mut state = lock_unpoisoned(&self.state);
        match scope {
            CovenScope::Applying => {
                assert!(!state.applying, "applying SQL scope cannot nest");
                state.applying = true;
            }
            CovenScope::Mutations(_) => {
                assert!(state.mutations.is_none(), "mutation capture cannot nest");
                state.mutations = Some(BTreeSet::new());
            }
        }
    }

    fn leave(&self, scope: &CovenScope) {
        let mut state = lock_unpoisoned(&self.state);
        match scope {
            CovenScope::Applying => state.applying = false,
            CovenScope::Mutations(into) => {
                *lock_unpoisoned(into) =
                    state.mutations.take().expect("enter starts the capture that leave ends");
            }
        }
    }

    /// Guard the hidden rowids of every table, or of the tables app SQL
    /// created or dropped since they were last read, or of every table after
    /// it altered one. Each table is read once as it appears, not again after
    /// every later statement: a schema of hundreds of tables would read them
    /// all after each one.
    fn refresh(&self, sql: &StoreSql, extent: Refresh) -> sqlive::rusqlite::Result<()> {
        let only = match extent {
            Refresh::Whole => None,
            Refresh::Changed => match self.take_changed_tables() {
                ChangedTables::None => return Ok(()),
                ChangedTables::Named(tables) => Some(tables),
                ChangedTables::All => None,
            },
        };
        let unambiguous = crate::sqlite::protect_rowids(sql, only.as_ref())?;
        self.set_hidden_rowids(only.as_ref(), unambiguous);
        Ok(())
    }
}

impl PolicyState {
    /// Judge an action the authorizer's own rules allowed, recording the
    /// tables it changes.
    fn judge(&mut self, action: &PolicyAction<'_>) -> Result<(), DbError> {
        self.record_mutation(action);
        // The policy skips coven's own statements but judges the app's
        // triggers and views that they run, as it does in an app statement
        // (§9.4).
        if action.owner != Owner::Host {
            if let Some(error) = self.refusal(action) {
                return Err(error);
            }
        }
        self.changed_tables.include(changed_table(action));
        Ok(())
    }

    fn record_mutation(&mut self, action: &PolicyAction<'_>) {
        if action.database != Some("main") {
            return;
        }
        let changed = match action.action {
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
    }

    fn refusal(&mut self, action: &PolicyAction<'_>) -> Option<DbError> {
        use AuthAction::*;
        let reserved = |name: &str| {
            crate::internal_schema::reserved_name(name)
                .then(|| DbError::InternalTable { table: name.into() })
        };
        if let DropTable { table_name } | DropTempTable { table_name } = action.action {
            self.dropping.insert(table_name.to_ascii_lowercase());
        }
        // Materializing merged rows writes coven's files table as it writes
        // every synced table; nothing else written there comes from app SQL.
        if self.applying
            && action.accessor.is_none()
            && matches!(
                action.action,
                Insert { table_name } | Update { table_name, .. } | Delete { table_name }
                    if crate::file_table::is_files(table_name)
            )
        {
            return None;
        }
        // The status and retry rows say what their writer last saw. A
        // read-only handle can't tell whether that is still so: a writer that
        // ended mid-pass leaves an unfinished row in `coven_moves` (§19.1).
        // Coven judges the app's triggers when it compiles them, and they
        // never run on a read-only handle, which changes no row.
        if let Read { table_name, .. } = action.action {
            if !self.writer_state
                && action.owner == Owner::App
                && crate::internal_schema::WRITER_STATE
                    .iter()
                    .any(|table| table_name.eq_ignore_ascii_case(table))
            {
                return Some(DbError::StatementForbidden {
                    operation: "reading the writer's sync state through a read-only handle",
                });
            }
        }
        let object = match action.action {
            // App SQL may read coven's views and its pins table, but not
            // change them. It may read their own columns, each column the
            // views' joins read, and a table the statement reached through a
            // view when it reads no column of that table.
            // SQLite names no view for that last read, so a statement that
            // names such a table beside the view could count its rows.
            Read { table_name, .. } if crate::internal_schema::app_readable(table_name) => None,
            Read { table_name, .. }
                if action.accessor.is_some_and(crate::internal_schema::app_readable) =>
            {
                self.through_views.insert(table_name.to_ascii_lowercase());
                None
            }
            Read { table_name, column_name: "" }
                if action.accessor.is_none()
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
            // coven_pin_requests, coven_moves, coven_upload_queue,
            // coven_pending, coven_sync_status and coven_retry_effect, the
            // tables coven changes row by row (§9.4).
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
        let forbidden = match action.action {
            Update { table_name, column_name }
                if column_name.eq_ignore_ascii_case("rowid")
                    && action.database.is_some_and(|schema| {
                        self.hidden_rowids.contains(&(
                            schema.to_ascii_lowercase(),
                            table_name.to_ascii_lowercase(),
                        ))
                    }) =>
            {
                Some("changing a hidden rowid")
            }
            CreateTempTrigger { trigger_name, .. }
                if self.shared.contains(&trigger_name.to_ascii_lowercase()) =>
            {
                Some("shadowing a shared trigger")
            }
            // Coven reruns a live query when a transfer starts or ends only
            // if the query reads the transfer columns of coven_uploads,
            // coven_sending or coven_downloading. If app SQL calls these
            // functions anywhere else, coven never reruns that query.
            Function { function_name }
                if crate::transfers_in_flight::FUNCTIONS
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(function_name))
                    && !action.accessor.is_some_and(crate::internal_schema::app_readable) =>
            {
                Some("reading what is being transferred outside coven_uploads, coven_sending and coven_downloading")
            }
            // The same holds for the network, which changes with no write.
            Function { function_name }
                if crate::network_facts::FUNCTIONS
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(function_name))
                    && !action.accessor.is_some_and(crate::internal_schema::app_readable) =>
            {
                Some("reading the network outside coven_network and coven_uploads")
            }
            _ => None,
        };
        if let Some(operation) = forbidden {
            return Some(DbError::StatementForbidden { operation });
        }
        if let (
            Some(trigger),
            Insert { table_name } | Update { table_name, .. } | Delete { table_name },
        ) = (action.accessor, action.action)
        {
            let shared = self.shared.contains(&trigger.to_ascii_lowercase());
            let synced = action.database == Some("main")
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
        if let (true, Insert { table_name } | Update { table_name, .. } | Delete { table_name }) =
            (action.scope.bulk, action.action)
        {
            if action.database == Some("main")
                && self.synced.contains(&table_name.to_ascii_lowercase())
            {
                return Some(DbError::SyncedTableInBulkWrite { table: table_name.into() });
            }
        }
        None
    }
}

/// What every connection to a store's database registers before any SQL
/// runs on it: coven's functions, with that connection's `policy`; the
/// app's `functions`; and coven's `views`, when the database has them.
pub(crate) fn setup<'a>(
    functions: &'a crate::SqlFunctions,
    views: Option<&'a Views<'a>>,
) -> impl Fn(
    &sqlive::Setup<'_, crate::app_policy::CovenPolicy>,
    &CovenPolicy,
) -> sqlive::rusqlite::Result<()>
       + 'a {
    move |setup, policy| {
        crate::coven_functions::register(setup, policy)?;
        functions.register(setup)?;
        match views {
            Some(views) => views(setup),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
#[path = "app_policy_tests.rs"]
pub(crate) mod tests;
