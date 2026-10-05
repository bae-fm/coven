//! The authorizer's app boundary and prepare-time trigger target rules.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

use crate::{DbError, SyncedTable};

pub(crate) struct SqlAuthorization {
    state: Arc<Mutex<AuthorizationState>>,
}

struct AuthorizationState {
    internal: bool,
    #[cfg(test)]
    integrity_checks: usize,
    altering: bool,
    denied: Option<DbError>,
    synced: BTreeSet<String>,
    shared: BTreeSet<String>,
}

impl SqlAuthorization {
    pub(crate) fn new(tables: &[SyncedTable]) -> Self {
        Self {
            state: Arc::new(Mutex::new(AuthorizationState {
                internal: false,
                #[cfg(test)]
                integrity_checks: 0,
                altering: false,
                denied: None,
                synced: tables.iter().map(|t| t.name.to_ascii_lowercase()).collect(),
                shared: tables
                    .iter()
                    .flat_map(|t| t.shared_triggers.iter().map(|s| s.to_ascii_lowercase()))
                    .collect(),
            })),
        }
    }

    pub(crate) fn callback(&self) -> impl FnMut(AuthContext<'_>) -> Authorization + Send + 'static {
        let state = Arc::clone(&self.state);
        move |context| {
            let mut state = state.lock().expect("SQL authorization lock poisoned");
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
            if state.internal {
                return Authorization::Allow;
            }
            match state.refusal(context) {
                Some(error) => {
                    state.denied = Some(error);
                    Authorization::Deny
                }
                None => Authorization::Allow,
            }
        }
    }

    pub(crate) fn internal(&self) -> InternalSql<'_> {
        let mut state = self.state.lock().expect("SQL authorization lock poisoned");
        assert!(!state.internal, "internal SQL scope cannot nest");
        state.internal = true;
        InternalSql(self)
    }

    pub(crate) fn begin_app_call(&self) {
        let mut state = self.state.lock().expect("SQL authorization lock poisoned");
        state.denied = None;
        state.altering = false;
    }

    pub(crate) fn check_sql(&self, sql: &str) -> rusqlite::Result<()> {
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

pub(crate) struct InternalSql<'a>(&'a SqlAuthorization);

impl Drop for InternalSql<'_> {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .expect("SQL authorization lock poisoned")
            .internal = false;
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
        if matches!(context.action, AlterTable { .. }) {
            self.altering = true;
        }
        // SQLite compiles this read-only check as part of ALTER ADD COLUMN.
        // The permission lasts for this statement, never the following one.
        let internal_check = self.altering;
        let forbidden = match context.action {
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
