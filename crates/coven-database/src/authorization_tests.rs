//! These tests try the authorizer's own rules on plain tables, with a policy
//! that allows what the rules allow and records each action the authorizer
//! asks it about.

use std::sync::{Arc, Mutex};

use rusqlite::hooks::AuthAction;
use rusqlite::Connection;

use super::{Owner, Policy, PolicyAction, Refresh, SqlAuthorization};
use crate::DbError;
use sqlive::WholeViews;

/// A policy that allows every action the authorizer asks it about and
/// records the owner of each table an insert reached. `Recording` treats an
/// object whose name starts with `host_` as the host's own.
#[derive(Default)]
struct Recording {
    inserts: Mutex<Vec<(String, Owner)>>,
    refused: Mutex<Vec<String>>,
}

impl Policy for Recording {
    type Refusal = DbError;
    type Failure = crate::store_failure::StoreFailure;

    fn judge(&self, action: &PolicyAction<'_>) -> Result<(), DbError> {
        if let AuthAction::Insert { table_name } = action.action {
            self.inserts.lock().unwrap().push((table_name.into(), action.owner));
            if self.refused.lock().unwrap().iter().any(|refused| refused == table_name) {
                return Err(DbError::InternalTable { table: table_name.into() });
            }
        }
        Ok(())
    }

    fn check_sql(&self, _tokens: &[sqlive::sql::Token]) -> Result<(), DbError> {
        Ok(())
    }

    fn begin_app_call(&self) {}

    fn host_object(&self, name: &str) -> bool {
        name.starts_with("host_")
    }

    fn statement_key(&self) -> u64 {
        0
    }

    type Scope = ();

    fn enter(&self, _scope: &()) {}

    fn leave(&self, _scope: &()) {}

    fn refresh(
        &self,
        _sql: &crate::sqlite::DatabaseConnection<Self>,
        _extent: Refresh,
    ) -> rusqlite::Result<()> {
        Ok(())
    }
}

/// An in-memory connection authorized by the rules and `policy`, holding the
/// table `local(x)`.
fn plain(policy: &Arc<Recording>) -> (Connection, SqlAuthorization<Recording>) {
    let mut connection = Connection::open_in_memory().unwrap();
    let authorization =
        SqlAuthorization::new(Arc::new(WholeViews::new([])), Arc::<Recording>::clone(policy));
    connection.authorizer(Some(authorization.callback())).unwrap();
    {
        let _host = authorization.internal();
        connection.execute_batch("CREATE TABLE local(x)").unwrap();
    }
    (connection, authorization)
}

/// The app's one statement `sql`, as an app call runs it.
fn app(
    connection: &Connection,
    authorization: &SqlAuthorization<Recording>,
    sql: &str,
) -> Result<(), DbError> {
    authorization.check_sql(sql)?;
    authorization.begin_app_call();
    authorization.app_result(connection.prepare(sql)).map(drop).map_err(DbError::from)
}

fn forbidden(result: Result<(), DbError>) -> Option<&'static str> {
    match result {
        Err(DbError::StatementForbidden { operation }) => Some(operation),
        Err(error) => panic!("expected a refusal, received {error:?}"),
        Ok(()) => None,
    }
}

#[test]
fn app_sql_cannot_escape_its_transaction_or_change_connection_settings() {
    for (sql, operation) in [
        ("COMMIT", "transaction control"),
        ("ROLLBACK", "transaction control"),
        ("SAVEPOINT app_savepoint", "transaction control"),
        ("PRAGMA writable_schema = ON", "PRAGMA"),
        ("PRAGMA foreign_keys = OFF", "PRAGMA"),
        ("PRAGMA user_version = 8", "PRAGMA"),
        ("ATTACH ':memory:' AS other", "attaching or detaching databases"),
        ("SELECT * FROM pragma_table_info('local')", "PRAGMA"),
        ("SELECT * FROM pragma_quick_check('local')", "PRAGMA"),
        ("SELECT load_extension('nowhere')", "loading an extension"),
    ] {
        let (connection, authorization) = plain(&Arc::default());
        assert_eq!(forbidden(app(&connection, &authorization, sql)), Some(operation), "{sql}");
    }
}

/// SQLite checks a column added with a CHECK by reading `pragma_quick_check`.
/// The authorizer lets that statement read it, and refuses the next
/// statement that does.
#[test]
fn adding_a_checked_column_may_run_the_check_but_the_next_statement_may_not() {
    let (connection, authorization) = plain(&Arc::default());
    let add = "ALTER TABLE local ADD COLUMN y INTEGER CHECK(y>0)";
    assert_eq!(forbidden(app(&connection, &authorization, add)), None);
    assert_eq!(
        forbidden(app(&connection, &authorization, "SELECT * FROM pragma_quick_check('local')")),
        Some("PRAGMA")
    );
}

#[test]
fn a_read_may_only_read() {
    let (connection, authorization) = plain(&Arc::default());
    let _reading = authorization.reading();
    assert_eq!(forbidden(app(&connection, &authorization, "SELECT x FROM local")), None);
    for sql in ["INSERT INTO local VALUES (1)", "DELETE FROM local", "CREATE TABLE more(x)"] {
        assert_eq!(
            forbidden(app(&connection, &authorization, sql)),
            Some("writing through a read context"),
            "{sql}"
        );
    }
}

#[test]
fn a_write_changes_no_schema_and_gathers_no_statistics() {
    let (connection, authorization) = plain(&Arc::default());
    assert_eq!(forbidden(app(&connection, &authorization, "ANALYZE")), None);
    let _writing = authorization.write();
    assert_eq!(forbidden(app(&connection, &authorization, "INSERT INTO local VALUES (1)")), None);
    for sql in [
        "CREATE TABLE more(x)",
        "CREATE INDEX more ON local(x)",
        "CREATE VIEW more AS SELECT x FROM local",
        "CREATE TRIGGER more AFTER INSERT ON local BEGIN SELECT 1; END",
        "DROP TABLE local",
        "ALTER TABLE local ADD COLUMN y",
    ] {
        assert_eq!(
            forbidden(app(&connection, &authorization, sql)),
            Some("schema changes in a write"),
            "{sql}"
        );
    }
    assert_eq!(forbidden(app(&connection, &authorization, "ANALYZE")), Some("ANALYZE in a write"));
}

/// The session extension reads a table's columns by `table_xinfo` as a
/// statement steps, which no app SQL may name.
#[test]
fn only_a_step_may_read_a_tables_columns_by_table_xinfo() {
    let (connection, authorization) = plain(&Arc::default());
    let xinfo = "PRAGMA main.table_xinfo(local)";
    assert!(authorization.app_result(connection.prepare(xinfo)).is_err());
    let _step = authorization.step();
    authorization.begin_app_call();
    assert!(authorization.app_result(connection.prepare(xinfo)).is_ok());
}

/// FTS5 prepares this read itself.
#[test]
fn the_data_version_reads_when_sqlite_prepares_it_but_not_as_app_text() {
    let (connection, authorization) = plain(&Arc::default());
    assert!(connection.prepare("PRAGMA data_version").is_ok());
    assert_eq!(forbidden(app(&connection, &authorization, "PRAGMA data_version")), Some("PRAGMA"));
}

#[test]
fn the_policy_is_asked_after_the_rules_allow_and_can_refuse() {
    let policy = Arc::new(Recording::default());
    policy.refused.lock().unwrap().push("local".into());
    let (connection, authorization) = plain(&policy);
    let insert = "INSERT INTO local VALUES (1)";
    assert!(matches!(
        app(&connection, &authorization, insert),
        Err(DbError::InternalTable { table }) if table == "local"
    ));
    // The authorizer never calls the policy's `judge` for an action the
    // rules refuse.
    policy.inserts.lock().unwrap().clear();
    let _reading = authorization.reading();
    assert_eq!(
        forbidden(app(&connection, &authorization, insert)),
        Some("writing through a read context")
    );
    assert!(policy.inserts.lock().unwrap().is_empty());
}

#[test]
fn the_policy_learns_whose_statement_each_action_belongs_to() {
    let policy = Arc::new(Recording::default());
    let (connection, authorization) = plain(&policy);
    {
        let _host = authorization.internal();
        connection
            .execute_batch(
                "CREATE TABLE host_rows(x);
                 CREATE TABLE host_log(x);
                 CREATE TABLE app_log(x);
                 CREATE TRIGGER host_trigger AFTER INSERT ON host_rows BEGIN INSERT INTO host_log VALUES (1); END;
                 CREATE TRIGGER app_trigger AFTER INSERT ON host_rows BEGIN INSERT INTO app_log VALUES (1); END;",
            )
            .unwrap();
        policy.inserts.lock().unwrap().clear();
        connection.prepare("INSERT INTO host_rows VALUES (1)").unwrap();
    }
    authorization.begin_app_call();
    connection.prepare("INSERT INTO local VALUES (1)").unwrap();
    let mut inserts = policy.inserts.lock().unwrap().clone();
    inserts.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        inserts,
        [
            ("app_log".to_owned(), Owner::AppTriggerOfHost),
            ("host_log".to_owned(), Owner::Host),
            ("host_rows".to_owned(), Owner::Host),
            ("local".to_owned(), Owner::App),
        ]
    );
}
