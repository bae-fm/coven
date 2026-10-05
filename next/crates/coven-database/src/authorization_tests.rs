use crate::{
    tests::{database_error, TestStore},
    *,
};

#[tokio::test]
async fn app_statements_cannot_read_or_mutate_internal_objects() {
    for attack in [
        "SELECT * FROM coven_rows",
        "SELECT count(*) FROM COVEN_ROWS",
        "INSERT INTO coven_operations(kind,last_step,data,started_by) VALUES ('x',0,x'','x')",
        "UPDATE coven_rows SET audience = 'store'",
        "DELETE FROM coven_uploads",
        "DROP TABLE coven_columns",
        "ALTER TABLE coven_rows ADD COLUMN bad INTEGER",
        "CREATE INDEX bad ON coven_rows(audience)",
        "CREATE TRIGGER bad AFTER INSERT ON coven_rows BEGIN SELECT 1; END",
        "CREATE TABLE coven_fake (x)",
        "CREATE TABLE local(x); ALTER TABLE local RENAME TO coven_fake",
        "CREATE TEMP TABLE coven_cells(x)",
        "CREATE VIEW hidden AS SELECT * FROM coven_rows; SELECT * FROM hidden",
    ] {
        let store = TestStore::new();
        let error = store
            .builder(vec![], vec![Migration::sql(1, "attack", attack)])
            .open()
            .await
            .err()
            .unwrap();
        assert!(
            matches!(database_error(error), DbError::InternalTable { .. }),
            "{attack}"
        );
    }
}

#[tokio::test]
async fn app_sql_cannot_escape_its_transaction_or_change_connection_settings() {
    for attack in [
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT app_savepoint",
        "PRAGMA writable_schema = ON",
        "PRAGMA foreign_keys = OFF",
        "PRAGMA user_version = 8",
        "ATTACH ':memory:' AS other",
        "SELECT * FROM pragma_table_info('coven_rows')",
        "CREATE TABLE local(x); ALTER TABLE local ADD COLUMN y INTEGER CHECK(y>0); SELECT * FROM pragma_quick_check('coven_rows')",
        "CREATE TABLE local(x); ALTER TABLE local ADD COLUMN y INTEGER CHECK(y>0); PRAGMA quick_check(coven_rows)",
    ] {
        let store = TestStore::new();
        let error = store
            .builder(vec![], vec![Migration::sql(1, "attack", attack)])
            .open()
            .await
            .err()
            .unwrap();
        let error = database_error(error);
        assert!(
            matches!(error, DbError::StatementForbidden { .. }),
            "{attack}: {error:?}"
        );
    }
}

#[tokio::test]
async fn trigger_targets_are_rejected_while_preparing_before_any_row_changes() {
    for shared in [false, true] {
        let store = TestStore::new();
        let mut table = SyncedTable::new("notes", RowIdentity::SharedKey);
        if shared {
            table = table.shared_trigger("effect");
        }
        let db = store.builder(vec![table], vec![Migration::run(1, "triggers", move |sql| {
            sql.execute_batch("CREATE TABLE notes(id TEXT PRIMARY KEY, n INTEGER); CREATE TABLE local(n INTEGER); INSERT INTO local VALUES (0)")?;
            sql.execute_batch(if shared {
                "CREATE TRIGGER effect AFTER INSERT ON notes WHEN NOT coven_applying() BEGIN UPDATE local SET n = n+1; END"
            } else {
                "CREATE TRIGGER effect AFTER INSERT ON notes BEGIN UPDATE notes SET n = n+1; END"
            })?;
            let error = DbError::from(sql.execute("INSERT INTO notes VALUES ('a',0)", []).unwrap_err());
            assert!(matches!(error, DbError::TriggerTarget { trigger, table } if trigger == "effect" && table == if shared { "local" } else { "notes" }));
            assert_eq!(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?, 0);
            assert_eq!(sql.query_row("SELECT n FROM local", [], |r| r.get::<_, i64>(0))?, 0);
            Ok(())
        })]).open().await.unwrap();
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn permitted_local_and_shared_triggers_run_with_applying_false() {
    let store = TestStore::new();
    let db = store.builder(vec![SyncedTable::new("notes", RowIdentity::SharedKey).shared_trigger("shared")], vec![Migration::run(1, "triggers", |sql| {
        sql.execute_batch("CREATE TABLE notes(id TEXT PRIMARY KEY, n INTEGER); CREATE TABLE local(n INTEGER); INSERT INTO local VALUES (0); CREATE TRIGGER local_effect AFTER INSERT ON notes BEGIN UPDATE local SET n = n+1; END; CREATE TRIGGER shared AFTER INSERT ON notes WHEN NOT coven_applying() BEGIN UPDATE notes SET n = 7 WHERE id = new.id; END; INSERT INTO notes VALUES ('a',0)")?;
        assert_eq!(sql.query_row("SELECT n FROM notes", [], |r| r.get::<_, i64>(0))?, 7);
        assert_eq!(sql.query_row("SELECT n FROM local", [], |r| r.get::<_, i64>(0))?, 1);
        assert!(!sql.query_row("SELECT coven_applying()", [], |r| r.get::<_, bool>(0))?);
        Ok(())
    })]).open().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn app_reindex_cannot_write_an_internal_index() {
    for name in ["coven_write_position", "sqlite_autoindex_coven_columns_1"] {
        let store = TestStore::new();
        let error = store
            .builder(
                vec![],
                vec![Migration::run(1, "reindex", move |sql| {
                    sql.execute_batch(&format!("REINDEX {name}"))?;
                    Ok(())
                })],
            )
            .open()
            .await
            .err()
            .unwrap();
        assert!(matches!(database_error(error), DbError::InternalTable { table } if table == name));
    }
}

#[tokio::test]
async fn a_temporary_table_with_a_synced_name_is_still_local() {
    let store = TestStore::new();
    let db = store.builder(vec![SyncedTable::new("notes", RowIdentity::SharedKey).shared_trigger("shared")], vec![Migration::run(1,"shadow", |sql| {
        sql.execute_batch("CREATE TABLE notes(id TEXT PRIMARY KEY); CREATE TRIGGER shared AFTER INSERT ON notes WHEN NOT coven_applying() BEGIN SELECT 1; END; CREATE TEMP TABLE notes(id TEXT); CREATE TEMP TABLE source(id TEXT); CREATE TEMP TRIGGER local AFTER INSERT ON source BEGIN INSERT INTO notes VALUES(new.id); END; INSERT INTO source VALUES('local')")?;
        assert_eq!(sql.query_row("SELECT count(*) FROM temp.notes", [], |r| r.get::<_, i64>(0))?,1);
        Ok(())
    })]).open().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_temporary_trigger_cannot_impersonate_a_declared_shared_trigger() {
    let store = TestStore::new();
    let error = store.builder(vec![SyncedTable::new("notes", RowIdentity::SharedKey).shared_trigger("shared")], vec![Migration::sql(1,"shadow", "CREATE TABLE notes(id TEXT PRIMARY KEY); CREATE TRIGGER shared AFTER INSERT ON notes WHEN NOT coven_applying() BEGIN SELECT 1; END; CREATE TEMP TRIGGER shared AFTER INSERT ON main.notes BEGIN UPDATE notes SET id = 'unguarded'; END; INSERT INTO notes VALUES('x')")]).open().await.err().unwrap();
    assert!(matches!(
        database_error(error),
        DbError::StatementForbidden { .. }
    ));
}

impl super::SqlAuthorization {
    pub(crate) fn integrity_checks(&self) -> usize {
        self.state.lock().unwrap().integrity_checks
    }
}
