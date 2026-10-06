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
            sql.execute_batch("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, n INTEGER); CREATE TABLE local(n INTEGER); INSERT INTO local VALUES (0)")?;
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
        sql.execute_batch("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, n INTEGER); CREATE TABLE local(n INTEGER); INSERT INTO local VALUES (0); CREATE TRIGGER local_effect AFTER INSERT ON notes BEGIN UPDATE local SET n = n+1; END; CREATE TRIGGER shared AFTER INSERT ON notes WHEN NOT coven_applying() BEGIN UPDATE notes SET n = 7 WHERE id = new.id; END; INSERT INTO notes VALUES ('a',0)")?;
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
        sql.execute_batch("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE TRIGGER shared AFTER INSERT ON notes WHEN NOT coven_applying() BEGIN SELECT 1; END; CREATE TEMP TABLE notes(id TEXT); CREATE TEMP TABLE source(id TEXT); CREATE TEMP TRIGGER local AFTER INSERT ON source BEGIN INSERT INTO notes VALUES(new.id); END; INSERT INTO source VALUES('local')")?;
        assert_eq!(sql.query_row("SELECT count(*) FROM temp.notes", [], |r| r.get::<_, i64>(0))?,1);
        Ok(())
    })]).open().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_temporary_trigger_cannot_impersonate_a_declared_shared_trigger() {
    let store = TestStore::new();
    let error = store.builder(vec![SyncedTable::new("notes", RowIdentity::SharedKey).shared_trigger("shared")], vec![Migration::sql(1,"shadow", "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE TRIGGER shared AFTER INSERT ON notes WHEN NOT coven_applying() BEGIN SELECT 1; END; CREATE TEMP TRIGGER shared AFTER INSERT ON main.notes BEGIN UPDATE notes SET id = 'unguarded'; END; INSERT INTO notes VALUES('x')")]).open().await.err().unwrap();
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

#[tokio::test]
async fn hidden_rowid_assignments_are_refused_in_writes_and_migrations() {
    for key in [
        "id TEXT NOT NULL PRIMARY KEY",
        "id TEXT PRIMARY KEY",
        "id TEXT",
        "id INTEGER PRIMARY KEY DESC",
    ] {
        for column in [
            "rowid",
            "_rowid_",
            "oid",
            "\"RoWiD\"",
            "[OID]",
            "`_rowid_`",
            "'rowid'",
        ] {
            let operation = format!("UPDATE items SET {column}=2");
            let store = TestStore::new();
            let schema=format!("CREATE TABLE items({key},value TEXT); INSERT INTO items(id,value) VALUES(1,'original')");
            let migration_schema = schema.clone();
            let migration_operation = operation.clone();
            let error = store
                .builder(
                    vec![],
                    vec![Migration::run(1, "forbidden rowid", move |sql| {
                        sql.execute_batch(&migration_schema)?;
                        sql.execute_batch(&migration_operation)?;
                        Ok(())
                    })],
                )
                .open()
                .await
                .err()
                .unwrap();
            assert!(
                matches!(
                    database_error(error),
                    DbError::StatementForbidden {
                        operation: "changing a hidden rowid"
                    }
                ),
                "migration: {key}: {operation}"
            );
            let db = store
                .builder(
                    vec![],
                    vec![Migration::run(1, "schema", move |sql| {
                        sql.execute_batch(&schema)?;
                        Ok(())
                    })],
                )
                .open()
                .await
                .unwrap();
            let statement = operation.clone();
            let error = db
                .write(Default::default(), move |sql| {
                    sql.execute(&statement, [])?;
                    Ok(())
                })
                .await
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    DbError::StatementForbidden {
                        operation: "changing a hidden rowid"
                    }
                ),
                "write: {key}: {operation}: {error:?}"
            );
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn integer_primary_key_aliases_and_real_rowid_named_columns_remain_writable() {
    for (shape, suffix) in [
        ("id INTEGER PRIMARY KEY,value TEXT", ""),
        ("id INTEGER,value TEXT,PRIMARY KEY(id DESC)", ""),
        (
            "id TEXT NOT NULL PRIMARY KEY,rowid INTEGER,_rowid_ INTEGER,oid INTEGER,value TEXT",
            "",
        ),
        (
            "id TEXT PRIMARY KEY,rowid INTEGER,_rowid_ INTEGER,oid INTEGER,value TEXT",
            " WITHOUT ROWID",
        ),
    ] {
        for column in ["rowid", "_rowid_", "oid"] {
            let store = TestStore::new();
            let schema=format!("CREATE TABLE items({shape}){suffix}; INSERT INTO items(id,value) VALUES(1,'one'); UPDATE items SET {column}=2 WHERE id=1; INSERT INTO items({column},id,value) VALUES(3,3,'three')");
            let db = store
                .builder(
                    vec![],
                    vec![Migration::run(1, "rowid aliases", move |sql| {
                        sql.execute_batch(&schema)?;
                        Ok(())
                    })],
                )
                .open()
                .await
                .unwrap();
            db.write(Default::default(), move |sql| {
                sql.execute(&format!("UPDATE items SET {column}=4 WHERE id=3"), [])?;
                sql.execute(
                    &format!("INSERT INTO items({column},id,value) VALUES(5,5,'five')"),
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn rowid_policy_covers_tuple_assignments_ctes_upserts_and_returning() {
    let store = TestStore::new();
    let db=store.schema(vec![],"CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY,value TEXT); INSERT INTO items VALUES('a','original')").await.unwrap();
    for statement in [
        "UPDATE items AS i SET (value,_rowid_)=('changed',2) RETURNING value",
        "WITH n(x) AS (SELECT 2) UPDATE main.items SET oid=(SELECT x FROM n) RETURNING value",
        "INSERT INTO items(id,value) VALUES('a','changed') ON CONFLICT(id) DO UPDATE SET rowid=2 RETURNING value",
    ] {
        for query_row in [true,false] {
            let error=db.write(Default::default(),move |sql| {
                if query_row { sql.query_row(statement,[],|_|Ok(()))?; }
                else { sql.query(statement,[],|_|Ok(()))?; }
                Ok(())
            }).await.unwrap_err();
            assert!(matches!(error,DbError::StatementForbidden { operation: "changing a hidden rowid" }),"{statement}: {error:?}");
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn rowid_updates_in_triggers_are_refused_before_any_changes() {
    for trigger in ["TRIGGER", "TEMP TRIGGER"] {
        for operation in [
            "UPDATE target SET rowid=2",
            "INSERT INTO target(id) VALUES('a') ON CONFLICT(id) DO UPDATE SET oid=2",
        ] {
            let store = TestStore::new();
            let db = store.builder(vec![], vec![Migration::run(1,"trigger targets", move |sql| {
                sql.execute_batch(&format!("CREATE TABLE source(id INTEGER PRIMARY KEY); CREATE {trigger} effect AFTER INSERT ON source BEGIN {operation}; END; CREATE TABLE target(id TEXT NOT NULL PRIMARY KEY); INSERT INTO target VALUES('a')"))?;
                let error = DbError::from(sql.execute("INSERT INTO source VALUES(1)", []).unwrap_err());
                assert!(matches!(error, DbError::StatementForbidden { operation: "changing a hidden rowid" }),"{trigger}: {operation}: {error:?}");
                assert_eq!(sql.query_row("SELECT count(*) FROM source",[],|r|r.get::<_,i64>(0))?,0);
                assert_eq!(sql.query_row("SELECT rowid FROM target",[],|r|r.get::<_,i64>(0))?,1);
                Ok(())
            })]).open().await.unwrap();
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn rowid_policy_distinguishes_temporary_tables_and_declared_alias_columns() {
    let store = TestStore::new();
    let db = store.builder(vec![],vec![Migration::run(1,"row addresses",|sql| {
        sql.execute_batch("CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY, rowid INTEGER); INSERT INTO items VALUES('a',1); CREATE TEMP TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items(rowid) VALUES(7); UPDATE items SET _rowid_=8; UPDATE main.items SET rowid=2;")?;
        assert_eq!(sql.query_row("SELECT rowid FROM temp.items",[],|r|r.get::<_,i64>(0))?,8);
        assert_eq!(sql.query_row("SELECT rowid FROM main.items",[],|r|r.get::<_,i64>(0))?,2);
        sql.execute("UPDATE main.items SET oid=9",[])?;
        sql.execute("INSERT INTO main.items(_rowid_,id) VALUES(10,'b')",[])?;
        assert_eq!(sql.query_row("SELECT oid FROM main.items WHERE id='a'",[],|r|r.get::<_,i64>(0))?,9);
        Ok(())
    })]).open().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn rowid_policy_distinguishes_same_named_main_and_temp_triggers() {
    for event in [
        "INSERT ON other",
        "DELETE ON source",
        "UPDATE OF oid ON source",
    ] {
        let store = TestStore::new();
        let db = store.builder(vec![],vec![Migration::run(1,"distinct triggers",move |sql| {
            sql.execute_batch(&format!("CREATE TABLE source(id INTEGER PRIMARY KEY); CREATE TABLE other(id INTEGER PRIMARY KEY); CREATE TABLE allowed(id INTEGER PRIMARY KEY); CREATE TEMP TRIGGER same AFTER {event} BEGIN UPDATE target SET rowid=2; END; CREATE TRIGGER same AFTER UPDATE OF _rowid_ ON source BEGIN INSERT INTO allowed VALUES(3); END; CREATE TABLE target(id TEXT NOT NULL PRIMARY KEY); INSERT INTO source VALUES(1); UPDATE source SET _rowid_=2;"))?;
            assert_eq!(sql.query_row("SELECT id FROM source",[],|r|r.get::<_,i64>(0))?,2);
            assert_eq!(sql.query_row("SELECT id FROM allowed",[],|r|r.get::<_,i64>(0))?,3);
            assert_eq!(sql.query_row("SELECT count(*) FROM target",[],|r|r.get::<_,i64>(0))?,0);
            Ok(())
        })]).open().await.unwrap();
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn rowid_policy_follows_nested_alias_updates_and_foreign_key_actions() {
    for (schema, statement) in [
        (
            "CREATE TABLE source(id INTEGER PRIMARY KEY); CREATE TABLE middle(id INTEGER PRIMARY KEY); INSERT INTO middle VALUES(1); CREATE TRIGGER a_child AFTER UPDATE OF oid ON middle BEGIN UPDATE target SET rowid=2; END; CREATE TRIGGER z_parent AFTER UPDATE OF _rowid_ ON source BEGIN UPDATE middle SET oid=3; END;",
            "UPDATE source SET _rowid_=2",
        ),
        (
            "CREATE TABLE source(id INTEGER PRIMARY KEY); CREATE TABLE middle(id TEXT PRIMARY KEY, parent INTEGER REFERENCES source(id) ON UPDATE CASCADE); CREATE TRIGGER child AFTER UPDATE OF parent ON middle BEGIN UPDATE target SET rowid=2; END;",
            "UPDATE source SET id=2",
        ),
    ] {
        let store = TestStore::new();
        let db = store.builder(vec![],vec![Migration::run(1,"indirect row addresses",move |sql| {
            sql.execute_batch(schema)?;
            sql.execute_batch("CREATE TABLE target(id TEXT NOT NULL PRIMARY KEY); INSERT INTO target VALUES('a'); INSERT INTO source VALUES(1)")?;
            if statement == "UPDATE source SET id=2" {
                sql.execute("INSERT INTO middle VALUES('b',1)",[])?;
            }
            let error = DbError::from(sql.execute(statement,[]).unwrap_err());
            assert!(matches!(error, DbError::StatementForbidden { operation: "changing a hidden rowid" }),"{statement}: {error:?}");
            assert_eq!(sql.query_row("SELECT id FROM source",[],|r|r.get::<_,i64>(0))?,1);
            assert_eq!(sql.query_row("SELECT rowid FROM target",[],|r|r.get::<_,i64>(0))?,1);
            Ok(())
        })]).open().await.unwrap();
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn rowid_policy_uses_the_storage_schema_of_implicitly_temporary_triggers() {
    let store = TestStore::new();
    let db = store.builder(vec![],vec![Migration::run(1,"implicit temp trigger",|sql| {
        sql.execute_batch("CREATE TABLE target(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE source(id INTEGER PRIMARY KEY); CREATE TEMP TABLE target(id INTEGER PRIMARY KEY); CREATE TEMP TABLE source(id INTEGER PRIMARY KEY); CREATE TRIGGER effect AFTER INSERT ON temp.source BEGIN INSERT INTO target(rowid) VALUES(4); END; INSERT INTO temp.source VALUES(1)")?;
        assert_eq!(sql.query_row("SELECT id FROM temp.target",[],|r|r.get::<_,i64>(0))?,4);
        sql.execute_batch("CREATE TRIGGER main.effect AFTER INSERT ON source BEGIN UPDATE target SET rowid=4; END")?;
        let error = DbError::from(sql.execute("INSERT INTO main.source VALUES(1)",[]).unwrap_err());
        assert!(matches!(error,DbError::StatementForbidden { operation: "changing a hidden rowid" }));
        Ok(())
    })]).open().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn rowid_policy_refreshes_for_migrations_and_reopening() {
    fn migrations() -> Vec<Migration> {
        vec![
            Migration::sql(
                1,
                "integer key",
                "CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)",
            ),
            Migration::run(2, "rebuild key", |sql| {
                sql.execute_batch("ALTER TABLE items RENAME TO old_items; CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY); INSERT INTO items(rowid,id) VALUES(7,'a'); DROP TABLE old_items")?;
                let error = DbError::from(sql.execute("UPDATE items SET rowid=8", []).unwrap_err());
                assert!(matches!(
                    error,
                    DbError::StatementForbidden {
                        operation: "changing a hidden rowid"
                    }
                ));
                sql.execute_batch("ALTER TABLE items ADD COLUMN oid INTEGER; UPDATE items SET oid=4; ALTER TABLE items RENAME COLUMN oid TO saved_oid")?;
                let error = DbError::from(sql.execute("UPDATE items SET rowid=8", []).unwrap_err());
                assert!(matches!(
                    error,
                    DbError::StatementForbidden {
                        operation: "changing a hidden rowid"
                    }
                ));
                sql.execute_batch("CREATE TEMP TABLE items(id INTEGER PRIMARY KEY); INSERT INTO temp.items VALUES(1); UPDATE temp.items SET rowid=2; DROP TABLE temp.items")?;
                let error = DbError::from(sql.execute("UPDATE items SET rowid=8", []).unwrap_err());
                assert!(matches!(
                    error,
                    DbError::StatementForbidden {
                        operation: "changing a hidden rowid"
                    }
                ));
                Ok(())
            }),
        ]
    }
    let store = TestStore::new();
    let mut initial = migrations();
    initial.truncate(1);
    store
        .builder(vec![], initial)
        .open()
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    for _ in 0..2 {
        let db = store.builder(vec![], migrations()).open().await.unwrap();
        let error = db
            .write(Default::default(), |sql| {
                sql.execute("UPDATE items SET _rowid_=8", [])?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DbError::StatementForbidden {
                operation: "changing a hidden rowid"
            }
        ));
        assert_eq!(
            db.read(
                |sql| Ok(sql.query_row("SELECT rowid FROM items", [], |r| r.get::<_, i64>(0))?)
            )
            .await
            .unwrap(),
            7
        );
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn rowid_policy_excludes_each_declared_alias_name() {
    for alias in ["RoWiD", "_ROWID_", "OiD"] {
        let store = TestStore::new();
        let db=store.builder(vec![],vec![Migration::run(1,"declared column",move |sql| {
            sql.execute_batch(&format!("CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY,{alias} INTEGER); INSERT INTO items VALUES('a',1)"))?;
            Ok(())
        })]).open().await.unwrap();
        db.write(Default::default(), move |sql| {
            sql.execute(&format!("UPDATE items SET {alias}=2"), [])?;
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            db.read(move |sql| Ok(sql.query_row(
                &format!("SELECT {alias} FROM items"),
                [],
                |r| r.get::<_, i64>(0)
            )?))
            .await
            .unwrap(),
            2
        );
        db.close().await.unwrap();
    }
}
