use crate::{
    tests::{database_error, remote_update, try_remote_update, TestStore},
    *,
};
use coven_format::value::Value;

fn shared(name: &str) -> SyncedTable {
    SyncedTable::new(name, RowIdentity::SharedKey)
}

const PARENTS: &str = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,code TEXT UNIQUE)";

/// Coven applies a change to a synced non-key value with an UPDATE, so a local
/// key into it, or into a local value a following key rewrites, must follow
/// the update; key columns change by delete and insert (§8.4).
#[tokio::test]
async fn local_keys_into_values_coven_updates_must_follow_them() {
    let pins = "CREATE TABLE pins(id TEXT PRIMARY KEY,code TEXT UNIQUE REFERENCES parents(code) ON DELETE CASCADE";
    for (local, refused) in [
        (format!("{pins})"), Some("pins")),
        (format!("{pins} ON UPDATE NO ACTION)"), Some("pins")),
        (format!("{pins} ON UPDATE RESTRICT)"), Some("pins")),
        (format!("{pins} ON UPDATE CASCADE)"), None),
        (format!("{pins} ON UPDATE SET NULL)"), None),
        (
            "CREATE TABLE pins(note TEXT REFERENCES parents(id) ON DELETE CASCADE)".into(),
            None,
        ),
        (
            format!("{pins} ON UPDATE CASCADE); CREATE TABLE marks(code TEXT REFERENCES pins(code) ON DELETE CASCADE)"),
            Some("marks"),
        ),
        (
            format!("{pins} ON UPDATE SET NULL); CREATE TABLE marks(code TEXT REFERENCES pins(code) ON DELETE CASCADE ON UPDATE RESTRICT)"),
            Some("marks"),
        ),
        (
            format!("{pins} ON UPDATE CASCADE); CREATE TABLE marks(code TEXT REFERENCES pins(code) ON DELETE CASCADE ON UPDATE CASCADE)"),
            None,
        ),
        (
            format!("{pins} ON UPDATE CASCADE); CREATE TABLE marks(pin TEXT REFERENCES pins(id) ON DELETE CASCADE)"),
            None,
        ),
    ] {
        let schema = format!("{PARENTS}; {local}");
        let check = |result: Result<Database, CovenError>| match refused {
            Some(table) => assert!(
                matches!(database_error(result.err().expect("refused")), DbError::Schema(SchemaError::LocalChildUpdate { table: t, column }) if t == table && column == "code"),
                "{local}"
            ),
            None => drop(result.expect(&local)),
        };
        let store = TestStore::new();
        let migration = schema.clone();
        check(
            store
                .builder(
                    vec![shared("parents")],
                    vec![Migration::run(1, "schema", move |sql| {
                        sql.execute_batch(&migration)?;
                        Ok(())
                    })],
                )
                .open()
                .await,
        );
        let store = TestStore::new();
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        db.inspect_writer(|writer| writer.batch(&schema).unwrap());
        db.close().await.unwrap();
        check(store.builder(vec![shared("parents")], vec![]).open().await);
    }
}

/// The counterexample `LocalUpdate.stuck_forever`: a local pin added outside
/// the schema check holds the old code with the default ON UPDATE NO ACTION, so
/// applying Ben's rename fails at commit, and fails again on retry.
#[tokio::test]
async fn a_local_key_that_does_not_follow_stops_every_apply() {
    let store = TestStore::new();
    let db = store
        .schema(vec![shared("parents")], PARENTS)
        .await
        .unwrap();
    crate::write::tests::sql(&db, "INSERT INTO parents VALUES('p','a')")
        .await
        .unwrap();
    db.inspect_writer(|writer| {
        writer
            .batch("CREATE TABLE pins(code TEXT REFERENCES parents(code) ON DELETE CASCADE); INSERT INTO pins VALUES('a')")
            .unwrap()
    });
    for _ in 0..2 {
        let error = try_remote_update(&db, "parents", "p", &[("code", Value::Text("b".into()))])
            .await
            .expect_err("the commit check fails");
        assert!(
            error.to_string().contains("FOREIGN KEY constraint failed"),
            "{error}"
        );
    }
    db.close().await.unwrap();
    assert!(matches!(
        database_error(
            store
                .builder(
                    vec![shared("parents")],
                    vec![Migration::sql(1, "schema", PARENTS)]
                )
                .open()
                .await
                .err()
                .expect("refused")
        ),
        DbError::Schema(SchemaError::LocalChildUpdate { .. })
    ));
}

/// `LocalUpdate.chain_holds`: accepted keys follow the applied rename through
/// a chain of local tables.
#[tokio::test]
async fn an_applied_rename_reaches_every_following_local_key() {
    for (action, expected) in [("CASCADE", Some("b")), ("SET NULL", None)] {
        let store = TestStore::new();
        let schema = format!("{PARENTS}; CREATE TABLE pins(code TEXT UNIQUE REFERENCES parents(code) ON DELETE CASCADE ON UPDATE {action}); CREATE TABLE marks(code TEXT REFERENCES pins(code) ON DELETE CASCADE ON UPDATE CASCADE)");
        let db = store
            .builder(
                vec![shared("parents")],
                vec![Migration::run(1, "schema", move |sql| {
                    sql.execute_batch(&schema)?;
                    Ok(())
                })],
            )
            .open()
            .await
            .unwrap();
        crate::write::tests::sql(
            &db,
            "INSERT INTO parents VALUES('p','a'); INSERT INTO pins VALUES('a'); INSERT INTO marks VALUES('a')",
        )
        .await
        .unwrap();
        remote_update(&db, "parents", "p", &[("code", Value::Text("b".into()))]).await;
        db.read(move |sql| {
            for table in ["pins", "marks"] {
                assert_eq!(
                    sql.query_row(&format!("SELECT code FROM {table}"), [], |r| {
                        r.get::<_, Option<String>>(0)
                    })?
                    .as_deref(),
                    expected,
                    "{action} {table}"
                );
            }
            Ok(())
        })
        .await
        .unwrap();
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn set_default_between_local_tables_remains_sqlites_action() {
    const SCHEMA: &str = "CREATE TABLE synced(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE parent(id TEXT PRIMARY KEY); CREATE TABLE child(p TEXT NOT NULL DEFAULT 'fallback' REFERENCES parent(id) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT); CREATE TABLE null_default(p TEXT NOT NULL REFERENCES parent(id) ON DELETE SET DEFAULT)";
    let store = TestStore::new();
    let db = store.schema(vec![shared("synced")], SCHEMA).await.unwrap();
    crate::write::tests::sql(&db, "INSERT INTO parent VALUES('p'),('fallback'); INSERT INTO child VALUES('p'); UPDATE parent SET id='q' WHERE id='p'").await.unwrap();
    db.read(|sql| {
        assert_eq!(
            sql.query_row("SELECT p FROM child", [], |r| r.get::<_, String>(0))?,
            "fallback"
        );
        Ok(())
    })
    .await
    .unwrap();
    crate::write::tests::sql(
        &db,
        "UPDATE child SET p='q'; DELETE FROM parent WHERE id='q'",
    )
    .await
    .unwrap();
    db.read(|sql| {
        assert_eq!(
            sql.query_row("SELECT p FROM child", [], |r| r.get::<_, String>(0))?,
            "fallback"
        );
        Ok(())
    })
    .await
    .unwrap();
    db.close().await.unwrap();
    store
        .schema(vec![shared("synced")], SCHEMA)
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    store
        .builder(
            vec![shared("synced")],
            vec![Migration::sql(1, "schema", SCHEMA)],
        )
        .open_read_only()
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn local_children_can_reference_unique_nonprimary_values() {
    for synced_parent in [false, true] {
        let store = TestStore::new();
        let tables = if synced_parent {
            vec![shared("parents")]
        } else {
            vec![]
        };
        let db = store.schema(tables, "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,code TEXT UNIQUE); CREATE TABLE children(parent TEXT REFERENCES parents(code) ON DELETE CASCADE ON UPDATE CASCADE)").await.unwrap();
        crate::write::tests::sql(&db, "INSERT INTO parents VALUES('p','old'); INSERT INTO children VALUES('old'); UPDATE parents SET code='new'").await.unwrap();
        db.read(|sql| {
            assert_eq!(
                sql.query_row("SELECT parent FROM children", [], |r| r.get::<_, String>(0))?,
                "new"
            );
            Ok(())
        })
        .await
        .unwrap();
        db.close().await.unwrap();
    }
}
