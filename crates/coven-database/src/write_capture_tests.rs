use crate::tests::TestStore;
use crate::write::tests::{count, records, sql, NOTES};
use crate::{DbError, RowIdentity, RowKey, SyncedTable};
use coven_merge::Operation;
use rusqlite::types::Value;

const V4: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
const V7: &str = "018f22bb-aaaa-7777-8ccc-000000000001";

fn independent() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::IndependentUuid)]
}

fn assert_key_error(error: DbError, expected: Vec<Value>) {
    match error {
        DbError::KeyNotUuid { table, key } => {
            assert_eq!(table, "notes");
            assert_eq!(key, RowKey(expected));
        }
        error => panic!("expected KeyNotUuid, received {error:?}"),
    }
}

#[tokio::test]
async fn independent_key_insert_refuses_invalid_uuid_and_rolls_back() {
    let store = TestStore::new();
    let db = store.schema(independent(), NOTES).await.unwrap();
    for invalid in [
        "not a uuid",
        "F47AC10B-58CC-4372-A567-0E02B2C3D479",
        "f47ac10b-58cc-1372-a567-0e02b2c3d479",
        "f47ac10b-58cc-5372-a567-0e02b2c3d479",
        "f47ac10b-58cc-8372-a567-0e02b2c3d479",
        "f47ac10b-58cc-4372-7567-0e02b2c3d479",
        "f47ac10b-58cc-4372-c567-0e02b2c3d479",
        "f47ac10b58cc4372a5670e02b2c3d479",
        "{f47ac10b-58cc-4372-a567-0e02b2c3d479}",
        "urn:uuid:f47ac10b-58cc-4372-a567-0e02b2c3d479",
        "f47ac10b-58cc-4372-a567-0e02b2c3d479 ",
        "",
        "42",
    ]
    .into_iter()
    .map(|s| Value::Text(s.into()))
    .chain([Value::Blob(V4.as_bytes().into())])
    {
        let input = invalid.clone();
        let error = db
            .write(move |context| {
                context.execute("INSERT INTO local_rows VALUES('rollback')", [])?;
                context.execute("INSERT INTO notes VALUES(?1,'valid','')", [V4])?;
                context.execute(
                    "INSERT INTO notes VALUES(?1,?2,'')",
                    rusqlite::params![input, V7],
                )?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert_key_error(error, vec![invalid]);
        for table in [
            "notes",
            "local_rows",
            "_coven_writes",
            "_coven_uploads",
            "_coven_rows",
            "_coven_cells",
            "_coven_positions",
        ] {
            assert_eq!(count(&db, table), 0, "{table}");
        }
    }
    db.write(|context| {
        for key in [V4, V7] {
            context.execute("INSERT INTO notes VALUES(?1,'valid','')", [key])?;
        }
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(count(&db, "notes"), 2);
    assert_eq!(records(&db)[0].header.position.number, 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn each_composite_independent_key_can_hold_its_uuid_in_a_different_column() {
    let store = TestStore::new();
    let db = store.schema(
        vec![SyncedTable::new("notes", RowIdentity::IndependentUuid).key_columns(["second", "first"])],
        "CREATE TABLE notes(first TEXT NOT NULL,second BLOB NOT NULL,title TEXT,PRIMARY KEY(second,first))",
    ).await.unwrap();
    db.write(|context| {
        context.execute("INSERT INTO notes VALUES(?1,7,'first column')", [V4])?;
        // The schema needs one text-affinity key column; each row may put its
        // UUID text in any key column, including one with another affinity.
        context.execute("INSERT INTO notes VALUES('name',?1,'second column')", [V7])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(count(&db, "notes"), 2);
    let error = db
        .write(|context| {
            context.execute("INSERT INTO notes VALUES('invalid',9,?1)", [V4])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert_key_error(
        error,
        vec![Value::Integer(9), Value::Text("invalid".into())],
    );
    assert_eq!(count(&db, "notes"), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn independent_key_changes_validate_the_raw_insert_before_collation() {
    for schema in [
        NOTES,
        "CREATE TABLE notes(id TEXT COLLATE NOCASE NOT NULL PRIMARY KEY,title TEXT,body TEXT); CREATE TABLE local_rows(id TEXT PRIMARY KEY)",
        "CREATE TABLE notes(id TEXT COLLATE RTRIM NOT NULL PRIMARY KEY,title TEXT,body TEXT); CREATE TABLE local_rows(id TEXT PRIMARY KEY)",
    ] {
        let store = TestStore::new();
        let db = store.schema(independent(), schema).await.unwrap();
        db.write(|context| {
            context.execute("INSERT INTO notes VALUES(?1,'original','')", [V4])?;
            Ok(())
        }).await.unwrap();
        for invalid in ["not a uuid".to_owned(), V4.to_uppercase(), format!("{V4} ")] {
            let input = invalid.clone();
            let error = db.write(move |context| {
                context.execute("INSERT INTO local_rows VALUES('rollback')", [])?;
                context.execute("UPDATE notes SET id=?1,title='changed'", [input])?;
                Ok(())
            }).await.unwrap_err();
            assert_key_error(error, vec![Value::Text(invalid)]);
            assert_eq!(count(&db, "local_rows"), 0);
            assert_eq!(records(&db).len(), 1);
            assert_eq!(db.read(|sql| Ok(sql.query_row("SELECT id,title FROM notes", [], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?)).await.unwrap(), (V4.into(), "original".into()));
        }
        db.write(|context| {
            context.execute("UPDATE notes SET id=?1", [V7])?;
            Ok(())
        }).await.unwrap();
        let writes = records(&db);
        let rows = &writes[1].parts[0].rows;
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|r| matches!(r.change.operation, Operation::Delete)));
        assert!(rows.iter().any(|r| matches!(r.change.operation, Operation::Insert(_))));
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn valid_readds_advance_generations_and_canceled_inserts_leave_no_record() {
    let store = TestStore::new();
    let db = store.schema(independent(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('not a uuid','temporary',''); DELETE FROM notes; INSERT INTO local_rows VALUES('kept')").await.unwrap();
    assert!(records(&db).is_empty());
    for insert in [true, false, true] {
        db.write(move |context| {
            if insert {
                context.execute("INSERT INTO notes VALUES(?1,'valid','')", [V4])?;
            } else {
                context.execute("DELETE FROM notes WHERE id=?1", [V4])?;
            }
            Ok(())
        })
        .await
        .unwrap();
    }
    assert_eq!(
        records(&db)
            .iter()
            .map(|r| r.parts[0].rows[0].change.generation)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn readding_a_rule_removed_row_keeps_its_generation() {
    let store = TestStore::new();
    let db = store.schema(independent(), "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,body TEXT NOT NULL DEFAULT '',CONSTRAINT title_present CHECK(title <> ''))").await.unwrap();
    db.write(|context| {
        context.execute("INSERT INTO notes VALUES(?1,'original','')", [V4])?;
        Ok(())
    })
    .await
    .unwrap();
    crate::removal::tests::remove(
        &db,
        "notes",
        V4,
        &[("title", coven_format::value::Value::Text(String::new()))],
        [coven_merge::Rule::Check("title_present".into())].into(),
    );
    assert_eq!(db.lost_values().await.unwrap().len(), 1);
    db.write(|context| {
        context.execute("INSERT INTO notes VALUES(?1,'restored','')", [V4])?;
        Ok(())
    })
    .await
    .unwrap();
    let restored = records(&db).pop().unwrap();
    assert_eq!(restored.parts[0].rows[0].change.generation, 1);
    assert!(matches!(
        restored.parts[0].rows[0].change.operation,
        Operation::Update(_)
    ));
    assert!(db.lost_values().await.unwrap().is_empty());
    assert_eq!(count(&db, "notes"), 1);
    db.close().await.unwrap();
}
