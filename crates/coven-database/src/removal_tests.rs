use std::collections::{BTreeMap, BTreeSet};

use coven_format::{merge_fields, value::Value};
use coven_merge::{ColumnValue, ForeignKey, Operation, Rule};

use crate::tests::TestStore;
use crate::write::tests::{count, records, sql};
use crate::{Database, RowIdentity, SyncedTable};

fn table(name: &str) -> SyncedTable {
    SyncedTable::new(name, RowIdentity::SharedKey)
}

type RemovedRow = (String, BTreeMap<String, ColumnValue<Value>>, BTreeSet<Rule>);

fn losses(database: &Database) -> Vec<RemovedRow> {
    database.inspect_writer(|db| db.query("SELECT table_name,value,replaced_by FROM _coven_lost WHERE column_id IS NULL ORDER BY table_name,key", [], |r| Ok((r.get(0)?, merge_fields::decode_columns(&r.get::<_, Vec<u8>>(1)?).unwrap(), merge_fields::decode_rules(&r.get::<_, Vec<u8>>(2)?).unwrap()))).unwrap())
}

#[tokio::test]
async fn readding_a_removed_shared_key_updates_every_column_and_clears_its_check() {
    let store = TestStore::new();
    let db = store.schema(vec![table("ranges")], "CREATE TABLE ranges(id TEXT NOT NULL PRIMARY KEY, start INT, end INT, CONSTRAINT ordered CHECK(start <= end))").await.unwrap();
    sql(&db, "INSERT INTO ranges VALUES('urgent',5,12)").await.unwrap();
    crate::tests::remote_update(
        &db,
        "ranges",
        "urgent",
        &[("start", Value::Integer(10)), ("end", Value::Integer(8))],
    )
    .await;
    sql(&db, "INSERT INTO ranges VALUES('urgent',10,20)").await.unwrap();
    let record = &records(&db)[1];
    let change = &record.parts[0].rows[0];
    assert_eq!(change.change.generation, 1);
    assert!(matches!(&change.change.operation, Operation::Update(columns) if columns.len() == 3));
    assert_eq!(change.old["end"], Value::Integer(8));
    assert_eq!(count(&db, "_coven_lost"), 0);
    assert_eq!(count(&db, "ranges"), 1);
    assert_eq!(count(&db, "_coven_rows"), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn deleting_groceries_winner_returns_note_46_and_its_link() {
    let store = TestStore::new();
    let db = store.schema(vec![table("notes"), table("links")], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id) ON DELETE SET NULL); CREATE TABLE counts(n INTEGER); INSERT INTO counts VALUES(0); CREATE TRIGGER counted_insert AFTER INSERT ON notes BEGIN UPDATE counts SET n=n+1; END; CREATE TRIGGER counted_delete AFTER DELETE ON notes BEGIN UPDATE counts SET n=n-1; END;").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('45','Groceries')").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('46','Shopping'); INSERT INTO links VALUES('7','46')")
        .await
        .unwrap();

    crate::tests::remote_update(&db, "notes", "46", &[("title", Value::Text("Groceries".into()))])
        .await;
    sql(&db, "SELECT 1").await.unwrap();
    assert_eq!(losses(&db).len(), 2);
    assert_eq!(records(&db).len(), 2);
    sql(&db, "DELETE FROM notes WHERE id='45'").await.unwrap();
    assert!(losses(&db).is_empty());
    assert_eq!(count(&db, "notes"), 1);
    assert_eq!(count(&db, "links"), 1);
    let (title, parent, count): (String, String, i64) = db.inspect_writer(|db| {
        db.query_row("SELECT notes.title,links.note_id,counts.n FROM notes,links,counts", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap()
    });
    assert_eq!((title.as_str(), parent.as_str(), count), ("Groceries", "46", 1));
    assert_eq!(records(&db)[2].parts[0].rows.len(), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn deleted_note_43_nulls_link_6_but_takes_out_stale_children() {
    for (action, restored) in
        [("SET NULL", true), ("CASCADE", false), ("RESTRICT", false), ("NO ACTION", false)]
    {
        let store = TestStore::new();
        let schema = format!("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id) ON DELETE {action}); CREATE TABLE counts(n INT); INSERT INTO counts VALUES(0); CREATE TRIGGER link_insert AFTER INSERT ON links BEGIN UPDATE counts SET n=n+1; END; CREATE TRIGGER link_delete AFTER DELETE ON links BEGIN UPDATE counts SET n=n-1; END;");
        let db = store
            .builder(
                vec![table("notes"), table("links")],
                vec![crate::Migration::addition_with(1, "references", move |context| {
                    context.execute_batch(&schema)?;
                    Ok(())
                })],
            )
            .open()
            .await
            .unwrap();
        sql(&db, "INSERT INTO notes VALUES('43'); INSERT INTO links VALUES('6','43')")
            .await
            .unwrap();
        let mut deletion =
            records(&db)[0].parts[0].rows.iter().find(|r| r.row.table == "notes").unwrap().clone();
        deletion.change.generation = 1;
        deletion.change.operation = Operation::Delete;
        crate::tests::remote_write(&db, deletion).await;
        sql(&db, "DELETE FROM notes").await.unwrap();
        assert_eq!(count(&db, "links"), i64::from(restored), "{action}");
        if restored {
            let parent: Option<String> = db.inspect_writer(|db| {
                db.query_row("SELECT note_id FROM links", [], |r| r.get(0)).unwrap()
            });
            assert_eq!(parent, None);
        } else {
            assert_eq!(
                losses(&db)[0].2,
                BTreeSet::from([Rule::ForeignKey(ForeignKey::new(["note_id"], "notes", ["id"]))])
            );
            sql(&db, "INSERT INTO notes VALUES('43')").await.unwrap();
            assert_eq!(count(&db, "links"), 0);
            sql(&db, "INSERT INTO links VALUES('6','43')").await.unwrap();
            assert_eq!(count(&db, "links"), 1);
            assert!(losses(&db).is_empty());
        }
        assert_eq!(
            db.inspect_writer(|db| db
                .query_row("SELECT n FROM counts", [], |r| r.get::<_, i64>(0))
                .unwrap()),
            1
        );
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn local_parent_deletion_records_cascade_and_null_as_ordinary_changes() {
    let store = TestStore::new();
    let db = store.schema(vec![table("notes"), table("attachments"), table("links")], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id) ON DELETE CASCADE); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id) ON DELETE SET NULL)").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('43'); INSERT INTO attachments VALUES('8','43'); INSERT INTO links VALUES('5','43')").await.unwrap();
    sql(&db, "DELETE FROM notes").await.unwrap();
    let record = records(&db).pop().unwrap();
    let rows = &record.parts[0].rows;
    assert_eq!(rows.len(), 3);
    for table in ["notes", "attachments"] {
        let row = rows.iter().find(|r| r.row.table == table).unwrap();
        assert_eq!(row.change.generation, 1);
        assert!(matches!(row.change.operation, Operation::Delete));
    }
    let link = rows.iter().find(|r| r.row.table == "links").unwrap();
    assert_eq!(link.change.generation, 1);
    let Operation::Update(columns) = &link.change.operation else { panic!("set null update") };
    assert_eq!(columns.len(), 1);
    assert_eq!(columns["note_id"].value, Value::Null);
    assert!(columns["note_id"].parents.is_empty());
    assert_eq!(link.old["note_id"], Value::Text("43".into()));
    assert!(db.lost_values().await.unwrap().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn local_restrict_and_no_action_children_refuse_parent_deletion_atomically() {
    for action in ["RESTRICT", "NO ACTION"] {
        let store = TestStore::new();
        let schema = format!("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id) ON DELETE {action})");
        let db = store
            .builder(
                vec![table("notes"), table("links")],
                vec![crate::Migration::addition_with(1, "references", move |context| {
                    context.execute_batch(&schema)?;
                    Ok(())
                })],
            )
            .open()
            .await
            .unwrap();
        sql(&db, "INSERT INTO notes VALUES('43'); INSERT INTO links VALUES('6','43')")
            .await
            .unwrap();
        let before = crate::tests::contents(&db);
        assert!(
            matches!(sql(&db, "DELETE FROM notes").await, Err(crate::DbError::Sqlite(_))),
            "{action}"
        );
        assert_eq!(crate::tests::contents(&db), before, "{action}");
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_check_that_forbids_null_preserves_the_written_reference_as_a_removed_row() {
    let ids = coven_foundation::id_source::SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let store = TestStore::with_ids(&ids);
    let tables = || vec![table("notes"), table("links")];
    let schema = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id) ON DELETE SET NULL, CONSTRAINT has_parent CHECK(note_id IS NOT NULL))";
    let source = source_store.schema(tables(), schema).await.unwrap();
    let db = store.schema(tables(), schema).await.unwrap();
    sql(&source, "INSERT INTO notes VALUES('43'),('44')").await.unwrap();
    db.receive(records(&source)[0].clone().into()).await.unwrap();
    sql(&db, "INSERT INTO links VALUES('6','43')").await.unwrap();
    let initial = records(&db).pop().unwrap();
    sql(&source, "DELETE FROM notes WHERE id='43'").await.unwrap();
    db.receive(records(&source)[1].clone().into()).await.unwrap();

    assert_eq!(count(&db, "links"), 0);
    let removed = losses(&db);
    let foreign_key = ForeignKey::new(["note_id"], "notes", ["id"]);
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].0, "links");
    assert_eq!(removed[0].2, [Rule::ForeignKey(foreign_key.clone())].into());
    assert_eq!(removed[0].1["note_id"].value, Value::Text("43".into()));
    assert_eq!(removed[0].1["note_id"].parents[&foreign_key].generation, 1);
    db.inspect_writer_schema(|db, schema| {
        let app = crate::write_rows::AppView::after(db, schema);
        let merge = crate::merge_store::MergeStore::new(db, &app);
        let link = initial.parts[0].rows.iter().find(|r| r.row.table == "links").unwrap();
        let state = merge.row(&link.row).unwrap().state;
        assert_eq!(state.generation(), 1);
        assert_eq!(state.cells()["note_id"].write, initial.header.position);
    });

    sql(&db, "INSERT INTO links VALUES('6','44')").await.unwrap();
    assert!(losses(&db).is_empty());
    assert_eq!(
        db.inspect_writer(|db| db
            .query_row("SELECT note_id FROM links", [], |r| r.get::<_, String>(0))
            .unwrap()),
        "44"
    );
    let record = records(&db).pop().unwrap();
    assert_eq!(record.parts[0].rows[0].change.generation, 1);
    assert!(matches!(record.parts[0].rows[0].change.operation, Operation::Update(_)));
    db.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn a_check_on_one_generated_column_can_fail_when_its_inputs_merge() {
    let ids = coven_foundation::id_source::SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let schema = "CREATE TABLE ranges(id TEXT NOT NULL PRIMARY KEY,start INT,end INT,width INT GENERATED ALWAYS AS(end-start),CONSTRAINT nonnegative CHECK(width>=0))";
    let a = a_store.schema(vec![table("ranges")], schema).await.unwrap();
    let b = b_store.schema(vec![table("ranges")], schema).await.unwrap();
    sql(&a, "INSERT INTO ranges(id,start,end) VALUES('r',0,1)").await.unwrap();
    a.receive(records(&a)[0].clone().into()).await.unwrap();
    b.receive(records(&a)[0].clone().into()).await.unwrap();
    sql(&a, "UPDATE ranges SET start=1").await.unwrap();
    sql(&b, "UPDATE ranges SET end=0").await.unwrap();
    let a_write = records(&a).pop().unwrap();
    let b_write = records(&b).pop().unwrap();
    for (db, write) in [(&a, b_write), (&b, a_write)] {
        db.receive(write.into()).await.unwrap();
        assert_eq!(count(db, "ranges"), 0);
        assert_eq!(losses(db)[0].2, [Rule::Check("nonnegative".into())].into());
    }
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn every_final_check_and_foreign_key_reason_is_retained() {
    let store = TestStore::new();
    let db = store.schema(vec![table("lists"), table("todos")], "CREATE TABLE lists(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE todos(id TEXT NOT NULL PRIMARY KEY,list_id TEXT REFERENCES lists(id),start INT,end INT,CONSTRAINT ordered CHECK(start <= end),CONSTRAINT nonnegative CHECK(start >= 0))").await.unwrap();
    sql(&db, "INSERT INTO lists VALUES('Work'); INSERT INTO todos VALUES('7','Work',5,12)")
        .await
        .unwrap();
    crate::tests::remote_update(
        &db,
        "todos",
        "7",
        &[("start", Value::Integer(-1)), ("end", Value::Integer(-2))],
    )
    .await;
    sql(&db, "DELETE FROM lists").await.unwrap();
    assert_eq!(
        losses(&db)[0].2,
        BTreeSet::from([
            Rule::Check("ordered".into()),
            Rule::Check("nonnegative".into()),
            Rule::ForeignKey(ForeignKey::new(["list_id"], "lists", ["id"]))
        ])
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn checks_keep_sqlite_affinity_and_quoted_expressions() {
    let store = TestStore::new();
    let db = store.schema(vec![table("ranges")], "CREATE TABLE ranges(id TEXT NOT NULL PRIMARY KEY, start TEXT, end TEXT, CONSTRAINT \"numeric test\" CHECK(CAST(ranges.start AS REAL) <= CAST(end AS REAL) + 0.25), CHECK(start < '9'))").await.unwrap();
    sql(&db, "INSERT INTO ranges VALUES('7','10','12')").await.unwrap();
    crate::tests::remote_update(&db, "ranges", "7", &[("end", Value::Text("8".into()))]).await;
    sql(&db, "SELECT 1").await.unwrap();
    assert_eq!(losses(&db)[0].2, BTreeSet::from([Rule::Check("numeric test".into())]));
    db.close().await.unwrap();
}

#[tokio::test]
async fn expression_and_partial_unique_claims_use_sqlite_equality() {
    let store = TestStore::new();
    let db = store.schema(vec![table("notes")], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT,folder TEXT,active INTEGER,ignored INTEGER); CREATE UNIQUE INDEX claimed ON notes(lower(title) DESC,folder COLLATE BINARY) WHERE active=1").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('45','Groceries','Work',1,0)").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('46','Shopping','Work',1,0); INSERT INTO notes VALUES('47',NULL,'Work',1,0)").await.unwrap();
    let before = crate::tests::contents(&db);
    let error = sql(&db, "UPDATE notes SET title='GROCERIES' WHERE id='46'").await.unwrap_err();
    assert!(
        matches!(error, crate::DbError::Sqlite(rusqlite::Error::SqliteFailure(e, _))
        if e.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE),
        "{error:?}"
    );
    assert_eq!(crate::tests::contents(&db), before);
    crate::tests::remote_update(&db, "notes", "46", &[("title", Value::Text("GROCERIES".into()))])
        .await;
    sql(&db, "UPDATE notes SET ignored=1 WHERE id='45'").await.unwrap();
    assert_eq!(
        losses(&db)[0].2,
        BTreeSet::from([Rule::Unique(coven_merge::UniqueConstraint {
            terms: vec!["lower(title)".into(), "folder COLLATE BINARY".into()],
            partial: Some("active=1".into())
        })])
    );
    assert_eq!(count(&db, "notes"), 2);
    sql(&db, "UPDATE notes SET active=0 WHERE id='45'").await.unwrap();
    assert!(losses(&db).is_empty());
    assert_eq!(count(&db, "notes"), 3);
    db.close().await.unwrap();
}

/// SQLite compares `+title` by title's NOCASE collation in an ordinary
/// expression, but builds the unique index over it with BINARY; claims follow
/// the index, so differently cased titles are two values.
#[tokio::test]
async fn an_expression_over_a_folded_column_claims_by_bytes_like_its_index() {
    let store = TestStore::new();
    let db = store.schema(vec![table("notes")], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT COLLATE NOCASE,edited INTEGER); CREATE UNIQUE INDEX titled ON notes(+title)").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('45','Groceries',0),('46','Shopping',0)").await.unwrap();
    crate::tests::remote_update(&db, "notes", "46", &[("title", Value::Text("GROCERIES".into()))])
        .await;
    sql(&db, "UPDATE notes SET edited=1 WHERE id='45'").await.unwrap();
    assert!(losses(&db).is_empty());
    assert_eq!(count(&db, "notes"), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn restoration_skips_shared_triggers_and_runs_local_triggers() {
    let store = TestStore::new();
    let db = store.schema(vec![table("notes").shared_trigger("edited")], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE,edited INT); CREATE TRIGGER edited AFTER INSERT ON notes WHEN NOT coven_applying() BEGIN UPDATE notes SET edited=edited+1 WHERE id=new.id; END; CREATE TABLE counts(n INT); INSERT INTO counts VALUES(0); CREATE TRIGGER counted_insert AFTER INSERT ON notes BEGIN UPDATE counts SET n=n+1; END; CREATE TRIGGER counted_delete AFTER DELETE ON notes BEGIN UPDATE counts SET n=n-1; END;").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('45','Groceries',0)").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('46','Shopping',0)").await.unwrap();
    crate::tests::remote_update(&db, "notes", "46", &[("title", Value::Text("Groceries".into()))])
        .await;
    sql(&db, "DELETE FROM notes WHERE id='45'").await.unwrap();
    assert_eq!(
        db.inspect_writer(|db| db
            .query_row("SELECT edited FROM notes", [], |r| r.get::<_, i64>(0))
            .unwrap()),
        1
    );
    assert_eq!(
        db.inspect_writer(|db| db
            .query_row("SELECT n FROM counts", [], |r| r.get::<_, i64>(0))
            .unwrap()),
        1
    );
    assert_eq!(records(&db)[2].parts[0].rows.len(), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn materialization_and_loss_failures_roll_back_the_entire_write() {
    for (point, statement) in [
        ("AFTER INSERT ON notes WHEN coven_applying()", "DELETE FROM notes WHERE id='45'"),
        ("AFTER DELETE ON _coven_lost", "DELETE FROM notes WHERE id='45'"),
        ("AFTER UPDATE ON _coven_lost", "DELETE FROM notes WHERE id='45'"),
    ] {
        let store = TestStore::new();
        let db = store
            .schema(
                vec![table("notes")],
                "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE,parent TEXT REFERENCES notes(id))",
            )
            .await
            .unwrap();
        sql(&db, "INSERT INTO notes VALUES('45','Groceries',NULL)").await.unwrap();
        sql(
            &db,
            if point == "AFTER UPDATE ON _coven_lost" {
                "INSERT INTO notes VALUES('46','Shopping','45')"
            } else {
                "INSERT INTO notes VALUES('46','Shopping',NULL)"
            },
        )
        .await
        .unwrap();
        crate::tests::remote_update(
            &db,
            "notes",
            "46",
            &[("title", Value::Text("Groceries".into()))],
        )
        .await;
        let before = crate::tests::contents(&db);
        db.inspect_writer(|db| db.fail_at("_coven_fail", point, "materialization failed"));
        assert!(matches!(sql(&db, statement).await, Err(crate::DbError::Sqlite(_))), "{point}");
        assert_eq!(crate::tests::contents(&db), before, "{point}");
        db.inspect_writer(|db| db.batch("DROP TRIGGER _coven_fail").unwrap());
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn taking_a_row_out_runs_foreign_key_actions_on_local_tables() {
    let store = TestStore::new();
    let db = store.schema(vec![table("notes")], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE,parent TEXT REFERENCES notes(id)); CREATE TABLE search_index(note_id TEXT REFERENCES notes(id) ON DELETE CASCADE); CREATE TABLE selection(note_id TEXT REFERENCES notes(id) ON DELETE SET NULL);").await.unwrap();
    hidden_plan_note(&db).await;
    sql(&db, "INSERT INTO search_index VALUES('1'); INSERT INTO selection VALUES('1')")
        .await
        .unwrap();

    sql(&db, "DELETE FROM notes WHERE id='3'; UPDATE notes SET title='Plan' WHERE id='1'")
        .await
        .unwrap();
    assert_eq!(count(&db, "notes"), 0);
    assert_eq!(count(&db, "search_index"), 0);
    assert_eq!(
        db.inspect_writer(|db| db
            .query_row("SELECT note_id FROM selection", [], |r| r.get::<_, Option<String>>(0))
            .unwrap()),
        None
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_trigger_cannot_silently_ignore_a_required_restoration() {
    let store = TestStore::new();
    let db = store.schema(vec![table("notes")], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE); CREATE TRIGGER refuse_restore BEFORE INSERT ON notes WHEN coven_applying() BEGIN SELECT RAISE(IGNORE); END;").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('45','Groceries')").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('46','Shopping')").await.unwrap();
    crate::tests::remote_update(&db, "notes", "46", &[("title", Value::Text("Groceries".into()))])
        .await;
    assert!(matches!(
        sql(&db, "DELETE FROM notes WHERE id='45'").await,
        Err(crate::DbError::Sqlite(rusqlite::Error::StatementChangedRows(0)))
    ));
    assert_eq!(records(&db).len(), 2);
    assert_eq!(count(&db, "notes"), 1);
    assert_eq!(losses(&db).len(), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn moving_note_42_also_moves_its_removed_attachments() {
    let store = TestStore::new();
    let db = store.schema(vec![SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"), SyncedTable::new("attachments", RowIdentity::IndependentUuid).audience_from("note_id")], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL); CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id),start INT,end INT,CONSTRAINT ordered CHECK(start<=end))").await.unwrap();
    store.join_circle(
        &db,
        coven_foundation::id_source::CircleId(
            uuid::Uuid::parse_str("00000000-0000-4000-8000-00000000000a").unwrap(),
        ),
    );
    sql(
        &db,
        "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000042','store'); INSERT INTO attachments VALUES('00000000-0000-4000-8000-000000000008','00000000-0000-4000-8000-000000000042',5,12)",
    )
    .await
    .unwrap();
    crate::tests::remote_update(
        &db,
        "attachments",
        "00000000-0000-4000-8000-000000000008",
        &[("start", Value::Integer(10)), ("end", Value::Integer(8))],
    )
    .await;
    sql(&db, "UPDATE notes SET audience='00000000-0000-4000-8000-00000000000a'").await.unwrap();
    let record = records(&db).pop().unwrap();
    assert_eq!(record.parts.len(), 2);
    assert!(record.parts.iter().all(|part| part.rows.len() == 2));
    assert_eq!(losses(&db)[0].2, BTreeSet::from([Rule::Check("ordered".into())]));
    let audience: String = db.inspect_writer(|db| {
        db.query_row("SELECT audience FROM _coven_lost", [], |r| r.get(0)).unwrap()
    });
    assert_eq!(audience, "00000000-0000-4000-8000-00000000000a");
    assert_eq!(count(&db, "attachments"), 0);
    sql(
        &db,
        "UPDATE notes SET audience='store'; INSERT INTO attachments VALUES('00000000-0000-4000-8000-000000000008','00000000-0000-4000-8000-000000000042',10,20)",
    )
    .await
    .unwrap();
    assert!(losses(&db).is_empty());
    assert_eq!(count(&db, "attachments"), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn store_wins_one_key_and_circles_use_their_generation_start() {
    const ANA: &str = "00000000-0000-4000-8000-00000000000a";
    const BEN: &str = "00000000-0000-4000-8000-00000000000b";
    for other in ["store", BEN] {
        let store = TestStore::new();
        let db = store.schema(vec![SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience")], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,title TEXT)").await.unwrap();
        store.join_circle(
            &db,
            coven_foundation::id_source::CircleId(
                uuid::Uuid::parse_str("00000000-0000-4000-8000-00000000000a").unwrap(),
            ),
        );
        db.write(|context| {
            context.execute(
                "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001',?1,'Circle')",
                [ANA],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        let mut other_row = records(&db)[0].parts[0].rows[0].clone();
        other_row.row.audience = if other == "store" {
            coven_merge::Audience::Store
        } else {
            coven_merge::Audience::Circle(coven_foundation::id_source::CircleId(
                uuid::Uuid::parse_str(other).unwrap(),
            ))
        };
        let Operation::Insert(columns) = &mut other_row.change.operation else { panic!("insert") };
        columns.get_mut("audience").unwrap().value = Value::Text(other.into());
        columns.get_mut("title").unwrap().value = Value::Text("Other".into());
        crate::tests::remote_write(&db, other_row).await;
        let winner: String = db.inspect_writer(|db| {
            db.query_row("SELECT audience FROM notes", [], |r| r.get(0)).unwrap()
        });
        assert_eq!(winner, if other == "store" { "store" } else { ANA });
        assert_eq!(losses(&db)[0].2, BTreeSet::from([Rule::OtherAudience]));
        if other == "store" {
            sql(&db, "DELETE FROM notes WHERE audience='store'").await.unwrap();
            assert!(losses(&db).is_empty());
            let audience: String = db.inspect_writer(|db| {
                db.query_row("SELECT audience FROM notes", [], |r| r.get(0)).unwrap()
            });
            assert_eq!(audience, ANA);
        } else {
            let deleted =
                coven_foundation::id_source::CircleId(uuid::Uuid::parse_str(ANA).unwrap());
            materialize_deleted(&db, "notes", &["00000000-0000-4000-8000-000000000001"], deleted)
                .unwrap();
            assert_eq!(records(&db).len(), 1);
            assert_eq!(losses(&db)[0].2, BTreeSet::from([Rule::DeletedCircle(deleting_entry())]));
            let audience: String = db.inspect_writer(|db| {
                db.query_row("SELECT audience FROM notes", [], |r| r.get(0)).unwrap()
            });
            assert_eq!(audience, BEN);
        }
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn unique_then_parent_removal_keeps_both_plan_notes_out() {
    let store = TestStore::new();
    let db = store.schema(vec![table("notes")], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE,parent TEXT REFERENCES notes(id) ON DELETE CASCADE)").await.unwrap();
    hidden_plan_note(&db).await;

    sql(&db, "DELETE FROM notes WHERE id='3'; UPDATE notes SET title='Plan' WHERE id='1'")
        .await
        .unwrap();
    assert_eq!(count(&db, "notes"), 0);
    let removed = losses(&db);
    assert_eq!(removed.len(), 2);
    assert!(removed[0].2.iter().any(|r| matches!(r, Rule::Unique(_))));
    assert_eq!(
        removed[1].2,
        BTreeSet::from([Rule::ForeignKey(ForeignKey::new(["parent"], "notes", ["id"]))])
    );
    assert_eq!(records(&db).last().unwrap().parts[0].rows.len(), 2);
    sql(&db, "INSERT INTO notes VALUES('1','Plan',NULL)").await.unwrap();
    assert_eq!(count(&db, "notes"), 0);
    assert_eq!(losses(&db).len(), 2);
    assert!(matches!(
        records(&db).last().unwrap().parts[0].rows[0].change.operation,
        Operation::Update(_)
    ));
    db.close().await.unwrap();
}

#[tokio::test]
async fn unique_constraint_identity_preserves_repeated_columns() {
    let store = TestStore::new();
    let db=store.schema(vec![table("notes")],"CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT,edited INT,UNIQUE(title,title))").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('45','Groceries',0)").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('46','Shopping',0)").await.unwrap();
    crate::tests::remote_update(&db, "notes", "46", &[("title", Value::Text("Groceries".into()))])
        .await;
    sql(&db, "UPDATE notes SET edited=1 WHERE id='45'").await.unwrap();
    assert_eq!(losses(&db)[0].2, BTreeSet::from([Rule::Unique(["title", "title"].into())]));
    db.close().await.unwrap();
}

#[tokio::test]
async fn unique_indexes_with_the_same_columns_keep_each_claim() {
    let store = TestStore::new();
    let db=store.schema(vec![table("notes")],"CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT,parent TEXT REFERENCES notes(id),edited INT); CREATE UNIQUE INDEX whole_title ON notes(lower(title)); CREATE UNIQUE INDEX initial ON notes(lower(substr(title,1,1)))").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('45','alpha',NULL,0)").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('46','beta','45',0)").await.unwrap();
    crate::tests::remote_update(&db, "notes", "46", &[("title", Value::Text("APPLE".into()))])
        .await;
    sql(&db, "UPDATE notes SET edited=1 WHERE id='45'").await.unwrap();
    assert_eq!(count(&db, "notes"), 1);
    assert_eq!(
        losses(&db)[0].2,
        BTreeSet::from([Rule::Unique(["lower(substr(title,1,1))"].into())])
    );
    db.close().await.unwrap();
}

/// A generated column has no cell, so a claim on it dates from the latest
/// write that set a stored column it is computed from, through any chain of
/// generated columns: `tag` reads `folded`, which reads `title`. Renaming
/// "Plan" to "PLAN" leaves `tag` as it was, and still dates the claim (§9.2).
#[tokio::test]
async fn a_generated_unique_claim_dates_from_its_stored_inputs() {
    let fixture = TestStore::new();
    let db = fixture
        .schema(
            vec![table("notes")],
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT,folded TEXT AS(lower(title)),tag TEXT AS(folded || '-') UNIQUE)",
        )
        .await
        .unwrap();
    sql(&db, "INSERT INTO notes(id,title) VALUES('n','Draft')").await.unwrap();
    sql(&db, "UPDATE notes SET title='Plan'").await.unwrap();
    sql(&db, "UPDATE notes SET title='PLAN'").await.unwrap();
    let writes = records(&db);
    assert!(writes[2].header.timestamp > writes[1].header.timestamp);
    db.inspect_writer_schema(|db, schema| {
        let row = &writes[0].parts[0].rows[0].row;
        let app = crate::write_rows::AppView::after(db, schema);
        let store = crate::merge_store::MergeStore::new(db, &app);
        let updates = BTreeMap::new();
        let deleted = BTreeMap::new();
        let view = crate::removal_view::DatabaseRemovalView::new(
            db, &store, schema, &app, &updates, &deleted, None,
        )
        .unwrap();
        let evaluated = view.evaluated(row).unwrap();
        assert_eq!(evaluated.values["tag"], Value::Text("plan-".into()));
        assert_eq!(
            evaluated.constraints.unique[&coven_merge::UniqueConstraint::from(["tag"])].order,
            coven_merge::ClaimOrder::Local { number: writes[2].header.position.number }
        );
    });
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_constant_unique_index_keeps_its_expression_term() {
    let store = TestStore::new();
    let db=store.schema(vec![table("notes")],"CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE UNIQUE INDEX one_note ON notes((1))").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('45')").await.unwrap();
    let mut second = records(&db)[0].parts[0].rows[0].clone();
    second.row.key = coven_format::key::encode_key(&[Value::Text("46".into())]).unwrap();
    let Operation::Insert(columns) = &mut second.change.operation else { panic!("insert") };
    columns.get_mut("id").unwrap().value = Value::Text("46".into());
    crate::tests::remote_write(&db, second).await;

    assert_eq!(count(&db, "notes"), 1);
    assert_eq!(
        db.inspect_writer(|db| db
            .query_row("SELECT id FROM notes", [], |r| r.get::<_, String>(0))
            .unwrap()),
        "45"
    );
    assert_eq!(losses(&db)[0].2, BTreeSet::from([Rule::Unique(["(1)"].into())]));
    db.close().await.unwrap();
}

#[tokio::test]
async fn references_to_mutable_nonkey_columns_are_refused() {
    let fixture = TestStore::new();
    let schema = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,code TEXT UNIQUE); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(code) ON UPDATE CASCADE)";
    let result = fixture.schema(vec![table("parents"), table("children")], schema).await;
    let error = crate::tests::database_error(result.err().expect("mutable parent key refused"));
    assert!(
        matches!(error, crate::DbError::Schema(crate::SchemaError::ForeignKeyNotPrimaryKey { table, key })
        if table == "children" && key == "(\"parent\") REFERENCES \"parents\" (\"code\")")
    );
}

pub(crate) fn materialize_deleted(
    db: &crate::Database,
    table: &str,
    keys: &[&str],
    circle: coven_foundation::id_source::CircleId,
) -> Result<(), crate::DbError> {
    db.inspect_writer_schema(|db, schema| {
        db.transaction(|db| {
            db.batch("PRAGMA defer_foreign_keys=ON")?;
            let app = crate::write_rows::AppView::after(db, schema);
            let store = crate::merge_store::MergeStore::new(db, &app);
            let updates = std::collections::BTreeMap::new();
            let deleted = [(circle, deleting_entry())].into();
            let view = crate::removal_view::DatabaseRemovalView::new(
                db, &store, schema, &app, &updates, &deleted, None,
            )?;
            let rows = keys.iter().map(|key| coven_merge::RowId {
                table: table.into(),
                key: coven_format::key::encode_key(&[Value::Text((*key).into())]).unwrap(),
                audience: coven_merge::Audience::Circle(circle),
            });
            let result = match coven_merge::recompute(&view, &view, rows) {
                Ok(result) => result,
                Err(crate::removal_view::RemovalFailure::Database(error)) => return Err(error),
                Err(crate::removal_view::RemovalFailure::Merge(error)) => {
                    panic!("database removal view: {error}")
                }
            };
            crate::removal::materialize(
                db,
                schema,
                &app,
                &view,
                &result,
                &std::collections::BTreeMap::new(),
            )
        })
    })
}

/// A competing title hides the child while its parent remains writable.
pub(crate) async fn hidden_plan_note(db: &Database) {
    sql(
        db,
        "INSERT INTO notes VALUES('1','Ideas',NULL); INSERT INTO notes VALUES('3','Plan',NULL)",
    )
    .await
    .unwrap();
    sql(db, "INSERT INTO notes VALUES('2','Draft','1')").await.unwrap();
    crate::tests::remote_update(db, "notes", "2", &[("title", Value::Text("Plan".into()))]).await;
    assert_eq!(count(db, "notes"), 2);
    assert_eq!(losses(db)[0].2, [Rule::Unique(["title"].into())].into());
}

fn deleting_entry() -> crate::Slot {
    crate::Slot(1)
}

/// A child moving its unique value and taking a parent that returns with the
/// value it gives up: the parent cannot be inserted while the child still
/// holds the value, so the child's update comes first and its deferred
/// foreign key is checked at commit.
#[test]
fn unique_claims_order_before_deferrable_parents() {
    let row = |key: &str| coven_merge::RowId {
        table: "rows".into(),
        key: key.as_bytes().to_vec(),
        audience: coven_merge::Audience::Store,
    };
    let (child, parent) = (row("child"), row("parent"));
    let rows = std::collections::BTreeSet::from([child.clone(), parent.clone()]);
    let claims = std::collections::BTreeMap::from([(
        parent.clone(),
        std::collections::BTreeSet::from([child.clone()]),
    )]);
    let parents = std::collections::BTreeMap::from([(
        child.clone(),
        std::collections::BTreeSet::from([parent.clone()]),
    )]);
    assert_eq!(
        super::dependency_order(rows.clone(), &claims, &parents),
        [child.clone(), parent.clone()]
    );
    // Without the claim the parent is put first.
    assert_eq!(
        super::dependency_order(rows, &std::collections::BTreeMap::new(), &parents),
        [parent, child]
    );
}
