use std::collections::{BTreeMap, BTreeSet};

use coven_format::{merge_fields, value::Value};
use coven_merge::{ColumnValue, ForeignKey, Operation, Rule};

use crate::tests::TestStore;
use crate::write::tests::{count, records, sql};
use crate::{Database, RowIdentity, SyncedTable};

fn table(name: &str) -> SyncedTable {
    SyncedTable::new(name, RowIdentity::SharedKey)
}

// Fixture an already-merged hidden row. Downloaded-write application is a
// different database capability; these tests start at its persistent boundary.
pub(crate) fn remove(
    database: &Database,
    table: &str,
    id: &str,
    changes: &[(&str, Value)],
    rules: BTreeSet<Rule>,
) {
    database.inspect_writer_schema(|db,schema| {
        let (row, generation, audience): (i64, Vec<u8>, String) = db.query_row("SELECT id,generation,audience FROM coven_rows WHERE table_name=?1 AND key=?2 ORDER BY generation DESC LIMIT 1", crate::params![table, coven_format::key::encode_key(&[Value::Text(id.into())]).unwrap()], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
        let identity = coven_merge::RowId { table: table.into(), key:coven_format::key::encode_key(&[Value::Text(id.into())]).unwrap(), audience:crate::write_encoding::audience(&audience).unwrap() };
        let app = crate::write_rows::AppView::after(db,schema);
        let store = crate::merge_store::MergeStore::new(db,&app);
        let state = store.row(&identity).unwrap().state;
        let mut columns: BTreeMap<_,_> = state.cells().iter().map(|(name,cell)| (name.clone(),cell.value.clone())).collect();
        for (name, value) in changes { columns.get_mut(*name).unwrap().value = value.clone(); }
        let setters = state.cells().iter().map(|(name,cell)| (name.clone(),cell.write)).collect();
        db.internal_execute("INSERT INTO coven_lost(table_name,key,audience,generation,column_id,value,set_by,replacement_kind,replaced_by) VALUES(?1,?2,?3,?4,NULL,?5,?6,'rules',?7)", crate::params![table, identity.key, audience, generation, merge_fields::encode_columns(&columns).unwrap(), merge_fields::encode_setters(&setters).unwrap(), merge_fields::encode_rules(&rules).unwrap()]).unwrap();
        let values = columns.into_iter().map(|(n,c)| (n,c.value)).collect();
        let claims = crate::removal_sql::constraints(db,schema.table(table),&schema.rules[table],&state,&values,|id| store.stamp(id)).unwrap().unique;
        for (constraint,claim) in claims {
            let constraint = crate::row_queries::constraint(db,table,&constraint).unwrap();
            db.internal_execute("INSERT INTO coven_claims(row_id,constraint_id,audience,value) VALUES(?1,?2,?3,?4)",crate::params![row,constraint,audience,claim.value]).unwrap();
        }
        db.materialize(|db| { db.internal_execute(&format!("DELETE FROM {} WHERE id=?1", crate::sql::identifier(table)), [id])?; Ok(()) }).unwrap();
    });
}

type RemovedRow = (String, BTreeMap<String, ColumnValue<Value>>, BTreeSet<Rule>);

fn losses(database: &Database) -> Vec<RemovedRow> {
    database.inspect_writer(|db| db.query("SELECT table_name,value,replaced_by FROM coven_lost WHERE column_id IS NULL ORDER BY table_name,key", [], |r| Ok((r.get(0)?, merge_fields::decode_columns(&r.get::<_, Vec<u8>>(1)?).unwrap(), merge_fields::decode_rules(&r.get::<_, Vec<u8>>(2)?).unwrap()))).unwrap())
}

#[tokio::test]
async fn readding_a_removed_shared_key_updates_every_column_and_clears_its_check() {
    let store = TestStore::new();
    let db = store.schema(vec![table("ranges")], "CREATE TABLE ranges(id TEXT NOT NULL PRIMARY KEY, start INT, end INT, CONSTRAINT ordered CHECK(start <= end))").await.unwrap();
    sql(&db, "INSERT INTO ranges VALUES('urgent',5,12)")
        .await
        .unwrap();
    remove(
        &db,
        "ranges",
        "urgent",
        &[("start", Value::Integer(10)), ("end", Value::Integer(8))],
        BTreeSet::from([Rule::Check("ordered".into())]),
    );
    sql(&db, "INSERT INTO ranges VALUES('urgent',10,20)")
        .await
        .unwrap();
    let record = &records(&db)[1];
    let change = &record.parts[0].rows[0];
    assert_eq!(change.change.generation, 1);
    assert!(matches!(&change.change.operation, Operation::Update(columns) if columns.len() == 3));
    assert_eq!(change.old["end"], Value::Integer(8));
    assert_eq!(count(&db, "coven_lost"), 0);
    assert_eq!(count(&db, "ranges"), 1);
    assert_eq!(count(&db, "coven_rows"), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn deleting_groceries_winner_returns_note_46_and_its_link() {
    let store = TestStore::new();
    let db = store.schema(vec![table("notes"), table("links")], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id) ON DELETE SET NULL); CREATE TABLE counts(n INTEGER); INSERT INTO counts VALUES(0); CREATE TRIGGER counted_insert AFTER INSERT ON notes BEGIN UPDATE counts SET n=n+1; END; CREATE TRIGGER counted_delete AFTER DELETE ON notes BEGIN UPDATE counts SET n=n-1; END;").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('45','Groceries')")
        .await
        .unwrap();
    sql(
        &db,
        "INSERT INTO notes VALUES('46','Shopping'); INSERT INTO links VALUES('7','46')",
    )
    .await
    .unwrap();
    remove(
        &db,
        "links",
        "7",
        &[],
        BTreeSet::from([Rule::ForeignKey(ForeignKey::new(
            ["note_id"],
            "notes",
            ["id"],
        ))]),
    );
    remove(
        &db,
        "notes",
        "46",
        &[("title", Value::Text("Groceries".into()))],
        BTreeSet::from([Rule::Unique(["title"].into())]),
    );
    sql(&db, "SELECT 1").await.unwrap();
    assert_eq!(losses(&db).len(), 2);
    assert_eq!(records(&db).len(), 2);
    sql(&db, "DELETE FROM notes WHERE id='45'").await.unwrap();
    assert!(losses(&db).is_empty());
    assert_eq!(count(&db, "notes"), 1);
    assert_eq!(count(&db, "links"), 1);
    let (title, parent, count): (String, String, i64) = db.inspect_writer(|db| {
        db.query_row(
            "SELECT notes.title,links.note_id,counts.n FROM notes,links,counts",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap()
    });
    assert_eq!(
        (title.as_str(), parent.as_str(), count),
        ("Groceries", "46", 1)
    );
    assert_eq!(records(&db)[2].parts[0].rows.len(), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn readding_inbox_returns_note_50_without_changing_its_reference_setter() {
    let store = TestStore::new();
    let db = store.schema(vec![table("folders"), table("notes")], "CREATE TABLE folders(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,folder TEXT DEFAULT 'Inbox' REFERENCES folders(id) ON DELETE SET DEFAULT)").await.unwrap();
    sql(
        &db,
        "INSERT INTO folders VALUES('Work'),('Inbox'); INSERT INTO notes VALUES('50','Work')",
    )
    .await
    .unwrap();
    remove(
        &db,
        "notes",
        "50",
        &[],
        BTreeSet::from([Rule::ForeignKey(ForeignKey::new(
            ["folder"],
            "folders",
            ["id"],
        ))]),
    );
    sql(&db, "DELETE FROM folders").await.unwrap();
    assert_eq!(count(&db, "notes"), 0);
    assert_eq!(
        losses(&db)[0].1["folder"].value,
        Value::Text("Inbox".into())
    );
    let setters: Vec<i64> = db.inspect_writer(|db| db.query("SELECT write_id FROM coven_cells WHERE row_id IN (SELECT id FROM coven_rows WHERE table_name='notes') ORDER BY column_id", [], |r| r.get(0)).unwrap());
    sql(&db, "INSERT INTO folders VALUES('Inbox')")
        .await
        .unwrap();
    assert_eq!(count(&db, "notes"), 1);
    assert!(losses(&db).is_empty());
    let restored: String = db.inspect_writer(|db| {
        db.query_row("SELECT folder FROM notes", [], |r| r.get(0))
            .unwrap()
    });
    assert_eq!(restored, "Inbox");
    assert_eq!(setters, db.inspect_writer(|db| db.query("SELECT write_id FROM coven_cells WHERE row_id IN (SELECT id FROM coven_rows WHERE table_name='notes') ORDER BY column_id", [], |r| r.get::<_, i64>(0)).unwrap()));
    db.close().await.unwrap();
}

#[tokio::test]
async fn deleted_note_43_nulls_link_6_but_takes_out_stale_children() {
    for (action, restored) in [
        ("SET NULL", true),
        ("CASCADE", false),
        ("RESTRICT", false),
        ("NO ACTION", false),
    ] {
        let store = TestStore::new();
        let schema = format!("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,note_id TEXT REFERENCES notes(id) ON DELETE {action}); CREATE TABLE counts(n INT); INSERT INTO counts VALUES(0); CREATE TRIGGER link_insert AFTER INSERT ON links BEGIN UPDATE counts SET n=n+1; END; CREATE TRIGGER link_delete AFTER DELETE ON links BEGIN UPDATE counts SET n=n-1; END;");
        let db = store
            .builder(
                vec![table("notes"), table("links")],
                vec![crate::Migration::run(1, "references", move |context| {
                    context.execute_batch(&schema)?;
                    Ok(())
                })],
            )
            .open()
            .await
            .unwrap();
        sql(
            &db,
            "INSERT INTO notes VALUES('43'); INSERT INTO links VALUES('6','43')",
        )
        .await
        .unwrap();
        remove(
            &db,
            "links",
            "6",
            &[],
            BTreeSet::from([Rule::ForeignKey(ForeignKey::new(
                ["note_id"],
                "notes",
                ["id"],
            ))]),
        );
        sql(&db, "DELETE FROM notes").await.unwrap();
        assert_eq!(count(&db, "links"), i64::from(restored), "{action}");
        if restored {
            let parent: Option<String> = db.inspect_writer(|db| {
                db.query_row("SELECT note_id FROM links", [], |r| r.get(0))
                    .unwrap()
            });
            assert_eq!(parent, None);
        } else {
            assert_eq!(
                losses(&db)[0].2,
                BTreeSet::from([Rule::ForeignKey(ForeignKey::new(
                    ["note_id"],
                    "notes",
                    ["id"]
                ))])
            );
            sql(&db, "INSERT INTO notes VALUES('43')").await.unwrap();
            assert_eq!(count(&db, "links"), 0);
            sql(&db, "INSERT INTO links VALUES('6','43')")
                .await
                .unwrap();
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
async fn every_final_check_and_foreign_key_reason_is_retained() {
    let store = TestStore::new();
    let db = store.schema(vec![table("lists"), table("todos")], "CREATE TABLE lists(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE todos(id TEXT NOT NULL PRIMARY KEY,list_id TEXT REFERENCES lists(id),start INT,end INT,CONSTRAINT ordered CHECK(start <= end),CONSTRAINT nonnegative CHECK(start >= 0))").await.unwrap();
    sql(
        &db,
        "INSERT INTO lists VALUES('Work'); INSERT INTO todos VALUES('7','Work',5,12)",
    )
    .await
    .unwrap();
    remove(
        &db,
        "todos",
        "7",
        &[("start", Value::Integer(-1)), ("end", Value::Integer(-2))],
        BTreeSet::from([Rule::Check("ordered".into())]),
    );
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
async fn a_default_can_name_an_absent_parent_by_its_other_unique_column() {
    let store = TestStore::new();
    let db = store.schema(vec![table("folders"), table("notes")], "CREATE TABLE folders(id TEXT NOT NULL PRIMARY KEY,name TEXT UNIQUE); CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,folder TEXT DEFAULT 'Inbox' REFERENCES folders(name) ON DELETE SET DEFAULT)").await.unwrap();
    sql(
        &db,
        "INSERT INTO folders VALUES('1','Work'); INSERT INTO notes VALUES('50','Work')",
    )
    .await
    .unwrap();
    remove(
        &db,
        "notes",
        "50",
        &[],
        BTreeSet::from([Rule::ForeignKey(ForeignKey::new(
            ["folder"],
            "folders",
            ["id"],
        ))]),
    );
    sql(&db, "DELETE FROM folders").await.unwrap();
    assert_eq!(
        losses(&db)[0].1["folder"].value,
        Value::Text("Inbox".into())
    );
    sql(&db, "INSERT INTO folders VALUES('2','Inbox')")
        .await
        .unwrap();
    assert_eq!(count(&db, "notes"), 1);
    assert!(losses(&db).is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn checks_keep_sqlite_affinity_and_quoted_expressions() {
    let store = TestStore::new();
    let db = store.schema(vec![table("ranges")], "CREATE TABLE ranges(id TEXT NOT NULL PRIMARY KEY, start TEXT, end TEXT, CONSTRAINT \"numeric test\" CHECK(CAST(ranges.start AS REAL) <= CAST(end AS REAL) + 0.25), CHECK(start < '9'))").await.unwrap();
    sql(&db, "INSERT INTO ranges VALUES('7','10','12')")
        .await
        .unwrap();
    remove(
        &db,
        "ranges",
        "7",
        &[("end", Value::Text("8".into()))],
        BTreeSet::from([Rule::Check("numeric test".into())]),
    );
    sql(&db, "SELECT 1").await.unwrap();
    assert_eq!(
        losses(&db)[0].2,
        BTreeSet::from([Rule::Check("numeric test".into())])
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn expression_partial_and_collated_unique_claims_use_sqlite_equality() {
    let store = TestStore::new();
    let db = store.schema(vec![table("notes")], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT,folder TEXT COLLATE NOCASE,active INTEGER,ignored INTEGER); CREATE UNIQUE INDEX claimed ON notes(lower(title) DESC,folder COLLATE RTRIM) WHERE active=1").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('45','Groceries','Work',1,0)")
        .await
        .unwrap();
    sql(&db, "INSERT INTO notes VALUES('46','Shopping','Work ',1,0); INSERT INTO notes VALUES('47',NULL,'Work',1,0)").await.unwrap();
    remove(
        &db,
        "notes",
        "46",
        &[("title", Value::Text("GROCERIES".into()))],
        BTreeSet::from([Rule::Unique(coven_merge::UniqueConstraint {
            terms: vec!["lower(title)".into(), "folder COLLATE RTRIM".into()],
            partial: Some("active=1".into()),
        })]),
    );
    sql(&db, "UPDATE notes SET ignored=1 WHERE id='45'")
        .await
        .unwrap();
    assert_eq!(
        losses(&db)[0].2,
        BTreeSet::from([Rule::Unique(coven_merge::UniqueConstraint {
            terms: vec!["lower(title)".into(), "folder COLLATE RTRIM".into()],
            partial: Some("active=1".into())
        })])
    );
    assert_eq!(count(&db, "notes"), 2);
    sql(&db, "UPDATE notes SET active=0 WHERE id='45'")
        .await
        .unwrap();
    assert!(losses(&db).is_empty());
    assert_eq!(count(&db, "notes"), 3);
    db.close().await.unwrap();
}

#[tokio::test]
async fn restoration_skips_shared_triggers_and_runs_local_triggers() {
    let store = TestStore::new();
    let db = store.schema(vec![table("notes").shared_trigger("edited")], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE,edited INT); CREATE TRIGGER edited AFTER INSERT ON notes WHEN NOT coven_applying() BEGIN UPDATE notes SET edited=edited+1 WHERE id=new.id; END; CREATE TABLE counts(n INT); INSERT INTO counts VALUES(0); CREATE TRIGGER counted_insert AFTER INSERT ON notes BEGIN UPDATE counts SET n=n+1; END; CREATE TRIGGER counted_delete AFTER DELETE ON notes BEGIN UPDATE counts SET n=n-1; END;").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('45','Groceries',0)")
        .await
        .unwrap();
    sql(&db, "INSERT INTO notes VALUES('46','Shopping',0)")
        .await
        .unwrap();
    remove(
        &db,
        "notes",
        "46",
        &[("title", Value::Text("Groceries".into()))],
        BTreeSet::from([Rule::Unique(["title"].into())]),
    );
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
        (
            "AFTER INSERT ON notes WHEN coven_applying()",
            "DELETE FROM notes WHERE id='45'",
        ),
        (
            "AFTER DELETE ON coven_lost",
            "DELETE FROM notes WHERE id='45'",
        ),
        (
            "AFTER UPDATE ON coven_lost",
            "UPDATE notes SET title='Changed' WHERE id='45'",
        ),
    ] {
        let store = TestStore::new();
        let db = store
            .schema(
                vec![table("notes")],
                if point=="AFTER UPDATE ON coven_lost" {
                    "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE,CHECK(id<>'46' OR title<>'Groceries'))"
                } else {
                    "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE)"
                },
            )
            .await
            .unwrap();
        sql(&db, "INSERT INTO notes VALUES('45','Groceries')")
            .await
            .unwrap();
        sql(&db, "INSERT INTO notes VALUES('46','Shopping')")
            .await
            .unwrap();
        remove(
            &db,
            "notes",
            "46",
            &[("title", Value::Text("Groceries".into()))],
            BTreeSet::from([Rule::Unique(["title"].into())]),
        );
        let snapshot = || {
            db.inspect_writer(|db| {
                [
                    "notes",
                    "coven_writes",
                    "coven_uploads",
                    "coven_rows",
                    "coven_cells",
                    "coven_lost",
                ]
                .iter()
                .map(|table| {
                    db.query(&format!("SELECT * FROM {table} ORDER BY rowid"), [], |r| {
                        (0..r.as_ref().column_count())
                            .map(|i| r.get::<_, rusqlite::types::Value>(i))
                            .collect::<rusqlite::Result<Vec<_>>>()
                    })
                    .unwrap()
                })
                .collect::<Vec<_>>()
            })
        };
        let before = snapshot();
        db.inspect_writer(|db| db.batch(&format!("CREATE TRIGGER coven_fail {point} BEGIN SELECT RAISE(ABORT,'materialization failed'); END")).unwrap());
        assert!(sql(&db, statement).await.is_err(), "{point}");
        assert_eq!(snapshot(), before, "{point}");
        db.inspect_writer(|db| db.batch("DROP TRIGGER coven_fail").unwrap());
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn taking_a_row_out_runs_foreign_key_actions_on_local_tables() {
    let store = TestStore::new();
    let db = store.schema(vec![table("notes")], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE,parent TEXT REFERENCES notes(id)); CREATE TABLE search_index(note_id TEXT REFERENCES notes(id) ON DELETE CASCADE); CREATE TABLE selection(note_id TEXT REFERENCES notes(id) ON DELETE SET NULL);").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('1','Ideas',NULL); INSERT INTO notes VALUES('2','Plan','1'); INSERT INTO search_index VALUES('1'); INSERT INTO selection VALUES('1')").await.unwrap();
    remove(
        &db,
        "notes",
        "2",
        &[],
        BTreeSet::from([Rule::ForeignKey(ForeignKey::new(
            ["parent"],
            "notes",
            ["id"],
        ))]),
    );
    sql(&db, "UPDATE notes SET title='Plan' WHERE id='1'")
        .await
        .unwrap();
    assert_eq!(count(&db, "notes"), 0);
    assert_eq!(count(&db, "search_index"), 0);
    assert_eq!(
        db.inspect_writer(|db| db
            .query_row("SELECT note_id FROM selection", [], |r| r
                .get::<_, Option<String>>(0))
            .unwrap()),
        None
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_trigger_cannot_silently_ignore_a_required_restoration() {
    let store = TestStore::new();
    let db = store.schema(vec![table("notes")], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE); CREATE TRIGGER refuse_restore BEFORE INSERT ON notes WHEN coven_applying() BEGIN SELECT RAISE(IGNORE); END;").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('45','Groceries')")
        .await
        .unwrap();
    sql(&db, "INSERT INTO notes VALUES('46','Shopping')")
        .await
        .unwrap();
    remove(
        &db,
        "notes",
        "46",
        &[("title", Value::Text("Groceries".into()))],
        BTreeSet::from([Rule::Unique(["title"].into())]),
    );
    assert!(matches!(
        sql(&db, "DELETE FROM notes WHERE id='45'").await,
        Err(crate::DbError::Sqlite(
            rusqlite::Error::StatementChangedRows(0)
        ))
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
    sql(
        &db,
        "INSERT INTO notes VALUES('42','store'); INSERT INTO attachments VALUES('8','42',5,12)",
    )
    .await
    .unwrap();
    remove(
        &db,
        "attachments",
        "8",
        &[("start", Value::Integer(10)), ("end", Value::Integer(8))],
        BTreeSet::from([Rule::Check("ordered".into())]),
    );
    sql(
        &db,
        "UPDATE notes SET audience='00000000-0000-4000-8000-00000000000a'",
    )
    .await
    .unwrap();
    let record = records(&db).pop().unwrap();
    assert_eq!(record.parts.len(), 2);
    assert!(record.parts.iter().all(|part| part.rows.len() == 2));
    assert_eq!(
        losses(&db)[0].2,
        BTreeSet::from([Rule::Check("ordered".into())])
    );
    let audience: String = db.inspect_writer(|db| {
        db.query_row("SELECT audience FROM coven_lost", [], |r| r.get(0))
            .unwrap()
    });
    assert_eq!(audience, "00000000-0000-4000-8000-00000000000a");
    assert_eq!(count(&db, "attachments"), 0);
    sql(
        &db,
        "UPDATE notes SET audience='store'; INSERT INTO attachments VALUES('8','42',10,20)",
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
        db.write(|context| {
            context.execute("INSERT INTO notes VALUES('1',?1,'Circle')", [ANA])?;
            Ok(())
        })
        .await
        .unwrap();
        remove(
            &db,
            "notes",
            "1",
            &[],
            BTreeSet::from([Rule::OtherAudience]),
        );
        db.write(move |context| {
            context.execute("INSERT INTO notes VALUES('1',?1,'Other')", [other])?;
            Ok(())
        })
        .await
        .unwrap();
        let winner: String = db.inspect_writer(|db| {
            db.query_row("SELECT audience FROM notes", [], |r| r.get(0))
                .unwrap()
        });
        assert_eq!(winner, if other == "store" { "store" } else { ANA });
        assert_eq!(losses(&db)[0].2, BTreeSet::from([Rule::OtherAudience]));
        if other == "store" {
            sql(&db, "DELETE FROM notes WHERE audience='store'")
                .await
                .unwrap();
            assert!(losses(&db).is_empty());
            let audience: String = db.inspect_writer(|db| {
                db.query_row("SELECT audience FROM notes", [], |r| r.get(0))
                    .unwrap()
            });
            assert_eq!(audience, ANA);
        } else {
            let deleted =
                coven_foundation::id_source::CircleId(uuid::Uuid::parse_str(ANA).unwrap());
            materialize_deleted(&db, "notes", &["1"], deleted).unwrap();
            assert_eq!(records(&db).len(), 2);
            assert_eq!(losses(&db)[0].2, BTreeSet::from([Rule::DeletedCircle]));
            let audience: String = db.inspect_writer(|db| {
                db.query_row("SELECT audience FROM notes", [], |r| r.get(0))
                    .unwrap()
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
    sql(
        &db,
        "INSERT INTO notes VALUES('1','Ideas',NULL); INSERT INTO notes VALUES('2','Plan','1')",
    )
    .await
    .unwrap();
    // Make note 1's claim later than note 2's, then retain both merged values.
    sql(&db, "UPDATE notes SET title='Later' WHERE id='1'")
        .await
        .unwrap();
    remove(
        &db,
        "notes",
        "2",
        &[],
        BTreeSet::from([Rule::ForeignKey(ForeignKey::new(
            ["parent"],
            "notes",
            ["id"],
        ))]),
    );
    sql(&db, "UPDATE notes SET title='Plan' WHERE id='1'")
        .await
        .unwrap();
    assert_eq!(count(&db, "notes"), 0);
    let removed = losses(&db);
    assert_eq!(removed.len(), 2);
    assert!(removed[0].2.iter().any(|r| matches!(r, Rule::Unique(_))));
    assert_eq!(
        removed[1].2,
        BTreeSet::from([Rule::ForeignKey(ForeignKey::new(
            ["parent"],
            "notes",
            ["id"]
        ))])
    );
    assert_eq!(records(&db)[2].parts[0].rows.len(), 1);
    sql(&db, "INSERT INTO notes VALUES('1','Plan',NULL)")
        .await
        .unwrap();
    assert_eq!(count(&db, "notes"), 0);
    assert_eq!(losses(&db).len(), 2);
    assert!(matches!(
        records(&db)[3].parts[0].rows[0].change.operation,
        Operation::Update(_)
    ));
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_removed_defaults_former_unique_value_does_not_name_its_updated_row() {
    let store = TestStore::new();
    let db=store.schema(vec![table("folders"),table("notes")],"CREATE TABLE folders(id TEXT NOT NULL PRIMARY KEY,name TEXT UNIQUE); CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,folder TEXT DEFAULT 'Inbox' REFERENCES folders(name) ON DELETE SET DEFAULT)").await.unwrap();
    sql(&db,"INSERT INTO folders VALUES('1','Work'),('2','Inbox'); INSERT INTO notes VALUES('50','Work')").await.unwrap();
    remove(
        &db,
        "notes",
        "50",
        &[],
        [Rule::ForeignKey(ForeignKey::new(
            ["folder"],
            "folders",
            ["id"],
        ))]
        .into(),
    );
    remove(
        &db,
        "folders",
        "2",
        &[],
        [Rule::Unique(["name"].into())].into(),
    );
    sql(
        &db,
        "INSERT INTO folders VALUES('2','Other'); DELETE FROM folders WHERE id='1'",
    )
    .await
    .unwrap();
    assert_eq!(count(&db, "notes"), 0);
    assert_eq!(
        losses(&db)[0].1["folder"].value,
        Value::Text("Inbox".into())
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn unique_constraint_identity_preserves_repeated_columns() {
    let store = TestStore::new();
    let db=store.schema(vec![table("notes")],"CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT,edited INT,UNIQUE(title,title))").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('45','Groceries',0)")
        .await
        .unwrap();
    sql(&db, "INSERT INTO notes VALUES('46','Shopping',0)")
        .await
        .unwrap();
    remove(
        &db,
        "notes",
        "46",
        &[("title", Value::Text("Groceries".into()))],
        [Rule::Unique(["title", "title"].into())].into(),
    );
    sql(&db, "UPDATE notes SET edited=1 WHERE id='45'")
        .await
        .unwrap();
    assert_eq!(
        losses(&db)[0].2,
        BTreeSet::from([Rule::Unique(["title", "title"].into())])
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn unique_indexes_with_the_same_columns_keep_each_claim() {
    let store = TestStore::new();
    let db=store.schema(vec![table("notes")],"CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT,parent TEXT REFERENCES notes(id),edited INT); CREATE UNIQUE INDEX whole_title ON notes(lower(title)); CREATE UNIQUE INDEX initial ON notes(lower(substr(title,1,1)))").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('45','alpha',NULL,0)")
        .await
        .unwrap();
    sql(&db, "INSERT INTO notes VALUES('46','beta','45',0)")
        .await
        .unwrap();
    remove(
        &db,
        "notes",
        "46",
        &[("title", Value::Text("APPLE".into()))],
        [Rule::Unique(["lower(substr(title,1,1))"].into())].into(),
    );
    sql(&db, "UPDATE notes SET edited=1 WHERE id='45'")
        .await
        .unwrap();
    assert_eq!(count(&db, "notes"), 1);
    assert_eq!(
        losses(&db)[0].2,
        BTreeSet::from([Rule::Unique(["lower(substr(title,1,1))"].into())])
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_constant_unique_index_keeps_its_expression_term() {
    let store = TestStore::new();
    let db=store.schema(vec![table("notes")],"CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE UNIQUE INDEX one_note ON notes((1))").await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('45')").await.unwrap();
    remove(
        &db,
        "notes",
        "45",
        &[],
        [Rule::Unique(["(1)"].into())].into(),
    );
    sql(&db, "INSERT INTO notes VALUES('46')").await.unwrap();
    assert_eq!(count(&db, "notes"), 1);
    assert_eq!(
        db.inspect_writer(|db| db
            .query_row("SELECT id FROM notes", [], |r| r.get::<_, String>(0))
            .unwrap()),
        "45"
    );
    assert_eq!(
        losses(&db)[0].2,
        BTreeSet::from([Rule::Unique(["(1)"].into())])
    );
    db.close().await.unwrap();
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
            let deleted = [circle].into();
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
            crate::removal::materialize(db, schema, &app, &view, &result)
        })
    })
}
