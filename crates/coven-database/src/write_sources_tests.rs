use super::*;
use crate::{
    tests::TestStore,
    write::tests::{notes, records, sql, NOTES},
    ApplyOutcome,
};
use coven_format::write::WriteRecord;
use coven_merge::WritePast;
use serde_json::json;
use std::collections::BTreeSet;

#[tokio::test]
async fn captures_each_actual_setter_and_keeps_empty_delete_sources() {
    let author_store = TestStore::new();
    let db = author_store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','first','body')")
        .await
        .unwrap();
    sql(&db, "UPDATE notes SET title='second'").await.unwrap();
    sql(&db, "UPDATE notes SET title='third',body='new'")
        .await
        .unwrap();
    sql(&db, "DELETE FROM notes").await.unwrap();
    let records = records(&db);
    let first = &records[0];
    db.inspect_writer(|db| {
        let judgments = db
            .query("SELECT admission FROM _coven_authored_changes", [], |r| {
                r.get::<_, Option<Vec<u8>>>(0)
            })
            .unwrap();
        assert!(judgments.iter().all(|a| a.as_deref() == Some(&[][..])));
    });
    assert!(first.parts[0].rows[0].sources.changes().next().is_none());
    let third = &records[2].parts[0].rows[0];
    assert_eq!(
        third.sources.generation.as_ref().unwrap().write,
        first.header.position
    );
    assert_eq!(
        third.sources.setters["body"].change.write,
        first.header.position
    );
    assert_eq!(
        third.sources.setters["title"].change.write,
        records[1].header.position
    );
    let delete = &records[3].parts[0].rows[0];
    assert_eq!(delete.sources.setters.len(), 3);
    let receiver_store = TestStore::new();
    let receiver = receiver_store.schema(notes(), NOTES).await.unwrap();
    for record in &records[..3] {
        assert_eq!(
            receiver.receive(record.clone().into()).await.unwrap(),
            ApplyOutcome::Applied
        );
    }
    let mut deletion = records[3].clone();
    deletion.parts[0].rows[0].old.clear();
    assert_eq!(
        receiver.receive(deletion.clone().into()).await.unwrap(),
        ApplyOutcome::Applied
    );
    receiver.inspect_writer(|db| {
        let id = ChangeId {
            write: deletion.header.position,
            row: deletion.parts[0].rows[0].row.clone(),
        };
        let retained = evidence(db, &id).unwrap();
        assert!(retained.original.old.is_empty());
        assert_eq!(retained.original.sources, delete.sources);
    });
    receiver.close().await.unwrap();
    db.close().await.unwrap();
}

async fn clocked(store: &TestStore, milliseconds: u64) -> crate::Database {
    store
        .builder(notes(), vec![crate::Migration::sql(1, "notes", NOTES)])
        .clock(std::sync::Arc::new(
            coven_foundation::clock::FixedClock::new(
                std::time::UNIX_EPOCH + std::time::Duration::from_millis(milliseconds),
            ),
        ))
        .open()
        .await
        .unwrap()
}

#[tokio::test]
async fn author_source_survives_a_different_canonical_generation_winner() {
    let ids = coven_foundation::id_source::SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let c_store = TestStore::with_ids(&ids);
    let a = clocked(&a_store, 1).await;
    let b = clocked(&b_store, 2).await;
    let c = clocked(&c_store, 3).await;
    for db in [&a, &b] {
        sql(db, "INSERT INTO notes VALUES('n','equal','equal')")
            .await
            .unwrap();
    }
    sql(&b, "UPDATE notes SET title='replacement'")
        .await
        .unwrap();
    let early = records(&a).remove(0);
    let later = records(&b);
    for record in [&early, &later[0], &later[1]] {
        assert_eq!(
            c.receive(record.clone().into()).await.unwrap(),
            ApplyOutcome::Applied
        );
    }
    let source = later[1].parts[0].rows[0]
        .sources
        .generation
        .as_ref()
        .unwrap();
    assert_eq!(source.write, later[0].header.position);
    c.inspect_writer_schema(|db, schema| {
        let app = crate::write_rows::AppView::after(db, schema);
        let state = MergeStore::new(db, &app).row(&source.row).unwrap().state;
        assert_eq!(state.generations()[&1], early.header.position);
        assert_eq!(state.cells()["title"].write, later[1].header.position);
        assert_eq!(
            evidence(db, source).unwrap().original,
            later[0].parts[0].rows[0]
        );
    });
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn equal_scalars_keep_distinct_setters() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','equal','equal')")
        .await
        .unwrap();
    sql(&db, "UPDATE notes SET title='different'")
        .await
        .unwrap();
    sql(&db, "UPDATE notes SET title='equal'").await.unwrap();
    sql(&db, "UPDATE notes SET title='next',body='next'")
        .await
        .unwrap();
    let history = records(&db);
    let row = &history[3].parts[0].rows[0];
    assert_eq!(row.old["title"], row.old["body"]);
    assert_eq!(
        row.sources.setters["title"].change.write,
        history[2].header.position
    );
    assert_eq!(
        row.sources.setters["body"].change.write,
        history[0].header.position
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn missing_originals_wait_even_when_the_position_was_consumed() {
    let author_store = TestStore::new();
    let receiver_store = TestStore::new();
    let author = author_store.schema(notes(), NOTES).await.unwrap();
    let receiver = receiver_store.schema(notes(), NOTES).await.unwrap();
    sql(&author, "INSERT INTO notes VALUES('n','one','body')")
        .await
        .unwrap();
    sql(&author, "UPDATE notes SET title='two'").await.unwrap();
    let history = records(&author);
    receiver
        .receive(crate::DownloadedWrite {
            header: history[0].header.clone(),
            parts: vec![crate::DownloadedPart::Skipped(coven_merge::Audience::Store)],
        })
        .await
        .unwrap();
    let before = crate::tests::contents(&receiver);
    let outcome = receiver.receive(history[1].clone().into()).await.unwrap();
    assert_eq!(
        outcome,
        ApplyOutcome::Waiting(crate::WriteWait::Sources(vec![history[1].parts[0].rows[0]
            .sources
            .generation
            .clone()
            .unwrap()]))
    );
    assert_eq!(crate::tests::contents(&receiver), before);
    for db in [author, receiver] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn forged_roles_refuse_without_consumption_or_partial_rows() {
    let author_store = TestStore::new();
    let author = author_store.schema(notes(), NOTES).await.unwrap();
    sql(&author, "INSERT INTO notes VALUES('n','same','same')")
        .await
        .unwrap();
    sql(&author, "UPDATE notes SET title='two'").await.unwrap();
    sql(&author, "UPDATE notes SET title='three'")
        .await
        .unwrap();
    let history = records(&author);
    for forgery in 0..6 {
        let store = TestStore::new();
        let receiver = store.schema(notes(), NOTES).await.unwrap();
        for record in &history[..2] {
            receiver.receive(record.clone().into()).await.unwrap();
        }
        let mut record = history[2].clone();
        let row = &mut record.parts[0].rows[0];
        match forgery {
            0 => row.sources.generation = None,
            1 => row.sources.generation.as_mut().unwrap().write = history[1].header.position,
            2 => row.sources.setters.get_mut("title").unwrap().column = "body".into(),
            3 => row.sources.generation.as_mut().unwrap().row.table = "absent".into(),
            4 => {
                row.sources.setters.get_mut("title").unwrap().change.write = record.header.position
            }
            5 => record.header.timestamp = history[0].header.timestamp,
            _ => unreachable!(),
        }
        let before = crate::tests::contents(&receiver);
        let error = receiver.receive(record.into()).await.unwrap_err();
        assert!(
            matches!(
                error,
                DbError::WriteFormat(_) | DbError::InvalidWrite { .. }
            ),
            "{forgery}: {error:?}"
        );
        assert_eq!(crate::tests::contents(&receiver), before, "{forgery}");
        receiver.close().await.unwrap();
    }
    author.close().await.unwrap();
}

#[tokio::test]
async fn schema_renames_preserve_original_sources_and_authored_records() {
    let ids = coven_foundation::id_source::SequentialIds::new();
    let store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','first','body')")
        .await
        .unwrap();
    sql(&db, "UPDATE notes SET title='second'").await.unwrap();
    let original = records(&db);
    let receiver = receiver_store.schema(notes(), NOTES).await.unwrap();
    for record in &original {
        receiver.receive(record.clone().into()).await.unwrap();
    }
    receiver.close().await.unwrap();
    db.close().await.unwrap();
    let tables = || {
        vec![crate::SyncedTable::new(
            "renamed",
            crate::RowIdentity::SharedKey,
        )]
    };
    let migrations = || {
        vec![
        crate::Migration::sql(1,"notes",NOTES),
        crate::Migration::sql(2,"rename","ALTER TABLE notes RENAME TO renamed; ALTER TABLE renamed RENAME COLUMN title TO name")
            .writes(|r| { r.table = "renamed".into(); r.rename_column("title","name"); Ok(()) }),
    ]
    };
    let db = store.builder(tables(), migrations()).open().await.unwrap();
    let receiver = receiver_store
        .builder(tables(), migrations())
        .open()
        .await
        .unwrap();
    sql(&db, "UPDATE renamed SET name='third'").await.unwrap();
    let history = records(&db);
    let row = &history.last().unwrap().parts[0].rows[0];
    assert_eq!(row.row.table, "renamed");
    assert_eq!(row.sources.generation.as_ref().unwrap().row.table, "notes");
    let setter = &row.sources.setters["name"];
    assert_eq!(setter.column, "title");
    assert_eq!(setter.change.write, original[1].header.position);
    assert_eq!(setter.change.row, original[1].parts[0].rows[0].row);
    db.inspect_writer(|db| {
        assert_eq!(
            evidence(db, &setter.change).unwrap().original,
            original[1].parts[0].rows[0]
        );
        let uploads = db
            .query(
                "SELECT record FROM _coven_uploads ORDER BY number",
                [],
                |r| r.get::<_, Vec<u8>>(0),
            )
            .unwrap();
        for (bytes, original) in uploads.iter().zip(&original) {
            assert_eq!(
                coven_format::write_stream::decode_plaintext(bytes).unwrap(),
                *original
            );
        }
    });
    let bytes = coven_format::test_utils::write_plaintext(history.last().unwrap()).unwrap();
    assert_eq!(
        coven_format::write_stream::decode_plaintext(&bytes).unwrap(),
        *history.last().unwrap()
    );
    let transmitted = db.inspect_writer(|db| {
        db.query(
            "SELECT record FROM _coven_uploads ORDER BY number",
            [],
            |r| r.get::<_, Vec<u8>>(0),
        )
        .unwrap()
    });
    for bytes in transmitted.iter().skip(original.len()) {
        let record = coven_format::write_stream::decode_plaintext(bytes).unwrap();
        assert_eq!(
            receiver.receive(record.into()).await.unwrap(),
            ApplyOutcome::Applied
        );
    }
    assert_eq!(
        receiver
            .read(|db| Ok(db.query_row("SELECT name FROM renamed", [], |r| r.get::<_, String>(0))?))
            .await
            .unwrap(),
        "third"
    );
    receiver.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn old_incarnation_setters_and_nonexistent_rows_refuse_before_consumption() {
    let ids = coven_foundation::id_source::SequentialIds::new();
    let store = TestStore::with_ids(&ids);
    let target_store = TestStore::with_ids(&ids);
    let db = store.schema(notes(), NOTES).await.unwrap();
    let target = target_store.schema(notes(), NOTES).await.unwrap();
    for statement in [
        "INSERT INTO notes VALUES('n','same','same')",
        "DELETE FROM notes",
        "INSERT INTO notes VALUES('n','same','same')",
        "UPDATE notes SET title='next'",
    ] {
        sql(&db, statement).await.unwrap();
    }
    let history = records(&db);
    for record in &history[..3] {
        target.receive(record.clone().into()).await.unwrap();
    }
    for damage in 0..3 {
        let mut record = history[3].clone();
        let source = record.parts[0].rows[0]
            .sources
            .setters
            .get_mut("title")
            .unwrap();
        match damage {
            0 => source.change.write = history[0].header.position,
            1 => source.change.write = history[1].header.position,
            2 => {
                source.change.row.key =
                    coven_format::key::encode_key(&[Value::Text("absent".into())]).unwrap()
            }
            _ => unreachable!(),
        }
        let before = crate::tests::contents(&target);
        assert!(
            target.receive(record.into()).await.is_err(),
            "damage {damage}"
        );
        assert_eq!(crate::tests::contents(&target), before);
    }
    for db in [db, target] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn frozen_display_and_dismissal_preserve_original_parents_sources_and_admission() {
    let ids = coven_foundation::id_source::SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let target_store = TestStore::with_ids(&ids);
    let tables = || {
        vec![
            crate::SyncedTable::new("p", crate::RowIdentity::SharedKey),
            crate::SyncedTable::new("c", crate::RowIdentity::SharedKey),
        ]
    };
    let schema = "CREATE TABLE p(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE c(id TEXT NOT NULL PRIMARY KEY,p TEXT REFERENCES p(id))";
    let source = source_store.schema(tables(), schema).await.unwrap();
    let target = target_store.schema(tables(), schema).await.unwrap();
    sql(
        &source,
        "INSERT INTO p VALUES('p'); INSERT INTO c VALUES('c','p')",
    )
    .await
    .unwrap();
    let mut write = records(&source).remove(0);
    write.header.disposition = coven_format::write::WriteDisposition::Lost(2);
    target.receive(write.clone().into()).await.unwrap();
    let original = write.parts[0]
        .rows
        .iter()
        .find(|r| r.row.table == "c")
        .unwrap();
    let id = ChangeId {
        write: write.header.position,
        row: original.row.clone(),
    };
    assert!(!match &original.change.operation {
        Operation::Insert(c) => &c["p"].parents,
        _ => unreachable!(),
    }
    .is_empty());
    target.inspect_writer(|db| {
        let losses = db
            .query(crate::loss_record::SELECT, [], crate::loss_record::read)
            .unwrap();
        for loss in losses {
            let coven_format::loss::LossValues::Row(cells) = loss.values else {
                panic!("row loss")
            };
            assert!(cells.values().all(|c| c.value.parents.is_empty()));
        }
    });
    let before = target.inspect_writer(|db| {
        db.query(
            "SELECT record,admission FROM _coven_authored_changes ORDER BY table_name,key",
            [],
            |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?)),
        )
        .unwrap()
    });
    target
        .dismiss_lost_values(&target.lost_values().await.unwrap())
        .await
        .unwrap();
    assert!(target.lost_values().await.unwrap().is_empty());
    assert_eq!(
        target.receive(write.clone().into()).await.unwrap(),
        ApplyOutcome::AlreadyApplied
    );
    target.inspect_writer(|db| {
        assert_eq!(evidence(db, &id).unwrap().original, *original);
        assert_eq!(
            db.query(
                "SELECT record,admission FROM _coven_authored_changes ORDER BY table_name,key",
                [],
                |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?))
            )
            .unwrap(),
            before
        );
        for (_, cause) in before {
            assert_eq!(
                merge_fields::decode_lost_write_cause(&cause).unwrap(),
                LostWriteCause::SchemaChange(2)
            );
        }
    });
    for db in [source, target] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn covered_source_streams_authenticate_atomically_and_refuse_conflicting_copies() {
    let source_store = TestStore::new();
    let target_store = TestStore::new();
    let source = source_store.schema(notes(), NOTES).await.unwrap();
    let target = target_store.schema(notes(), NOTES).await.unwrap();
    sql(
        &source,
        "INSERT INTO notes VALUES('a','one',''),('b','two','')",
    )
    .await
    .unwrap();
    sql(&source, "UPDATE notes SET title='changed'")
        .await
        .unwrap();
    let writes = records(&source);
    target
        .receive(crate::DownloadedWrite {
            header: writes[0].header.clone(),
            parts: vec![crate::DownloadedPart::Skipped(coven_merge::Audience::Store)],
        })
        .await
        .unwrap();
    let before = crate::tests::contents(&target);
    let stream = || crate::snapshot_write::tests::stream(&writes[0]);
    assert!(target
        .receive_stream(
            stream(),
            coven_format::value::EntryPositions(vec![]),
            || Err(DbError::DamagedDatabase)
        )
        .await
        .is_err());
    assert_eq!(crate::tests::contents(&target), before);
    let update = || crate::snapshot_write::tests::stream(&writes[1]);
    assert!(matches!(
        target
            .receive_stream(
                update(),
                coven_format::value::EntryPositions(vec![]),
                || Ok(())
            )
            .await
            .unwrap(),
        ApplyOutcome::Waiting(crate::WriteWait::Sources(_))
    ));
    assert_eq!(crate::tests::contents(&target), before);
    assert_eq!(
        target
            .receive_stream(
                stream(),
                coven_format::value::EntryPositions(vec![]),
                || Ok(())
            )
            .await
            .unwrap(),
        ApplyOutcome::AlreadyApplied
    );
    assert_eq!(crate::write::tests::count(&target, "notes"), 0);
    target.inspect_writer(|db| {
        let unknown: i64 = db
            .query_row(
                "SELECT count(*) FROM _coven_authored_changes WHERE admission IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(unknown, 2);
    });
    let retained = crate::tests::contents(&target);
    for damage in 0..3 {
        let mut record = writes[0].clone();
        match damage {
            0 => {
                record.header.timestamp = coven_merge::Timestamp::new(
                    record.header.timestamp.milliseconds() + 1,
                    0,
                    record.header.position.device,
                )
                .unwrap()
            }
            1 => {
                record.parts[0].rows[0].row.key =
                    coven_format::key::encode_key(&[Value::Text("0".into())]).unwrap()
            }
            2 => {
                let Operation::Insert(columns) = &mut record.parts[0].rows[0].change.operation
                else {
                    unreachable!()
                };
                columns.get_mut("title").unwrap().value = Value::Text("forged".into());
            }
            _ => unreachable!(),
        }
        assert!(target.receive(record.into()).await.is_err());
        assert_eq!(crate::tests::contents(&target), retained);
    }
    for db in [source, target] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_schema_default_has_no_setter_until_a_write_sets_the_cell() {
    let ids = coven_foundation::id_source::SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let author_store = TestStore::with_ids(&ids);
    let source = source_store.schema(notes(), NOTES).await.unwrap();
    let author = author_store
        .builder(
            notes(),
            vec![
                crate::Migration::sql(1, "notes", NOTES),
                crate::Migration::sql(
                    2,
                    "color",
                    "ALTER TABLE notes ADD COLUMN color TEXT NOT NULL DEFAULT 'blue'",
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    sql(&source, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    let first = records(&source).remove(0);
    author.receive(first.clone().into()).await.unwrap();
    author.inspect_writer(|db| {
        assert_eq!(
            crate::source_names::table(db, 2, "notes", 1).unwrap(),
            Some("notes".into())
        );
        assert_eq!(
            crate::source_names::column(db, 2, "notes", "title", 1, "notes").unwrap(),
            Some("title".into())
        );
    });
    sql(&author, "UPDATE notes SET color='red'").await.unwrap();
    let update = records(&author).pop().unwrap();
    assert!(update.parts[0].rows[0].sources.setters.is_empty());
    assert_eq!(
        update.parts[0].rows[0]
            .sources
            .generation
            .as_ref()
            .unwrap()
            .write,
        first.header.position
    );
    sql(&author, "DELETE FROM notes").await.unwrap();
    let delete = records(&author).pop().unwrap();
    assert_eq!(
        delete.parts[0].rows[0].sources.setters["color"]
            .change
            .write,
        update.header.position
    );
    for db in [source, author] {
        db.close().await.unwrap();
    }
}

fn input(history: &[WriteRecord]) -> serde_json::Value {
    let rows: Vec<_> = history
        .iter()
        .flat_map(|w| &w.parts)
        .flat_map(|p| &p.rows)
        .map(|r| r.row.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let columns = ["body", "id", "title"];
    let write_id = |id| {
        history
            .iter()
            .position(|w| w.header.position == id)
            .unwrap()
    };
    let row_id = |row: &RowId| rows.iter().position(|r| r == row).unwrap();
    let column_id = |name: &str| columns.iter().position(|n| *n == name).unwrap();
    let writes: Vec<_> = history.iter().enumerate().map(|(index,w)| {
        let past = w.header.had_read.causal_past(w.header.position);
        let changes: Vec<_> = w.parts.iter().flat_map(|p| &p.rows).map(|row| {
            let (kind,columns) = match &row.change.operation {
                Operation::Insert(c) => (0,c.keys().map(|c| column_id(c)).collect::<Vec<_>>()),
                Operation::Update(c) => (1,c.keys().map(|c| column_id(c)).collect()),
                Operation::Delete => (2,vec![]),
            };
            for source in row.sources.changes() { assert_eq!(source.row,row.row); }
            for (name,source) in &row.sources.setters { assert_eq!(name,&source.column); }
            json!({"row":row_id(&row.row),"kind":kind,"generation":row.change.generation,
                "columns":columns,"source":row.sources.generation.as_ref().map(|s| write_id(s.write)),
                "setters":row.sources.setters.iter().map(|(c,s)| [column_id(c),write_id(s.change.write)]).collect::<Vec<_>>()})
        }).collect();
        // The history is sorted by the real timestamp; this is its order-preserving rank.
        json!({"timestamp":index,"past":history.iter().enumerate().filter(|(_,a)|past.contains(&a.header.position)).map(|(i,_)|i).collect::<Vec<_>>(),"changes":changes})
    }).collect();
    json!({"rows":rows.len(),"columns":columns.len(),"writes":writes})
}

#[tokio::test]
#[ignore = "requires COVEN_AUTHORED_LEAN; scripts/check.sh builds authoredRunner"]
async fn authored_sources_differential() {
    let runner = std::env::var_os("COVEN_AUTHORED_LEAN").expect("built authoredRunner");
    let ids = coven_foundation::id_source::SequentialIds::new();
    for seed in 0..24 {
        let mut random = crate::test_utils::tests::Generator::new(seed);
        let stores: Vec<_> = (0..3).map(|_| TestStore::with_ids(&ids)).collect();
        let mut databases = Vec::new();
        for store in &stores {
            databases.push(store.schema(notes(), NOTES).await.unwrap());
        }
        let mut history: Vec<WriteRecord> = Vec::new();
        for step in 0..24 {
            let author = random.pick(databases.len());
            let db = &databases[author];
            if random.pick(3) == 0 {
                for record in &history {
                    assert!(matches!(
                        db.receive(record.clone().into()).await.unwrap(),
                        crate::ApplyOutcome::Applied | crate::ApplyOutcome::AlreadyApplied
                    ));
                }
            }
            let row = random.pick(3).to_string();
            let key = row.clone();
            let present = db
                .read(move |sql| {
                    Ok(sql.query_row(
                        "SELECT EXISTS(SELECT 1 FROM notes WHERE id=?1)",
                        [key],
                        |r| r.get::<_, bool>(0),
                    )?)
                })
                .await
                .unwrap();
            let operation = random.pick(5);
            let statement = if !present {
                format!("INSERT INTO notes VALUES('{row}','equal','equal')")
            } else if operation == 0 {
                format!("DELETE FROM notes WHERE id='{row}'")
            } else if operation == 1 {
                format!("UPDATE notes SET title='title-{step}',body='body-{step}' WHERE id='{row}'")
            } else {
                format!(
                    "UPDATE notes SET {}='value-{step}' WHERE id='{row}'",
                    if operation == 2 { "body" } else { "title" }
                )
            };
            db.write(move |sql| {
                sql.execute_batch(&statement)?;
                Ok(())
            })
            .await
            .unwrap();
            let mut record = records(db).pop().unwrap();
            // Extract evidence from the committed original, rather than a second
            // implementation of the source-capture algorithm in the generator.
            db.inspect_writer(|db| {
                for row in record.parts.iter_mut().flat_map(|p| &mut p.rows) {
                    let retained = evidence(
                        db,
                        &ChangeId {
                            write: record.header.position,
                            row: row.row.clone(),
                        },
                    )
                    .unwrap();
                    assert_eq!(retained.header, record.header);
                    assert_eq!(&retained.original, row);
                    *row = retained.original;
                }
            });
            history.push(record);
            history.sort_by_key(|w| w.header.timestamp);
        }
        let model_input = input(&history);
        let model = crate::tests::lean_model(&runner, model_input.clone());
        assert_eq!(model["authored"], true, "seed {seed}: {model_input}");
        assert_eq!(model["dataValid"], true, "seed {seed}");
        for expected in model["expected"].as_array().unwrap() {
            let w = expected["write"].as_u64().unwrap() as usize;
            let row = model_input["writes"][w]["changes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|c| c["row"] == expected["row"])
                .unwrap();
            assert_eq!(row["source"], expected["source"], "seed {seed}, write {w}");
            assert_eq!(
                row["setters"], expected["setters"],
                "seed {seed}, write {w}"
            );
            let mut roles = row["source"].as_u64().into_iter().collect::<Vec<_>>();
            roles.extend(
                row["setters"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|s| s[1].as_u64().unwrap()),
            );
            assert_eq!(json!(roles), expected["roles"]);
        }
        // Both models check actual supplied evidence. A self-source is neither
        // earlier nor read; no receiver may consume that altered record.
        let index = history
            .iter()
            .position(|w| {
                w.parts
                    .iter()
                    .flat_map(|p| &p.rows)
                    .any(|r| r.sources.generation.is_some())
            })
            .unwrap();
        let mut forged = history.clone();
        let write = forged[index].header.position;
        forged[index].parts[0].rows[0]
            .sources
            .generation
            .as_mut()
            .unwrap()
            .write = write;
        let refused = crate::tests::lean_model(&runner, input(&forged));
        assert_eq!(refused["authored"], false);
        assert_eq!(refused["dataValid"], false);
        databases[0].inspect_writer(|db| {
            assert!(validate(db, &forged[index].header, &forged[index].parts[0].rows[0]).is_err());
        });
        for db in databases {
            db.close().await.unwrap();
        }
    }
}
