use crate::tests::TestStore;
use crate::write::tests::{count, records, sql};
use crate::{Database, RowIdentity, SyncedTable};
use coven_format::value::Value;
use coven_foundation::id_source::SequentialIds;
use coven_merge::{Audience, RowId};

async fn open(store: &TestStore, seconds: u64) -> Database {
    store.builder(tables(), vec![crate::Migration::addition_with(1, "references", move |sql| {
        sql.execute_batch("CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE SET NULL)")?;
        Ok(())
    })]).clock(std::sync::Arc::new(coven_foundation::clock::FixedClock::new(std::time::UNIX_EPOCH+std::time::Duration::from_secs(seconds)))).open().await.unwrap()
}

fn tables() -> Vec<SyncedTable> {
    ["parents", "children"]
        .into_iter()
        .map(|name| SyncedTable::new(name, RowIdentity::SharedKey))
        .collect()
}

async fn reference(db: &Database, lost: bool) -> Option<String> {
    if lost {
        return db
            .lost_values()
            .await
            .unwrap()
            .into_iter()
            .find_map(|loss| match loss.lost {
                crate::Lost::Cell(cell) if cell.column == "parent" => Some(match cell.value {
                    crate::types::Value::Null => None,
                    crate::types::Value::Text(text) => Some(text),
                    other => panic!("unexpected reference {other:?}"),
                }),
                _ => None,
            })
            .expect("lost reference");
    }
    db.read(|sql| Ok(sql.query_row("SELECT parent FROM children", [], |r| r.get(0))?))
        .await
        .unwrap()
}

#[tokio::test]
async fn restored_parents_restore_live_references_while_losses_keep_written_values() {
    for (lost, cleared) in [(false, false), (true, false), (false, true)] {
        let ids = SequentialIds::new();
        let a_store = TestStore::with_ids(&ids);
        let b_store = TestStore::with_ids(&ids);
        let a = open(&a_store, 1).await;
        let b = open(&b_store, 2).await;
        sql(&a, "INSERT INTO parents VALUES('parent'),('Inbox')")
            .await
            .unwrap();
        b.receive(records(&a).remove(0).into()).await.unwrap();
        sql(&b, "INSERT INTO children VALUES('child','parent')")
            .await
            .unwrap();
        if lost {
            let c_store = TestStore::with_ids(&ids);
            let c = open(&c_store, 3).await;
            c.receive(records(&a).remove(0).into()).await.unwrap();
            sql(&c, "INSERT INTO children VALUES('child','Inbox')")
                .await
                .unwrap();
            b.receive(records(&c).remove(0).into()).await.unwrap();
            c.close().await.unwrap();
        }
        // Save only the parent's input metadata for the reset fixture. The
        // production recompute below owns all changes to the child.
        let parent = RowId {
            table: "parents".into(),
            key: coven_format::key::encode_key(&[Value::Text("parent".into())]).unwrap(),
            audience: Audience::Store,
        };
        let cells = b.inspect_writer(|db| db.query("SELECT c.column_id,c.row_id,c.write_id FROM _coven_cells c JOIN _coven_rows r ON r.id=c.row_id WHERE r.table_name='parents' AND r.key=?1", [&parent.key], |r| Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?))).unwrap());
        sql(&a, "DELETE FROM parents WHERE id='parent'")
            .await
            .unwrap();
        b.receive(records(&a).remove(1).into()).await.unwrap();
        assert_eq!(
            reference(&b, lost).await,
            if lost { Some("parent".into()) } else { None }
        );
        b.inspect_writer_schema(|db, schema| {
            let app = crate::write_rows::AppView::after(db, schema);
            let merge = crate::merge_store::MergeStore::new(db, &app);
            let child = RowId {
                table: "children".into(),
                key: coven_format::key::encode_key(&[Value::Text("child".into())]).unwrap(),
                audience: Audience::Store,
            };
            let state = merge.row(&child).unwrap().state;
            let written = if lost {
                &state
                    .lost()
                    .iter()
                    .find(|(key, _)| key.column == "parent")
                    .unwrap()
                    .1
                    .value
            } else {
                &state.cells()["parent"].value
            };
            assert_eq!(written.value, Value::Text("parent".into()));
            assert_eq!(written.parents.values().next().unwrap().generation, 1);
        });
        if cleared {
            let queued = records(&b).len();
            sql(&b, "UPDATE children SET id=id").await.unwrap();
            assert_eq!(
                records(&b).len(),
                queued,
                "an unrelated no-op retains the link"
            );
            sql(&b, "UPDATE children SET parent=NULL").await.unwrap();
            let writes = records(&b);
            assert_eq!(
                writes.len(),
                queued + 1,
                "explicit NULL must clear the written link"
            );
            let change = &writes.last().unwrap().parts[0].rows[0];
            assert_eq!(change.old["parent"], Value::Text("parent".into()));
            let coven_merge::Operation::Update(columns) = &change.change.operation else {
                panic!("clearing the link is an update");
            };
            assert_eq!(columns["parent"].value, Value::Null);
            assert!(columns["parent"].parents.is_empty());
            sql(&b, "UPDATE children SET parent=NULL").await.unwrap();
            assert_eq!(
                records(&b).len(),
                queued + 1,
                "an ordinary NULL no-op records nothing"
            );
        }
        b.close().await.unwrap();
        let b = open(&b_store, 2).await;
        b.inspect_writer_schema(|db, schema| {
            db.transaction(|db| {
                db.internal_execute("DELETE FROM _coven_rows WHERE table_name='parents' AND key=?1 AND generation=?2", crate::params![parent.key, 2u64.to_be_bytes().as_slice()])?;
                db.materialize(|db| { db.internal_execute("INSERT INTO parents VALUES('parent')", [])?; Ok(()) })?;
                let row:i64=db.query_row("SELECT id FROM _coven_rows WHERE table_name='parents' AND key=?1 AND generation=?2",crate::params![parent.key,1u64.to_be_bytes()],|r|r.get(0))?;
                db.internal_execute("DELETE FROM _coven_cells WHERE row_id=?1",[row])?;
                for (column,_,write) in cells {
                    db.internal_execute("INSERT INTO _coven_cells(column_id,row_id,write_id) VALUES(?1,?2,?3)", crate::params![column,row,write])?;
                }
                let app = crate::write_rows::AppView::after(db,schema);
                let store = crate::merge_store::MergeStore::new(db,&app);
                crate::write_apply::WriteApply::new(db,schema,&store,&app,&app,&Default::default()).apply(None,[parent].into())
            }).unwrap();
        });
        assert_eq!(
            reference(&b, lost).await,
            if cleared { None } else { Some("parent".into()) },
            "lost={lost}, cleared={cleared}"
        );
        assert_eq!(crate::write::tests::count(&b, "_coven_written_values"), 0);
        for db in [a, b] {
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn migrations_freeze_deleted_rows_and_preserve_surviving_written_references() {
    let clock = |seconds| {
        std::sync::Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds),
        ))
    };
    let initial = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE SET NULL,guard TEXT REFERENCES parents(id) ON DELETE CASCADE)";
    let migrations = |rename| {
        let sql = initial;
        let mut migrations = vec![crate::Migration::addition_with(
            1,
            "references",
            move |db| {
                db.execute_batch(sql)?;
                Ok(())
            },
        )];
        if rename {
            migrations.push(crate::Migration::breaking(
                2,
                "rename",
                "ALTER TABLE children RENAME COLUMN parent TO root",
                crate::OldWrites::Drop,
            ));
        }
        migrations
    };
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store
        .builder(tables(), migrations(false))
        .clock(clock(1))
        .open()
        .await
        .unwrap();
    let b = b_store
        .builder(tables(), migrations(false))
        .clock(clock(2))
        .open()
        .await
        .unwrap();
    let c_store = TestStore::with_ids(&ids);
    let c = c_store
        .builder(tables(), migrations(false))
        .clock(clock(3))
        .open()
        .await
        .unwrap();
    sql(
        &a,
        "INSERT INTO parents VALUES('parent'),('Inbox'),('Archive')",
    )
    .await
    .unwrap();
    c.receive(records(&a).remove(0).into()).await.unwrap();
    b.receive(records(&a).remove(0).into()).await.unwrap();
    sql(
        &a,
        "INSERT INTO children VALUES('removed','parent','parent'),('live','parent','Inbox')",
    )
    .await
    .unwrap();
    b.receive(records(&a).remove(1).into()).await.unwrap();
    // Concurrent assignments leave a cell loss on the row that CASCADE removes.
    sql(&a, "UPDATE children SET parent='Inbox' WHERE id='removed'")
        .await
        .unwrap();
    sql(
        &b,
        "UPDATE children SET parent='Archive' WHERE id='removed'",
    )
    .await
    .unwrap();
    for write in records(&a) {
        a.receive(write.into()).await.unwrap();
    }
    a.receive(records(&b).remove(0).into()).await.unwrap();
    sql(&c, "DELETE FROM parents WHERE id='parent'")
        .await
        .unwrap();
    a.receive(records(&c).remove(0).into()).await.unwrap();
    c.close().await.unwrap();
    let losses = a.lost_values().await.unwrap();
    let row = losses
        .iter()
        .find_map(|loss| match &loss.lost {
            crate::Lost::Row(cells) => Some(cells),
            _ => None,
        })
        .expect("removed child");
    assert_eq!(
        row.iter()
            .find(|cell| cell.column == "parent")
            .unwrap()
            .value,
        crate::types::Value::Text("Archive".into())
    );
    let snapshot_store = TestStore::with_ids(&ids);
    let snapshot_target = snapshot_store
        .builder(tables(), migrations(false))
        .open()
        .await
        .unwrap();
    crate::snapshot_write::tests::assert_loaded_losses(&a, &snapshot_target).await;
    snapshot_target.close().await.unwrap();
    a.close().await.unwrap();
    let a = a_store
        .builder(tables(), migrations(true))
        .open()
        .await
        .unwrap();
    // The removed child is deleted at its generation: one frozen loss keeps its
    // old names and written values. The migration write had read the
    // concurrent cell, so its delete acknowledges that loss (§18.7).
    let retained = a.lost_values().await.unwrap();
    let removed = losses
        .iter()
        .find(|l| matches!(l.replaced_by, crate::Replacement::Rules(_)))
        .unwrap();
    assert_eq!(retained.len(), 1, "the migration delete read the cell loss");
    assert_eq!(
        retained[0].replaced_by,
        crate::Replacement::SchemaChange { version: 2 }
    );
    assert_eq!(
        (&retained[0].table, &retained[0].key, &retained[0].lost),
        (&removed.table, &removed.key, &removed.lost)
    );
    assert!(matches!(
        retained[0].target,
        crate::lost::LossTarget::Cells(_)
    ));
    a.inspect_writer_schema(|db, schema| {
        db.for_each::<_, crate::DbError>(
            "SELECT replacement_kind,value FROM _coven_lost WHERE frozen=1",
            [],
            |r| {
                let bytes = r.get::<_, Vec<u8>>(1)?;
                if r.get::<_, String>(0)? != "write" {
                    for value in coven_format::merge_fields::decode_columns(&bytes)
                        .unwrap()
                        .values()
                    {
                        assert!(value.parents.is_empty(), "frozen row values are plain");
                    }
                } else {
                    assert!(
                        coven_format::merge_fields::decode_column_value(&bytes)
                            .unwrap()
                            .parents
                            .is_empty(),
                        "frozen cell values are plain"
                    );
                }
                Ok(())
            },
        )
        .unwrap();
        let app = crate::write_rows::AppView::after(db, schema);
        let merge = crate::merge_store::MergeStore::new(db, &app);
        let live = RowId {
            table: "children".into(),
            key: coven_format::key::encode_key(&[Value::Text("live".into())]).unwrap(),
            audience: Audience::Store,
        };
        assert_eq!(
            merge.row(&live).unwrap().state.cells()["root"].value.value,
            Value::Text("parent".into())
        );
    });
    let snapshot_target = snapshot_store
        .builder(tables(), migrations(true))
        .open()
        .await
        .unwrap();
    for write in records(&snapshot_target) {
        a.receive(write.into()).await.unwrap();
    }
    crate::snapshot_write::tests::assert_loaded_losses(&a, &snapshot_target).await;
    snapshot_target.close().await.unwrap();
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn dropping_a_lost_reference_keeps_its_written_value_as_plain_data() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let c_store = TestStore::with_ids(&ids);
    let a = open(&a_store, 1).await;
    let b = open(&b_store, 2).await;
    let c = open(&c_store, 3).await;
    sql(&a, "INSERT INTO parents VALUES('parent'),('Inbox')")
        .await
        .unwrap();
    for target in [&b, &c] {
        target.receive(records(&a).remove(0).into()).await.unwrap();
    }
    sql(&b, "INSERT INTO children VALUES('child','parent')")
        .await
        .unwrap();
    sql(&c, "INSERT INTO children VALUES('child','Inbox')")
        .await
        .unwrap();
    b.receive(records(&c).remove(0).into()).await.unwrap();
    sql(&c, "DELETE FROM children").await.unwrap();
    b.receive(records(&c).remove(1).into()).await.unwrap();
    sql(&a, "DELETE FROM parents WHERE id='parent'")
        .await
        .unwrap();
    b.receive(records(&a).remove(1).into()).await.unwrap();
    assert_eq!(reference(&b, true).await, Some("parent".into()));
    b.close().await.unwrap();
    let b = b_store.builder(tables(), vec![
        crate::Migration::addition_with(1, "references", move |db| {
            db.execute_batch("CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE SET NULL)")?; Ok(())
        }),
        crate::Migration::breaking(2, "drop reference", "CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY,parent TEXT); INSERT INTO rebuilt SELECT * FROM children; DROP TABLE children; ALTER TABLE rebuilt RENAME TO children", crate::OldWrites::Drop),
    ]).open().await.unwrap();
    assert_eq!(reference(&b, true).await, Some("parent".into()));
    b.inspect_writer(|db| {
        let bytes: Vec<u8> = db.query_row("SELECT l.value FROM _coven_lost l JOIN _coven_columns c ON c.id=l.column_id WHERE c.column_name='parent'", [], |r| r.get(0)).unwrap();
        let lost = coven_format::merge_fields::decode_column_value(&bytes).unwrap();
        assert!(lost.parents.is_empty());
        assert_eq!(lost.value, Value::Text("parent".into()));
    });
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

/// §8.2: a lost value is one some write set. A generated cell can display a
/// value no write set, computed from a NULL-substituted reference or from
/// inputs other writes merged in; every arrival order records the written one.
#[tokio::test]
async fn generated_cells_lose_their_written_values_in_every_arrival_order() {
    let schema = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE SET NULL,a TEXT,b TEXT,label TEXT AS (coalesce(parent,'none')),joined TEXT AS (a||b))";
    let open = |store: &TestStore, seconds: u64| {
        store
            .builder(
                tables(),
                vec![crate::Migration::addition_with(
                    1,
                    "generated",
                    move |sql| {
                        sql.execute_batch(schema)?;
                        Ok(())
                    },
                )],
            )
            .clock(std::sync::Arc::new(
                coven_foundation::clock::FixedClock::new(
                    std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds),
                ),
            ))
            .open()
    };
    let data = |db: &Database| {
        let db = db.clone();
        async move { crate::tests::data(&db, Audience::Store) }
    };
    // Each case: three concurrent writes stamped in this order, and the
    // generated column and values those writes set.
    let cases: [(&str, [&str; 3], &str, &[&str]); 2] = [
        (
            // Ben deletes p without having read Ana's reference to it; on a
            // device that applies his delete before Dan's write, label shows
            // 'none', which no write set.
            "reference",
            [
                "UPDATE children SET parent='p'",
                "DELETE FROM parents WHERE id='p'",
                "UPDATE children SET parent='q'",
            ],
            "label",
            &["p"],
        ),
        (
            // Ana and Ben set different inputs; once both apply, joined shows
            // 'AB', which no write set.
            "inputs",
            [
                "UPDATE children SET a='A'",
                "UPDATE children SET b='B'",
                "UPDATE children SET a='D',b='D'",
            ],
            "joined",
            &["Ay", "xB"],
        ),
    ];
    for (case, statements, column, written) in cases {
        let ids = SequentialIds::new();
        let origin_store = TestStore::with_ids(&ids);
        let origin = open(&origin_store, 1).await.unwrap();
        sql(
            &origin,
            "INSERT INTO parents VALUES('o'),('p'),('q'); INSERT INTO children(id,parent,a,b) VALUES('c','o','x','y')",
        )
        .await
        .unwrap();
        let initial = records(&origin).remove(0);
        let mut writes = Vec::new();
        for (index, statement) in statements.into_iter().enumerate() {
            let store = TestStore::with_ids(&ids);
            let author = open(&store, 2 + index as u64).await.unwrap();
            author.receive(initial.clone().into()).await.unwrap();
            sql(&author, statement).await.unwrap();
            writes.push(records(&author).remove(0));
            author.close().await.unwrap();
        }
        let mut outcomes = Vec::new();
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let store = TestStore::with_ids(&ids);
            let receiver = open(&store, 10).await.unwrap();
            receiver.receive(initial.clone().into()).await.unwrap();
            for index in order {
                receiver
                    .receive(writes[index].clone().into())
                    .await
                    .unwrap();
            }
            let losses = receiver.lost_values().await.unwrap();
            let generated: Vec<_> = losses
                .iter()
                .filter_map(|loss| match &loss.lost {
                    crate::Lost::Cell(cell) if cell.column == column => Some(cell.value.clone()),
                    _ => None,
                })
                .collect();
            assert!(!generated.is_empty(), "{case} {order:?}");
            for value in &generated {
                assert!(
                    written
                        .iter()
                        .any(|w| *value == crate::types::Value::Text((*w).into())),
                    "{case} {order:?} lost {value:?}, which no write set"
                );
            }
            // The listing's order follows local column ids; compare as a set.
            let mut listed: Vec<_> = losses.iter().map(|loss| format!("{loss:?}")).collect();
            listed.sort();
            outcomes.push((
                listed,
                data(&receiver).await,
                count(&receiver, "_coven_lost"),
            ));
            receiver.close().await.unwrap();
        }
        for outcome in &outcomes[1..] {
            assert_eq!(outcome, &outcomes[0], "{case}");
        }
        // A snapshot taken while the generated cell shows a value no write set
        // carries the written one, and the loaded copy loses it the same way.
        let source_store = TestStore::with_ids(&ids);
        let source = open(&source_store, 10).await.unwrap();
        let target_store = TestStore::with_ids(&ids);
        let target = open(&target_store, 10).await.unwrap();
        for write in [&initial, &writes[0], &writes[1]] {
            source.receive(write.clone().into()).await.unwrap();
        }
        crate::snapshot_write::tests::assert_loaded_losses(&source, &target).await;
        for db in [&source, &target] {
            db.receive(writes[2].clone().into()).await.unwrap();
        }
        crate::snapshot_write::tests::assert_loaded_losses(&source, &target).await;
        for db in [source, target, origin] {
            db.close().await.unwrap();
        }
    }
}
#[tokio::test]
async fn null_assignments_follow_their_row_and_complete_reference_group() {
    use crate::tests::TestStore;
    use crate::write::tests::{records, sql};
    use coven_format::value::Value;
    use coven_foundation::id_source::SequentialIds;
    use coven_merge::{Audience, Operation, RowId};

    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let tables = || {
        vec![
            crate::SyncedTable::new("parents", crate::RowIdentity::SharedKey)
                .key_columns(["id", "part"]),
            crate::SyncedTable::new("children", crate::RowIdentity::SharedKey)
                .shared_trigger("clear_inner"),
        ]
    };
    let schema = "CREATE TABLE parents(id TEXT NOT NULL, part TEXT NOT NULL, PRIMARY KEY(id,part));
    CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,pa TEXT,pb TEXT,label TEXT,
    FOREIGN KEY(pa,pb) REFERENCES parents(id,part) ON DELETE SET NULL);
    CREATE TRIGGER clear_inner AFTER UPDATE OF label ON children
    WHEN NOT coven_applying() AND new.id='outer'
    BEGIN UPDATE children SET pa=NULL WHERE id='inner'; END";
    let a = sa.schema(tables(), schema).await.unwrap();
    let b = sb.schema(tables(), schema).await.unwrap();
    sql(&a, "INSERT INTO parents VALUES('p','1')")
        .await
        .unwrap();
    b.receive(records(&a).remove(0).into()).await.unwrap();
    sql(
        &b,
        "INSERT INTO children VALUES('outer','p','1','old'),('inner','p','1','old')",
    )
    .await
    .unwrap();
    sql(&a, "DELETE FROM parents").await.unwrap();
    b.receive(records(&a).remove(1).into()).await.unwrap();
    let queued = records(&b).len();
    // Capture objects roll back with a failed app statement, and cannot be
    // named by app SQL even while the legitimate assignment triggers exist.
    assert!(sql(
        &b,
        "INSERT INTO _coven_null_assignments VALUES('children','pa')"
    )
    .await
    .is_err());
    b.inspect_writer(|db| {
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM temp.sqlite_schema WHERE name GLOB '_coven_null_assignment*'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
        db.fail_at("refuse", "BEFORE INSERT ON _coven_uploads", "refuse");
    });
    assert!(
        sql(&b, "UPDATE children SET label='edited' WHERE id='outer'")
            .await
            .is_err()
    );
    assert_eq!(records(&b).len(), queued);
    b.inspect_writer(|db| db.batch("DROP TRIGGER refuse").unwrap());
    sql(&b, "UPDATE children SET label='edited' WHERE id='outer'")
        .await
        .unwrap();
    let write = records(&b).pop().unwrap();
    assert_eq!(write.parts[0].rows.len(), 2);
    let inner = write.parts[0]
        .rows
        .iter()
        .find(|change| {
            change.row.key == coven_format::key::encode_key(&[Value::Text("inner".into())]).unwrap()
        })
        .unwrap();
    let Operation::Update(columns) = &inner.change.operation else {
        panic!("update");
    };
    assert_eq!(
        columns.keys().map(String::as_str).collect::<Vec<_>>(),
        ["pa", "pb"]
    );
    for column in ["pa", "pb"] {
        assert_eq!(columns[column].value, Value::Null);
        assert!(columns[column].parents.is_empty());
    }
    assert_eq!(inner.old["pa"], Value::Text("p".into()));
    assert_eq!(inner.old["pb"], Value::Text("1".into()));
    b.inspect_writer_schema(|db, schema| {
        let app = crate::write_rows::AppView::after(db, schema);
        let store = crate::merge_store::MergeStore::new(db, &app);
        let row = RowId {
            table: "children".into(),
            key: coven_format::key::encode_key(&[Value::Text("outer".into())]).unwrap(),
            audience: Audience::Store,
        };
        assert_eq!(
            store.row(&row).unwrap().state.cells()["pa"].value.value,
            Value::Text("p".into()),
            "a nested trigger must not clear the outer row"
        );
    });
    sql(
        &b,
        "UPDATE children SET pa=pa WHERE id='outer'; UPDATE children SET pa=NULL WHERE id='outer'",
    )
    .await
    .unwrap();
    let write = records(&b).pop().unwrap();
    assert_eq!(write.parts[0].rows.len(), 1);
    let steps = &write.parts[0].rows[0].execution.steps;
    assert_eq!(steps.len(), 2);
    assert!(matches!(&steps[0].change.operation, Operation::Update(columns) if columns.len() == 2));
    assert!(matches!(&steps[1].change.operation, Operation::Update(columns) if columns.is_empty()));
    let queued = records(&b).len();
    sql(&b, "UPDATE children SET pa=NULL,pb=NULL")
        .await
        .unwrap();
    assert_eq!(records(&b).len(), queued);
    b.inspect_writer(|db| {
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM temp.sqlite_schema WHERE name GLOB '_coven_null_assignment*'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        )
    });
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn rolled_back_null_assignments_do_not_clear_written_references() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let schema = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY);
    CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE SET NULL);
    CREATE TRIGGER refuse AFTER UPDATE ON children WHEN NOT coven_applying() AND new.id='refuse'
    BEGIN SELECT RAISE(ABORT,'refused'); END";
    let a = sa.schema(tables(), schema).await.unwrap();
    let b = sb.schema(tables(), schema).await.unwrap();
    sql(&a, "INSERT INTO parents VALUES('p')").await.unwrap();
    b.receive(records(&a).remove(0).into()).await.unwrap();
    sql(&b, "INSERT INTO children VALUES('refuse','p'),('okay','p')")
        .await
        .unwrap();
    sql(&a, "DELETE FROM parents").await.unwrap();
    b.receive(records(&a).remove(1).into()).await.unwrap();
    let queued = records(&b).len();
    b.write(|sql| {
        assert!(sql.execute("UPDATE children SET parent=NULL", []).is_err());
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        records(&b).len(),
        queued,
        "a rolled-back assignment must record nothing"
    );
    b.write(|sql| {
        assert!(sql
            .execute("UPDATE children SET parent=NULL WHERE id='refuse'", [])
            .is_err());
        sql.execute("UPDATE children SET parent=NULL WHERE id='okay'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let write = records(&b).pop().unwrap();
    assert_eq!(write.parts[0].rows.len(), 1);
    assert_eq!(
        write.parts[0].rows[0].row.key,
        coven_format::key::encode_key(&[Value::Text("okay".into())]).unwrap()
    );
    a.close().await.unwrap();
    b.close().await.unwrap();
}
