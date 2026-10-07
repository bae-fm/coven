use crate::tests::TestStore;
use crate::write::tests::{records, sql};
use crate::{Database, RowIdentity, SyncedTable};
use coven_format::value::Value;
use coven_foundation::id_source::SequentialIds;
use coven_merge::{Audience, RowId};

async fn open(store: &TestStore, action: &'static str, seconds: u64) -> Database {
    store.builder(tables(), vec![crate::Migration::run(1,"references",move |sql| {
        sql.execute_batch(&format!("CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY COLLATE NOCASE); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT DEFAULT 'Inbox' REFERENCES parents(id) ON DELETE {action})"))?;
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
    for (action, lost) in ["SET NULL", "SET DEFAULT"]
        .into_iter()
        .flat_map(|action| [false, true].map(|lost| (action, lost)))
    {
        let ids = SequentialIds::new();
        let a_store = TestStore::with_ids(&ids);
        let b_store = TestStore::with_ids(&ids);
        let a = open(&a_store, action, 1).await;
        let b = open(&b_store, action, 2).await;
        sql(&a, "INSERT INTO parents VALUES('parent'),('Inbox')")
            .await
            .unwrap();
        b.apply_downloaded(records(&a).remove(0).into())
            .await
            .unwrap();
        sql(&b, "INSERT INTO children VALUES('child','PaReNt')")
            .await
            .unwrap();
        if lost {
            let c_store = TestStore::with_ids(&ids);
            let c = open(&c_store, action, 3).await;
            c.apply_downloaded(records(&a).remove(0).into())
                .await
                .unwrap();
            sql(&c, "INSERT INTO children VALUES('child','Inbox')")
                .await
                .unwrap();
            b.apply_downloaded(records(&c).remove(0).into())
                .await
                .unwrap();
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
        b.apply_downloaded(records(&a).remove(1).into())
            .await
            .unwrap();
        assert_eq!(
            reference(&b, lost).await,
            if lost {
                Some("PaReNt".into())
            } else if action == "SET NULL" {
                None
            } else {
                Some("Inbox".into())
            }
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
            assert_eq!(written.value, Value::Text("PaReNt".into()));
            assert_eq!(written.parents.values().next().unwrap().generation, 1);
        });
        b.close().await.unwrap();
        let b = open(&b_store, action, 2).await;
        b.inspect_writer_schema(|db, schema| {
            db.transaction(|db| {
                db.internal_execute("DELETE FROM _coven_rows WHERE table_name='parents' AND key=?1 AND generation=?2", crate::params![parent.key, 2u64.to_be_bytes().as_slice()])?;
                db.materialize(|db| { db.internal_execute("INSERT INTO parents VALUES('parent')", [])?; Ok(()) })?;
                for (column,row,write) in cells {
                    db.internal_execute("INSERT INTO _coven_cells(column_id,row_id,write_id) VALUES(?1,?2,?3)", crate::params![column,row,write])?;
                }
                let app = crate::write_rows::AppView::after(db,schema);
                let store = crate::merge_store::MergeStore::new(db,&app);
                crate::write_apply::WriteApply::new(db,schema,&store,&app,&app,&Default::default()).apply(None,[parent].into())
            }).unwrap();
        });
        assert_eq!(
            reference(&b, lost).await,
            Some("PaReNt".into()),
            "{action}, lost={lost}"
        );
        assert_eq!(crate::write::tests::count(&b, "_coven_reference_values"), 0);
        for db in [a, b] {
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn migrations_freeze_retired_losses_and_preserve_surviving_written_references() {
    let clock = |seconds| {
        std::sync::Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds),
        ))
    };
    for action in ["SET NULL", "SET DEFAULT"] {
        let initial = format!("CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY COLLATE NOCASE); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT DEFAULT 'Inbox' REFERENCES parents(id) ON DELETE {action},guard TEXT REFERENCES parents(id) ON DELETE CASCADE)");
        let migrations = |rename| {
            let sql = initial.clone();
            let mut migrations = vec![crate::Migration::run(1, "references", move |db| {
                db.execute_batch(&sql)?;
                Ok(())
            })];
            if rename {
                migrations.push(crate::Migration::sql(
                    2,
                    "rename",
                    "ALTER TABLE children RENAME COLUMN parent TO root",
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
        sql(&a, "INSERT INTO parents VALUES('parent'),('Inbox')")
            .await
            .unwrap();
        c.apply_downloaded(records(&a).remove(0).into())
            .await
            .unwrap();
        b.apply_downloaded(records(&a).remove(0).into())
            .await
            .unwrap();
        sql(
            &a,
            "INSERT INTO children VALUES('removed','PaReNt','parent'),('live','PaReNt','Inbox')",
        )
        .await
        .unwrap();
        b.apply_downloaded(records(&a).remove(1).into())
            .await
            .unwrap();
        // Concurrent assignments leave a cell loss on the row that CASCADE removes.
        sql(&a, "UPDATE children SET parent='Inbox' WHERE id='removed'")
            .await
            .unwrap();
        sql(&b, "UPDATE children SET parent='PARENT' WHERE id='removed'")
            .await
            .unwrap();
        a.apply_downloaded(records(&b).remove(0).into())
            .await
            .unwrap();
        sql(&c, "DELETE FROM parents WHERE id='parent'")
            .await
            .unwrap();
        a.apply_downloaded(records(&c).remove(0).into())
            .await
            .unwrap();
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
            crate::types::Value::Text("PARENT".into())
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
        let retained = a.lost_values().await.unwrap();
        assert_eq!(retained.len(), losses.len());
        for (after, before) in retained.iter().zip(&losses) {
            assert_eq!(
                (&after.table, &after.key, &after.lost, &after.replaced_by),
                (
                    &before.table,
                    &before.key,
                    &before.lost,
                    &before.replaced_by
                )
            );
            assert!(matches!(after.target, crate::lost::LossTarget::Cells(_)));
        }
        a.inspect_writer_schema(|db, schema| {
            db.visit(
                "SELECT replacement_kind,value FROM _coven_lost WHERE retired=1",
                [],
                |r| {
                    let bytes = r.get::<_, Vec<u8>>(1)?;
                    if r.get::<_, String>(0)? == "rules" {
                        for value in coven_format::merge_fields::decode_columns(&bytes)
                            .unwrap()
                            .values()
                        {
                            assert!(value.parents.is_empty(), "retired row values are plain");
                        }
                    } else {
                        assert!(
                            coven_format::merge_fields::decode_column_value(&bytes)
                                .unwrap()
                                .parents
                                .is_empty(),
                            "retired cell values are plain"
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
                Value::Text("PaReNt".into())
            );
        });
        let snapshot_target = snapshot_store
            .builder(tables(), migrations(true))
            .open()
            .await
            .unwrap();
        for write in records(&snapshot_target) {
            a.apply_downloaded(write.into()).await.unwrap();
        }
        crate::snapshot_write::tests::assert_loaded_losses(&a, &snapshot_target).await;
        snapshot_target.close().await.unwrap();
        for db in [a, b] {
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn dropping_a_lost_reference_keeps_its_written_value_as_plain_data() {
    for action in ["SET NULL", "SET DEFAULT"] {
        let ids = SequentialIds::new();
        let a_store = TestStore::with_ids(&ids);
        let b_store = TestStore::with_ids(&ids);
        let c_store = TestStore::with_ids(&ids);
        let a = open(&a_store, action, 1).await;
        let b = open(&b_store, action, 2).await;
        let c = open(&c_store, action, 3).await;
        sql(&a, "INSERT INTO parents VALUES('parent'),('Inbox')")
            .await
            .unwrap();
        for target in [&b, &c] {
            target
                .apply_downloaded(records(&a).remove(0).into())
                .await
                .unwrap();
        }
        sql(&b, "INSERT INTO children VALUES('child','PaReNt')")
            .await
            .unwrap();
        sql(&c, "INSERT INTO children VALUES('child','Inbox')")
            .await
            .unwrap();
        b.apply_downloaded(records(&c).remove(0).into())
            .await
            .unwrap();
        sql(&c, "DELETE FROM children").await.unwrap();
        b.apply_downloaded(records(&c).remove(1).into())
            .await
            .unwrap();
        sql(&a, "DELETE FROM parents WHERE id='parent'")
            .await
            .unwrap();
        b.apply_downloaded(records(&a).remove(1).into())
            .await
            .unwrap();
        assert_eq!(reference(&b, true).await, Some("PaReNt".into()));
        b.close().await.unwrap();
        let b = b_store.builder(tables(), vec![
            crate::Migration::run(1,"references",move |db| {
                db.execute_batch(&format!("CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY COLLATE NOCASE); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT DEFAULT 'Inbox' REFERENCES parents(id) ON DELETE {action})"))?; Ok(())
            }),
            crate::Migration::sql(2,"drop reference","CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY,parent TEXT DEFAULT 'Inbox'); INSERT INTO rebuilt SELECT * FROM children; DROP TABLE children; ALTER TABLE rebuilt RENAME TO children"),
        ]).open().await.unwrap();
        assert_eq!(reference(&b, true).await, Some("PaReNt".into()));
        b.inspect_writer(|db| {
            let bytes: Vec<u8> = db.query_row("SELECT l.value FROM _coven_lost l JOIN _coven_columns c ON c.id=l.column_id WHERE c.column_name='parent'", [], |r| r.get(0)).unwrap();
            let lost = coven_format::merge_fields::decode_column_value(&bytes).unwrap();
            assert!(lost.parents.is_empty());
            assert_eq!(lost.value, Value::Text("PaReNt".into()));
        });
        for db in [a, b, c] {
            db.close().await.unwrap();
        }
    }
}
