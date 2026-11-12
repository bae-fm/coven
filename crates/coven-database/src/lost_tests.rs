/// Losses under schema a migration drops (§18.7, D7): Lean
/// `Migration.freezeDropped` keeps every affected record, frozen under its
/// old names with its original cause and setters.
mod dropped_schema {
    use crate::snapshot_write::tests::assert_loaded_losses;
    use crate::write::tests::{count, records, sql};
    use crate::{tests::TestStore, Database, Lost, Migration, Replacement, RowIdentity};
    use crate::{LostValue, SyncedTable};
    use coven_foundation::id_source::SequentialIds;

    const TAGS: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,body TEXT NOT NULL DEFAULT ''); CREATE TABLE tags(id TEXT NOT NULL PRIMARY KEY,label TEXT NOT NULL)";

    fn tables(names: &[&str]) -> Vec<SyncedTable> {
        names.iter().map(|name| SyncedTable::new(*name, RowIdentity::SharedKey)).collect()
    }

    /// `b` edits `column` of `row` concurrently with `a`; `a` keeps one loss.
    async fn concurrent_loss(
        a: &Database,
        b: &Database,
        inserted: &'static str,
        a_edit: &'static str,
        b_edit: &'static str,
    ) {
        sql(a, inserted).await.unwrap();
        a.receive(records(a).last().unwrap().clone().into()).await.unwrap();
        b.receive(records(a).last().unwrap().clone().into()).await.unwrap();
        sql(b, b_edit).await.unwrap();
        sql(a, a_edit).await.unwrap();
        a.receive(records(b).last().unwrap().clone().into()).await.unwrap();
        assert_eq!(a.lost_values().await.unwrap().len(), 1);
    }

    async fn assert_frozen(db: &Database, before: &[LostValue]) {
        let losses = db.lost_values().await.unwrap();
        assert_eq!(
            losses.len(),
            1,
            "schema {}, slot {}",
            db.schema_version().await.unwrap(),
            db.applied_slot().await.unwrap()
        );
        assert_eq!(
            (&losses[0].table, &losses[0].key, &losses[0].lost, &losses[0].replaced_by),
            (&before[0].table, &before[0].key, &before[0].lost, &before[0].replaced_by)
        );
        assert_eq!(cell_column(&losses[0]), "body");
    }

    fn cell_column(loss: &LostValue) -> &str {
        match &loss.lost {
            Lost::Cell(cell) => &cell.column,
            Lost::Row(_) => panic!("{loss:?}"),
        }
    }

    fn evidence(db: &Database, table: &str) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        db.inspect_writer(|sql| {
            sql.query(
                "SELECT c.write_id,a.admission FROM _coven_authored_changes c LEFT JOIN _coven_authored_admissions a USING(write_id,audience,table_name,key) WHERE c.table_name=?1 ORDER BY c.write_id,c.key",
                [table],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap()
        })
    }

    /// A dropped table's cell loss stays displayed, frozen under the table's
    /// name; its original changes and verdicts stay as source evidence. A
    /// snapshot carries the loss to a device that never had the table, with
    /// equal snapshot records, and dismissal removes it on both.
    #[tokio::test]
    async fn a_dropped_tables_losses_stay_frozen_under_its_name() {
        let ids = SequentialIds::new();
        let stores: Vec<_> = (0..3).map(|_| TestStore::with_ids(&ids)).collect();
        let a = stores[0].schema(tables(&["notes", "tags"]), TAGS).await.unwrap();
        let b = stores[1].schema(tables(&["notes", "tags"]), TAGS).await.unwrap();
        concurrent_loss(
            &a,
            &b,
            "INSERT INTO tags VALUES('t','first')",
            "UPDATE tags SET label='from a'",
            "UPDATE tags SET label='from b'",
        )
        .await;
        let before = a.lost_values().await.unwrap();
        let sources = evidence(&a, "tags");
        assert!(!sources.is_empty());
        a.close().await.unwrap();
        let migrations = || {
            vec![
                Migration::addition(1, "tags", TAGS),
                Migration::breaking(
                    2,
                    "drop tags",
                    "DROP TABLE tags",
                    crate::OldWrites::Drop,
                    Vec::new(),
                ),
            ]
        };
        let a = stores[0].builder(tables(&["notes"]), migrations()).open().await.unwrap();
        let after = a.lost_values().await.unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(
            (&after[0].table, &after[0].key, &after[0].lost, &after[0].replaced_by),
            (&before[0].table, &before[0].key, &before[0].lost, &before[0].replaced_by)
        );
        assert_eq!(after[0].table, "tags");
        assert_eq!(cell_column(&after[0]), "label");
        assert!(matches!(after[0].target, crate::lost::LossTarget::Acknowledgements(_)));
        assert_eq!(evidence(&a, "tags"), sources, "verdicts outlive the table");
        let fresh = stores[2].builder(tables(&["notes"]), migrations()).open().await.unwrap();
        for write in records(&fresh) {
            a.receive(write.into()).await.unwrap();
        }
        assert_loaded_losses(&a, &fresh).await;
        a.dismiss_lost_values(&after).await.unwrap();
        assert!(a.lost_values().await.unwrap().is_empty());
        assert_loaded_losses(&a, &fresh).await;
        for db in [a, b, fresh] {
            db.close().await.unwrap();
        }
    }

    /// A dropped column's cell loss stays frozen with the old name. A later
    /// migration adding a column of that name, then renaming it, moves only
    /// live cells: the frozen loss still names the dropped column, also after
    /// a snapshot reload with equal snapshot records.
    #[tokio::test]
    async fn a_dropped_columns_loss_keeps_its_name_through_reuse_and_rename() {
        let ids = SequentialIds::new();
        let stores: Vec<_> = (0..3).map(|_| TestStore::with_ids(&ids)).collect();
        let a = stores[0].schema(tables(&["notes"]), TAGS).await.unwrap();
        let b = stores[1].schema(tables(&["notes"]), TAGS).await.unwrap();
        concurrent_loss(
            &a,
            &b,
            "INSERT INTO notes VALUES('n','title','first')",
            "UPDATE notes SET body='from a'",
            "UPDATE notes SET body='from b'",
        )
        .await;
        let before = a.lost_values().await.unwrap();
        a.close().await.unwrap();
        let migrations = |through: u32| {
            [
                Migration::addition(1, "tags", TAGS),
                Migration::breaking(
                    2,
                    "drop body",
                    "ALTER TABLE notes DROP COLUMN body",
                    crate::OldWrites::Drop,
                    tables(&["notes"]),
                ),
                Migration::breaking(
                    3,
                    "body again",
                    "ALTER TABLE notes ADD COLUMN body TEXT NOT NULL DEFAULT ''",
                    crate::OldWrites::Drop,
                    tables(&["notes"]),
                ),
                Migration::breaking(
                    4,
                    "rename body",
                    "ALTER TABLE notes RENAME COLUMN body TO content",
                    crate::OldWrites::Drop,
                    tables(&["notes"]),
                ),
            ]
            .into_iter()
            .take(through as usize)
            .collect::<Vec<_>>()
        };
        let a = stores[0].builder(tables(&["notes"]), migrations(2)).open().await.unwrap();
        assert_frozen(&a, &before).await;
        a.close().await.unwrap();
        let a = stores[0].builder(tables(&["notes"]), migrations(3)).open().await.unwrap();
        sql(&a, "UPDATE notes SET body='new body'").await.unwrap();
        assert_frozen(&a, &before).await;
        a.close().await.unwrap();
        let a = stores[0].builder(tables(&["notes"]), migrations(4)).open().await.unwrap();
        assert_frozen(&a, &before).await;
        let content: String = a
            .read(|sql| {
                Ok::<_, crate::DbError>(
                    sql.query_row("SELECT content FROM notes", [], |r| r.get(0))?,
                )
            })
            .await
            .unwrap();
        assert_eq!(content, "new body");
        let fresh = stores[2].builder(tables(&["notes"]), migrations(4)).open().await.unwrap();
        for write in records(&fresh) {
            a.receive(write.into()).await.unwrap();
        }
        assert_loaded_losses(&a, &fresh).await;
        assert_frozen(&fresh, &before).await;
        assert_eq!(count(&fresh, "notes"), 1);
        for db in [a, b, fresh] {
            db.close().await.unwrap();
        }
    }

    /// A dropped column keeps its `_coven_columns` row only while a frozen
    /// lost value names it: dismissing the last one deletes the row in the
    /// same write, so the old column no longer counts as synced (§18.7).
    #[tokio::test]
    async fn dismissing_a_dropped_columns_last_loss_deletes_its_column() {
        let ids = SequentialIds::new();
        let stores: Vec<_> = (0..2).map(|_| TestStore::with_ids(&ids)).collect();
        let a = stores[0].schema(tables(&["notes"]), TAGS).await.unwrap();
        let b = stores[1].schema(tables(&["notes"]), TAGS).await.unwrap();
        concurrent_loss(
            &a,
            &b,
            "INSERT INTO notes VALUES('n','title','first')",
            "UPDATE notes SET body='from a'",
            "UPDATE notes SET body='from b'",
        )
        .await;
        a.close().await.unwrap();
        let a = stores[0]
            .builder(
                tables(&["notes"]),
                vec![
                    Migration::addition(1, "tags", TAGS),
                    Migration::breaking(
                        2,
                        "drop body",
                        "ALTER TABLE notes DROP COLUMN body",
                        crate::OldWrites::Drop,
                        tables(&["notes"]),
                    ),
                ],
            )
            .open()
            .await
            .unwrap();
        let body = |db: &Database| {
            db.inspect_writer(|db| {
                db.query_row(
                    "SELECT count(*) FROM _coven_columns WHERE table_name='notes' AND column_name='body'",
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap()
            })
        };
        assert_eq!(body(&a), 1);
        let losses = a.lost_values().await.unwrap();
        assert_eq!(losses.len(), 1);
        a.dismiss_lost_values(&losses).await.unwrap();
        assert!(a.lost_values().await.unwrap().is_empty());
        assert_eq!(body(&a), 0);
        for db in [a, b] {
            db.close().await.unwrap();
        }
    }

    /// Migration freezes deleted-circle values with their original columns.
    #[tokio::test]
    async fn a_hidden_rows_dropped_column_is_kept_in_a_frozen_whole_row_loss() {
        use crate::migration_state::tests::{circle_row, circle_schema, circle_tables};
        let store = TestStore::new();
        let db = store.schema(circle_tables(), circle_schema(true)).await.unwrap();
        let row = circle_row();
        db.receive(row.into()).await.unwrap();
        let circle = coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(1));
        crate::tests::delete_circle(&db, circle).await.unwrap();
        let hidden = db.lost_values().await.unwrap();
        assert_eq!(hidden.len(), 1);
        db.close().await.unwrap();
        let db = store
            .builder(
                circle_tables(),
                vec![
                    Migration::addition(1, "rows", circle_schema(true)),
                    Migration::breaking(
                        2,
                        "drop c",
                        "ALTER TABLE rows DROP COLUMN c",
                        crate::OldWrites::Drop,
                        circle_tables(),
                    ),
                ],
            )
            .open()
            .await
            .unwrap();
        let losses = db.lost_values().await.unwrap();
        assert_eq!(losses.len(), 1, "{losses:#?}");
        let columns = |loss: &LostValue| match &loss.lost {
            Lost::Row(cells) => cells.iter().map(|c| c.column.clone()).collect::<Vec<_>>(),
            Lost::Cell(_) => panic!("{loss:?}"),
        };
        let frozen = losses
            .iter()
            .find(|l| matches!(l.target, crate::lost::LossTarget::Acknowledgements(_)))
            .unwrap();
        assert_eq!(&frozen.lost, &hidden[0].lost);
        assert!(matches!(frozen.replaced_by, Replacement::SchemaChange { version: 2 }));
        assert!(columns(frozen).contains(&"c".to_string()));
        assert_eq!(count(&db, "rows"), 0);
        db.close().await.unwrap();
    }
}
