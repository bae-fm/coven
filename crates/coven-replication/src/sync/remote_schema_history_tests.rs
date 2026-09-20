use crate::sync::test_helpers::*;
use coven_database::{
    ChangesetColumn, ChangesetOperation, ChangesetUpdate, DbError, Migration,
    TableChangesetMigration,
};
use coven_protocol::synced_schema::{RowIdentity, SyncedTable};
use rusqlite::types::Value;

fn migrations(reject_update: std::sync::Arc<std::sync::atomic::AtomicBool>) -> Vec<Migration> {
    vec![
        Migration::sql(
            1,
            "notes",
            "CREATE TABLE notes (
            id TEXT PRIMARY KEY, title TEXT NOT NULL, shared INTEGER NOT NULL,
            _updated_at TEXT NOT NULL, created_at TEXT NOT NULL, origin TEXT
        ) STRICT;",
        ),
        Migration::sql(2, "remove_origin", "ALTER TABLE notes DROP COLUMN origin").changesets(
            vec![TableChangesetMigration::new("notes", &[], |row, _| {
                row.change.retain_columns(|name| name != "origin");
                Ok(())
            })],
        ),
        Migration::sql(
            3,
            "note_kind",
            "ALTER TABLE notes ADD COLUMN kind TEXT NOT NULL DEFAULT 'note'",
        )
        .changesets(vec![TableChangesetMigration::new(
            "notes",
            &["created_at"],
            move |row, context| {
                if matches!(&row.change, ChangesetOperation::Update(_))
                    && reject_update.load(std::sync::atomic::Ordering::SeqCst)
                {
                    return Err(DbError::Message("rejected historical update".into()));
                }
                assert_eq!(
                    context.get("created_at"),
                    Some(&Value::Text("2026-01-01".into()))
                );
                match &mut row.change {
                    ChangesetOperation::Insert(columns) | ChangesetOperation::Delete(columns) => {
                        columns.push(ChangesetColumn {
                            name: "kind".into(),
                            primary_key: false,
                            value: Value::Text("note".into()),
                        });
                    }
                    ChangesetOperation::Update(columns) => {
                        columns.push(ChangesetColumn {
                            name: "kind".into(),
                            primary_key: false,
                            value: ChangesetUpdate {
                                old: None,
                                new: None,
                            },
                        });
                    }
                }
                Ok(())
            },
        )]),
    ]
}

#[tokio::test]
async fn old_insert_and_sparse_update_pull_after_schema_upgrade() {
    pull_historical_writes(false).await;
}

#[tokio::test]
async fn failed_historical_conversion_rolls_back_rows_and_frontier_then_retries() {
    pull_historical_writes(true).await;
}

async fn pull_historical_writes(reject_first_pull: bool) {
    let reject_update = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(reject_first_pull));
    let tables = vec![SyncedTable::new("notes", RowIdentity::SharedKey).gated_by("shared")];
    let author_dir = test_store_dir();
    let author_db = open_test_db_schema(
        author_dir.clone(),
        tables.clone(),
        migrations(reject_update.clone())[..1].to_vec(),
    );
    let signer = user_keypair_from_seed([73; 32]);
    let store = TestStore::create(
        &author_db,
        author_dir.clone(),
        "historical-notes",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .unwrap();
    let author = store
        .bind_device_in(&author_db, author_dir.clone(), &signer)
        .await
        .unwrap();
    let peer_dir = test_store_dir();
    let peer = store
        .activate_joined_device_from_snapshot(
            &author_db,
            author_dir,
            peer_dir,
            &signer,
            "2026-07-19T00:00:00Z",
            tables,
            migrations(reject_update.clone()),
            3,
        )
        .await
        .unwrap();
    author.pull_store().await.unwrap();

    author_db.execute_test_host_write("INSERT INTO notes VALUES ('historical-note', 'Before', 1, '0000000001000-0000-author', '2026-01-01', 'tags')").await;
    assert!(author.prepare_pending_store_write().await.unwrap());
    assert_eq!(author.drain_store_writes().await.unwrap(), 1);
    author_db.execute_test_host_write("UPDATE notes SET title = 'After', _updated_at = '0000000002000-0000-author' WHERE id = 'historical-note'").await;
    assert!(author.prepare_pending_store_write().await.unwrap());
    assert_eq!(author.drain_store_writes().await.unwrap(), 1);

    if reject_first_pull {
        let before = peer.materialized_frontier().await.unwrap();
        let error = peer
            .pull_store()
            .await
            .expect_err("reject historical UPDATE");
        assert!(
            format!("{error:?}").contains("rejected historical update"),
            "{error:?}"
        );
        assert_eq!(peer.materialized_frontier().await.unwrap(), before);
        assert!(
            !peer.test_row_exists("SELECT id FROM notes").await,
            "earlier INSERT must roll back with rejected UPDATE"
        );
        reject_update.store(false, std::sync::atomic::Ordering::SeqCst);
    }
    let (_, result) = peer.pull_store().await.expect("pull historical writes");
    assert!(
        result.held_positions.is_empty(),
        "{:?}",
        result.held_positions
    );
    assert_eq!(
        peer.query_test_text("SELECT title FROM notes WHERE id = 'historical-note'")
            .await,
        "After"
    );
    assert_eq!(
        peer.query_test_text("SELECT kind FROM notes WHERE id = 'historical-note'")
            .await,
        "note"
    );
    assert_eq!(peer.replay_row_count_for_test("notes").await.unwrap(), 1);
}
