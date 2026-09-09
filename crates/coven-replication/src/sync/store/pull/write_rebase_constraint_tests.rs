use super::{test_synced_tables, RebaseFixture};
use coven_database::StoreDatabase;

#[tokio::test]
async fn snapshot_rebase_preserves_unique_swaps_and_their_foreign_key_children() {
    exercise_unique_swap(true).await;
}

#[tokio::test]
async fn unpublished_unique_swap_preserves_children_during_pull_and_publication() {
    exercise_unique_swap(false).await;
}

async fn exercise_unique_swap(snapshot: bool) {
    let migrations = vec![coven_database::Migration::run(
        1,
        "Unique note titles",
        |schema| {
            coven_database::synthetic_store::create_synced_schema(schema)?;
            schema
                .execute_batch("CREATE UNIQUE INDEX notes_unique_title ON notes(title);")
                .map_err(coven_database::DbError::from)
        },
    )];
    let fixture = RebaseFixture::with_schema(test_synced_tables(), migrations).await;
    fixture
        .source
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
         ('second', 'Second title', 1, '0000000001500-0000-owner', '2026-01-01'); \
         INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) VALUES \
         ('first-child', 'shared', 'first', '0000000001500-0000-owner', '2026-01-01'), \
         ('second-child', 'second', 'second', '0000000001500-0000-owner', '2026-01-01')",
        )
        .await;
    RebaseFixture::publish(&fixture.owner).await;
    fixture
        .peer
        .pull_store()
        .await
        .expect("peer observes constrained graph");
    assert_child_rows(&fixture.source, "owner before the swap").await;
    assert_child_rows(&fixture.target, "peer before the swap").await;
    fixture.source.execute_test_host_write(
        "UPDATE notes SET title = 'Temporary swap title' WHERE id = 'shared'; \
         UPDATE notes SET title = 'Original title', _updated_at = '0000000002000-0000-owner' WHERE id = 'second'; \
         UPDATE notes SET title = 'Second title', _updated_at = '0000000002000-0000-owner' WHERE id = 'shared'",
    ).await;
    assert_child_rows(&fixture.source, "owner after the captured swap").await;
    let database = StoreDatabase::new(&fixture.source);
    let mut writer = fixture
        .owner
        .authorize_writer()
        .await
        .expect("authorize swap");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare captured swap"));
    drop(writer);
    assert_child_rows(&fixture.source, "owner after candidate preparation").await;
    let original = database
        .active_store_publication()
        .await
        .expect("read swap reservation")
        .expect("swap reserved");
    if snapshot {
        fixture.snapshot_peer_edit(false).await;
    } else {
        RebaseFixture::capture_host_edit(&fixture.target, &fixture.routing,
            "UPDATE notes SET body = 'Peer changed body', _updated_at = '0000000003000-0000-peer' WHERE id = 'shared'",
        ).await;
        RebaseFixture::publish(&fixture.peer).await;
    }
    assert_child_rows(&fixture.target, "peer after its publication").await;
    fixture
        .owner
        .pull_store()
        .await
        .expect("replay captured swap without repeating cascade effects");
    let awaiting = database
        .active_store_publication()
        .await
        .expect("read replacement")
        .expect("replacement reserved");
    assert_eq!(awaiting.is_awaiting_preparation(), snapshot);
    assert_eq!(awaiting.commit_reservation(), original.commit_reservation());
    assert_constrained_rows(&fixture.source, "owner after pulling peer history").await;
    let mut writer = fixture.owner.authorize_writer().await.expect("resume swap");
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("publish rebased swap"),
        1
    );
    drop(writer);
    assert_constrained_rows(&fixture.source, "owner after replacement publication").await;
    fixture
        .peer
        .pull_store()
        .await
        .expect("peer applies replacement swap");
    assert_constrained_rows(&fixture.target, "peer after replacement pull").await;
}

async fn assert_constrained_rows(database: &coven_database::Database, state: &str) {
    assert_child_rows(database, state).await;
    assert_eq!(
        database
            .query_test_text("SELECT title FROM notes WHERE id = 'shared'")
            .await,
        "Second title"
    );
    assert_eq!(
        database
            .query_test_text("SELECT title FROM notes WHERE id = 'second'")
            .await,
        "Original title"
    );
    assert_eq!(
        database
            .query_test_text("SELECT body FROM notes WHERE id = 'shared'")
            .await,
        "Peer changed body"
    );
    assert_eq!(
        database
            .query_test_text("SELECT note_id FROM note_tags WHERE id = 'first-child'")
            .await,
        "shared"
    );
    assert_eq!(
        database
            .query_test_text("SELECT note_id FROM note_tags WHERE id = 'second-child'")
            .await,
        "second"
    );
}

async fn assert_child_rows(database: &coven_database::Database, state: &str) {
    for child in ["first-child", "second-child"] {
        assert!(
            database
                .test_row_exists(&format!("SELECT id FROM note_tags WHERE id = '{child}'"))
                .await,
            "{state}: child {child} is absent"
        );
    }
}
