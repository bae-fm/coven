use super::{test_synced_tables, RebaseFixture};
use coven_database::{DbError, HostWriteOperation, StoreDatabase, StoreRowWrites, WriteBatch};

#[tokio::test]
async fn snapshot_rebase_reports_the_write_whose_circle_was_deleted() {
    let mut tables = test_synced_tables();
    tables.push(
        coven_protocol::synced_schema::SyncedTable::new(
            "documents",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )
        .scoped_by("audience"),
    );
    let migrations = vec![coven_database::Migration::run(
        1,
        "Circle documents",
        |schema| {
            coven_database::synthetic_store::create_synced_schema(schema)?;
            schema
                .execute_batch(
                    "CREATE TABLE documents (
                id TEXT PRIMARY KEY,
                audience TEXT,
                title TEXT NOT NULL,
                _updated_at TEXT NOT NULL
            ) STRICT;",
                )
                .map_err(DbError::from)
        },
    )];
    let fixture = RebaseFixture::with_schema(tables, migrations).await;
    let database = StoreDatabase::new(&fixture.source);
    let stamp = database.stamp().to_string();
    let circle = fixture
        .owner
        .create_circle(&stamp, "Recorded Circle")
        .await
        .expect("create Circle before row capture");
    fixture
        .peer
        .pull_store()
        .await
        .expect("peer observes Circle");
    let captured = StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(WriteBatch::new(), move |sql| {
                sql.execute_batch(&format!(
                    "UPDATE notes SET title = 'Mixed Store edit', \
                     _updated_at = '0000000002000-0000-owner' WHERE id = 'shared'; \
                     INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
                     ('private', 'Mixed private edit', 0, '0000000002000-0000-owner', '2026-01-01'); \
                     INSERT INTO documents (id, audience, title, _updated_at) VALUES \
                     ('recorded-document', '{circle}', 'Mixed Circle edit', '0000000002000-0000-owner')"
                ))?;
                Ok::<_, DbError>(())
            }),
            Some(fixture.routing.clone()),
            None,
        )
        .await
        .expect("capture one atomic Store/Circle/private write");
    let mut writer = fixture
        .owner
        .authorize_writer()
        .await
        .expect("authorize mixed write");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare mixed write"));
    drop(writer);
    let active = database
        .active_store_publication()
        .await
        .expect("original reservation");
    let boundary = database
        .store_current_publication()
        .await
        .expect("original boundary");
    fixture
        .peer
        .delete_circle(circle)
        .await
        .expect("accept Circle deletion");
    fixture.snapshot_peer_edit(false).await;
    let mut writer = fixture
        .owner
        .authorize_writer()
        .await
        .expect("resume recorded write");
    let error = writer
        .drain_store_writes()
        .await
        .expect_err("deleted Circle blocks captured write");
    drop(writer);
    assert!(
        format!("{error:?}").contains("WriteRebaseConflict"),
        "{error:?}"
    );
    let status = database
        .write_status(&captured.write_id)
        .await
        .expect("affected write status");
    assert!(
        matches!(status,
            coven_protocol::write::WriteStatus::Blocked(coven_protocol::write::WriteBlock::RebaseConflict(ref conflict))
                if conflict.write_id == captured.write_id
                    && conflict.reason == coven_protocol::write::WriteRebaseConflictReason::InvalidCircleContext { circle_id: circle }
                    && conflict.affected_rows.iter().any(|row| row.table == "documents" && row.primary_key == "recorded-document")
        ),
        "{status:?}"
    );
    assert_eq!(
        database
            .active_store_publication()
            .await
            .expect("preserved reservation"),
        active
    );
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("preserved boundary"),
        boundary
    );
    assert_eq!(
        fixture
            .source
            .query_test_text("SELECT title FROM notes WHERE id = 'shared'")
            .await,
        "Mixed Store edit"
    );
    assert_eq!(
        fixture
            .source
            .query_test_text("SELECT title FROM notes WHERE id = 'private'")
            .await,
        "Mixed private edit"
    );
    assert_eq!(
        fixture
            .source
            .query_test_text("SELECT title FROM documents WHERE id = 'recorded-document'")
            .await,
        "Mixed Circle edit"
    );
}
