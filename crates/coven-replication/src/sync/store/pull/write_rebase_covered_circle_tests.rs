use super::{test_store_dir, test_synced_tables, RebaseFixture};
use coven_database::{DbError, HostWriteOperation, StoreDatabase, StoreRowWrites, WriteBatch};
use coven_protocol::blob::{CacheFill, Provenance};
use coven_protocol::synced_schema::{BlobDecl, RowIdentity, SyncedTable};
use coven_protocol::write::{PublishedWrite, WriteStatus};
use coven_storage::CloudSyncObjectStorage;

#[tokio::test]
async fn covered_write_cleanup_preserves_a_circle_blob_used_by_its_accepted_replacement() {
    covered_circle_replacement(false).await;
}

#[tokio::test]
async fn covered_circle_restoration_survives_independent_store_package_retirement() {
    covered_circle_replacement(true).await;
}

async fn covered_circle_replacement(retire_store_package: bool) {
    let mut tables = test_synced_tables();
    tables.extend([
        SyncedTable::new("documents", RowIdentity::SharedKey).scoped_by("audience"),
        SyncedTable::new("document_files", RowIdentity::SharedKey)
            .inherits_audience_through("document_id")
            .carries_blob(BlobDecl::new(
                "files",
                Provenance::HostProvided,
                CacheFill::CacheLazy,
            )),
    ]);
    let migrations = vec![coven_database::Migration::run(
        1,
        "Audience-scoped documents with files",
        |schema| {
            coven_database::synthetic_store::create_synced_schema(schema)?;
            schema.execute_batch(
                "CREATE TABLE documents (
                     id TEXT PRIMARY KEY,
                     audience TEXT,
                     title TEXT NOT NULL,
                     _updated_at TEXT NOT NULL
                 ) STRICT;
                 CREATE TABLE document_files (
                     id TEXT PRIMARY KEY,
                     document_id TEXT NOT NULL REFERENCES documents(id),
                     size INTEGER NOT NULL,
                     hash TEXT NOT NULL,
                     _updated_at TEXT NOT NULL
                 ) STRICT;",
            )?;
            Ok(())
        },
    )];
    let fixture = RebaseFixture::with_schema(tables.clone(), migrations.clone()).await;
    let database = StoreDatabase::new(&fixture.source);
    let circle = fixture
        .owner
        .create_circle(&database.stamp().to_string(), "Covered document")
        .await
        .expect("publish Circle authority before capturing the document");
    fixture
        .peer
        .pull_store()
        .await
        .expect("peer observes Circle");

    let bytes = b"Circle file shared by two candidates for the same logical write";
    let mut batch = WriteBatch::new();
    batch.put_blob("files", "covered-file", bytes.to_vec());
    StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(batch, move |sql| {
                sql.execute_batch(&format!(
                    "INSERT INTO documents(id, audience, title, _updated_at)
                     VALUES ('covered-document', 'local', 'Private document',
                             '0000000002000-0000-owner');
                     INSERT INTO document_files(id, document_id, size, hash, _updated_at)
                     VALUES ('covered-file', 'covered-document', {}, '{}',
                             '0000000002000-0000-owner')",
                    bytes.len(),
                    coven_protocol::blob::content_hash(bytes),
                ))?;
                Ok::<_, DbError>(())
            }),
            Some(fixture.routing.clone()),
            None,
        )
        .await
        .expect("capture the private file source");
    let captured = StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(WriteBatch::new(), move |sql| {
                sql.execute_batch(&format!(
                    "UPDATE documents SET audience = '{circle}',
                         _updated_at = '0000000002500-0000-owner'
                     WHERE id = 'covered-document';
                     UPDATE notes SET title = 'Shared Circle document',
                         _updated_at = '0000000002500-0000-owner'
                     WHERE id = 'shared'"
                ))?;
                Ok::<_, DbError>(())
            }),
            Some(fixture.routing.clone()),
            Some(Box::new(fixture.owner.host_write_blob_staging())),
        )
        .await
        .expect("capture one Store and Circle publication");
    let mut writer = fixture.owner.authorize_writer().await.unwrap();
    assert!(writer.prepare_pending_store_write().await.unwrap());
    let original = database
        .oldest_prepared_store_write()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.commit.value.write_id, captured.write_id);
    let original_commit = original.commit.value.reference().clone();
    let original_package = original.audiences.packages[0].object().clone();
    let shared_blob = original.audiences.blobs[0].blob().clone();
    assert_eq!(
        shared_blob.locator().audience(),
        coven_protocol::blob::locator::RemoteAudience::Circle(circle),
    );
    let objects = database
        .prepared_remote_objects(&captured.write_id)
        .await
        .unwrap();
    let package_position = objects
        .iter()
        .position(|object| object.closed.object() == &original_package)
        .expect("original package belongs to the upload manifest");
    fixture
        .store
        .fail_exact_create_before_call(package_position + 1);
    writer
        .drain_store_writes()
        .await
        .expect_err("interrupt package publication while the Circle blob uploads");
    drop(writer);
    database.retire_uploaded_blob_spools().await.unwrap();
    fixture
        .storage
        .verify_blob_object(&shared_blob)
        .await
        .unwrap();

    // A restored directory owns the same durable WriteId and author reservation.
    // It publishes a replacement after the peer advances the accepted baseline.
    let continuation_dir = test_store_dir();
    let continuation_path = continuation_dir.db_path();
    fixture
        .source
        .vacuum_into_for_test(continuation_path.to_str().unwrap().into())
        .await
        .unwrap();
    crate::sync::test_helpers::copy_payload_files(&fixture.source_dir, &continuation_dir);
    let copied_blob = continuation_dir
        .local_blob_path("files", "covered-file")
        .unwrap();
    tokio::fs::create_dir_all(copied_blob.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::copy(
        fixture
            .source_dir
            .local_blob_path("files", "covered-file")
            .unwrap(),
        copied_blob,
    )
    .await
    .unwrap();
    let continued = RebaseFixture::open_schema(
        &continuation_path,
        continuation_dir.clone(),
        tables,
        &migrations,
    );
    fixture.snapshot_peer_edit(false).await;
    let continuation = fixture
        .store
        .bind_device_in(&continued, continuation_dir, &fixture.signer)
        .await
        .unwrap();
    let mut writer = continuation.authorize_writer().await.unwrap();
    assert_eq!(writer.drain_store_writes().await.unwrap(), 1);
    drop(writer);
    let replacement = continuation
        .latest_local_store_position()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replacement.coord, original_commit.coord);
    assert_ne!(replacement.commit_hash, original_commit.commit_hash);
    assert_eq!(
        continuation
            .load_commit_for_test(&replacement)
            .await
            .unwrap()
            .write_id,
        captured.write_id,
    );
    assert_eq!(
        StoreDatabase::new(&continued)
            .row_blob_ref("document_files", "covered-file")
            .await
            .unwrap()
            .stored(),
        Some(&shared_blob),
        "the accepted replacement reuses the original exact Circle blob",
    );
    fixture
        .peer
        .pull_store()
        .await
        .expect("peer installs the accepted Circle package");
    assert_eq!(
        StoreDatabase::new(&fixture.target)
            .row_blob_ref("document_files", "covered-file")
            .await
            .unwrap()
            .stored(),
        Some(&shared_blob),
    );
    fixture
        .peer
        .publish_snapshot_generation_for_test()
        .await
        .unwrap();

    if retire_store_package {
        let accepted = continuation
            .load_commit_for_test(&replacement)
            .await
            .unwrap();
        let store_package = accepted
            .store_package()
            .expect("mixed write has a Store package");
        let [circle_package] = accepted.circle_packages() else {
            panic!("mixed write has exactly one Circle package");
        };
        let reclaimed = fixture
            .peer
            .reclaim_packages()
            .await
            .expect("retire accepted Store packages");
        assert!(
            !fixture.home.contains_exact_object(&store_package.object),
            "the accepted Store package is physically retired before receiver restoration: {reclaimed:?}",
        );
        assert!(
            fixture
                .home
                .contains_exact_object(&circle_package.package.object),
            "the Circle package remains required without a covering Circle image",
        );
        fixture
            .peer
            .publish_snapshot_generation_for_test()
            .await
            .expect("publish the Store receipt while retaining Circle package evidence");
    }

    // Normal received-image preparation resolves the recipient's Circle images
    // before logical completion determines which exact pending objects to release.
    fixture
        .owner
        .pull_store()
        .await
        .expect("install Store coverage and recipient Circle state");
    assert!(
        fixture
            .source
            .circle_document_present_for_test("covered-document")
            .await
            .unwrap(),
        "the received checkpoint installs the accepted Circle document before cleanup"
    );
    fixture.home.clear_exact_creates();
    let mut writer = fixture.owner.authorize_writer().await.unwrap();
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("settle the covered logical write"),
        1
    );
    drop(writer);
    assert!(matches!(
        database.write_status(&captured.write_id).await.unwrap(),
        WriteStatus::Published(receipt) if matches!(receipt.as_ref(), PublishedWrite::Snapshot(_)),
    ));
    assert!(database.active_store_publication().await.unwrap().is_none());
    assert!(
        fixture.home.exact_creates().is_empty(),
        "coverage must not republish the write"
    );
    assert_eq!(
        fixture
            .source
            .query_test_text("SELECT id FROM documents")
            .await,
        "covered-document",
        "logical completion retains the accepted Circle document",
    );
    assert_eq!(
        fixture
            .source
            .query_test_text("SELECT id FROM document_files")
            .await,
        "covered-file",
        "logical completion retains the accepted Circle attachment",
    );
    fixture
        .storage
        .verify_blob_object(&shared_blob)
        .await
        .expect("the peer's accepted Circle blob survives covered candidate cleanup");
}
