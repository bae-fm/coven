use super::*;
use coven_database::{DbError, HostWriteOperation, StoreDatabase, StoreRowWrites, WriteBatch};
use coven_protocol::blob::locator::StoredBlobRef;

struct BrowsableFixture {
    source: Database,
    source_dir: coven_foundation::store_dir::StoreDir,
    target: Database,
    target_dir: coven_foundation::store_dir::StoreDir,
    store: Arc<TestStore>,
    storage: Arc<CloudSyncConnection>,
    owner: TestDevice,
    peer: TestDevice,
}

impl BrowsableFixture {
    async fn new() -> Self {
        let source_dir = test_store_dir();
        let source = open_test_db_with_blob(source_dir.clone(), replaceable_photo_decl());
        let target_dir = test_store_dir();
        let target = open_test_db_with_blob(target_dir.clone(), replaceable_photo_decl());
        let signer = UserKeypair::generate();
        let (store, storage) = TestStore::create_browsable_with_connection(
            &source,
            source_dir.clone(),
            "browsable-versions",
            signer.clone(),
            test_cloud_home(),
        )
        .await
        .expect("create browsable Store");
        let peer = store
            .activate_joined_device(
                &source,
                source_dir.clone(),
                &target,
                target_dir.clone(),
                &signer,
                "2026-01-01T00:00:00Z",
            )
            .await
            .expect("activate the second device");
        let owner = store
            .bind_device_in(&source, source_dir.clone(), &signer)
            .await
            .expect("bind source device");
        owner.pull_store().await.expect("observe joined device");
        capture(
            &source,
            None,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
             ('n1', 'Attachments', 1, '0000000001000-0000-owner', '2026-01-01')"
                .into(),
        )
        .await;
        publish(&owner).await;
        peer.pull_store().await.expect("pull the shared parent");
        Self {
            source,
            source_dir,
            target,
            target_dir,
            store,
            storage,
            owner,
            peer,
        }
    }

    async fn insert_source(&self, blob_id: &str, path: &str, bytes: &[u8]) -> StoredBlobRef {
        capture(
            &self.source,
            Some((blob_id, bytes)),
            insert_photo("photo", blob_id, path, bytes, "0000000002000-0000-owner"),
        )
        .await;
        publish(&self.owner).await;
        stored(&self.source, "photo").await
    }

    async fn assert_bytes(&self, reference: &StoredBlobRef, expected: &[u8]) {
        assert_eq!(
            self.store.read_exact_blob(&self.storage, reference).await,
            expected,
            "the earlier exact reference still identifies its own bytes"
        );
    }

    async fn assert_peer(&self, reference: &StoredBlobRef, expected: &[u8]) {
        self.peer
            .pull_store()
            .await
            .expect("peer pulls the published row");
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            StoreDatabase::new(&self.target),
            self.target_dir.clone(),
        )
        .fill_eager_cache(self.storage.clone())
        .await
        .expect("peer fills its eager blob cache");
        let actual = self.target.exact_row_blob_ref("note_photos", "photo").await;
        assert_eq!(actual.stored(), Some(reference));
        assert_eq!(
            tokio::fs::read(exact_cache_path(&self.target_dir, &actual))
                .await
                .expect("peer cached the exact row object"),
            expected
        );
    }
}

async fn capture(
    database: &Database,
    blob: Option<(&str, &[u8])>,
    sql: String,
) -> coven_protocol::write::WriteId {
    let mut batch = WriteBatch::new();
    if let Some((id, bytes)) = blob {
        batch.put_blob("photos", id, bytes.to_vec());
    }
    StoreRowWrites::new(StoreDatabase::new(database))
        .execute(
            HostWriteOperation::new(batch, move |transaction| {
                transaction.execute_batch(&sql)?;
                Ok::<_, DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("capture row and its source bytes atomically")
        .write_id
}

async fn publish(device: &TestDevice) {
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize publisher");
    assert!(writer
        .prepare_pending_store_write()
        .await
        .expect("prepare captured write"));
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("publish captured write"),
        1
    );
}

fn insert_photo(row: &str, blob: &str, path: &str, bytes: &[u8], stamp: &str) -> String {
    format!(
        "INSERT INTO note_photos \
         (id, note_id, kind, blob_id, size, hash, cloud_path, _updated_at, created_at) VALUES \
         ('{row}', 'n1', 'attachment', '{blob}', {}, '{}', '{path}', '{stamp}', '2026-01-01')",
        bytes.len(),
        coven_protocol::blob::content_hash(bytes),
    )
}

async fn stored(database: &Database, row: &str) -> StoredBlobRef {
    database
        .exact_row_blob_ref("note_photos", row)
        .await
        .stored()
        .expect("published row has an exact object")
        .clone()
}

#[tokio::test]
async fn readable_path_rename_republishes_remote_content_without_local_bytes() {
    let fixture = BrowsableFixture::new().await;
    let bytes = b"unchanged attachment bytes";
    let previous = fixture
        .insert_source("content", "Original/photo-content.jpg", bytes)
        .await;
    fixture.assert_peer(&previous, bytes).await;
    fixture
        .source_dir
        .remove_local_blob("photos", "content")
        .await
        .expect("remove local source");
    let reference = fixture
        .source
        .exact_row_blob_ref("note_photos", "photo")
        .await;
    let cache = exact_cache_path(&fixture.source_dir, &reference);
    if cache.exists() {
        tokio::fs::remove_file(&cache)
            .await
            .expect("evict source cache");
    }
    let write_id = capture(
        &fixture.source,
        None,
        "UPDATE note_photos SET cloud_path = 'Renamed/photo-content.jpg', \
         _updated_at = '0000000003000-0000-owner' WHERE id = 'photo'"
            .into(),
    )
    .await;
    let captured = StoreDatabase::new(&fixture.source)
        .store_write_capture_for_test(write_id)
        .await
        .expect("read the rename's captured source");
    let facts: coven_database::StoreWriteBlobFacts =
        serde_json::from_str(&captured.2).expect("decode captured blob facts");
    let [fact] = facts.blobs.as_slice() else {
        panic!("rename captures one blob: {facts:?}");
    };
    assert_eq!(
        fact.blob.cloud_path.as_deref(),
        Some("Renamed/photo-content.jpg")
    );
    assert_eq!(
        fact.previous
            .as_ref()
            .expect("rename retains remote plaintext source")
            .stored,
        previous
    );
    assert!(!fixture
        .source_dir
        .local_blob_path("photos", "content")
        .expect("local source path")
        .exists());
    assert!(!cache.exists());
    publish(&fixture.owner).await;
    let renamed = stored(&fixture.source, "photo").await;
    assert_ne!(previous.object(), renamed.object());
    assert_eq!(
        renamed.locator().cloud_path(),
        Some("Renamed/photo-content.jpg")
    );
    fixture.assert_bytes(&previous, bytes).await;
    fixture.assert_bytes(&renamed, bytes).await;
    fixture.assert_peer(&renamed, bytes).await;
}
