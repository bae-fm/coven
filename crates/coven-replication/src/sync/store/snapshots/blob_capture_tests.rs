use crate::sync::test_helpers::{
    open_test_db_with_blob, test_cloud_home, test_store_dir, TestDevice, TestStore,
};
use coven_database::{
    Database, DbError, HostWriteOperation, StoreDatabase, StoreRowWrites, WriteBatch,
};
use coven_keys::encryption::EncryptionService;
use coven_keys::keys::UserKeypair;
use coven_protocol::blob::locator::StoredBlobRef;
use coven_protocol::blob::{CacheFill, Provenance};
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{SnapshotMeta, StoreRootRef};
use coven_protocol::synced_schema::BlobDecl;
use coven_storage::CloudSyncObjectStorage;

fn photo_declaration() -> BlobDecl {
    BlobDecl::new("photos", Provenance::HostProvided, CacheFill::CacheEager)
        .with_id_column("blob_id")
}

async fn insert_shared_photo(database: &Database, row: &str, blob_id: &str, bytes: &[u8]) {
    let mut batch = WriteBatch::new();
    batch.put_blob("photos", blob_id, bytes.to_vec());
    let sql = format!(
        "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES
         ('root-{row}', 'Shared photo', 1, '0000000002000-0000-owner', '2026-09-08');
         INSERT INTO note_photos (id, note_id, kind, blob_id, size, hash, _updated_at, created_at) VALUES
         ('{row}', 'root-{row}', 'image', '{blob_id}', {}, '{}',
          '0000000002000-0000-owner', '2026-09-08')",
        bytes.len(),
        coven_protocol::blob::content_hash(bytes),
    );
    StoreRowWrites::new(StoreDatabase::new(database))
        .execute(
            HostWriteOperation::new(batch, move |sql_context| {
                sql_context.execute_batch(&sql)?;
                Ok::<_, DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("capture shared photo and its source atomically");
}

async fn publish_photo(device: &TestDevice) {
    assert!(device
        .publish_pending_store_database()
        .await
        .expect("publish photo and finish source cleanup"));
}

enum PendingPhotoChange {
    ReplaceContent,
    DeleteRow,
    MakeLocal,
}

#[tokio::test]
async fn snapshot_rejects_blob_plan_for_row_absent_from_captured_image() {
    let directory = test_store_dir();
    let source = open_test_db_with_blob(directory.clone(), photo_declaration());
    let signer = UserKeypair::generate();
    let (store, _) = TestStore::create_with_connection(
        &source,
        directory.clone(),
        "snapshot-mismatched-image-row",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&source, directory, &signer)
        .await
        .expect("bind owner");
    insert_shared_photo(&source, "photo", "accepted-photo", b"accepted bytes").await;
    publish_photo(&owner).await;
    let database = StoreDatabase::new(&source);
    let before = database
        .store_current_publication()
        .await
        .expect("accepted boundary");
    let mut writer = owner.authorize_writer().await.expect("authorize snapshot");
    let mut snapshots = writer.snapshots();
    let cut = snapshots
        .capture_snapshot_cut(Some(&EncryptionService::from_key([42; 32])))
        .await
        .expect("capture accepted photo");
    let image = coven_database::DatabaseImageTest::open(cut.snapshot.image_path_for_test())
        .expect("open captured image for fault injection");
    image
        .execute("DELETE FROM note_photos WHERE id = 'photo'", [])
        .expect("remove captured row while retaining its blob plan");
    drop(image);

    let error = snapshots
        .push_snapshot_cut(cut, "2026-09-08T00:00:01Z".to_string())
        .await
        .expect_err("blob plans must match the image rather than the live database");

    assert!(error.to_string().contains("snapshot blob row"), "{error}");
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("unchanged boundary"),
        before
    );
    assert!(database
        .outbound_snapshot_publication()
        .await
        .expect("snapshot journal")
        .is_none());
    assert!(database
        .active_store_publication()
        .await
        .expect("publication reservation")
        .is_none());
    assert!(
        source
            .test_row_exists("SELECT 1 FROM note_photos WHERE id = 'photo'")
            .await
    );
}

#[tokio::test]
async fn snapshot_preserves_accepted_blob_while_content_replacement_is_unpublished() {
    assert_snapshot_preserves_pending_photo(PendingPhotoChange::ReplaceContent).await;
}

#[tokio::test]
async fn snapshot_preserves_accepted_blob_while_row_deletion_is_unpublished() {
    assert_snapshot_preserves_pending_photo(PendingPhotoChange::DeleteRow).await;
}

#[tokio::test]
async fn snapshot_preserves_accepted_blob_while_move_to_local_is_unpublished() {
    assert_snapshot_preserves_pending_photo(PendingPhotoChange::MakeLocal).await;
}

async fn assert_snapshot_preserves_pending_photo(change: PendingPhotoChange) {
    let source_dir = test_store_dir();
    let source = open_test_db_with_blob(source_dir.clone(), photo_declaration());
    let signer = UserKeypair::generate();
    let (store, storage) = TestStore::create_with_connection(
        &source,
        source_dir.clone(),
        "snapshot-pending-photo",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create photo Store");
    let owner = store
        .bind_device_in(&source, source_dir.clone(), &signer)
        .await
        .expect("bind snapshot author");
    let accepted_bytes = b"accepted photo bytes";
    insert_shared_photo(&source, "photo", "accepted-blob", accepted_bytes).await;
    publish_photo(&owner).await;
    let database = StoreDatabase::new(&source);
    let accepted = database
        .row_blob_ref("note_photos", "photo")
        .await
        .expect("read accepted row binding");
    let accepted_blob = accepted
        .stored()
        .expect("accepted photo is uploaded")
        .clone();
    assert_remote_plaintext(storage.as_ref(), &accepted_blob, accepted_bytes).await;

    let replacement_bytes = b"unpublished replacement bytes";
    let mut batch = WriteBatch::new();
    let sql = match change {
        PendingPhotoChange::ReplaceContent => {
            batch.put_blob("photos", "replacement-blob", replacement_bytes.to_vec());
            format!(
                "UPDATE note_photos SET blob_id = 'replacement-blob', size = {}, hash = '{}',
                 _updated_at = '0000000003000-0000-owner' WHERE id = 'photo'",
                replacement_bytes.len(),
                coven_protocol::blob::content_hash(replacement_bytes),
            )
        }
        PendingPhotoChange::DeleteRow => "DELETE FROM note_photos WHERE id = 'photo'".to_string(),
        PendingPhotoChange::MakeLocal => {
            "UPDATE notes SET shared = 0, _updated_at = '0000000003000-0000-owner'
             WHERE id = 'root-photo'"
                .to_string()
        }
    };
    let pending = StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(batch, move |sql_context| {
                sql_context.execute_batch(&sql)?;
                Ok::<_, DbError>(())
            }),
            None,
            Some(Box::new(owner.host_write_blob_staging())),
        )
        .await
        .expect("capture unpublished photo change");
    let pending_status = database
        .write_status(&pending.write_id)
        .await
        .expect("read pending write status");
    let pending_before = database
        .store_write_capture_for_test(pending.write_id.clone())
        .await
        .expect("read original pending capture");
    let rows_before = source.query_test_text(PHOTO_ROWS).await;
    let bindings_before = database
        .row_blob_bindings_for_test()
        .await
        .expect("read exact live bindings");

    let mut writer = owner.authorize_writer().await.expect("authorize snapshot");
    let mut snapshots = writer.snapshots();
    let cut = snapshots
        .capture_snapshot_cut(Some(&EncryptionService::from_key([42; 32])))
        .await
        .expect("capture accepted photo despite unpublished change");
    let published = snapshots
        .push_snapshot_cut(cut, "2026-09-08T00:00:01Z".to_string())
        .await
        .expect("publish accepted photo without changing unpublished work");
    let image = read_published_image(storage.as_ref(), &store.root(), &published).await;
    assert_image_photo_bindings(&image, &[("photo", &accepted_blob)]);
    assert_remote_plaintext(storage.as_ref(), &accepted_blob, accepted_bytes).await;
    assert_eq!(
        source.query_test_text(PHOTO_ROWS).await,
        rows_before,
        "snapshot settlement preserves unpublished rows and audience"
    );
    assert_eq!(
        database
            .row_blob_bindings_for_test()
            .await
            .expect("read exact live bindings"),
        bindings_before,
        "snapshot settlement does not install accepted bindings over live pending state"
    );
    assert_eq!(
        database
            .store_write_capture_for_test(pending.write_id.clone())
            .await
            .expect("read surviving pending capture"),
        pending_before,
        "snapshot settlement preserves pending write identity, status, and blob facts"
    );
    assert_eq!(
        database
            .write_status(&pending.write_id)
            .await
            .expect("read surviving write"),
        pending_status
    );
    if matches!(change, PendingPhotoChange::ReplaceContent) {
        assert_eq!(
            coven_foundation::store_dir::StoreDir::read_local_blob(
                &source_dir,
                "photos",
                "replacement-blob",
                replacement_bytes.len() as u64,
            )
            .await
            .expect("read unpublished replacement source"),
            Some(replacement_bytes.to_vec())
        );
    }
}

#[tokio::test]
async fn snapshot_preserves_distinct_exact_objects_with_identical_blob_content() {
    let source_dir = test_store_dir();
    let source = open_test_db_with_blob(source_dir.clone(), photo_declaration());
    let signer = UserKeypair::generate();
    let (store, storage) = TestStore::create_with_connection(
        &source,
        source_dir.clone(),
        "snapshot-distinct-objects",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let peer_dir = test_store_dir();
    let peer_database = open_test_db_with_blob(peer_dir.clone(), photo_declaration());
    let peer = store
        .activate_joined_device(
            &source,
            source_dir.clone(),
            &peer_database,
            peer_dir,
            &signer,
            "2026-09-08T00:00:00Z",
        )
        .await
        .expect("activate independent uploader");
    let owner = store
        .bind_device_in(&source, source_dir, &signer)
        .await
        .expect("bind owner");
    let (_, pulled) = owner.pull_store().await.expect("observe peer activation");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");

    let bytes = b"same content from independent uploaders";
    insert_shared_photo(&source, "photo-a", "same-blob", bytes).await;
    insert_shared_photo(&peer_database, "photo-b", "same-blob", bytes).await;
    publish_photo(&owner).await;
    let (_, pulled) = peer
        .pull_store()
        .await
        .expect("observe accepted owner photo");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    publish_photo(&peer).await;
    let (_, pulled) = owner
        .pull_store()
        .await
        .expect("observe accepted peer photo");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let database = StoreDatabase::new(&source);
    let row_a = database
        .row_blob_ref("note_photos", "photo-a")
        .await
        .expect("first binding");
    let row_b = database
        .row_blob_ref("note_photos", "photo-b")
        .await
        .expect("second binding");
    let object_a = row_a.stored().expect("first accepted upload");
    let object_b = row_b.stored().expect("second accepted upload");
    assert_ne!(
        object_a.object(),
        object_b.object(),
        "independent uploads have distinct exact identity"
    );
    assert_eq!(object_a.locator().blob_id(), object_b.locator().blob_id());
    assert_eq!(
        object_a.locator().plaintext_hash(),
        object_b.locator().plaintext_hash()
    );
    let live_bindings = database
        .row_blob_bindings_for_test()
        .await
        .expect("read exact live bindings");

    let mut writer = owner.authorize_writer().await.expect("authorize snapshot");
    let mut snapshots = writer.snapshots();
    let cut = snapshots
        .capture_snapshot_cut(Some(&EncryptionService::from_key([42; 32])))
        .await
        .expect("capture distinct accepted objects");
    let published = snapshots
        .push_snapshot_cut(cut, "2026-09-08T00:00:01Z".to_string())
        .await
        .expect("publish without substituting content-equivalent objects");
    let image = read_published_image(storage.as_ref(), &store.root(), &published).await;
    assert_image_photo_bindings(&image, &[("photo-a", object_a), ("photo-b", object_b)]);
    assert_remote_plaintext(storage.as_ref(), object_a, bytes).await;
    assert_remote_plaintext(storage.as_ref(), object_b, bytes).await;
    assert_eq!(
        database
            .row_blob_bindings_for_test()
            .await
            .expect("read exact live bindings"),
        live_bindings
    );

    let retained_a = read_remote_record(&source, object_a).await;
    let retained_b = read_remote_record(&source, object_b).await;
    let successor_cut = snapshots
        .capture_snapshot_cut(Some(&EncryptionService::from_key([42; 32])))
        .await
        .expect("capture the next accepted snapshot");
    let successor = snapshots
        .push_snapshot_cut(successor_cut, "2026-09-08T00:00:02Z".to_string())
        .await
        .expect("transfer reused objects to the accepted successor");
    let accepted_successor = database
        .latest_local_store_snapshot()
        .await
        .expect("read accepted successor")
        .expect("successor was published");
    assert_eq!(accepted_successor.meta, successor);
    let expected_owner = coven_protocol::remote_object::SnapshotObjectOwner::Store {
        metadata_slot: accepted_successor.reference.object.slot().clone(),
    };
    for (stored, before) in [(object_a, retained_a), (object_b, retained_b)] {
        let after = read_remote_record(&source, stored).await;
        assert_eq!(
            after.snapshot_owners().collect::<Vec<_>>(),
            [&expected_owner],
            "reused objects retain only their current accepted Store snapshot owner"
        );
        assert_eq!(
            after.stored_blob_commit_owners(),
            before.stored_blob_commit_owners()
        );
        assert_eq!(
            after.retained_replay_owners().collect::<Vec<_>>(),
            before.retained_replay_owners().collect::<Vec<_>>()
        );
        assert_remote_plaintext(storage.as_ref(), stored, bytes).await;
    }
    let image = read_published_image(storage.as_ref(), &store.root(), &successor).await;
    assert_image_photo_bindings(&image, &[("photo-a", object_a), ("photo-b", object_b)]);
    assert_eq!(
        database
            .row_blob_bindings_for_test()
            .await
            .expect("read exact live bindings"),
        live_bindings
    );
}

#[tokio::test]
async fn snapshot_missing_accepted_blob_preserves_publication_for_retry_after_restart() {
    let temporary = tempfile::tempdir().expect("create persistent test Store directory");
    let directory = coven_foundation::store_dir::StoreDir::new_ephemeral(temporary.path());
    directory.ensure_created().expect("create Store layout");
    let path = directory.db_path();
    let open = || {
        Database::open_synthetic_for_test(
            &path,
            directory.clone(),
            crate::sync::test_helpers::test_synced_tables_with_blob(photo_declaration()),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "snapshot-retry-owner".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &crate::sync::test_helpers::test_migrations(),
        )
        .expect("open persistent Store database")
    };
    let source = open();
    let signer = UserKeypair::generate();
    let home = test_cloud_home();
    let (store, storage) = TestStore::create_with_connection(
        &source,
        directory.clone(),
        "snapshot-missing-accepted-blob",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let root = store.root();
    let owner = store
        .bind_device_in(&source, directory.clone(), &signer)
        .await
        .expect("bind owner");
    let bytes = b"accepted photo";
    insert_shared_photo(&source, "photo", "accepted-blob", bytes).await;
    publish_photo(&owner).await;
    let database = StoreDatabase::new(&source);
    let blob = database
        .row_blob_ref("note_photos", "photo")
        .await
        .expect("read accepted row")
        .stored()
        .expect("accepted row has exact remote object")
        .clone();
    let stored_bytes = home.stored_exact_object(blob.object().slot());
    let before = database
        .store_current_publication()
        .await
        .expect("accepted boundary");
    {
        let mut writer = owner.authorize_writer().await.expect("authorize snapshot");
        let mut snapshots = writer.snapshots();
        let cut = snapshots
            .capture_snapshot_cut(Some(&EncryptionService::from_key([42; 32])))
            .await
            .expect("capture accepted image");
        home.remove_exact_object(blob.object().slot());
        let failure = snapshots
            .push_snapshot_cut(cut, "2026-09-08T00:00:01Z".to_string())
            .await
            .expect_err("snapshot cannot publish with a missing accepted payload");
        assert!(
            matches!(
                failure,
                super::SnapshotError::Bucket(coven_protocol::objects::StorageError::NotFound(_))
            ),
            "{failure}"
        );
    }
    let pending = database
        .outbound_snapshot_publication()
        .await
        .expect("read failed publication")
        .expect("publication remains durable");
    assert_eq!(pending.blobs.len(), 1);
    assert_eq!(pending.blobs[0].bindings[0].blob(), &blob);
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("unchanged boundary"),
        before
    );
    assert!(database
        .latest_local_store_snapshot()
        .await
        .expect("read published snapshot")
        .is_none());
    assert!(!home.contains_exact_object(&pending.reference.object));
    let pending_reference = pending.reference.clone();
    let pending_image = pending.meta.value.image.clone();
    drop(pending);

    StoreRowWrites::new(database.clone())
        .execute(
            HostWriteOperation::new(WriteBatch::new(), |sql| {
                sql.execute_batch("DELETE FROM note_photos WHERE id = 'photo'")?;
                Ok::<_, DbError>(())
            }),
            None,
            None,
        )
        .await
        .expect("capture a later local deletion while publication is pending");
    let local_rows = source.query_test_text(PHOTO_ROWS).await;
    let local_bindings = database
        .row_blob_bindings_for_test()
        .await
        .expect("read exact live bindings");
    drop(owner);
    drop(store);
    drop(database);
    drop(source);

    let reopened = open();
    let database = StoreDatabase::new(&reopened);
    let store = crate::sync::store::Store::load(
        database.clone(),
        storage.clone(),
        directory.clone(),
        signer,
        Some(EncryptionService::from_key([42; 32])),
    )
    .await
    .expect("load Store after database restart");
    let mut writer = store
        .authorize_writer()
        .await
        .expect("authorize durable retry");
    let error = writer
        .resume_snapshot_publication()
        .await
        .expect_err("restart cannot hide the missing accepted blob");
    assert!(
        matches!(
            error,
            super::SnapshotError::Bucket(coven_protocol::objects::StorageError::NotFound(_))
        ),
        "{error}"
    );
    let retained = database
        .outbound_snapshot_publication()
        .await
        .expect("read retry journal")
        .expect("failed retry retains candidate");
    assert_eq!(retained.reference, pending_reference);
    assert_eq!(retained.meta.value.image, pending_image);
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("retry boundary"),
        before
    );

    home.restore_exact_object(blob.object().slot(), stored_bytes);
    let published = writer
        .resume_snapshot_publication()
        .await
        .expect("retry with restored accepted payload")
        .expect("publish retained snapshot");
    assert_eq!(published.image, pending_image);
    assert_eq!(
        database
            .latest_local_store_snapshot()
            .await
            .expect("read accepted snapshot")
            .expect("accepted snapshot exists")
            .reference,
        pending_reference
    );
    assert!(database
        .outbound_snapshot_publication()
        .await
        .expect("settled journal")
        .is_none());
    assert!(database
        .active_store_publication()
        .await
        .expect("settled reservation")
        .is_none());
    assert_eq!(reopened.query_test_text(PHOTO_ROWS).await, local_rows);
    assert_eq!(
        database
            .row_blob_bindings_for_test()
            .await
            .expect("read exact live bindings"),
        local_bindings
    );
    let image = read_published_image(storage.as_ref(), &root, &published).await;
    assert_image_photo_bindings(&image, &[("photo", &blob)]);
    assert_remote_plaintext(storage.as_ref(), &blob, bytes).await;
}

async fn read_remote_record(
    source: &Database,
    stored: &StoredBlobRef,
) -> coven_protocol::remote_object::RemoteObjectRecord {
    source
        .remote_object_for_test(stored.object().clone())
        .await
        .expect("read retained exact object ownership")
}

const PHOTO_ROWS: &str = "SELECT json_array(
    (SELECT json_group_array(json_array(id, shared, _updated_at)) FROM (SELECT * FROM notes ORDER BY id)),
    (SELECT json_group_array(json_array(id, note_id, blob_id, size, hash, _updated_at))
     FROM (SELECT * FROM note_photos ORDER BY id)))";

async fn read_published_image(
    storage: &dyn CloudSyncObjectStorage,
    root: &StoreRootRef,
    snapshot: &SnapshotMeta,
) -> Vec<u8> {
    storage
        .read_protocol_object(
            &ProtocolObjectContext::store_encrypted(
                root.store_root_hash,
                ProtocolObjectDomain::StoreSnapshotImage,
            ),
            &snapshot.image.object,
            &coven_protocol::store_commit::semantic_prefix_from_exact_object(
                &snapshot.image.object,
                ".db",
            )
            .expect("validated image path"),
        )
        .await
        .expect("read exact published image")
}

fn assert_image_photo_bindings(bytes: &[u8], expected: &[(&str, &StoredBlobRef)]) {
    let image = coven_database::DatabaseImageTest::from_bytes(bytes).expect("open published image");
    let count: i64 = image
        .query_row("SELECT COUNT(*) FROM note_photos", [], |row| row.get(0))
        .expect("count snapshot photos");
    assert_eq!(
        count,
        i64::try_from(expected.len()).expect("row count fits SQLite")
    );
    for (row_id, stored) in expected {
        let (blob_id, size, hash, shared, row_stamp): (String, i64, String, bool, String) = image
            .query_row(
                "SELECT p.blob_id, p.size, p.hash, n.shared, p._updated_at FROM note_photos p
             JOIN notes n ON n.id = p.note_id WHERE p.id = ?1",
                [row_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .expect("read accepted snapshot row");
        assert!(shared, "snapshot retains accepted Store audience");
        assert_eq!(blob_id, stored.locator().blob_id());
        assert_eq!(
            u64::try_from(size).expect("nonnegative blob size"),
            stored.locator().plaintext_size()
        );
        assert_eq!(hash, stored.locator().plaintext_hash().to_string());
        let remote = image
            .row_blob_remote_object("note_photos", row_id, "blob_id", &row_stamp)
            .expect("snapshot carries the exact binding at the accepted row stamp");
        remote.validate().expect("valid snapshot object ownership");
        assert_eq!(remote.object(), stored.object());
        assert_eq!(
            remote.snapshot_owners().count(),
            1,
            "snapshot owns the referenced exact object"
        );
    }
}

async fn assert_remote_plaintext(
    storage: &dyn CloudSyncObjectStorage,
    blob: &StoredBlobRef,
    bytes: &[u8],
) {
    let directory = tempfile::tempdir().expect("create plaintext inspection directory");
    let destination = directory.path().join("photo");
    let stage = coven_foundation::store_dir::StoreDir::new_ephemeral(directory.path())
        .stage_atomic_file(&destination)
        .await
        .expect("stage plaintext inspection");
    let plaintext = storage
        .stage_verified_store_blob_plaintext(
            blob,
            stage,
            coven_storage::cloud::no_download_progress(),
        )
        .await
        .expect("open exact accepted photo object");
    assert_eq!(
        tokio::fs::read(plaintext.path())
            .await
            .expect("read photo bytes"),
        bytes
    );
    plaintext
        .commit()
        .await
        .expect("finish plaintext inspection");
}
