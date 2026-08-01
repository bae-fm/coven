use std::path::Path;
use std::sync::Arc;

use super::*;
use crate::database::{Database, StoreDatabase};
use crate::keys::UserKeypair;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};

fn open(path: &Path, device_id: &str) -> Database {
    Database::open(
        path,
        crate::sync::test_helpers::test_synced_tables(),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        device_id.to_string(),
        Arc::new(crate::clock::SystemClock),
        &crate::sync::test_helpers::test_migrations(),
    )
    .expect("open Circle snapshot test database")
    .0
}

fn store_database(database: &Database) -> StoreDatabase {
    StoreDatabase::new(database)
}

fn storage(home: &InMemoryCloudHome, signer: &UserKeypair) -> Arc<CloudSyncStorage> {
    Arc::new(
        CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "circle-snapshot-store",
            signer.clone(),
        )
        .expect("construct Circle snapshot test storage"),
    )
}

async fn initialize(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    signer: &UserKeypair,
) -> (StoreRootRef, String) {
    let initialized = crate::sync::store::Store::create(
        store_database(db),
        storage.clone(),
        "circle-snapshot-store",
        signer,
    )
    .await
    .expect("create Circle snapshot test Store");
    let root_ref = initialized.store.store_root().clone();
    let origin = crate::protocol::store_commit::StoreDeviceRegistrationOrigin::Founder {
        creation_id: initialized
            .store
            .protocol_root_for_test()
            .descriptor
            .creation_id,
    };
    let device_id = crate::protocol::store_commit::StoreDeviceId::derive(&root_ref, &origin);
    (root_ref, device_id.to_string())
}

#[tokio::test]
async fn circle_snapshot_authors_and_installs_as_a_bootstrap_image() {
    let directory = tempfile::tempdir().expect("snapshot database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(&path, "circle-snapshot-device");
    let (root_ref, _) = initialize(&db, &storage, &signer).await;
    db.test_sql(|database| database.apply_coven_routing_schema())
        .await
        .expect("apply routing schema");
    let (circle_id, control) = db
        .test_sql(|conn| {
            Ok(crate::sync::test_helpers::install_test_active_circle(
                &conn, "snap",
            ))
        })
        .await
        .expect("install active Circle");
    let (encryption, key_fingerprint) = store_database(&db)
        .circle_publication_context(circle_id, control.clone())
        .await
        .expect("resolve Circle publication context");

    crate::sync::store::push_circle_snapshots_for_test(
        &db,
        &storage,
        directory.path().join("snap-temp"),
        db.schema_version(),
        &signer,
        "2026-07-16T00:00:00Z",
        &crate::encryption::EncryptionService::from_key([42; 32]),
    )
    .await
    .expect("author Circle snapshots");

    let stream = crate::sync::store::load_circle_snapshot_metas_for_test(
        &db,
        &storage,
        circle_id,
        encryption.clone(),
        &signer,
    )
    .await
    .expect("load Circle snapshot stream");
    assert_eq!(stream.len(), 1);
    let selected = select_maximal_circle_snapshot(stream).expect("a maximal Circle snapshot");
    assert_eq!(selected.generation, 0);
    assert_eq!(selected.circle_id, circle_id);
    assert_eq!(selected.control, control);
    assert_eq!(selected.key_fingerprint, key_fingerprint);

    let image_context = ProtocolObjectContext::circle(
        root_ref.store_root_hash,
        ProtocolObjectDomain::CircleSnapshotImage,
        encryption.clone(),
    );
    let image = storage
        .read_protocol_object(
            &image_context,
            &selected.bootstrap.image.object,
            &circle_snapshot_image_semantic_prefix(
                circle_id,
                &selected.author_registration.device_id.to_string(),
                selected.bootstrap.image.image_hash,
            ),
        )
        .await
        .expect("read Circle snapshot image");
    let routing_key =
        crate::protocol::circle::derive_row_routing_key(&encryption, root_ref.store_root_hash)
            .expect("derive Circle row routing key");
    verify_circle_bootstrap_image(
        &image,
        &selected.bootstrap,
        circle_id,
        &crate::sync::test_helpers::test_synced_tables(),
        Some(&routing_key),
    )
    .expect("Circle snapshot is installable as a bootstrap image");
}

#[tokio::test]
async fn non_member_cannot_decrypt_circle_snapshot() {
    let directory = tempfile::tempdir().expect("snapshot database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(&path, "circle-snapshot-outsider");
    let (root_ref, device_id) = initialize(&db, &storage, &signer).await;
    db.test_sql(|database| database.apply_coven_routing_schema())
        .await
        .expect("apply routing schema");
    let (circle_id, _control) = db
        .test_sql(|conn| {
            Ok(crate::sync::test_helpers::install_test_active_circle(
                &conn, "snap",
            ))
        })
        .await
        .expect("install active Circle");
    crate::sync::store::push_circle_snapshots_for_test(
        &db,
        &storage,
        directory.path().join("snap-temp"),
        db.schema_version(),
        &signer,
        "2026-07-16T00:00:00Z",
        &crate::encryption::EncryptionService::from_key([42; 32]),
    )
    .await
    .expect("author Circle snapshots");

    let outsider = crate::encryption::EncryptionService::from_key([7u8; 32]);
    let meta_context = ProtocolObjectContext::circle(
        root_ref.store_root_hash,
        ProtocolObjectDomain::CircleSnapshotMeta,
        outsider,
    );
    let prefix =
        crate::protocol::store_commit::circle_snapshot_slot_prefix(circle_id, &device_id, 0);
    let slot =
        crate::storage::cloud::ObjectSlot::logical(format!("{prefix}.json")).expect("gen-0 slot");
    let denied = storage
        .read_protocol_slot(&meta_context, &slot, &prefix)
        .await;
    assert!(denied.is_err());
}
