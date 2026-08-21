use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use super::*;
use coven_database::Database;
use coven_database::StoreDatabase;
use coven_storage::cloud::test_utils::InMemoryCloudHome;
use coven_storage::{BlobPathScheme, CloudCipher, CloudSyncConnection};

fn open(path: &Path, device_id: &str) -> (Database, coven_foundation::store_dir::StoreDir) {
    let store_dir = crate::sync::test_helpers::store_dir_for_test_database(path);
    let database = Database::open_synthetic_for_test(
        path,
        store_dir.clone(),
        crate::sync::test_helpers::test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        device_id.to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &crate::sync::test_helpers::test_migrations(),
    )
    .expect("open snapshot test database");
    (database, store_dir)
}

fn store_database(database: &Database) -> StoreDatabase {
    StoreDatabase::new(database)
}

fn storage(home: &InMemoryCloudHome, signer: &UserKeypair) -> Arc<CloudSyncConnection> {
    Arc::new(CloudSyncConnection::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "snapshot-exact-store",
        signer.clone(),
    ))
}

async fn initialize(
    db: &Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    storage: &Arc<CloudSyncConnection>,
    signer: &UserKeypair,
) -> crate::sync::test_helpers::TestDevice {
    crate::sync::test_helpers::TestDevice::create(
        db,
        db_store_dir.clone(),
        storage.clone(),
        "snapshot-exact-store",
        signer.clone(),
    )
    .await
    .expect("create snapshot test Store")
}

fn snapshot(bytes: &[u8]) -> CreatedSnapshot {
    CreatedSnapshot::new(
        crate::sync::test_helpers::staged_snapshot_image(bytes),
        Vec::new(),
    )
}

#[tokio::test]
async fn selector_keeps_semantic_and_stored_snapshot_hashes_distinct() {
    Box::pin(async {
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let signer = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &db,
            db_store_dir.clone(),
            "snapshot-selector-hash-domains",
            signer.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create exact snapshot selector Store");
        let device = store
            .open_into(&db, db_store_dir.clone())
            .await
            .expect("open exact snapshot selector Store");
        let membership = device
            .membership_for_test()
            .await
            .expect("load exact snapshot selector membership");
        let published = device
            .authorize_writer()
            .await
            .expect("authorize exact snapshot selector writer")
            .snapshots()
            .push_store_snapshot(
                snapshot(b"snapshot selector image"),
                CommitFrontier(BTreeMap::new()),
                1,
                "2026-07-16T00:00:00Z".to_string(),
            )
            .await
            .expect("publish exact snapshot selector fixture");
        device
            .stage_acknowledgement(
                CommitFrontier(BTreeMap::new()),
                "2026-07-16T00:00:01Z".to_string(),
            )
            .await
            .expect("stage exact snapshot selector acknowledgement");
        device
            .drain_acknowledgements()
            .await
            .expect("activate exact snapshot selector acknowledgement");

        let destination = tempfile::tempdir().expect("snapshot selector destination");
        let database_path = destination.path().join("store.db");
        let selected = store
            .prepare_snapshot_bootstrap(
                &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
                1,
                &database_path,
                &signer,
            )
            .await
            .expect("select verified exact snapshot");

        assert_eq!(
            selected.selected_snapshot_hash_for_test(),
            published.snapshot_hash()
        );
        assert_ne!(
            selected.selected_snapshot_hash_for_test(),
            selected.selected_snapshot_object_hash_for_test(),
        );
        assert_eq!(
            selected
                .staged_database_bytes_for_test()
                .expect("read selected snapshot image"),
            b"snapshot selector image"
        );
    })
    .await;
}

#[tokio::test]
async fn staged_snapshot_reuses_image_and_metadata_objects_after_restart() {
    let directory = tempfile::tempdir().expect("snapshot database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(&path, "snapshot-test-device");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    home.fail_exact_create_before_call(1);
    assert!(device
        .publish_snapshot_at(
            b"restart image".to_vec(),
            CommitFrontier(BTreeMap::new()),
            "2026-07-16T00:00:00Z",
        )
        .await
        .is_err());
    let staged = store_database(&db)
        .outbound_snapshot_publication()
        .await
        .expect("read snapshot outbox")
        .expect("staged snapshot exists");
    drop(device);
    drop(db);

    let (reopened, reopened_store_dir) = open(&path, "snapshot-test-device");
    let reopened_device = crate::sync::test_helpers::TestDevice::load(
        &reopened,
        reopened_store_dir.clone(),
        storage.clone(),
        signer.clone(),
    )
    .await
    .expect("reopen snapshot test Store");
    let published = reopened_device
        .resume_snapshot_publication()
        .await
        .expect("resume snapshot publication")
        .expect("snapshot was pending");
    assert_eq!(published.snapshot_hash(), staged.reference.snapshot_hash);
    assert_eq!(published.image, staged.meta.value.image);
    assert!(store_database(&reopened)
        .outbound_snapshot_publication()
        .await
        .expect("read drained snapshot outbox")
        .is_none());
}

#[tokio::test]
async fn exact_snapshot_loader_rejects_a_tampered_continuation_reference() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(Path::new(":memory:"), "snapshot-test-device");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    assert!(coven_database::StoreDatabase::new(&db)
        .export_activated_device_continuation(&signer)
        .await
        .expect("export continuation before any snapshot")
        .latest_snapshot
        .is_none());
    device
        .publish_snapshot_at(
            b"continued snapshot".to_vec(),
            CommitFrontier(BTreeMap::new()),
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("publish continued snapshot");
    let published = store_database(&db)
        .latest_local_store_snapshot()
        .await
        .expect("load continued snapshot journal")
        .expect("continued snapshot journal exists");
    assert_eq!(
        coven_database::StoreDatabase::new(&db)
            .export_activated_device_continuation(&signer)
            .await
            .expect("export continuation after snapshot")
            .latest_snapshot,
        Some(published.reference.clone()),
    );
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize continued snapshot writer");
    writer
        .load_own_snapshot_for_test(&published.reference)
        .await
        .expect("load exact continued snapshot");

    let mut wrong_reference = published.reference.clone();
    wrong_reference.generation += 1;
    assert!(writer
        .load_own_snapshot_for_test(&wrong_reference)
        .await
        .is_err());

    let mut wrong_hash = published.reference.clone();
    wrong_hash.snapshot_hash = ObjectHash::digest(b"another snapshot");
    assert!(writer
        .load_own_snapshot_for_test(&wrong_hash)
        .await
        .is_err());

    let mut wrong_author = published.meta.clone();
    wrong_author
        .body_mut()
        .author_registration
        .registration_hash = ObjectHash::digest(b"another author");
    assert!(writer
        .snapshots()
        .verify_own_snapshot_bytes_for_test(&published.reference, &wrong_author.to_bytes())
        .is_err());

    let mut wrong_successor = published.meta;
    wrong_successor.body_mut().successor.next_slot =
        coven_protocol::objects::ObjectSlot::logical("wrong-successor.json".to_string())
            .expect("valid wrong successor slot");
    assert!(writer
        .snapshots()
        .verify_own_snapshot_bytes_for_test(&published.reference, &wrong_successor.to_bytes())
        .is_err());
}

#[tokio::test]
async fn lost_snapshot_image_create_response_is_resolved_before_metadata_creation() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(Path::new(":memory:"), "snapshot-test-device");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    home.fail_exact_create_after_call(1);

    let published = device
        .publish_snapshot_at(
            b"lost response image".to_vec(),
            CommitFrontier(BTreeMap::new()),
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("resolve exact image-create response loss");
    // The image create whose response was lost, then the resumed publication:
    // membership rollup, then metadata.
    assert_eq!(home.exact_create_count(), 3);
    assert_eq!(
        published.image.image_hash,
        ObjectHash::digest(b"lost response image")
    );
    assert!(store_database(&db)
        .outbound_snapshot_publication()
        .await
        .expect("read completed snapshot outbox")
        .is_none());
}

#[tokio::test]
async fn snapshot_image_is_durable_before_metadata_can_be_created() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(Path::new(":memory:"), "snapshot-test-device");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    home.fail_exact_create_before_call(2);

    assert!(device
        .publish_snapshot_at(
            b"ordered image".to_vec(),
            CommitFrontier(BTreeMap::new()),
            "2026-07-16T00:00:00Z",
        )
        .await
        .is_err());
    let pending = store_database(&db)
        .outbound_snapshot_publication()
        .await
        .expect("read retained snapshot outbox")
        .expect("snapshot remains staged");
    let image_hash = pending.meta.value.image.image_hash;
    let stored_hash = pending.image.prepared.reference().stored_hash();
    let rollup_hash = pending.meta.value.membership_rollup.rollup_hash;
    let rollup_stored_hash = pending.rollup.prepared.reference().stored_hash();
    let claims = store_database(&db)
        .outbound_store_snapshot_payload_claims_for_test()
        .await
        .expect("read staged snapshot payload claims");
    assert_eq!(
        claims,
        vec![image_hash, stored_hash, rollup_hash, rollup_stored_hash]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    );
    for hash in [image_hash, stored_hash, rollup_hash, rollup_stored_hash] {
        assert!(store_database(&db)
            .has_payload_for_test(hash)
            .await
            .expect("check staged snapshot payload storage"));
    }
    assert!(home
        .get(pending.image.prepared.reference().slot().logical_key())
        .is_some());
    assert!(home
        .get(pending.reference.object.slot().logical_key())
        .is_none());

    let completed = device
        .resume_snapshot_publication()
        .await
        .expect("retry ordered snapshot publication")
        .expect("snapshot remained pending");
    assert_eq!(completed.snapshot_hash(), pending.reference.snapshot_hash);
    assert!(store_database(&db)
        .outbound_store_snapshot_payload_claims_for_test()
        .await
        .expect("read completed snapshot payload claims")
        .is_empty());
    for hash in [image_hash, stored_hash, rollup_hash, rollup_stored_hash] {
        assert!(!store_database(&db)
            .has_payload_for_test(hash)
            .await
            .expect("check completed snapshot payload storage"));
    }
}

#[tokio::test]
async fn occupied_snapshot_image_slot_blocks_metadata_and_completion() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(Path::new(":memory:"), "snapshot-test-device");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    home.fail_exact_create_before_call(1);
    assert!(device
        .publish_snapshot_at(
            b"collision image".to_vec(),
            CommitFrontier(BTreeMap::new()),
            "2026-07-16T00:00:00Z",
        )
        .await
        .is_err());
    let pending = store_database(&db)
        .outbound_snapshot_publication()
        .await
        .expect("read snapshot outbox")
        .expect("snapshot remains staged");
    let image_slot = pending.image.prepared.reference().slot().clone();
    home.insert_exact_object(image_slot.logical_key(), b"competing image".to_vec());

    assert!(device.resume_snapshot_publication().await.is_err());
    assert_eq!(
        home.get(image_slot.logical_key()),
        Some(b"competing image".to_vec())
    );
    assert!(home
        .get(pending.reference.object.slot().logical_key())
        .is_none());
    assert!(store_database(&db)
        .outbound_snapshot_publication()
        .await
        .expect("read retained snapshot outbox")
        .is_some());
    assert!(store_database(&db)
        .latest_local_store_snapshot()
        .await
        .expect("read unpublished snapshot state")
        .is_none());
}

#[tokio::test]
async fn snapshot_predecessor_and_reserved_successor_form_one_exact_chain() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(Path::new(":memory:"), "snapshot-test-device");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    let first = device
        .publish_snapshot_at(
            b"first image".to_vec(),
            CommitFrontier(BTreeMap::new()),
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("publish first snapshot");
    assert_eq!(first.generation, 0);
    let image_ownership = db
        .remote_object_for_test(first.image.object.clone())
        .await
        .expect("load published snapshot image ownership");
    assert!(matches!(
        image_ownership,
        coven_protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record)
            if matches!(
                &record.identity.domain,
                coven_protocol::remote_object::SharedLiveSetObjectDomain::StoreSnapshotImage {
                    reference
                } if reference == &first.image
            )
    ));
    let first_published = store_database(&db)
        .latest_local_store_snapshot()
        .await
        .expect("read first snapshot")
        .expect("first snapshot exists");
    home.fail_exact_create_before_call(1);
    assert!(device
        .publish_snapshot_at(
            b"second image".to_vec(),
            CommitFrontier(BTreeMap::new()),
            "2026-07-16T00:00:01Z",
        )
        .await
        .is_err());
    let second = store_database(&db)
        .outbound_snapshot_publication()
        .await
        .expect("read second snapshot")
        .expect("second snapshot remains staged");

    assert_eq!(
        second.meta.value.predecessor,
        Some(first_published.reference.clone())
    );
    assert_eq!(
        second.meta.value.successor.predecessor,
        Some(first_published.reference.clone())
    );
    assert_eq!(second.reference.object.slot(), &first.successor.next_slot);
    assert_eq!(second.reference.generation, first.generation + 1);
    device
        .resume_snapshot_publication()
        .await
        .expect("resume second snapshot publication")
        .expect("publish staged second snapshot");
    let published_generations = db
        .table_row_count_for_test(coven_database::DatabaseTestTable::named(
            "published_store_snapshot",
        ))
        .await
        .expect("count published Store snapshot generations");
    assert_eq!(published_generations, 2);
}

/// Choosing the snapshot a device installs does not grow with how many
/// generations the Store has published.
///
/// Following a generation-linked stream costs a read per generation, and a
/// device join did that per owner device before it could weigh the first
/// candidate — a walk of history to answer a question about its newest point.
/// The slots name their own coordinates, so one listing enumerates them, and
/// each owner's newest generation is the only one a store that has not turned
/// it away ever needs read.
#[tokio::test]
async fn installable_snapshot_selection_does_not_grow_with_published_generations() {
    let shallow = installable_selection_reads("selection-shallow", 1).await;
    let deep = installable_selection_reads("selection-deep", 6).await;

    assert_eq!(
        shallow, deep,
        "selecting over six published generations cost {deep} snapshot operations \
         against {shallow} over one, so the selection still follows the stream",
    );
    // One listing of the snapshot prefix, and the one candidate it names as
    // this owner's newest.
    assert_eq!(
        deep, 2,
        "selecting an installable snapshot cost {deep} snapshot operations, not \
         the listing and the newest candidate it names",
    );
}

/// Provider operations one fresh reader spends choosing an installable
/// snapshot, over a Store that has published `generations` of them.
async fn installable_selection_reads(store_id: &str, generations: usize) -> usize {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(Path::new(":memory:"), store_id);
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    for generation in 0..generations {
        device
            .publish_snapshot(
                format!("image for generation {generation}").into_bytes(),
                CommitFrontier(BTreeMap::new()),
            )
            .await
            .expect("publish a Store snapshot generation");
    }

    let root = device.store_root().clone();
    let mut history = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
        .open_pinned(storage.as_ref(), &root)
        .await
        .expect("open the snapshot history authority");
    let founder = history
        .load_founder_registration()
        .await
        .expect("load the founder registration");
    let owners = [(
        coven_protocol::store_commit::StoreDeviceRegistrationRef::from_registration(
            &founder.value,
            founder.object.clone(),
        ),
        founder.value.clone(),
    )];

    home.clear_exact_reads();
    home.clear_exact_listings();
    let selected = Box::pin(
        history.select_listed_installable_store_snapshot(
            owners
                .iter()
                .map(|(registration_ref, registration)| (registration_ref, registration)),
            &mut crate::sync::store::commit_verification::merge_history::weigh_every_snapshot,
        ),
    )
    .await
    .expect("select an installable snapshot")
    .expect("a published snapshot is installable");
    assert_eq!(
        selected.snapshot.reference.generation,
        generations as u64 - 1,
        "the newest published generation is the one selected"
    );

    let counted = |key: &str| key.starts_with("store-v1/snapshots/");
    home.exact_reads()
        .iter()
        .filter(|slot| counted(slot.logical_key()))
        .count()
        + home
            .exact_listed_prefixes()
            .iter()
            .filter(|prefix| counted(prefix))
            .count()
}
