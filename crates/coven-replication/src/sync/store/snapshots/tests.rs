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

/// Two owner devices publish snapshots, and the reader takes the one whose
/// coverage reaches furthest — not the one holding the newest generation.
///
/// A single-owner store cannot tell those two apart: with one stream, "widest
/// coverage" and "highest generation" name the same snapshot every time. The
/// round logic in `StoreSnapshotDescent` that descends several authors' streams
/// together, and the coverage domination that decides between them, only do
/// anything once there are several streams, and neither had a test.
#[tokio::test]
async fn selection_takes_the_widest_coverage_across_two_owner_devices() {
    let shallow = two_owner_selection("two-owner-selection-shallow", 1).await;
    let deep = two_owner_selection("two-owner-selection-deep", 4).await;

    assert_eq!(
        shallow.reads, deep.reads,
        "selecting across two owners cost {} snapshot operations over four published \
         rounds against {} over one, so a round still descends the generations under \
         the newest",
        deep.reads, shallow.reads,
    );
    // One listing of the snapshot prefix, the newest generation of each owner's
    // stream, and one older generation of the winner's own stream that
    // verifying the winner reads back. None of the four scales with how many
    // generations either owner has published.
    assert_eq!(
        deep.reads, 4,
        "selecting across two owners cost {} snapshot operations, not the listing, \
         one candidate per owner, and the one generation verifying the winner reads",
        deep.reads,
    );

    for selection in [&shallow, &deep] {
        assert_eq!(
            selection.author,
            SelectedAuthor::Wider,
            "the picker took the narrower owner's snapshot",
        );
        // The narrower owner publishes three snapshots to the wider owner's one,
        // so it also holds the highest generation the store has. Selecting below
        // that is what says coverage decided this and generation did not.
        assert!(
            selection.selected_generation < selection.narrower_newest_generation,
            "the picker took generation {} while the narrower owner held {}, so the \
             two are not being told apart",
            selection.selected_generation,
            selection.narrower_newest_generation,
        );
    }
}

/// What one run of [`two_owner_selection`] observed.
struct TwoOwnerSelection {
    /// Provider operations the selection spent under the snapshot prefix.
    reads: usize,
    author: SelectedAuthor,
    selected_generation: u64,
    /// The newest generation the narrower owner published, which is also the
    /// newest generation in the store.
    narrower_newest_generation: u64,
}

#[derive(Debug, PartialEq, Eq)]
enum SelectedAuthor {
    Wider,
    Narrower,
}

/// Build a store with two owner devices, have each publish over `rounds` with
/// one reaching further than the other, and select over both.
async fn two_owner_selection(store_id: &str, rounds: usize) -> TwoOwnerSelection {
    let home = InMemoryCloudHome::new();
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);

    let (owner_db, owner_dir) = open(Path::new(":memory:"), store_id);
    let (store, connection) = crate::sync::test_helpers::TestStore::create_with_connection(
        &owner_db,
        owner_dir.clone(),
        store_id,
        owner.clone(),
        Arc::new(home.clone()),
    )
    .await
    .expect("create the two-owner test Store");

    let member_pubkey = coven_keys::keys::public_key_hex(&member);
    store
        .admit_member(
            &owner_db,
            owner_dir.clone(),
            &owner,
            &member_pubkey,
            None,
            coven_protocol::membership::MemberRole::Member,
            &encryption,
            "Two Owner Store",
        )
        .await
        .expect("admit the second device's identity");
    let (member_db, member_dir) = open(Path::new(":memory:"), &format!("{store_id}-second"));
    store
        .activate_joined_device(
            &owner_db,
            owner_dir.clone(),
            &member_db,
            member_dir.clone(),
            &member,
            "2026-07-21T00:00:00Z",
        )
        .await
        .expect("activate the second device");
    store
        .promote_active_member_fixture(
            &owner_db,
            owner_dir.clone(),
            &member_db,
            member_dir.clone(),
            &owner,
            &member,
            &encryption,
        )
        .await
        .expect("promote the second device to Owner");

    // Both owners publish, round for round. The first owner writes a commit
    // before each of its snapshots and the second never pulls again, so the
    // first owner's coverage runs ahead of the second's and strictly dominates
    // it — which is what makes the two candidates comparable at all rather than
    // settled by the snapshot-hash tie-break.
    let first = store
        .bind_device_in(&owner_db, owner_dir.clone(), &owner)
        .await
        .expect("bind the first owner device");
    let second = store
        .bind_device_in(&member_db, member_dir.clone(), &member)
        .await
        .expect("bind the second owner device");
    let mut narrower_newest_generation = 0;
    for round in 0..rounds {
        let changeset = owner_db
            .capture_test_changeset(&[&format!(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n{round}', 'Round {round}', NULL, 1, \
                 '000000000{}000-0000-dev1', '2026-01-01')",
                round + 1,
            )])
            .await;
        let previous = first
            .latest_local_store_position()
            .await
            .expect("read the first owner's Store position")
            .map_or(0, |position| position.coord.sequence());
        first
            .publish_changeset_after_for_test(changeset, previous)
            .await
            .expect("publish a commit the first owner's snapshot covers");
        first
            .publish_snapshot(
                format!("first owner image {round}").into_bytes(),
                CommitFrontier::from_refs(
                    first
                        .materialized_frontier()
                        .await
                        .expect("read the first owner's frontier"),
                )
                .expect("the materialized frontier names valid author streams"),
            )
            .await
            .expect("publish the first owner's snapshot");
        // Three to the first owner's one, so the narrower owner is also the one
        // holding the store's highest generation.
        for extra in 0..3 {
            let meta = second
                .publish_snapshot(
                    format!("second owner image {round}.{extra}").into_bytes(),
                    CommitFrontier::from_refs(
                        second
                            .materialized_frontier()
                            .await
                            .expect("read the second owner's frontier"),
                    )
                    .expect("the materialized frontier names valid author streams"),
                )
                .await
                .expect("publish the second owner's snapshot");
            narrower_newest_generation = meta.generation;
        }
    }

    let root = first.store_root().clone();
    let mut history = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
        .open_pinned(connection.as_ref(), &root)
        .await
        .expect("open the snapshot history authority");
    let owners = store_database(&owner_db)
        .activated_store_device_registration_records()
        .await
        .expect("load the store's activated device registrations");
    assert_eq!(owners.len(), 2, "the fixture built two owner devices");

    home.clear_exact_reads();
    home.clear_exact_listings();
    let selected = Box::pin(
        history.select_listed_installable_store_snapshot(
            owners
                .iter()
                .map(|registration| (registration.reference(), registration.value())),
            &mut crate::sync::store::commit_verification::merge_history::weigh_every_snapshot,
        ),
    )
    .await
    .expect("select an installable snapshot")
    .expect("a published snapshot is installable");

    let counted = |key: &str| key.starts_with("store-v1/snapshots/");
    TwoOwnerSelection {
        reads: home
            .exact_reads()
            .iter()
            .filter(|slot| counted(slot.logical_key()))
            .count()
            + home
                .exact_listed_prefixes()
                .iter()
                .filter(|prefix| counted(prefix))
                .count(),
        author: if selected.snapshot.meta.author_registration.device_id == first.typed_device_id() {
            SelectedAuthor::Wider
        } else {
            SelectedAuthor::Narrower
        },
        selected_generation: selected.snapshot.reference.generation,
        narrower_newest_generation,
    }
}
