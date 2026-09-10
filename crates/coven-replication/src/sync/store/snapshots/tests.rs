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

async fn snapshot_image(
    database: &Database,
    root: &coven_protocol::store_commit::StoreRootRef,
) -> Vec<u8> {
    let directory = tempfile::tempdir().expect("snapshot capture directory");
    store_database(database)
        .capture_snapshot_image_for_test(root.clone(), directory.path().to_path_buf(), None)
        .await
        .expect("capture the Store snapshot image")
}

fn snapshot(bytes: &[u8]) -> CreatedSnapshot {
    CreatedSnapshot::new(
        crate::sync::test_helpers::staged_snapshot_image(bytes),
        Vec::new(),
    )
}

#[tokio::test]
async fn retained_snapshot_authority_rejects_changed_device_body_with_unchanged_hash() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, store_dir) = open(Path::new(":memory:"), "snapshot-state-body-binding");
    let device = initialize(&db, store_dir, &storage, &signer).await;
    device
        .publish_snapshot(
            snapshot_image(&db, device.store_root()).await,
            CommitFrontier(BTreeMap::new()),
        )
        .await
        .expect("publish snapshot");
    let snapshot = store_database(&db)
        .latest_local_store_snapshot()
        .await
        .expect("read snapshot")
        .expect("snapshot is published");
    let root = device.store_root().clone();
    let mut history = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
        .open_pinned(storage.as_ref(), &root)
        .await
        .expect("open snapshot verifier");
    let mut authority = history
        .verify_installable_snapshot(&snapshot)
        .await
        .expect("verify original snapshot")
        .into_authority();
    authority.validate().expect("original authority validates");

    let proposal_bytes = b"exclusion absent from the signed snapshot";
    let proposal_hash = ObjectHash::digest(proposal_bytes);
    let proposal_id =
        coven_protocol::store_commit::StoreDeviceExclusionProposalId::from_hash(proposal_hash);
    let founder_device_id = authority.founder_registration.device_id;
    let founder = authority
        .metadata
        .body_mut()
        .state
        .devices
        .devices
        .get_mut(&founder_device_id)
        .expect("snapshot contains founder");
    let proposal = coven_protocol::store_commit::StoreDeviceExclusionProposalRef {
        proposal_id,
        target: founder.registration.clone(),
        proposal_hash,
        object: coven_protocol::objects::ExactObjectRef::new(
            coven_protocol::objects::ObjectSlot::logical(format!(
                "{}.json",
                coven_protocol::store_commit::device_exclusion_proposal_semantic_prefix(
                    founder.registration.device_id,
                    proposal_id,
                    proposal_hash,
                ),
            ))
            .expect("valid proposal slot"),
            proposal_bytes.len() as u64,
            proposal_hash,
        ),
    };
    proposal.validate_path().expect("valid proposal reference");
    founder.proposals.insert(
        proposal_id,
        coven_protocol::store_commit::StoreDeviceProposalState::Pending { proposal },
    );
    let canonical = coven_protocol::store_commit::ResolvedStoreDeviceState::merge([authority
        .metadata
        .state
        .devices
        .clone()])
    .expect("changed body is valid when its hash is recomputed");
    canonical
        .validate_canonical()
        .expect("canonical changed state");
    assert_ne!(
        canonical.state_hash,
        authority.metadata.state.devices.state_hash
    );
    assert_eq!(
        authority.metadata.state.devices.state_hash,
        snapshot.meta.state.devices.state_hash
    );
    assert!(authority
        .metadata
        .state
        .devices
        .validate_canonical()
        .is_err());
    assert!(
        authority.validate().is_err(),
        "snapshot authority must authenticate the device body, not its supplied hash field",
    );
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
                snapshot(&snapshot_image(&db, &store.root()).await),
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
            ObjectHash::digest(
                &selected
                    .staged_database_bytes_for_test()
                    .expect("read selected snapshot image")
            ),
            published.image.image_hash
        );
    })
    .await;
}

#[tokio::test]
async fn staged_snapshot_reuses_exact_objects_after_restart() {
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
            snapshot_image(&db, device.store_root()).await,
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
    let retained = store_database(&reopened)
        .outbound_snapshot_publication()
        .await
        .expect("read reopened snapshot outbox")
        .expect("snapshot remains staged after reopening");
    assert_eq!(retained.reference, staged.reference);
    assert_eq!(retained.meta.bytes, staged.meta.bytes);
    assert_eq!(retained.meta.prepared, staged.meta.prepared);
    assert_eq!(retained.image.value, staged.image.value);
    assert_eq!(retained.image.prepared, staged.image.prepared);
    assert_eq!(retained.rollup.bytes, staged.rollup.bytes);
    assert_eq!(retained.rollup.prepared, staged.rollup.prepared);
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
    assert_eq!(
        published.membership_rollup,
        staged.meta.value.membership_rollup
    );
    assert!(store_database(&reopened)
        .outbound_snapshot_publication()
        .await
        .expect("read drained snapshot outbox")
        .is_none());
}

#[tokio::test]
async fn exact_snapshot_loader_rejects_a_tampered_accepted_reference() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(Path::new(":memory:"), "snapshot-test-device");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    assert!(store_database(&db)
        .store_current_publication()
        .await
        .expect("read publication before any snapshot")
        .record()
        .latest_snapshot()
        .is_none());
    device
        .publish_snapshot_at(
            snapshot_image(&db, device.store_root()).await,
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
        store_database(&db)
            .store_current_publication()
            .await
            .expect("read publication after snapshot")
            .record()
            .latest_snapshot()
            .expect("current publication names its snapshot")
            .snapshot,
        published.reference,
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
    wrong_reference.object = coven_protocol::objects::ExactObjectRef::new(
        coven_protocol::objects::ObjectSlot::logical(format!(
            "{}.json",
            snapshot_candidate_semantic_prefix(
                &published.meta.author_registration.device_id.to_string(),
                "unpublished-candidate",
            ),
        ))
        .expect("valid different candidate slot"),
        wrong_reference.object.stored_size(),
        wrong_reference.object.stored_hash(),
    );
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

    let mut wrong_predecessor = published.meta;
    wrong_predecessor.body_mut().publication_predecessor = store_database(&db)
        .store_current_publication()
        .await
        .expect("load a different publication boundary")
        .record()
        .clone();
    assert!(writer
        .snapshots()
        .verify_own_snapshot_bytes_for_test(&published.reference, &wrong_predecessor.to_bytes())
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
            snapshot_image(&db, device.store_root()).await,
            CommitFrontier(BTreeMap::new()),
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("resolve exact image-create response loss");
    // The image create whose response was lost, followed by the membership
    // rollup, metadata, and the exact shared publication entry.
    assert_eq!(home.exact_create_count(), 4);
    let stored_image = storage
        .read_protocol_object(
            &ProtocolObjectContext::store_encrypted(
                device.store_root().store_root_hash,
                ProtocolObjectDomain::StoreSnapshotImage,
            ),
            &published.image.object,
            &coven_protocol::store_commit::semantic_prefix_from_exact_object(
                &published.image.object,
                ".db",
            )
            .expect("validated image path"),
        )
        .await
        .expect("read the exact image whose create response was lost");
    assert_eq!(
        published.image.image_hash,
        ObjectHash::digest(&stored_image)
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
            snapshot_image(&db, device.store_root()).await,
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
            snapshot_image(&db, device.store_root()).await,
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
async fn snapshot_candidates_extend_the_exact_accepted_publication() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(Path::new(":memory:"), "snapshot-test-device");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    let first = device
        .publish_snapshot_at(
            snapshot_image(&db, device.store_root()).await,
            CommitFrontier(BTreeMap::new()),
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("publish first snapshot");
    let first_current = store_database(&db)
        .store_current_publication()
        .await
        .expect("read accepted first snapshot");
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
            snapshot_image(&db, device.store_root()).await,
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
        second.meta.value.publication_predecessor,
        *first_current.record()
    );
    assert_eq!(&second.publication.previous, first_current.record(),);
    assert_eq!(
        second
            .meta
            .value
            .publication_predecessor
            .latest_snapshot()
            .expect("the predecessor names the first accepted snapshot")
            .snapshot,
        first_published.reference,
    );
    assert_ne!(
        second.reference.object.slot(),
        first_published.reference.object.slot()
    );
    assert_eq!(
        store_database(&db)
            .store_current_publication()
            .await
            .expect("failed upload retains the accepted boundary"),
        first_current,
    );
    device
        .resume_snapshot_publication()
        .await
        .expect("resume second snapshot publication")
        .expect("publish staged second snapshot");
    assert_eq!(
        store_database(&db)
            .store_publication_entries()
            .await
            .expect("read publication entries before baseline installation")
            .len(),
        2,
        "acceptance retains the local replay prefix until baseline installation"
    );
    assert!(matches!(
        device
            .stand_on_accepted_snapshot()
            .await
            .expect("install the accepted snapshot baseline"),
        crate::sync::store::ReplayBaselineAdvance::Advanced(_)
    ));
    let retained_publications = store_database(&db)
        .store_publication_entries()
        .await
        .expect("read the compacted publication interval");
    assert_eq!(retained_publications.len(), 1);
    assert!(matches!(
        &retained_publications[0].value.payload,
        coven_protocol::store_commit::StorePublicationPayload::Snapshot(snapshot)
            if snapshot == &second.reference
    ));
    let published_snapshots = db
        .table_row_count_for_test(coven_database::DatabaseTestTable::named(
            "published_store_snapshot",
        ))
        .await
        .expect("count published Store snapshots");
    assert_eq!(
        published_snapshots, 1,
        "retirement releases the superseded snapshot journal"
    );
}

/// The current publication names one exact snapshot independently of the
/// number of earlier snapshots and without listing candidate objects.
#[tokio::test]
async fn installable_snapshot_selection_does_not_grow_with_published_snapshots() {
    let shallow = installable_selection_reads("selection-shallow", 1).await;
    let deep = installable_selection_reads("selection-deep", 6).await;
    assert_eq!(
        shallow, deep,
        "snapshot metadata reads grew with retired history"
    );
    assert_eq!(
        deep, 1,
        "selection reads only the current accepted snapshot metadata"
    );
}

async fn installable_selection_reads(store_id: &str, snapshots: usize) -> usize {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(Path::new(":memory:"), store_id);
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    for _ in 0..snapshots {
        device
            .publish_snapshot(
                snapshot_image(&db, device.store_root()).await,
                CommitFrontier(BTreeMap::new()),
            )
            .await
            .expect("publish a Store snapshot");
    }
    accepted_snapshot_selection_reads(&home, storage.as_ref(), device.store_root(), &db).await
}

async fn accepted_snapshot_selection_reads(
    home: &InMemoryCloudHome,
    storage: &dyn CloudSyncObjectStorage,
    root: &coven_protocol::store_commit::StoreRootRef,
    database: &Database,
) -> usize {
    let current = store_database(database)
        .store_current_publication()
        .await
        .expect("read accepted publication");
    let expected = current
        .record()
        .latest_snapshot()
        .expect("the current publication has an accepted snapshot")
        .clone();
    let mut history = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
        .open_pinned(storage, root)
        .await
        .expect("open snapshot authority");
    home.clear_exact_reads();
    home.clear_exact_listings();
    let selected = history
        .current_accepted_snapshot()
        .await
        .expect("verify the accepted snapshot")
        .expect("the current record names a snapshot");
    assert_eq!(selected.reference(), expected);
    assert!(
        home.exact_listed_prefixes().iter().all(|prefix| prefix.starts_with("store-v1/membership/heads/")),
        "snapshot selection may discover current membership authority, but must not enumerate snapshot candidates: {:?}",
        home.exact_listed_prefixes(),
    );
    home.exact_reads()
        .iter()
        .filter(|slot| slot.logical_key().starts_with("store-v1/snapshots/"))
        .count()
}

#[tokio::test]
async fn selection_uses_current_publication_across_two_owner_devices() {
    let shallow = two_owner_selection("two-owner-selection-shallow", 1).await;
    let deep = two_owner_selection("two-owner-selection-deep", 4).await;
    assert_eq!(
        shallow, deep,
        "snapshot reads grew with prior publications across owners"
    );
    assert_eq!(deep, 1, "only the accepted current snapshot is selected");
}

async fn two_owner_selection(store_id: &str, rounds: usize) -> usize {
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

    let first = store
        .bind_device_in(&owner_db, owner_dir.clone(), &owner)
        .await
        .expect("bind the first owner device");
    let second = store
        .bind_device_in(&member_db, member_dir.clone(), &member)
        .await
        .expect("bind the second owner device");
    for round in 0..rounds {
        first
            .pull_store()
            .await
            .expect("first owner installs the accepted boundary");
        owner_db
            .execute_test_host_write(&format!(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n{round}', 'Round {round}', NULL, 1, \
             '0000000001000-0000-first', '2026-01-01')",
            ))
            .await;
        assert!(first
            .prepare_pending_store_write()
            .await
            .expect("prepare the first owner's row"));
        assert_eq!(
            first
                .drain_store_writes()
                .await
                .expect("publish the first owner's row"),
            1
        );
        first
            .publish_snapshot(
                snapshot_image(&owner_db, first.store_root()).await,
                CommitFrontier::from_refs(
                    first.materialized_frontier().await.expect("first frontier"),
                )
                .expect("valid first frontier"),
            )
            .await
            .expect("publish the first owner's snapshot");
        let first_snapshot = store_database(&owner_db)
            .store_current_publication()
            .await
            .expect("read first accepted snapshot")
            .record()
            .latest_snapshot()
            .expect("first snapshot is accepted")
            .clone();
        second
            .pull_store()
            .await
            .expect("second owner installs the entire accepted prefix");
        // Several snapshots by one owner do not create a separate author order.
        for _ in 0..3 {
            second
                .publish_snapshot(
                    snapshot_image(&member_db, second.store_root()).await,
                    CommitFrontier::from_refs(
                        second
                            .materialized_frontier()
                            .await
                            .expect("second frontier"),
                    )
                    .expect("valid second frontier"),
                )
                .await
                .expect("publish the second owner's snapshot");
        }
        let latest = store_database(&member_db)
            .latest_local_store_snapshot()
            .await
            .expect("read second snapshot")
            .expect("second snapshot was published");
        assert_eq!(
            latest.meta.author_registration.device_id,
            second.typed_device_id()
        );
        assert!(latest.meta.coverage.covers(
            &store_database(&owner_db)
                .latest_local_store_snapshot()
                .await
                .expect("read first snapshot")
                .expect("first snapshot was published")
                .meta
                .coverage
        ));
        let current = store_database(&member_db)
            .store_current_publication()
            .await
            .expect("read second accepted boundary");
        let accepted = current
            .record()
            .latest_snapshot()
            .expect("second snapshot is accepted");
        assert_eq!(accepted.snapshot, latest.reference);
        assert!(accepted.publication.position > first_snapshot.publication.position);
        for written in 0..=round {
            assert!(
                member_db
                    .test_row_exists(&format!("SELECT 1 FROM notes WHERE id = 'n{written}'",))
                    .await,
                "the later owner's snapshot must include every earlier accepted row"
            );
        }
    }
    accepted_snapshot_selection_reads(&home, connection.as_ref(), first.store_root(), &member_db)
        .await
}
