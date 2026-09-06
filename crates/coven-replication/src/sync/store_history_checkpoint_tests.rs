use crate::sync::test_helpers::TestStore;
use coven_keys::keys::UserKeypair;
use coven_protocol::membership::MembershipChain;
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::objects::ObjectSlot;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::ObjectHash;
use coven_storage::CloudSyncObjectStorage;

#[path = "snapshot_device_history_tests.rs"]
mod snapshot_device_history_tests;

fn store_database(db: &coven_database::Database) -> coven_database::StoreDatabase {
    coven_database::StoreDatabase::new(db)
}

struct HistoryPublisher<'fixture> {
    database: &'fixture coven_database::Database,
    device: &'fixture crate::sync::test_helpers::TestDevice,
}

impl<'fixture> HistoryPublisher<'fixture> {
    fn new(
        database: &'fixture coven_database::Database,
        device: &'fixture crate::sync::test_helpers::TestDevice,
    ) -> Self {
        Self { database, device }
    }

    async fn publish_note(&self, sequence: u64) {
        self.database
            .execute_test_host_write(&format!(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('history-{sequence}', 'history', NULL, 1, \
                         '0000000001000-0000-history', '2026-07-21')"
            ))
            .await;
        assert!(
            self.device
                .prepare_pending_store_write()
                .await
                .expect("prepare Merge Store write"),
            "host write produces a prepared Store commit",
        );
        assert_eq!(
            self.device
                .drain_store_writes()
                .await
                .expect("publish Merge Store write"),
            1,
            "one prepared Store commit is published",
        );
    }
}

struct PublishedHistory {
    db: coven_database::Database,
    home: std::sync::Arc<coven_storage::InMemoryCloudHome>,
    storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
    device: crate::sync::test_helpers::TestDevice,
    membership: MembershipChain,
}

impl PublishedHistory {
    async fn publish(history_length: u64) -> Self {
        let signer = UserKeypair::generate();
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, storage) = TestStore::create_with_connection(
            &db,
            db_store_dir.clone(),
            &format!("checkpoint-sabotage-{history_length}"),
            signer.clone(),
            home.clone(),
        )
        .await
        .expect("create Merge Store");
        let device = store
            .bind_device_in(&db, db_store_dir.clone(), &signer)
            .await
            .expect("load Merge Store");
        let membership = device
            .membership_for_test()
            .await
            .expect("load Merge membership");
        let publisher = HistoryPublisher::new(&db, &device);
        for sequence in 1..=history_length {
            publisher.publish_note(sequence).await;
        }
        let fixture = Self {
            db,
            home,
            storage,
            device,
            membership,
        };
        assert_eq!(
            fixture.retained_history().await.len() as u64,
            history_length,
            "every published Merge commit has retained verified inputs",
        );
        fixture
    }

    async fn retained_history(&self) -> Vec<coven_database::OwnedVerifiedMergeMaterialization> {
        self.device
            .retained_merge_replay_inputs_for_test()
            .await
            .expect("load retained verified Merge history")
    }

    async fn historical_read_slots(&self) -> (Vec<ObjectSlot>, ObjectSlot) {
        let retained = self.retained_history().await;
        let history_length = retained.len() as u64;
        let historical_slots = retained
            .iter()
            .flat_map(|entry| {
                [
                    entry.commit_ref().object.slot().clone(),
                    entry.activation_head_object().slot().clone(),
                ]
            })
            .collect::<Vec<_>>();
        let registration_anchor_head_slot = retained
            .first()
            .expect("published history has a first retained commit")
            .activation_head_object()
            .slot()
            .clone();

        self.home.clear_exact_reads();
        HistoryPublisher::new(&self.db, &self.device)
            .publish_note(history_length + 1)
            .await;

        let reread = self
            .home
            .exact_reads()
            .into_iter()
            .filter(|slot| historical_slots.contains(slot))
            .collect();
        (reread, registration_anchor_head_slot)
    }

    async fn prepare_sabotaged_successor(&self) -> String {
        self.db
            .execute_test_host_write(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('sabotaged-successor', 'history', NULL, 1, \
                 '0000000002000-0000-history', '2026-07-21')",
            )
            .await;
        match self.device.prepare_pending_store_write().await {
            Err(error) => error.to_string(),
            Ok(true) => self
                .device
                .drain_store_writes()
                .await
                .expect_err("checkpoint sabotage must fail before remote publication")
                .to_string(),
            Ok(false) => panic!("sabotaged host write produced no pending Store write"),
        }
    }
}

#[tokio::test]
async fn announcement_position_rejects_a_commit_from_another_coordinate() {
    let fixture = PublishedHistory::publish(2).await;
    let retained = fixture.retained_history().await;
    let first = &retained[0];
    let second = &retained[1];
    let registration_ref = second.activation_head().author_registration.clone();
    let registration = fixture
        .device
        .load_registration_for_test(&registration_ref)
        .await
        .expect("load announcement author");
    let context = ProtocolObjectContext::signed_plaintext(
        fixture.device.store_root().store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );

    let first_replacement = fixture
        .device
        .sign_device_head_for_test(
            second.commit_ref().clone(),
            first.activation_head().successor.clone(),
        )
        .await
        .expect("sign first replacement head");
    let first_prefix =
        coven_protocol::store_commit::head_slot_prefix(&registration.device_id.to_string(), 1);
    let first_prepared = fixture
        .storage
        .prepare_protocol_object(
            &context,
            first.activation_head_object().slot().clone(),
            &first_prefix,
            first_replacement.to_bytes(),
        )
        .expect("prepare first replacement head");

    let mut second_successor = second.activation_head().successor.clone();
    second_successor.predecessor = Some(first_prepared.reference().clone());
    let second_replacement = fixture
        .device
        .sign_device_head_for_test(second.commit_ref().clone(), second_successor)
        .await
        .expect("sign second replacement head");
    let second_prefix =
        coven_protocol::store_commit::head_slot_prefix(&registration.device_id.to_string(), 2);
    let second_prepared = fixture
        .storage
        .prepare_protocol_object(
            &context,
            second.activation_head_object().slot().clone(),
            &second_prefix,
            second_replacement.to_bytes(),
        )
        .expect("prepare second replacement head");
    fixture.home.replace_exact_object(
        first.activation_head_object().slot(),
        first_prepared.stored_bytes().to_vec(),
    );
    fixture.home.replace_exact_object(
        second.activation_head_object().slot(),
        second_prepared.stored_bytes().to_vec(),
    );

    let error = fixture
        .device
        .exact_next_announcement_slot_for_test(
            &registration_ref,
            &registration,
            Some(second.commit_ref()),
        )
        .await
        .expect_err("announcement slot one must name commit coordinate one");
    assert!(
        error.to_string().contains("coordinate"),
        "unexpected announcement coordinate error: {error}"
    );
}

#[tokio::test]
async fn merge_successor_publication_does_not_reread_materialized_history() {
    let (shallow, _) = PublishedHistory::publish(1)
        .await
        .historical_read_slots()
        .await;
    let (deeper, registration_anchor_head_slot) = PublishedHistory::publish(100)
        .await
        .historical_read_slots()
        .await;
    assert!(
        shallow.is_empty()
            && deeper.is_empty()
            && !deeper.contains(&registration_anchor_head_slot),
        "publishing after one retained commit reread {} historical commit/head objects; \
         publishing after 100 reread {} (registration anchor head {registration_anchor_head_slot:?}): \
         shallow={shallow:?}, deeper={deeper:?}",
        shallow.len(),
        deeper.len(),
    );
}

#[tokio::test]
async fn retained_history_reuses_each_verified_device_head_within_a_cycle() {
    let fixture = PublishedHistory::publish(12).await;
    let head_slots = fixture
        .retained_history()
        .await
        .into_iter()
        .map(|materialization| materialization.activation_head_object().slot().clone())
        .collect::<Vec<_>>();
    fixture.home.clear_exact_reads();

    fixture
        .device
        .run_cycle(None)
        .await
        .expect("pull retained announcement history");

    let reads = fixture.home.exact_reads();
    let counts = head_slots
        .into_iter()
        .map(|slot| {
            let count = reads.iter().filter(|read| *read == &slot).count();
            (slot, count)
        })
        .collect::<Vec<_>>();
    let maximum = counts.iter().map(|(_, count)| *count).max().unwrap_or(0);
    assert!(
        maximum <= 1,
        "retained history verification restarted accepted announcement paths: {counts:?}",
    );
}

async fn membership_head_reads_for_retained_history(history_length: u64) -> usize {
    let fixture = PublishedHistory::publish(history_length).await;
    let head_slots = fixture
        .membership
        .head_refs()
        .iter()
        .map(|reference| reference.object.slot().clone())
        .collect::<Vec<_>>();
    fixture.home.clear_exact_reads();

    fixture
        .device
        .run_cycle(None)
        .await
        .expect("pull retained membership history");

    fixture
        .home
        .exact_reads()
        .into_iter()
        .filter(|slot| head_slots.contains(slot))
        .count()
}

#[tokio::test]
async fn retained_history_depth_does_not_repeat_exact_membership_head_reads() {
    let shallow = membership_head_reads_for_retained_history(1).await;
    let deep = membership_head_reads_for_retained_history(12).await;

    assert!(
        deep <= shallow,
        "one retained commit read exact membership heads {shallow} times; twelve read them {deep} times",
    );
}

async fn registration_reads_for_retained_history(history_length: u64) -> usize {
    let fixture = PublishedHistory::publish(history_length).await;
    let registration_slot = fixture.retained_history().await[0]
        .activation_head()
        .author_registration
        .object
        .slot()
        .clone();
    fixture.home.clear_exact_reads();

    fixture
        .device
        .run_cycle(None)
        .await
        .expect("pull retained registration history");

    fixture
        .home
        .exact_reads()
        .into_iter()
        .filter(|slot| slot == &registration_slot)
        .count()
}

#[tokio::test]
async fn retained_history_depth_does_not_repeat_exact_registration_reads() {
    let shallow = registration_reads_for_retained_history(1).await;
    let deep = registration_reads_for_retained_history(12).await;

    assert!(
        deep <= shallow,
        "one retained commit read its exact registration {shallow} times; twelve read it {deep} times",
    );
}

#[tokio::test]
async fn retained_history_reuses_each_verified_acknowledgement_within_a_cycle() {
    let fixture = PublishedHistory::publish(1).await;
    let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
        store_database(&fixture.db)
            .materialized_frontier()
            .await
            .expect("load acknowledgement coverage"),
    )
    .expect("derive acknowledgement coverage");
    fixture
        .device
        .publish_acknowledgement(coverage)
        .await
        .expect("publish retained acknowledgement");
    let publisher = HistoryPublisher::new(&fixture.db, &fixture.device);
    for sequence in 2..=12 {
        publisher.publish_note(sequence).await;
    }
    let acknowledgement_slots = fixture
        .retained_history()
        .await
        .into_iter()
        .filter_map(|materialization| {
            materialization
                .history_evidence()
                .acknowledgement
                .as_ref()
                .map(|acknowledgement| acknowledgement.acknowledgement().0.object.slot().clone())
        })
        .collect::<Vec<_>>();
    assert!(
        !acknowledgement_slots.is_empty(),
        "published retained history carries acknowledgements",
    );
    fixture.home.clear_exact_reads();

    fixture
        .device
        .run_cycle(None)
        .await
        .expect("pull retained acknowledgement history");

    let reads = fixture.home.exact_reads();
    let counts = acknowledgement_slots
        .into_iter()
        .map(|slot| {
            let count = reads.iter().filter(|read| *read == &slot).count();
            (slot, count)
        })
        .collect::<Vec<_>>();
    let maximum = counts.iter().map(|(_, count)| *count).max().unwrap_or(0);
    assert!(
        maximum <= 1,
        "retained history verification reread acknowledgement prefixes: {counts:?}",
    );
}

#[tokio::test]
async fn open_connection_reuses_a_verified_retained_history_checkpoint() {
    let fixture = PublishedHistory::publish(3).await;
    let first = fixture
        .retained_history()
        .await
        .into_iter()
        .find(|materialization| materialization.commit_ref().coord.sequence() == 1)
        .expect("retained history contains the first commit");
    let encoded = serde_json::to_string(first.commit_ref()).expect("serialize first commit ref");
    fixture
        .db
        .delete_device_state_snapshot_for_test(encoded)
        .await
        .expect("remove state after its checkpoint was verified");

    HistoryPublisher::new(&fixture.db, &fixture.device)
        .publish_note(4)
        .await;
}

enum VerifiedAuthoritySabotage {
    StoreRoot,
    Registration,
}

async fn publish_after_verified_authority_sabotage(sabotage: VerifiedAuthoritySabotage) {
    let fixture = PublishedHistory::publish(1).await;
    let registration = fixture.retained_history().await[0]
        .activation_head()
        .author_registration
        .clone();
    match sabotage {
        VerifiedAuthoritySabotage::StoreRoot => {
            fixture
                .db
                .replace_store_root_hash_for_test(None)
                .await
                .expect("delete verified Store root authority");
        }
        VerifiedAuthoritySabotage::Registration => {
            fixture
                .db
                .corrupt_store_device_registration_bytes_for_test(registration)
                .await
                .expect("corrupt verified Store registration authority");
        }
    }

    HistoryPublisher::new(&fixture.db, &fixture.device)
        .publish_note(2)
        .await;
}

#[tokio::test]
async fn open_connection_reuses_verified_store_root_authority() {
    publish_after_verified_authority_sabotage(VerifiedAuthoritySabotage::StoreRoot).await;
}

#[tokio::test]
async fn open_connection_reuses_verified_registration_authority() {
    publish_after_verified_authority_sabotage(VerifiedAuthoritySabotage::Registration).await;
}

fn open_persistent_history_database(
    path: &std::path::Path,
    device_id: &str,
) -> (
    coven_database::Database,
    coven_foundation::store_dir::StoreDir,
) {
    let store_dir = crate::sync::test_helpers::store_dir_for_test_database(path);
    let migrations = crate::sync::test_helpers::test_migrations();
    let database = coven_database::Database::open_synthetic_for_test(
        path,
        store_dir.clone(),
        crate::sync::test_helpers::test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        device_id.to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &migrations,
    )
    .expect("open persistent history database");
    (database, store_dir)
}

async fn reopen_after_verified_authority_sabotage(sabotage: VerifiedAuthoritySabotage) -> String {
    let directory = tempfile::tempdir().expect("create authority reopen directory");
    let path = directory.path().join("authority.sqlite");
    let signer = UserKeypair::generate();
    let (database, database_store_dir) =
        open_persistent_history_database(&path, "authority-reopen-device");
    let (store, storage) = TestStore::create_with_connection(
        &database,
        database_store_dir.clone(),
        "authority-reopen",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create authority reopen Store");
    let device = store
        .bind_device_in(&database, database_store_dir.clone(), &signer)
        .await
        .expect("bind authority reopen Store");
    let store_database = store_database(&database);
    store_database
        .validated_store_owner(device.store_root())
        .await
        .expect("verify Store root and founder registration before sabotage");
    let registration = store_database
        .local_activated_registration_ref()
        .await
        .expect("load local registration reference")
        .expect("local registration is activated");
    match sabotage {
        VerifiedAuthoritySabotage::StoreRoot => database
            .replace_store_root_hash_for_test(None)
            .await
            .expect("remove verified Store root"),
        VerifiedAuthoritySabotage::Registration => database
            .corrupt_store_device_registration_bytes_for_test(registration)
            .await
            .expect("corrupt verified Store registration"),
    }
    drop(device);
    drop(store);
    drop(store_database);
    drop(database);

    let (reopened, reopened_store_dir) =
        open_persistent_history_database(&path, "authority-reopen-device");
    match crate::sync::test_helpers::TestDevice::load(
        &reopened,
        reopened_store_dir.clone(),
        storage,
        signer,
    )
    .await
    {
        Ok(device) => coven_database::StoreDatabase::new(&reopened)
            .validated_store_owner(device.store_root())
            .await
            .expect_err("first verification accepted altered durable Store authority")
            .to_string(),
        Err(error) => error.to_string(),
    }
}

#[tokio::test]
async fn reopened_connection_rejects_an_altered_store_root_before_first_verification() {
    let error =
        reopen_after_verified_authority_sabotage(VerifiedAuthoritySabotage::StoreRoot).await;
    assert!(
        error.contains("root"),
        "unexpected Store root error: {error}"
    );
}

#[tokio::test]
async fn reopened_connection_rejects_an_altered_registration_before_first_verification() {
    let error =
        reopen_after_verified_authority_sabotage(VerifiedAuthoritySabotage::Registration).await;
    assert!(
        error.contains("registration"),
        "unexpected Store registration error: {error}"
    );
}

#[tokio::test]
async fn missing_frontier_retained_row_has_no_cloud_fallback() {
    let fixture = PublishedHistory::publish(1).await;
    let retained = fixture.retained_history().await;
    let reference = retained[0].commit_ref().clone();
    let reference = reference.clone();
    fixture
        .db
        .delete_retained_materialization_without_foreign_keys_for_test(reference)
        .await
        .expect("remove retained frontier row");

    let error = fixture.prepare_sabotaged_successor().await;
    assert!(
        !error.is_empty(),
        "missing retained frontier returned an empty error"
    );
}

#[tokio::test]
async fn outbound_successor_rejects_missing_or_forged_device_state() {
    for delete_state in [true, false] {
        let fixture = PublishedHistory::publish(1).await;
        let retained = fixture.retained_history().await;
        let encoded =
            serde_json::to_string(retained[0].commit_ref()).expect("serialize commit ref");
        if delete_state {
            fixture
                .db
                .delete_device_state_snapshot_for_test(encoded)
                .await
                .expect("delete checkpoint state");
        } else {
            let database = coven_database::StoreDatabase::new(&fixture.db);
            let root = database
                .local_store_root_ref()
                .await
                .expect("load Store root")
                .expect("Store root exists");
            let state = database
                .store_device_state_for_history_cut(&coven_protocol::store_commit::StoreHistoryCut(
                    std::collections::BTreeMap::from([(
                        retained[0].commit_ref().coord.stream_id,
                        retained[0].commit_ref().clone(),
                    )]),
                ))
                .await
                .expect("resolve retained checkpoint state")
                .1;
            let grant = coven_protocol::membership::MembershipGrantId(ObjectHash::digest(
                b"forged-checkpoint-recovery",
            ));
            let anchor = coven_protocol::store_commit::GrantStreamAnchor::OwnerRecovery {
                first_slot: retained[0].activation_head_object().slot().clone(),
            };
            let activation = coven_protocol::store_commit::OwnerRecoveryActivationId::derive(
                &root,
                "forged-checkpoint-owner",
                &grant,
                &anchor,
            )
            .expect("derive forged recovery activation");
            let forged = state
                .activate_owner_recovery(grant, activation)
                .expect("construct another canonical device state");
            fixture
                .db
                .replace_device_state_snapshot_for_test(encoded, forged)
                .await
                .expect("forge canonical checkpoint state");
        }
        let error = fixture.prepare_sabotaged_successor().await;
        assert!(
            !error.is_empty(),
            "checkpoint-state sabotage returned an empty error"
        );
    }
}

#[tokio::test]
async fn retained_commit_evidence_rejects_an_omitted_acknowledgement() {
    let fixture = PublishedHistory::publish(1).await;
    let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
        store_database(&fixture.db)
            .materialized_frontier()
            .await
            .expect("load acknowledgement coverage"),
    )
    .expect("derive acknowledgement coverage");
    fixture
        .device
        .publish_acknowledgement(coverage)
        .await
        .expect("publish retained acknowledgement");

    let retained = fixture.retained_history().await;
    let current = retained.last().expect("acknowledgement commit is retained");
    assert!(current.history_evidence().acknowledgement.is_some());
    let mut omitted = current.history_evidence().clone();
    omitted.acknowledgement = None;
    assert!(omitted
        .validate_for(current.commit_ref(), current.commit())
        .is_err());
}

struct MemberRemovalHistory {
    db: coven_database::Database,
    store: std::sync::Arc<TestStore>,
    device: crate::sync::test_helpers::TestDevice,
    removal: coven_database::OwnedVerifiedMergeMaterialization,
}

impl MemberRemovalHistory {
    async fn create() -> Self {
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let member_pubkey = crate::sync::test_helpers::pubkey_hex(&member);
        let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
        let store = TestStore::create(
            &db,
            db_store_dir.clone(),
            "retained-removal-proof",
            owner.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create removal-proof Store");
        store
            .admit_member(
                &db,
                db_store_dir.clone(),
                &owner,
                &member_pubkey,
                None,
                coven_protocol::membership::MemberRole::Member,
                &encryption,
                "Retained removal proof",
            )
            .await
            .expect("admit removable member");
        let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
        let member_db = crate::sync::test_helpers::open_test_db(member_db_store_dir.clone());
        store
            .activate_joined_device(
                &db,
                db_store_dir.clone(),
                &member_db,
                member_db_store_dir.clone(),
                &member,
                "2026-07-21T00:00:00Z",
            )
            .await
            .expect("activate removable member device");
        store
            .promote_active_member_fixture(
                &db,
                db_store_dir.clone(),
                &member_db,
                member_db_store_dir.clone(),
                &owner,
                &member,
                &encryption,
            )
            .await
            .expect("promote removable member to Owner");
        let custody = crate::sync::test_helpers::TestCustody::default();
        store
            .remove_member(
                &db,
                db_store_dir.clone(),
                &owner,
                &member_pubkey,
                &encryption,
                &custody,
            )
            .await
            .expect("remove retained member");
        let device = store
            .bind_device_in(&db, db_store_dir.clone(), &owner)
            .await
            .expect("bind retained-removal Store");
        let retained = device
            .retained_merge_replay_inputs_for_test()
            .await
            .expect("load retained removal history");
        let removal = retained
            .into_iter()
            .find(|materialization| {
                materialization
                    .history_evidence()
                    .membership_proof
                    .as_ref()
                    .is_some_and(|proof| {
                        matches!(
                            proof.entry_value.change,
                            coven_protocol::membership::MembershipChange::RemoveMember { .. }
                        )
                    })
            })
            .expect("removal activation is retained");
        Self {
            db,
            store,
            device,
            removal,
        }
    }

    async fn publish_snapshot(
        &self,
    ) -> (
        coven_protocol::store_commit::SnapshotMeta,
        coven_database::PublishedStoreSnapshot,
    ) {
        let directory = tempfile::tempdir().expect("create snapshot image directory");
        let database = store_database(&self.db);
        let image = database
            .capture_snapshot_image_for_test(
                self.store.root().clone(),
                directory.path().to_path_buf(),
                None,
            )
            .await
            .expect("create checkpoint snapshot image");
        let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
            database
                .materialized_frontier()
                .await
                .expect("load snapshot coverage"),
        )
        .expect("derive snapshot coverage");
        let meta = self
            .device
            .publish_snapshot(image, coverage)
            .await
            .expect("publish checkpoint snapshot");
        let published = database
            .latest_local_store_snapshot()
            .await
            .expect("load published snapshot")
            .expect("published snapshot is recorded");
        (meta, published)
    }
}

#[tokio::test]
async fn retained_commit_evidence_rejects_an_omitted_membership_removal() {
    let fixture = Box::pin(MemberRemovalHistory::create()).await;
    let mut omitted = fixture.removal.history_evidence().clone();
    omitted.membership_proof = None;
    assert!(omitted
        .validate_for(fixture.removal.commit_ref(), fixture.removal.commit())
        .is_err());
}

#[tokio::test]
async fn membership_checkpoint_floor_includes_the_activating_control() {
    let fixture = Box::pin(MemberRemovalHistory::create()).await;
    let (meta, _) = fixture.publish_snapshot().await;
    let control = meta
        .history_summary
        .membership_proofs
        .values()
        .find(|proof| {
            matches!(
                proof.entry_value.change,
                coven_protocol::membership::MembershipChange::RemoveMember { .. }
            )
        })
        .expect("retained history contains the removal control proof");
    assert!(meta
        .history_summary
        .membership_floor
        .effective_coordinates
        .contains(&control.entry.coord));
}

#[tokio::test]
async fn retained_membership_proof_rejects_an_incomplete_resolution_authority() {
    let fixture = Box::pin(MemberRemovalHistory::create()).await;
    let mut evidence = fixture.removal.history_evidence().clone();
    let proof = evidence
        .membership_proof
        .as_mut()
        .expect("retained history contains a membership proof");
    let bytes = b"incomplete retained resolution authority";
    proof.resolution = Some(
        coven_protocol::membership::StoreMembershipConflictResolutionRef {
            conflict_hash: ObjectHash::digest(b"retained resolution conflict"),
            resolver_pubkey: "retained-resolution-resolver".to_string(),
            resolution_hash: ObjectHash::digest(bytes),
            object: ExactObjectRef::new(
                ObjectSlot::logical(
                    "store-v1/tests/incomplete-retained-resolution.json".to_string(),
                )
                .expect("valid retained resolution slot"),
                bytes.len() as u64,
                ObjectHash::digest(bytes),
            ),
        },
    );
    assert!(
        evidence
            .validate_for(fixture.removal.commit_ref(), fixture.removal.commit())
            .is_err(),
        "retained membership proof accepted a resolution reference without its signed value",
    );
}

#[tokio::test]
async fn signed_snapshot_rejects_an_omitted_pre_snapshot_membership_control() {
    let fixture = Box::pin(MemberRemovalHistory::create()).await;
    let (meta, published) = fixture.publish_snapshot().await;
    let mut forged = meta;
    let summary = &mut forged.body_mut().history_summary;
    let removal = summary
        .membership_proofs
        .iter()
        .find_map(|(reference, proof)| {
            matches!(
                proof.entry_value.change,
                coven_protocol::membership::MembershipChange::RemoveMember { .. }
            )
            .then(|| reference.clone())
        })
        .expect("snapshot retains pre-snapshot removal control");
    summary.membership_proofs.remove(&removal);
    let forged = fixture
        .device
        .resign_snapshot_meta_for_test(forged)
        .await
        .expect("re-sign internally valid snapshot through Store authority");
    let forged_bytes = forged.to_bytes();
    let forged_reference = coven_protocol::store_commit::StoreSnapshotRef {
        generation: forged.generation,
        snapshot_hash: forged.snapshot_hash(),
        object: ExactObjectRef::new(
            published.reference.object.slot().clone(),
            forged_bytes.len() as u64,
            ObjectHash::digest(&forged_bytes),
        ),
    };
    assert_eq!(
        fixture
            .device
            .parse_local_snapshot_meta_for_test(&forged_bytes, &forged_reference)
            .await
            .expect("re-signed omitted summary is internally valid"),
        forged,
    );
    let forged = coven_database::PublishedStoreSnapshot {
        reference: forged_reference,
        successor_slot: published.successor_slot,
        meta: forged,
    };
    assert!(
        fixture
            .device
            .verify_installable_snapshots_for_test(std::slice::from_ref(&forged))
            .await
            .is_err(),
        "snapshot authority accepted a signed summary that omitted exact cut history",
    );
}

#[tokio::test]
async fn conflict_resolution_authorization_reads_retained_checkpoints_not_store_history() {
    let fixture = PublishedHistory::publish(4).await;
    let retained = fixture.retained_history().await;
    let historical_slots = retained
        .iter()
        .flat_map(|entry| {
            [
                entry.commit_ref().object.slot().clone(),
                entry.activation_head_object().slot().clone(),
            ]
        })
        .collect::<Vec<_>>();
    fixture.home.clear_exact_reads();
    fixture
        .device
        .prepare_conflict_resolution_plan_for_test(fixture.membership.head_refs())
        .await
        .expect("authorize from retained conflict-resolution predecessor");
    let reread = fixture
        .home
        .exact_reads()
        .into_iter()
        .filter(|slot| historical_slots.contains(slot))
        .collect::<Vec<_>>();
    assert!(
        reread.is_empty(),
        "conflict-resolution authorization reread historical Store commit/head slots: {reread:?}",
    );
}

/// The pull-side twin of
/// `conflict_resolution_authorization_reads_retained_checkpoints_not_store_history`.
///
/// Every other reuse assertion in this file is scoped `within_a_cycle` — they
/// prove one cycle never reads the same object twice, which was already true
/// while every cycle still re-read the whole retained history once. This is the
/// across-cycle claim: a pull over history the device already verified reaches
/// the provider for none of it, on every pull, and the count does not grow with
/// how much history there is.
///
/// One full cycle runs first and is not measured. It publishes a snapshot,
/// acknowledges it, advances this device's replay baseline over it and reclaims
/// behind it — which is the state the claim is about, a device holding retained
/// rows above a coverage rather than its whole past. Measuring cycles instead of
/// pulls would measure the reclaim leg's own verification, which reads per
/// target and has nothing to do with history reuse.
async fn retained_object_reads_per_pull(history_length: u64, pulls: u32) -> Vec<(usize, usize)> {
    let fixture = PublishedHistory::publish(history_length).await;
    let retained = fixture.retained_history().await;
    let retained_slots = retained
        .iter()
        .flat_map(|entry| {
            [
                entry.commit_ref().object.slot().clone(),
                entry.activation_head_object().slot().clone(),
            ]
        })
        .collect::<Vec<_>>();
    fixture
        .device
        .run_cycle(None)
        .await
        .expect("publish, acknowledge and advance over a covering snapshot");

    let mut per_pull = Vec::new();
    for _ in 0..pulls {
        fixture.home.clear_exact_reads();
        fixture
            .device
            .pull_store()
            .await
            .expect("pull retained history");
        let reads = fixture.home.exact_reads();
        let retained_reads = reads
            .iter()
            .filter(|slot| retained_slots.contains(slot))
            .count();
        per_pull.push((retained_reads, reads.len()));
    }
    per_pull
}

#[tokio::test]
async fn repeated_pulls_over_unchanged_retained_history_read_none_of_it() {
    let deep = retained_object_reads_per_pull(24, 3).await;

    assert!(
        deep.iter().all(|(retained, _)| *retained == 0),
        "pulls re-read Store commit/head objects the device had already verified: \
         (retained_reads, total_reads) per pull = {deep:?}",
    );
}

#[tokio::test]
async fn retained_history_depth_does_not_change_what_a_pull_reads() {
    let shallow = retained_object_reads_per_pull(1, 2).await;
    let deep = retained_object_reads_per_pull(24, 2).await;

    assert_eq!(
        shallow.iter().map(|(_, total)| *total).collect::<Vec<_>>(),
        deep.iter().map(|(_, total)| *total).collect::<Vec<_>>(),
        "a pull's provider reads grew with history depth: one commit read \
         {shallow:?}, twenty-four read {deep:?}",
    );
}

/// Seeding the verifier from retained rows must cover exactly what is retained
/// and not one slot more. The announcement walk resumes at the first sequence
/// the accepted path does not cover, so that slot is still probed on every
/// cycle — this is how a commit another device published gets discovered, and
/// losing it would make the device silently stop pulling.
#[tokio::test]
async fn a_pull_over_retained_history_still_probes_the_next_announcement_slot() {
    let fixture = PublishedHistory::publish(6).await;

    for cycle in 1..=2 {
        // Read the probe target fresh: a cycle publishes its own acknowledgement
        // commits, so each cycle retains more history and probes further along.
        let next_slot = fixture
            .retained_history()
            .await
            .last()
            .expect("published history has a last retained commit")
            .activation_head()
            .successor
            .next_slot
            .clone();
        fixture.home.clear_exact_reads();
        fixture
            .device
            .run_cycle(None)
            .await
            .expect("pull retained history");
        assert!(
            fixture.home.exact_reads().contains(&next_slot),
            "cycle {cycle} stopped probing the announcement slot after its retained history \
             ({next_slot:?})",
        );
    }
}

/// Two devices, each acknowledging the other's commits.
///
/// The single-device fixture above cannot reach this: a lone device publishes an
/// acknowledgement only now and then, so almost none of its retained commits
/// activate one. With two devices nearly every commit does, and the retained
/// path's per-commit cost lives behind exactly that — which is why a fix
/// measured only against `PublishedHistory` looked complete and was not.
struct AcknowledgedHistory {
    db: coven_database::Database,
    home: std::sync::Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
    device: crate::sync::test_helpers::TestDevice,
    peer_db: coven_database::Database,
    peer: crate::sync::test_helpers::TestDevice,
    /// Held for the fixture's life, not read: the Store outlives every device
    /// bound against it.
    _store: std::sync::Arc<crate::sync::test_helpers::TestStore>,
}

impl AcknowledgedHistory {
    async fn publish(rounds: u64) -> Self {
        let founder = UserKeypair::generate();
        let member = UserKeypair::generate();
        let member_pubkey = hex::encode(member.public_key());
        let store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(store_dir.clone());
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, _storage) = TestStore::create_with_connection(
            &db,
            store_dir.clone(),
            "acknowledged-history",
            founder.clone(),
            home.clone(),
        )
        .await
        .expect("create Merge Store");
        let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
        store
            .admit_member(
                &db,
                store_dir.clone(),
                &founder,
                &member_pubkey,
                None,
                coven_protocol::membership::MemberRole::Member,
                &encryption,
                "Acknowledged Store",
            )
            .await
            .expect("admit the peer as a Member");
        let peer_store_dir = crate::sync::test_helpers::test_store_dir();
        let peer_db = crate::sync::test_helpers::open_test_db(peer_store_dir.clone());
        let peer = store
            .activate_joined_device(
                &db,
                store_dir.clone(),
                &peer_db,
                peer_store_dir.clone(),
                &member,
                "2026-03-01T00:00:45Z",
            )
            .await
            .expect("activate the peer device");
        let device = store
            .bind_device_in(&db, store_dir.clone(), &founder)
            .await
            .expect("bind the local device");

        let fixture = Self {
            db,
            home,
            device,
            peer_db,
            peer,
            _store: store,
        };
        for round in 1..=rounds {
            fixture.publish_round(round).await;
        }
        fixture
    }

    /// One note from each side, with a cycle each way afterwards so both devices
    /// see and acknowledge the other's commit.
    async fn publish_round(&self, round: u64) {
        HistoryPublisher::new(&self.db, &self.device)
            .publish_note(round * 2 - 1)
            .await;
        self.peer
            .run_cycle(None)
            .await
            .expect("peer pulls and acknowledges");
        HistoryPublisher::new(&self.peer_db, &self.peer)
            .publish_note(round * 2)
            .await;
        self.device
            .run_cycle(None)
            .await
            .expect("local device pulls and acknowledges");
    }

    async fn retained_history(&self) -> Vec<coven_database::OwnedVerifiedMergeMaterialization> {
        self.device
            .retained_merge_replay_inputs_for_test()
            .await
            .expect("load retained verified Merge history")
    }

    /// Provider operations the cycle after a newly published snapshot asks
    /// for — the cycle that has to verify it and acknowledge it.
    ///
    /// A snapshot is published over a baseline the device has already advanced
    /// onto, so the acknowledgements under that coverage are retired rows: the
    /// shape a store is in every time its owner publishes a generation.
    async fn cycle_requests_after_a_new_snapshot(&self) -> u64 {
        self.cycle_requests_after_snapshot_number(2).await
    }

    /// The same measurement with `generations` snapshots published in front of
    /// the measured one, so the cost can be checked against how many the store
    /// has ever published as well as against how deep its history is.
    async fn cycle_requests_after_snapshot_number(&self, generations: u64) -> u64 {
        for _ in 1..generations {
            self.publish_snapshot_now().await;
            self.settle_onto_the_published_snapshot().await;
        }
        self.publish_snapshot_now().await;
        let before = self._store.provider_requests_issued();
        self.device
            .run_cycle(None)
            .await
            .expect("cycle after the new snapshot");
        self._store.provider_requests_issued() - before
    }

    /// Publish a Store snapshot over the device's current frontier.
    async fn publish_snapshot_now(&self) {
        let image_dir = tempfile::tempdir().expect("snapshot image dir");
        let image = coven_database::StoreDatabase::new(&self.db)
            .capture_snapshot_image_for_test(
                self._store.root().clone(),
                image_dir.path().to_path_buf(),
                None,
            )
            .await
            .expect("capture a snapshot image");
        let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
            coven_database::StoreDatabase::new(&self.db)
                .materialized_frontier()
                .await
                .expect("materialized frontier"),
        )
        .expect("frontier");
        self.device
            .publish_snapshot(image, coverage)
            .await
            .expect("publish the snapshot");
    }

    /// Provider operations a cycle with nothing new to do asks for — every
    /// call, not only the reads, which is the unit the cycle log budgets in.
    async fn settled_cycle_requests(&self) -> u64 {
        self.device.run_cycle(None).await.expect("settle the cycle");
        self.device
            .run_cycle(None)
            .await
            .expect("settle the acknowledgement it just made");
        let before = self._store.provider_requests_issued();
        self.device
            .run_cycle(None)
            .await
            .expect("run a settled cycle");
        self._store.provider_requests_issued() - before
    }

    /// Provider reads made by a cycle that has nothing new to do. The first
    /// settling cycle publishes this device's own acknowledgement of what it
    /// just pulled; the one measured after that is the steady state.
    async fn settled_cycle_reads(&self) -> usize {
        self.device.run_cycle(None).await.expect("settle the cycle");
        self.home.clear_exact_reads();
        self.device
            .run_cycle(None)
            .await
            .expect("run a settled cycle");
        self.home.exact_reads().len()
    }
}

/// The across-cycle claim, on the history shape that actually occurs in the
/// field: a device that has already verified two-device history with
/// acknowledgement evidence reaches the provider for none of it again.
#[tokio::test]
async fn repeat_cycles_over_acknowledged_history_read_none_of_it() {
    let fixture = AcknowledgedHistory::publish(4).await;
    let retained = fixture.retained_history().await;
    assert!(
        retained
            .iter()
            .filter(|entry| entry.history_evidence().acknowledgement.is_some())
            .count()
            >= 2,
        "the fixture must retain commits that activate acknowledgements, or it \
         cannot exercise the ack path at all",
    );
    let retained_slots = retained
        .iter()
        .flat_map(|entry| {
            let mut slots = vec![
                entry.commit_ref().object.slot().clone(),
                entry.activation_head_object().slot().clone(),
            ];
            if let Some(acknowledgement) = &entry.history_evidence().acknowledgement {
                slots.push(acknowledgement.acknowledgement().0.object.slot().clone());
            }
            slots
        })
        .collect::<Vec<_>>();

    for cycle in 1..=3 {
        fixture.home.clear_exact_reads();
        fixture
            .device
            .run_cycle(None)
            .await
            .expect("pull acknowledged retained history");
        let reread = fixture
            .home
            .exact_reads()
            .into_iter()
            .filter(|slot| retained_slots.contains(slot))
            .collect::<Vec<_>>();
        assert!(
            reread.is_empty(),
            "cycle {cycle} reread {} retained commit/head/acknowledgement objects it had \
             already verified: {reread:?}",
            reread.len(),
        );
    }
}

/// The assertion above names the object kinds a two-device history retains, so
/// it only catches a re-read of something already thought of. This one does not
/// look at kinds at all: whatever a settled cycle reads, reading it must not
/// depend on how much history the device has behind it. A per-commit cost of any
/// shape shows up here.
#[tokio::test]
async fn acknowledged_history_depth_does_not_change_what_a_settled_cycle_reads() {
    let shallow = AcknowledgedHistory::publish(2)
        .await
        .settled_cycle_reads()
        .await;
    let deep = AcknowledgedHistory::publish(6)
        .await
        .settled_cycle_reads()
        .await;

    assert_eq!(
        shallow, deep,
        "a settled cycle's provider reads grew with two-device history depth: \
         two rounds read {shallow}, six read {deep}",
    );
}

/// A settled store's cycle is local.
///
/// Everything a cycle asks the provider for at rest is a probe for something
/// new: one announcement slot per author stream, one owner-recovery slot, and
/// the membership heads. Everything else it needs it already knows, and the
/// three stages that used to re-derive their answers every thirty seconds —
/// which snapshot to acknowledge, whether to stand on one, and whether any
/// package may be reclaimed — now ask only when a local fact they depend on has
/// moved. A live store spent thirty-one of its thirty-nine cycle seconds and
/// 391 of its 457 requests re-deriving "nothing to do".
///
/// The number is exact on purpose. A budget written as "not too many" is one
/// nobody notices doubling.
#[tokio::test]
async fn a_settled_cycle_asks_the_provider_only_for_what_could_be_new() {
    let settled = AcknowledgedHistory::publish(4)
        .await
        .settled_cycle_requests()
        .await;

    assert_eq!(
        settled, 14,
        "a settled two-device cycle asked the provider for {settled} operations",
    );
}

/// And the budget is a property of the store's shape, not of how much has
/// happened in it.
#[tokio::test]
async fn history_depth_does_not_change_what_a_settled_cycle_asks_for() {
    let shallow = AcknowledgedHistory::publish(2)
        .await
        .settled_cycle_requests()
        .await;
    let deep = AcknowledgedHistory::publish(6)
        .await
        .settled_cycle_requests()
        .await;

    assert_eq!(
        shallow, deep,
        "a settled cycle's provider operations grew with history depth: two \
         rounds asked for {shallow}, six asked for {deep}",
    );
}

/// Acknowledging a new snapshot costs what the snapshot is, not what the store
/// has been through.
///
/// Verifying a snapshot recomposes its history summary, and completing each
/// device's acknowledgement chain used to walk that chain back to sequence one
/// — over acknowledgements the device's own baseline advance had retired, so
/// every one of them was a provider read. A live store paid 235 requests and
/// twenty seconds to acknowledge one new generation. The signature over the
/// snapshot the baseline stands on already states those chains; below the
/// coverage they resolve to the coverage, like everything else.
///
/// Two histories of different depth, because the number itself is only
/// interesting if it is the same one.
#[tokio::test]
async fn acknowledging_a_snapshot_costs_the_same_whatever_the_history_behind_it() {
    let shallow = AcknowledgedHistory::publish(2)
        .await
        .cycle_requests_after_a_new_snapshot()
        .await;
    let deep = AcknowledgedHistory::publish(6)
        .await
        .cycle_requests_after_a_new_snapshot()
        .await;

    assert_eq!(
        shallow, deep,
        "the cycle that acknowledges a new snapshot grew with history depth: \
         two rounds asked for {shallow} operations, six asked for {deep}",
    );
    assert_eq!(
        deep, 33,
        "the cycle that acknowledges a new snapshot asked for {deep} operations",
    );
}

/// And the same however many generations the store has published before it.
///
/// Choosing which snapshot to acknowledge reads the publisher's snapshot
/// stream and verifies a candidate out of it. Reading the stream is how a
/// device finds a generation it has not seen; verifying every candidate to
/// pick the one that dominates them all is not, and the dominant one is known
/// before any of them is verified.
#[tokio::test]
async fn acknowledging_a_snapshot_costs_the_same_whatever_was_published_before_it() {
    let few = AcknowledgedHistory::publish(3)
        .await
        .cycle_requests_after_snapshot_number(2)
        .await;
    let many = AcknowledgedHistory::publish(3)
        .await
        .cycle_requests_after_snapshot_number(5)
        .await;

    assert_eq!(
        few, many,
        "the cycle that acknowledges a new snapshot grew with the generations \
         published before it: the second generation asked for {few} operations, \
         the fifth asked for {many}",
    );
}

/// A retained row holds one acknowledgement or none — never a chain of them.
///
/// This replaces `retained_materialization_rows_do_not_repeat_predecessor_
/// history`, which compared sequence 2 against sequence 12 with a 1 024-byte
/// tolerance. The row was growing about 6 KB per acknowledging commit the whole
/// time that test was green: over ten commits the growth fit inside the
/// tolerance, and over three hundred it was a 223 MB table.
///
/// Two assertions, because neither alone is enough. The structural one is exact
/// and is the invariant: a row carries at most one acknowledgement. The size one
/// catches anything else that might start accumulating, and its bound is derived
/// rather than picked — the spread across a whole history must stay under the
/// size of a single acknowledgement, so a row cannot have gained even one extra.
/// An exact byte equality is not available and would be false: an
/// acknowledgement names a cut, a cut names sequence numbers, and JSON writes
/// those in decimal, so the same shape encodes three bytes wider once sequences
/// reach two digits.
async fn retained_acknowledgement_evidence(rounds: u64) -> Vec<(u64, usize, usize)> {
    let fixture = AcknowledgedHistory::publish(rounds).await;
    let retained = fixture.retained_history().await;
    let stream = retained
        .last()
        .expect("published history")
        .commit_ref()
        .coord
        .stream_id
        .to_string();
    let mut sequences = retained
        .iter()
        .filter(|entry| entry.commit_ref().coord.stream_id.to_string() == stream)
        .map(|entry| entry.commit_ref().coord.sequence())
        .collect::<Vec<_>>();
    sequences.sort();
    let mut rows = Vec::new();
    for sequence in sequences {
        let bytes = fixture
            .db
            .retained_canonical_input_for_test(stream.clone(), sequence)
            .await
            .expect("read the retained row");
        let row: serde_json::Value = serde_json::from_slice(&bytes).expect("row is JSON");
        let evidence = &row["history_evidence"];
        let acknowledgements = match &evidence["acknowledgement"] {
            serde_json::Value::Null => 0,
            activated => {
                assert!(
                    activated.get("chain").is_none(),
                    "a retained row carries an acknowledgement chain at sequence {sequence}",
                );
                activated["acknowledgement"]
                    .as_array()
                    .map(|_| 1)
                    .expect("an activated acknowledgement is one reference and value")
            }
        };
        rows.push((
            sequence,
            acknowledgements,
            serde_json::to_vec(evidence).expect("evidence").len(),
        ));
    }
    rows
}

/// One acknowledgement per row is the length of the chain a row may carry, and
/// it does not change with how much history the row sits on top of.
#[tokio::test]
async fn a_retained_row_never_carries_an_acknowledgement_chain() {
    for rounds in [4_u64, 30] {
        let rows = retained_acknowledgement_evidence(rounds).await;
        assert!(
            rows.iter().all(|(_, count, _)| *count <= 1),
            "a retained row carried more than one acknowledgement at {rounds} rounds",
        );
        assert!(
            rows.iter().any(|(_, count, _)| *count == 1),
            "the fixture must retain acknowledging commits to mean anything",
        );
    }
}

#[tokio::test]
async fn a_retained_row_costs_the_same_at_every_sequence() {
    let rows = retained_acknowledgement_evidence(25).await;
    // A stream's first commits introduce the device and have a different shape.
    let acknowledging = rows[2..]
        .iter()
        .filter(|(_, count, _)| *count == 1)
        .collect::<Vec<_>>();
    assert!(
        acknowledging.len() >= 8,
        "the fixture must retain enough acknowledging commits to compare: {acknowledging:?}",
    );
    let smallest = acknowledging
        .iter()
        .map(|(_, _, bytes)| *bytes)
        .min()
        .expect("acknowledging rows exist");
    let largest = acknowledging
        .iter()
        .map(|(_, _, bytes)| *bytes)
        .max()
        .expect("acknowledging rows exist");
    // One acknowledgement is the unit that used to accumulate, so the whole
    // history's spread staying under one of them is the statement that none of
    // these rows gained a second.
    assert!(
        largest - smallest < smallest / 2,
        "a retained row's evidence grew across the history: smallest {smallest}, \
         largest {largest} — {acknowledging:?}",
    );

    let plain = rows[2..]
        .iter()
        .filter(|(_, count, _)| *count == 0)
        .collect::<Vec<_>>();
    let first_plain = plain[0].2;
    assert!(
        plain.iter().all(|(_, _, bytes)| *bytes == first_plain),
        "a row with no acknowledgement stopped being a fixed size: {plain:?}",
    );
}

/// A Store where nothing is happening stops growing.
///
/// Publishing an acknowledgement appends a commit, and that commit moves the
/// frontier the next acknowledgement would name — so a device that acknowledges
/// whatever it currently sees acknowledges its own acknowledgement, and an idle
/// Store gains a commit per device per cycle without end. The live store this
/// was found on carried 385 commits behind 16 host writes.
#[tokio::test]
async fn an_idle_cycle_appends_no_commit() {
    let fixture = AcknowledgedHistory::publish(2).await;
    // The fixture's last round leaves this device with the peer's commit to
    // acknowledge. This cycle says that; every cycle after it has nothing to add.
    fixture
        .device
        .run_cycle(None)
        .await
        .expect("settle the cycle");
    let settled = fixture.retained_history().await.len();

    for cycle in 1..=4 {
        fixture
            .device
            .run_cycle(None)
            .await
            .expect("run an idle cycle");
        assert_eq!(
            fixture.retained_history().await.len(),
            settled,
            "idle cycle {cycle} appended a commit to a Store where nothing happened",
        );
    }
}

/// And starts again the moment there is something to say. The guard withholds an
/// acknowledgement that repeats itself, never one that carries news.
#[tokio::test]
async fn a_cycle_with_something_to_say_acknowledges_it() {
    let fixture = AcknowledgedHistory::publish(2).await;
    fixture
        .device
        .run_cycle(None)
        .await
        .expect("settle the cycle");
    fixture
        .device
        .run_cycle(None)
        .await
        .expect("confirm the settled cycle is idle");
    let settled = fixture.retained_history().await.len();

    HistoryPublisher::new(&fixture.peer_db, &fixture.peer)
        .publish_note(99)
        .await;
    fixture
        .device
        .run_cycle(None)
        .await
        .expect("pull and acknowledge the peer's commit");
    assert_eq!(
        fixture.retained_history().await.len(),
        settled + 2,
        "the peer's commit and this device's acknowledgement of it both land",
    );

    fixture
        .device
        .run_cycle(None)
        .await
        .expect("run an idle cycle again");
    assert_eq!(
        fixture.retained_history().await.len(),
        settled + 2,
        "and the Store settles again once the news is acknowledged",
    );
}

impl AcknowledgedHistory {
    /// Provider operations one snapshot publication asks for.
    async fn snapshot_publication_requests(&self) -> u64 {
        let image_dir = tempfile::tempdir().expect("snapshot image dir");
        let image = coven_database::StoreDatabase::new(&self.db)
            .capture_snapshot_image_for_test(
                self.device.store_root().clone(),
                image_dir.path().to_path_buf(),
                None,
            )
            .await
            .expect("capture a snapshot image");
        let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
            coven_database::StoreDatabase::new(&self.db)
                .materialized_frontier()
                .await
                .expect("materialized frontier"),
        )
        .expect("frontier");
        let before = self._store.provider_requests_issued();
        self.device
            .publish_snapshot(image, coverage)
            .await
            .expect("publish the snapshot");
        self._store.provider_requests_issued() - before
    }

    /// Let both devices see the snapshot just published, and this one advance
    /// its replay baseline onto the acknowledgement it makes of it — which is
    /// what retires the retained rows the next composition would otherwise
    /// have read its acknowledgements out of.
    async fn settle_onto_the_published_snapshot(&self) {
        self.device.run_cycle(None).await.expect("settle the cycle");
        self.peer.run_cycle(None).await.expect("the peer settles");
        self.device
            .run_cycle(None)
            .await
            .expect("stand on the snapshot just acknowledged");
        self.peer
            .run_cycle(None)
            .await
            .expect("the peer stands on the snapshot it acknowledged");
        self.device
            .run_cycle(None)
            .await
            .expect("the device observes the peer acknowledgement");
    }
}

/// Publishing a snapshot asks the provider for the objects it writes, and
/// nothing that depends on how much history stands behind it or how many
/// snapshots came before.
///
/// The composition it runs is the same one snapshot verification runs, and it
/// fails the same way: a summary states every device's acknowledgement chain
/// from sequence one, the walk that builds it stops at the first
/// acknowledgement the verifier already holds, and a device that has advanced
/// its baseline holds none of them — the rows a verifier seeds from are the
/// retained materializations, which the advance retires. Measured against a
/// build without the baseline's summary admitted, publishing climbed on both
/// axes at once: 21 requests at four rounds of history rising to 43 by the
/// fourth generation over it. Live, publishing generation 2 of a five-release
/// library cost 186 requests and 23 seconds.
///
/// So this is a budget, held at an exact number rather than a bound: the
/// publication writes what it writes, and asks nothing per commit, per
/// acknowledgement, or per generation.
#[tokio::test]
async fn publishing_a_snapshot_asks_for_what_it_writes_and_nothing_per_commit() {
    /// Allocate and write the image, the membership rollup and the metadata,
    /// resolve the predecessor slot, and read the membership the rollup states.
    /// Every one of them is a fact about this publication, not about the past.
    const PUBLICATION_REQUESTS: u64 = 17;

    for rounds in [1u64, 4, 8] {
        let fixture = AcknowledgedHistory::publish(rounds).await;
        fixture.settle_onto_the_published_snapshot().await;
        for generation in 0..4 {
            assert_eq!(
                fixture.snapshot_publication_requests().await,
                PUBLICATION_REQUESTS,
                "generation {generation} over {rounds} rounds of history",
            );
            fixture.settle_onto_the_published_snapshot().await;
        }
    }
}

/// The owner's journal for a join is retired when the joined device arrives,
/// and not before.
///
/// The owner's half of a join ended at a published activation commit and stayed
/// there for the life of the store: `ActivationPrepared` is permanently a
/// "hand the activation over" action, and the owner has no artifact by which it
/// could learn the joining device took it — the same asymmetry that makes the
/// joiner delete the attempt's transport slots. So every join a device ever
/// hosted left a row behind, holding the completion and the activation.
///
/// What the owner can see is the joined device's own first commit. Everything
/// else about the join is something the owner wrote — the registration goes
/// Active from the owner's own activation commit, so it says nothing about
/// whether that device ever ran. A stream under the joined registration's
/// announcement stream id appearing in the materialized frontier is a commit
/// that device signed and this one verified.
#[tokio::test]
async fn an_owner_retires_a_join_when_the_joined_device_arrives() {
    use crate::sync::store::DeviceJoinAction;

    let founder = UserKeypair::generate();
    let member = UserKeypair::generate();
    let member_pubkey = hex::encode(member.public_key());
    let store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(store_dir.clone());
    let home = crate::sync::test_helpers::test_cloud_home();
    let (store, _storage) = TestStore::create_with_connection(
        &db,
        store_dir.clone(),
        "join-retirement",
        founder.clone(),
        home.clone(),
    )
    .await
    .expect("create Merge Store");
    let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    store
        .admit_member(
            &db,
            store_dir.clone(),
            &founder,
            &member_pubkey,
            None,
            coven_protocol::membership::MemberRole::Member,
            &encryption,
            "Join Retirement Store",
        )
        .await
        .expect("admit the peer as a Member");
    let peer_store_dir = crate::sync::test_helpers::test_store_dir();
    let peer_db = crate::sync::test_helpers::open_test_db(peer_store_dir.clone());
    let peer = store
        .activate_joined_device(
            &db,
            store_dir.clone(),
            &peer_db,
            peer_store_dir.clone(),
            &member,
            "2026-03-01T00:00:45Z",
        )
        .await
        .expect("activate the peer device");
    let owner = store
        .bind_device_in(&db, store_dir.clone(), &founder)
        .await
        .expect("bind the owner device");
    let owner_db = coven_database::StoreDatabase::new(&db);

    let awaiting_activation = |actions: &[DeviceJoinAction]| {
        actions.iter().any(|action| {
            matches!(
                action,
                DeviceJoinAction::TransferActivation(_)
                    | DeviceJoinAction::TransferSamePrincipalJoin(_)
            )
        })
    };
    assert!(
        awaiting_activation(
            &owner_db
                .device_join_actions()
                .await
                .expect("read the owner's join actions")
        ),
        "the owner holds the join it published for the joining device",
    );

    // A cycle before the joined device has published anything of its own must
    // leave the row alone: the registration is already Active, because the
    // owner activated it, and that is exactly the evidence that proves nothing.
    owner
        .run_cycle(None)
        .await
        .expect("run a cycle before the joined device arrives");
    assert!(
        awaiting_activation(
            &owner_db
                .device_join_actions()
                .await
                .expect("read the owner's join actions again")
        ),
        "a device that has published nothing has not arrived",
    );

    HistoryPublisher::new(&peer_db, &peer).publish_note(1).await;
    owner
        .run_cycle(None)
        .await
        .expect("pull the joined device's first commit");

    assert!(
        !awaiting_activation(
            &owner_db
                .device_join_actions()
                .await
                .expect("read the owner's join actions after arrival")
        ),
        "the joined device published its own commit, so the owner's journal is retired",
    );
}
