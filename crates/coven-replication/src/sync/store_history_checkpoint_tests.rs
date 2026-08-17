use crate::sync::test_helpers::TestStore;
use coven_keys::keys::UserKeypair;
use coven_protocol::membership::MembershipChain;
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::objects::ObjectSlot;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::ObjectHash;
use coven_storage::CloudSyncObjectStorage;

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

    async fn retained_input_size(&self, sequence: u64) -> usize {
        let retained = self.retained_history().await;
        let stream_id = retained
            .iter()
            .find(|materialization| materialization.commit_ref().coord.sequence() == sequence)
            .expect("retained history contains requested sequence")
            .commit_ref()
            .coord
            .stream_id
            .to_string();
        self.db
            .retained_canonical_input_for_test(stream_id, sequence)
            .await
            .expect("load retained materialization input")
            .len()
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
async fn retained_materialization_rows_do_not_repeat_predecessor_history() {
    let fixture = PublishedHistory::publish(12).await;
    let first_successor = fixture.retained_input_size(2).await;
    let last = fixture.retained_input_size(12).await;

    assert!(
        last <= first_successor + 1_024,
        "retained input grew with predecessor history: first successor={first_successor} bytes, \
         last={last} bytes",
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
                .and_then(|acknowledgement| acknowledgement.latest())
                .map(|(reference, _)| reference.object.slot().clone())
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
            .verify_snapshots_for_acknowledgement_for_test(std::slice::from_ref(&forged))
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
