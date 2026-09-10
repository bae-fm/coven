use crate::sync::test_helpers::TestStore;
use coven_keys::keys::UserKeypair;
use coven_protocol::membership::MembershipChain;
use coven_protocol::objects::ObjectSlot;
use coven_protocol::store_commit::ObjectHash;

#[path = "acknowledged_history_tests.rs"]
mod acknowledged_history_tests;
#[path = "snapshot_membership_history_tests.rs"]
mod snapshot_membership_history_tests;

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
    device: crate::sync::test_helpers::TestDevice,
    membership: MembershipChain,
}

impl PublishedHistory {
    async fn publish(history_length: u64) -> Self {
        let signer = UserKeypair::generate();
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, _storage) = TestStore::create_with_connection(
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
                    entry
                        .acceptance()
                        .exact_publication()
                        .expect("uncompacted history has exact acceptance")
                        .reference()
                        .object
                        .slot()
                        .clone(),
                ]
            })
            .collect::<Vec<_>>();
        let registration_anchor_publication_slot = retained
            .first()
            .expect("published history has a first retained commit")
            .acceptance()
            .exact_publication()
            .expect("uncompacted history has exact acceptance")
            .reference()
            .object
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
        (reread, registration_anchor_publication_slot)
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
async fn merge_successor_publication_does_not_reread_materialized_history() {
    let (shallow, _) = PublishedHistory::publish(1)
        .await
        .historical_read_slots()
        .await;
    let (deeper, registration_anchor_publication_slot) = PublishedHistory::publish(100)
        .await
        .historical_read_slots()
        .await;
    assert!(
        shallow.is_empty()
            && deeper.is_empty()
            && !deeper.contains(&registration_anchor_publication_slot),
        "publishing after one retained commit reread {} historical commit/publication objects; \
         publishing after 100 reread {} (registration anchor publication {registration_anchor_publication_slot:?}): \
         shallow={shallow:?}, deeper={deeper:?}",
        shallow.len(),
        deeper.len(),
    );
}

#[tokio::test]
async fn retained_history_reuses_each_verified_publication_within_a_pull() {
    let fixture = PublishedHistory::publish(12).await;
    let publication_slots = fixture
        .retained_history()
        .await
        .into_iter()
        .map(|materialization| {
            materialization
                .acceptance()
                .exact_publication()
                .expect("uncompacted history has exact acceptance")
                .reference()
                .object
                .slot()
                .clone()
        })
        .collect::<Vec<_>>();
    fixture.home.clear_exact_reads();

    fixture
        .device
        .pull_store()
        .await
        .expect("pull retained announcement history");

    let reads = fixture.home.exact_reads();
    let counts = publication_slots
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

#[tokio::test]
async fn snapshot_capture_keeps_new_retirement_obligations_without_probing_them() {
    let fixture = PublishedHistory::publish(12).await;
    let publications = fixture
        .retained_history()
        .await
        .into_iter()
        .map(|input| {
            input
                .acceptance()
                .exact_publication()
                .expect("uncompacted publication")
                .reference()
                .object
                .slot()
                .clone()
        })
        .collect::<Vec<_>>();
    let directory = tempfile::tempdir().expect("snapshot image directory");
    let database = store_database(&fixture.db);
    let image = database
        .capture_snapshot_image_for_test(
            fixture.device.store_root().clone(),
            directory.path().to_path_buf(),
            None,
        )
        .await
        .expect("capture accepted image");
    let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
        database
            .materialized_frontier()
            .await
            .expect("accepted frontier"),
    )
    .expect("accepted coverage");
    fixture.home.clear_exact_reads();
    let metadata = fixture
        .device
        .publish_snapshot(image, coverage)
        .await
        .expect("publish accepted snapshot");
    let obligations = &metadata.history_summary.reclaim.publications;
    assert!(
        publications.iter().all(|slot| obligations
            .values()
            .any(|publication| publication.object.slot() == slot)),
        "every newly covered publication has a deletion owner"
    );
    let reread = fixture
        .home
        .exact_reads()
        .into_iter()
        .filter(|slot| publications.contains(slot))
        .collect::<Vec<_>>();
    assert!(
        reread.is_empty(),
        "new deletion obligations do not require absence probes: {reread:?}"
    );
}

async fn membership_head_reads_for_retained_history(history_length: u64) -> usize {
    let fixture = PublishedHistory::publish(history_length).await;
    let publication_slots = fixture
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
        .filter(|slot| publication_slots.contains(slot))
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
        .commit()
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
        .commit()
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
                first_slot: retained[0]
                    .acceptance()
                    .exact_publication()
                    .expect("uncompacted history has exact acceptance")
                    .reference()
                    .object
                    .slot()
                    .clone(),
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

#[tokio::test]
async fn operation_authorization_reads_retained_checkpoints_not_store_history() {
    let fixture = PublishedHistory::publish(4).await;
    let retained = fixture.retained_history().await;
    let historical_slots = retained
        .iter()
        .flat_map(|entry| {
            [
                entry.commit_ref().object.slot().clone(),
                entry
                    .acceptance()
                    .exact_publication()
                    .expect("uncompacted history has exact acceptance")
                    .reference()
                    .object
                    .slot()
                    .clone(),
            ]
        })
        .collect::<Vec<_>>();
    fixture.home.clear_exact_reads();
    fixture
        .device
        .prepare_store_operation_plan_for_test()
        .await
        .expect("authorize from retained operation predecessor");
    let reread = fixture
        .home
        .exact_reads()
        .into_iter()
        .filter(|slot| historical_slots.contains(slot))
        .collect::<Vec<_>>();
    assert!(
        reread.is_empty(),
        "operation authorization reread historical Store commit/publication slots: {reread:?}",
    );
}

/// The pull-side twin of
/// `operation_authorization_reads_retained_checkpoints_not_store_history`.
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
                entry
                    .acceptance()
                    .exact_publication()
                    .expect("uncompacted history has exact acceptance")
                    .reference()
                    .object
                    .slot()
                    .clone(),
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
        "pulls re-read Store commit/publication objects the device had already verified: \
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
