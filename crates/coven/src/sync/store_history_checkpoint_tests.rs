use crate::keys::UserKeypair;
use crate::protocol::membership::MembershipChain;
use crate::protocol::objects::ExactObjectRef;
use crate::protocol::objects::ObjectSlot;
use crate::protocol::store_commit::{ObjectHash, StoreDeviceHeadRef};
use crate::sync::test_helpers::{open_test_db, temp_store_dir, TestStore};

fn store_database(db: &crate::database::Database) -> crate::database::StoreDatabase {
    crate::database::StoreDatabase::new(db)
}

struct HistoryPublisher<'fixture> {
    database: &'fixture crate::database::Database,
    device: &'fixture crate::sync::test_helpers::TestDevice,
    store_dir: &'fixture coven_foundation::store_dir::StoreDir,
}

impl<'fixture> HistoryPublisher<'fixture> {
    fn new(
        database: &'fixture crate::database::Database,
        device: &'fixture crate::sync::test_helpers::TestDevice,
        store_dir: &'fixture coven_foundation::store_dir::StoreDir,
    ) -> Self {
        Self {
            database,
            device,
            store_dir,
        }
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
                .prepare_pending_store_write(self.store_dir)
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
    db: crate::database::Database,
    home: std::sync::Arc<crate::InMemoryCloudHome>,
    device: crate::sync::test_helpers::TestDevice,
    membership: MembershipChain,
    _temp: tempfile::TempDir,
    store_dir: coven_foundation::store_dir::StoreDir,
}

impl PublishedHistory {
    async fn publish(history_length: u64) -> Self {
        let signer = UserKeypair::generate();
        let db = open_test_db();
        let home = crate::sync::test_helpers::test_cloud_home();
        let store = TestStore::create(
            &db,
            &format!("checkpoint-sabotage-{history_length}"),
            signer.clone(),
            home.clone(),
        )
        .await
        .expect("create Merge Store");
        let device = store
            .bind_device(&db, &signer)
            .await
            .expect("load Merge Store");
        let membership = device
            .membership_for_test()
            .await
            .expect("load Merge membership");
        let (temp, store_dir) = temp_store_dir();
        let publisher = HistoryPublisher::new(&db, &device, &store_dir);
        for sequence in 1..=history_length {
            publisher.publish_note(sequence).await;
        }
        let fixture = Self {
            db,
            home,
            device,
            membership,
            _temp: temp,
            store_dir,
        };
        assert_eq!(
            fixture.retained_history().await.len() as u64,
            history_length,
            "every published Merge commit has retained verified inputs",
        );
        fixture
    }

    async fn retained_history(&self) -> Vec<crate::database::OwnedVerifiedMergeMaterialization> {
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
        HistoryPublisher::new(&self.db, &self.device, &self.store_dir)
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
        match self
            .device
            .prepare_pending_store_write(&self.store_dir)
            .await
        {
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

async fn assert_signed_head_rejects_summary(
    device: &crate::sync::test_helpers::TestDevice,
    retained: &crate::database::OwnedVerifiedMergeMaterialization,
    summary: &crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
) {
    let state = device
        .resolved_store_device_state_for_test(&retained.history_summary().post_state)
        .await
        .expect("load retained post-state");
    let head_ref = StoreDeviceHeadRef {
        head_hash: retained.activation_head().head_hash(),
        object: retained.activation_head_object().clone(),
    };
    assert!(
        summary
            .open(
                retained.commit(),
                retained.commit_ref(),
                retained.activation_head(),
                &head_ref,
                &state,
            )
            .is_err(),
        "changed retained summary must fail its accepted signed head",
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
async fn missing_frontier_retained_row_has_no_cloud_fallback() {
    let fixture = PublishedHistory::publish(1).await;
    let retained = fixture.retained_history().await;
    let reference = retained[0].commit_ref().clone();
    let reference = reference.clone();
    fixture
        .db
        .test_sql(move |database| {
            database.delete_retained_materialization_without_foreign_keys(&reference)
        })
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
                .test_sql(move |conn| conn.delete_device_state_snapshot(&encoded))
                .await
                .expect("delete checkpoint state");
        } else {
            let state = fixture
                .device
                .resolved_store_device_state_for_test(&retained[0].history_summary().post_state)
                .await
                .expect("load canonical retained state");
            let root = crate::database::StoreDatabase::new(&fixture.db)
                .local_store_root_ref()
                .await
                .expect("load Store root")
                .expect("Store root exists");
            let grant = crate::protocol::membership::MembershipGrantId(ObjectHash::digest(
                b"forged-checkpoint-recovery",
            ));
            let anchor = crate::protocol::store_commit::GrantStreamAnchor::OwnerRecovery {
                first_slot: retained[0].activation_head_object().slot().clone(),
            };
            let activation = crate::protocol::store_commit::OwnerRecoveryActivationId::derive(
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
                .test_sql(move |conn| conn.replace_device_state_snapshot(&encoded, &forged))
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
async fn changed_and_locally_rehashed_summary_omissions_are_rejected() {
    let fixture = PublishedHistory::publish(2).await;
    let retained = fixture.retained_history().await;
    let current = retained.last().expect("two retained commits");
    let state = fixture
        .device
        .resolved_store_device_state_for_test(&current.history_summary().post_state)
        .await
        .expect("load retained post-state");
    let original_head_ref = StoreDeviceHeadRef {
        head_hash: current.activation_head().head_hash(),
        object: current.activation_head_object().clone(),
    };
    let mut omitted = current.history_summary().clone();
    omitted
        .registrations
        .remove(&current.commit().author_registration.device_id);
    assert!(
        omitted
            .open(
                current.commit(),
                current.commit_ref(),
                current.activation_head(),
                &original_head_ref,
                &state,
            )
            .is_err(),
        "changing summary bytes must fail the accepted head digest",
    );

    let forged_head = fixture
        .device
        .sign_device_head_for_test(
            current.commit_ref().clone(),
            omitted.digest(),
            current.activation_head().successor.clone(),
        )
        .await
        .expect("sign locally rehashed head through Store authority");
    let forged_bytes = forged_head.to_bytes();
    let forged_head_ref = StoreDeviceHeadRef {
        head_hash: forged_head.head_hash(),
        object: ExactObjectRef::new(
            current.activation_head_object().slot().clone(),
            forged_bytes.len() as u64,
            ObjectHash::digest(&forged_bytes),
        ),
    };
    assert!(
        omitted
            .open(
                current.commit(),
                current.commit_ref(),
                &forged_head,
                &forged_head_ref,
                &state,
            )
            .is_err(),
        "a locally rehashed summary cannot omit its current registration proof",
    );

    let mut omitted_head = current.history_summary().clone();
    omitted_head.announcement_frontier.clear();
    let forged_head = fixture
        .device
        .sign_device_head_for_test(
            current.commit_ref().clone(),
            omitted_head.digest(),
            current.activation_head().successor.clone(),
        )
        .await
        .expect("sign head-omitting checkpoint through Store authority");
    let forged_bytes = forged_head.to_bytes();
    let forged_head_ref = StoreDeviceHeadRef {
        head_hash: forged_head.head_hash(),
        object: ExactObjectRef::new(
            current.activation_head_object().slot().clone(),
            forged_bytes.len() as u64,
            ObjectHash::digest(&forged_bytes),
        ),
    };
    assert!(
        omitted_head
            .open(
                current.commit(),
                current.commit_ref(),
                &forged_head,
                &forged_head_ref,
                &state,
            )
            .is_err(),
        "a locally rehashed summary cannot omit the predecessor announcement head",
    );
}

#[tokio::test]
async fn signed_head_rejects_an_omitted_acknowledgement() {
    let fixture = PublishedHistory::publish(1).await;
    let coverage = crate::protocol::store_commit::CommitFrontier::from_refs(
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
    assert_eq!(
        current.history_summary().acknowledgements.len(),
        1,
        "acknowledgement fixture retains one latest exact acknowledgement",
    );
    let mut omitted = current.history_summary().clone();
    omitted.acknowledgements.clear();
    assert_signed_head_rejects_summary(&fixture.device, current, &omitted).await;
}

struct MemberRemovalHistory {
    db: crate::database::Database,
    store: std::sync::Arc<TestStore>,
    device: crate::sync::test_helpers::TestDevice,
    summary: crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
}

impl MemberRemovalHistory {
    async fn create() -> Self {
        let db = open_test_db();
        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let member_pubkey = crate::sync::test_helpers::pubkey_hex(&member);
        let encryption = crate::encryption::EncryptionService::from_key([42; 32]);
        let store = TestStore::create(
            &db,
            "retained-removal-proof",
            owner.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create removal-proof Store");
        store
            .invite_member(
                &db,
                &owner,
                &member_pubkey,
                None,
                crate::protocol::membership::MemberRole::Member,
                &encryption,
                "Retained removal proof",
            )
            .await
            .expect("invite removable member");
        let member_db = open_test_db();
        store
            .activate_joined_device(&db, &member_db, &member, "2026-07-21T00:00:00Z")
            .await
            .expect("activate removable member device");
        store
            .promote_active_member_fixture(&db, &member_db, &owner, &member, &encryption)
            .await
            .expect("promote removable member to Owner");
        let custody = crate::sync::test_helpers::TestCustody::default();
        store
            .remove_member(&db, &owner, &member_pubkey, &encryption, &custody)
            .await
            .expect("remove retained member");
        let device = store
            .bind_device(&db, &owner)
            .await
            .expect("bind retained-removal Store");
        let retained = device
            .retained_merge_replay_inputs_for_test()
            .await
            .expect("load retained removal history");
        let summary = retained
            .last()
            .expect("removal activation is retained")
            .history_summary()
            .clone();
        Self {
            db,
            store,
            device,
            summary,
        }
    }
}

#[tokio::test]
async fn signed_head_rejects_an_omitted_membership_removal() {
    let fixture = Box::pin(MemberRemovalHistory::create()).await;
    let retained = fixture
        .device
        .retained_merge_replay_inputs_for_test()
        .await
        .expect("load retained removal history");
    let current = retained.last().expect("removal activation is retained");
    let removal = fixture
        .summary
        .membership_proofs
        .iter()
        .find_map(|(reference, proof)| {
            matches!(
                proof.entry_value.change,
                crate::protocol::membership::MembershipChange::RemoveMember { .. }
            )
            .then(|| reference.clone())
        })
        .expect("retained history contains the removal control proof");
    let mut omitted = fixture.summary;
    omitted.membership_proofs.remove(&removal);
    assert_signed_head_rejects_summary(&fixture.device, current, &omitted).await;
}

#[tokio::test]
async fn membership_checkpoint_floor_includes_the_activating_control() {
    let fixture = Box::pin(MemberRemovalHistory::create()).await;
    let control = fixture
        .summary
        .membership_proofs
        .values()
        .find(|proof| {
            matches!(
                proof.entry_value.change,
                crate::protocol::membership::MembershipChange::RemoveMember { .. }
            )
        })
        .expect("retained history contains the removal control proof");
    assert!(fixture
        .summary
        .membership_floor
        .effective_coordinates
        .contains(&control.entry.coord));
}

#[tokio::test]
async fn retained_membership_proof_rejects_an_incomplete_resolution_authority() {
    let mut fixture = Box::pin(MemberRemovalHistory::create()).await;
    let proof = fixture
        .summary
        .membership_proofs
        .values_mut()
        .find(|proof| {
            matches!(
                proof.entry_value.change,
                crate::protocol::membership::MembershipChange::RemoveMember { .. }
            )
        })
        .expect("retained history contains a membership proof");
    let bytes = b"incomplete retained resolution authority";
    proof.resolution = Some(
        crate::protocol::membership::StoreMembershipConflictResolutionRef {
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
        fixture.summary.validate_shape().is_err(),
        "retained membership proof accepted a resolution reference without its signed value",
    );
}

#[tokio::test]
async fn signed_snapshot_rejects_an_omitted_pre_snapshot_membership_control() {
    let fixture = Box::pin(MemberRemovalHistory::create()).await;
    let directory = tempfile::tempdir().expect("create snapshot image directory");
    let snapshot_dir = directory.path().to_path_buf();
    let database = store_database(&fixture.db);
    let image = database
        .capture_snapshot_image_for_test(fixture.store.root.clone(), snapshot_dir, None)
        .await
        .expect("create checkpoint snapshot image");
    let coverage = crate::protocol::store_commit::CommitFrontier::from_refs(
        database
            .materialized_frontier()
            .await
            .expect("load snapshot coverage"),
    )
    .expect("derive snapshot coverage");
    let meta = fixture
        .device
        .publish_snapshot(image, coverage)
        .await
        .expect("publish checkpoint snapshot");
    let published = crate::database::StoreDatabase::new(&fixture.db)
        .latest_local_store_snapshot()
        .await
        .expect("load published snapshot")
        .expect("published snapshot is recorded");
    let mut forged = meta;
    let summary = &mut forged.body_mut().history_summary;
    let removal = summary
        .membership_proofs
        .iter()
        .find_map(|(reference, proof)| {
            matches!(
                proof.entry_value.change,
                crate::protocol::membership::MembershipChange::RemoveMember { .. }
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
    let forged_reference = crate::protocol::store_commit::StoreSnapshotRef {
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
    let forged = crate::database::PublishedStoreSnapshot {
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
