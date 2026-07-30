use crate::keys::UserKeypair;
use crate::protocol::membership::MembershipChain;
use crate::protocol::store_commit::{ObjectHash, StoreDeviceHeadRef};
use crate::storage::cloud::ObjectSlot;
use crate::storage::ExactObjectRef;
use crate::sync::test_helpers::{host_exec, open_test_db, temp_store_dir, TestStore};

fn store_database(db: &crate::database::Database) -> crate::database::StoreDatabase {
    crate::database::StoreDatabase::new(db)
}

async fn retained_history(
    db: &crate::database::Database,
    store: &TestStore,
) -> Vec<crate::database::OwnedVerifiedMergeMaterialization> {
    store
        .bind_device(db, &store.signer)
        .await
        .expect("load retained-history Store")
        .retained_merge_replay_inputs_for_test()
        .await
        .expect("load retained verified Merge history")
}

async fn publish_note(
    db: &crate::database::Database,
    store: &TestStore,
    store_dir: &crate::store_dir::StoreDir,
    sequence: u64,
) {
    host_exec(
        db,
        &format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('history-{sequence}', 'history', NULL, 1, \
                     '0000000001000-0000-history', '2026-07-21')"
        ),
    )
    .await;
    let loaded = store
        .bind_device(db, &store.signer)
        .await
        .expect("load Merge Store");
    let mut writer = loaded
        .authorize_writer()
        .await
        .expect("authorize Merge Store writer");
    assert!(
        writer
            .prepare_pending_store_write(store_dir)
            .await
            .expect("prepare Merge Store write"),
        "host write produces a prepared Store commit",
    );
    assert_eq!(
        writer
            .drain_store_writes()
            .await
            .expect("publish Merge Store write"),
        1,
        "one prepared Store commit is published",
    );
}

async fn historical_read_slots(history_length: u64) -> (Vec<ObjectSlot>, ObjectSlot) {
    let signer = UserKeypair::generate();
    let db = open_test_db();
    let store = TestStore::create(
        &db,
        &format!("materialized-history-{history_length}"),
        signer,
    )
    .await
    .expect("create Merge Store");
    let (_temp, store_dir) = temp_store_dir();

    for sequence in 1..=history_length {
        publish_note(&db, &store, &store_dir, sequence).await;
    }

    let retained = retained_history(&db, &store).await;
    assert_eq!(
        retained.len() as u64,
        history_length,
        "every published Merge commit has retained verified inputs",
    );
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

    store.home.clear_exact_reads();
    publish_note(&db, &store, &store_dir, history_length + 1).await;

    let reread = store
        .home
        .exact_reads()
        .into_iter()
        .filter(|slot| historical_slots.contains(slot))
        .collect();
    (reread, registration_anchor_head_slot)
}

async fn published_history(
    history_length: u64,
) -> (
    crate::database::Database,
    TestStore,
    String,
    MembershipChain,
    tempfile::TempDir,
    crate::store_dir::StoreDir,
) {
    let signer = UserKeypair::generate();
    let db = open_test_db();
    let store = TestStore::create(
        &db,
        &format!("checkpoint-sabotage-{history_length}"),
        signer,
    )
    .await
    .expect("create Merge Store");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("load local Store device id")
        .expect("created Store has a local device id");
    let membership = store
        .bind_device(&db, &store.signer)
        .await
        .expect("load Merge Store")
        .membership_for_test()
        .await
        .expect("load Merge membership");
    let (temp, store_dir) = temp_store_dir();
    for sequence in 1..=history_length {
        publish_note(&db, &store, &store_dir, sequence).await;
    }
    (db, store, device_id, membership, temp, store_dir)
}

async fn prepare_sabotaged_successor(
    db: &crate::database::Database,
    store: &TestStore,
    store_dir: &crate::store_dir::StoreDir,
) -> String {
    host_exec(
        db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('sabotaged-successor', 'history', NULL, 1, \
                 '0000000002000-0000-history', '2026-07-21')",
    )
    .await;
    let loaded = store
        .bind_device(db, &store.signer)
        .await
        .expect("load sabotaged successor Store");
    let mut writer = loaded
        .authorize_writer()
        .await
        .expect("authorize sabotaged successor writer");
    match writer.prepare_pending_store_write(store_dir).await {
        Err(error) => error.to_string(),
        Ok(true) => writer
            .drain_store_writes()
            .await
            .expect_err("checkpoint sabotage must fail before remote publication")
            .to_string(),
        Ok(false) => panic!("sabotaged host write produced no pending Store write"),
    }
}

async fn assert_signed_head_rejects_summary(
    db: &crate::database::Database,
    retained: &crate::database::OwnedVerifiedMergeMaterialization,
    summary: &crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
) {
    let state = store_database(db)
        .resolved_store_device_state(&retained.history_summary().post_state)
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
    let (shallow, _) = historical_read_slots(1).await;
    let (deeper, registration_anchor_head_slot) = historical_read_slots(100).await;
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
    let (db, store, _device_id, _membership, _temp, store_dir) = published_history(1).await;
    let retained = retained_history(&db, &store).await;
    let reference = retained[0].commit_ref().clone();
    let stream_id = reference.coord.stream_id.to_string();
    let sequence = reference.coord.sequence;
    db.call(move |conn| {
        conn.pragma_update(None, "foreign_keys", "OFF")
            .map_err(crate::database::DbError::from)?;
        conn.execute(
            "DELETE FROM retained_merge_materializations WHERE device_id = ?1 AND seq = ?2",
            rusqlite::params![
                stream_id,
                i64::try_from(sequence).expect("test sequence fits")
            ],
        )
        .map_err(crate::database::DbError::from)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(crate::database::DbError::from)?;
        Ok(())
    })
    .await
    .expect("remove retained frontier row");

    let error = prepare_sabotaged_successor(&db, &store, &store_dir).await;
    assert!(
        !error.is_empty(),
        "missing retained frontier returned an empty error"
    );
}

#[tokio::test]
async fn outbound_successor_rejects_missing_or_forged_device_state() {
    for delete_state in [true, false] {
        let (db, store, _device_id, _membership, _temp, store_dir) = published_history(1).await;
        let retained = retained_history(&db, &store).await;
        let encoded =
            serde_json::to_string(retained[0].commit_ref()).expect("serialize commit ref");
        if delete_state {
            db.call(move |conn| {
                let deleted = conn
                    .execute(
                        "DELETE FROM store_device_state_snapshots WHERE commit_ref = ?1",
                        [encoded],
                    )
                    .map_err(crate::database::DbError::from)?;
                if deleted != 1 {
                    return Err(crate::database::DbError::Message(
                        "checkpoint state sabotage found no exact row".to_string(),
                    ));
                }
                Ok(())
            })
            .await
            .expect("delete checkpoint state");
        } else {
            let state = store_database(&db)
                .resolved_store_device_state(&retained[0].history_summary().post_state)
                .await
                .expect("load canonical retained state");
            let root = crate::database::StoreDatabase::new(&db)
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
            let encoded_state =
                serde_json::to_string(&forged).expect("serialize forged canonical device state");
            db.call(move |conn| {
                let updated = conn
                    .execute(
                        "UPDATE store_device_state_snapshots SET state = ?1 WHERE commit_ref = ?2",
                        rusqlite::params![encoded_state, encoded],
                    )
                    .map_err(crate::database::DbError::from)?;
                if updated != 1 {
                    return Err(crate::database::DbError::Message(
                        "checkpoint state forgery found no exact row".to_string(),
                    ));
                }
                Ok(())
            })
            .await
            .expect("forge canonical checkpoint state");
        }
        let error = prepare_sabotaged_successor(&db, &store, &store_dir).await;
        assert!(
            !error.is_empty(),
            "checkpoint-state sabotage returned an empty error"
        );
    }
}

#[tokio::test]
async fn changed_and_locally_rehashed_summary_omissions_are_rejected() {
    let (db, store, _device_id, _membership, _temp, _store_dir) = published_history(2).await;
    let device = store
        .bind_device(&db, &store.signer)
        .await
        .expect("bind checkpoint Store");
    let retained = retained_history(&db, &store).await;
    let current = retained.last().expect("two retained commits");
    let state = store_database(&db)
        .resolved_store_device_state(&current.history_summary().post_state)
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

    let forged_head = device
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
    let forged_head = device
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
    let (db, store, _device_id, _membership, _temp, _store_dir) = published_history(1).await;
    let coverage = crate::protocol::store_commit::CommitFrontier::from_refs(
        store_database(&db)
            .materialized_frontier()
            .await
            .expect("load acknowledgement coverage"),
    )
    .expect("derive acknowledgement coverage");
    let device = store
        .bind_device(&db, &store.signer)
        .await
        .expect("bind retained acknowledgement Store");
    device
        .publish_acknowledgement(coverage)
        .await
        .expect("publish retained acknowledgement");

    let retained = retained_history(&db, &store).await;
    let current = retained.last().expect("acknowledgement commit is retained");
    assert_eq!(
        current.history_summary().acknowledgements.len(),
        1,
        "acknowledgement fixture retains one latest exact acknowledgement",
    );
    let mut omitted = current.history_summary().clone();
    omitted.acknowledgements.clear();
    assert_signed_head_rejects_summary(&db, current, &omitted).await;
}

async fn history_with_member_removal() -> (
    crate::database::Database,
    TestStore,
    crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
) {
    let db = open_test_db();
    let owner = UserKeypair::generate();
    let member = UserKeypair::generate();
    let member_pubkey = crate::sync::test_helpers::pubkey_hex(&member);
    let encryption = crate::encryption::EncryptionService::from_key([42; 32]);
    let store = TestStore::create(&db, "retained-removal-proof", owner.clone())
        .await
        .expect("create removal-proof Store");
    store
        .invite_member(
            &db,
            &owner,
            &super::hlc::Hlc::new("retained-removal-proof".to_string()),
            &member_pubkey,
            None,
            crate::protocol::membership::MemberRole::Member,
            &encryption,
            "Retained removal proof",
        )
        .await
        .expect("invite removable member");
    let member_db = open_test_db();
    crate::sync::test_helpers::install_active_device_fixture(
        &store,
        &db,
        &member_db,
        &member,
        "2026-07-21T00:00:00Z",
    )
    .await
    .expect("activate removable member device");
    crate::sync::test_helpers::promote_active_member_fixture(
        &store,
        &db,
        &member_db,
        &owner,
        &member,
        &encryption,
    )
    .await
    .expect("promote removable member to Owner");
    let custody = crate::sync::test_helpers::TestCustody::default();
    let security = crate::sync::test_helpers::test_store_security(
        "retained-removal-proof",
        std::sync::Arc::new(custody),
    );
    store
        .remove_member(
            &db,
            &owner,
            &super::hlc::Hlc::new("retained-removal-proof-remove".to_string()),
            &member_pubkey,
            &encryption,
            &security,
        )
        .await
        .expect("remove retained member");
    let retained = retained_history(&db, &store).await;
    let summary = retained
        .last()
        .expect("removal activation is retained")
        .history_summary()
        .clone();
    (db, store, summary)
}

#[tokio::test]
async fn signed_head_rejects_an_omitted_membership_removal() {
    let (db, store, summary) = Box::pin(history_with_member_removal()).await;
    let retained = retained_history(&db, &store).await;
    let current = retained.last().expect("removal activation is retained");
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
        .expect("retained history contains the removal control proof");
    let mut omitted = summary;
    omitted.membership_proofs.remove(&removal);
    assert_signed_head_rejects_summary(&db, current, &omitted).await;
}

#[tokio::test]
async fn membership_checkpoint_floor_includes_the_activating_control() {
    let (_db, _store, summary) = Box::pin(history_with_member_removal()).await;
    let control = summary
        .membership_proofs
        .values()
        .find(|proof| {
            matches!(
                proof.entry_value.change,
                crate::protocol::membership::MembershipChange::RemoveMember { .. }
            )
        })
        .expect("retained history contains the removal control proof");
    assert!(summary
        .membership_floor
        .effective_coordinates
        .contains(&control.entry.coord));
}

#[tokio::test]
async fn retained_membership_proof_rejects_an_incomplete_resolution_authority() {
    let (_db, _store, mut summary) = Box::pin(history_with_member_removal()).await;
    let proof = summary
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
        summary.validate_shape().is_err(),
        "retained membership proof accepted a resolution reference without its signed value",
    );
}

#[tokio::test]
async fn signed_snapshot_rejects_an_omitted_pre_snapshot_membership_control() {
    run_signed_snapshot_rejects_an_omitted_pre_snapshot_membership_control().await;
}

async fn run_signed_snapshot_rejects_an_omitted_pre_snapshot_membership_control() {
    let (db, store, _summary) = Box::pin(history_with_member_removal()).await;
    let device = store
        .bind_device(&db, &store.signer)
        .await
        .expect("load snapshot Store");
    let directory = tempfile::tempdir().expect("create snapshot image directory");
    let snapshot_dir = directory.path().to_path_buf();
    let database = store_database(&db);
    let image = database
        .capture_snapshot_image_for_test(store.root.clone(), snapshot_dir, None)
        .await
        .expect("create checkpoint snapshot image");
    let coverage = crate::protocol::store_commit::CommitFrontier::from_refs(
        database
            .materialized_frontier()
            .await
            .expect("load snapshot coverage"),
    )
    .expect("derive snapshot coverage");
    let meta = device
        .publish_snapshot(image, coverage)
        .await
        .expect("publish checkpoint snapshot");
    let published = crate::database::StoreDatabase::new(&db)
        .latest_local_store_snapshot()
        .await
        .expect("load published snapshot")
        .expect("published snapshot is recorded");
    let mut forged = meta;
    let summary = &mut forged.history_summary;
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
    let forged = device
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
        device
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
        device
            .verify_snapshots_for_acknowledgement_for_test(std::slice::from_ref(&forged))
            .await
            .is_err(),
        "snapshot authority accepted a signed summary that omitted exact cut history",
    );
}

#[tokio::test]
async fn conflict_resolution_authorization_reads_retained_checkpoints_not_store_history() {
    let (db, store, _device_id, membership, _temp, _store_dir) = published_history(4).await;
    let retained = retained_history(&db, &store).await;
    let historical_slots = retained
        .iter()
        .flat_map(|entry| {
            [
                entry.commit_ref().object.slot().clone(),
                entry.activation_head_object().slot().clone(),
            ]
        })
        .collect::<Vec<_>>();
    let device = store
        .bind_device(&db, &store.signer)
        .await
        .expect("retain checkpoint Store device");

    store.home.clear_exact_reads();
    device
        .prepare_conflict_resolution_plan_for_test(membership.head_refs())
        .await
        .expect("authorize from retained conflict-resolution predecessor");
    let reread = store
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
