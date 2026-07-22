use super::*;
use crate::sync::store_commit::{
    serial_head_key, DeviceStreamAnchor, OwnerRecoveryNodeRef, StoreCommitAnchor,
};

async fn one_retained_checkpoint() -> (
    Database,
    crate::sync::test_helpers::TestStore,
    MembershipChain,
    OpenedRetainedMergeHistorySummary,
) {
    let db = crate::sync::test_helpers::open_test_db();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        "retained-checkpoint-conflict",
        crate::keys::UserKeypair::generate(),
    )
    .await
    .expect("create retained-checkpoint Store");
    let membership = super::super::pull::load_cycle_membership(&store.storage, &db)
        .await
        .expect("load checkpoint membership")
        .chain
        .expect("Merge Store has membership");
    crate::sync::test_helpers::host_exec(
        &db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('checkpoint-conflict', 'checkpoint', NULL, 1, \
                 '0000000001000-0000-checkpoint', '2026-07-21')",
    )
    .await;
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("load checkpoint device id")
        .expect("checkpoint device id exists");
    let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
    assert!(
        super::super::store_engine::merge::preparation::prepare_store_write(
            &db,
            &store.storage,
            &device_id,
            "2026-07-21T00:00:00Z",
            &store.signer,
            &store_dir,
            &membership,
        )
        .await
        .expect("prepare checkpoint commit")
    );
    assert_eq!(
        super::super::store_engine::merge::publication::drain_store_writes(&db, &store.storage)
            .await
            .expect("publish checkpoint commit"),
        1,
    );
    let reference = db
        .latest_local_store_position()
        .await
        .expect("load checkpoint position")
        .expect("checkpoint position exists");
    let mut retained = db
        .retained_merge_history_frontier(vec![reference])
        .await
        .expect("open retained checkpoint");
    assert_eq!(retained.len(), 1);
    (db, store, membership, retained.remove(0))
}

#[tokio::test]
async fn retained_checkpoint_merge_rejects_same_coordinate_competitors() {
    let (_db, store, membership, checkpoint) = Box::pin(one_retained_checkpoint()).await;

    let mut conflicting_commit = checkpoint.clone();
    let (coordinate, reference) = conflicting_commit
        .summary
        .causal_cut
        .first_key_value()
        .map(|(coordinate, reference)| (coordinate.clone(), reference.clone()))
        .expect("checkpoint causal cut is nonempty");
    let mut replacement = reference;
    replacement.commit_hash = ObjectHash::digest(b"same-coordinate competing commit");
    conflicting_commit
        .summary
        .causal_cut
        .insert(coordinate, replacement);
    assert!(merge_retained_merge_history(
        &store.root,
        &membership,
        vec![checkpoint.clone(), conflicting_commit],
    )
    .is_err());

    let mut conflicting_head = checkpoint.clone();
    let announcement = conflicting_head
        .announcement_frontier
        .values_mut()
        .next()
        .expect("opened checkpoint has an announcement frontier");
    announcement.reference.head_hash = ObjectHash::digest(b"same-stream competing head");
    assert!(merge_retained_merge_history(
        &store.root,
        &membership,
        vec![checkpoint, conflicting_head],
    )
    .is_err());
}

#[tokio::test]
async fn retained_checkpoint_merge_rejects_different_sequence_acknowledgement_forks() {
    let (db, store, _membership, checkpoint) = Box::pin(one_retained_checkpoint()).await;
    let coverage = CommitFrontier::from_refs(
        crate::WritePolicy::MergeConcurrent,
        db.materialized_frontier()
            .await
            .expect("load acknowledgement coverage"),
    )
    .expect("derive acknowledgement coverage");
    crate::sync::test_helpers::publish_merge_store_ack_fixture(
        &db,
        &store.storage,
        coverage,
        &store.signer,
    )
    .await
    .expect("publish retained acknowledgement");
    let acknowledgement_commit = db
        .latest_local_store_position()
        .await
        .expect("load acknowledgement commit")
        .expect("acknowledgement commit exists");
    let mut retained = db
        .retained_merge_history_frontier(vec![acknowledgement_commit])
        .await
        .expect("open acknowledgement checkpoint");
    let acknowledgement = retained
        .remove(0)
        .summary
        .acknowledgements
        .into_values()
        .next()
        .expect("checkpoint retains its acknowledgement");
    let mut forged_higher_fork = acknowledgement.clone();
    let (latest_ref, latest_value) = acknowledgement
        .latest()
        .expect("acknowledgement proof chain has a latest entry");
    let device_id = latest_ref.registration.device_id;
    let mut forked_at_same_sequence = (latest_ref.clone(), latest_value.clone());
    forked_at_same_sequence.0.ack_hash = ObjectHash::digest(b"forked acknowledgement");
    forged_higher_fork
        .chain
        .insert(latest_ref.sequence, forked_at_same_sequence.clone());
    let higher_sequence = latest_ref.sequence + 1;
    forked_at_same_sequence.0.sequence = higher_sequence;
    forked_at_same_sequence.1.sequence = higher_sequence;
    forged_higher_fork
        .chain
        .insert(higher_sequence, forked_at_same_sequence);

    let mut merged = checkpoint.summary.acknowledgements;
    insert_latest_acknowledgement(&mut merged, device_id, acknowledgement)
        .expect("first acknowledgement establishes the retained stream");
    assert!(insert_latest_acknowledgement(&mut merged, device_id, forged_higher_fork,).is_err());
}

#[test]
fn recovery_cursor_requires_the_exact_origin_activation_pair() {
    let recovery_id = super::super::store_commit::DeviceRecoveryId::from_hash(ObjectHash::digest(
        b"recovery cursor id",
    ));
    let owner_grant = super::super::causal_grants::MembershipGrantId(ObjectHash::digest(
        b"recovery cursor owner grant",
    ));
    let recovery_slot = crate::storage::cloud::ObjectSlot::opaque(
        "store-v1/test/recovery.json".to_string(),
        "recovery-cursor-slot".to_string(),
    )
    .expect("construct recovery cursor slot");
    let node = OwnerRecoveryNodeRef {
        owner_pubkey: "recovery-owner".to_string(),
        owner_grant: owner_grant.clone(),
        sequence: 1,
        node_hash: ObjectHash::digest(b"recovery cursor node"),
        object: ExactObjectRef::new(
            recovery_slot.clone(),
            1,
            ObjectHash::digest(b"recovery cursor bytes"),
        ),
    };
    let origin = StoreDeviceRegistrationOrigin::Recovery {
        recovery_id,
        recovery_slot,
        owner_grant: owner_grant.clone(),
    };
    let activation = StoreDeviceRegistrationActivation::Recovery {
        recovery_id,
        node: node.clone(),
    };

    assert_eq!(
        registration_recovery_cursor(&origin, &activation).expect("derive exact recovery cursor"),
        Some(OwnerRecoveryCursor {
            owner_grant,
            position: OwnerRecoveryPosition::At { node: node.clone() },
        })
    );

    let wrong_activation = StoreDeviceRegistrationActivation::Recovery {
        recovery_id: super::super::store_commit::DeviceRecoveryId::from_hash(ObjectHash::digest(
            b"another recovery cursor id",
        )),
        node,
    };
    assert!(registration_recovery_cursor(&origin, &wrong_activation).is_err());
}

#[tokio::test]
async fn cycle_authorization_rejects_an_absent_serial_coordination_head() {
    let db = crate::sync::test_helpers::open_serial_test_db();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        "absent-serial-cycle-head",
        crate::keys::UserKeypair::generate(),
    )
    .await
    .expect("create Serial Store");
    store.home.remove(serial_head_key());

    let result = load_serial_cycle_authorization(
        &store.storage,
        store
            .storage
            .serial_coordination()
            .expect("Serial coordination"),
        &store.root,
    )
    .await;

    assert!(matches!(
        result,
        Err(StorePullError::Serial(reason)) if reason == "global head is absent"
    ));
}

#[tokio::test]
async fn cycle_authorization_rejects_a_nonfounder_serial_genesis_head() {
    let db = crate::sync::test_helpers::open_serial_test_db();
    let store = crate::sync::test_helpers::TestStore::create(
        &db,
        "nonfounder-serial-genesis-head",
        crate::keys::UserKeypair::generate(),
    )
    .await
    .expect("create Serial Store");
    let (_, founder_registration, _) = store
        .founder_device_authority()
        .await
        .expect("load founder Store device");
    let other_identity = crate::keys::UserKeypair::generate();
    let other_origin = StoreDeviceRegistrationOrigin::Join {
        attempt_id: super::super::store_commit::DeviceJoinAttemptId::from_hash(ObjectHash::digest(
            b"non-founder genesis registration",
        )),
        attempt_slot: crate::storage::cloud::ObjectSlot::logical(
            "store-v1/test/non-founder-genesis/attempt.json".to_string(),
        )
        .expect("construct attempt slot"),
        outcome_slot: crate::storage::cloud::ObjectSlot::logical(
            "store-v1/test/non-founder-genesis/outcome.json".to_string(),
        )
        .expect("construct outcome slot"),
    };
    let other = StoreDeviceRegistration::signed(
        store.root.clone(),
        other_origin,
        founder_registration.provider,
        StoreCommitAnchor::Serial,
        DeviceStreamAnchor::StoreAcknowledgements {
            first_slot: crate::storage::cloud::ObjectSlot::logical(
                "store-v1/test/non-founder-genesis/ack/1.json".to_string(),
            )
            .expect("construct acknowledgement slot"),
        },
        DeviceStreamAnchor::StoreSnapshots {
            first_slot: crate::storage::cloud::ObjectSlot::logical(
                "store-v1/test/non-founder-genesis/snapshot/1.json".to_string(),
            )
            .expect("construct snapshot slot"),
        },
        &other_identity,
    )
    .expect("sign another Store registration");
    let other_signer = other
        .device_signer(&other_identity)
        .expect("derive another device signer");
    let registration_prefix =
        super::super::store_commit::registration_semantic_prefix(&other.device_id.to_string());
    let registration_context = ProtocolObjectContext::signed_plaintext(
        store.root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    let registration_slot = store
        .storage
        .allocate_protocol_slot(&registration_context, &registration_prefix, ".json")
        .await
        .expect("allocate another registration slot");
    let prepared = store
        .storage
        .prepare_protocol_object(
            &registration_context,
            registration_slot,
            &registration_prefix,
            other.to_bytes(),
        )
        .expect("prepare another registration");
    let registration_object =
        super::super::store_objects::create_exact_object(&store.storage, &prepared)
            .await
            .expect("publish another registration");
    let other_registration =
        StoreDeviceRegistrationRef::from_registration(&other, registration_object);
    let forged = StoreSerialHead::signed(
        store.root.store_root_hash,
        StoreSerialHeadState::Genesis {
            root: store.root.clone(),
            founder_registration: other_registration,
        },
        &other_signer,
    )
    .expect("sign non-founder genesis head");
    let coordination = store
        .storage
        .serial_coordination()
        .expect("Serial coordination");
    let current = coordination
        .read_head(serial_head_key())
        .await
        .expect("read current Serial head");
    coordination
        .replace_head(serial_head_key(), &current.version, &forged.to_bytes())
        .await
        .expect("replace Serial head with non-founder genesis");

    let result = load_serial_cycle_authorization(&store.storage, coordination, &store.root).await;

    match result {
        Err(StorePullError::Serial(reason)) => assert_eq!(
            reason,
            "Serial genesis head does not name the exact Store founder"
        ),
        Err(error) => panic!("unexpected error: {error:?}"),
        Ok(_) => panic!("non-founder Serial genesis head was accepted"),
    }
}

#[tokio::test]
async fn merge_outbound_projects_membership_to_the_commits_predecessors() {
    let founder = crate::sync::test_helpers::user_keypair_from_seed([42; 32]);
    let founder_db = crate::sync::test_helpers::open_test_db();
    let store = crate::sync::test_helpers::TestStore::create(
        &founder_db,
        "causal-membership-proof",
        founder.clone(),
    )
    .await
    .expect("create Merge Store");
    let candidate = crate::sync::test_helpers::user_keypair_from_seed([43; 32]);
    let encryption = crate::encryption::EncryptionService::from_key([73; 32]);
    crate::sync::membership_ops::invite_member(
        &store.storage,
        store.home.as_ref(),
        &founder,
        &super::super::hlc::Hlc::new("causal-membership-proof".to_string()),
        &crate::sync::test_helpers::pubkey_hex(&candidate),
        None,
        super::super::membership::MemberRole::Member,
        &encryption,
        "causal-membership-proof",
        "Causal Membership Proof",
        &founder_db,
    )
    .await
    .expect("invite exact Store member");

    let candidate_db = crate::sync::test_helpers::open_test_db();
    crate::sync::test_helpers::install_active_device_fixture(
        &store,
        &founder_db,
        &candidate_db,
        &candidate,
        "2026-07-21T00:00:00Z",
    )
    .await
    .expect("activate candidate device");
    crate::sync::test_helpers::promote_active_member_fixture(
        &store,
        &founder_db,
        &candidate_db,
        &founder,
        &candidate,
        &encryption,
    )
    .await
    .expect("promote candidate Owner");
    let candidate_membership =
        super::super::pull::load_cycle_membership(&store.storage, &candidate_db)
            .await
            .expect("load candidate Owner membership");
    let (_candidate_temp, candidate_store_dir) = crate::sync::test_helpers::temp_store_dir();
    let candidate_pull = Box::pin(crate::sync::store_engine::pull_store_commits(
        &candidate_db,
        candidate_db.synced_tables(),
        &store.storage,
        None,
        store.root.store_root_hash,
        &candidate_store_dir,
        candidate_membership.chain.as_ref(),
        Some(&candidate),
    ))
    .await
    .expect("pull candidate Owner to the common Store history");
    assert!(candidate_pull.held_positions.is_empty());

    let earlier_db = &candidate_db;
    let earlier_owner = &candidate;
    let later_db = &founder_db;
    let later_owner = &founder;

    let mut earlier_membership =
        super::super::pull::load_cycle_membership(&store.storage, earlier_db)
            .await
            .expect("load earlier Owner membership")
            .chain
            .expect("initialized Store has membership");
    let _rotated = super::super::invite::revoke_member_durable(
        &store.storage,
        store.home.as_ref(),
        store.root.store_root_hash,
        &mut earlier_membership,
        earlier_owner,
        &crate::sync::test_helpers::pubkey_hex(&candidate),
        &store.root.store_root_id.to_string(),
        "0000000003000-0000-causal-proof",
        &encryption,
        &super::super::cloud_storage::PendingRotation::none(),
        earlier_db,
    )
    .await
    .expect("publish traversal-earlier Owner removal control");
    let earlier_control = earlier_db
        .latest_local_store_position()
        .await
        .expect("load earlier Owner position")
        .expect("earlier Owner published the membership control");
    let (earlier_value, _) = load_commit_with_author(&store.storage, &store.root, &earlier_control)
        .await
        .expect("load traversal-earlier control");
    let Some(super::super::store_commit::StoreControl::MergeMembership { transition }) =
        earlier_value.control()
    else {
        panic!("earlier Owner position is not a Merge membership control");
    };

    let changeset = crate::sync::test_helpers::capture_bytes(
        &crate::sync::test_helpers::open_test_db(),
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('causal-proof-row', 'causal proof', NULL, \
                   '0000000001000-0000-causal-proof', '2026-07-21')",
        ],
    )
    .await;
    later_db
        .enqueue_store_changeset_for_test(changeset)
        .await
        .expect("enqueue later concurrent write");
    let later_membership = super::super::pull::load_cycle_membership(&store.storage, later_db)
        .await
        .expect("load membership containing the concurrent control");
    let caller_membership = later_membership
        .chain
        .as_ref()
        .expect("initialized Store has membership");
    let earlier_head_ref = caller_membership
        .head_refs()
        .iter()
        .find(|head| head.coord == transition.body.entry.coord)
        .expect("caller membership contains the concurrent control")
        .clone();
    let earlier_head = super::super::membership_ops::load_exact_membership_head(
        &store.storage,
        &store.root,
        &earlier_head_ref,
    )
    .await
    .expect("load concurrent membership head");
    let later_device_id = later_db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("load later Owner device id")
        .expect("later Owner device is activated");
    let (_later_temp, later_store_dir) = crate::sync::test_helpers::temp_store_dir();
    assert!(
        super::super::store_engine::merge::preparation::prepare_store_write(
            later_db,
            &store.storage,
            &later_device_id,
            "2026-07-21T00:02:00Z",
            later_owner,
            &later_store_dir,
            later_membership
                .chain
                .as_ref()
                .expect("later Merge membership chain"),
        )
        .await
        .expect("prepare later concurrent write")
    );
    super::super::store_engine::merge::publication::drain_store_writes(later_db, &store.storage)
        .await
        .expect("publish later concurrent write");
    let later_commit = later_db
        .latest_local_store_position()
        .await
        .expect("load later Owner position")
        .expect("later Owner published the data commit");

    let (later_value, _) = load_commit_with_author(&store.storage, &store.root, &later_commit)
        .await
        .expect("load later concurrent commit");
    let later_predecessors = commit_predecessor_references(&later_value);
    assert!(!later_predecessors.contains(&earlier_control));
    let super::super::circle_control::StoreMembershipStateRef::MergeConcurrent(signed_membership) =
        &later_value.membership_state
    else {
        panic!("later commit carries Serial membership state");
    };
    assert!(!signed_membership
        .heads
        .iter()
        .any(|head| head.coord == transition.body.entry.coord));

    let verified = verify_merge_history_refs(
        &store.storage,
        &store.root,
        [later_commit.clone(), earlier_control.clone()],
    )
    .await
    .expect("verify both concurrent commits");
    let later_prefix = verified_merge_membership_prefix(&verified.commits, later_predecessors)
        .expect("derive the later commit's exact membership prefix");
    assert_eq!(
        later_prefix
            .classify_head(&earlier_head_ref, &earlier_head, &earlier_control,)
            .expect("classify concurrent control against later prefix"),
        VerifiedMergePrefixHeadStatus::OutsidePrefix,
    );
}

#[tokio::test]
async fn merge_gap_reports_the_exact_signed_predecessor() {
    let source = crate::sync::test_helpers::open_test_db();
    let store = crate::sync::test_helpers::TestStore::create(
        &source,
        "exact-predecessor-test",
        crate::keys::UserKeypair::generate(),
    )
    .await
    .expect("create exact predecessor test Store");
    let changeset = crate::sync::test_helpers::capture_bytes(
        &crate::sync::test_helpers::open_test_db(),
        &[
            "INSERT INTO notes (id, title, body, _updated_at, created_at) \
           VALUES ('gap-row', 'gap', NULL, '0000000001000-0000-gap', '2026-01-01')",
        ],
    )
    .await;
    let first = store
        .publish_changeset("founder", 1, &changeset, source.schema_version())
        .await
        .expect("publish first exact commit");
    let second = store
        .publish_changeset("founder", 2, &changeset, source.schema_version())
        .await
        .expect("publish second exact commit");
    let third = store
        .publish_changeset("founder", 3, &changeset, source.schema_version())
        .await
        .expect("publish third exact commit");
    let (_, founder, _) = store
        .founder_device_authority()
        .await
        .expect("load founder authority");
    let commit = super::super::store_objects::load_commit_ref(
        &store.storage,
        store.root.store_root_hash,
        &third,
        &founder,
    )
    .await
    .expect("load third exact commit")
    .value;
    let stream_id = commit_stream_id(&first.coord);
    let frontier = BTreeMap::from([(stream_id.clone(), first.clone())]);
    let coverage = CommitFrontier::from_refs(crate::WritePolicy::MergeConcurrent, frontier.clone())
        .expect("build exact frontier");
    let CommitFrontier::MergeConcurrent(device_cut) = coverage.clone() else {
        panic!("Merge test frontier changed policy")
    };
    let (_, device_state) = source
        .store_device_state_for_history_cut(&StoreHistoryCut::MergeConcurrent(device_cut))
        .await
        .expect("load exact device state");
    let target = crate::sync::test_helpers::open_test_db();

    let readiness = readiness(
        &target,
        &store.storage,
        &store.root,
        &coverage,
        &frontier,
        &device_state,
        &[],
        &third,
        &commit,
    )
    .await
    .expect("evaluate exact predecessor gap");

    assert!(matches!(
        readiness,
        Readiness::Held(HeldStorePosition {
            reason: HeldStorePositionReason::MissingPredecessor(missing),
            ..
        }) if missing == second
    ));
}
