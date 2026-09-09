use crate::sync::test_helpers::{pubkey_hex, TestStore};
use coven_keys::keys::UserKeypair;
use coven_protocol::membership::MemberRole;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{
    owner_recovery_semantic_prefix, OwnerRecoveryNode, StoreDeviceRegistration,
    StoreDeviceRegistrationOrigin,
};
use coven_storage::CloudSyncObjectStorage;
use std::sync::Arc;

#[tokio::test]
async fn installed_ordinary_commit_cannot_complete_owner_recovery() {
    let owner = UserKeypair::generate();
    let store_dir = crate::sync::test_helpers::test_store_dir();
    let database = crate::sync::test_helpers::open_test_db(store_dir.clone());
    let store = TestStore::create(
        &database,
        store_dir.clone(),
        "unrelated-recovery-completion",
        owner.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Store");
    let admission = store
        .admit_member(
            &database,
            store_dir.clone(),
            &owner,
            &pubkey_hex(&UserKeypair::generate()),
            None,
            MemberRole::Member,
            &coven_keys::encryption::EncryptionService::from_key([42; 32]),
            "Test Store",
        )
        .await
        .expect("publish an unrelated membership activation");
    let [admission_head] = admission.membership_floor.0.as_slice() else {
        panic!("the admission extends the founder's sole authority stream");
    };
    let unrelated_result = finalized_membership_result(&database, admission_head).await;
    let device = store
        .bind_device(&database, store_dir, &owner)
        .await
        .expect("bind founder");
    device
        .publish_fixture_position("ordinary-accepted-write")
        .await;
    let reference = device
        .latest_local_store_position()
        .await
        .expect("read installed position")
        .expect("ordinary write is installed");
    let database = coven_database::StoreDatabase::new(&database);
    let retained = database
        .retained_merge_materialization(store.root(), reference)
        .await
        .expect("load installed ordinary write");
    let commit = retained.verified_commit().clone();
    let evidence = database
        .installed_store_commit_evidence(commit.clone())
        .await
        .expect("read exact installed evidence")
        .expect("write has installed evidence");
    let registration = database
        .activated_store_device_registration_with_authority(
            &store.root(),
            commit.author_registration.clone(),
        )
        .await
        .expect("load activated author");
    database
        .complete_owner_recovery(
            commit,
            coven_database::StoreCommitPublicationOutcome::Installed(evidence),
            retained.history_evidence().clone(),
            registration,
            unrelated_result,
        )
        .await
        .expect_err("an installed ordinary commit cannot prove Owner recovery completion");
}

#[tokio::test]
async fn pulling_a_peer_commit_preserves_the_pending_recovery_attempt() {
    assert_pending_recovery_after_peer_publication(false).await;
}

#[tokio::test]
async fn reopening_recovery_rejects_a_corrupted_publication_signature() {
    assert_pending_recovery_after_peer_publication(true).await;
}

async fn assert_pending_recovery_after_peer_publication(corrupt_signature: bool) {
    let owner = UserKeypair::generate();
    let source_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_dir.clone());
    let home = crate::sync::test_helpers::test_cloud_home();
    let store = TestStore::create(
        &source,
        source_dir.clone(),
        "pending-recovery-observation",
        owner.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let peer_dir = crate::sync::test_helpers::test_store_dir();
    let peer_database = crate::sync::test_helpers::open_test_db(peer_dir.clone());
    let peer = store
        .admit_and_activate_peer(
            &source,
            source_dir.clone(),
            &peer_database,
            peer_dir,
            &UserKeypair::generate(),
        )
        .await
        .expect("activate peer");
    let device = store
        .bind_device(&source, source_dir, &owner)
        .await
        .expect("bind recovery source");
    let authority = store.founder_recovery_authority().await;
    let mut recovery = device
        .owner_recovery_for_test()
        .await
        .expect("authorize recovery");
    home.fail_exact_create_before_call(4);
    recovery
        .recover_owner_device(&authority, None)
        .await
        .expect_err("interrupt before uploading the staged activation");
    let database = coven_database::StoreDatabase::new(&source);
    let staged = database
        .owner_recovery_publication()
        .await
        .expect("read staged recovery")
        .expect("recovery is durable");
    let reservation = database
        .active_store_publication()
        .await
        .expect("read reservation");
    peer.publish_fixture_position("peer-during-recovery").await;
    let pulled = recovery
        .pull(None)
        .await
        .expect("observe accepted peer history");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    assert_ne!(
        database
            .store_current_publication()
            .await
            .expect("read observed boundary")
            .record(),
        &staged.publication.previous
    );
    if corrupt_signature {
        let mut corrupted = staged.publication.clone();
        corrupted.replacement.corrupt_signature_for_test();
        let active = reservation.as_ref().expect("recovery has a reservation");
        let corrupted = active
            .replace_attempt(corrupted)
            .expect("retain attempt identity");
        let encoded = serde_json::to_string(&corrupted).expect("encode damaged journal");
        source
            .execute_test_sql(&format!(
                "UPDATE active_store_publication SET state = '{}' WHERE singleton = 1",
                encoded.replace('\'', "''"),
            ))
            .await;
        let result = database.owner_recovery_publication().await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("recovery reopened with a corrupted current-record signature"),
        };
        assert!(error.to_string().contains("signature"), "{error}");
        return;
    }
    let reopened = database
        .owner_recovery_publication()
        .await
        .expect("an unresolved recovery attempt remains readable after pulling a peer")
        .expect("recovery is still pending");
    assert_eq!(reopened.commit.bytes, staged.commit.bytes);
    assert_eq!(reopened.commit.prepared, staged.commit.prepared);
    assert_eq!(reopened.publication, staged.publication);
    assert_eq!(
        database
            .active_store_publication()
            .await
            .expect("read retained reservation"),
        reservation
    );
}

#[tokio::test]
async fn recovery_node_keeps_its_historical_membership_when_activation_membership_advances() {
    let founder = UserKeypair::generate();
    let founder_store_dir = crate::sync::test_helpers::test_store_dir();
    let founder_db = crate::sync::test_helpers::open_test_db(founder_store_dir.clone());
    let home = crate::sync::test_helpers::test_cloud_home();
    let (store, cloud_storage) = TestStore::create_with_connection(
        &founder_db,
        founder_store_dir.clone(),
        "historical-recovery-membership",
        founder.clone(),
        home.clone(),
    )
    .await
    .expect("create historical recovery Store");
    let co_owner = UserKeypair::generate();
    let co_owner_store_dir = crate::sync::test_helpers::test_store_dir();
    let co_owner_db = crate::sync::test_helpers::open_test_db(co_owner_store_dir.clone());
    store
        .admit_and_activate_peer(
            &founder_db,
            founder_store_dir.clone(),
            &co_owner_db,
            co_owner_store_dir.clone(),
            &co_owner,
        )
        .await
        .expect("activate member device");
    let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    store
        .promote_active_member_fixture(
            &founder_db,
            founder_store_dir.clone(),
            &co_owner_db,
            co_owner_store_dir.clone(),
            &founder,
            &co_owner,
            &encryption,
        )
        .await
        .expect("promote the second Owner");

    let founder_device = store
        .bind_device(&founder_db, founder_store_dir.clone(), &founder)
        .await
        .expect("bind recovery Store");
    let authority = store.founder_recovery_authority().await;
    let database = coven_database::StoreDatabase::new(&founder_db);
    let mut recovery = founder_device
        .owner_recovery_for_test()
        .await
        .expect("authorize Owner recovery Store");
    home.fail_exact_create_before_call(3);
    recovery
        .recover_owner_device(&authority, Some(&encryption))
        .await
        .expect_err("interrupt recovery before publishing its authority node");

    let later_member = UserKeypair::generate();
    store
        .admit_member(
            &co_owner_db,
            co_owner_store_dir.clone(),
            &co_owner,
            &pubkey_hex(&later_member),
            None,
            MemberRole::Member,
            &encryption,
            "Test Store",
        )
        .await
        .expect("advance membership through the second Owner");
    let co_owner_device = store
        .bind_device(&co_owner_db, co_owner_store_dir.clone(), &co_owner)
        .await
        .expect("reload the second Owner");
    co_owner_device
        .publish_fixture_position("membership-after-recovery-readiness")
        .await;

    home.fail_exact_create_before_call(2);
    recovery
        .recover_owner_device(&authority, Some(&encryption))
        .await
        .expect_err("interrupt activation after its exact authority is staged");
    let durable = database
        .latest_local_store_device_registration()
        .await
        .expect("read recovery registration")
        .expect("recovery registration is durable");
    let registration = StoreDeviceRegistration::parse_at(
        &durable.registration_bytes,
        &store.root(),
        durable.device_id,
    )
    .expect("parse recovery registration");
    let StoreDeviceRegistrationOrigin::Recovery { recovery_slot, .. } = registration.origin.clone()
    else {
        panic!("replacement registration is not a recovery registration");
    };
    let node_prefix =
        owner_recovery_semantic_prefix(&pubkey_hex(&founder), authority.owner_grant.clone(), 1);
    let node_context = ProtocolObjectContext::signed_plaintext(
        store.root().store_root_hash,
        ProtocolObjectDomain::OwnerRecoveryNode,
    );
    let (node_bytes_before, node_prepared_before) = cloud_storage
        .read_prepared_protocol_slot(&node_context, &recovery_slot, &node_prefix)
        .await
        .expect("read historical recovery node");
    let node: OwnerRecoveryNode =
        serde_json::from_slice(&node_bytes_before).expect("parse historical recovery node");
    let staged = database
        .owner_recovery_publication()
        .await
        .expect("read staged recovery activation")
        .expect("recovery activation is staged");
    assert_ne!(
        node.membership,
        staged.commit.value.value().membership_state,
        "the immutable recovery node keeps the authority it was created under while the activation names the later membership",
    );

    let _recovered = recovery
        .recover_owner_device(&authority, Some(&encryption))
        .await
        .expect("retry accepts the node's historical membership");
    let (node_bytes_after, node_prepared_after) = cloud_storage
        .read_prepared_protocol_slot(&node_context, &recovery_slot, &node_prefix)
        .await
        .expect("read recovery node after retry");
    assert_eq!(node_bytes_after, node_bytes_before);
    assert_eq!(node_prepared_after, node_prepared_before);

    let commit_value = staged.commit.value.value();
    let commit_prefix = coven_protocol::store_commit::commit_semantic_prefix(
        commit_value.candidate_family(),
        &staged.commit.value.reference().coord.stream_id.to_string(),
        commit_value.seq(),
        commit_value.commit_hash(),
    );
    let (commit_bytes, commit_prepared) = cloud_storage
        .read_prepared_protocol_slot(
            &ProtocolObjectContext::signed_plaintext(
                store.root().store_root_hash,
                ProtocolObjectDomain::StoreCommit,
            ),
            staged.commit.prepared.reference().slot(),
            &commit_prefix,
        )
        .await
        .expect("read retried recovery commit");
    assert_eq!(commit_bytes, staged.commit.bytes);
    assert_eq!(commit_prepared, staged.commit.prepared);
    let publication_prefix = coven_protocol::store_commit::store_publication_entry_semantic_prefix(
        &staged.publication.entry,
    );
    let (publication_bytes, publication_prepared) = cloud_storage
        .read_prepared_protocol_slot(
            &ProtocolObjectContext::signed_plaintext(
                store.root().store_root_hash,
                ProtocolObjectDomain::StorePublicationEntry,
            ),
            staged.publication.entry_object.slot(),
            &publication_prefix,
        )
        .await
        .expect("read retried recovery publication entry");
    assert_eq!(publication_bytes, staged.publication.entry.to_bytes());
    assert_eq!(
        publication_prepared,
        staged
            .publication
            .prepared_entry()
            .expect("prepare staged publication entry")
    );
}

#[tokio::test]
async fn recovery_adopts_an_accepted_publication_after_local_completion_fails() {
    assert_recovery_completion_retry(true).await;
}

#[tokio::test]
async fn recovery_retries_atomic_local_publication_completion() {
    assert_recovery_completion_retry(false).await;
}

async fn assert_recovery_completion_retry(pull_accepted: bool) {
    let owner = UserKeypair::generate();
    let source_store_dir = crate::sync::test_helpers::test_store_dir();
    let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
    let home = crate::sync::test_helpers::test_cloud_home();
    let store = TestStore::create(
        &source,
        source_store_dir,
        "barrier-activation-adoption",
        owner,
        home,
    )
    .await
    .expect("create recovery source Store");
    let authority = store.founder_recovery_authority().await;

    let first_store_dir = crate::sync::test_helpers::test_store_dir();
    let first_database = crate::sync::test_helpers::open_test_db(first_store_dir.clone());
    let first_device = store
        .open_into(&first_database, first_store_dir)
        .await
        .expect("open first recovery target");
    first_database.fail_next_merge_materialization_at(
        coven_database::MergeMaterializationFailurePoint::SummaryMaterialization,
    );
    let mut first_recovery = first_device
        .owner_recovery_for_test()
        .await
        .expect("authorize first recovery");
    let first_error = first_recovery
        .recover_owner_device(&authority, None)
        .await
        .expect_err("fail local completion after accepting recovery publication");
    assert!(
        first_error.to_string().contains("injected failure"),
        "{first_error}"
    );
    let database = coven_database::StoreDatabase::new(&first_database);
    let staged = database
        .owner_recovery_publication()
        .await
        .expect("read rolled back recovery journal")
        .expect("failed local completion retains the recovery journal");
    let observed = database
        .store_current_publication()
        .await
        .expect("read rolled back publication boundary");
    assert_eq!(observed.record(), &staged.publication.previous);
    assert_eq!(
        observed
            .require_observed()
            .expect("recovery provider observation")
            .version(),
        &staged.publication.previous_version
    );
    assert_eq!(
        database
            .active_store_publication()
            .await
            .expect("read retained publication reservation")
            .expect("failed completion retains its reservation")
            .attempt()
            .expect("prepared publication"),
        &staged.publication
    );
    assert_recovery_uploads(&first_database, &staged, false).await;
    if pull_accepted {
        let pulled = first_recovery
            .pull(None)
            .await
            .expect("pull the accepted recovery activation");
        assert!(pulled.held_positions.is_empty());
    }
    first_recovery
        .recover_owner_device(&authority, None)
        .await
        .expect("retry completes the same accepted recovery activation");
    assert!(database
        .owner_recovery_publication()
        .await
        .expect("read completed recovery journal")
        .is_none());
    assert!(database
        .active_store_publication()
        .await
        .expect("read completed publication reservation")
        .is_none());
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("read accepted recovery boundary")
            .record(),
        &staged.publication.replacement
    );

    assert_recovery_uploads(&first_database, &staged, true).await;
    let registration = database
        .activated_store_device_registration_with_authority(
            &store.root(),
            staged.commit.value.author_registration.clone(),
        )
        .await
        .expect("read completed recovery authority");
    let evidence = database
        .installed_store_commit_evidence(staged.commit.value.clone())
        .await
        .expect("read completed recovery acceptance")
        .expect("completed recovery is installed");
    let acceptance_result = finalized_membership_result(
        &first_database,
        &staged
            .history_evidence
            .membership_proof
            .as_ref()
            .expect("recovery retains its exact authority proof")
            .head,
    )
    .await;
    database
        .complete_owner_recovery(
            staged.commit.value.clone(),
            coven_database::StoreCommitPublicationOutcome::Installed(evidence),
            staged.history_evidence.clone(),
            registration,
            acceptance_result,
        )
        .await
        .expect("exact installed recovery completion is idempotent after journal removal");
    assert_recovery_uploads(&first_database, &staged, true).await;

    let retry_store_dir = crate::sync::test_helpers::test_store_dir();
    let retry_database = crate::sync::test_helpers::open_test_db(retry_store_dir.clone());
    let retry_device = store
        .open_into(&retry_database, retry_store_dir)
        .await
        .expect("open recovery retry without the first attempt's journal");
    retry_device
        .owner_recovery_for_test()
        .await
        .expect("authorize recovery retry")
        .recover_owner_device(&authority, None)
        .await
        .expect("adopt the accepted first head pulled by the predecessor barrier");
}

async fn finalized_membership_result(
    database: &coven_database::Database,
    expected_head: &coven_protocol::membership::MembershipHeadRef,
) -> coven_protocol::remote_object::RemoteObjectRecord {
    use coven_protocol::remote_object::{RemoteObjectRecord, RetainedAuthorityObjectDomain};

    let objects = database
        .remote_objects_for_test()
        .await
        .expect("read actual remote object ownership");
    let mut results = objects.into_iter().filter(|object| {
        matches!(object, RemoteObjectRecord::RetainedAuthority(record)
            if matches!(&record.identity.domain,
                RetainedAuthorityObjectDomain::MembershipHeadAcceptance { head, .. }
                    if head == expected_head))
    });
    let result = results
        .next()
        .expect("the exact head has a finalized result");
    assert!(results.next().is_none(), "one result owns the exact head");
    assert!(result.records_verified_upload());
    result
}

async fn assert_recovery_uploads(
    database: &coven_database::Database,
    staged: &coven_database::OwnerRecoveryPublication,
    activated: bool,
) {
    assert!(!database
        .remote_object_exists_for_test(staged.publication.entry_object.clone())
        .await
        .expect("publication entries are not generic candidate objects"));
    let store = coven_database::StoreDatabase::new(database);
    if activated {
        let accepted = store
            .store_publication_entries()
            .await
            .expect("read accepted recovery entry")
            .into_iter()
            .find(|entry| {
                entry.value.payload
                    == coven_protocol::store_commit::StorePublicationPayload::Commit(
                        staged.commit.value.reference().clone(),
                    )
            })
            .expect("completed recovery retains its accepted entry");
        assert_eq!(accepted.value, staged.publication.entry);
        assert_eq!(
            accepted.prepared.reference(),
            &staged.publication.entry_object
        );
    } else {
        let active = store
            .active_store_publication()
            .await
            .expect("read retained recovery attempt")
            .expect("incomplete recovery owns its publication");
        assert_eq!(
            active.attempt().expect("prepared publication"),
            &staged.publication
        );
    }
    let object = &staged.commit.value.reference().object;
    let id = coven_protocol::remote_object::remote_object_id(object);
    let state = database
        .query_test_text(&format!(
            "SELECT state FROM remote_objects WHERE object_id = '{id}'"
        ))
        .await;
    let remote: coven_protocol::remote_object::RemoteObjectRecord =
        serde_json::from_str(&state).expect("read retained recovery upload");
    assert_eq!(remote.object(), object);
    assert!(remote.records_verified_upload());
    if activated {
        let coven_protocol::remote_object::RemoteObjectRecord::RetainedAuthority(record) = remote
        else {
            panic!("completed recovery retains an unactivated candidate: {remote:?}");
        };
        let coven_protocol::remote_object::RetainedAuthorityObjectState::UploadedVerified {
            ownership,
        } = record.state
        else {
            panic!("completed recovery object is not retained as uploaded");
        };
        assert!(ownership.pending.is_empty());
        assert_eq!(
            ownership.activated,
            std::collections::BTreeSet::from([staged.commit.value.reference().clone()])
        );
    }
}

#[tokio::test]
async fn cold_snapshot_recovery_keeps_covered_and_new_concurrent_tips() {
    let founder = UserKeypair::generate();
    let founder_store_dir = crate::sync::test_helpers::test_store_dir();
    let founder_db = crate::sync::test_helpers::open_test_db(founder_store_dir.clone());
    let home = crate::sync::test_helpers::test_cloud_home();
    let (store, cloud_storage) = TestStore::create_with_connection(
        &founder_db,
        founder_store_dir.clone(),
        "cold-snapshot-recovery",
        founder.clone(),
        home,
    )
    .await
    .expect("create cold snapshot recovery Store");
    let peer = UserKeypair::generate();
    let peer_store_dir = crate::sync::test_helpers::test_store_dir();
    let peer_db = crate::sync::test_helpers::open_test_db(peer_store_dir.clone());
    let peer_device = store
        .admit_and_activate_peer(
            &founder_db,
            founder_store_dir.clone(),
            &peer_db,
            peer_store_dir,
            &peer,
        )
        .await
        .expect("activate peer writer");
    let founder_device = store
        .bind_device(&founder_db, founder_store_dir.clone(), &founder)
        .await
        .expect("bind founder writer");
    founder_device
        .publish_fixture_position("snapshot-covered-founder")
        .await;
    let founder_tip = founder_device
        .latest_local_store_position()
        .await
        .expect("read founder snapshot tip")
        .expect("founder snapshot tip exists");
    let membership = founder_device
        .membership_for_test()
        .await
        .expect("load snapshot membership");
    let founder_database = coven_database::StoreDatabase::new(&founder_db);
    let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
        founder_database
            .materialized_frontier()
            .await
            .expect("read founder snapshot coverage"),
    )
    .expect("shape founder snapshot coverage");
    let image_dir = tempfile::tempdir().expect("create snapshot image directory");
    let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
    let image = founder_database
        .capture_snapshot_image_for_test(
            store.root(),
            image_dir.path().to_path_buf(),
            Some(encryption.clone()),
        )
        .await
        .expect("capture founder snapshot image");
    founder_device
        .publish_snapshot(image, coverage)
        .await
        .expect("publish founder snapshot");

    let (_restore_temp, restore_dir) = crate::sync::test_helpers::temp_store_dir();
    let database_path = restore_dir.db_path();
    let bootstrap = store
        .prepare_snapshot_bootstrap(
            &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
            1,
            &database_path,
            &founder,
        )
        .await
        .expect("prepare founder snapshot bootstrap");
    let restoring = bootstrap
        .install(
            &restore_dir,
            crate::sync::test_helpers::test_synced_tables(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "restored-device".to_string(),
            Arc::new(coven_foundation::clock::SystemClock),
            &crate::sync::test_helpers::test_migrations(),
            coven_database::CovenMigrationPolicy::ApplyPending,
            Some(&encryption),
        )
        .await
        .expect("install founder snapshot");
    drop(restoring);

    let reopened = coven_database::Database::open(
        &restore_dir.db_path(),
        crate::sync::test_helpers::test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "restored-device".to_string(),
        Arc::new(coven_foundation::clock::SystemClock),
        coven_database::CovenMigrationPolicy::ApplyPending,
        &crate::sync::test_helpers::test_migrations(),
    )
    .expect("reopen installed snapshot database");
    let loaded = super::Store::load(
        coven_database::StoreDatabase::new(&reopened),
        cloud_storage,
        restore_dir,
        founder.clone(),
        Some(encryption.clone()),
    )
    .await
    .expect("load cold snapshot Store");
    let mut recovery = loaded
        .owner_recovery_for_test()
        .await
        .expect("authorize recovery from the cold snapshot");
    let initial_pull = recovery
        .pull(Some(&encryption))
        .await
        .expect("seed retained snapshot history");
    assert!(initial_pull.held_positions.is_empty());

    peer_device
        .publish_fixture_position("concurrent-after-snapshot")
        .await;
    let peer_tip = peer_device
        .latest_local_store_position()
        .await
        .expect("read peer position after snapshot")
        .expect("peer position after snapshot exists");
    assert!(
        peer_tip.coord.stream_id.ne(&founder_tip.coord.stream_id),
        "the concurrent tips belong to distinct writers",
    );
    let authority = store.founder_recovery_authority().await;
    let recovered_registration = recovery
        .recover_owner_device(&authority, Some(&encryption))
        .await
        .expect("recover over retained and newly published history");
    drop(recovery);

    let database = coven_database::StoreDatabase::new(&reopened);
    let mut activation = None;
    for reference in database
        .materialized_frontier()
        .await
        .expect("read recovered snapshot frontier")
        .into_values()
    {
        let commit = loaded
            .load_commit_for_test(&reference)
            .await
            .expect("load recovered snapshot frontier commit");
        if commit.value().author_registration == recovered_registration {
            activation = Some(commit);
            break;
        }
    }
    let activation = activation.expect("snapshot recovery activation is materialized");
    assert_eq!(
        activation
            .value()
            .order
            .dependencies
            .get(&founder_tip.coord.stream_id),
        Some(&founder_tip),
        "the activation retains the founder tip carried only by the snapshot",
    );
    assert_eq!(
        activation
            .value()
            .order
            .dependencies
            .get(&peer_tip.coord.stream_id),
        Some(&peer_tip),
        "the activation also orders itself after the concurrent peer tip",
    );
}

#[tokio::test]
async fn adopted_recovery_keeps_its_current_ack_when_preparation_fails() {
    let owner = UserKeypair::generate();
    let store_dir = crate::sync::test_helpers::test_store_dir();
    let database = crate::sync::test_helpers::open_test_db(store_dir.clone());
    let home = crate::sync::test_helpers::test_cloud_home();
    let (store, storage) = TestStore::create_with_connection(
        &database,
        store_dir.clone(),
        "atomic-recovery-continuation",
        owner.clone(),
        home.clone(),
    )
    .await
    .expect("create recovery Store");
    let founder = store
        .bind_device(&database, store_dir.clone(), &owner)
        .await
        .expect("bind founder");
    let authority = store.founder_recovery_authority().await;
    let registration = founder
        .owner_recovery_for_test()
        .await
        .expect("authorize first recovery")
        .recover_owner_device(&authority, None)
        .await
        .expect("activate recovery registration");
    let recovered = store
        .bind_device(&database, store_dir, &owner)
        .await
        .expect("bind recovered device");
    // Keep this verifier at the previous accepted history. The new ACK must
    // be loaded during adoption rather than already residing in its cache.
    let mut recovery = recovered
        .owner_recovery_for_test()
        .await
        .expect("authorize repeated recovery");
    recovered.publish_fixture_position("after-recovery").await;
    let frontier = recovered
        .acknowledgement_frontier()
        .await
        .expect("read acknowledgement frontier");
    recovered
        .publish_acknowledgement_without_advancing(frontier)
        .await
        .expect("publish recovered acknowledgement");
    let records = coven_database::StoreDatabase::new(&database);
    let before = records
        .latest_local_store_ack()
        .await
        .expect("read current local acknowledgement")
        .expect("recovered acknowledgement is installed");
    assert!(before.reference.sequence > 1);
    assert_eq!(before.reference.registration, registration);
    let journal = records
        .latest_local_store_device_registration()
        .await
        .expect("read local registration")
        .expect("recovered registration is installed");
    let boundary = records
        .store_current_publication()
        .await
        .expect("read accepted boundary");
    let context = ProtocolObjectContext::signed_plaintext(
        store.root().store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    let prefix = coven_protocol::store_commit::ack_slot_prefix(
        &registration.device_id.to_string(),
        before.reference.sequence,
    );
    let (bytes, prepared) = storage
        .read_prepared_protocol_slot(&context, before.reference.object.slot(), &prefix)
        .await
        .expect("retain exact acknowledgement for retry");
    storage
        .delete_protocol_object(&before.reference.object)
        .await
        .expect("make the current acknowledgement unavailable");
    home.clear_exact_reads();
    recovery
        .recover_owner_device(&authority, None)
        .await
        .expect_err("adoption cannot prepare the unavailable current acknowledgement");
    assert!(
        home.exact_reads().contains(before.reference.object.slot()),
        "recovery must reach the unavailable current acknowledgement",
    );
    let after = records
        .latest_local_store_ack()
        .await
        .expect("read acknowledgement after failed adoption")
        .expect("the current acknowledgement remains installed");
    assert_eq!(after.reference, before.reference);
    assert_eq!(after.successor_slot, before.successor_slot);
    assert_eq!(after.standing, before.standing);
    let after_journal = records
        .latest_local_store_device_registration()
        .await
        .expect("read registration after failed adoption")
        .expect("the registration remains installed");
    assert_eq!(after_journal.registration_bytes, journal.registration_bytes);
    assert_eq!(after_journal.prepared, journal.prepared);
    assert_eq!(after_journal.initial_ack_ref, journal.initial_ack_ref);
    assert_eq!(after_journal.state, journal.state);
    assert_eq!(
        records
            .store_current_publication()
            .await
            .expect("read preserved accepted boundary"),
        boundary,
    );
    storage
        .create_verified_protocol_object(&context, &prepared, &prefix, &bytes)
        .await
        .expect("restore the exact acknowledgement");
    assert_eq!(
        recovery
            .recover_owner_device(&authority, None)
            .await
            .expect("retry the adopted recovery"),
        registration,
    );
    let resumed = records
        .latest_local_store_ack()
        .await
        .expect("read resumed acknowledgement")
        .expect("resumed acknowledgement exists");
    assert_eq!(resumed.reference, before.reference);
    assert_eq!(resumed.successor_slot, before.successor_slot);
}
