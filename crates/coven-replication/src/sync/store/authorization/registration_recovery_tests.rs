use crate::sync::test_helpers::{
    pubkey_hex, InterceptedStorage, ProtocolRead, StorageInterceptor, TestDevice, TestStore,
};
use coven_keys::keys::UserKeypair;
use coven_protocol::membership::MemberRole;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{
    owner_recovery_semantic_prefix, OwnerRecoveryNode, StoreDeviceRegistration,
    StoreDeviceRegistrationOrigin,
};
use coven_storage::CloudSyncObjectStorage;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct PublishBetweenAuthorityPasses {
    probe_prefix: String,
    writer: Arc<TestDevice>,
    matching_reads: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl StorageInterceptor for PublishBetweenAuthorityPasses {
    async fn before_protocol_read(
        &self,
        read: ProtocolRead,
        semantic_prefix: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        if read != ProtocolRead::Slot || semantic_prefix != self.probe_prefix {
            return Ok(());
        }
        let read = self.matching_reads.fetch_add(1, Ordering::SeqCst) + 1;
        if read <= 3 {
            self.writer
                .publish_fixture_position(&format!("authority-read-{read}"))
                .await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn current_authority_finishes_while_data_only_frontiers_keep_advancing() {
    let founder = UserKeypair::generate();
    let founder_store_dir = crate::sync::test_helpers::test_store_dir();
    let founder_db = crate::sync::test_helpers::open_test_db(founder_store_dir.clone());
    let home = crate::sync::test_helpers::test_cloud_home();
    let (store, cloud_storage) = TestStore::create_with_connection(
        &founder_db,
        founder_store_dir.clone(),
        "advancing-authority-frontier",
        founder.clone(),
        home,
    )
    .await
    .expect("create advancing authority Store");
    let peer = UserKeypair::generate();
    let peer_store_dir = crate::sync::test_helpers::test_store_dir();
    let peer_db = crate::sync::test_helpers::open_test_db(peer_store_dir.clone());
    let peer_device = store
        .admit_and_activate_peer(
            &founder_db,
            founder_store_dir.clone(),
            &peer_db,
            peer_store_dir.clone(),
            &peer,
        )
        .await
        .expect("activate peer writer");
    let founder_device = store
        .bind_device(&founder_db, founder_store_dir.clone(), &founder)
        .await
        .expect("bind founder writer");
    let membership = founder_device
        .membership_for_test()
        .await
        .expect("load current membership");
    let founder_id = founder_device.typed_device_id();
    let peer_id = peer_device.typed_device_id();
    let (probe_database, probe_store_dir, probe_identity, probe_id, probe_tip, writer) =
        if founder_id < peer_id {
            let tip = founder_device
                .latest_local_store_position()
                .await
                .expect("read founder tip");
            (
                &founder_db,
                founder_store_dir,
                &founder,
                founder_id,
                tip,
                Arc::new(peer_device),
            )
        } else {
            let tip = peer_device
                .latest_local_store_position()
                .await
                .expect("read peer tip");
            (
                &peer_db,
                peer_store_dir,
                &peer,
                peer_id,
                tip,
                Arc::new(founder_device),
            )
        };
    let next_sequence = probe_tip
        .as_ref()
        .map_or(1, |reference| reference.coord.sequence().saturating_add(1));
    let matching_reads = Arc::new(AtomicUsize::new(0));
    let intercepted: Arc<dyn CloudSyncObjectStorage> = Arc::new(InterceptedStorage::new(
        Arc::new(cloud_storage.connection_for_test_identity(probe_identity.clone())),
        PublishBetweenAuthorityPasses {
            probe_prefix: coven_protocol::store_commit::head_slot_prefix(
                &probe_id.to_string(),
                next_sequence,
            ),
            writer: writer.clone(),
            matching_reads: matching_reads.clone(),
        },
    ));
    let loaded = super::Store::load(
        coven_database::StoreDatabase::new(probe_database),
        intercepted,
        probe_store_dir,
        probe_identity.clone(),
    )
    .await
    .expect("load authority verifier with intercepted storage");
    let mut history = loaded
        .authorize_history()
        .await
        .expect("authorize current history");

    let target = history
        .current_merge_authority_cut(&membership)
        .await
        .expect("capture one self-consistent authority cut");
    let last_published = writer
        .latest_local_store_position()
        .await
        .expect("read advancing writer tip")
        .expect("the interceptor published a writer position");

    assert_eq!(
        target.0.get(&last_published.coord.stream_id),
        Some(&last_published),
        "the returned authority includes the data tip observed during its pass",
    );
    assert!(
        matching_reads.load(Ordering::SeqCst) <= 2,
        "authority discovery repeated a self-consistent unchanged device state",
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

    let recovered = recovery
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
    let head_prefix = coven_protocol::store_commit::head_slot_prefix(
        &recovered.device_id.to_string(),
        staged.head.value.slot_sequence(),
    );
    let (head_bytes, head_prepared) = cloud_storage
        .read_prepared_protocol_slot(
            &ProtocolObjectContext::signed_plaintext(
                store.root().store_root_hash,
                ProtocolObjectDomain::StoreHead,
            ),
            staged.head.prepared.reference().slot(),
            &head_prefix,
        )
        .await
        .expect("read retried recovery head");
    assert_eq!(head_bytes, staged.head.bytes);
    assert_eq!(head_prepared, staged.head.prepared);
}

#[tokio::test]
async fn recovery_adopts_a_first_head_discovered_at_its_predecessor_barrier() {
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
        .expect_err("fail local completion after publishing the first head");
    assert!(first_error.to_string().contains("injected failure"));
    let pulled = first_recovery
        .pull(None)
        .await
        .expect("pull the published recovery activation into its original database");
    assert!(pulled.held_positions.is_empty());
    assert!(
        coven_database::StoreDatabase::new(&first_database)
            .owner_recovery_publication()
            .await
            .expect("read publication journal after pulled activation")
            .is_none(),
        "accepted recovery activation consumes its exact publication journal",
    );
    first_recovery
        .recover_owner_device(&authority, None)
        .await
        .expect("retry adopts the activation pulled into the original database");

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
