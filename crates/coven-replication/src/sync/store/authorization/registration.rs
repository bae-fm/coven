//! Durable append-only Store device registration and recovery.

use coven_protocol::objects::StoreObjectError;

#[cfg(test)]
use super::RegistrationOutbox;
#[cfg(test)]
use coven_keys::keys::UserKeypair;
#[cfg(test)]
use coven_protocol::objects::ProtocolObjectDomain;
#[cfg(test)]
use coven_protocol::store_commit::{
    owner_recovery_semantic_prefix, StoreCommitCoord, StoreDeviceRegistration,
    StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef,
};

#[derive(Debug, thiserror::Error)]
pub enum StoreRegistrationError {
    #[error("Store device registration database state: {0}")]
    Database(#[from] coven_database::DbError),
    #[error("Store device registration protocol: {0}")]
    Protocol(#[from] coven_protocol::store_commit::StoreProtocolError),
    #[error("Store device registration JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Store device registration author stream: {0}")]
    AuthorStreamId(#[from] coven_protocol::causal_grants::AuthorStreamIdParseError),
    #[error("Store device registration history: {0}")]
    History(#[source] Box<crate::sync::store::StorePullError>),
    #[error("Store device registration snapshot stream: {0}")]
    SnapshotStream(#[source] Box<crate::sync::store::SnapshotError>),
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("exact Store root authority is absent")]
    ExactRootAuthorityMissing,
    #[error("Store device registration bytes are invalid: {0}")]
    Invalid(String),
    #[error("this Store installation requires an activated Join or Recovery registration")]
    ActivationRequired,
    #[error("registration publish count has no representable successor")]
    PublishCountExhausted,
    #[error("Store device registration activation: {0}")]
    Outbound(#[from] crate::sync::store::StoreError),
}

impl From<crate::sync::store::StorePullError> for StoreRegistrationError {
    fn from(error: crate::sync::store::StorePullError) -> Self {
        Self::History(Box::new(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::test_helpers::TestStore;
    use coven_database::Database;
    use coven_protocol::store_commit::StoreBatchCommitRef;
    use coven_storage::CloudSyncObjectStorage;

    async fn initialized() -> (
        std::sync::Arc<TestStore>,
        Database,
        coven_foundation::store_dir::StoreDir,
        UserKeypair,
    ) {
        let signer = UserKeypair::generate();
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let store = TestStore::create(
            &db,
            db_store_dir.clone(),
            "registration-store-test",
            signer.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create exact registration test Store");
        (store, db, db_store_dir, signer)
    }

    async fn recovered_author() -> (
        std::sync::Arc<TestStore>,
        Database,
        coven_foundation::store_dir::StoreDir,
        StoreDeviceRegistrationRef,
        StoreBatchCommitRef,
    ) {
        let (store, db, db_store_dir, signer) = initialized().await;
        let loaded = store
            .bind_device(&db, db_store_dir.clone(), &signer)
            .await
            .expect("load recovery Store");
        let authority = store.founder_recovery_authority().await;
        let database = coven_database::StoreDatabase::new(&db);
        let registration = loaded
            .owner_recovery_for_test()
            .await
            .expect("authorize Owner recovery Store")
            .recover_owner_device(&authority, None)
            .await
            .expect("recover Owner device");
        let loaded = store
            .bind_device(&db, db_store_dir.clone(), &signer)
            .await
            .expect("reload recovered Store");
        for reference in database
            .materialized_frontier()
            .await
            .expect("load materialized Store frontier")
            .into_values()
        {
            let commit = loaded
                .load_commit_for_test(&reference)
                .await
                .expect("load materialized recovery commit");
            if commit.value().author_registration == registration {
                return (store, db, db_store_dir, registration, reference);
            }
        }
        panic!("recovery commit is materialized")
    }

    #[tokio::test]
    async fn store_root_state_failures_keep_registration_error_variants() {
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let database = coven_database::StoreDatabase::new(&db);
        let initialized_db_store_dir = crate::sync::test_helpers::test_store_dir();
        let initialized_db =
            crate::sync::test_helpers::open_test_db(initialized_db_store_dir.clone());
        let (_, cloud_storage) = TestStore::create_with_connection(
            &initialized_db,
            initialized_db_store_dir.clone(),
            "registration-missing-root-storage",
            UserKeypair::generate(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create registration failure test Store");

        let result = super::RegistrationOutbox::new(database, &*cloud_storage)
            .drain()
            .await;
        assert!(
            matches!(
                result,
                Err(StoreRegistrationError::ExactRootAuthorityMissing)
            ),
            "unexpected registration outbox result: {result:?}",
        );
    }

    #[tokio::test]
    async fn exact_founder_registration_is_already_activated() {
        let (store, db, db_store_dir, signer) = initialized().await;
        let database = coven_database::StoreDatabase::new(&db);
        let loaded = store
            .bind_device(&db, db_store_dir.clone(), &signer)
            .await
            .expect("load founder Store");
        loaded
            .authorize_writer()
            .await
            .expect("founder registration remains active");
        let activated = database
            .activated_store_device_registrations()
            .await
            .unwrap();
        assert_eq!(activated.len(), 1);
        assert_eq!(activated[0].store_root, store.root());
    }

    #[tokio::test]
    async fn owner_recovery_publishes_and_activates_replacement_device() {
        let (store, db, db_store_dir, signer) = initialized().await;
        let loaded = store
            .bind_device(&db, db_store_dir.clone(), &signer)
            .await
            .expect("load recovery Store");
        let authority = store.founder_recovery_authority().await;
        let database = coven_database::StoreDatabase::new(&db);
        let registration = loaded
            .owner_recovery_for_test()
            .await
            .expect("authorize Owner recovery Store")
            .recover_owner_device(&authority, None)
            .await
            .expect("recover Owner device");

        let durable = database
            .latest_local_store_device_registration()
            .await
            .expect("load replacement registration")
            .expect("replacement registration exists");
        assert_eq!(durable.device_id, registration.device_id);
        assert!(durable.is_activated());
        let loaded = store
            .bind_device(&db, db_store_dir.clone(), &signer)
            .await
            .expect("load recovered Owner Store");
        loaded
            .authorize_writer()
            .await
            .expect("replacement registration is usable");
    }

    #[tokio::test]
    async fn recovery_materialization_reopens_its_retained_introduced_author() {
        let (_store, db, _db_store_dir, registration, reference) = recovered_author().await;
        let registration = registration.clone();
        db.corrupt_store_device_registration_bytes_for_test(registration)
            .await
            .expect("corrupt activated recovery registration fixture");

        let frontier = coven_database::StoreDatabase::new(&db)
            .materialized_frontier()
            .await
            .expect("retained recovery author does not depend on mutable registration rows");
        let StoreCommitCoord { stream_id, .. } = &reference.coord;
        assert_eq!(frontier.get(&stream_id.to_string()), Some(&reference));
    }

    #[tokio::test]
    async fn recovery_materialization_rejects_tampered_retained_registration_bytes() {
        let (store, db, _db_store_dir, _registration, reference) = recovered_author().await;
        db.tamper_retained_recovery_registration_for_test(
            &reference,
            coven_database::RetainedRegistrationTamper::CanonicalRegistration,
        )
        .await;

        let root = store.root().clone();
        db.validate_retained_merge_replay_for_test(root)
            .await
            .expect_err(
            "tampered retained recovery registration bytes must fail durable history verification",
        );
    }

    #[tokio::test]
    async fn recovery_materialization_rejects_tampered_retained_registration_authority() {
        let (store, db, _db_store_dir, _registration, reference) = recovered_author().await;
        db.tamper_retained_recovery_registration_for_test(
            &reference,
            coven_database::RetainedRegistrationTamper::ActivationAuthority,
        )
        .await;

        let root = store.root().clone();
        db
            .validate_retained_merge_replay_for_test(root)
            .await
            .expect_err(
                "tampered retained recovery registration authority must fail durable history verification",
            );
    }

    #[tokio::test]
    async fn owner_recovery_retry_reuses_each_published_readiness_prefix() {
        for failed_call in [2, 3, 4] {
            let signer = UserKeypair::generate();
            let db_store_dir = crate::sync::test_helpers::test_store_dir();
            let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
            let home = crate::sync::test_helpers::test_cloud_home();
            let (store, cloud_storage) = TestStore::create_with_connection(
                &db,
                db_store_dir.clone(),
                &format!("recovery-prefix-{failed_call}"),
                signer.clone(),
                home.clone(),
            )
            .await
            .expect("create recovery prefix Store");
            let loaded = store
                .bind_device(&db, db_store_dir.clone(), &signer)
                .await
                .expect("load recovery Store");
            let authority = store.founder_recovery_authority().await;
            let database = coven_database::StoreDatabase::new(&db);
            let mut recovery = loaded
                .owner_recovery_for_test()
                .await
                .expect("authorize Owner recovery Store");
            home.fail_exact_create_before_call(failed_call);
            assert!(
                recovery
                    .recover_owner_device(&authority, None)
                    .await
                    .is_err(),
                "failure before exact create {failed_call} interrupts recovery",
            );

            let interrupted = database
                .latest_local_store_device_registration()
                .await
                .expect("read interrupted recovery journal")
                .expect("interrupted recovery journal exists");
            let interrupted_node = if failed_call == 4 {
                let registration = StoreDeviceRegistration::parse_at(
                    &interrupted.registration_bytes,
                    &store.root(),
                    interrupted.device_id,
                )
                .expect("parse interrupted recovery registration");
                let StoreDeviceRegistrationOrigin::Recovery { recovery_slot, .. } =
                    registration.origin.clone()
                else {
                    panic!("interrupted registration is not a Recovery registration");
                };
                let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                    store.root().store_root_hash,
                    ProtocolObjectDomain::OwnerRecoveryNode,
                );
                Some(
                    cloud_storage
                        .read_prepared_protocol_slot(
                            &context,
                            &recovery_slot,
                            &owner_recovery_semantic_prefix(
                                &coven_keys::keys::public_key_hex(&signer),
                                authority.owner_grant.clone(),
                                1,
                            ),
                        )
                        .await
                        .expect("read published recovery node")
                        .1,
                )
            } else {
                None
            };

            recovery
                .recover_owner_device(&authority, None)
                .await
                .expect("retry completes absent recovery suffix");
            assert_eq!(
                home.exact_create_count(),
                6,
                "retry after boundary {failed_call} creates only the absent suffix",
            );
            let completed = database
                .latest_local_store_device_registration()
                .await
                .expect("read completed recovery journal")
                .expect("completed recovery journal exists");
            assert_eq!(
                completed.prepared.reference(),
                interrupted.prepared.reference(),
            );
            assert_eq!(
                completed.prepared.stored_bytes(),
                interrupted.prepared.stored_bytes(),
            );
            if failed_call >= 3 {
                assert_eq!(
                    completed.initial_ack.prepared.reference(),
                    interrupted.initial_ack.prepared.reference(),
                );
                assert_eq!(
                    completed.initial_ack.prepared.stored_bytes(),
                    interrupted.initial_ack.prepared.stored_bytes(),
                );
            }
            if let Some(interrupted_node) = interrupted_node {
                let registration = StoreDeviceRegistration::parse_at(
                    &completed.registration_bytes,
                    &store.root(),
                    completed.device_id,
                )
                .expect("parse completed recovery registration");
                let StoreDeviceRegistrationOrigin::Recovery { recovery_slot, .. } =
                    registration.origin.clone()
                else {
                    panic!("completed registration is not a Recovery registration");
                };
                let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                    store.root().store_root_hash,
                    ProtocolObjectDomain::OwnerRecoveryNode,
                );
                let completed_node = cloud_storage
                    .read_prepared_protocol_slot(
                        &context,
                        &recovery_slot,
                        &owner_recovery_semantic_prefix(
                            &coven_keys::keys::public_key_hex(&signer),
                            authority.owner_grant.clone(),
                            1,
                        ),
                    )
                    .await
                    .expect("read completed recovery node")
                    .1;
                assert_eq!(completed_node.reference(), interrupted_node.reference());
                assert_eq!(
                    completed_node.stored_bytes(),
                    interrupted_node.stored_bytes(),
                );
            }
        }
    }

    #[tokio::test]
    async fn owner_recovery_retry_reuses_its_staged_activation_after_history_advances() {
        let founder = UserKeypair::generate();
        let founder_store_dir = crate::sync::test_helpers::test_store_dir();
        let founder_db = crate::sync::test_helpers::open_test_db(founder_store_dir.clone());
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, cloud_storage) = TestStore::create_with_connection(
            &founder_db,
            founder_store_dir.clone(),
            "staged-recovery-retry",
            founder.clone(),
            home.clone(),
        )
        .await
        .expect("create recovery retry Store");
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
            .expect("bind recovery Store");
        let authority = store.founder_recovery_authority().await;
        let database = coven_database::StoreDatabase::new(&founder_db);
        let mut recovery = founder_device
            .owner_recovery_for_test()
            .await
            .expect("authorize Owner recovery Store");

        home.fail_exact_create_before_call(4);
        recovery
            .recover_owner_device(&authority, None)
            .await
            .expect_err("activation commit publication is interrupted");
        let staged_before = database
            .owner_recovery_publication()
            .await
            .expect("read staged recovery publication")
            .expect("recovery activation is staged before publication");

        peer_device
            .publish_fixture_position("staged-recovery")
            .await;
        let recovered = recovery
            .recover_owner_device(&authority, None)
            .await
            .expect("retry publishes the exact staged activation");
        let commit_value = staged_before.commit.value.value();
        let commit_prefix = coven_protocol::store_commit::commit_semantic_prefix(
            commit_value.candidate_family(),
            &staged_before
                .commit
                .value
                .reference()
                .coord
                .stream_id
                .to_string(),
            commit_value.seq(),
            commit_value.commit_hash(),
        );
        let published_commit = cloud_storage
            .read_prepared_protocol_slot(
                &coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                    store.root().store_root_hash,
                    ProtocolObjectDomain::StoreCommit,
                ),
                staged_before.commit.prepared.reference().slot(),
                &commit_prefix,
            )
            .await
            .expect("read published recovery commit")
            .1;
        assert_eq!(
            published_commit.reference(),
            staged_before.commit.prepared.reference(),
        );
        assert_eq!(
            published_commit.stored_bytes(),
            staged_before.commit.prepared.stored_bytes(),
        );
        let head_prefix = coven_protocol::store_commit::head_slot_prefix(
            &recovered.device_id.to_string(),
            staged_before.head.value.slot_sequence(),
        );
        let published_head = cloud_storage
            .read_prepared_protocol_slot(
                &coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                    store.root().store_root_hash,
                    ProtocolObjectDomain::StoreHead,
                ),
                staged_before.head.prepared.reference().slot(),
                &head_prefix,
            )
            .await
            .expect("read published recovery head")
            .1;
        assert_eq!(
            published_head.reference(),
            staged_before.head.prepared.reference(),
        );
        assert_eq!(
            published_head.stored_bytes(),
            staged_before.head.prepared.stored_bytes(),
        );
    }

    #[tokio::test]
    async fn owner_recovery_activation_covers_history_published_after_its_initial_ack() {
        let founder = UserKeypair::generate();
        let founder_store_dir = crate::sync::test_helpers::test_store_dir();
        let founder_db = crate::sync::test_helpers::open_test_db(founder_store_dir.clone());
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, _cloud_storage) = TestStore::create_with_connection(
            &founder_db,
            founder_store_dir.clone(),
            "recovery-predecessor-barrier",
            founder.clone(),
            home.clone(),
        )
        .await
        .expect("create recovery barrier Store");
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
            .expect("bind recovery Store");
        let authority = store.founder_recovery_authority().await;
        let mut recovery = founder_device
            .owner_recovery_for_test()
            .await
            .expect("authorize Owner recovery Store");
        let (node_published, release_recovery) = home.pause_after_exact_create_call(3);

        let recover = recovery.recover_owner_device(&authority, None);
        let publish = async {
            node_published.notified().await;
            peer_device
                .publish_fixture_position("recovery-predecessor")
                .await;
            let reference = peer_device
                .latest_local_store_position()
                .await
                .expect("read peer position")
                .expect("peer position exists");
            release_recovery.notify_one();
            reference
        };
        let (recovered, peer_reference) = tokio::join!(recover, publish);
        let recovered_registration = recovered.expect("recover over the fixed predecessor cut");

        let loaded = store
            .bind_device(&founder_db, founder_store_dir, &founder)
            .await
            .expect("reload recovered Store");
        let database = coven_database::StoreDatabase::new(&founder_db);
        let mut activation = None;
        for reference in database
            .materialized_frontier()
            .await
            .expect("read recovery frontier")
            .into_values()
        {
            let commit = loaded
                .load_commit_for_test(&reference)
                .await
                .expect("load recovery frontier commit");
            if commit.value().author_registration == recovered_registration {
                activation = Some(commit);
                break;
            }
        }
        let activation = activation.expect("recovery activation is materialized");
        assert_eq!(
            activation
                .value()
                .order
                .dependencies
                .get(&peer_reference.coord.stream_id),
            Some(&peer_reference),
            "the recovery activation orders itself after the peer history visible before staging",
        );
    }

    #[tokio::test]
    async fn owner_recovery_does_not_stage_activation_across_held_history() {
        let founder = UserKeypair::generate();
        let founder_store_dir = crate::sync::test_helpers::test_store_dir();
        let founder_db = crate::sync::test_helpers::open_test_db(founder_store_dir.clone());
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, _cloud_storage) = TestStore::create_with_connection(
            &founder_db,
            founder_store_dir.clone(),
            "held-recovery-predecessor",
            founder.clone(),
            home.clone(),
        )
        .await
        .expect("create held recovery Store");
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
        peer_device.publish_fixture_position("held-recovery").await;
        let peer_reference = peer_device
            .latest_local_store_position()
            .await
            .expect("read held peer position")
            .expect("held peer position exists");
        let peer_commit = peer_device
            .load_commit_for_test(&peer_reference)
            .await
            .expect("load held peer commit");
        let package = peer_commit
            .value()
            .store_package()
            .expect("peer commit carries its Store package");
        home.remove_exact_object(package.object.slot());

        let founder_device = store
            .bind_device(&founder_db, founder_store_dir, &founder)
            .await
            .expect("bind recovery Store");
        let authority = store.founder_recovery_authority().await;
        let database = coven_database::StoreDatabase::new(&founder_db);
        let error = founder_device
            .owner_recovery_for_test()
            .await
            .expect("authorize Owner recovery Store")
            .recover_owner_device(&authority, None)
            .await
            .expect_err("held predecessor history blocks activation staging");

        assert!(
            error.to_string().contains("is held at"),
            "unexpected held recovery error: {error}",
        );
        assert!(
            database
                .owner_recovery_publication()
                .await
                .expect("read recovery publication state")
                .is_none(),
            "no activation is staged without its complete predecessor history",
        );
    }

    #[tokio::test]
    async fn published_owner_recovery_blocks_snapshot_retirement_until_activation() {
        let founder = UserKeypair::generate();
        let founder_store_dir = crate::sync::test_helpers::test_store_dir();
        let founder_db = crate::sync::test_helpers::open_test_db(founder_store_dir.clone());
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, _cloud_storage) = TestStore::create_with_connection(
            &founder_db,
            founder_store_dir.clone(),
            "pending-recovery-retirement",
            founder.clone(),
            home.clone(),
        )
        .await
        .expect("create recovery retirement Store");
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
        peer_device
            .publish_fixture_position("retirement-snapshot-input")
            .await;
        let (_, founder_pull) = founder_device
            .pull_store()
            .await
            .expect("pull snapshot input into founder");
        assert!(founder_pull.held_positions.is_empty());

        let founder_database = coven_database::StoreDatabase::new(&founder_db);
        let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
            founder_database
                .materialized_frontier()
                .await
                .expect("read snapshot coverage"),
        )
        .expect("shape snapshot coverage");
        let image_dir = tempfile::tempdir().expect("create snapshot image directory");
        let encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
        let image = founder_database
            .capture_snapshot_image_for_test(
                store.root(),
                image_dir.path().to_path_buf(),
                Some(encryption.clone()),
            )
            .await
            .expect("capture snapshot image");
        founder_device
            .publish_snapshot(image, coverage.clone())
            .await
            .expect("publish retirement snapshot");
        founder_device
            .publish_acknowledgement_without_advancing(coverage.clone())
            .await
            .expect("publish founder crossing acknowledgement");
        peer_device
            .publish_acknowledgement_without_advancing(coverage.clone())
            .await
            .expect("publish peer crossing acknowledgement");
        founder_device
            .publish_fixture_position("founder-acknowledgement-activation")
            .await;
        peer_device
            .publish_fixture_position("peer-acknowledgement-activation")
            .await;
        let (_, peer_pull) = peer_device
            .pull_store()
            .await
            .expect("materialize the acknowledgement closure");
        assert!(peer_pull.held_positions.is_empty());

        let authority = store.founder_recovery_authority().await;
        let mut recovery = founder_device
            .owner_recovery_for_test()
            .await
            .expect("authorize Owner recovery Store");
        let (node_published, release_recovery) = home.pause_after_exact_create_call(3);
        let recover = recovery.recover_owner_device(&authority, Some(&encryption));
        let retire = async {
            node_published.notified().await;
            let outcome = peer_device
                .stand_on_acknowledged_snapshot()
                .await
                .expect("evaluate retirement while recovery is pending");
            release_recovery.notify_one();
            outcome
        };
        let (recovered, retirement) = tokio::join!(recover, retire);
        recovered.expect("recovery completes after retirement declines");
        assert!(
            matches!(
                retirement,
                crate::sync::store::ReplayBaselineAdvance::Declined(
                    crate::sync::store::ReplayBaselineDecline::PendingOwnerRecovery { .. }
                )
            ),
            "published recovery registration must block retirement: {retirement:?}",
        );
    }
}
