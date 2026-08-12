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
            .recover_owner_device(&authority)
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
            .recover_owner_device(&authority)
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
                recovery.recover_owner_device(&authority).await.is_err(),
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
                .recover_owner_device(&authority)
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
}
