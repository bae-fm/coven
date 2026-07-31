//! Durable append-only Store device registration and recovery.

use crate::storage::StoreObjectError;

#[cfg(test)]
use crate::database::{Database, StoreDatabase};
#[cfg(test)]
use crate::keys::UserKeypair;
#[cfg(test)]
use crate::protocol::store_commit::{
    owner_recovery_semantic_prefix, ObjectHash, OwnerRecoveryPosition, StoreCommitCoord,
    StoreDeviceRegistration, StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef,
};
#[cfg(test)]
use crate::storage::ProtocolObjectDomain;

#[derive(Debug, thiserror::Error)]
pub enum StoreRegistrationError {
    #[error("Store device registration database state: {0}")]
    Database(String),
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("exact Store root authority is absent")]
    ExactRootAuthorityMissing,
    #[error("Store device registration bytes are invalid: {0}")]
    Invalid(String),
    #[error("this Store installation requires an activated Join or Recovery registration")]
    ActivationRequired,
    #[error("Store device registration activation: {0}")]
    Outbound(#[from] crate::sync::store::StoreError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::store_commit::StoreBatchCommitRef;
    use crate::storage::SyncStorage;
    use crate::sync::test_helpers::{open_test_db, TestStore};

    async fn founder_recovery_authority(
        store: &TestStore,
    ) -> crate::restoration::OwnerRecoveryAuthority {
        let device = store.founder_device().await.expect("load founder Store");
        let protocol_root = device.protocol_root_for_test();
        let owner_grant = protocol_root.descriptor.founder_grant.clone();
        let activation = crate::protocol::store_commit::OwnerRecoveryActivationId::derive(
            &store.root,
            &crate::keys::public_key_hex(&store.signer),
            &owner_grant,
            &protocol_root.descriptor.founder_recovery,
        )
        .expect("derive founder recovery activation");
        crate::restoration::OwnerRecoveryAuthority {
            owner_identity_secret: hex::encode(store.signer.to_keypair_bytes()),
            owner_grant: owner_grant.clone(),
            recovery: crate::protocol::store_commit::OwnerRecoveryCursor {
                owner_grant,
                position: OwnerRecoveryPosition::BeforeFirst { activation },
            },
            published_at: "2026-07-17T00:00:00Z".to_string(),
        }
    }

    async fn initialized() -> (TestStore, Database) {
        let signer = UserKeypair::generate();
        let db = open_test_db();
        let store = TestStore::create(&db, "registration-store-test", signer)
            .await
            .expect("create exact registration test Store");
        (store, db)
    }

    async fn restoring_store<'storage>(
        store: &'storage crate::sync::test_helpers::TestDevice,
    ) -> super::super::restore::RestoringStore<'storage> {
        store
            .restoring_for_test()
            .await
            .expect("authorize Owner recovery Store")
    }

    async fn recovered_author() -> (
        TestStore,
        Database,
        StoreDeviceRegistrationRef,
        StoreBatchCommitRef,
    ) {
        let (store, db) = initialized().await;
        let loaded = store
            .bind_device(&db, &store.signer)
            .await
            .expect("load recovery Store");
        let authority = founder_recovery_authority(&store).await;
        let database = crate::database::StoreDatabase::new(&db);
        let registration = restoring_store(&loaded)
            .await
            .recover_owner_device(&authority)
            .await
            .expect("recover Owner device");
        let loaded = store
            .bind_device(&db, &store.signer)
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
                return (store, db, registration, reference);
            }
        }
        panic!("recovery commit is materialized")
    }

    #[derive(Clone, Copy)]
    enum RetainedRegistrationTamper {
        CanonicalRegistration,
        ActivationAuthority,
    }

    async fn tamper_retained_recovery_registration(
        db: &Database,
        reference: &StoreBatchCommitRef,
        tamper: RetainedRegistrationTamper,
    ) {
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &reference.coord;
        let stream_id = stream_id.to_string();
        let sequence = i64::try_from(*sequence).expect("recovery sequence fits SQLite");
        db.call(move |conn| {
            let (commit_ref, canonical_input): (String, Vec<u8>) = conn
                .query_row(
                    "SELECT commit_ref, canonical_input
                     FROM retained_merge_materializations
                     WHERE device_id = ?1 AND seq = ?2",
                    (&stream_id, sequence),
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(crate::database::DbError::from)?;
            let mut input: serde_json::Value = serde_json::from_slice(&canonical_input)
                .expect("parse retained recovery materialization");
            let registration = input
                .get_mut("activation")
                .and_then(|value| value.get_mut("registrations"))
                .and_then(|value| value.get_mut("registrations"))
                .and_then(serde_json::Value::as_array_mut)
                .and_then(|values| values.first_mut())
                .expect("retained recovery registration");
            match tamper {
                RetainedRegistrationTamper::CanonicalRegistration => registration
                    .get_mut("canonical_registration")
                    .and_then(serde_json::Value::as_array_mut)
                    .expect("canonical recovery registration bytes")
                    .push(serde_json::Value::from(b' ')),
                RetainedRegistrationTamper::ActivationAuthority => {
                    let recovery = registration
                        .get_mut("authority")
                        .and_then(|value| value.get_mut("recovery"))
                        .and_then(serde_json::Value::as_object_mut)
                        .expect("retained recovery authority");
                    recovery.insert(
                        "recovery_id".to_string(),
                        serde_json::Value::String("0".repeat(64)),
                    );
                }
            }
            let canonical_input = serde_json::to_vec(&input)
                .expect("serialize tampered retained recovery materialization");
            let input_hash = ObjectHash::digest(&canonical_input).to_string();
            let tx = conn
                .unchecked_transaction()
                .map_err(crate::database::DbError::from)?;
            tx.execute(
                "DELETE FROM materialized_commits WHERE device_id = ?1 AND seq = ?2",
                (&stream_id, sequence),
            )
            .map_err(crate::database::DbError::from)?;
            tx.execute(
                "UPDATE retained_merge_materializations
                 SET input_hash = ?3, canonical_input = ?4
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![&stream_id, sequence, &input_hash, &canonical_input],
            )
            .map_err(crate::database::DbError::from)?;
            tx.execute(
                "INSERT INTO materialized_commits
                 (device_id, seq, commit_ref, retained_commit_ref, retained_input_hash)
                 VALUES (?1, ?2, ?3, ?3, ?4)",
                rusqlite::params![&stream_id, sequence, &commit_ref, &input_hash],
            )
            .map_err(crate::database::DbError::from)?;
            tx.commit().map_err(crate::database::DbError::from)
        })
        .await
        .expect("install tampered retained recovery registration");
    }

    #[tokio::test]
    async fn store_root_state_failures_keep_registration_error_variants() {
        let db = open_test_db();
        let database = crate::database::StoreDatabase::new(&db);
        let store = TestStore::for_store("registration-missing-root-storage").await;

        assert!(matches!(
            super::super::RegistrationOutbox::new(database, &store.storage)
                .drain()
                .await,
            Err(StoreRegistrationError::ExactRootAuthorityMissing)
        ));
    }

    #[tokio::test]
    async fn exact_founder_registration_is_already_activated() {
        let (store, db) = initialized().await;
        let database = crate::database::StoreDatabase::new(&db);
        let loaded = store
            .bind_device(&db, &store.signer)
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
        assert_eq!(activated[0].store_root, store.root);
    }

    #[tokio::test]
    async fn owner_recovery_publishes_and_activates_replacement_device() {
        let (store, db) = initialized().await;
        let loaded = store
            .bind_device(&db, &store.signer)
            .await
            .expect("load recovery Store");
        let authority = founder_recovery_authority(&store).await;
        let database = crate::database::StoreDatabase::new(&db);
        let registration = restoring_store(&loaded)
            .await
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
            .bind_device(&db, &store.signer)
            .await
            .expect("load recovered Owner Store");
        loaded
            .authorize_writer()
            .await
            .expect("replacement registration is usable");
    }

    #[tokio::test]
    async fn recovery_materialization_reopens_its_retained_introduced_author() {
        let (_store, db, registration, reference) = recovered_author().await;
        let device_id = registration.device_id.to_string();
        let registration_hash = registration.registration_hash.to_string();
        db.call(move |conn| {
            conn.execute(
                "UPDATE store_device_registration_activations
                 SET registration_bytes = X'00'
                 WHERE device_id = ?1 AND registration_hash = ?2",
                (&device_id, &registration_hash),
            )
            .map_err(crate::database::DbError::from)?;
            Ok(())
        })
        .await
        .expect("corrupt activated recovery registration fixture");

        let frontier = crate::database::StoreDatabase::new(&db)
            .materialized_frontier()
            .await
            .expect("retained recovery author does not depend on mutable registration rows");
        let StoreCommitCoord { stream_id, .. } = &reference.coord;
        assert_eq!(frontier.get(&stream_id.to_string()), Some(&reference));
    }

    #[tokio::test]
    async fn recovery_materialization_rejects_tampered_retained_registration_bytes() {
        let (store, db, _registration, reference) = recovered_author().await;
        tamper_retained_recovery_registration(
            &db,
            &reference,
            RetainedRegistrationTamper::CanonicalRegistration,
        )
        .await;

        let root = store.root.clone();
        db.call(move |conn| {
            StoreDatabase::load_retained_merge_replay_inputs_on(conn, &root).map(drop)
        })
        .await
        .expect_err(
            "tampered retained recovery registration bytes must fail durable history verification",
        );
    }

    #[tokio::test]
    async fn recovery_materialization_rejects_tampered_retained_registration_authority() {
        let (store, db, _registration, reference) = recovered_author().await;
        tamper_retained_recovery_registration(
            &db,
            &reference,
            RetainedRegistrationTamper::ActivationAuthority,
        )
        .await;

        let root = store.root.clone();
        db.call(move |conn| {
            StoreDatabase::load_retained_merge_replay_inputs_on(conn, &root).map(drop)
        })
            .await
            .expect_err(
                "tampered retained recovery registration authority must fail durable history verification",
            );
    }

    #[tokio::test]
    async fn owner_recovery_retry_reuses_each_published_readiness_prefix() {
        for failed_call in [2, 3, 4] {
            let signer = UserKeypair::generate();
            let db = open_test_db();
            let store = TestStore::create(&db, &format!("recovery-prefix-{failed_call}"), signer)
                .await
                .expect("create recovery prefix Store");
            let loaded = store
                .bind_device(&db, &store.signer)
                .await
                .expect("load recovery Store");
            let authority = founder_recovery_authority(&store).await;
            let database = crate::database::StoreDatabase::new(&db);
            let mut restoring = restoring_store(&loaded).await;
            store.home.fail_exact_create_before_call(failed_call);
            assert!(
                restoring.recover_owner_device(&authority).await.is_err(),
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
                    &store.root,
                    interrupted.device_id,
                )
                .expect("parse interrupted recovery registration");
                let StoreDeviceRegistrationOrigin::Recovery { recovery_slot, .. } =
                    registration.origin
                else {
                    panic!("interrupted registration is not a Recovery registration");
                };
                let context = crate::storage::ProtocolObjectContext::signed_plaintext(
                    store.root.store_root_hash,
                    ProtocolObjectDomain::OwnerRecoveryNode,
                );
                Some(
                    store
                        .storage
                        .read_prepared_protocol_slot(
                            &context,
                            &recovery_slot,
                            &owner_recovery_semantic_prefix(
                                &crate::keys::public_key_hex(&store.signer),
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

            restoring
                .recover_owner_device(&authority)
                .await
                .expect("retry completes absent recovery suffix");
            assert_eq!(
                store.home.exact_create_count(),
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
                    &store.root,
                    completed.device_id,
                )
                .expect("parse completed recovery registration");
                let StoreDeviceRegistrationOrigin::Recovery { recovery_slot, .. } =
                    registration.origin
                else {
                    panic!("completed registration is not a Recovery registration");
                };
                let context = crate::storage::ProtocolObjectContext::signed_plaintext(
                    store.root.store_root_hash,
                    ProtocolObjectDomain::OwnerRecoveryNode,
                );
                let completed_node = store
                    .storage
                    .read_prepared_protocol_slot(
                        &context,
                        &recovery_slot,
                        &owner_recovery_semantic_prefix(
                            &crate::keys::public_key_hex(&store.signer),
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
