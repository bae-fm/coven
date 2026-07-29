//! Durable append-only Store device registration and recovery.

#[cfg(test)]
use crate::database::Database;
use crate::keys::UserKeypair;

use crate::sync::storage::{PreparedExactObject, ProtocolObjectDomain, SyncStorage};
use crate::sync::store::database::StoreDatabase;
use crate::sync::store_commit::{
    ack_slot_prefix, DeviceJoinAttempt, DeviceJoinAttemptDecisionRef, DeviceJoinAttemptRef,
    DeviceReadinessProof, DeviceStreamAnchor, StoreAck, StoreAckExclusionState, StoreAckRef,
    StoreBatchCommitRef, StoreDeviceRegistration, StoreDeviceRegistrationOrigin,
    StoreDeviceRegistrationRef, SuccessorLink,
};
use crate::sync::store_objects::StoreObjectError;

#[cfg(test)]
use crate::sync::store_commit::{
    owner_recovery_semantic_prefix, ObjectHash, OwnerRecoveryPosition, StoreCommitCoord,
};

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

pub(crate) async fn install_existing_founder_device(
    database: &StoreDatabase,
    commit_verifier: &super::StoreCommitVerifier<'_>,
    signer: &UserKeypair,
) -> Result<(), StoreRegistrationError> {
    let storage = commit_verifier.storage();
    let root = commit_verifier.root();
    let founder = commit_verifier.load_founder_registration().await?;
    if founder.value.author_pubkey != crate::keys::public_key_hex(signer) {
        return Err(StoreRegistrationError::Invalid(
            "Store founder registration belongs to another identity".to_string(),
        ));
    }
    if founder.value.provider
        != storage
            .provider_binding()
            .await
            .map_err(StoreObjectError::from)?
            .device
    {
        return Err(StoreRegistrationError::Invalid(
            "Store founder registration belongs to another provider principal".to_string(),
        ));
    }
    founder
        .value
        .device_signer(signer)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;

    let registration_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    let registration_prefix = crate::sync::store_commit::founder_registration_semantic_prefix(
        match founder.value.origin {
            StoreDeviceRegistrationOrigin::Founder { creation_id } => creation_id,
            _ => {
                return Err(StoreRegistrationError::Invalid(
                    "Store founder registration has a non-founder origin".to_string(),
                ))
            }
        },
    );
    let (registration_bytes, registration_prepared) = storage
        .read_prepared_protocol_slot(
            &registration_context,
            founder.object.slot(),
            &registration_prefix,
        )
        .await
        .map_err(StoreObjectError::from)?;
    if registration_bytes != founder.bytes || registration_prepared.reference() != &founder.object {
        return Err(StoreRegistrationError::Invalid(
            "prepared founder registration differs from its verified exact object".to_string(),
        ));
    }
    let registration_ref =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let DeviceStreamAnchor::StoreAcknowledgements { first_slot } = &founder.value.acknowledgements
    else {
        return Err(StoreRegistrationError::Invalid(
            "Store founder registration has no acknowledgement anchor".to_string(),
        ));
    };
    let ack_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    let ack_prefix = ack_slot_prefix(&founder.value.device_id.to_string(), 1);
    let (ack_bytes, ack_prepared) = storage
        .read_prepared_protocol_slot(&ack_context, first_slot, &ack_prefix)
        .await
        .map_err(StoreObjectError::from)?;
    let unverified_ack: StoreAck = serde_json::from_slice(&ack_bytes)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let ack_ref = StoreAckRef {
        registration: registration_ref.clone(),
        sequence: unverified_ack.sequence,
        ack_hash: unverified_ack.ack_hash(),
        object: ack_prepared.reference().clone(),
    };
    let ack = StoreAck::parse_at(&ack_bytes, root, &ack_ref, &founder.value)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    if ack.registration != registration_ref {
        return Err(StoreRegistrationError::Invalid(
            "Store founder acknowledgement names another registration".to_string(),
        ));
    }
    database
        .install_existing_local_founder_device(
            crate::database::ExactProtocolObject {
                value: founder.value,
                bytes: registration_bytes,
                object: registration_prepared.reference().clone(),
                prepared: registration_prepared,
            },
            ack_ref,
            crate::database::ExactProtocolObject {
                value: ack,
                bytes: ack_bytes,
                object: ack_prepared.reference().clone(),
                prepared: ack_prepared,
            },
        )
        .await
        .map_err(database_error)
}

pub(crate) async fn prepare_registration_for_origin(
    storage: &dyn SyncStorage,
    identity_signer: &UserKeypair,
    store_root: crate::sync::store_commit::StoreRootRef,
    origin: StoreDeviceRegistrationOrigin,
    reserved_slot: crate::storage::cloud::ObjectSlot,
    expected_provider: crate::sync::storage::ProviderDeviceBinding,
    store_commits: DeviceStreamAnchor,
    acknowledgements: DeviceStreamAnchor,
    snapshots: DeviceStreamAnchor,
) -> Result<(StoreDeviceRegistration, PreparedExactObject), StoreRegistrationError> {
    let provider = storage
        .provider_binding()
        .await
        .map_err(StoreObjectError::from)?
        .device;
    if provider != expected_provider {
        return Err(StoreRegistrationError::Invalid(
            "live provider principal differs from the reserved founder authority".to_string(),
        ));
    }
    let registration = StoreDeviceRegistration::signed(
        store_root,
        origin,
        provider,
        store_commits,
        acknowledgements,
        snapshots,
        identity_signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let prepared = prepare_registration_object(storage, &registration, reserved_slot)?;
    Ok((registration, prepared))
}

fn prepare_registration_object(
    storage: &dyn SyncStorage,
    registration: &StoreDeviceRegistration,
    slot: crate::storage::cloud::ObjectSlot,
) -> Result<PreparedExactObject, StoreRegistrationError> {
    let semantic_prefix = slot
        .logical_key()
        .strip_suffix(".json")
        .ok_or_else(|| {
            StoreRegistrationError::Invalid(
                "reserved registration slot has no .json suffix".to_string(),
            )
        })?
        .to_string();
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        registration.store_root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    storage
        .prepare_protocol_object(&context, slot, &semantic_prefix, registration.to_bytes())
        .map_err(StoreObjectError::from)
        .map_err(StoreRegistrationError::from)
}

pub(crate) async fn bootstrap_pending_device(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    identity_signer: &UserKeypair,
    attempt_ref: DeviceJoinAttemptRef,
    verified_attempt: crate::sync::store_objects::VerifiedObject<DeviceJoinAttempt>,
    bootstrap_plan: crate::sync::store::owner::pull::DeviceJoinBootstrapPlan,
    attempt_activation: StoreBatchCommitRef,
    owner: &StoreDeviceRegistration,
    published_at: &str,
) -> Result<DeviceReadinessProof, StoreRegistrationError> {
    if verified_attempt.semantic_hash != attempt_ref.attempt_hash
        || verified_attempt.object != attempt_ref.object
    {
        return Err(StoreRegistrationError::Invalid(
            "verified device join attempt differs from its exact reference".to_string(),
        ));
    }
    let attempt = verified_attempt.value;
    let activation_stream = attempt_activation.coord.stream_id.to_string();
    let verified_activation = bootstrap_plan
        .verified_commit(&attempt_activation)
        .cloned()
        .ok_or_else(|| {
            StoreRegistrationError::Invalid(
                "device join bootstrap omits its attempt activation".to_string(),
            )
        })?;
    Box::pin(database.install_device_join_bootstrap(attempt.store_root.clone(), bootstrap_plan))
        .await
        .map_err(database_error)?;
    if Box::pin(
        database.exact_materialized_ref(&activation_stream, attempt_activation.coord.sequence()),
    )
    .await
    .map_err(database_error)?
    .as_ref()
        != Some(&attempt_activation)
    {
        return Err(StoreRegistrationError::ActivationRequired);
    }
    let activation_commit = verified_activation.value();
    if verified_activation.author() != owner
        || activation_commit.author_registration != attempt.owner_registration
        || !activation_commit
            .device_join_attempt_decisions()
            .iter()
            .any(|decision| {
                matches!(
                    decision,
                    DeviceJoinAttemptDecisionRef::Attempt(reference)
                        if reference == &attempt_ref
                )
            })
        || activation_commit
            .order
            .predecessor_cut()
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
            != attempt.bootstrap_cut
        || activation_commit.membership_state != attempt.membership
    {
        return Err(StoreRegistrationError::Invalid(
            "device join attempt is not activated by the named exact Store commit".to_string(),
        ));
    }
    let provider = Box::pin(storage.provider_binding())
        .await
        .map_err(StoreObjectError::from)?;
    if provider.device != attempt.expected_registration.provider {
        return Err(StoreRegistrationError::Invalid(
            "joiner provider principal differs from the signed device join attempt".to_string(),
        ));
    }
    let expected_registration = attempt.expected_registration.clone();
    if expected_registration.author_pubkey != crate::keys::public_key_hex(identity_signer) {
        return Err(StoreRegistrationError::Invalid(
            "joiner identity differs from the signed device registration request".to_string(),
        ));
    }
    let existing = Box::pin(database.latest_local_store_device_registration())
        .await
        .map_err(database_error)?;
    if let Some(existing) = existing.as_ref() {
        if existing.registration_bytes != expected_registration.to_bytes()
            || existing.prepared.reference().slot() != &attempt.registration_slot
            || existing.initial_ack.value.store_cut != attempt.bootstrap_cut
        {
            return Err(StoreRegistrationError::Invalid(
                "local join journal owns different exact registration bytes".to_string(),
            ));
        }
    } else {
        let registration_prepared = prepare_registration_object(
            storage,
            &expected_registration,
            attempt.registration_slot.clone(),
        )?;
        let registration_ref =
            crate::sync::store_commit::StoreDeviceRegistrationRef::from_registration(
                &expected_registration,
                registration_prepared.reference().clone(),
            );
        let DeviceStreamAnchor::StoreAcknowledgements { first_slot } =
            &expected_registration.acknowledgements
        else {
            return Err(StoreRegistrationError::Invalid(
                "join registration has no acknowledgement anchor".to_string(),
            ));
        };
        let ack_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            attempt.store_root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let next_slot = Box::pin(storage.allocate_protocol_slot(
            &ack_context,
            &ack_slot_prefix(&expected_registration.device_id.to_string(), 2),
            ".json",
        ))
        .await
        .map_err(StoreObjectError::from)?;
        let device_signer = expected_registration
            .device_signer(identity_signer)
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let (device_state, _) =
            Box::pin(database.store_device_state_for_history_cut(&attempt.bootstrap_cut))
                .await
                .map_err(database_error)?;
        let initial_ack = StoreAck::signed(
            attempt.store_root.store_root_hash,
            registration_ref.clone(),
            1,
            attempt.bootstrap_cut.clone(),
            device_state,
            None,
            StoreAckExclusionState {
                proposal_freezes: Vec::new(),
            },
            published_at.to_string(),
            SuccessorLink {
                activation: expected_registration
                    .store_acknowledgement_activation(&registration_ref)
                    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
                    .activation_id(),
                predecessor: None,
                next_slot,
            },
            &device_signer,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let ack_prepared = storage
            .prepare_protocol_object(
                &ack_context,
                first_slot.clone(),
                &ack_slot_prefix(&expected_registration.device_id.to_string(), 1),
                initial_ack.to_bytes(),
            )
            .map_err(StoreObjectError::from)?;
        let initial_ack_ref = StoreAckRef {
            registration: registration_ref,
            sequence: 1,
            ack_hash: initial_ack.ack_hash(),
            object: ack_prepared.reference().clone(),
        };
        Box::pin(database.stage_local_store_device_registration(
            crate::database::ExactProtocolObject {
                value: expected_registration.clone(),
                bytes: expected_registration.to_bytes(),
                object: registration_prepared.reference().clone(),
                prepared: registration_prepared,
            },
            initial_ack_ref,
            crate::database::ExactProtocolObject {
                value: initial_ack.clone(),
                bytes: initial_ack.to_bytes(),
                object: ack_prepared.reference().clone(),
                prepared: ack_prepared,
            },
        ))
        .await
        .map_err(database_error)?;
    }
    Box::pin(super::registration_outbox::drain(database, storage)).await?;
    let durable = Box::pin(database.latest_local_store_device_registration())
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::ActivationRequired)?;
    if !matches!(
        durable.state,
        crate::database::LocalDeviceRegistrationState::Created
            | crate::database::LocalDeviceRegistrationState::Activated { .. }
    ) {
        return Err(StoreRegistrationError::ActivationRequired);
    }
    let registration = StoreDeviceRegistration::parse_at(
        &durable.registration_bytes,
        &attempt.store_root,
        durable.device_id,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let registration_ref = crate::sync::store_commit::StoreDeviceRegistrationRef::from_registration(
        &registration,
        durable.prepared.reference().clone(),
    );
    let device_signer = registration
        .device_signer(identity_signer)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    DeviceReadinessProof::signed(
        attempt_ref,
        registration_ref,
        durable.initial_ack_ref,
        attempt.bootstrap_cut,
        &registration,
        &device_signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))
}

fn database_error(error: crate::database::DbError) -> StoreRegistrationError {
    StoreRegistrationError::Database(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::test_helpers::{open_test_db, TestStore};

    async fn founder_recovery_authority(
        store: &TestStore,
    ) -> crate::sync::restore_code::OwnerRecoveryAuthority {
        let device = store.founder_device().await.expect("load founder Store");
        let protocol_root = device.protocol_root_for_test();
        let owner_grant = protocol_root.descriptor.founder_grant.clone();
        let activation = crate::sync::store_commit::OwnerRecoveryActivationId::derive(
            &store.root,
            &crate::keys::public_key_hex(&store.signer),
            &owner_grant,
            &protocol_root.descriptor.founder_recovery,
        )
        .expect("derive founder recovery activation");
        crate::sync::restore_code::OwnerRecoveryAuthority {
            owner_identity_secret: hex::encode(store.signer.to_keypair_bytes()),
            owner_grant: owner_grant.clone(),
            recovery: crate::sync::store_commit::OwnerRecoveryCursor {
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
        let database = StoreDatabase::new(&db);
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
        let database = StoreDatabase::new(&db);
        let store = TestStore::for_store("registration-missing-root-storage").await;

        assert!(matches!(
            super::super::registration_outbox::drain(&database, &store.storage).await,
            Err(StoreRegistrationError::ExactRootAuthorityMissing)
        ));
    }

    #[tokio::test]
    async fn exact_founder_registration_is_already_activated() {
        let (store, db) = initialized().await;
        let database = StoreDatabase::new(&db);
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
        let database = StoreDatabase::new(&db);
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

        let frontier = crate::sync::store::database::StoreDatabase::new(&db)
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
            let database = StoreDatabase::new(&db);
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
                let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
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
                let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
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
