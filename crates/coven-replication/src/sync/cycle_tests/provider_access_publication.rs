use super::*;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_storage::cloud::ConditionalWriteOutcome;

#[tokio::test]
async fn device_join_completes_while_a_later_publication_is_held() {
    assert_device_join_publication_hold(false).await;
}

#[tokio::test]
async fn device_join_completion_waits_for_its_activation_prerequisite() {
    assert_device_join_publication_hold(true).await;
}

async fn assert_device_join_publication_hold(prerequisite: bool) {
    use crate::sync::store::{DeviceJoinError, DeviceJoinRole, StoreError};
    use coven_protocol::store_commit::device_join_journal::{
        DeviceJoinJournalRecord, DeviceJoinRoleProgress, JoinerJoinProgress,
    };

    let OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        cloud_storage,
        member,
    } = cross_principal_owner_and_member().await;
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &member,
        &EncryptionService::from_key([43; 32]),
    )
    .await;
    let observer = storage
        .bind_device_in(&owner_db, owner_db_store_dir, &owner)
        .await
        .expect("bind admitting owner");
    observer
        .ensure_device_join_snapshot_for_test()
        .await
        .expect("publish the join snapshot");
    let pending_directory = tempfile::tempdir().expect("create pending journal directory");
    let pending = crate::sync::store::DeviceJoinJournalDatabase::open_for_test(
        pending_directory.path().join("pending.sqlite"),
    )
    .expect("open pending join journal");
    let peer = storage
        .cross_principal_device_for_test(&member, "joining-account")
        .await
        .expect("bind joining provider principal");
    let offer = observer
        .begin_device_join(&pubkey_hex(&member))
        .await
        .expect("offer the device join");
    let mut pending_join = peer
        .open_pending_device_join(&pending, &member, offer.clone())
        .await
        .expect("open pending join owner");
    let request = pending_join
        .prepare_provider_access_request()
        .await
        .expect("request provider access");
    let approval = peer
        .authorize_device_provider_access(&observer, request)
        .await
        .expect("approve provider access");
    let request = pending_join
        .prepare_registration_request(approval)
        .await
        .expect("request device registration");
    let provisional = observer
        .accept_device_registration_request(request)
        .await
        .expect("accept device registration");
    let provider_ready = observer
        .publish_device_provider_challenge(provisional)
        .await
        .expect("publish the provider challenge");
    drop(pending_join);

    let joining_directory = crate::sync::test_helpers::test_store_dir();
    let tables = crate::sync::test_helpers::test_synced_tables();
    let migrations = crate::sync::test_helpers::test_migrations();
    let restoring = peer
        .install_store_snapshot(
            &joining_directory,
            &offer.store_root,
            &observer.membership().await.expect("read owner membership"),
            &member,
            offer.attempt_id.to_string(),
            SCHEMA_VERSION,
            tables.clone(),
            &migrations,
        )
        .await
        .expect("install the joining snapshot");
    let attempt = offer.attempt_id;
    let mut joining = restoring
        .begin_device_join(&pending, offer)
        .await
        .expect("open snapshot-backed join owner");
    let readiness = joining
        .bootstrap(provider_ready, T0, None)
        .await
        .expect("install bootstrap history and publish readiness");
    let journal_ready = pending
        .load(attempt, DeviceJoinRole::Joiner)
        .expect("read ready journal")
        .expect("the join has a readiness journal");
    assert_eq!(
        journal_ready,
        DeviceJoinJournalRecord {
            attempt_id: attempt,
            progress: Box::new(DeviceJoinRoleProgress::Joiner(JoinerJoinProgress::Ready(
                readiness.clone(),
            ))),
        }
    );
    let completion = observer
        .complete_device_provider_admission(readiness.clone())
        .await
        .expect("complete provider admission");
    let (activation, successor) = if prerequisite {
        observer.publish_fixture_position("join-boundary-row").await;
        let row = observer
            .latest_local_store_position()
            .await
            .expect("read prerequisite position")
            .expect("the row has a Store commit");
        let activation = observer
            .finalize_device_join(completion)
            .await
            .expect("publish activation after the row");
        (activation, row)
    } else {
        let activation = observer
            .finalize_device_join(completion)
            .await
            .expect("publish registration activation");
        observer.publish_fixture_position("join-boundary-row").await;
        let row = observer
            .latest_local_store_position()
            .await
            .expect("read successor position")
            .expect("the row has a Store commit");
        (activation, row)
    };

    // This secondary reader observes the database the snapshot owner opened;
    // it cannot install history or modify the joining device's registration.
    let reader = StoreDatabase::from_database(
        Database::open_read_only(
            &joining_directory.db_path(),
            tables,
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            attempt.to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &migrations,
        )
        .expect("open a secondary reader of the joining database"),
    );
    let publication_before = reader
        .store_current_publication()
        .await
        .expect("read installed bootstrap publication");
    let registration_before = reader
        .latest_local_store_device_registration()
        .await
        .expect("read pending local registration")
        .expect("bootstrap installed the local registration");
    assert!(!registration_before.is_activated());

    let commit = observer
        .load_commit_for_test(&successor)
        .await
        .expect("load the published successor");
    if prerequisite {
        let activation_commit = observer
            .load_commit_for_test(&activation.outcome_activation)
            .await
            .expect("load activation prerequisites");
        assert_eq!(
            activation_commit.order.predecessor.as_ref(),
            Some(&successor)
        );
    } else {
        assert_eq!(
            commit.order.predecessor.as_ref(),
            Some(&activation.outcome_activation)
        );
    }
    let package = commit
        .store_package()
        .expect("the successor carries row data");
    let context = ProtocolObjectContext::store_encrypted(
        storage.root().store_root_hash,
        ProtocolObjectDomain::StorePackage,
    );
    let prefix = coven_protocol::store_commit::package_semantic_prefix(
        commit.candidate_family(),
        &successor.coord.stream_id.to_string(),
        successor.coord.sequence(),
        package.content_hash,
    );
    let (package_bytes, prepared_package) = cloud_storage
        .read_prepared_protocol_slot(&context, package.object.slot(), &prefix)
        .await
        .expect("retain the exact successor package for restoration");
    assert_eq!(prepared_package.reference(), &package.object);
    cloud_storage
        .delete_protocol_object(&package.object)
        .await
        .expect("make the successor package unavailable");
    let accepted = StoreDatabase::new(&owner_db)
        .store_current_publication()
        .await
        .expect("read the publication accepting activation and successor");
    assert_ne!(accepted, publication_before);

    let result = joining.complete(activation.clone()).await;
    if prerequisite {
        let rejection = result.expect_err("join completion waits for its activation prerequisite");
        let DeviceJoinError::Outbound(StoreError::PublicationHeld(held)) = rejection else {
            panic!("join completion failed outside package materialization: {rejection}")
        };
        assert!(held.iter().any(|position| {
            matches!(
                &position.coordinate,
                crate::sync::store::HeldStoreCoordinate::Package { device_id, seq, package_hash }
                    if device_id == &successor.coord.stream_id.to_string()
                        && *seq == successor.coord.sequence()
                        && *package_hash == package.content_hash
            )
        }));
    } else {
        let joined = result.expect("a later unavailable row cannot block the installed activation");
        assert_eq!(joined.activation, activation);
    }
    assert_eq!(
        reader
            .store_current_publication()
            .await
            .expect("read held publication"),
        accepted
    );
    let registration_held = reader
        .latest_local_store_device_registration()
        .await
        .expect("read held local registration")
        .expect("the local registration remains present");
    let mut activated_registration = registration_before.clone();
    activated_registration.state = coven_database::LocalDeviceRegistrationState::Activated {
        authority: coven_protocol::store_commit::StoreDeviceRegistrationActivation::Join {
            attempt_id: attempt,
        },
    };
    assert_registration_matches(
        &registration_held,
        if prerequisite {
            &registration_before
        } else {
            &activated_registration
        },
    );
    assert_eq!(
        reader
            .test_query_optional_text("SELECT id FROM notes WHERE id = 'join-boundary-row'".into())
            .await
            .expect("read held successor row"),
        None
    );
    assert_eq!(
        pending
            .load(attempt, DeviceJoinRole::Joiner)
            .expect("read held join journal"),
        if prerequisite {
            Some(DeviceJoinJournalRecord {
                attempt_id: attempt,
                progress: Box::new(DeviceJoinRoleProgress::Joiner(
                    JoinerJoinProgress::ActivationObserved {
                        readiness,
                        activation: activation.clone(),
                    },
                )),
            })
        } else {
            None
        }
    );

    cloud_storage
        .create_verified_protocol_object(&context, &prepared_package, &prefix, &package_bytes)
        .await
        .expect("restore the exact successor package");
    if !prerequisite {
        let pulled = joining
            .pull_store_history(None)
            .await
            .expect("ordinary pull retries the held row after join completion");
        assert!(pulled.held_positions.is_empty(), "{pulled:?}");
        assert_eq!(
            reader
                .test_query_optional_text(
                    "SELECT id FROM notes WHERE id = 'join-boundary-row'".into()
                )
                .await
                .expect("read the row before repeating join completion"),
            Some("join-boundary-row".to_string())
        );
        assert!(pending
            .records()
            .expect("ordinary pull does not recreate the completed join journal")
            .is_empty());
    }
    let joined = joining
        .complete(activation.clone())
        .await
        .expect("retry the same join owner after restoring the package");
    assert_eq!(joined.activation, activation);
    assert_eq!(
        reader
            .store_current_publication()
            .await
            .expect("read completed publication"),
        accepted
    );
    let registration_after = reader
        .latest_local_store_device_registration()
        .await
        .expect("read activated local registration")
        .expect("the activated registration remains present");
    assert_registration_matches(&registration_after, &activated_registration);
    assert_eq!(
        joined.registration.object,
        registration_after.prepared.reference().clone()
    );
    assert_eq!(
        reader
            .test_query_optional_text("SELECT id FROM notes WHERE id = 'join-boundary-row'".into())
            .await
            .expect("read installed successor row"),
        Some("join-boundary-row".to_string())
    );
    assert!(pending
        .records()
        .expect("read completed join journal")
        .is_empty());
}

fn assert_registration_matches(
    actual: &coven_database::DurableDeviceRegistration,
    expected: &coven_database::DurableDeviceRegistration,
) {
    assert_eq!(actual.device_id, expected.device_id);
    assert_eq!(actual.registration_hash, expected.registration_hash);
    assert_eq!(actual.registration_bytes, expected.registration_bytes);
    assert_eq!(actual.prepared, expected.prepared);
    assert_eq!(actual.initial_ack_ref, expected.initial_ack_ref);
    assert_eq!(actual.initial_ack.value, expected.initial_ack.value);
    assert_eq!(actual.initial_ack.bytes, expected.initial_ack.bytes);
    assert_eq!(actual.initial_ack.prepared, expected.initial_ack.prepared);
    assert_eq!(actual.state, expected.state);
}

#[tokio::test]
async fn provider_approval_requires_its_activation_in_the_observed_publication() {
    let OwnerAndMember {
        owner,
        owner_db,
        owner_db_store_dir,
        storage,
        cloud_storage,
        member,
    } = cross_principal_owner_and_member().await;
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &member,
        &EncryptionService::from_key([43; 32]),
    )
    .await;
    let database = StoreDatabase::new(&owner_db);
    let before = database
        .store_current_publication()
        .await
        .expect("read initial acceptance");
    let directory = tempfile::tempdir().expect("create pending join directory");
    let pending = crate::sync::store::DeviceJoinJournalDatabase::open_for_test(
        directory.path().join("pending.sqlite"),
    )
    .expect("open pending join journal");
    let peer = storage
        .cross_principal_device_for_test(&member, "joining-account")
        .await
        .expect("bind joining provider principal");
    let (mut joiner, owner_device, approval) = prepare_cross_principal_approval(
        &owner_db,
        owner_db_store_dir,
        &storage,
        &owner,
        &member,
        &pending,
        &peer,
    )
    .await;
    let attempt = approval.request.offer.attempt_id;
    let journal_before = pending.status(attempt).expect("read pending join state");
    let accepted = database
        .store_current_publication()
        .await
        .expect("read accepted approval");
    assert_ne!(accepted.record(), before.record());
    let slot = &owner_device
        .protocol_root_for_test()
        .descriptor
        .current_publication_slot;
    let context = ProtocolObjectContext::signed_plaintext(
        storage.root().store_root_hash,
        ProtocolObjectDomain::StoreCurrentPublication,
    );
    let prefix = coven_protocol::store_commit::store_current_publication_semantic_prefix();
    // All signed approval and activation objects remain readable, but the
    // provider serves an authenticated boundary that does not accept them.
    let ConditionalWriteOutcome::Replaced(stale_version) = cloud_storage
        .replace_protocol_record_if_version(
            &context,
            slot,
            prefix,
            accepted
                .require_observed()
                .expect("accepted provider observation")
                .version(),
            before.record().to_bytes(),
        )
        .await
        .expect("serve the earlier authenticated boundary")
    else {
        panic!("test owns the current provider revision")
    };
    let rejection = joiner
        .prepare_registration_request(approval.clone())
        .await
        .expect_err("an authentic activation must also be accepted");
    assert!(
        rejection
            .to_string()
            .contains("absent from current accepted Store history"),
        "{rejection}"
    );
    assert_eq!(
        pending.status(attempt).expect("read preserved join state"),
        journal_before
    );
    let ConditionalWriteOutcome::Replaced(restored_version) = cloud_storage
        .replace_protocol_record_if_version(
            &context,
            slot,
            prefix,
            &stale_version,
            accepted.record().to_bytes(),
        )
        .await
        .expect("restore the accepted boundary")
    else {
        panic!("test owns the current provider revision")
    };
    joiner
        .prepare_registration_request(approval.clone())
        .await
        .expect("the same join can continue once its activation is accepted");

    let journal_accepted = pending.records().expect("read accepted join journal");
    // Reuse the owner that accepted the activation. Its cache cannot establish
    // acceptance when the provider serves a boundary that omits the commit.
    let ConditionalWriteOutcome::Replaced(stale_version) = cloud_storage
        .replace_protocol_record_if_version(
            &context,
            slot,
            prefix,
            &restored_version,
            before.record().to_bytes(),
        )
        .await
        .expect("serve the earlier boundary after acceptance")
    else {
        panic!("test owns the current provider revision")
    };
    let rejection = joiner
        .prepare_registration_request(approval)
        .await
        .expect_err("cached acceptance cannot replace the observed publication");
    assert!(
        rejection
            .to_string()
            .contains("absent from current accepted Store history"),
        "{rejection}"
    );
    assert_eq!(
        pending
            .records()
            .expect("read preserved accepted join journal"),
        journal_accepted
    );
    assert!(matches!(
        cloud_storage
            .replace_protocol_record_if_version(
                &context,
                slot,
                prefix,
                &stale_version,
                accepted.record().to_bytes(),
            )
            .await
            .expect("restore the accepted boundary after cache verification"),
        ConditionalWriteOutcome::Replaced(_)
    ));
}
