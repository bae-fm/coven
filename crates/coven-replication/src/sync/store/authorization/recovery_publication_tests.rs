use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
use coven_database::StoreDatabase;
use coven_keys::keys::UserKeypair;
use coven_protocol::store_commit::StorePublicationPayload;

#[tokio::test]
async fn recovery_preserves_a_prepared_writers_authority_until_its_write_settles() {
    assert_recovery_preserves_prepared_writer(RecoveryState::New).await;
}

#[tokio::test]
async fn accepted_recovery_preserves_a_prepared_writers_authority_until_its_write_settles() {
    assert_recovery_preserves_prepared_writer(RecoveryState::AcceptedElsewhere).await;
}

#[tokio::test]
async fn adopting_the_current_recovery_keeps_its_prepared_write_publishable() {
    assert_recovery_preserves_prepared_writer(RecoveryState::AlreadyCurrent).await;
}

enum RecoveryState {
    New,
    AcceptedElsewhere,
    AlreadyCurrent,
}

async fn assert_recovery_preserves_prepared_writer(recovery_state: RecoveryState) {
    let owner = UserKeypair::generate();
    let store_dir = test_store_dir();
    let database = open_test_db(store_dir.clone());
    let store = TestStore::create(
        &database,
        store_dir.clone(),
        "recovery-with-prepared-write",
        owner.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let authority = store.founder_recovery_authority().await;
    if matches!(recovery_state, RecoveryState::AlreadyCurrent) {
        store
            .bind_device(&database, store_dir.clone(), &owner)
            .await
            .expect("bind founder for initial recovery")
            .owner_recovery_for_test()
            .await
            .expect("authorize initial recovery")
            .recover_owner_device(&authority, None)
            .await
            .expect("activate the writer's recovery registration");
    }
    let device = store
        .bind_device(&database, store_dir.clone(), &owner)
        .await
        .expect("bind original writer");
    database
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('before-recovery', 'original write', NULL, 1, \
                     '0000000001000-0000-owner', '2026-07-21')",
        )
        .await;
    assert!(device.prepare_pending_store_write().await.unwrap());
    let records = StoreDatabase::new(&database);
    let reserved = records.active_store_publication().await.unwrap().unwrap();
    let (_, original_registration, _) = reserved.commit_reservation().unwrap();
    let original_registration = original_registration.clone();
    let StorePublicationPayload::Commit(original_commit) =
        &reserved.attempt().unwrap().entry.payload
    else {
        panic!("a prepared host write owns its exact commit");
    };
    let original_commit = original_commit.clone();
    let original_ack = records.latest_local_store_ack().await.unwrap().unwrap();
    let original_journal = records
        .latest_local_store_device_registration()
        .await
        .unwrap()
        .unwrap();
    let original_pending = records.pending_writes().await.unwrap();
    if matches!(recovery_state, RecoveryState::AcceptedElsewhere) {
        let restored_dir = test_store_dir();
        let restored_database = open_test_db(restored_dir.clone());
        let restored = store
            .open_into(&restored_database, restored_dir)
            .await
            .expect("open another recovery target");
        restored
            .owner_recovery_for_test()
            .await
            .expect("authorize the other recovery target")
            .recover_owner_device(&authority, None)
            .await
            .expect("accept recovery before the original writer adopts it");
    }
    let recovery_result = device
        .owner_recovery_for_test()
        .await
        .expect("authorize recovery")
        .recover_owner_device(&authority, None)
        .await;
    if matches!(recovery_state, RecoveryState::AlreadyCurrent) {
        assert!(
            recovery_result.is_ok(),
            "adopting the same author must preserve its usable prepared write: {recovery_result:?}"
        );
    }

    assert_eq!(
        records.active_store_publication().await.unwrap(),
        Some(reserved),
        "recovery must retain the original candidate"
    );
    assert_eq!(
        records.pending_writes().await.unwrap(),
        original_pending,
        "recovery must retain the unsettled logical write"
    );
    if let Err(error) = &recovery_result {
        let after = records
            .latest_local_store_device_registration()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after.device_id, original_journal.device_id,
            "refused recovery must not replace the prepared writer's registration: {error}"
        );
        assert_eq!(after.registration_hash, original_journal.registration_hash);
        assert_eq!(
            after.registration_bytes,
            original_journal.registration_bytes
        );
        assert_eq!(after.prepared, original_journal.prepared);
        assert_eq!(after.initial_ack_ref, original_journal.initial_ack_ref);
        assert_eq!(after.initial_ack.value, original_journal.initial_ack.value);
        assert_eq!(after.initial_ack.bytes, original_journal.initial_ack.bytes);
        assert_eq!(
            after.initial_ack.prepared,
            original_journal.initial_ack.prepared
        );
        assert_eq!(after.state, original_journal.state);
        assert_eq!(
            records.local_activated_registration_ref().await.unwrap(),
            Some(original_registration.clone())
        );
        let ack_after = records.latest_local_store_ack().await.unwrap().unwrap();
        assert_eq!(ack_after.reference, original_ack.reference);
        assert_eq!(ack_after.successor_slot, original_ack.successor_slot);
        assert_eq!(ack_after.standing, original_ack.standing);
    }
    assert!(records
        .owner_recovery_publication()
        .await
        .unwrap()
        .is_none());

    // Reopen authorization rather than relying on the old in-memory writer.
    let reopened = store
        .bind_device(&database, store_dir.clone(), &owner)
        .await
        .expect("reopen current writer after the recovery attempt");
    assert_eq!(
        reopened
            .drain_store_writes()
            .await
            .expect("the current identity must settle the exact old prepared write"),
        1
    );
    assert_eq!(
        records.published_write_commits().await.unwrap(),
        vec![original_commit.clone()]
    );
    let recovered_registration = match recovery_result {
        Ok(registration) => registration,
        Err(_) => reopened
            .owner_recovery_for_test()
            .await
            .expect("authorize recovery after settling the original write")
            .recover_owner_device(&authority, None)
            .await
            .expect("recover after settling the original write"),
    };
    if matches!(recovery_state, RecoveryState::AlreadyCurrent) {
        assert_eq!(recovered_registration, original_registration);
    } else {
        assert_ne!(recovered_registration, original_registration);
    }
    let recovered = store
        .bind_device(&database, store_dir, &owner)
        .await
        .expect("bind recovered writer");
    recovered.publish_fixture_position("after-recovery").await;

    let writes = records.published_write_commits().await.unwrap();
    assert_eq!(writes.len(), 2, "each logical write is published once");
    assert_eq!(writes[0], original_commit);
    let new_commit = recovered.load_commit_for_test(&writes[1]).await.unwrap();
    assert_eq!(new_commit.author_registration, recovered_registration);
    assert_eq!(
        records
            .store_publication_entries()
            .await
            .unwrap()
            .iter()
            .filter(|entry| {
                entry.value.payload == StorePublicationPayload::Commit(original_commit.clone())
            })
            .count(),
        1,
        "recovery cannot republish the old logical write under another identity"
    );
    assert!(records.pending_writes().await.unwrap().is_empty());
    assert!(records.active_store_publication().await.unwrap().is_none());
}
