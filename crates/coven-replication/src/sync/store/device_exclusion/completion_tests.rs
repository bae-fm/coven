use super::*;
use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
use coven_keys::keys::UserKeypair;
use coven_protocol::membership::MembershipHeadActivation;
use coven_protocol::store_commit::StorePublicationPayload;

enum CompletionContinuation {
    Suspended,
    Restarted,
}

async fn complete_after_snapshot_retirement(continuation: CompletionContinuation) {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let store = TestStore::create(
        &source,
        source_dir.clone(),
        "device-completion-retirement",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let peer_dir = test_store_dir();
    let peer_database = open_test_db(peer_dir.clone());
    let peer = store
        .activate_joined_device(
            &source,
            source_dir.clone(),
            &peer_database,
            peer_dir,
            &signer,
            "2026-09-08T00:00:00Z",
        )
        .await
        .expect("activate peer snapshot publisher");
    let owner = store
        .bind_device_in(&source, source_dir.clone(), &signer)
        .await
        .expect("bind owner");
    let database = StoreDatabase::new(&source);
    let target = database
        .activated_store_device_registration_records()
        .await
        .expect("device registrations")
        .into_iter()
        .map(|record| record.reference().clone())
        .find(|reference| reference.device_id.to_string() != owner.device_id().as_str())
        .expect("peer registration");
    // Proposal, authority entry, head, commit, publication entry, then its
    // acceptance result. Check the exact slot after reaching this provider pause.
    let (reached, release) = store.pause_after_exact_create_call(6);
    let mut publication = Box::pin(owner.propose_device_exclusion(&target));
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        tokio::select! {
            _ = reached.notified() => {},
            result = &mut publication => panic!("publication returned before result upload: {result:?}"),
        }
    })
    .await
    .expect("reach acceptance result before local completion");
    let operation = database
        .active_outbound_store_device_exclusion()
        .await
        .expect("load active operation")
        .expect("accepted proposal still owns journal completion");
    let candidate = operation.candidate().expect("exact candidate");
    let membership = candidate
        .prepared_membership_publication()
        .expect("candidate authority graph");
    let MembershipHeadActivation::StoreCommit {
        acceptance_slot, ..
    } = &membership.head.activation
    else {
        panic!("device authority is Store activated");
    };
    assert_eq!(store.exact_creates().last(), Some(acceptance_slot));
    assert!(!operation.is_completed());
    let (_, pulled) = peer
        .pull_store()
        .await
        .expect("peer installs accepted proposal");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    peer.publish_snapshot_generation_for_test()
        .await
        .expect("peer publishes snapshot covering finalized authority");
    let (_, pulled) = owner
        .pull_store()
        .await
        .expect("install covering snapshot while the original publisher waits");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let baseline = database
        .installed_replay_baseline()
        .await
        .expect("installed snapshot authority");
    assert!(baseline.snapshot().is_some());
    assert!(baseline.coverage().covers_commit(&candidate.reference));
    assert!(!database
        .store_publication_entries()
        .await
        .expect("retired publication interval")
        .iter()
        .any(|entry| entry.value.payload
            == StorePublicationPayload::Commit(candidate.reference.clone())));
    let before = database
        .store_current_publication()
        .await
        .expect("snapshot boundary before completion");
    assert_eq!(
        database
            .active_outbound_store_device_exclusion()
            .await
            .expect("journal remains owned after pull"),
        Some(operation.clone())
    );
    let completed = match continuation {
        CompletionContinuation::Suspended => {
            release.notify_one();
            tokio::time::timeout(std::time::Duration::from_secs(30), &mut publication)
                .await
                .expect("publisher resumes")
                .expect("complete the same accepted device operation under its snapshot evidence")
        }
        CompletionContinuation::Restarted => {
            drop(publication);
            let reopened = store
                .bind_device_in(&source, source_dir, &signer)
                .await
                .expect("reopen the device with accepted finalization still pending");
            let mut writer = reopened
                .authorize_writer()
                .await
                .expect("authorize reopened owner");
            writer
                .device_exclusion()
                .resume()
                .await
                .expect("resume accepted device authority after snapshot retirement")
                .expect("the durable operation remains owned")
        }
    };
    assert!(
        matches!(completed, StoreDeviceExclusionResult::ProposalActivated { commit, .. }
        if commit == candidate.reference)
    );
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("completion preserves snapshot boundary"),
        before
    );
    assert!(database
        .active_outbound_store_device_exclusion()
        .await
        .expect("journal completed")
        .is_none());
    assert!(database
        .active_store_publication()
        .await
        .expect("reservation released")
        .is_none());
}

#[tokio::test]
async fn device_authority_completion_survives_snapshot_retirement_after_receipt_upload() {
    complete_after_snapshot_retirement(CompletionContinuation::Suspended).await;
}

#[tokio::test]
async fn device_authority_completion_restarts_after_snapshot_retirement() {
    complete_after_snapshot_retirement(CompletionContinuation::Restarted).await;
}
