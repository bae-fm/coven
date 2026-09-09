use super::*;
use coven_protocol::membership::MembershipHeadActivation;
use coven_protocol::store_commit::{
    GrantStreamAnchor, OwnerPromotionAcceptance, StorePublicationPayload,
};

#[tokio::test]
async fn member_accepts_a_request_from_the_installed_snapshot_proof() {
    let fixture = PromotionCandidate::build("retired-request-member-acceptance").await;
    let owner = fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .expect("bind request publisher");
    let request = owner
        .begin_owner_promotion(fixture.member_registration.clone())
        .await
        .expect("publish request and exact result");
    let snapshot = owner
        .publish_snapshot_generation_for_test()
        .await
        .expect("retain the pending request in an accepted snapshot");
    let proof = &snapshot.meta.history_summary.pending_owner_promotions[&request.promotion_id];
    assert_eq!(proof.request().expect("retained request"), &request);
    let member = fixture
        .store
        .bind_device_in(
            &fixture.member_db,
            fixture.member_db_store_dir.clone(),
            &fixture.member,
        )
        .await
        .expect("reopen target without the request in its live history");
    let (_, pulled) = member
        .pull_store()
        .await
        .expect("install accepted request snapshot");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let database = StoreDatabase::new(&fixture.member_db);
    assert!(database
        .installed_replay_baseline()
        .await
        .expect("target baseline")
        .coverage()
        .covers_commit(&proof.publication.value.commit));
    assert!(!database
        .store_publication_entries()
        .await
        .expect("target retained interval")
        .iter()
        .any(|entry| entry.value.payload
            == StorePublicationPayload::Commit(proof.publication.value.commit.clone())));
    let acceptance = member
        .accept_owner_promotion(request)
        .await
        .expect("accept the exact request carried by the verified snapshot");
    assert_eq!(acceptance.activation, *proof.publication.value);
    owner
        .finalize_owner_promotion(&fixture.encryption, acceptance)
        .await
        .expect("finalize using the request's continuing proof");
}

async fn finalization_resumes_after_snapshot(pending_result: bool) {
    let fixture = PromotionCandidate::build("retired-request-finalizer").await;
    let peer_dir = crate::sync::test_helpers::test_store_dir();
    let peer_db = crate::sync::test_helpers::open_test_db(peer_dir.clone());
    let peer = fixture
        .store
        .activate_joined_device(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &peer_db,
            peer_dir,
            &fixture.owner,
            "2026-07-20T00:00:00Z",
        )
        .await
        .expect("activate independent compactor");
    let owner = fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .expect("bind promoter");
    let member = fixture
        .store
        .bind_device_in(
            &fixture.member_db,
            fixture.member_db_store_dir.clone(),
            &fixture.member,
        )
        .await
        .expect("bind target");
    let request = owner
        .begin_owner_promotion(fixture.member_registration.clone())
        .await
        .expect("publish request");
    let acceptance = member
        .accept_owner_promotion(request)
        .await
        .expect("accept request");
    let database = StoreDatabase::new(&fixture.owner_db);
    let mut finalizing =
        Box::pin(owner.finalize_owner_promotion(&fixture.encryption, acceptance.clone()));
    if pending_result {
        let (accepted, release) = fixture.home.pause_next_conditional_replace();
        tokio::select! {
            _ = accepted.notified() => {},
            result = &mut finalizing => panic!("finalizer ended before acceptance: {result:?}"),
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => panic!("finalization did not reach provider acceptance"),
        }
        let (uploaded, _release_result) = fixture.home.pause_after_exact_create_call(1);
        release.notify_one();
        tokio::select! {
            _ = uploaded.notified() => {},
            result = &mut finalizing => panic!("finalizer ended before result upload: {result:?}"),
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => panic!("result did not become visible"),
        }
    } else {
        (&mut finalizing)
            .await
            .expect("complete accepted promotion");
    }
    let journal = database
        .load_owner_promotion_journal(acceptance.request.promotion_id)
        .await
        .expect("durable finalization")
        .expect("promotion remains owned");
    let publication = match &journal.state {
        OwnerPromotionJournalState::MergeHeadPrepared { publication, .. } if pending_result => {
            publication
        }
        OwnerPromotionJournalState::Finalized { receipt, .. } if !pending_result => {
            &receipt.publication
        }
        state => panic!("wrong finalization state: {state:?}"),
    };
    let MembershipHeadActivation::StoreCommit {
        acceptance_slot, ..
    } = &publication.head.activation
    else {
        panic!("promotion is Store-activated");
    };
    assert!(fixture.home.stored_exact_bytes(acceptance_slot).is_some());
    let (_, pulled) = peer
        .pull_store()
        .await
        .expect("peer accepts the finalized authority");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let snapshot = peer
        .publish_snapshot_generation_for_test()
        .await
        .expect("compact finalized promotion");
    assert!(!snapshot
        .meta
        .history_summary
        .pending_owner_promotions
        .contains_key(&journal.promotion_id));
    assert!(snapshot
        .meta
        .coverage
        .covers_commit(&acceptance.activation.commit));
    drop(finalizing);
    let reopened = fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .expect("restart finalizer");
    let (_, pulled) = reopened
        .pull_store()
        .await
        .expect("install compacted authority before resuming");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    assert!(database
        .installed_replay_baseline()
        .await
        .expect("installed baseline")
        .coverage()
        .covers_commit(&acceptance.activation.commit));
    assert!(!database
        .store_publication_entries()
        .await
        .expect("retained interval")
        .iter()
        .any(|entry| entry.value.payload
            == StorePublicationPayload::Commit(acceptance.activation.commit.clone())));
    reopened
        .finalize_owner_promotion(&fixture.encryption, acceptance.clone())
        .await
        .expect("resume the exact durable finalization without retired request history");
    assert!(
        matches!(database.load_owner_promotion_journal(journal.promotion_id).await.expect("completed journal").expect("retained completion").state, OwnerPromotionJournalState::Finalized { acceptance: completed, .. } if completed == acceptance)
    );
    assert!(database
        .active_store_publication()
        .await
        .expect("released reservation")
        .is_none());
}

#[tokio::test]
async fn completed_promotion_retries_after_its_request_is_retired() {
    finalization_resumes_after_snapshot(false).await;
}

#[tokio::test]
async fn pending_promotion_finalization_restarts_after_its_request_is_retired() {
    finalization_resumes_after_snapshot(true).await;
}

#[tokio::test]
async fn completed_promotion_rejects_another_valid_acceptance_at_the_same_id() {
    let fixture = PromotionCandidate::build("terminal-promotion-acceptance-identity").await;
    let owner = fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .expect("bind promoter");
    let member = fixture
        .store
        .bind_device_in(
            &fixture.member_db,
            fixture.member_db_store_dir.clone(),
            &fixture.member,
        )
        .await
        .expect("bind target");
    let request = owner
        .begin_owner_promotion(fixture.member_registration.clone())
        .await
        .expect("publish request");
    let acceptance = member
        .accept_owner_promotion(request)
        .await
        .expect("accept request");
    owner
        .finalize_owner_promotion(&fixture.encryption, acceptance.clone())
        .await
        .expect("complete promotion");
    let database = StoreDatabase::new(&fixture.owner_db);
    let before = database
        .store_current_publication()
        .await
        .expect("accepted promotion");
    let registration = StoreDatabase::new(&fixture.member_db)
        .activated_store_device_registration(fixture.member_registration.clone())
        .await
        .expect("target registration");
    let mut anchors = acceptance.anchors.clone();
    anchors.membership = GrantStreamAnchor::StoreMembership {
        first_slot: coven_protocol::objects::ObjectSlot::opaque(
            anchors.membership.first_slot().logical_key().to_string(),
            "another-valid-promotion-anchor".into(),
        )
        .expect("different exact membership anchor"),
    };
    let substituted = OwnerPromotionAcceptance::signed(
        acceptance.request.as_ref().clone(),
        acceptance.activation.clone(),
        anchors,
        registration.value(),
        &fixture.member,
    )
    .expect("target signs a different structurally valid acceptance");
    substituted
        .verify(registration.value())
        .expect("valid replacement signature");
    assert_ne!(substituted, acceptance);
    let error = owner
        .finalize_owner_promotion(&fixture.encryption, substituted)
        .await
        .expect_err("terminal retry must match the exact persisted acceptance");
    assert!(
        error.to_string().contains("persisted acceptance"),
        "wrong rejection: {error}"
    );
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("unchanged accepted boundary"),
        before
    );
    assert!(
        matches!(database.load_owner_promotion_journal(acceptance.request.promotion_id).await.expect("journal").expect("owned completion").state, OwnerPromotionJournalState::Finalized { acceptance: completed, .. } if completed == acceptance)
    );
}
