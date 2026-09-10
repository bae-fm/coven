use super::*;
use coven_protocol::membership::{MembershipHeadAcceptance, MembershipHeadActivation};
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::owner_promotion_journal::OwnerPromotionJournal;
use coven_protocol::store_commit::{
    StoreCurrentPublicationRecord, StorePublicationPayload, StorePublicationRef,
};
use coven_storage::CloudSyncObjectStorage;

#[derive(Clone, Copy)]
enum InterruptedAcceptance {
    Restart,
    LostResponse,
}

enum SnapshotAuthorityCheck {
    None,
    Publisher,
    Receiver,
}

async fn promotion_finalization_survives(
    interruption: InterruptedAcceptance,
    snapshot_check: SnapshotAuthorityCheck,
) {
    let (fixture, storage) =
        PromotionCandidate::build_with_connection("promotion-finalization-interruption").await;
    let peer = if !matches!(snapshot_check, SnapshotAuthorityCheck::None) {
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
            .expect("activate independent snapshot publisher");
        Some((peer, peer_db))
    } else {
        None
    };
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
        .expect("bind promotion target");
    let request = owner
        .begin_owner_promotion(fixture.member_registration.clone())
        .await
        .expect("publish promotion request");
    let acceptance = member
        .accept_owner_promotion(request)
        .await
        .expect("accept request");
    let database = StoreDatabase::new(&fixture.owner_db);
    let before = database
        .store_current_publication()
        .await
        .expect("local accepted boundary");
    let (reached, release) = fixture.home.pause_next_conditional_replace();
    if matches!(interruption, InterruptedAcceptance::LostResponse) {
        fixture.home.lose_next_conditional_replace_response();
    }
    let mut finalize =
        Box::pin(owner.finalize_owner_promotion(&fixture.encryption, acceptance.clone()));
    tokio::select! {
        _ = reached.notified() => {},
        result = &mut finalize => panic!("finalization ended before provider acceptance: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => panic!("publication did not reach conditional replacement"),
    }
    let journal = database
        .load_owner_promotion_journal(acceptance.request.promotion_id)
        .await
        .expect("load interrupted journal")
        .expect("journal remains owned");
    let OwnerPromotionJournalState::MergeHeadPrepared { candidate, .. } = &journal.state else {
        panic!(
            "accepted transition must retain finalization work: {:?}",
            journal.state
        );
    };
    let publication = candidate
        .prepared_membership_publication()
        .expect("prepared exact publication");
    let MembershipHeadActivation::StoreCommit {
        acceptance_slot, ..
    } = &publication.head.activation
    else {
        panic!("promotion has a Store activation");
    };
    assert!(
        fixture.home.stored_exact_bytes(acceptance_slot).is_none(),
        "receipt cannot precede acceptance settlement"
    );
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("unmaterialized local boundary"),
        before
    );
    let active = database
        .active_store_publication()
        .await
        .expect("active attempt")
        .expect("accepted operation retains reservation");
    let current_context = ProtocolObjectContext::signed_plaintext(
        fixture.store.root().store_root_hash,
        ProtocolObjectDomain::StoreCurrentPublication,
    );
    let remote = storage
        .read_versioned_protocol_record(
            &current_context,
            &owner
                .protocol_root_for_test()
                .descriptor
                .current_publication_slot,
            coven_protocol::store_commit::store_current_publication_semantic_prefix(),
        )
        .await
        .expect("read actual accepted provider current");
    let current: StoreCurrentPublicationRecord =
        coven_protocol::objects::decode_protocol_object(&remote.0)
            .expect("decode accepted current");
    assert_eq!(
        current,
        active.attempt().expect("prepared attempt").replacement
    );
    assert_eq!(
        active.attempt().expect("prepared attempt").entry.payload,
        StorePublicationPayload::Commit(candidate.reference.clone())
    );
    let accepted_ref = active
        .attempt()
        .expect("prepared attempt")
        .reference()
        .expect("winning exact publication");
    let mut cold_history = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
        .open_pinned(storage.as_ref(), &fixture.store.root())
        .await
        .expect("open rooted verifier while finalization is pending");
    let error = cold_history
        .load_current_accepted_snapshot()
        .await
        .expect_err("accepted authority cannot omit its post-publication result");
    assert!(
        error
            .to_string()
            .contains("awaiting publication finalization"),
        "wrong authority rejection: {error}"
    );
    if let Some((peer, peer_db)) = &peer {
        let (_, pulled) = peer
            .pull_store()
            .await
            .expect("live accepted interval independently authenticates the promotion");
        assert!(pulled.held_positions.is_empty(), "{pulled:?}");
        let error = peer
            .publish_snapshot_generation_for_test()
            .await
            .expect_err("compaction cannot pass an unfinished authority result");
        assert!(
            error
                .to_string()
                .contains("awaiting publication finalization"),
            "{error}"
        );
        let (after, _) = storage
            .read_versioned_protocol_record(
                &current_context,
                &owner
                    .protocol_root_for_test()
                    .descriptor
                    .current_publication_slot,
                coven_protocol::store_commit::store_current_publication_semantic_prefix(),
            )
            .await
            .expect("read boundary after refused compaction");
        let after: StoreCurrentPublicationRecord =
            coven_protocol::objects::decode_protocol_object(&after).expect("accepted boundary");
        assert_eq!(
            after, current,
            "refused compaction cannot replace the accepted interval"
        );
        if matches!(snapshot_check, SnapshotAuthorityCheck::Receiver) {
            let pending = StoreDatabase::new(peer_db)
                .outbound_snapshot_publication()
                .await
                .expect("read refused snapshot candidate")
                .expect("candidate retains its exact uploaded snapshot");
            let attempt = &pending.publication;
            let context = ProtocolObjectContext::signed_plaintext(
                fixture.store.root().store_root_hash,
                ProtocolObjectDomain::StorePublicationEntry,
            );
            let prefix = coven_protocol::store_commit::store_publication_entry_semantic_prefix(
                &attempt.entry,
            );
            let prepared = storage
                .prepare_protocol_object(
                    &context,
                    attempt.entry_object.slot().clone(),
                    &prefix,
                    attempt.entry.to_bytes(),
                )
                .expect("prepare the signed candidate entry at its reserved slot");
            assert_eq!(prepared.reference(), &attempt.entry_object);
            storage
                .create_protocol_object(&prepared)
                .await
                .expect("serve signed snapshot entry");
            let changed = storage
                .replace_protocol_record_if_version(
                    &current_context,
                    &owner
                        .protocol_root_for_test()
                        .descriptor
                        .current_publication_slot,
                    coven_protocol::store_commit::store_current_publication_semantic_prefix(),
                    &remote.1,
                    attempt.replacement.to_bytes(),
                )
                .await
                .expect("provider accepts candidate record without running recipient checks");
            assert!(matches!(
                changed,
                coven_storage::cloud::ConditionalWriteOutcome::Replaced(_)
            ));
            let baseline_before = database
                .installed_replay_baseline()
                .await
                .expect("receiver baseline");
            let observed_before = database
                .store_current_publication()
                .await
                .expect("receiver observation");
            let error = owner
                .pull_store()
                .await
                .expect_err("receiver must reject compaction through unfinished authority");
            assert!(
                error
                    .to_string()
                    .contains("awaiting publication finalization"),
                "{error}"
            );
            let baseline_after = database
                .installed_replay_baseline()
                .await
                .expect("unchanged baseline");
            assert_eq!(baseline_after.snapshot(), baseline_before.snapshot());
            assert_eq!(baseline_after.coverage(), baseline_before.coverage());
            assert_eq!(
                baseline_after.covered_states().collect::<Vec<_>>(),
                baseline_before.covered_states().collect::<Vec<_>>(),
            );
            assert_eq!(
                database
                    .store_current_publication()
                    .await
                    .expect("unchanged observation"),
                observed_before
            );
            assert!(matches!(
                database
                    .load_owner_promotion_journal(journal.promotion_id)
                    .await
                    .expect("owned journal")
                    .expect("unfinished promotion")
                    .state,
                OwnerPromotionJournalState::MergeHeadPrepared { .. }
            ));
            return;
        }
    }
    let deleted_before = fixture.home.deletes_seen();
    match interruption {
        InterruptedAcceptance::Restart => {
            drop(finalize);
            let reopened = fixture
                .store
                .bind_device_in(
                    &fixture.owner_db,
                    fixture.owner_db_store_dir.clone(),
                    &fixture.owner,
                )
                .await
                .expect("reopen promoter with unfinished finalization");
            reopened
                .finalize_owner_promotion(&fixture.encryption, acceptance.clone())
                .await
                .expect("settle accepted transition and resume finalization");
        }
        InterruptedAcceptance::LostResponse => {
            release.notify_one();
            finalize
                .await
                .expect("settle lost conditional response before finalization");
        }
    }
    let result_context = ProtocolObjectContext::signed_plaintext(
        fixture.store.root().store_root_hash,
        ProtocolObjectDomain::StoreMembershipHeadAcceptance,
    );
    let prefix = coven_protocol::membership::membership_head_acceptance_semantic_prefix(
        &publication.head_ref.coord,
    );
    let (bytes, object) = storage
        .read_protocol_slot(&result_context, acceptance_slot, &prefix)
        .await
        .expect("read exact finalization result");
    let result: MembershipHeadAcceptance = coven_protocol::objects::decode_protocol_object(&bytes)
        .expect("decode finalization result");
    assert_eq!(result.head, publication.head_ref);
    assert_eq!(
        result.publication().expect("accepted publication"),
        &accepted_ref
    );
    let retained = fixture
        .owner_db
        .remote_object_for_test(object)
        .await
        .expect("accepted result retains ownership");
    assert!(retained.records_verified_upload());
    let completed = database
        .load_owner_promotion_journal(journal.promotion_id)
        .await
        .expect("completed journal")
        .expect("finalized receipt remains durable");
    assert!(matches!(
        completed.state,
        OwnerPromotionJournalState::Finalized { .. }
    ));
    assert!(database
        .active_store_publication()
        .await
        .expect("completed reservation")
        .is_none());
    let entries = database
        .store_publication_entries()
        .await
        .expect("accepted entries");
    let matches = entries
        .iter()
        .filter(|entry| {
            entry.value.payload == StorePublicationPayload::Commit(candidate.reference.clone())
        })
        .collect::<Vec<_>>();
    assert_eq!(matches.len(), 1, "logical promotion is accepted once");
    assert_eq!(
        StorePublicationRef::from_entry(&matches[0].value, matches[0].prepared.reference().clone())
            .expect("accepted entry ref"),
        accepted_ref
    );
    assert_eq!(
        fixture.home.deletes_seen(),
        deleted_before,
        "unfinished authority cannot be reclaimed"
    );
}

#[tokio::test]
async fn accepted_owner_promotion_finalization_resumes_after_restart() {
    promotion_finalization_survives(InterruptedAcceptance::Restart, SnapshotAuthorityCheck::None)
        .await;
}

#[tokio::test]
async fn accepted_owner_promotion_finalization_settles_a_lost_response() {
    promotion_finalization_survives(
        InterruptedAcceptance::LostResponse,
        SnapshotAuthorityCheck::None,
    )
    .await;
}

#[tokio::test]
async fn accepted_promotion_allows_live_replay_but_blocks_compaction_until_finalized() {
    promotion_finalization_survives(
        InterruptedAcceptance::LostResponse,
        SnapshotAuthorityCheck::Publisher,
    )
    .await;
}

enum RequestSnapshotCheck {
    None,
    RefuseUnfinalized,
    Restart,
}

async fn request_keeps_winning_publication(snapshot_check: RequestSnapshotCheck) {
    let fixture = PromotionCandidate::build("promotion-request-winning-envelope").await;
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
        .expect("activate independent Owner contender");
    let owner = fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .expect("bind promoter");
    fixture.home.fail_exact_create_before_call(1);
    owner
        .begin_owner_promotion(fixture.member_registration.clone())
        .await
        .expect_err("hold request before upload");
    let database = StoreDatabase::new(&fixture.owner_db);
    let journal = database
        .load_owner_promotion_target(target_key(&fixture.member_registration).expect("target key"))
        .await
        .expect("request journal")
        .expect("prepared request");
    let OwnerPromotionJournalState::RequestPrepared { request, candidate } = &journal.state else {
        panic!("request must be prepared");
    };
    peer_db.execute_test_host_write("INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES ('promotion-contender', 'peer title', 1, '0000000001000-0000-peer', '2026-07-20')").await;
    let mut peer_writer = peer.authorize_writer().await.expect("authorize contender");
    assert!(peer_writer
        .prepare_pending_store_write()
        .await
        .expect("prepare competitor"));
    assert_eq!(
        peer_writer
            .drain_store_writes()
            .await
            .expect("publish competitor"),
        1
    );
    drop(peer_writer);
    let covered_winner = if !matches!(snapshot_check, RequestSnapshotCheck::None) {
        let (reached, release) = fixture.home.pause_next_conditional_replace();
        let mut publishing =
            Box::pin(owner.begin_owner_promotion(fixture.member_registration.clone()));
        tokio::select! {
            _ = reached.notified() => {},
            result = &mut publishing => panic!("request ended before accepted replacement: {result:?}"),
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => panic!("request did not reach provider acceptance"),
        }
        let active = database
            .active_store_publication()
            .await
            .expect("accepted request reservation")
            .expect("request retains its winning attempt");
        let winning = active
            .attempt()
            .expect("prepared request attempt")
            .reference()
            .expect("actual accepted request envelope");
        assert_ne!(
            winning,
            candidate.publication.reference().expect("losing envelope")
        );
        if matches!(snapshot_check, RequestSnapshotCheck::Restart) {
            let (result_reached, _result_release) = fixture.home.pause_after_exact_create_call(1);
            release.notify_one();
            tokio::select! {
                _ = result_reached.notified() => {},
                result = &mut publishing => panic!("request ended before its result upload: {result:?}"),
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => panic!("request result was not uploaded"),
            }
            assert!(
                fixture
                    .home
                    .stored_exact_bytes(&request.publication_slot)
                    .is_some(),
                "pause must observe this request's exact publication result"
            );
            assert!(matches!(
                database
                    .load_owner_promotion_journal(journal.promotion_id)
                    .await
                    .expect("result journal")
                    .expect("owned result")
                    .state,
                OwnerPromotionJournalState::RequestAccepted { .. }
            ));
        }
        let (_, pulled) = peer
            .pull_store()
            .await
            .expect("peer installs accepted request");
        assert!(pulled.held_positions.is_empty(), "{pulled:?}");
        if matches!(snapshot_check, RequestSnapshotCheck::RefuseUnfinalized) {
            let peer_database = StoreDatabase::new(&peer_db);
            let before = peer_database
                .store_current_publication()
                .await
                .expect("accepted request boundary");
            let error = peer
                .publish_snapshot_generation_for_test()
                .await
                .expect_err("a request without its publication result cannot be retired");
            assert!(
                error
                    .to_string()
                    .contains("awaiting publication finalization"),
                "wrong request retirement rejection: {error}"
            );
            assert_eq!(
                peer_database
                    .store_current_publication()
                    .await
                    .expect("unchanged boundary"),
                before,
            );
            assert_eq!(
                database
                    .active_store_publication()
                    .await
                    .expect("retained request reservation"),
                Some(active),
            );
            assert!(matches!(
                database
                    .load_owner_promotion_journal(journal.promotion_id)
                    .await
                    .expect("request journal")
                    .expect("owned request")
                    .state,
                OwnerPromotionJournalState::RequestPrepared { .. }
            ));
            return;
        }
        peer.publish_snapshot_generation_for_test()
            .await
            .expect("peer compacts accepted request");
        let peer_database = StoreDatabase::new(&peer_db);
        let observed = peer_database
            .store_current_publication()
            .await
            .expect("accepted covering snapshot");
        assert!(peer_database
            .installed_replay_baseline()
            .await
            .expect("peer installed baseline")
            .coverage()
            .covers_commit(&candidate.reference));
        assert!(!peer_database
            .store_publication_entries()
            .await
            .expect("compacted entries")
            .iter()
            .any(|entry| entry.value.payload
                == StorePublicationPayload::Commit(candidate.reference.clone())));
        drop(publishing);
        let reopened = fixture
            .store
            .bind_device_in(
                &fixture.owner_db,
                fixture.owner_db_store_dir.clone(),
                &fixture.owner,
            )
            .await
            .expect("reopen request publisher after retirement");
        let (_, pulled) = reopened
            .pull_store()
            .await
            .expect("install the covering snapshot before resuming the result journal");
        assert!(pulled.held_positions.is_empty(), "{pulled:?}");
        assert!(database
            .installed_replay_baseline()
            .await
            .expect("restarted publisher baseline")
            .coverage()
            .covers_commit(&candidate.reference));
        assert!(!database
            .store_publication_entries()
            .await
            .expect("retired local publication evidence")
            .iter()
            .any(|entry| entry.value.payload
                == StorePublicationPayload::Commit(candidate.reference.clone())));
        reopened
            .begin_owner_promotion(fixture.member_registration.clone())
            .await
            .expect("retain accepted request proof through restart and compaction");
        assert_eq!(
            database
                .store_current_publication()
                .await
                .expect("current boundary"),
            observed
        );
        Some(winning)
    } else {
        owner
            .begin_owner_promotion(fixture.member_registration.clone())
            .await
            .expect("replace request envelope through actual publisher");
        None
    };
    let completed = database
        .load_owner_promotion_journal(journal.promotion_id)
        .await
        .expect("completed request")
        .expect("request remains durable");
    let OwnerPromotionJournalState::AwaitingAcceptance { activation, .. } = completed.state else {
        panic!("request must await target acceptance");
    };
    assert_eq!(activation.commit, candidate.reference);
    let accepted_ref = match covered_winner {
        Some(winning) => winning,
        None => {
            let entries = database
                .store_publication_entries()
                .await
                .expect("accepted history");
            let accepted = entries
                .iter()
                .find(|entry| {
                    entry.value.payload
                        == StorePublicationPayload::Commit(candidate.reference.clone())
                })
                .expect("accepted exact request");
            StorePublicationRef::from_entry(&accepted.value, accepted.prepared.reference().clone())
                .expect("winning envelope")
        }
    };
    assert_ne!(
        accepted_ref,
        candidate
            .publication
            .reference()
            .expect("original losing envelope")
    );
    assert_eq!(activation.publication, accepted_ref);
    assert!(database
        .active_store_publication()
        .await
        .expect("completed reservation")
        .is_none());
}

#[tokio::test]
async fn promotion_request_records_its_replaced_winning_publication() {
    request_keeps_winning_publication(RequestSnapshotCheck::None).await;
}

#[tokio::test]
async fn promotion_request_restarts_after_its_winning_publication_is_compacted() {
    request_keeps_winning_publication(RequestSnapshotCheck::Restart).await;
}

#[tokio::test]
async fn promotion_request_cannot_be_retired_before_its_publication_result() {
    request_keeps_winning_publication(RequestSnapshotCheck::RefuseUnfinalized).await;
}

#[tokio::test]
async fn received_snapshot_cannot_retire_unfinalized_authority() {
    promotion_finalization_survives(
        InterruptedAcceptance::LostResponse,
        SnapshotAuthorityCheck::Receiver,
    )
    .await;
}

#[tokio::test]
async fn promotion_request_journal_requires_accepted_publication_evidence() {
    let fixture = PromotionCandidate::build("promotion-request-requires-acceptance").await;
    let owner = fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .expect("bind request author");
    fixture.home.fail_exact_create_before_call(1);
    owner
        .begin_owner_promotion(fixture.member_registration.clone())
        .await
        .expect_err("retain unpublished request");
    let database = StoreDatabase::new(&fixture.owner_db);
    let journal = database
        .load_owner_promotion_target(target_key(&fixture.member_registration).expect("target key"))
        .await
        .expect("read request journal")
        .expect("prepared request");
    let before = database
        .store_current_publication()
        .await
        .expect("accepted boundary");
    let active = database
        .active_store_publication()
        .await
        .expect("owned reservation");
    let (previous, state) = journal
        .into_predecessor()
        .expect("exact journal predecessor");
    let OwnerPromotionJournalState::RequestPrepared { request, candidate } = state else {
        panic!("request remains prepared");
    };
    let author = database
        .activated_store_device_registration(request.promoter_registration.clone())
        .await
        .expect("actual promoter registration");
    let signer = author
        .value()
        .device_signer(&fixture.owner)
        .expect("actual device signer");
    let value = coven_protocol::store_commit::OwnerPromotionRequestPublication::signed(
        &candidate.commit,
        &candidate.publication.entry,
        &candidate
            .publication
            .reference()
            .expect("unaccepted envelope"),
        author.value(),
        &signer,
    )
    .expect("valid signature cannot manufacture provider acceptance");
    let bytes = value.to_bytes();
    let publication = coven_protocol::store_commit::RetainedOwnerPromotionRequestPublication {
        value,
        object: ExactObjectRef::new(
            request.publication_slot.clone(),
            bytes.len() as u64,
            ObjectHash::digest(&bytes),
        ),
    };
    let next = OwnerPromotionJournal {
        promotion_id: previous.promotion_id,
        target: previous.target.clone(),
        state: OwnerPromotionJournalState::RequestAccepted {
            request,
            candidate,
            publication,
        },
    };
    database
        .advance_owner_promotion_journal(
            previous
                .transition_to(&next)
                .expect("structurally valid request transition"),
        )
        .await
        .expect_err("journal cannot claim a prepared candidate was accepted");
    assert_eq!(
        database
            .store_current_publication()
            .await
            .expect("unchanged boundary"),
        before
    );
    assert_eq!(
        database
            .active_store_publication()
            .await
            .expect("unchanged reservation"),
        active
    );
    assert!(matches!(
        database
            .load_owner_promotion_journal(previous.promotion_id)
            .await
            .expect("preserved journal")
            .expect("owned request")
            .state,
        OwnerPromotionJournalState::RequestPrepared { .. }
    ));
}
