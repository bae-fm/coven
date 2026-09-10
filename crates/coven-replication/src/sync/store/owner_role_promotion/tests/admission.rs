use super::*;
use coven_protocol::membership::MemberRole;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::prepared_commit::PreparedStoreOperationCommit;
use coven_protocol::store_commit::*;
use coven_storage::CloudSyncObjectStorage;

#[tokio::test]
async fn a_staged_admission_reprepares_its_keyring_after_an_accepted_rotation() {
    staged_admission(AdmissionContinuation::Rotation).await;
}

#[tokio::test]
async fn a_staged_admission_finishes_when_a_peer_admits_the_same_member() {
    staged_admission(AdmissionContinuation::SameMember).await;
}

#[tokio::test]
async fn a_staged_admission_releases_its_abandoned_intent_when_a_peer_assigns_another_role() {
    staged_admission(AdmissionContinuation::DifferentRole).await;
}

#[tokio::test]
async fn a_staged_admission_releases_its_abandoned_intent_when_a_peer_assigns_another_email() {
    staged_admission(AdmissionContinuation::DifferentProviderEmail).await;
}

#[tokio::test]
async fn a_reprepared_admission_restores_access_revoked_after_its_original_provider_step() {
    staged_admission(AdmissionContinuation::AdmitThenRemove).await;
}

#[tokio::test]
async fn rejected_admission_completion_refuses_incomplete_candidate_cleanup() {
    staged_admission_with_guard(
        AdmissionContinuation::DifferentProviderEmail,
        Some(AdmissionCompletionGuard::IncompleteCleanup),
    )
    .await;
}

#[tokio::test]
async fn rejected_admission_completion_refuses_a_matching_accepted_grant() {
    staged_admission_with_guard(
        AdmissionContinuation::SameMember,
        Some(AdmissionCompletionGuard::MatchingGrant),
    )
    .await;
}

#[tokio::test]
async fn rejected_admission_completion_refuses_a_stale_accepted_boundary() {
    staged_admission_with_guard(
        AdmissionContinuation::DifferentProviderEmail,
        Some(AdmissionCompletionGuard::StaleBoundary),
    )
    .await;
}

#[tokio::test]
async fn a_staged_admission_ends_without_authoring_after_its_issuer_is_removed() {
    staged_admission(AdmissionContinuation::IssuerRemoved).await;
}

#[tokio::test]
async fn a_removed_admission_issuer_resumes_exact_cleanup_after_restart() {
    staged_admission(AdmissionContinuation::IssuerRemovedDuringCleanup).await;
}

#[tokio::test]
async fn issuer_retirement_cannot_nonactivate_an_already_accepted_admission() {
    staged_admission(AdmissionContinuation::AcceptedBeforeIssuerRemoval).await;
}

#[tokio::test]
async fn issuer_retirement_finishes_an_interrupted_admission_abandonment() {
    staged_admission(AdmissionContinuation::IssuerRemovedDuringAbandonment).await;
}

#[tokio::test]
async fn accepted_abandonment_retains_the_original_request_through_cleanup_restart() {
    staged_admission(AdmissionContinuation::IssuerRemovedDuringAbandonmentCleanup).await;
}

#[tokio::test]
async fn issuer_retirement_settles_an_abandonment_accepted_before_a_lost_response() {
    staged_admission(AdmissionContinuation::IssuerRemovedAfterAbandonmentAccepted).await;
}

#[derive(Clone, Copy)]
enum AdmissionCompletionGuard {
    IncompleteCleanup,
    MatchingGrant,
    StaleBoundary,
}

enum AdmissionContinuation {
    Rotation,
    SameMember,
    DifferentRole,
    DifferentProviderEmail,
    AdmitThenRemove,
    IssuerRemoved,
    IssuerRemovedDuringCleanup,
    AcceptedBeforeIssuerRemoval,
    IssuerRemovedDuringAbandonment,
    IssuerRemovedDuringAbandonmentCleanup,
    IssuerRemovedAfterAbandonmentAccepted,
}

async fn staged_admission(continuation: AdmissionContinuation) {
    staged_admission_with_guard(continuation, None).await;
}

async fn staged_admission_with_guard(
    continuation: AdmissionContinuation,
    completion_guard: Option<AdmissionCompletionGuard>,
) {
    Box::pin(async {
        let (fixture, storage) =
            PromotionCandidate::build_with_connection("admission-after-peer-rotation").await;
        fixture
            .store
            .promote_active_member_fixture(
                &fixture.owner_db,
                fixture.owner_db_store_dir.clone(),
                &fixture.member_db,
                fixture.member_db_store_dir.clone(),
                &fixture.owner,
                &fixture.member,
                &fixture.encryption,
            )
            .await
            .unwrap();
        let mut founder = fixture
            .store
            .bind_device_in(
                &fixture.owner_db,
                fixture.owner_db_store_dir.clone(),
                &fixture.owner,
            )
            .await
            .unwrap();
        let peer_storage =
            std::sync::Arc::new(storage.connection_for_test_identity(fixture.member.clone()));
        let peer = crate::sync::test_helpers::TestDevice::load_with_database(
            StoreDatabase::new(&fixture.member_db),
            peer_storage.clone(),
            fixture.member.clone(),
            fixture.member_db_store_dir.clone(),
        )
        .await
        .unwrap();
        let root = fixture.store.root();
        let retired = UserKeypair::generate();
        founder
            .admit_member(
                &keys::public_key_hex(&retired),
                None,
                MemberRole::Member,
                &fixture.encryption,
                &root.store_root_id.to_string(),
                "Retired member",
            )
            .await
            .unwrap();
        let admitted = UserKeypair::generate();
        let admitted_pubkey = keys::public_key_hex(&admitted);
        let requested_email = match continuation {
            AdmissionContinuation::DifferentProviderEmail | AdmissionContinuation::AdmitThenRemove => {
                Some("original@example.com")
            }
            _ => None,
        };
        let issuer_removed = matches!(continuation,
            AdmissionContinuation::IssuerRemoved | AdmissionContinuation::IssuerRemovedDuringCleanup
            | AdmissionContinuation::AcceptedBeforeIssuerRemoval | AdmissionContinuation::IssuerRemovedDuringAbandonment
            | AdmissionContinuation::IssuerRemovedDuringAbandonmentCleanup
            | AdmissionContinuation::IssuerRemovedAfterAbandonmentAccepted);
        fixture.home.fail_exact_create_before_call(if issuer_removed { 3 } else { 1 });
        founder
            .admit_member(
                &admitted_pubkey,
                requested_email,
                MemberRole::Member,
                &fixture.encryption,
                &root.store_root_id.to_string(),
                "Pending admission",
            )
            .await
            .expect_err("retain admission after provider access but before acceptance");
        let mut database = StoreDatabase::new(&fixture.owner_db);
        let pending = database
            .outbound_membership_mutation()
            .await
            .unwrap()
            .unwrap();
        let envelope: serde_json::Value = serde_json::from_slice(&pending.plan_bytes).unwrap();
        let original: PreparedStoreOperationCommit =
            serde_json::from_value(envelope["plan"]["candidate"].clone()).unwrap();
        let old_wrap: coven_protocol::wrapped_store_key::PreparedWrappedStoreKey =
            serde_json::from_value(envelope["plan"]["wrapped_key"].clone()).unwrap();
        if issuer_removed {
            assert_eq!(storage.observe_exact_slot(old_wrap.reference.object.slot()).await.unwrap(),
                Some(old_wrap.reference.object.clone()),
                "the failed admission must leave an actual uploaded wrapped key to retire");
            let mut history = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
                .open_pinned(storage.as_ref(), &root).await.unwrap();
            let candidate = history.authenticate_bytes(&original.reference, &original.commit.to_bytes()).await.unwrap();
            assert!(history.candidate_grant_retirement(&database, &candidate).await.unwrap().is_none(),
                "an active issuer's unaccepted candidate has no grant-retirement authority");
            assert_eq!(database.outbound_membership_mutation().await.unwrap().unwrap().plan_bytes, pending.plan_bytes);
        }
        if matches!(continuation, AdmissionContinuation::AcceptedBeforeIssuerRemoval) {
            founder.admit_member(&admitted_pubkey, requested_email, MemberRole::Member,
                &fixture.encryption, &root.store_root_id.to_string(), "Pending admission").await.unwrap();
            assert!(database.outbound_membership_mutation().await.unwrap().is_none());
            assert!(database.materialized_frontier().await.unwrap().values().any(|accepted|
                accepted.coord.stream_id == original.reference.coord.stream_id
                    && accepted.coord.sequence >= original.reference.coord.sequence));
        }
        let mut interrupted_abandonment = None;
        let peer_admission = match continuation {
            AdmissionContinuation::IssuerRemovedDuringAbandonment
            | AdmissionContinuation::IssuerRemovedDuringAbandonmentCleanup
            | AdmissionContinuation::IssuerRemovedAfterAbandonmentAccepted => {
                peer.remove_member(&keys::public_key_hex(&retired), &fixture.encryption,
                    &crate::sync::test_helpers::TestCustody::default(), peer_storage.as_ref(), peer_storage.as_ref()).await.unwrap();
                if matches!(continuation, AdmissionContinuation::IssuerRemovedAfterAbandonmentAccepted) {
                    let (accepted, _release) = fixture.home.pause_next_conditional_replace();
                    {
                        let store_id = root.store_root_id.to_string();
                        let admission = founder.admit_member(&admitted_pubkey, requested_email, MemberRole::Member,
                            &fixture.encryption, &store_id, "Pending admission");
                        tokio::pin!(admission);
                        tokio::select! {
                            result = &mut admission => panic!("abandonment must pause after acceptance: {result:?}"),
                            _ = accepted.notified() => {}
                        }
                    }
                } else {
                    // The original wrap, entry, head, and commit precede the
                    // abandonment commit; stop at its publication entry.
                    fixture.home.fail_exact_create_before_call(6);
                    let error = founder.admit_member(&admitted_pubkey, requested_email, MemberRole::Member,
                        &fixture.encryption, &root.store_root_id.to_string(), "Pending admission").await
                        .expect_err("interrupt abandonment after its commit upload but before acceptance");
                    assert!(error.to_string().contains("forced failure before exact create call 6"), "{error}");
                }
                let active = database.active_store_publication().await.unwrap().unwrap();
                let abandonment = active.membership_abandonment().expect("the retained request owns its exact abandonment").clone();
                assert!(storage.observe_exact_slot(abandonment.reference.object.slot()).await.unwrap().is_some(),
                    "the interrupted abandonment must leave actual candidate bytes");
                assert!(abandonment.commit.membership_authority.is_none(),
                    "abandonment uses device authority rather than the original membership grant");
                assert_eq!(database.outbound_membership_mutation().await.unwrap().unwrap().plan_bytes, pending.plan_bytes);
                interrupted_abandonment = Some(abandonment);
                peer.remove_member(&keys::public_key_hex(&fixture.owner), &fixture.encryption,
                    &crate::sync::test_helpers::TestCustody::default(), peer_storage.as_ref(), peer_storage.as_ref()).await.unwrap();
                None
            }
            AdmissionContinuation::AdmitThenRemove => {
                peer.admit_member(
                    &admitted_pubkey, requested_email, MemberRole::Member,
                    &fixture.encryption, &root.store_root_id.to_string(), "Peer admission",
                ).await.unwrap();
                peer.remove_member(
                    &admitted_pubkey, &fixture.encryption,
                    &crate::sync::test_helpers::TestCustody::default(),
                    peer_storage.as_ref(), peer_storage.as_ref(),
                ).await.unwrap();
                assert!(matches!(fixture.home.access_requests().last(),
                    Some(coven_storage::cloud::CloudAccessState::Absent { member_pubkey, .. })
                        if member_pubkey == &admitted_pubkey));
                None
            }
            AdmissionContinuation::Rotation | AdmissionContinuation::IssuerRemoved
            | AdmissionContinuation::IssuerRemovedDuringCleanup
            | AdmissionContinuation::AcceptedBeforeIssuerRemoval => {
                let custody = crate::sync::test_helpers::TestCustody::default();
                peer.remove_member(
                    &keys::public_key_hex(match continuation {
                        AdmissionContinuation::IssuerRemoved | AdmissionContinuation::IssuerRemovedDuringCleanup
                        | AdmissionContinuation::AcceptedBeforeIssuerRemoval => &fixture.owner,
                        _ => &retired,
                    }),
                    &fixture.encryption,
                    &custody,
                    peer_storage.as_ref(),
                    peer_storage.as_ref(),
                )
                .await
                .unwrap();
                None
            }
            AdmissionContinuation::SameMember
            | AdmissionContinuation::DifferentRole
            | AdmissionContinuation::DifferentProviderEmail => {
                let role = match continuation {
                    AdmissionContinuation::SameMember
                    | AdmissionContinuation::DifferentProviderEmail => MemberRole::Member,
                    AdmissionContinuation::DifferentRole => MemberRole::Follower,
                    AdmissionContinuation::Rotation | AdmissionContinuation::AdmitThenRemove
                    | AdmissionContinuation::IssuerRemoved | AdmissionContinuation::IssuerRemovedDuringCleanup
                    | AdmissionContinuation::AcceptedBeforeIssuerRemoval | AdmissionContinuation::IssuerRemovedDuringAbandonment
                    | AdmissionContinuation::IssuerRemovedDuringAbandonmentCleanup
                    | AdmissionContinuation::IssuerRemovedAfterAbandonmentAccepted => unreachable!(),
                };
                let email = match continuation {
                    AdmissionContinuation::DifferentProviderEmail => Some("accepted@example.com"),
                    _ => None,
                };
                Some(
                    peer.admit_member(
                        &admitted_pubkey,
                        email,
                        role,
                        &fixture.encryption,
                        &root.store_root_id.to_string(),
                        "Peer admission",
                    )
                    .await
                    .unwrap(),
                )
            }
        };
        if matches!(continuation, AdmissionContinuation::AcceptedBeforeIssuerRemoval) {
            founder.pull_store_with_encryption(&fixture.encryption).await.unwrap();
            let before = database.store_current_publication().await.unwrap();
            let mut history = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
                .open_pinned(storage.as_ref(), &root).await.unwrap();
            let candidate = history.authenticate_bytes(&original.reference, &original.commit.to_bytes()).await.unwrap();
            assert!(history.candidate_grant_retirement(&database, &candidate).await.unwrap().is_none(),
                "issuer retirement cannot invalidate an already covered candidate sequence");
            assert_eq!(database.store_current_publication().await.unwrap(), before);
            assert!(storage.observe_exact_slot(original.reference.object.slot()).await.unwrap().is_some());
            assert!(storage.observe_exact_slot(old_wrap.reference.object.slot()).await.unwrap().is_some());
            assert!(peer.membership_for_test().await.unwrap().is_member_now(&admitted_pubkey));
            return;
        }
        if completion_guard.is_some() {
            fixture.home.fail_nth_exact_delete_of(
                &[old_wrap.reference.object.slot()],
                1,
            );
        }
        let accepted_before_retry = StoreDatabase::new(&fixture.member_db)
            .store_current_publication().await.unwrap().record().clone();
        if matches!(continuation, AdmissionContinuation::IssuerRemovedDuringCleanup
            | AdmissionContinuation::IssuerRemovedDuringAbandonmentCleanup) {
            fixture.home.fail_nth_exact_delete_of(&[old_wrap.reference.object.slot()], 1);
        }
        fixture.home.clear_exact_creates();
        let mut access_requests_before_retry = fixture.home.access_requests().len();
        let mut result = founder
            .admit_member(
                &admitted_pubkey,
                requested_email,
                MemberRole::Member,
                &fixture.encryption,
                &root.store_root_id.to_string(),
                "Pending admission",
            )
            .await;
        if matches!(continuation, AdmissionContinuation::IssuerRemovedDuringCleanup
            | AdmissionContinuation::IssuerRemovedDuringAbandonmentCleanup) {
            let error = result.expect_err("interrupt exact retirement cleanup");
            assert!(error.to_string().contains("forced exact delete failure"), "{error}");
            let retained = database.outbound_membership_mutation().await.unwrap().unwrap();
            assert_eq!(retained.plan_bytes, pending.plan_bytes);
            assert_eq!(retained.progress_bytes, pending.progress_bytes);
            let active = database.active_store_publication().await.unwrap().unwrap();
            assert!(active.is_awaiting_preparation());
            let mut expected_coord = original.reference.coord.clone();
            if let Some(abandonment) = &interrupted_abandonment {
                expected_coord.sequence += 1;
                assert!(matches!(active.retired_candidates()[0].nonactivation.proof(),
                    coven_protocol::remote_object::CandidateNonactivationProof::AcceptedAbandonment { abandonment: accepted }
                        if accepted.object == abandonment.reference.object));
                assert!(storage.observe_exact_slot(abandonment.reference.object.slot()).await.unwrap().is_some());
            } else {
                assert!(matches!(active.retired_candidates()[0].nonactivation.proof(),
                    coven_protocol::remote_object::CandidateNonactivationProof::AuthorityRetirement { .. }));
            }
            assert_eq!(active.commit_reservation().unwrap().2, &expected_coord);
            assert_eq!(active.retired_candidates().len(), 1);
            assert!(storage.observe_exact_slot(old_wrap.reference.object.slot()).await.unwrap().is_some());
            if interrupted_abandonment.is_none() {
                assert!(fixture.home.exact_creates().is_empty());
            }
            let directory = crate::sync::test_helpers::test_store_dir();
            fixture.owner_db.vacuum_into_for_test(directory.db_path().to_string_lossy().into_owned()).await.unwrap();
            crate::sync::test_helpers::copy_payload_files(&fixture.owner_db_store_dir, &directory);
            let reopened = coven_database::Database::open_synthetic_for_test(
                &directory.db_path(), directory.clone(), crate::sync::test_helpers::test_synced_tables(),
                coven_protocol::blob::BLOB_TOMBSTONE_GRACE, coven_protocol::blob::TransferLimits::one_at_a_time(),
                "test-device".into(), std::sync::Arc::new(coven_foundation::clock::SystemClock),
                &crate::sync::test_helpers::test_migrations(),
            ).expect("reopen the retained admission from its database and payload files");
            founder = fixture.store.bind_device_in(&reopened, directory, &fixture.owner).await.unwrap();
            database = StoreDatabase::new(&reopened);
            result = founder.admit_member(&admitted_pubkey, requested_email, MemberRole::Member,
                &fixture.encryption, &root.store_root_id.to_string(), "Pending admission").await;
        }
        if issuer_removed {
            let error = result.expect_err("a removed issuer cannot admit another member");
            assert!(matches!(&error, crate::sync::store::MembershipOpsError::Mutation(
                crate::sync::store::MembershipMutationError::InitiatingAuthorityRetired)), "{error}");
            assert!(database.outbound_membership_mutation().await.unwrap().is_none(),
                "authenticated issuer retirement must release the retained admission: {error}");
            assert!(database.active_store_publication().await.unwrap().is_none(),
                "authenticated issuer retirement must release the author reservation");
            if let Some(abandonment) = &interrupted_abandonment {
                let entries = database.store_publication_entries().await.unwrap();
                let accepted = entries.iter().find(|entry| matches!(&entry.value.payload,
                    StorePublicationPayload::Commit(candidate) if candidate == &abandonment.reference))
                    .expect("the exact previously authored abandonment is accepted");
                let allowed = [abandonment.reference.object.slot(), abandonment.publication.entry_object.slot(),
                    accepted.prepared.reference().slot()];
                assert!(fixture.home.exact_creates().iter().all(|slot| allowed.contains(&slot)),
                    "only the retained abandonment and its publication may be uploaded");
                if matches!(continuation, AdmissionContinuation::IssuerRemovedAfterAbandonmentAccepted) {
                    assert_eq!(database.store_current_publication().await.unwrap().record(), &accepted_before_retry);
                    assert!(fixture.home.exact_creates().is_empty(),
                        "already accepted abandonment settles without any upload");
                }
            } else {
                assert_eq!(database.store_current_publication().await.unwrap().record(), &accepted_before_retry,
                    "retirement must not publish a new abandonment after the issuer lost authority");
                assert!(fixture.home.exact_creates().is_empty(),
                    "retirement must not author another candidate or membership object");
            }
            assert_eq!(fixture.home.access_requests().len(), access_requests_before_retry,
                "the removed issuer must not change provider access");
            for object in [&original.reference.object, &old_wrap.reference.object] {
                assert!(storage.observe_exact_slot(object.slot()).await.unwrap().is_none(),
                    "retirement must finish the exact candidate cleanup");
            }
            if let Some(abandonment) = interrupted_abandonment {
                assert!(storage.observe_exact_slot(abandonment.reference.object.slot()).await.unwrap().is_some(),
                    "the accepted abandonment remains part of shared history");
            }
            assert!(storage.observe_exact_slot(original.publication.entry_object.slot()).await.unwrap().is_none());
            let membership = peer.membership_for_test().await.unwrap();
            assert!(!membership.is_member_now(&admitted_pubkey));
            assert!(!membership.is_member_now(&keys::public_key_hex(&fixture.owner)));
            return;
        }
        if let Some(guard) = completion_guard {
            let error = result.expect_err("interrupt the original candidate's physical cleanup");
            assert!(
                error.to_string().contains("forced exact delete failure"),
                "expected the targeted provider failure, got {error}"
            );
            let retained = database.outbound_membership_mutation().await.unwrap().unwrap();
            assert_eq!(retained.intent_hash, pending.intent_hash);
            assert_eq!(retained.plan_bytes, pending.plan_bytes);
            assert_eq!(retained.progress_bytes, pending.progress_bytes);
            let active = database.active_store_publication().await.unwrap().unwrap();
            assert!(active.is_awaiting_preparation());
            assert_eq!(active.retired_candidates().len(), 1);
            assert!(storage
                .observe_exact_slot(old_wrap.reference.object.slot())
                .await.unwrap().is_some());
            let membership = founder.membership_for_test().await.unwrap();
            let accepted_wrap = peer_admission.as_ref().unwrap().wrapped_key.clone();
            assert_eq!(
                membership.wrapped_key_authority_for(&admitted_pubkey).unwrap(),
                vec![accepted_wrap.clone()]
            );
            let accepted = database.store_current_publication().await.unwrap().record().clone();
            if !matches!(guard, AdmissionCompletionGuard::IncompleteCleanup) {
                crate::sync::store::authorization::retire_store_write_candidates(
                    &database, storage.as_ref(), active.clone(),
                ).await.unwrap();
                assert!(database.retired_store_write_cleanup(active.clone()).await.unwrap().is_empty());
            }
            if matches!(guard, AdmissionCompletionGuard::StaleBoundary) {
                let another = UserKeypair::generate();
                peer.admit_member(
                    &keys::public_key_hex(&another), None, MemberRole::Member,
                    &fixture.encryption, &root.store_root_id.to_string(), "Another accepted member",
                ).await.unwrap();
                founder.pull_store_with_encryption(&fixture.encryption).await.unwrap();
                assert_ne!(database.store_current_publication().await.unwrap().record(), &accepted);
            }
            let error = database.complete_rejected_membership_admission(
                retained.intent_hash, active.clone(), accepted, membership,
            ).await.expect_err("terminal rejection must prove all of its conditions");
            let expected = match guard {
                AdmissionCompletionGuard::IncompleteCleanup => "membership candidate cleanup is incomplete",
                AdmissionCompletionGuard::MatchingGrant => "retained membership admission is not rejected by an accepted grant",
                AdmissionCompletionGuard::StaleBoundary => "accepted membership changed before completing the retained request",
            };
            assert!(matches!(&error, coven_database::DbError::Message(message) if message == expected),
                "expected {expected}, got {error}");
            let preserved = database.outbound_membership_mutation().await.unwrap().unwrap();
            assert_eq!(preserved.intent_hash, retained.intent_hash);
            assert_eq!(preserved.plan_bytes, retained.plan_bytes);
            assert_eq!(preserved.progress_bytes, retained.progress_bytes);
            assert_eq!(database.active_store_publication().await.unwrap().as_ref(), Some(&active));
            assert_eq!(
                founder.membership_for_test().await.unwrap()
                    .wrapped_key_authority_for(&admitted_pubkey).unwrap(),
                vec![accepted_wrap]
            );
            if matches!(guard, AdmissionCompletionGuard::IncompleteCleanup) {
                assert!(storage.observe_exact_slot(old_wrap.reference.object.slot()).await.unwrap().is_some());
                crate::sync::store::authorization::retire_store_write_candidates(
                    &database, storage.as_ref(), active,
                ).await.unwrap();
            }
            access_requests_before_retry = fixture.home.access_requests().len();
            result = founder.admit_member(
                &admitted_pubkey, requested_email, MemberRole::Member,
                &fixture.encryption, &root.store_root_id.to_string(), "Pending admission",
            ).await;
        }
        if matches!(
            continuation,
            AdmissionContinuation::DifferentRole | AdmissionContinuation::DifferentProviderEmail
        ) {
            let error = result
                .expect_err("the original Member request must not demote or replace a peer grant");
            assert!(
                matches!(
                    error,
                    crate::sync::store::MembershipOpsError::ExistingMemberMismatch
                ),
                "actual conflicting admission: {error}"
            );
            assert!(
                database
                    .outbound_membership_mutation()
                    .await
                    .unwrap()
                    .is_none(),
                "the proven abandoned conflicting request must release its journal"
            );
            assert!(
                database.active_store_publication().await.unwrap().is_none(),
                "the failed logical request must release its author reservation"
            );
            let membership = founder.membership_for_test().await.unwrap();
            let (expected_role, expected_email) = match continuation {
                AdmissionContinuation::DifferentRole => (MemberRole::Follower, None),
                AdmissionContinuation::DifferentProviderEmail => {
                    (MemberRole::Member, Some("accepted@example.com"))
                }
                _ => unreachable!(),
            };
            assert!(membership
                .current_members()
                .contains(&(admitted_pubkey.clone(), expected_role)));
            assert_eq!(
                membership.current_member_provider_email(&admitted_pubkey),
                expected_email
            );
            if matches!(continuation, AdmissionContinuation::DifferentProviderEmail) {
                assert_eq!(
                    &fixture.home.access_requests()[access_requests_before_retry..],
                    &[coven_storage::cloud::CloudAccessState::Present {
                        member_pubkey: admitted_pubkey.clone(),
                        provider_account_email: expected_email.map(str::to_string),
                    }],
                    "rejection preserves accepted access without reapplying the obsolete email or revoking the member"
                );
            }
            assert_eq!(
                membership
                    .wrapped_key_authority_for(&admitted_pubkey)
                    .unwrap(),
                vec![peer_admission.unwrap().wrapped_key]
            );
            for object in [&original.reference.object, &old_wrap.reference.object] {
                assert!(storage
                    .observe_exact_slot(object.slot())
                    .await
                    .unwrap()
                    .is_none());
            }
            return;
        }
        let admission = result.expect("resume the retained admission against accepted authority");
        if matches!(continuation, AdmissionContinuation::SameMember) {
            assert_eq!(admission.wrapped_key, peer_admission.unwrap().wrapped_key);
            assert!(database
                .outbound_membership_mutation()
                .await
                .unwrap()
                .is_none());
            assert!(database.active_store_publication().await.unwrap().is_none());
            assert!(storage
                .observe_exact_slot(original.reference.object.slot())
                .await
                .unwrap()
                .is_none());
            return;
        }
        assert!(admission.wrapped_key.generation > old_wrap.reference.generation);
        assert_eq!(
            &fixture.home.access_requests()[access_requests_before_retry..],
            &[coven_storage::cloud::CloudAccessState::Present {
                member_pubkey: admitted_pubkey.clone(),
                provider_account_email: requested_email.map(str::to_string),
            }],
            "replacement establishes its own provider step exactly once"
        );
        assert_ne!(admission.wrapped_key, old_wrap.reference);
        assert!(database
            .outbound_membership_mutation()
            .await
            .unwrap()
            .is_none());
        assert!(database.active_store_publication().await.unwrap().is_none());
        assert!(storage
            .observe_exact_slot(original.reference.object.slot())
            .await
            .unwrap()
            .is_none());
        let accepted = database.store_publication_entries().await.unwrap();
        let replacement = accepted
            .iter()
            .rev()
            .find_map(|entry| match &entry.value.payload {
                StorePublicationPayload::Commit(reference)
                    if reference.coord.stream_id == original.reference.coord.stream_id =>
                {
                    Some(reference)
                }
                _ => None,
            })
            .unwrap();
        assert!(replacement.coord.sequence > original.reference.coord.sequence);
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreCommit,
        );
        let prefix = semantic_prefix_from_exact_object(&replacement.object, ".json").unwrap();
        let bytes = storage
            .read_protocol_object(&context, &replacement.object, &prefix)
            .await
            .unwrap();
        let commit: StoreBatchCommit =
            coven_protocol::objects::decode_protocol_object(&bytes).unwrap();
        assert_eq!(commit.write_id, original.commit.write_id);
        assert!(!accepted.iter().any(|entry| matches!(&entry.value.payload,
            StorePublicationPayload::Commit(reference) if reference == &original.reference)));
        let membership = crate::sync::store::HistoryConstructionAuthority::admission()
            .open_pinned(storage.as_ref(), &root)
            .await
            .unwrap()
            .load_accepted_anchored_membership(
                &admission.membership_floor.0,
                Some(&admission.owner_pubkey),
            )
            .await
            .unwrap();
        if matches!(continuation, AdmissionContinuation::Rotation) {
            assert!(!membership.is_member_now(&keys::public_key_hex(&retired)));
        }
        let keys =
            crate::sync::store::authorization::StoreKeyrings::new(storage.as_ref(), root.clone());
        let received = keys
            .open_containing(&admitted, &membership, &admission.wrapped_key)
            .await
            .unwrap();
        let owner_wraps = membership
            .wrapped_key_authority_for(&keys::public_key_hex(&fixture.owner))
            .unwrap();
        let current = keys
            .open_containing(
                &fixture.owner,
                &membership,
                owner_wraps
                    .iter()
                    .max_by_key(|reference| reference.generation)
                    .unwrap(),
            )
            .await
            .unwrap();
        let ciphertext =
            current.encrypt(b"content after the accepted rotation", b"admission keyring");
        assert_eq!(
            received.decrypt(&ciphertext, b"admission keyring").unwrap(),
            b"content after the accepted rotation"
        );
    })
    .await;
}
