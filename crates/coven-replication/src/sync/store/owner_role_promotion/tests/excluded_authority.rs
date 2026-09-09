use super::*;
use coven_protocol::membership::{MemberRole, MembershipHeadAcceptance, MembershipHeadActivation};
use coven_protocol::membership_mutation::PreparedMembershipPublication;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::prepared_commit::PreparedStoreOperationCommit;
use coven_protocol::store_commit::*;
use coven_storage::CloudSyncObjectStorage;

#[tokio::test]
async fn accepted_member_retirement_survives_a_prepared_removal_of_its_issuer() {
    accepted_member_retirement(RetirementContinuation::Publish).await;
}

#[tokio::test]
async fn accepted_member_retirement_survives_interrupted_abandonment_cleanup() {
    accepted_member_retirement(RetirementContinuation::InterruptedCleanup).await;
}

#[tokio::test]
async fn cold_authority_rejects_a_stale_removal_with_a_signed_winning_receipt() {
    accepted_member_retirement(RetirementContinuation::ForgedReceipt).await;
}

#[tokio::test]
async fn an_abandoned_removal_finishes_when_the_accepted_peer_removal_satisfies_it() {
    accepted_member_retirement(RetirementContinuation::Satisfied).await;
}

#[tokio::test]
async fn cleaned_abandonment_cannot_finish_an_unsatisfied_removal() {
    accepted_member_retirement(RetirementContinuation::RejectUnsatisfiedCompletion).await;
}

#[tokio::test]
async fn satisfied_removal_releases_its_rotation_without_a_caller_generation() {
    accepted_member_retirement(RetirementContinuation::RotationCleanup).await;
}

enum RetirementContinuation {
    Publish,
    InterruptedCleanup,
    ForgedReceipt,
    Satisfied,
    RejectUnsatisfiedCompletion,
    RotationCleanup,
}

async fn accepted_member_retirement(continuation: RetirementContinuation) {
    Box::pin(async {
        let (fixture, storage) =
            PromotionCandidate::build_with_connection("accepted-member-retirement-permanence")
                .await;
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
            .expect("activate the second Owner");
        let founder = fixture
            .store
            .bind_device_in(
                &fixture.owner_db,
                fixture.owner_db_store_dir.clone(),
                &fixture.owner,
            )
            .await
            .expect("bind founder");
        let member_storage = std::sync::Arc::new(
            storage.connection_for_test_identity(fixture.member.clone()),
        );
        let retiring_owner = crate::sync::test_helpers::TestDevice::load_with_database(
            StoreDatabase::new(&fixture.member_db),
            member_storage.clone(),
            fixture.member.clone(),
            fixture.member_db_store_dir.clone(),
        )
            .await
            .expect("bind the second Owner");
        let third = UserKeypair::generate();
        let third_pubkey = keys::public_key_hex(&third);
        let root = fixture.store.root();
        founder
            .admit_member(
                &third_pubkey,
                None,
                MemberRole::Member,
                &fixture.encryption,
                &root.store_root_id.to_string(),
                "Member retirement",
            )
            .await
            .expect("accept the third principal's Member grant");
        let creation = founder
            .membership_for_test()
            .await
            .expect("read the accepted Member grant")
            .write_grant_authority(&third_pubkey)
            .expect("the third principal has an exact write grant");
        let revokee = if matches!(continuation, RetirementContinuation::Satisfied | RetirementContinuation::RotationCleanup) {
            third_pubkey.clone()
        } else {
            keys::public_key_hex(&fixture.member)
        };
        let owner_database = StoreDatabase::new(&fixture.owner_db);
        let custody = crate::sync::test_helpers::TestCustody::default();
        fixture.home.fail_exact_create_before_call(1);
        fixture
            .store
            .remove_member(
                &fixture.owner_db,
                fixture.owner_db_store_dir.clone(),
                &fixture.owner,
                &revokee,
                &fixture.encryption,
                &custody,
            )
            .await
            .expect_err("retain the Owner removal before its first upload");
        let staged = owner_database
            .outbound_membership_mutation()
            .await
            .unwrap()
            .expect("Owner removal remains durable");
        retiring_owner
            .remove_member(
                &third_pubkey,
                &fixture.encryption,
                &custody,
                member_storage.as_ref(),
                member_storage.as_ref(),
            )
            .await
            .expect("the still-active Owner accepts the Member retirement");
        let retired = retiring_owner
            .membership_for_test()
            .await
            .expect("read the accepted Member retirement");
        assert!(!retired.authorizes_write_authority(&creation, &third_pubkey));
        assert!(retired
            .write_authority_retirement(&creation, &third_pubkey)
            .is_some());
        let accepted_retirement = StoreDatabase::new(&fixture.member_db)
            .store_current_publication()
            .await
            .unwrap();
        assert_eq!(
            owner_database
                .outbound_membership_mutation()
                .await
                .unwrap()
                .unwrap()
                .plan_bytes,
            staged.plan_bytes,
            "the Owner removal retains its earlier retirement frontier"
        );
        let durable: serde_json::Value = serde_json::from_slice(&staged.plan_bytes).unwrap();
        let original: PreparedStoreOperationCommit = serde_json::from_value(
            durable["plan"]["publication"]["candidate"].clone(),
        ).expect("actual retained removal candidate");
        let publication: PreparedMembershipPublication = serde_json::from_value(
            durable["plan"]["publication"]["publication"].clone(),
        ).expect("actual retained removal head");
        if matches!(continuation, RetirementContinuation::ForgedReceipt) {
            let registration = founder.latest_local_store_device_registration().await.unwrap().unwrap();
            let registration: StoreDeviceRegistration = serde_json::from_slice(&registration.registration_bytes).unwrap();
            let signer = registration.device_signer(&fixture.owner).unwrap();
            let verified = VerifiedStoreBatchCommit::parse_prepared(
                &original.commit.to_bytes(), root.store_root_hash, original.reference.coord.clone(),
                original.reference.object.clone(), &registration,
            ).unwrap();
            let entry = StorePublicationEntry::signed_commit(accepted_retirement.record(), &verified, &signer).unwrap();
            let context = ProtocolObjectContext::signed_plaintext(root.store_root_hash, ProtocolObjectDomain::StorePublicationEntry);
            let prefix = store_publication_entry_semantic_prefix(&entry);
            let slot = storage.allocate_protocol_slot(&context, &prefix, ".json").await.unwrap();
            let object = storage.prepare_protocol_object(&context, slot, &prefix, entry.to_bytes()).unwrap();
            let reference = StorePublicationRef::from_entry(&entry, object.reference().clone()).unwrap();
            let current = StoreCurrentPublicationRecord::advance_commit(
                accepted_retirement.record(), &entry, reference, &verified, &signer,
            ).unwrap();
            let result = MembershipHeadAcceptance::signed(
                root.store_root_hash, publication.head_ref.clone(), &publication.head,
                &entry, &current, coven_protocol::membership::MembershipFloor(retired.head_refs().to_vec()),
                &registration, &signer,
            ).unwrap();
            result.verify_for(root.store_root_hash, &publication.head_ref, &publication.head, &registration).unwrap();
            for prepared in [publication.prepared_entry().unwrap(), publication.prepared_head().unwrap()] {
                storage.create_protocol_object(&prepared).await.unwrap();
            }
            let MembershipHeadActivation::StoreCommit { acceptance_slot, .. } = &publication.head.activation else { panic!("Store-backed removal"); };
            let context = ProtocolObjectContext::signed_plaintext(root.store_root_hash, ProtocolObjectDomain::StoreMembershipHeadAcceptance);
            let prefix = coven_protocol::membership::membership_head_acceptance_semantic_prefix(&publication.head_ref.coord);
            let prepared = storage.prepare_protocol_object(&context, acceptance_slot.clone(), &prefix, result.to_bytes()).unwrap();
            storage.create_protocol_object(&prepared).await.unwrap();
            let error = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
                .open_pinned(storage.as_ref(), &root).await.unwrap()
                .load_accepted_anchored_membership(&[], Some(&keys::public_key_hex(&fixture.owner))).await
                .expect_err("valid signatures cannot accept a stale grant-state transition");
            assert!(error.to_string().contains("prepared against different accepted authority"), "reject stale authority: {error}");
            assert_eq!(StoreDatabase::new(&fixture.member_db).store_current_publication().await.unwrap(), accepted_retirement);
            return;
        }
        let (completion_db, completion_dir) = if matches!(continuation, RetirementContinuation::InterruptedCleanup | RetirementContinuation::RejectUnsatisfiedCompletion | RetirementContinuation::RotationCleanup) {
            fixture.home.fail_nth_exact_delete_of(&[publication.head_ref.object.slot()], 1);
            fixture.store.remove_member(
                &fixture.owner_db, fixture.owner_db_store_dir.clone(), &fixture.owner,
                &revokee, &fixture.encryption, &custody,
            ).await.expect_err("fail exact old-head cleanup after accepted abandonment");
            let active = owner_database.active_store_publication().await.unwrap().unwrap();
            assert!(active.is_awaiting_preparation());
            assert_eq!(active.commit_reservation().unwrap().0, &original.commit.write_id);
            assert_eq!(active.retired_candidates().len(), 1);
            assert_eq!(owner_database.outbound_membership_mutation().await.unwrap().unwrap().plan_bytes, staged.plan_bytes);
            assert_eq!(storage.observe_exact_slot(publication.head_ref.object.slot()).await.unwrap(), Some(publication.head_ref.object.clone()));
            if matches!(continuation, RetirementContinuation::RejectUnsatisfiedCompletion | RetirementContinuation::RotationCleanup) {
                crate::sync::store::authorization::retire_store_write_candidates(
                    &owner_database, storage.as_ref(), active,
                ).await.expect("finish exact abandoned object cleanup");
                let awaiting = owner_database.active_store_publication().await.unwrap().unwrap();
                assert_eq!(awaiting.retired_candidates().len(), 1, "original proof survives absence marking");
                assert!(storage.observe_exact_slot(publication.head_ref.object.slot()).await.unwrap().is_none());
                let accepted = owner_database.store_current_publication().await.unwrap();
                let membership = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
                    .open_pinned(storage.as_ref(), &root).await.unwrap()
                    .load_accepted_anchored_membership(&[], Some(&keys::public_key_hex(&fixture.owner))).await.unwrap();
                let rotation = owner_database.load_rotation_gate().await.unwrap();
                let journal = owner_database.outbound_membership_mutation().await.unwrap().unwrap();
                if matches!(continuation, RetirementContinuation::RotationCleanup) {
                    assert!(!membership.is_member_now(&revokee));
                    assert!(matches!(rotation, Some(coven_protocol::objects::RotationGate::Local(_)
                        | coven_protocol::objects::RotationGate::LocalAndPeer { .. })));
                    owner_database.complete_satisfied_membership_mutation(
                        staged.intent_hash, awaiting, accepted.record().clone(), membership,
                    ).await.expect("complete the accepted removal from its retained request");
                    assert!(owner_database.outbound_membership_mutation().await.unwrap().is_none());
                    assert!(owner_database.active_store_publication().await.unwrap().is_none());
                    assert!(matches!(owner_database.load_rotation_gate().await.unwrap(),
                        None | Some(coven_protocol::objects::RotationGate::Peer { .. })),
                        "completed removal must release its own rotation candidate");
                    return;
                }
                assert!(membership.is_owner_now(&revokee));
                let error = owner_database.complete_satisfied_membership_mutation(
                    staged.intent_hash, awaiting.clone(), accepted.record().clone(), membership,
                ).await.expect_err("active target cannot satisfy a cleaned abandonment");
                assert!(error.to_string().contains("retained membership removal is not satisfied"), "reject the actual target: {error}");
                assert_eq!(owner_database.active_store_publication().await.unwrap(), Some(awaiting));
                assert_eq!(owner_database.load_rotation_gate().await.unwrap(), rotation);
                let unchanged = owner_database.outbound_membership_mutation().await.unwrap().unwrap();
                assert_eq!(unchanged.intent_hash, journal.intent_hash);
                assert_eq!(unchanged.plan_bytes, journal.plan_bytes);
                assert_eq!(unchanged.progress_bytes, journal.progress_bytes);
            }
            let directory = crate::sync::test_helpers::test_store_dir();
            fixture.owner_db.vacuum_into_for_test(directory.db_path().to_string_lossy().into_owned()).await.unwrap();
            crate::sync::test_helpers::copy_payload_files(&fixture.owner_db_store_dir, &directory);
            let database = coven_database::Database::open_synthetic_for_test(
                &directory.db_path(), directory.clone(), crate::sync::test_helpers::test_synced_tables(),
                coven_protocol::blob::BLOB_TOMBSTONE_GRACE, coven_protocol::blob::TransferLimits::one_at_a_time(),
                "test-device".into(), std::sync::Arc::new(coven_foundation::clock::SystemClock),
                &crate::sync::test_helpers::test_migrations(),
            ).expect("restore writer from its durable journal and payloads");
            (database, directory)
        } else {
            (fixture.owner_db.clone(), fixture.owner_db_store_dir.clone())
        };
        fixture.store.remove_member(
            &completion_db, completion_dir, &fixture.owner,
            &revokee, &fixture.encryption, &custody,
        ).await.expect("accept the replacement Owner removal after the Member retirement");
        let completed = StoreDatabase::new(&completion_db);
        let accepted_removal = completed.store_current_publication().await.unwrap();
        let accepted_entries = completed.store_publication_entries().await.unwrap();
        let replacement_ref = accepted_entries.iter().rev().find_map(|entry| match &entry.value.payload {
            StorePublicationPayload::Commit(reference) if reference.coord.stream_id == original.reference.coord.stream_id => Some(reference),
            _ => None,
        }).expect("accepted replacement commit");
        if matches!(continuation, RetirementContinuation::Satisfied) {
            assert_eq!(replacement_ref.coord, original.reference.coord);
        } else {
            assert!(replacement_ref.coord.sequence > original.reference.coord.sequence);
        }
        let context = ProtocolObjectContext::signed_plaintext(root.store_root_hash, ProtocolObjectDomain::StoreCommit);
        let prefix = semantic_prefix_from_exact_object(&replacement_ref.object, ".json").unwrap();
        let replacement_bytes = storage.read_protocol_object(&context, &replacement_ref.object, &prefix).await.unwrap();
        let replacement: StoreBatchCommit = coven_protocol::objects::decode_protocol_object(&replacement_bytes).unwrap();
        assert_eq!(replacement.write_id, original.commit.write_id, "replacement preserves its logical mutation identity");
        assert!(!accepted_entries.iter().any(|entry| matches!(&entry.value.payload, StorePublicationPayload::Commit(reference) if reference == &original.reference)));
        assert!(storage.observe_exact_slot(original.reference.object.slot()).await.unwrap().is_none());
        assert_ne!(storage.observe_exact_slot(publication.head_ref.object.slot()).await.unwrap(), Some(publication.head_ref.object));
        assert!(
            accepted_removal.record().accepted().unwrap().position
                > accepted_retirement.record().accepted().unwrap().position
        );
        let membership = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
            .open_pinned(storage.as_ref(), &root)
            .await
            .expect("open a cold rooted authority reader")
            .load_accepted_anchored_membership(&[], Some(&keys::public_key_hex(&fixture.owner)))
            .await
            .expect("read authority after both accepted removals");
        assert_eq!(membership.is_owner_now(&keys::public_key_hex(&fixture.member)), matches!(continuation, RetirementContinuation::Satisfied));
        if matches!(continuation, RetirementContinuation::Satisfied) {
            assert!(completed.outbound_membership_mutation().await.unwrap().is_none());
            assert!(completed.active_store_publication().await.unwrap().is_none());
            assert!(!membership.authorizes_write_authority(&creation, &third_pubkey));
            assert!(!replacement.abandoned_candidates().is_empty());
            return;
        }
        let coven_protocol::membership::StoreAuthorityChange::RemoveMember { wrapped_keys, .. } = &publication.entry.change else { panic!("retained removal"); };
        let exposed = wrapped_keys.iter().find(|key| key.recipient_pubkey == third_pubkey)
            .expect("old candidate offered its proposed key to the subsequently retired Member");
        let active_keys = membership.wrapped_key_authority_for(&keys::public_key_hex(&fixture.owner)).unwrap();
        assert!(!active_keys.is_empty());
        let latest = active_keys.iter().max_by_key(|key| key.generation).unwrap();
        let accepted_keyring = crate::sync::store::authorization::StoreKeyrings::new(storage.as_ref(), root.clone())
            .open_containing(&fixture.owner, &membership, latest).await.unwrap();
        let abandoned_keyring = EncryptionService::from_keyring_payload(
            serde_json::from_value(durable["plan"]["keyring_payload"].clone()).unwrap(),
        ).unwrap();
        assert!(accepted_keyring.current_generation() > exposed.generation);
        let ciphertext = accepted_keyring.encrypt(b"content after both removals", b"retirement authority");
        assert!(abandoned_keyring.decrypt(&ciphertext, b"retirement authority").is_err(),
            "the abandoned key offered to the retired Member cannot open later content");
        assert!(
            !membership.authorizes_write_authority(&creation, &third_pubkey)
                && membership
                    .write_authority_retirement(&creation, &third_pubkey)
                    .is_some(),
            "an accepted retirement used as permanent nonactivation proof must not revive its exact grant"
        );
    })
    .await;
}

#[tokio::test]
async fn owner_removal_preserves_device_authority_accepted_after_its_preparation() {
    Box::pin(async {
        let (fixture, storage) =
            PromotionCandidate::build_with_connection("concurrent-owner-removal-device-effect")
                .await;
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
            .expect("activate the second Owner");
        let founder = fixture
            .store
            .bind_device_in(
                &fixture.owner_db,
                fixture.owner_db_store_dir.clone(),
                &fixture.owner,
            )
            .await
            .expect("bind founder");
        let target = fixture
            .store
            .bind_device_in(
                &fixture.member_db,
                fixture.member_db_store_dir.clone(),
                &fixture.member,
            )
            .await
            .expect("bind target Owner");
        let founder_registration = founder
            .latest_local_store_device_registration()
            .await
            .expect("read founder registration")
            .expect("founder registration exists");
        let founder_value: StoreDeviceRegistration =
            serde_json::from_slice(&founder_registration.registration_bytes)
                .expect("decode founder registration");
        let root = fixture.store.root();
        let founder_ref = StoreDeviceRegistrationRef::from_registration(
            &founder_value,
            founder_registration.prepared.reference().clone(),
        );
        let owner_database = StoreDatabase::new(&fixture.owner_db);
        let before = owner_database
            .store_current_publication()
            .await
            .expect("read removal preparation boundary");
        let custody = crate::sync::test_helpers::TestCustody::default();
        fixture.home.fail_exact_create_before_call(1);
        fixture
            .store
            .remove_member(
                &fixture.owner_db,
                fixture.owner_db_store_dir.clone(),
                &fixture.owner,
                &keys::public_key_hex(&fixture.member),
                &fixture.encryption,
                &custody,
            )
            .await
            .expect_err("retain the removal before its first object upload");
        let staged = owner_database
            .outbound_membership_mutation()
            .await
            .expect("read staged removal")
            .expect("removal remains durable");
        let proposal = target.prepare_peer_exclusion(&founder_ref).await;
        let accepted = StoreDatabase::new(&fixture.member_db)
            .store_current_publication()
            .await
            .expect("read accepted proposal boundary");
        assert!(
            accepted
                .record()
                .accepted()
                .expect("accepted proposal")
                .position
                > before
                    .record()
                    .accepted()
                    .expect("prepared predecessor")
                    .position,
            "the device effect was accepted after removal preparation"
        );
        assert_eq!(
            owner_database
                .outbound_membership_mutation()
                .await
                .expect("read unchanged removal")
                .expect("same removal remains durable")
                .plan_bytes,
            staged.plan_bytes,
            "the intervening proposal did not replace the removal candidate"
        );
        fixture
            .store
            .remove_member(
                &fixture.owner_db,
                fixture.owner_db_store_dir.clone(),
                &fixture.owner,
                &keys::public_key_hex(&fixture.member),
                &fixture.encryption,
                &custody,
            )
            .await
            .expect("accept the removal after the target's device effect");
        assert!(!founder
            .membership_for_test()
            .await
            .expect("read accepted removal")
            .can_write_now(&keys::public_key_hex(&fixture.member)));
        let published = founder
            .publish_snapshot_generation_for_test()
            .await
            .expect("snapshot every earlier accepted device effect");
        let mut history = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
            .open_pinned(storage.as_ref(), &root)
            .await
            .expect("open cold snapshot reader");
        let snapshot = history
            .load_current_accepted_snapshot()
            .await
            .expect("verify the accepted snapshot after grant retirement");
        assert_eq!(snapshot.reference, published.reference);
        assert!(
            snapshot
                .meta
                .publication_predecessor
                .accepted()
                .expect("snapshot accepted prefix")
                .position
                > accepted
                    .record()
                    .accepted()
                    .expect("accepted target proposal")
                    .position,
            "the snapshot includes both the proposal and the later removal"
        );
        assert!(
            matches!(
                snapshot.meta.state.devices.devices[&founder_ref.device_id]
                    .proposals.get(&proposal.proposal_id),
                Some(StoreDeviceProposalState::Pending { proposal: actual }) if actual == &proposal
            ),
            "retiring the proposer grant cannot erase its earlier accepted device effect"
        );
    })
    .await;
}

#[tokio::test]
async fn exclusion_preserves_authority_accepted_after_its_candidate_was_prepared() {
    Box::pin(async {
        let (fixture, storage) =
            PromotionCandidate::build_with_connection("concurrent-device-authority-exclusion")
                .await;
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
            .expect("activate the second principal's Owner grant");
        let founder = fixture
            .store
            .bind_device_in(
                &fixture.owner_db,
                fixture.owner_db_store_dir.clone(),
                &fixture.owner,
            )
            .await
            .expect("bind founder");
        let target = fixture
            .store
            .bind_device_in(
                &fixture.member_db,
                fixture.member_db_store_dir.clone(),
                &fixture.member,
            )
            .await
            .expect("bind target Owner");
        let proposal = founder
            .prepare_peer_exclusion(&fixture.member_registration)
            .await;
        let (staged, resume) = fixture
            .owner_db
            .arm_test_pause(coven_database::DatabaseTestPoint::StoreDeviceExclusionCandidateStaged);
        let publisher = founder.clone();
        let finalization =
            tokio::spawn(async move { publisher.finalize_device_exclusion(&proposal).await });
        staged.notified().await;
        let prepared_boundary = StoreDatabase::new(&fixture.owner_db)
            .store_current_publication()
            .await
            .expect("boundary captured before finalization");
        let invited = keys::public_key_hex(&UserKeypair::generate());
        let root = fixture.store.root();
        let admission = target
            .admit_member(
                &invited,
                None,
                MemberRole::Member,
                &fixture.encryption,
                &root.store_root_id.to_string(),
                "Concurrent admission",
            )
            .await;
        let admitted_heads = target
            .membership_for_test()
            .await
            .expect("accepted target heads")
            .head_refs()
            .to_vec();
        // Release the durable operation even when an assertion below fails.
        resume.notify_one();
        admission.expect("the target remains authorized while exclusion is pending");
        let admitted_boundary = StoreDatabase::new(&fixture.member_db)
            .store_current_publication()
            .await
            .expect("accepted target authority boundary");
        assert!(
            admitted_boundary.record().accepted().unwrap().position
                > prepared_boundary.record().accepted().unwrap().position,
            "target authority was accepted after outcome preparation"
        );
        finalization
            .await
            .expect("join finalization")
            .expect("finalize against the winning shared predecessor");
        let founder_membership = founder
            .membership_for_test()
            .await
            .expect("accepted finalization heads");
        let exclusion_head = founder_membership
            .head_refs()
            .iter()
            .find(|head| head.coord.author_pubkey == keys::public_key_hex(&fixture.owner))
            .expect("founder finalization head");
        let head_context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let head_prefix =
            semantic_prefix_from_exact_object(&exclusion_head.object, ".json").unwrap();
        let head_bytes = storage
            .read_protocol_object(&head_context, &exclusion_head.object, &head_prefix)
            .await
            .expect("read finalization head");
        let head: coven_protocol::membership::AuthorHead =
            coven_protocol::objects::decode_protocol_object(&head_bytes)
                .expect("decode finalization head");
        let MembershipHeadActivation::StoreCommit {
            acceptance_slot, ..
        } = &head.activation
        else {
            panic!("exclusion has a Store activation")
        };
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipHeadAcceptance,
        );
        let prefix = coven_protocol::membership::membership_head_acceptance_semantic_prefix(
            &exclusion_head.coord,
        );
        let (bytes, _) = storage
            .read_protocol_slot(&context, acceptance_slot, &prefix)
            .await
            .expect("read winning acceptance result");
        let accepted: MembershipHeadAcceptance =
            coven_protocol::objects::decode_protocol_object(&bytes)
                .expect("decode winning acceptance result");
        let registration = founder
            .latest_local_store_device_registration()
            .await
            .unwrap()
            .unwrap();
        let registration: StoreDeviceRegistration =
            serde_json::from_slice(&registration.registration_bytes).unwrap();
        accepted
            .verify_for(root.store_root_hash, exclusion_head, &head, &registration)
            .expect("verify exact winning result");
        let target_head = admitted_heads
            .iter()
            .find(|head| head.coord.author_pubkey == keys::public_key_hex(&fixture.member))
            .expect("intervening target head");
        assert!(
            accepted.accepted_predecessor.0.contains(target_head),
            "the receipt binds the intervening accepted head, not the candidate's older predecessor"
        );
        let membership = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
            .open_pinned(storage.as_ref(), &root)
            .await
            .expect("open cold rooted authority reader")
            .load_accepted_anchored_membership(&[], Some(&keys::public_key_hex(&fixture.owner)))
            .await
            .expect("verify accepted authority after exclusion");
        assert!(
            membership.can_write_now(&invited),
            "exclusion preserves authority accepted before its winning publication"
        );
    })
    .await;
}

#[tokio::test]
async fn excluded_device_cannot_append_authority_with_a_backdated_acceptance_result() {
    excluded_device_authority_tail(AuthorityTail::FabricatedResult).await;
}

#[tokio::test]
async fn excluded_device_pending_authority_head_does_not_block_rooted_reads() {
    excluded_device_authority_tail(AuthorityTail::Pending).await;
}

#[tokio::test]
async fn excluded_pending_successor_cannot_change_its_predecessors_accepted_result() {
    excluded_device_authority_tail(AuthorityTail::ChangedPredecessor).await;
}

enum AuthorityTail {
    FabricatedResult,
    Pending,
    ChangedPredecessor,
}

async fn excluded_device_authority_tail(tail: AuthorityTail) {
    let publish_fabricated_result = matches!(tail, AuthorityTail::FabricatedResult);
    let (fixture, storage) =
        PromotionCandidate::build_with_connection("excluded-device-authority-append").await;
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
        .expect("activate the second principal's Owner grant");
    let founder = fixture
        .store
        .bind_device_in(
            &fixture.owner_db,
            fixture.owner_db_store_dir.clone(),
            &fixture.owner,
        )
        .await
        .expect("bind founder");
    let device = fixture
        .store
        .bind_device_in(
            &fixture.member_db,
            fixture.member_db_store_dir.clone(),
            &fixture.member,
        )
        .await
        .expect("bind second Owner device");
    let invited = keys::public_key_hex(&UserKeypair::generate());
    let root = fixture.store.root();
    let member_database = StoreDatabase::new(&fixture.member_db);
    let mut carried_rollup = if matches!(tail, AuthorityTail::ChangedPredecessor) {
        device
            .admit_member(
                &keys::public_key_hex(&UserKeypair::generate()),
                None,
                MemberRole::Member,
                &fixture.encryption,
                &root.store_root_id.to_string(),
                "Accepted predecessor",
            )
            .await
            .expect("target accepts its own predecessor control");
        founder
            .pull_store()
            .await
            .expect("publisher receives target authority");
        let snapshot = founder
            .publish_snapshot_generation_for_test()
            .await
            .expect("publish predecessor rollup");
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipRollup,
        );
        let prefix =
            semantic_prefix_from_exact_object(&snapshot.meta.membership_rollup.object, ".json")
                .unwrap();
        let bytes = storage
            .read_protocol_object(&context, &snapshot.meta.membership_rollup.object, &prefix)
            .await
            .expect("read actual predecessor rollup");
        Some(
            coven_protocol::objects::decode_protocol_object::<MembershipRollup>(&bytes)
                .expect("decode predecessor rollup"),
        )
    } else {
        None
    };
    device
        .pull_store()
        .await
        .expect("capture the accepted preparation boundary");
    let before = member_database
        .store_current_publication()
        .await
        .expect("current accepted publication");
    fixture.home.fail_exact_create_before_call(1);
    device
        .admit_member(
            &invited,
            None,
            MemberRole::Member,
            &fixture.encryption,
            &root.store_root_id.to_string(),
            "Owner admission",
        )
        .await
        .expect_err("retain the actual candidate before its first upload");
    let mutation = member_database
        .outbound_membership_mutation()
        .await
        .expect("read actual admission journal")
        .expect("admission remains pending");
    let durable: serde_json::Value =
        serde_json::from_slice(&mutation.plan_bytes).expect("decode durable admission envelope");
    assert_eq!(durable["kind"], "admission");
    let activation = &durable["plan"]["activation"];
    let candidate: PreparedStoreOperationCommit =
        serde_json::from_value(activation["candidate"].clone()).expect("actual staged candidate");
    let mut publication: PreparedMembershipPublication =
        serde_json::from_value(activation["publication"].clone()).expect("actual staged head");
    let wrapped: coven_protocol::wrapped_store_key::PreparedWrappedStoreKey =
        serde_json::from_value(durable["plan"]["wrapped_key"].clone())
            .expect("actual staged key wrap");
    wrapped.validate().expect("exact wrapped key");
    candidate.validate_closed_shape().expect("closed candidate");
    publication
        .validate()
        .expect("exact membership publication");
    assert_eq!(candidate.publication.previous, *before.record());
    assert_eq!(
        member_database.store_current_publication().await.unwrap(),
        before,
        "preparation did not accept the proposed admission"
    );
    assert!(storage
        .observe_exact_slot(publication.head_ref.object.slot())
        .await
        .expect("inspect unuploaded head")
        .is_none());

    founder
        .finalize_peer_exclusion(&fixture.member_registration)
        .await;
    let founder_database = StoreDatabase::new(&fixture.owner_db);
    let frontier = CommitFrontier::from_refs(
        founder_database
            .materialized_frontier()
            .await
            .expect("exclusion frontier"),
    )
    .expect("exact accepted cut");
    let (_, state) = founder_database
        .store_device_state_for_history_cut(&StoreHistoryCut(frontier.0))
        .await
        .expect("accepted exclusion state");
    assert!(matches!(
        state.devices[&fixture.member_registration.device_id].status,
        StoreDeviceStatus::Inactive { .. }
    ));
    assert!(founder
        .membership_for_test()
        .await
        .expect("membership after device exclusion")
        .is_owner_now(&keys::public_key_hex(&fixture.member)));
    let accepted = founder_database
        .store_current_publication()
        .await
        .expect("accepted exclusion boundary");
    assert!(!founder_database
        .store_publication_entries()
        .await
        .expect("accepted entries")
        .iter()
        .any(|entry| entry.value.payload
            == StorePublicationPayload::Commit(candidate.reference.clone())));

    let before_append = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
        .open_pinned(storage.as_ref(), &root)
        .await
        .expect("open rooted reader before the fabricated append")
        .load_accepted_anchored_membership(&[], Some(&keys::public_key_hex(&fixture.owner)))
        .await
        .expect("genuine accepted authority remains readable after exclusion");
    assert!(!before_append.can_write_now(&invited));
    assert!(before_append.is_owner_now(&keys::public_key_hex(&fixture.member)));

    let registration = device
        .latest_local_store_device_registration()
        .await
        .expect("retained registration")
        .expect("registered Owner device");
    let registration: StoreDeviceRegistration =
        serde_json::from_slice(&registration.registration_bytes).expect("registration bytes");
    let signer = registration
        .device_signer(&fixture.member)
        .expect("retained device key");
    if let Some(rollup) = &mut carried_rollup {
        let predecessor = publication
            .head
            .body
            .predecessor
            .as_ref()
            .expect("pending successor has an accepted predecessor");
        let previous_head = rollup
            .streams
            .iter()
            .flat_map(|stream| &stream.heads)
            .find(|carried| &carried.head == predecessor.head())
            .expect("published rollup carries the actual predecessor")
            .head_value
            .clone();
        let original_object = predecessor
            .acceptance()
            .expect("exact predecessor result")
            .clone();
        let original = fixture
            .home
            .stored_exact_bytes(original_object.slot())
            .expect("the accepted predecessor result remains at its actual slot");
        let mut alternate: MembershipHeadAcceptance =
            coven_protocol::objects::decode_protocol_object(&original)
                .expect("actual accepted predecessor result");
        let selected_head = alternate.head.clone();
        alternate.body_mut().accepted_predecessor =
            coven_protocol::membership::MembershipFloor(vec![selected_head]);
        alternate.resign(&signer);
        alternate
            .verify_for(
                root.store_root_hash,
                &alternate.head,
                &previous_head,
                &registration,
            )
            .expect("alternate result retains a canonical floor and valid issuer signature");
        let bytes = alternate.to_bytes();
        let replacement = coven_protocol::objects::ExactObjectRef::new(
            original_object.slot().clone(),
            bytes.len() as u64,
            ObjectHash::digest(&bytes),
        );
        assert_ne!(replacement, original_object);
        let coven_protocol::membership::MembershipHeadPredecessor::Accepted { acceptance, .. } =
            publication
                .head
                .body_mut()
                .body
                .predecessor
                .as_mut()
                .unwrap()
        else {
            panic!("accepted predecessor result");
        };
        *acceptance = replacement;
        publication.head.resign(&signer);
        publication.head_ref.head_hash = publication.head.head_hash();
        let bytes = publication.head.to_bytes();
        publication.head_ref.object = coven_protocol::objects::ExactObjectRef::new(
            publication.head_ref.object.slot().clone(),
            bytes.len() as u64,
            ObjectHash::digest(&bytes),
        );
        publication
            .validate()
            .expect("pending head still has its exact entry and valid signature");
        rollup
            .body_mut()
            .streams
            .iter_mut()
            .find(|stream| stream.author_pubkey == keys::public_key_hex(&fixture.member))
            .expect("target stream")
            .heads
            .push(MembershipRollupHead {
                head: publication.head_ref.clone(),
                head_value: publication.head.clone(),
                entry: publication.entry_ref.clone(),
                entry_value: publication.entry.clone(),
                predecessor_acceptance: Some(alternate),
            });
        let publisher = founder
            .latest_local_store_device_registration()
            .await
            .unwrap()
            .unwrap();
        let publisher: StoreDeviceRegistration =
            serde_json::from_slice(&publisher.registration_bytes).unwrap();
        rollup.resign(&publisher.device_signer(&fixture.owner).unwrap());
        rollup
            .validate_shape()
            .expect("carried pending head and alternate result are internally exact");
        assert_eq!(
            fixture.home.stored_exact_bytes(original_object.slot()),
            Some(original),
            "the genuine accepted result is unchanged at the provider"
        );
    }
    let proposed_ref = candidate
        .publication
        .reference()
        .expect("unaccepted proposed envelope");
    assert!(proposed_ref.position < accepted.record().accepted().unwrap().position);
    let result = MembershipHeadAcceptance::signed(
        root.store_root_hash,
        publication.head_ref.clone(),
        &publication.head,
        &candidate.publication.entry,
        &candidate.publication.replacement,
        coven_protocol::membership::MembershipFloor(
            candidate.commit.membership_state.heads.clone(),
        ),
        &registration,
        &signer,
    )
    .expect("excluded key can sign a fabricated acceptance assertion");
    result
        .verify_for(
            root.store_root_hash,
            &publication.head_ref,
            &publication.head,
            &registration,
        )
        .expect("fabricated assertion has valid signatures and exact bindings");
    let MembershipHeadActivation::StoreCommit {
        acceptance_slot, ..
    } = &publication.head.activation
    else {
        panic!("real admission is Store-bound");
    };
    for (context, prepared) in [
        (
            ProtocolObjectContext::recipient_sealed(
                root.store_root_hash,
                ProtocolObjectDomain::StoreWrappedKey,
            ),
            wrapped.object,
        ),
        (
            ProtocolObjectContext::signed_plaintext(
                root.store_root_hash,
                ProtocolObjectDomain::StoreMembershipEntry,
            ),
            publication.prepared_entry().unwrap(),
        ),
        (
            ProtocolObjectContext::signed_plaintext(
                root.store_root_hash,
                ProtocolObjectDomain::StoreMembershipHead,
            ),
            publication.prepared_head().unwrap(),
        ),
    ] {
        let prefix = semantic_prefix_from_exact_object(prepared.reference(), ".json").unwrap();
        storage
            .create_verified_protocol_object(&context, &prepared, &prefix, prepared.stored_bytes())
            .await
            .expect("publish the exact previously staged authority object after exclusion");
    }
    if publish_fabricated_result {
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipHeadAcceptance,
        );
        let prefix = coven_protocol::membership::membership_head_acceptance_semantic_prefix(
            &publication.head_ref.coord,
        );
        let prepared = storage
            .prepare_protocol_object(
                &context,
                acceptance_slot.clone(),
                &prefix,
                result.to_bytes(),
            )
            .expect("prepare fabricated result at the head-owned slot");
        storage
            .create_verified_protocol_object(&context, &prepared, &prefix, &result.to_bytes())
            .await
            .expect("publish fabricated acceptance result");
    }
    let history = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
        .open_pinned(storage.as_ref(), &root)
        .await
        .expect("open cold rooted authority reader");
    if let Some(rollup) = carried_rollup {
        history
            .admit_membership_rollup(&rollup)
            .await
            .expect("admit a signed rollup whose pending successor is not an accepted authority");
    }
    let result = history
        .load_accepted_anchored_membership(&[], Some(&keys::public_key_hex(&fixture.owner)))
        .await;
    assert_eq!(
        founder_database.store_current_publication().await.unwrap(),
        accepted,
        "the authority append did not enter the actual accepted publication history"
    );
    match result {
        Ok(membership) => assert!(
            !membership.can_write_now(&invited),
            "an excluded device's fabricated acceptance result activated its new authority entry"
        ),
        Err(error) => {
            assert!(
                publish_fabricated_result,
                "an excluded pending tail must be inert: {error}"
            );
            assert!(
                error.to_string().contains(
                    "membership authority repeats an accepted Store publication position"
                ),
                "reject the excluded authority itself: {error}"
            );
        }
    }
}
