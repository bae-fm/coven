use super::*;
use crate::sync::store::OwnerPromotionError;
use crate::sync::test_helpers::{self, InterceptedStorage, StorageInterceptor, TestDevice};
use coven_protocol::membership::MemberRole;
use coven_protocol::owner_promotion_journal::{OwnerPromotionJournal, OwnerPromotionStaleEvidence};
use coven_protocol::remote_object::CandidateNonactivationProof;
use coven_protocol::store_commit::{OwnerPromotionAcceptance, StoreDeviceRegistrationRef};
use coven_storage::CloudSyncObjectStorage;

#[derive(Clone, Copy)]
enum PromotionStage {
    Request,
    MergeHead,
}

enum PreparedPromotion {
    Request(StoreDeviceRegistrationRef),
    MergeHead(OwnerPromotionAcceptance),
}

impl PreparedPromotion {
    async fn retry(
        &self,
        device: &TestDevice,
        encryption: &EncryptionService,
    ) -> Result<(), OwnerPromotionError> {
        match self {
            Self::Request(target) => device
                .begin_owner_promotion(target.clone())
                .await
                .map(|_| ()),
            Self::MergeHead(acceptance) => device
                .finalize_owner_promotion(encryption, acceptance.clone())
                .await
                .map(|_| ()),
        }
    }
}

#[tokio::test]
async fn an_unaccepted_promotion_request_retires_after_its_issuer_is_removed() {
    issuer_retirement(PromotionStage::Request, Interruption::Upload(false)).await;
}

#[tokio::test]
async fn an_unaccepted_promotion_merge_head_retires_after_its_issuer_is_removed() {
    issuer_retirement(PromotionStage::MergeHead, Interruption::Upload(false)).await;
}

#[tokio::test]
async fn a_removed_promotion_request_issuer_resumes_interrupted_cleanup_after_reopen() {
    issuer_retirement(PromotionStage::Request, Interruption::Upload(true)).await;
}

#[tokio::test]
async fn a_removed_promotion_merge_head_issuer_resumes_interrupted_cleanup_after_reopen() {
    issuer_retirement(PromotionStage::MergeHead, Interruption::Upload(true)).await;
}

#[tokio::test]
async fn a_retired_promotion_keeps_its_outcome_after_a_new_target_attempt() {
    issuer_retirement(PromotionStage::MergeHead, Interruption::ReplaceTerminal).await;
}

enum Interruption {
    Upload(bool),
    BeforeAcceptance,
    ConcurrentUpload,
    ReplaceTerminal,
}

struct PausePromotionUpload {
    object: ExactObjectRef,
    exercised: std::sync::atomic::AtomicBool,
    reached: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl StorageInterceptor for PausePromotionUpload {
    async fn before_protocol_create(
        &self,
        prepared: &coven_protocol::objects::PreparedExactObject,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        if prepared.reference() == &self.object
            && !self
                .exercised
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.reached.notify_one();
            self.resume.notified().await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_pending_promotion_upload_cannot_recreate_objects_after_concurrent_retirement() {
    issuer_retirement(PromotionStage::Request, Interruption::ConcurrentUpload).await;
}

#[tokio::test]
async fn a_promotion_request_cannot_be_accepted_when_its_issuer_is_removed_during_publication() {
    issuer_retirement(PromotionStage::Request, Interruption::BeforeAcceptance).await;
}

fn assert_retired(journal: &OwnerPromotionJournal, stage: PromotionStage) {
    let proof = match (&journal.state, stage) {
        (
            OwnerPromotionJournalState::Nonactivated { nonactivation, .. },
            PromotionStage::Request,
        ) => nonactivation,
        (OwnerPromotionJournalState::Stale { evidence, .. }, PromotionStage::MergeHead) => {
            let OwnerPromotionStaleEvidence::Candidate { nonactivation, .. } = evidence.as_ref()
            else {
                panic!("prepared promotion must retain exact candidate retirement");
            };
            nonactivation
        }
        _ => panic!(
            "issuer retirement must persist its exact terminal decision: {:?}",
            journal.state
        ),
    };
    assert!(matches!(
        proof.proof(),
        CandidateNonactivationProof::AuthorityRetirement { .. }
    ));
}

async fn reopen(
    database: &coven_database::Database,
    source: &coven_foundation::store_dir::StoreDir,
) -> (
    coven_database::Database,
    coven_foundation::store_dir::StoreDir,
) {
    let directory = test_helpers::test_store_dir();
    database
        .vacuum_into_for_test(directory.db_path().to_string_lossy().into_owned())
        .await
        .unwrap();
    test_helpers::copy_payload_files(source, &directory);
    let reopened = coven_database::Database::open_synthetic_for_test(
        &directory.db_path(),
        directory.clone(),
        test_helpers::test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "test-device".into(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &test_helpers::test_migrations(),
    )
    .expect("reopen the actual promotion journal and candidate payloads");
    (reopened, directory)
}

async fn issuer_retirement(stage: PromotionStage, interruption: Interruption) {
    let interrupt_cleanup = matches!(interruption, Interruption::Upload(true));
    Box::pin(async {
        let (fixture, storage) =
            PromotionCandidate::build_with_connection("promotion-issuer-retirement").await;
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
        let peer_storage =
            std::sync::Arc::new(storage.connection_for_test_identity(fixture.member.clone()));
        let peer = TestDevice::load_with_database(
            StoreDatabase::new(&fixture.member_db),
            peer_storage.clone(),
            fixture.member.clone(),
            fixture.member_db_store_dir.clone(),
        )
        .await
        .unwrap();
        let target_identity = UserKeypair::generate();
        let target_pubkey = keys::public_key_hex(&target_identity);
        fixture
            .store
            .admit_member(
                &fixture.owner_db,
                fixture.owner_db_store_dir.clone(),
                &fixture.owner,
                &target_pubkey,
                None,
                MemberRole::Member,
                &fixture.encryption,
                "Promotion target",
            )
            .await
            .unwrap();
        let target_directory = test_helpers::test_store_dir();
        let target_database = test_helpers::open_test_db(target_directory.clone());
        let target = fixture
            .store
            .activate_joined_device(
                &fixture.owner_db,
                fixture.owner_db_store_dir.clone(),
                &target_database,
                target_directory,
                &target_identity,
                "2026-07-20T00:00:00Z",
            )
            .await
            .unwrap();
        let target_registration = target.owner_promotion_target_for_test().await.unwrap();
        let issuer = fixture
            .store
            .bind_device_in(
                &fixture.owner_db,
                fixture.owner_db_store_dir.clone(),
                &fixture.owner,
            )
            .await
            .unwrap();
        let preparation = match stage {
            PromotionStage::Request => PreparedPromotion::Request(target_registration.clone()),
            PromotionStage::MergeHead => {
                let request = issuer
                    .begin_owner_promotion(target_registration.clone())
                    .await
                    .unwrap();
                PreparedPromotion::MergeHead(target.accept_owner_promotion(request).await.unwrap())
            }
        };
        let pause = match interruption {
            Interruption::Upload(_) | Interruption::ConcurrentUpload | Interruption::ReplaceTerminal => {
                fixture.home.fail_exact_create_before_call(match stage {
                    PromotionStage::Request => 2,
                    PromotionStage::MergeHead => 5,
                });
                None
            }
            Interruption::BeforeAcceptance => Some(fixture.home.pause_after_exact_create_call(2)),
        };
        let mut publishing = Box::pin(preparation.retry(&issuer, &fixture.encryption));
        if let Some((reached, _)) = &pause {
            tokio::select! {
                _ = reached.notified() => {},
                result = &mut publishing => panic!("publication ended before the exact entry upload: {result:?}"),
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => panic!("publication did not reach its entry upload"),
            }
        } else {
            publishing.as_mut().await.expect_err("interrupt a real upload before Store publication acceptance");
        }
        let database = StoreDatabase::new(&fixture.owner_db);
        let original = database
            .load_owner_promotion_target(target_key(&target_registration).unwrap())
            .await
            .unwrap()
            .unwrap();
        let (candidate, objects) = match (&original.state, stage) {
            (
                OwnerPromotionJournalState::RequestPrepared { candidate, .. },
                PromotionStage::Request,
            ) => (candidate.as_ref(), vec![candidate.reference.object.clone()]),
            (
                OwnerPromotionJournalState::MergeHeadPrepared {
                    candidate,
                    wrapped_key,
                    ..
                },
                PromotionStage::MergeHead,
            ) => (
                candidate.as_ref(),
                candidate.merge_membership_activation_remote_objects(std::slice::from_ref(wrapped_key))
                    .unwrap().iter().map(|remote| remote.record().object().clone()).collect::<Vec<_>>(),
            ),
            _ => panic!(
                "upload failure did not preserve the requested preparation: {:?}",
                original.state
            ),
        };
        assert_eq!(
            storage
                .observe_exact_slot(candidate.reference.object.slot())
                .await
                .unwrap(),
            Some(candidate.reference.object.clone()),
            "the candidate bytes must exist before retirement"
        );
        let reserved = database.active_store_publication().await.unwrap().unwrap();
        assert_eq!(
            reserved.commit_reservation().unwrap().2,
            &candidate.reference.coord
        );
        assert_eq!(
            reserved.attempt().unwrap().entry.payload,
            coven_protocol::store_commit::StorePublicationPayload::Commit(
                candidate.reference.clone()
            )
        );
        let concurrent_pause = std::sync::Arc::new(PausePromotionUpload {
            object: reserved.attempt().unwrap().entry_object.clone(),
            exercised: std::sync::atomic::AtomicBool::new(false),
            reached: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        let intercepted = if matches!(interruption, Interruption::ConcurrentUpload) {
            Some(fixture.store.open_founder_store_with_storage(
                database.clone(),
                std::sync::Arc::new(InterceptedStorage::new(storage.clone(), concurrent_pause.clone())),
                fixture.owner_db_store_dir.clone(),
            ).await.unwrap())
        } else {
            None
        };
        let mut concurrent_upload = intercepted.as_ref().map(|store| {
            let target = target_registration.clone();
            Box::pin(async move { store.begin_owner_promotion(target).await })
        });
        if let Some(upload) = concurrent_upload.as_mut() {
            tokio::select! {
                _ = concurrent_pause.reached.notified() => {},
                result = upload => panic!("concurrent retry ended before its entry upload: {result:?}"),
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => panic!("concurrent retry did not reach its entry upload"),
            }
        }
        peer.remove_member(
            &keys::public_key_hex(&fixture.owner),
            &fixture.encryption,
            &test_helpers::TestCustody::default(),
            peer_storage.as_ref(),
            peer_storage.as_ref(),
        )
        .await
        .unwrap();
        let accepted = StoreDatabase::new(&fixture.member_db)
            .store_current_publication()
            .await
            .unwrap()
            .record()
            .clone();
        assert!(!peer
            .membership_for_test()
            .await
            .unwrap()
            .is_owner_now(&target_pubkey));
        if let Some((_, release)) = pause {
            fixture.home.clear_exact_creates();
            release.notify_one();
            publishing.as_mut().await.expect_err("a promotion request must recheck its grant after losing the publication race");
            assert!(fixture.home.exact_creates().is_empty(), "retired issuer must not publish a replacement envelope");
            peer.pull_store_with_encryption(&fixture.encryption).await.unwrap();
            assert_eq!(StoreDatabase::new(&fixture.member_db).store_current_publication().await.unwrap().record(), &accepted);
        }
        if let Some(mut upload) = concurrent_upload {
            let mut retirement = Box::pin(preparation.retry(&issuer, &fixture.encryption));
            let completed = tokio::select! {
                result = &mut retirement => Some(result),
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => None,
            };
            if let Some(result) = &completed {
                assert!(result.is_err(), "the second caller cannot accept a retired request");
                assert!(database.active_store_publication().await.unwrap().is_none());
                assert!(storage.observe_exact_slot(concurrent_pause.object.slot()).await.unwrap().is_none());
            }
            concurrent_pause.resume.notify_one();
            tokio::time::timeout(std::time::Duration::from_secs(30), upload.as_mut()).await.expect("the original upload must return after release").expect_err("the prior publisher cannot accept its retired request");
            if completed.is_none() {
                tokio::time::timeout(std::time::Duration::from_secs(30), retirement.as_mut()).await.expect("retirement must continue after the prior publisher releases authorship").expect_err("the waiting caller retires the unaccepted request");
            }
            assert!(database.active_store_publication().await.unwrap().is_none());
            assert_retired(&database.load_owner_promotion_journal(original.promotion_id).await.unwrap().unwrap(), stage);
            assert_eq!(database.store_current_publication().await.unwrap().record(), &accepted);
            for object in objects.iter().chain(std::iter::once(&concurrent_pause.object)) {
                assert!(storage.observe_exact_slot(object.slot()).await.unwrap().is_none(), "an in-flight publisher recreated a retired object after cleanup released the reservation: {object:?}");
            }
            return;
        }
        drop(publishing);
        let (reopened, directory) = reopen(&fixture.owner_db, &fixture.owner_db_store_dir).await;
        let mut issuer = fixture
            .store
            .bind_device_in(&reopened, directory.clone(), &fixture.owner)
            .await
            .unwrap();
        let mut database = StoreDatabase::new(&reopened);
        let access_count = fixture.home.access_requests().len();
        fixture.home.clear_exact_creates();
        if interrupt_cleanup {
            fixture
                .home
                .fail_nth_exact_delete_of(&[candidate.reference.object.slot()], 1);
        }
        let mut result = preparation.retry(&issuer, &fixture.encryption).await;
        if interrupt_cleanup {
            let error = result.expect_err("the exact candidate delete must fail to its initiator");
            assert!(
                error.to_string().contains("forced exact delete failure"),
                "{error}"
            );
            let interrupted = database
                .load_owner_promotion_journal(original.promotion_id)
                .await
                .unwrap()
                .unwrap();
            assert_retired(&interrupted, stage);
            let active = database.active_store_publication().await.unwrap().unwrap();
            assert_eq!(active.commit_reservation(), reserved.commit_reservation());
            assert!(storage
                .observe_exact_slot(candidate.reference.object.slot())
                .await
                .unwrap()
                .is_some());
            assert!(fixture.home.exact_creates().is_empty());
            let (reopened_again, next_directory) = reopen(&reopened, &directory).await;
            issuer = fixture
                .store
                .bind_device_in(&reopened_again, next_directory, &fixture.owner)
                .await
                .unwrap();
            database = StoreDatabase::new(&reopened_again);
            result = preparation.retry(&issuer, &fixture.encryption).await;
        }
        let error = result.expect_err("an unaccepted promotion cannot survive its issuer grant");
        assert!(
            database.active_store_publication().await.unwrap().is_none(),
            "authenticated issuer retirement must release the promotion reservation: {error}"
        );
        let retired = database
            .load_owner_promotion_journal(original.promotion_id)
            .await
            .unwrap()
            .unwrap();
        assert_retired(&retired, stage);
        assert_eq!(retired.target, original.target);
        assert_eq!(
            database.store_current_publication().await.unwrap().record(),
            &accepted,
            "cleanup cannot publish an abandonment after issuer authority is gone"
        );
        assert!(
            fixture.home.exact_creates().is_empty(),
            "cleanup must not upload another candidate or membership object"
        );
        assert_eq!(fixture.home.access_requests().len(), access_count);
        for object in objects
            .iter()
            .chain(std::iter::once(&reserved.attempt().unwrap().entry_object))
        {
            assert!(
                storage
                    .observe_exact_slot(object.slot())
                    .await
                    .unwrap()
                    .is_none(),
                "retired promotion still owns remote bytes: {object:?}"
            );
        }
        let membership = peer.membership_for_test().await.unwrap();
        assert!(!membership.is_member_now(&keys::public_key_hex(&fixture.owner)));
        assert!(membership.is_member_now(&target_pubkey));
        assert!(!membership.is_owner_now(&target_pubkey));
        if matches!(interruption, Interruption::ReplaceTerminal) {
            let old_id = retired.promotion_id;
            let original_terminal = serde_json::to_string(&retired).unwrap();
            let replacement = OwnerPromotionJournal {
                promotion_id: coven_protocol::store_commit::OwnerPromotionId::from_generated(
                    database.new_store_write_id().to_string(),
                ),
                target: retired.target.clone(),
                state: OwnerPromotionJournalState::Allocated,
            };
            let replacement = database
                .replace_failed_owner_promotion_journal(retired, replacement)
                .await
                .expect("a completed failed attempt releases its target for another request");
            let expected_target = serde_json::to_string(&replacement).unwrap();
            let error = preparation
                .retry(&issuer, &fixture.encryption)
                .await
                .expect_err("the old acceptance must retain its terminal outcome");
            assert!(
                matches!(&error, OwnerPromotionError::Stale(reason)
                    if **reason == coven_protocol::store_commit::OwnerPromotionStaleReason::MergeActivationRejected),
                "a new target attempt must not replace the old promotion's terminal outcome: {error}"
            );
            assert_eq!(
                serde_json::to_string(&database.load_owner_promotion_journal(old_id).await.unwrap().unwrap()).unwrap(),
                original_terminal,
            );
            assert_eq!(
                serde_json::to_string(&database.load_owner_promotion_target(target_key(&target_registration).unwrap()).await.unwrap().unwrap()).unwrap(),
                expected_target,
            );
            assert!(database.active_store_publication().await.unwrap().is_none());
            assert_eq!(database.store_current_publication().await.unwrap().record(), &accepted);
            assert!(fixture.home.exact_creates().is_empty());
            assert_eq!(fixture.home.access_requests().len(), access_count);
        }
    })
    .await;
}
