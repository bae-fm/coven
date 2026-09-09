use super::*;

#[tokio::test]
async fn a_cached_promoter_cannot_strand_an_operation_after_device_recovery() {
    Box::pin(async {
        let fixture = PromotionCandidate::build("promotion-after-device-recovery").await;
        let original = fixture
            .store
            .bind_device(
                &fixture.owner_db,
                fixture.owner_db_store_dir.clone(),
                &fixture.owner,
            )
            .await
            .expect("bind original promoter");
        let mut cached_writer = original
            .authorize_writer()
            .await
            .expect("cache the original authorized writer");
        let authority = fixture.store.founder_recovery_authority().await;
        let recovered_registration = original
            .owner_recovery_for_test()
            .await
            .expect("authorize owner recovery")
            .recover_owner_device(&authority, None)
            .await
            .expect("replace the local device before the cached writer prepares");

        fixture.home.arm_write_failures();
        cached_writer
            .owner_promotion()
            .begin(fixture.member_registration.clone())
            .await
            .expect_err("interrupt any cached promotion before its first upload");
        fixture.home.clear_write_failures();
        drop(cached_writer);
        drop(original);

        let database = StoreDatabase::new(&fixture.owner_db);
        let prior = database
            .load_owner_promotion_target(target_key(&fixture.member_registration).unwrap())
            .await
            .expect("read any retained attempt after interruption");
        let recovered = fixture
            .store
            .bind_device(
                &fixture.owner_db,
                fixture.owner_db_store_dir.clone(),
                &fixture.owner,
            )
            .await
            .expect("reopen the current promoter");
        let request = recovered
            .begin_owner_promotion(fixture.member_registration.clone())
            .await
            .expect("the current device must complete promotion after the cached attempt");
        if let Some(prior) = prior {
            assert_eq!(request.promotion_id, prior.promotion_id);
        }
        let member = fixture
            .store
            .bind_device(
                &fixture.member_db,
                fixture.member_db_store_dir.clone(),
                &fixture.member,
            )
            .await
            .expect("bind the promotion recipient");
        let acceptance = member
            .accept_owner_promotion(request.clone())
            .await
            .expect("accept the current device's request");
        recovered
            .finalize_owner_promotion(&fixture.encryption, acceptance)
            .await
            .expect("the current device must finish its promotion");
        let completed = database
            .load_owner_promotion_target(target_key(&fixture.member_registration).unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.promotion_id, request.promotion_id);
        assert!(matches!(
            completed.state,
            OwnerPromotionJournalState::Finalized { .. }
        ));
        assert!(database.active_store_publication().await.unwrap().is_none());
        recovered
            .publish_fixture_position("after-promotion-recovery")
            .await;
        let continuation = recovered
            .latest_local_store_position()
            .await
            .unwrap()
            .unwrap();
        let commit = recovered.load_commit_for_test(&continuation).await.unwrap();
        assert_eq!(commit.author_registration, recovered_registration);
    })
    .await;
}
