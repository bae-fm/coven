use super::*;

#[tokio::test]
async fn circle_acknowledgement_stays_readable_across_epoch_rotation() {
    let fixture = RotationFixture::build("rotation-ack-read").await;
    let owner_pk = keys::public_key_hex(&fixture.signer);

    // A cycle publishes the owner's Circle acknowledgement under the current
    // (soon-rotated-away) epoch.
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("cycle publishes the owner's Circle acknowledgement");
    let (old_authoring, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &owner_pk)
        .await
        .expect("old Circle authoring context");
    let old_control = old_authoring.control.coord.clone();
    let old_epoch = old_authoring.control.value.epoch_id();
    let acknowledgements = StoreDatabase::new(&fixture.db)
        .activated_circle_acks(fixture.circle_id)
        .await
        .expect("read activated Circle acknowledgements");
    let ack_ref = acknowledgements
        .first()
        .cloned()
        .expect("the owner published a Circle acknowledgement");
    let before = fixture
        .store
        .load_circle_acknowledgement(&fixture.db, fixture.store_dir.clone(), &ack_ref)
        .await
        .expect("read acknowledgement through its exact control");
    assert_eq!(before.epoch_id, old_epoch);
    assert_eq!(before.control, old_control);
    // The owner authored the Circle; its projection never came from an image, so
    // the acknowledgement names no seed coverage.
    assert!(before.seeded_from.is_none());

    // Remove the roster member: the old epoch closes and a successor epoch/key
    // activates.
    fixture.remove_store_member().await;
    fixture.close_epoch_by_removing_the_circle_member().await;
    let (new_authoring, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &owner_pk)
        .await
        .expect("successor Circle authoring context");
    let new_control = new_authoring.control.coord.clone();
    assert_ne!(new_control, old_control, "the epoch rotated");

    // The pre-rotation acknowledgement, sealed under the rotated-away epoch key,
    // stays readable after the epoch rotates: the read resolves that epoch's key
    // from the retained activation of the control the acknowledgement names.
    let after = fixture
        .store
        .load_circle_acknowledgement(&fixture.db, fixture.store_dir.clone(), &ack_ref)
        .await
        .expect("read the pre-rotation acknowledgement after the epoch rotated");
    assert_eq!(after.epoch_id, old_epoch);
    assert_eq!(after.control, old_control);
}

#[tokio::test]
async fn circle_snapshot_stability_requires_every_access_device_to_acknowledge() {
    let fixture = RotationFixture::build("snapshot-stability").await;

    // Author a Circle snapshot before any device has acknowledged coverage.
    fixture
        .author_standalone_circle_snapshot("2026-07-23T00:00:00Z")
        .await;
    let published = StoreDatabase::new(&fixture.db)
        .latest_local_circle_snapshot(fixture.circle_id)
        .await
        .expect("read published Circle snapshot")
        .expect("a Circle snapshot was published");

    // The owner acknowledges its own coverage past the cut. The second member's
    // device holds active Circle access but has not acknowledged, so the snapshot
    // is not stable: an access-holding device that never acknowledged keeps the
    // snapshot unusable as coverage evidence.
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("owner publishes its Circle acknowledgement");
    assert!(!fixture
        .store
        .circle_snapshot_is_stable(
            &fixture.db,
            fixture.store_dir.clone(),
            fixture.circle_id,
            &published.cut
        )
        .await
        .expect("evaluate stability before the member acknowledges"));

    // The member device installs the Circle bootstrap and publishes its own Circle
    // acknowledgement covering the cut; the owner pulls and activates it.
    let member_view = &fixture.member_device;
    member_view.pull().await;
    member_view
        .publish_acknowledgements("2026-07-23T01:00:00Z")
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("owner activates the member's Circle acknowledgement");

    // Every access-holding device has now acknowledged coverage past the cut.
    assert!(fixture
        .store
        .circle_snapshot_is_stable(
            &fixture.db,
            fixture.store_dir.clone(),
            fixture.circle_id,
            &published.cut
        )
        .await
        .expect("evaluate stability once every access device acknowledged"));
}

#[tokio::test]
async fn circle_snapshot_stays_readable_across_epoch_rotation() {
    let fixture = RotationFixture::build("rotation-snapshot-read").await;
    let owner_pk = keys::public_key_hex(&fixture.signer);
    let (old_authoring, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &owner_pk)
        .await
        .expect("old Circle authoring context");
    let old_control = old_authoring.control.coord.clone();
    let old_epoch = old_authoring.control.value.epoch_id();

    // Author a Circle snapshot under the current (soon-rotated-away) epoch.
    fixture
        .author_standalone_circle_snapshot("2026-07-23T00:00:00Z")
        .await;

    // Rotate the epoch by removing the roster member.
    fixture.remove_store_member().await;
    fixture.close_epoch_by_removing_the_circle_member().await;

    // The old-epoch snapshot, sealed under the rotated-away key, stays readable
    // to a current member: resolve the key from the retained activation of the
    // control the snapshot names.
    let device = fixture
        .store
        .bind_device(&fixture.db, fixture.store_dir.clone(), &fixture.signer)
        .await
        .expect("bind retained Circle activation Store");
    let retained = device
        .circle_epoch_access(fixture.circle_id, old_control.clone())
        .await
        .expect("read retained Circle activation")
        .expect("the pre-rotation control's activation is retained");
    let metas = fixture
        .store
        .load_circle_snapshot_metas(
            &fixture.db,
            fixture.store_dir.clone(),
            fixture.circle_id,
            &retained,
        )
        .await
        .expect("read the pre-rotation Circle snapshot after the epoch rotated");
    let old = metas
        .iter()
        .find(|meta| meta.epoch_id == old_epoch)
        .expect("the pre-rotation snapshot remains readable");
    assert_eq!(old.control, old_control);
}

/// A standalone Circle snapshot carries row-routing ids in its image, and those
/// ids must be derived from the Store generation-one key — the key that routed
/// the rows when the host captured them — not the per-Circle epoch key that only
/// seals the published objects. This authors a snapshot over Circle content and
/// checks that a recipient authenticates its routing state against the true Store
/// routing key. When authoring derived routing from the Circle epoch key instead,
/// the projection's routes failed to authenticate against the Store-keyed rows and
/// no image could be authored at all.
#[tokio::test]
async fn standalone_circle_snapshot_authenticates_under_the_true_store_routing_key() {
    let fixture = RotationFixture::build("standalone-snapshot-true-routing").await;
    let owner_pk = keys::public_key_hex(&fixture.signer);

    // Circle content captured through the host write path, routed with the Store key.
    fixture
        .capture_document(
            "00000000-0000-4000-8000-000000000099",
            Some(fixture.circle_id),
            "0000000002000-0000-owner",
        )
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish Circle content");

    fixture
        .author_standalone_circle_snapshot("2026-07-24T00:00:00Z")
        .await;

    // A recipient reads the image with the Circle epoch key and authenticates its
    // routing state against the true Store routing key.
    let (authoring, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(fixture.circle_id, &owner_pk)
        .await
        .expect("Circle authoring context");
    let access = StoreDatabase::new(&fixture.db)
        .circle_publication_context(fixture.circle_id, authoring.control.coord.clone())
        .await
        .expect("Circle publication context");
    fixture
        .store
        .verify_standalone_circle_snapshot_image(
            &fixture.db,
            fixture.store_dir.clone(),
            fixture.circle_id,
            &access,
            &EncryptionService::from_key([42; 32]),
        )
        .await
        .expect("standalone Circle snapshot authenticates under the true Store routing key");
}

/// Removing a roster member closes the Circle epoch and rotates its key away from
/// the generation-one founding key, so the rotated Circle key has no generation-one
/// entry to derive a row-routing key from. Standalone snapshot authoring must still
/// succeed, because it derives routing from the Store generation-one key — which an
/// epoch close never touches. Deriving routing from the rotated Circle key errored
/// `MissingGenerationOne`, killing snapshot authoring for the Circle every cycle
/// after the close.
#[tokio::test]
async fn standalone_circle_snapshot_authoring_survives_epoch_rotation() {
    let fixture = RotationFixture::build("standalone-snapshot-rotation").await;

    fixture
        .capture_document(
            "00000000-0000-4000-8000-000000000099",
            Some(fixture.circle_id),
            "0000000002000-0000-owner",
        )
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish pre-close Circle content");

    // Close the epoch by removing the roster member, rotating the Circle key.
    fixture.close_epoch_by_removing_the_circle_member().await;

    // Authoring derives routing from the Store generation-one key, so the rotated
    // Circle key having no generation-one entry no longer aborts the capture.
    fixture
        .author_standalone_circle_snapshot("2026-07-24T00:00:00Z")
        .await;

    assert!(
        StoreDatabase::new(&fixture.db)
            .latest_local_circle_snapshot(fixture.circle_id)
            .await
            .expect("read the published Circle snapshot")
            .is_some(),
        "a standalone Circle snapshot is published after the rotation"
    );
}

#[tokio::test]
async fn member_circle_acknowledgement_names_its_seed_bootstrap_coverage() {
    let fixture = RotationFixture::build("circle-ack-seeded-from").await;
    let circle_id = fixture.circle_id;

    // The member device installs the Circle bootstrap: its projection seeds from a
    // real coverage row the install recorded.
    let member_view = &fixture.member_device;
    member_view.pull().await;
    let member_coverage = StoreDatabase::new(&fixture.member_db)
        .circle_bootstrap_coverage_ref(circle_id)
        .await
        .expect("read member Circle bootstrap coverage")
        .expect("the member's projection seeded from a real bootstrap coverage row");

    // The member publishes its Circle acknowledgement; the owner pulls and
    // activates it alongside its own.
    member_view
        .publish_acknowledgements("2026-07-23T01:00:00Z")
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("owner activates the member's Circle acknowledgement");

    // The owner reads and verifies the member's acknowledgement: its seed coverage
    // is present and names the exact bootstrap coverage row the member installed.
    let member_device_id = fixture
        .member_db
        .local_store_device_id_for_test()
        .await
        .expect("read local Store device id");
    let member_ack_ref = StoreDatabase::new(&fixture.db)
        .activated_circle_ack(circle_id, member_device_id)
        .await
        .expect("read activated member Circle acknowledgement")
        .expect("the owner activated the member's Circle acknowledgement");
    let member_ack = fixture
        .store
        .load_circle_acknowledgement(&fixture.db, fixture.store_dir.clone(), &member_ack_ref)
        .await
        .expect("owner reads the member's Circle acknowledgement");
    assert_eq!(
        member_ack.seeded_from.as_ref(),
        Some(&member_coverage),
        "the member's acknowledgement names its exact seed coverage row"
    );

    // The founder authored the Circle; its projection never came from an image, so
    // its own acknowledgement names no seed coverage.
    let owner_device_id = fixture
        .db
        .local_store_device_id_for_test()
        .await
        .expect("read local Store device id");
    let owner_ack_ref = StoreDatabase::new(&fixture.db)
        .activated_circle_ack(circle_id, owner_device_id)
        .await
        .expect("read activated owner Circle acknowledgement")
        .expect("the owner activated its own Circle acknowledgement");
    let owner_ack = fixture
        .store
        .load_circle_acknowledgement(&fixture.db, fixture.store_dir.clone(), &owner_ack_ref)
        .await
        .expect("owner reads its own Circle acknowledgement");
    assert!(
        owner_ack.seeded_from.is_none(),
        "the founder's acknowledgement names no seed coverage"
    );
}

#[tokio::test]
async fn a_removed_member_cannot_read_a_successor_epoch_circle_snapshot() {
    let fixture = RotationFixture::build("successor-snapshot-unreadable").await;
    let circle_id = fixture.circle_id;
    let owner_pk = keys::public_key_hex(&fixture.signer);

    // The member installs the Circle bootstrap and holds the current epoch key.
    let member_view = &fixture.member_device;
    member_view.pull().await;

    // Close the epoch by removing the member from the Circle; the owner drives the
    // close to successor activation. The member stays a Store member.
    fixture.close_epoch_by_removing_the_circle_member().await;

    let (successor, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(circle_id, &owner_pk)
        .await
        .expect("successor Circle authoring context");
    let successor_control = successor.control.coord.clone();

    // A successor-epoch Circle snapshot is sealed under the successor epoch key.
    // The owner, a remaining member, resolves that key from its retained
    // activation — so it could author and read such a snapshot.
    let owner_device = fixture
        .store
        .bind_device(&fixture.db, fixture.store_dir.clone(), &fixture.signer)
        .await
        .expect("bind owner successor Circle Store");
    assert!(
        owner_device
            .circle_epoch_access(circle_id, successor_control.clone())
            .await
            .expect("read owner successor access")
            .is_some(),
        "the owner resolves the successor epoch key"
    );

    // The removed member pulls the post-close state but never received the
    // successor epoch key: it cannot resolve the key that seals a successor-epoch
    // snapshot, so any such snapshot is unreadable to it.
    member_view.pull().await;
    let member_device = fixture
        .store
        .bind_device(
            &fixture.member_db,
            fixture.member_device.store_dir.clone(),
            &fixture.member,
        )
        .await
        .expect("bind removed member Circle Store");
    assert!(
        member_device
            .circle_epoch_access(circle_id, successor_control)
            .await
            .expect("read member successor access")
            .is_none(),
        "the removed member cannot resolve the successor epoch key"
    );
}

#[tokio::test]
async fn circle_snapshot_publication_resumes_idempotently_across_upload_boundaries() {
    // Derive the number of exact object creates one Circle snapshot publication
    // makes (the image, then its metadata) from a clean run rather than hardcoding
    // it, then use the metadata create as the crash boundary.
    let baseline = RotationFixture::build("snapshot-resume-baseline").await;
    let before = baseline.home.exact_create_count();
    baseline
        .drive_circle_snapshots("2026-07-23T00:00:00Z")
        .await
        .expect("clean Circle snapshot publication");
    let meta_create = baseline.home.exact_create_count() - before;
    assert_eq!(
        meta_create, 2,
        "a blobless Circle snapshot uploads an image, then its metadata"
    );
    assert!(
        StoreDatabase::new(&baseline.db)
            .latest_local_circle_snapshot(baseline.circle_id)
            .await
            .expect("read baseline snapshot")
            .is_some(),
        "the clean publication completes"
    );

    // Boundary — image upload to metadata publication: the image is uploaded but
    // the metadata upload is interrupted before its bytes land. The publication is
    // left durable and pending with no completed snapshot; the caller receives the
    // failure. The next run resumes it and completes it exactly once,
    // reopening the retained image payload while its upload marker skips publication.
    {
        let fixture = RotationFixture::build("snapshot-resume-image-meta").await;
        let circle_id = fixture.circle_id;
        fixture.home.fail_exact_create_before_call(meta_create);
        let error = fixture
            .drive_circle_snapshots("2026-07-23T00:00:00Z")
            .await
            .expect_err("the interrupted publication fails to its initiator");
        assert!(
            crate::sync::error::error_chain_contains_transport(&error),
            "{error}"
        );
        assert!(
            fixture.latest_circle_snapshot(circle_id).await.is_none(),
            "no snapshot completes when the metadata upload is interrupted"
        );
        assert!(
            fixture.pending_circle_snapshot(circle_id).await.is_some(),
            "the interrupted publication remains durable for resume"
        );

        fixture
            .drive_circle_snapshots("2026-07-23T00:00:01Z")
            .await
            .expect("resume completes the pending publication");
        assert!(
            fixture.latest_circle_snapshot(circle_id).await.is_some(),
            "the resumed publication completes"
        );
        assert!(
            fixture.pending_circle_snapshot(circle_id).await.is_none(),
            "no durable publication remains after resume"
        );
        assert_eq!(
            fixture
                .latest_circle_snapshot(circle_id)
                .await
                .map(|snapshot| snapshot.reference.generation),
            Some(0),
            "the resume opens no second generation"
        );
    }

    // Boundary — metadata publication to completion: the metadata bytes are durable
    // but the upload response is lost before the publication is recorded complete.
    // The provider's exact-upload verification settles the lost upload within the
    // run, so the publication completes without duplication and a re-run opens no
    // new generation.
    {
        let fixture = RotationFixture::build("snapshot-resume-meta-complete").await;
        let circle_id = fixture.circle_id;
        fixture.home.fail_exact_create_after_call(meta_create);
        fixture
            .drive_circle_snapshots("2026-07-23T00:00:00Z")
            .await
            .expect("the lost metadata-upload response is settled by provider verification");
        assert_eq!(
            fixture
                .latest_circle_snapshot(circle_id)
                .await
                .map(|snapshot| snapshot.reference.generation),
            Some(0),
            "the settled publication completes exactly one generation"
        );
        assert!(
            fixture.pending_circle_snapshot(circle_id).await.is_none(),
            "no durable publication remains after the settled upload"
        );

        fixture
            .drive_circle_snapshots("2026-07-23T00:00:01Z")
            .await
            .expect("a re-run is idempotent");
        assert_eq!(
            fixture
                .latest_circle_snapshot(circle_id)
                .await
                .map(|snapshot| snapshot.reference.generation),
            Some(0),
            "the re-run duplicates no generation"
        );
        assert!(
            fixture.pending_circle_snapshot(circle_id).await.is_none(),
            "the idempotent re-run opens no new publication"
        );
    }
}

#[tokio::test]
async fn circle_package_reclaim_reads_an_acknowledgement_sealed_under_a_rotated_epoch() {
    // Each acknowledgement reference names the control that resolves its epoch
    // key. After rotation, a pre-rotation acknowledgement remains readable through
    // that retained exact control.
    let fixture = RotationFixture::build("circle-reclaim-rotated-ack").await;
    let circle_id = fixture.circle_id;
    let owner_pk = keys::public_key_hex(&fixture.signer);

    // The owner publishes its Circle acknowledgement under the current (soon-rotated)
    // epoch.
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("owner acknowledges under the pre-rotation epoch");
    let (old_authoring, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(circle_id, &owner_pk)
        .await
        .expect("pre-rotation Circle authoring context");
    let old_control = old_authoring.control.coord.clone();
    let old_epoch = old_authoring.control.value.epoch_id();
    let owner_device_id = fixture
        .db
        .local_store_device_id_for_test()
        .await
        .expect("read local Store device id");
    let owner_ack_ref = StoreDatabase::new(&fixture.db)
        .activated_circle_ack(circle_id, owner_device_id)
        .await
        .expect("read owner activated acknowledgement")
        .expect("the owner published a Circle acknowledgement");

    // Rotate the epoch: remove the roster member and finalize the close.
    fixture.remove_store_member().await;
    fixture.close_epoch_by_removing_the_circle_member().await;
    let (new_authoring, _) = StoreDatabase::new(&fixture.db)
        .circle_authoring_context(circle_id, &owner_pk)
        .await
        .expect("successor Circle authoring context");
    let new_control = new_authoring.control.coord.clone();
    assert_ne!(new_control, old_control, "the epoch rotated");

    // The reclaim ack reader resolves each acknowledgement's epoch key from the
    // retained activation of the control it names — not from a live keyring. After
    // the epoch rotates, the old control stays retained, so the reader (the exact
    // path reclaim stability uses) still reads the pre-rotation acknowledgement.
    let acknowledgement = fixture
        .store
        .load_circle_acknowledgement(&fixture.db, fixture.store_dir.clone(), &owner_ack_ref)
        .await
        .expect("reclaim reads a rotated-epoch acknowledgement via its retained control");
    assert_eq!(
        acknowledgement.epoch_id, old_epoch,
        "the acknowledgement was sealed under the rotated-away epoch"
    );
}
