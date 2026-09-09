use super::*;

/// A device authors its Circle snapshots as one create-once stream of
/// generations, and a reader finds any generation only by walking that stream from
/// generation zero. Once a later generation is acknowledgement-stable and its cut
/// strictly dominates an earlier one, nobody will install the earlier image again
/// — so reclamation deletes that image while leaving the whole metadata chain, and
/// the surviving generation's own image, intact.
#[tokio::test]
async fn circle_snapshot_reclaim_deletes_a_superseded_generation_image() {
    let fixture = RotationFixture::build("circle-snapshot-generation-reclaim").await;
    let member_view = &fixture.member_device;
    member_view.pull().await;

    fixture
        .publish_covered_circle_package(member_view, "00000000-0000-4000-8000-0000000000d1")
        .await;
    let before = fixture.owner_circle_snapshot_stream().await;
    let (_, latest) = before
        .last()
        .expect("the owner authored a Circle snapshot")
        .clone();

    // Nothing supersedes the stream's latest generation, so reclamation refuses it
    // and its image stays readable.
    fixture
        .reclaim_packages()
        .await
        .expect("run reclamation with no superseding Circle snapshot generation");
    assert!(
        fixture
            .store
            .contains_circle_snapshot_image(fixture.circle_id, &latest)
            .await
            .expect("read the exact Circle snapshot image"),
        "the latest generation's image survives while nothing supersedes it"
    );

    // Publish Circle content past the acknowledged frontier and author a snapshot
    // over it. That generation strictly dominates the previous one, but no device
    // has acknowledged its cut — so it supersedes nothing and the earlier image
    // stays.
    fixture
        .capture_document(
            "00000000-0000-4000-8000-0000000000d3",
            Some(fixture.circle_id),
            "2026-07-23T00:30:00Z",
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
        .expect("publish Circle content past the acknowledged frontier");
    fixture
        .drive_circle_snapshots("2026-07-23T00:35:00Z")
        .await
        .expect("author an unacknowledged Circle snapshot generation");
    let unacknowledged = fixture.owner_circle_snapshot_stream().await;
    let (_, unstable) = unacknowledged
        .last()
        .expect("the owner authored a later Circle snapshot")
        .clone();
    assert!(
        unstable.generation > latest.generation
            && unstable
                .bootstrap
                .coverage
                .covers(&latest.bootstrap.coverage)
            && unstable.bootstrap.coverage != latest.bootstrap.coverage,
        "the unacknowledged generation's cut strictly dominates the earlier one"
    );
    fixture
        .reclaim_packages()
        .await
        .expect("run reclamation against an unacknowledged superseding generation");
    assert!(
        fixture
            .store
            .contains_circle_snapshot_image(fixture.circle_id, &latest)
            .await
            .expect("read the exact Circle snapshot image"),
        "a superseding generation nobody acknowledged does not release the earlier image"
    );

    // A second round of Circle content drives both devices to acknowledge the later
    // generations, so the once-latest generation is superseded.
    fixture
        .publish_covered_circle_package(member_view, "00000000-0000-4000-8000-0000000000d2")
        .await;
    fixture
        .reclaim_packages()
        .await
        .expect("reclaim the superseded Circle snapshot image");

    let after = fixture.owner_circle_snapshot_stream().await;
    let (_, newest) = after
        .last()
        .expect("the owner authored later Circle snapshots")
        .clone();
    assert!(
        newest.generation > latest.generation
            && newest.bootstrap.coverage.covers(&latest.bootstrap.coverage)
            && newest.bootstrap.coverage != latest.bootstrap.coverage,
        "a later generation's cut strictly dominates the earlier one"
    );
    assert!(
        !fixture
            .store
            .contains_circle_snapshot_image(fixture.circle_id, &latest)
            .await
            .expect("read the exact Circle snapshot image"),
        "the superseded generation's image is deleted"
    );
    assert!(
        fixture
            .store
            .contains_circle_snapshot_image(fixture.circle_id, &newest)
            .await
            .expect("read the exact Circle snapshot image"),
        "the generation no later snapshot supersedes keeps its image"
    );
    assert!(
        after
            .iter()
            .map(|(reference, _)| reference.generation)
            .collect::<Vec<_>>()
            .starts_with(
                &before
                    .iter()
                    .map(|(reference, _)| reference.generation)
                    .collect::<Vec<_>>()
            ),
        "every generation's metadata survives, so the stream stays walkable"
    );
}

#[tokio::test]
async fn circle_package_reclaim_deletes_a_snapshot_covered_package() {
    let fixture = RotationFixture::build("circle-package-reclaim").await;
    let member_view = &fixture.member_device;
    member_view.pull().await;

    let (circle_package, published) = fixture
        .publish_covered_circle_package(member_view, "00000000-0000-4000-8000-0000000000c1")
        .await;
    let owner_device = fixture
        .store
        .bind_device(&fixture.db, fixture.store_dir.clone(), &fixture.signer)
        .await
        .expect("bind Circle package replay Store");

    // A freshly published Circle package is a retained replay input: reclamation
    // refuses it until a superseding cut releases that ownership.
    assert!(
        owner_device
            .circle_package_is_retained_for_replay_for_test(
                circle_package.clone(),
                published.clone(),
            )
            .await
            .expect("read Circle package replay retention"),
        "a freshly published Circle package is retained for replay"
    );
    fixture.release_retained_replay_ownership().await;

    let result = fixture
        .reclaim_packages()
        .await
        .expect("reclaim the covered Circle package");
    assert!(
        result.packages_deleted >= 1,
        "reclamation deleted the snapshot-covered Circle package: {result:?}"
    );

    // The delete counted above required the production readback-absence check to
    // pass, so the ciphertext is gone from storage. Its ownership record is retired
    // and the materialized row is untouched.
    assert!(
        !owner_device
            .circle_package_is_retained_for_replay_for_test(
                circle_package.clone(),
                published.clone(),
            )
            .await
            .expect("read Circle package replay retention after reclaim"),
        "the reclaimed Circle package no longer has an ownership record"
    );
    let row_present = StoreDatabase::new(&fixture.db)
        .read(|sql| {
            sql.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM documents
                     WHERE id = '00000000-0000-4000-8000-0000000000c1'
                 )",
                [],
                |row| row.get::<_, bool>(0),
            )
            .map_err(DbError::from)
        })
        .await
        .expect("access the owner's documents projection")
        .expect("read the owner's documents projection");
    assert!(
        row_present,
        "reclamation leaves the materialized row intact"
    );
}

#[tokio::test]
async fn circle_package_reclaim_refuses_a_replay_retained_package() {
    let fixture = RotationFixture::build("circle-package-replay-retained").await;
    let member_view = &fixture.member_device;
    member_view.pull().await;

    let (circle_package, published) = fixture
        .publish_covered_circle_package(member_view, "00000000-0000-4000-8000-0000000000c2")
        .await;
    let owner_device = fixture
        .store
        .bind_device(&fixture.db, fixture.store_dir.clone(), &fixture.signer)
        .await
        .expect("bind replay-retained Circle package Store");

    // The snapshot covers the package and every device acknowledged its cut, but
    // the package is still a retained replay input: the per-Circle guard refuses
    // reclamation and the object survives.
    assert!(
        owner_device
            .circle_package_is_retained_for_replay_for_test(
                circle_package.clone(),
                published.clone(),
            )
            .await
            .expect("read Circle package replay retention"),
        "the Circle package is retained for replay"
    );
    // Reclamation may still delete the member's now-superseded seed bootstrap image
    // (the member advanced past it), but the replay-retained package itself is never
    // reclaimed while its ownership survives.
    fixture
        .reclaim_packages()
        .await
        .expect("run reclamation with the package still retained");
    assert!(
        owner_device
            .circle_package_is_retained_for_replay_for_test(circle_package, published)
            .await
            .expect("read Circle package replay retention after refused reclaim"),
        "the replay-retained Circle package still owns its object"
    );
}

#[tokio::test]
async fn circle_package_reclaim_verifies_a_cross_device_seeded_acknowledgement() {
    let fixture = RotationFixture::build("circle-package-seeded-ack").await;
    let circle_id = fixture.circle_id;
    let member_view = &fixture.member_device;
    member_view.pull().await;

    // The member's projection seeds from a real bootstrap coverage row the install
    // recorded — the coverage its acknowledgements will name.
    let member_coverage = StoreDatabase::new(&fixture.member_db)
        .circle_bootstrap_coverage_ref(circle_id)
        .await
        .expect("read member Circle bootstrap coverage")
        .expect("the member's projection seeded from a real bootstrap coverage row");

    let (circle_package, published) = fixture
        .publish_covered_circle_package(member_view, "00000000-0000-4000-8000-0000000000c3")
        .await;

    // The member's activated acknowledgement names its exact seed coverage — the
    // cross-device evidence the owner reads and dominates to prove stability.
    let member_device_id = fixture
        .member_db
        .local_store_device_id_for_test()
        .await
        .expect("read local Store device id");
    let member_ack_ref = StoreDatabase::new(&fixture.db)
        .activated_circle_ack(circle_id, member_device_id)
        .await
        .expect("read activated member acknowledgement")
        .expect("the owner activated the member acknowledgement");
    let member_ack = fixture
        .store
        .load_circle_acknowledgement(&fixture.db, fixture.store_dir.clone(), &member_ack_ref)
        .await
        .expect("owner reads the member acknowledgement");
    assert_eq!(
        member_ack.seeded_from.as_ref(),
        Some(&member_coverage),
        "the member's acknowledgement names its exact seed coverage row"
    );

    // Reclamation proceeds only because the owner could read and dominate that
    // seed-anchored cross-device acknowledgement.
    fixture.release_retained_replay_ownership().await;
    let result = fixture
        .reclaim_packages()
        .await
        .expect("reclaim after cross-device verifying the seeded acknowledgement");
    assert!(
        result.packages_deleted >= 1,
        "reclamation proceeded on the strength of the member's seeded acknowledgement: {result:?}"
    );
    let owner_device = fixture
        .store
        .bind_device(&fixture.db, fixture.store_dir.clone(), &fixture.signer)
        .await
        .expect("bind reclaimed Circle package Store");
    assert!(
        !owner_device
            .circle_package_is_retained_for_replay_for_test(circle_package, published)
            .await
            .expect("read Circle package replay retention after reclaim"),
        "the reclaimed Circle package no longer owns its object"
    );
}

#[tokio::test]
async fn two_circle_recipients_never_share_one_bootstrap_image() {
    // A bootstrap image's storage path is keyed by the recipient's slot, so two
    // recipients bootstrapped from the same underlying snapshot cut land on two
    // DISTINCT image objects, each owned by exactly the one add-member commit that
    // activated it. Sharing one image between recipients is therefore structurally
    // impossible, and the single-owner requirement in the reclaim eligibility check
    // (`validate_reclaimable_circle_bootstrap_image`) is always satisfiable: deleting
    // one recipient's seed can never remove another recipient's.
    let fixture = RotationFixture::build("circle-bootstrap-two-recipients").await;
    let circle_id = fixture.circle_id;
    let member_view = &fixture.member_device;
    member_view.pull().await;

    let first_image = fixture.member_seed_image(circle_id).await;
    assert_eq!(
        fixture.bootstrap_image_owner_count(&first_image).await,
        1,
        "the first recipient's seed image has exactly one activating owner"
    );

    // Onboard a second Store member and add it to the same Circle. Its bootstrap is
    // cut from the Circle's current content, the same underlying state the first
    // recipient was seeded from.
    let second = UserKeypair::generate();
    let second_pubkey = keys::public_key_hex(&second);
    fixture
        .store
        .admit_member(
            &fixture.db,
            fixture.store_dir.clone(),
            &fixture.signer,
            &second_pubkey,
            None,
            MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            "Rotation Store",
        )
        .await
        .expect("admit the second Store member");
    let second_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let second_db = open_circle_routing_test_db(second_db_store_dir.clone());
    fixture
        .store
        .activate_joined_device(
            &fixture.db,
            fixture.store_dir.clone(),
            &second_db,
            second_db_store_dir,
            &second,
            "2026-07-25T02:00:00Z",
        )
        .await
        .expect("activate the second member device");
    fixture
        .components
        .add_circle_member(circle_id, second_pubkey.clone(), CircleRole::Member)
        .await
        .expect("add the second Circle member");
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("activate the second member's access");

    // Two recipients, two distinct image objects, each with exactly one owner.
    let images = fixture.live_bootstrap_images().await;
    assert_eq!(
        images.len(),
        2,
        "each recipient has its own bootstrap image: {images:?}"
    );
    let objects = images
        .iter()
        .map(|(object, _)| object.slot().logical_key().to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        objects.len(),
        2,
        "the two recipients' bootstrap images are distinct objects: {objects:?}"
    );
    for (object, owners) in &images {
        assert_eq!(
            *owners,
            1,
            "bootstrap image {} is owned by exactly one activating commit",
            object.slot().logical_key()
        );
    }
    assert!(
        images.iter().any(|(object, _)| *object == first_image),
        "the first recipient's seed image is untouched by the second recipient's activation"
    );
}

#[tokio::test]
async fn circle_bootstrap_reclaim_unblocks_when_recipient_advances_past_its_seed() {
    let fixture = RotationFixture::build("circle-bootstrap-reclaim-advance").await;
    let circle_id = fixture.circle_id;
    let member_view = &fixture.member_device;
    member_view.pull().await;

    let image_object = fixture.member_seed_image(circle_id).await;
    assert!(
        fixture.bootstrap_image_present(&image_object).await,
        "the recipient's seed image exists before reclamation"
    );

    // No later Circle snapshot supersedes the recipient's seed yet, so reclamation
    // leaves the image in place.
    fixture
        .reclaim_packages()
        .await
        .expect("run reclamation before a later snapshot exists");
    assert!(
        fixture.bootstrap_image_present(&image_object).await,
        "the seed image survives while no later sufficient snapshot supersedes it"
    );

    // The member pulls a later Circle package and every active-access device
    // acknowledges a stable snapshot whose cut strictly dominates the seed. The
    // recipient has moved to a later sufficient snapshot, so its seed is reclaimable.
    fixture
        .publish_covered_circle_package(member_view, "00000000-0000-4000-8000-0000000000b1")
        .await;
    fixture
        .reclaim_packages()
        .await
        .expect("reclaim the superseded seed image");
    assert!(
        !fixture.bootstrap_image_present(&image_object).await,
        "the seed image is reclaimed once a later sufficient snapshot supersedes it"
    );
}

#[tokio::test]
async fn circle_bootstrap_reclaim_unblocks_when_recipient_loses_authority() {
    let fixture = RotationFixture::build("circle-bootstrap-reclaim-removed").await;
    let circle_id = fixture.circle_id;
    let member_view = &fixture.member_device;
    member_view.pull().await;

    let image_object = fixture.member_seed_image(circle_id).await;

    // The member acknowledges its seed (naming its seed coverage) and the Owner
    // activates that acknowledgement — the exact evidence the Owner reads after the
    // member is removed to prove which seed the member held. No later snapshot is
    // authored, so while the member still holds access its seed is not superseded
    // and the automatic reclamation in the cycle leaves the image in place.
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
        .expect("owner activates the member acknowledgement");
    assert!(
        fixture.bootstrap_image_present(&image_object).await,
        "an active member's seed survives while no later snapshot supersedes it"
    );

    // Remove the member from the Circle; the epoch closes and a successor control
    // activates whose roster excludes the member.
    fixture.close_epoch_by_removing_the_circle_member().await;
    assert!(
        !StoreDatabase::new(&fixture.db)
            .circle_current_roster_members(circle_id)
            .await
            .expect("read successor roster")
            .contains(&fixture.member_pubkey),
        "the removed member is absent from the successor roster"
    );

    // The removed member lost authority under the activated successor control: its
    // seed image is reclaimed, re-verified from its own signed acknowledgement.
    fixture
        .reclaim_packages()
        .await
        .expect("reclaim the removed member's seed image");
    assert!(
        !fixture.bootstrap_image_present(&image_object).await,
        "the removed member's seed image is reclaimed under the successor control"
    );
}

#[tokio::test]
async fn store_membership_revocation_cascades_into_bootstrap_reclaim() {
    // Revoking a recipient's STORE membership does not by itself exclude it from the
    // Circle roster: it marks the Circle rotation-required and waits. The operator's
    // Circle-member removal then closes the epoch, and the successor roster — the one
    // piece of evidence the lost-authority arm reads — omits the identity. This proves
    // the Store-revocation trigger reaches the same roster-exclusion evidence rather
    // than a second, separate authority path.
    let fixture = RotationFixture::build("circle-bootstrap-store-revocation").await;
    let circle_id = fixture.circle_id;
    let member_view = &fixture.member_device;
    member_view.pull().await;
    let image_object = fixture.member_seed_image(circle_id).await;

    // The recipient acknowledges its seed: the signed evidence naming the exact
    // coverage the Owner will later delete.
    member_view
        .publish_acknowledgements("2026-07-25T03:00:00Z")
        .await;
    fixture
        .components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("owner activates the member acknowledgement");

    // Revoke the recipient's Store membership. The Circle becomes rotation-required
    // but its roster still names the identity, so no lost-authority evidence exists
    // yet and the seed image survives.
    fixture.remove_store_member().await;
    assert!(
        fixture
            .circles()
            .await
            .iter()
            .find(|circle| circle.id() == circle_id)
            .expect("affected Circle listed after Store removal")
            .rotation_required(),
        "revoking Store membership marks the Circle rotation-required"
    );
    assert!(
        StoreDatabase::new(&fixture.db)
            .circle_current_roster_members(circle_id)
            .await
            .expect("read roster after Store revocation")
            .contains(&fixture.member_pubkey),
        "Store revocation alone does not yet exclude the identity from the Circle roster"
    );
    fixture
        .reclaim_packages()
        .await
        .expect("run reclamation while the roster still names the identity");
    assert!(
        fixture.bootstrap_image_present(&image_object).await,
        "Store revocation alone does not reclaim the recipient's seed image"
    );

    // Completing the cascade — the Circle-member removal that clears rotation —
    // activates a successor control whose roster omits the identity. That is the same
    // evidence the lost-authority arm consumes, and the seed image is now reclaimed.
    fixture.close_epoch_by_removing_the_circle_member().await;
    assert!(
        !StoreDatabase::new(&fixture.db)
            .circle_current_roster_members(circle_id)
            .await
            .expect("read successor roster")
            .contains(&fixture.member_pubkey),
        "the cascade excludes the revoked identity from the successor roster"
    );
    assert!(
        !fixture.bootstrap_image_present(&image_object).await,
        "the revoked identity's seed image is reclaimed once the cascade completes"
    );
}
