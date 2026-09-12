use super::*;

/// A stable, acknowledged Store snapshot of a Circle store with one active
/// member, cut without an epoch close (so Circle history stays live and no
/// coverage is reclaimed). The shared base for the restore-selection cases that
/// differ only in who restores and what the storage provider serves.
/// A Store with an activated founder Circle, one admitted Store member on that
/// Circle's roster, and the production sync components the owner drives. Every
/// restore case starts from this shape and differs only in what it does next.
struct CircleWithOneMember {
    db: Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    store: std::sync::Arc<TestStore>,
    home: Arc<coven_storage::InMemoryCloudHome>,
    signer: UserKeypair,
    components: SyncComponents,
    circle_id: CircleId,
    member: UserKeypair,
    member_pubkey: String,
}

impl CircleWithOneMember {
    async fn build(name: &str) -> Self {
        let routing = EncryptionService::from_key([42; 32]);
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = open_circle_routing_test_db(db_store_dir.clone());
        let (store, home, signer, founder) =
            persist_merge_operation(&db, db_store_dir.clone(), name).await;
        let circle_id = founder.circle_id();
        store
            .bind_device_in(&db, db_store_dir.clone(), &signer)
            .await
            .expect("bind Circle test Store")
            .resume_circle_operations()
            .await
            .expect("activate founder transition");

        let member = UserKeypair::generate();
        let member_pubkey = keys::public_key_hex(&member);
        store
            .admit_member(
                &db,
                db_store_dir.clone(),
                &signer,
                &member_pubkey,
                None,
                MemberRole::Member,
                &routing,
                "Restore Store",
            )
            .await
            .expect("admit Store member");
        let components = prepare_owner_sync_components(
            &db,
            &store,
            &home,
            &db_store_dir,
            &signer,
            name,
            circle_test_custody(),
        )
        .await;
        components
            .add_circle_member(circle_id, member_pubkey.clone(), CircleRole::Member)
            .await
            .expect("add Circle member");

        Self {
            db,
            db_store_dir,
            store,
            home,
            signer,
            components,
            circle_id,
            member,
            member_pubkey,
        }
    }
}

/// Publishes a Store snapshot over the current frontier and acknowledges it
/// stable from the sole owner device, then reads the membership chain a restore
/// from that snapshot is floored against.
async fn publish_acknowledged_store_snapshot(
    store: &TestStore,
    db: &Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    signer: &UserKeypair,
    routing: &EncryptionService,
    cut_stamp: &str,
    acknowledged_at: &str,
) -> coven_protocol::membership::MembershipChain {
    let loaded_store = store
        .bind_device_in(db, db_store_dir.clone(), signer)
        .await
        .expect("load the Store snapshot");
    let mut authorized = loaded_store
        .authorize_writer()
        .await
        .expect("authorize the Store snapshot");
    let cut = authorized
        .snapshots()
        .capture_snapshot_cut(Some(routing))
        .await
        .expect("capture the Store snapshot cut");
    let coverage = cut.coverage().clone();
    authorized
        .snapshots()
        .push_snapshot_cut(cut, cut_stamp.to_string())
        .await
        .expect("publish the Store snapshot");
    loaded_store
        .stage_acknowledgement(coverage, acknowledged_at.to_string())
        .await
        .expect("stage snapshot stability acknowledgement");
    loaded_store
        .drain_acknowledgements()
        .await
        .expect("activate snapshot stability acknowledgement");

    store
        .bind_device_in(db, db_store_dir.clone(), signer)
        .await
        .expect("load snapshot Store")
        .membership_for_test()
        .await
        .expect("load membership for snapshot restore")
}

/// The fresh directory a Store snapshot restores into. The temp dir must outlive
/// the restore, and both the database path and the Store dir are read from it.
struct RestoreTarget {
    _temp: tempfile::TempDir,
    database_path: std::path::PathBuf,
    store_dir: coven_foundation::store_dir::StoreDir,
}

impl RestoreTarget {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("restore destination");
        Self {
            database_path: temp.path().join("store.db"),
            store_dir: coven_foundation::store_dir::StoreDir::new_ephemeral(temp.path()),
            _temp: temp,
        }
    }
}

/// The payload spool files a store directory holds, by path and content, so a
/// test can say exactly which files an attempt added or left alone.
fn payload_spool_files(
    store_dir: &coven_foundation::store_dir::StoreDir,
) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    let spool = store_dir.payload_spool_dir();
    let Ok(entries) = std::fs::read_dir(&spool) else {
        return std::collections::BTreeMap::new();
    };
    entries
        .map(|entry| {
            let path = entry.expect("read payload spool entry").path();
            let bytes = std::fs::read(&path).expect("read payload spool file");
            (path, bytes)
        })
        .collect()
}

/// Asserts that a failed restore left the destination exactly as it found it:
/// no database, no SQLite sidecars, and no payload file it wrote.
fn assert_restore_left_nothing(
    target: &RestoreTarget,
    before: &std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
) {
    for suffix in ["", "-wal", "-shm"] {
        let path = std::path::PathBuf::from(format!("{}{suffix}", target.database_path.display()));
        assert!(
            !path.exists(),
            "a failed restore left {} behind",
            path.display()
        );
    }
    assert_eq!(
        payload_spool_files(&target.store_dir),
        *before,
        "a failed restore changed the destination payload spool"
    );
}

/// Restores the Store snapshot as `restorer` and installs it into `target`. The
/// preparation is expected to verify; the install outcome is the caller's, since
/// the failure cases are exactly what several of these tests assert on.
async fn restore_store_snapshot<'a>(
    store: &'a TestStore,
    db: &Database,
    membership: &coven_protocol::membership::MembershipChain,
    restorer: &UserKeypair,
    target: &'a RestoreTarget,
    device_id: &str,
) -> Result<crate::sync::store::RestoringStore<'a>, crate::sync::store::SnapshotError> {
    store
        .prepare_snapshot_bootstrap(
            &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
            db.schema_version(),
            &target.database_path,
            restorer,
        )
        .await
        .expect("restore the Store snapshot")
        .install(
            &target.store_dir,
            circle_routing_tables(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            device_id.to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &circle_routing_migrations(),
            coven_database::CovenMigrationPolicy::ApplyPending,
            Some(&EncryptionService::from_key([42; 32])),
        )
        .await
}

struct ActiveMemberCircleSnapshot {
    db: Database,
    store: std::sync::Arc<TestStore>,
    home: Arc<coven_storage::InMemoryCloudHome>,
    signer: UserKeypair,
    member: UserKeypair,
    routing: EncryptionService,
    circle_id: coven_protocol::circle::CircleId,
    membership: coven_protocol::membership::MembershipChain,
}

/// How much Circle history the fixture builds before the Store snapshot cut.
enum CircleFixtureMode {
    /// Live Circle content, no epoch close — no image is reclaimed.
    Live,
    /// The epoch is closed by removing the member; the successor bootstrap covers
    /// the pre-close content and the remaining owner's leaf names it.
    Closed,
}

impl ActiveMemberCircleSnapshot {
    async fn build(name: &str, mode: CircleFixtureMode) -> Self {
        let routing = EncryptionService::from_key([42; 32]);
        let CircleWithOneMember {
            db,
            db_store_dir,
            store,
            home,
            signer,
            components,
            circle_id,
            member,
            member_pubkey,
        } = CircleWithOneMember::build(name).await;

        // Pre-close Circle content the member holds access to, under the live epoch.
        db.capture_document_for_test(
            "00000000-0000-4000-8000-000000000090",
            Some(circle_id),
            "0000000002000-0000-owner",
        )
        .await
        .expect("capture Circle content");
        components
            .run_cycle(
                &coven_foundation::clock::SystemClock,
                None,
                coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
            )
            .await
            .expect("publish pre-close Circle content");

        // Close the epoch by removing the member. The successor bootstrap's activating
        // commit is retained, so the restored device resolves the head control and the
        // remaining owner's own leaf names that successor bootstrap image (in storage)
        // as its Circle image.
        if matches!(mode, CircleFixtureMode::Closed) {
            components
                .remove_circle_member(circle_id, member_pubkey.clone())
                .await
                .expect("close the epoch by removing the roster member");
            finalize_circle_epoch_close(&store, &db, db_store_dir.clone(), &signer, &components)
                .await;
        }

        let membership = publish_acknowledged_store_snapshot(
            &store,
            &db,
            db_store_dir.clone(),
            &signer,
            &routing,
            "2026-07-24T01:00:00Z",
            "2026-07-24T01:00:01Z",
        )
        .await;

        Self {
            db,
            store,
            home,
            signer,
            member,
            routing,
            circle_id,
            membership,
        }
    }
}

#[tokio::test]
async fn restore_reports_a_circle_with_no_coverage_image() {
    let base =
        ActiveMemberCircleSnapshot::build("snapshot-restore-no-image", CircleFixtureMode::Live)
            .await;
    let ActiveMemberCircleSnapshot {
        db,
        store,
        signer,
        circle_id,
        membership,
        home,
        ..
    } = base;

    // The owner restores a Circle it holds access to. The owner's own database
    // carries no bootstrap coverage row for it — it never installed one,
    // because it created the Circle — but the control activation it retains
    // names an epoch-access image that is really in the cloud, and the
    // restoring device holds no rows for the Circle at all. Staging that image
    // as the restored device's coverage is what gives it the Circle's rows;
    // refusing to would restore a device with access to a Circle and nothing in
    // it.
    let target = RestoreTarget::new();
    let restored = restore_store_snapshot(
        &store,
        &db,
        &membership,
        &signer,
        &target,
        "no-image-device",
    )
    .await
    .expect("a Circle with active access restores without error");

    let coverage = restored
        .circle_bootstrap_coverage_for_test(circle_id)
        .await
        .expect("read restored Circle coverage")
        .expect("selection stages the coverage whose image the Circle's access names");
    assert_eq!(coverage.circle_id, circle_id);
    // Bucket keys carry the provider locator after the logical key, so match on
    // the logical prefix rather than the whole key.
    let image_key = coverage.bootstrap.image.object.slot().logical_key();
    assert!(
        home.keys().iter().any(|key| key.starts_with(image_key)),
        "the staged coverage names an image the cloud holds: {image_key}",
    );

    let control_count: i64 = restored
        .circle_control_activation_count_for_test(circle_id)
        .await
        .expect("count restored Circle control indexes");
    assert!(
        control_count > 0,
        "the Store image still restores the Circle control indexes"
    );
}

#[tokio::test]
async fn restore_rejects_a_sabotaged_circle_image_and_exposes_no_database() {
    let base =
        ActiveMemberCircleSnapshot::build("snapshot-restore-sabotage", CircleFixtureMode::Closed)
            .await;
    let ActiveMemberCircleSnapshot {
        db,
        store,
        home,
        signer,
        membership,
        ..
    } = base;

    // A hostile storage provider serves the wrong bytes for the Circle bootstrap
    // image the owner's own access leaf names. The bytes are input to the verifier,
    // not trusted for being served: their digest no longer matches the image hash
    // the signed access leaf pins, so `verify_circle_bootstrap_image` rejects them
    // and the whole restore fails with no database left behind. (An image with a
    // wrong schema/routing contract or a row outside the audience closure changes
    // the bytes too, so it fails this same digest check first — those reach the
    // verifier only from a malicious author, whose defense is the verifier's own
    // tests, not a storage provider.)
    let sabotaged_keys: Vec<String> = home
        .keys()
        .into_iter()
        .filter(|key| key.contains("/bootstraps/"))
        .collect();
    assert!(
        !sabotaged_keys.is_empty(),
        "the Circle bootstrap image was uploaded to storage"
    );
    for key in &sabotaged_keys {
        home.insert_exact_object(key, b"sabotaged Circle bootstrap image".to_vec());
    }

    // The Store snapshot itself verifies; only the Circle image is sabotaged.
    let target = RestoreTarget::new();
    let before = payload_spool_files(&target.store_dir);
    let outcome = restore_store_snapshot(
        &store,
        &db,
        &membership,
        &signer,
        &target,
        "sabotaged-restore-device",
    )
    .await;
    let error = match outcome {
        Ok(_) => panic!("a sabotaged Circle image must fail the whole restore"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("image")
            || error.to_string().contains("hash")
            || error.to_string().contains("digest"),
        "the restore fails on image verification: {error}"
    );
    // The Store image installed before the Circle selection ran, so this also
    // says the committed install's payload files went with the destination.
    assert_restore_left_nothing(&target, &before);
}

#[tokio::test]
async fn restore_releases_the_destination_when_the_circle_install_fails() {
    let base =
        ActiveMemberCircleSnapshot::build("snapshot-restore-crash", CircleFixtureMode::Live).await;
    let ActiveMemberCircleSnapshot {
        db,
        store,
        member,
        routing,
        membership,
        ..
    } = base;

    // The member restore selects its own leaf bootstrap as a Circle image to
    // install. A failure injected after that install wrote its payload files —
    // the stand-in for a crash between the Store and Circle installs — must
    // release the whole destination: no database, not even the Store image on
    // its own, and none of the payload files either install created.
    let target = RestoreTarget::new();
    let before = payload_spool_files(&target.store_dir);
    let outcome = store
        .prepare_snapshot_bootstrap(
            &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
            db.schema_version(),
            &target.database_path,
            &member,
        )
        .await
        .expect("restore the Store snapshot")
        .fail_circle_install_for_test()
        .install(
            &target.store_dir,
            circle_routing_tables(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "crash-restore-device".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &circle_routing_migrations(),
            coven_database::CovenMigrationPolicy::ApplyPending,
            Some(&routing),
        )
        .await;
    match outcome {
        Ok(_) => panic!("an injected Circle-install failure must fail the whole restore"),
        Err(error) => assert!(
            error
                .to_string()
                .contains("injected Circle install failure"),
            "the restore fails at the Circle-install step: {error}"
        ),
    }
    assert_restore_left_nothing(&target, &before);
}

#[tokio::test]
async fn a_cold_restore_dropped_mid_way_removes_its_database_and_payloads() {
    let base =
        ActiveMemberCircleSnapshot::build("snapshot-restore-abandoned", CircleFixtureMode::Live)
            .await;
    let ActiveMemberCircleSnapshot {
        db,
        store,
        member,
        routing,
        membership,
        ..
    } = base;

    let target = RestoreTarget::new();
    let before = payload_spool_files(&target.store_dir);
    let bootstrap = store
        .prepare_snapshot_bootstrap(
            &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
            db.schema_version(),
            &target.database_path,
            &member,
        )
        .await
        .expect("restore the Store snapshot");
    let migrations = circle_routing_migrations();
    let mut install = Box::pin(bootstrap.install(
        &target.store_dir,
        circle_routing_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "abandoned-restore-device".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &migrations,
        coven_database::CovenMigrationPolicy::ApplyPending,
        Some(&routing),
    ));
    // The Store install runs to completion inside the first poll, which then
    // waits on the restore that follows it. That is the abandonment window the
    // guard has to cover: the install is committed and nothing owns it yet.
    let pending = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(install.as_mut(), context))
    })
    .await;
    assert!(
        pending.is_pending(),
        "the restore must still be in flight when it is abandoned"
    );
    assert!(
        target.database_path.exists(),
        "the Store install committed before the restore was abandoned"
    );
    assert_ne!(
        payload_spool_files(&target.store_dir),
        before,
        "the Store install wrote payload files before the restore was abandoned"
    );

    drop(install);

    assert_restore_left_nothing(&target, &before);
}

#[tokio::test]
async fn a_finished_cold_restore_seeds_its_clock_with_recipient_rows() {
    let base =
        ActiveMemberCircleSnapshot::build("snapshot-restore-clock", CircleFixtureMode::Live).await;
    let ActiveMemberCircleSnapshot {
        db,
        store,
        member,
        membership,
        ..
    } = base;

    let target = RestoreTarget::new();
    let restored = restore_store_snapshot(
        &store,
        &db,
        &membership,
        &member,
        &target,
        "clock-restore-device",
    )
    .await
    .expect("restore the member's Circle content");

    // The Circle rows land after the Store image opened, so the register clock
    // is seeded once the restore is finished rather than at the open.
    let rows = coven_database::DatabaseImageTest::open(&target.database_path)
        .expect("read the restored rows");
    let installed: String = rows
        .query_row("SELECT MAX(_updated_at) FROM documents", [], |row| {
            row.get(0)
        })
        .expect("the restore installs recipient Circle rows");
    assert!(
        restored.stamp_for_test() > installed,
        "the restored clock must stand above its installed rows: {installed}"
    );
}

#[tokio::test]
async fn a_cold_restore_leaves_unrelated_spool_files_alone() {
    let base =
        ActiveMemberCircleSnapshot::build("snapshot-restore-reuse", CircleFixtureMode::Live).await;
    let ActiveMemberCircleSnapshot {
        db,
        store,
        member,
        routing,
        membership,
        ..
    } = base;

    // One finished restore, to learn what a restore of this snapshot puts in a
    // destination's payload spool.
    let installed = RestoreTarget::new();
    let _restored = restore_store_snapshot(
        &store,
        &db,
        &membership,
        &member,
        &installed,
        "spool-source-device",
    )
    .await
    .expect("restore the member's Circle content");
    let payloads = payload_spool_files(&installed.store_dir);
    assert!(!payloads.is_empty(), "a restore writes payload spool files");

    // A second destination whose spool already holds payload files of exactly
    // that shape. They belong to whoever put them there; the failing attempt
    // below owns only what it writes itself.
    let target = RestoreTarget::new();
    let spool = target.store_dir.payload_spool_dir();
    std::fs::create_dir_all(&spool).expect("create the destination payload spool");
    let before = payloads
        .into_iter()
        .map(|(source, bytes)| {
            let path = spool.join(source.file_name().expect("payload file name"));
            std::fs::write(&path, &bytes).expect("seed the destination payload spool");
            (path, bytes)
        })
        .collect::<std::collections::BTreeMap<_, _>>();

    let outcome = store
        .prepare_snapshot_bootstrap(
            &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
            db.schema_version(),
            &target.database_path,
            &member,
        )
        .await
        .expect("restore the Store snapshot")
        .fail_circle_install_for_test()
        .install(
            &target.store_dir,
            circle_routing_tables(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            "reuse-restore-device".to_string(),
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            &circle_routing_migrations(),
            coven_database::CovenMigrationPolicy::ApplyPending,
            Some(&routing),
        )
        .await;
    assert!(
        outcome.is_err(),
        "an injected Circle-install failure must fail the whole restore"
    );
    assert_restore_left_nothing(&target, &before);
}

#[tokio::test]
async fn post_close_circle_store_snapshot_restores_and_converges() {
    let routing = EncryptionService::from_key([42; 32]);
    // The Circle member is a Store member without an active device, so the
    // snapshot's stability quorum stays the single owner device.
    let CircleWithOneMember {
        db,
        db_store_dir,
        store,
        signer,
        components,
        circle_id,
        member,
        member_pubkey,
        ..
    } = CircleWithOneMember::build("snapshot-restore-after-close").await;

    // Old-epoch Circle content, published under the initial epoch.
    {
        let routing = routing.clone();
        let audience = Some(circle_id.to_string());
        coven_database::StoreDatabase::new(&db)
            .run_host_store_write_for_test(Some(routing), None, move |transaction| {
                transaction
                    .execute(
                        "INSERT INTO documents (id, audience, _updated_at)
                             VALUES (?1, ?2, ?3)",
                        rusqlite::params![
                            "00000000-0000-4000-8000-000000000090",
                            audience,
                            "0000000002000-0000-owner"
                        ],
                    )
                    .map(|_| ())
                    .map_err(DbError::from)
            })
            .await
            .expect("capture old-epoch Circle content");
    }
    components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish old-epoch Circle content");

    // Drive the member-removal epoch close through to successor activation. Its
    // successor bootstrap covers the old-epoch content up to the accepted cutoff.
    components
        .remove_circle_member(circle_id, member_pubkey.clone())
        .await
        .expect("close the epoch by removing the roster member");
    finalize_circle_epoch_close(&store, &db, db_store_dir.clone(), &signer, &components).await;

    // Publish a Store snapshot covering the post-close frontier; the single owner
    // device acknowledges it stable. Its image prunes the old-epoch retained rows
    // now covered by the successor bootstrap.
    //
    // A device then restores from that snapshot. Installation validates the image's
    // retained inputs against the retention rule; the successor bootstrap's
    // coverage keeps retained rows a Store snapshot of a Circle store legitimately
    // carries, which the validator must accept.
    let membership = publish_acknowledged_store_snapshot(
        &store,
        &db,
        db_store_dir.clone(),
        &signer,
        &routing,
        "2026-07-24T01:00:00Z",
        "2026-07-24T01:00:01Z",
    )
    .await;
    let target = RestoreTarget::new();
    let mut restored = restore_store_snapshot(
        &store,
        &db,
        &membership,
        &signer,
        &target,
        "restored-device",
    )
    .await
    .expect("install the restored snapshot database");

    // The restored device pulls and converges to the owner's accepted Store
    // frontier: the installed snapshot represents the closed epoch exactly, so
    // nothing is held and the projections agree.
    let pull = restored
        .pull(Some(&routing))
        .await
        .expect("the restored device pulls the close without a foreign-key violation");
    assert!(
        pull.held_positions.is_empty(),
        "the restored device holds no positions after the close: {:?}",
        pull.held_positions
    );
    let owner_frontier = coven_database::StoreDatabase::new(&db)
        .materialized_frontier()
        .await
        .expect("read owner Store frontier");
    let restored_frontier = restored
        .materialized_frontier_for_test()
        .await
        .expect("read restored Store frontier");
    assert_eq!(
        restored_frontier, owner_frontier,
        "the restored device converges to the owner's accepted Store frontier"
    );

    // The same snapshot, restored by the REMOVED member, must not resurrect the
    // Circle. The Store image carries the owner's preserved coverage row, but the
    // removed member cannot decrypt the Circle, so selection clears it — and a
    // forced full replay then materializes none of the Circle's content. If the
    // clear were skipped, the preserved row would reconstruct the image and hand
    // the removed member the very rows the epoch close took away.
    let removed_target = RestoreTarget::new();
    let removed_db = restore_store_snapshot(
        &store,
        &db,
        &membership,
        &member,
        &removed_target,
        "removed-member-device",
    )
    .await
    .expect("install the removed-member restore database");

    let removed_coverage = removed_db
        .circle_bootstrap_coverage_for_test(circle_id)
        .await
        .expect("read removed-member Circle coverage");
    assert!(
        removed_coverage.is_none(),
        "the removed member retains no Circle coverage row"
    );

    let control_count: i64 = removed_db
        .circle_control_activation_count_for_test(circle_id)
        .await
        .expect("count restored Circle control indexes");
    assert!(
        control_count > 0,
        "the removed member still restores the Circle control indexes"
    );

    // The exact input a full replay reconstructs Circle images from carries no
    // entry for this Circle: with the coverage row cleared there is nothing to
    // rebuild, so no replay can hand the removed member the content the epoch close
    // took away. Were the clear skipped, the preserved row would reappear here and
    // re-arm the replay — this assertion is what makes the clear load-bearing.
    let replay_inputs = removed_db
        .circle_bootstrap_replay_inputs_for_test()
        .await
        .expect("read removed-member Circle replay inputs");
    assert!(
        replay_inputs.is_empty(),
        "the removed member has no Circle image to replay"
    );
}

/// End-to-end receipt that restore selection installs from a standalone Circle
/// snapshot when it dominates the other coverage candidates. The fixture authors
/// content under the successor epoch after the close and cuts a standalone snapshot
/// over it, so its coverage strictly dominates both the leaf-named successor
/// bootstrap and the preserved author-coverage row (both fixed at the close cutoff).
/// A fresh device restores, and the staged Install decision must name the standalone
/// snapshot's image.
///
/// The standalone snapshot is authored under the SUCCESSOR control, which the head
/// control is — so it is a retained activation and restore selection does not skip
/// it as reclaimed (the reclaimed-control skip only drops snapshots authored under a
/// control a later close reclaimed). Selection reads the standalone metadata and
/// image with the Circle epoch key the restorer's active leaf carries. That
/// threading is load-bearing: decrypting the standalone stream with any other key
/// fails the read outright (a wrong key is an error, not absence), so the restore
/// cannot install the snapshot and this assertion does not hold.
#[tokio::test]
async fn restore_installs_a_dominating_standalone_circle_snapshot() {
    let routing = EncryptionService::from_key([42; 32]);
    let CircleWithOneMember {
        db,
        db_store_dir,
        store,
        signer,
        components,
        circle_id,
        member_pubkey,
        ..
    } = CircleWithOneMember::build("standalone-restore-dominates").await;

    // Pre-close content, then close the epoch by removing the member. The successor
    // bootstrap and the owner's successor leaf both cover the pre-close cutoff.
    db.capture_document_for_test(
        "00000000-0000-4000-8000-000000000090",
        Some(circle_id),
        "0000000002000-0000-owner",
    )
    .await
    .expect("capture Circle content");
    components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish pre-close Circle content");
    components
        .remove_circle_member(circle_id, member_pubkey.clone())
        .await
        .expect("close the epoch by removing the roster member");
    finalize_circle_epoch_close(&store, &db, db_store_dir.clone(), &signer, &components).await;

    // Post-close content under the successor epoch advances the frontier past the
    // close cutoff, so a snapshot over it dominates the close-cutoff candidates.
    db.capture_document_for_test(
        "00000000-0000-4000-8000-000000000091",
        Some(circle_id),
        "0000000004000-0000-owner",
    )
    .await
    .expect("capture Circle content");
    components
        .run_cycle(
            &coven_foundation::clock::SystemClock,
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish post-close Circle content");

    // Author the dominating standalone Circle snapshot under the successor epoch.
    let (_standalone_temp, standalone_dir) = temp_store_dir();
    let standalone = store
        .push_circle_snapshots(
            &db,
            db_store_dir.clone(),
            standalone_dir.as_ref().join("standalone"),
            db.schema_version(),
            "2026-07-24T02:00:00Z",
            &routing,
        )
        .await
        .expect("author the dominating standalone Circle snapshot");
    let standalone_image_hash = standalone.bootstrap.image.image_hash;

    // A Store snapshot covering the post-close frontier, acknowledged stable, that
    // a fresh device then restores from.
    let membership = publish_acknowledged_store_snapshot(
        &store,
        &db,
        db_store_dir.clone(),
        &signer,
        &routing,
        "2026-07-24T02:00:01Z",
        "2026-07-24T02:00:02Z",
    )
    .await;
    let target = RestoreTarget::new();
    let restored = restore_store_snapshot(
        &store,
        &db,
        &membership,
        &signer,
        &target,
        "standalone-restore-device",
    )
    .await
    .expect("install the restored snapshot database");

    // The staged Install decision chose the dominating standalone snapshot: the
    // coverage row names its image.
    let coverage_row = restored
        .circle_bootstrap_coverage_for_test(circle_id)
        .await
        .expect("read restored Circle coverage")
        .expect("the restore installs a Circle coverage row");
    assert_eq!(
        coverage_row.bootstrap.image.image_hash, standalone_image_hash,
        "restore installs the dominating standalone snapshot's image, not a \
         close-cutoff bootstrap"
    );
}
