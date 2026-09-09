use super::*;

/// Adopting an accepted snapshot releases the replay package pins it replaces.
#[tokio::test]
async fn a_standing_device_advances_its_baseline_and_releases_what_it_pinned() {
    let fixture = ReclaimJourneyFixture::build("reclaim-baseline-advance").await;

    let result = fixture.reclaim().await.expect("reclaim covered packages");

    assert_eq!(
        result.store_packages.retained_for_replay, 0,
        "snapshot adoption left no target pinned for replay",
    );
    assert_eq!(
        result.store_packages.authorized, result.store_packages.targets_considered,
        "and every considered target is authorized instead of declined",
    );
    assert!(
        result.store_packages.targets_considered > 0,
        "the run had targets to consider in the first place",
    );
}

/// Repeated adoption at the same accepted boundary performs no rebuild.
#[tokio::test]
async fn a_baseline_already_at_the_coverage_does_not_advance_again() {
    let fixture = ReclaimJourneyFixture::build("reclaim-baseline-settled").await;
    let frontier = fixture.materialized_frontier().await;

    let again = fixture
        .device
        .advance_baseline_by_acknowledging(frontier)
        .await
        .expect("acknowledge the snapshot a second time");

    assert!(
        again.is_none(),
        "the baseline already stands at the accepted snapshot, so nothing is rebuilt",
    );
}

/// Replay still reconstructs the store after the baseline moves.
///
/// The advance retires the retained rows the new cut covers, which is only safe
/// because the baseline image restates what they replayed. If it did not, this
/// count would come back short by the retired commits' rows.
#[tokio::test]
async fn replay_reconstructs_the_store_after_the_baseline_advances() {
    let fixture = ReclaimJourneyFixture::build("reclaim-baseline-replay").await;

    let after = fixture
        .replay_note_count()
        .await
        .expect("replay after advancing");
    assert_eq!(
        after, 2,
        "replay from the advanced baseline reproduces both published notes",
    );
}

/// The adoption stage advances even when no new acknowledgement is staged.
#[tokio::test]
async fn an_accepted_snapshot_advances_a_baseline_that_never_moved() {
    let fixture = StandingAcknowledgementFixture::build("standing-ack-advance").await;

    let advanced = fixture.stand_on_accepted_snapshot().await;

    assert!(
        advanced.retired_commits > 0,
        "advancing retires the retained materializations the accepted cut covers, retired {}",
        advanced.retired_commits,
    );
    assert!(
        advanced.released_pins > 0,
        "and releases the replay pins those materializations held, released {}",
        advanced.released_pins,
    );
}

/// Joining a writer adopts the accepted snapshot while preparing its handoff.
/// The later device state does not invalidate that retired replay boundary.
#[tokio::test]
async fn joining_a_writer_preserves_retirement_at_the_accepted_snapshot() {
    let fixture = StandingAcknowledgementFixture::build("standing-ack-overtaken").await;
    let database = coven_database::StoreDatabase::new(&fixture.db);
    assert!(
        database
            .installed_replay_baseline()
            .await
            .expect("read baseline before joining")
            .snapshot()
            .is_none(),
        "the published snapshot has not been adopted before the join",
    );
    let snapshot = database
        .latest_local_store_snapshot()
        .await
        .expect("read accepted snapshot before joining")
        .expect("snapshot is published");
    fixture.advance_device_state_after_snapshot().await;
    fixture.acknowledge_current_device_state().await;

    assert!(
        fixture
            .standing_acknowledgement_has_newer_device_state()
            .await,
        "the current acknowledgement describes device state beyond the accepted snapshot",
    );

    let outcome = fixture
        .device
        .stand_on_accepted_snapshot()
        .await
        .expect("evaluate the accepted snapshot");

    assert_eq!(
        outcome,
        crate::sync::store::ReplayBaselineAdvance::Declined(
            crate::sync::store::ReplayBaselineDecline::BaselineAtCoverage {
                snapshot: snapshot.reference.clone(),
            },
        ),
        "joining already retired the accepted prefix; the newer device state does not rebuild it",
    );
    let baseline = database
        .installed_replay_baseline()
        .await
        .expect("read baseline after joining");
    assert_eq!(
        baseline
            .snapshot()
            .expect("snapshot remains installed")
            .reference,
        snapshot.reference,
    );
    assert_eq!(baseline.coverage(), &snapshot.meta.coverage);
    assert_eq!(
        fixture
            .device
            .replay_row_count_for_test("notes")
            .await
            .expect("replay from the retired baseline"),
        2,
        "both covered notes remain reconstructible after the join",
    );
}

/// Once a device has caught up, the stage says so and reads nothing.
#[tokio::test]
async fn a_baseline_at_the_accepted_coverage_declines_and_says_why() {
    let fixture = StandingAcknowledgementFixture::build("standing-ack-settled").await;
    fixture.stand_on_accepted_snapshot().await;

    let outcome = fixture
        .device
        .stand_on_accepted_snapshot()
        .await
        .expect("stand on the accepted snapshot again");

    assert_eq!(
        outcome,
        crate::sync::store::ReplayBaselineAdvance::Declined(
            crate::sync::store::ReplayBaselineDecline::BaselineAtCoverage {
                snapshot: coven_database::StoreDatabase::new(&fixture.db)
                    .installed_replay_baseline()
                    .await
                    .expect("read installed baseline")
                    .snapshot()
                    .expect("baseline has snapshot")
                    .reference
                    .clone(),
            },
        ),
        "the second pass reports the steady state rather than a silent nothing",
    );
}

/// A device with no accepted snapshot reports why adoption cannot advance.
#[tokio::test]
async fn a_device_without_an_accepted_snapshot_declines_and_says_why() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let signer = UserKeypair::generate();
    let (store, _storage) = crate::sync::test_helpers::TestStore::create_with_connection(
        &db,
        db_store_dir.clone(),
        "no-acknowledged-snapshot",
        signer.clone(),
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind Store");

    let outcome = device
        .stand_on_accepted_snapshot()
        .await
        .expect("stand on nothing");

    assert_eq!(
        outcome,
        crate::sync::store::ReplayBaselineAdvance::Declined(
            crate::sync::store::ReplayBaselineDecline::NoAcceptedSnapshot,
        ),
    );
}

/// Accepted rows and a snapshot whose separate adoption stage has not run.
struct StandingAcknowledgementFixture {
    db: coven_database::Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    store: std::sync::Arc<crate::sync::test_helpers::TestStore>,
    device: crate::sync::test_helpers::TestDevice,
    signer: UserKeypair,
}

impl StandingAcknowledgementFixture {
    async fn build(store_id: &str) -> Self {
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let signer = UserKeypair::generate();
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, _storage) = crate::sync::test_helpers::TestStore::create_with_connection(
            &db,
            db_store_dir.clone(),
            store_id,
            signer.clone(),
            home,
        )
        .await
        .expect("create Store");
        let device = store
            .bind_device_in(&db, db_store_dir.clone(), &signer)
            .await
            .expect("bind Store");
        for (sequence, row) in [
            (
                1,
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('standing-1', 'first', NULL, \
                 '0000000001000-0000-standing', '2026-01-01')",
            ),
            (
                2,
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
                 VALUES ('standing-2', 'second', NULL, \
                 '0000000002000-0000-standing', '2026-01-01')",
            ),
        ] {
            let changeset = crate::sync::test_helpers::open_test_db(
                crate::sync::test_helpers::test_store_dir(),
            )
            .capture_test_changeset(&[row])
            .await;
            store
                .publish_changeset("founder", sequence, &changeset, db.schema_version())
                .await
                .expect("publish package activation");
        }
        let image_dir = tempfile::tempdir().expect("snapshot image dir");
        let image = coven_database::StoreDatabase::new(&db)
            .capture_snapshot_image_for_test(
                store.root().clone(),
                image_dir.path().to_path_buf(),
                None,
            )
            .await
            .expect("capture a real snapshot image");
        let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
            coven_database::StoreDatabase::new(&db)
                .materialized_frontier()
                .await
                .expect("materialized frontier"),
        )
        .expect("frontier");
        device
            .publish_snapshot(image, coverage.clone())
            .await
            .expect("publish the snapshot");
        device
            .publish_acknowledgement_without_advancing(coverage)
            .await
            .expect("publish an acknowledgement while leaving adoption pending");
        Self {
            db,
            db_store_dir,
            store,
            device,
            signer,
        }
    }

    async fn frontier(&self) -> coven_protocol::store_commit::CommitFrontier {
        coven_protocol::store_commit::CommitFrontier::from_refs(
            coven_database::StoreDatabase::new(&self.db)
                .materialized_frontier()
                .await
                .expect("read materialized frontier"),
        )
        .expect("shape materialized frontier")
    }

    async fn stand_on_accepted_snapshot(&self) -> coven_database::AdvancedReplayBaseline {
        match self
            .device
            .stand_on_accepted_snapshot()
            .await
            .expect("stand on the accepted snapshot")
        {
            crate::sync::store::ReplayBaselineAdvance::Advanced(advanced) => advanced,
            crate::sync::store::ReplayBaselineAdvance::Declined(decline) => {
                panic!("declined to advance: {}", decline.as_str())
            }
        }
    }

    /// Acknowledge the registration accepted after snapshot capture.
    async fn acknowledge_current_device_state(&self) {
        self.device
            .publish_acknowledgement_without_advancing(self.frontier().await)
            .await
            .expect("publish the current device state acknowledgement");
    }

    async fn standing_acknowledgement_has_newer_device_state(&self) -> bool {
        let database = coven_database::StoreDatabase::new(&self.db);
        let snapshot = database
            .latest_local_store_snapshot()
            .await
            .expect("read accepted local snapshot")
            .expect("snapshot exists");
        let standing = database
            .latest_local_store_ack()
            .await
            .expect("read the standing acknowledgement")
            .and_then(|published| published.standing)
            .expect("the device has published an acknowledgement");
        standing.assertion.device_state != snapshot.meta.history_summary.post_state
    }

    /// Register a second device after the snapshot's accepted cut.
    async fn advance_device_state_after_snapshot(&self) {
        let joining_store_dir = crate::sync::test_helpers::test_store_dir();
        self.store
            .activate_joined_device_from_snapshot(
                &self.db,
                self.db_store_dir.clone(),
                joining_store_dir,
                &self.signer,
                "2026-07-16T00:00:04Z",
                crate::sync::test_helpers::test_synced_tables(),
                crate::sync::test_helpers::test_migrations(),
                self.db.schema_version(),
            )
            .await
            .expect("activate a second device");
    }
}

/// An accepted snapshot licenses replay retirement without a new acknowledgement.
/// Adoption releases pins atomically with replacing the local replay baseline.
#[tokio::test]
async fn an_accepted_snapshot_advances_the_baseline_without_a_new_acknowledgement() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let signer = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let (store, _storage) = crate::sync::test_helpers::TestStore::create_with_connection(
        &db,
        db_store_dir.clone(),
        "reclaim-baseline-unacknowledged",
        signer.clone(),
        home.clone(),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind reclaim Store");

    let changeset =
        crate::sync::test_helpers::open_test_db(crate::sync::test_helpers::test_store_dir())
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
             VALUES ('unacknowledged-1', 'first', NULL, \
             '0000000001000-0000-unacknowledged', '2026-01-01')",
            ])
            .await;
    store
        .publish_changeset("founder", 1, &changeset, db.schema_version())
        .await
        .expect("publish package activation");

    // Publish the real image without staging an acknowledgement.
    let image_dir = tempfile::tempdir().expect("snapshot image dir");
    let image = coven_database::StoreDatabase::new(&db)
        .capture_snapshot_image_for_test(store.root().clone(), image_dir.path().to_path_buf(), None)
        .await
        .expect("capture a real snapshot image");
    let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
        coven_database::StoreDatabase::new(&db)
            .materialized_frontier()
            .await
            .expect("materialized frontier"),
    )
    .expect("frontier");
    device
        .publish_snapshot(image, coverage)
        .await
        .expect("publish the snapshot without an acknowledgement");

    let result = device
        .reclaim_packages()
        .await
        .expect("reclaim runs even with nothing it may delete behind");
    assert!(
        matches!(
            result.store_packages.coverage,
            super::StorePackageReclaimCoverage::Snapshot { .. }
        ),
        "an accepted snapshot licenses reclaim independently of acknowledgements",
    );

    let database = coven_database::StoreDatabase::new(&db);
    let acknowledgement_before = database
        .latest_local_store_ack()
        .await
        .expect("read acknowledgement before adoption")
        .map(|acknowledgement| acknowledgement.reference);
    let advanced = match device
        .stand_on_accepted_snapshot()
        .await
        .expect("adopt the accepted snapshot")
    {
        crate::sync::store::ReplayBaselineAdvance::Advanced(advanced) => advanced,
        outcome => panic!("accepted snapshot did not advance: {outcome:?}"),
    };
    assert_eq!(
        database
            .latest_local_store_ack()
            .await
            .expect("read acknowledgement after adoption")
            .map(|acknowledgement| acknowledgement.reference),
        acknowledgement_before,
        "adoption must not create an acknowledgement"
    );
    assert!(
        advanced.retired_commits > 0,
        "advancing retires the retained materializations the new cut covers, retired {}",
        advanced.retired_commits,
    );
    assert!(
        advanced.released_pins > 0,
        "and releases the replay pins those materializations held, released {}",
        advanced.released_pins,
    );
}

/// Advancing the baseline folds the settled write journal into the image and
/// deletes it, so a device's journal is bounded by what it has not yet settled
/// rather than by everything it has ever written.
///
/// A local partition is stated nowhere else: no commit carries one, and a
/// snapshot image projected for an audience may not. So before this, the journal
/// was the durable home of every local row the device had ever written — replayed
/// in full on every canonical rebuild, and never any shorter. The baseline image
/// is this device's own rewind point, which is the one image that may hold them,
/// and holding them is what lets the advance drop the rows.
#[tokio::test]
async fn advancing_the_baseline_folds_the_settled_write_journal_into_it() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let signer = UserKeypair::generate();
    let home = crate::sync::test_helpers::test_cloud_home();
    let (store, _storage) = crate::sync::test_helpers::TestStore::create_with_connection(
        &db,
        db_store_dir.clone(),
        "baseline-folds-writes",
        signer.clone(),
        home,
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind Store");
    let changeset =
        crate::sync::test_helpers::open_test_db(crate::sync::test_helpers::test_store_dir())
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('shared-note', 'shared', NULL, 1, \
                 '0000000001000-0000-folding', '2026-01-01')",
            ])
            .await;
    store
        .publish_changeset("founder", 1, &changeset, db.schema_version())
        .await
        .expect("publish package activation");

    let store_database = coven_database::StoreDatabase::new(&db);
    // Creating the Store journalled its own writes; the local ones are counted
    // on top of whatever those left.
    let (settled_before, claims_before) = store_database
        .store_write_journal_counts_for_test()
        .await
        .expect("read the journal the Store creation left");
    assert_eq!(
        (settled_before, claims_before),
        (1, 1),
        "creating the Store published one write of its own",
    );
    const LOCAL_WRITES: i64 = 12;
    for tick in 0..LOCAL_WRITES {
        store_database
            .run_host_store_write_for_test(None, None, move |tx| {
                tx.execute(
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                     VALUES (?1, 'local', NULL, 0, ?2, '2026-01-01')",
                    rusqlite::params![
                        format!("local-note-{tick}"),
                        format!("000000000{}000-0000-folding", 2 + tick),
                    ],
                )?;
                Ok::<_, coven_database::DbError>(())
            })
            .await
            .expect("capture a local-only write");
    }
    assert_eq!(
        store_database
            .store_write_journal_counts_for_test()
            .await
            .expect("read the journal before the advance"),
        (settled_before + LOCAL_WRITES, claims_before + LOCAL_WRITES),
        "each local-only write is journalled while the baseline still stands behind it",
    );

    let image_dir = tempfile::tempdir().expect("snapshot image dir");
    let image = coven_database::StoreDatabase::new(&db)
        .capture_snapshot_image_for_test(store.root().clone(), image_dir.path().to_path_buf(), None)
        .await
        .expect("capture a real snapshot image");
    let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
        coven_database::StoreDatabase::new(&db)
            .materialized_frontier()
            .await
            .expect("materialized frontier"),
    )
    .expect("frontier");
    device
        .publish_snapshot(image, coverage.clone())
        .await
        .expect("publish the snapshot");
    let advanced = device
        .advance_baseline_by_acknowledging(coverage)
        .await
        .expect("acknowledge the published snapshot")
        .expect("the accepted snapshot licenses the advance");

    assert_eq!(
        advanced.folded_writes,
        u64::try_from(settled_before + LOCAL_WRITES).expect("count fits"),
        "every settled write is folded into the image the advance adopts",
    );
    assert_eq!(
        store_database
            .store_write_journal_counts_for_test()
            .await
            .expect("read the journal after the advance"),
        (settled_before, 0),
        "the local-only writes are gone outright and every payload claim with \
         them; what is left is one receipt per write that reached the cloud, \
         which is this device's record of where its own writes landed",
    );
    assert_eq!(
        device
            .replay_row_count_for_test("notes")
            .await
            .expect("replay the notes from the new baseline alone"),
        LOCAL_WRITES + 1,
        "the local rows are in the baseline image now, not owed by a journal",
    );

    // The bound moves with the baseline rather than with the device's lifetime:
    // what a settled device journals from here is what it has written since the
    // snapshot it stands on, and nothing before it.
    for tick in LOCAL_WRITES..LOCAL_WRITES + 5 {
        store_database
            .run_host_store_write_for_test(None, None, move |tx| {
                tx.execute(
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                     VALUES (?1, 'local', NULL, 0, ?2, '2026-01-01')",
                    rusqlite::params![
                        format!("local-note-{tick}"),
                        format!("00000000{}000-0000-folding", 20 + tick),
                    ],
                )?;
                Ok::<_, coven_database::DbError>(())
            })
            .await
            .expect("capture a local-only write after the advance");
    }
    assert_eq!(
        store_database
            .store_write_journal_counts_for_test()
            .await
            .expect("read the journal after writing past the advance"),
        (settled_before + 5, 5),
        "only the writes the standing baseline does not state are journalled",
    );
}
