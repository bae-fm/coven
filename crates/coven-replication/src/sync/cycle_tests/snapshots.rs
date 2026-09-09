use super::*;

/// Snapshot metadata records its creation time as RFC 3339.
#[tokio::test]
async fn snapshot_cycle_writes_rfc3339_metadata_timestamp() {
    let keypair = UserKeypair::generate();
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let storage = cycle_test_store(
        &db,
        db_store_dir.clone(),
        &keypair,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    // local_seq past 0 with no snapshot yet → the snapshot policy fires this cycle.
    db.set_protocol_state("local_seq", "1")
        .await
        .expect("seed local_seq");

    let cycle_device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("open exact test Store");
    cycle_device
        .run_cycle(None)
        .await
        .expect("run snapshot timestamp cycle");
    let snapshot = db
        .latest_store_snapshot_meta()
        .await
        .expect("the cycle published one snapshot");
    assert!(
        chrono::DateTime::parse_from_rfc3339(&snapshot.created_at).is_ok(),
        "snapshot creation time must be RFC 3339, got {:?}",
        snapshot.created_at,
    );
}

#[tokio::test]
async fn snapshot_count_cadence_counts_accepted_local_commits() {
    tokio::spawn(async {
        let owner = UserKeypair::generate();
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let storage = cycle_test_store(
            &db,
            db_store_dir.clone(),
            &owner,
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await;
        let source_store_dir = crate::sync::test_helpers::test_store_dir();
        let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
        let changeset = source
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
          VALUES ('cadence', 'Cadence', NULL, 1, '0000000001000-0000-source', '2026-01-01')",
            ])
            .await;

        storage
            .publish_changeset("local", 1, &changeset, SCHEMA_VERSION)
            .await
            .expect("publish local Store commit before peer setup");
        let mut peer_at_snapshot = None;
        for sequence in 1..=6 {
            peer_at_snapshot = Some(
                storage
                    .publish_changeset("peer", sequence, &changeset, SCHEMA_VERSION)
                    .await
                    .expect("publish peer Store commit before snapshot"),
            );
        }
        let peer_at_snapshot = peer_at_snapshot.expect("peer Store stream reaches sequence 6");
        storage.pull_into(&db, &db_store_dir).await;
        let local_at_snapshot = storage
            .latest_store_position()
            .await
            .expect("read local Store position after peer setup")
            .expect("local Store stream has an exact snapshot position");
        let local_snapshot_sequence = local_at_snapshot.coord.sequence();
        let local_stream = local_at_snapshot.coord.stream_id;
        let peer_stream = peer_at_snapshot.coord.stream_id;
        let snapshot_device = storage
            .open_into(&db, db_store_dir.clone())
            .await
            .expect("open Store before publishing cadence snapshot");
        let snapshot = publish_current_snapshot(&snapshot_device, T0).await;
        assert_eq!(
            snapshot.coverage,
            coven_protocol::store_commit::CommitFrontier(BTreeMap::from([
                (local_stream, local_at_snapshot),
                (peer_stream, peer_at_snapshot),
            ])),
            "the cadence snapshot captures both accepted streams",
        );

        let local_after_snapshot = local_snapshot_sequence
            .checked_add(100)
            .expect("local snapshot cadence sequence does not overflow");
        for sequence in local_snapshot_sequence + 1..=local_after_snapshot {
            storage
                .publish_changeset("local", sequence, &changeset, SCHEMA_VERSION)
                .await
                .expect("publish local Store commit after snapshot");
        }
        assert_eq!(
            storage
                .latest_store_position()
                .await
                .expect("read latest local Store commit")
                .expect("local Store stream has commits")
                .coord
                .sequence(),
            local_after_snapshot,
        );

        let snapshot_before_cycle = store_database(&db)
            .latest_local_store_snapshot()
            .await
            .expect("read Store snapshot before cadence cycle")
            .expect("the cadence baseline snapshot exists")
            .reference;

        let cycle_device = storage
            .open_into(&db, db_store_dir.clone())
            .await
            .expect("open exact test Store");
        cycle_device
            .run_cycle(None)
            .await
            .expect("run snapshot cadence cycle");

        assert_ne!(
            store_database(&db)
                .latest_local_store_snapshot()
                .await
                .expect("read latest Store snapshot")
                .expect("count cadence publishes a Store snapshot")
                .reference,
            snapshot_before_cycle,
        );
    })
    .await
    .expect("snapshot cadence orchestration completes");
}

#[tokio::test]
async fn elapsed_time_does_not_trigger_snapshot_below_commit_threshold() {
    tokio::spawn(async {
        let owner = UserKeypair::generate();
        let db_store_dir = crate::sync::test_helpers::test_store_dir();
        let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
        let storage = cycle_test_store(
            &db,
            db_store_dir.clone(),
            &owner,
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await;
        let source_store_dir = crate::sync::test_helpers::test_store_dir();
        let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
        let first = source
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('time-cadence-1', 'First', NULL, 1, \
                         '0000000001000-0000-source', '2026-01-01')",
            ])
            .await;
        let second = source
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('time-cadence-2', 'Second', NULL, 1, \
                         '0000000002000-0000-source', '2026-01-01')",
            ])
            .await;

        let at_snapshot = storage
            .publish_changeset("local", 1, &first, SCHEMA_VERSION)
            .await
            .expect("publish Store commit before snapshot");
        let snapshot_device = storage
            .open_into(&db, db_store_dir.clone())
            .await
            .expect("open Store before publishing timed snapshot");
        let snapshot = publish_current_snapshot(&snapshot_device, T0).await;
        assert_eq!(
            snapshot.coverage,
            coven_protocol::store_commit::CommitFrontier(BTreeMap::from([(
                at_snapshot.coord.stream_id,
                at_snapshot,
            )])),
        );
        storage
            .publish_changeset("local", 2, &second, SCHEMA_VERSION)
            .await
            .expect("publish one Store commit after snapshot");

        let now = chrono::DateTime::parse_from_rfc3339("2024-01-02T01:00:00Z")
            .expect("parse timed snapshot clock")
            .with_timezone(&chrono::Utc);
        snapshot_device
            .run_cycle_with(&FixedClock(now), None, None)
            .await
            .expect("run timed snapshot cycle");

        let published = store_database(&db)
            .latest_local_store_snapshot()
            .await
            .expect("read timed Store snapshot")
            .expect("the accepted snapshot remains available");
        assert_eq!(
            published.meta, snapshot,
            "elapsed time cannot bypass the accepted-commit threshold",
        );
    })
    .await
    .expect("snapshot time cadence orchestration completes");
}

/// A registered Member publishes rows but cannot author a catalog snapshot.
#[tokio::test]
async fn member_device_does_not_create_a_snapshot() {
    let owner = UserKeypair::generate();
    let owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let owner_db = crate::sync::test_helpers::open_test_db(owner_db_store_dir.clone());
    let storage = cycle_test_store(
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let member = UserKeypair::generate();
    let encryption = EncryptionService::from_key([42; 32]);
    admit_test_member(
        &storage,
        &owner_db,
        owner_db_store_dir.clone(),
        &owner,
        &member,
        &encryption,
    )
    .await;

    let member_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let member_db = crate::sync::test_helpers::open_test_db(member_db_store_dir.clone());
    let member_device = storage
        .activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &member_db,
            member_db_store_dir.clone(),
            &member,
            T0,
        )
        .await
        .expect("activate exact joined test device");
    member_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', NULL, 1, '0000000001000-0000-member', '2026-01-01')",
        )
        .await;

    let member_storage = Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage)));
    run_cycle_in_task(member_storage, member_device)
        .await
        .expect("Member Store cycle succeeds");

    assert!(
        storage
            .local_store_package_exists(&member_db, member_db_store_dir.clone(), 1)
            .await,
        "the Member's row publishes through its exact Store stream",
    );
    assert!(
        member_db.latest_store_snapshot_meta().await.is_none(),
        "a Member device cannot author catalog snapshot metadata",
    );
}

#[tokio::test]
async fn pull_refreshes_snapshot_authority_before_publication() {
    use coven_protocol::membership::MemberRole;

    let founder = UserKeypair::generate();
    let founder_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let founder_db = crate::sync::test_helpers::open_test_db(founder_db_store_dir.clone());
    let storage = cycle_test_store(
        &founder_db,
        founder_db_store_dir.clone(),
        &founder,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let successor_owner = UserKeypair::generate();
    let encryption = EncryptionService::from_key([64; 32]);
    storage
        .admit_member(
            &founder_db,
            founder_db_store_dir.clone(),
            &founder,
            &pubkey_hex(&successor_owner),
            None,
            MemberRole::Member,
            &encryption,
            "Test Store",
        )
        .await
        .expect("admit successor Owner");
    let successor_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let successor_db = crate::sync::test_helpers::open_test_db(successor_db_store_dir.clone());
    storage
        .activate_joined_device(
            &founder_db,
            founder_db_store_dir.clone(),
            &successor_db,
            successor_db_store_dir.clone(),
            &successor_owner,
            "2026-07-26T00:00:00Z",
        )
        .await
        .expect("activate successor Owner device");
    storage
        .promote_active_member_fixture(
            &founder_db,
            founder_db_store_dir.clone(),
            &successor_db,
            successor_db_store_dir.clone(),
            &founder,
            &successor_owner,
            &encryption,
        )
        .await
        .expect("promote successor Owner");

    let founder_store = storage
        .bind_device_in(&founder_db, founder_db_store_dir.clone(), &founder)
        .await
        .expect("load founder Store");
    let mut authorized = founder_store
        .authorize_writer()
        .await
        .expect("authorize founder before removal");
    let snapshot_before_pull = StoreDatabase::new(&founder_db)
        .latest_local_store_snapshot()
        .await
        .expect("read founder snapshot before removal")
        .map(|snapshot| snapshot.reference);

    let custody = TestCustody::default();
    storage
        .remove_member(
            &successor_db,
            successor_db_store_dir.clone(),
            &successor_owner,
            &pubkey_hex(&founder),
            &encryption,
            &custody,
        )
        .await
        .expect("remove founder after cycle authorization");

    authorized
        .pull(Some(&encryption))
        .await
        .expect("pull founder removal");
    authorized
        .snapshots()
        .publish_due_snapshots(
            "2026-07-26T01:00:00Z",
            Some(&encryption),
            false,
            std::num::NonZeroU64::new(1).unwrap(),
        )
        .await
        .expect("evaluate snapshot after pull");

    assert_eq!(
        StoreDatabase::new(&founder_db)
            .latest_local_store_snapshot()
            .await
            .expect("read founder snapshot state")
            .map(|snapshot| snapshot.reference),
        snapshot_before_pull,
        "a removed Owner must not publish from pre-pull membership authority",
    );
}

/// The mirror of the above: an Owner device with local data and itself pinned as the
/// owner DOES author the snapshot — the founder/initial-sync path a freshly-founded
/// store bootstraps from is preserved by the gate's owner branch.
#[tokio::test]
async fn owner_device_creates_a_snapshot() {
    let owner = UserKeypair::generate();
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
    let storage = cycle_test_store(
        &db,
        db_store_dir.clone(),
        &owner,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('n1', 'Album', NULL, 1, '0000000001000-0000-M', '2026-01-01')",
    )
    .await;

    let cycle_device = storage
        .open_into(&db, db_store_dir.clone())
        .await
        .expect("open exact test Store");
    cycle_device
        .run_cycle(None)
        .await
        .expect("run owner snapshot cycle");

    assert!(
        db.latest_store_snapshot_meta().await.is_some(),
        "an owner device must author catalog snapshot metadata",
    );
}
