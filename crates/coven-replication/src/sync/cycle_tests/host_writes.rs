use super::*;

/// A host write made WHILE a cycle is in its push/pull network phase
/// must land in the device's NEXT outgoing changeset. It is recorded by the same
/// durable journal path as any other host write.
///
/// Setup: a peer "A" has a changeset in shared storage. Device "M" runs a cycle
/// that pulls it; the storage wrapper injects a host INSERT into M at the
/// immutable package-read await inside the pull. We then assert the
/// injected row is (a) present locally on M and (b) carried in M's next outgoing
/// changeset — proven by pulling that changeset into a fresh peer.
///
/// Mutation proof: route the injected write through raw `Database::call` instead
/// of the host journal. The row commits locally, but it is absent from M's next
/// changeset and assertion (b) fails.
#[tokio::test]
async fn host_write_during_pull_lands_in_next_outgoing_changeset() {
    let tasks = tokio::task::LocalSet::new();
    tasks
        .run_until(async {
            tokio::task::spawn_local(async {
                let keypair = UserKeypair::generate();
                // A peer A has published one changeset (an insert of note 'a1') to shared
                // storage, so M's cycle has something to fetch — the await we inject at.
                let producer_db_store_dir = crate::sync::test_helpers::test_store_dir();
                let producer_db =
                    crate::sync::test_helpers::open_test_db(producer_db_store_dir.clone());
                let inner = cycle_test_store(
                    &producer_db,
                    producer_db_store_dir.clone(),
                    &keypair,
                    crate::sync::test_helpers::test_cloud_home(),
                )
                .await;
                let a_src_store_dir = crate::sync::test_helpers::test_store_dir();
                let a_src = crate::sync::test_helpers::open_test_db(a_src_store_dir.clone());
                let a_cs = a_src
                    .capture_test_changeset(&[
                        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'FromA', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
                    ])
                    .await;
                // M's database. The injector runs this INSERT into M at the package-read
                // await, mid-pull.
                let db_m_store_dir = crate::sync::test_helpers::test_store_dir();
                let db_m = crate::sync::test_helpers::open_test_db(db_m_store_dir.clone());
                let device_m = inner
                    .activate_joined_device(
                        &producer_db,
                        producer_db_store_dir.clone(),
                        &db_m,
                        db_m_store_dir.clone(),
                        &keypair,
                        T0,
                    )
                    .await
                    .expect("activate exact joined test device");
                inner
                    .retain_store_packages_for_assertion(&db_m, db_m_store_dir.clone())
                    .await;
                let peer_sequence = inner
                    .latest_store_position()
                    .await
                    .expect("read producer Store position after activating M")
                    .expect("M's activation advances the producer Store stream")
                    .coord
                    .sequence()
                    .checked_add(1)
                    .expect("producer Store sequence remains representable");
                inner
                    .publish_changeset("A", peer_sequence, &a_cs, SCHEMA_VERSION)
                    .await
                    .expect("publish exact peer changeset after activating M");
                let storage = CycleStorageInterceptor::inject_host_write(
                    inner,
                    db_m.clone(),
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('m_mid', 'WrittenMidCycle', NULL, 1, '0000000002000-0000-M', '2026-01-01')",
                );
                let db_c_store_dir = crate::sync::test_helpers::test_store_dir();
                let db_c = crate::sync::test_helpers::open_test_db(db_c_store_dir.clone());
                let device_c = storage
                    .activate_joined_device(
                        &producer_db,
                        producer_db_store_dir.clone(),
                        &db_c,
                        db_c_store_dir.clone(),
                        &keypair,
                        T0,
                    )
                    .await
                    .expect("activate exact joined test device");

                drop(a_src);
                drop(producer_db);
                tokio::spawn(async move {
                    let cycle = InterceptedCycle::new(&storage, device_m);
                    // Cycle 1: M pulls A's changeset; the host write fires mid-pull.
                    cycle.run().await;

                    // (a) The injected row is present locally on M.
                    assert!(
                        db_m.test_row_exists("SELECT 1 FROM notes WHERE id = 'm_mid'")
                            .await,
                        "the package read injects the host write into M",
                    );
                    assert_eq!(
                        db_m.query_test_text("SELECT title FROM notes WHERE id = 'm_mid'")
                            .await,
                        "WrittenMidCycle",
                        "the mid-cycle host write committed to M's local db",
                    );

                    // (b) The injected row has its own pending write. Cycle 2 publishes it. A fresh
                    // peer C pulls M's output and must receive 'm_mid'.
                    cycle.run().await;

                    device_c
                        .pull_store()
                        .await
                        .expect("pull injected host write into C");
                    assert!(
                        db_c.test_row_exists("SELECT 1 FROM notes WHERE id = 'm_mid'")
                            .await,
                        "M's next Store commit carries the injected host write",
                    );
                    assert_eq!(
                        db_c.query_test_text("SELECT title FROM notes WHERE id = 'm_mid'")
                            .await,
                        "WrittenMidCycle",
                        "the mid-cycle host write reached a peer via M's next outgoing changeset",
                    );
                })
                .await
                .expect("mid-pull host write cycle task");
            })
            .await
            .expect("mid-pull host write setup task");
        })
        .await;
}

/// The other half of the write-ledger invariant: an applied row must not echo.
/// After M applies a peer's changeset, M's own next Store commit must not carry the
/// applied rows because remote apply does not use the host transaction path.
///
/// Mutation proof: route the apply through `run_internal_store_write_transaction_on`.
/// The applied rows then enter M's write ledger and republish, so device C receives
/// note 'a1' attributed to M and the assertion fails.
#[tokio::test]
async fn applied_rows_do_not_echo_into_next_outgoing_changeset() {
    let keypair = UserKeypair::generate();
    // Peer A publishes a changeset; M pulls and applies it in cycle 1.
    let producer_db_store_dir = crate::sync::test_helpers::test_store_dir();
    let producer_db = crate::sync::test_helpers::open_test_db(producer_db_store_dir.clone());
    let storage = cycle_test_store(
        &producer_db,
        producer_db_store_dir.clone(),
        &keypair,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;
    let db_m_store_dir = crate::sync::test_helpers::test_store_dir();
    let db_m = crate::sync::test_helpers::open_test_db(db_m_store_dir.clone());
    let device_m = storage
        .activate_joined_device(
            &producer_db,
            producer_db_store_dir.clone(),
            &db_m,
            db_m_store_dir.clone(),
            &keypair,
            T0,
        )
        .await
        .expect("activate exact joined test device");
    let cycle_storage = Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage)));
    let a_src_store_dir = crate::sync::test_helpers::test_store_dir();
    let a_src = crate::sync::test_helpers::open_test_db(a_src_store_dir.clone());
    let a_cs = a_src
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'FromA', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ])
        .await;
    let peer_sequence = storage
        .latest_store_position()
        .await
        .expect("read producer Store position after activating M")
        .expect("M's activation advances the producer Store stream")
        .coord
        .sequence()
        .checked_add(1)
        .expect("producer Store sequence remains representable");
    storage
        .publish_changeset("A", peer_sequence, &a_cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");

    run_cycle_in_task(Arc::clone(&cycle_storage), device_m.clone())
        .await
        .expect("M's pull cycle succeeds");
    assert_eq!(
        db_m.query_test_text("SELECT title FROM notes WHERE id = 'a1'")
            .await,
        "FromA",
        "M applied A's changeset",
    );

    // Cycle 2 has no host write. The applied row must not create a local data
    // commit because apply bypasses the host write ledger — and having already
    // acknowledged A's commit in cycle 1, M has nothing left to say, so the
    // cycle appends nothing at all.
    let before = device_m
        .latest_local_store_position()
        .await
        .expect("read local Store position before the empty cycle")
        .expect("cycle 1 published M's acknowledgement of A's commit");
    run_cycle_in_task(cycle_storage, device_m.clone())
        .await
        .expect("M's empty cycle succeeds");
    let after = device_m
        .latest_local_store_position()
        .await
        .expect("read local Store position after the empty cycle");
    assert_eq!(
        after.as_ref(),
        Some(&before),
        "an empty cycle over an applied changeset appends no commit",
    );
    let registration = coven_database::StoreDatabase::new(&db_m)
        .local_blob_write_authority()
        .await
        .expect("load local Store registration");
    let commit = device_m
        .load_commit_for_test(&before)
        .await
        .expect("load M's acknowledgement commit");
    assert_eq!(commit.author(), registration.value());
    assert!(commit.acknowledgement().is_some());
    assert!(
        commit.store_package().is_none(),
        "applying A's rows created no data commit of M's own",
    );
}
