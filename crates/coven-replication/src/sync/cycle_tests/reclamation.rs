use super::*;

// ---- changeset reclamation through a real cycle ----

/// A cycle installs accepted snapshot coverage and releases the covered Store
/// package while preserving its rows. Later acknowledgement and reclamation
/// commits may advance the same author stream beyond that original row commit.
#[tokio::test]
async fn cycle_reclaims_a_fully_acked_changeset_once_the_baseline_advances() {
    let keypair = UserKeypair::generate();
    let db_m_store_dir = crate::sync::test_helpers::test_store_dir();
    let db_m = crate::sync::test_helpers::open_test_db(db_m_store_dir.clone());
    let storage = cycle_test_store(
        &db_m,
        db_m_store_dir.clone(),
        &keypair,
        crate::sync::test_helpers::test_cloud_home(),
    )
    .await;

    // Peer A's changeset 1 (a shareable note).
    let a_src_store_dir = crate::sync::test_helpers::test_store_dir();
    let a_src = crate::sync::test_helpers::open_test_db(a_src_store_dir.clone());
    let a_cs = a_src
        .capture_test_changeset(&[
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
           VALUES ('a1', 'FromA', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
        ])
        .await;
    let published = storage
        .publish_changeset("A", 1, &a_cs, SCHEMA_VERSION)
        .await
        .expect("publish exact Store changeset");
    let published_stream = published.coord.stream_id;

    // M's cycle pulls A->1, acks A->1, snapshots covering A->1, then reclaims.
    let device = storage
        .open_into(&db_m, db_m_store_dir.clone())
        .await
        .expect("bind retained-replay Store device");
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
    )
    .await
    .expect("retained-replay cycle succeeds");
    let device = storage
        .bind_device_in(&db_m, db_m_store_dir.clone(), &keypair)
        .await
        .expect("bind retained-replay Store device for retirement");
    run_cycle_in_task(
        Arc::new(CycleStorageInterceptor::pass_through(Arc::clone(&storage))),
        device,
    )
    .await
    .expect("retained-replay retirement cycle succeeds");

    let snapshot = db_m
        .latest_store_snapshot_meta()
        .await
        .expect("the reclamation cycle publishes a covering snapshot");
    assert!(matches!(
        &snapshot.coverage,
        coven_protocol::store_commit::CommitFrontier(frontier)
            if frontier.get(&published_stream) == Some(&published)
    ));
    let ack_ref = store_database(&db_m)
        .latest_local_store_ack()
        .await
        .expect("read reclamation acknowledgement")
        .expect("the reclamation cycle publishes an acknowledgement")
        .reference;
    let device = storage
        .bind_device_in(&db_m, db_m_store_dir.clone(), &keypair)
        .await
        .expect("bind reclamation acknowledgement Store");
    let local_device = db_m
        .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local Store device")
        .expect("local Store device exists");
    let registrations = coven_database::StoreDatabase::new(&db_m)
        .activated_store_device_registration_records()
        .await
        .expect("read reclamation Store registrations");
    assert_eq!(registrations.len(), 1, "{registrations:?}");
    let registration = registrations
        .into_iter()
        .find(|registration| registration.value().device_id.to_string() == local_device)
        .expect("local reclamation Store registration is active");
    let acknowledgement = device
        .load_store_ack_for_test(&ack_ref, registration.value())
        .await
        .expect("load exact reclamation acknowledgement");
    assert!(
        acknowledgement
            .store_cut
            .frontier()
            .covers(&snapshot.coverage),
        "the current acknowledgement covers the accepted snapshot",
    );
    assert_eq!(
        db_m.query_test_text("SELECT title FROM notes WHERE id = 'a1'")
            .await,
        "FromA",
        "retirement preserves the covered row",
    );
    assert!(
        !storage
            .store_package_exists(&db_m, db_m_store_dir.clone(), &published)
            .await,
        "the advanced replay baseline releases the Store package it pinned",
    );
}

/// Snapshot coverage releases the first Store package while a later package
/// remains necessary for the accepted continuation. A joined device can still
/// pull that continuation after the covered package has been deleted.
#[tokio::test]
async fn cycle_preserves_packages_beyond_the_accepted_snapshot() {
    Box::pin(async {
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
        let source_store_dir = crate::sync::test_helpers::test_store_dir();
        let source = crate::sync::test_helpers::open_test_db(source_store_dir.clone());
        let first_changeset = source
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('a1', 'Title Alpha', NULL, 1, '0000000001000-0000-A', '2026-01-01')",
            ])
            .await;
        let first_commit = storage
            .publish_changeset("owner", 1, &first_changeset, SCHEMA_VERSION)
            .await
            .expect("publish first exact Store changeset");

        let behind = UserKeypair::generate();
        storage
            .admit_member(
                &owner_db,
                owner_db_store_dir.clone(),
                &owner,
                &pubkey_hex(&behind),
                None,
                coven_protocol::membership::MemberRole::Member,
                &EncryptionService::from_key([42; 32]),
                "Test Store",
            )
            .await
            .expect("admit exact behind Member identity");
        // Joined the way production joins: the device installs the owner's
        // snapshot and carries only the history published after it. A device
        // joined by replaying from genesis would pin every package the store
        // ever wrote, including the one this cycle deletes.
        let behind_db_store_dir = crate::sync::test_helpers::test_store_dir();
        let behind_store = storage
            .activate_joined_device_from_snapshot(
                &owner_db,
                owner_db_store_dir.clone(),
                behind_db_store_dir.clone(),
                &behind,
                T0,
                crate::sync::test_helpers::test_synced_tables(),
                crate::sync::test_helpers::test_migrations(),
                SCHEMA_VERSION,
            )
            .await
            .expect("activate exact joined test device");
        behind_store
            .pull_store()
            .await
            .expect("pull initial behind Member Store state");

        let behind_frontier = coven_protocol::store_commit::CommitFrontier::from_refs(
            behind_store
                .materialized_frontier()
                .await
                .expect("read behind device frontier"),
        )
        .expect("validate behind device frontier");
        behind_store
            .stage_acknowledgement(behind_frontier, T0.to_string())
            .await
            .expect("stage behind device acknowledgement");
        behind_store
            .drain_acknowledgements()
            .await
            .expect("publish behind device acknowledgement");

        let second_changeset = source
            .capture_test_changeset(&[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('a2', 'Title Beta', NULL, 1, '0000000002000-0000-A', '2026-01-01')",
            ])
            .await;
        let second_sequence = storage
            .latest_store_position()
            .await
            .expect("read owner Store position after registration activation")
            .expect("registration activation advances the owner Store stream")
            .coord
            .sequence()
            .checked_add(1)
            .expect("owner Store sequence remains representable");
        let second_commit = storage
            .publish_changeset("owner", second_sequence, &second_changeset, SCHEMA_VERSION)
            .await
            .expect("publish second exact Store changeset after registration activation");

        drop(source);
        tokio::spawn(async move {
            let cycle_device = storage
                .open_into(&owner_db, owner_db_store_dir.clone())
                .await
                .expect("open exact test Store");
            cycle_device
                .run_cycle(None)
                .await
                .expect("run package-retention cycle");

            assert!(
                !storage
                    .store_package_exists(&owner_db, owner_db_store_dir.clone(), &first_commit)
                    .await,
                "reclamation deletes the package covered by the accepted snapshot",
            );
            assert!(
                storage
                    .store_package_exists(&owner_db, owner_db_store_dir.clone(), &second_commit)
                    .await,
                "reclamation keeps the package beyond the accepted snapshot",
            );

            behind_store
                .pull_store()
                .await
                .expect("pull retained changeset into behind Member Store");
            assert!(
                behind_store
                    .test_row_exists("SELECT 1 FROM notes WHERE id = 'a2'")
                    .await,
                "the behind device pulls the retained changeset",
            );
        })
        .await
        .expect("snapshot coverage reclamation cycle task");
    })
    .await;
}
