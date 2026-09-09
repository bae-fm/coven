use super::*;

#[tokio::test]
async fn acknowledgement_completes_after_a_peer_wins_its_prepared_publication_position() {
    acknowledgement_survives_competing_publication(CompetingPublication::Acknowledgement).await;
}

#[tokio::test]
async fn acknowledgement_completes_after_a_snapshot_replaces_its_prepared_base() {
    acknowledgement_survives_competing_publication(CompetingPublication::Snapshot).await;
}

#[tokio::test]
async fn uploaded_acknowledgement_survives_a_snapshot_with_newer_peer_rows() {
    acknowledgement_survives_competing_publication(CompetingPublication::NewRowsSnapshot).await;
}

#[tokio::test]
async fn acknowledgement_retires_a_superseded_entry_before_replacing_its_snapshot_base() {
    acknowledgement_survives_competing_publication(
        CompetingPublication::EntryCleanupFailureThenSnapshot,
    )
    .await;
}

enum CompetingPublication {
    Acknowledgement,
    Snapshot,
    NewRowsSnapshot,
    EntryCleanupFailureThenSnapshot,
}

#[tokio::test]
async fn an_unuploaded_acknowledgement_refreshes_its_reserved_slot_after_a_peer_snapshot() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let (db, directory) = open(Path::new(":memory:"), "queued-ack-owner");
    let (store, _) = crate::sync::test_helpers::TestStore::create_with_connection(
        &db,
        directory.clone(),
        "queued-ack-store",
        signer.clone(),
        Arc::new(home.clone()),
    )
    .await
    .unwrap();
    let owner = store
        .bind_device_in(&db, directory.clone(), &signer)
        .await
        .unwrap();
    let (peer_db, peer_directory) = open(Path::new(":memory:"), "queued-ack-peer");
    let peer = store
        .activate_joined_device(
            &db,
            directory,
            &peer_db,
            peer_directory,
            &signer,
            "2026-07-16T00:00:00Z",
        )
        .await
        .unwrap();
    owner.pull_store().await.unwrap();
    owner
        .stage_current_acknowledgement("2026-07-16T00:00:01Z")
        .await
        .unwrap();
    let pending = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        pending.activation,
        coven_database::OutboundStoreAckActivation::AwaitingCandidate
    ));
    assert!(!home.contains_exact_object(&pending.reference.object));
    peer_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
         VALUES ('after-queued-ack', 'Peer row after staging', 1, \
         '0000000002000-0000-peer', '2026-07-16')",
        )
        .await;
    assert!(peer.prepare_pending_store_write().await.unwrap());
    assert_eq!(peer.drain_store_writes().await.unwrap(), 1);
    peer.publish_snapshot_generation_for_test().await.unwrap();
    owner.pull_store().await.unwrap();
    assert_eq!(
        owner
            .drain_acknowledgements_exact()
            .await
            .expect("refresh the unuploaded assertion"),
        1
    );
    let published = store_database(&db)
        .latest_local_store_ack()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(published.reference.sequence, pending.reference.sequence);
    assert_eq!(
        published.reference.object.slot(),
        pending.reference.object.slot()
    );
    assert_ne!(published.reference, pending.reference);
    let position = owner.latest_local_store_position().await.unwrap().unwrap();
    let commit = owner.load_commit_for_test(&position).await.unwrap();
    let ack = owner
        .load_store_ack_for_test(&published.reference, commit.author())
        .await
        .unwrap();
    assert_eq!(ack.store_cut, commit.order.predecessor_cut().unwrap());
    assert_eq!(ack.device_state, commit.device_state);
    assert_eq!(ack.successor, pending.ack.value.successor);
    assert_eq!(
        db.query_test_text("SELECT title FROM notes WHERE id = 'after-queued-ack'")
            .await,
        "Peer row after staging"
    );
    assert!(store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .is_none());
}

async fn acknowledgement_survives_competing_publication(competing: CompetingPublication) {
    let snapshot_wins = !matches!(competing, CompetingPublication::Acknowledgement);
    let new_rows = matches!(competing, CompetingPublication::NewRowsSnapshot);
    let directory = tempfile::tempdir().expect("acknowledgement race database directory");
    let path = directory.path().join("owner.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let (db, db_store_dir) = open(&path, "ack-race-owner");
    let (store, storage) = crate::sync::test_helpers::TestStore::create_with_connection(
        &db,
        db_store_dir.clone(),
        "ack-race-store",
        signer.clone(),
        Arc::new(home.clone()),
    )
    .await
    .expect("create acknowledgement race Store");
    let owner = store
        .bind_device_in(&db, db_store_dir.clone(), &signer)
        .await
        .expect("bind owner");
    let (peer_db, peer_dir) = open(Path::new(":memory:"), "ack-race-peer");
    let peer = store
        .activate_joined_device(
            &db,
            db_store_dir,
            &peer_db,
            peer_dir,
            &signer,
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("activate peer");
    owner.pull_store().await.expect("pull peer activation");
    owner
        .stage_current_acknowledgement("2026-07-16T00:00:01Z")
        .await
        .expect("stage owner acknowledgement");
    let pending = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .expect("read owner outbox")
        .expect("owner acknowledgement is pending");
    let candidate = owner
        .prepare_acknowledgement_candidate_for_test(&pending)
        .await;
    let previous_ack = store_database(&db)
        .latest_local_store_ack()
        .await
        .expect("read published acknowledgement before contention")
        .expect("registration has its initial acknowledgement");

    if matches!(
        competing,
        CompetingPublication::EntryCleanupFailureThenSnapshot
    ) {
        storage
            .create_protocol_object(&candidate.publication.prepared_entry().unwrap())
            .await
            .expect("upload the original publication entry");
        peer.stage_current_acknowledgement("2026-07-16T00:00:02Z")
            .await
            .expect("stage competing peer acknowledgement");
        assert_eq!(peer.drain_acknowledgements_exact().await.unwrap(), 1);
        home.fail_nth_exact_delete_of(&[candidate.publication.entry_object.slot()], 1);
        owner
            .drain_acknowledgements_exact()
            .await
            .expect_err("interrupted old-entry deletion prevents publication completion");
        let active = store_database(&db)
            .active_store_publication()
            .await
            .expect("read interrupted publication")
            .expect("publication remains reserved");
        assert_eq!(
            active.superseded_entry(),
            Some(&candidate.publication.reference().unwrap()),
            "the failed exact deletion remains owned by the pending publication",
        );
        assert!(home.contains_exact_object(&candidate.publication.entry_object));
    }

    if new_rows {
        storage
            .create_protocol_object(&pending.ack.prepared)
            .await
            .expect("upload the immutable acknowledgement before the peer writes");
        let remote = candidate
            .acknowledgement_remote_objects(&pending.ack)
            .expect("read exact acknowledgement candidate ownership")
            .into_iter()
            .find(|remote| remote.object() == &pending.reference.object)
            .expect("candidate owns its acknowledgement");
        store_database(&db)
            .mark_remote_object_uploaded(remote.into_record())
            .await
            .expect("record verified acknowledgement upload");
        peer_db
            .execute_test_host_write(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('after-ack-preparation', 'New peer row', 1, \
             '0000000002000-0000-peer', '2026-07-16')",
            )
            .await;
        assert!(peer
            .prepare_pending_store_write()
            .await
            .expect("prepare newer peer row"));
        assert_eq!(
            peer.drain_store_writes()
                .await
                .expect("publish newer peer row"),
            1
        );
    }

    if snapshot_wins {
        peer.publish_snapshot_generation_for_test()
            .await
            .expect("peer publishes a snapshot before the prepared acknowledgement");
        if matches!(
            competing,
            CompetingPublication::EntryCleanupFailureThenSnapshot
        ) {
            owner
                .pull_store()
                .await
                .expect("install the peer snapshot before retrying cleanup");
            assert!(home.contains_exact_object(&candidate.publication.entry_object));
        }
    } else {
        peer.stage_current_acknowledgement("2026-07-16T00:00:02Z")
            .await
            .expect("stage competing peer acknowledgement");
        assert_eq!(
            peer.drain_acknowledgements_exact()
                .await
                .expect("peer wins publication"),
            1,
        );
    }
    assert_eq!(
        store_database(&db)
            .latest_local_store_ack()
            .await
            .expect("read receipt before replacement activation")
            .expect("previous receipt remains published")
            .reference,
        previous_ack.reference,
        "uploading an assertion does not publish its activation",
    );
    assert_eq!(
        owner
            .drain_acknowledgements_exact()
            .await
            .expect("complete the replacement publication"),
        1,
    );
    let published = store_database(&db)
        .latest_local_store_ack()
        .await
        .expect("read owner acknowledgement receipt")
        .expect("owner acknowledgement is published");
    if snapshot_wins {
        assert_eq!(published.reference.sequence, pending.reference.sequence + 1);
        assert_eq!(
            home.stored_exact_object(pending.reference.object.slot()),
            pending.ack.prepared.stored_bytes(),
            "the superseded assertion remains an immutable chain predecessor",
        );
    } else {
        assert_eq!(published.reference, pending.reference);
    }
    let position = owner
        .latest_local_store_position()
        .await
        .expect("read owner position")
        .expect("owner acknowledgement has an accepted commit");
    assert_eq!(position.coord, candidate.reference.coord);
    let accepted_commit = owner
        .load_commit_for_test(&position)
        .await
        .expect("read the accepted acknowledgement commit");
    assert_eq!(accepted_commit.write_id, candidate.commit.write_id);
    let published_ack = owner
        .load_store_ack_for_test(&published.reference, accepted_commit.author())
        .await
        .expect("read the exact accepted acknowledgement");
    assert_eq!(
        published_ack.store_cut,
        accepted_commit.order.predecessor_cut().unwrap()
    );
    assert_eq!(published_ack.device_state, accepted_commit.device_state);
    if snapshot_wins {
        assert_eq!(
            published_ack.successor.predecessor,
            Some(pending.reference.object.clone())
        );
        assert_eq!(
            published.reference.object.slot(),
            &pending.ack.value.successor.next_slot
        );
    }
    if new_rows {
        assert_eq!(
            db.query_test_text("SELECT title FROM notes WHERE id = 'after-ack-preparation'")
                .await,
            "New peer row",
        );
        assert_ne!(published_ack.store_cut, pending.ack.value.store_cut);
    }
    if snapshot_wins {
        assert_ne!(
            position, candidate.reference,
            "the replacement commit names the winning snapshot base"
        );
    } else {
        assert_eq!(
            position, candidate.reference,
            "replacing only the publication entry preserves the original commit"
        );
    }
    let entries = store_database(&db)
        .store_publication_entries()
        .await
        .expect("read accepted entries");
    let accepted = entries
        .iter()
        .find(|entry| {
            matches!(
                &entry.value.payload,
                coven_protocol::store_commit::StorePublicationPayload::Commit(reference)
                    if reference == &position
            )
        })
        .expect("the acknowledgement commit is accepted");
    assert!(accepted.value.position > candidate.publication.entry.position);
    assert!(store_database(&db)
        .active_store_publication()
        .await
        .expect("read publication reservation")
        .is_none());
    assert!(store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .expect("read completed outbox")
        .is_none());
    if matches!(
        competing,
        CompetingPublication::EntryCleanupFailureThenSnapshot
    ) {
        assert!(!home.contains_exact_object(&candidate.publication.entry_object));
    }

    drop(owner);
    drop(db);
    let (reopened, reopened_dir) = open(&path, "ack-race-owner");
    let reopened_owner = store
        .bind_device_in(&reopened, reopened_dir, &signer)
        .await
        .expect("reopen owner");
    assert_eq!(
        reopened_owner
            .drain_acknowledgements_exact()
            .await
            .expect("retry after restart"),
        0
    );
    assert_eq!(
        reopened_owner
            .latest_local_store_position()
            .await
            .expect("read unchanged position"),
        Some(position)
    );
    let (_, pulled) = peer
        .pull_store()
        .await
        .expect("peer pulls the owner acknowledgement");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    if new_rows {
        let snapshot = peer
            .publish_snapshot_generation_for_test()
            .await
            .expect("compose the successor and its unactivated predecessor into a snapshot");
        let chain = snapshot
            .meta
            .history_summary
            .acknowledgements
            .get(&pending.reference.registration.device_id)
            .expect("snapshot retains the owner's acknowledgement chain");
        assert_eq!(
            chain.chain.get(&pending.reference.sequence),
            Some(&(pending.reference.clone(), pending.ack.value.clone()))
        );
        assert_eq!(
            chain.latest().map(|(reference, _)| reference),
            Some(&published.reference)
        );
        let (cold_db, cold_dir) = open(Path::new(":memory:"), "ack-race-cold-reader");
        let cold = store
            .open_into(&cold_db, cold_dir)
            .await
            .expect("cold reader verifies the retained acknowledgement chain");
        let (_, pulled) = cold
            .pull_store()
            .await
            .expect("cold reader installs the accepted snapshot");
        assert!(pulled.held_positions.is_empty(), "{pulled:?}");
        assert_eq!(
            cold_db
                .query_test_text("SELECT title FROM notes WHERE id = 'after-ack-preparation'")
                .await,
            "New peer row",
        );
    }
}
