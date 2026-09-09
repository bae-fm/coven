use super::tests::{open, store_database};
use crate::sync::test_helpers::TestStore;
use coven_keys::keys::UserKeypair;
use coven_storage::cloud::test_utils::InMemoryCloudHome;
use std::{path::Path, sync::Arc};

#[tokio::test]
async fn idle_devices_do_not_acknowledge_each_others_acknowledgements() {
    idle_acknowledgements(false, false).await;
}

#[tokio::test]
async fn idle_circle_devices_do_not_acknowledge_each_others_acknowledgements() {
    idle_acknowledgements(true, false).await;
}

#[tokio::test]
async fn idle_devices_do_not_acknowledge_again_after_snapshot_retirement() {
    idle_acknowledgements(false, true).await;
}

#[tokio::test]
async fn idle_circle_devices_do_not_acknowledge_again_after_snapshot_retirement() {
    idle_acknowledgements(true, true).await;
}

#[tokio::test]
async fn snapshot_publication_does_not_create_a_new_acknowledgement_assertion() {
    let signer = UserKeypair::generate();
    let (database, directory) = open(Path::new(":memory:"), "idle-snapshot-owner");
    let (store, _) = TestStore::create_with_connection(
        &database,
        directory.clone(),
        "idle-snapshot-store",
        signer.clone(),
        Arc::new(InMemoryCloudHome::new()),
    )
    .await
    .expect("create Store");
    let device = store
        .bind_device_in(&database, directory, &signer)
        .await
        .expect("bind owner");
    device
        .stage_current_acknowledgement_if_new("2026-07-16T00:00:00Z")
        .await
        .expect("establish the current assertion");
    device
        .drain_acknowledgements_exact()
        .await
        .expect("publish the current assertion");
    assert!(device
        .stage_current_acknowledgement_if_new("2026-07-16T00:00:01Z")
        .await
        .expect("compare the standing assertion")
        .is_none());
    {
        let mut writer = device.authorize_writer().await.expect("authorize snapshot");
        let cut = writer
            .snapshots()
            .capture_snapshot_cut(None)
            .await
            .expect("capture the acknowledged state");
        writer
            .snapshots()
            .push_snapshot_cut(cut, "2026-07-16T00:00:02Z".into())
            .await
            .expect("publish the acknowledged state as a snapshot");
    }
    assert!(
        device
            .stage_current_acknowledgement_if_new("2026-07-16T00:00:03Z")
            .await
            .expect("compare the assertion after compaction")
            .is_none(),
        "publishing the same state as a snapshot must not generate another acknowledgement",
    );
}

async fn idle_acknowledgements(with_circle: bool, compact: bool) {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let (owner_db, owner_dir) = open(Path::new(":memory:"), "idle-owner");
    let (store, _) = TestStore::create_with_connection(
        &owner_db,
        owner_dir.clone(),
        "idle-ack-store",
        signer.clone(),
        Arc::new(home),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&owner_db, owner_dir.clone(), &signer)
        .await
        .expect("bind owner");
    if with_circle {
        owner
            .create_circle("0000000001000-0000-owner", "Household")
            .await
            .expect("create Circle through its Store owner");
    }
    let (peer_db, peer_dir) = open(Path::new(":memory:"), "idle-peer");
    let peer = store
        .activate_joined_device(
            &owner_db,
            owner_dir,
            &peer_db,
            peer_dir,
            &signer,
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("activate peer");

    // Both devices observe the join and its snapshot before establishing their
    // standing assertions. Subsequent rounds deliver only those assertions.
    for _ in 0..3 {
        for device in [&owner, &peer] {
            let (_, pulled) = device.pull_store().await.expect("pull accepted history");
            assert!(pulled.held_positions.is_empty(), "{pulled:?}");
            let frontier = device
                .acknowledgement_frontier()
                .await
                .expect("read frontier");
            device
                .stage_circle_acknowledgements(&frontier, "2026-07-16T00:00:01Z")
                .await
                .expect("stage changed Circle observation");
            device
                .stage_current_acknowledgement_if_new("2026-07-16T00:00:01Z")
                .await
                .expect("stage changed observation");
            device
                .drain_acknowledgements_exact()
                .await
                .expect("publish changed observation");
            if compact {
                let mut writer = device.authorize_writer().await.unwrap();
                let cut = writer.snapshots().capture_snapshot_cut(None).await.unwrap();
                writer
                    .snapshots()
                    .push_snapshot_cut(cut, "2026-07-16T00:00:01Z".into())
                    .await
                    .expect("compact the accepted observations");
                writer
                    .acknowledgements()
                    .stand_on_accepted_snapshot(None)
                    .await
                    .expect("retire the compacted history");
            }
        }
    }
    owner
        .pull_store()
        .await
        .expect("observe final peer assertion");
    let boundary = store_database(&owner_db)
        .store_current_publication()
        .await
        .expect("read settled boundary");
    for device in [&owner, &peer] {
        let frontier = device
            .acknowledgement_frontier()
            .await
            .expect("read idle frontier");
        device
            .stage_circle_acknowledgements(&frontier, "2026-07-16T00:00:02Z")
            .await
            .expect("compare standing Circle observation");
        assert!(
            device
                .stage_current_acknowledgement_if_new("2026-07-16T00:00:02Z")
                .await
                .expect("compare standing observation")
                .is_none(),
            "receiving an acknowledgement alone must not create another acknowledgement",
        );
        assert_eq!(
            device
                .drain_acknowledgements_exact()
                .await
                .expect("drain idle outbox"),
            0,
        );
    }
    assert_eq!(
        store_database(&owner_db)
            .store_current_publication()
            .await
            .unwrap(),
        boundary,
    );

    owner_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('idle-new', 'new observation', NULL, 1, '0000000001000-0000-owner', '2026-07-16')",
        )
        .await;
    assert!(owner
        .prepare_pending_store_write()
        .await
        .expect("prepare new edit"));
    assert_eq!(
        owner.drain_store_writes().await.expect("publish new edit"),
        1
    );
    if compact {
        let mut writer = owner.authorize_writer().await.unwrap();
        let cut = writer.snapshots().capture_snapshot_cut(None).await.unwrap();
        writer
            .snapshots()
            .push_snapshot_cut(cut, "2026-07-16T00:00:03Z".into())
            .await
            .expect("compact the actual edit before acknowledging it");
        writer
            .acknowledgements()
            .stand_on_accepted_snapshot(None)
            .await
            .expect("retire the actual edit's replay history");
    }
    peer.pull_store().await.expect("observe the actual edit");
    for device in [&owner, &peer] {
        if compact {
            device
                .stand_on_accepted_snapshot()
                .await
                .expect("stand on the snapshot containing the actual edit");
        }
        assert!(
            device
                .stage_current_acknowledgement_if_new("2026-07-16T00:00:03Z")
                .await
                .expect("acknowledge the actual edit")
                .is_some(),
            "suppressing acknowledgement-only traffic must preserve new edit observations"
        );
        assert_eq!(
            device
                .drain_acknowledgements_exact()
                .await
                .expect("publish new observation"),
            1
        );
    }
}

#[tokio::test]
async fn an_accepted_peer_snapshot_invalidates_the_reclaim_decision() {
    let signer = UserKeypair::generate();
    let (owner_db, owner_dir) = open(Path::new(":memory:"), "reclaim-owner");
    let (store, _) = TestStore::create_with_connection(
        &owner_db,
        owner_dir.clone(),
        "reclaim-snapshot-store",
        signer.clone(),
        Arc::new(InMemoryCloudHome::new()),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&owner_db, owner_dir.clone(), &signer)
        .await
        .expect("bind owner");
    let (peer_db, peer_dir) = open(Path::new(":memory:"), "reclaim-peer");
    let peer = store
        .activate_joined_device(
            &owner_db,
            owner_dir,
            &peer_db,
            peer_dir,
            &signer,
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("activate peer");
    peer.pull_store().await.expect("install initial snapshot");
    let database = store_database(&peer_db);
    let frontier = database.materialized_frontier().await.unwrap();
    let settled = crate::sync::store::SettledCycle::default();
    {
        let writer = peer.authorize_writer().await.unwrap();
        let inputs = crate::sync::store::CycleInputs::read(&database, writer.membership())
            .await
            .unwrap();
        settled.record_reclaim_evaluated(inputs);
    }
    {
        let mut writer = owner.authorize_writer().await.unwrap();
        let cut = writer.snapshots().capture_snapshot_cut(None).await.unwrap();
        writer
            .snapshots()
            .push_snapshot_cut(cut, "2026-07-16T00:00:01Z".into())
            .await
            .expect("publish another snapshot without a new commit");
    }
    peer.pull_store().await.expect("install peer snapshot");
    assert_eq!(database.materialized_frontier().await.unwrap(), frontier);
    let writer = peer.authorize_writer().await.unwrap();
    let inputs = crate::sync::store::CycleInputs::read(&database, writer.membership())
        .await
        .unwrap();
    assert!(
        !settled.reclaim_evaluated(&inputs),
        "a newly accepted peer snapshot changes what can be reclaimed even at the same commit frontier",
    );
}
