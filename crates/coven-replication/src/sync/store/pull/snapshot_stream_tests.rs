use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
use coven_database::StoreDatabase;
use coven_keys::keys::UserKeypair;
use coven_protocol::store_commit::CircleEntryOrigin;

/// A device that joined from a snapshot keeps the accepted Circle activations
/// the snapshot carried, so a successor control's inherited roster entry still
/// resolves to the exact earlier activation that introduced it — history the
/// joined device never walked commit by commit.
#[tokio::test]
async fn snapshot_join_preserves_circle_entry_provenance_through_successor_replay() {
    let source_dir = test_store_dir();
    let source = open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let store = TestStore::create(
        &source,
        source_dir.clone(),
        "snapshot-circle-streams",
        signer.clone(),
        test_cloud_home(),
    )
    .await
    .expect("create Store");
    let owner = store
        .bind_device_in(&source, source_dir.clone(), &signer)
        .await
        .expect("bind owner");
    let circle = owner
        .create_circle("0000000001000-0000-owner", "Before snapshot")
        .await
        .expect("create Circle");
    let creation = StoreDatabase::new(&source)
        .retained_merge_replay_inputs(store.root())
        .await
        .expect("read accepted Circle creation")
        .into_iter()
        .find(|input| {
            input
                .commit()
                .circle_controls()
                .iter()
                .any(|c| c.circle_id() == circle)
        })
        .expect("Circle creation is retained");
    // A Circle operations commit owns no author streams: its control, roster
    // and metadata are named by accepted Store history alone.
    assert!(creation
        .circle_activations()
        .stream_activations()
        .as_slice()
        .is_empty());
    let target_dir = test_store_dir();
    let target = open_test_db(target_dir.clone());
    let peer = store
        .admit_and_activate_peer(
            &source,
            source_dir,
            &target,
            target_dir,
            &UserKeypair::generate(),
        )
        .await
        .expect("join another principal from the snapshot");
    let database = StoreDatabase::new(&target);
    assert!(database
        .snapshot_coverage_frontier()
        .await
        .expect("read joined snapshot coverage")
        .covers_commit(creation.commit_ref()));

    for index in 0..2 {
        // The joining device retains the Circle creation as an accepted
        // activation, which is what every inherited entry resolves through.
        assert!(
            database
                .retained_circle_activation(
                    store.root().clone(),
                    circle,
                    creation.commit_ref().clone(),
                )
                .await
                .expect("resolve the retained Circle creation")
                .is_some()
        );

        owner
            .rename_circle(
                &format!("000000000300{index}-0000-owner"),
                circle,
                &format!("After snapshot {index}"),
            )
            .await
            .expect("publish successor Circle control");
        let (_, pulled) = peer.pull_store().await.expect("pull successor control");
        assert!(pulled.held_positions.is_empty(), "{pulled:?}");
        assert_eq!(pulled.changesets_applied, 1);
        let successor = owner
            .latest_store_position()
            .await
            .expect("read successor position")
            .expect("the owner published a successor control");
        let retained = database
            .retained_merge_replay_inputs(store.root())
            .await
            .expect("read received successor")
            .into_iter()
            .find(|input| input.commit_ref() == &successor)
            .expect("the successor was materialized");
        let [received] = retained.circle_activations().circles() else {
            panic!("the successor must retain its Circle control");
        };
        assert_eq!(
            Some(received.control.coord.clone()),
            StoreDatabase::new(&source)
                .current_circle_control(circle)
                .await
                .expect("read owner control")
        );
        // The successor inherits the founder's roster entry, and names the exact
        // accepted commit that introduced it — the one the snapshot retained.
        let roster = &received.reference.objects().roster_entries;
        assert_eq!(roster.len(), 1);
        assert!(roster.values().all(|entry| matches!(
            &entry.origin,
            CircleEntryOrigin::Inherited { activating_commit }
                if activating_commit == creation.commit_ref()
        )));
        let access = received
            .local_access
            .as_ref()
            .expect("the Store member receives its own access leaf");
        assert!(access.active.is_none());
        assert!(matches!(
            access.leaf.value.disposition,
            coven_protocol::circle::CircleAccessDisposition::Inactive
        ));
        assert!(database
            .current_circle_control(circle)
            .await
            .expect("read nonmember authoring control")
            .is_none());
    }
}
