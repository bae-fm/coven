use crate::sync::test_helpers::{open_test_db, test_cloud_home, test_store_dir, TestStore};
use coven_database::StoreDatabase;
use coven_keys::keys::UserKeypair;

#[tokio::test]
async fn snapshot_join_preserves_circle_streams_through_successor_replay() {
    assert_snapshot_stream_continuation(false).await;
}

#[tokio::test]
async fn snapshot_stream_locator_cannot_borrow_another_accepted_commits_authority() {
    assert_snapshot_stream_continuation(true).await;
}

async fn assert_snapshot_stream_continuation(corrupt_association: bool) {
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
    let activations = creation.circle_activations().stream_activations();
    assert!(!activations.as_slice().is_empty());
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
        for activation in activations.as_slice() {
            let registered = database
                .registered_stream_activation(activation.activation_id())
                .await
                .expect("read snapshot stream association")
                .expect("covered Circle stream remains available after joining and replay");
            assert_eq!(registered.activation(), activation);
            assert_eq!(registered.activating_commit(), creation.commit_ref());
        }
        let before_publication = database
            .store_current_publication()
            .await
            .expect("read boundary");
        let before_frontier = database
            .materialized_frontier()
            .await
            .expect("read frontier");
        let before_control = database
            .current_circle_control(circle)
            .await
            .expect("read control");
        if corrupt_association {
            let substitute = owner
                .latest_store_position()
                .await
                .expect("read owner position")
                .expect("the owner activated the joining device");
            assert_ne!(&substitute, creation.commit_ref());
            let substitute_commit = owner
                .load_commit_for_test(&substitute)
                .await
                .expect("read substitute commit");
            assert!(substitute_commit.value().stream_activations().is_empty());
            assert!(before_frontier
                .values()
                .any(|reference| reference == &substitute));
            let encoded = serde_json::to_string(&substitute)
                .expect("serialize substitute")
                .replace("'", "''");
            for activation in activations.as_slice() {
                target.execute_test_sql(&format!(
                    "UPDATE stream_activations SET activating_commit = '{encoded}' WHERE activation_id = '{}'",
                    activation.activation_id().as_hash(),
                )).await;
            }
        }
        owner
            .rename_circle(
                &format!("000000000300{index}-0000-owner"),
                circle,
                &format!("After snapshot {index}"),
            )
            .await
            .expect("publish successor Circle control");
        let (_, pulled) = peer.pull_store().await.expect("pull successor control");
        if corrupt_association {
            assert_eq!(pulled.changesets_applied, 0);
            assert_eq!(pulled.held_positions.len(), 1, "{pulled:?}");
            assert!(
                format!("{:?}", pulled.held_positions[0].reason)
                    .contains("outside the commit predecessor history"),
                "{pulled:?}"
            );
            assert_eq!(
                database
                    .store_current_publication()
                    .await
                    .expect("read held boundary"),
                StoreDatabase::new(&source)
                    .store_current_publication()
                    .await
                    .expect("read the authentic accepted successor")
            );
            assert_ne!(
                database
                    .store_current_publication()
                    .await
                    .expect("the accepted successor is retained for retry"),
                before_publication
            );
            assert_eq!(
                database
                    .materialized_frontier()
                    .await
                    .expect("read held frontier"),
                before_frontier
            );
            assert_eq!(
                database
                    .current_circle_control(circle)
                    .await
                    .expect("read held control"),
                before_control
            );
            return;
        }
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
