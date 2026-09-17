use super::*;

/// A Store whose Circle has been renamed once, with a published snapshot
/// standing as this device's replay baseline. The rename's control inherits the
/// founder's roster entry, so it names the founder's activating commit as that
/// entry's introduction — the commit the retention rule must keep.
struct RenamedCircleSnapshot {
    source: Database,
    store_dir: coven_foundation::store_dir::StoreDir,
    store: std::sync::Arc<crate::sync::test_helpers::TestStore>,
    signer: UserKeypair,
    circle_id: coven_protocol::circle::CircleId,
}

impl RenamedCircleSnapshot {
    async fn build(label: &str) -> Self {
        let store_dir = crate::sync::test_helpers::test_store_dir();
        let source = crate::sync::test_helpers::open_test_db(store_dir.clone());
        let signer = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            store_dir.clone(),
            label,
            signer.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create snapshot Store");
        let device = store
            .bind_device_in(&source, store_dir.clone(), &signer)
            .await
            .expect("bind snapshot publisher");
        let circle_id = device
            .create_circle("0000000001000-0000-owner", "Household")
            .await
            .expect("publish the founder Circle");
        device
            .rename_circle("0000000002000-0000-owner", circle_id, "Cottage")
            .await
            .expect("publish a rename that inherits the founder roster entry");
        Self {
            source,
            store_dir,
            store,
            signer,
            circle_id,
        }
    }

    fn database(&self) -> StoreDatabase {
        StoreDatabase::new(&self.source)
    }

    async fn device(&self) -> crate::sync::test_helpers::TestDevice {
        self.store
            .bind_device_in(&self.source, self.store_dir.clone(), &self.signer)
            .await
            .expect("bind snapshot publisher")
    }

    /// The commit the current control's inherited roster entry names as its
    /// introduction.
    async fn entry_introduction(&self) -> coven_protocol::store_commit::StoreBatchCommitRef {
        let database = self.database();
        let (current, _) = database
            .circle_authoring_context(
                self.circle_id,
                &coven_keys::keys::public_key_hex(&self.signer),
            )
            .await
            .expect("read the renamed Circle's current control");
        let (activation, _) = database
            .verified_circle_activation_context(
                self.store.root().clone(),
                self.circle_id,
                current.control.coord.clone(),
            )
            .await
            .expect("read the renamed activation")
            .expect("the renamed control is retained");
        let entries = activation.reference.objects().roster_entries.clone();
        let inherited = entries.values().next().expect("one roster entry");
        let coven_protocol::store_commit::CircleEntryOrigin::Inherited { activating_commit } =
            &inherited.origin
        else {
            panic!("a rename inherits the founder's roster entry")
        };
        activating_commit.clone()
    }

    /// Publish this Store's image and stand the replay baseline on it, which
    /// retires every retained materialization the coverage covers except the
    /// ones the retention rule keeps.
    async fn advance_baseline(&self) {
        let database = self.database();
        let image_dir = tempfile::tempdir().expect("snapshot capture directory");
        let image = database
            .capture_snapshot_image_for_test(
                self.store.root().clone(),
                image_dir.path().to_path_buf(),
                None,
            )
            .await
            .expect("capture snapshot");
        let device = self.device().await;
        device
            .publish_snapshot(image, captured_coverage(&database).await)
            .await
            .expect("publish snapshot");
        device
            .stand_on_accepted_snapshot()
            .await
            .expect("install the snapshot baseline");
    }
}

/// An image that drops the commit an inherited entry names is refused. Without
/// it a restored device holds an entry whose introduction it cannot resolve,
/// and the entry's original author and device would rest on nothing.
#[tokio::test]
async fn a_snapshot_image_missing_an_entry_introduction_is_refused() {
    let fixture = RenamedCircleSnapshot::build("snapshot-entry-introduction").await;
    let introduction = fixture.entry_introduction().await;
    fixture.advance_baseline().await;

    fixture
        .database()
        .assert_replay_baseline_requires_an_entry_introduction_for_test(introduction)
        .await
        .expect("exercise the retention rule against a dropped entry introduction");
}

/// The inverse: under an advanced baseline the introducing commit is still
/// retained, so an inherited entry still resolves to it and authoring
/// continues over the same provenance. Store commits are never reclaim targets
/// — reclamation retires object bodies — so there is no baseline-covered case
/// where an introduction resolves to a retired body.
#[tokio::test]
async fn an_advanced_baseline_keeps_the_commit_an_inherited_entry_names() {
    let fixture = RenamedCircleSnapshot::build("snapshot-introduction-retained").await;
    let introduction = fixture.entry_introduction().await;
    fixture.advance_baseline().await;

    let retained = fixture
        .database()
        .retained_circle_activation(
            fixture.store.root().clone(),
            fixture.circle_id,
            introduction.clone(),
        )
        .await
        .expect("resolve the introducing activation under the advanced baseline")
        .expect("the advanced baseline keeps the introducing commit");
    assert!(
        retained
            .reference
            .objects()
            .roster_entries
            .values()
            .any(|entry| entry.origin.is_introduced()),
        "the retained activation is the one that introduced the entry"
    );

    // Authoring continues over that provenance: the next control inherits the
    // same entry and names the same introduction, verified against the
    // retained activation rather than a predecessor walk.
    fixture
        .device()
        .await
        .rename_circle("0000000003000-0000-owner", fixture.circle_id, "Lodge")
        .await
        .expect("author over the advanced baseline");
    assert_eq!(
        fixture.entry_introduction().await,
        introduction,
        "the entry's introduction is unchanged by the baseline advance"
    );
}
