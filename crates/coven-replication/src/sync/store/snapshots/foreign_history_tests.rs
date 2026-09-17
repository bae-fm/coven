//! A snapshot image carries history its receiver has never authorized.
//!
//! The image is authenticated — the owner signed its hash, its coverage and its
//! summary — and every retained input inside it opens against its own commit.
//! None of that says the carried commits were authorized by the Store's
//! membership at the positions they claim. These tests build an image whose
//! carried Circle activation was re-signed by its original device with a
//! membership authority the chain never granted, and require every admission
//! path to refuse it before anything on the receiver changes.

use std::path::{Path, PathBuf};

use coven_database::{Database, DatabaseImageTest, StoreDatabase};
use coven_foundation::store_dir::StoreDir;
use coven_keys::keys::UserKeypair;
use coven_protocol::membership::{MembershipChain, MembershipCoord, MembershipFloor};
use coven_protocol::objects::{ExactObjectRef, ObjectSlot, PreparedExactObject};
use coven_protocol::store_commit::{
    commit_semantic_prefix, CommitFrontier, ObjectHash, StoreBatchCommit, StoreBatchCommitRef,
};

use crate::sync::test_helpers::{self, TestDevice, TestStore};

/// The publisher of one owner-signed image whose carried history is hostile,
/// and the membership floor a receiver restores against.
struct HostileSnapshotPublisher {
    store: std::sync::Arc<TestStore>,
    device: TestDevice,
    signer: UserKeypair,
    membership: MembershipChain,
    source: Database,
    source_dir: StoreDir,
}

/// Every file under `directory`, so a refused admission can be shown to have
/// left the destination exactly as empty as it found it.
fn files_under(directory: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![directory.to_path_buf()];
    while let Some(path) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

/// Re-sign `original` under a membership authority the Store's chain does not
/// grant its author, and rebuild the exact reference the result occupies.
///
/// Only the authority coordinate changes. The commit keeps its coordinate, its
/// author, its order and its whole operation body, so everything that binds a
/// retained row to its commit still holds — which is the point: an image can
/// carry this and pass every check its installation makes today.
async fn forge_unauthorized_authority(
    device: &TestDevice,
    signer: &UserKeypair,
    original: &StoreBatchCommit,
    coord: &coven_protocol::store_commit::StoreCommitCoord,
) -> (StoreBatchCommitRef, Vec<u8>) {
    let registration = device
        .load_registration_for_test(&original.author_registration)
        .await
        .expect("load the carried commit's author registration");
    let device_signer = registration
        .device_signer(signer)
        .expect("derive the author's device signing key");
    let authority = original
        .membership_authority
        .clone()
        .expect("an operations commit names its membership authority");
    let mut hostile = original.clone();
    hostile.body_mut().membership_authority = Some(MembershipCoord {
        entry_hash: ObjectHash::digest(b"admission-forged-membership-entry"),
        ..authority
    });
    hostile.resign(&device_signer);
    let bytes = hostile.to_bytes();
    let slot = ObjectSlot::logical(format!(
        "{}.json",
        commit_semantic_prefix(
            hostile.candidate_family(),
            &coord.stream_id.to_string(),
            coord.sequence(),
            hostile.commit_hash(),
        )
    ))
    .expect("forged commit slot");
    let object = ExactObjectRef::new(slot, bytes.len() as u64, ObjectHash::digest(&bytes));
    let reference = StoreBatchCommitRef::from_commit(&hostile, coord.clone(), object)
        .expect("forged commit reference");
    (reference, bytes)
}

/// A Store holding the commit the retention rule keeps under a covering
/// snapshot — a Circle activation — and a later ordinary write on the same
/// stream.
///
/// The later write is what leaves the activation covered by coordinate
/// rather than by exact reference, so a replacement of it stays inside the
/// signed coverage. That is what makes its carried acceptance resolve at
/// all: acceptance for a covered commit is synthesised from the coverage
/// the owner signed.
async fn hostile_publisher(store_id: &str) -> HostileSnapshotPublisher {
    let source_dir = test_helpers::test_store_dir();
    let source = test_helpers::open_test_db(source_dir.clone());
    let signer = UserKeypair::generate();
    let store = TestStore::create(
        &source,
        source_dir.clone(),
        store_id,
        signer.clone(),
        test_helpers::test_cloud_home(),
    )
    .await
    .expect("create the hostile publisher's Store");
    let device = store
        .open_into(&source, source_dir.clone())
        .await
        .expect("open the hostile publisher's Store");
    let membership = device
        .membership_for_test()
        .await
        .expect("project the publisher's membership");
    HostileSnapshotPublisher {
        store,
        device,
        signer,
        membership,
        source,
        source_dir,
    }
}

impl HostileSnapshotPublisher {
    /// Author the commit the hostile image will carry in place of: a Circle
    /// activation, which is what the retention rule keeps under a covering
    /// snapshot, followed by an ordinary write on the same stream.
    async fn author_retained_circle(&self, name: &str, note: &str) {
        self.device
            .create_circle("0000000001000-0000-owner", name)
            .await
            .expect("publish a Circle activation the retention rule keeps");
        self.device.publish_fixture_position(note).await;
    }

    /// Capture this Store's image, move its carried Circle activation onto a
    /// commit its own membership never authorized, and publish the result under
    /// the owner's signature.
    async fn publish_hostile_snapshot(&self) {
        let database = StoreDatabase::new(&self.source);
        let image_directory = tempfile::tempdir().expect("snapshot image directory");
        let image = database
            .capture_snapshot_image_for_test(
                self.store.root().clone(),
                image_directory.path().to_path_buf(),
                None,
            )
            .await
            .expect("capture the publisher's snapshot image");
        let coverage = CommitFrontier::from_refs(
            database
                .materialized_frontier()
                .await
                .expect("read the captured frontier"),
        )
        .expect("the captured frontier is a commit frontier");

        let retained = self
            .device
            .retained_merge_replay_inputs_for_test()
            .await
            .expect("read the publisher's retained history");
        let activation = retained
            .iter()
            .find(|materialization| !materialization.commit().circle_controls().is_empty())
            .expect("the Circle activation is retained");
        let coord = activation.commit_ref().coord.clone();
        let (reference, bytes) =
            forge_unauthorized_authority(&self.device, &self.signer, activation.commit(), &coord)
                .await;

        let image = DatabaseImageTest::from_bytes(&image).expect("open the captured image");
        image
            .replace_retained_materialization_commit(
                &coord.stream_id.to_string(),
                coord.sequence(),
                &serde_json::to_string(&reference).expect("serialize the forged reference"),
                &reference.commit_hash.to_string(),
                PreparedExactObject::new(reference.object.clone(), bytes)
                    .expect("forged commit object"),
            )
            .expect("carry the hostile activation in the image");
        let image = image.into_bytes().expect("re-serialize the hostile image");

        self.device
            .publish_snapshot(image, coverage.clone())
            .await
            .expect("the owner signs the hostile image");
        self.device
            .stage_acknowledgement(coverage, "2026-07-16T00:00:01Z".to_string())
            .await
            .expect("stage the snapshot acknowledgement");
        self.device
            .drain_acknowledgements()
            .await
            .expect("activate the snapshot acknowledgement");
    }

    fn floor(&self) -> MembershipFloor {
        MembershipFloor(self.membership.head_refs().to_vec())
    }
}

#[tokio::test]
async fn a_cold_restore_refuses_an_unauthorized_carried_history() {
    Box::pin(async {
        let publisher = hostile_publisher("admission-cold-restore").await;
        publisher
            .author_retained_circle("Admission Circle", "admission-note")
            .await;
        publisher.publish_hostile_snapshot().await;
        let destination = tempfile::tempdir().expect("cold restore destination");
        let store_dir = StoreDir::new_ephemeral(destination.path());
        let database_path = store_dir.db_path();
        assert!(
            files_under(destination.path()).is_empty(),
            "the destination starts empty",
        );

        let installed = publisher
            .store
            .prepare_snapshot_bootstrap(&publisher.floor(), 1, &database_path, &publisher.signer)
            .await
            .expect("the hostile image is a validly signed installable snapshot")
            .install(
                &store_dir,
                test_helpers::test_synced_tables(),
                coven_protocol::blob::TransferLimits::one_at_a_time(),
                "restoring-device".to_string(),
                std::sync::Arc::new(coven_foundation::clock::SystemClock),
                &test_helpers::test_migrations(),
                coven_database::CovenMigrationPolicy::ApplyPending,
                None,
            )
            .await;
        let Err(error) = installed else {
            panic!("cold restore installed a carried history it never authorized");
        };

        assert!(
            error.to_string().contains("membership authority"),
            "unexpected cold restore refusal: {error}",
        );
        assert_eq!(
            files_under(destination.path()),
            Vec::<PathBuf>::new(),
            "a refused cold restore left files behind",
        );
    })
    .await;
}

#[tokio::test]
async fn warm_adoption_refuses_an_unauthorized_carried_history() {
    Box::pin(async {
        let publisher = hostile_publisher("admission-warm-adoption").await;
        let peer_dir = test_helpers::test_store_dir();
        let peer_db = test_helpers::open_test_db(peer_dir.clone());
        let peer = publisher
            .store
            .activate_joined_device(
                &publisher.source,
                publisher.source_dir.clone(),
                &peer_db,
                peer_dir.clone(),
                &publisher.signer,
                "2026-07-16T00:00:00Z",
            )
            .await
            .expect("join a second device before the hostile image exists");
        peer.pull_store().await.expect("the peer reads the history");
        let installed = StoreDatabase::new(&peer_db);
        let boundary = installed
            .store_current_publication()
            .await
            .expect("read the peer's publication boundary");
        let baseline = installed
            .replay_baseline_for_test()
            .await
            .expect("read the peer's replay baseline");

        // The carried activation is one the peer has never pulled, so nothing
        // in the receiver contradicts its import. The publisher moving on is
        // also what leaves the peer's materialized frontier behind the coverage
        // the snapshot claims, so the pull has to adopt the image rather than
        // replay the interval.
        publisher
            .author_retained_circle("Unseen Circle", "admission-successor")
            .await;
        publisher.publish_hostile_snapshot().await;

        let error = peer
            .pull_store()
            .await
            .expect_err("warm adoption installed a carried history it never authorized");

        // Refused for the authority the carried commit claims. Before
        // admission this image was refused too, but by the receiver's own
        // device-state closure check during the import — an incidental
        // rejection of this particular shape, not a judgement on its history.
        assert!(
            format!("{error:?}").contains("membership authority"),
            "unexpected warm adoption refusal: {error:?}",
        );
        assert_eq!(
            installed
                .store_current_publication()
                .await
                .expect("re-read the peer's publication boundary"),
            boundary,
            "a refused adoption moved the receiver's publication boundary",
        );
        assert_eq!(
            format!(
                "{:?}",
                installed
                    .replay_baseline_for_test()
                    .await
                    .expect("re-read the peer's replay baseline")
                    .authority
            ),
            format!("{:?}", baseline.authority),
            "a refused adoption replaced the receiver's replay baseline",
        );
    })
    .await;
}

#[tokio::test]
async fn a_device_join_refuses_an_unauthorized_carried_history() {
    Box::pin(async {
        let publisher = hostile_publisher("admission-device-join").await;
        publisher
            .author_retained_circle("Admission Circle", "admission-note")
            .await;
        publisher.publish_hostile_snapshot().await;
        let joining_dir = test_helpers::test_store_dir();
        let joining_db = test_helpers::open_test_db(joining_dir.clone());
        let joining = StoreDatabase::new(&joining_db);
        assert_eq!(
            joining
                .local_store_root_ref()
                .await
                .expect("read the joining device's Store root"),
            None,
            "the joining device starts with no Store",
        );
        assert!(
            files_under(joining_dir.as_ref()).is_empty(),
            "the joining device's store directory starts empty",
        );

        let joined = publisher
            .store
            .activate_joined_device(
                &publisher.source,
                publisher.source_dir.clone(),
                &joining_db,
                joining_dir.clone(),
                &publisher.signer,
                "2026-07-16T00:00:02Z",
            )
            .await;
        let Err(error) = joined else {
            panic!("a device join installed a carried history it never authorized");
        };

        // Refused by the snapshot install itself. Before admission this same
        // image installed, and only the join's own history step refused it —
        // after the image had already become this device's Store.
        let error = format!("{error:?}");
        assert!(
            error.contains("Snapshot(StoreHistory(") && error.contains("membership authority"),
            "a device join refused the image somewhere other than its install: {error}",
        );
        assert_eq!(
            joining
                .local_store_root_ref()
                .await
                .expect("re-read the joining device's Store root"),
            None,
            "a refused device join left a Store behind",
        );
        let database_path = joining_dir.db_path();
        assert!(
            !database_path.exists()
                && !journal_path(&database_path, "wal").exists()
                && !journal_path(&database_path, "shm").exists(),
            "a refused device join published its destination database",
        );
        assert_eq!(
            files_under(joining_dir.as_ref()),
            Vec::<PathBuf>::new(),
            "a refused device join left files behind",
        );
    })
    .await;
}

/// A database's write-ahead log or shared-memory sidecar, which a refused
/// install has to take with the database rather than leave beside it.
fn journal_path(database: &Path, extension: &str) -> PathBuf {
    PathBuf::from(format!("{}-{extension}", database.display()))
}
