use std::path::Path;
use std::sync::Arc;

use super::*;
use crate::database::Database;
use crate::database::{AuthorExclusionLocatorTamper, StoreDatabase};
use crate::keys::UserKeypair;
use crate::protocol::store_commit::{
    StoreAckExclusionState, StoreCommitCoord, StoreDeviceExclusionRef, StoreDeviceRegistrationRef,
};
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::store::owner::history::abandonment::MergeCandidateAbandonment;
use crate::sync::test_helpers::{
    open_test_db, store_database, temp_store_dir, TestDevice, TestStore,
};
use crate::{StoreDir, WriteId};

fn open(path: &Path, device_id: &str) -> Database {
    Database::open(
        path,
        crate::sync::test_helpers::test_synced_tables(),
        crate::protocol::blob::BLOB_TOMBSTONE_GRACE,
        crate::protocol::blob::TransferLimits::one_at_a_time(),
        device_id.to_string(),
        std::sync::Arc::new(crate::clock::SystemClock),
        &crate::sync::test_helpers::test_migrations(),
    )
    .expect("open exclusion test database")
}

#[tokio::test]
async fn uploaded_proposal_resumes_after_restart_without_freezing_the_target() {
    let directory = tempfile::tempdir().expect("exclusion test directory");
    let path = directory.path().join("store.sqlite");
    let signer = UserKeypair::generate();
    let home = InMemoryCloudHome::new();
    let storage = Arc::new(
        CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "device-exclusion-store",
            signer.clone(),
        )
        .expect("construct exclusion test storage"),
    );
    let db = open(&path, "exclusion-host");
    let device = TestDevice::create(
        &db,
        storage.clone(),
        "device-exclusion-store",
        signer.clone(),
    )
    .await
    .expect("create exclusion test Store");
    let reference = device
        .stage_uploaded_device_exclusion_proposal_for_test()
        .await
        .expect("stage uploaded exclusion proposal");
    drop(device);
    drop(db);

    let reopened = open(&path, "exclusion-host");
    let reopened_store = TestDevice::load(&reopened, storage.clone(), signer.clone())
        .await
        .expect("bind resumed exclusion Store");
    let mut writer = reopened_store
        .authorize_writer()
        .await
        .expect("authorize resumed exclusion Store");
    let result = Box::pin(writer.device_exclusion().resume())
        .await
        .expect("resume exclusion proposal")
        .expect("pending exclusion operation");
    assert!(matches!(
        result,
        StoreDeviceExclusionResult::ProposalActivated { proposal, .. }
            if proposal == reference
    ));
    assert!(StoreDatabase::new(&reopened)
        .active_outbound_store_device_exclusion()
        .await
        .expect("read exclusion journal")
        .is_none());
    let freezes = store_database(&reopened)
        .store_device_exclusion_freezes()
        .await
        .expect("read exclusion freezes");
    assert!(
        freezes.is_empty(),
        "the exclusion target must not freeze its own Store stream"
    );
    let frontier = crate::protocol::store_commit::CommitFrontier::from_refs(
        store_database(&reopened)
            .materialized_frontier()
            .await
            .expect("read exclusion frontier"),
    )
    .expect("shape exclusion frontier");
    let acknowledgement = reopened_store
        .stage_acknowledgement(frontier, "2026-07-18T00:00:00Z".to_string())
        .await
        .expect("stage exclusion acknowledgement");
    let StoreAckExclusionState { proposal_freezes } = acknowledgement.exclusions.clone();
    assert!(proposal_freezes.is_empty());
    assert_eq!(
        reopened_store
            .drain_acknowledgements()
            .await
            .expect("publish exclusion acknowledgement"),
        1
    );
    let base_sequence = reopened_store
        .latest_local_store_position()
        .await
        .expect("read cancellation base")
        .expect("acknowledgement activation position")
        .coord
        .sequence();
    drop(writer);
    Box::pin(async move {
        let (candidate_staged, resume_candidate) = reopened.arm_test_pause(
            crate::database::DatabaseTestPoint::StoreDeviceExclusionCandidateStaged,
        );
        let cancel_device = reopened_store.clone();
        let cancel_reference = reference.clone();
        let cancellation_task = tokio::spawn(async move {
            cancel_device
                .cancel_device_exclusion(&cancel_reference)
                .await
        });
        candidate_staged.notified().await;

        let frontier = crate::protocol::store_commit::CommitFrontier::from_refs(
            store_database(&reopened)
                .materialized_frontier()
                .await
                .expect("read competing acknowledgement frontier"),
        )
        .expect("shape competing acknowledgement frontier");
        reopened_store
            .stage_acknowledgement(frontier, "2026-07-18T00:01:00Z".to_string())
            .await
            .expect("stage competing acknowledgement");
        assert_eq!(
            reopened_store
                .drain_acknowledgements()
                .await
                .expect("publish competing acknowledgement"),
            1
        );
        resume_candidate.notify_one();
        let cancellation = cancellation_task
            .await
            .expect("join cancellation publication")
            .expect("cancel exclusion proposal");
        assert!(matches!(
            &cancellation,
            StoreDeviceExclusionResult::OutcomeActivated {
                outcome: StoreDeviceExclusionOutcomeRef::Cancelled(_),
                commit,
            } if commit.coord.sequence() == base_sequence + 2
        ));
        assert!(store_database(&reopened)
            .store_device_exclusion_freezes()
            .await
            .expect("read released exclusion freezes")
            .is_empty());
        let operations = reopened_store
            .device_exclusion_operations_for_test()
            .await
            .expect("list exclusion operations");
        assert_eq!(operations.len(), 2);
        assert!(operations.iter().all(|operation| matches!(
            operation.status,
            StoreDeviceExclusionOperationStatus::Completed(_)
        )));
    })
    .await;
}

#[tokio::test]
async fn remaining_device_freezes_and_acknowledges_before_owner_exclusion() {
    Box::pin(async {
        let signer = UserKeypair::generate();
        let owner_db = open_test_db();
        let store = Arc::new(
            Box::pin(TestStore::create(
                &owner_db,
                "device-exclusion-two-device-store",
                signer.clone(),
                crate::sync::test_helpers::test_cloud_home(),
            ))
            .await
            .expect("create two-device exclusion Store"),
        );
        let owner_device = Box::pin(store.open_into(&owner_db))
            .await
            .expect("open two-device exclusion Store");
        let peer_db = open_test_db();
        Box::pin(store.activate_joined_device(
            &owner_db,
            &peer_db,
            &signer,
            "2026-07-18T00:00:00Z",
        ))
        .await
        .expect("activate peer Store device");

        let local_device_id = owner_device.device_id.clone();
        let target = store_database(&owner_db)
            .activated_store_device_registration_records()
            .await
            .expect("list active Store registrations")
            .into_iter()
            .map(|registration| registration.reference().clone())
            .find(|reference| reference.device_id.to_string() != local_device_id)
            .expect("peer Store registration");
        finalize_peer_exclusion_detached(owner_device, &target).await;
    })
    .await;
}

#[tokio::test]
async fn snapshot_preserves_author_exclusion_activation_evidence() {
    let signer = UserKeypair::generate();
    let owner_db = open_test_db();
    let home = crate::sync::test_helpers::test_cloud_home();
    let store = Arc::new(
        Box::pin(TestStore::create(
            &owner_db,
            "snapshot-author-exclusion-store",
            signer.clone(),
            home.clone(),
        ))
        .await
        .expect("create snapshot exclusion Store"),
    );
    let (_restore_store_dir_temp, restore_store_dir) = temp_store_dir();
    let owner_device = Box::pin(store.open_into(&owner_db))
        .await
        .expect("open snapshot exclusion Store");
    let peer_db = open_test_db();
    let peer_device = Box::pin(store.activate_joined_device(
        &owner_db,
        &peer_db,
        &signer,
        "2026-07-18T00:00:00Z",
    ))
    .await
    .expect("activate snapshot exclusion peer");
    let (_candidate_temp, _candidate_store_dir, candidate_write_id) =
        Box::pin(peer_device.prepare_blocked_transfer_candidate("snapshot-excluded-candidate"))
            .await;
    let owner_device_id = owner_device.device_id.clone();
    let target = store_database(&owner_db)
        .activated_store_device_registration_records()
        .await
        .expect("list snapshot exclusion registrations")
        .into_iter()
        .map(|registration| registration.reference().clone())
        .find(|reference| reference.device_id.to_string() != owner_device_id)
        .expect("snapshot exclusion peer registration");
    let exclusion = owner_device.finalize_peer_exclusion(&target).await;
    let restore = owner_device
        .restore_membership()
        .await
        .expect("retain post-exclusion snapshot membership authority");
    let live_evidence = owner_db
        .test_sql(|database| database.author_exclusion_activation_evidence())
        .await
        .expect("read live author exclusion evidence");

    let directory = tempfile::tempdir().expect("snapshot exclusion image directory");
    let snapshot_dir = directory.path().to_path_buf();
    let owner_database = store_database(&owner_db);
    let image = owner_database
        .capture_snapshot_image_for_test(store.root.clone(), snapshot_dir, None)
        .await
        .expect("create author exclusion snapshot");
    let snapshot_coverage = crate::protocol::store_commit::CommitFrontier::from_refs(
        owner_database
            .materialized_frontier()
            .await
            .expect("read author exclusion snapshot frontier"),
    )
    .expect("shape author exclusion snapshot frontier");
    owner_device
        .publish_snapshot(image.clone(), snapshot_coverage.clone())
        .await
        .expect("publish author exclusion snapshot");
    owner_device
        .publish_acknowledgement(snapshot_coverage)
        .await
        .expect("acknowledge author exclusion snapshot");
    let image = crate::database::DatabaseImageTest::from_bytes(&image)
        .expect("open author exclusion snapshot image");
    let stored: (String, String, String, String) = image
        .author_exclusion_activation_evidence()
        .expect("snapshot carries author exclusion evidence");
    assert_eq!(stored, live_evidence);
    assert_eq!(
        serde_json::from_str::<StoreDeviceExclusionRef>(&stored.0)
            .expect("parse snapshotted exclusion reference"),
        exclusion,
    );
    for tamper in [
        AuthorExclusionLocatorTamper::Missing,
        AuthorExclusionLocatorTamper::ExclusionReference,
        AuthorExclusionLocatorTamper::AcceptedCut,
        AuthorExclusionLocatorTamper::ActivationCommit,
        AuthorExclusionLocatorTamper::ActivationHead,
    ] {
        let mut snapshot = Box::pin(PublishedExclusionSnapshot::open(
            &store,
            &restore_store_dir,
            &restore.membership_floor,
            owner_db.schema_version(),
            &signer,
            target.device_id.to_string(),
        ))
        .await;
        let restored = &mut snapshot.restored;
        restored
            .transfer_prepared_write_from_for_test(
                &StoreDatabase::new(&peer_db),
                &candidate_write_id,
            )
            .await
            .expect("transfer prepared write");
        let transferred_candidate = restored
            .blocked_merge_candidate_for_test(candidate_write_id.clone())
            .await
            .expect("load candidate before tampering with snapshot evidence")
            .expect("transferred candidate exists before snapshot evidence tamper");
        restored
            .tamper_author_exclusion_locator_for_test(
                &exclusion,
                &transferred_candidate.head.value.commit,
                tamper,
            )
            .await
            .expect("tamper author exclusion locator");
        restored
            .abandon_merge_candidate_for_test(candidate_write_id.clone())
            .await
            .expect_err("tampered snapshot exclusion evidence must fail loud");
        assert!(restored
            .blocked_merge_candidate_for_test(candidate_write_id.clone())
            .await
            .expect("reload candidate after tampered snapshot evidence")
            .is_some());
        assert!(!restored
            .merge_candidate_cleanup_pending_for_test(&candidate_write_id)
            .await
            .expect("tampered snapshot evidence cannot start cleanup"));
    }

    let mut snapshot = Box::pin(PublishedExclusionSnapshot::open(
        &store,
        &restore_store_dir,
        &restore.membership_floor,
        owner_db.schema_version(),
        &signer,
        target.device_id.to_string(),
    ))
    .await;
    let restored = &mut snapshot.restored;
    restored
        .transfer_prepared_write_from_for_test(&StoreDatabase::new(&peer_db), &candidate_write_id)
        .await
        .expect("transfer prepared write");
    let transferred_candidate = restored
        .blocked_merge_candidate_for_test(candidate_write_id.clone())
        .await
        .expect("load restored exclusion candidate")
        .expect("restored exclusion candidate exists");
    restored
        .author_exclusion_activation_for_candidate_for_test(
            transferred_candidate.head.value.commit.clone(),
            transferred_candidate
                .commit
                .value
                .author_registration
                .clone(),
        )
        .await
        .expect("select snapshotted exclusion locator")
        .expect("snapshotted exclusion covers restored candidate");
    assert_eq!(
        restored
            .abandon_merge_candidate_for_test(candidate_write_id.clone())
            .await
            .expect("consume snapshotted exclusion evidence"),
        MergeCandidateAbandonment::Abandoned,
    );
    assert!(!restored
        .merge_candidate_cleanup_pending_for_test(&candidate_write_id)
        .await
        .expect("restored candidate cleanup completes"));
}

#[tokio::test]
async fn device_join_bootstrap_records_exclusion_replayed_after_snapshot() {
    Box::pin(async {
        let signer = UserKeypair::generate();
        let owner_db = open_test_db();
        let store = Arc::new(
            Box::pin(TestStore::create(
                &owner_db,
                "bootstrap-author-exclusion-store",
                signer.clone(),
                crate::sync::test_helpers::test_cloud_home(),
            ))
            .await
            .expect("create bootstrap exclusion Store"),
        );
        let owner_device = Box::pin(store.open_into(&owner_db))
            .await
            .expect("open bootstrap exclusion Store");
        let restore = owner_device
            .restore_membership()
            .await
            .expect("retain bootstrap exclusion membership authority");
        let peer_db = open_test_db();
        let peer_device = store
            .activate_joined_device(&owner_db, &peer_db, &signer, "2026-07-18T00:00:00Z")
            .await
            .expect("activate bootstrap exclusion peer");
        let (_candidate_temp, _candidate_store_dir, candidate_write_id) = Box::pin(
            peer_device.prepare_blocked_transfer_candidate("bootstrap-excluded-candidate"),
        )
        .await;
        let owner_device_id = owner_device.device_id.clone();
        let target = store_database(&owner_db)
            .activated_store_device_registration_records()
            .await
            .expect("list bootstrap exclusion registrations")
            .into_iter()
            .map(|registration| registration.reference().clone())
            .find(|reference| reference.device_id.to_string() != owner_device_id)
            .expect("bootstrap exclusion peer registration");
        let proposal = Box::pin(owner_device.prepare_peer_exclusion(&target)).await;

        let image_dir = tempfile::tempdir().expect("bootstrap snapshot image directory");
        let snapshot_dir = image_dir.path().to_path_buf();
        let owner_database = store_database(&owner_db);
        let image = owner_database
            .capture_snapshot_image_for_test(store.root.clone(), snapshot_dir, None)
            .await
            .expect("create pre-exclusion snapshot");
        let snapshot_coverage = crate::protocol::store_commit::CommitFrontier::from_refs(
            owner_database
                .materialized_frontier()
                .await
                .expect("read pre-exclusion frontier"),
        )
        .expect("shape pre-exclusion frontier");
        owner_device
            .publish_snapshot(image, snapshot_coverage.clone())
            .await
            .expect("publish pre-exclusion snapshot");
        let published_snapshot = crate::database::StoreDatabase::new(&owner_db)
            .latest_local_store_snapshot()
            .await
            .expect("read published pre-exclusion snapshot")
            .expect("published pre-exclusion snapshot exists");
        let (_peer_pull_temp, peer_pull_dir) = crate::sync::test_helpers::temp_store_dir();
        let peer_pull = store
            .pull_into_result(&peer_db, &peer_pull_dir)
            .await
            .expect("materialize pre-exclusion snapshot coverage on peer")
            .1;
        assert!(peer_pull.held_positions.is_empty());
        for (device, timestamp) in [
            (&owner_device, "2026-07-18T00:00:01Z"),
            (&peer_device, "2026-07-18T00:00:02Z"),
        ] {
            let acknowledgement = device
                .stage_acknowledgement(snapshot_coverage.clone(), timestamp.to_string())
                .await
                .expect("stage pre-exclusion snapshot acknowledgement");
            let locator = acknowledgement
                .snapshot
                .clone()
                .expect("acknowledgement selects the stable snapshot candidate");
            assert_eq!(
                locator.author_registration,
                published_snapshot.meta.author_registration
            );
            assert_eq!(locator.snapshot, published_snapshot.reference);
            device
                .drain_acknowledgements()
                .await
                .expect("activate pre-exclusion snapshot acknowledgement");
        }

        let exclusion = owner_device.activate_peer_exclusion(&proposal).await;
        let activation = owner_device
            .latest_local_store_position()
            .await
            .expect("read exclusion activation position")
            .expect("exclusion activation position exists");
        let activation_commit = owner_device
            .load_commit_for_test(&activation)
            .await
            .expect("load exclusion activation commit");
        assert!(activation_commit
            .value()
            .device_exclusion_outcomes()
            .contains(&StoreDeviceExclusionOutcomeRef::Excluded(exclusion.clone())));
        let replay_cut = activation_commit
            .value()
            .order
            .predecessor_cut()
            .expect("read exclusion activation predecessor");
        let plan = owner_device
            .prepare_device_join_bootstrap_for_test(
                &replay_cut,
                &activation,
                &activation_commit.value().membership_state,
            )
            .await
            .expect("prepare post-snapshot exclusion replay");

        let destination = tempfile::tempdir().expect("bootstrap exclusion destination");
        let database_path = destination.path().join("store.db");
        let bootstrap_floor = restore.membership_floor.clone();
        let bootstrap = Box::pin(store.prepare_snapshot_bootstrap(
            &bootstrap_floor,
            1,
            &database_path,
            &signer,
        ))
        .await
        .expect("verify pre-exclusion snapshot");
        let store_dir = StoreDir::new(destination.path());
        let mut joining_db = bootstrap
            .install(
                &store_dir,
                crate::sync::test_helpers::test_synced_tables(),
                crate::protocol::blob::BLOB_TOMBSTONE_GRACE,
                crate::protocol::blob::TransferLimits::one_at_a_time(),
                "post-snapshot-joining-device".to_string(),
                std::sync::Arc::new(crate::clock::SystemClock),
                &crate::sync::test_helpers::test_migrations(),
                None,
            )
            .await
            .expect("open pre-exclusion snapshot");
        joining_db
            .install_device_join_bootstrap_for_test(plan)
            .await
            .expect("replay exclusion after snapshot");
        joining_db
            .transfer_prepared_write_from_for_test(
                &StoreDatabase::new(&peer_db),
                &candidate_write_id,
            )
            .await
            .expect("transfer prepared write");

        let stored = joining_db
            .author_exclusion_activation_evidence_for_test(&exclusion)
            .await
            .expect("replayed exclusion has exact activation evidence");
        assert!(!stored.0.is_empty());
        assert!(!stored.1.is_empty());
        assert_eq!(
            joining_db
                .abandon_merge_candidate_for_test(candidate_write_id.clone())
                .await
                .expect("consume replayed exclusion evidence"),
            MergeCandidateAbandonment::Abandoned,
        );
        assert!(!joining_db
            .merge_candidate_cleanup_pending_for_test(&candidate_write_id)
            .await
            .expect("replayed exclusion candidate cleanup completes"));
    })
    .await;
}

async fn finalize_peer_exclusion_detached(
    owner_device: TestDevice,
    target: &StoreDeviceRegistrationRef,
) -> StoreDeviceExclusionRef {
    let target = target.clone();
    tokio::spawn(async move { Box::pin(owner_device.finalize_peer_exclusion(&target)).await })
        .await
        .expect("join peer exclusion finalization")
}

#[tokio::test]
async fn excluded_author_discards_a_candidate_without_a_head_after_restart_and_delete_failure() {
    Box::pin(run_excluded_author_candidate_cleanup(
        ExcludedCandidateHeadPublication::Absent,
        false,
        false,
        PreparedAbandonmentHeadPublication::Absent,
    ))
    .await;
}

#[tokio::test]
async fn excluded_author_removes_indexed_shared_blob_ownership_without_deleting_the_blob() {
    Box::pin(run_excluded_author_candidate_cleanup_case(
        ExcludedCandidateHeadPublication::Absent,
        false,
        false,
        PreparedAbandonmentHeadPublication::Absent,
        true,
        false,
        None,
    ))
    .await;
}

#[tokio::test]
async fn excluded_author_retains_an_exact_late_candidate_head_as_protocol_inert() {
    Box::pin(run_excluded_author_candidate_cleanup(
        ExcludedCandidateHeadPublication::ExactLate,
        false,
        false,
        PreparedAbandonmentHeadPublication::Absent,
    ))
    .await;
}

#[tokio::test]
async fn excluded_author_reconciles_an_exact_head_created_after_absent_proof() {
    Box::pin(run_excluded_author_candidate_cleanup(
        ExcludedCandidateHeadPublication::AfterAbsentProofExactLate,
        false,
        false,
        PreparedAbandonmentHeadPublication::Absent,
    ))
    .await;
}

#[tokio::test]
async fn excluded_author_accepts_an_authenticated_winner_created_after_absent_proof() {
    Box::pin(run_excluded_author_candidate_cleanup(
        ExcludedCandidateHeadPublication::AfterAbsentProofThirdWinner,
        false,
        false,
        PreparedAbandonmentHeadPublication::Absent,
    ))
    .await;
}

#[tokio::test]
async fn exclusion_materialized_after_commit_upload_blocks_candidate_head_creation() {
    Box::pin(run_excluded_author_candidate_cleanup(
        ExcludedCandidateHeadPublication::AfterCommitUpload,
        false,
        false,
        PreparedAbandonmentHeadPublication::Absent,
    ))
    .await;
}

#[tokio::test]
async fn exclusion_materialized_after_head_readback_blocks_activation_and_retains_the_head() {
    Box::pin(run_excluded_author_candidate_cleanup(
        ExcludedCandidateHeadPublication::AfterHeadReadBack,
        false,
        false,
        PreparedAbandonmentHeadPublication::Absent,
    ))
    .await;
}

#[tokio::test]
async fn accepted_candidate_is_retracted_when_its_author_exclusion_arrives() {
    Box::pin(run_excluded_author_candidate_cleanup_case(
        ExcludedCandidateHeadPublication::AfterHeadReadBack,
        false,
        false,
        PreparedAbandonmentHeadPublication::Absent,
        false,
        true,
        None,
    ))
    .await;
}

#[tokio::test]
async fn summary_materialization_failure_rolls_back_terminal_merge_transaction() {
    Box::pin(run_excluded_author_candidate_cleanup_case(
        ExcludedCandidateHeadPublication::AfterHeadReadBack,
        false,
        false,
        PreparedAbandonmentHeadPublication::Absent,
        false,
        true,
        Some(TerminalMergeTransactionFailure::Injected(
            crate::database::MergeMaterializationFailurePoint::SummaryMaterialization,
        )),
    ))
    .await;
}

#[tokio::test]
async fn retraction_deletion_failure_rolls_back_terminal_merge_transaction() {
    Box::pin(run_excluded_author_candidate_cleanup_case(
        ExcludedCandidateHeadPublication::AfterHeadReadBack,
        false,
        false,
        PreparedAbandonmentHeadPublication::Absent,
        false,
        true,
        Some(TerminalMergeTransactionFailure::Injected(
            crate::database::MergeMaterializationFailurePoint::RetractionDeletion,
        )),
    ))
    .await;
}

#[tokio::test]
async fn projection_replacement_failure_rolls_back_terminal_merge_transaction() {
    Box::pin(run_excluded_author_candidate_cleanup_case(
        ExcludedCandidateHeadPublication::AfterHeadReadBack,
        false,
        false,
        PreparedAbandonmentHeadPublication::Absent,
        false,
        true,
        Some(TerminalMergeTransactionFailure::Injected(
            crate::database::MergeMaterializationFailurePoint::ProjectionReplacement,
        )),
    ))
    .await;
}

#[tokio::test]
async fn missing_retracted_device_state_rolls_back_terminal_merge_transaction() {
    Box::pin(run_excluded_author_candidate_cleanup_case(
        ExcludedCandidateHeadPublication::AfterHeadReadBack,
        false,
        false,
        PreparedAbandonmentHeadPublication::Absent,
        false,
        true,
        Some(TerminalMergeTransactionFailure::DeleteDeviceStateDuringRetraction),
    ))
    .await;
}

#[tokio::test]
async fn mutated_author_exclusion_activation_head_blocks_reload_and_cleanup() {
    Box::pin(run_excluded_author_candidate_cleanup(
        ExcludedCandidateHeadPublication::Absent,
        true,
        false,
        PreparedAbandonmentHeadPublication::Absent,
    ))
    .await;
}

#[tokio::test]
async fn exclusion_nonactivates_a_prepared_merge_abandonment_and_original_candidate() {
    Box::pin(run_excluded_author_candidate_cleanup(
        ExcludedCandidateHeadPublication::Absent,
        false,
        true,
        PreparedAbandonmentHeadPublication::Absent,
    ))
    .await;
}

#[tokio::test]
async fn exclusion_nonactivates_prepared_abandonment_with_exact_original_head() {
    Box::pin(run_excluded_author_candidate_cleanup(
        ExcludedCandidateHeadPublication::Absent,
        false,
        true,
        PreparedAbandonmentHeadPublication::Original,
    ))
    .await;
}

#[tokio::test]
async fn exclusion_nonactivates_prepared_abandonment_with_exact_authority_head() {
    Box::pin(run_excluded_author_candidate_cleanup(
        ExcludedCandidateHeadPublication::Absent,
        false,
        true,
        PreparedAbandonmentHeadPublication::Authority,
    ))
    .await;
}

#[tokio::test]
async fn exclusion_nonactivates_prepared_abandonment_with_a_third_winner() {
    Box::pin(run_excluded_author_candidate_cleanup(
        ExcludedCandidateHeadPublication::Absent,
        false,
        true,
        PreparedAbandonmentHeadPublication::ThirdWinner,
    ))
    .await;
}

#[derive(Clone, Copy)]
enum ExcludedCandidateHeadPublication {
    Absent,
    ExactLate,
    AfterAbsentProofExactLate,
    AfterAbsentProofThirdWinner,
    AfterCommitUpload,
    AfterHeadReadBack,
}

#[derive(Clone, Copy)]
enum PreparedAbandonmentHeadPublication {
    Absent,
    Original,
    Authority,
    ThirdWinner,
}

enum ExpectedHeldCandidate<'a> {
    None,
    ConcurrentExactOrNone(&'a StoreBatchCommitRef),
}

struct ExcludedPeer<'a> {
    database: &'a Database,
    store: &'a TestStore,
    store_dir: &'a StoreDir,
}

impl<'a> ExcludedPeer<'a> {
    fn new(database: &'a Database, store: &'a TestStore, store_dir: &'a StoreDir) -> Self {
        Self {
            database,
            store,
            store_dir,
        }
    }

    async fn pull_exclusion(&self, expected_held: ExpectedHeldCandidate<'_>) {
        let pull = self
            .store
            .pull_into_result(self.database, self.store_dir)
            .await
            .expect("pull peer exclusion")
            .1;
        let is_exact_candidate_hold = |candidate: &StoreBatchCommitRef| {
            matches!(
                pull.held_positions.as_slice(),
                [crate::sync::store::owner::pull::HeldStorePosition {
                    coordinate:
                        crate::sync::store::owner::pull::HeldStoreCoordinate::Commit {
                            commit,
                            ..
                        },
                    reason:
                        crate::sync::store::owner::pull::HeldStorePositionReason::InactiveDevice {
                            ..
                        },
                }] if commit == candidate
            )
        };
        match expected_held {
            ExpectedHeldCandidate::None => assert!(
                pull.held_positions.is_empty(),
                "held: {:?}",
                pull.held_positions
            ),
            ExpectedHeldCandidate::ConcurrentExactOrNone(candidate) => assert!(
                pull.held_positions.is_empty() || is_exact_candidate_hold(candidate),
                "expected no hold or exact concurrent candidate {candidate:?}, held: {:?}",
                pull.held_positions,
            ),
        }
    }

    async fn finish_prepared_cleanup(
        &self,
        signer: &UserKeypair,
        write_id: WriteId,
        candidates: &crate::database::PreparedMergeAbandonmentCandidates,
        candidate_commit_context: &ProtocolObjectContext,
        publication: PreparedAbandonmentHeadPublication,
    ) {
        self.pull_exclusion(ExpectedHeldCandidate::None).await;
        match publication {
            PreparedAbandonmentHeadPublication::Absent => {}
            PreparedAbandonmentHeadPublication::Original => {
                self.store
                    .publish_prepared_protocol_object(&candidates.candidate.head.prepared)
                    .await
                    .expect("publish exact original candidate head");
            }
            PreparedAbandonmentHeadPublication::Authority => {
                self.store
                    .publish_prepared_protocol_object(&candidates.authority.commit.prepared)
                    .await
                    .expect("publish abandonment authority commit");
                self.store
                    .publish_prepared_protocol_object(&candidates.authority.head.prepared)
                    .await
                    .expect("publish exact abandonment authority head");
            }
            PreparedAbandonmentHeadPublication::ThirdWinner => {
                self.store
                    .publish_third_candidate_winner(self.database, &candidates.candidate)
                    .await;
            }
        }
        assert_eq!(
            self.store
                .bind_device(self.database, signer)
                .await
                .expect("bind Merge abandonment Store")
                .abandon_merge_candidate(write_id.clone())
                .await
                .expect("exclude prepared abandonment candidates"),
            MergeCandidateAbandonment::Abandoned,
        );
        for reference in [
            &candidates.candidate.head.value.commit,
            &candidates.authority.head.value.commit,
        ] {
            let prefix = crate::protocol::store_commit::semantic_prefix_from_exact_object(
                &reference.object,
                ".json",
            )
            .expect("derive cleaned candidate commit prefix");
            assert!(matches!(
                self.store
                    .read_exact_protocol_object(
                        candidate_commit_context,
                        &reference.object,
                        &prefix,
                    )
                    .await,
                Err(crate::protocol::objects::StorageError::NotFound(_))
            ));
        }
        let peer_store = self
            .store
            .bind_device(self.database, signer)
            .await
            .expect("bind exclusion cleanup Store");
        for commit in [
            &candidates.candidate.commit.value,
            &candidates.authority.commit.value,
        ] {
            if commit.store_package().is_some() {
                assert!(matches!(
                    peer_store
                        .load_store_package_for_test(commit.reference())
                        .await,
                    Err(StoreError::Object(
                        crate::protocol::objects::StoreObjectError::Storage(
                            crate::protocol::objects::StorageError::NotFound(_)
                        )
                    ))
                ));
            }
        }
        assert_eq!(
            crate::database::StoreDatabase::new(self.database)
                .discard_blocked_write(&write_id)
                .await
                .expect("discard excluded prepared abandonment write"),
            crate::database::BlockedWriteDiscard::Discarded(vec![write_id]),
        );
    }
}

fn indexed_shared_blob(
    label: &str,
    candidate: &StoreBatchCommitRef,
    uploader: &StoreDeviceRegistrationRef,
    activated: std::collections::BTreeSet<crate::protocol::remote_object::SharedObjectOwner>,
) -> crate::protocol::remote_object::RemoteObjectRecord {
    let stored_bytes = format!("stored excluded-author blob {label}").into_bytes();
    let locator = crate::protocol::blob::locator::BlobLocator::opaque(
        "excluded-author-test",
        label,
        uploader.clone(),
        crate::protocol::blob::locator::RemoteAudience::Store,
        crate::BlobScope::Master,
        crate::KeyFingerprint::from_bytes([17; 32]),
        1,
        ObjectHash::digest(format!("plaintext excluded-author blob {label}").as_bytes()),
    )
    .expect("construct indexed shared blob locator");
    let object = crate::protocol::objects::ExactObjectRef::new(
        crate::protocol::objects::ObjectSlot::logical(locator.semantic_key())
            .expect("construct indexed shared blob slot"),
        u64::try_from(stored_bytes.len()).expect("indexed shared blob size fits u64"),
        ObjectHash::digest(&stored_bytes),
    );
    let locator_bytes = locator.to_bytes();
    let record = crate::protocol::remote_object::RemoteObjectRecord::SharedLiveSet(
        crate::protocol::remote_object::SharedObjectRecord {
            identity: crate::protocol::remote_object::SharedLiveSetObjectRef {
                domain: crate::protocol::remote_object::SharedLiveSetObjectDomain::StoredBlob,
                semantic_hash: ObjectHash::digest(&locator_bytes),
                object: object.clone(),
            },
            bytes: crate::protocol::remote_object::RemoteObjectBytes::blob(locator_bytes, object)
                .expect("construct indexed shared blob bytes"),
            state: crate::protocol::remote_object::OwnedObjectState::UploadedVerified {
                ownership: crate::protocol::remote_object::SharedObjectOwnership {
                    pending: std::collections::BTreeSet::from([candidate.clone()]),
                    activated,
                    nonactivated: Vec::new(),
                },
            },
        },
    );
    record.validate().expect("validate indexed shared blob");
    record
}

async fn run_excluded_author_candidate_cleanup(
    head_publication: ExcludedCandidateHeadPublication,
    sabotage_activation_head: bool,
    prepare_abandonment: bool,
    prepared_head_publication: PreparedAbandonmentHeadPublication,
) {
    Box::pin(run_excluded_author_candidate_cleanup_case(
        head_publication,
        sabotage_activation_head,
        prepare_abandonment,
        prepared_head_publication,
        false,
        false,
        None,
    ))
    .await;
}

#[derive(Clone, Copy)]
enum TerminalMergeTransactionFailure {
    Injected(crate::database::MergeMaterializationFailurePoint),
    DeleteDeviceStateDuringRetraction,
}

async fn run_excluded_author_candidate_cleanup_case(
    head_publication: ExcludedCandidateHeadPublication,
    sabotage_activation_head: bool,
    prepare_abandonment: bool,
    prepared_head_publication: PreparedAbandonmentHeadPublication,
    index_shared_blobs: bool,
    materialize_before_exclusion: bool,
    transaction_failure: Option<TerminalMergeTransactionFailure>,
) {
    let signer = UserKeypair::generate();
    let owner_db = open_test_db();
    let home = crate::sync::test_helpers::test_cloud_home();
    let store = Arc::new(
        Box::pin(TestStore::create(
            &owner_db,
            "excluded-author-candidate-store",
            signer.clone(),
            home.clone(),
        ))
        .await
        .expect("create excluded-author Store"),
    );
    let owner_device = Box::pin(store.open_into(&owner_db))
        .await
        .expect("open excluded-author Store");
    let directory = tempfile::tempdir().expect("excluded-author database directory");
    let path = directory.path().join("excluded-peer.sqlite");
    let peer_db = open(&path, "excluded-peer-host");
    Box::pin(store.activate_joined_device(&owner_db, &peer_db, &signer, "2026-07-18T01:00:00Z"))
        .await
        .expect("activate excluded peer");
    let (_store_temp, store_dir) = temp_store_dir();
    if materialize_before_exclusion {
        Box::pin(async {
            owner_db
                .execute_test_host_write(
                    "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                     VALUES ('surviving-owner-note', 'surviving', NULL, 1, \
                             '0000000001500-0000-owner', '2026-07-18')",
                )
                .await;
            let owner_device = store
                .bind_device(&owner_db, &signer)
                .await
                .expect("bind surviving owner Store");
            assert!(owner_device
                .prepare_pending_store_write(&store_dir)
                .await
                .expect("prepare surviving owner commit"));
            owner_device
                .drain_store_writes()
                .await
                .expect("publish surviving owner commit");
            store
                .pull_into_result(&peer_db, &store_dir)
                .await
                .expect("materialize surviving owner commit on excluded peer");
        })
        .await;
    }
    peer_db
        .execute_test_host_write(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('excluded-peer-note', 'pending', NULL, 1, \
                     '0000000002000-0000-excluded-peer', '2026-07-18')",
        )
        .await;
    let peer_device_id = store
        .bind_device(&peer_db, &signer)
        .await
        .expect("bind excluded peer Store")
        .device_id;
    let peer_device = store
        .bind_device(&peer_db, &signer)
        .await
        .expect("bind excluded peer Store");
    assert!(peer_device
        .prepare_pending_store_write(&store_dir)
        .await
        .expect("prepare excluded peer candidate"));
    let candidate = crate::database::StoreDatabase::new(&peer_db)
        .oldest_prepared_store_write()
        .await
        .expect("load excluded peer candidate")
        .expect("excluded peer candidate exists");
    let candidate_ref = candidate.head.value.commit.clone();
    let candidate_graph_objects =
        crate::protocol::remote_object::CandidateObjectGraph::from_commit(&candidate.commit.value)
            .expect("read excluded candidate object graph")
            .exact_objects()
            .cloned()
            .collect::<Vec<_>>();
    let candidate_head = candidate.head.object.clone();
    let candidate_head_context = ProtocolObjectContext::signed_plaintext(
        store.root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let candidate_head_prefix = crate::protocol::store_commit::head_slot_prefix(
        &candidate
            .head
            .value
            .author_registration
            .device_id
            .to_string(),
        candidate_ref.coord.sequence(),
    );
    let candidate_commit_context = ProtocolObjectContext::signed_plaintext(
        store.root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let candidate_commit_prefix = crate::protocol::store_commit::semantic_prefix_from_exact_object(
        &candidate_ref.object,
        ".json",
    )
    .expect("derive excluded candidate commit prefix");
    let write_id = candidate.commit.value.write_id.clone();
    store
        .storage()
        .create_protocol_object(&candidate.commit.prepared)
        .await
        .expect("upload excluded peer candidate commit");
    crate::database::StoreDatabase::new(&peer_db)
        .mark_candidate_commit_uploaded(candidate_ref.clone())
        .await
        .expect("record uploaded excluded peer commit");
    let target_registration = store_database(&peer_db)
        .activated_store_device_registration_records()
        .await
        .expect("load excluded peer registration")
        .into_iter()
        .find(|registration| registration.reference().device_id.to_string() == peer_device_id)
        .expect("exact excluded peer registration");
    let target = target_registration.reference().clone();
    let prepared_abandonment = if prepare_abandonment {
        crate::database::StoreDatabase::new(&peer_db)
            .set_write_status(
                &write_id,
                crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                    reason: "prepare abandonment before exclusion".to_string(),
                }),
            )
            .await
            .expect("block candidate before abandonment preparation");
        let peer_device = store
            .bind_device(&peer_db, &signer)
            .await
            .expect("bind abandonment preparation Store");
        assert!(peer_device
            .prepare_merge_candidate_abandonment(write_id.clone())
            .await
            .expect("prepare abandonment before exclusion"));
        crate::database::StoreDatabase::new(&peer_db)
            .prepared_merge_abandonment_candidates(write_id.clone())
            .await
            .expect("load prepared abandonment candidates")
            .map(Box::new)
    } else {
        None
    };
    if materialize_before_exclusion {
        peer_device
            .drain_store_writes()
            .await
            .expect("publish excluded peer candidate before exclusion");
        let original = match crate::database::StoreDatabase::new(&peer_db)
            .write_status(&write_id)
            .await
            .expect("load accepted candidate status")
        {
            crate::WriteStatus::Published(position) => *position,
            status => panic!("candidate was not accepted before exclusion: {status:?}"),
        };
        assert_eq!(original.commit(), &candidate_ref);
        peer_db
            .execute_test_host_write(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('excluded-peer-local-note', 'local', NULL, 0, \
                         '0000000002001-0000-excluded-peer', '2026-07-18')",
            )
            .await;
        let (local_status, local_partitions, local_changeset_bytes) = peer_db
            .test_sql(|database| database.latest_local_write_facts())
            .await
            .expect("load local-only replay input");
        assert_eq!(local_status, "\"local_only\"");
        assert_eq!(local_partitions, 1);
        assert!(local_changeset_bytes > 0);
        finalize_peer_exclusion_detached(owner_device.clone(), &target).await;
        let activation_commit = crate::database::StoreDatabase::new(&owner_db)
            .author_exclusion_activation_for_candidate(
                store.root.clone(),
                candidate_ref.clone(),
                target.clone(),
            )
            .await
            .expect("load terminal transaction activation")
            .expect("owner exclusion covers the accepted candidate")
            .activation_commit()
            .clone();
        if let Some(failure) = transaction_failure {
            Box::pin(async {
                match failure {
                    TerminalMergeTransactionFailure::Injected(point) => {
                        peer_db.fail_next_merge_materialization_at(point);
                    }
                    TerminalMergeTransactionFailure::DeleteDeviceStateDuringRetraction => {
                        peer_db
                            .test_sql(|database| {
                                database.install_retracted_device_state_failure_trigger()
                            })
                            .await
                            .expect("install early device-state deletion trigger");
                    }
                }
                let error = store
                    .pull_into_result(&peer_db, &store_dir)
                    .await
                    .expect_err("injected terminal Merge transaction failure");
                let expected = match failure {
                    TerminalMergeTransactionFailure::Injected(_) => "injected failure",
                    TerminalMergeTransactionFailure::DeleteDeviceStateDuringRetraction => {
                        "retracted Merge device state disappeared"
                    }
                };
                assert!(
                    error.to_string().contains(expected),
                    "unexpected terminal transaction error: {error:?}"
                );
                let StoreCommitCoord {
                    stream_id,
                    sequence,
                } = &activation_commit.coord;
                assert!(store_database(&peer_db)
                    .exact_materialized_ref(&stream_id.to_string(), *sequence)
                    .await
                    .expect("reload rolled-back activation coordinate")
                    .is_none());
                store_database(&peer_db)
                    .retained_merge_materialization(store.root.clone(), original.commit().clone())
                    .await
                    .expect("rolled-back retraction retains the original materialization");
                assert!(matches!(
                    crate::database::StoreDatabase::new(&peer_db)
                        .write_status(&write_id)
                        .await
                        .expect("reload rolled-back write status"),
                    crate::WriteStatus::Published(position) if position.as_ref() == &original
                ));
                assert_eq!(
                    peer_db
                        .test_sql(|connection| {
                            connection
                                .query_row(
                                    "SELECT COUNT(*) FROM notes WHERE id IN (
                                         'excluded-peer-note',
                                         'excluded-peer-local-note',
                                         'surviving-owner-note'
                                     )",
                                    [],
                                    |row| row.get::<_, i64>(0),
                                )
                                .map_err(crate::database::DbError::from)
                        })
                        .await
                        .expect("count rows after transaction rollback"),
                    3,
                );
                assert!(!crate::database::StoreDatabase::new(&peer_db)
                    .merge_candidate_cleanup_pending(&write_id)
                    .await
                    .expect("rolled-back transaction created no cleanup"));
            })
            .await;
            if matches!(
                failure,
                TerminalMergeTransactionFailure::DeleteDeviceStateDuringRetraction
            ) {
                return;
            }
        }
        home.fail_exact_delete_on_call(1);
        assert!(store.pull_into_result(&peer_db, &store_dir).await.is_err());
        let witness = match crate::database::StoreDatabase::new(&peer_db)
            .write_status(&write_id)
            .await
            .expect("load retracted candidate status")
        {
            crate::WriteStatus::Resolved(crate::WriteResolution::Retracted { witness }) => witness,
            status => panic!("accepted candidate was not retracted: {status:?}"),
        };
        assert_eq!(witness.original_position(), &original);
        let row_count = peer_db
            .test_sql(|connection| {
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM notes WHERE id = 'excluded-peer-note'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(crate::database::DbError::from)
            })
            .await
            .expect("count retracted host row");
        assert_eq!(row_count, 0);
        let local_row_count = peer_db
            .test_sql(|connection| {
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM notes
                             WHERE id = 'excluded-peer-local-note'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(crate::database::DbError::from)
            })
            .await
            .expect("count retained local-only host row");
        assert_eq!(local_row_count, 1);
        let surviving_row_count = peer_db
            .test_sql(|connection| {
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM notes WHERE id = 'surviving-owner-note'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(crate::database::DbError::from)
            })
            .await
            .expect("count surviving retained Store-package row");
        assert_eq!(surviving_row_count, 1);
        assert!(crate::database::StoreDatabase::new(&peer_db)
            .merge_candidate_cleanup_pending(&write_id)
            .await
            .expect("retracted candidate requires cleanup"));
        drop(peer_db);
        let reopened = open(&path, "excluded-peer-host");
        ExcludedPeer::new(&reopened, store.as_ref(), &store_dir)
            .pull_exclusion(ExpectedHeldCandidate::None)
            .await;
        assert!(!crate::database::StoreDatabase::new(&reopened)
            .merge_candidate_cleanup_pending(&write_id)
            .await
            .expect("retracted candidate cleanup completed"));
        assert!(matches!(
            crate::database::StoreDatabase::new(&reopened)
                .write_status(&write_id)
                .await
                .expect("reload retracted candidate status"),
            crate::WriteStatus::Resolved(crate::WriteResolution::Retracted {
                witness: current,
            }) if current == witness
        ));
        let prepared_count = reopened
            .test_sql({
                let write_id = write_id.clone();
                move |database| database.prepared_write_count(&write_id)
            })
            .await
            .expect("count retracted candidate preparation");
        assert_eq!(prepared_count, 0);
        return;
    }
    finalize_peer_exclusion_detached(owner_device, &target).await;
    if let Some(candidates) = prepared_abandonment {
        Box::pin(
            ExcludedPeer::new(&peer_db, store.as_ref(), &store_dir).finish_prepared_cleanup(
                &signer,
                write_id,
                &candidates,
                &candidate_commit_context,
                prepared_head_publication,
            ),
        )
        .await;
        return;
    }
    let publication_pause = match head_publication {
        ExcludedCandidateHeadPublication::AfterCommitUpload => Some(
            crate::database::DatabaseTestPoint::StoreWriteCommitUploaded {
                write_id: write_id.clone(),
            },
        ),
        ExcludedCandidateHeadPublication::AfterHeadReadBack => {
            Some(crate::database::DatabaseTestPoint::StoreWriteHeadReadBack {
                write_id: write_id.clone(),
            })
        }
        ExcludedCandidateHeadPublication::Absent
        | ExcludedCandidateHeadPublication::ExactLate
        | ExcludedCandidateHeadPublication::AfterAbsentProofExactLate
        | ExcludedCandidateHeadPublication::AfterAbsentProofThirdWinner => None,
    };
    let publish_error = if let Some(point) = publication_pause {
        let (commit_uploaded, resume) = peer_db.arm_test_pause(point);
        let drain_db = peer_db.clone();
        let drain_store = store.clone();
        let drain_signer = signer.clone();
        let drain = tokio::spawn(async move {
            let device = drain_store
                .bind_device(&drain_db, &drain_signer)
                .await
                .expect("bind paused excluded-author Store");
            device.drain_store_writes().await
        });
        commit_uploaded.notified().await;
        let expected_held = if matches!(
            head_publication,
            ExcludedCandidateHeadPublication::AfterHeadReadBack
        ) {
            ExpectedHeldCandidate::ConcurrentExactOrNone(&candidate_ref)
        } else {
            ExpectedHeldCandidate::None
        };
        ExcludedPeer::new(&peer_db, store.as_ref(), &store_dir)
            .pull_exclusion(expected_held)
            .await;
        if matches!(
            head_publication,
            ExcludedCandidateHeadPublication::AfterHeadReadBack
        ) {
            ExcludedPeer::new(&peer_db, store.as_ref(), &store_dir)
                .pull_exclusion(ExpectedHeldCandidate::None)
                .await;
        }
        resume.notify_one();
        drain
            .await
            .expect("join excluded-author publication")
            .expect_err("second exclusion check blocks candidate head")
    } else {
        ExcludedPeer::new(&peer_db, store.as_ref(), &store_dir)
            .pull_exclusion(ExpectedHeldCandidate::None)
            .await;
        if matches!(
            head_publication,
            ExcludedCandidateHeadPublication::ExactLate
        ) {
            store
                .storage()
                .create_protocol_object(&candidate.head.prepared)
                .await
                .expect("publish exact late excluded-author head");
            assert_eq!(
                store
                    .storage()
                    .read_protocol_object(
                        &candidate_head_context,
                        &candidate_head,
                        &candidate_head_prefix,
                    )
                    .await
                    .expect("read exact late excluded-author head"),
                candidate.head.value.to_bytes(),
            );
        }
        peer_device
            .drain_store_writes()
            .await
            .expect_err("excluded peer cannot activate its late candidate")
    };
    let peer_store = Store::load(
        StoreDatabase::new(&peer_db),
        store.storage(),
        store_dir.clone(),
        signer.clone(),
    )
    .await
    .expect("bind excluded peer Store");
    let local_position = peer_store
        .latest_local_store_position()
        .await
        .expect("load excluded peer position");
    drop(peer_store);
    assert!(matches!(
        publish_error,
        crate::sync::store::StoreError::AuthorExcluded { .. }
    ));
    match crate::database::StoreDatabase::new(&peer_db)
        .write_status(&write_id)
        .await
        .expect("load excluded peer write status")
    {
        crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState { reason }) => {
            assert!(reason.contains("excluded"));
        }
        crate::WriteStatus::Resolved(crate::WriteResolution::Retracted { witness })
            if matches!(
                head_publication,
                ExcludedCandidateHeadPublication::AfterHeadReadBack
            ) =>
        {
            assert_eq!(witness.original_position().commit(), &candidate_ref);
        }
        status => panic!("excluded peer write has unexpected status: {status:?}"),
    }
    assert!(matches!(
        crate::database::StoreDatabase::new(&peer_db)
            .merge_abandonment_state(&write_id)
            .await
            .expect("load excluded peer abandonment state"),
        crate::database::MergeAbandonmentState::None
    ));
    let indexed_shared_blobs = if index_shared_blobs {
        let snapshot_owner = crate::protocol::remote_object::SharedObjectOwner::Snapshot(
            crate::protocol::remote_object::SnapshotObjectOwner {
                activation: target_registration
                    .value()
                    .store_snapshot_activation(&target)
                    .expect("derive shared blob snapshot activation")
                    .activation_id(),
                generation: 0,
            },
        );
        let records = vec![
            indexed_shared_blob(
                "candidate-only",
                &candidate_ref,
                &target,
                std::collections::BTreeSet::new(),
            ),
            indexed_shared_blob(
                "snapshot-owned",
                &candidate_ref,
                &target,
                std::collections::BTreeSet::from([snapshot_owner]),
            ),
        ];
        let identities = records
            .iter()
            .map(|record| (record.object_id(), record.object().clone()))
            .collect::<Vec<_>>();
        let indexed_write_id = write_id.clone();
        peer_db
            .test_sql(move |database| {
                database.install_indexed_shared_blobs(&indexed_write_id, records)
            })
            .await
            .expect("index shared blobs under excluded candidate");
        identities
    } else {
        Vec::new()
    };
    drop(peer_db);

    let reopened = open(&path, "excluded-peer-host");
    let cleanup_pending = crate::database::StoreDatabase::new(&reopened)
        .merge_candidate_cleanup_pending(&write_id)
        .await
        .expect("load excluded peer cleanup state");
    if cleanup_pending {
        home.fail_exact_delete_on_call(1);
        assert!(store
            .bind_device(&reopened, &signer)
            .await
            .expect("bind Merge abandonment Store")
            .abandon_merge_candidate(write_id.clone())
            .await
            .is_err());
        assert!(crate::database::StoreDatabase::new(&reopened)
            .merge_candidate_cleanup_pending(&write_id)
            .await
            .expect("excluded peer cleanup remains pending"));
    } else {
        assert!(matches!(
            store
                .bind_device(&reopened, &signer)
                .await
                .expect("bind Merge abandonment Store")
                .abandon_merge_candidate(write_id.clone())
                .await
                .expect("observe completed excluded peer cleanup"),
            MergeCandidateAbandonment::NotRequired | MergeCandidateAbandonment::Abandoned
        ));
    }
    if cleanup_pending && !indexed_shared_blobs.is_empty() {
        let cleanup_targets = crate::database::StoreDatabase::new(&reopened)
            .merge_candidate_cleanup_targets(write_id.clone())
            .await
            .expect("load excluded candidate cleanup targets");
        for (_, object) in &indexed_shared_blobs {
            assert!(cleanup_targets
                .iter()
                .all(|target| &target.object != object));
        }
        let indexed = indexed_shared_blobs.clone();
        for (index, (_, object)) in indexed.into_iter().enumerate() {
            let record = reopened
                .remote_object_for_test(object)
                .await
                .expect("load indexed shared blob ownership transition");
            let crate::protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record) = record
            else {
                panic!("indexed blob changed remote-object domain");
            };
            match (index, record.state) {
                (0, crate::protocol::remote_object::OwnedObjectState::RetirementPending { .. }) => {
                }
                (
                    1,
                    crate::protocol::remote_object::OwnedObjectState::UploadedVerified {
                        ownership,
                    },
                ) if ownership.pending.is_empty() && ownership.activated.len() == 1 => {}
                _ => panic!("excluded candidate retained indexed shared blob ownership"),
            }
        }
    }
    let post_proof_database = reopened.clone();
    let post_proof_store = store.clone();
    let post_proof_write_id = write_id.clone();
    tokio::spawn(async move {
        if !matches!(
            head_publication,
            ExcludedCandidateHeadPublication::AfterAbsentProofExactLate
                | ExcludedCandidateHeadPublication::AfterAbsentProofThirdWinner
        ) {
            return;
        }
        let candidate = crate::database::StoreDatabase::new(&post_proof_database)
            .blocked_merge_candidate(post_proof_write_id)
            .await
            .expect("reload post-proof candidate")
            .expect("post-proof candidate remains prepared");
        match head_publication {
            ExcludedCandidateHeadPublication::AfterAbsentProofExactLate => {
                post_proof_store
                    .storage()
                    .create_protocol_object(&candidate.head.prepared)
                    .await
                    .expect("publish candidate head after absent proof");
            }
            ExcludedCandidateHeadPublication::AfterAbsentProofThirdWinner => {
                post_proof_store
                    .publish_third_candidate_winner(&post_proof_database, &candidate)
                    .await;
            }
            ExcludedCandidateHeadPublication::Absent
            | ExcludedCandidateHeadPublication::ExactLate
            | ExcludedCandidateHeadPublication::AfterCommitUpload
            | ExcludedCandidateHeadPublication::AfterHeadReadBack => unreachable!(),
        }
    })
    .await
    .expect("join post-proof candidate-head publication");
    if !cleanup_pending
        && matches!(
            head_publication,
            ExcludedCandidateHeadPublication::AfterAbsentProofExactLate
                | ExcludedCandidateHeadPublication::AfterAbsentProofThirdWinner
        )
    {
        assert_eq!(
            store
                .bind_device(&reopened, &signer)
                .await
                .expect("bind Merge abandonment Store")
                .abandon_merge_candidate(write_id.clone())
                .await
                .expect("reconcile candidate head published after the absence proof"),
            MergeCandidateAbandonment::Abandoned,
        );
    }
    if cleanup_pending && sabotage_activation_head {
        let mut remote = reopened
            .remote_object_for_test(candidate_ref.object.clone())
            .await
            .expect("load cleanup candidate ownership");
        {
            let crate::protocol::remote_object::RemoteObjectRecord::CandidateCommit(record) =
                &mut remote
            else {
                panic!("cleanup candidate is not a commit");
            };
            let crate::protocol::remote_object::CandidateCommitState::CleanupPending {
                proof:
                    crate::protocol::remote_object::CandidateNonactivationProof::AuthorExclusion {
                        activation_head,
                        ..
                    },
            } = &mut record.state
            else {
                panic!("cleanup candidate has no author-exclusion proof");
            };
            activation_head.head_hash =
                ObjectHash::digest(b"different durable author-exclusion activation head");
        }
        reopened
            .replace_remote_object_for_test(candidate_ref.object.clone(), remote)
            .await
            .expect("sabotage durable activation head");
        assert!(crate::database::StoreDatabase::new(&reopened)
            .merge_candidate_cleanup_pending(&write_id)
            .await
            .is_err());
        assert!(store
            .bind_device(&reopened, &signer)
            .await
            .expect("bind Merge abandonment Store")
            .abandon_merge_candidate(write_id)
            .await
            .is_err());
        return;
    }
    let retried = if cleanup_pending {
        drop(reopened);
        let retried = open(&path, "excluded-peer-host");
        assert_eq!(
            store
                .bind_device(&retried, &signer)
                .await
                .expect("bind Merge abandonment Store")
                .abandon_merge_candidate(write_id.clone())
                .await
                .expect("resume excluded peer cleanup"),
            MergeCandidateAbandonment::Abandoned,
        );
        retried
    } else {
        reopened
    };
    let retried_store = Store::load(
        StoreDatabase::new(&retried),
        store.storage(),
        store_dir.clone(),
        signer.clone(),
    )
    .await
    .expect("bind retried excluded peer Store");
    assert_eq!(
        retried_store
            .latest_local_store_position()
            .await
            .expect("reload excluded peer position"),
        local_position,
    );
    match head_publication {
        ExcludedCandidateHeadPublication::Absent => {
            assert!(matches!(
                store
                    .storage()
                    .read_protocol_object(
                        &candidate_head_context,
                        &candidate_head,
                        &candidate_head_prefix,
                    )
                    .await,
                Err(crate::protocol::objects::StorageError::NotFound(_))
            ));
            assert!(crate::database::StoreDatabase::new(&retried)
                .protocol_inert_object(candidate_head.clone())
                .await
                .expect("read absent candidate head state")
                .is_none());
        }
        ExcludedCandidateHeadPublication::ExactLate
        | ExcludedCandidateHeadPublication::AfterAbsentProofExactLate
        | ExcludedCandidateHeadPublication::AfterHeadReadBack => {
            assert_eq!(
                store
                    .storage()
                    .read_protocol_object(
                        &candidate_head_context,
                        &candidate_head,
                        &candidate_head_prefix,
                    )
                    .await
                    .expect("reload retained exact late head"),
                candidate.head.value.to_bytes(),
            );
            let inert = crate::database::StoreDatabase::new(&retried)
                .protocol_inert_object(candidate_head.clone())
                .await
                .expect("read exact late candidate head state")
                .expect("exact late candidate head is protocol-inert");
            assert!(matches!(
                inert
                    .candidate_nonactivation_proof(&candidate_ref)
                    .expect("read exact late candidate proof"),
                Some(
                    crate::protocol::remote_object::CandidateNonactivationProof::AuthorExclusion { .. }
                )
            ));
            let mut mismatched = inert.clone();
            let mut mismatched_head: crate::protocol::store_commit::StoreDeviceHead =
                serde_json::from_slice(&mismatched.canonical_semantic_bytes)
                    .expect("parse inert candidate head");
            mismatched_head.body_mut().commit.object = candidate_head.clone();
            let mismatched_bytes = mismatched_head.to_bytes();
            let head_context = ProtocolObjectContext::signed_plaintext(
                store.root.store_root_hash,
                ProtocolObjectDomain::StoreHead,
            );
            let head_prefix = crate::protocol::store_commit::head_slot_prefix(
                &target.device_id.to_string(),
                candidate_ref.coord.sequence(),
            );
            let mismatched_prepared = store
                .storage()
                .prepare_protocol_object(
                    &head_context,
                    candidate_head.slot().clone(),
                    &head_prefix,
                    mismatched_bytes.clone(),
                )
                .expect("prepare mismatched inert head");
            mismatched.canonical_semantic_bytes = mismatched_bytes.clone();
            mismatched.identity.semantic_hash = ObjectHash::digest(&mismatched_bytes);
            mismatched.identity.object = mismatched_prepared.reference().clone();
            let crate::protocol::remote_object::RetainedAuthorityObjectDomain::DeviceHead {
                reference,
            } = &mut mismatched.identity.domain
            else {
                panic!("protocol-inert candidate object is not a Store head")
            };
            reference.head_hash = mismatched_head.head_hash();
            reference.object = mismatched_prepared.reference().clone();
            mismatched
                .validate()
                .expect("mismatched inert head remains internally valid");
            assert!(!mismatched
                .is_terminal_head_for(&candidate_ref, mismatched_prepared.reference(),)
                .expect("check candidate binding on mismatched inert head"));
        }
        ExcludedCandidateHeadPublication::AfterCommitUpload => {
            assert!(matches!(
                store
                    .storage()
                    .read_protocol_object(
                        &candidate_head_context,
                        &candidate_head,
                        &candidate_head_prefix,
                    )
                    .await,
                Err(crate::protocol::objects::StorageError::NotFound(_))
            ));
        }
        ExcludedCandidateHeadPublication::AfterAbsentProofThirdWinner => {
            assert!(crate::database::StoreDatabase::new(&retried)
                .protocol_inert_object(candidate_head.clone())
                .await
                .expect("read candidate head state after third winner")
                .is_none());
        }
    }
    assert!(matches!(
        store
            .storage()
            .read_protocol_object(
                &candidate_commit_context,
                &candidate_ref.object,
                &candidate_commit_prefix,
            )
            .await,
        Err(crate::protocol::objects::StorageError::NotFound(_))
    ));
    let store_package = candidate
        .commit
        .value
        .store_package()
        .expect("excluded candidate carries its Store package");
    assert_eq!(candidate_graph_objects, vec![store_package.object.clone()]);
    let retried_store = store
        .bind_device(&retried, &signer)
        .await
        .expect("bind retried exclusion Store");
    assert!(matches!(
        retried_store
            .load_store_package_for_test(candidate.commit.value.reference())
            .await,
        Err(StoreError::Object(
            crate::protocol::objects::StoreObjectError::Storage(
                crate::protocol::objects::StorageError::NotFound(_)
            )
        ))
    ));
    assert!(matches!(
        crate::database::StoreDatabase::new(&retried)
            .merge_abandonment_state(&write_id)
            .await
            .expect("reload excluded peer abandonment state"),
        crate::database::MergeAbandonmentState::None
    ));
    match crate::database::StoreDatabase::new(&retried)
        .write_status(&write_id)
        .await
        .expect("reload excluded peer write status")
    {
        crate::WriteStatus::Blocked(_) => {
            assert_eq!(
                crate::database::StoreDatabase::new(&retried)
                    .discard_blocked_write(&write_id)
                    .await
                    .expect("discard excluded peer write"),
                crate::database::BlockedWriteDiscard::Discarded(vec![write_id.clone()])
            );
        }
        crate::WriteStatus::Resolved(crate::WriteResolution::Retracted { witness }) => {
            assert_eq!(witness.original_position().commit(), &candidate_ref);
        }
        status => panic!("excluded peer write has unexpected terminal status: {status:?}"),
    }
    if matches!(
        head_publication,
        ExcludedCandidateHeadPublication::ExactLate
            | ExcludedCandidateHeadPublication::AfterAbsentProofExactLate
            | ExcludedCandidateHeadPublication::AfterHeadReadBack
    ) {
        assert!(crate::database::StoreDatabase::new(&retried)
            .protocol_inert_object(candidate_head)
            .await
            .expect("reload exact late candidate head state")
            .is_some());
    }
    if matches!(
        head_publication,
        ExcludedCandidateHeadPublication::ExactLate
            | ExcludedCandidateHeadPublication::AfterAbsentProofExactLate
            | ExcludedCandidateHeadPublication::AfterHeadReadBack
            | ExcludedCandidateHeadPublication::AfterAbsentProofThirdWinner
    ) {
        let (_owner_temp, owner_store_dir) = temp_store_dir();
        Box::pin(
            ExcludedPeer::new(&owner_db, store.as_ref(), &owner_store_dir)
                .pull_exclusion(ExpectedHeldCandidate::None),
        )
        .await;
    }
}

struct PublishedExclusionSnapshot<'storage> {
    _directory: tempfile::TempDir,
    restored: crate::sync::store::RestoringStore<'storage>,
}

impl<'storage> PublishedExclusionSnapshot<'storage> {
    async fn open(
        store: &'storage TestStore,
        store_dir: &'storage StoreDir,
        membership_floor: &crate::protocol::membership::MembershipFloor,
        schema_version: u32,
        identity: &UserKeypair,
        device_id: String,
    ) -> Self {
        let directory = tempfile::tempdir().expect("restored exclusion directory");
        let path = directory.path().join("restored.db");
        let bootstrap = store
            .prepare_snapshot_bootstrap(membership_floor, schema_version, &path, identity)
            .await
            .expect("verify author exclusion snapshot");
        let restored = bootstrap
            .install(
                store_dir,
                crate::sync::test_helpers::test_synced_tables(),
                crate::protocol::blob::BLOB_TOMBSTONE_GRACE,
                crate::protocol::blob::TransferLimits::one_at_a_time(),
                device_id,
                std::sync::Arc::new(crate::clock::SystemClock),
                &crate::sync::test_helpers::test_migrations(),
                None,
            )
            .await
            .expect("open author exclusion snapshot");
        Self {
            _directory: directory,
            restored,
        }
    }
}
