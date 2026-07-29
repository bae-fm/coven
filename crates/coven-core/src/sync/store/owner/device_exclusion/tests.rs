use std::path::Path;
use std::sync::Arc;

use super::*;
use crate::database::Database;
use crate::keys::UserKeypair;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::store::database::StoreDatabase;
use crate::sync::store::owner::history::abandonment::MergeCandidateAbandonment;
use crate::sync::store_commit::{
    StoreAckExclusionState, StoreBatchCommit, StoreCommitCoord, StoreDeviceExclusionRef,
    StoreDeviceHead, StoreDeviceRegistrationRef,
};
use crate::sync::test_helpers::{
    host_exec, install_active_device_fixture, open_test_db, pull_into_result, store_database,
    temp_store_dir, TestDevice, TestStore,
};
use crate::{StoreDir, WriteId};

fn open(path: &Path, device_id: &str) -> Database {
    Database::open(
        path,
        crate::sync::test_helpers::test_synced_tables(),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        device_id.to_string(),
        &crate::sync::test_helpers::test_migrations(),
    )
    .expect("open exclusion test database")
    .0
}

fn storage(home: &InMemoryCloudHome, signer: &UserKeypair) -> Arc<CloudSyncStorage> {
    Arc::new(
        CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "device-exclusion-store",
            signer.clone(),
        )
        .expect("construct exclusion test storage"),
    )
}

async fn prepare_pending_store_write(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    signer: &UserKeypair,
    store_dir: &StoreDir,
) -> Result<bool, StoreError> {
    let store = Store::load(store_database(db), storage.clone(), signer.clone()).await?;
    store
        .authorize_writer()
        .await
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
        .prepare_pending_store_write(store_dir)
        .await
}

async fn drain_store_writes(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    signer: &UserKeypair,
) -> Result<u64, StoreError> {
    let store = Store::load(store_database(db), storage.clone(), signer.clone()).await?;
    store
        .authorize_writer()
        .await
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
        .drain_store_writes()
        .await
}

async fn abandon_merge_candidate(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    signer: &UserKeypair,
    write_id: WriteId,
) -> Result<MergeCandidateAbandonment, StoreError> {
    let store = Store::load(store_database(db), storage.clone(), signer.clone()).await?;
    store.abandon_merge_candidate(write_id).await
}

async fn prepare_merge_candidate_abandonment(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    signer: &UserKeypair,
    write_id: WriteId,
) -> Result<bool, StoreError> {
    let store = Store::load(store_database(db), storage.clone(), signer.clone()).await?;
    store
        .authorize_writer()
        .await
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
        .prepare_merge_candidate_abandonment(write_id)
        .await
}

async fn get_device_exclusion_operations(
    db: &Database,
) -> Result<Vec<StoreDeviceExclusionOperationInfo>, StoreDeviceExclusionError> {
    super::get_device_exclusion_operations(&store_database(db)).await
}

#[tokio::test]
async fn uploaded_proposal_resumes_after_restart_without_freezing_the_target() {
    let directory = tempfile::tempdir().expect("exclusion test directory");
    let path = directory.path().join("store.sqlite");
    let signer = UserKeypair::generate();
    let home = InMemoryCloudHome::new();
    let storage = storage(&home, &signer);
    let db = open(&path, "exclusion-host");
    Box::pin(create_exclusion_test_store(&db, &storage, &signer)).await;
    let reference = Box::pin(stage_uploaded_proposal(&db, &storage, &signer)).await;
    drop(db);

    let (reopened, base_sequence) = Box::pin(resume_proposal_and_publish_freeze_ack(
        &path,
        storage.clone(),
        signer.clone(),
        &reference,
    ))
    .await;
    Box::pin(race_cancellation_with_ack(
        reopened,
        storage,
        signer,
        reference,
        base_sequence,
    ))
    .await;
}

#[tokio::test]
async fn remaining_device_freezes_and_acknowledges_before_owner_exclusion() {
    Box::pin(run_remaining_device_exclusion()).await;
}

async fn run_remaining_device_exclusion() {
    let signer = UserKeypair::generate();
    let owner_db = open_test_db();
    let store = Arc::new(
        Box::pin(TestStore::create(
            &owner_db,
            "device-exclusion-two-device-store",
            signer.clone(),
        ))
        .await
        .expect("create two-device exclusion Store"),
    );
    let owner_device = Box::pin(store.open_into(&owner_db))
        .await
        .expect("open two-device exclusion Store");
    let peer_db = open_test_db();
    Box::pin(install_active_device_fixture(
        &store,
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
        .map(|(reference, _)| reference)
        .find(|reference| reference.device_id.to_string() != local_device_id)
        .expect("peer Store registration");
    finalize_peer_exclusion_detached(owner_device, &target).await;
}

#[tokio::test]
async fn snapshot_preserves_author_exclusion_activation_evidence() {
    run_snapshot_preserves_author_exclusion_activation_evidence().await;
}

async fn run_snapshot_preserves_author_exclusion_activation_evidence() {
    let signer = UserKeypair::generate();
    let owner_db = open_test_db();
    let store = Arc::new(
        Box::pin(TestStore::create(
            &owner_db,
            "snapshot-author-exclusion-store",
            signer.clone(),
        ))
        .await
        .expect("create snapshot exclusion Store"),
    );
    let owner_device = Box::pin(store.open_into(&owner_db))
        .await
        .expect("open snapshot exclusion Store");
    let peer_db = open_test_db();
    Box::pin(install_active_device_fixture(
        &store,
        &owner_db,
        &peer_db,
        &signer,
        "2026-07-18T00:00:00Z",
    ))
    .await
    .expect("activate snapshot exclusion peer");
    let (_candidate_temp, _candidate_store_dir, candidate_write_id) = Box::pin(
        prepare_transfer_candidate(&peer_db, &store, &signer, "snapshot-excluded-candidate"),
    )
    .await;
    let owner_device_id = owner_device.device_id.clone();
    let target = store_database(&owner_db)
        .activated_store_device_registration_records()
        .await
        .expect("list snapshot exclusion registrations")
        .into_iter()
        .map(|(reference, _)| reference)
        .find(|reference| reference.device_id.to_string() != owner_device_id)
        .expect("snapshot exclusion peer registration");
    let exclusion = finalize_peer_exclusion(&owner_device, &target).await;
    let restore = owner_device
        .restore_membership()
        .await
        .expect("retain post-exclusion snapshot membership authority");
    let live_evidence = owner_db
        .call(|connection| {
            connection
                .query_row(
                    "SELECT exclusion_ref, accepted_cut, activation_commit, activation_head
                         FROM store_author_exclusion_activations",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
                .map_err(crate::database::DbError::from)
        })
        .await
        .expect("read live author exclusion evidence");

    let directory = tempfile::tempdir().expect("snapshot exclusion image directory");
    let snapshot_dir = directory.path().to_path_buf();
    let synced_tables = owner_db.synced_tables().to_vec();
    let snapshot_root = store.root.clone();
    let image = owner_db
        .call(move |connection| {
            crate::sync::store::snapshot::create_snapshot(
                connection,
                &snapshot_root,
                &snapshot_dir,
                &synced_tables,
                None,
            )
            .map_err(|error| crate::database::DbError::Message(error.to_string()))
        })
        .await
        .expect("create author exclusion snapshot");
    let snapshot_coverage = crate::sync::store_commit::CommitFrontier::from_refs(
        store_database(&owner_db)
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
    let image_path = directory.path().join("inspected.db");
    std::fs::write(&image_path, &image).expect("write author exclusion snapshot image");
    let image =
        rusqlite::Connection::open(&image_path).expect("open author exclusion snapshot image");
    let stored: (String, String, String, String) = image
        .query_row(
            "SELECT exclusion_ref, accepted_cut, activation_commit, activation_head
                 FROM store_author_exclusion_activations",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("snapshot carries author exclusion evidence");
    assert_eq!(stored, live_evidence);
    assert_eq!(
        serde_json::from_str::<StoreDeviceExclusionRef>(&stored.0)
            .expect("parse snapshotted exclusion reference"),
        exclusion,
    );
    drop(image);

    for tamper in [
        AuthorExclusionLocatorTamper::Missing,
        AuthorExclusionLocatorTamper::ExclusionReference,
        AuthorExclusionLocatorTamper::AcceptedCut,
        AuthorExclusionLocatorTamper::ActivationCommit,
        AuthorExclusionLocatorTamper::ActivationHead,
    ] {
        let (_restored_directory, restored) = Box::pin(open_published_exclusion_snapshot(
            &store,
            "snapshot-author-exclusion-store",
            &restore.membership_floor,
            owner_db.schema_version(),
            target.device_id.to_string(),
        ))
        .await;
        Box::pin(transfer_prepared_write(
            &peer_db,
            &restored,
            &candidate_write_id,
        ))
        .await;
        let transferred_candidate = crate::sync::store::database::StoreDatabase::new(&restored)
            .blocked_merge_candidate(candidate_write_id.clone())
            .await
            .expect("load candidate before tampering with snapshot evidence")
            .expect("transferred candidate exists before snapshot evidence tamper");
        Box::pin(tamper_author_exclusion_locator(
            &restored,
            &exclusion,
            &transferred_candidate.head.value.commit,
            tamper,
        ))
        .await;
        Box::pin(abandon_merge_candidate(
            &restored,
            &store.storage,
            &signer,
            candidate_write_id.clone(),
        ))
        .await
        .expect_err("tampered snapshot exclusion evidence must fail loud");
        assert!(crate::sync::store::database::StoreDatabase::new(&restored)
            .blocked_merge_candidate(candidate_write_id.clone())
            .await
            .expect("reload candidate after tampered snapshot evidence")
            .is_some());
        assert!(!crate::sync::store::database::StoreDatabase::new(&restored)
            .merge_candidate_cleanup_pending(&candidate_write_id)
            .await
            .expect("tampered snapshot evidence cannot start cleanup"));
    }

    let (_restored_directory, restored) = Box::pin(open_published_exclusion_snapshot(
        &store,
        "snapshot-author-exclusion-store",
        &restore.membership_floor,
        owner_db.schema_version(),
        target.device_id.to_string(),
    ))
    .await;
    Box::pin(transfer_prepared_write(
        &peer_db,
        &restored,
        &candidate_write_id,
    ))
    .await;
    let transferred_candidate = crate::sync::store::database::StoreDatabase::new(&restored)
        .blocked_merge_candidate(candidate_write_id.clone())
        .await
        .expect("load restored exclusion candidate")
        .expect("restored exclusion candidate exists");
    crate::sync::store::database::StoreDatabase::new(&restored)
        .author_exclusion_activation_for_candidate(
            store.root.clone(),
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
        Box::pin(abandon_merge_candidate(
            &restored,
            &store.storage,
            &signer,
            candidate_write_id.clone(),
        ))
        .await
        .expect("consume snapshotted exclusion evidence"),
        MergeCandidateAbandonment::Abandoned,
    );
    assert!(!crate::sync::store::database::StoreDatabase::new(&restored)
        .merge_candidate_cleanup_pending(&candidate_write_id)
        .await
        .expect("restored candidate cleanup completes"));
}

#[tokio::test]
async fn device_join_bootstrap_records_exclusion_replayed_after_snapshot() {
    Box::pin(run_device_join_bootstrap_records_exclusion_replayed_after_snapshot()).await;
}

async fn run_device_join_bootstrap_records_exclusion_replayed_after_snapshot() {
    let signer = UserKeypair::generate();
    let owner_db = open_test_db();
    let store = Arc::new(
        Box::pin(TestStore::create(
            &owner_db,
            "bootstrap-author-exclusion-store",
            signer.clone(),
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
    install_active_device_fixture(&store, &owner_db, &peer_db, &signer, "2026-07-18T00:00:00Z")
        .await
        .expect("activate bootstrap exclusion peer");
    let (_candidate_temp, _candidate_store_dir, candidate_write_id) = Box::pin(
        prepare_transfer_candidate(&peer_db, &store, &signer, "bootstrap-excluded-candidate"),
    )
    .await;
    let owner_device_id = owner_device.device_id.clone();
    let target = store_database(&owner_db)
        .activated_store_device_registration_records()
        .await
        .expect("list bootstrap exclusion registrations")
        .into_iter()
        .map(|(reference, _)| reference)
        .find(|reference| reference.device_id.to_string() != owner_device_id)
        .expect("bootstrap exclusion peer registration");
    let proposal = Box::pin(prepare_peer_exclusion(&owner_device, &target)).await;

    let image_dir = tempfile::tempdir().expect("bootstrap snapshot image directory");
    let snapshot_dir = image_dir.path().to_path_buf();
    let synced_tables = owner_db.synced_tables().to_vec();
    let snapshot_root = store.root.clone();
    let image = owner_db
        .call(move |connection| {
            crate::sync::store::snapshot::create_snapshot(
                connection,
                &snapshot_root,
                &snapshot_dir,
                &synced_tables,
                None,
            )
            .map_err(|error| crate::database::DbError::Message(error.to_string()))
        })
        .await
        .expect("create pre-exclusion snapshot");
    let snapshot_coverage = crate::sync::store_commit::CommitFrontier::from_refs(
        store_database(&owner_db)
            .materialized_frontier()
            .await
            .expect("read pre-exclusion frontier"),
    )
    .expect("shape pre-exclusion frontier");
    owner_device
        .publish_snapshot(image, snapshot_coverage.clone())
        .await
        .expect("publish pre-exclusion snapshot");
    let published_snapshot = crate::sync::store::database::StoreDatabase::new(&owner_db)
        .latest_local_store_snapshot()
        .await
        .expect("read published pre-exclusion snapshot")
        .expect("published pre-exclusion snapshot exists");
    let (_peer_pull_temp, peer_pull_dir) = crate::sync::test_helpers::temp_store_dir();
    let peer_pull = pull_into_result(&peer_db, &store, &peer_pull_dir)
        .await
        .expect("materialize pre-exclusion snapshot coverage on peer")
        .1;
    assert!(peer_pull.held_positions.is_empty());
    let peer_device = TestDevice::load(&peer_db, store.storage.clone(), signer.clone())
        .await
        .expect("bind peer acknowledgement Store");
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

    let exclusion = activate_peer_exclusion(&owner_device, &proposal).await;
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
    let bootstrap_root = store.root.clone();
    let bootstrap_floor = restore.membership_floor.clone();
    let bootstrap_identity = crate::keys::UserKeypair::generate();
    let bootstrap = Box::pin(crate::sync::store::snapshot::bootstrap_from_snapshot(
        &store.storage,
        "bootstrap-author-exclusion-store",
        bootstrap_root,
        &bootstrap_floor,
        1,
        &database_path,
        &bootstrap_identity,
    ))
    .await
    .expect("verify pre-exclusion snapshot");
    let joining_db = bootstrap
        .open_database(
            "bootstrap-author-exclusion-store",
            &database_path,
            crate::sync::test_helpers::test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            "post-snapshot-joining-device".to_string(),
            &crate::sync::test_helpers::test_migrations(),
            None,
        )
        .await
        .expect("open pre-exclusion snapshot");
    StoreDatabase::new(joining_db.database())
        .install_device_join_bootstrap(store.root.clone(), plan)
        .await
        .expect("replay exclusion after snapshot");
    let joining_db = joining_db.into_database();
    Box::pin(transfer_prepared_write(
        &peer_db,
        &joining_db,
        &candidate_write_id,
    ))
    .await;

    let exclusion_json = serde_json::to_string(&exclusion).expect("serialize exclusion ref");
    let stored = joining_db
        .call(move |connection| {
            connection
                .query_row(
                    "SELECT accepted_cut, activation_head
                         FROM store_author_exclusion_activations
                         WHERE exclusion_ref = ?1",
                    [&exclusion_json],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .map_err(crate::database::DbError::from)
        })
        .await
        .expect("replayed exclusion has exact activation evidence");
    assert!(!stored.0.is_empty());
    assert!(!stored.1.is_empty());
    assert_eq!(
        Box::pin(abandon_merge_candidate(
            &joining_db,
            &store.storage,
            &signer,
            candidate_write_id.clone(),
        ))
        .await
        .expect("consume replayed exclusion evidence"),
        MergeCandidateAbandonment::Abandoned,
    );
    assert!(
        !crate::sync::store::database::StoreDatabase::new(&joining_db)
            .merge_candidate_cleanup_pending(&candidate_write_id)
            .await
            .expect("replayed exclusion candidate cleanup completes")
    );
}

async fn finalize_peer_exclusion(
    owner_device: &TestDevice,
    target: &StoreDeviceRegistrationRef,
) -> StoreDeviceExclusionRef {
    let proposal = prepare_peer_exclusion(owner_device, target).await;
    activate_peer_exclusion(owner_device, &proposal).await
}

async fn prepare_peer_exclusion(
    owner_device: &TestDevice,
    target: &StoreDeviceRegistrationRef,
) -> StoreDeviceExclusionProposalRef {
    let proposal = match owner_device
        .propose_device_exclusion(target)
        .await
        .expect("propose peer exclusion")
    {
        StoreDeviceExclusionResult::ProposalActivated { proposal, .. } => proposal,
        result => panic!("unexpected exclusion proposal result: {result:?}"),
    };
    let freezes = store_database(&owner_device.db)
        .store_device_exclusion_freezes()
        .await
        .expect("read owner exclusion freeze");
    assert_eq!(freezes.len(), 1);
    assert_eq!(freezes[0].proposal, proposal);
    assert_eq!(&freezes[0].proposal.target, target);

    let frontier = crate::sync::store_commit::CommitFrontier::from_refs(
        store_database(&owner_device.db)
            .materialized_frontier()
            .await
            .expect("read owner exclusion frontier"),
    )
    .expect("shape owner exclusion frontier");
    let acknowledgement = owner_device
        .stage_acknowledgement(frontier, "2026-07-18T00:01:00Z".to_string())
        .await
        .expect("stage owner exclusion acknowledgement");
    let StoreAckExclusionState { proposal_freezes } = acknowledgement.exclusions;
    assert_eq!(proposal_freezes, freezes);
    assert_eq!(
        owner_device
            .drain_acknowledgements()
            .await
            .expect("publish owner exclusion acknowledgement"),
        1
    );
    proposal
}

async fn activate_peer_exclusion(
    owner_device: &TestDevice,
    proposal: &StoreDeviceExclusionProposalRef,
) -> StoreDeviceExclusionRef {
    let result = owner_device
        .finalize_device_exclusion(proposal)
        .await
        .expect("finalize peer exclusion");
    let StoreDeviceExclusionResult::OutcomeActivated {
        outcome: StoreDeviceExclusionOutcomeRef::Excluded(exclusion),
        ..
    } = result
    else {
        panic!("unexpected exclusion result: {result:?}")
    };
    assert!(store_database(&owner_device.db)
        .store_device_exclusion_freezes()
        .await
        .expect("read released owner exclusion freeze")
        .is_empty());
    exclusion
}

async fn finalize_peer_exclusion_detached(
    owner_device: TestDevice,
    target: &StoreDeviceRegistrationRef,
) -> StoreDeviceExclusionRef {
    let target = target.clone();
    tokio::spawn(async move { Box::pin(finalize_peer_exclusion(&owner_device, &target)).await })
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

fn indexed_shared_blob(
    label: &str,
    candidate: &StoreBatchCommitRef,
    uploader: &StoreDeviceRegistrationRef,
    activated: std::collections::BTreeSet<crate::sync::remote_object::SharedObjectOwner>,
) -> crate::sync::remote_object::RemoteObjectRecord {
    let stored_bytes = format!("stored excluded-author blob {label}").into_bytes();
    let locator = crate::blob::locator::BlobLocator::opaque(
        "excluded-author-test",
        label,
        uploader.clone(),
        crate::blob::locator::RemoteAudience::Store,
        crate::BlobScope::Master,
        crate::KeyFingerprint::from_bytes([17; 32]),
        1,
        ObjectHash::digest(format!("plaintext excluded-author blob {label}").as_bytes()),
    )
    .expect("construct indexed shared blob locator");
    let object = crate::sync::storage::ExactObjectRef::new(
        crate::storage::cloud::ObjectSlot::logical(locator.semantic_key())
            .expect("construct indexed shared blob slot"),
        u64::try_from(stored_bytes.len()).expect("indexed shared blob size fits u64"),
        ObjectHash::digest(&stored_bytes),
    );
    let locator_bytes = locator.to_bytes();
    let record = crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(
        crate::sync::remote_object::SharedObjectRecord {
            identity: crate::sync::remote_object::SharedLiveSetObjectRef {
                domain: crate::sync::remote_object::SharedLiveSetObjectDomain::StoredBlob,
                semantic_hash: ObjectHash::digest(&locator_bytes),
                object: object.clone(),
            },
            bytes: crate::sync::remote_object::RemoteObjectBytes::blob(locator_bytes, object)
                .expect("construct indexed shared blob bytes"),
            state: crate::sync::remote_object::OwnedObjectState::UploadedVerified {
                ownership: crate::sync::remote_object::SharedObjectOwnership {
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

async fn materialize_surviving_owner_commit(
    owner_db: &Database,
    peer_db: &Database,
    store: &TestStore,
    signer: &UserKeypair,
    store_dir: &StoreDir,
) {
    host_exec(
        owner_db,
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('surviving-owner-note', 'surviving', NULL, 1, \
                     '0000000001500-0000-owner', '2026-07-18')",
    )
    .await;
    assert!(Box::pin(prepare_pending_store_write(
        owner_db,
        &store.storage,
        signer,
        store_dir,
    ))
    .await
    .expect("prepare surviving owner commit"));
    Box::pin(drain_store_writes(owner_db, &store.storage, signer))
        .await
        .expect("publish surviving owner commit");
    pull_into_result(peer_db, store, store_dir)
        .await
        .expect("materialize surviving owner commit on excluded peer");
}

#[derive(Clone, Copy)]
enum TerminalMergeTransactionFailure {
    Injected(crate::database::MergeMaterializationFailurePoint),
    DeleteDeviceStateDuringRetraction,
}

#[allow(clippy::too_many_arguments)]
async fn assert_terminal_merge_transaction_rollback(
    peer_db: &Database,
    store: &TestStore,
    store_dir: &StoreDir,
    write_id: &crate::WriteId,
    original: &crate::PublishedPosition,
    activation_commit: &StoreBatchCommitRef,
    failure: TerminalMergeTransactionFailure,
) {
    match failure {
        TerminalMergeTransactionFailure::Injected(point) => {
            peer_db.fail_next_merge_materialization_at(point);
        }
        TerminalMergeTransactionFailure::DeleteDeviceStateDuringRetraction => {
            peer_db
                .call(|connection| {
                    connection
                        .execute_batch(
                            "CREATE TRIGGER delete_retracted_device_state_early
                                 AFTER DELETE ON materialized_commits
                                 BEGIN
                                   DELETE FROM store_device_state_snapshots
                                   WHERE commit_ref = OLD.commit_ref;
                                 END;",
                        )
                        .map_err(crate::database::DbError::from)?;
                    Ok(())
                })
                .await
                .expect("install early device-state deletion trigger");
        }
    }
    let error = pull_into_result(peer_db, store, store_dir)
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
    assert!(store_database(peer_db)
        .exact_materialized_ref(&stream_id.to_string(), *sequence)
        .await
        .expect("reload rolled-back activation coordinate")
        .is_none());
    store_database(peer_db)
        .retained_merge_materialization(store.root.clone(), original.commit().clone())
        .await
        .expect("rolled-back retraction retains the original materialization");
    assert!(matches!(
        peer_db
            .write_status(write_id)
            .await
            .expect("reload rolled-back write status"),
        crate::WriteStatus::Published(position) if position.as_ref() == original
    ));
    assert_eq!(
        peer_db
            .call(|connection| {
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
    assert!(!crate::sync::store::database::StoreDatabase::new(peer_db)
        .merge_candidate_cleanup_pending(write_id)
        .await
        .expect("rolled-back transaction created no cleanup"));
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
    let store = Arc::new(
        Box::pin(TestStore::create(
            &owner_db,
            "excluded-author-candidate-store",
            signer.clone(),
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
    Box::pin(install_active_device_fixture(
        &store,
        &owner_db,
        &peer_db,
        &signer,
        "2026-07-18T01:00:00Z",
    ))
    .await
    .expect("activate excluded peer");
    let (_store_temp, store_dir) = temp_store_dir();
    if materialize_before_exclusion {
        Box::pin(materialize_surviving_owner_commit(
            &owner_db,
            &peer_db,
            store.as_ref(),
            &signer,
            &store_dir,
        ))
        .await;
    }
    host_exec(
        &peer_db,
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
    assert!(Box::pin(prepare_pending_store_write(
        &peer_db,
        &store.storage,
        &signer,
        &store_dir,
    ))
    .await
    .expect("prepare excluded peer candidate"));
    let candidate = crate::sync::store::database::StoreDatabase::new(&peer_db)
        .oldest_prepared_store_write()
        .await
        .expect("load excluded peer candidate")
        .expect("excluded peer candidate exists");
    let candidate_ref = candidate.head.value.commit.clone();
    let candidate_graph_objects =
        crate::sync::remote_object::CandidateObjectGraph::from_commit(&candidate.commit.value)
            .expect("read excluded candidate object graph")
            .exact_objects()
            .cloned()
            .collect::<Vec<_>>();
    let candidate_head = candidate.head.object.clone();
    let candidate_head_context = ProtocolObjectContext::signed_plaintext(
        store.root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let candidate_head_prefix = crate::sync::store_commit::head_slot_prefix(
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
    let candidate_commit_prefix = crate::sync::store_commit::semantic_prefix_from_exact_object(
        &candidate_ref.object,
        ".json",
    )
    .expect("derive excluded candidate commit prefix");
    let write_id = candidate.commit.value.write_id.clone();
    store
        .storage
        .create_protocol_object(&candidate.commit.prepared)
        .await
        .expect("upload excluded peer candidate commit");
    crate::sync::store::database::StoreDatabase::new(&peer_db)
        .mark_candidate_commit_uploaded(candidate_ref.clone())
        .await
        .expect("record uploaded excluded peer commit");
    let (target, target_registration) = store_database(&peer_db)
        .activated_store_device_registration_records()
        .await
        .expect("load excluded peer registration")
        .into_iter()
        .find(|(reference, _)| reference.device_id.to_string() == peer_device_id)
        .expect("exact excluded peer registration");
    let prepared_abandonment = Box::pin(maybe_prepare_merge_abandonment(
        &peer_db,
        store.as_ref(),
        &signer,
        &write_id,
        prepare_abandonment,
    ))
    .await;
    if materialize_before_exclusion {
        Box::pin(drain_store_writes(&peer_db, &store.storage, &signer))
            .await
            .expect("publish excluded peer candidate before exclusion");
        let original = match peer_db
            .write_status(&write_id)
            .await
            .expect("load accepted candidate status")
        {
            crate::WriteStatus::Published(position) => *position,
            status => panic!("candidate was not accepted before exclusion: {status:?}"),
        };
        assert_eq!(original.commit(), &candidate_ref);
        host_exec(
            &peer_db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('excluded-peer-local-note', 'local', NULL, 0, \
                         '0000000002001-0000-excluded-peer', '2026-07-18')",
        )
        .await;
        let (local_status, local_partitions, local_changeset_bytes) = peer_db
            .call(|connection| {
                connection
                    .query_row(
                        "SELECT status,
                                    (SELECT COUNT(*) FROM store_write_partitions p
                                     WHERE p.write_id = w.write_id),
                                    (SELECT COALESCE(SUM(length(changeset)), 0)
                                     FROM store_write_partitions p
                                     WHERE p.write_id = w.write_id)
                             FROM store_writes w ORDER BY ordinal DESC LIMIT 1",
                        [],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, i64>(2)?,
                            ))
                        },
                    )
                    .map_err(crate::database::DbError::from)
            })
            .await
            .expect("load local-only replay input");
        assert_eq!(local_status, "\"local_only\"");
        assert_eq!(local_partitions, 1);
        assert!(local_changeset_bytes > 0);
        finalize_peer_exclusion_detached(owner_device.clone(), &target).await;
        let activation_commit = crate::sync::store::database::StoreDatabase::new(&owner_db)
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
            Box::pin(assert_terminal_merge_transaction_rollback(
                &peer_db,
                store.as_ref(),
                &store_dir,
                &write_id,
                &original,
                &activation_commit,
                failure,
            ))
            .await;
            if matches!(
                failure,
                TerminalMergeTransactionFailure::DeleteDeviceStateDuringRetraction
            ) {
                return;
            }
        }
        store.home.fail_exact_delete_on_call(1);
        assert!(pull_into_result(&peer_db, &store, &store_dir)
            .await
            .is_err());
        let witness = match peer_db
            .write_status(&write_id)
            .await
            .expect("load retracted candidate status")
        {
            crate::WriteStatus::Resolved(crate::WriteResolution::Retracted { witness }) => witness,
            status => panic!("accepted candidate was not retracted: {status:?}"),
        };
        assert_eq!(witness.original_position(), &original);
        let row_count = peer_db
            .call(|connection| {
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
            .call(|connection| {
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
            .call(|connection| {
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
        assert!(crate::sync::store::database::StoreDatabase::new(&peer_db)
            .merge_candidate_cleanup_pending(&write_id)
            .await
            .expect("retracted candidate requires cleanup"));
        drop(peer_db);
        let reopened = open(&path, "excluded-peer-host");
        pull_peer_exclusion(
            &reopened,
            store.as_ref(),
            &store_dir,
            ExpectedHeldCandidate::None,
        )
        .await;
        assert!(!crate::sync::store::database::StoreDatabase::new(&reopened)
            .merge_candidate_cleanup_pending(&write_id)
            .await
            .expect("retracted candidate cleanup completed"));
        assert!(matches!(
            reopened
                .write_status(&write_id)
                .await
                .expect("reload retracted candidate status"),
            crate::WriteStatus::Resolved(crate::WriteResolution::Retracted {
                witness: current,
            }) if current == witness
        ));
        let prepared_count = reopened
            .call({
                let write_id = write_id.clone();
                move |connection| {
                    connection
                        .query_row(
                            "SELECT COUNT(*) FROM store_writes
                                 WHERE write_id = ?1 AND prepared IS NOT NULL",
                            [write_id.as_str()],
                            |row| row.get::<_, i64>(0),
                        )
                        .map_err(crate::database::DbError::from)
                }
            })
            .await
            .expect("count retracted candidate preparation");
        assert_eq!(prepared_count, 0);
        return;
    }
    finalize_peer_exclusion_detached(owner_device, &target).await;
    if let Some(candidates) = prepared_abandonment {
        Box::pin(finish_prepared_exclusion_cleanup(
            &peer_db,
            store.as_ref(),
            &store_dir,
            &signer,
            write_id,
            &candidates,
            &candidate_commit_context,
            prepared_head_publication,
        ))
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
        let drain = tokio::spawn(async move {
            drain_store_writes(&drain_db, &drain_store.storage, &drain_store.signer).await
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
        pull_peer_exclusion(&peer_db, store.as_ref(), &store_dir, expected_held).await;
        if matches!(
            head_publication,
            ExcludedCandidateHeadPublication::AfterHeadReadBack
        ) {
            pull_peer_exclusion(
                &peer_db,
                store.as_ref(),
                &store_dir,
                ExpectedHeldCandidate::None,
            )
            .await;
        }
        resume.notify_one();
        drain
            .await
            .expect("join excluded-author publication")
            .expect_err("second exclusion check blocks candidate head")
    } else {
        pull_peer_exclusion(
            &peer_db,
            store.as_ref(),
            &store_dir,
            ExpectedHeldCandidate::None,
        )
        .await;
        if matches!(
            head_publication,
            ExcludedCandidateHeadPublication::ExactLate
        ) {
            store
                .storage
                .create_protocol_object(&candidate.head.prepared)
                .await
                .expect("publish exact late excluded-author head");
            assert_eq!(
                store
                    .storage
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
        drain_store_writes(&peer_db, &store.storage, &signer)
            .await
            .expect_err("excluded peer cannot activate its late candidate")
    };
    let peer_store = Store::load(
        StoreDatabase::new(&peer_db),
        store.storage.clone(),
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
    match peer_db
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
        crate::sync::store::database::StoreDatabase::new(&peer_db)
            .merge_abandonment_state(&write_id)
            .await
            .expect("load excluded peer abandonment state"),
        crate::database::MergeAbandonmentState::None
    ));
    let indexed_shared_blobs = if index_shared_blobs {
        let snapshot_owner = crate::sync::remote_object::SharedObjectOwner::Snapshot(
            crate::sync::remote_object::SnapshotObjectOwner {
                activation: target_registration
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
            .call(move |connection| {
                let tx = connection
                    .unchecked_transaction()
                    .map_err(crate::database::DbError::from)?;
                for (index, record) in records.into_iter().enumerate() {
                    let object_id = record.object_id();
                    tx.execute(
                        "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)",
                        rusqlite::params![
                            object_id.to_string(),
                            serde_json::to_string(&record).map_err(|error| {
                                crate::database::DbError::Message(error.to_string())
                            })?
                        ],
                    )
                    .map_err(crate::database::DbError::from)?;
                    tx.execute(
                        "INSERT INTO store_write_blobs
                             (write_id, audience, locator_hash, remote_object_id, spool_path)
                             VALUES (?1, 'store', ?2, ?3, NULL)",
                        rusqlite::params![
                            indexed_write_id.as_str(),
                            ObjectHash::digest(format!("indexed shared blob {index}").as_bytes())
                                .to_string(),
                            object_id.to_string(),
                        ],
                    )
                    .map_err(crate::database::DbError::from)?;
                }
                tx.commit().map_err(crate::database::DbError::from)
            })
            .await
            .expect("index shared blobs under excluded candidate");
        identities
    } else {
        Vec::new()
    };
    drop(peer_db);

    let reopened = open(&path, "excluded-peer-host");
    let cleanup_pending = crate::sync::store::database::StoreDatabase::new(&reopened)
        .merge_candidate_cleanup_pending(&write_id)
        .await
        .expect("load excluded peer cleanup state");
    if cleanup_pending {
        store.home.fail_exact_delete_on_call(1);
        assert!(Box::pin(abandon_merge_candidate(
            &reopened,
            &store.storage,
            &signer,
            write_id.clone(),
        ))
        .await
        .is_err());
        assert!(crate::sync::store::database::StoreDatabase::new(&reopened)
            .merge_candidate_cleanup_pending(&write_id)
            .await
            .expect("excluded peer cleanup remains pending"));
    } else {
        assert!(matches!(
            Box::pin(abandon_merge_candidate(
                &reopened,
                &store.storage,
                &signer,
                write_id.clone(),
            ))
            .await
            .expect("observe completed excluded peer cleanup"),
            MergeCandidateAbandonment::NotRequired | MergeCandidateAbandonment::Abandoned
        ));
    }
    if cleanup_pending && !indexed_shared_blobs.is_empty() {
        let cleanup_targets = crate::sync::store::database::StoreDatabase::new(&reopened)
            .merge_candidate_cleanup_targets(write_id.clone())
            .await
            .expect("load excluded candidate cleanup targets");
        for (_, object) in &indexed_shared_blobs {
            assert!(cleanup_targets
                .iter()
                .all(|target| &target.object != object));
        }
        let indexed = indexed_shared_blobs.clone();
        reopened
            .call(move |connection| {
                for (index, (object_id, _)) in indexed.into_iter().enumerate() {
                    let state: String = connection
                        .query_row(
                            "SELECT state FROM remote_objects WHERE object_id = ?1",
                            [object_id.to_string()],
                            |row| row.get(0),
                        )
                        .map_err(crate::database::DbError::from)?;
                    let record: crate::sync::remote_object::RemoteObjectRecord =
                        serde_json::from_str(&state).map_err(|error| {
                            crate::database::DbError::Message(error.to_string())
                        })?;
                    let crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(record) =
                        record
                    else {
                        return Err(crate::database::DbError::Message(
                            "indexed blob changed remote-object domain".to_string(),
                        ));
                    };
                    match (index, record.state) {
                        (
                            0,
                            crate::sync::remote_object::OwnedObjectState::RetirementPending {
                                ..
                            },
                        ) => {}
                        (
                            1,
                            crate::sync::remote_object::OwnedObjectState::UploadedVerified {
                                ownership,
                            },
                        ) if ownership.pending.is_empty() && ownership.activated.len() == 1 => {}
                        _ => {
                            return Err(crate::database::DbError::Message(
                                "excluded candidate retained indexed shared blob ownership"
                                    .to_string(),
                            ));
                        }
                    }
                }
                Ok(())
            })
            .await
            .expect("verify indexed shared blob ownership transition");
    }
    publish_after_absent_proof_detached(
        head_publication,
        reopened.clone(),
        store.clone(),
        signer.clone(),
        write_id.clone(),
    )
    .await;
    if !cleanup_pending
        && matches!(
            head_publication,
            ExcludedCandidateHeadPublication::AfterAbsentProofExactLate
                | ExcludedCandidateHeadPublication::AfterAbsentProofThirdWinner
        )
    {
        assert_eq!(
            Box::pin(abandon_merge_candidate(
                &reopened,
                &store.storage,
                &signer,
                write_id.clone(),
            ))
            .await
            .expect("reconcile candidate head published after the absence proof"),
            MergeCandidateAbandonment::Abandoned,
        );
    }
    if cleanup_pending && sabotage_activation_head {
        let candidate_object_id =
            crate::sync::remote_object::remote_object_id(&candidate_ref.object);
        reopened
            .call(move |connection| {
                let state: String = connection
                    .query_row(
                        "SELECT state FROM remote_objects WHERE object_id = ?1",
                        [candidate_object_id.to_string()],
                        |row| row.get(0),
                    )
                    .map_err(crate::database::DbError::from)?;
                let mut remote: crate::sync::remote_object::RemoteObjectRecord =
                    serde_json::from_str(&state)
                        .map_err(|error| crate::database::DbError::Message(error.to_string()))?;
                let crate::sync::remote_object::RemoteObjectRecord::CandidateCommit(record) =
                    &mut remote
                else {
                    return Err(crate::database::DbError::Message(
                        "cleanup candidate is not a commit".to_string(),
                    ));
                };
                let crate::sync::remote_object::CandidateCommitState::CleanupPending {
                    proof:
                        crate::sync::remote_object::CandidateNonactivationProof::AuthorExclusion {
                            activation_head,
                            ..
                        },
                } = &mut record.state
                else {
                    return Err(crate::database::DbError::Message(
                        "cleanup candidate has no author-exclusion proof".to_string(),
                    ));
                };
                activation_head.head_hash =
                    ObjectHash::digest(b"different durable author-exclusion activation head");
                connection
                    .execute(
                        "UPDATE remote_objects SET state = ?2 WHERE object_id = ?1",
                        (
                            candidate_object_id.to_string(),
                            serde_json::to_string(&remote).map_err(|error| {
                                crate::database::DbError::Message(error.to_string())
                            })?,
                        ),
                    )
                    .map_err(crate::database::DbError::from)?;
                Ok(())
            })
            .await
            .expect("sabotage durable activation head");
        assert!(crate::sync::store::database::StoreDatabase::new(&reopened)
            .merge_candidate_cleanup_pending(&write_id)
            .await
            .is_err());
        assert!(Box::pin(abandon_merge_candidate(
            &reopened,
            &store.storage,
            &signer,
            write_id,
        ))
        .await
        .is_err());
        return;
    }
    let retried = if cleanup_pending {
        drop(reopened);
        let retried = open(&path, "excluded-peer-host");
        assert_eq!(
            Box::pin(abandon_merge_candidate(
                &retried,
                &store.storage,
                &signer,
                write_id.clone(),
            ))
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
        store.storage.clone(),
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
                    .storage
                    .read_protocol_object(
                        &candidate_head_context,
                        &candidate_head,
                        &candidate_head_prefix,
                    )
                    .await,
                Err(crate::sync::storage::StorageError::NotFound(_))
            ));
            assert!(crate::sync::store::database::StoreDatabase::new(&retried)
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
                    .storage
                    .read_protocol_object(
                        &candidate_head_context,
                        &candidate_head,
                        &candidate_head_prefix,
                    )
                    .await
                    .expect("reload retained exact late head"),
                candidate.head.value.to_bytes(),
            );
            let inert = crate::sync::store::database::StoreDatabase::new(&retried)
                .protocol_inert_object(candidate_head.clone())
                .await
                .expect("read exact late candidate head state")
                .expect("exact late candidate head is protocol-inert");
            assert!(matches!(
                inert
                    .candidate_nonactivation_proof(&candidate_ref)
                    .expect("read exact late candidate proof"),
                Some(
                    crate::sync::remote_object::CandidateNonactivationProof::AuthorExclusion { .. }
                )
            ));
            let mut mismatched = inert.clone();
            let mut mismatched_head: crate::sync::store_commit::StoreDeviceHead =
                serde_json::from_slice(&mismatched.canonical_semantic_bytes)
                    .expect("parse inert candidate head");
            mismatched_head.commit.object = candidate_head.clone();
            let mismatched_bytes = mismatched_head.to_bytes();
            let head_context = ProtocolObjectContext::signed_plaintext(
                store.root.store_root_hash,
                ProtocolObjectDomain::StoreHead,
            );
            let head_prefix = crate::sync::store_commit::head_slot_prefix(
                &target.device_id.to_string(),
                candidate_ref.coord.sequence(),
            );
            let mismatched_prepared = store
                .storage
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
            let crate::sync::remote_object::RetainedAuthorityObjectDomain::DeviceHead { reference } =
                &mut mismatched.identity.domain
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
                    .storage
                    .read_protocol_object(
                        &candidate_head_context,
                        &candidate_head,
                        &candidate_head_prefix,
                    )
                    .await,
                Err(crate::sync::storage::StorageError::NotFound(_))
            ));
        }
        ExcludedCandidateHeadPublication::AfterAbsentProofThirdWinner => {
            assert!(crate::sync::store::database::StoreDatabase::new(&retried)
                .protocol_inert_object(candidate_head.clone())
                .await
                .expect("read candidate head state after third winner")
                .is_none());
        }
    }
    assert!(matches!(
        store
            .storage
            .read_protocol_object(
                &candidate_commit_context,
                &candidate_ref.object,
                &candidate_commit_prefix,
            )
            .await,
        Err(crate::sync::storage::StorageError::NotFound(_))
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
            .store
            .load_store_package_for_test(candidate.commit.value.reference())
            .await,
        Err(StoreError::Object(
            crate::sync::store_objects::StoreObjectError::Storage(
                crate::sync::storage::StorageError::NotFound(_)
            )
        ))
    ));
    assert!(matches!(
        crate::sync::store::database::StoreDatabase::new(&retried)
            .merge_abandonment_state(&write_id)
            .await
            .expect("reload excluded peer abandonment state"),
        crate::database::MergeAbandonmentState::None
    ));
    match retried
        .write_status(&write_id)
        .await
        .expect("reload excluded peer write status")
    {
        crate::WriteStatus::Blocked(_) => {
            assert_eq!(
                crate::sync::store::database::StoreDatabase::new(&retried)
                    .discard_blocked_write(&write_id)
                    .await
                    .expect("discard excluded peer write"),
                crate::sync::store::BlockedWriteDiscard::Discarded(vec![write_id.clone()])
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
        assert!(crate::sync::store::database::StoreDatabase::new(&retried)
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
        Box::pin(pull_peer_exclusion(
            &owner_db,
            store.as_ref(),
            &owner_store_dir,
            ExpectedHeldCandidate::None,
        ))
        .await;
    }
}

async fn maybe_prepare_merge_abandonment(
    peer_db: &Database,
    store: &TestStore,
    signer: &UserKeypair,
    write_id: &WriteId,
    prepare: bool,
) -> Option<Box<crate::database::PreparedMergeAbandonmentCandidates>> {
    if !prepare {
        return None;
    }
    crate::sync::store::database::StoreDatabase::new(peer_db)
        .set_write_status(
            write_id,
            crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                reason: "prepare abandonment before exclusion".to_string(),
            }),
        )
        .await
        .expect("block candidate before abandonment preparation");
    assert!(Box::pin(prepare_merge_candidate_abandonment(
        peer_db,
        &store.storage,
        signer,
        write_id.clone(),
    ))
    .await
    .expect("prepare abandonment before exclusion"));
    crate::sync::store::database::StoreDatabase::new(peer_db)
        .prepared_merge_abandonment_candidates(write_id.clone())
        .await
        .expect("load prepared abandonment candidates")
        .map(Box::new)
}

async fn publish_prepared_abandonment_head(
    peer_db: &Database,
    store: &TestStore,
    signer: &UserKeypair,
    candidates: &crate::database::PreparedMergeAbandonmentCandidates,
    publication: PreparedAbandonmentHeadPublication,
) {
    match publication {
        PreparedAbandonmentHeadPublication::Absent => {}
        PreparedAbandonmentHeadPublication::Original => {
            store
                .storage
                .create_protocol_object(&candidates.candidate.head.prepared)
                .await
                .expect("publish exact original candidate head");
        }
        PreparedAbandonmentHeadPublication::Authority => {
            store
                .storage
                .create_protocol_object(&candidates.authority.commit.prepared)
                .await
                .expect("publish abandonment authority commit");
            store
                .storage
                .create_protocol_object(&candidates.authority.head.prepared)
                .await
                .expect("publish exact abandonment authority head");
        }
        PreparedAbandonmentHeadPublication::ThirdWinner => {
            Box::pin(publish_third_candidate_winner(
                peer_db,
                store,
                signer,
                &candidates.candidate,
            ))
            .await;
        }
    }
}

async fn publish_third_candidate_winner(
    peer_db: &Database,
    store: &TestStore,
    signer: &UserKeypair,
    candidate: &crate::database::BlockedMergeCandidate,
) {
    let registration = store_database(peer_db)
        .activated_store_device_registration(candidate.commit.value.author_registration.clone())
        .await
        .expect("load third-winner device registration");
    let device_signer = registration
        .device_signer(signer)
        .expect("derive third-winner device signer");
    let coord = candidate.head.value.commit.coord.clone();
    let candidate_family = candidate.commit.value.candidate_family();
    let package = crate::sync::audience_package::AudiencePackage::store(
        store.root.store_root_hash,
        candidate_family,
        candidate.commit.value.write_id.clone(),
        coord.clone(),
        peer_db.schema_version(),
        b"third winner package".to_vec(),
        Vec::new(),
    )
    .expect("construct third winner package");
    let StoreCommitCoord {
        stream_id,
        sequence,
    } = coord.clone();
    let package_bytes = package.to_bytes();
    let package_context = ProtocolObjectContext::store_encrypted(
        store.root.store_root_hash,
        ProtocolObjectDomain::StorePackage,
    );
    let package_prefix = crate::sync::store_commit::package_semantic_prefix(
        candidate_family,
        &stream_id.to_string(),
        sequence,
        ObjectHash::digest(&package_bytes),
    );
    let package_slot = store
        .storage
        .allocate_protocol_slot(&package_context, &package_prefix, ".pkg")
        .await
        .expect("allocate third winner package slot");
    let package_prepared = store
        .storage
        .prepare_protocol_object(
            &package_context,
            package_slot,
            &package_prefix,
            package_bytes.clone(),
        )
        .expect("prepare third winner package");
    let third = StoreBatchCommit::signed(
        store.root.store_root_hash,
        candidate.commit.value.write_id.clone(),
        coord.clone(),
        candidate.commit.value.author_registration.clone(),
        &registration,
        candidate.commit.value.order.clone(),
        candidate.commit.value.membership_state.clone(),
        candidate.commit.value.device_state.clone(),
        candidate
            .commit
            .value
            .operations_membership_authority()
            .expect("load third winner membership authority"),
        crate::sync::store_commit::StorePackageInput {
            candidate_family,
            schema_version: peer_db.schema_version(),
            bytes: &package_bytes,
            object: package_prepared.reference().clone(),
        },
        &device_signer,
    )
    .expect("sign third ordinary winner");
    let commit_context = ProtocolObjectContext::signed_plaintext(
        store.root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let commit_prefix = crate::sync::store_commit::commit_semantic_prefix(
        third.candidate_family(),
        &stream_id.to_string(),
        sequence,
        third.commit_hash(),
    );
    let commit_slot = store
        .storage
        .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
        .await
        .expect("allocate third winner commit slot");
    let third_prepared = store
        .storage
        .prepare_protocol_object(
            &commit_context,
            commit_slot,
            &commit_prefix,
            third.to_bytes(),
        )
        .expect("prepare third winner commit");
    store
        .storage
        .create_protocol_object(&third_prepared)
        .await
        .expect("publish third winner commit");
    let third_ref =
        StoreBatchCommitRef::from_commit(&third, coord, third_prepared.reference().clone())
            .expect("reference third winner commit");
    let third_head = StoreDeviceHead::signed(
        store.root.store_root_hash,
        candidate.commit.value.author_registration.clone(),
        third_ref,
        candidate.head.value.history_summary,
        candidate.head.value.successor.clone(),
        &device_signer,
    )
    .expect("sign third winner head");
    let head_context = ProtocolObjectContext::signed_plaintext(
        store.root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let head_prefix = crate::sync::store_commit::head_slot_prefix(
        &candidate
            .commit
            .value
            .author_registration
            .device_id
            .to_string(),
        sequence,
    );
    let head_prepared = store
        .storage
        .prepare_protocol_object(
            &head_context,
            candidate.head.object.slot().clone(),
            &head_prefix,
            third_head.to_bytes(),
        )
        .expect("prepare third winner head");
    store
        .storage
        .create_protocol_object(&head_prepared)
        .await
        .expect("publish third winner head");
}

async fn publish_after_absent_proof_detached(
    publication: ExcludedCandidateHeadPublication,
    peer_db: Database,
    store: Arc<TestStore>,
    signer: UserKeypair,
    write_id: WriteId,
) {
    tokio::spawn(async move {
        if !matches!(
            publication,
            ExcludedCandidateHeadPublication::AfterAbsentProofExactLate
                | ExcludedCandidateHeadPublication::AfterAbsentProofThirdWinner
        ) {
            return;
        }
        let candidate = crate::sync::store::database::StoreDatabase::new(&peer_db)
            .blocked_merge_candidate(write_id)
            .await
            .expect("reload post-proof candidate")
            .expect("post-proof candidate remains prepared");
        match publication {
            ExcludedCandidateHeadPublication::AfterAbsentProofExactLate => {
                store
                    .storage
                    .create_protocol_object(&candidate.head.prepared)
                    .await
                    .expect("publish candidate head after absent proof");
            }
            ExcludedCandidateHeadPublication::AfterAbsentProofThirdWinner => {
                Box::pin(publish_third_candidate_winner(
                    &peer_db,
                    store.as_ref(),
                    &signer,
                    &candidate,
                ))
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
}

#[allow(clippy::too_many_arguments)]
async fn finish_prepared_exclusion_cleanup(
    peer_db: &Database,
    store: &TestStore,
    store_dir: &StoreDir,
    signer: &UserKeypair,
    write_id: WriteId,
    candidates: &crate::database::PreparedMergeAbandonmentCandidates,
    candidate_commit_context: &ProtocolObjectContext,
    publication: PreparedAbandonmentHeadPublication,
) {
    pull_peer_exclusion(peer_db, store, store_dir, ExpectedHeldCandidate::None).await;
    Box::pin(publish_prepared_abandonment_head(
        peer_db,
        store,
        signer,
        candidates,
        publication,
    ))
    .await;
    assert_eq!(
        Box::pin(abandon_merge_candidate(
            peer_db,
            &store.storage,
            signer,
            write_id.clone(),
        ))
        .await
        .expect("exclude prepared abandonment candidates"),
        MergeCandidateAbandonment::Abandoned,
    );
    for reference in [
        &candidates.candidate.head.value.commit,
        &candidates.authority.head.value.commit,
    ] {
        let prefix = crate::sync::store_commit::semantic_prefix_from_exact_object(
            &reference.object,
            ".json",
        )
        .expect("derive cleaned candidate commit prefix");
        assert!(matches!(
            store
                .storage
                .read_protocol_object(candidate_commit_context, &reference.object, &prefix)
                .await,
            Err(crate::sync::storage::StorageError::NotFound(_))
        ));
    }
    let peer_store = store
        .bind_device(peer_db, signer)
        .await
        .expect("bind exclusion cleanup Store");
    for commit in [
        &candidates.candidate.commit.value,
        &candidates.authority.commit.value,
    ] {
        if commit.store_package().is_some() {
            assert!(matches!(
                peer_store
                    .store
                    .load_store_package_for_test(commit.reference())
                    .await,
                Err(StoreError::Object(
                    crate::sync::store_objects::StoreObjectError::Storage(
                        crate::sync::storage::StorageError::NotFound(_)
                    )
                ))
            ));
        }
    }
    assert_eq!(
        crate::sync::store::database::StoreDatabase::new(peer_db)
            .discard_blocked_write(&write_id)
            .await
            .expect("discard excluded prepared abandonment write"),
        crate::sync::store::BlockedWriteDiscard::Discarded(vec![write_id]),
    );
}

async fn pull_peer_exclusion(
    peer_db: &Database,
    store: &TestStore,
    store_dir: &StoreDir,
    expected_held: ExpectedHeldCandidate<'_>,
) {
    let pull = pull_into_result(peer_db, store, store_dir)
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
                reason: crate::sync::store::owner::pull::HeldStorePositionReason::InactiveDevice {
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

#[derive(Clone, Copy, Debug)]
enum AuthorExclusionLocatorTamper {
    Missing,
    ExclusionReference,
    AcceptedCut,
    ActivationCommit,
    ActivationHead,
}

async fn open_published_exclusion_snapshot(
    store: &TestStore,
    store_id: &str,
    membership_floor: &crate::join_code::MembershipFloor,
    schema_version: u32,
    device_id: String,
) -> (tempfile::TempDir, Database) {
    let directory = tempfile::tempdir().expect("restored exclusion directory");
    let path = directory.path().join("restored.db");
    let restorer_identity = crate::keys::UserKeypair::generate();
    let bootstrap = crate::sync::store::snapshot::bootstrap_from_snapshot(
        &store.storage,
        store_id,
        store.root.clone(),
        membership_floor,
        schema_version,
        &path,
        &restorer_identity,
    )
    .await
    .expect("verify author exclusion snapshot");
    let database = bootstrap
        .open_database(
            store_id,
            &path,
            crate::sync::test_helpers::test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            device_id,
            &crate::sync::test_helpers::test_migrations(),
            None,
        )
        .await
        .expect("open author exclusion snapshot");
    (directory, database.into_database())
}

async fn tamper_author_exclusion_locator(
    database: &Database,
    exclusion: &StoreDeviceExclusionRef,
    candidate: &StoreBatchCommitRef,
    tamper: AuthorExclusionLocatorTamper,
) {
    let exclusion = exclusion.clone();
    let candidate = candidate.clone();
    database
        .call(move |connection| {
            let exact = serde_json::to_string(&exclusion).map_err(|error| {
                crate::database::DbError::Message(format!(
                    "serialize exact exclusion reference: {error}"
                ))
            })?;
            let affected = match tamper {
                AuthorExclusionLocatorTamper::Missing => connection.execute(
                    "DELETE FROM store_author_exclusion_activations
                         WHERE exclusion_ref = ?1",
                    [&exact],
                ),
                AuthorExclusionLocatorTamper::ExclusionReference => {
                    let mut wrong = exclusion;
                    wrong.outcome_hash = ObjectHash::digest(b"wrong exclusion reference");
                    let wrong = serde_json::to_string(&wrong).map_err(|error| {
                        crate::database::DbError::Message(format!(
                            "serialize wrong exclusion reference: {error}"
                        ))
                    })?;
                    connection.execute(
                        "UPDATE store_author_exclusion_activations
                             SET exclusion_ref = ?1 WHERE exclusion_ref = ?2",
                        (&wrong, &exact),
                    )
                }
                AuthorExclusionLocatorTamper::AcceptedCut => {
                    let cut: String = connection
                        .query_row(
                            "SELECT accepted_cut
                                 FROM store_author_exclusion_activations
                                 WHERE exclusion_ref = ?1",
                            [&exact],
                            |row| row.get(0),
                        )
                        .map_err(crate::database::DbError::from)?;
                    let mut cut: std::collections::BTreeMap<
                        crate::sync::causal_grants::AuthorStreamId,
                        StoreBatchCommitRef,
                    > = serde_json::from_str(&cut).map_err(|error| {
                        crate::database::DbError::Message(format!(
                            "parse exclusion accepted cut: {error}"
                        ))
                    })?;
                    cut.insert(
                        crate::sync::causal_grants::AuthorStreamId::from_digest(
                            ObjectHash::digest(b"wrong exclusion accepted-cut stream"),
                        ),
                        candidate.clone(),
                    );
                    let wrong = serde_json::to_string(&cut).map_err(|error| {
                        crate::database::DbError::Message(format!(
                            "serialize wrong exclusion accepted cut: {error}"
                        ))
                    })?;
                    connection.execute(
                        "UPDATE store_author_exclusion_activations
                             SET accepted_cut = ?1 WHERE exclusion_ref = ?2",
                        (&wrong, &exact),
                    )
                }
                AuthorExclusionLocatorTamper::ActivationCommit => {
                    let wrong = serde_json::to_string(&candidate).map_err(|error| {
                        crate::database::DbError::Message(format!(
                            "serialize wrong exclusion activation commit: {error}"
                        ))
                    })?;
                    connection.execute(
                        "UPDATE store_author_exclusion_activations
                             SET activation_commit = ?1 WHERE exclusion_ref = ?2",
                        (&wrong, &exact),
                    )
                }
                AuthorExclusionLocatorTamper::ActivationHead => {
                    let head: String = connection
                        .query_row(
                            "SELECT activation_head
                                 FROM store_author_exclusion_activations
                                 WHERE exclusion_ref = ?1",
                            [&exact],
                            |row| row.get(0),
                        )
                        .map_err(crate::database::DbError::from)?;
                    let mut head: crate::sync::store_commit::StoreDeviceHeadRef =
                        serde_json::from_str(&head).map_err(|error| {
                            crate::database::DbError::Message(format!(
                                "parse exclusion activation head: {error}"
                            ))
                        })?;
                    head.head_hash = ObjectHash::digest(b"wrong exclusion activation head");
                    let wrong = serde_json::to_string(&head).map_err(|error| {
                        crate::database::DbError::Message(format!(
                            "serialize wrong exclusion activation head: {error}"
                        ))
                    })?;
                    connection.execute(
                        "UPDATE store_author_exclusion_activations
                             SET activation_head = ?1 WHERE exclusion_ref = ?2",
                        (&wrong, &exact),
                    )
                }
            }
            .map_err(crate::database::DbError::from)?;
            if affected != 1 {
                return Err(crate::database::DbError::Message(format!(
                    "locator tamper {tamper:?} changed {affected} rows"
                )));
            }
            Ok(())
        })
        .await
        .expect("tamper author exclusion locator");
}

struct PreparedWriteTransfer {
    write: (String, String, Vec<u8>, Vec<u8>, String, String, String),
    partitions: Vec<(String, Option<String>, Vec<u8>)>,
    packages: Vec<(String, String)>,
    blobs: Vec<(String, String, String, Option<String>)>,
    remotes: Vec<(String, String)>,
}

async fn transfer_prepared_write(source: &Database, destination: &Database, write_id: &WriteId) {
    let source_write_id = write_id.clone();
    let transfer = source
        .call(move |connection| {
            let write = connection
                .query_row(
                    "SELECT status, affected_rows, changeset, inverse_changeset,
                                base, blob_facts, prepared
                         FROM store_writes WHERE write_id = ?1",
                    [source_write_id.as_str()],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                            row.get(6)?,
                        ))
                    },
                )
                .map_err(crate::database::DbError::from)?;
            let partitions = {
                let mut statement = connection
                    .prepare(
                        "SELECT audience, control_coord, changeset
                             FROM store_write_partitions WHERE write_id = ?1 ORDER BY audience",
                    )
                    .map_err(crate::database::DbError::from)?;
                let rows = statement
                    .query_map([source_write_id.as_str()], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .map_err(crate::database::DbError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(crate::database::DbError::from)?;
                rows
            };
            let packages = {
                let mut statement = connection
                    .prepare(
                        "SELECT audience, remote_object_id
                             FROM store_write_packages WHERE write_id = ?1 ORDER BY audience",
                    )
                    .map_err(crate::database::DbError::from)?;
                let rows = statement
                    .query_map([source_write_id.as_str()], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })
                    .map_err(crate::database::DbError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(crate::database::DbError::from)?;
                rows
            };
            let blobs = {
                let mut statement = connection
                    .prepare(
                        "SELECT audience, locator_hash, remote_object_id, spool_path
                             FROM store_write_blobs WHERE write_id = ?1
                             ORDER BY audience, remote_object_id",
                    )
                    .map_err(crate::database::DbError::from)?;
                let rows = statement
                    .query_map([source_write_id.as_str()], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })
                    .map_err(crate::database::DbError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(crate::database::DbError::from)?;
                rows
            };
            let remotes = {
                let mut statement = connection
                    .prepare("SELECT object_id, state FROM remote_objects ORDER BY object_id")
                    .map_err(crate::database::DbError::from)?;
                let rows = statement
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .map_err(crate::database::DbError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(crate::database::DbError::from)?;
                rows
            };
            Ok(PreparedWriteTransfer {
                write,
                partitions,
                packages,
                blobs,
                remotes,
            })
        })
        .await
        .expect("export prepared write");
    let destination_write_id = write_id.clone();
    destination
        .call(move |connection| {
            let tx = connection
                .unchecked_transaction()
                .map_err(crate::database::DbError::from)?;
            for (object_id, state) in transfer.remotes {
                let imported = tx
                    .execute(
                        "INSERT INTO remote_objects (object_id, state) VALUES (?1, ?2)
                             ON CONFLICT(object_id) DO UPDATE SET state = excluded.state
                             WHERE remote_objects.state = excluded.state",
                        (object_id, state),
                    )
                    .map_err(crate::database::DbError::from)?;
                if imported != 1 {
                    return Err(crate::database::DbError::Message(
                        "prepared write remote object conflicts with restored state".to_string(),
                    ));
                }
            }
            tx.execute(
                "INSERT INTO store_writes
                     (write_id, status, affected_rows, changeset, inverse_changeset,
                      base, blob_facts, prepared)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    destination_write_id.as_str(),
                    transfer.write.0,
                    transfer.write.1,
                    transfer.write.2,
                    transfer.write.3,
                    transfer.write.4,
                    transfer.write.5,
                    transfer.write.6,
                ],
            )
            .map_err(crate::database::DbError::from)?;
            for (audience, control, changeset) in transfer.partitions {
                tx.execute(
                    "INSERT INTO store_write_partitions
                         (write_id, audience, control_coord, changeset) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![destination_write_id.as_str(), audience, control, changeset],
                )
                .map_err(crate::database::DbError::from)?;
            }
            for (audience, object_id) in transfer.packages {
                tx.execute(
                    "INSERT INTO store_write_packages
                         (write_id, audience, remote_object_id) VALUES (?1, ?2, ?3)",
                    rusqlite::params![destination_write_id.as_str(), audience, object_id],
                )
                .map_err(crate::database::DbError::from)?;
            }
            for (audience, locator_hash, object_id, spool_path) in transfer.blobs {
                tx.execute(
                    "INSERT INTO store_write_blobs
                         (write_id, audience, locator_hash, remote_object_id, spool_path)
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                    rusqlite::params![
                        destination_write_id.as_str(),
                        audience,
                        locator_hash,
                        object_id,
                        spool_path
                    ],
                )
                .map_err(crate::database::DbError::from)?;
            }
            tx.commit().map_err(crate::database::DbError::from)
        })
        .await
        .expect("import prepared write");
}

async fn prepare_transfer_candidate(
    peer_db: &Database,
    store: &TestStore,
    signer: &UserKeypair,
    label: &str,
) -> (tempfile::TempDir, StoreDir, WriteId) {
    host_exec(
        peer_db,
        &format!(
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('{label}', 'pending', NULL, 1, \
                         '0000000002000-0000-{label}', '2026-07-18')"
        ),
    )
    .await;
    let (temporary, store_dir) = temp_store_dir();
    assert!(Box::pin(prepare_pending_store_write(
        peer_db,
        &store.storage,
        signer,
        &store_dir,
    ))
    .await
    .expect("prepare transfer candidate"));
    let candidate = crate::sync::store::database::StoreDatabase::new(peer_db)
        .oldest_prepared_store_write()
        .await
        .expect("load transfer candidate")
        .expect("transfer candidate exists");
    let write_id = candidate.commit.value.write_id.clone();
    crate::sync::store::database::StoreDatabase::new(peer_db)
        .set_write_status(
            &write_id,
            crate::WriteStatus::Blocked(crate::WriteBlock::InvalidProtocolState {
                reason: "exercise restored author-exclusion evidence".to_string(),
            }),
        )
        .await
        .expect("block transfer candidate");
    (temporary, store_dir, write_id)
}

async fn create_exclusion_test_store(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    signer: &UserKeypair,
) {
    Store::create(
        store_database(db),
        storage.clone(),
        "device-exclusion-store",
        signer,
    )
    .await
    .expect("create exclusion test Store");
}

async fn stage_uploaded_proposal(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    signer: &UserKeypair,
) -> StoreDeviceExclusionProposalRef {
    let store = Store::load(store_database(db), storage.clone(), signer.clone())
        .await
        .expect("bind exclusion test Store");
    let mut writer = store
        .authorize_writer()
        .await
        .expect("authorize exclusion test writer");
    let plan = writer
        .prepare_plan()
        .await
        .expect("prepare exclusion proposal predecessor");
    let target = plan.registration_ref().clone();
    let proposal_id = StoreDeviceExclusionProposalId::from_hash(ObjectHash::digest(
        b"restart exclusion proposal",
    ));
    let outcome_prefix = device_exclusion_outcome_semantic_prefix(target.device_id, proposal_id);
    let outcome_context = ProtocolObjectContext::signed_plaintext(
        plan.root().store_root_hash,
        ProtocolObjectDomain::StoreDeviceExclusionOutcome,
    );
    let outcome_slot = storage
        .allocate_protocol_slot(&outcome_context, &outcome_prefix, ".json")
        .await
        .expect("allocate exclusion outcome slot");
    let proposal = StoreDeviceExclusionProposal::signed(
        plan.root().store_root_hash,
        proposal_id,
        target.clone(),
        plan.registration(),
        plan.device_state().clone(),
        outcome_slot,
        target.clone(),
        plan.owner_grant().expect("founder Owner grant").clone(),
        plan.registration(),
        plan.device_signer(),
    )
    .expect("sign exclusion proposal");
    let prefix = device_exclusion_proposal_semantic_prefix(
        target.device_id,
        proposal_id,
        proposal.proposal_hash(),
    );
    let context = ProtocolObjectContext::signed_plaintext(
        plan.root().store_root_hash,
        ProtocolObjectDomain::StoreDeviceExclusionProposal,
    );
    let slot = storage
        .allocate_protocol_slot(&context, &prefix, ".json")
        .await
        .expect("allocate exclusion proposal slot");
    let prepared = storage
        .prepare_protocol_object(&context, slot, &prefix, proposal.to_bytes())
        .expect("prepare exclusion proposal object");
    let reference =
        StoreDeviceExclusionProposalRef::from_proposal(&proposal, prepared.reference().clone())
            .expect("reference exclusion proposal");
    let retained = crate::sync::store_commit::RetainedStoreDeviceExclusionProposal::from_exact(
        reference.clone(),
        &proposal,
        plan.registration(),
        plan.registration(),
    )
    .expect("retain prepared exclusion proposal");
    let candidate = writer
        .prepare_candidate(plan, StoreOperationBatch::DeviceExclusionProposal(retained))
        .await
        .expect("prepare exclusion activation candidate");
    let operation = DurableStoreDeviceExclusionOperation::prepared(
        DurableStoreDeviceExclusionObject::Proposal {
            reference: reference.clone(),
            value: proposal,
            prepared,
        },
        candidate,
    )
    .expect("close exclusion journal");
    let durable = StoreDatabase::new(db)
        .begin_outbound_store_device_exclusion(operation)
        .await
        .expect("persist exclusion journal");
    durable
        .create_exact_object(storage.as_ref())
        .await
        .expect("upload exclusion proposal before simulated restart");
    StoreDatabase::new(db)
        .mark_store_device_exclusion_authority_uploaded(durable)
        .await
        .expect("persist proposal upload");
    reference
}

async fn resume_proposal_and_publish_freeze_ack(
    path: &Path,
    storage: Arc<CloudSyncStorage>,
    signer: UserKeypair,
    reference: &StoreDeviceExclusionProposalRef,
) -> (Database, u64) {
    let reopened = open(path, "exclusion-host");
    let reopened_store = TestDevice::load(&reopened, storage.clone(), signer.clone())
        .await
        .expect("bind resumed exclusion Store");
    let mut writer = reopened_store
        .authorize_writer()
        .await
        .expect("authorize resumed exclusion Store");
    let result = Box::pin(AuthorizedDeviceExclusion::new(&mut writer).resume())
        .await
        .expect("resume exclusion proposal")
        .expect("pending exclusion operation");
    assert!(matches!(
        result,
        StoreDeviceExclusionResult::ProposalActivated { proposal, .. }
            if proposal == *reference
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

    let frontier = crate::sync::store_commit::CommitFrontier::from_refs(
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
    let StoreAckExclusionState { proposal_freezes } = acknowledgement.exclusions;
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
    (reopened, base_sequence)
}

async fn race_cancellation_with_ack(
    reopened: Database,
    storage: Arc<CloudSyncStorage>,
    signer: UserKeypair,
    reference: StoreDeviceExclusionProposalRef,
    base_sequence: u64,
) {
    let device = TestDevice::load(&reopened, storage, signer)
        .await
        .expect("bind cancellation Store");
    let (candidate_staged, resume_candidate) = reopened
        .arm_test_pause(crate::database::DatabaseTestPoint::StoreDeviceExclusionCandidateStaged);
    let cancel_device = device.clone();
    let cancel_reference = reference.clone();
    let cancellation_task = tokio::spawn(async move {
        cancel_device
            .cancel_device_exclusion(&cancel_reference)
            .await
    });
    candidate_staged.notified().await;

    let frontier = crate::sync::store_commit::CommitFrontier::from_refs(
        store_database(&reopened)
            .materialized_frontier()
            .await
            .expect("read competing acknowledgement frontier"),
    )
    .expect("shape competing acknowledgement frontier");
    device
        .stage_acknowledgement(frontier, "2026-07-18T00:01:00Z".to_string())
        .await
        .expect("stage competing acknowledgement");
    assert_eq!(
        device
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
    let operations = get_device_exclusion_operations(&reopened)
        .await
        .expect("list exclusion operations");
    assert_eq!(operations.len(), 2);
    assert!(operations.iter().all(|operation| matches!(
        operation.status,
        StoreDeviceExclusionOperationStatus::Completed(_)
    )));
}
