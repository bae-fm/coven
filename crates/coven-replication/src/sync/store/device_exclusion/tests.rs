use std::path::Path;
use std::sync::Arc;

use super::*;
use crate::sync::test_helpers::{store_database, TestDevice, TestStore};
use coven_database::Database;
use coven_foundation::store_dir::StoreDir;
use coven_keys::keys::UserKeypair;
use coven_protocol::store_commit::{StoreDeviceExclusionRef, StoreDeviceRegistrationRef};
use coven_storage::cloud::test_utils::InMemoryCloudHome;
use coven_storage::{BlobPathScheme, CloudCipher, CloudSyncConnection};

fn open(path: &Path, device_id: &str) -> (Database, StoreDir) {
    let store_dir = crate::sync::test_helpers::store_dir_for_test_database(path);
    let database = Database::open_synthetic_for_test(
        path,
        store_dir.clone(),
        crate::sync::test_helpers::test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        device_id.to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &crate::sync::test_helpers::test_migrations(),
    )
    .expect("open exclusion test database");
    (database, store_dir)
}

#[tokio::test]
async fn uploaded_proposal_resumes_after_restart_and_target_can_cancel() {
    let directory = tempfile::tempdir().expect("exclusion test directory");
    let path = directory.path().join("store.sqlite");
    let signer = UserKeypair::generate();
    let home = InMemoryCloudHome::new();
    let storage = Arc::new(CloudSyncConnection::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "device-exclusion-store",
        signer.clone(),
    ));
    let (db, db_store_dir) = open(&path, "exclusion-host");
    let device = TestDevice::create(
        &db,
        db_store_dir.clone(),
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

    let (reopened, reopened_store_dir) = open(&path, "exclusion-host");
    let reopened_store = TestDevice::load(
        &reopened,
        reopened_store_dir.clone(),
        storage.clone(),
        signer.clone(),
    )
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
    let frontier = coven_protocol::store_commit::CommitFrontier::from_refs(
        store_database(&reopened)
            .materialized_frontier()
            .await
            .expect("read exclusion frontier"),
    )
    .expect("shape exclusion frontier");
    reopened_store
        .stage_acknowledgement(frontier, "2026-07-18T00:00:00Z".to_string())
        .await
        .expect("stage exclusion acknowledgement")
        .expect("the reopened device has published no acknowledgement yet");
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
        let (candidate_staged, resume_candidate) = reopened
            .arm_test_pause(coven_database::DatabaseTestPoint::StoreDeviceExclusionCandidateStaged);
        let cancel_device = reopened_store.clone();
        let cancel_reference = reference.clone();
        let cancellation_task = tokio::spawn(async move {
            cancel_device
                .cancel_device_exclusion(&cancel_reference)
                .await
        });
        candidate_staged.notified().await;

        // The competing acknowledgement needs something to acknowledge: a device
        // does not acknowledge its own acknowledgement, so without a write ahead
        // of it the standing one still holds and nothing races the cancellation.
        reopened
            .execute_test_host_write(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('race-1', 'race', NULL, 1, '0000000001000-0000-race', '2026-07-18')",
            )
            .await;
        assert!(!reopened_store
            .prepare_pending_store_write()
            .await
            .expect("defer the write behind the reserved publication"));
        assert_eq!(
            reopened_store
                .latest_local_store_position()
                .await
                .expect("read reserved cancellation position")
                .expect("the prior acknowledgement remains current")
                .coord
                .sequence(),
            base_sequence
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
            } if commit.coord.sequence() == base_sequence + 1
        ));
        assert!(reopened_store
            .prepare_pending_store_write()
            .await
            .expect("prepare the write after the cancellation"));
        assert_eq!(
            reopened_store
                .drain_store_writes()
                .await
                .expect("publish the write after the cancellation"),
            1
        );
        let frontier = coven_protocol::store_commit::CommitFrontier::from_refs(
            store_database(&reopened)
                .materialized_frontier()
                .await
                .expect("read acknowledgement frontier"),
        )
        .expect("shape acknowledgement frontier");
        reopened_store
            .stage_acknowledgement(frontier, "2026-07-18T00:01:00Z".to_string())
            .await
            .expect("stage acknowledgement")
            .expect("the published write is new, so it is acknowledged");
        assert_eq!(
            reopened_store
                .drain_acknowledgements()
                .await
                .expect("publish acknowledgement"),
            1
        );
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
async fn owner_finalizes_exclusion_without_remaining_device_acknowledgements() {
    Box::pin(async {
        let signer = UserKeypair::generate();
        let owner_db_store_dir = crate::sync::test_helpers::test_store_dir();
        let owner_db = crate::sync::test_helpers::open_test_db(owner_db_store_dir.clone());
        let store = Arc::new(
            Box::pin(TestStore::create(
                &owner_db,
                owner_db_store_dir.clone(),
                "device-exclusion-two-device-store",
                signer.clone(),
                crate::sync::test_helpers::test_cloud_home(),
            ))
            .await
            .expect("create two-device exclusion Store"),
        );
        let owner_device = Box::pin(store.open_into(&owner_db, owner_db_store_dir.clone()))
            .await
            .expect("open two-device exclusion Store");
        let peer_db_store_dir = crate::sync::test_helpers::test_store_dir();
        let peer_db = crate::sync::test_helpers::open_test_db(peer_db_store_dir.clone());
        Box::pin(store.activate_joined_device(
            &owner_db,
            owner_db_store_dir.clone(),
            &peer_db,
            peer_db_store_dir.clone(),
            &signer,
            "2026-07-18T00:00:00Z",
        ))
        .await
        .expect("activate peer Store device");

        let local_device_id = owner_device.device_id().clone();
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
async fn occupied_outcome_releases_unuploaded_membership_candidate_objects() {
    Box::pin(async {
        let owner_dir = crate::sync::test_helpers::test_store_dir();
        let owner_db = crate::sync::test_helpers::open_test_db(owner_dir.clone());
        let signer = UserKeypair::generate();
        let store = TestStore::create(
            &owner_db,
            owner_dir.clone(),
            "device-exclusion-outcome-collision",
            signer.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create exclusion Store");
        let owner = store
            .open_into(&owner_db, owner_dir.clone())
            .await
            .expect("open owner device");
        let peer_dir = crate::sync::test_helpers::test_store_dir();
        let peer_db = crate::sync::test_helpers::open_test_db(peer_dir.clone());
        let peer = store
            .activate_joined_device(
                &owner_db,
                owner_dir,
                &peer_db,
                peer_dir,
                &signer,
                "2026-09-08T00:00:00Z",
            )
            .await
            .expect("activate another Owner device");
        let target = StoreDatabase::new(&owner_db)
            .activated_store_device_registration_records()
            .await
            .expect("read active registrations")
            .into_iter()
            .map(|registration| registration.reference().clone())
            .find(|registration| registration.device_id.to_string() == peer.device_id())
            .expect("peer registration");
        let proposal = {
            let mut writer = owner.authorize_writer().await.expect("authorize proposal");
            match writer
                .device_exclusion()
                .propose(&target)
                .await
                .expect("publish proposal")
            {
                StoreDeviceExclusionResult::ProposalActivated { proposal, .. } => proposal,
                other => panic!("expected activated proposal, got {other:?}"),
            }
        };
        peer.pull_store().await.expect("peer observes proposal");
        let mut peer_writer = peer
            .authorize_writer()
            .await
            .expect("authorize peer outcome");
        let pending = peer_writer
            .device_exclusion()
            .prepare_outcome(&proposal, OutcomeIntent::Cancel)
            .await
            .expect("reserve peer cancellation without uploading it");
        let objects = pending.remote_objects().expect("candidate object graph");
        assert_eq!(
            objects.len(),
            4,
            "commit, membership entry, head, and outcome"
        );
        assert_eq!(
            objects
                .iter()
                .filter(|object| matches!(
                    object.record(),
                    coven_protocol::remote_object::RemoteObjectRecord::CandidateExclusive(_)
                ))
                .count(),
            2,
        );
        let accepted = owner
            .cancel_device_exclusion(&proposal)
            .await
            .expect("another device occupies the outcome slot");
        let StoreDeviceExclusionResult::OutcomeActivated {
            outcome: winner, ..
        } = accepted
        else {
            panic!("expected accepted cancellation");
        };
        let DurableStoreDeviceExclusionObject::Outcome {
            reference: intended,
            ..
        } = pending.object()
        else {
            panic!("expected prepared cancellation");
        };
        assert_ne!(intended, &winner);
        assert_eq!(intended.object().slot(), winner.object().slot());
        let result = peer_writer
            .device_exclusion()
            .resume()
            .await
            .expect("settle the occupied outcome without uploading its losing candidate")
            .expect("pending cancellation");
        assert_eq!(
            result,
            StoreDeviceExclusionResult::OutcomeSlotOccupied {
                intended: intended.clone(),
                winner,
            }
        );
        assert!(StoreDatabase::new(&peer_db)
            .active_store_publication()
            .await
            .expect("read publication reservation")
            .is_none());
        assert!(StoreDatabase::new(&peer_db)
            .active_outbound_store_device_exclusion()
            .await
            .expect("read exclusion journal")
            .is_none());
        for object in objects {
            assert!(
                !peer_db
                    .remote_object_id_exists_for_test(object.object_id())
                    .await
                    .expect("read losing object ownership"),
                "the unuploaded losing object has no remaining owner"
            );
        }
    })
    .await;
}
