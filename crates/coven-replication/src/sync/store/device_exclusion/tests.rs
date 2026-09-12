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
async fn staged_proposal_resumes_after_restart_and_target_can_cancel() {
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
    let staged = device
        .stage_device_exclusion_proposal_for_test()
        .await
        .expect("stage exclusion proposal");
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
            if proposal == staged
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
        let cancel_proposal = staged.clone();
        let cancellation_task = tokio::spawn(async move {
            cancel_device
                .cancel_device_exclusion(&cancel_proposal)
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
        let intended = &pending
            .outcome()
            .expect("expected prepared cancellation")
            .reference;
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

/// A commit whose control entry issues a proposal carries nothing else about
/// devices. The entry is the only place the proposal lives, so a commit that
/// disagrees with it has no second object to fall back on.
#[tokio::test]
async fn a_proposal_entry_that_disagrees_with_its_commit_is_refused() {
    use coven_protocol::store_commit::{
        RetainedStoreDeviceExclusionProposal, RetainedStoreDeviceOperations, StoreBatchCommit,
        StoreCommitOperationsInput, VerifiedStoreDeviceOperations,
    };
    use coven_storage::CloudSyncObjectStorage;

    Box::pin(async {
        let owner_dir = crate::sync::test_helpers::test_store_dir();
        let owner_db = crate::sync::test_helpers::open_test_db(owner_dir.clone());
        let signer = UserKeypair::generate();
        let home = crate::sync::test_helpers::test_cloud_home();
        let (store, storage) = TestStore::create_with_connection(
            &owner_db,
            owner_dir.clone(),
            "exclusion-entry-disagreement",
            signer.clone(),
            home.clone(),
        )
        .await
        .expect("create exclusion Store");
        let owner = store
            .bind_device_in(&owner_db, owner_dir.clone(), &signer)
            .await
            .expect("bind owner");
        let peer_dir = crate::sync::test_helpers::test_store_dir();
        let peer_db = crate::sync::test_helpers::open_test_db(peer_dir.clone());
        store
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
        let database = StoreDatabase::new(&owner_db);
        let target = database
            .activated_store_device_registration_records()
            .await
            .expect("read active registrations")
            .into_iter()
            .map(|registration| registration.reference().clone())
            .find(|registration| registration.device_id.to_string() != owner.device_id().as_str())
            .expect("peer registration");

        // Stage the real proposal candidate and stop before its first upload, so
        // the Owner-signed entry and its commit are both genuine.
        home.fail_exact_create_before_call(1);
        owner
            .propose_device_exclusion(&target)
            .await
            .expect_err("interrupt the proposal before its authority upload");
        let operation = database
            .active_outbound_store_device_exclusion()
            .await
            .expect("read exclusion journal")
            .expect("the staged proposal owns the journal");
        let DurableStoreDeviceExclusionOperation::ProposalPrepared {
            proposal,
            candidate,
        } = operation
        else {
            panic!("staged operation is not a proposal");
        };
        let entry = candidate
            .prepared_membership_publication()
            .expect("staged candidate owns its membership publication")
            .entry;
        let root = store.root();
        let author = database
            .activated_store_device_registration(candidate.commit.author_registration.clone())
            .await
            .expect("read the proposing device");
        let target_registration = database
            .activated_store_device_registration(proposal.target.clone())
            .await
            .expect("read the target device");
        let device_signer = author
            .value()
            .device_signer(&signer)
            .expect("proposing device signer");
        let retained = || {
            RetainedStoreDeviceExclusionProposal::from_exact(
                proposal.clone(),
                target_registration.value(),
            )
            .expect("retain the proposal against its target registration")
        };
        RetainedStoreDeviceOperations::from_sources(Some(retained()), Vec::new())
            .verify_for(&root, &candidate.commit, Some(&entry))
            .expect("the staged commit agrees with the entry that issues its proposal");

        let outcome = StoreDeviceExclusionOutcome::Cancelled(
            coven_protocol::store_commit::StoreDeviceExclusionCancellation::signed(
                proposal.clone(),
                author.reference().clone(),
                entry.author_owner_grant.clone(),
                author.value(),
                &device_signer,
            )
            .expect("sign a cancellation of the staged proposal"),
        );
        let prefix = device_exclusion_outcome_semantic_prefix(
            proposal.target.device_id,
            proposal.proposal_id,
        );
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceExclusionOutcome,
        );
        let prepared = storage
            .prepare_protocol_object(
                &context,
                proposal.outcome_slot.clone(),
                &prefix,
                outcome.to_bytes(),
            )
            .expect("prepare the outcome at the proposal's slot");
        let outcome_ref = StoreDeviceExclusionOutcomeRef::from_outcome(
            &outcome,
            &proposal,
            prepared.reference().clone(),
        )
        .expect("exact outcome reference");

        let original = candidate.commit.clone();
        let operations = original
            .operations()
            .expect("the proposal commit carries operations");
        let altered = StoreBatchCommit::signed_operations(
            original.store_root_hash,
            original.write_id.clone(),
            candidate.reference.coord.clone(),
            original.author_registration.clone(),
            author.value(),
            original.order.clone(),
            original.publication_base().clone(),
            original.membership_state.clone(),
            original.device_state.clone(),
            original
                .operations_membership_authority()
                .expect("the proposal commit carries membership authority"),
            StoreCommitOperationsInput {
                control: operations.control.clone(),
                device_exclusion_outcomes: vec![outcome_ref],
                ..StoreCommitOperationsInput::empty()
            },
            &device_signer,
        )
        .expect("the general batch signer permits a control beside an outcome");

        assert!(
            RetainedStoreDeviceOperations::from_sources(Some(retained()), Vec::new())
                .verify_for(&root, &altered, Some(&entry))
                .is_err()
        );
        assert!(VerifiedStoreDeviceOperations::without_exclusions(&altered, Some(&entry)).is_err());
    })
    .await;
}

/// Nothing but the accepted membership entries records a proposal, so a reader
/// that starts from the published snapshot alone still sees it pending.
#[tokio::test]
async fn a_cold_reader_rebuilds_pending_proposals_from_accepted_entries() {
    Box::pin(async {
        let owner_dir = crate::sync::test_helpers::test_store_dir();
        let owner_db = crate::sync::test_helpers::open_test_db(owner_dir.clone());
        let signer = UserKeypair::generate();
        let (store, storage) = TestStore::create_with_connection(
            &owner_db,
            owner_dir.clone(),
            "exclusion-cold-reader",
            signer.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create exclusion Store");
        let owner = store
            .bind_device_in(&owner_db, owner_dir.clone(), &signer)
            .await
            .expect("bind owner");
        let peer_dir = crate::sync::test_helpers::test_store_dir();
        let peer_db = crate::sync::test_helpers::open_test_db(peer_dir.clone());
        store
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
        let database = StoreDatabase::new(&owner_db);
        let target = database
            .activated_store_device_registration_records()
            .await
            .expect("read active registrations")
            .into_iter()
            .map(|registration| registration.reference().clone())
            .find(|registration| registration.device_id.to_string() != owner.device_id().as_str())
            .expect("peer registration");
        let proposal = match owner
            .propose_device_exclusion(&target)
            .await
            .expect("publish proposal")
        {
            StoreDeviceExclusionResult::ProposalActivated { proposal, .. } => proposal,
            other => panic!("expected activated proposal, got {other:?}"),
        };
        owner
            .publish_snapshot_generation_for_test()
            .await
            .expect("publish a snapshot covering the pending proposal");
        let root = store.root();
        let selected = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
            .open_pinned(storage.as_ref(), &root)
            .await
            .expect("open cold verifier")
            .load_current_accepted_snapshot()
            .await
            .expect("a cold reader rebuilds the proposal from accepted authority entries");
        let record = selected
            .snapshot
            .meta
            .state
            .devices
            .devices
            .get(&target.device_id)
            .expect("the snapshot carries the target device");
        assert!(matches!(
            record.proposals.get(&proposal.proposal_id),
            Some(StoreDeviceProposalState::Pending { proposal: pending }) if pending == &proposal
        ));
        assert!(
            !store.exact_creates().iter().any(|slot| slot
                .logical_key()
                .starts_with("store-v1/device-exclusion-proposals/")),
            "a proposal writes no object of its own"
        );
    })
    .await;
}

/// Every control-carrying batch names the entry its control was prepared from,
/// so a locally authored batch whose entry proposes an exclusion cannot pass the
/// device-operation check by being some other kind of batch.
#[tokio::test]
async fn a_locally_authored_control_entry_that_proposes_is_refused() {
    use crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch;
    use coven_storage::CloudSyncObjectStorage;

    Box::pin(async {
        let owner_dir = crate::sync::test_helpers::test_store_dir();
        let owner_db = crate::sync::test_helpers::open_test_db(owner_dir.clone());
        let signer = UserKeypair::generate();
        let (store, storage) = TestStore::create_with_connection(
            &owner_db,
            owner_dir.clone(),
            "exclusion-proposing-control-entry",
            signer.clone(),
            crate::sync::test_helpers::test_cloud_home(),
        )
        .await
        .expect("create exclusion Store");
        let owner = store
            .bind_device_in(&owner_db, owner_dir.clone(), &signer)
            .await
            .expect("bind owner");
        let peer_dir = crate::sync::test_helpers::test_store_dir();
        let peer_db = crate::sync::test_helpers::open_test_db(peer_dir.clone());
        store
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
            .find(|registration| registration.device_id.to_string() != owner.device_id().as_str())
            .expect("peer registration");

        let mut writer = owner.authorize_writer().await.expect("authorize owner");
        let plan = writer
            .prepare_plan()
            .await
            .expect("reserve an author position");
        let proposal_id = StoreDeviceExclusionProposalId::from_hash(ObjectHash::digest(
            b"locally authored proposing control entry",
        ));
        let prefix = device_exclusion_outcome_semantic_prefix(target.device_id, proposal_id);
        let context = ProtocolObjectContext::signed_plaintext(
            plan.root().store_root_hash,
            ProtocolObjectDomain::StoreDeviceExclusionOutcome,
        );
        let outcome_slot = storage
            .allocate_protocol_slot(&context, &prefix, ".json")
            .await
            .expect("reserve the outcome slot");
        let proposal = StoreDeviceExclusionProposal {
            proposal_id,
            target,
            outcome_slot,
        };
        let transition = writer
            .prepare_authority_change(
                plan.membership(),
                coven_protocol::membership::StoreAuthorityChange::DeviceExclusionProposal {
                    proposal,
                },
            )
            .await
            .expect("prepare the proposing authority change");

        let error = writer
            .prepare_candidate(
                &plan,
                StoreOperationBatch::MergeMembershipActivation {
                    entry: transition.entry.clone(),
                    transition: transition.transition.clone(),
                    stream_activations: Vec::new(),
                },
            )
            .await
            .expect_err("a membership-activation batch cannot issue an exclusion proposal");
        assert!(
            error
                .to_string()
                .contains("Store device state differs from its signed predecessor state"),
            "{error}"
        );
    })
    .await;
}
