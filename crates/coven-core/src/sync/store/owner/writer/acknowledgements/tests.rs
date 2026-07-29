use std::path::Path;
use std::sync::Arc;

use super::*;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::store_commit::CommitFrontier;
use crate::sync::test_helpers::TestDevice;

fn open(path: &Path, device_id: &str) -> Database {
    Database::open(
        path,
        crate::sync::test_helpers::test_synced_tables(),
        crate::blob::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::one_at_a_time(),
        device_id.to_string(),
        &crate::sync::test_helpers::test_migrations(),
    )
    .expect("open acknowledgement test database")
    .0
}

fn store_database(database: &Database) -> StoreDatabase {
    StoreDatabase::new(database)
}

fn storage(home: &InMemoryCloudHome, signer: &UserKeypair) -> Arc<CloudSyncStorage> {
    Arc::new(
        CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "ack-exact-store",
            signer.clone(),
        )
        .expect("construct acknowledgement test storage"),
    )
}

async fn initialize(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    signer: &UserKeypair,
) -> TestDevice {
    TestDevice::create(db, storage.clone(), "ack-exact-store", signer.clone())
        .await
        .expect("create acknowledgement test Store")
}

async fn stage(device: &TestDevice) -> StoreAck {
    stage_at(device, "2026-07-16T00:00:00Z").await
}

async fn stage_at(device: &TestDevice, last_sync: &str) -> StoreAck {
    let frontier = CommitFrontier::from_refs(
        crate::sync::store::database::StoreDatabase::new(&device.db)
            .materialized_frontier()
            .await
            .expect("read acknowledgement frontier"),
    )
    .expect("shape acknowledgement frontier");
    device
        .stage_acknowledgement_exact(frontier, last_sync.to_string())
        .await
        .expect("stage exact acknowledgement")
}

async fn drain(device: &TestDevice) -> Result<u64, StoreAckError> {
    device.drain_acknowledgements_exact().await
}

async fn persist_candidate(
    device: &TestDevice,
    outbound: &crate::database::OutboundStoreAck,
) -> crate::sync::store::operations::PreparedStoreOperationCommit {
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize acknowledgement writer");
    let plan = writer
        .prepare_plan()
        .await
        .expect("prepare acknowledgement activation");
    plan.common()
        .validate_acknowledgement(&outbound.ack.value)
        .expect("acknowledgement matches activation predecessor");
    let candidate = writer
        .prepare_candidate(
            plan,
            crate::sync::store::operations::StoreOperationBatch::Acknowledgement {
                reference: outbound.reference.clone(),
                value: outbound.ack.value.clone(),
                circle_acknowledgements: Vec::new(),
            },
        )
        .await
        .expect("prepare acknowledgement candidate");
    device
        .prepare_acknowledgement_activation_for_test(outbound.reference.clone(), candidate.clone())
        .await
        .expect("persist acknowledgement candidate");
    candidate
}

struct LosingAckFixture {
    home: InMemoryCloudHome,
    signer: UserKeypair,
    storage: Arc<CloudSyncStorage>,
    db: Database,
    device: TestDevice,
    outbound: crate::database::OutboundStoreAck,
    losing: crate::sync::store::operations::PreparedStoreOperationCommit,
}

async fn losing_ack_fixture(path: &Path) -> LosingAckFixture {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(path, "ack-loser-device");
    let device = Box::pin(initialize(&db, &storage, &signer)).await;
    Box::pin(stage(&device)).await;
    let outbound = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .expect("staged acknowledgement exists");
    let losing = Box::pin(persist_candidate(&device, &outbound)).await;
    let mut writer = device
        .authorize_writer()
        .await
        .expect("authorize competing acknowledgement writer");
    let competing_plan = writer
        .prepare_plan()
        .await
        .expect("prepare competing Store operation");
    let grant_id = crate::sync::provider::ProviderAccessGrantId::from_random_bytes([91; 32]);
    let grant_prefix = crate::sync::store_commit::provider_access_grant_semantic_prefix(&grant_id);
    let grant_bytes = b"competing provider grant";
    let grant = crate::sync::provider::StoreMemberProviderAccessGrantRef {
        grant_id,
        grant_hash: crate::sync::store_commit::ObjectHash::digest(grant_bytes),
        object: crate::sync::storage::ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(format!("{grant_prefix}.json"))
                .expect("valid provider grant slot"),
            grant_bytes.len() as u64,
            crate::sync::store_commit::ObjectHash::digest(grant_bytes),
        ),
    };
    let competing = writer
        .prepare_candidate(
            competing_plan,
            crate::sync::store::operations::StoreOperationBatch::ProviderAccessGrant(grant),
        )
        .await
        .expect("prepare competing candidate");
    assert_ne!(competing.reference, losing.reference);
    let (_, competing_head) = competing.publication_for_test();
    storage
        .create_protocol_object(&competing.prepared)
        .await
        .expect("publish competing commit");
    storage
        .create_protocol_object(competing_head)
        .await
        .expect("publish competing head");
    LosingAckFixture {
        home,
        signer,
        storage,
        db,
        device,
        outbound,
        losing,
    }
}

#[tokio::test]
async fn staged_acknowledgement_reuses_its_exact_object_after_restart_and_lost_response() {
    let directory = tempfile::tempdir().expect("acknowledgement database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(&path, "ack-test-device");
    let device = initialize(&db, &storage, &signer).await;
    let founder_ack = store_database(&db)
        .latest_local_store_ack()
        .await
        .expect("read founder acknowledgement")
        .expect("Store creation publishes its founder acknowledgement");
    let ack = stage(&device).await;
    assert_eq!(ack.sequence, founder_ack.reference.sequence + 1);
    assert_eq!(
        ack.successor.predecessor,
        Some(founder_ack.reference.object)
    );
    let staged = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .expect("read acknowledgement outbox")
        .expect("staged acknowledgement exists");
    drop(device);
    drop(db);

    let reopened = open(&path, "ack-test-device");
    let reopened_device = TestDevice::load(&reopened, storage.clone(), signer.clone())
        .await
        .expect("bind reopened acknowledgement Store");
    home.fail_exact_create_after_call(1);
    assert_eq!(
        drain(&reopened_device)
            .await
            .expect("resolve lost exact-create response"),
        1
    );
    let published = store_database(&reopened)
        .latest_local_store_ack()
        .await
        .expect("read published acknowledgement")
        .expect("published acknowledgement exists");
    assert_eq!(published.reference, staged.reference);
    assert_eq!(published.reference.ack_hash, ack.ack_hash());
    assert!(store_database(&reopened)
        .oldest_outbound_store_ack()
        .await
        .expect("read drained acknowledgement outbox")
        .is_none());
    assert_eq!(home.exact_create_count(), 3);
}

#[tokio::test]
async fn invalid_acknowledgement_slot_bytes_are_never_replaced_or_completed() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(Path::new(":memory:"), "ack-test-device");
    let device = initialize(&db, &storage, &signer).await;
    let founder_ack = store_database(&db)
        .latest_local_store_ack()
        .await
        .expect("read founder acknowledgement")
        .expect("Store creation publishes its founder acknowledgement");
    stage(&device).await;
    let pending = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .expect("read acknowledgement outbox")
        .expect("staged acknowledgement exists");
    let slot = pending.reference.object.slot().clone();
    home.insert_exact_object(slot.logical_key(), b"competing bytes".to_vec());

    assert!(drain(&device).await.is_err());
    assert_eq!(
        home.get(slot.logical_key()),
        Some(b"competing bytes".to_vec())
    );
    assert!(store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .expect("read retained acknowledgement outbox")
        .is_some());
    assert_eq!(
        store_database(&db)
            .latest_local_store_ack()
            .await
            .expect("read unchanged published acknowledgement state")
            .expect("founder acknowledgement remains published")
            .reference,
        founder_ack.reference
    );
}

async fn assert_valid_acknowledgement_slot_winner_is_adopted_and_activated() {
    let directory = tempfile::tempdir().expect("acknowledgement database directory");
    let seed_path = directory.path().join("seed.sqlite3");
    let winner_path = directory.path().join("winner.sqlite3");
    let loser_path = directory.path().join("loser.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let seed = open(&seed_path, "ack-slot-race-device");
    let seed_device = initialize(&seed, &storage, &signer).await;
    for destination in [&winner_path, &loser_path] {
        let destination = destination
            .to_str()
            .expect("temporary database path is UTF-8")
            .to_string();
        seed.call(move |connection| {
            connection
                .execute("VACUUM INTO ?1", [destination])
                .map(|_| ())
                .map_err(crate::DbError::from)
        })
        .await
        .expect("copy acknowledgement database");
    }
    drop(seed_device);
    drop(seed);

    let winner_db = open(&winner_path, "ack-slot-race-device");
    let winner_device = TestDevice::load(&winner_db, storage.clone(), signer.clone())
        .await
        .expect("bind winner acknowledgement Store");
    stage_at(&winner_device, "2026-07-16T00:00:01Z").await;
    let winner = store_database(&winner_db)
        .oldest_outbound_store_ack()
        .await
        .expect("read winner acknowledgement")
        .expect("winner acknowledgement exists");
    storage
        .create_protocol_object(&winner.ack.prepared)
        .await
        .expect("publish winner acknowledgement");

    let loser_db = open(&loser_path, "ack-slot-race-device");
    let loser_device = TestDevice::load(&loser_db, storage.clone(), signer.clone())
        .await
        .expect("bind loser acknowledgement Store");
    stage_at(&loser_device, "2026-07-16T00:00:02Z").await;
    let loser = store_database(&loser_db)
        .oldest_outbound_store_ack()
        .await
        .expect("read losing acknowledgement")
        .expect("losing acknowledgement exists");
    assert_eq!(
        winner.reference.object.slot(),
        loser.reference.object.slot()
    );
    assert_ne!(winner.reference, loser.reference);
    let losing_candidate = persist_candidate(&loser_device, &loser).await;
    let losing_object_ids = losing_candidate
        .acknowledgement_remote_objects(&loser.ack)
        .expect("load losing acknowledgement candidate graph")
        .into_iter()
        .map(|remote| remote.object_id())
        .collect::<Vec<_>>();

    let result = drain(&loser_device).await;
    assert_eq!(
        result.expect("adopt and activate acknowledgement slot winner"),
        1
    );
    assert_eq!(
        store_database(&loser_db)
            .latest_local_store_ack()
            .await
            .expect("read adopted acknowledgement")
            .expect("adopted acknowledgement is published")
            .reference,
        winner.reference
    );
    assert!(store_database(&loser_db)
        .oldest_outbound_store_ack()
        .await
        .expect("read drained acknowledgement outbox")
        .is_none());
    assert!(crate::sync::store::database::StoreDatabase::new(&loser_db)
        .protocol_inert_object(loser.reference.object)
        .await
        .expect("read losing acknowledgement inert state")
        .is_none());
    for object_id in losing_object_ids {
        let exists = loser_db
            .call(move |connection| {
                connection
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM remote_objects WHERE object_id = ?1)",
                        [object_id.to_string()],
                        |row| row.get::<_, bool>(0),
                    )
                    .map_err(crate::DbError::from)
            })
            .await
            .expect("read losing acknowledgement candidate ownership");
        assert!(!exists);
    }
}

#[tokio::test]
async fn valid_acknowledgement_slot_winner_is_adopted_and_activated() {
    Box::pin(assert_valid_acknowledgement_slot_winner_is_adopted_and_activated()).await;
}

#[tokio::test]
async fn acknowledgement_predecessor_and_reserved_successor_form_one_exact_chain() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(Path::new(":memory:"), "ack-test-device");
    let device = initialize(&db, &storage, &signer).await;
    let first = stage(&device).await;
    drain(&device).await.expect("publish first acknowledgement");
    let first_published = store_database(&db)
        .latest_local_store_ack()
        .await
        .expect("read first acknowledgement")
        .expect("first acknowledgement exists");
    let second = stage(&device).await;
    let second_pending = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .expect("read second acknowledgement")
        .expect("second acknowledgement exists");

    assert_eq!(
        second.successor.predecessor,
        Some(first_published.reference.object)
    );
    assert_eq!(
        second_pending.reference.object.slot(),
        &first.successor.next_slot
    );
    assert_eq!(second.sequence, first.sequence + 1);
    drain(&device)
        .await
        .expect("publish successor acknowledgement after an activated predecessor");
    assert_eq!(
        store_database(&db)
            .latest_local_store_ack()
            .await
            .unwrap()
            .expect("successor acknowledgement exists")
            .reference,
        second_pending.reference
    );
}

#[tokio::test]
async fn activated_acknowledgement_completes_its_outbox_after_restart_without_another_commit() {
    let directory = tempfile::tempdir().expect("acknowledgement database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(&path, "ack-test-device");
    let device = Box::pin(initialize(&db, &storage, &signer)).await;
    Box::pin(stage(&device)).await;
    let outbound = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .expect("staged acknowledgement exists");
    storage
        .create_protocol_object(&outbound.ack.prepared)
        .await
        .expect("publish acknowledgement object");
    let candidate = persist_candidate(&device, &outbound).await;
    let acknowledgement_remote = candidate
        .acknowledgement_remote_objects(&outbound.ack)
        .expect("candidate owns acknowledgement")
        .into_iter()
        .find(|remote| remote.object() == &outbound.reference.object)
        .expect("acknowledgement remote object");
    crate::sync::store::database::StoreDatabase::new(&db)
        .mark_remote_object_uploaded(acknowledgement_remote)
        .await
        .expect("record acknowledgement upload");
    device
        .authorize_writer()
        .await
        .expect("authorize acknowledgement activation")
        .publish_prepared(Box::new(candidate), None, None)
        .await
        .expect("activate acknowledgement commit");
    let activated_position = device.latest_local_store_position().await.unwrap();
    drop(device);
    drop(db);

    let reopened = open(&path, "ack-test-device");
    let reopened_store = TestDevice::load(&reopened, storage.clone(), signer.clone())
        .await
        .expect("bind reopened acknowledgement Store");
    assert_eq!(drain(&reopened_store).await.unwrap(), 1);
    assert_eq!(
        reopened_store.latest_local_store_position().await.unwrap(),
        activated_position
    );
    assert!(store_database(&reopened)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn prepared_activation_candidate_resumes_exactly_after_restart() {
    let directory = tempfile::tempdir().expect("acknowledgement database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(&path, "ack-test-device");
    let device = initialize(&db, &storage, &signer).await;
    stage(&device).await;
    let outbound = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .expect("staged acknowledgement exists");
    let candidate = persist_candidate(&device, &outbound).await;
    let expected = candidate.reference.clone();
    drop(device);
    drop(db);

    let reopened = open(&path, "ack-test-device");
    let reopened_store = TestDevice::load(&reopened, storage.clone(), signer.clone())
        .await
        .expect("bind reopened acknowledgement Store");
    let resumed = store_database(&reopened)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .expect("prepared acknowledgement exists after restart");
    assert!(matches!(
        resumed.activation,
        crate::database::OutboundStoreAckActivation::Prepared(ref prepared)
            if prepared.reference == expected
    ));
    assert_eq!(drain(&reopened_store).await.unwrap(), 1);
    assert_eq!(
        reopened_store.latest_local_store_position().await.unwrap(),
        Some(expected)
    );
}

#[tokio::test]
async fn uploaded_acknowledgement_accepts_its_sole_candidate_nonactivation() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(Path::new(":memory:"), "ack-nonactivation-device");
    let device = initialize(&db, &storage, &signer).await;
    stage(&device).await;
    let outbound = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .expect("staged acknowledgement exists");
    let candidate = persist_candidate(&device, &outbound).await;
    let mut acknowledgement = candidate
        .acknowledgement_remote_objects(&outbound.ack)
        .expect("candidate owns acknowledgement")
        .into_iter()
        .find(|remote| remote.object() == &outbound.reference.object)
        .expect("acknowledgement ownership record");
    acknowledgement
        .mark_uploaded_verified()
        .expect("acknowledgement upload is durable");
    let winner_bytes = b"different valid winner head";
    let winner_object = crate::sync::storage::ExactObjectRef::new(
        crate::storage::cloud::ObjectSlot::logical(
            "store-v1/heads/ack-nonactivation-winner.json".to_string(),
        )
        .expect("valid winner slot"),
        winner_bytes.len() as u64,
        crate::sync::store_commit::ObjectHash::digest(winner_bytes),
    );
    let nonactivation = crate::sync::remote_object::CandidateNonactivation::unverified_for_test(
        crate::sync::store_commit::StoreBatchCommitDeletionTarget {
            coord: candidate.reference.coord.clone(),
            object: candidate.reference.object.clone(),
            canonical_signed_bytes: candidate.commit.to_bytes(),
        },
        crate::sync::remote_object::CandidateNonactivationProof::MergeWinner {
            winner_head: crate::sync::store_commit::StoreDeviceHeadRef {
                head_hash: crate::sync::store_commit::ObjectHash::digest(winner_bytes),
                object: winner_object,
            },
        },
    );

    assert!(acknowledgement
        .begin_candidate_nonactivation(nonactivation)
        .expect("uploaded acknowledgement accepts candidate loss")
        .is_some());
}

#[tokio::test]
async fn losing_activation_inerts_the_uploaded_acknowledgement() {
    let LosingAckFixture {
        home,
        signer: _,
        storage: _,
        db,
        device,
        outbound,
        losing,
    } = Box::pin(losing_ack_fixture(Path::new(":memory:"))).await;

    assert_eq!(
        drain(&device)
            .await
            .expect("settle losing acknowledgement activation"),
        1
    );
    assert!(store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        store_database(&db)
            .latest_local_store_ack()
            .await
            .unwrap()
            .expect("inert acknowledgement still advances its physical stream")
            .reference,
        outbound.reference
    );
    let inert = crate::sync::store::database::StoreDatabase::new(&db)
        .protocol_inert_object(outbound.reference.object.clone())
        .await
        .unwrap()
        .expect("losing acknowledgement is retained outside reducer state");
    assert!(matches!(
        inert.identity.domain,
        crate::sync::remote_object::RetainedAuthorityObjectDomain::Acknowledgement {
            ref reference
        } if reference == &outbound.reference
    ));
    assert!(inert
        .candidate_nonactivation_proof(&losing.reference)
        .expect("inert acknowledgement proof is valid")
        .is_some());
    assert!(home
        .get(losing.reference.object.slot().logical_key())
        .is_none());
    assert_ne!(
        store_database(&db)
            .activated_store_ack(&outbound.reference.registration)
            .await
            .unwrap(),
        Some(outbound.reference.clone())
    );
}

#[tokio::test]
async fn acknowledgement_nonactivation_resumes_after_delete_failure_and_restart() {
    let directory = tempfile::tempdir().expect("acknowledgement database directory");
    let path = directory.path().join("store.sqlite3");
    let LosingAckFixture {
        home,
        signer,
        storage,
        db,
        device,
        outbound,
        losing,
    } = Box::pin(losing_ack_fixture(&path)).await;
    home.fail_exact_delete_on_call(1);

    assert!(drain(&device).await.is_err());
    assert!(matches!(
        store_database(&db).oldest_outbound_store_ack()
            .await
            .unwrap()
            .expect("nonactivating acknowledgement remains durable")
            .activation,
        crate::database::OutboundStoreAckActivation::Nonactivating(ref candidate)
            if candidate.reference == losing.reference
    ));
    assert!(crate::sync::store::database::StoreDatabase::new(&db)
        .protocol_inert_object(outbound.reference.object.clone())
        .await
        .unwrap()
        .is_some());
    drop(device);
    drop(db);

    let reopened = open(&path, "ack-loser-device");
    let reopened_device = TestDevice::load(&reopened, storage.clone(), signer.clone())
        .await
        .expect("bind reopened losing acknowledgement Store");
    assert_eq!(drain(&reopened_device).await.unwrap(), 1);
    assert!(store_database(&reopened)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .is_none());
    assert!(home
        .get(losing.reference.object.slot().logical_key())
        .is_none());
}

#[tokio::test]
async fn acknowledgement_completion_rejects_mismatched_durable_loss_proofs() {
    Box::pin(run_acknowledgement_completion_rejects_mismatched_durable_loss_proofs()).await;
}

async fn run_acknowledgement_completion_rejects_mismatched_durable_loss_proofs() {
    let LosingAckFixture {
        home,
        signer: _,
        storage: _,
        db,
        device,
        outbound,
        losing,
    } = Box::pin(losing_ack_fixture(Path::new(":memory:"))).await;
    home.fail_exact_delete_on_call(1);
    assert!(Box::pin(drain(&device)).await.is_err());

    let head = crate::sync::store_commit::StoreDeviceHeadRef {
        head_hash: losing.head.head_hash(),
        object: losing.prepared_head.reference().clone(),
    };
    db.call(move |conn| {
        let object_id = crate::sync::remote_object::remote_object_id(&head.object);
        let encoded: String = conn
            .query_row(
                "SELECT state FROM remote_objects WHERE object_id = ?1",
                [object_id.to_string()],
                |row| row.get(0),
            )
            .map_err(crate::DbError::from)?;
        let mut remote: crate::sync::remote_object::RemoteObjectRecord =
            serde_json::from_str(&encoded).map_err(|error| {
                crate::DbError::Message(format!("parse test head ownership: {error}"))
            })?;
        let crate::sync::remote_object::RemoteObjectRecord::RetainedAuthority(record) = &mut remote
        else {
            return Err(crate::DbError::Message(
                "test head is not retained authority".to_string(),
            ));
        };
        let crate::sync::remote_object::RetainedAuthorityObjectState::UncreatedVerified {
            former_candidates,
        } = &mut record.state
        else {
            return Err(crate::DbError::Message(
                "test head is not proven uncreated".to_string(),
            ));
        };
        let Some(nonactivation) = former_candidates.first_mut() else {
            return Err(crate::DbError::Message(
                "test head has no loss proof".to_string(),
            ));
        };
        let crate::sync::remote_object::CandidateNonactivationProof::MergeWinner { winner_head } =
            nonactivation.proof_mut_for_test()
        else {
            return Err(crate::DbError::Message(
                "test head has a non-Merge loss proof".to_string(),
            ));
        };
        let tampered_bytes = b"different winner at the same head slot";
        winner_head.object = crate::sync::storage::ExactObjectRef::new(
            winner_head.object.slot().clone(),
            tampered_bytes.len() as u64,
            crate::sync::store_commit::ObjectHash::digest(tampered_bytes),
        );
        remote.validate().map_err(|error| {
            crate::DbError::Message(format!("validate test head ownership: {error}"))
        })?;
        conn.execute(
            "UPDATE remote_objects SET state = ?2 WHERE object_id = ?1",
            (
                object_id.to_string(),
                serde_json::to_string(&remote).map_err(|error| {
                    crate::DbError::Message(format!("serialize test head ownership: {error}"))
                })?,
            ),
        )
        .map_err(crate::DbError::from)?;
        Ok(())
    })
    .await
    .expect("install mismatched durable head proof");

    assert!(Box::pin(drain(&device)).await.is_err());
    assert_eq!(
        store_database(&db)
            .oldest_outbound_store_ack()
            .await
            .unwrap()
            .expect("mismatched proof keeps acknowledgement pending")
            .reference,
        outbound.reference
    );
}

#[tokio::test]
async fn alternate_head_for_the_same_ack_candidate_is_adopted() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(Path::new(":memory:"), "ack-alternate-head-device");
    let device = initialize(&db, &storage, &signer).await;
    stage(&device).await;
    let outbound = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .expect("staged acknowledgement exists");
    let candidate = persist_candidate(&device, &outbound).await;
    let expected_head = candidate.head.clone();
    let expected_prepared = candidate.prepared_head.clone();
    let alternate_next = crate::storage::cloud::ObjectSlot::opaque(
        expected_head.successor.next_slot.logical_key().to_string(),
        "alternate-next-slot".to_string(),
    )
    .expect("valid alternate successor slot");
    let alternate_head = device
        .sign_device_head_for_test(
            candidate.reference.clone(),
            expected_head.history_summary,
            crate::sync::store_commit::SuccessorLink {
                activation: expected_head.successor.activation,
                predecessor: expected_head.successor.predecessor.clone(),
                next_slot: alternate_next,
            },
        )
        .await
        .expect("sign alternate head");
    let head_context = ProtocolObjectContext::signed_plaintext(
        device.store_root().store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let head_prefix = crate::sync::store_commit::head_slot_prefix(
        &outbound.reference.registration.device_id.to_string(),
        candidate.commit.seq(),
    );
    let alternate_prepared = storage
        .prepare_protocol_object(
            &head_context,
            expected_prepared.reference().slot().clone(),
            &head_prefix,
            alternate_head.to_bytes(),
        )
        .expect("prepare alternate head at the same slot");
    assert_ne!(
        alternate_prepared.reference(),
        expected_prepared.reference()
    );
    storage
        .create_protocol_object(&alternate_prepared)
        .await
        .expect("publish alternate head");

    assert_eq!(drain(&device).await.unwrap(), 1);
    assert_eq!(
        store_database(&db)
            .activated_store_ack(&outbound.reference.registration)
            .await
            .unwrap(),
        Some(outbound.reference)
    );
}

async fn local_device_id(db: &Database) -> crate::sync::store_commit::StoreDeviceId {
    db.get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .unwrap()
        .expect("local Store device id")
        .parse()
        .expect("parse local Store device id")
}

async fn current_frontier(db: &Database) -> CommitFrontier {
    CommitFrontier::from_refs(
        store_database(db)
            .materialized_frontier()
            .await
            .expect("read frontier"),
    )
    .expect("shape frontier")
}

#[tokio::test]
async fn circle_acknowledgement_publishes_activates_and_is_read_back() {
    let directory = tempfile::tempdir().expect("acknowledgement database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(&path, "circle-ack-device");
    let device = initialize(&db, &storage, &signer).await;
    let (circle_id, _control) = db
        .call(|conn| {
            Ok(crate::sync::test_helpers::install_test_active_circle(
                conn,
                "ack-circle",
            ))
        })
        .await
        .expect("install active Circle");

    let frontier = current_frontier(&db).await;
    stage(&device).await;
    device
        .stage_circle_acknowledgements(&frontier, "2026-07-16T00:00:00Z")
        .await
        .expect("stage Circle acknowledgements");
    assert_eq!(drain(&device).await.unwrap(), 1);

    let device_id = local_device_id(&db).await;
    let reference = store_database(&db)
        .activated_circle_ack(circle_id, device_id)
        .await
        .expect("read activated Circle acknowledgement")
        .expect("Circle acknowledgement activated with the Store commit");
    assert_eq!(reference.circle_id, circle_id);
    assert_eq!(reference.sequence, 1);
    // Reading and verifying an activated Circle acknowledgement — including under
    // a rotated-away epoch and its exact seed coverage — is exercised on the
    // production close/two-device fixtures in circle_controls::tests, where the
    // retained control activations the reader resolves the epoch key from exist.
}

#[tokio::test]
async fn inactive_circle_stages_no_acknowledgement() {
    let directory = tempfile::tempdir().expect("acknowledgement database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(&path, "inactive-circle-device");
    let device = initialize(&db, &storage, &signer).await;
    let (circle_id, _control) = db
        .call(|conn| {
            Ok(crate::sync::test_helpers::install_test_inactive_circle(
                conn,
                "inactive-circle",
            ))
        })
        .await
        .expect("install inactive Circle");

    let frontier = current_frontier(&db).await;
    stage(&device).await;
    device
        .stage_circle_acknowledgements(&frontier, "2026-07-16T00:00:00Z")
        .await
        .expect("stage Circle acknowledgements");
    assert_eq!(drain(&device).await.unwrap(), 1);

    let device_id = local_device_id(&db).await;
    assert_eq!(
        store_database(&db)
            .activated_circle_ack(circle_id, device_id)
            .await
            .expect("read activated Circle acknowledgement"),
        None,
        "an inactive recipient publishes no Circle acknowledgement"
    );
}

#[tokio::test]
async fn circle_acknowledgement_resumes_idempotently_across_restart() {
    let directory = tempfile::tempdir().expect("acknowledgement database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(&path, "circle-ack-restart");
    let device = initialize(&db, &storage, &signer).await;
    let (circle_id, _control) = db
        .call(|conn| {
            Ok(crate::sync::test_helpers::install_test_active_circle(
                conn,
                "restart-circle",
            ))
        })
        .await
        .expect("install active Circle");

    let frontier = current_frontier(&db).await;
    stage(&device).await;
    device
        .stage_circle_acknowledgements(&frontier, "2026-07-16T00:00:00Z")
        .await
        .expect("stage Circle acknowledgements");
    // Crash between staging and draining: the outbound Circle acknowledgement is
    // durable and its object is not yet activated.
    let device_id = local_device_id(&db).await;
    assert_eq!(
        store_database(&db)
            .activated_circle_ack(circle_id, device_id)
            .await
            .unwrap(),
        None
    );
    drop(device);
    drop(db);

    let reopened = open(&path, "circle-ack-restart");
    let reopened_device = TestDevice::load(&reopened, storage.clone(), signer.clone())
        .await
        .expect("bind reopened Circle acknowledgement Store");
    assert_eq!(drain(&reopened_device).await.unwrap(), 1);
    let device_id = local_device_id(&reopened).await;
    let reference = store_database(&reopened)
        .activated_circle_ack(circle_id, device_id)
        .await
        .unwrap()
        .expect("resumed drain activates the Circle acknowledgement exactly once");
    assert_eq!(reference.circle_id, circle_id);
    assert_eq!(reference.sequence, 1);
    // A repeat drain is a no-op: nothing remains queued.
    assert_eq!(drain(&reopened_device).await.unwrap(), 0);
}

#[tokio::test]
async fn circle_acknowledgement_slot_collision_fails_loud() {
    let directory = tempfile::tempdir().expect("acknowledgement database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(&path, "circle-ack-collision");
    let device = initialize(&db, &storage, &signer).await;
    db.call(|conn| {
        Ok(crate::sync::test_helpers::install_test_active_circle(
            conn,
            "collision-circle",
        ))
    })
    .await
    .expect("install active Circle");

    let frontier = current_frontier(&db).await;
    stage(&device).await;
    device
        .stage_circle_acknowledgements(&frontier, "2026-07-16T00:00:00Z")
        .await
        .expect("stage Circle acknowledgements");

    // Read the exact slot the staged Circle acknowledgement reserved, then occupy
    // it with different bytes before the drain uploads its object.
    let prepared_object: String = db
        .call(|conn| {
            conn.query_row(
                "SELECT prepared_object FROM outbound_circle_acks",
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(crate::database::DbError::from)
        })
        .await
        .expect("read staged Circle acknowledgement object");
    let prepared: crate::sync::storage::PreparedExactObject =
        serde_json::from_str(&prepared_object).expect("parse staged Circle acknowledgement object");
    let sabotage = b"different bytes at the reserved Circle acknowledgement slot".to_vec();
    let sabotage_ref = crate::sync::storage::ExactObjectRef::new(
        prepared.reference().slot().clone(),
        sabotage.len() as u64,
        crate::sync::store_commit::ObjectHash::digest(&sabotage),
    );
    assert_ne!(&sabotage_ref, prepared.reference());
    let sabotage_prepared = crate::sync::storage::PreparedExactObject::new(sabotage_ref, sabotage)
        .expect("build sabotage object");
    storage
        .create_protocol_object(&sabotage_prepared)
        .await
        .expect("occupy the reserved Circle acknowledgement slot");

    // Create-once refuses the different bytes: the drain fails loud rather than
    // silently adopting a foreign object on this device's per-Circle stream.
    let result = drain(&device).await;
    assert!(
        matches!(result, Err(StoreAckError::InvalidOutbound(_))),
        "unexpected drain outcome: {result:?}"
    );
}
