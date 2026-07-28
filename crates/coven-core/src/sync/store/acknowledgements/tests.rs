use std::path::Path;
use std::sync::Arc;

use super::*;
use crate::storage::cloud::test_utils::InMemoryCloudHome;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::store_commit::CommitFrontier;

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

fn storage(home: &InMemoryCloudHome, signer: &UserKeypair) -> CloudSyncStorage {
    CloudSyncStorage::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "ack-exact-store",
        signer.clone(),
    )
    .expect("construct acknowledgement test storage")
}

async fn history_verifier<'a>(
    database: &StoreDatabase,
    storage: &'a CloudSyncStorage,
) -> crate::sync::store::pull::MergeHistoryVerifier<'a> {
    let root = database
        .local_store_root_ref()
        .await
        .expect("read acknowledgement test root")
        .expect("acknowledgement test Store root");
    crate::sync::store::pull::MergeHistoryVerifier::new(storage, &root)
        .await
        .expect("verify acknowledgement test history")
}

async fn initialize(db: &Database, storage: &CloudSyncStorage, signer: &UserKeypair) {
    crate::sync::store::protocol_root::create_store(
        &store_database(db),
        storage,
        "ack-exact-store",
        signer,
    )
    .await
    .expect("create acknowledgement test Store");
    crate::sync::store::ensure_active_registration(&StoreDatabase::new(db), storage)
        .await
        .expect("activate acknowledgement test registration");
}

async fn stage(db: &Database, storage: &CloudSyncStorage, signer: &UserKeypair) -> StoreAck {
    stage_at(db, storage, signer, "2026-07-16T00:00:00Z").await
}

async fn stage_at(
    db: &Database,
    storage: &CloudSyncStorage,
    signer: &UserKeypair,
    last_sync: &str,
) -> StoreAck {
    let frontier = CommitFrontier::from_refs(
        crate::sync::store::database::StoreDatabase::new(db)
            .materialized_frontier()
            .await
            .expect("read acknowledgement frontier"),
    )
    .expect("shape acknowledgement frontier");
    crate::sync::store::stage_store_acknowledgement_for_test(
        db,
        storage,
        frontier,
        last_sync.to_string(),
        signer,
    )
    .await
    .expect("stage exact acknowledgement")
}

fn drain<'a>(
    db: &'a Database,
    storage: &'a CloudSyncStorage,
    signer: &'a UserKeypair,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, StoreAckError>> + 'a>> {
    Box::pin(async move {
        crate::sync::store::drain_store_acknowledgements_for_test(db, storage, signer).await
    })
}

async fn persist_candidate(
    db: &Database,
    storage: &CloudSyncStorage,
    signer: &UserKeypair,
    outbound: &crate::database::OutboundStoreAck,
) -> crate::sync::store::operations::PreparedStoreOperationCommit {
    let database = StoreDatabase::new(db);
    let membership = crate::sync::store::pull::load_cycle_membership(storage, &store_database(db))
        .await
        .expect("load acknowledgement test membership");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .unwrap()
        .expect("local Store device id");
    let mut history_verifier = history_verifier(&database, storage).await;
    let plan = crate::sync::store::operations::prepare_plan(
        &database,
        &mut history_verifier,
        &membership,
        &device_id,
        signer,
    )
    .await
    .expect("prepare acknowledgement activation");
    plan.common()
        .validate_acknowledgement(&outbound.ack.value)
        .expect("acknowledgement matches activation predecessor");
    let candidate = crate::sync::store::operations::prepare_candidate(
        &database,
        storage,
        plan,
        crate::sync::store::operations::StoreOperationBatch::Acknowledgement {
            reference: outbound.reference.clone(),
            value: outbound.ack.value.clone(),
            circle_acknowledgements: Vec::new(),
        },
    )
    .await
    .expect("prepare acknowledgement candidate");
    crate::sync::store::prepare_store_acknowledgement_activation_for_test(
        db,
        outbound.reference.clone(),
        candidate.clone(),
    )
    .await
    .expect("persist acknowledgement candidate");
    candidate
}

struct LosingAckFixture {
    home: InMemoryCloudHome,
    signer: UserKeypair,
    storage: CloudSyncStorage,
    db: Database,
    outbound: crate::database::OutboundStoreAck,
    losing: crate::sync::store::operations::PreparedStoreOperationCommit,
}

async fn losing_ack_fixture(path: &Path) -> LosingAckFixture {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(path, "ack-loser-device");
    let database = StoreDatabase::new(&db);
    Box::pin(initialize(&db, &storage, &signer)).await;
    Box::pin(stage(&db, &storage, &signer)).await;
    let outbound = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .expect("staged acknowledgement exists");
    let losing = Box::pin(persist_candidate(&db, &storage, &signer, &outbound)).await;
    let membership = Box::pin(crate::sync::store::pull::load_cycle_membership(
        &storage,
        &store_database(&db),
    ))
    .await
    .expect("load acknowledgement test membership");
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .unwrap()
        .expect("local Store device id");
    let mut history_verifier = history_verifier(&database, &storage).await;
    let competing_plan = Box::pin(crate::sync::store::operations::prepare_plan(
        &database,
        &mut history_verifier,
        &membership,
        &device_id,
        &signer,
    ))
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
    let competing = Box::pin(crate::sync::store::operations::prepare_candidate(
        &database,
        &storage,
        competing_plan,
        crate::sync::store::operations::StoreOperationBatch::ProviderAccessGrant(grant),
    ))
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
    initialize(&db, &storage, &signer).await;
    let founder_ack = store_database(&db)
        .latest_local_store_ack()
        .await
        .expect("read founder acknowledgement")
        .expect("Store creation publishes its founder acknowledgement");
    let ack = stage(&db, &storage, &signer).await;
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
    drop(db);

    let reopened = open(&path, "ack-test-device");
    home.fail_exact_create_after_call(1);
    assert_eq!(
        drain(&reopened, &storage, &signer)
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
    initialize(&db, &storage, &signer).await;
    let founder_ack = store_database(&db)
        .latest_local_store_ack()
        .await
        .expect("read founder acknowledgement")
        .expect("Store creation publishes its founder acknowledgement");
    stage(&db, &storage, &signer).await;
    let pending = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .expect("read acknowledgement outbox")
        .expect("staged acknowledgement exists");
    let slot = pending.reference.object.slot().clone();
    home.insert_exact_object(slot.logical_key(), b"competing bytes".to_vec());

    assert!(drain(&db, &storage, &signer).await.is_err());
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
    initialize(&seed, &storage, &signer).await;
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
    drop(seed);

    let winner_db = open(&winner_path, "ack-slot-race-device");
    stage_at(&winner_db, &storage, &signer, "2026-07-16T00:00:01Z").await;
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
    stage_at(&loser_db, &storage, &signer, "2026-07-16T00:00:02Z").await;
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
    let losing_candidate = persist_candidate(&loser_db, &storage, &signer, &loser).await;
    let losing_object_ids = losing_candidate
        .acknowledgement_remote_objects(&loser.ack)
        .expect("load losing acknowledgement candidate graph")
        .into_iter()
        .map(|remote| remote.object_id())
        .collect::<Vec<_>>();

    let result = drain(&loser_db, &storage, &signer).await;
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
    initialize(&db, &storage, &signer).await;
    let first = stage(&db, &storage, &signer).await;
    drain(&db, &storage, &signer)
        .await
        .expect("publish first acknowledgement");
    let first_published = store_database(&db)
        .latest_local_store_ack()
        .await
        .expect("read first acknowledgement")
        .expect("first acknowledgement exists");
    let second = stage(&db, &storage, &signer).await;
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
    drain(&db, &storage, &signer)
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
    Box::pin(initialize(&db, &storage, &signer)).await;
    Box::pin(stage(&db, &storage, &signer)).await;
    let outbound = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .expect("staged acknowledgement exists");
    storage
        .create_protocol_object(&outbound.ack.prepared)
        .await
        .expect("publish acknowledgement object");
    let candidate = persist_candidate(&db, &storage, &signer, &outbound).await;
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
    let database = StoreDatabase::new(&db);
    let mut history_verifier = history_verifier(&database, &storage).await;
    crate::sync::store::operations::publish_prepared(
        &database,
        &mut history_verifier,
        Box::new(candidate),
        None,
        None,
    )
    .await
    .expect("activate acknowledgement commit");
    let activated_position = crate::sync::store::database::StoreDatabase::new(&db)
        .latest_local_store_position()
        .await
        .unwrap();
    drop(db);

    let reopened = open(&path, "ack-test-device");
    assert_eq!(drain(&reopened, &storage, &signer).await.unwrap(), 1);
    assert_eq!(
        crate::sync::store::database::StoreDatabase::new(&reopened)
            .latest_local_store_position()
            .await
            .unwrap(),
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
    initialize(&db, &storage, &signer).await;
    stage(&db, &storage, &signer).await;
    let outbound = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .expect("staged acknowledgement exists");
    let candidate = persist_candidate(&db, &storage, &signer, &outbound).await;
    let expected = candidate.reference.clone();
    drop(db);

    let reopened = open(&path, "ack-test-device");
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
    assert_eq!(drain(&reopened, &storage, &signer).await.unwrap(), 1);
    assert_eq!(
        crate::sync::store::database::StoreDatabase::new(&reopened)
            .latest_local_store_position()
            .await
            .unwrap(),
        Some(expected)
    );
}

#[tokio::test]
async fn uploaded_acknowledgement_accepts_its_sole_candidate_nonactivation() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(Path::new(":memory:"), "ack-nonactivation-device");
    initialize(&db, &storage, &signer).await;
    stage(&db, &storage, &signer).await;
    let outbound = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .expect("staged acknowledgement exists");
    let candidate = persist_candidate(&db, &storage, &signer, &outbound).await;
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
        signer,
        storage,
        db,
        outbound,
        losing,
    } = Box::pin(losing_ack_fixture(Path::new(":memory:"))).await;

    assert_eq!(
        drain(&db, &storage, &signer)
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
        outbound,
        losing,
    } = Box::pin(losing_ack_fixture(&path)).await;
    home.fail_exact_delete_on_call(1);

    assert!(drain(&db, &storage, &signer).await.is_err());
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
    drop(db);

    let reopened = open(&path, "ack-loser-device");
    assert_eq!(drain(&reopened, &storage, &signer).await.unwrap(), 1);
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
        signer,
        storage,
        db,
        outbound,
        losing,
    } = Box::pin(losing_ack_fixture(Path::new(":memory:"))).await;
    home.fail_exact_delete_on_call(1);
    assert!(Box::pin(drain(&db, &storage, &signer)).await.is_err());

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

    assert!(Box::pin(drain(&db, &storage, &signer)).await.is_err());
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
    initialize(&db, &storage, &signer).await;
    stage(&db, &storage, &signer).await;
    let outbound = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .expect("staged acknowledgement exists");
    let candidate = persist_candidate(&db, &storage, &signer, &outbound).await;
    let expected_head = candidate.head.clone();
    let expected_prepared = candidate.prepared_head.clone();
    let (root, registration_ref, _, device_signer) =
        crate::sync::store::operations::load_local_store_authority(
            &StoreDatabase::new(&db),
            &outbound.reference.registration.device_id.to_string(),
            &signer,
        )
        .await
        .expect("load local device signing authority");
    let alternate_next = crate::storage::cloud::ObjectSlot::opaque(
        expected_head.successor.next_slot.logical_key().to_string(),
        "alternate-next-slot".to_string(),
    )
    .expect("valid alternate successor slot");
    let alternate_head = crate::sync::store_commit::StoreDeviceHead::signed(
        root.store_root_hash,
        registration_ref,
        candidate.reference.clone(),
        expected_head.history_summary,
        crate::sync::store_commit::SuccessorLink {
            activation: expected_head.successor.activation,
            predecessor: expected_head.successor.predecessor.clone(),
            next_slot: alternate_next,
        },
        &device_signer,
    )
    .expect("sign alternate head");
    let head_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
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

    assert_eq!(drain(&db, &storage, &signer).await.unwrap(), 1);
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
    initialize(&db, &storage, &signer).await;
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
    stage(&db, &storage, &signer).await;
    crate::sync::store::stage_circle_acknowledgements_for_test(
        &db,
        &storage,
        &frontier,
        "2026-07-16T00:00:00Z",
        &signer,
    )
    .await
    .expect("stage Circle acknowledgements");
    assert_eq!(drain(&db, &storage, &signer).await.unwrap(), 1);

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
    initialize(&db, &storage, &signer).await;
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
    stage(&db, &storage, &signer).await;
    crate::sync::store::stage_circle_acknowledgements_for_test(
        &db,
        &storage,
        &frontier,
        "2026-07-16T00:00:00Z",
        &signer,
    )
    .await
    .expect("stage Circle acknowledgements");
    assert_eq!(drain(&db, &storage, &signer).await.unwrap(), 1);

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
    initialize(&db, &storage, &signer).await;
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
    stage(&db, &storage, &signer).await;
    crate::sync::store::stage_circle_acknowledgements_for_test(
        &db,
        &storage,
        &frontier,
        "2026-07-16T00:00:00Z",
        &signer,
    )
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
    drop(db);

    let reopened = open(&path, "circle-ack-restart");
    assert_eq!(drain(&reopened, &storage, &signer).await.unwrap(), 1);
    let device_id = local_device_id(&reopened).await;
    let reference = store_database(&reopened)
        .activated_circle_ack(circle_id, device_id)
        .await
        .unwrap()
        .expect("resumed drain activates the Circle acknowledgement exactly once");
    assert_eq!(reference.circle_id, circle_id);
    assert_eq!(reference.sequence, 1);
    // A repeat drain is a no-op: nothing remains queued.
    assert_eq!(drain(&reopened, &storage, &signer).await.unwrap(), 0);
}

#[tokio::test]
async fn circle_acknowledgement_slot_collision_fails_loud() {
    let directory = tempfile::tempdir().expect("acknowledgement database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let db = open(&path, "circle-ack-collision");
    initialize(&db, &storage, &signer).await;
    db.call(|conn| {
        Ok(crate::sync::test_helpers::install_test_active_circle(
            conn,
            "collision-circle",
        ))
    })
    .await
    .expect("install active Circle");

    let frontier = current_frontier(&db).await;
    stage(&db, &storage, &signer).await;
    crate::sync::store::stage_circle_acknowledgements_for_test(
        &db,
        &storage,
        &frontier,
        "2026-07-16T00:00:00Z",
        &signer,
    )
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
    let result = drain(&db, &storage, &signer).await;
    assert!(
        matches!(result, Err(StoreAckError::InvalidOutbound(_))),
        "unexpected drain outcome: {result:?}"
    );
}
