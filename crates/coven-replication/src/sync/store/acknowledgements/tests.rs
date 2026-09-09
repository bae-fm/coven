use std::path::Path;
use std::sync::Arc;

use super::*;
use crate::sync::test_helpers::TestDevice;
use coven_database::Database;
use coven_keys::keys::UserKeypair;
use coven_storage::cloud::test_utils::InMemoryCloudHome;
use coven_storage::{BlobPathScheme, CloudCipher, CloudSyncConnection};

pub(super) fn open(
    path: &Path,
    device_id: &str,
) -> (Database, coven_foundation::store_dir::StoreDir) {
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
    .expect("open acknowledgement test database");
    (database, store_dir)
}

pub(super) fn store_database(database: &Database) -> StoreDatabase {
    StoreDatabase::new(database)
}

fn storage(home: &InMemoryCloudHome, signer: &UserKeypair) -> Arc<CloudSyncConnection> {
    Arc::new(CloudSyncConnection::new(
        Arc::new(home.clone()),
        CloudCipher::Plaintext,
        BlobPathScheme::Plain,
        "ack-exact-store",
        signer.clone(),
    ))
}

async fn initialize(
    db: &Database,
    db_store_dir: coven_foundation::store_dir::StoreDir,
    storage: &Arc<CloudSyncConnection>,
    signer: &UserKeypair,
) -> TestDevice {
    TestDevice::create(
        db,
        db_store_dir.clone(),
        storage.clone(),
        "ack-exact-store",
        signer.clone(),
    )
    .await
    .expect("create acknowledgement test Store")
}

#[tokio::test]
async fn a_standing_acknowledgement_survives_reopening_without_another_publication() {
    let directory = tempfile::tempdir().expect("acknowledgement database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, store_dir) = open(&path, "standing-ack-owner");
    let device = initialize(&db, store_dir, &storage, &signer).await;
    device
        .stage_current_acknowledgement("2026-07-16T00:00:01Z")
        .await
        .expect("stage the initial statement");
    assert_eq!(device.drain_acknowledgements_exact().await.unwrap(), 1);
    let accepted = store_database(&db)
        .store_current_publication()
        .await
        .expect("read accepted acknowledgement boundary");
    let standing = store_database(&db)
        .latest_local_store_ack()
        .await
        .expect("read standing acknowledgement")
        .expect("acknowledgement was published");
    drop(device);
    drop(db);

    let (reopened, store_dir) = open(&path, "standing-ack-owner");
    let device = TestDevice::load(&reopened, store_dir, storage, signer)
        .await
        .expect("reopen the acknowledgement owner");
    assert!(
        device
            .stage_current_acknowledgement_if_new("2026-07-16T00:00:02Z")
            .await
            .expect("compare the persisted statement")
            .is_none(),
        "reopening does not create a new assertion"
    );
    assert_eq!(device.drain_acknowledgements_exact().await.unwrap(), 0);
    let database = store_database(&reopened);
    assert_eq!(
        database.store_current_publication().await.unwrap(),
        accepted
    );
    assert_eq!(
        database
            .latest_local_store_ack()
            .await
            .unwrap()
            .unwrap()
            .reference,
        standing.reference,
    );
}

mod publication_race_tests;

#[tokio::test]
async fn acknowledgement_upload_keeps_the_local_author_position_reserved() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, directory) = open(Path::new(":memory:"), "ack-authorship-owner");
    let device = initialize(&db, directory, &storage, &signer).await;
    device
        .stage_current_acknowledgement("2026-07-16T00:00:01Z")
        .await
        .unwrap();
    let (reached, release) = home.pause_after_exact_create_call(1);
    let mut drain = Box::pin(device.drain_acknowledgements_exact());
    tokio::select! {
        result = &mut drain => panic!("drain finished before the ACK upload pause: {result:?}"),
        () = reached.notified() => {}
    }
    let database = store_database(&db);
    assert!(database.active_store_publication().await.unwrap().is_some());
    assert!(
        futures_util::FutureExt::now_or_never(database.author_own_stream()).is_none(),
        "another local writer must wait while the acknowledgement owns its author position",
    );
    release.notify_one();
    assert_eq!(drain.await.unwrap(), 1);
    assert!(database.active_store_publication().await.unwrap().is_none());
    assert!(futures_util::FutureExt::now_or_never(database.author_own_stream()).is_some());
}

#[tokio::test]
async fn a_prepared_acknowledgement_cannot_complete_without_its_installed_activation() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(Path::new(":memory:"), "unaccepted-ack-device");
    let device = initialize(&db, db_store_dir, &storage, &signer).await;
    let database = store_database(&db);
    let previous = database
        .latest_local_store_ack()
        .await
        .expect("read previous acknowledgement")
        .expect("founder acknowledgement");
    device
        .stage_current_acknowledgement("2026-07-16T00:00:00Z")
        .await
        .expect("stage acknowledgement");
    let pending = database
        .oldest_outbound_store_ack()
        .await
        .expect("read outbox")
        .expect("pending acknowledgement");
    let candidate = device
        .prepare_acknowledgement_candidate_for_test(&pending)
        .await;
    let active = database
        .active_store_publication()
        .await
        .expect("read reserved publication");

    database
        .complete_outbound_store_ack(pending.reference.clone(), candidate.reference.clone())
        .await
        .expect_err("an unaccepted acknowledgement cannot complete");

    assert_eq!(
        database
            .active_store_publication()
            .await
            .expect("read retained reservation"),
        active
    );
    assert_eq!(
        database
            .oldest_outbound_store_ack()
            .await
            .expect("read retained outbox")
            .expect("outbox survives")
            .reference,
        pending.reference
    );
    assert_eq!(
        database
            .latest_local_store_ack()
            .await
            .expect("read unchanged acknowledgement")
            .expect("previous acknowledgement survives")
            .reference,
        previous.reference
    );
    assert_eq!(
        device
            .drain_acknowledgements_exact()
            .await
            .expect("publish after refused premature completion"),
        1
    );
}

#[tokio::test]
async fn staged_acknowledgement_reuses_its_exact_object_after_restart_and_lost_response() {
    let directory = tempfile::tempdir().expect("acknowledgement database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(&path, "ack-test-device");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    let founder_ack = store_database(&db)
        .latest_local_store_ack()
        .await
        .expect("read founder acknowledgement")
        .expect("Store creation publishes its founder acknowledgement");
    let ack = device
        .stage_current_acknowledgement("2026-07-16T00:00:00Z")
        .await
        .expect("stage exact acknowledgement");
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

    let (reopened, reopened_store_dir) = open(&path, "ack-test-device");
    let reopened_device = TestDevice::load(
        &reopened,
        reopened_store_dir.clone(),
        storage.clone(),
        signer.clone(),
    )
    .await
    .expect("bind reopened acknowledgement Store");
    home.fail_exact_create_after_call(1);
    assert_eq!(
        reopened_device
            .drain_acknowledgements_exact()
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
    let (db, db_store_dir) = open(Path::new(":memory:"), "ack-test-device");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    let founder_ack = store_database(&db)
        .latest_local_store_ack()
        .await
        .expect("read founder acknowledgement")
        .expect("Store creation publishes its founder acknowledgement");
    device
        .stage_current_acknowledgement("2026-07-16T00:00:00Z")
        .await
        .expect("stage exact acknowledgement");
    let pending = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .expect("read acknowledgement outbox")
        .expect("staged acknowledgement exists");
    let slot = pending.reference.object.slot().clone();
    home.insert_exact_object(slot.logical_key(), b"competing bytes".to_vec());

    assert!(device.drain_acknowledgements_exact().await.is_err());
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

#[tokio::test]
async fn valid_acknowledgement_slot_winner_is_adopted_and_activated() {
    acknowledgement_slot_winner_is_adopted(SlotWinnerState::Unaccepted, false).await;
}

#[tokio::test]
async fn accepted_acknowledgement_slot_winner_completes_the_losing_outbox() {
    acknowledgement_slot_winner_is_adopted(SlotWinnerState::Accepted, false).await;
}

#[tokio::test]
async fn acknowledgement_slot_winner_preserves_its_queued_circle_acknowledgement() {
    acknowledgement_slot_winner_is_adopted(SlotWinnerState::Unaccepted, true).await;
}

#[tokio::test]
async fn accepted_acknowledgement_slot_winner_leaves_unactivated_circle_work_queued() {
    acknowledgement_slot_winner_is_adopted(SlotWinnerState::Accepted, true).await;
}

#[tokio::test]
async fn created_acknowledgement_keeps_its_bytes_after_a_newer_snapshot() {
    acknowledgement_slot_winner_is_adopted(SlotWinnerState::CreatedBeforeSnapshot, false).await;
}

#[derive(Clone, Copy)]
enum SlotWinnerState {
    Unaccepted,
    Accepted,
    CreatedBeforeSnapshot,
}

async fn acknowledgement_slot_winner_is_adopted(state: SlotWinnerState, carries_circle: bool) {
    Box::pin(async {
        let directory = tempfile::tempdir().expect("acknowledgement database directory");
        let seed_path = directory.path().join("seed.sqlite3");
        let winner_path = directory.path().join("winner.sqlite3");
        let loser_path = directory.path().join("loser.sqlite3");
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = if carries_circle {
            Arc::new(CloudSyncConnection::new(
                Arc::new(home.clone()),
                CloudCipher::Encrypted(coven_keys::encryption::EncryptionService::from_key([42; 32])),
                BlobPathScheme::Hashed,
                "ack-exact-store",
                signer.clone(),
            ))
        } else {
            storage(&home, &signer)
        };
        let (seed, seed_store_dir) = open(&seed_path, "ack-slot-race-device");
        let seed_device = initialize(&seed, seed_store_dir.clone(), &storage, &signer).await;
        let circle_id = if carries_circle {
            Some(seed_device.create_circle("0000000001000-0000-owner", "Slot winner Circle")
                .await.expect("publish the Circle activation before copying the device"))
        } else {
            None
        };
        for destination in [&winner_path, &loser_path] {
            let destination = destination
                .to_str()
                .expect("temporary database path is UTF-8")
                .to_string();
            seed.vacuum_into_for_test(destination)
                .await
                .expect("copy acknowledgement database");
        }
        drop(seed_device);
        drop(seed);

        let (winner_db, winner_db_store_dir) = open(&winner_path, "ack-slot-race-device");
        let winner_device = TestDevice::load(
            &winner_db,
            winner_db_store_dir.clone(),
            storage.clone(),
            signer.clone(),
        )
        .await
        .expect("bind winner acknowledgement Store");
        winner_device
            .stage_current_acknowledgement("2026-07-16T00:00:01Z")
            .await
            .expect("stage exact acknowledgement");
        let winner = store_database(&winner_db)
            .oldest_outbound_store_ack()
            .await
            .expect("read winner acknowledgement")
            .expect("winner acknowledgement exists");
        storage
            .create_protocol_object(&winner.ack.prepared)
            .await
            .expect("publish winner acknowledgement");

        let (loser_db, loser_db_store_dir) = open(&loser_path, "ack-slot-race-device");
        let loser_device = TestDevice::load(
            &loser_db,
            loser_db_store_dir.clone(),
            storage.clone(),
            signer.clone(),
        )
        .await
        .expect("bind loser acknowledgement Store");
        if carries_circle {
            let frontier = loser_device.acknowledgement_frontier().await.unwrap();
            loser_device
                .stage_circle_acknowledgements(&frontier, "2026-07-16T00:00:02Z")
                .await
                .expect("queue the Circle statement before the Store candidate");
        }
        loser_device
            .stage_current_acknowledgement("2026-07-16T00:00:02Z")
            .await
            .expect("stage exact acknowledgement");
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
        let losing_candidate = loser_device
            .prepare_acknowledgement_candidate_for_test(&loser)
            .await;
        let losing_object_ids = losing_candidate
            .acknowledgement_remote_objects(&loser.ack)
            .expect("load losing acknowledgement candidate graph")
            .into_iter()
            .map(|remote| remote.object_id())
            .collect::<Vec<_>>();

        if matches!(state, SlotWinnerState::Accepted) {
            assert_eq!(
                winner_device.drain_acknowledgements_exact().await.unwrap(),
                1
            );
            loser_device
                .pull_store()
                .await
                .expect("install the accepted slot winner");
        }

        if matches!(state, SlotWinnerState::CreatedBeforeSnapshot) {
            let context = ProtocolObjectContext::signed_plaintext(
                winner.ack.value.store_root_hash, ProtocolObjectDomain::StoreAck,
            );
            let (bytes, prepared) = storage.read_prepared_protocol_slot(
                &context, winner.reference.object.slot(),
                &ack_slot_prefix(&winner.reference.registration.device_id.to_string(), winner.reference.sequence),
            ).await.expect("read the exact provider slot winner");
            store_database(&loser_db).adopt_outbound_store_ack_slot_winner(
                loser.reference.clone(), bytes, prepared,
            ).await.expect("persist the created winner before restarting publication");
            loser_db.execute_test_host_write(
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) VALUES \
                 ('after-created-ack', 'New accepted state', 1, '0000000003000-0000-owner', '2026-07-16')",
            ).await;
            assert!(loser_device.prepare_pending_store_write().await.unwrap());
            assert_eq!(loser_device.drain_store_writes().await.unwrap(), 1);
            let mut writer = loser_device.authorize_writer().await.unwrap();
            let mut snapshots = writer.snapshots();
            let routing = coven_keys::encryption::EncryptionService::from_key([42; 32]);
            let cut = snapshots.capture_snapshot_cut(Some(&routing)).await.unwrap();
            snapshots.push_snapshot_cut(cut, "2026-07-16T00:00:03Z".into()).await.unwrap();
        }

        let result = loser_device.drain_acknowledgements_exact().await;
        assert_eq!(
            result.expect("adopt and activate acknowledgement slot winner"),
            1
        );
        let published = store_database(&loser_db)
                .latest_local_store_ack()
                .await
                .expect("read adopted acknowledgement")
                .expect("adopted acknowledgement is published")
                .reference;
        if matches!(state, SlotWinnerState::CreatedBeforeSnapshot) {
            assert_eq!(published.sequence, winner.reference.sequence + 1);
            assert_eq!(published.object.slot(), &winner.ack.value.successor.next_slot);
            assert_eq!(home.get(winner.reference.object.slot().logical_key()), Some(winner.ack.bytes.clone()));
            let retained = loser_device.retained_merge_replay_inputs_for_test().await.unwrap();
            let proof = retained.iter().filter_map(|row| row.history_evidence().acknowledgement.as_ref())
                .find(|proof| proof.acknowledgement.0 == published)
                .expect("retain the successor's exact predecessor proof");
            assert_eq!(proof.predecessors, vec![(winner.reference.clone(), winner.ack.value.clone())]);
        } else {
            assert_eq!(published, winner.reference);
        }
        assert!(
            store_database(&loser_db)
                .oldest_outbound_store_ack()
                .await
                .expect("read drained acknowledgement outbox")
                .is_none()
        );
        assert!(
            coven_database::StoreDatabase::new(&loser_db)
                .protocol_inert_object(loser.reference.object)
                .await
                .expect("read losing acknowledgement inert state")
                .is_none()
        );
        for object_id in losing_object_ids {
            let exists = loser_db
                .remote_object_id_exists_for_test(object_id)
                .await
                .expect("read losing acknowledgement candidate ownership");
            assert!(!exists);
        }
        if let Some(circle_id) = circle_id {
            let database = store_database(&loser_db);
            if matches!(state, SlotWinnerState::Accepted) {
                assert!(database.outbound_circle_acks_pending().await.unwrap());
                assert!(
                    database
                        .activated_circle_ack(circle_id, loser_device.typed_device_id())
                        .await
                        .unwrap()
                        .is_none()
                );
                loser_device
                    .stage_current_acknowledgement("2026-07-16T00:00:03Z")
                    .await
                    .expect("stage the remaining Circle work");
                assert_eq!(
                    loser_device.drain_acknowledgements_exact().await.unwrap(),
                    1
                );
            }
            assert!(!database.outbound_circle_acks_pending().await.unwrap());
            assert!(
                database
                    .activated_circle_ack(circle_id, loser_device.typed_device_id())
                    .await
                    .unwrap()
                    .is_some()
            );
        }
    })
    .await;
}

#[tokio::test]
async fn acknowledgement_predecessor_and_reserved_successor_form_one_exact_chain() {
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(Path::new(":memory:"), "ack-test-device");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    let first = device
        .stage_current_acknowledgement("2026-07-16T00:00:00Z")
        .await
        .expect("stage exact acknowledgement");
    device
        .drain_acknowledgements_exact()
        .await
        .expect("publish first acknowledgement");
    let first_published = store_database(&db)
        .latest_local_store_ack()
        .await
        .expect("read first acknowledgement")
        .expect("first acknowledgement exists");
    // Something for the second acknowledgement to say. Without it the standing
    // one still holds — a device does not acknowledge its own acknowledgement —
    // and there would be no second link to check the chain against.
    db.execute_test_host_write(
        "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
         VALUES ('chain-1', 'chain', NULL, 1, '0000000001000-0000-chain', '2026-07-16')",
    )
    .await;
    assert!(device
        .prepare_pending_store_write()
        .await
        .expect("prepare the write the second acknowledgement covers"));
    assert_eq!(
        device
            .drain_store_writes()
            .await
            .expect("publish the write the second acknowledgement covers"),
        1
    );
    let second = device
        .stage_current_acknowledgement("2026-07-16T00:00:00Z")
        .await
        .expect("stage exact acknowledgement");
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
    device
        .drain_acknowledgements_exact()
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
    let (db, db_store_dir) = open(&path, "ack-test-device");
    let device = Box::pin(initialize(&db, db_store_dir.clone(), &storage, &signer)).await;
    Box::pin(device.stage_current_acknowledgement("2026-07-16T00:00:00Z"))
        .await
        .expect("stage exact acknowledgement");
    let outbound = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .expect("staged acknowledgement exists");
    storage
        .create_protocol_object(&outbound.ack.prepared)
        .await
        .expect("publish acknowledgement object");
    let candidate = device
        .prepare_acknowledgement_candidate_for_test(&outbound)
        .await;
    let acknowledgement_remote = candidate
        .acknowledgement_remote_objects(&outbound.ack)
        .expect("candidate owns acknowledgement")
        .into_iter()
        .find(|remote| remote.object() == &outbound.reference.object)
        .expect("acknowledgement remote object");
    coven_database::StoreDatabase::new(&db)
        .mark_remote_object_uploaded(acknowledgement_remote.into_record())
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

    let (reopened, reopened_store_dir) = open(&path, "ack-test-device");
    let reopened_store = TestDevice::load(
        &reopened,
        reopened_store_dir.clone(),
        storage.clone(),
        signer.clone(),
    )
    .await
    .expect("bind reopened acknowledgement Store");
    assert_eq!(
        reopened_store.drain_acknowledgements_exact().await.unwrap(),
        1
    );
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
    let (db, db_store_dir) = open(&path, "ack-test-device");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    device
        .stage_current_acknowledgement("2026-07-16T00:00:00Z")
        .await
        .expect("stage exact acknowledgement");
    let outbound = store_database(&db)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .expect("staged acknowledgement exists");
    let candidate = device
        .prepare_acknowledgement_candidate_for_test(&outbound)
        .await;
    let expected = candidate.reference.clone();
    drop(device);
    drop(db);

    let (reopened, reopened_store_dir) = open(&path, "ack-test-device");
    let reopened_store = TestDevice::load(
        &reopened,
        reopened_store_dir.clone(),
        storage.clone(),
        signer.clone(),
    )
    .await
    .expect("bind reopened acknowledgement Store");
    let resumed = store_database(&reopened)
        .oldest_outbound_store_ack()
        .await
        .unwrap()
        .expect("prepared acknowledgement exists after restart");
    assert!(matches!(
        resumed.activation,
        coven_database::OutboundStoreAckActivation::Prepared(ref prepared)
            if prepared.reference == expected
    ));
    assert_eq!(
        reopened_store.drain_acknowledgements_exact().await.unwrap(),
        1
    );
    assert_eq!(
        reopened_store.latest_local_store_position().await.unwrap(),
        Some(expected)
    );
}

#[tokio::test]
async fn circle_acknowledgement_publishes_activates_and_is_read_back() {
    let directory = tempfile::tempdir().expect("acknowledgement database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(&path, "circle-ack-device");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    let circle_id = store_database(&db)
        .install_test_active_circle("ack-circle".to_string())
        .await
        .expect("install active Circle");

    let frontier = device
        .acknowledgement_frontier()
        .await
        .expect("read frontier");
    device
        .stage_current_acknowledgement("2026-07-16T00:00:00Z")
        .await
        .expect("stage exact acknowledgement");
    device
        .stage_circle_acknowledgements(&frontier, "2026-07-16T00:00:00Z")
        .await
        .expect("stage Circle acknowledgements");
    assert_eq!(device.drain_acknowledgements_exact().await.unwrap(), 1);

    let device_id = device.typed_device_id();
    let reference = store_database(&db)
        .activated_circle_ack(circle_id, device_id)
        .await
        .expect("read activated Circle acknowledgement")
        .expect("Circle acknowledgement activated with the Store commit");
    assert_eq!(reference.circle_id, circle_id);
    assert_eq!(reference.sequence, 1);
    // Reading and verifying an activated Circle acknowledgement — including under
    // a rotated-away epoch and its exact seed coverage — is exercised on the
    // production close/two-device fixtures in circles::tests, where the
    // retained control activations the reader resolves the epoch key from exist.
}

#[tokio::test]
async fn inactive_circle_stages_no_acknowledgement() {
    let directory = tempfile::tempdir().expect("acknowledgement database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(&path, "inactive-circle-device");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    let circle_id = store_database(&db)
        .install_test_inactive_circle("inactive-circle".to_string())
        .await
        .expect("install inactive Circle");

    let frontier = device
        .acknowledgement_frontier()
        .await
        .expect("read frontier");
    device
        .stage_current_acknowledgement("2026-07-16T00:00:00Z")
        .await
        .expect("stage exact acknowledgement");
    device
        .stage_circle_acknowledgements(&frontier, "2026-07-16T00:00:00Z")
        .await
        .expect("stage Circle acknowledgements");
    assert_eq!(device.drain_acknowledgements_exact().await.unwrap(), 1);

    let device_id = device.typed_device_id();
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
    let (db, db_store_dir) = open(&path, "circle-ack-restart");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    let circle_id = store_database(&db)
        .install_test_active_circle("restart-circle".to_string())
        .await
        .expect("install active Circle");

    let frontier = device
        .acknowledgement_frontier()
        .await
        .expect("read frontier");
    device
        .stage_current_acknowledgement("2026-07-16T00:00:00Z")
        .await
        .expect("stage exact acknowledgement");
    device
        .stage_circle_acknowledgements(&frontier, "2026-07-16T00:00:00Z")
        .await
        .expect("stage Circle acknowledgements");
    // Crash between staging and draining: the outbound Circle acknowledgement is
    // durable and its object is not yet activated.
    let device_id = device.typed_device_id();
    assert_eq!(
        store_database(&db)
            .activated_circle_ack(circle_id, device_id)
            .await
            .unwrap(),
        None
    );
    drop(device);
    drop(db);

    let (reopened, reopened_store_dir) = open(&path, "circle-ack-restart");
    let reopened_device = TestDevice::load(
        &reopened,
        reopened_store_dir.clone(),
        storage.clone(),
        signer.clone(),
    )
    .await
    .expect("bind reopened Circle acknowledgement Store");
    assert_eq!(
        reopened_device
            .drain_acknowledgements_exact()
            .await
            .unwrap(),
        1
    );
    let device_id = reopened_device.typed_device_id();
    let reference = store_database(&reopened)
        .activated_circle_ack(circle_id, device_id)
        .await
        .unwrap()
        .expect("resumed drain activates the Circle acknowledgement exactly once");
    assert_eq!(reference.circle_id, circle_id);
    assert_eq!(reference.sequence, 1);
    // A repeat drain is a no-op: nothing remains queued.
    assert_eq!(
        reopened_device
            .drain_acknowledgements_exact()
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn circle_acknowledgement_slot_collision_fails_loud() {
    let directory = tempfile::tempdir().expect("acknowledgement database directory");
    let path = directory.path().join("store.sqlite3");
    let home = InMemoryCloudHome::new();
    let signer = UserKeypair::generate();
    let storage = storage(&home, &signer);
    let (db, db_store_dir) = open(&path, "circle-ack-collision");
    let device = initialize(&db, db_store_dir.clone(), &storage, &signer).await;
    store_database(&db)
        .install_test_active_circle("collision-circle".to_string())
        .await
        .expect("install active Circle");

    let frontier = device
        .acknowledgement_frontier()
        .await
        .expect("read frontier");
    device
        .stage_current_acknowledgement("2026-07-16T00:00:00Z")
        .await
        .expect("stage exact acknowledgement");
    device
        .stage_circle_acknowledgements(&frontier, "2026-07-16T00:00:00Z")
        .await
        .expect("stage Circle acknowledgements");

    // Read the exact slot the staged Circle acknowledgement reserved, then occupy
    // it with different bytes before the drain uploads its object.
    let prepared = db
        .staged_circle_acknowledgement_object_for_test()
        .await
        .expect("read staged Circle acknowledgement object");
    let sabotage = b"different bytes at the reserved Circle acknowledgement slot".to_vec();
    let sabotage_ref = coven_protocol::objects::ExactObjectRef::new(
        prepared.reference().slot().clone(),
        sabotage.len() as u64,
        coven_protocol::store_commit::ObjectHash::digest(&sabotage),
    );
    assert_ne!(&sabotage_ref, prepared.reference());
    let sabotage_prepared =
        coven_protocol::objects::PreparedExactObject::new(sabotage_ref, sabotage)
            .expect("build sabotage object");
    storage
        .create_protocol_object(&sabotage_prepared)
        .await
        .expect("occupy the reserved Circle acknowledgement slot");

    // Create-once refuses the different bytes: the drain fails loud rather than
    // silently adopting a foreign object on this device's per-Circle stream.
    let result = device.drain_acknowledgements_exact().await;
    assert!(
        matches!(result, Err(StoreAckError::InvalidOutbound(_))),
        "unexpected drain outcome: {result:?}"
    );
}
