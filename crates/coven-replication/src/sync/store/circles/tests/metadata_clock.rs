use super::*;
use coven_foundation::clock::{ClockRef, FixedClock};
use coven_foundation::store_dir::StoreDir;
use coven_protocol::hlc::Hlc;

fn open_clock_database(
    path: &std::path::Path,
    directory: StoreDir,
    host_id: String,
    clock: ClockRef,
) -> (Database, Arc<Hlc>) {
    let hlc = Arc::new(Hlc::new(host_id, clock));
    let database = Database::open_synthetic_with_hlc_for_test(
        path,
        directory,
        test_synced_tables(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        hlc.clone(),
        &test_migrations(),
    )
    .expect("open the metadata clock database");
    (database, hlc)
}

#[tokio::test]
async fn metadata_only_pull_advances_the_clock_before_a_local_rename() {
    rename_after_metadata_pull(PullClockCase::Live).await;
}

#[tokio::test]
async fn metadata_only_pull_preserves_the_clock_floor_after_reopen() {
    rename_after_metadata_pull(PullClockCase::Reopen).await;
}

#[tokio::test]
async fn metadata_only_pull_rolls_back_the_clock_with_the_metadata() {
    rename_after_metadata_pull(PullClockCase::FailedPull).await;
}

#[tokio::test]
async fn metadata_only_pull_rejects_gross_future_metadata_atomically() {
    rename_after_metadata_pull(PullClockCase::GrossFuture).await;
}

#[tokio::test]
async fn metadata_only_outbound_rejects_malformed_metadata_before_publication() {
    rename_after_metadata_pull(PullClockCase::Malformed).await;
}

enum PullClockCase {
    Live,
    Reopen,
    FailedPull,
    GrossFuture,
    Malformed,
}

async fn rename_after_metadata_pull(case: PullClockCase) {
    let reopen = matches!(case, PullClockCase::Reopen);
    let name = "circle-metadata-clock";
    let owner_clock: ClockRef = Arc::new(FixedClock(
        "2026-07-25T12:00:00Z".parse().expect("owner clock"),
    ));
    let receiver_clock: ClockRef = Arc::new(FixedClock(
        "2026-07-24T12:00:00Z".parse().expect("receiver clock"),
    ));
    let owner_directory = crate::sync::test_helpers::test_store_dir();
    let (owner_database, _) = open_clock_database(
        std::path::Path::new(":memory:"),
        owner_directory.clone(),
        "metadata-owner".to_string(),
        owner_clock,
    );
    let (store, _home, identity, founder) =
        persist_merge_operation(&owner_database, owner_directory.clone(), name).await;
    let circle = founder.circle_id();
    let owner = store
        .bind_device_in(&owner_database, owner_directory.clone(), &identity)
        .await
        .expect("bind Circle owner");
    owner
        .resume_circle_operations()
        .await
        .expect("publish founder Circle");

    let receiver_directory = crate::sync::test_helpers::test_store_dir();
    let (mut receiver_database, receiver_hlc) = open_clock_database(
        std::path::Path::new(":memory:"),
        receiver_directory.clone(),
        "metadata-receiver".to_string(),
        receiver_clock.clone(),
    );
    let mut receiver = store
        .activate_joined_device_with_clock(
            &owner_database,
            owner_directory,
            &receiver_database,
            receiver_directory.clone(),
            &identity,
            "2026-07-24T12:00:00Z",
            receiver_clock.clone(),
        )
        .await
        .expect("join another device for the Circle owner");
    let owner_stamp = match case {
        PullClockCase::GrossFuture => {
            let mut stamp =
                coven_protocol::hlc::Timestamp::parse(&StoreDatabase::new(&owner_database).stamp())
                    .expect("parse owner clock");
            stamp.millis += coven_protocol::hlc::MAX_FUTURE_SKEW_MS + 1;
            stamp.to_string()
        }
        PullClockCase::Malformed => "zz-invalid-clock".to_string(),
        _ => StoreDatabase::new(&owner_database).stamp(),
    };
    if matches!(case, PullClockCase::Malformed) {
        let source = StoreDatabase::new(&owner_database);
        let previous = source
            .store_current_publication()
            .await
            .expect("read publication before invalid rename");
        owner
            .rename_circle(&owner_stamp, circle, "Invalid rename")
            .await
            .expect_err("malformed metadata cannot become a published control");
        let after = source
            .store_current_publication()
            .await
            .expect("read publication after invalid rename");
        assert_eq!(after.record(), previous.record());
        return;
    }
    assert!(
        receiver_hlc.high_water().to_string() < owner_stamp,
        "the join must preserve the receiver clock before observing the later rename"
    );
    owner
        .rename_circle(&owner_stamp, circle, "Name from the clock ahead")
        .await
        .expect("publish metadata without changing any host rows");
    if matches!(case, PullClockCase::GrossFuture) {
        let receiver_store = StoreDatabase::new(&receiver_database);
        let before = receiver_store
            .circle_authoring_context(circle, &keys::public_key_hex(&identity))
            .await
            .expect("read the state before invalid metadata");
        let floor_before = receiver_store
            .get_protocol_state(coven_protocol::hlc::HIGHWATER_STATE_KEY)
            .await
            .expect("read the clock before invalid metadata");
        let clock_before = receiver_hlc.high_water();
        let error = receiver
            .pull_store()
            .await
            .expect_err("inadmissible metadata must fail the pull");
        assert!(error.to_string().contains("metadata stamp"), "{error}");
        let after = receiver_store
            .circle_authoring_context(circle, &keys::public_key_hex(&identity))
            .await
            .expect("read metadata after failed pull");
        assert_eq!(after.0.metadata, before.0.metadata);
        assert_eq!(after.0.control, before.0.control);
        assert_eq!(after.1, before.1);
        assert_eq!(
            receiver_store
                .get_protocol_state(coven_protocol::hlc::HIGHWATER_STATE_KEY)
                .await
                .expect("read persisted clock after failed pull"),
            floor_before
        );
        assert_eq!(
            receiver_hlc.high_water(),
            clock_before,
            "invalid metadata changed the clock"
        );
        return;
    }
    if matches!(case, PullClockCase::FailedPull) {
        let receiver_store = StoreDatabase::new(&receiver_database);
        let floor_before = receiver_store
            .get_protocol_state(coven_protocol::hlc::HIGHWATER_STATE_KEY)
            .await
            .expect("read the clock floor before failed acceptance");
        receiver_database.fail_next_merge_materialization_at(
            coven_database::MergeMaterializationFailurePoint::ProjectionReplacement,
        );
        let clock_before = receiver_hlc.high_water();
        let error = receiver
            .pull_store()
            .await
            .expect_err("injected pull must fail");
        assert!(error.to_string().contains("injected failure"), "{error}");
        assert_eq!(
            receiver_store
                .get_protocol_state(coven_protocol::hlc::HIGHWATER_STATE_KEY)
                .await
                .expect("read the rolled-back clock floor"),
            floor_before,
        );
        assert_eq!(
            receiver_hlc.high_water(),
            clock_before,
            "failed acceptance advanced the in-memory clock"
        );
    }
    let (_, pulled) = receiver
        .pull_store()
        .await
        .expect("pull the peer's metadata-only change");
    assert!(pulled.held_positions.is_empty(), "{pulled:?}");
    let (observed, _) = StoreDatabase::new(&receiver_database)
        .circle_authoring_context(circle, &keys::public_key_hex(&identity))
        .await
        .expect("read accepted peer metadata");
    assert_eq!(observed.metadata.metadata_stamp, owner_stamp);
    assert_eq!(observed.metadata.name, "Name from the clock ahead");

    let reopened_directory = tempfile::tempdir().expect("create reopened database directory");
    if reopen {
        // The join fixture's receiving connection is in memory. Save that exact
        // accepted state before closing it; no sync cycle flushes the clock.
        let database_path = reopened_directory.path().join("receiver.sqlite3");
        let host_id = StoreDatabase::new(&receiver_database)
            .get_protocol_state("host_device_id")
            .await
            .expect("read the installed host identity")
            .expect("the installed database pins its host identity");
        receiver_database
            .vacuum_into_for_test(database_path.to_string_lossy().into_owned())
            .await
            .expect("save the accepted receiving database");
        drop(receiver);
        std::thread::spawn(move || drop(receiver_database))
            .join()
            .expect("close the receiving database");
        (receiver_database, _) = open_clock_database(
            &database_path,
            receiver_directory.clone(),
            host_id,
            receiver_clock,
        );
        receiver = store
            .bind_device_in(&receiver_database, receiver_directory, &identity)
            .await
            .expect("bind the reopened receiving device");
    }

    let receiver_stamp = StoreDatabase::new(&receiver_database).stamp();
    assert!(
        receiver_stamp > owner_stamp,
        "a local rename must follow accepted metadata even with a slower wall clock: \
         receiver={receiver_stamp}, accepted={owner_stamp}, reopened={reopen}"
    );
    receiver
        .rename_circle(&receiver_stamp, circle, "Name edited after the pull")
        .await
        .expect("publish the later local rename");
    let (renamed, _) = StoreDatabase::new(&receiver_database)
        .circle_authoring_context(circle, &keys::public_key_hex(&identity))
        .await
        .expect("read the renamed Circle");
    assert_eq!(renamed.metadata.name, "Name edited after the pull");
    assert_eq!(renamed.metadata.metadata_stamp, receiver_stamp);
}
