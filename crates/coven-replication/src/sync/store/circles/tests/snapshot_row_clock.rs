use super::*;
use coven_foundation::clock::{ClockRef, FixedClock};

#[tokio::test]
async fn device_join_clock_follows_rows_restored_only_from_a_circle_snapshot() {
    let clock: ClockRef = Arc::new(FixedClock(
        "2026-07-24T12:00:00Z".parse().expect("receiver wall clock"),
    ));
    let tables = vec![coven_protocol::synced_schema::SyncedTable::new(
        "documents",
        coven_protocol::synced_schema::RowIdentity::IndependentUuid,
    )
    .scoped_by("audience")];
    let migrations = vec![coven_database::Migration::sql(
        1,
        "Circle snapshot row clock",
        "CREATE TABLE documents (
            id TEXT PRIMARY KEY,
            audience TEXT,
            _updated_at TEXT NOT NULL
        ) STRICT;",
    )];
    let source_dir = crate::sync::test_helpers::test_store_dir();
    let source = Database::open_synthetic_for_test(
        std::path::Path::new(":memory:"),
        source_dir.clone(),
        tables.clone(),
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "snapshot-row-clock-owner".to_string(),
        clock.clone(),
        &migrations,
    )
    .expect("open the source database");
    let ((store, connection), home, identity, founder) =
        persist_merge_operation_fixture(&source, source_dir.clone(), "snapshot-row-clock").await;
    let circle = founder.circle_id();
    let owner = store
        .bind_device_in(&source, source_dir.clone(), &identity)
        .await
        .expect("bind the Circle owner");
    owner
        .resume_circle_operations()
        .await
        .expect("publish the founder Circle");
    let components = prepare_owner_sync_components(
        &source,
        &store,
        &home,
        &source_dir,
        &identity,
        "snapshot-row-clock",
        circle_test_custody(),
    )
    .await;
    let row_id = "00000000-0000-4000-8000-000000000001";
    let row_stamp = coven_protocol::hlc::Timestamp::new(
        "2026-07-25T12:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .expect("row clock ahead of receiver")
            .timestamp_millis()
            .try_into()
            .expect("positive row clock"),
        0,
        "snapshot-row-author".to_string(),
    )
    .to_string();
    source
        .capture_circle_document_for_test(row_id, circle, &row_stamp)
        .await
        .expect("capture the Circle row with an honest clock ahead");
    components
        .run_cycle(
            clock.as_ref(),
            None,
            coven_foundation::config::Config::DEFAULT_SNAPSHOT_COMMIT_THRESHOLD,
        )
        .await
        .expect("publish the Circle row");
    let routing = EncryptionService::from_key([42; 32]);
    let mut writer = owner
        .authorize_writer()
        .await
        .expect("authorize the snapshot publisher");
    let cut = writer
        .snapshots()
        .capture_snapshot_cut(Some(&routing))
        .await
        .expect("capture the Store and Circle snapshots");
    writer
        .snapshots()
        .push_snapshot_cut(cut, "2026-07-24T12:00:00Z".to_string())
        .await
        .expect("publish snapshots covering the Circle row");
    drop(writer);
    owner
        .ensure_device_join_snapshot_for_test()
        .await
        .expect("acknowledge the snapshot before joining");

    let pending_dir = tempfile::tempdir().expect("create the pending join directory");
    let pending = crate::sync::store::DeviceJoinJournalDatabase::open_for_test(
        pending_dir.path().join("pending.sqlite"),
    )
    .expect("open the pending join journal");
    let offer = owner
        .begin_device_join(&keys::public_key_hex(&identity))
        .await
        .expect("offer a second device for the owner identity");
    let mut pending_join = owner
        .open_pending_device_join_for_test(&pending, &identity, offer)
        .await
        .expect("open the joining authority");
    let request = pending_join
        .prepare_provider_access_request()
        .await
        .expect("request provider access");
    let approval = owner
        .authorize_device_provider_access(request, None)
        .await
        .expect("approve the same provider principal");
    let request = pending_join
        .prepare_registration_request(approval)
        .await
        .expect("request device registration");
    let join = owner
        .activate_same_principal_join_for_test(request)
        .await
        .expect("publish the joining device activation");
    assert!(!join.installation.bootstrap.commits.is_empty());
    for retained in &join.installation.bootstrap.commits {
        let commit: StoreBatchCommit = serde_json::from_slice(&retained.canonical_commit)
            .expect("parse the carried tail commit");
        assert!(commit.store_package().is_none());
        assert!(commit.circle_packages().is_empty());
    }
    drop(pending_join);
    let joining_dir = crate::sync::test_helpers::test_store_dir();
    let storage: Arc<dyn CloudSyncObjectStorage> = connection;
    let progress: crate::sync::JoiningDeviceJoinProgressObserver = Arc::new(|_| {});
    let cancel = tokio::sync::watch::channel(false).1;
    let device_id = join
        .bootstrap
        .bootstrap
        .request
        .expected_registration()
        .device_id
        .to_string();
    let history = crate::sync::store::HistoryConstructionAuthority::admission()
        .open_pinned(storage.as_ref(), &join.installation.authority.store_root)
        .await
        .expect("pin the joining Store root");
    let membership = history
        .load_accepted_membership_authority(
            &join.installation.bootstrap.membership.0,
            Some(&keys::public_key_hex(&identity)),
        )
        .await
        .expect("verify the accepted joining registration");
    let prepared = crate::sync::store::PreparedDeviceJoinSnapshot::prepare(
        &storage,
        (*join.installation).clone(),
        &membership,
        source.schema_version(),
        &joining_dir.db_path(),
        &progress,
        &cancel,
    )
    .await
    .expect("download and verify the Store snapshot");
    let installed = prepared
        .install(
            tables,
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            device_id,
            clock,
            &migrations,
            coven_database::CovenMigrationPolicy::ApplyPending,
            &routing,
        )
        .expect("install the Store image using the receiver clock");
    installed
        .complete_and_assert_circle_snapshot_clock_for_test(
            &pending,
            &storage,
            &joining_dir,
            &identity,
            join,
            "2026-07-24T12:00:00Z",
            &routing,
            row_id,
            &row_stamp,
        )
        .await;
}
