use super::*;
use crate::device_log_sync::tests::{group, sql, storage};
use crate::store_log_sync::tests::Device;
use coven_database::{DatabaseBuilder, RowIdentity, SyncedTable};
use coven_foundation::files::StoreLayout;
use coven_storage::test_utils::{MemoryRequest, MemoryStorage};

fn tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
}

fn migrations() -> Vec<coven_database::Migration> {
    vec![coven_database::Migration::sql(
        1,
        "notes",
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,body TEXT NOT NULL);",
    )]
}

fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Back up the whole closed store directory, as a device backup would, then
/// reopen the original installation with its app's tables.
pub(crate) async fn backup(
    device: &mut Device,
    storage: &Arc<MemoryStorage>,
    app: (Vec<SyncedTable>, Vec<coven_database::Migration>),
) -> tempfile::TempDir {
    device.db.close().await.unwrap();
    let copy = tempfile::tempdir().unwrap();
    copy_tree(device._temporary.path(), copy.path());
    device.reopen(storage.clone(), app.0, app.1).await;
    copy
}

/// Open a backup as a second live copy holding the same device id and keys.
pub(crate) async fn restored(
    original: &Device,
    backup: tempfile::TempDir,
    storage: &Arc<MemoryStorage>,
    app: (Vec<SyncedTable>, Vec<coven_database::Migration>),
) -> Device {
    let directory = StoreLayout::new(backup.path().into()).store_dir(&original.directory.id());
    let db = DatabaseBuilder::new(directory.clone())
        .synced_tables(app.0)
        .migrations(app.1)
        .clock(original.clock.clone())
        .open()
        .await
        .unwrap();
    let custody = Arc::new(
        KeySession::store(Arc::new(coven_crypto::custody::InMemoryCustody::new(
            original.custody.read().unwrap().unwrap(),
        )))
        .unwrap(),
    );
    let send = Arc::new(crate::SendGate::new(original.clock.clone()));
    let waits = Arc::new(
        crate::RetryWaits::open(db.clone(), original.clock.clone())
            .await
            .unwrap(),
    );
    let storage = Arc::new(
        storage
            .with_publication_gate(send.clone())
            .with_cooldown(waits.clone()),
    );
    let writes = crate::DeviceLogSync::new(
        storage.clone(),
        db.clone(),
        custody.clone(),
        original.identity.clone(),
        original.clock.clone(),
        send.clone(),
    );
    let sync = StoreLogSync::new(
        storage.clone(),
        db.clone(),
        custody.clone(),
        original.identity.clone(),
        original.clock.clone(),
        send.clone(),
        original.ids.clone(),
        directory.clone(),
        waits.clone(),
        crate::RestoreCodeCustody::Unkept,
    );
    Device {
        waits,
        storage,
        sync,
        writes,
        db,
        directory,
        custody,
        identity: original.identity.clone(),
        member: original.member.clone(),
        clock: original.clock.clone(),
        send,
        ids: original.ids.clone(),
        _temporary: backup,
    }
}

fn sends(trace: &[MemoryRequest]) -> usize {
    trace
        .iter()
        .filter(|r| matches!(r, MemoryRequest::Create(_) | MemoryRequest::Replace(_)))
        .count()
}

fn evidence(error: SyncError) -> crate::DeviceIdEvidence {
    match error {
        SyncError::DeviceIdInUse(evidence) => evidence,
        error => panic!("expected another copy using this id, got {error:?}"),
    }
}

/// Stopping records nothing: no reset decision, and the database still works.
async fn nothing_recorded(device: &Device) {
    assert_eq!(device.directory.device_reset().unwrap(), None);
    assert_eq!(device.db.device_reset().await.unwrap(), None);
    device.db.local_store_log().await.unwrap();
}

#[tokio::test]
async fn a_restored_backup_behind_storage_stops_before_sending_its_queue() {
    let storage = storage();
    let mut devices = group(storage.clone(), 1).await;
    let original = &mut devices[0];
    sql(&original.db, "INSERT INTO notes VALUES('first','t','b')").await;
    original.writes.upload_writes().await.unwrap();
    let copy = backup(original, &storage, (tables(), migrations())).await;
    original.catch_up().await;
    sql(&original.db, "INSERT INTO notes VALUES('second','t','b')").await;
    sql(&original.db, "INSERT INTO notes VALUES('third','t','b')").await;
    assert_eq!(original.writes.upload_writes().await.unwrap().len(), 2);
    let mut restored = restored(original, copy, &storage, (tables(), migrations())).await;
    // An edit made only in the restored copy reserves write number 2, which
    // storage already holds; storage's write 3 is beyond every reservation.
    sql(&restored.db, "INSERT INTO notes VALUES('restored','t','b')").await;
    let before = storage.request_trace().len();
    assert_eq!(
        evidence(restored.sync.sync_store_log().await.unwrap_err()),
        crate::DeviceIdEvidence::StorageAhead
    );
    assert!(restored.writes.upload_writes().await.is_err());
    assert_eq!(
        evidence(restored.sync.sync_store_log().await.unwrap_err()),
        crate::DeviceIdEvidence::StorageAhead
    );
    assert_eq!(sends(&storage.request_trace()[before..]), 0);
    nothing_recorded(&restored).await;
    // Unrelated reads still answer while sending is stopped.
    let titles: Vec<String> = restored
        .db
        .read(|sql| Ok(sql.query("SELECT id FROM notes ORDER BY id", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(titles, ["first", "restored"]);
    // Reopening finds the same evidence again; still nothing is sent.
    restored
        .reopen(storage.clone(), tables(), migrations())
        .await;
    let before = storage.request_trace().len();
    assert_eq!(
        evidence(restored.sync.sync_store_log().await.unwrap_err()),
        crate::DeviceIdEvidence::StorageAhead
    );
    assert_eq!(sends(&storage.request_trace()[before..]), 0);
    nothing_recorded(&restored).await;
}

#[tokio::test]
async fn a_backup_that_storage_has_not_passed_continues_as_the_same_device() {
    let storage = storage();
    let mut devices = group(storage.clone(), 1).await;
    let original = &mut devices[0];
    sql(&original.db, "INSERT INTO notes VALUES('queued','t','b')").await;
    // The queued write was backed up before any attempt; nothing newer is stored.
    let copy = backup(original, &storage, (tables(), migrations())).await;
    original.db.close().await.unwrap();
    let mut restored = restored(original, copy, &storage, (tables(), migrations())).await;
    restored.sync.sync_store_log().await.unwrap();
    assert_eq!(restored.writes.upload_writes().await.unwrap().len(), 1);
    assert_eq!(restored.directory.device_reset().unwrap(), None);
}

#[tokio::test]
async fn a_second_live_copy_that_loses_a_slot_stops() {
    let storage = storage();
    let mut devices = group(storage.clone(), 1).await;
    let original = &mut devices[0];
    let copy = backup(original, &storage, (tables(), migrations())).await;
    let mut second = restored(original, copy, &storage, (tables(), migrations())).await;
    original.catch_up().await;
    second.catch_up().await;
    sql(&original.db, "INSERT INTO notes VALUES('one','t','b')").await;
    sql(&second.db, "INSERT INTO notes VALUES('two','t','b')").await;
    assert_eq!(original.writes.upload_writes().await.unwrap().len(), 1);
    // Its attempt finds the path occupied by different bytes.
    assert_eq!(
        evidence(second.writes.upload_writes().await.unwrap_err()),
        crate::DeviceIdEvidence::DifferentBytes
    );
    let before = storage.request_trace().len();
    // The losing copy's later sends wait; the pass reports the same evidence.
    assert!(second.writes.upload_writes().await.is_err());
    assert_eq!(
        evidence(second.sync.sync_store_log().await.unwrap_err()),
        crate::DeviceIdEvidence::DifferentBytes
    );
    assert_eq!(sends(&storage.request_trace()[before..]), 0);
    nothing_recorded(&second).await;
    // Reopened, its first attempt meets the same occupied path again.
    second.reopen(storage.clone(), tables(), migrations()).await;
    second.catch_up().await;
    assert_eq!(
        evidence(second.writes.upload_writes().await.unwrap_err()),
        crate::DeviceIdEvidence::DifferentBytes
    );
    nothing_recorded(&second).await;
    // The winning copy's bytes stay, and it keeps syncing.
    original.sync.sync_store_log().await.unwrap();
}

#[tokio::test]
async fn reading_its_own_replacement_stops_the_old_copy() {
    let storage = storage();
    let mut devices = group(storage.clone(), 1).await;
    // A reset of the first copy registers itself under a fresh id, replacing
    // the first device's id.
    let mut replacement =
        crate::device_log_sync::tests::device_of(storage.clone(), 9, devices[0].member.clone())
            .await;
    replacement.sync.sync_store_log().await.unwrap();
    replacement
        .sync
        .make_and_upload_entry(coven_format::store_log::StoreChange::AddDevice {
            name: "replacement".into(),
            replaces: Some(coven_foundation::id_source::DeviceId(1)),
        })
        .await
        .unwrap();
    let old = &mut devices[0];
    let before = storage.request_trace().len();
    assert_eq!(
        evidence(old.sync.sync_store_log().await.unwrap_err()),
        crate::DeviceIdEvidence::Replaced
    );
    assert_eq!(sends(&storage.request_trace()[before..]), 0);
    nothing_recorded(old).await;
    // The applied replacement stops it again after reopening.
    old.reopen(storage.clone(), tables(), migrations()).await;
    let before = storage.request_trace().len();
    assert_eq!(
        evidence(old.sync.sync_store_log().await.unwrap_err()),
        crate::DeviceIdEvidence::Replaced
    );
    assert_eq!(sends(&storage.request_trace()[before..]), 0);
    nothing_recorded(old).await;
}

#[tokio::test]
async fn session_evidence_is_read_once_until_reconnecting() {
    let storage = storage();
    let mut devices = group(storage.clone(), 1).await;
    let device = &mut devices[0];
    device.send.invalidate();
    let listings = |trace: &[MemoryRequest]| {
        trace
            .iter()
            .filter(|r| {
                matches!(r, MemoryRequest::List(prefix)
                    if *prefix == coven_storage::ObjectPrefix::snapshots(device.directory.id()))
            })
            .count()
    };
    let before = storage.request_trace().len();
    device.catch_up().await;
    device.catch_up().await;
    assert_eq!(listings(&storage.request_trace()[before..]), 1);
    device.send.invalidate();
    let before = storage.request_trace().len();
    device.catch_up().await;
    assert_eq!(listings(&storage.request_trace()[before..]), 1);
}

/// Lean `IdentityCounters.Sequence` against the snapshot reservation the
/// check reads. Snapshot numbers have their own sequence: each preparation
/// reserves the next, operations started and finished between them reserve
/// none, and the reservation is that sequence's last number
/// (`key_copies_counted`, `operation_ids_hide_newer_snapshot`).
#[tokio::test]
#[ignore = "requires COVEN_IDENTITY_LEAN"]
async fn st_differential_snapshot_sequence() {
    use crate::operation_data::Data;
    use crate::snapshot_data::{SnapshotJob, SnapshotTask};
    use serde_json::json;
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for pattern in 0..1u32 << 5 {
        let storage = storage();
        let devices = group(storage.clone(), 1).await;
        let device = &devices[0];
        // Joining the group reserved no snapshot number.
        assert_eq!(
            device.db.identity_reservations().await.unwrap().snapshots,
            0
        );
        let mut steps = Vec::new();
        let mut last = 0;
        for bit in 0..5 {
            if pattern & (1 << bit) != 0 {
                last = device.db.reserve_snapshot_number().await.unwrap().get();
                steps.push(true);
                continue;
            }
            let data = Data::Snapshots(SnapshotTask {
                job: SnapshotJob::Reload {
                    scope: crate::snapshot_data::ReloadScope::All,
                    files: None,
                },
                temporary: Vec::new(),
            });
            let id = device
                .db
                .start_operation(data.new_operation("coven").unwrap())
                .await
                .unwrap();
            // A finished operation never lowers the reservation.
            if bit % 2 == 0 {
                device.db.finish_operation(id).await.unwrap();
            }
        }
        let reserved = device.db.identity_reservations().await.unwrap().snapshots;
        cases.push(json!({"check": "sequence", "last": 0, "snapshots": steps}));
        actual.push(json!({"last": last, "snapshot": reserved}));
    }
    assert_eq!(actual, crate::identity::tests::identity_runner(&cases));
}

/// T157's `operation_ids_hide_newer_snapshot` in Rust: operations a restored
/// backup finishes before its first check advance the operation sequence
/// past its newer self's snapshot, but reserve no snapshot number, so the
/// check still finds storage ahead (§10).
#[tokio::test]
async fn other_operations_never_hide_a_newer_own_snapshot() {
    for extra in [0u64, 10] {
        let storage = storage();
        let mut devices = group(storage.clone(), 1).await;
        let original = &mut devices[0];
        sql(&original.db, "INSERT INTO notes VALUES('first','t','b')").await;
        original.writes.upload_writes().await.unwrap();
        let copy = backup(original, &storage, (tables(), migrations())).await;
        original.catch_up().await;
        let snapshot = original
            .write_snapshot(coven_merge::Audience::Store)
            .await
            .unwrap();
        let mut restored = restored(original, copy, &storage, (tables(), migrations())).await;
        let mut last = 0;
        for _ in 0..extra {
            let data = crate::operation_data::Data::Snapshots(crate::snapshot_data::SnapshotTask {
                job: crate::snapshot_data::SnapshotJob::Reload {
                    scope: crate::snapshot_data::ReloadScope::All,
                    files: None,
                },
                temporary: Vec::new(),
            });
            let id = restored
                .db
                .start_operation(data.new_operation("coven").unwrap())
                .await
                .unwrap();
            restored.db.finish_operation(id).await.unwrap();
            last = id.0 as u64;
        }
        // Operation ids now pass the newer snapshot's number, but reserve none.
        assert_eq!(extra == 0, last < snapshot.number);
        let reserved = restored.db.identity_reservations().await.unwrap();
        assert!(
            reserved.snapshots < snapshot.number,
            "{extra}: {reserved:?}"
        );
        let before = storage.request_trace().len();
        assert_eq!(
            evidence(restored.sync.sync_store_log().await.unwrap_err()),
            crate::DeviceIdEvidence::StorageAhead,
            "{extra} other operations"
        );
        assert_eq!(sends(&storage.request_trace()[before..]), 0);
    }
}
