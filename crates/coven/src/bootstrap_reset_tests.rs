use super::*;
use crate::tests::wait_for_sent;
use coven_storage::{
    test_utils::{MemoryRequest, MemoryStorage},
    Storage,
};
use std::time::{Duration, UNIX_EPOCH};

struct Fixture {
    app: TestCoven,
    handle: CovenHandle,
    storage: Arc<MemoryStorage>,
    clock: Arc<FixedClock>,
    directory: StoreDir,
    _root: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let app = TestCoven::new();
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
        let storage = Arc::new(
            MemoryStorage::builder()
                .clock(clock.clone())
                .transfer_limits(1024 * 1024, 64 * 1024)
                .build()
                .unwrap(),
        );
        let directory = app
            .create_closed_store(
                &StoreLayout::new(root.path().into()),
                "Reset",
                Arc::new(UuidIds),
            )
            .await
            .unwrap();
        let handle = builder(&app, &root, &clock, &storage)
            .open(directory.id())
            .await
            .unwrap();

        Self {
            app,
            handle,
            storage,
            clock,
            directory,
            _root: root,
        }
    }

    async fn setup(&self) -> Result<StorageSetup, StorageSetupError> {
        self.handle
            .setup_s3_storage(
                self.storage.config(),
                "Test device",
                "member-key".into(),
                SecretText::new("secret".into()),
            )
            .await
    }
}

fn builder(
    app: &TestCoven,
    root: &tempfile::TempDir,
    clock: &Arc<FixedClock>,
    storage: &Arc<MemoryStorage>,
) -> CovenBuilder {
    app.builder(StoreLayout::new(root.path().into()))
        .clock(clock.clone())
        .storage_connector(storage.clone())
        .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
        .migrations(vec![Migration::sql(
            1,
            "notes",
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,body TEXT NOT NULL)",
        )])
}

async fn status(handle: &CovenHandle, expected: impl Fn(&SyncStatus) -> bool) {
    let mut status = handle.subscribe_sync_status();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if expected(&status.borrow_and_update()) {
                return;
            }
            status.changed().await.expect("open loop status");
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out at {:?}", &*status.borrow()));
}

async fn uploaded(f: &Fixture, id: &'static str) {
    f.handle
        .write(move |sql| {
            sql.execute("INSERT INTO notes VALUES(?1,'uploaded')", [id])?;
            Ok(())
        })
        .await
        .unwrap();
    wait_for_sent(&f.handle).await;
}

async fn notes(handle: &CovenHandle) -> Vec<String> {
    handle
        .read(|sql| Ok(sql.query("SELECT id FROM notes ORDER BY id", [], |r| r.get(0))?))
        .await
        .unwrap()
}

fn reopen(f: &Fixture) -> CovenBuilder {
    builder(&f.app, &f._root, &f.clock, &f.storage)
}

/// What an approved reset records before loading, as if it stopped there.
fn interrupted_reset(f: &Fixture) -> DeviceReset {
    f.directory
        .record_device_reset(&DeviceReset {
            old_device: f.directory.settings().unwrap().device_id,
            new_device: UuidIds.new_device_id(),
        })
        .unwrap()
}

/// The installation's id in device-only custody, which backups do not copy.
fn custody(f: &Fixture) -> coven_crypto::custody::StoreKeychain {
    f.app.store_keychain(f.directory.id())
}

#[tokio::test]
async fn missing_custody_resets_as_a_new_device_and_keeps_the_notice() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    uploaded(&f, "kept").await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f.handle
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('unsent','local')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    f.handle.close().await.unwrap();
    let old = f.directory.settings().unwrap().device_id;
    custody(&f).delete_device_id().unwrap();
    let before = f.storage.request_count();
    // Opening refuses and changes nothing: no reset is recorded, nothing sent.
    for _ in 0..2 {
        assert!(matches!(
            reopen(&f).open(f.directory.id()).await,
            Err(CovenError::DeviceKeyMissing { store }) if store == f.directory.id()
        ));
    }
    assert_eq!(f.directory.device_reset().unwrap(), None);
    assert_eq!(f.directory.settings().unwrap().device_id, old);
    assert_eq!(f.storage.request_count(), before);
    // Calling reset_device is the app's approval.
    let handle = reopen(&f)
        .reset_device(f.directory.id(), "Restored phone")
        .await
        .unwrap();
    let reset = handle.device_reset().await.unwrap().unwrap();
    assert_eq!(reset.old_device, old);
    assert_ne!(reset.new_device, old);
    assert_eq!(f.directory.settings().unwrap().device_id, reset.new_device);
    assert_eq!(custody(&f).device_id().unwrap(), Some(reset.new_device));
    // Stored history loads; the unsent edit is discarded, not renumbered.
    assert_eq!(notes(&handle).await, ["kept"]);
    let devices: Vec<_> = handle
        .members()
        .await
        .unwrap()
        .into_iter()
        .flat_map(|member| member.devices)
        .map(|device| (device.id, device.state))
        .collect();
    // The registration names the old id it replaces; both ids stay listed (E9).
    assert!(devices.contains(&(reset.new_device, crate::DeviceState::Active)));
    assert!(devices.contains(&(reset.old_device, crate::DeviceState::Replaced)));
    let mut notices = handle.subscribe_device_reset();
    assert_eq!(notices.next().await.unwrap(), Some(reset));
    handle.close().await.unwrap();
    let handle = reopen(&f).open(f.directory.id()).await.unwrap();
    assert_eq!(handle.device_reset().await.unwrap(), Some(reset));
    handle.close().await.unwrap();
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

fn sends(f: &Fixture) -> usize {
    f.storage
        .request_trace()
        .iter()
        .filter(|r| matches!(r, MemoryRequest::Create(_) | MemoryRequest::Replace(_)))
        .count()
}

/// A backup restored after its device uploaded more: another copy used this
/// device's id. Sync stops sending and says why, recording nothing; the
/// database keeps working, reopening finds the same evidence, and calling
/// reset_device, the app's choice, starts a new device (§10).
#[tokio::test]
async fn a_restored_copy_behind_storage_stops_until_the_app_resets_it() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    uploaded(&f, "kept").await;
    f.handle.close().await.unwrap();
    let store = f.directory.database_path().parent().unwrap().to_owned();
    let backup = tempfile::tempdir().unwrap();
    copy_tree(&store, backup.path());
    let handle = reopen(&f).open(f.directory.id()).await.unwrap();
    // The first pass captures an empty queue before this write commits. Its
    // later completion must not let closing discard the write's upload.
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_read(ObjectPath::floor(f.directory.id()), entered, resumed)
        .await;
    handle.start_sync().await.unwrap();
    entering.await.unwrap();
    handle
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('newer','uploaded')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    resume.send(()).unwrap();
    wait_for_sent(&handle).await;
    assert!(handle.test_queued_writes().await.unwrap().is_empty());
    handle.close().await.unwrap();
    std::fs::remove_dir_all(&store).unwrap();
    copy_tree(backup.path(), &store);
    let old = f.directory.settings().unwrap().device_id;
    let before = sends(&f);
    for opening in 0..2 {
        let handle = reopen(&f).open(f.directory.id()).await.unwrap();
        handle.start_sync().await.unwrap();
        status(&handle, |s| {
            matches!(
                s,
                SyncStatus::Failed {
                    error: SyncFailure::DeviceIdInUse(DeviceIdEvidence::UnattemptedSlot { .. })
                }
            )
        })
        .await;
        // Reads and local writes still work; nothing is recorded.
        assert_eq!(notes(&handle).await, ["kept"]);
        assert_eq!(handle.device_reset().await.unwrap(), None);
        if opening == 1 {
            handle
                .write(|sql| {
                    sql.execute("INSERT INTO notes VALUES('unsent','local')", [])?;
                    Ok(())
                })
                .await
                .unwrap();
        }
        handle.close().await.unwrap();
        assert_eq!(f.directory.device_reset().unwrap(), None);
        assert_eq!(f.directory.settings().unwrap().device_id, old);
    }
    assert_eq!(sends(&f), before);
    let handle = reopen(&f)
        .reset_device(f.directory.id(), "Restored phone")
        .await
        .unwrap();
    let reset = handle.device_reset().await.unwrap().unwrap();
    assert_eq!(reset.old_device, old);
    assert_ne!(reset.new_device, old);
    assert_eq!(f.directory.settings().unwrap().device_id, reset.new_device);
    // Stored history loads; the copy's unsent edit is discarded.
    assert_eq!(notes(&handle).await, ["kept", "newer"]);
    handle.close().await.unwrap();
}

#[tokio::test]
async fn an_interrupted_reset_keeps_its_fresh_id_and_stays_unavailable() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    uploaded(&f, "kept").await;
    f.handle.close().await.unwrap();
    custody(&f).delete_device_id().unwrap();
    // An approved reset recorded its decision, then stopped.
    let decided = interrupted_reset(&f);
    f.storage.set_online(false);
    assert!(reopen(&f)
        .reset_device(f.directory.id(), "Restored phone")
        .await
        .is_err());
    assert!(matches!(
        reopen(&f).open(f.directory.id()).await,
        Err(CovenError::Lock(StoreLockError::BootstrapPending(_)))
    ));
    f.storage.set_online(true);
    let handle = reopen(&f)
        .reset_device(f.directory.id(), "Restored phone")
        .await
        .unwrap();
    assert_eq!(handle.device_reset().await.unwrap(), Some(decided));
    assert_eq!(notes(&handle).await, ["kept"]);
    handle.close().await.unwrap();
}

#[tokio::test]
async fn an_unconnected_store_without_a_decided_reset_keeps_its_files() {
    let f = Fixture::new().await;
    f.handle.close().await.unwrap();
    let database = std::fs::read(f.directory.database_path()).unwrap();
    // Only storage can supply a replacement for a damaged database (§19.2).
    assert!(matches!(
        reopen(&f).reset_device(f.directory.id(), "Phone").await,
        Err(BootstrapError::Sync(SyncError::NoStorage))
    ));
    assert_eq!(
        std::fs::read(f.directory.database_path()).unwrap(),
        database
    );
    assert_eq!(f.directory.device_reset().unwrap(), None);
    reopen(&f)
        .open(f.directory.id())
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn a_keychain_without_the_store_refuses_and_names_the_missing_member_keys() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
    let old = f.directory.settings().unwrap().device_id;
    // Another keychain, as after the app changes its service name: neither the
    // device entry nor the member keys are there, so only a restore continues.
    let renamed = TestCoven::new();
    assert!(matches!(
        builder(&renamed, &f._root, &f.clock, &f.storage)
            .open(f.directory.id())
            .await,
        Err(CovenError::MemberKeysMissing { store }) if store == f.directory.id()
    ));
    assert_eq!(f.directory.device_reset().unwrap(), None);
    assert_eq!(f.directory.settings().unwrap().device_id, old);
    // The original keychain still opens the untouched store.
    let handle = reopen(&f).open(f.directory.id()).await.unwrap();
    assert_eq!(handle.device_reset().await.unwrap(), None);
    handle.close().await.unwrap();
}
