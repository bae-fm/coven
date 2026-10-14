use super::*;
use coven_storage::{test_utils::MemoryStorage, Storage};
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
            .create_store(
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
        handle.initialize_identity().unwrap();
        Self {
            app,
            handle,
            storage,
            clock,
            directory,
            _root: root,
        }
    }

    async fn setup(&self) -> Result<ConnectedStorage, StorageSetupError> {
        self.handle
            .setup_s3_storage(
                self.storage.config(),
                "Test device",
                "member-key".into(),
                SecretText::new("secret".into()),
            )
            .await
    }

    fn advance(&self, duration: Duration) {
        self.clock.set(self.clock.now() + duration);
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
    f.advance(Duration::from_secs(1));
    let written = f.clock.now();
    f.handle
        .write(move |sql| {
            sql.execute("INSERT INTO notes VALUES(?1,'uploaded')", [id])?;
            Ok(())
        })
        .await
        .unwrap();
    status(
        &f.handle,
        |s| matches!(s, SyncStatus::Synced { finished_at } if *finished_at >= written),
    )
    .await;
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
    assert!(matches!(
        reopen(&f).open(f.directory.id()).await,
        Err(CovenError::Lock(StoreLockError::BootstrapPending(_)))
    ));
    // Deciding sends nothing; ordinary opens keep refusing the old copy.
    assert_eq!(f.storage.request_count(), before);
    assert!(matches!(
        reopen(&f).open(f.directory.id()).await,
        Err(CovenError::Lock(StoreLockError::BootstrapPending(_)))
    ));
    let handle = reopen(&f)
        .reset_device(f.directory.id(), "Restored phone")
        .await
        .unwrap();
    let reset = handle.device_reset().await.unwrap().unwrap();
    assert_eq!(reset.old_device, old);
    assert_ne!(reset.new_device, old);
    assert_eq!(reset.reason, DeviceResetReason::IdentityMismatch);
    assert_eq!(f.directory.settings().unwrap().device_id, reset.new_device);
    assert_eq!(custody(&f).device_id().unwrap(), Some(reset.new_device));
    // Stored history loads; the unsent edit is discarded, not renumbered.
    assert_eq!(notes(&handle).await, ["kept"]);
    let devices: Vec<_> = handle
        .get_members()
        .await
        .unwrap()
        .into_iter()
        .flat_map(|member| member.devices)
        .collect();
    assert!(devices.contains(&reset.new_device));
    let mut notices = handle.subscribe_device_reset();
    assert_eq!(notices.next().await.unwrap(), Some(reset));
    handle.close().await.unwrap();
    let handle = reopen(&f).open(f.directory.id()).await.unwrap();
    assert_eq!(handle.device_reset().await.unwrap(), Some(reset));
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
    assert!(reopen(&f).open(f.directory.id()).await.is_err());
    let decided = f.directory.device_reset().unwrap().unwrap();
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
async fn reset_device_without_a_recorded_reset_refuses_a_published_store() {
    let f = Fixture::new().await;
    f.handle.close().await.unwrap();
    assert!(matches!(
        reopen(&f).reset_device(f.directory.id(), "Phone").await,
        Err(BootstrapError::Directory(BootstrapDirectoryError::Create(
            coven_foundation::files::StoreCreationError::AlreadyExists(_)
        )))
    ));
}
