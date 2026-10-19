use super::*;
use crate::tests::{held, wait_for_sent};
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
        .migrations(vec![Migration::addition(
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

/// The hidden directory a replacement loads in, inside the store's (§19.3).
fn loading_area(f: &Fixture) -> std::path::PathBuf {
    f.directory
        .database_path()
        .parent()
        .unwrap()
        .join(".coven-replacement")
}

/// The installation's id in device-only custody, which backups do not copy.
fn custody(f: &Fixture) -> coven_crypto::custody::StoreKeychain {
    f.app.store_keychain(f.directory.id())
}

#[tokio::test]
async fn reset_keeps_passphrase_sessions_until_the_replacement_handle_closes() {
    use coven_crypto::custody::{KeyringCustody, PassphraseCustody};
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    uploaded(&f, "kept").await;
    f.handle.close().await.unwrap();
    let keychain = Arc::new(custody(&f));
    let member = MemberKeyCustody::unlock(&KeyringCustody::new(keychain.clone()))
        .unwrap()
        .unwrap();
    let ring = StoreKeyCustody::unlock(&KeyringCustody::new(keychain))
        .unwrap()
        .unwrap();
    let member_session = KeySession::member(Arc::new(PassphraseCustody::new(
        Passphrase::new("member secret".into()),
        f.directory.owned_file(StoreFile::MemberKeys),
        f.directory.id(),
    )))
    .unwrap();
    member_session.persist(&member).unwrap();
    let key_session = KeySession::store(Arc::new(PassphraseCustody::new(
        Passphrase::new("store secret".into()),
        f.directory.owned_file(StoreFile::StoreKeys),
        f.directory.id(),
    )))
    .unwrap();
    key_session.persist(&ring).unwrap();
    drop((member_session, key_session));
    let configured = || {
        reopen(&f)
            .identity_custody(IdentityCustody::Passphrase(Passphrase::new(
                "member secret".into(),
            )))
            .key_custody(KeyCustody::Passphrase(Passphrase::new(
                "store secret".into(),
            )))
    };
    let handle = configured()
        .reset_device(f.directory.id(), "Replacement")
        .await
        .unwrap();
    assert_eq!(notes(&handle).await, ["kept"]);
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
    handle.close().await.unwrap();
    let reopened = configured().open(f.directory.id()).await.unwrap();
    assert_eq!(notes(&reopened).await, ["kept"]);
    assert_eq!(
        reopened.store_key_state().unwrap(),
        StoreKeyState::Available
    );
    reopened.close().await.unwrap();
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
/// reset_device, the app's choice, starts a new device (§19.3).
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

/// A reset stopped right after its swap names its decision and the store
/// waits for it; the next reset finishes it with the same fresh id (§19.3).
#[tokio::test]
async fn a_reset_interrupted_after_its_swap_is_finished_by_the_next_reset() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    uploaded(&f, "kept").await;
    f.handle.close().await.unwrap();
    custody(&f).delete_device_id().unwrap();
    assert!(matches!(
        reopen(&f)
            .interrupt_after_swap()
            .reset_device(f.directory.id(), "Restored phone")
            .await,
        Err(BootstrapError::Cancelled)
    ));
    let decided = f.directory.device_reset().unwrap().unwrap();
    assert!(matches!(
        reopen(&f).open(f.directory.id()).await,
        Err(CovenError::Lock(StoreLockError::BootstrapPending(_)))
    ));
    let handle = reopen(&f)
        .reset_device(f.directory.id(), "Restored phone")
        .await
        .unwrap();
    assert_eq!(handle.device_reset().await.unwrap(), Some(decided));
    assert_eq!(notes(&handle).await, ["kept"]);
    assert!(!loading_area(&f).exists());
    handle.close().await.unwrap();
}

/// A reset that fails while its new device loads changes nothing: the store
/// opens as before, and the next reset continues the load (§19.3).
#[tokio::test]
async fn a_reset_failing_while_it_loads_leaves_the_store_as_it_was() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    uploaded(&f, "kept").await;
    f.handle.close().await.unwrap();
    let old = f.directory.settings().unwrap().device_id;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_read(ObjectPath::floor(f.directory.id()), entered, resumed)
        .await;
    let reset = tokio::spawn(reopen(&f).reset_device(f.directory.id(), "Phone"));
    entering.await.unwrap();
    f.storage.set_online(false);
    resume.send(()).unwrap();
    assert!(reset.await.unwrap().is_err());
    f.storage.set_online(true);
    assert!(loading_area(&f).exists());
    assert_eq!(f.directory.device_reset().unwrap(), None);
    assert_eq!(f.directory.settings().unwrap().device_id, old);
    assert_eq!(custody(&f).device_id().unwrap(), Some(old));
    let handle = reopen(&f).open(f.directory.id()).await.unwrap();
    assert_eq!(handle.device_reset().await.unwrap(), None);
    assert_eq!(notes(&handle).await, ["kept"]);
    handle.close().await.unwrap();
    let handle = reopen(&f)
        .reset_device(f.directory.id(), "Phone")
        .await
        .unwrap();
    let reset = handle.device_reset().await.unwrap().unwrap();
    assert_eq!(reset.old_device, old);
    assert_eq!(notes(&handle).await, ["kept"]);
    assert!(!loading_area(&f).exists());
    handle.close().await.unwrap();
}

#[tokio::test]
async fn an_intact_unconnected_store_resets_to_an_empty_database_with_a_fresh_id() {
    let f = Fixture::new().await;
    f.handle
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('unsent','local')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    f.handle.close().await.unwrap();
    let old = f.directory.settings().unwrap().device_id;
    assert_eq!(custody(&f).device_id().unwrap(), Some(old));
    let before = f.storage.request_count();
    let handle = reopen(&f)
        .reset_device(f.directory.id(), "Phone")
        .await
        .unwrap();
    let reset = handle.device_reset().await.unwrap().unwrap();
    assert_eq!(reset.old_device, old);
    assert_ne!(reset.new_device, old);
    assert_eq!(f.directory.settings().unwrap().device_id, reset.new_device);
    assert_eq!(custody(&f).device_id().unwrap(), Some(reset.new_device));
    assert!(notes(&handle).await.is_empty());
    assert_eq!(f.storage.request_count(), before);
    handle.close().await.unwrap();
    let handle = reopen(&f).open(f.directory.id()).await.unwrap();
    assert_eq!(handle.device_reset().await.unwrap(), Some(reset));
    assert!(notes(&handle).await.is_empty());
    handle.close().await.unwrap();
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

#[tokio::test]
async fn an_unconnected_reset_resumes_with_the_local_creation_name() {
    let f = Fixture::new().await;
    f.handle.close().await.unwrap();
    assert!(matches!(
        reopen(&f)
            .interrupt_after_swap()
            .reset_device(f.directory.id(), "Phone")
            .await,
        Err(BootstrapError::Cancelled)
    ));
    let reset = f.directory.device_reset().unwrap().unwrap();
    assert_eq!(f.directory.settings().unwrap().device_id, reset.new_device);
    let handle = reopen(&f)
        .reset_device(f.directory.id(), "Phone")
        .await
        .unwrap();
    assert_eq!(handle.store_name().await.unwrap(), "Reset");
    assert_eq!(handle.device_reset().await.unwrap(), Some(reset));
    handle.close().await.unwrap();
}

#[tokio::test]
async fn an_unconnected_reset_keeps_files_when_the_local_name_cannot_be_read() {
    let f = Fixture::new().await;
    f.handle.close().await.unwrap();
    let old = f.directory.settings().unwrap();
    std::fs::write(f.directory.database_path(), b"damaged database").unwrap();
    assert!(matches!(
        reopen(&f).reset_device(f.directory.id(), "Phone").await,
        Err(BootstrapError::Store(CovenError::Database(
            DbError::DamagedDatabase
        )))
    ));
    assert_eq!(
        std::fs::read(f.directory.database_path()).unwrap(),
        b"damaged database"
    );
    assert_eq!(f.directory.settings().unwrap(), old);
    assert_eq!(f.directory.device_reset().unwrap(), None);
    assert!(!loading_area(&f).exists());
}

/// Ana replaces Carol's member keys with a pair Carol prepared elsewhere.
/// Carol's laptop still holds the old pair, so resetting it would register a
/// device with keys history no longer binds: the reset refuses before
/// anything moves, every retry refuses the same way, and the store still
/// opens with its keychain unchanged (§19.3).
#[tokio::test]
async fn a_reset_with_replaced_member_keys_refuses_and_changes_nothing() {
    let mut network = crate::tests::Network::new(2).await;
    let crate::tests::KeysReplaced { carol, member, .. } = network.replace_keys_elsewhere().await;
    let directory = carol.layout.store_dir(&carol.store);
    let settings = directory.settings().unwrap();
    let before = held(&carol.app, carol.store);
    let devices = network.devices[0].handle.members().await.unwrap();
    for _ in 0..2 {
        let reset = network
            .reopen(&carol)
            .reset_device(carol.store, "Carol's laptop")
            .await;
        assert!(
            matches!(&reset, Err(BootstrapError::Sync(SyncError::MemberKeysReplaced { member: found })) if *found == member),
            "{:?}",
            reset.as_ref().err()
        );
        assert_eq!(directory.device_reset().unwrap(), None);
        assert_eq!(directory.settings().unwrap(), settings);
        assert!(!directory
            .database_path()
            .parent()
            .unwrap()
            .join(".coven-replacement")
            .exists());
        assert_eq!(held(&carol.app, carol.store), before);
    }
    // Nothing was registered for a device that cannot count.
    network.sync(0).await;
    assert_eq!(network.devices[0].handle.members().await.unwrap(), devices);
    let handle = network.reopen(&carol).open(carol.store).await.unwrap();
    assert_eq!(handle.device_reset().await.unwrap(), None);
    assert_eq!(handle.device_id(), settings.device_id);
    handle.close().await.unwrap();
    network.close().await;
}
