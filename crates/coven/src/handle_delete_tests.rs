//! Deleting a connected store refuses unsent work and retires this device;
//! a store never connected has no peers to lose writes to.

use crate::tests::Network;
use crate::*;
use coven_crypto::custody::KeyringCustody;
use coven_foundation::clock::HANG_GUARD;
use coven_foundation::files::StoreDirAccess;
use coven_storage::test_utils::Faults;
use std::sync::Arc;

async fn insert(handle: &CovenHandle, id: &str) {
    let id = id.to_owned();
    handle
        .write(move |sql| {
            sql.execute("INSERT INTO parents(id,label,value) VALUES(?1,?1,1)", [&id])?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
}

/// Stop the device's sync, so no pass runs until it starts again.
async fn stopped(device: &crate::tests::Device) {
    device.stop_sync().await;
}

#[tokio::test]
async fn deleting_refuses_unsent_writes_then_retires_this_device() {
    let mut network = Network::new(2).await;
    let phone = &network.devices[1];
    let directory = phone.app.store_dir(&phone.store);
    let device = directory.settings().unwrap().device_id;
    stopped(phone).await;
    insert(&phone.handle, "unsent").await;
    assert!(matches!(
        phone.handle.delete_store(IfUnsent::Refuse).await,
        Err(StoreDeletionError::Unsent(UnsentWork { writes: 1, files: 0 }))
    ));
    // Nothing was deleted: the store stays open and the write still sends.
    assert_eq!(phone.app.stores().await.unwrap().len(), 1);
    phone.handle.start_sync().await.unwrap();
    network.sync(1).await;
    network.devices[1].handle.delete_store(IfUnsent::Refuse).await.unwrap();
    assert!(network.devices[1].app.stores().await.unwrap().is_empty());
    network.devices.remove(1);
    // The owner reads the write, and the retirement that stops its polling.
    network.sync(0).await;
    let owner = &network.devices[0].handle;
    let labels: Vec<String> = owner
        .read(|sql| Ok::<_, CovenError>(sql.query("SELECT label FROM parents", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert!(labels.contains(&"unsent".to_owned()));
    let state = owner
        .members()
        .await
        .unwrap()
        .into_iter()
        .flat_map(|member| member.devices)
        .find(|d| d.id == device)
        .unwrap()
        .state;
    assert_eq!(state, DeviceState::Removed);
    network.close().await;
}

#[tokio::test]
async fn discarding_still_retires_this_device() {
    let mut network = Network::new(2).await;
    let phone = &network.devices[1];
    let device = phone.app.store_dir(&phone.store).settings().unwrap().device_id;
    stopped(phone).await;
    insert(&phone.handle, "discarded").await;
    phone.handle.delete_store(IfUnsent::Discard).await.unwrap();
    assert!(phone.app.stores().await.unwrap().is_empty());
    network.devices.remove(1);
    network.sync(0).await;
    let owner = &network.devices[0].handle;
    let removed = owner
        .members()
        .await
        .unwrap()
        .into_iter()
        .flat_map(|member| member.devices)
        .any(|d| d.id == device && d.state == DeviceState::Removed);
    assert!(removed);
    network.close().await;
}

#[tokio::test]
async fn a_store_never_connected_deletes_with_its_local_writes() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let directory = app.create_closed_store("Local", std::sync::Arc::new(UuidIds)).await.unwrap();
    let handle = app
        .builder()
        .synced_tables(vec![SyncedTable::new("parents", RowIdentity::SharedKey)])
        .migrations(vec![Migration::addition(1, "parents", "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,label TEXT NOT NULL,value INTEGER NOT NULL)")])
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    insert(&handle, "local").await;
    handle.delete_store(IfUnsent::Refuse).await.unwrap();
    assert!(app.stores().await.unwrap().is_empty());
    assert_eq!(app.store_keychain(directory.id()).device_id().unwrap(), None);
}

/// A store on memory storage, synced once, then with its passes held and
/// uploads paused, so committed writes and attached files stay unsent until
/// the test sends them.
async fn stopped_store_with_files() -> (tempfile::TempDir, Coven, CovenHandle) {
    use coven_storage::{test_utils::MemoryStorage, Storage};
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let directory = app.create_closed_store("Files", std::sync::Arc::new(UuidIds)).await.unwrap();
    let memory = std::sync::Arc::new(
        MemoryStorage::builder()
            .clock(std::sync::Arc::new(SystemClock))
            .transfer_limits(65536, 65536)
            .build()
            .unwrap(),
    );
    let connector = crate::tests::Connector::new(memory.clone());
    let handle = app
        .builder()
        .storage_connector(connector.clone())
        .synced_tables(vec![
            SyncedTable::new("parents", RowIdentity::SharedKey),
            SyncedTable::new("files", RowIdentity::SharedKey).file_column(FileColumn::new("file")),
        ])
        .namespaces(vec![Namespace::new("files", Provenance::AppProvided, 1 << 30)])
        .migrations(vec![Migration::addition(
            1,
            "parents and files",
            "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,label TEXT NOT NULL,value INTEGER NOT NULL);
             CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT)",
        )])
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    handle
        .setup_s3_storage(memory.config(), "owner".into(), SecretText::new("secret".into()), |_| {})
        .await
        .unwrap();
    // The first pass after setup ends synced.
    handle.test_last_pass().wait_for(|last| *last == Some(LastPass::Synced)).await;
    handle.test_hold_passes(true).await;
    handle.set_uploads_paused(true).await.unwrap();
    (root, app, handle)
}

async fn attach(handle: &CovenHandle, id: &str) {
    let row = id.to_owned();
    handle
        .test_staged_write(
            move |batch| {
                Ok::<_, CovenError>(batch.put_file("files", vec![7; 2048], crate::Place::Cloud))
            },
            move |sql, file| {
                sql.execute("INSERT INTO files(id,file) VALUES(?1,?2)", (&row, file))?;
                Ok(())
            },
        )
        .await
        .unwrap();
}

async fn until(work: &mut LiveQuery<UnsentWork, crate::CovenError>, expected: UnsentWork) {
    tokio::time::timeout(HANG_GUARD, async {
        while work.next().await.expect("the live query answers").unwrap() != expected {}
    })
    .await
    .unwrap_or_else(|_| panic!("unsent work never reached {expected:?}"));
}

#[tokio::test]
async fn unsent_work_is_what_deleting_refuses_until_uploads_confirm() {
    let (_root, _app, handle) = stopped_store_with_files().await;
    assert!(handle.unsent_work().await.unwrap().is_empty());
    insert(&handle, "unsent").await;
    attach(&handle, "photo").await;
    // The staging write is a write too: two writes, one file.
    let work = handle.unsent_work().await.unwrap();
    assert_eq!(work, UnsentWork { writes: 2, files: 1 });
    match handle.delete_store(IfUnsent::Refuse).await {
        Err(StoreDeletionError::Unsent(refused)) => assert_eq!(refused, work),
        other => panic!("expected the unsent refusal, got {other:?}"),
    }
    // A confirmed upload is one more write, switching the file's record to
    // the cloud (§17.2); only a sync pass sends the writes.
    handle.set_uploads_paused(false).await.unwrap();
    handle.retry_uploads_now().await.unwrap();
    assert_eq!(handle.unsent_work().await.unwrap(), UnsentWork { writes: 3, files: 0 });
    let mut live = handle.subscribe_unsent_work();
    handle.test_hold_passes(false).await;
    until(&mut live, UnsentWork { writes: 0, files: 0 }).await;
    assert!(handle.unsent_work().await.unwrap().is_empty());
    handle.delete_store(IfUnsent::Refuse).await.unwrap();
}

#[tokio::test]
async fn subscribed_unsent_work_follows_each_commit_and_confirmation() {
    let (_root, _app, handle) = stopped_store_with_files().await;
    let mut live = handle.subscribe_unsent_work();
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        UnsentWork { writes: 0, files: 0 }
    );
    insert(&handle, "first").await;
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        UnsentWork { writes: 1, files: 0 }
    );
    attach(&handle, "photo").await;
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        UnsentWork { writes: 2, files: 1 }
    );
    handle.set_uploads_paused(false).await.unwrap();
    handle.test_hold_passes(false).await;
    until(&mut live, UnsentWork { writes: 0, files: 0 }).await;
    handle.close().await.unwrap();
}

/// Every entry this device kept for the store is gone.
fn device_entries_gone(app: &Coven, store: StoreId) {
    let keychain = Arc::new(app.store_keychain(store));
    assert!(keychain.device_id().unwrap().is_none());
    assert!(keychain.storage_credentials().unwrap().is_none());
    assert!(keychain.host_secret("api-token").unwrap().is_none());
    assert!(Custody::<StoreKeyring>::unlock(&KeyringCustody::<StoreKeyring>::new(
        keychain.clone()
    ))
    .unwrap()
    .is_none());
    assert!(Custody::<MemberIdentity>::unlock(&KeyringCustody::<MemberIdentity>::new(keychain))
        .unwrap()
        .is_none());
}

/// Ana deletes Household from her phone, or discards it there. The phone's
/// own entries go. Her restore code and S3 key stay in iCloud Keychain, so her
/// laptop still offers the store and restores from them (§13.1).
#[tokio::test]
async fn removing_a_store_from_one_device_keeps_the_person_entries() {
    for discard in [false, true] {
        let mut network = Network::with_schema(CloudProvider::S3, Vec::new(), Vec::new()).await;
        let phone = network.devices.remove(0);
        let root = tempfile::tempdir().unwrap();
        let laptop = phone.app.another_device(root.path().into());
        let saved = crate::tests::readable(laptop.saved_restore_codes().await.unwrap());
        assert_eq!(saved.len(), 1);
        assert!(saved[0].has_s3_credentials);
        phone.handle.set_host_secret("api-token", "phone token").unwrap();
        if discard {
            phone.handle.close().await.unwrap();
            phone.app.discard_store(&phone.app.store_dir(&phone.store)).await.unwrap();
        } else {
            phone.handle.delete_store(IfUnsent::Refuse).await.unwrap();
        }
        assert!(phone.app.stores().await.unwrap().is_empty());
        device_entries_gone(&phone.app, phone.store);
        assert_eq!(crate::tests::readable(laptop.saved_restore_codes().await.unwrap()), saved);
        assert_eq!(crate::tests::readable(phone.app.saved_restore_codes().await.unwrap()), saved);
        let (_, cancel) = tokio::sync::watch::channel(false);
        let restored = restore_from_saved_code(
            laptop
                .builder()
                .synced_tables(Vec::new())
                .migrations(Vec::new())
                .clock(network.polling.clone())
                .storage_connector(Arc::new(phone.storage.for_device())),
            &saved[0],
            None,
            "Ana's laptop",
            |_| {},
            &cancel,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(restored.store_id(), phone.store);
        // Deleting the restored store leaves no file named for it, its
        // restore's bootstrap lock included.
        if discard {
            restored.close().await.unwrap();
            laptop.discard_store(&laptop.store_dir(&phone.store)).await.unwrap();
        } else {
            restored.delete_store(IfUnsent::Discard).await.unwrap();
        }
        assert_eq!(files_named_for(root.path(), phone.store), Vec::<String>::new());
        network.close().await;
    }
}

/// The entries in an app's stores directory whose names contain `store`.
fn files_named_for(app_dir: &std::path::Path, store: StoreId) -> Vec<String> {
    std::fs::read_dir(app_dir.join("stores"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.contains(&store.to_string()))
        .collect()
}

/// The phone's deletion is writing its record when the app closes the
/// handle. The owner closing waits on stays held until the record is written,
/// so no later open can meet the write half done (E1).
#[tokio::test]
async fn closing_waits_for_the_deletion_record_being_written() {
    let mut network = Network::new(2).await;
    let phone = &network.devices[1];
    let (reached, arrived) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    phone.handle.storage.gate_deletion_record(move || {
        reached.send(()).unwrap();
        released.recv().unwrap();
    });
    let handle = phone.handle.clone();
    let deleting = tokio::spawn(async move { handle.delete_store(IfUnsent::Refuse).await });
    tokio::time::timeout(HANG_GUARD, arrived).await.unwrap().unwrap();
    // One poll: a free owner would close here.
    tokio::select! {
        biased;
        () = phone.handle.codes.close() => panic!("closing didn't wait for the record"),
        () = std::future::ready(()) => {}
    }
    release.send(()).unwrap();
    deleting.await.unwrap().unwrap();
    network.devices.remove(1);
    network.close().await;
}

/// Closing fails once the phone's removal is published: deleting says so
/// apart from a failed removal, nothing is deleted yet, and discarding the
/// store finishes it. The owner lists the phone removed (§14.1, E1).
#[tokio::test]
async fn a_close_failing_after_the_removal_is_finished_by_discarding() {
    let mut network = Network::new(2).await;
    let phone = &network.devices[1];
    let directory = phone.app.store_dir(&phone.store);
    let device = directory.settings().unwrap().device_id;
    phone.handle.storage.fail_next_close(SyncError::Database(DbError::RollbackFailed));
    let failed = phone.handle.delete_store(IfUnsent::Refuse).await;
    assert!(
        matches!(
            failed,
            Err(StoreDeletionError::Closing(SyncError::Database(DbError::RollbackFailed)))
        ),
        "{failed:?}"
    );
    assert_eq!(phone.app.stores().await.unwrap().len(), 1);
    phone.app.discard_store(&directory).await.unwrap();
    assert!(phone.app.stores().await.unwrap().is_empty());
    network.devices.remove(1);
    network.sync(0).await;
    let removed = network.devices[0]
        .handle
        .members()
        .await
        .unwrap()
        .into_iter()
        .flat_map(|member| member.devices)
        .any(|d| d.id == device && d.state == DeviceState::Removed);
    assert!(removed);
    network.close().await;
}

/// Discarding a closed store loses its unsent writes and does not publish a
/// removal: the device stays one of the store's devices until it is removed
/// (E1).
#[tokio::test]
async fn discarding_a_closed_store_keeps_its_device_a_member_until_removed() {
    let mut network = Network::new(2).await;
    let phone = network.devices.remove(1);
    let directory = phone.app.store_dir(&phone.store);
    let device = directory.settings().unwrap().device_id;
    stopped(&phone).await;
    insert(&phone.handle, "unsent").await;
    phone.handle.close().await.unwrap();
    phone.app.discard_store(&directory).await.unwrap();
    assert!(phone.app.stores().await.unwrap().is_empty());
    network.sync(0).await;
    let owner = &network.devices[0].handle;
    let state = owner
        .members()
        .await
        .unwrap()
        .into_iter()
        .flat_map(|member| member.devices)
        .find(|d| d.id == device)
        .expect("the discarded device is still one of the store's devices")
        .state;
    assert_ne!(state, DeviceState::Removed);
    let labels: Vec<String> = owner
        .read(|sql| Ok::<_, CovenError>(sql.query("SELECT label FROM parents", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert!(!labels.contains(&"unsent".to_owned()));
    network.close().await;
}

/// Whether the owner lists `device` as removed, once it has read storage.
async fn removed(network: &mut Network, device: DeviceId) -> bool {
    network.sync(0).await;
    network.devices[0]
        .handle
        .members()
        .await
        .unwrap()
        .into_iter()
        .flat_map(|member| member.devices)
        .any(|d| d.id == device && d.state == DeviceState::Removed)
}

/// The phone's store as its listing shows it.
async fn listed(device: &crate::tests::Device) -> Vec<StoreInfo> {
    device.app.readable_stores().await
}

/// The provider refuses the phone's first request, before its removal is
/// journaled: the deletion fails as `Sync`, its record goes, and the store is
/// as it was, in use, until deleting again finishes (§11.6, E1).
#[tokio::test]
async fn a_deletion_failing_before_its_removal_was_journaled_leaves_the_store_as_it_was() {
    let mut network = Network::new(2).await;
    let phone = &network.devices[1];
    let device = phone.app.store_dir(&phone.store).settings().unwrap().device_id;
    stopped(phone).await;
    phone.storage.set_faults(Faults { fail_next: 1, ..Faults::none() }).await;
    let failed = phone.handle.delete_store(IfUnsent::Refuse).await;
    phone.storage.set_faults(Faults::none()).await;
    assert!(matches!(failed, Err(StoreDeletionError::Sync(_))), "{failed:?}");
    assert_eq!(
        listed(phone).await,
        [StoreInfo { id: phone.store, name: "Household".into(), unfinished: None }]
    );
    assert!(phone.handle.unsent_work().await.unwrap().is_empty());
    phone.handle.delete_store(IfUnsent::Refuse).await.unwrap();
    assert!(phone.app.stores().await.unwrap().is_empty());
    network.devices.remove(1);
    assert!(removed(&mut network, device).await);
    network.close().await;
}

/// The phone's removal lands in storage, and the provider answers with an
/// error, so the call fails after the removal was journaled. Past that
/// point the deletion only goes forward: the handle closes, the store lists
/// as being deleted and refuses opens, so it is never used while its removal
/// is under way, and `resume_deletion` finishes it (§11.6, E1).
#[tokio::test]
async fn a_deletion_failing_after_its_removal_was_journaled_is_listed_and_resumed() {
    let mut network = Network::new(2).await;
    let phone = &network.devices[1];
    let device = phone.app.store_dir(&phone.store).settings().unwrap().device_id;
    stopped(phone).await;
    phone.storage.set_faults(Faults { refuse_after_storing: 1, ..Faults::none() }).await;
    let failed = phone.handle.delete_store(IfUnsent::Refuse).await;
    phone.storage.set_faults(Faults::none()).await;
    assert!(matches!(failed, Err(StoreDeletionError::Unfinished(_))), "{failed:?}");
    assert!(phone.handle.unsent_work().await.is_err(), "the handle is still in use");
    assert_eq!(
        listed(phone).await,
        [StoreInfo {
            id: phone.store,
            name: "Household".into(),
            unfinished: Some(Unfinished::Deletion)
        }]
    );
    let opened = network.reopen(phone).open(phone.store, |_| ()).await;
    assert!(
        matches!(opened, Err(CovenError::Lock(StoreLockError::DeletionUnfinished(store))) if store == phone.store),
        "{:?}",
        opened.map(drop)
    );
    network.reopen(phone).resume_deletion(phone.store).await.unwrap();
    assert!(phone.app.stores().await.unwrap().is_empty());
    network.devices.remove(1);
    assert!(removed(&mut network, device).await);
    network.close().await;
}

/// The provider refuses the phone's removal outright: nothing is stored,
/// and the deletion fails with its removal journaled and claimed, waiting
/// for the app to call again. `resume_deletion` runs that removal, which
/// sends it again, rather than journaling a second one whose claim would
/// meet the first's (§11.4, E1).
#[tokio::test]
async fn a_resumed_deletion_runs_the_removal_the_provider_refused() {
    let mut network = Network::new(2).await;
    let phone = &network.devices[1];
    let device = phone.app.store_dir(&phone.store).settings().unwrap().device_id;
    stopped(phone).await;
    let slot = std::num::NonZeroU64::new(phone.handle.applied_slot().await + 1).unwrap();
    let removal = coven_storage::ObjectPath::slot(phone.store, slot);
    let refused = (coven_storage::StorageFailure::ProviderUnavailable, 503);
    phone
        .storage
        .set_faults(Faults { create_failures: [(removal, refused)].into(), ..Faults::none() })
        .await;
    let failed = phone.handle.delete_store(IfUnsent::Refuse).await;
    phone.storage.set_faults(Faults::none()).await;
    assert!(matches!(failed, Err(StoreDeletionError::Unfinished(_))), "{failed:?}");
    network.reopen(phone).resume_deletion(phone.store).await.unwrap();
    assert!(phone.app.stores().await.unwrap().is_empty());
    network.devices.remove(1);
    assert!(removed(&mut network, device).await);
    network.close().await;
}

/// The phone's removal applies, and closing its handle fails, as a crash
/// right after the removal would leave it. The store is recorded as being
/// deleted: it doesn't open, read only or for use, as a removed store with
/// its data and keys, nor reset, and `resume_deletion` finishes it (§11.6,
/// E1).
#[tokio::test]
async fn a_deletion_stopped_after_its_removal_applied_refuses_opens_and_resumes() {
    let mut network = Network::new(2).await;
    let phone = &network.devices[1];
    let device = phone.app.store_dir(&phone.store).settings().unwrap().device_id;
    phone.handle.storage.fail_next_close(SyncError::Database(DbError::RollbackFailed));
    assert!(matches!(
        phone.handle.delete_store(IfUnsent::Refuse).await,
        Err(StoreDeletionError::Closing(_))
    ));
    let deleting = |error: &StoreLockError| matches!(error, StoreLockError::DeletionUnfinished(store) if *store == phone.store);
    assert!(matches!(
        network.reopen(phone).open(phone.store, |_| ()).await,
        Err(CovenError::Lock(error)) if deleting(&error)
    ));
    assert!(matches!(
        network.reopen(phone).open_read_only(phone.store).await,
        Err(ReadOnlyOpenError::Local(CovenError::Lock(error))) if deleting(&error)
    ));
    assert!(matches!(
        network.reopen(phone).reset_device(phone.store, "Phone", |_| ()).await,
        Err(BootstrapError::Directory(BootstrapDirectoryError::Lock(error))) if deleting(&error)
    ));
    assert_eq!(listed(phone).await[0].unfinished, Some(Unfinished::Deletion));
    network.reopen(phone).resume_deletion(phone.store).await.unwrap();
    assert!(phone.app.stores().await.unwrap().is_empty());
    network.devices.remove(1);
    assert!(removed(&mut network, device).await);
    network.close().await;
}

/// The phone holds no credentials for Household's recorded location: the
/// person signed out. It can't publish its removal, so deleting is refused,
/// naming the sign-in, and nothing changes. Deleting the store anyway would
/// leave the phone one of Household's devices for good (E1).
#[tokio::test]
async fn deleting_a_signed_out_store_is_refused_naming_the_sign_in() {
    let mut network = Network::new(2).await;
    let phone = network.devices.remove(1);
    phone.handle.close().await.unwrap();
    phone.app.lose_storage_credentials(phone.store).unwrap();
    let reopened = network.reopen(&phone).open(phone.store, |_| ()).await.unwrap();
    let refused = reopened.delete_store(IfUnsent::Discard).await;
    assert!(
        matches!(refused, Err(StoreDeletionError::SignInRequired(CloudProvider::S3))),
        "{refused:?}"
    );
    assert_eq!(listed(&phone).await.len(), 1);
    assert_eq!(listed(&phone).await[0].unfinished, None);
    reopened.close().await.unwrap();
    network.close().await;
}

/// The phone recorded its deletion, refusing unsent work, and stopped
/// before journaling its removal, with a write still unsent. Resuming
/// checks again with the recorded choice: it refuses, removes the record,
/// and the store opens as before, its write still to send (§11.6, E1).
#[tokio::test]
async fn a_resumed_deletion_before_its_journal_refuses_unsent_work_and_forgets_itself() {
    let mut network = Network::new(2).await;
    let phone = network.devices.remove(1);
    stopped(&phone).await;
    insert(&phone.handle, "unsent").await;
    phone.handle.close().await.unwrap();
    phone.app.store_dir(&phone.store).record_deletion(IfUnsent::Refuse).unwrap();
    assert_eq!(listed(&phone).await[0].unfinished, Some(Unfinished::Deletion));
    let resumed = network.reopen(&phone).resume_deletion(phone.store).await;
    assert!(
        matches!(resumed, Err(StoreDeletionError::Unsent(UnsentWork { writes: 1, files: 0 }))),
        "{resumed:?}"
    );
    assert_eq!(listed(&phone).await[0].unfinished, None);
    let reopened = network.reopen(&phone).open(phone.store, |_| ()).await.unwrap();
    assert_eq!(reopened.unsent_work().await.unwrap(), UnsentWork { writes: 1, files: 0 });
    reopened.close().await.unwrap();
    network.close().await;
}

/// A key custody whose close panics, as a bug in the app's custody would.
struct PanicsOnClose;
impl Custody<StoreKeyring> for PanicsOnClose {
    fn unlock(&self) -> Result<Option<StoreKeyring>, KeyError> {
        Ok(None)
    }
    fn persist(&self, _: &StoreKeyring) -> Result<(), KeyError> {
        Ok(())
    }
    fn forget(&self) -> Result<(), KeyError> {
        Ok(())
    }
    fn close(&self) {
        panic!("the custody's close failed")
    }
}

/// Closing reports every failure of closing workers and connections, several
/// chained as `SyncError::Cleanup`, and closes every clone (E1).
#[tokio::test]
async fn closing_reports_several_failures_chained() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let handle = app
        .builder()
        .synced_tables(Vec::new())
        .migrations(Vec::new())
        .key_custody(CustodyChoice::Custom(Arc::new(PanicsOnClose)))
        .create_store("session", "Test device")
        .await
        .unwrap();
    let clone = handle.clone();
    handle.storage.fail_next_close(SyncError::Database(DbError::RollbackFailed));
    let failed = handle.close().await;
    assert!(
        matches!(
            &failed,
            Err(SyncError::Cleanup { operation, cleanup })
                if matches!(**operation, SyncError::Database(DbError::RollbackFailed))
                    && matches!(**cleanup, SyncError::Bug(_))
        ),
        "{failed:?}"
    );
    assert!(matches!(
        clone.read(|_| Ok(())).await,
        Err(CovenError::Database(DbError::StoreClosed))
    ));
    assert!(matches!(clone.store_key_state(), Err(KeyError::StoreClosed)));
}
