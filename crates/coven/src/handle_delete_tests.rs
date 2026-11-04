//! Deleting a connected store refuses unsent work and retires this device;
//! a store never connected has no peers to lose writes to.

use crate::tests::Network;
use crate::*;
use coven_crypto::custody::{KeyringCustody, MemberKeyCustody, StoreKeyCustody};
use coven_foundation::files::StoreDirAccess;
use std::{sync::Arc, time::Duration};

async fn insert(handle: &CovenHandle, id: &str) {
    let id = id.to_owned();
    handle
        .write(move |sql| {
            sql.execute("INSERT INTO parents(id,label,value) VALUES(?1,?1,1)", [&id])?;
            Ok(())
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
        .read(|sql| Ok(sql.query("SELECT label FROM parents", [], |r| r.get(0))?))
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
        .setup_s3_storage(
            memory.config(),
            "Laptop",
            "owner".into(),
            SecretText::new("secret".into()),
            |_| {},
        )
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
            move |batch| Ok(batch.put_file("files", vec![7; 2048], crate::Place::Cloud)),
            move |sql, file| {
                sql.execute("INSERT INTO files(id,file) VALUES(?1,?2)", (&row, file))?;
                Ok(())
            },
        )
        .await
        .unwrap();
}

async fn until(work: &mut LiveQuery<UnsentWork>, expected: UnsentWork) {
    tokio::time::timeout(Duration::from_secs(10), async {
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
    assert!(StoreKeyCustody::unlock(&KeyringCustody::<StoreKeyring>::new(keychain.clone()))
        .unwrap()
        .is_none());
    assert!(MemberKeyCustody::unlock(&KeyringCustody::<MemberIdentity>::new(keychain))
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
                .clock(Arc::new(crate::tests::PollingClock::new(network.clock.clone())))
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
