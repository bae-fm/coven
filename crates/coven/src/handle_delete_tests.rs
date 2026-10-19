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

async fn stopped(handle: &CovenHandle) {
    let mut status = handle.subscribe_sync_status();
    handle.stop_sync();
    tokio::time::timeout(
        Duration::from_secs(5),
        status.wait_for(|s| matches!(s, SyncStatus::Stopped)),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn deleting_refuses_unsent_writes_then_retires_this_device() {
    let mut network = Network::new(2).await;
    let phone = &network.devices[1];
    let directory = phone.layout.store_dir(&phone.store);
    let device = directory.settings().unwrap().device_id;
    stopped(&phone.handle).await;
    insert(&phone.handle, "unsent").await;
    assert!(matches!(
        phone.handle.delete_store(IfUnsent::Refuse).await,
        Err(StoreDeletionError::Unsent(UnsentWork {
            writes: 1,
            files: 0
        }))
    ));
    // Nothing was deleted: the store stays open and the write still sends.
    assert_eq!(phone.layout.stores().await.unwrap().len(), 1);
    phone.handle.start_sync().await.unwrap();
    network.sync(1).await;
    network.devices[1]
        .handle
        .delete_store(IfUnsent::Refuse)
        .await
        .unwrap();
    assert!(network.devices[1].layout.stores().await.unwrap().is_empty());
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
    let device = phone
        .layout
        .store_dir(&phone.store)
        .settings()
        .unwrap()
        .device_id;
    stopped(&phone.handle).await;
    insert(&phone.handle, "discarded").await;
    phone.handle.delete_store(IfUnsent::Discard).await.unwrap();
    assert!(phone.layout.stores().await.unwrap().is_empty());
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
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let directory = app
        .create_closed_store(&layout, "Local", std::sync::Arc::new(UuidIds))
        .await
        .unwrap();
    let handle = app
        .builder(layout.clone())
        .synced_tables(vec![SyncedTable::new("parents", RowIdentity::SharedKey)])
        .migrations(vec![Migration::addition(1, "parents", "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,label TEXT NOT NULL,value INTEGER NOT NULL)")])
        .open(directory.id())
        .await
        .unwrap();
    insert(&handle, "local").await;
    handle.delete_store(IfUnsent::Refuse).await.unwrap();
    assert!(layout.stores().await.unwrap().is_empty());
    assert_eq!(
        app.store_keychain(directory.id()).device_id().unwrap(),
        None
    );
}

/// A store on memory storage, synced once, then stopped with uploads paused,
/// so committed writes and attached files stay unsent until the test sends them.
async fn stopped_store_with_files() -> (tempfile::TempDir, TestCoven, CovenHandle) {
    use coven_storage::{test_utils::MemoryStorage, Storage};
    let root = tempfile::tempdir().unwrap();
    let app = TestCoven::new();
    let layout = StoreLayout::new(root.path().into());
    let directory = app
        .create_closed_store(&layout, "Files", std::sync::Arc::new(UuidIds))
        .await
        .unwrap();
    let memory = std::sync::Arc::new(
        MemoryStorage::builder()
            .clock(std::sync::Arc::new(SystemClock))
            .transfer_limits(65536, 65536)
            .build()
            .unwrap(),
    );
    let handle = app
        .builder(layout)
        .storage_connector(memory.clone())
        .synced_tables(vec![
            SyncedTable::new("parents", RowIdentity::SharedKey),
            SyncedTable::new("files", RowIdentity::SharedKey).carries_files(FileDecl::new(
                "files",
                1 << 30,
                Provenance::AppProvided,
                CacheFill::CacheLazy,
            )),
        ])
        .migrations(vec![Migration::addition(
            1,
            "parents and files",
            "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,label TEXT NOT NULL,value INTEGER NOT NULL);
             CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT)",
        )])
        .open(directory.id())
        .await
        .unwrap();
    handle
        .setup_s3_storage(
            memory.config(),
            "Laptop",
            "owner".into(),
            SecretText::new("secret".into()),
        )
        .await
        .unwrap();
    let mut status = handle.subscribe_sync_status();
    status
        .wait_for(|s| matches!(s, SyncStatus::Synced { .. }))
        .await
        .unwrap();
    stopped(&handle).await;
    handle.unlock_store_key().await.unwrap();
    handle.set_uploads_paused(true).await.unwrap();
    (root, app, handle)
}

async fn attach(handle: &CovenHandle, id: &str) {
    let (file, row) = (id.to_owned(), id.to_owned());
    handle
        .write_with_files(
            move |batch| {
                batch.put_file("files", file, vec![7; 2048]);
                Ok(())
            },
            move |sql| {
                sql.execute("INSERT INTO files(id) VALUES(?1)", [&row])?;
                Ok(())
            },
        )
        .await
        .unwrap();
}

async fn until(work: &mut LiveQuery<UnsentWork>, expected: UnsentWork) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while work.next().await.unwrap() != expected {}
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
    // The attaching write is a write too: two writes, one file.
    let work = handle.unsent_work().await.unwrap();
    assert_eq!(
        work,
        UnsentWork {
            writes: 2,
            files: 1
        }
    );
    match handle.delete_store(IfUnsent::Refuse).await {
        Err(StoreDeletionError::Unsent(refused)) => assert_eq!(refused, work),
        other => panic!("expected the unsent refusal, got {other:?}"),
    }
    // Confirmed file uploads leave the writes, which only a sync pass sends.
    handle.set_uploads_paused(false).await.unwrap();
    handle.retry_uploads_now().await.unwrap();
    assert_eq!(
        handle.unsent_work().await.unwrap(),
        UnsentWork {
            writes: 2,
            files: 0
        }
    );
    let mut live = handle.subscribe_unsent_work();
    handle.start_sync().await.unwrap();
    until(
        &mut live,
        UnsentWork {
            writes: 0,
            files: 0,
        },
    )
    .await;
    assert!(handle.unsent_work().await.unwrap().is_empty());
    handle.delete_store(IfUnsent::Refuse).await.unwrap();
}

#[tokio::test]
async fn subscribed_unsent_work_follows_each_commit_and_confirmation() {
    let (_root, _app, handle) = stopped_store_with_files().await;
    let mut live = handle.subscribe_unsent_work();
    assert_eq!(
        live.next().await.unwrap(),
        UnsentWork {
            writes: 0,
            files: 0
        }
    );
    insert(&handle, "first").await;
    assert_eq!(
        live.next().await.unwrap(),
        UnsentWork {
            writes: 1,
            files: 0
        }
    );
    attach(&handle, "photo").await;
    assert_eq!(
        live.next().await.unwrap(),
        UnsentWork {
            writes: 2,
            files: 1
        }
    );
    handle.set_uploads_paused(false).await.unwrap();
    handle.start_sync().await.unwrap();
    until(
        &mut live,
        UnsentWork {
            writes: 0,
            files: 0,
        },
    )
    .await;
    handle.close().await.unwrap();
}

/// Every entry this device kept for the store is gone.
fn device_entries_gone(app: &TestCoven, store: StoreId) {
    let keychain = Arc::new(app.store_keychain(store));
    assert!(keychain.device_id().unwrap().is_none());
    assert!(keychain.storage_credentials().unwrap().is_none());
    assert!(keychain.host_secret("api-token").unwrap().is_none());
    assert!(
        StoreKeyCustody::unlock(&KeyringCustody::<StoreKeyring>::new(keychain.clone()))
            .unwrap()
            .is_none()
    );
    assert!(
        MemberKeyCustody::unlock(&KeyringCustody::<MemberIdentity>::new(keychain))
            .unwrap()
            .is_none()
    );
}

/// Ana deletes Household from her phone, or discards it there. The phone's
/// own entries go. Her restore code and S3 key stay in iCloud Keychain, so her
/// laptop still offers the store and restores from them (§13.1).
#[tokio::test]
async fn removing_a_store_from_one_device_keeps_the_persons_synced_entries() {
    for discard in [false, true] {
        let mut network = Network::with_schema(CloudProvider::S3, Vec::new(), Vec::new()).await;
        let phone = network.devices.remove(0);
        let laptop = phone.app.another_device();
        let saved = laptop.saved_restore_codes().await.unwrap();
        assert_eq!(saved.len(), 1);
        assert!(saved[0].has_s3_credentials);
        phone
            .handle
            .set_host_secret("api-token", "phone token")
            .unwrap();
        if discard {
            phone.handle.close().await.unwrap();
            phone
                .app
                .discard_store(&phone.layout.store_dir(&phone.store))
                .await
                .unwrap();
        } else {
            phone.handle.delete_store(IfUnsent::Refuse).await.unwrap();
        }
        assert!(phone.layout.stores().await.unwrap().is_empty());
        device_entries_gone(&phone.app, phone.store);
        assert_eq!(laptop.saved_restore_codes().await.unwrap(), saved);
        assert_eq!(phone.app.saved_restore_codes().await.unwrap(), saved);
        let root = tempfile::tempdir().unwrap();
        let (_, cancel) = tokio::sync::watch::channel(false);
        let restored = restore_from_keychain(
            laptop
                .builder(StoreLayout::new(root.path().into()))
                .synced_tables(Vec::new())
                .migrations(Vec::new())
                .clock(Arc::new(crate::tests::PollingClock(network.clock.clone())))
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
        restored.close().await.unwrap();
        network.close().await;
    }
}
