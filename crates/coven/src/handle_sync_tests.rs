//! Wakes coalesce: a local write racing a pass retains one more pass, the
//! pass's own bookkeeping retains none, and a stopped or closed loop runs
//! nothing (SP step 9, waiting). File uploads run beside passes, sharing
//! their connection's lifetime and cooldown.

use crate::tests::Network;
use crate::*;
use coven_storage::{
    test_utils::{Faults, MemoryRequest},
    ObjectPrefix, Storage, StorageFailure,
};
use std::time::Duration;

/// Each pass lists positions exactly once (SP step 3).
fn passes(device: &crate::tests::Device) -> usize {
    let positions = ObjectPrefix::positions(device.store);
    device
        .storage
        .request_trace()
        .iter()
        .filter(|request| matches!(request, MemoryRequest::List(prefix) if *prefix == positions))
        .count()
}

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

async fn stored_writes(device: &crate::tests::Device) -> usize {
    device
        .storage
        .list(&ObjectPrefix::device_logs(device.store))
        .await
        .unwrap()
        .len()
}

async fn wait_for_writes(device: &crate::tests::Device, count: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while stored_writes(device).await < count {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn a_write_racing_a_pass_retains_one_pass_and_own_commits_retain_none() {
    let network = Network::new(1).await;
    let device = &network.devices[0];
    let writes = stored_writes(device).await;
    let before = passes(device);
    let (started, held) = tokio::sync::oneshot::channel();
    let (resume, holding) = tokio::sync::oneshot::channel();
    device
        .storage
        .hold_next_creation(ObjectPrefix::device_logs(device.store), started, holding)
        .await;
    insert(&device.handle, "first").await;
    held.await.unwrap();
    // Two writes commit while the pass is uploading the first.
    insert(&device.handle, "second").await;
    insert(&device.handle, "third").await;
    resume.send(()).unwrap();
    wait_for_writes(device, writes + 3).await;
    // Retiring uploaded rows and posting positions schedule nothing more.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(passes(device) - before, 2);
    network.close().await;
}

#[tokio::test]
async fn a_stopped_loop_runs_no_pass_until_started_and_close_ends_it() {
    let network = Network::new(1).await;
    let device = &network.devices[0];
    let writes = stored_writes(device).await;
    let mut status = device.handle.subscribe_sync_status();
    device.handle.stop_sync();
    tokio::time::timeout(
        Duration::from_secs(5),
        status.wait_for(|s| matches!(s, SyncStatus::Stopped)),
    )
    .await
    .unwrap()
    .unwrap();
    let before = passes(device);
    insert(&device.handle, "while stopped").await;
    device.handle.sync_now();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(passes(device), before);
    device.handle.start_sync().await.unwrap();
    wait_for_writes(device, writes + 1).await;
    assert_eq!(passes(device) - before, 1);
    // Closing ends the loop: a write committed after stopping is not sent.
    status.borrow_and_update();
    device.handle.stop_sync();
    tokio::time::timeout(
        Duration::from_secs(5),
        status.wait_for(|s| matches!(s, SyncStatus::Stopped)),
    )
    .await
    .unwrap()
    .unwrap();
    insert(&device.handle, "after stopping").await;
    let storage = device.storage.clone();
    let requests = storage.request_trace().len();
    network.close().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(storage.request_trace().len(), requests);
}

/// The sync loop waits for a provider cooldown; requests made meanwhile
/// coalesce into the pass that follows it (SP waiting).
#[tokio::test]
async fn a_retry_after_holds_passes_and_sync_now_coalesces() {
    let network = Network::new(1).await;
    let device = &network.devices[0];
    let mut status = device.handle.subscribe_sync_status();
    status.borrow_and_update();
    device
        .storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            // Two hundred seconds pass in two hundred milliseconds here.
            retry_after: Some(200),
            ..Faults::none()
        })
        .await;
    device.handle.sync_now();
    tokio::time::timeout(
        Duration::from_secs(5),
        status.wait_for(|s| matches!(s, SyncStatus::Failed { .. })),
    )
    .await
    .unwrap()
    .unwrap();
    let requests = device.storage.request_trace().len();
    for _ in 0..3 {
        device.handle.sync_now();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        device.storage.request_trace().len(),
        requests,
        "a pass ran inside the provider cooldown"
    );
    status.borrow_and_update();
    tokio::time::timeout(
        Duration::from_secs(5),
        status.wait_for(|s| matches!(s, SyncStatus::Synced { .. })),
    )
    .await
    .unwrap()
    .unwrap();
    network.close().await;
}

/// One plain table and one carrying app-provided files.
fn file_schema() -> (Vec<SyncedTable>, Vec<Migration>) {
    (
        vec![
            SyncedTable::new("parents", RowIdentity::SharedKey),
            SyncedTable::new("files", RowIdentity::SharedKey).carries_files(FileDecl::new(
                "files",
                Provenance::AppProvided,
                CacheFill::CacheLazy,
            )),
        ],
        vec![Migration::sql(
            1,
            "records",
            "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY, label TEXT NOT NULL UNIQUE, value INTEGER NOT NULL);
             CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY, size INTEGER, hash BLOB, location TEXT);",
        )],
    )
}

async fn file_network() -> Network {
    let (tables, migrations) = file_schema();
    Network::with_schema(coven_storage::CloudProvider::S3, tables, migrations).await
}

async fn attach(handle: &CovenHandle, id: &str, bytes: Vec<u8>) {
    let (file, row) = (id.to_owned(), id.to_owned());
    handle
        .write_with_files(
            move |batch| {
                batch.put_file(
                    "files",
                    file,
                    FileSource::Stream(Box::pin(std::io::Cursor::new(bytes))),
                );
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

/// Creates under the store's file folder this device has sent, held or not.
fn file_creates(device: &crate::tests::Device) -> usize {
    let files = ObjectPrefix::files(device.store);
    device
        .storage
        .request_trace()
        .iter()
        .filter(|request| matches!(request, MemoryRequest::Create(path) if files.contains(path)))
        .count()
}

async fn wait_for_empty_queue(handle: &CovenHandle) {
    let mut uploads = handle.subscribe_uploads();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !uploads.next().await.unwrap().files.is_empty() {}
    })
    .await
    .unwrap();
}

async fn stored(device: &crate::tests::Device, id: &str) -> bool {
    let file = device.handle.file_ref("files", id).await.unwrap();
    let path = coven_storage::ObjectPath::file(device.store, file.uploader(), file.id());
    device.storage.status(&path).await.unwrap().is_some()
}

/// A pass neither waits for an upload in flight nor sends file bytes; the
/// upload's completion schedules no pass (SP, file uploads outside the pass).
#[tokio::test]
async fn a_held_file_upload_delays_no_pass_and_its_completion_schedules_none() {
    let network = file_network().await;
    let device = &network.devices[0];
    let writes = stored_writes(device).await;
    let creates = file_creates(device);
    let (started, held) = tokio::sync::oneshot::channel();
    let (resume, holding) = tokio::sync::oneshot::channel();
    device
        .storage
        .hold_next_creation(ObjectPrefix::files(device.store), started, holding)
        .await;
    attach(&device.handle, "photo", vec![7; 2048]).await;
    held.await.unwrap();
    // A later write uploads and positions post while the file's create waits.
    let posted = |device: &crate::tests::Device| {
        let positions = ObjectPrefix::positions(device.store);
        device
            .storage
            .request_trace()
            .iter()
            .filter(|request| matches!(request, MemoryRequest::Replace(path) if positions.contains(path)))
            .count()
    };
    let before = posted(device);
    insert(&device.handle, "later").await;
    network.sync(0).await;
    wait_for_writes(device, writes + 2).await;
    assert!(
        posted(device) > before,
        "positions waited for the file upload"
    );
    assert_eq!(file_creates(device), creates + 1, "a pass sent file bytes");
    assert!(!stored(device, "photo").await);
    resume.send(()).unwrap();
    wait_for_empty_queue(&device.handle).await;
    assert!(stored(device, "photo").await);
    let after = passes(device);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(passes(device), after, "upload completion scheduled a pass");
    network.close().await;
}

/// Stopping finishes the transfer in flight (E5). While stopped nothing is
/// sent; restarting resumes the queue after the new connection's catch-up.
#[tokio::test]
async fn stopping_finishes_a_held_file_upload_and_restarting_resumes_the_queue() {
    let network = file_network().await;
    let device = &network.devices[0];
    let mut status = device.handle.subscribe_sync_status();
    let creates = file_creates(device);
    let (started, held) = tokio::sync::oneshot::channel();
    let (resume, holding) = tokio::sync::oneshot::channel();
    device
        .storage
        .hold_next_creation(ObjectPrefix::files(device.store), started, holding)
        .await;
    attach(&device.handle, "first", vec![1; 2048]).await;
    held.await.unwrap();
    device.handle.stop_sync();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!matches!(&*status.borrow_and_update(), SyncStatus::Stopped));
    resume.send(()).unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        status.wait_for(|s| matches!(s, SyncStatus::Stopped)),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(stored(device, "first").await);
    attach(&device.handle, "second", vec![2; 2048]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        file_creates(device),
        creates + 1,
        "a stopped connection sent a file"
    );
    device.handle.start_sync().await.unwrap();
    wait_for_empty_queue(&device.handle).await;
    assert!(stored(device, "second").await);
    network.close().await;
}

/// An upload's Retry-After holds every worker of the connection, the sync
/// loop included; both continue once it ends (SP waiting).
#[tokio::test]
async fn an_uploads_retry_after_holds_the_next_pass() {
    let network = file_network().await;
    let device = &network.devices[0];
    device.handle.set_uploads_paused(true);
    attach(&device.handle, "photo", vec![3; 2048]).await;
    network.sync(0).await;
    device
        .storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            // Two hundred seconds pass in two hundred milliseconds here.
            retry_after: Some(200),
            ..Faults::none()
        })
        .await;
    let mut uploads = device.handle.subscribe_uploads();
    device.handle.set_uploads_paused(false);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let queue = uploads.next().await.unwrap();
            if queue.files.iter().any(|file| {
                matches!(file.last_failure.as_deref(),
                    Some(UploadFailure::Storage(error)) if error.failure() == StorageFailure::RateLimited)
            }) {
                break;
            }
        }
    })
    .await
    .unwrap();
    let requests = device.storage.request_trace().len();
    device.handle.sync_now();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        device.storage.request_trace().len(),
        requests,
        "a pass ran inside the upload's provider cooldown"
    );
    wait_for_empty_queue(&device.handle).await;
    assert!(stored(device, "photo").await);
    network.close().await;
}
