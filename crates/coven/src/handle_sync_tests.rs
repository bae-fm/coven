//! Wakes coalesce: a local write racing a pass retains one more pass, the
//! pass's own bookkeeping retains none, and a stopped or closed loop runs
//! nothing (SP step 9, waiting).

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
