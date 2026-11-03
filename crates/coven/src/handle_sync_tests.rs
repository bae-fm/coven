//! Wakes coalesce: a local write racing a pass retains one more pass, the
//! pass's own bookkeeping retains none, and a stopped or closed loop runs
//! nothing (§6.7). File uploads run beside passes, sharing
//! their connection's lifetime and cooldown.

use crate::tests::{
    attach, file_network, file_stored, upload_path, wait_for_empty_queue, wait_for_sent, Network,
};
use crate::*;
use coven_database::test_utils::Followed;
use coven_storage::{
    test_utils::{Faults, MemoryRequest},
    ObjectPrefix, Storage, StorageFailure,
};
use std::time::Duration;

/// Each pass reads forward to a missing slot, then reads the floor (§6.1
/// and §6.2). A claim's own floor read follows its create instead.
fn passes(device: &crate::tests::Device) -> usize {
    let floor = coven_storage::ObjectPath::floor(device.store);
    device
        .storage
        .request_trace()
        .windows(2)
        .filter(|pair| {
            matches!(&pair[0], MemoryRequest::Read(path, None) if path.slot_number().is_some())
                && matches!(&pair[1], MemoryRequest::Read(path, None) if *path == floor)
        })
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

#[tokio::test]
async fn a_write_racing_a_pass_retains_one_pass_and_own_commits_retain_none() {
    let network = Network::new(1).await;
    let device = &network.devices[0];
    let before = passes(device);
    let (started, held) = tokio::sync::oneshot::channel();
    let (resume, holding) = tokio::sync::oneshot::channel();
    device.storage.hold_next_creation(ObjectPrefix::all(device.store), started, holding).await;
    insert(&device.handle, "first").await;
    held.await.unwrap();
    // Two writes commit while the pass is claiming a slot for the first.
    insert(&device.handle, "second").await;
    insert(&device.handle, "third").await;
    resume.send(()).unwrap();
    wait_for_sent(&device.handle).await;
    // The racing writes retain one more pass, which claims their slot.
    let counted = || passes(device).saturating_sub(before);
    tokio::time::timeout(Duration::from_secs(10), async {
        while counted() < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the retained pass never ran: {} passes", counted()));
    // The retained pass claimed a slot, so the state shows synced again
    // only once it ends.
    let mut status = device.handle.test_sync_status();
    status.wait_for(|s| s.state == SyncState::Synced).await;
    // Applying its own slots schedules nothing more: no third pass follows
    // the retained one.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(counted(), 2);
    network.close().await;
}

#[tokio::test]
async fn a_stopped_loop_runs_no_pass_until_started_and_close_ends_it() {
    let network = Network::new(1).await;
    let device = &network.devices[0];
    device.stop_sync().await;
    let before = passes(device);
    insert(&device.handle, "while stopped").await;
    device.handle.sync_now();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(passes(device), before);
    device.handle.start_sync().await.unwrap();
    wait_for_sent(&device.handle).await;
    assert_eq!(passes(device) - before, 1);
    // Closing ends the loop: a write committed after stopping is not sent.
    device.stop_sync().await;
    insert(&device.handle, "after stopping").await;
    let storage = device.storage.clone();
    let requests = storage.request_trace().len();
    network.close().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(storage.request_trace().len(), requests);
}

/// The sync loop waits for a provider cooldown; requests made meanwhile
/// coalesce into the pass that follows it (§6.7).
#[tokio::test]
async fn a_retry_after_holds_passes_and_sync_now_coalesces() {
    let network = Network::new(1).await;
    let device = &network.devices[0];
    let mut last = device.handle.test_last_pass();
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
    last.wait_for(|last| matches!(last, Some(LastPass::Failed { .. }))).await;
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
    last.wait_for(|last| *last == Some(LastPass::Synced)).await;
    network.close().await;
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

/// Slots this device has claimed, each carrying the writes one pass sent.
fn slot_creates(device: &crate::tests::Device) -> usize {
    device
        .storage
        .request_trace()
        .iter()
        .filter(|request| matches!(request, MemoryRequest::Create(path) if path.slot_number().is_some()))
        .count()
}

/// A pass neither waits for an upload in flight nor sends file bytes. Once the
/// file is stored, the write naming it in its row goes out in a later pass of
/// its own (§6.6).
#[tokio::test]
async fn a_held_file_upload_delays_no_pass_and_a_later_pass_sends_its_completion() {
    let network = file_network().await;
    let device = &network.devices[0];
    let creates = file_creates(device);
    let (started, held) = tokio::sync::oneshot::channel();
    let (resume, holding) = tokio::sync::oneshot::channel();
    device.storage.hold_next_creation(ObjectPrefix::files(device.store), started, holding).await;
    let photo = attach(&device.handle, "photo", vec![7; 2048]).await;
    held.await.unwrap();
    // A later write claims its slot while the file's create waits.
    insert(&device.handle, "later").await;
    network.sync(0).await;
    wait_for_sent(&device.handle).await;
    assert_eq!(file_creates(device), creates + 1, "a pass sent file bytes");
    assert!(!file_stored(device, photo).await);
    let slots = slot_creates(device);
    resume.send(()).unwrap();
    wait_for_empty_queue(&device.handle).await;
    assert!(file_stored(device, photo).await);
    wait_for_sent(&device.handle).await;
    assert_eq!(slot_creates(device), slots + 1, "one pass sent the write naming the stored file");
    network.close().await;
}

/// Stopping finishes the transfer in flight (E5). While stopped nothing is
/// sent; restarting resumes the queue after the new connection's catch-up.
#[tokio::test]
async fn stopping_finishes_a_held_file_upload_and_restarting_resumes_the_queue() {
    let network = file_network().await;
    let device = &network.devices[0];
    let creates = file_creates(device);
    let (started, held) = tokio::sync::oneshot::channel();
    let (resume, holding) = tokio::sync::oneshot::channel();
    device.storage.hold_next_creation(ObjectPrefix::files(device.store), started, holding).await;
    let first = attach(&device.handle, "first", vec![1; 2048]).await;
    held.await.unwrap();
    device.handle.stop_sync().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Stopping has not cut the upload in flight: it is still being sent.
    assert_eq!(device.handle.sync_status().await.unwrap().state, SyncState::Syncing);
    resume.send(()).unwrap();
    // The release waits for the upload, then drops the provider's client.
    device.connector.clients.released().await;
    assert!(file_stored(device, first).await);
    let second = attach(&device.handle, "second", vec![2; 2048]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(file_creates(device), creates + 1, "a stopped connection sent a file");
    device.handle.start_sync().await.unwrap();
    wait_for_empty_queue(&device.handle).await;
    assert!(file_stored(device, second).await);
    network.close().await;
}

/// An upload's Retry-After holds every worker of the connection, the sync
/// loop included; both continue once it ends (§6.7).
#[tokio::test]
async fn an_uploads_retry_after_holds_the_next_pass() {
    let network = file_network().await;
    let device = &network.devices[0];
    device.handle.set_uploads_paused(true).await.unwrap();
    let photo = attach(&device.handle, "photo", vec![3; 2048]).await;
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
    let mut pending = subscribe_records(&device.handle);
    device.handle.set_uploads_paused(false).await.unwrap();
    // The upload's 429 is its file's record.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !pending.next().await.expect("the live query answers").unwrap().iter().any(|record| {
            matches!(record.subject, PendingSubject::Upload { .. })
                && matches!(record.reason, PendingReason::Storage { failure, .. } if failure == StorageFailure::RateLimited)
        }) {}
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
    assert!(file_stored(device, photo).await);
    network.close().await;
}

/// One bad photo while the rest syncs leaves the last pass `Synced`, and
/// its upload's pending record names the state (E5): a provider's refusal
/// of one subject does not fail the pass.
#[tokio::test]
async fn a_refused_file_upload_leaves_the_pass_synced_with_its_pending_record() {
    let network = file_network().await;
    let device = &network.devices[0];
    device.handle.set_uploads_paused(true).await.unwrap();
    let photo = attach(&device.handle, "photo", vec![3; 2048]).await;
    network.sync(0).await;
    let path = upload_path(device, photo).await;
    let mut faults = Faults::none();
    faults.create_failures.insert(path, (StorageFailure::QuotaExceeded, 507));
    device.storage.set_faults(faults).await;
    let refused = |records: &[PendingRecord]| {
        records.iter().any(|record| {
            matches!(record.subject, PendingSubject::Upload { .. })
                && matches!(record.reason, PendingReason::Storage { failure, .. } if failure == StorageFailure::QuotaExceeded)
        })
    };
    let mut pending = subscribe_records(&device.handle);
    device.handle.set_uploads_paused(false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !refused(&pending.next().await.expect("the live query answers").unwrap()) {}
    })
    .await
    .unwrap();
    // The fixed clock moves, so the pass's end moves the last synced time.
    network.clock.set(network.clock.now() + Duration::from_secs(1));
    let after = network.clock.now();
    let mut status = device.handle.test_sync_status();
    insert(&device.handle, "the rest").await;
    let settled = status.wait_for(|s| s.last_synced >= Some(after)).await;
    assert_eq!(device.handle.test_last_pass().latest().await, Some(LastPass::Synced));
    assert!(
        matches!(&settled.state, SyncState::StorageFull(SyncCause::Pending(record)) if refused(std::slice::from_ref(record))),
        "{settled:?}"
    );
    wait_for_sent(&device.handle).await;
    assert!(refused(&device.handle.pending().await.unwrap()));
    device.storage.set_faults(Faults::none()).await;
    wait_for_empty_queue(&device.handle).await;
    assert!(!refused(&device.handle.pending().await.unwrap()));
    network.close().await;
}

/// Remove device 1's member, with the provider refusing the store-key
/// rotation that removal requires as `failure`; returns the rotation's path.
async fn refuse_rotation(
    network: &Network,
    failure: StorageFailure,
    status: u16,
) -> coven_storage::ObjectPath {
    let owner = &network.devices[0];
    network.devices[1].handle.stop_sync().await.unwrap();
    let members = owner.handle.members().await.unwrap();
    let removed = members.iter().find(|m| !m.is_self).unwrap().id;
    network.sync(0).await;
    // The removal claims the next slot; the store-key copy of the rotation
    // it requires follows.
    let rotation = coven_storage::ObjectPath::slot(
        owner.store,
        (owner.handle.applied_slot().await + 2).try_into().unwrap(),
    );
    let mut faults = Faults::none();
    faults.create_failures.insert(rotation.clone(), (failure, status));
    owner.storage.set_faults(faults).await;
    owner.handle.remove_member(&removed).await.unwrap();
    rotation
}

/// Waits until the last pass reports the failing rotation.
async fn rotation_failed(last: &mut Followed<Option<LastPass>>) {
    last.wait_for(|last| {
        matches!(last, Some(LastPass::Failed { failure: StatusFailure::Operation(record), .. })
            if matches!(record.subject, PendingSubject::Operation { kind: OperationKind::RotateKey, .. }))
    })
    .await;
}

/// Coven's own rotation that the provider refuses for want of space is
/// never shown as `Synced`: the pass reports it while it retries itself, and
/// the last pass turns `Synced` once a retry succeeds, with no app call (E5).
#[tokio::test]
async fn a_refused_rotation_is_reported_until_it_retries_itself() {
    let network = Network::new(2).await;
    let owner = &network.devices[0];
    let rotation = refuse_rotation(&network, StorageFailure::QuotaExceeded, 507).await;
    // On the polling clock the rotation's first retry follows its refusal by
    // a millisecond and replaces the report before a busy test can see it.
    // Its waits now end when the test moves the fixed clock.
    network.polling.hold_waits();
    let mut last = owner.handle.test_last_pass();
    owner.handle.sync_now();
    rotation_failed(&mut last).await;
    owner.storage.set_faults(Faults::none()).await;
    // The first retry waits a second.
    network.clock.set(network.clock.now() + Duration::from_secs(1));
    owner.handle.sync_now();
    last.wait_for(|last| *last == Some(LastPass::Synced)).await;
    assert!(owner.storage.status(&rotation).await.unwrap().is_some());
    assert!(!owner.handle.pending().await.unwrap().iter().any(|record| matches!(
        record.subject,
        PendingSubject::Operation { kind: OperationKind::RotateKey, .. }
    )));
    network.close().await;
}

/// A rotation the provider refuses outright never clears: sync stops, the
/// last pass stays `Failed` with its record, the state shows it failed, and
/// no later pass runs (E5).
#[tokio::test]
async fn a_rotation_refused_outright_stalls_sync() {
    let network = Network::new(2).await;
    let owner = &network.devices[0];
    refuse_rotation(&network, StorageFailure::Refused, 400).await;
    let mut last = owner.handle.test_last_pass();
    owner.handle.sync_now();
    rotation_failed(&mut last).await;
    let before = passes(owner);
    owner.handle.sync_now();
    insert(&owner.handle, "after the stall").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(passes(owner), before);
    assert!(matches!(
        owner.handle.test_last_pass().latest().await,
        Some(LastPass::Failed { failure: StatusFailure::Operation(record), .. }) if record.retry() == Retry::Never
    ));
    assert!(matches!(
        owner.handle.test_sync_status().latest().await.state,
        SyncState::Failed(SyncCause::Pass(LastPass::Failed { .. }))
    ));
    network.close().await;
}

/// The failing request's storage failure, for either failed last pass.
fn storage_failure(last: &Option<LastPass>) -> Option<StorageFailure> {
    match last {
        Some(
            LastPass::Offline { failure, .. }
            | LastPass::Failed { failure: StatusFailure::Storage(failure), .. },
        ) => Some(*failure),
        _ => None,
    }
}

/// Request a pass and return the first last pass from the current one on
/// that `expected` accepts, which must name a storage failure. Callers
/// expect a last pass the current one is not, so it comes from the
/// requested pass.
async fn attempt(
    device: &crate::tests::Device,
    expected: impl Fn(&Option<LastPass>) -> bool,
) -> Option<LastPass> {
    let mut last = device.handle.test_last_pass();
    device.handle.sync_now();
    let last = last.wait_for(expected).await;
    assert!(storage_failure(&last).is_some(), "a storage failure: {last:?}");
    last
}

/// The last pass describes the latest attempt, not the connection's history
/// (E5): no response is Offline, a provider's error response is Failed.
#[tokio::test]
async fn each_attempt_is_offline_or_failed_by_whether_storage_answered() {
    let network = Network::new(1).await;
    let device = &network.devices[0];
    // Ana syncs, then loses Wi-Fi: the next attempt is Offline.
    device.storage.set_online(false);
    // Offline means no provider response.
    let offline = attempt(device, |s| matches!(s, Some(LastPass::Offline { .. }))).await;
    assert_eq!(storage_failure(&offline), Some(StorageFailure::Network));
    device.storage.set_online(true);
    // A provider outage is classified Network, but storage answered.
    device
        .storage
        .set_faults(Faults {
            fail_next: usize::MAX,
            failure: StorageFailure::Network,
            response: Some(503),
            ..Faults::none()
        })
        .await;
    // Failed with a storage failure means storage answered.
    let outage = attempt(device, |s| matches!(s, Some(LastPass::Failed { .. }))).await;
    assert_eq!(storage_failure(&outage), Some(StorageFailure::Network));
    // A denied read fails the pass too.
    device
        .storage
        .set_faults(Faults {
            fail_next: usize::MAX,
            failure: StorageFailure::PermissionDenied,
            response: Some(403),
            ..Faults::none()
        })
        .await;
    let denied = attempt(device, |s| {
        storage_failure(s).is_some_and(|failure| failure == StorageFailure::PermissionDenied)
    })
    .await;
    assert!(matches!(denied, Some(LastPass::Failed { .. })), "{denied:?}");
    // An earlier answer does not make a later unanswered attempt Failed.
    device.storage.set_faults(Faults::none()).await;
    device.storage.set_online(false);
    // Permission failures require app action; a new connection permits a retry.
    device.stop_sync().await;
    device.handle.start_sync().await.unwrap();
    attempt(device, |s| matches!(s, Some(LastPass::Offline { .. }))).await;
    device.storage.set_online(true);
    let mut last = device.handle.test_last_pass();
    device.handle.sync_now();
    last.wait_for(|last| *last == Some(LastPass::Synced)).await;
    network.close().await;
}

/// A pass that reached storage, then lost the network, is Offline: its own
/// earlier responses do not decide the request that failed.
#[tokio::test]
async fn a_pass_losing_the_network_after_reaching_storage_is_offline() {
    let network = Network::new(1).await;
    let device = &network.devices[0];
    let (listed, listing) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    // The pass reads the floor, then claims a slot for this write.
    device
        .storage
        .hold_next_read(coven_storage::ObjectPath::floor(device.store), listed, resumed)
        .await;
    let mut last = device.handle.test_last_pass();
    let before = last.latest().await;
    insert(&device.handle, "offline").await;
    listing.await.unwrap();
    device.storage.set_online(false);
    resume.send(()).unwrap();
    let settled = last.wait_for(|last| *last != before).await;
    assert!(matches!(settled, Some(LastPass::Offline { .. })), "{settled:?}");
    network.close().await;
}

/// The connection's persisted wait, once `ready` holds for it.
async fn connection_wait(
    device: &crate::tests::Device,
    ready: impl Fn(&coven_database::RetryWait) -> bool,
) -> coven_database::RetryWait {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(wait) = device.handle.test_connection_retry_wait().await {
                if ready(&wait) {
                    return wait;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the connection's wait")
}

/// Ana's phone locked on a train: every pass found no network and backed off.
/// Once it unlocks, `sync_now` still waits out the backoff, but the app's
/// network report ends it and a pass runs at once (§6.7).
#[tokio::test]
async fn a_network_report_ends_an_offline_backoff_and_a_pass_runs() {
    let network = Network::new(1).await;
    let device = &network.devices[0];
    // Waits end only when the test moves the fixed clock, which it never does.
    network.polling.hold_waits();
    device.storage.set_online(false);
    attempt(device, |s| matches!(s, Some(LastPass::Offline { .. }))).await;
    let wait = connection_wait(device, |_| true).await;
    assert_eq!(wait.failures, 1);
    assert!(!wait.wait.is_zero());
    device.storage.set_online(true);
    let requests = device.storage.request_trace().len();
    for _ in 0..3 {
        device.handle.sync_now();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        device.storage.request_trace().len(),
        requests,
        "sync_now ran a pass inside the connection backoff"
    );
    let mut last = device.handle.test_last_pass();
    device.handle.network_changed();
    last.wait_for(|last| *last == Some(LastPass::Synced)).await;
    assert_eq!(device.handle.test_connection_retry_wait().await, None);
    network.close().await;
}

/// A provider that answered asked for the wait: a new network changes
/// neither its Retry-After nor the backoff after its answer (§6.7).
#[tokio::test]
async fn a_network_report_leaves_a_provider_cooldown_and_an_answered_backoff() {
    let network = Network::new(1).await;
    let device = &network.devices[0];
    network.polling.hold_waits();
    device
        .storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            retry_after: Some(200),
            ..Faults::none()
        })
        .await;
    attempt(device, |s| matches!(s, Some(LastPass::Failed { .. }))).await;
    let wait = connection_wait(device, |_| true).await;
    let requests = device.storage.request_trace().len();
    device.handle.network_changed();
    device.handle.network_changed();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        device.storage.request_trace().len(),
        requests,
        "a network report ended a provider's wait"
    );
    assert_eq!(device.handle.test_connection_retry_wait().await, Some(wait));
    network.close().await;
}

/// Reports while a pass runs coalesce into one: the pass fails for want of
/// a network, the reports end the wait it failed into, and exactly one more
/// pass runs. That pass, still offline, waits out its backoff (§6.7).
#[tokio::test]
async fn network_reports_during_a_pass_coalesce_into_one_more_pass() {
    let network = Network::new(1).await;
    let device = &network.devices[0];
    network.polling.hold_waits();
    let (read, reading) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    device
        .storage
        .hold_next_read(coven_storage::ObjectPath::floor(device.store), read, resumed)
        .await;
    // The pass reads the floor, then claims a slot for this write.
    insert(&device.handle, "offline").await;
    reading.await.unwrap();
    for _ in 0..3 {
        device.handle.network_changed();
    }
    device.storage.set_online(false);
    resume.send(()).unwrap();
    // The first failure's wait ended; the second pass's failure waits.
    let wait = connection_wait(device, |wait| wait.failures == 2).await;
    assert!(!wait.wait.is_zero());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(device.handle.test_connection_retry_wait().await, Some(wait));
    assert!(matches!(
        device.handle.test_last_pass().latest().await,
        Some(LastPass::Offline { .. })
    ));
    network.close().await;
}

/// Android stopped the app while Ana's phone was locked. The reopened store
/// keeps the backoff, and her report still ends it while connecting: the
/// connection's pending record says no provider answered (§6.7).
#[tokio::test]
async fn a_network_report_ends_an_offline_backoff_kept_across_reopening() {
    let mut network = Network::new(1).await;
    network.polling.hold_waits();
    let device = &network.devices[0];
    device.storage.set_online(false);
    attempt(device, |s| matches!(s, Some(LastPass::Offline { .. }))).await;
    let wait = connection_wait(device, |_| true).await;
    device.handle.close().await.unwrap();
    device.storage.set_online(true);
    let handle = network.reopen(device).open(device.store, |_| ()).await.unwrap();
    network.devices[0].handle = handle;
    let device = &network.devices[0];
    let requests = device.storage.request_trace().len();
    device.handle.start_sync().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(device.storage.request_trace().len(), requests, "reopening discarded the backoff");
    assert_eq!(device.handle.test_connection_retry_wait().await, Some(wait));
    let mut last = device.handle.test_last_pass();
    device.handle.network_changed();
    last.wait_for(|last| *last == Some(LastPass::Synced)).await;
    assert_eq!(device.handle.test_connection_retry_wait().await, None);
    network.close().await;
}

/// Ana's photo was uploading when her phone locked: the upload found no
/// network and waits its own backoff. Once she unlocks, the network report
/// ends that wait too, and the photo uploads at once (§6.7).
#[tokio::test]
async fn a_network_report_ends_an_offline_upload_backoff() {
    let network = file_network().await;
    let device = &network.devices[0];
    device.handle.set_uploads_paused(true).await.unwrap();
    let photo = attach(&device.handle, "photo", vec![3; 2048]).await;
    network.sync(0).await;
    network.polling.hold_waits();
    device.storage.set_online(false);
    let offline = |records: &[PendingRecord]| {
        records.iter().any(|record| {
            matches!(record.subject, PendingSubject::Upload { .. })
                && matches!(
                    record.reason,
                    PendingReason::Storage {
                        failure: StorageFailure::Network,
                        answered: false,
                        ..
                    }
                )
        })
    };
    let mut pending = subscribe_records(&device.handle);
    device.handle.set_uploads_paused(false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !offline(&pending.next().await.expect("the live query answers").unwrap()) {}
    })
    .await
    .unwrap();
    device.storage.set_online(true);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!file_stored(device, photo).await, "the upload skipped its backoff");
    device.handle.network_changed();
    wait_for_empty_queue(&device.handle).await;
    assert!(file_stored(device, photo).await);
    network.close().await;
}

/// Ana's photo upload got a server error, so it waits its own backoff and a
/// network report leaves it alone. She pulls to refresh: `retry_now` ends
/// that wait too, and the photo uploads at once (§6.7).
#[tokio::test]
async fn retry_now_ends_an_answered_upload_backoff() {
    let network = file_network().await;
    let device = &network.devices[0];
    device.handle.set_uploads_paused(true).await.unwrap();
    let photo = attach(&device.handle, "photo", vec![4; 2048]).await;
    network.sync(0).await;
    network.polling.hold_waits();
    device
        .storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::Network,
            response: Some(503),
            ..Faults::none()
        })
        .await;
    let answered = |records: &[PendingRecord]| {
        records.iter().any(|record| {
            matches!(record.subject, PendingSubject::Upload { .. })
                && matches!(record.reason, PendingReason::Storage { answered: true, .. })
        })
    };
    let mut pending = subscribe_records(&device.handle);
    device.handle.set_uploads_paused(false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !answered(&pending.next().await.expect("the live query answers").unwrap()) {}
    })
    .await
    .unwrap();
    device.handle.network_changed();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!file_stored(device, photo).await, "a network report ended an answered backoff");
    device.handle.retry_now().await.unwrap();
    wait_for_empty_queue(&device.handle).await;
    assert!(file_stored(device, photo).await);
    network.close().await;
}

/// Ana's pending list is one live query over `coven_pending` and her own
/// rows. Each row arrives with its error's text in `cause`, written with
/// the record (E4, E5).
#[tokio::test]
async fn a_view_record_arrives_with_its_cause() {
    let network = file_network().await;
    let device = &network.devices[0];
    device.handle.set_uploads_paused(true).await.unwrap();
    let photo = attach(&device.handle, "photo", vec![3; 2048]).await;
    network.sync(0).await;
    let path = upload_path(device, photo).await;
    let mut faults = Faults::none();
    faults.create_failures.insert(path, (StorageFailure::QuotaExceeded, 507));
    device.storage.set_faults(faults).await;
    let mut list = device.handle.subscribe(|sql| {
        Ok(sql.query(
            "SELECT e.record,e.cause,f.id FROM coven_pending AS e JOIN files AS f ON f.file=e.file",
            [],
            |row| {
                Ok((
                    row.get::<_, PendingRecord>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?)
    });
    device.handle.set_uploads_paused(false).await.unwrap();
    let (record, cause, id) = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let rows = list.next().await.expect("the live query answers").unwrap();
            if let Some(row) = rows.into_iter().next() {
                break row;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(id, "photo");
    assert!(
        matches!(record.reason, PendingReason::Storage { failure, .. } if failure == StorageFailure::QuotaExceeded)
    );
    let cause = cause.expect("the record arrives with its error's text");
    assert!(cause.contains("Quota"), "{cause}");
    let pending = device.handle.pending().await.unwrap();
    let listed = pending.into_iter().find(|listed| *listed == record).unwrap();
    assert_eq!(listed.cause, Some(cause));
    network.close().await;
}

/// Every pending record as the app's SQL reads it from `coven_pending`,
/// as a live query (E4).
fn subscribe_records(handle: &CovenHandle) -> LiveQuery<Vec<PendingRecord>> {
    handle.subscribe(|sql| {
        Ok(sql.query("SELECT record FROM coven_pending ORDER BY position", [], |r| {
            r.get::<_, PendingRecord>(0)
        })?)
    })
}

/// The sync state an app shows (§19.1, E5), as `subscribe_sync_status`
/// follows it: synced means data and files are in storage, a queued file
/// names what it waits for, one whose original is missing names failed,
/// offline with a file or a write queued is offline with unsent changes, a
/// location without credentials is signed out, and the last synced time
/// survives every state and every reopen without going backward.
mod state {
    use crate::tests::{upload_path, wait_for_sent, Device, Network};
    use crate::*;
    use coven_storage::test_utils::Faults;
    use std::time::Duration;

    fn tables() -> Vec<SyncedTable> {
        vec![
            SyncedTable::new("covers", RowIdentity::SharedKey)
                .file_column(FileColumn::new("file").bound()),
            SyncedTable::new("photos", RowIdentity::SharedKey).file_column(FileColumn::new("file")),
            SyncedTable::new("notes", RowIdentity::SharedKey),
        ]
    }

    const SCHEMA: &str = "CREATE TABLE covers(id TEXT NOT NULL PRIMARY KEY, file TEXT);
        CREATE TABLE photos(id TEXT NOT NULL PRIMARY KEY, file TEXT);
        CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);";

    /// Ana's Household store on S3.
    async fn household() -> Network {
        Network::with_schema(
            CloudProvider::S3,
            tables(),
            vec![Migration::addition(1, "covers", SCHEMA)],
        )
        .await
    }

    /// Stage `bytes` in the app's `files` namespace for the cloud, in row `id`
    /// of `table`.
    async fn stage(device: &Device, table: &'static str, id: &str, bytes: Vec<u8>) -> FileId {
        let id = id.to_owned();
        device
            .handle
            .test_staged_write(
                move |batch| Ok(batch.put_file("files", bytes, Place::Cloud)),
                move |sql, file| {
                    sql.execute(
                        &format!("INSERT INTO {table}(id,file) VALUES(?1,?2)"),
                        (&id, file),
                    )?;
                    Ok(file)
                },
            )
            .await
            .unwrap()
    }

    async fn state(device: &Device, expected: impl Fn(&SyncState) -> bool) -> SyncStatus {
        device.handle.test_sync_status().wait_for(|status| expected(&status.state)).await
    }

    /// Ana pauses uploads and takes a photo: its row goes out with the photo on
    /// her phone only, so she waits to sync, on the pause. Once she resumes,
    /// the photo uploads and she is synced (§17.4).
    #[tokio::test]
    async fn a_paused_photo_waits_to_sync_until_it_uploads() {
        let network = household().await;
        let ana = &network.devices[0];
        ana.handle.set_uploads_paused(true).await.unwrap();
        stage(ana, "photos", "beach", vec![7; 4096]).await;
        wait_for_sent(&ana.handle).await;
        state(ana, |state| *state == SyncState::WaitingToSync(UploadWait::Paused)).await;
        ana.handle.set_uploads_paused(false).await.unwrap();
        state(ana, |state| *state == SyncState::Synced).await;
        assert!(ana.handle.uploads().await.unwrap().files.is_empty());
        network.close().await;
    }

    /// A cover is bound, so the pause doesn't hold it. While sync is stopped it
    /// waits on that; once a server error fails its upload, it waits on its own
    /// backoff, paused or not, and the error names nothing of its own (§17.4,
    /// §6.7).
    #[tokio::test]
    async fn a_bound_file_waits_on_its_retry_while_uploads_are_paused() {
        let network = household().await;
        let ana = &network.devices[0];
        ana.handle.stop_sync().await.unwrap();
        ana.handle.set_uploads_paused(true).await.unwrap();
        let cover = stage(ana, "covers", "album", vec![6; 4096]).await;
        state(ana, |state| *state == SyncState::WaitingToSync(UploadWait::NotSyncing)).await;
        let mut faults = Faults::none();
        faults
            .create_failures
            .insert(upload_path(ana, cover).await, (StorageFailure::Network, 503));
        ana.storage.set_faults(faults).await;
        ana.handle.start_sync().await.unwrap();
        let waiting = state(ana, |state| {
            matches!(state, SyncState::WaitingToSync(UploadWait::Retrying { .. }))
        })
        .await;
        let SyncState::WaitingToSync(UploadWait::Retrying { at }) = waiting.state else {
            unreachable!("waited for a retry")
        };
        assert!(at > network.clock.now(), "it retries after its backoff: {at:?}");
        ana.storage.set_faults(Faults::none()).await;
        ana.handle.retry_uploads_now().await.unwrap();
        state(ana, |state| *state == SyncState::Synced).await;
        network.close().await;
    }

    /// Dan's track sits on a drive he unplugs before it uploads: his state is
    /// failed, naming the track's move, and stays so while the track is gone.
    /// Once the drive is back and the track uploads, he is synced (§17.3).
    #[tokio::test]
    async fn a_missing_original_fails_naming_its_move_until_it_uploads() {
        let network = household().await;
        let dan = &network.devices[0];
        let drive = tempfile::tempdir().unwrap();
        let path = drive.path().join("track.flac");
        std::fs::write(&path, vec![9; 4096]).unwrap();
        let prepared = prepare_user_file(&path, |_| {}).await.unwrap();
        dan.handle.set_uploads_paused(true).await.unwrap();
        dan.handle
            .write(move |sql| {
                let track = sql.put_user_file("tracks", prepared, Place::Cloud)?;
                sql.execute("INSERT INTO photos(id,file) VALUES('track',?1)", [track])?;
                Ok(())
            })
            .await
            .unwrap();
        let aside = drive.path().join("unplugged");
        std::fs::rename(&path, &aside).unwrap();
        dan.handle.set_uploads_paused(false).await.unwrap();
        let failed = state(dan, |state| matches!(state, SyncState::Failed(_))).await;
        let SyncState::Failed(SyncCause::Pending(record)) = failed.state else {
            panic!("the move's record names failed: {failed:?}");
        };
        assert!(matches!(record.subject, PendingSubject::Move { .. }), "{record:?}");
        assert_eq!(
            record.reason,
            PendingReason::Original { path: path.clone(), failure: FileSourceFailure::Missing }
        );
        std::fs::rename(&aside, &path).unwrap();
        dan.handle.retry_uploads_now().await.unwrap();
        state(dan, |state| *state == SyncState::Synced).await;
        network.close().await;
    }

    /// Ben's phone loses its network: his state is offline. He takes a photo,
    /// and his state is offline with unsent changes, with no pass reaching
    /// storage; the last synced time stays where it was (§19.1).
    #[tokio::test]
    async fn a_photo_taken_offline_shows_unsent_changes() {
        let network = household().await;
        let ben = &network.devices[0];
        let synced = ben.handle.sync_status().await.unwrap().last_synced;
        assert!(synced.is_some(), "setup and its passes synced");
        ben.storage
            .set_faults(Faults {
                fail_next: usize::MAX,
                failure: StorageFailure::Network,
                response: None,
                ..Faults::none()
            })
            .await;
        network.clock.set(network.clock.now() + Duration::from_secs(1));
        let answer = ben.handle.retry_now().await.unwrap().await;
        assert!(matches!(answer, RetryOutcome::Failed(PassFailure::Offline { .. })), "{answer:?}");
        let offline = state(ben, |state| matches!(state, SyncState::Offline(_))).await;
        assert_eq!(offline.last_synced, synced);
        stage(ben, "photos", "train", vec![3; 4096]).await;
        let unsent =
            state(ben, |state| matches!(state, SyncState::OfflineWithUnsentChanges(_))).await;
        assert_eq!(unsent.last_synced, synced);
        network.close().await;
    }

    /// A store with no storage set up is not synced from its first value. One
    /// whose credentials are gone from the keychain is signed out from its
    /// first value on reopening, naming its provider with no cause, and keeps
    /// the last synced time, which a clock set back never moves back (§19.1).
    #[tokio::test]
    async fn the_first_value_is_the_state_and_the_last_synced_time_is_kept() {
        let app = TestCoven::new();
        let root = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(root.path().into());
        let directory =
            app.create_closed_store(&layout, "Notes", std::sync::Arc::new(UuidIds)).await.unwrap();
        let alone = app
            .builder(layout)
            .synced_tables(tables())
            .migrations(vec![Migration::addition(1, "covers", SCHEMA)])
            .open(directory.id(), |_| ())
            .await
            .unwrap();
        assert_eq!(
            alone.test_sync_status().next().await,
            SyncStatus { state: SyncState::NotSynced, last_synced: None }
        );
        alone.close().await.unwrap();

        let mut network = household().await;
        let synced = network.devices[0].handle.sync_status().await.unwrap().last_synced;
        network.clock.set(network.clock.now() - Duration::from_secs(500));
        network.pass(0).await;
        assert_eq!(network.devices[0].handle.sync_status().await.unwrap().last_synced, synced);
        let device = &network.devices[0];
        device.handle.close().await.unwrap();
        device.app.lose_storage_credentials(device.store).unwrap();
        let reopened = network.reopen(device).open(device.store, |_| ()).await.unwrap();
        assert_eq!(
            reopened.test_sync_status().next().await,
            SyncStatus {
                state: SyncState::SignedOut { provider: CloudProvider::S3, cause: None },
                last_synced: synced,
            }
        );
        network.devices[0].handle = reopened;
        network.close().await;
    }
}
