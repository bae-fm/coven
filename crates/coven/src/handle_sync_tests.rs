//! Wakes coalesce: a local write racing a pass retains one more pass, the
//! pass's own bookkeeping retains none, and a stopped or closed loop runs
//! nothing (§6.7). File uploads run beside passes, sharing
//! their connection's lifetime and cooldown.

use crate::tests::{
    attach, file_network, file_stored, wait_for_empty_queue, wait_for_sent, Network,
};
use crate::*;
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
    device
        .storage
        .hold_next_creation(ObjectPrefix::all(device.store), started, holding)
        .await;
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
    let mut status = device.handle.subscribe_sync_status();
    tokio::time::timeout(
        Duration::from_secs(10),
        status.wait_for(|s| matches!(s, SyncStatus::Synced { .. })),
    )
    .await
    .unwrap()
    .unwrap();
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
    wait_for_sent(&device.handle).await;
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
/// coalesce into the pass that follows it (§6.7).
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
    device
        .storage
        .hold_next_creation(ObjectPrefix::files(device.store), started, holding)
        .await;
    attach(&device.handle, "photo", vec![7; 2048]).await;
    held.await.unwrap();
    // A later write claims its slot while the file's create waits.
    insert(&device.handle, "later").await;
    network.sync(0).await;
    wait_for_sent(&device.handle).await;
    assert_eq!(file_creates(device), creates + 1, "a pass sent file bytes");
    assert!(!file_stored(device, "photo").await);
    let slots = slot_creates(device);
    resume.send(()).unwrap();
    wait_for_empty_queue(&device.handle).await;
    assert!(file_stored(device, "photo").await);
    wait_for_sent(&device.handle).await;
    assert_eq!(
        slot_creates(device),
        slots + 1,
        "one pass sent the write naming the stored file"
    );
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
    assert!(file_stored(device, "first").await);
    attach(&device.handle, "second", vec![2; 2048]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        file_creates(device),
        creates + 1,
        "a stopped connection sent a file"
    );
    device.handle.start_sync().await.unwrap();
    wait_for_empty_queue(&device.handle).await;
    assert!(file_stored(device, "second").await);
    network.close().await;
}

/// An upload's Retry-After holds every worker of the connection, the sync
/// loop included; both continue once it ends (§6.7).
#[tokio::test]
async fn an_uploads_retry_after_holds_the_next_pass() {
    let network = file_network().await;
    let device = &network.devices[0];
    device.handle.set_uploads_paused(true).await.unwrap();
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
    let mut pending = device.handle.subscribe_pending();
    device.handle.set_uploads_paused(false).await.unwrap();
    // The upload's 429 is its file's record.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !pending.next().await.unwrap().iter().any(|record| {
            matches!(record.subject, PendingSubject::File { .. })
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
    assert!(file_stored(device, "photo").await);
    network.close().await;
}

/// One bad photo while the rest syncs leaves `Synced` with a pending file
/// record (E5): a provider's refusal of one subject does not fail the pass.
#[tokio::test]
async fn a_refused_file_upload_leaves_the_pass_synced_with_its_pending_record() {
    let network = file_network().await;
    let device = &network.devices[0];
    device.handle.set_uploads_paused(true).await.unwrap();
    attach(&device.handle, "photo", vec![3; 2048]).await;
    network.sync(0).await;
    let file = device.handle.file_ref("files", "photo").await.unwrap();
    let path = coven_storage::ObjectPath::file(device.store, file.uploader(), file.id());
    let mut faults = Faults::none();
    faults
        .create_failures
        .insert(path, (StorageFailure::QuotaExceeded, 507));
    device.storage.set_faults(faults).await;
    let refused = |records: &[PendingRecord]| {
        records.iter().any(|record| {
            matches!(record.subject, PendingSubject::File { .. })
                && matches!(record.reason, PendingReason::Storage { failure, .. } if failure == StorageFailure::QuotaExceeded)
        })
    };
    let mut pending = device.handle.subscribe_pending();
    device.handle.set_uploads_paused(false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !refused(&pending.next().await.unwrap()) {}
    })
    .await
    .unwrap();
    let mut status = device.handle.subscribe_sync_status();
    status.borrow_and_update();
    insert(&device.handle, "the rest").await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            status.changed().await.unwrap();
            if !matches!(&*status.borrow(), SyncStatus::Syncing) {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(
        matches!(&*status.borrow(), SyncStatus::Synced { .. }),
        "{:?}",
        &*status.borrow()
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
    network.devices[1].handle.stop_sync();
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
    faults
        .create_failures
        .insert(rotation.clone(), (failure, status));
    owner.storage.set_faults(faults).await;
    owner.handle.remove_member(&removed).await.unwrap();
    rotation
}

/// Waits until status reports the failing rotation.
async fn rotation_failed(status: &mut tokio::sync::watch::Receiver<SyncStatus>) {
    tokio::time::timeout(
        Duration::from_secs(15),
        status.wait_for(|s| {
            matches!(s, SyncStatus::Failed { error: SyncFailure::Operation(record) }
                if matches!(record.subject, PendingSubject::Operation { kind: OperationKind::RotateKey, .. }))
        }),
    )
    .await
    .unwrap()
    .unwrap();
}

/// Coven's own rotation that the provider refuses for want of space is
/// never shown as `Synced`: the pass reports it while it retries itself, and
/// status turns `Synced` once a retry succeeds, with no app call (E5).
#[tokio::test]
async fn a_refused_rotation_is_reported_until_it_retries_itself() {
    let network = Network::new(2).await;
    let owner = &network.devices[0];
    let rotation = refuse_rotation(&network, StorageFailure::QuotaExceeded, 507).await;
    // On the polling clock the rotation's first retry follows its refusal by
    // a millisecond and replaces the report before a busy test can see it.
    // Its waits now end when the test moves the fixed clock.
    network.polling.hold_waits();
    let mut status = owner.handle.subscribe_sync_status();
    owner.handle.sync_now();
    rotation_failed(&mut status).await;
    owner.storage.set_faults(Faults::none()).await;
    // The first retry waits a second.
    network
        .clock
        .set(network.clock.now() + Duration::from_secs(1));
    tokio::time::timeout(Duration::from_secs(15), async {
        owner.handle.sync_now();
        loop {
            status.changed().await.unwrap();
            match &*status.borrow_and_update() {
                SyncStatus::Synced { .. } => break,
                SyncStatus::Syncing => {}
                _ => owner.handle.sync_now(),
            }
        }
    })
    .await
    .unwrap();
    assert!(owner.storage.status(&rotation).await.unwrap().is_some());
    assert!(!owner
        .handle
        .pending()
        .await
        .unwrap()
        .iter()
        .any(|record| matches!(
            record.subject,
            PendingSubject::Operation {
                kind: OperationKind::RotateKey,
                ..
            }
        )));
    network.close().await;
}

/// A rotation the provider refuses outright never clears: sync stops,
/// status stays `Failed` with its record, and no later pass runs (E5).
#[tokio::test]
async fn a_rotation_refused_outright_stalls_sync() {
    let network = Network::new(2).await;
    let owner = &network.devices[0];
    refuse_rotation(&network, StorageFailure::Refused, 400).await;
    let mut status = owner.handle.subscribe_sync_status();
    owner.handle.sync_now();
    rotation_failed(&mut status).await;
    let before = passes(owner);
    owner.handle.sync_now();
    insert(&owner.handle, "after the stall").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(passes(owner), before);
    assert!(matches!(
        &*status.borrow(),
        SyncStatus::Failed { error: SyncFailure::Operation(record) } if record.retry() == Retry::Never
    ));
    network.close().await;
}

/// The failing request's storage error, for either failed status.
fn storage_error(status: &SyncStatus) -> Option<std::sync::Arc<coven_storage::StorageError>> {
    match status {
        SyncStatus::Offline { error }
        | SyncStatus::Failed {
            error: SyncFailure::Storage(error),
        } => Some(error.clone()),
        _ => None,
    }
}

/// Request a pass and return its outcome once a status after it matches.
async fn attempt(
    device: &crate::tests::Device,
    expected: impl Fn(&SyncStatus) -> bool,
) -> std::sync::Arc<coven_storage::StorageError> {
    let mut status = device.handle.subscribe_sync_status();
    status.borrow_and_update();
    device.handle.sync_now();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            status.changed().await.unwrap();
            if expected(&status.borrow()) {
                break;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out at {:?}", &*status.borrow()));
    let error = storage_error(&status.borrow());
    error.expect("a storage failure")
}

/// Status describes the latest attempt, not the connection's history (E5):
/// no response is Offline, a provider's error response is Failed.
#[tokio::test]
async fn each_attempt_is_offline_or_failed_by_whether_storage_answered() {
    let network = Network::new(1).await;
    let device = &network.devices[0];
    // Ana syncs, then loses Wi-Fi: the next attempt is Offline.
    device.storage.set_online(false);
    let offline = attempt(device, |s| matches!(s, SyncStatus::Offline { .. })).await;
    assert_eq!(offline.failure(), StorageFailure::Network);
    assert!(!offline.answered());
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
    let outage = attempt(device, |s| matches!(s, SyncStatus::Failed { .. })).await;
    assert_eq!(outage.failure(), StorageFailure::Network);
    assert!(outage.answered());
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
        storage_error(s).is_some_and(|error| error.failure() == StorageFailure::PermissionDenied)
    })
    .await;
    assert!(denied.answered());
    // An earlier answer does not make a later unanswered attempt Failed.
    device.storage.set_faults(Faults::none()).await;
    device.storage.set_online(false);
    // Permission failures require app action; a new connection permits a retry.
    let mut status = device.handle.subscribe_sync_status();
    device.handle.stop_sync();
    tokio::time::timeout(
        Duration::from_secs(5),
        status.wait_for(|s| matches!(s, SyncStatus::Stopped)),
    )
    .await
    .unwrap()
    .unwrap();
    device.handle.start_sync().await.unwrap();
    attempt(device, |s| matches!(s, SyncStatus::Offline { .. })).await;
    device.storage.set_online(true);
    let mut status = device.handle.subscribe_sync_status();
    device.handle.sync_now();
    tokio::time::timeout(
        Duration::from_secs(5),
        status.wait_for(|s| matches!(s, SyncStatus::Synced { .. })),
    )
    .await
    .unwrap()
    .unwrap();
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
        .hold_next_read(
            coven_storage::ObjectPath::floor(device.store),
            listed,
            resumed,
        )
        .await;
    let mut status = device.handle.subscribe_sync_status();
    status.borrow_and_update();
    insert(&device.handle, "offline").await;
    listing.await.unwrap();
    device.storage.set_online(false);
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            status.changed().await.unwrap();
            if !matches!(&*status.borrow(), SyncStatus::Syncing) {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(
        matches!(&*status.borrow(), SyncStatus::Offline { .. }),
        "{:?}",
        &*status.borrow()
    );
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
    attempt(device, |s| matches!(s, SyncStatus::Offline { .. })).await;
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
    let mut status = device.handle.subscribe_sync_status();
    status.borrow_and_update();
    device.handle.network_changed();
    tokio::time::timeout(
        Duration::from_secs(5),
        status.wait_for(|s| matches!(s, SyncStatus::Synced { .. })),
    )
    .await
    .unwrap()
    .unwrap();
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
    attempt(device, |s| matches!(s, SyncStatus::Failed { .. })).await;
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
        .hold_next_read(
            coven_storage::ObjectPath::floor(device.store),
            read,
            resumed,
        )
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
        &*device.handle.subscribe_sync_status().borrow(),
        SyncStatus::Offline { .. }
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
    attempt(device, |s| matches!(s, SyncStatus::Offline { .. })).await;
    let wait = connection_wait(device, |_| true).await;
    device.handle.close().await.unwrap();
    device.storage.set_online(true);
    let handle = network.reopen(device).open(device.store).await.unwrap();
    network.devices[0].handle = handle;
    let device = &network.devices[0];
    let requests = device.storage.request_trace().len();
    device.handle.start_sync().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        device.storage.request_trace().len(),
        requests,
        "reopening discarded the backoff"
    );
    assert_eq!(device.handle.test_connection_retry_wait().await, Some(wait));
    let mut status = device.handle.subscribe_sync_status();
    device.handle.network_changed();
    tokio::time::timeout(
        Duration::from_secs(5),
        status.wait_for(|s| matches!(s, SyncStatus::Synced { .. })),
    )
    .await
    .unwrap()
    .unwrap();
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
    attach(&device.handle, "photo", vec![3; 2048]).await;
    network.sync(0).await;
    network.polling.hold_waits();
    device.storage.set_online(false);
    let offline = |records: &[PendingRecord]| {
        records.iter().any(|record| {
            matches!(record.subject, PendingSubject::File { .. })
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
    let mut pending = device.handle.subscribe_pending();
    device.handle.set_uploads_paused(false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !offline(&pending.next().await.unwrap()) {}
    })
    .await
    .unwrap();
    device.storage.set_online(true);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !file_stored(device, "photo").await,
        "the upload skipped its backoff"
    );
    device.handle.network_changed();
    wait_for_empty_queue(&device.handle).await;
    assert!(file_stored(device, "photo").await);
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
    attach(&device.handle, "photo", vec![4; 2048]).await;
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
            matches!(record.subject, PendingSubject::File { .. })
                && matches!(record.reason, PendingReason::Storage { answered: true, .. })
        })
    };
    let mut pending = device.handle.subscribe_pending();
    device.handle.set_uploads_paused(false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !answered(&pending.next().await.unwrap()) {}
    })
    .await
    .unwrap();
    device.handle.network_changed();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !file_stored(device, "photo").await,
        "a network report ended an answered backoff"
    );
    device.handle.retry_now();
    wait_for_empty_queue(&device.handle).await;
    assert!(file_stored(device, "photo").await);
    network.close().await;
}
