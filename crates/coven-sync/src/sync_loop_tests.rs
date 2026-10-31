use crate::sequence_sync::tests::*;
use crate::*;
use coven_crypto::custody::Keychain;

/// E5 and §6.5: an incomplete read is retryable input,
/// never evidence that the slot's bytes cannot apply.
#[tokio::test]
async fn slot_read_failures_keep_their_pass_status_and_retry() {
    use coven_storage::{
        test_utils::{BodyFailure, Faults},
        StorageFailure,
    };
    for body in [false, true] {
        for failure in [StorageFailure::Network, StorageFailure::PermissionDenied] {
            if body && failure != StorageFailure::Network {
                continue;
            }
            let group = Group::new();
            let ana = group.device(1).await;
            let ben = group.device(2).await;
            group.create(&ana).await;
            group.admit(&ana, &ben).await;
            ben.sync.pass().await.unwrap();
            let before = ben.database().applied_slot().await.unwrap();
            insert(ana.database(), "a", "read after retry").await;
            ana.sync.pass().await.unwrap();
            let path = ana.sync.path(before + 1).unwrap();
            let mut faults = Faults::none();
            if body {
                faults.body_failures.insert(path, BodyFailure::Interrupted(1));
            } else {
                faults.read_failures.insert(path, failure);
            }
            ben.storage.set_faults(faults).await;
            let error = ben.sync.pass().await.unwrap_err();
            let shown = super::failed(&ben.sync, error);
            match failure {
                StorageFailure::Network => {
                    assert!(matches!(shown, PassFailure::Offline { .. }), "{shown:?}")
                }
                _ => assert!(matches!(shown, PassFailure::Failed { .. }), "{shown:?}"),
            }
            assert_eq!(ben.database().applied_slot().await.unwrap(), before);
            assert!(ben.database().pending::<PendingRecord>().await.unwrap().is_empty());
            ben.storage.set_faults(Faults::none()).await;
            ben.sync.pass().await.unwrap();
            assert_eq!(rows(ben.database()).await, [("a".into(), "read after retry".into())]);
            for device in [&ana, &ben] {
                device.database().close().await.unwrap();
            }
        }
    }
}

async fn reached(device: &Device, slot: u64) {
    let mut status = device.sync.state.subscribe();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        status.wait_for(
            |status| matches!(status, SequenceStatus::Synced { through } if *through >= slot),
        ),
    )
    .await
    .expect("the loop reached the slot")
    .unwrap();
}

#[tokio::test]
async fn an_app_commit_wakes_a_pass_and_a_requested_pass_reads_it_elsewhere() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let (ana_codes, _) = codes(&ana, Keychain::in_memory("sync-loop-ana").unwrap());
    let ana_loop = SyncLoop::new(ana.sync.clone(), ana_codes);
    let (ben_codes, _) = codes(&ben, Keychain::in_memory("sync-loop-ben").unwrap());
    let ben_loop = SyncLoop::new(ben.sync.clone(), ben_codes);
    ana_loop.start().unwrap();
    ben_loop.start().unwrap();
    let next = ana.sync.database.applied_slot().await.unwrap() + 1;
    insert(&ana.sync.database, "a", "woken").await;
    reached(&ana, next).await;
    ben_loop.sync_now().unwrap();
    reached(&ben, next).await;
    assert_eq!(rows(&ben.sync.database).await, [("a".into(), "woken".into())]);
    ana_loop.close().await.unwrap();
    ben_loop.close().await.unwrap();
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// An invite's wait that is already over, waiting for a request, does not
/// keep the loop from running the pass an app write requested.
#[tokio::test]
async fn an_ended_operation_wait_does_not_starve_a_requested_pass() {
    let group = Group::shared();
    let ana = group.device(1).await;
    group
        .create_with_access(
            &ana,
            coven_format::MemberAccess::ProviderAccount("ana@example.org".into()),
        )
        .await;
    ana.sync
        .create_invite(
            InviteTarget::NewMember {
                role: coven_format::admin::MemberRole::Member,
                name: "Carol".into(),
            },
            InviteAccess::ProviderAccount { email: "carol@example.org".into() },
            crate::InviteDelivery::Sent { lifetime: crate::InviteLifetime::Day },
        )
        .await
        .unwrap();
    group.clock.set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(5));
    let (ana_codes, _) = codes(&ana, Keychain::in_memory("sync-loop-ana").unwrap());
    let ana_loop = SyncLoop::new(ana.sync.clone(), ana_codes);
    ana_loop.start().unwrap();
    let next = ana.sync.database.applied_slot().await.unwrap() + 1;
    insert(&ana.sync.database, "a", "sent").await;
    reached(&ana, next).await;
    ana_loop.close().await.unwrap();
    ana.sync.database.close().await.unwrap();
}

/// A slot that cannot apply shows as `Blocked` with its cause (E5).
#[tokio::test]
async fn a_damaged_slot_shows_the_loop_blocked_on_it() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    insert(&ana.sync.database, "a", "damaged").await;
    ana.sync.pass().await.unwrap();
    let slot = ana.sync.database.applied_slot().await.unwrap();
    group
        .storage
        .corrupt_byte(&coven_storage::ObjectPath::slot(group.store, slot.try_into().unwrap()), 0)
        .await
        .unwrap();
    let mut status = ben.sync.state.subscribe_sync();
    let (ben_codes, _) = codes(&ben, Keychain::in_memory("sync-loop-ben").unwrap());
    let ben_loop = SyncLoop::new(ben.sync.clone(), ben_codes);
    ben_loop.start().unwrap();
    let shown = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        status.wait_for(|status| matches!(status, SyncStatus::Blocked { .. })),
    )
    .await
    .unwrap()
    .unwrap()
    .clone();
    assert!(
        matches!(&shown, SyncStatus::Blocked { slot: blocked, reason: PendingReason::Refused(_), .. } if blocked.0 == slot),
        "{shown:?}"
    );
    // The status carries the error that blocked the slot.
    let SyncStatus::Blocked { error: Some(error), .. } = &shown else {
        panic!("the blocking error was dropped: {shown:?}");
    };
    assert!(error.to_string().contains("damaged object"), "{error}");
    ben_loop.close().await.unwrap();
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// A local failure that clears by itself, here a keychain refusing to save
/// Ben's keys while he applies a slot, retries on the shared backoff with no
/// app action: no one else calls for the pass again (§6.7, E5).
#[tokio::test]
async fn a_local_failure_retries_the_pass_on_its_own() {
    let mut group = Group::new();
    let ana = group.device(1).await;
    group.custody = Custody::Keychain;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    insert(ana.database(), "a", "applied on retry").await;
    ana.sync.pass().await.unwrap();
    let slot = ana.database().applied_slot().await.unwrap();
    ben.keychain.fail_next_operation();
    let mut status = ben.sync.state.subscribe_sync();
    let (ben_codes, _) = codes(&ben, Keychain::in_memory("sync-loop-ben").unwrap());
    let ben_loop = SyncLoop::new(ben.sync.clone(), ben_codes);
    ben_loop.start().unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        status.wait_for(|status| matches!(status, SyncStatus::Failed { .. })),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        ben.database().pending::<PendingRecord>().await.unwrap(),
        [PendingRecord::new(
            PendingSubject::Connection,
            PendingReason::Failed(LocalFailure::KeyCustody)
        )]
    );
    // The first retry waits one second, well before the idle pass.
    group.clock.set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1));
    reached(&ben, slot).await;
    assert_eq!(rows(ben.database()).await, [("a".into(), "applied on retry".into())]);
    ben_loop.close().await.unwrap();
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// A removed device never retries, and a retry is answered with why at once.
#[tokio::test]
async fn a_removed_device_never_retries_on_a_new_connection() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ana.sync
        .administer(coven_format::admin::StoreChange::RemoveDevice { device: ben.sync.device })
        .await
        .unwrap();
    let mut status = ben.sync.state.subscribe_sync();
    let (ben_codes, _) = codes(&ben, Keychain::in_memory("sync-loop-ben").unwrap());
    let worker = SyncLoop::new(ben.sync.clone(), ben_codes);
    worker.start().unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        status.wait_for(|s| matches!(s, SyncStatus::Failed { error: SyncFailure::Removed })),
    )
    .await
    .unwrap()
    .unwrap();
    worker.close().await.unwrap();
    let ben = group.reopen(ben).await;
    let requests = ben.storage.request_count();
    let mut status = ben.sync.state.subscribe_sync();
    let (ben_codes, _) = codes(&ben, Keychain::in_memory("sync-loop-ben").unwrap());
    let worker = SyncLoop::new(ben.sync.clone(), ben_codes);
    worker.start().unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        status.wait_for(|s| matches!(s, SyncStatus::Failed { error: SyncFailure::Removed })),
    )
    .await
    .unwrap()
    .unwrap();
    group.clock.set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(600));
    worker.sync_now().unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // Pulling to refresh is told why at once, and runs no pass.
    let answer =
        tokio::time::timeout(std::time::Duration::from_secs(5), ben.sync.waits.retry_requested())
            .await
            .unwrap();
    assert!(
        matches!(answer, RetryOutcome::Failed(PassFailure::Failed { error: SyncFailure::Removed })),
        "{answer:?}"
    );
    assert_eq!(ben.storage.request_count(), requests);
    assert!(ben.database().retry_wait(coven_database::RetrySubject::Sync).await.unwrap().is_none());
    worker.close().await.unwrap();
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// Set the group's clock `seconds` after the epoch.
fn at(group: &Group, seconds: u64) {
    group.clock.set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds));
}

/// Wait for the pass that finishes `seconds` after the epoch.
async fn synced_at(status: &mut tokio::sync::watch::Receiver<SyncStatus>, seconds: u64) {
    let finished = std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds);
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        status.wait_for(
            |status| matches!(status, SyncStatus::Synced { finished_at } if *finished_at == finished),
        ),
    )
    .await
    .unwrap_or_else(|_| panic!("no pass finished at {seconds} s"))
    .unwrap();
}

/// No pass starts at the clock's current time. Absence has no event to
/// wait on, so the loop gets a fixed window in which to start one wrongly.
async fn no_pass(device: &Device, status: &mut tokio::sync::watch::Receiver<SyncStatus>) {
    let requests = device.storage.request_count();
    status.mark_unchanged();
    let changed =
        tokio::time::timeout(std::time::Duration::from_millis(200), status.changed()).await;
    assert!(changed.is_err(), "a pass ran: {:?}", *status.borrow());
    assert_eq!(device.storage.request_count(), requests);
}

/// While an app holds a guard, idle passes come at its interval, the
/// shortest held winning; after the last one drops, every 30 seconds (E5).
#[tokio::test]
async fn idle_passes_come_at_the_shortest_interval_held() {
    use std::time::Duration;
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let mut status = ana.sync.state.subscribe_sync();
    let (ana_codes, _) = codes(&ana, Keychain::in_memory("sync-often-ana").unwrap());
    let ana_loop = SyncLoop::new(ana.sync.clone(), ana_codes);
    ana_loop.start().unwrap();
    synced_at(&mut status, 0).await;
    let every_ten = ana.sync.state.sync_often(Duration::from_secs(10));
    let every_five = ana.sync.state.sync_often(Duration::from_secs(5));
    at(&group, 5);
    synced_at(&mut status, 5).await;
    at(&group, 10);
    synced_at(&mut status, 10).await;
    // The ten-second guard still holds: the next pass is 10 s after the last.
    drop(every_five);
    at(&group, 15);
    no_pass(&ana, &mut status).await;
    at(&group, 20);
    synced_at(&mut status, 20).await;
    drop(every_ten);
    at(&group, 49);
    no_pass(&ana, &mut status).await;
    at(&group, 50);
    synced_at(&mut status, 50).await;
    ana_loop.close().await.unwrap();
    ana.database().close().await.unwrap();
}

/// A guard taken well after the last pass runs one at once, since its
/// interval has passed; one taken during a provider cooldown, such as a
/// provider's Retry-After arms, waits for the cooldown to end (E5).
#[tokio::test]
async fn a_held_interval_honors_the_provider_cooldown() {
    use std::time::Duration;
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let mut status = ana.sync.state.subscribe_sync();
    let (ana_codes, _) = codes(&ana, Keychain::in_memory("sync-often-ana").unwrap());
    let ana_loop = SyncLoop::new(ana.sync.clone(), ana_codes);
    ana_loop.start().unwrap();
    synced_at(&mut status, 0).await;
    at(&group, 20);
    let often = ana.sync.state.sync_often(Duration::from_secs(5));
    synced_at(&mut status, 20).await;
    coven_storage::Cooldown::record(ana.sync.waits.as_ref(), Duration::from_secs(60))
        .await
        .unwrap();
    at(&group, 25);
    no_pass(&ana, &mut status).await;
    at(&group, 80);
    synced_at(&mut status, 80).await;
    drop(often);
    ana_loop.close().await.unwrap();
    ana.database().close().await.unwrap();
}
