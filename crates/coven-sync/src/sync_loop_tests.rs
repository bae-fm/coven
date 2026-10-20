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
                faults
                    .body_failures
                    .insert(path, BodyFailure::Interrupted(1));
            } else {
                faults.read_failures.insert(path, failure);
            }
            ben.storage.set_faults(faults).await;
            let error = ben.sync.pass().await.unwrap_err();
            let shown = super::failed(&ben.sync, error);
            match failure {
                StorageFailure::Network => {
                    assert!(matches!(shown, SyncStatus::Offline { .. }), "{shown:?}")
                }
                _ => assert!(matches!(shown, SyncStatus::Failed { .. }), "{shown:?}"),
            }
            assert_eq!(ben.database().applied_slot().await.unwrap(), before);
            assert!(ben
                .database()
                .pending::<PendingRecord>()
                .await
                .unwrap()
                .is_empty());
            ben.storage.set_faults(Faults::none()).await;
            ben.sync.pass().await.unwrap();
            assert_eq!(
                rows(ben.database()).await,
                [("a".into(), "read after retry".into())]
            );
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
    assert_eq!(
        rows(&ben.sync.database).await,
        [("a".into(), "woken".into())]
    );
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
            },
            InviteAccess::ProviderAccount {
                email: "carol@example.org".into(),
            },
        )
        .await
        .unwrap();
    group
        .clock
        .set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(5));
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
        .corrupt_byte(
            &coven_storage::ObjectPath::slot(group.store, slot.try_into().unwrap()),
            0,
        )
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
        matches!(shown, SyncStatus::Blocked { slot: blocked, reason: PendingReason::Refused(_) } if blocked.0 == slot),
        "{shown:?}"
    );
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
    group
        .clock
        .set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1));
    reached(&ben, slot).await;
    assert_eq!(
        rows(ben.database()).await,
        [("a".into(), "applied on retry".into())]
    );
    ben_loop.close().await.unwrap();
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

#[tokio::test]
async fn a_removed_device_never_retries_on_a_new_connection() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ana.sync
        .administer(coven_format::admin::StoreChange::RemoveDevice {
            device: ben.sync.device,
        })
        .await
        .unwrap();
    let mut status = ben.sync.state.subscribe_sync();
    let (ben_codes, _) = codes(&ben, Keychain::in_memory("sync-loop-ben").unwrap());
    let worker = SyncLoop::new(ben.sync.clone(), ben_codes);
    worker.start().unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        status.wait_for(|s| {
            matches!(
                s,
                SyncStatus::Failed {
                    error: SyncFailure::Removed
                }
            )
        }),
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
        status.wait_for(|s| {
            matches!(
                s,
                SyncStatus::Failed {
                    error: SyncFailure::Removed
                }
            )
        }),
    )
    .await
    .unwrap()
    .unwrap();
    group
        .clock
        .set(std::time::UNIX_EPOCH + std::time::Duration::from_secs(600));
    worker.sync_now().unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(ben.storage.request_count(), requests);
    assert!(ben
        .database()
        .retry_wait(coven_database::RetrySubject::Sync)
        .await
        .unwrap()
        .is_none());
    worker.close().await.unwrap();
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}
