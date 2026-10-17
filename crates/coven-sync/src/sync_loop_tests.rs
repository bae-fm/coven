use crate::sequence_sync::tests::*;
use crate::*;

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
    let ana_loop = SyncLoop::new(ana.sync.clone(), None);
    let ben_loop = SyncLoop::new(ben.sync.clone(), None);
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
    let ana_loop = SyncLoop::new(ana.sync.clone(), None);
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
    let ben_loop = SyncLoop::new(ben.sync.clone(), None);
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
