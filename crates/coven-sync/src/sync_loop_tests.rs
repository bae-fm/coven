use crate::sequence_sync::tests::*;
use crate::*;

async fn reached(device: &Device, slot: u64) {
    let mut status = device.sync.state.subscribe();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        status.wait_for(
            |status| matches!(status, SyncStatus::Synced { through } if *through >= slot),
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
