use crate::sequence_sync::tests::*;
use crate::*;
use coven_storage::Storage;

#[tokio::test]
async fn reset_loads_the_announced_body_and_retains_later_eligible_writes() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "before reset").await;
    ana.sync.pass().await.unwrap();
    ana.sync.reset(coven_merge::Audience::Store).await.unwrap();
    insert(&ana.sync.database, "b", "after reset").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(
        rows(&ana.sync.database).await,
        rows(&ben.sync.database).await
    );
    assert_eq!(rows(&ben.sync.database).await.len(), 2);
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn resetting_a_circle_preserves_other_audiences_without_requiring_their_snapshots() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "store-row", "kept").await;
    ana.sync.pass().await.unwrap();
    let circle = ana.sync.create_circle("Gifts".into()).await.unwrap();
    ana.sync
        .reset(coven_merge::Audience::Circle(circle))
        .await
        .unwrap();
    assert_eq!(
        rows(&ana.sync.database).await,
        [("store-row".into(), "kept".into())]
    );
    assert!(ana
        .sync
        .database
        .reloading_audiences()
        .await
        .unwrap()
        .is_empty());
    ana.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn loading_a_snapshot_checks_its_announced_digest_before_its_signature() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let slot = ana
        .sync
        .snapshot(coven_merge::Audience::Store)
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    let history = ben.sync.session.lock().await.history.clone();
    let announcement = &history.announcements[&slot];
    let path = crate::snapshot_object::path(group.store, &announcement.snapshot).unwrap();
    let size = group.storage.status(&path).await.unwrap().unwrap().size;
    group
        .storage
        .corrupt_byte(&path, size as usize - 1)
        .await
        .unwrap();
    let snapshot = crate::snapshot_object::SnapshotObject::read_prefix(
        &ben.sync.database,
        ben.sync.storage.as_ref(),
        &path,
    )
    .await
    .unwrap();
    let ring = ben.sync.keys.read().unwrap().unwrap();
    snapshot
        .plaintext(ben.sync.storage.as_ref(), &ring)
        .await
        .unwrap();
    let result = snapshot
        .authenticate(&history, &announcement.snapshot, &announcement.digest)
        .await;
    assert!(
        matches!(
            result,
            Err(SyncError::Damaged(DamagedObject {
                failure: Refusal::WrongIdentity { .. },
                ..
            }))
        ),
        "{result:?}"
    );
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}
