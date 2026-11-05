//! Which audiences a floor names and which history it deletes (§16.4, §16.6).
use crate::sequence_sync::sequence_reload::tests::stored_floor;
use crate::sequence_sync::sequence_retention::GRACE;
use crate::sequence_sync::tests::*;
use coven_format::admin::StoreChange;
use coven_merge::Audience;
use coven_storage::{ObjectPath, Storage};

/// The floor names the store and every introduced circle, including one the
/// writer isn't in and one that was deleted, and waits while any lacks a
/// snapshot (§16.4, §16.6).
#[tokio::test]
async fn a_floor_names_deleted_circles_and_ones_its_writer_cannot_open() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    // Gifts and Old are Ben's: Ana is in neither, so she cannot open them.
    let gifts = ben.sync.create_circle("Gifts".into()).await.unwrap();
    let old = ben.sync.create_circle("Old".into()).await.unwrap();
    ben.sync.snapshot(Audience::Circle(old)).await.unwrap();
    ben.sync.administer(StoreChange::DeleteCircle { circle: old }).await.unwrap();
    insert(ana.database(), "a", "store data").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + GRACE + std::time::Duration::from_secs(1));
    ana.sync.pass().await.unwrap();
    let floor_path = ObjectPath::floor(group.store);
    assert!(
        group.storage.status(&floor_path).await.unwrap().is_none(),
        "no floor while Gifts has no snapshot"
    );

    ben.sync.snapshot(Audience::Circle(gifts)).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + 2 * GRACE + std::time::Duration::from_secs(2));
    ana.sync.pass().await.unwrap();
    let floor = stored_floor(&ana).await;
    let session = ana.sync.session.lock().await;
    let named: std::collections::BTreeSet<_> = floor
        .header
        .snapshots
        .iter()
        .map(|slot| session.history.announcements[slot].snapshot.audience.clone())
        .collect();
    assert_eq!(
        named,
        [Audience::Store, Audience::Circle(gifts), Audience::Circle(old)].into_iter().collect()
    );
    assert_eq!(floor.header.snapshots.len(), 3);
    drop(session);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}
