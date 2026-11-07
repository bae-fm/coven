use crate::sequence_sync::tests::*;
use coven_foundation::id_source::IdSource;
use coven_merge::slot;
use coven_merge::Audience;
use coven_storage::{ObjectPath, Storage};
use std::{sync::Arc, time::UNIX_EPOCH};

/// Ben, whose clock runs a grace period behind the provider's, announces a
/// snapshot and goes quiet. Ana announces two more, and once the newer has
/// passed grace her floor's first kept slot passes all of Ben's slots and
/// both his announcement's and her older one's. Ben reloads from her floor;
/// by his clock her newer snapshot hasn't passed grace. Choosing a
/// replacement for his old snapshot, or what bounds `first`, he never reads
/// the slot of her older announcement, which retention may have deleted
/// (§16.6, `Retention.kept_replacement`, `Retention.kept_candidates_same_first`).
#[tokio::test]
async fn retention_never_reads_an_announcement_slot_below_the_floor() {
    let group = Group::new();
    let ana = group.device(1).await;
    let clock = Arc::new(coven_foundation::clock::FixedClock::new(UNIX_EPOCH));
    let identity = coven_crypto::MemberIdentity::new(
        coven_foundation::id_source::MemberId(group.ids.new_id()),
        coven_format::test_utils::keys_from_seed(2),
    );
    let (temp, _) = group.directory();
    let ben = group
        .open_with_clock(
            temp,
            identity,
            Some(group.ring.clone()),
            group.storage.for_device(),
            clock.clone(),
        )
        .await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    insert(&ben.sync.database, "ben", "before his snapshot").await;
    ben.sync.pass().await.unwrap();
    ben.sync.snapshot(Audience::Store).await.unwrap();
    let his = ben.sync.database.applied_slot().await.unwrap();
    let id = ben.sync.session.lock().await.history.announcements[&slot(his)].snapshot.clone();
    let body = crate::snapshot_object::path(group.store, &id).unwrap();

    ana.sync.pass().await.unwrap();
    insert(&ana.sync.database, "a", "before her older snapshot").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    let older = ana.sync.database.applied_slot().await.unwrap();
    insert(&ana.sync.database, "b", "before her newer snapshot").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(UNIX_EPOCH + super::GRACE + std::time::Duration::from_secs(1));
    ana.sync.pass().await.unwrap();
    let floor = crate::sequence_sync::sequence_reload::tests::stored_floor(&ana).await;
    assert!(older < floor.header.first.get(), "{:?}", floor.header);
    for slot in [his, older] {
        let path = ObjectPath::slot(group.store, slot.try_into().unwrap());
        assert!(group.storage.status(&path).await.unwrap().is_none(), "slot {slot}");
    }

    let mut ring = ben.sync.keys.read().unwrap().unwrap();
    let mut session = ben.sync.session.lock().await;
    ben.sync.read_forward(&mut session, &mut ring, &ben.identity).await.unwrap();
    assert!(session.history.announcements.contains_key(&slot(older)));
    ben.sync.retain_snapshots(&mut session, &mut ring, &ben.identity).await.unwrap();
    assert!(
        group.storage.status(&body).await.unwrap().is_some(),
        "no replacement has passed grace"
    );
    ben.sync.retain_sequence(&mut session, &mut ring, &ben.identity).await.unwrap();
    drop(session);
    ben.sync.pass().await.unwrap();
    // Once Ana's newer snapshot passes grace by his clock, it replaces his.
    clock.set(UNIX_EPOCH + super::GRACE + std::time::Duration::from_secs(1));
    ben.sync.pass().await.unwrap();
    assert!(group.storage.status(&body).await.unwrap().is_none());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}
