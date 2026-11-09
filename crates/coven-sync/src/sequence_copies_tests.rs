use crate::sequence_sync::tests::*;
use crate::{PendingReason, PendingRecord, PendingSubject, SyncError};
use coven_format::admin::StoreChange;
use coven_merge::Audience;

/// §12.1: Ben's circle has Carol too. Ana, an admin outside it, replaces
/// Ben's keys and can send him only the store key. Carol's next pass sends him
/// the circle's key, which his new phone opens.
#[tokio::test]
async fn a_circle_member_sends_the_circle_key_to_a_member_an_outside_admin_recovered() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &carol).await;
    let circle = ben.sync.create_circle("Ben's".into()).await.unwrap();
    ben.sync
        .administer(StoreChange::AddCircleMember { circle, member: carol.identity.member() })
        .await
        .unwrap();
    carol.sync.pass().await.unwrap();
    assert_eq!(carol.sync.keys.read().unwrap().unwrap().circle_key_ids(circle).count(), 1);
    ana.sync.pass().await.unwrap();
    let replacement = coven_crypto::MemberIdentity::new(
        ben.identity.member(),
        coven_format::test_utils::keys_from_seed(12),
    );
    ana.sync
        .replace_member_keys_for(ben.identity.member(), replacement.keys().public_keys())
        .await
        .unwrap();

    let phone = group.newcomer(replacement.clone(), group.storage.for_device()).await;
    phone.sync.pass().await.unwrap();
    assert_eq!(phone.sync.keys.read().unwrap().unwrap().circle_key_ids(circle).count(), 0);

    carol.sync.pass().await.unwrap();
    assert!(carol.sync.database.operations().await.unwrap().is_empty());
    phone.sync.pass().await.unwrap();
    // Replacing Ben's keys exposed the circle's key to his retired sealing
    // key, so Carol's pass rotated it and sealed the new key to his new one.
    // The older key reaches him only by the copy this pass owed him.
    let held = |device: &Device| {
        let ring = device.sync.keys.read().unwrap().unwrap();
        let mut ids: Vec<_> = ring.circle_key_ids(circle).collect();
        ids.sort();
        ids
    };
    assert_eq!(held(&carol).len(), 2);
    assert_eq!(held(&phone), held(&carol));

    // The copy is in the sequence now: another pass sends none.
    let through = carol.sync.session.lock().await.through;
    carol.sync.pass().await.unwrap();
    assert_eq!(carol.sync.session.lock().await.through, through);
    for device in [&ana, &ben, &carol, &phone] {
        device.sync.database.close().await.unwrap();
    }
}

/// §5.4, §12.1: Ana, an admin outside Gifts, replaces Ben's keys. Ben's new
/// phone opens before Carol's Gifts copy lands and stops at Carol's Gifts
/// write, whose key it has no copy of, and its record names the Gifts key.
/// Carol's pass sends the copy in a later slot; the stuck phone scans ahead
/// for it and reads on, with no fresh restore.
#[tokio::test]
async fn a_stuck_reader_finds_a_late_circle_copy_and_reads_on() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &carol).await;
    let gifts = carol.sync.create_circle("Gifts".into()).await.unwrap();
    carol
        .sync
        .administer(StoreChange::AddCircleMember { circle: gifts, member: ben.identity.member() })
        .await
        .unwrap();
    pin(carol.database(), "00000000-0000-4000-8000-000000000001", gifts, "scarf").await;
    carol.sync.pass().await.unwrap();
    let write = carol.sync.session.lock().await.through;
    ana.sync.pass().await.unwrap();
    let replacement = coven_crypto::MemberIdentity::new(
        ben.identity.member(),
        coven_format::test_utils::keys_from_seed(12),
    );
    ana.sync
        .replace_member_keys_for(ben.identity.member(), replacement.keys().public_keys())
        .await
        .unwrap();

    let phone = group.newcomer(replacement, group.storage.for_device()).await;
    assert!(matches!(phone.sync.pass().await, Err(SyncError::KeyUnavailable { .. })));
    assert_eq!(phone.database().applied_slot().await.unwrap(), write - 1);
    let stuck = coven_merge::slot(write);
    assert!(phone.database().pending::<PendingRecord>().await.unwrap().iter().any(|record| {
        record.subject == PendingSubject::Slot(stuck)
            && matches!(record.reason, PendingReason::KeyUnavailable {
                audience: Audience::Circle(circle), ..
            } if circle == gifts)
    }));

    carol.sync.pass().await.unwrap();
    phone.sync.pass().await.unwrap();
    assert_eq!(
        pins(phone.database()).await,
        [("00000000-0000-4000-8000-000000000001".into(), "scarf".into())]
    );
    assert!(phone
        .database()
        .pending::<PendingRecord>()
        .await
        .unwrap()
        .iter()
        .all(|record| record.subject != PendingSubject::Slot(stuck)));
    for device in [&ana, &ben, &carol, &phone] {
        device.sync.database.close().await.unwrap();
    }
}

/// §12.1: a member no longer in the circle is sent nothing, whatever sealing
/// key they now have.
#[tokio::test]
async fn no_copy_is_owed_to_a_removed_member() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &carol).await;
    let circle = ben.sync.create_circle("Ben's".into()).await.unwrap();
    ben.sync
        .administer(StoreChange::AddCircleMember { circle, member: carol.identity.member() })
        .await
        .unwrap();
    carol.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    let keys = coven_format::test_utils::keys_from_seed(12).public_keys();
    ana.sync.replace_member_keys_for(ben.identity.member(), keys).await.unwrap();
    carol.sync.pass().await.unwrap();
    // Ben's copy for his new key is already there; take him out of the circle
    // and replace his keys again, so the new key has none.
    carol
        .sync
        .administer(StoreChange::RemoveCircleMember { circle, member: ben.identity.member() })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    let again = coven_format::test_utils::keys_from_seed(13).public_keys();
    ana.sync.replace_member_keys_for(ben.identity.member(), again).await.unwrap();
    carol.sync.pass().await.unwrap();
    let session = carol.sync.session.lock().await;
    let ring = carol.sync.keys.read().unwrap().unwrap();
    let owed = session.history.owed_copies(&ring, carol.identity.member());
    assert!(owed.iter().all(|route| route.member != ben.identity.member()), "{}", owed.len());
    assert!(!session.history.state.reads_circle(circle, &ben.identity.member()));
    assert!(session.history.copies.iter().all(|copy| match &copy.prefix.kind {
        coven_format::sealed_slot::SlotKind::KeyCopy(copy) => {
            copy.routing.sealing != again.sealing
                || copy.routing.audience != Audience::Circle(circle)
        }
        _ => true,
    }));
    drop(session);
    for device in [&ana, &ben, &carol] {
        device.sync.database.close().await.unwrap();
    }
}

/// §12.1: a member who holds no copy of a circle's key has nothing to send,
/// and an admin outside the circle owes none of its keys.
#[tokio::test]
async fn a_device_without_the_key_owes_nothing() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.create_circle("Ben's".into()).await.unwrap();
    ana.sync.pass().await.unwrap();
    let keys = coven_format::test_utils::keys_from_seed(12).public_keys();
    ana.sync.replace_member_keys_for(ben.identity.member(), keys).await.unwrap();
    let session = ana.sync.session.lock().await;
    let ana_ring = ana.sync.keys.read().unwrap().unwrap();
    // Ana is outside the circle.
    assert!(session.history.owed_copies(&ana_ring, ana.identity.member()).is_empty());
    // Ben belongs to it, but Ana's ring holds none of its keys.
    assert!(session
        .history
        .owed_copies(&ana_ring, ben.identity.member())
        .iter()
        .all(|route| route.audience == Audience::Store));
    drop(session);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}
