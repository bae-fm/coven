use crate::sequence_sync::tests::*;
use crate::{PendingReason, PendingRecord, PendingSubject, SyncError};
use coven_format::admin::StoreChange;
use coven_merge::Audience;
use coven_storage::Storage;

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

/// §12.1, §16.4: Carol snapshots Gifts and the store, and the floor names
/// both. Ana, outside Gifts, then replaces Ben's keys. Ben's new phone starts
/// from the floor, and can't open its Gifts snapshot until Carol's copy lands
/// above it; then it scans the slots above the floor, finds the copy and loads
/// on, with no fresh restore.
#[tokio::test]
async fn a_new_device_finds_the_circle_key_of_a_floor_snapshot_above_the_floor() {
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
    carol.sync.snapshot(Audience::Circle(gifts)).await.unwrap();
    carol.sync.snapshot(Audience::Store).await.unwrap();
    carol.sync.pass().await.unwrap();
    let floor = crate::sequence_sync::sequence_reload::tests::stored_floor(&carol).await;
    assert_eq!(floor.header.snapshots.len(), 2, "the floor names Gifts' snapshot too");
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
    assert!(matches!(
        phone.sync.pass().await,
        Err(SyncError::KeyUnavailable { audience: Audience::Circle(circle), .. }) if circle == gifts
    ));
    assert_eq!(phone.database().applied_slot().await.unwrap(), 0);
    // Nothing loaded, so custody still holds no store key, and the next pass
    // runs the key scan for this member's copies again, to the end.
    assert!(phone.sync.keys.read().unwrap().is_none());

    carol.sync.pass().await.unwrap();
    phone.sync.pass().await.unwrap();
    assert_eq!(
        pins(phone.database()).await,
        [("00000000-0000-4000-8000-000000000001".into(), "scarf".into())]
    );
    for device in [&ana, &ben, &carol, &phone] {
        device.sync.database.close().await.unwrap();
    }
}

/// §6.1, §12.1, §16.4: Ben's new phone stops at Carol's Gifts write, whose
/// key it has no copy of, and goes quiet. Carol's pass sends the copy, Carol
/// snapshots Gifts and the store, and Ana's retention deletes the slot the
/// phone waits at. The phone loads the floor past the deleted slot, opening
/// the copy the floor's history holds, with no fresh restore.
#[tokio::test]
async fn a_device_stuck_on_a_circle_key_loads_the_floor_once_retention_deletes_its_slot() {
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

    carol.sync.pass().await.unwrap();
    carol.sync.snapshot(Audience::Circle(gifts)).await.unwrap();
    carol.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    let stuck = coven_storage::ObjectPath::slot(group.store, write.try_into().unwrap());
    assert!(group.storage.status(&stuck).await.unwrap().is_none(), "retention deleted the stop");

    phone.sync.pass().await.unwrap();
    assert_eq!(
        pins(phone.database()).await,
        [("00000000-0000-4000-8000-000000000001".into(), "scarf".into())]
    );
    assert!(phone.database().applied_slot().await.unwrap() > write);
    assert!(phone
        .database()
        .pending::<PendingRecord>()
        .await
        .unwrap()
        .iter()
        .all(|record| record.subject != PendingSubject::Slot(coven_merge::slot(write))));
    for device in [&ana, &ben, &carol, &phone] {
        device.sync.database.close().await.unwrap();
    }
}

/// §6.1, §12.1, §16.4: Carol snapshots Gifts and the store after her Gifts
/// write. The floor write of Ana's pass that replaces Ben's keys loses an
/// upload part and waits, journaled and listed in `coven_pending` with its
/// cause, so the floor names no snapshot covering Carol's write, and Ben's
/// new phone reads up to that write and stops there. Ana's next
/// pass, past grace, writes the floor and deletes that slot while Carol is
/// away, so Carol's copy for Ben lands above the floor. The phone scans the
/// kept slots for it and loads the floor.
#[tokio::test]
async fn a_device_stuck_on_a_circle_key_scans_above_the_floor_for_its_copy() {
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
    carol.sync.snapshot(Audience::Circle(gifts)).await.unwrap();
    carol.sync.snapshot(Audience::Store).await.unwrap();
    let floor = coven_storage::ObjectPath::floor(group.store);
    let before = group.storage.status(&floor).await.unwrap();
    ana.storage
        .set_faults(coven_storage::test_utils::Faults {
            drop_part: true,
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    let replacement = coven_crypto::MemberIdentity::new(
        ben.identity.member(),
        coven_format::test_utils::keys_from_seed(12),
    );
    ana.sync
        .replace_member_keys_for(ben.identity.member(), replacement.keys().public_keys())
        .await
        .unwrap();
    assert_eq!(group.storage.status(&floor).await.unwrap(), before, "the floor write failed");
    // The app sees the waiting floor write as its operation's record, with
    // the error behind it (§19.1).
    let listed = ana
        .database()
        .read(|sql| {
            Ok::<_, coven_database::DbError>(sql.query(
                "SELECT record,cause FROM coven_pending",
                [],
                |row| Ok((row.get::<_, PendingRecord>(0)?, row.get::<_, Option<String>>(1)?)),
            )?)
        })
        .await
        .unwrap();
    let (record, cause) = listed
        .iter()
        .find(|(record, _)| {
            matches!(
                record.subject,
                PendingSubject::Operation { kind: crate::OperationKind::RetainSequence, .. }
            )
        })
        .expect("the floor write's record");
    assert!(matches!(
        record.reason,
        PendingReason::Storage { failure: coven_storage::StorageFailure::Network, .. }
    ));
    let cause = cause.as_deref().expect("the record keeps its error's text");
    assert!(cause.contains("Network"), "{cause}");
    ana.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    let phone = group.newcomer(replacement, group.storage.for_device()).await;
    assert!(matches!(phone.sync.pass().await, Err(SyncError::KeyUnavailable { .. })));
    assert_eq!(phone.database().applied_slot().await.unwrap(), write - 1);

    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    // The first pass finishes the floor write the recovery's pass left
    // journaled; the next one moves the floor's first kept slot.
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    let stuck = coven_storage::ObjectPath::slot(group.store, write.try_into().unwrap());
    assert!(group.storage.status(&stuck).await.unwrap().is_none(), "retention deleted the stop");
    assert!(phone.sync.pass().await.is_err(), "no copy of the Gifts key has landed yet");
    let deleting = group.storage.status(&floor).await.unwrap().unwrap();

    // Carol's pass sends the copy; its own floor write loses an upload part
    // and waits, so the floor's history stops below the copy.
    carol
        .storage
        .set_faults(coven_storage::test_utils::Faults {
            drop_part: true,
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    carol.sync.pass().await.unwrap();
    carol.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    assert!(group.storage.status(&floor).await.unwrap().is_some_and(|stored| stored == deleting));
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
        .all(|record| record.subject != PendingSubject::Slot(coven_merge::slot(write))));
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
    assert!(session.history.copies.iter().all(|copy| {
        copy.route().sealing != again.sealing || copy.route().audience != Audience::Circle(circle)
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
