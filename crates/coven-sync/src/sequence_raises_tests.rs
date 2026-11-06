//! Which device raises a circle, and the writes that wait for a circle's
//! raise.

use crate::sequence_sync::tests::*;

/// Ana's phone prepares Garden on the old app, so its creation entry records
/// base 0, and her app updates with a breaking migration before the entry
/// applies. Her migration raised only what she read then, the store, and that
/// raise has finished. Garden has no rows when its creation applies, so her
/// phone holds its state at version 2 and raises it; without that, Garden
/// would stay below her version and none of her Garden snapshots would count
/// (§15.5, §18.3).
#[tokio::test]
async fn a_circle_its_maker_migrated_before_creating_is_raised_by_the_maker() {
    use crate::sequence_sync::sequence_operations::Request;
    use coven_foundation::id_source::{CircleId, IdSource, KeyId};
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync.database.store_state().await.unwrap().reached(&coven_merge::Audience::Store),
        2
    );
    assert!(ana.sync.database.raise_captures().await.unwrap().is_empty());
    assert!(ana.sync.database.operations().await.unwrap().is_empty());

    // The entry as her phone prepared it before the update, its key in
    // custody.
    let garden = CircleId(group.ids.new_id());
    let key = coven_crypto::CircleKey::from_bytes(garden, KeyId(group.ids.new_id()), [9; 32]);
    let mut ring = ana.sync.keys.read().unwrap().unwrap();
    let (key_id, hash) = (key.id(), key.hash());
    ring.insert_circle_key(key).unwrap();
    ana.sync.keys.persist(&ring).unwrap();
    ana.sync
        .start_request_row(
            Request::Change(coven_format::admin::StoreChange::CreateCircle {
                circle: garden,
                name: "Garden".into(),
                breaking_version: 0,
                key: key_id,
                key_hash: hash,
            }),
            coven_database::operation_kind::COVEN,
        )
        .await
        .unwrap();
    for _ in 0..3 {
        ana.sync.pass().await.unwrap();
    }
    let state = ana.sync.database.store_state().await.unwrap();
    assert!(state.reads_circle(garden, &ana.identity.member()));
    assert_eq!(state.circles[&garden].base, 0);
    assert_eq!(state.reached(&coven_merge::Audience::Circle(garden)), 2);
    assert!(ana.sync.database.raise_captures().await.unwrap().is_empty());
    let announced = ana.sync.snapshot(coven_merge::Audience::Circle(garden)).await.unwrap();
    let history = ana.sync.session.lock().await.history.clone();
    assert_eq!(history.announcements[&announced].breaking_version, 2);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben's app migrates while he reads only the store, and Ana, still on the old
/// app, then adds him to Garden. Ben never held Garden's rows at version 0, so
/// he can't make its raise body and doesn't raise it. Ana's update captures
/// Garden, which she held, and raises it; Ben reloads Garden from her raise,
/// and his Garden snapshots count from then on (§15.7, §18.3).
#[tokio::test]
async fn a_circle_joined_after_migrating_is_raised_by_a_member_who_held_it() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let garden = ana.sync.create_circle("Garden".into()).await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ben = group.reopen(ben).await;
    ana.sync
        .administer(coven_format::admin::StoreChange::AddCircleMember {
            circle: garden,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    let state = ben.sync.database.store_state().await.unwrap();
    assert!(state.reads_circle(garden, &ben.identity.member()));
    assert_eq!(state.reached(&coven_merge::Audience::Store), 2);
    assert_eq!(state.reached(&coven_merge::Audience::Circle(garden)), 0);
    assert!(ben.sync.database.raise_captures().await.unwrap().is_empty());

    let ana = group.reopen(ana).await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    let state = ben.sync.database.store_state().await.unwrap();
    assert_eq!(state.reached(&coven_merge::Audience::Circle(garden)), 2);
    assert_eq!(
        state.schema[&coven_merge::Audience::Circle(garden)].snapshot.device,
        ana.sync.device
    );
    let announced = ben.sync.snapshot(coven_merge::Audience::Circle(garden)).await.unwrap();
    let history = ben.sync.session.lock().await.history.clone();
    assert_eq!(history.announcements[&announced].breaking_version, 2);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben's app reached version 2 before Ana added him to Garden, which nobody
/// has raised. His pin in Garden is written at version 2, which Garden hasn't
/// reached, and every device would refuse it there, so it waits in his queue,
/// and his later writes behind it, until Ana's update raises Garden (§18.5).
#[tokio::test]
async fn a_write_into_a_circle_below_its_version_waits_for_the_circles_raise() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let garden = ana.sync.create_circle("Garden".into()).await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ben = group.reopen(ben).await;
    ana.sync
        .administer(coven_format::admin::StoreChange::AddCircleMember {
            circle: garden,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    let state = ben.sync.database.store_state().await.unwrap();
    assert_eq!(state.reached(&coven_merge::Audience::Store), 2);
    assert_eq!(state.reached(&coven_merge::Audience::Circle(garden)), 0);
    let id = "00000000-0000-4000-8000-000000000001";
    pin(ben.database(), id, garden, "seeds").await;
    insert(ben.database(), "00000000-0000-4000-8000-000000000002", "after the pin").await;
    ben.sync.pass().await.unwrap();
    let waiting = async |ben: &Device| ben.database().waiting_snapshot_headers().await.unwrap();
    assert_eq!(waiting(&ben).await.len(), 2, "the pin and the write behind it wait");
    // The pin's record says what it waits for.
    let held = waiting(&ben).await[0].header.position;
    let records = async |ben: &Device| {
        ben.database()
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .into_iter()
            .filter(|record| {
                matches!(
                    record.reason,
                    crate::PendingReason::Waits(crate::Prerequisite::Raise { .. })
                )
            })
            .collect::<Vec<_>>()
    };
    let recorded = records(&ben).await;
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    assert_eq!(
        recorded[0].subject,
        crate::PendingSubject::Write(coven_merge::WriteId {
            device: held.device,
            number: held.number
        })
    );
    assert_eq!(
        recorded[0].reason,
        crate::PendingReason::Waits(crate::Prerequisite::Raise {
            audience: coven_merge::Audience::Circle(garden),
            version: 2,
        })
    );

    let ana = group.reopen(ana).await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    // A pass decides what it sends from the slots applied before it reads,
    // so the pass that applies Garden's raise sends nothing yet.
    ben.sync.pass().await.unwrap();
    let state = ben.sync.database.store_state().await.unwrap();
    assert_eq!(state.reached(&coven_merge::Audience::Circle(garden)), 2);
    // The raise ended the wait as it applied.
    assert!(records(&ben).await.is_empty());
    ben.sync.pass().await.unwrap();
    assert!(waiting(&ben).await.is_empty());
    ana.sync.pass().await.unwrap();
    assert_eq!(pins(ana.database()).await, [(id.to_string(), "seeds".to_string())]);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben's one write pins into Garden and into Orchard, neither raised. Ana
/// left Orchard after adding him, so only Garden's raise comes: it ends the
/// write's wait for Garden, and the write records its wait for Orchard in
/// the same commit (§18.5).
#[tokio::test]
async fn a_raise_that_leaves_a_write_below_another_circle_records_that_wait() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let garden = ana.sync.create_circle("Garden".into()).await.unwrap();
    let orchard = ana.sync.create_circle("Orchard".into()).await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ben = group.reopen(ben).await;
    for circle in [garden, orchard] {
        ana.sync
            .administer(coven_format::admin::StoreChange::AddCircleMember {
                circle,
                member: ben.identity.member(),
            })
            .await
            .unwrap();
    }
    ana.sync
        .administer(coven_format::admin::StoreChange::RemoveCircleMember {
            circle: orchard,
            member: ana.identity.member(),
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    ben.database()
        .write(move |sql| {
            for (id, circle) in [
                ("00000000-0000-4000-8000-000000000001", garden),
                ("00000000-0000-4000-8000-000000000002", orchard),
            ] {
                sql.execute("INSERT INTO pins VALUES(?1,?2,'seeds')", (id, circle.to_string()))?;
            }
            Ok(())
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    let awaited = async |ben: &Device| {
        ben.database()
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .into_iter()
            .filter_map(|record| match record.reason {
                crate::PendingReason::Waits(crate::Prerequisite::Raise { audience, .. }) => {
                    Some(audience)
                }
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(awaited(&ben).await, [coven_merge::Audience::Circle(garden)]);

    let ana = group.reopen(ana).await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    let state = ben.sync.database.store_state().await.unwrap();
    assert_eq!(state.reached(&coven_merge::Audience::Circle(garden)), 2);
    assert_eq!(state.reached(&coven_merge::Audience::Circle(orchard)), 0);
    assert_eq!(awaited(&ben).await, [coven_merge::Audience::Circle(orchard)]);
    assert_eq!(ben.database().waiting_snapshot_headers().await.unwrap().len(), 1);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben's pin in Garden waits for Garden's raise, and he deletes Garden
/// instead. Nobody can raise a deleted circle, so the pin waits for nothing:
/// applying the deletion ends its record, his next pass sends it and the
/// note behind it, and Garden takes the pin out with its other rows (§15.9,
/// §18.5).
#[tokio::test]
async fn a_write_into_a_circle_deleted_below_its_version_waits_no_more() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let garden = ana.sync.create_circle("Garden".into()).await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ben = group.reopen(ben).await;
    ana.sync
        .administer(coven_format::admin::StoreChange::AddCircleMember {
            circle: garden,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    pin(ben.database(), "00000000-0000-4000-8000-000000000001", garden, "seeds").await;
    insert(ben.database(), "00000000-0000-4000-8000-000000000002", "after the pin").await;
    ben.sync.pass().await.unwrap();
    let waiting = async |ben: &Device| ben.database().waiting_snapshot_headers().await.unwrap();
    let raise_waits = async |ben: &Device| {
        ben.database()
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .into_iter()
            .filter(|record| {
                matches!(
                    record.reason,
                    crate::PendingReason::Waits(crate::Prerequisite::Raise { .. })
                )
            })
            .count()
    };
    assert_eq!(waiting(&ben).await.len(), 2);
    assert_eq!(raise_waits(&ben).await, 1);

    ben.sync
        .administer(coven_format::admin::StoreChange::DeleteCircle { circle: garden })
        .await
        .unwrap();
    assert_eq!(raise_waits(&ben).await, 0, "applying the deletion ended the wait");
    ben.sync.pass().await.unwrap();
    assert!(waiting(&ben).await.is_empty());
    let ana = group.reopen(ana).await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert!(pins(ana.database()).await.is_empty());
    assert!(rows(ana.database())
        .await
        .contains(&("00000000-0000-4000-8000-000000000002".into(), "after the pin".into())));
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana's update starts raising Garden, and storage refuses the raise's
/// snapshot, so the raise stays journaled. Ben deletes Garden before it
/// resumes. Nobody raises a deleted circle, so on resuming the raise finds
/// nothing to claim: it adopts that state as it would an established
/// version, deletes its snapshot and ends satisfied, not rejected (§15.9,
/// §18.3).
#[tokio::test]
async fn a_raise_started_before_its_circle_is_deleted_claims_nothing() {
    use coven_storage::test_utils::Faults;
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let garden = ana.sync.create_circle("Garden".into()).await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ben = group.reopen(ben).await;
    ana.sync
        .administer(coven_format::admin::StoreChange::AddCircleMember {
            circle: garden,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();

    let ana = group.reopen(ana).await;
    let refused: std::collections::BTreeMap<_, _> = (1..=8)
        .map(|number| {
            let path = coven_storage::ObjectPath::snapshot(
                group.store,
                coven_merge::Audience::Circle(garden),
                ana.sync.device,
                std::num::NonZeroU64::new(number).unwrap(),
            );
            (path, (coven_storage::StorageFailure::Network, 503))
        })
        .collect();
    ana.storage.set_faults(Faults { create_failures: refused, ..Faults::none() }).await;
    ana.sync.pass().await.unwrap();
    let raise = ana
        .database()
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == coven_database::operation_kind::SEQUENCE)
        .expect("the raise stays journaled, waiting to upload its snapshot");
    ana.storage.set_faults(Faults::none()).await;

    ben.sync
        .administer(coven_format::admin::StoreChange::DeleteCircle { circle: garden })
        .await
        .unwrap();
    // The pass applies the deletion; the raise waits for its retry.
    ana.sync.pass().await.unwrap();
    ana.sync.retry_operation(raise.id).await.unwrap();
    let member = ana.sync.member.read().unwrap().unwrap();
    let mut session = ana.sync.session.lock().await;
    let mut ring = ana.sync.ring(&mut session, &member).await.unwrap();
    let outcome =
        ana.sync.run_administration(&mut session, &mut ring, &member, raise.id).await.unwrap();
    assert_eq!(outcome, crate::OperationOutcome::Satisfied);
    drop(session);
    let operations = ana.database().operations().await.unwrap();
    assert!(operations.is_empty(), "{operations:?}");
    let session = ana.sync.session.lock().await;
    assert!(!session
        .history
        .announcements
        .values()
        .any(
            |announcement| announcement.snapshot.audience == coven_merge::Audience::Circle(garden)
        ));
    drop(session);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}
