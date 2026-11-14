//! Which device raises a circle, and the writes that wait for a circle's
//! raise.

use crate::sequence_sync::tests::*;
use coven_database::{Database, DbError, Replacement};
use coven_merge::Audience;

async fn deed(
    db: &Database,
    id: &'static str,
    circle: coven_foundation::id_source::CircleId,
    title: &'static str,
) {
    db.write(move |sql| {
        sql.execute("INSERT INTO deeds VALUES(?1,?2,?3)", (id, circle.to_string(), title))?;
        Ok(())
    })
    .await
    .unwrap();
}
async fn deeds(db: &Database) -> Vec<(String, String)> {
    db.read(|sql| {
        Ok::<_, DbError>(sql.query("SELECT id,title FROM deeds ORDER BY id", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?)
    })
    .await
    .unwrap()
}

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
/// Ben's one write pins into Garden and into Orchard, neither raised. Ana
/// was removed from Orchard after adding him, so only Garden's raise comes: it ends the
/// write's wait for Garden, and the write records its wait for Orchard in
/// Ben's pin in Garden waits for Garden's raise, and he deletes Garden
/// instead. Nobody can raise a deleted circle, so the pin waits for nothing:
/// applying the deletion ends its record, his next pass sends it and the
/// note behind it, and Garden takes the pin out with its other rows (§15.9,
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

/// Ana's breaking update journals the store's raise to version 2, and her
/// phone stops before the raise takes its snapshot. Her app then updates
/// again, to version 3, adding a column: her capture moves to version 3, so
/// the snapshot she takes is at version 3, and the journaled raise to 2 has
/// nothing left to raise. It ends, and the raise publishes version 3, the
/// version its snapshot is taken at (§18.5).
#[tokio::test]
async fn an_addition_moves_a_raise_journaled_before_its_snapshot() {
    use crate::sequence_sync::sequence_operations::Request;
    use crate::sequence_sync::snapshot_capture::SnapshotRequest;
    use coven_merge::Audience;
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    group.create(&ana).await;
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    ana.sync
        .start_request_row(
            Request::Snapshot(SnapshotRequest {
                audience: Audience::Store,
                purpose: coven_format::sequence::Purpose::Raise(2),
            }),
            coven_database::operation_kind::COVEN,
        )
        .await
        .unwrap();

    group.version.store(3, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    let moved = ana.database().raise_captures().await.unwrap();
    assert_eq!(moved.iter().map(|capture| capture.version).collect::<Vec<_>>(), [3]);
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    let state = ana.database().store_state().await.unwrap();
    assert_eq!(state.reached(&Audience::Store), 3);
    assert!(ana.database().raise_captures().await.unwrap().is_empty());
    let operations = ana.database().operations().await.unwrap();
    assert!(operations.is_empty(), "{operations:?}");
    ana.sync.database.close().await.unwrap();
}

/// Ana's update journals the store's raise to version 2, and her phone stops
/// at the raise's snapshot step: its migration marker hasn't landed yet. Ben
/// then writes, and her next pass applies his slot and lands her marker, so
/// the state her raise prepared at is behind the one she has applied. The
/// raise takes its capture again from that state when it resumes, and
/// publishes (§16.3, §18.5).
#[tokio::test]
async fn a_raise_resumed_at_its_snapshot_after_a_pass_applied_more_publishes() {
    use crate::sequence_sync::sequence_operations::Request;
    use crate::sequence_sync::snapshot_capture::SnapshotRequest;
    use coven_merge::Audience;
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    let raise = ana
        .sync
        .start_request_row(
            Request::Snapshot(SnapshotRequest {
                audience: Audience::Store,
                purpose: coven_format::sequence::Purpose::Raise(2),
            }),
            coven_database::operation_kind::COVEN,
        )
        .await
        .unwrap();
    let member = ana.sync.member.read().unwrap().unwrap();
    let mut session = ana.sync.session.lock().await;
    let mut ring = ana.sync.ring(&mut session, &member).await.unwrap();
    let stopped = ana.sync.run_administration(&mut session, &mut ring, &member, raise).await;
    assert!(
        matches!(stopped, Err(crate::SyncError::Database(coven_database::DbError::ClaimUnsettled))),
        "{stopped:?}"
    );
    drop(session);

    insert(ben.database(), "b", "Ben's note").await;
    ben.sync.pass().await.unwrap();
    ana.sync.retry_operation(raise).await.unwrap();
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    let state = ana.database().store_state().await.unwrap();
    assert_eq!(state.reached(&Audience::Store), 2);
    assert!(ana.database().raise_captures().await.unwrap().is_empty());
    let operations = ana.database().operations().await.unwrap();
    assert!(operations.is_empty(), "{operations:?}");
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben's app migrates while he reads only the store. Ana, still on the old
/// app, pins in Garden, adds him, pins again and writes a store row. His passes
/// go past the admission and Garden's later writes without stopping: the store
/// goes on, Garden's parts pass unread, writes into Garden are refused, and the
/// device queues the migration write its rebuild of Garden will run under,
/// read through the admission's slot (§18.3, §18.4).
#[tokio::test]
async fn a_circle_readable_below_the_devices_version_waits_for_its_rebuild_while_the_pass_goes_on()
{
    use coven_merge::Audience;
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
    pin(ana.database(), "00000000-0000-4000-8000-000000000001", garden, "before").await;
    ana.sync
        .administer(coven_format::admin::StoreChange::AddCircleMember {
            circle: garden,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    let admitted = ana.database().applied_slot().await.unwrap();
    ana.sync.pass().await.unwrap();
    pin(ana.database(), "00000000-0000-4000-8000-000000000002", garden, "after").await;
    insert(ana.database(), "00000000-0000-4000-8000-000000000003", "store row").await;
    ana.sync.pass().await.unwrap();
    let written = ana.database().applied_slot().await.unwrap();
    assert!(written > admitted);

    ben.sync.pass().await.unwrap();
    // The device's rebuild marker is queued, read through the admission.
    let queued = ben.database().waiting_snapshot_headers().await.unwrap();
    assert_eq!(queued.len(), 1, "{queued:?}");
    let marker = &queued[0].header;
    assert_eq!(marker.disposition, coven_format::write::WriteDisposition::Migration);
    assert_eq!(marker.read_through, admitted);
    assert_eq!(marker.schema_version, 2);
    assert!(queued[0].parts.is_empty());
    let waiting = ben.database().rebuilds_waiting().await.unwrap();
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[&Audience::Circle(garden)], marker.position);
    assert!(ben
        .database()
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .iter()
        .any(|record| record.reason
            == crate::PendingReason::Waits(crate::Prerequisite::Reload(Audience::Circle(garden)))));

    ben.sync.pass().await.unwrap();
    assert!(ben.database().waiting_snapshot_headers().await.unwrap().is_empty());
    // The pass went on through the slots after the admission; Garden's parts
    // passed unread.
    assert!(ben.database().applied_slot().await.unwrap() >= written);
    assert!(pins(ben.database()).await.is_empty());
    let refused = ben
        .database()
        .write(move |sql| {
            sql.execute(
                "INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000004',?1,'mine')",
                [garden.to_string()],
            )?;
            Ok(())
        })
        .await;
    assert!(
        matches!(refused, Err(coven_database::DbError::AudienceReloading(Audience::Circle(c))) if c == garden),
        "{refused:?}"
    );
    // Its own marker lands for the device like any migration marker, and the
    // pass that reads it rebuilds the circle.
    ben.sync.pass().await.unwrap();
    assert!(ben.database().rebuilds_waiting().await.unwrap().is_empty());
    assert!(ben.database().reloading_audiences().await.unwrap().is_empty());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben's app migrated before Ana, still on the old app, added him to Garden,
/// where she had pinned. No device that migrated holds Garden's rows at
/// version 2, so Ben rebuilds it: once his marker has a slot he migrates
/// Garden and the store as they were when he started reading it, reads on to
/// the end of the sequence, holds Ana's first pin at version 2 and raises
/// Garden and the store. Her second pin landed after the slot he started
/// reading at, so it is a late write, which this migration drops into lost
/// values, as it would on a device that held Garden then (§18.6). Ana's
/// update finds both raised and loads them. A pin Ben makes after the rebuild
/// waits for Garden's raise and then sends (§18.3).
#[tokio::test]
async fn a_circle_joined_after_migrating_is_rebuilt_and_raised_by_the_member_who_joined() {
    use coven_merge::Audience;
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
    let before = "00000000-0000-4000-8000-000000000001";
    let after = "00000000-0000-4000-8000-000000000002";
    pin(ana.database(), before, garden, "before").await;
    ana.sync.pass().await.unwrap();
    ana.sync
        .administer(coven_format::admin::StoreChange::AddCircleMember {
            circle: garden,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    pin(ana.database(), after, garden, "after").await;
    ana.sync.pass().await.unwrap();

    // His marker is queued, sent and lands; the pass after it rebuilds.
    for _ in 0..4 {
        ben.sync.pass().await.unwrap();
    }
    assert!(ben.database().rebuilds_waiting().await.unwrap().is_empty());
    assert!(ben.database().reloading_audiences().await.unwrap().is_empty());
    assert_eq!(pins(ben.database()).await, [(before.to_string(), "before".to_string())]);
    for _ in 0..3 {
        ben.sync.pass().await.unwrap();
    }
    // He raised Garden himself.
    let state = ben.sync.database.store_state().await.unwrap();
    assert_eq!(state.reached(&Audience::Circle(garden)), 2);
    assert_eq!(state.schema[&Audience::Circle(garden)].snapshot.device, ben.sync.device);
    assert!(ben.database().raise_captures().await.unwrap().is_empty());
    let mine = "00000000-0000-4000-8000-000000000003";
    pin(ben.database(), mine, garden, "mine").await;
    ben.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();

    let ana = group.reopen(ana).await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    let state = ana.sync.database.store_state().await.unwrap();
    assert_eq!(state.reached(&Audience::Circle(garden)), 2);
    assert_eq!(state.schema[&Audience::Circle(garden)].snapshot.device, ben.sync.device);
    assert!(ana.sync.database.raise_captures().await.unwrap().is_empty());
    ana.sync.pass().await.unwrap();
    assert_eq!(pins(ana.database()).await, pins(ben.database()).await);
    assert_eq!(pins(ana.database()).await.len(), 2);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Garden holds a deed only its creator, Ana, may change. Ben, who rebuilds
/// Garden, is not its author, so the snapshot of his rebuild carries her
/// write's evidence, and the load of it checks that evidence as it checks a
/// snapshot of any other member (§9.6, §16.2). Ana's update loads his raise
/// and holds the deed.
#[tokio::test]
async fn a_rebuilt_circle_carries_the_evidence_of_another_members_owned_rows() {
    use coven_merge::Audience;
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    group.owned.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let garden = ana.sync.create_circle("Garden".into()).await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ben = group.reopen(ben).await;
    let deed_id = "00000000-0000-4000-8000-0000000000d1";
    deed(ana.database(), deed_id, garden, "the deed").await;
    ana.sync.pass().await.unwrap();
    ana.sync
        .administer(coven_format::admin::StoreChange::AddCircleMember {
            circle: garden,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    for _ in 0..7 {
        ben.sync.pass().await.unwrap();
    }
    assert!(ben.database().rebuilds_waiting().await.unwrap().is_empty());
    assert_eq!(deeds(ben.database()).await, [(deed_id.to_string(), "the deed".to_string())]);
    let state = ben.sync.database.store_state().await.unwrap();
    assert_eq!(state.reached(&Audience::Circle(garden)), 2);
    assert_eq!(state.schema[&Audience::Circle(garden)].snapshot.device, ben.sync.device);
    let ana = group.reopen(ana).await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(deeds(ana.database()).await, deeds(ben.database()).await);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben's pin into Garden, which waits for its rebuild, is refused, and the
/// note behind it goes on. Ana's update raises Garden; Ben loads her raise,
/// which ends the wait, and his pin then writes and sends (§18.3).
#[tokio::test]
async fn a_write_into_a_circle_awaiting_its_rebuild_is_refused_until_a_raise_of_the_circle_loads() {
    use coven_merge::Audience;
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
    assert_eq!(ben.database().rebuilds_waiting().await.unwrap().len(), 1);
    let id = "00000000-0000-4000-8000-000000000001";
    let refused = ben
        .database()
        .write(move |sql| {
            sql.execute("INSERT INTO pins VALUES(?1,?2,'seeds')", (id, garden.to_string()))?;
            Ok(())
        })
        .await;
    assert!(matches!(refused, Err(coven_database::DbError::AudienceReloading(_))), "{refused:?}");
    insert(ben.database(), "00000000-0000-4000-8000-000000000002", "after the pin").await;
    ben.sync.pass().await.unwrap();

    let ana = group.reopen(ana).await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    let state = ben.sync.database.store_state().await.unwrap();
    assert_eq!(state.reached(&Audience::Circle(garden)), 2);
    assert!(ben.database().rebuilds_waiting().await.unwrap().is_empty());
    assert!(ben.database().reloading_audiences().await.unwrap().is_empty());
    pin(ben.database(), id, garden, "seeds").await;
    ben.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(pins(ana.database()).await, [(id.to_string(), "seeds".to_string())]);
    assert!(rows(ana.database())
        .await
        .contains(&("00000000-0000-4000-8000-000000000002".into(), "after the pin".into())));
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben is added to Garden and Orchard after his app migrated; Ana can raise
/// only Garden, since Cy removed her from Orchard, and Cy has not migrated.
/// Ben raises Orchard by his rebuild of it, and loads Ana's raise of Garden
/// (§18.3).
#[tokio::test]
async fn a_circle_only_the_joined_member_can_raise_is_raised_by_his_rebuild() {
    use coven_merge::Audience;
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let cy = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &cy).await;
    let garden = ana.sync.create_circle("Garden".into()).await.unwrap();
    let orchard = ana.sync.create_circle("Orchard".into()).await.unwrap();
    ben.sync.pass().await.unwrap();
    cy.sync.pass().await.unwrap();
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
        .administer(coven_format::admin::StoreChange::AddCircleMember {
            circle: orchard,
            member: cy.identity.member(),
        })
        .await
        .unwrap();
    // Cy, who has not migrated, removes Ana from Orchard, and Ana reads that
    // before she migrates.
    cy.sync.pass().await.unwrap();
    cy.sync.remove_circle_member(orchard, ana.identity.member()).await.unwrap();
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    let waiting = |ben: &Device| {
        let database = ben.database().clone();
        async move { database.rebuilds_waiting().await.unwrap().into_keys().collect::<Vec<_>>() }
    };
    let both = waiting(&ben).await;
    assert_eq!(both.len(), 2, "{both:?}");

    let ana = group.reopen(ana).await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    for _ in 0..3 {
        ben.sync.pass().await.unwrap();
    }
    let state = ben.sync.database.store_state().await.unwrap();
    assert_eq!(state.reached(&Audience::Circle(garden)), 2);
    assert_eq!(state.reached(&Audience::Circle(orchard)), 2);
    assert!(waiting(&ben).await.is_empty());
    assert_eq!(state.schema[&Audience::Circle(orchard)].snapshot.device, ben.sync.device);
    for device in [&ana, &ben, &cy] {
        device.sync.database.close().await.unwrap();
    }
}

/// Garden waits for its rebuild and Ben deletes it. Nobody can raise or
/// rebuild a deleted circle, so applying the deletion ends the wait and its
/// record, and writes into Garden meet the deletion (§15.9, §18.3).
#[tokio::test]
async fn a_circle_deleted_while_it_awaits_its_rebuild_waits_no_more() {
    use coven_merge::Audience;
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
    let waits = async |ben: &Device| {
        ben.database()
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .into_iter()
            .filter(|record| {
                record.reason
                    == crate::PendingReason::Waits(crate::Prerequisite::Reload(Audience::Circle(
                        garden,
                    )))
            })
            .count()
    };
    assert_eq!(ben.database().rebuilds_waiting().await.unwrap().len(), 1);
    assert_eq!(waits(&ben).await, 1);

    ben.sync
        .administer(coven_format::admin::StoreChange::DeleteCircle { circle: garden })
        .await
        .unwrap();
    assert!(ben.database().rebuilds_waiting().await.unwrap().is_empty());
    assert!(ben.database().reloading_audiences().await.unwrap().is_empty());
    assert_eq!(waits(&ben).await, 0, "applying the deletion ended the wait");
    let refused = ben
        .database()
        .write(move |sql| {
            sql.execute(
                "INSERT INTO pins VALUES('00000000-0000-4000-8000-000000000001',?1,'seeds')",
                [garden.to_string()],
            )?;
            Ok(())
        })
        .await;
    assert!(
        matches!(refused, Err(coven_database::DbError::DeletedCircle(c)) if c == garden),
        "{refused:?}"
    );
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Garden waits for Ben's rebuild when his app runs a second breaking
/// migration. The rebuild now reaches that migration's version, so it runs
/// under that migration's write (§18.4).
#[tokio::test]
async fn a_second_breaking_migration_points_a_waiting_rebuild_at_its_marker() {
    use coven_merge::Audience;
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
    let first = ben.database().rebuilds_waiting().await.unwrap()[&Audience::Circle(garden)];

    group.version.store(4, std::sync::atomic::Ordering::SeqCst);
    let ben = group.reopen(ben).await;
    let queued = ben.database().waiting_snapshot_headers().await.unwrap();
    let marker = &queued.last().unwrap().header;
    assert_eq!(marker.schema_version, 4);
    let waiting = ben.database().rebuilds_waiting().await.unwrap();
    assert_eq!(waiting[&Audience::Circle(garden)], marker.position);
    assert_ne!(waiting[&Audience::Circle(garden)], first);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben waits for Garden's rebuild when his app runs a second breaking
/// migration. Garden is not a circle he ever held at any version, so the
/// migration captures the store and the circle his admission made, not
/// Garden; the rebuild, which now reaches the
/// second migration's version, installs the capture of Garden itself and Ben
/// raises it at that version (§18.3).
#[tokio::test]
async fn a_second_breaking_migration_does_not_capture_a_circle_awaiting_its_rebuild() {
    use coven_merge::Audience;
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
    assert!(ben
        .database()
        .rebuilds_waiting()
        .await
        .unwrap()
        .contains_key(&Audience::Circle(garden)));

    group.version.store(4, std::sync::atomic::Ordering::SeqCst);
    let ben = group.reopen(ben).await;
    let captured: Vec<_> = ben
        .database()
        .raise_captures()
        .await
        .unwrap()
        .into_iter()
        .map(|capture| capture.audience)
        .collect();
    // The circle Ben's admission made for him is one he holds at every
    // version, so it is captured with the store; Garden, which he never held
    // at any version, is not.
    assert!(captured.contains(&Audience::Store), "{captured:?}");
    assert!(!captured.contains(&Audience::Circle(garden)), "{captured:?}");
    assert_eq!(captured.len(), 2, "{captured:?}");

    for _ in 0..8 {
        ben.sync.pass().await.unwrap();
    }
    assert!(ben.database().rebuilds_waiting().await.unwrap().is_empty());
    let state = ben.sync.database.store_state().await.unwrap();
    assert_eq!(state.reached(&Audience::Circle(garden)), 4);
    assert_eq!(state.schema[&Audience::Circle(garden)].snapshot.device, ben.sync.device);
    assert!(ben.database().raise_captures().await.unwrap().is_empty());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Raise and snapshot, at `version`, the circle `ben`'s admission made, the one
/// circle of his below the store's version: retention keeps what a rebuild of
/// a circle below the store would read until he does (§16.6).
pub(crate) async fn raise_admitted_circle(ben: &Device, version: u32) {
    let own = {
        let session = ben.sync.session.lock().await;
        let state = &session.history.state;
        let store = state.reached(&coven_merge::Audience::Store);
        state
            .circles
            .keys()
            .copied()
            .find(|circle| state.reached(&coven_merge::Audience::Circle(*circle)) < store)
            .expect("the circle Ben's admission made")
    };
    let audience = coven_merge::Audience::Circle(own);
    ben.sync.raise_schema(audience.clone(), version).await.unwrap();
    ben.sync.snapshot(audience).await.unwrap();
}

// A part ahead of its audience's version, §18.5: a write a device made on its
// new app and sent in a capture before its raise.

/// The version the store reached and the device whose raise established it.
async fn store_raise(device: &Device) -> (u32, coven_foundation::id_source::DeviceId) {
    let state = device.sync.database.store_state().await.unwrap();
    let version = state.schema.get(&Audience::Store).expect("the store was raised");
    (version.number, version.snapshot.device)
}

/// Faults refusing every snapshot `device` would create of `audiences`.
pub(crate) fn refuse_snapshots(
    group: &Group,
    device: &Device,
    audiences: &[Audience],
) -> coven_storage::test_utils::Faults {
    let refused: std::collections::BTreeMap<_, _> = audiences
        .iter()
        .flat_map(|audience| {
            (1..=8).map(move |number| {
                let path = coven_storage::ObjectPath::snapshot(
                    group.store,
                    audience.clone(),
                    device.sync.device,
                    std::num::NonZeroU64::new(number).unwrap(),
                );
                (path, (coven_storage::StorageFailure::Network, 503))
            })
        })
        .collect();
    coven_storage::test_utils::Faults {
        create_failures: refused,
        ..coven_storage::test_utils::Faults::none()
    }
}

/// Ana writes note `b` at version 2, then both devices reach version 4, where
/// the migrations to 2 and 4 are breaking with `Drop` and 3 is an addition.
async fn ana_writes_between_two_breaking_migrations(group: &Group) -> (Device, Device) {
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    insert(&ana.sync.database, "b", "between the migrations").await;
    group.version.store(4, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    let ben = group.reopen(ben).await;
    (ana, ben)
}

#[tokio::test]
async fn a_captured_write_sent_ahead_of_the_raise_is_in_the_writers_raise() {
    let group = Group::new();
    let (ana, ben) = ana_writes_between_two_breaking_migrations(&group).await;
    for _ in 0..4 {
        ana.sync.pass().await.unwrap();
        ben.sync.pass().await.unwrap();
    }
    for device in [&ana, &ben] {
        assert_eq!(store_raise(device).await, (4, ana.sync.device));
        assert_eq!(
            rows(device.database()).await,
            vec![("b".to_string(), "between the migrations".to_string())]
        );
        assert!(device.database().lost_values().await.unwrap().is_empty());
    }
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_captured_write_a_racing_raise_covers_is_a_lost_value_everywhere() {
    let group = Group::new();
    let (ana, ben) = ana_writes_between_two_breaking_migrations(&group).await;
    ana.storage.set_faults(refuse_snapshots(&group, &ana, &[Audience::Store])).await;
    ana.sync.pass().await.unwrap();
    for _ in 0..4 {
        ben.sync.pass().await.unwrap();
    }
    ana.storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    for _ in 0..4 {
        ana.sync.pass().await.unwrap();
    }
    ben.sync.pass().await.unwrap();
    let mut kept = Vec::new();
    for device in [&ana, &ben] {
        assert_eq!(store_raise(device).await, (4, ben.sync.device));
        assert!(rows(device.database()).await.is_empty());
        let lost = device.database().lost_values().await.unwrap();
        assert_eq!(lost.len(), 1);
        assert_eq!(lost[0].table, "notes");
        assert!(lost[0].frozen);
        assert_eq!(lost[0].replaced_by, Replacement::SchemaChange { version: 4 });
        kept.push((lost[0].key.clone(), lost[0].lost.clone()));
    }
    assert_eq!(kept[0], kept[1]);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}
