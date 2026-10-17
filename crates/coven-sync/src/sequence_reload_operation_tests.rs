use crate::sequence_sync::sequence_reload::tests::stored_floor;
use crate::sequence_sync::tests::*;
use crate::{SequenceSync, SyncError};
use coven_database::DbError;
use coven_format::admin::StoreChange;
use coven_merge::Audience;
use coven_storage::{test_utils::Faults, ObjectPath, Storage, StorageFailure};
use serde_json::{json, Value};

// §15: retention can remove the selected snapshot or its required suffix.
#[tokio::test]
async fn a_retained_reload_uses_a_newer_floor_when_its_suffix_is_deleted() {
    for (older_circle_coverage, departure) in [(false, false), (true, false), (true, true)] {
        retained_reload(older_circle_coverage, departure).await;
    }
}

async fn retained_reload(
    older_circle_coverage: bool,
    departure: bool,
) -> (serde_json::Value, serde_json::Value) {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    if departure {
        let keeper = ana.sync.create_circle("Private".into()).await.unwrap();
        ana.sync.snapshot(Audience::Circle(keeper)).await.unwrap();
    }
    let circle = if older_circle_coverage {
        let circle = ana.sync.create_circle("Ana's".into()).await.unwrap();
        if departure {
            ana.sync
                .administer(StoreChange::AddCircleMember {
                    circle,
                    member: ben.identity.member(),
                })
                .await
                .unwrap();
        }
        let snapshot = ana.sync.snapshot(Audience::Circle(circle)).await.unwrap();
        Some((circle, snapshot))
    } else {
        None
    };
    insert(ana.database(), "a", "first").await;
    ana.sync.pass().await.unwrap();
    let first = ana.sync.snapshot(Audience::Store).await.unwrap();
    ben.sync.pass().await.unwrap();
    let old_target = ben.database().applied_slot().await.unwrap();
    let selected = if departure { circle.unwrap().1 } else { first };
    let old = ben.sync.session.lock().await.history.announcements[&selected].clone();
    let path = crate::snapshot_object::path(group.store, &old.snapshot).unwrap();
    ben.storage
        .set_faults(Faults {
            read_failures: [(path.clone(), StorageFailure::PermissionDenied)].into(),
            ..Faults::none()
        })
        .await;
    assert!(ben.sync.reload_from_snapshot().await.is_err());
    assert!(ben
        .database()
        .operations()
        .await
        .unwrap()
        .iter()
        .any(|r| r.kind == "sequence-reload"));
    ben.storage.set_faults(Faults::none()).await;
    insert(ana.database(), "b", "covered by the newer floor").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    let departed_at = if departure {
        ana.sync
            .snapshot(Audience::Circle(circle.unwrap().0))
            .await
            .unwrap();
        ana.sync
            .administer(StoreChange::RemoveCircleMember {
                circle: circle.unwrap().0,
                member: ben.identity.member(),
            })
            .await
            .unwrap();
        Some(ana.database().applied_slot().await.unwrap())
    } else {
        None
    };
    insert(ana.database(), "suffix", "after the replacement snapshot").await;
    ana.sync.pass().await.unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert!(ana.storage.status(&path).await.unwrap().is_none());
    let floor = stored_floor(&ana).await;
    assert_eq!(
        floor.header.first.0 > old.covers + 1,
        !older_circle_coverage
    );
    let history = ana.sync.session.lock().await.history.clone();
    let floor_selections: Vec<_> = floor
        .header
        .snapshots
        .iter()
        .enumerate()
        .map(|(audience, slot)| {
            let announcement = &history.announcements[slot];
            serde_json::json!({"slot":slot.0,"audience":audience,"covers":announcement.covers})
        })
        .collect();
    let covers: Vec<_> = floor
        .header
        .snapshots
        .iter()
        .filter_map(|slot| {
            let announcement = &history.announcements[slot];
            (announcement.snapshot.audience == Audience::Store
                || history
                    .state
                    .readable_circles(&ben.identity.member())
                    .into_iter()
                    .any(|circle| announcement.snapshot.audience == Audience::Circle(circle)))
            .then_some(announcement.covers)
        })
        .collect();
    let missing = if floor.header.first.0 > old.covers + 1 {
        None
    } else {
        let audience = floor
            .header
            .snapshots
            .iter()
            .position(|slot| history.announcements[slot].snapshot.audience == old.snapshot.audience)
            .unwrap();
        Some(serde_json::json!({"slot":selected.0,"audience":audience,"covers":old.covers}))
    };
    let input = serde_json::json!({"kind":"reselect","old_target":old_target,
        "floor_at":floor.header.at,"covers":covers,"missing":missing,"floor":floor_selections});
    let ben = group.reopen(ben).await;
    ben.sync.reload_from_snapshot().await.unwrap();
    assert!(ben.database().applied_slot().await.unwrap() >= floor.header.first.0 - 1);
    if let Some(departed_at) = departed_at {
        // §15: the chosen followed set and installed prefix agree.
        assert!(ben.database().applied_slot().await.unwrap() >= departed_at);
        assert!(!ben
            .database()
            .store_state()
            .await
            .unwrap()
            .reads_circle(circle.unwrap().0, &ben.identity.member(),));
    }
    assert_eq!(rows(ben.database()).await, rows(ana.database()).await);
    assert!(ben.database().operations().await.unwrap().is_empty());
    let actual = serde_json::json!(ben.database().applied_slot().await.unwrap());
    ana.database().close().await.unwrap();
    ben.database().close().await.unwrap();
    (input, actual)
}

/// A stale retention writer publishes an older floor after Ben selected a
/// newer snapshot. Missing that selected body must remain NotFound.
async fn older_floor() -> (serde_json::Value, serde_json::Value) {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(ana.database(), "a", "older floor").await;
    ana.sync.pass().await.unwrap();
    let floor_slot = ana.sync.snapshot(Audience::Store).await.unwrap();
    insert(ben.database(), "b", "newer selection").await;
    ben.sync.pass().await.unwrap();
    let selected_slot = ben.sync.snapshot(Audience::Store).await.unwrap();
    let old_target = ben.database().applied_slot().await.unwrap();
    let selected = ben.sync.session.lock().await.history.announcements[&selected_slot].clone();
    let path = crate::snapshot_object::path(group.store, &selected.snapshot).unwrap();
    ben.storage
        .set_faults(Faults {
            read_failures: [(path.clone(), StorageFailure::PermissionDenied)].into(),
            ..Faults::none()
        })
        .await;
    assert!(ben.sync.reload_from_snapshot().await.is_err());
    let record = ben
        .database()
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == "sequence-reload")
        .unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    let mut session = ana.sync.session.lock().await;
    ana.sync
        .retain_sequence(&mut session, &mut group.ring.clone(), &ana.identity)
        .await
        .unwrap();
    let mut ring = group.ring.clone();
    let floor = crate::floor_object::FloorObject::read(
        ana.database(),
        ana.sync.storage.as_ref(),
        &ObjectPath::floor(group.store),
        None,
        &mut ring,
        &ana.identity,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(floor.header.snapshots, [floor_slot]);
    let replacement = &session.history.announcements[&floor_slot];
    assert!(replacement.covers < selected.covers);
    let input = json!({"kind":"reselect","old_target":old_target,"floor_at":floor.header.at,
        "covers":[replacement.covers],
        "missing":{"slot":selected_slot.0,"audience":0,"covers":selected.covers},
        "floor":[{"slot":floor_slot.0,"audience":0,"covers":replacement.covers}]});
    drop(session);
    ben.storage
        .set_faults(Faults {
            read_failures: [(path, StorageFailure::NotFound)].into(),
            ..Faults::none()
        })
        .await;
    let before = rows(ben.database()).await;
    let result = ben.sync.reload_from_snapshot().await;
    assert!(
        matches!(&result, Err(SyncError::Storage(error))
        if error.failure() == StorageFailure::NotFound),
        "{result:?}"
    );
    assert_eq!(ben.database().applied_slot().await.unwrap(), old_target);
    assert_eq!(rows(ben.database()).await, before);
    assert_eq!(
        ben.database()
            .operation(record.id)
            .await
            .unwrap()
            .unwrap()
            .data,
        record.data
    );
    ana.database().close().await.unwrap();
    ben.database().close().await.unwrap();
    (input, serde_json::Value::Null)
}

#[tokio::test]
async fn a_missing_reload_body_does_not_reselect_an_older_floor() {
    older_floor().await;
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn sequence_reload_reselection_matches_lean() {
    for (older_circle_coverage, departure) in [(false, false), (true, false), (true, true)] {
        let (input, actual) = retained_reload(older_circle_coverage, departure).await;
        assert_eq!(
            actual,
            crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input),
            "{input}"
        );
    }
    let (input, actual) = older_floor().await;
    assert_eq!(
        actual,
        crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input),
        "{input}"
    );
}

async fn read(device: &Device) {
    let mut session = device.sync.session.lock().await;
    let mut ring = device.sync.keys.read().unwrap().unwrap();
    SequenceSync::read_forward(&device.sync, &mut session, &mut ring, &device.identity)
        .await
        .unwrap();
}

async fn during_load(
    device: &Device,
    path: ObjectPath,
    write: impl std::future::Future<Output = ()>,
) {
    let (started, observed) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel();
    device.storage.hold_next_read(path, started, held).await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let loading = read(device);
        tokio::pin!(loading);
        tokio::select! {
            () = &mut loading => panic!("reload finished before its held snapshot read"),
            result = observed => result.unwrap(),
        }
        write.await;
        release.send(()).unwrap();
        loading.await;
    })
    .await
    .unwrap();
}

// An original authored without the reset has the same past whether queued
// before the reload gate or admitted through a hypothetical gate-free commit.
async fn reset() -> Value {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ana.sync.reset(Audience::Store).await.unwrap();
    insert(ben.database(), "queued", "value").await;
    let originals = ben.database().waiting_snapshot_headers().await.unwrap();
    let snapshot = ana
        .sync
        .session
        .lock()
        .await
        .history
        .boundaries
        .last()
        .unwrap()
        .boundary
        .snapshot()
        .clone();
    let path = crate::snapshot_object::path(group.store, &snapshot).unwrap();
    during_load(&ben, path, async {
        assert!(matches!(
            ben.database()
                .write(|sql| {
                    sql.execute("INSERT INTO notes VALUES('during','value')", [])?;
                    Ok(())
                })
                .await,
            Err(DbError::AudienceReloading(Audience::Store))
        ));
    })
    .await;
    assert_eq!(
        ben.database().waiting_snapshot_headers().await.unwrap(),
        originals
    );
    let replayed = rows(ben.database()).await.len();
    assert_eq!(replayed, 0);
    assert!(ben.database().lost_values().await.unwrap().is_empty());
    insert(ben.database(), "queued", "value").await;
    let after = rows(ben.database()).await;
    assert_eq!(after, [("queued".into(), "value".into())]);
    let waiting = ben.database().waiting_snapshot_headers().await.unwrap();
    assert_eq!(waiting.len(), 2);
    ben.sync.reload_from_snapshot().await.unwrap();
    assert_eq!(rows(ben.database()).await, after);
    assert_eq!(
        ben.database().waiting_snapshot_headers().await.unwrap(),
        waiting
    );
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
    json!({"retained": originals.len(), "replayed": replayed, "after_replayed": after.len()})
}

#[tokio::test]
async fn reset_queue_preservation_does_not_make_an_unread_reset_writable() {
    reset().await;
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn sequence_queue_reset_matches_lean() {
    assert_eq!(
        reset().await,
        crate::tests::run_lean(
            "COVEN_SEQUENCE_LEAN",
            &json!({"kind":"queueReload", "scenario":"reset"})
        )
    );
}
