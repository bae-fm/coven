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

async fn migrate(group: &Group, device: Device) -> Device {
    device.database().close().await.unwrap();
    open_migrated(group, device._temp, device.identity).await
}

async fn open_migrated(
    group: &Group,
    temp: tempfile::TempDir,
    identity: coven_crypto::MemberIdentity,
) -> Device {
    use coven_database::{DatabaseBuilder, Migration, RowIdentity, SyncedTable};
    use coven_foundation::files::StoreLayout;
    let directory = StoreLayout::new(temp.path().into()).store_dir(&group.store);
    let database = DatabaseBuilder::new(directory)
        .synced_tables(vec![
            SyncedTable::new("notes", RowIdentity::SharedKey),
            SyncedTable::new("pins", RowIdentity::IndependentUuid).audience_column("audience"),
        ])
        .migrations(vec![
            Migration::sql(1, "notes", "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL);
                CREATE TABLE pins(id TEXT PRIMARY KEY NOT NULL,audience TEXT NOT NULL,title TEXT NOT NULL)"),
            Migration::sql(2, "titles", "CREATE INDEX titles ON notes(title);
                INSERT INTO notes SELECT 'derived',title FROM notes WHERE id='source'").writes(|_| Ok(())),
        ])
        .open().await.unwrap();
    group
        .attach(
            temp,
            identity,
            Some(group.ring.clone()),
            group.storage.for_device(),
            database,
        )
        .await
}

async fn breaking_raise() -> Value {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(ana.database(), "source", "value").await;
    ana.sync.pass().await.unwrap();
    // Ben migrates before reading source; Ana migrates after it. Receiving
    // source through the converter cannot rerun the data-changing migration.
    let ben = migrate(&group, ben).await;
    assert!(rows(ben.database()).await.is_empty());
    let ana = migrate(&group, ana).await;
    read(&ben).await;
    assert_eq!(
        rows(ben.database()).await,
        [("source".into(), "value".into())]
    );
    assert_eq!(rows(ana.database()).await.len(), 2);
    ben.database().close().await.unwrap();
    let copy = tempfile::tempdir().unwrap();
    copy_tree(ben._temp.path(), copy.path());
    let queued = open_migrated(&group, copy, ben.identity.clone()).await;
    let ben = open_migrated(&group, ben._temp, ben.identity).await;
    let before = queued
        .database()
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('derived','value')", [])?;
            Ok(())
        })
        .await;
    assert!(before.is_ok(), "{before:?}");
    let originals = queued.database().waiting_snapshot_headers().await.unwrap();
    let new_schema = originals.last().unwrap().header.schema_version == 2;
    assert!(new_schema);
    ana.sync.pass().await.unwrap();
    ana.sync.raise_schema(Audience::Store, 2).await.unwrap();
    read(&ben).await;
    assert_eq!(rows(ben.database()).await, rows(ana.database()).await);
    let after = ben
        .database()
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('derived','value')", [])?;
            Ok(())
        })
        .await;
    assert!(
        matches!(&after, Err(DbError::Sqlite(error))
        if error.to_string() == "UNIQUE constraint failed: notes.id"),
        "{after:?}"
    );
    for device in [&ana, &ben, &queued] {
        device.database().close().await.unwrap();
    }
    json!({"before_allowed": before.is_ok(), "after_allowed": after.is_ok(), "new_schema":new_schema})
}

#[tokio::test]
async fn a_breaking_raise_changes_the_commit_key_check_even_at_the_new_schema() {
    breaking_raise().await;
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn sequence_queue_breaking_raise_matches_lean() {
    assert_eq!(
        breaking_raise().await,
        crate::tests::run_lean(
            "COVEN_SEQUENCE_LEAN",
            &json!({"kind":"queueReload", "scenario":"breakingRaise"})
        )
    );
}

async fn circle_device(group: &Group, seed: u8) -> Device {
    use coven_database::{DatabaseBuilder, Migration, RowIdentity, SyncedTable};
    use coven_foundation::id_source::{IdSource, MemberId};
    let (temp, directory) = group.directory();
    let identity = coven_crypto::MemberIdentity::new(
        MemberId(group.ids.new_id()),
        coven_format::test_utils::keys_from_seed(seed),
    );
    let database = DatabaseBuilder::new(directory)
        .synced_tables(vec![
            SyncedTable::new("notes", RowIdentity::SharedKey),
            SyncedTable::new("pins", RowIdentity::SharedKey)
                .key_columns(["audience", "id"])
                .audience_column("audience"),
        ])
        .migrations(vec![Migration::sql(
            1,
            "shared circle keys",
            "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL);
             CREATE TABLE pins(id TEXT NOT NULL,audience TEXT NOT NULL,title TEXT NOT NULL,
                PRIMARY KEY(audience,id))",
        )])
        .open()
        .await
        .unwrap();
    group
        .attach(
            temp,
            identity,
            Some(group.ring.clone()),
            group.storage.for_device(),
            database,
        )
        .await
}

async fn newly_readable() -> Value {
    use coven_format::admin::StoreChange;
    let group = Group::new();
    let ana = circle_device(&group, 1).await;
    let ben = circle_device(&group, 2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let circle = ana.sync.create_circle("Shared keys".into()).await.unwrap();
    pin(ana.database(), "7", circle, "loaded").await;
    ana.sync.pass().await.unwrap();
    let snapshot = ana.sync.snapshot(Audience::Circle(circle)).await.unwrap();
    read(&ben).await;
    let before = ben
        .database()
        .write(move |sql| {
            sql.execute(
                "INSERT INTO pins VALUES('8',?1,'fresh')",
                [circle.to_string()],
            )?;
            Ok(())
        })
        .await;
    assert!(
        matches!(&before, Err(DbError::NotInCircle(_))),
        "{before:?}"
    );
    // Observe the actual SQL key check before history is loaded. Membership
    // still refuses the transaction; projecting future membership cannot
    // change this key check into the check against the loaded rows.
    let (send, empty_key) = tokio::sync::oneshot::channel();
    let empty = ben
        .database()
        .write(move |sql| {
            let result = sql.execute(
                "INSERT INTO pins VALUES('7',?1,'candidate')",
                [circle.to_string()],
            );
            send.send(result.is_ok()).unwrap();
            result?;
            Ok(())
        })
        .await;
    assert!(matches!(empty, Err(DbError::NotInCircle(_))), "{empty:?}");
    let empty_key_allowed = empty_key.await.unwrap();
    assert!(empty_key_allowed);
    ana.sync
        .administer(StoreChange::AddCircleMember {
            circle,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    let snapshot = ana.sync.session.lock().await.history.announcements[&snapshot]
        .snapshot
        .clone();
    let path = crate::snapshot_object::path(group.store, &snapshot).unwrap();
    during_load(&ben, path, async {
        let during = ben
            .database()
            .write(move |sql| {
                sql.execute(
                    "INSERT INTO pins VALUES('8',?1,'fresh')",
                    [circle.to_string()],
                )?;
                Ok(())
            })
            .await;
        assert!(
            matches!(during, Err(DbError::AudienceReloading(Audience::Circle(c))) if c == circle)
        );
    })
    .await;
    let loaded = ben
        .database()
        .write(move |sql| {
            sql.execute(
                "INSERT INTO pins VALUES('7',?1,'candidate')",
                [circle.to_string()],
            )?;
            Ok(())
        })
        .await;
    assert!(
        matches!(&loaded, Err(DbError::Sqlite(error))
        if error.to_string() == "UNIQUE constraint failed: pins.audience, pins.id"),
        "{loaded:?}"
    );
    let after = ben
        .database()
        .write(move |sql| {
            sql.execute(
                "INSERT INTO pins VALUES('8',?1,'fresh')",
                [circle.to_string()],
            )?;
            Ok(())
        })
        .await;
    assert!(after.is_ok(), "{after:?}");
    assert_eq!(
        pins(ben.database()).await,
        [("7".into(), "loaded".into()), ("8".into(), "fresh".into())]
    );
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
    json!({"before_allowed":before.is_ok(),"after_allowed":after.is_ok(),
        "empty_key_allowed":empty_key_allowed,"loaded_key_allowed":loaded.is_ok()})
}

#[tokio::test]
async fn a_new_circle_changes_membership_and_key_checks_when_history_installs() {
    newly_readable().await;
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn sequence_queue_newly_readable_matches_lean() {
    assert_eq!(
        newly_readable().await,
        crate::tests::run_lean(
            "COVEN_SEQUENCE_LEAN",
            &json!({"kind":"queueReload", "scenario":"newlyReadable"})
        )
    );
}
