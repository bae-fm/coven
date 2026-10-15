use crate::pending::tests::Record;
use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{
    Boundary, BoundaryCoverage, EntryId, EntryOutcome, PendingValue, SnapshotReload, SnapshotSource,
};
use coven_format::{store_log::SnapshotId, value::WritePositions};
use coven_foundation::id_source::{DeviceId, SequentialIds};
use coven_merge::Audience;

async fn drop_boundary(target: &crate::Database, entry: EntryId) {
    fate(target, entry, false).await.unwrap();
}

pub(crate) async fn fate(
    target: &crate::Database,
    entry: EntryId,
    kept: bool,
) -> Result<(), crate::DbError> {
    let log = target.store_log().await.unwrap();
    let mut next = log.clone();
    next.replay.entries.insert(
        entry,
        if kept {
            EntryOutcome::Kept
        } else {
            EntryOutcome::Dropped(crate::DropReason::NotAllowed)
        },
    );
    next.replay.state.resets.clear();
    next.replay.state.schema.clear();
    for boundary in next.boundaries().collect::<Vec<_>>() {
        boundary.apply(&mut next.replay.state);
    }
    target
        .apply_store_log_batch::<Record>(
            log,
            vec![],
            next.replay,
            vec![],
            vec![],
            Vec::new(),
            Vec::new(),
        )
        .await
}

async fn reset(target: &crate::Database, number: u64) -> EntryId {
    let entry = EntryId {
        device: DeviceId(99),
        number,
    };
    target
        .check_boundary_fixture(Boundary::Reset {
            entry,
            snapshot: SnapshotId {
                audience: Audience::Store,
                device: entry.device,
                number,
            },
        })
        .await
        .unwrap();
    entry
}

/// Reset the store to an empty snapshot, reading the applied `writes` the
/// reset ignores to their positions.
async fn empty(target: &crate::Database, writes: &[coven_format::write::WriteRecord]) {
    let log = target.store_log().await.unwrap();
    let mut reload = SnapshotReload::<std::io::Empty, _>::new(
        vec![],
        writes
            .iter()
            .map(crate::snapshot_write::tests::stream)
            .collect(),
        std::sync::Arc::new(crate::test_utils::TestAuthorship),
    );
    reload.snapshots = vec![SnapshotSource::Empty(Audience::Store)];
    reload.boundaries = Some(
        log.boundaries()
            .map(|b| BoundaryCoverage {
                entry: b.entry(),
                included: WritePositions(vec![]),
            })
            .collect(),
    );
    target
        .load_snapshots::<crate::pending::tests::Record, _, _>(reload)
        .await
        .unwrap();
}

#[tokio::test]
async fn dropping_a_loaded_reset_restores_rows_and_suppressed_judgments() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let target_store = TestStore::with_ids(&ids);
    let source = source_store.schema(notes(), NOTES).await.unwrap();
    let target = target_store.schema(notes(), NOTES).await.unwrap();
    sql(&source, "INSERT INTO notes VALUES('n','retained','body')")
        .await
        .unwrap();
    let write = records(&source).remove(0);
    target.receive(write.clone().into()).await.unwrap();
    let own = target.sync_state(vec![]).await.unwrap().device;
    let pending = Record::dropped_entry(
        EntryId {
            device: own,
            number: 123,
        },
        crate::DropReason::NotAllowed,
    );
    target.record_pending(pending.clone()).await.unwrap();
    let entry = EntryId {
        device: DeviceId(99),
        number: 1,
    };
    target
        .check_boundary_fixture(Boundary::Reset {
            entry,
            snapshot: SnapshotId {
                audience: Audience::Store,
                device: entry.device,
                number: 1,
            },
        })
        .await
        .unwrap();
    let mut reload = SnapshotReload::<std::io::Empty, _>::new(
        vec![],
        vec![crate::snapshot_write::tests::stream(&write)],
        std::sync::Arc::new(crate::test_utils::TestAuthorship),
    );
    reload.snapshots = vec![SnapshotSource::Empty(Audience::Store)];
    reload.boundaries = Some(vec![BoundaryCoverage {
        entry,
        included: WritePositions(vec![]),
    }]);
    target
        .load_snapshots::<crate::pending::tests::Record, _, _>(reload)
        .await
        .unwrap();
    assert_eq!(count(&target, "notes"), 0);
    assert!(target.pending::<Record>().await.unwrap().is_empty());
    drop_boundary(&target, entry).await;
    assert_eq!(
        count(&target, "notes"),
        1,
        "reset reversal must restore its retained input"
    );
    assert_eq!(target.pending::<Record>().await.unwrap(), [pending]);
    target.close().await.unwrap();
    let target = target_store.schema(notes(), NOTES).await.unwrap();
    assert_eq!(count(&target, "notes"), 1);
    target.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn dropping_a_raise_rejudges_retained_late_writes() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let target_store = TestStore::with_ids(&ids);
    let source = source_store.schema(notes(), NOTES).await.unwrap();
    let target = target_store.schema(notes(), NOTES).await.unwrap();
    let boundary = target.schema_fixture(Audience::Store, 2).await.unwrap();
    let entry = target.check_boundary_fixture(boundary).await.unwrap();
    let mut reload = SnapshotReload::<std::io::Empty, std::io::Empty>::new(
        vec![],
        vec![],
        std::sync::Arc::new(crate::test_utils::TestAuthorship),
    );
    reload.snapshots = vec![SnapshotSource::Empty(Audience::Store)];
    reload.boundaries = Some(vec![BoundaryCoverage {
        entry,
        included: WritePositions(vec![]),
    }]);
    target
        .load_snapshots::<crate::pending::tests::Record, _, _>(reload)
        .await
        .unwrap();
    sql(&source, "INSERT INTO notes VALUES('n','late','body')")
        .await
        .unwrap();
    let write = records(&source).remove(0);
    let position = write.header.position;
    target.receive(write.into()).await.unwrap();
    assert_eq!(count(&target, "notes"), 0);
    assert_eq!(target.lost_values().await.unwrap().len(), 1);
    drop_boundary(&target, entry).await;
    assert_eq!(count(&target, "notes"), 1);
    assert!(target.lost_values().await.unwrap().is_empty());
    assert!(target
        .sync_state(vec![])
        .await
        .unwrap()
        .positions
        .covers(position));
    target.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn an_unavailable_boundary_keeps_the_old_view_and_rejects_writes_until_reversed() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let target_store = TestStore::with_ids(&ids);
    let source = source_store.schema(notes(), NOTES).await.unwrap();
    let target = target_store.schema(notes(), NOTES).await.unwrap();
    sql(&target, "INSERT INTO notes VALUES('old','visible','')")
        .await
        .unwrap();
    let queued = records(&target);
    let entry = reset(&target, 1).await;
    assert_eq!(count(&target, "notes"), 1);
    assert!(matches!(
        sql(&target, "UPDATE notes SET title='blocked'").await,
        Err(crate::DbError::AudienceReloading(Audience::Store))
    ));
    assert_eq!(records(&target), queued);
    sql(&target, "INSERT INTO local_rows VALUES('still writable')")
        .await
        .unwrap();
    sql(&source, "INSERT INTO notes VALUES('remote','waiting','')")
        .await
        .unwrap();
    let write = records(&source).remove(0);
    assert_eq!(
        target.receive(write.clone().into()).await.unwrap(),
        crate::ApplyOutcome::Waiting(crate::WriteWait::Reload(Audience::Store))
    );
    assert!(!target
        .sync_state(vec![])
        .await
        .unwrap()
        .positions
        .covers(write.header.position));
    assert_eq!(target.pending::<Record>().await.unwrap().len(), 1);
    drop_boundary(&target, entry).await;
    assert!(target.pending::<Record>().await.unwrap().is_empty());
    sql(&target, "UPDATE notes SET title='unblocked'")
        .await
        .unwrap();
    assert_eq!(
        target.receive(write.into()).await.unwrap(),
        crate::ApplyOutcome::Applied
    );
    assert_eq!(count(&target, "notes"), 2);
    for db in [source, target] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn later_observations_and_completed_work_survive_reset_reversals() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    let own = db.sync_state(vec![]).await.unwrap().device;
    let record = |number| {
        Record::dropped_entry(
            EntryId {
                device: own,
                number,
            },
            crate::DropReason::NotAllowed,
        )
    };
    let old = record(40);
    let completed = record(41);
    db.record_pending(old.clone()).await.unwrap();
    db.record_pending(completed.clone()).await.unwrap();
    let first = reset(&db, 1).await;
    empty(&db, &[]).await;
    assert!(db.pending::<Record>().await.unwrap().is_empty());
    let second = reset(&db, 2).await;
    empty(&db, &[]).await;
    db.clear_pending(completed.subject().clone()).await.unwrap();
    db.record_pending(old.clone()).await.unwrap();
    assert_eq!(
        db.pending::<Record>().await.unwrap(),
        std::slice::from_ref(&old)
    );
    drop_boundary(&db, first).await;
    assert_eq!(
        db.pending::<Record>().await.unwrap(),
        std::slice::from_ref(&old)
    );
    drop_boundary(&db, second).await;
    assert_eq!(db.pending::<Record>().await.unwrap(), [old]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn every_kept_reset_suppresses_earlier_observations_across_restart() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    let old = Record::dropped_entry(
        EntryId {
            device: DeviceId(88),
            number: 50,
        },
        crate::DropReason::NotAllowed,
    );
    db.replace_pending_reports(vec![old.clone()]).await.unwrap();
    let first = reset(&db, 1).await;
    empty(&db, &[]).await;
    let second = reset(&db, 2).await;
    empty(&db, &[]).await;
    drop_boundary(&db, first).await;
    assert!(db.pending::<Record>().await.unwrap().is_empty());
    db.close().await.unwrap();
    let db = store.schema(notes(), NOTES).await.unwrap();
    assert!(db.pending::<Record>().await.unwrap().is_empty());
    drop_boundary(&db, second).await;
    assert_eq!(db.pending::<Record>().await.unwrap(), [old]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_kept_reset_never_hides_an_s3_key_awaiting_deletion() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    db.record_access_key_deletions::<Record>(
        None,
        vec![crate::AccessKeyDeletion {
            key: crate::AccessKeyToDelete {
                access_key_id: "old-key".into(),
                principal: coven_format::S3Principal::Console,
                member: None,
            },
            operation: crate::NewOperation {
                kind: "delete-access-key".into(),
                data: vec![],
                started_by: "coven".into(),
                pauses_writes: false,
            },
        }],
    )
    .await
    .unwrap();
    let before = db.pending::<Record>().await.unwrap();
    assert_eq!(before.len(), 1);
    reset(&db, 1).await;
    empty(&db, &[]).await;
    assert_eq!(db.pending::<Record>().await.unwrap(), before);
    db.close().await.unwrap();
}

#[tokio::test]
async fn failed_projection_rolls_back_replay_and_readers_observe_only_its_committed_result() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let store = TestStore::with_ids(&ids);
    let source = source_store.schema(notes(), NOTES).await.unwrap();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&source, "INSERT INTO notes VALUES('n','retained','')")
        .await
        .unwrap();
    let received = records(&source).remove(0);
    db.receive(received.clone().into()).await.unwrap();
    let entry = reset(&db, 1).await;
    empty(&db, &[received]).await;
    let log = db.store_log().await.unwrap();
    let before = crate::tests::contents(&db);
    let mut live = db.subscribe(move |sql| {
        Ok((
            sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?,
            sql.store_log()?.replay.entries[&entry].clone(),
        ))
    });
    assert_eq!(live.next().await.unwrap(), (0, EntryOutcome::Kept));
    db.inspect_writer(|sql| sql.batch("CREATE TEMP TRIGGER fail_projection BEFORE INSERT ON notes BEGIN SELECT RAISE(ABORT,'projection failure'); END").unwrap());
    assert!(fate(&db, entry, false).await.is_err());
    assert_eq!(db.store_log().await.unwrap(), log);
    assert_eq!(crate::tests::contents(&db), before);
    assert!(!live.is_marked_for_rerun());
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER fail_projection").unwrap());
    drop_boundary(&db, entry).await;
    assert_eq!(
        live.next().await.unwrap(),
        (1, EntryOutcome::Dropped(crate::DropReason::NotAllowed))
    );
    assert!(!live.is_marked_for_rerun());
    let restored = crate::tests::contents(&db);
    fate(&db, entry, false).await.unwrap();
    assert_eq!(crate::tests::contents(&db), restored);
    db.close().await.unwrap();
    let db = store.schema(notes(), NOTES).await.unwrap();
    assert_eq!(count(&db, "notes"), 1);
    for db in [source, db] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn peer_progress_removes_suppressed_reports_without_reviving_unchanged_reports() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    let report = |number| {
        Record::dropped_entry(
            EntryId {
                device: DeviceId(88),
                number,
            },
            crate::DropReason::NotAllowed,
        )
    };
    let old = report(40);
    db.replace_pending_reports(vec![old.clone(), report(41)])
        .await
        .unwrap();
    let entry = reset(&db, 1).await;
    empty(&db, &[]).await;
    db.replace_pending_reports(vec![old.clone(), report(42)])
        .await
        .unwrap();
    assert_eq!(db.pending::<Record>().await.unwrap(), [report(42)]);
    drop_boundary(&db, entry).await;
    assert_eq!(db.pending::<Record>().await.unwrap(), [old, report(42)]);
    db.close().await.unwrap();
}

async fn reset_at(target: &crate::Database, number: u64, seconds: u64) -> EntryId {
    let entry = EntryId {
        device: DeviceId(99),
        number,
    };
    target
        .check_boundary_fixture_at(
            Boundary::Reset {
                entry,
                snapshot: SnapshotId {
                    audience: Audience::Store,
                    device: entry.device,
                    number,
                },
            },
            coven_format::value::StorageTime::since_epoch(std::time::Duration::from_secs(seconds)),
        )
        .await
        .unwrap();
    entry
}

/// Retained data inputs, retained write parts, pending rows (suppressed or
/// not) and reset suppression links.
fn retained(db: &crate::Database) -> (i64, i64, i64, i64) {
    db.inspect_writer(|sql| {
        sql.query_row(
            "SELECT (SELECT count(*) FROM _coven_data_inputs),(SELECT count(*) FROM _coven_write_inputs),
                    (SELECT count(*) FROM _coven_pending),(SELECT count(*) FROM _coven_pending_resets)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap()
    })
}

fn notes_rows(db: &crate::Database) -> Vec<(String, String)> {
    db.inspect_writer(|sql| {
        sql.query("SELECT id,title FROM notes ORDER BY id", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap()
    })
}

async fn horizon(db: &crate::Database, seconds: u64) {
    db.advance_store_log_horizon(
        db.store_log().await.unwrap(),
        coven_format::value::StorageTime::since_epoch(std::time::Duration::from_secs(seconds)),
        coven_format::value::StorageTime::since_epoch(std::time::Duration::from_secs(seconds)),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn final_boundaries_release_inputs_and_a_later_boundary_retains_again() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let target_store = TestStore::with_ids(&ids);
    let source = source_store.schema(notes(), NOTES).await.unwrap();
    let target = target_store.schema(notes(), NOTES).await.unwrap();
    sql(&source, "INSERT INTO notes VALUES('early','before','')")
        .await
        .unwrap();
    // With a horizon no unseen retirement can still exclude a received write,
    // so its landing retains nothing (§10).
    horizon(&target, 0).await;
    let early = records(&source).remove(0);
    target.receive(early.clone().into()).await.unwrap();
    assert_eq!(
        retained(&target),
        (0, 0, 0, 0),
        "nothing depends on an entry"
    );
    let own = target.sync_state(vec![]).await.unwrap().device;
    let judged = Record::dropped_entry(
        EntryId {
            device: own,
            number: 77,
        },
        crate::DropReason::NotAllowed,
    );
    target.record_pending(judged).await.unwrap();
    reset_at(&target, 1, 10).await;
    empty(&target, &[early]).await;
    assert_eq!(count(&target, "notes"), 0);
    let other_store = TestStore::with_ids(&ids);
    let other = other_store.schema(notes(), NOTES).await.unwrap();
    sql(&other, "INSERT INTO notes VALUES('late','unread','')")
        .await
        .unwrap();
    assert_eq!(
        target
            .receive(records(&other).remove(0).into())
            .await
            .unwrap(),
        crate::ApplyOutcome::Applied
    );
    // The early write the reset ignored and the late one both stay retained
    // while the reset can still drop.
    let (inputs, writes, pending, links) = retained(&target);
    assert!(inputs > 0);
    assert_eq!((writes, pending, links), (2, 1, 1));
    assert!(target.pending::<Record>().await.unwrap().is_empty());
    let rows = notes_rows(&target);
    // Finality is strict: an entry stored exactly at the horizon is not final.
    horizon(&target, 10).await;
    assert_eq!(retained(&target).1, 2);
    horizon(&target, 11).await;
    assert_eq!(retained(&target), (0, 0, 0, 0));
    assert!(target.pending::<Record>().await.unwrap().is_empty());
    assert_eq!(notes_rows(&target), rows, "release changes no data");
    sql(&target, "INSERT INTO notes VALUES('after','final','')")
        .await
        .unwrap();
    let second = reset_at(&target, 2, 20).await;
    assert!(retained(&target).0 > 0);
    assert!(matches!(
        sql(&target, "UPDATE notes SET title='blocked'").await,
        Err(crate::DbError::AudienceReloading(Audience::Store))
    ));
    let before_second = count(&target, "notes");
    drop_boundary(&target, second).await;
    assert_eq!(count(&target, "notes"), before_second);
    assert!(target.pending::<Record>().await.unwrap().is_empty());
    sql(&target, "UPDATE notes SET title='writable'")
        .await
        .unwrap();
    horizon(&target, 21).await;
    assert_eq!(retained(&target), (0, 0, 0, 0));
    for db in [source, other, target] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_reload_covering_unreceived_writes_reloads_again_when_its_boundary_drops() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let target_store = TestStore::with_ids(&ids);
    let source = source_store.schema(notes(), NOTES).await.unwrap();
    let target = target_store.schema(notes(), NOTES).await.unwrap();
    sql(&source, "INSERT INTO notes VALUES('a','received','')")
        .await
        .unwrap();
    sql(
        &source,
        "INSERT INTO notes VALUES('b','only in the snapshot','')",
    )
    .await
    .unwrap();
    let writes = records(&source);
    target.receive(writes[0].clone().into()).await.unwrap();
    let entry = reset(&target, 1).await;
    let snapshot = crate::snapshot_write::tests::frames_for(
        &source,
        SnapshotId {
            audience: Audience::Store,
            device: entry.device,
            number: entry.number,
        },
    )
    .await;
    let mut reload = SnapshotReload::<_, std::io::Empty>::new(
        vec![snapshot.input()],
        vec![],
        std::sync::Arc::new(crate::test_utils::TestAuthorship),
    );
    reload.boundaries = Some(vec![BoundaryCoverage {
        entry,
        included: WritePositions(vec![writes[1].header.position]),
    }]);
    target
        .load_snapshots::<crate::pending::tests::Record, _, _>(reload)
        .await
        .unwrap();
    assert_eq!(count(&target, "notes"), 2);
    // The origin cannot reproduce write b, which arrived only inside the snapshot.
    drop_boundary(&target, entry).await;
    assert_eq!(count(&target, "notes"), 2);
    assert_eq!(target.pending::<Record>().await.unwrap().len(), 1);
    assert!(matches!(
        sql(&target, "INSERT INTO notes VALUES('c','','')").await,
        Err(crate::DbError::AudienceReloading(Audience::Store))
    ));
    for db in [source, target] {
        db.close().await.unwrap();
    }
}

/// Ben's track is in the store and Ana's review of it in their circle, so
/// deleting the track cascades into the circle's part of Ben's write. The
/// delete replaced a title Ben's other device set in a write that later
/// lands outside its device's retirement window. Replay rebuilds the store
/// without that write, so the delete used dropped data; the review's
/// cascade, in the circle, drops with it, and the circle is rebuilt too: the
/// review returns (§8, §8.8, §10).
#[tokio::test]
async fn a_rebuild_rejudges_actions_its_writes_caused_in_another_audience() {
    use crate::author_judgement::tests::deliver;
    use crate::judge::tests::{key, text, verdict};
    use crate::write_landing::tests::{at, retire, DAY};
    use coven_format::snapshot_rows::ChangeVerdict;
    use coven_format::write_sources::ChangeId;
    const REVIEW: &str = "00000000-0000-4000-8000-0000000000d1";
    let circle = coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(10));
    let declared = || {
        vec![
            crate::SyncedTable::new("tracks", crate::RowIdentity::SharedKey),
            crate::SyncedTable::new("reviews", crate::RowIdentity::IndependentUuid)
                .audience_column("audience"),
        ]
    };
    let ids = SequentialIds::new();
    let stores: Vec<_> = (0..4).map(|_| TestStore::with_ids(&ids)).collect();
    let mut dbs = Vec::new();
    for store in &stores {
        let db = store
            .schema(
                declared(),
                "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, title TEXT);
                 CREATE TABLE reviews(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL,
                   track TEXT REFERENCES tracks(id) ON DELETE CASCADE, body TEXT)",
            )
            .await
            .unwrap();
        store.join_circle(&db, circle);
        dbs.push(db);
    }
    let [ben, other, ana, target] = [&dbs[0], &dbs[1], &dbs[2], &dbs[3]];
    sql(ben, "INSERT INTO tracks VALUES('t','Song')")
        .await
        .unwrap();
    deliver(ben, other).await;
    deliver(ben, ana).await;
    sql(other, "UPDATE tracks SET title='Renamed' WHERE id='t'")
        .await
        .unwrap();
    deliver(other, ben).await;
    let audience = circle.to_string();
    ana.write(move |sql| {
        sql.execute(
            "INSERT INTO reviews VALUES(?1,?2,'t','good')",
            [REVIEW, audience.as_str()],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    deliver(ana, ben).await;
    sql(ben, "DELETE FROM tracks WHERE id='t'").await.unwrap();
    let ben_writes = records(ben);
    let (create, delete) = (&ben_writes[0], ben_writes.last().unwrap());
    let rename = &records(other)[0];
    let review_write = &records(ana)[0];
    let parts: Vec<_> = delete
        .parts
        .iter()
        .map(|part| part.audience.clone())
        .collect();
    assert_eq!(parts, [Audience::Store, Audience::Circle(circle)]);
    for (write, day) in [(create, 0), (rename, 31), (review_write, 0), (delete, 0)] {
        target
            .apply_downloaded::<Record>(
                write.clone().into(),
                crate::test_utils::published(at(day * DAY)),
            )
            .await
            .unwrap();
    }
    let review = coven_merge::RowId {
        audience: Audience::Circle(circle),
        ..key("reviews", REVIEW)
    };
    assert_eq!(count(target, "reviews"), 0);
    assert_eq!(
        verdict(target, delete, &review),
        ChangeVerdict::Derived(vec![ChangeId {
            write: delete.header.position,
            row: key("tracks", "t"),
        }])
    );
    retire(target, stores[1].device(), 1, "device", 0).await;
    assert_eq!(
        verdict(target, delete, &review),
        ChangeVerdict::Dependent(vec![ChangeId {
            write: delete.header.position,
            row: key("tracks", "t"),
        }])
    );
    assert_eq!(
        text(target, "SELECT (SELECT body FROM reviews)"),
        Some("good".into())
    );
    for db in dbs {
        db.close().await.unwrap();
    }
}
