use super::*;
use crate::entry_effects::tests::fate;
use crate::pending::tests::Record;
use crate::store_log::tests::{checked, keys, member};
use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{DeviceState, EntryOutcome, StoreDevice, StoreMember};
use coven_format::store_log::{MemberRole, StoreChange, StoreLogEntry};
use coven_format::value::EntryPositions;
use coven_foundation::id_source::SequentialIds;

pub(crate) const DAY: u64 = 24 * 60 * 60;

pub(crate) fn at(seconds: u64) -> StorageTime {
    StorageTime::since_epoch(Duration::from_secs(seconds))
}

/// A retiring entry about `writer`, a device of member 2, as replay supplies
/// it: a kept removal of the device or its member, or its replacement; or the
/// device's own registration, dropped, with no kept entry registering it.
pub(crate) async fn retire(
    target: &crate::Database,
    writer: DeviceId,
    number: u64,
    kind: &str,
    seconds: u64,
) -> EntryId {
    let log = target.store_log().await.unwrap();
    let id = EntryId {
        device: DeviceId(97),
        number,
    };
    let owner = member(2);
    let mut replay = log.replay;
    replay.state.members.entry(owner).or_insert(StoreMember {
        keys: keys(2),
        access: coven_format::MemberAccess::S3AccessKey {
            access_key_id: "fixture-access-key".into(),
            principal: coven_format::S3Principal::Console,
        },
        role: MemberRole::Member,
        removed: false,
    });
    replay.state.devices.entry(writer).or_insert(StoreDevice {
        member: owner,
        name: "writer".into(),
        state: DeviceState::Active,
    });
    let change = match kind {
        "device" => {
            replay.state.devices.get_mut(&writer).unwrap().state = DeviceState::Removed;
            StoreChange::RemoveDevice { device: writer }
        }
        "member" => {
            replay.state.members.get_mut(&owner).unwrap().removed = true;
            replay.state.devices.get_mut(&writer).unwrap().state = DeviceState::Removed;
            StoreChange::RemoveMember { member: owner }
        }
        "replacement" => {
            let fresh = DeviceId(900 + number);
            replay.state.devices.get_mut(&writer).unwrap().state = DeviceState::Replaced;
            replay.state.devices.insert(
                fresh,
                StoreDevice {
                    member: member(2),
                    name: "restored".into(),
                    state: DeviceState::Active,
                },
            );
            StoreChange::AddDevice {
                device: fresh,
                name: "restored".into(),
                replaces: Some(writer),
            }
        }
        "registration" => {
            replay.state.devices.remove(&writer);
            StoreChange::AddDevice {
                device: writer,
                name: "writer".into(),
                replaces: None,
            }
        }
        _ => unreachable!(),
    };
    let time = log.entries.iter().map(|e| e.entry.timestamp).max();
    let entry = StoreLogEntry {
        position: id,
        timestamp: coven_merge::Timestamp::new(
            time.map_or(1, |t| t.milliseconds() + 1),
            0,
            id.device,
        )
        .unwrap(),
        author: member(1),
        had_read: EntryPositions(Vec::new()),
        change,
    };
    replay.entries.insert(
        id,
        if kind == "registration" {
            EntryOutcome::Dropped(crate::DropReason::NotAllowed)
        } else {
            EntryOutcome::Kept
        },
    );
    let mut checked = checked(entry);
    checked.stored_at = at(seconds);
    target
        .apply_store_log::<Record>(checked, replay, Vec::new())
        .await
        .unwrap();
    id
}

struct Pair {
    _source_store: TestStore,
    _target_store: TestStore,
    source: crate::Database,
    target: crate::Database,
}

async fn pair() -> Pair {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let target_store = TestStore::with_ids(&ids);
    let source = source_store.schema(notes(), NOTES).await.unwrap();
    let target = target_store.schema(notes(), NOTES).await.unwrap();
    Pair {
        _source_store: source_store,
        _target_store: target_store,
        source,
        target,
    }
}

pub(crate) fn dropped_reports(db: &crate::Database) -> Vec<Record> {
    db.inspect_writer(|sql| crate::pending::read::<Record>(sql).unwrap())
        .into_iter()
        .filter(|record| record.dropped_write().is_some())
        .collect()
}

/// Device removal, member removal, replacement and a dropped registration of
/// a device no kept entry registers each give the write thirty storage days:
/// exactly thirty counts, one second more is consumed without effects or
/// losses and reported as landed too late (§10, D5,
/// `Writers.late_after_dropped_registration`).
#[tokio::test]
async fn every_retirement_admits_exactly_thirty_days_and_consumes_later_writes() {
    for kind in ["device", "member", "replacement", "registration"] {
        for (offset, late) in [(0, false), (1, true)] {
            let p = pair().await;
            sql(&p.source, "INSERT INTO notes VALUES('n','title','body')")
                .await
                .unwrap();
            let write = records(&p.source).remove(0);
            let position = write.header.position;
            retire(&p.target, position.device, 1, kind, 0).await;
            p.target
                .apply_downloaded::<Record>(
                    write.into(),
                    crate::test_utils::published(at(30 * DAY + offset)),
                )
                .await
                .unwrap();
            assert_eq!(count(&p.target, "notes"), i64::from(!late), "{kind}");
            assert!(p.target.lost_values().await.unwrap().is_empty());
            assert!(p
                .target
                .sync_state(vec![])
                .await
                .unwrap()
                .positions
                .covers(position));
            assert_eq!(
                dropped_reports(&p.target)
                    .iter()
                    .map(|r| r.dropped_write().unwrap())
                    .collect::<Vec<_>>(),
                if late { vec![position] } else { vec![] }
            );
        }
    }
}

/// A write whose recorded past read a kept replacement is bounded by that
/// replacement's thirty days, like an unread one. Its authority check, on the
/// replay of that past, already refused it if that view kept the replacement;
/// one that reaches landing had a view that dropped it (§10).
#[tokio::test]
async fn a_read_retirement_bounds_the_write_by_storage_time_alone() {
    for (offset, late) in [(0, false), (1, true)] {
        let p = pair().await;
        sql(&p.source, "INSERT INTO notes VALUES('n','title','body')")
            .await
            .unwrap();
        let mut write = records(&p.source).remove(0);
        let read = retire(&p.target, write.header.position.device, 1, "replacement", 0).await;
        write.header.store_log_read = EntryPositions(vec![read]);
        p.target
            .apply_downloaded::<Record>(
                write.into(),
                crate::test_utils::published(at(30 * DAY + offset)),
            )
            .await
            .unwrap();
        assert_eq!(count(&p.target, "notes"), i64::from(!late));
        assert_eq!(dropped_reports(&p.target).len(), usize::from(late));
        p.target.close().await.unwrap();
        p.source.close().await.unwrap();
    }
}

/// Every kept retirement imposes its own window; dropping the deciding one
/// rebuilds the audience with the write, and keeping it again removes it.
#[tokio::test]
async fn replay_recomputes_the_verdict_against_every_kept_retirement() {
    let p = pair().await;
    sql(&p.source, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    let write = records(&p.source).remove(0);
    let device = write.header.position.device;
    let first = retire(&p.target, device, 1, "device", 0).await;
    let second = retire(&p.target, device, 2, "replacement", 10 * DAY).await;
    p.target
        .apply_downloaded::<Record>(write.into(), crate::test_utils::published(at(35 * DAY)))
        .await
        .unwrap();
    assert_eq!(count(&p.target, "notes"), 0);
    // The later replacement's window still admits it.
    fate(&p.target, first, false).await.unwrap();
    assert_eq!(count(&p.target, "notes"), 1);
    assert!(dropped_reports(&p.target).is_empty());
    fate(&p.target, first, true).await.unwrap();
    assert_eq!(count(&p.target, "notes"), 0);
    assert_eq!(dropped_reports(&p.target).len(), 1);
    // The removal alone still excludes it without the replacement.
    fate(&p.target, second, false).await.unwrap();
    assert_eq!(count(&p.target, "notes"), 0);
    assert_eq!(dropped_reports(&p.target).len(), 1);
    p.target.close().await.unwrap();
    p.source.close().await.unwrap();
}

/// A retirement that arrives after the write it excludes rebuilds the audience
/// from the input retained when the unsettled write landed: the write leaves
/// with no loss and app writes need no reload (§9, §10).
#[tokio::test]
async fn a_retirement_arriving_after_its_write_rebuilds_without_it() {
    let p = pair().await;
    sql(&p.source, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    let write = records(&p.source).remove(0);
    let device = write.header.position.device;
    p.target
        .apply_downloaded::<Record>(write.into(), crate::test_utils::published(at(31 * DAY)))
        .await
        .unwrap();
    assert_eq!(count(&p.target, "notes"), 1);
    retire(&p.target, device, 1, "device", 0).await;
    assert_eq!(count(&p.target, "notes"), 0);
    assert!(p.target.lost_values().await.unwrap().is_empty());
    sql(&p.target, "INSERT INTO notes VALUES('m','t','b')")
        .await
        .unwrap();
    assert_eq!(dropped_reports(&p.target).len(), 1);
}

/// Settled verdicts and their retained inputs are released with the horizon.
#[tokio::test]
async fn a_settled_verdict_releases_its_retained_inputs() {
    let p = pair().await;
    sql(&p.source, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    let write = records(&p.source).remove(0);
    let device = write.header.position.device;
    retire(&p.target, device, 1, "device", 0).await;
    p.target
        .apply_downloaded::<Record>(write.into(), crate::test_utils::published(at(31 * DAY)))
        .await
        .unwrap();
    let retained = |db: &crate::Database| {
        db.inspect_writer(|sql| {
            sql.query_row(
                "SELECT (SELECT count(*) FROM _coven_data_inputs),(SELECT count(*) FROM _coven_write_landings)",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
            )
            .unwrap()
        })
    };
    assert_eq!(retained(&p.target), (1, 1));
    // The deciding kept retirement is final: the late verdict cannot change
    // (`retirement_verdict_final`), so its inputs and verdict go.
    p.target
        .advance_store_log_horizon(p.target.store_log().await.unwrap(), at(1), at(1))
        .await
        .unwrap();
    assert_eq!(retained(&p.target), (0, 0));
    assert_eq!(count(&p.target, "notes"), 0);
    // The report stays: it is the write's own outcome (§19.1).
    assert_eq!(dropped_reports(&p.target).len(), 1);
}

/// An admitted write's verdict is final only once every retirement that could
/// exclude it, stored more than thirty days earlier, lies in the final prefix.
#[tokio::test]
async fn an_admitted_verdict_settles_when_the_horizon_passes_its_window() {
    let p = pair().await;
    sql(&p.source, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    let write = records(&p.source).remove(0);
    p.target
        .apply_downloaded::<Record>(write.into(), crate::test_utils::published(at(31 * DAY)))
        .await
        .unwrap();
    let landings = |db: &crate::Database| {
        db.inspect_writer(|sql| {
            sql.query_row("SELECT count(*) FROM _coven_write_landings", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap()
        })
    };
    assert_eq!(landings(&p.target), 1);
    let horizon = |seconds| {
        let db = &p.target;
        async move {
            db.advance_store_log_horizon(db.store_log().await.unwrap(), at(seconds), at(seconds))
                .await
                .unwrap()
        }
    };
    horizon(DAY - 1).await;
    assert_eq!(landings(&p.target), 1);
    horizon(DAY).await;
    assert_eq!(landings(&p.target), 0);
    assert_eq!(count(&p.target, "notes"), 1);
}

/// Lean `Receive.withinWindow`, `window_exact` and `retirement_verdict_final`
/// against the Rust window: every kept retirement, exact boundaries, reads of
/// kept retirements (which the window ignores: the write's own view decides
/// exclusion), and later retirement sets agreeing on a final prefix through an
/// anchor.
#[test]
#[ignore = "requires COVEN_RECEIVE_LEAN"]
fn mg_differential_retirement_window() {
    use serde_json::json;
    use std::io::Write;
    use std::process::{Command, Stdio};
    let entry = |n: u64| EntryId {
        device: DeviceId(1),
        number: n,
    };
    let pool = [(1, 0), (2, 5 * DAY), (3, 10 * DAY), (4, 10 * DAY)];
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for chosen in 0..1u32 << pool.len() {
        let kept: Vec<_> = pool
            .iter()
            .enumerate()
            .filter(|(bit, _)| chosen & (1 << bit) != 0)
            .map(|(_, &(n, s))| Retirement {
                entry: entry(n),
                stored_at: at(s),
            })
            .collect();
        for stored in [
            0,
            30 * DAY - 1,
            30 * DAY,
            30 * DAY + 1,
            35 * DAY,
            40 * DAY,
            40 * DAY + 1,
        ] {
            for read in [vec![], vec![1], vec![3]] {
                let verdict = within_window(at(stored), &kept);
                let json_of = |set: &[Retirement]| {
                    set.iter()
                        .map(|r| {
                            json!({"device": 7, "entry": r.entry.number,
                                   "stored": r.stored_at.seconds()})
                        })
                        .collect::<Vec<_>>()
                };
                let mut case = json!({"check": "window", "device": 7, "stored": stored,
                                      "read": read, "kept": json_of(&kept)});
                // A later set: the final prefix through the earliest kept
                // retirement is fixed; later retirements appear or vanish.
                if let Some(anchor) = kept.iter().min_by_key(|r| r.stored_at) {
                    let mut later: Vec<_> = kept
                        .iter()
                        .filter(|r| r.stored_at <= anchor.stored_at)
                        .copied()
                        .collect();
                    later.push(Retirement {
                        entry: entry(9),
                        stored_at: anchor.stored_at + Duration::from_secs(DAY),
                    });
                    let again = within_window(at(stored), &later);
                    assert_eq!(again, verdict, "retirement verdict changed after finality");
                    case["later"] = json!(json_of(&later));
                    actual.push(json!([verdict, again]));
                    cases.push(case);
                    continue;
                }
                actual.push(json!(verdict));
                cases.push(case);
            }
        }
    }
    let runner = std::env::var("COVEN_RECEIVE_LEAN").expect("Lean receive runner");
    let mut child = Command::new(runner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&cases).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let expected: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(serde_json::Value::Array(actual), expected);
}

/// B17's four histories (§10 example): Ana's creation lands one second
/// outside her removed device's window. Ben read it, edited the note and, in
/// the same write, renamed his own playlist; Carol deleted the note. Ben's
/// note edit and Carol's delete become frozen losses naming the creation;
/// the playlist rename applies; both receive orders agree. Dropping the
/// removal restores the creation and both dependents (§8, §10).
#[tokio::test]
async fn changes_built_on_a_late_write_are_frozen_and_unrelated_ones_apply() {
    use crate::Replacement;
    let ids = SequentialIds::new();
    let stores: Vec<_> = (0..5).map(|_| TestStore::with_ids(&ids)).collect();
    let mut dbs = Vec::new();
    for store in &stores {
        dbs.push(store.schema(notes(), NOTES).await.unwrap());
    }
    let [ana, ben, carol, first, second] = &dbs[..] else {
        unreachable!()
    };
    sql(ana, "INSERT INTO notes VALUES('note','draft','body')")
        .await
        .unwrap();
    let creation = records(ana).remove(0);
    sql(ben, "INSERT INTO notes VALUES('playlist','Mix','')")
        .await
        .unwrap();
    for reader in [ben, carol] {
        reader.receive(creation.clone().into()).await.unwrap();
    }
    sql(
        ben,
        "UPDATE notes SET title='edited' WHERE id='note'; UPDATE notes SET title='Road trip' WHERE id='playlist'",
    )
    .await
    .unwrap();
    sql(carol, "DELETE FROM notes WHERE id='note'")
        .await
        .unwrap();
    let ben_writes = records(ben);
    let delete = records(carol).remove(0);
    let rows = |db: &crate::Database| {
        db.inspect_writer(|sql| {
            sql.query("SELECT id,title FROM notes ORDER BY id", [], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })
            .unwrap()
        })
    };
    let mut results = Vec::new();
    for (target, delete_first) in [(first, false), (second, true)] {
        let removal = retire(target, creation.header.position.device, 1, "device", 0).await;
        target
            .apply_downloaded::<Record>(
                creation.clone().into(),
                crate::test_utils::published(at(30 * DAY + 1)),
            )
            .await
            .unwrap();
        target.receive(ben_writes[0].clone().into()).await.unwrap();
        if delete_first {
            target.receive(delete.clone().into()).await.unwrap();
            target.receive(ben_writes[1].clone().into()).await.unwrap();
        } else {
            target.receive(ben_writes[1].clone().into()).await.unwrap();
            target.receive(delete.clone().into()).await.unwrap();
        }
        assert_eq!(rows(target), [("playlist".into(), "Road trip".into())]);
        let mut losses = target.lost_values().await.unwrap();
        losses.sort_by_key(|loss| format!("{:?}", loss.replaced_by));
        let causes: Vec<_> = losses
            .iter()
            .map(|loss| match &loss.replaced_by {
                Replacement::DataDependency { write, sources } => {
                    (*write, sources.iter().map(|s| s.write).collect::<Vec<_>>())
                }
                other => panic!("{other:?}"),
            })
            .collect();
        let created = creation.header.position;
        let mut expected = vec![
            (ben_writes[1].header.position, vec![created, created]),
            // Generation, then the id, title and body setters it replaced.
            (delete.header.position, vec![created; 4]),
        ];
        expected.sort_by_key(|(write, _)| format!("{write:?}"));
        let mut causes = causes;
        causes.sort_by_key(|(write, _)| format!("{write:?}"));
        assert_eq!(causes, expected);
        results.push((rows(target), causes));
        // Restored, Ben's edit and Carol's concurrent delete merge as usual:
        // the delete wins and Ben's edited title is an ordinary lost cell.
        fate(target, removal, false).await.unwrap();
        let losses = target.lost_values().await.unwrap();
        assert_eq!(losses.len(), 1);
        assert_eq!(
            losses[0].replaced_by,
            Replacement::Write(delete.header.position)
        );
        assert!(rows(target).iter().all(|(id, _)| id != "note"));
        assert_eq!(rows(target).len(), 1);
    }
    assert_eq!(results[0], results[1]);
}

/// A device that has a row only through a snapshot holds its original
/// changes as source evidence: it authors its own edit of that row, applies a
/// peer's later edit naming those sources, and keeps a dropped source dropped
/// after its loss was dismissed (§8, §15, D7).
#[tokio::test]
async fn snapshot_evidence_keeps_sources_and_verdicts_for_later_changes() {
    use crate::snapshot_write::tests::{frames, id, load_one};
    use crate::Replacement;
    let ids = SequentialIds::new();
    let stores: Vec<_> = (0..4).map(|_| TestStore::with_ids(&ids)).collect();
    let mut dbs = Vec::new();
    for store in &stores {
        dbs.push(store.schema(notes(), NOTES).await.unwrap());
    }
    let [ana, ben, source, fresh] = &dbs[..] else {
        unreachable!()
    };
    sql(
        ana,
        "INSERT INTO notes VALUES('kept','one',''),('late','two','')",
    )
    .await
    .unwrap();
    let ana_write = records(ana).remove(0);
    // Ana's write lands late at the source; Ben, who read it, edits both rows.
    ben.receive(ana_write.clone().into()).await.unwrap();
    sql(ben, "UPDATE notes SET title='edited'").await.unwrap();
    let ben_write = records(ben).remove(0);
    retire(source, ana_write.header.position.device, 1, "device", 0).await;
    source
        .apply_downloaded::<Record>(
            ana_write.clone().into(),
            crate::test_utils::published(at(30 * DAY + 1)),
        )
        .await
        .unwrap();
    source.receive(ben_write.clone().into()).await.unwrap();
    let losses = source.lost_values().await.unwrap();
    assert_eq!(losses.len(), 2);
    assert!(losses
        .iter()
        .all(|loss| matches!(loss.replaced_by, Replacement::DataDependency { .. })));
    // Dismissing the displayed losses changes no verdict.
    source.dismiss_lost_values(&losses).await.unwrap();
    assert!(source.lost_values().await.unwrap().is_empty());
    let snapshot = frames(source, coven_merge::Audience::Store).await;
    // The fresh device has none of these writes, only the snapshot.
    retire(fresh, ana_write.header.position.device, 1, "device", 0).await;
    load_one(
        fresh,
        id(coven_merge::Audience::Store),
        snapshot.prefix.clone(),
        std::io::Cursor::new(snapshot.concat()),
    )
    .await
    .unwrap();
    // Ben edits again, naming his own frozen change and Ana's dropped one.
    sql(ben, "UPDATE notes SET body='again'").await.unwrap();
    let again = records(ben).remove(1);
    assert_eq!(
        fresh.receive(again.clone().into()).await.unwrap(),
        crate::ApplyOutcome::Applied
    );
    let frozen = fresh.lost_values().await.unwrap();
    assert_eq!(frozen.len(), 2);
    assert!(frozen.iter().all(|loss| matches!(
        &loss.replaced_by,
        Replacement::DataDependency { write, .. } if *write == again.header.position
    )));
    // An identical snapshot's evidence loads again; a different original
    // under the same identity is refused and nothing commits.
    load_one(
        fresh,
        id(coven_merge::Audience::Store),
        snapshot.prefix.clone(),
        std::io::Cursor::new(snapshot.concat()),
    )
    .await
    .unwrap();
}

/// A device that loaded a row from a snapshot authors its own change to it,
/// naming the snapshot's original changes as its sources.
#[tokio::test]
async fn a_row_known_only_from_a_snapshot_can_be_edited_locally() {
    use crate::snapshot_write::tests::{frames, id, load_one};
    let ids = SequentialIds::new();
    let stores: Vec<_> = (0..3).map(|_| TestStore::with_ids(&ids)).collect();
    let mut dbs = Vec::new();
    for store in &stores {
        dbs.push(store.schema(notes(), NOTES).await.unwrap());
    }
    let [author, source, fresh] = &dbs[..] else {
        unreachable!()
    };
    sql(author, "INSERT INTO notes VALUES('n','one','')")
        .await
        .unwrap();
    source
        .receive(records(author).remove(0).into())
        .await
        .unwrap();
    let snapshot = frames(source, coven_merge::Audience::Store).await;
    load_one(
        fresh,
        id(coven_merge::Audience::Store),
        snapshot.prefix.clone(),
        std::io::Cursor::new(snapshot.concat()),
    )
    .await
    .unwrap();
    sql(fresh, "UPDATE notes SET title='two'").await.unwrap();
    let edit = records(fresh).remove(0);
    let sources: Vec<_> = edit.parts[0].rows[0].sources.changes().cloned().collect();
    assert_eq!(sources.len(), 2);
    assert!(sources
        .iter()
        .all(|source| source.write == records(author)[0].header.position));
    assert_eq!(
        source.receive(edit.into()).await.unwrap(),
        crate::ApplyOutcome::Applied
    );
}

/// The window compares provider times at their full precision on every
/// platform: one nanosecond past thirty days is late, exactly thirty days is
/// admitted. A 100 ns Windows `SystemTime` could not tell these apart (§10).
#[test]
fn one_nanosecond_past_the_window_is_late() {
    let retired = StorageTime::new(1_760_400_000, 123_456_789).unwrap();
    let kept = [Retirement {
        entry: EntryId {
            device: DeviceId(97),
            number: 1,
        },
        stored_at: retired,
    }];
    assert!(within_window(retired + WRITE_WINDOW, &kept));
    assert!(!within_window(
        retired + WRITE_WINDOW + Duration::from_nanos(1),
        &kept
    ));
    assert!(within_window(retired, &kept));
}

/// What a snapshot of the store waits for before preparing: the deciding
/// retirement until it is final, and receipt reaching thirty days before the
/// write, whichever way replay currently judges it (§15).
async fn verdict_wait(db: &crate::Database, through: Option<u64>) -> Option<VerdictWait> {
    db.snapshot_verdict_wait(
        Audience::Store,
        keys(1).signing,
        through.map(|t| at(t) + WRITE_WINDOW),
    )
    .await
    .unwrap()
}

/// Whether the removal is kept (the write is late) or dropped (it is admitted),
/// a snapshot covering the write waits for the removal to be final, and for a
/// catch-up whose receipt reaches thirty days before the write. Another
/// audience is not held; once the horizon passes the removal nothing is.
#[tokio::test]
async fn preparation_waits_for_a_covered_writes_deciding_retirement_to_be_final() {
    for kept in [true, false] {
        let p = pair().await;
        sql(&p.source, "INSERT INTO notes VALUES('n','title','body')")
            .await
            .unwrap();
        let write = records(&p.source).remove(0);
        let removal = retire(&p.target, write.header.position.device, 1, "device", 0).await;
        if !kept {
            fate(&p.target, removal, false).await.unwrap();
        }
        p.target
            .apply_downloaded::<Record>(
                write.into(),
                crate::test_utils::published(at(30 * DAY + 1)),
            )
            .await
            .unwrap();
        assert_eq!(count(&p.target, "notes"), i64::from(!kept));
        assert_eq!(
            verdict_wait(&p.target, None).await,
            Some(VerdictWait::CatchUp)
        );
        assert_eq!(
            verdict_wait(&p.target, Some(0)).await,
            Some(VerdictWait::CatchUp)
        );
        assert_eq!(
            verdict_wait(&p.target, Some(1)).await,
            Some(VerdictWait::Entry(removal))
        );
        let circle = Audience::Circle(coven_foundation::id_source::CircleId(
            uuid::Uuid::from_u128(5),
        ));
        assert_eq!(
            p.target
                .snapshot_verdict_wait(circle, keys(1).signing, Some(at(1) + WRITE_WINDOW))
                .await
                .unwrap(),
            None
        );
        p.target
            .advance_store_log_horizon(p.target.store_log().await.unwrap(), at(1), at(1))
            .await
            .unwrap();
        assert_eq!(verdict_wait(&p.target, Some(1)).await, None, "kept {kept}");
        // A late verdict anchored by its final removal needs no receipt.
        if kept {
            assert_eq!(verdict_wait(&p.target, None).await, None);
        }
    }
}

/// Writes alone never hold a snapshot: an admitted write whose device has no
/// retirement waits for nothing but receipt, with no horizon at all (§15).
#[tokio::test]
async fn a_write_without_retirements_waits_only_for_receipt() {
    let p = pair().await;
    sql(&p.source, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    p.target
        .apply_downloaded::<Record>(
            records(&p.source).remove(0).into(),
            crate::test_utils::published(at(DAY)),
        )
        .await
        .unwrap();
    assert_eq!(p.target.store_log().await.unwrap().horizon, None);
    assert_eq!(verdict_wait(&p.target, Some(0)).await, None);
    assert_eq!(
        verdict_wait(&p.target, None).await,
        Some(VerdictWait::CatchUp)
    );
}

/// This device's own writes have no recorded publication time: a retirement of
/// its own device holds its snapshots until final, even while dropped (§15).
#[tokio::test]
async fn an_own_devices_unfinal_retirement_holds_its_snapshots() {
    let p = pair().await;
    let own = p.target.sync_state(vec![]).await.unwrap().device;
    let removal = retire(&p.target, own, 1, "device", 5).await;
    fate(&p.target, removal, false).await.unwrap();
    assert_eq!(
        verdict_wait(&p.target, Some(10)).await,
        Some(VerdictWait::Entry(removal))
    );
    p.target
        .advance_store_log_horizon(p.target.store_log().await.unwrap(), at(6), at(6))
        .await
        .unwrap();
    assert_eq!(verdict_wait(&p.target, Some(10)).await, None);
}

/// The history of storelog-data `gated_snapshot_waits_then_loads`: Ben's write lands
/// one second past his removal's deadline while Ana's replay keeps the removal.
/// Her snapshot waits; once the removal is dropped and final it carries the
/// write admitted. Ben, who recorded the admission unfixed, and Carol, who
/// never received the write, both load it and hold the same rows (§10, §15).
#[tokio::test]
async fn a_snapshot_waits_for_a_dropped_removal_then_loads_everywhere() {
    use crate::snapshot_write::tests::{frames, id, load_one};
    let ids = SequentialIds::new();
    let stores: Vec<_> = (0..4).map(|_| TestStore::with_ids(&ids)).collect();
    let mut dbs = Vec::new();
    for store in &stores {
        dbs.push(store.schema(notes(), NOTES).await.unwrap());
    }
    let [writer, ana, ben, carol] = &dbs[..] else {
        unreachable!()
    };
    sql(writer, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    let write = records(writer).remove(0);
    let device = write.header.position.device;
    let published = crate::test_utils::published(at(30 * DAY + 1));
    let removal = retire(ana, device, 1, "device", 0).await;
    ana.apply_downloaded::<Record>(write.clone().into(), published)
        .await
        .unwrap();
    assert_eq!(count(ana, "notes"), 0);
    assert_eq!(
        verdict_wait(ana, Some(51 * DAY)).await,
        Some(VerdictWait::Entry(removal))
    );
    fate(ana, removal, false).await.unwrap();
    assert_eq!(count(ana, "notes"), 1);
    assert_eq!(
        verdict_wait(ana, Some(51 * DAY)).await,
        Some(VerdictWait::Entry(removal))
    );
    ana.advance_store_log_horizon(ana.store_log().await.unwrap(), at(61 * DAY), at(91 * DAY))
        .await
        .unwrap();
    assert_eq!(verdict_wait(ana, Some(91 * DAY)).await, None);
    let snapshot = frames(ana, Audience::Store).await;

    retire(ben, device, 1, "device", 0).await;
    fate(ben, removal, false).await.unwrap();
    ben.apply_downloaded::<Record>(write.clone().into(), published)
        .await
        .unwrap();
    retire(carol, device, 1, "device", 0).await;
    fate(carol, removal, false).await.unwrap();
    for loader in [ben, carol] {
        load_one(
            loader,
            id(Audience::Store),
            snapshot.prefix.clone(),
            std::io::Cursor::new(snapshot.concat()),
        )
        .await
        .unwrap();
        assert_eq!(count(loader, "notes"), 1);
    }
    let rows = |db: &crate::Database| {
        db.inspect_writer(|sql| {
            sql.query("SELECT id,title,body FROM notes", [], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })
            .unwrap()
        })
    };
    assert_eq!(rows(ben), rows(carol));
    assert_eq!(rows(ben), rows(ana));
}
