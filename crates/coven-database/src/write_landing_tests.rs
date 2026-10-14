use super::*;
use crate::entry_effects::tests::fate;
use crate::pending::tests::Record;
use crate::store_log::tests::{checked, keys};
use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{DeviceState, EntryOutcome, StoreDevice, StoreMember};
use coven_format::store_log::{MemberRole, StoreLogEntry};
use coven_format::value::EntryPositions;
use coven_foundation::id_source::SequentialIds;

const DAY: u64 = 24 * 60 * 60;

fn at(seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds)
}

/// A kept retiring entry about `writer`, a device of member 2, as replay
/// supplies it: removal of the device or its member, or its replacement.
async fn retire(
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
    let owner = keys(2).signing;
    let mut replay = log.replay;
    replay
        .state
        .members
        .entry(owner.clone())
        .or_insert(StoreMember {
            sealing: keys(2).sealing,
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "fixture-access-key".into(),
                principal: coven_format::S3Principal::Console,
            },
            role: MemberRole::Member,
            removed: false,
        });
    replay.state.devices.entry(writer).or_insert(StoreDevice {
        member: owner.clone(),
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
                    member: keys(2).signing,
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
        author: keys(1).signing,
        had_read: EntryPositions(Vec::new()),
        change,
    };
    replay.entries.insert(id, EntryOutcome::Kept);
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

fn dropped_reports(db: &crate::Database) -> Vec<Record> {
    db.inspect_writer(|sql| crate::pending::read::<Record>(sql).unwrap())
        .into_iter()
        .filter(|record| record.dropped_write().is_some())
        .collect()
}

/// Device removal, member removal and replacement each give the write thirty
/// storage days: exactly thirty counts, one second more is consumed without
/// effects or losses and reported as landed too late (§10, D5).
#[tokio::test]
async fn every_retirement_admits_exactly_thirty_days_and_consumes_later_writes() {
    for kind in ["device", "member", "replacement"] {
        for (offset, late) in [(0, false), (1, true)] {
            let p = pair().await;
            sql(&p.source, "INSERT INTO notes VALUES('n','title','body')")
                .await
                .unwrap();
            let write = records(&p.source).remove(0);
            let position = write.header.position;
            retire(&p.target, position.device, 1, kind, 0).await;
            p.target
                .apply_downloaded::<Record>(write.into(), at(30 * DAY + offset))
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
        .apply_downloaded::<Record>(write.into(), at(35 * DAY))
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

/// A retirement that arrives after the write it excludes finds no retained
/// input: the audience keeps its rows, refuses writes and waits for a reload.
#[tokio::test]
async fn a_retirement_arriving_after_its_write_requires_a_reload() {
    let p = pair().await;
    sql(&p.source, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    let write = records(&p.source).remove(0);
    let device = write.header.position.device;
    p.target
        .apply_downloaded::<Record>(write.into(), at(31 * DAY))
        .await
        .unwrap();
    assert_eq!(count(&p.target, "notes"), 1);
    retire(&p.target, device, 1, "device", 0).await;
    assert_eq!(count(&p.target, "notes"), 1);
    assert!(sql(&p.target, "INSERT INTO notes VALUES('m','t','b')")
        .await
        .is_err());
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
        .apply_downloaded::<Record>(write.into(), at(31 * DAY))
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
        .advance_store_log_horizon(p.target.store_log().await.unwrap(), Duration::from_secs(1))
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
        .apply_downloaded::<Record>(write.into(), at(31 * DAY))
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
            db.advance_store_log_horizon(
                db.store_log().await.unwrap(),
                Duration::from_secs(seconds),
            )
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
/// against the Rust window: every kept retirement, reads, exact boundaries,
/// and later retirement sets agreeing on a final prefix through an anchor.
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
                let reads = |e: EntryId| read.contains(&e.number);
                let verdict = within_window(at(stored), reads, &kept);
                let json_of = |set: &[Retirement]| {
                    set.iter()
                        .map(|r| {
                            json!({"device": 7, "entry": r.entry.number,
                                   "stored": r.stored_at.duration_since(UNIX_EPOCH).unwrap().as_secs()})
                        })
                        .collect::<Vec<_>>()
                };
                let mut case = json!({"check": "window", "device": 7, "stored": stored,
                                      "read": read, "kept": json_of(&kept)});
                // A later set: the final prefix through the earliest kept
                // retirement is fixed; later retirements appear or vanish. The
                // send gate rules out reads of any of them.
                if let Some(anchor) = kept.iter().min_by_key(|r| r.stored_at) {
                    if read.is_empty() {
                        let mut later: Vec<_> = kept
                            .iter()
                            .filter(|r| r.stored_at <= anchor.stored_at)
                            .copied()
                            .collect();
                        later.push(Retirement {
                            entry: entry(9),
                            stored_at: anchor.stored_at + Duration::from_secs(DAY),
                        });
                        let again = within_window(at(stored), reads, &later);
                        assert_eq!(again, verdict, "retirement verdict changed after finality");
                        case["later"] = json!(json_of(&later));
                        actual.push(json!([verdict, again]));
                        cases.push(case);
                        continue;
                    }
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
            .apply_downloaded::<Record>(creation.clone().into(), at(30 * DAY + 1))
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
