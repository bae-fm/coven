//! The reload's rounds against generated histories (§15; io `CommonReload`,
//! `FixedReload`).

use crate::store_log_sync::snapshots::catalog::{newest_first, Verified};
use crate::store_log_sync::snapshots::common_point::{
    common_point, target, Fresh, FreshAudience, Origin, Outcome, Plan, Reads, Start,
};
use crate::store_log_sync::tests::store;
use crate::SyncError;
use coven_format::sealed_snapshot::SnapshotObjectPrefix;
use coven_format::store_log::SnapshotId;
use coven_format::value::{EntryPositions, StorageTime, WritePositions};
use coven_foundation::id_source::{CircleId, DeviceId, KeyId};
use coven_merge::{Audience, WriteId};
use coven_storage::{ObjectPath, StoredObject};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const DAY: u64 = StorageTime::DAY_SECONDS;

/// A landed snapshot: `(audience, writer, number, stored, caught, positions)`.
type Snap = (u64, u64, u64, u64, u64, Vec<(u64, u64)>);

/// One catalog's landed snapshots and deleted writes, both cumulative.
type Catalog = (Vec<Snap>, BTreeSet<(u64, u64)>);

/// A generated history. Catalog `k` is listed before round `k`'s reads, and
/// round `r` reads after the landings and deletions in catalog `r + 1`; past
/// the last catalog nothing changes.
#[derive(Clone)]
struct History {
    audiences: Vec<u64>,
    required: Vec<(u64, u64)>,
    /// `(writer, number, audiences)`, by writer and number.
    writes: Vec<(u64, u64, Vec<u64>)>,
    catalogs: Vec<Catalog>,
}

fn audience(n: u64) -> Audience {
    match n {
        0 => Audience::Store,
        n => Audience::Circle(CircleId(uuid::Uuid::from_u128(n.into()))),
    }
}

fn number(audience: &Audience) -> u64 {
    match audience {
        Audience::Store => 0,
        Audience::Circle(circle) => circle.0.as_u128() as u64,
    }
}

fn positions(pairs: &[(u64, u64)]) -> WritePositions {
    let mut positions: Vec<_> = pairs
        .iter()
        .filter(|(_, number)| *number > 0)
        .map(|(device, number)| WriteId {
            device: DeviceId(*device),
            number: *number,
        })
        .collect();
    positions.sort();
    WritePositions(positions)
}

/// A landed snapshot as the catalog lists it once its prefix verifies.
fn verified(snap: &Snap) -> Verified {
    let (on, writer, number, stored, caught, pairs) = snap;
    Verified {
        object: StoredObject {
            path: ObjectPath::snapshot(
                store(1),
                audience(*on),
                DeviceId(*writer),
                (*number).try_into().unwrap(),
            ),
            size: 0,
            stored_at: StorageTime::new(*stored, 0).unwrap(),
            time_resolution: Duration::from_secs(1),
            object_id: "id".into(),
            revision: "1".into(),
            checksum: None,
        },
        prefix: SnapshotObjectPrefix {
            audience: audience(*on),
            key: KeyId(uuid::Uuid::from_u128(1)),
            caught_at: StorageTime::new(*caught, 0).unwrap(),
            raise: None,
            writes: positions(pairs),
            store_log: EntryPositions(Vec::new()),
        },
        load_waits: None,
    }
}

impl History {
    fn catalog(&self, k: usize) -> &Catalog {
        self.catalogs
            .get(k)
            .unwrap_or_else(|| self.catalogs.last().unwrap())
    }

    /// Catalog `k` as the reload sees it: every audience readable, each
    /// listing only its counting snapshots, newest first (§15).
    fn fresh(&self, k: usize) -> Fresh {
        let mut readable = BTreeMap::new();
        for on in &self.audiences {
            let mut listed: Vec<_> = self
                .catalog(k)
                .0
                .iter()
                .filter(|snap| snap.0 == *on)
                .map(verified)
                .filter(|snapshot| snapshot.prefix.counts(snapshot.object.stored_at))
                .collect();
            listed.sort_by(|a, b| newest_first((&a.object, &a.prefix), (&b.object, &b.prefix)));
            let fresh = FreshAudience {
                verified: listed,
                unknown: None,
            };
            readable.insert(audience(*on), fresh);
        }
        Fresh {
            readable,
            others: Vec::new(),
        }
    }

    fn starts(&self) -> BTreeMap<Audience, Start<SnapshotId>> {
        let fresh = self.fresh(0);
        let log = coven_database::StoreLog::default();
        self.audiences
            .iter()
            .map(|on| {
                let on = audience(*on);
                let start = match fresh.current(&on, &log) {
                    Some(current) => Start {
                        positions: current.prefix.writes.clone(),
                        selected_at: 0,
                        origin: Origin::Selected(current.object.path.snapshot_id().unwrap()),
                    },
                    None => Start {
                        positions: WritePositions(Vec::new()),
                        selected_at: 0,
                        origin: Origin::Empty,
                    },
                };
                (on, start)
            })
            .collect()
    }

    fn case(&self) -> Value {
        let snap = |(on, writer, number, stored, caught, pairs): &Snap| {
            json!({"audience": on, "writer": writer, "number": number,
                "stored": stored, "caught": caught, "positions": pairs})
        };
        json!({
            "check": "reload",
            "audiences": self.audiences,
            "required": self.required,
            "writes": self.writes.iter().map(|(writer, number, audiences)| json!({
                "writer": writer, "number": number, "audiences": audiences,
            })).collect::<Vec<_>>(),
            "catalogs": self.catalogs.iter().map(|(landed, deleted)| json!({
                "landed": landed.iter().map(snap).collect::<Vec<_>>(),
                "deleted": deleted,
            })).collect::<Vec<_>>(),
        })
    }
}

/// The generated history as storage: each round's statuses and the catalog
/// listed after them. A replacement ends its round.
struct Fake<'a> {
    history: &'a History,
    round: usize,
    replaced: Vec<u64>,
}

impl Reads for Fake<'_> {
    type Body = SnapshotId;

    async fn present(&mut self, id: WriteId) -> Result<bool, SyncError> {
        let key = (id.device.0, id.number);
        let published = self
            .history
            .writes
            .iter()
            .any(|(writer, number, _)| (*writer, *number) == key);
        Ok(published && !self.history.catalog(self.round + 1).1.contains(&key))
    }

    async fn refresh(&mut self) -> Result<Fresh, SyncError> {
        Ok(self.history.fresh(self.round + 1))
    }

    async fn select(&mut self, on: &Audience) -> Result<SnapshotId, SyncError> {
        let fresh = self.history.fresh(self.round + 1);
        let current = fresh
            .current(on, &coven_database::StoreLog::default())
            .expect("a replacement reaches the missing write");
        self.round += 1;
        self.replaced.push(number(on));
        Ok(current.object.path.snapshot_id().unwrap())
    }
}

/// Run the real rounds over `history`: the replaced audiences and the plan.
async fn run(history: &History) -> (Vec<u64>, Outcome<SnapshotId>) {
    let mut fake = Fake {
        history,
        round: 0,
        replaced: Vec::new(),
    };
    let outcome = common_point(
        &mut fake,
        &coven_database::StoreLog::default(),
        history.starts(),
        &positions(&history.required),
        &BTreeSet::new(),
    )
    .await
    .unwrap();
    (fake.replaced, outcome)
}

/// The rounds, selections, target and receipts, in the runner's terms.
fn output(history: &History, replaced: &[u64], outcome: &Outcome<SnapshotId>) -> Value {
    let plan = match outcome {
        Outcome::Complete(plan) => plan,
        Outcome::Unpublished(id) => return json!({"uncertified": [id.device.0, id.number]}),
        Outcome::Unknown(unknown) => panic!("a generated catalog is complete: {unknown:?}"),
    };
    let mut rounds: Vec<Value> = replaced.iter().map(|on| json!(on)).collect();
    rounds.push(Value::Null);
    let selections: Vec<_> = plan
        .starts
        .values()
        .map(|start| match &start.origin {
            Origin::Selected(id) => json!([id.device.0, id.number]),
            _ => Value::Null,
        })
        .collect();
    let target: Vec<_> = target(&positions(&history.required), &plan.starts)
        .into_iter()
        .map(|(device, number)| json!([device.0, number]))
        .collect();
    let received: Vec<_> = plan
        .starts
        .iter()
        .map(|(on, start)| {
            let read: Vec<_> = plan
                .present
                .iter()
                .filter(|id| !start.positions.covers(**id))
                .map(|id| json!([id.device.0, id.number]))
                .collect();
            json!([number(on), read])
        })
        .collect();
    json!({"rounds": rounds, "selections": selections, "target": target, "received": received})
}

fn plan(outcome: Outcome<SnapshotId>) -> Plan<SnapshotId> {
    match outcome {
        Outcome::Complete(plan) => plan,
        _ => panic!("the reload commits"),
    }
}

fn ids(pairs: &[(u64, u64)]) -> BTreeSet<WriteId> {
    pairs
        .iter()
        .map(|(device, number)| WriteId {
            device: DeviceId(*device),
            number: *number,
        })
        .collect()
}

/// io `replacement_fills_other_audience`: the store has no snapshot and
/// Gifts' covers Ana's write 1. Write 1 is deleted once a store snapshot
/// through 2 lands; the store is replaced by it, which raises the common
/// target to 2, so Gifts reads write 2 from the log, and both commit at 2.
#[tokio::test]
async fn a_replacement_fills_the_other_audience_to_its_target() {
    let history = History {
        audiences: vec![0, 1],
        required: vec![(1, 1)],
        writes: vec![(1, 1, vec![0, 1]), (1, 2, vec![0, 1])],
        catalogs: vec![
            (vec![(1, 1, 1, 0, 0, vec![(1, 1)])], BTreeSet::new()),
            (
                vec![(1, 1, 1, 0, 0, vec![(1, 1)]), (0, 1, 2, 1, 1, vec![(1, 2)])],
                BTreeSet::from([(1, 1)]),
            ),
        ],
    };
    let (replaced, outcome) = run(&history).await;
    assert_eq!(replaced, [0]);
    let plan = plan(outcome);
    assert_eq!(
        target(&positions(&history.required), &plan.starts),
        BTreeMap::from([(DeviceId(1), 2)])
    );
    assert_eq!(plan.present, ids(&[(1, 2)]));
    assert!(plan.deleted.is_empty());
}

/// io `skipped_has_no_part`: Ben loads Gifts through Ana's write 8 and finds
/// write 9 deleted. A store snapshot through 9 shows it was published, but
/// Gifts' current snapshot still stops at 8, so write 9 has no Gifts part:
/// Gifts skips it, and the commit passes its number.
#[tokio::test]
async fn a_deleted_write_outside_an_audiences_current_snapshot_is_skipped_there() {
    let history = History {
        audiences: vec![0, 1],
        required: vec![(1, 10)],
        writes: (1..=10).map(|n| (1, n, vec![0, 1])).collect(),
        catalogs: vec![
            (
                vec![
                    (0, 1, 1, 0, 0, vec![(1, 10)]),
                    (1, 1, 2, 0, 0, vec![(1, 8)]),
                ],
                BTreeSet::new(),
            ),
            (
                vec![
                    (0, 1, 1, 0, 0, vec![(1, 10)]),
                    (1, 1, 2, 0, 0, vec![(1, 8)]),
                ],
                BTreeSet::from([(1, 9)]),
            ),
        ],
    };
    let (replaced, outcome) = run(&history).await;
    assert!(replaced.is_empty());
    let plan = plan(outcome);
    assert_eq!(plan.present, ids(&[(1, 10)]));
    assert_eq!(plan.deleted, ids(&[(1, 9)]));
}

/// Only a circle Ben cannot read proves Ana's write 1 was published: its
/// absence is a deletion, and the store, whose current snapshot does not
/// reach it, skips it.
#[tokio::test]
async fn another_audiences_prefix_alone_establishes_a_deleted_number() {
    let history = History {
        audiences: vec![0],
        required: vec![(1, 2)],
        writes: vec![(1, 1, vec![1]), (1, 2, vec![0])],
        catalogs: vec![(Vec::new(), BTreeSet::from([(1, 1)]))],
    };
    let mut fake = Fake {
        history: &history,
        round: 0,
        replaced: Vec::new(),
    };
    let log = coven_database::StoreLog::default();
    let required = positions(&history.required);
    let waiting = BTreeSet::new();
    let unproved = common_point(&mut fake, &log, history.starts(), &required, &waiting);
    assert!(matches!(
        unproved.await.unwrap(),
        Outcome::Unpublished(id) if id == WriteId { device: DeviceId(1), number: 1 }
    ));
    struct Circle<'a>(Fake<'a>);
    impl Reads for Circle<'_> {
        type Body = SnapshotId;
        async fn present(&mut self, id: WriteId) -> Result<bool, SyncError> {
            self.0.present(id).await
        }
        async fn refresh(&mut self) -> Result<Fresh, SyncError> {
            let mut fresh = self.0.refresh().await?;
            fresh.others.push(positions(&[(1, 1)]));
            Ok(fresh)
        }
        async fn select(&mut self, on: &Audience) -> Result<SnapshotId, SyncError> {
            self.0.select(on).await
        }
    }
    let mut circle = Circle(Fake {
        history: &history,
        round: 0,
        replaced: Vec::new(),
    });
    let outcome = common_point(&mut circle, &log, history.starts(), &required, &waiting);
    let plan = plan(outcome.await.unwrap());
    assert_eq!(plan.present, ids(&[(1, 2)]));
    assert_eq!(plan.deleted, ids(&[(1, 1)]));
}

/// New writes and snapshots landing during the reads change neither the
/// selections nor the target (io `retry_stale`): only a deleted number the
/// selection lacks forces a replacement.
#[tokio::test]
async fn new_tails_and_snapshots_do_not_move_the_selection() {
    let history = History {
        audiences: vec![0],
        required: Vec::new(),
        writes: (1..=4).map(|n| (1, n, vec![0])).collect(),
        catalogs: vec![
            (vec![(0, 1, 1, 0, 0, vec![(1, 2)])], BTreeSet::new()),
            (
                vec![(0, 1, 1, 0, 0, vec![(1, 2)]), (0, 1, 2, 0, 0, vec![(1, 4)])],
                BTreeSet::new(),
            ),
        ],
    };
    let (replaced, outcome) = run(&history).await;
    assert!(replaced.is_empty());
    let plan = plan(outcome);
    assert!(matches!(
        &plan.starts[&Audience::Store].origin,
        Origin::Selected(id) if id.number == 1
    ));
    assert!(plan.present.is_empty() && plan.deleted.is_empty());
}

/// Generate a history the IO model's premises hold for. Every write up to
/// each writer's frontier is published. A counting snapshot of an audience
/// covers at least what that audience's current newest does, and a write is
/// deleted only while every audience it has a part in covers it. An audience
/// without a part that does not cover it then never will: its absence there
/// is the deletion the model's selection catalog already knows.
fn history(generator: &mut crate::tests::Generator) -> History {
    let writers = 1 + generator.pick(2) as u64;
    let audiences: Vec<u64> = (0..1 + generator.pick(2) as u64).collect();
    let frontier: BTreeMap<u64, u64> = (1..=writers)
        .map(|writer| (writer, 1 + generator.pick(5) as u64))
        .collect();
    let mut writes = Vec::new();
    for (writer, top) in &frontier {
        for number in 1..=*top {
            let mut parts: Vec<_> = audiences
                .iter()
                .copied()
                .filter(|_| generator.pick(2) == 0)
                .collect();
            if parts.is_empty() {
                parts.push(audiences[generator.pick(audiences.len())]);
            }
            writes.push((*writer, number, parts));
        }
    }
    let required = frontier
        .iter()
        .map(|(writer, top)| (*writer, generator.pick(*top as usize + 1) as u64))
        .filter(|(_, number)| *number > 0)
        .collect();
    // Per audience and writer: the current newest's position, and the most
    // any later snapshot may cover.
    let mut chain: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    let mut cap: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    let mut landed = Vec::new();
    let mut deleted = BTreeSet::new();
    let mut numbers: BTreeMap<u64, u64> = BTreeMap::new();
    let mut catalogs = Vec::new();
    let delete = |generator: &mut crate::tests::Generator,
                  chain: &BTreeMap<(u64, u64), u64>,
                  cap: &mut BTreeMap<(u64, u64), u64>,
                  deleted: &mut BTreeSet<(u64, u64)>| {
        for (writer, number, parts) in &writes {
            if deleted.contains(&(*writer, *number)) || generator.pick(3) != 0 {
                continue;
            }
            let held = |on: &u64| chain.get(&(*on, *writer)).copied().unwrap_or(0);
            if !parts.iter().all(|on| held(on) >= *number) {
                continue;
            }
            deleted.insert((*writer, *number));
            for on in &audiences {
                if held(on) < *number {
                    let bound = cap.entry((*on, *writer)).or_insert(u64::MAX);
                    *bound = (*bound).min(number - 1);
                }
            }
        }
    };
    for k in 0..1 + generator.pick(4) as u64 {
        // Snapshots land, then writes they cover can go, all before the
        // reads that precede catalog `k + 1`'s listing.
        for _ in 0..generator.pick(3) {
            let on = audiences[generator.pick(audiences.len())];
            let author = 1 + generator.pick(writers as usize) as u64;
            let number = numbers.entry(author).or_insert(0);
            *number += 1;
            let mut pairs = Vec::new();
            for (writer, top) in &frontier {
                let low = chain.get(&(on, *writer)).copied().unwrap_or(0);
                let high = (*top).min(cap.get(&(on, *writer)).copied().unwrap_or(u64::MAX));
                pairs.push((
                    *writer,
                    low + generator.pick((high - low + 1) as usize) as u64,
                ));
            }
            let caught = 10 * k;
            let stored = caught + [0, 5, DAY, DAY + 1][generator.pick(4)];
            if stored <= caught + DAY {
                for (writer, position) in &pairs {
                    chain.insert((on, *writer), *position);
                }
            }
            pairs.retain(|(_, position)| *position > 0);
            landed.push((on, author, *number, stored, caught, pairs));
        }
        delete(generator, &chain, &mut cap, &mut deleted);
        catalogs.push((landed.clone(), deleted.clone()));
    }
    History {
        audiences,
        required,
        writes,
        catalogs,
    }
}

/// IO `CommonReload.round`/`replace` over generated histories, each round's
/// audience scans being `FixedReload.scan`: the real rounds' replacements,
/// final selections, common target (`committed_number`, `committed_safe`)
/// and log receipts agree with the model's. Once deletion stops, at most one
/// replacement per audience follows (`terminates`).
#[tokio::test]
#[ignore = "requires COVEN_IO_RUNNER, supplied by scripts/check.sh"]
async fn io_differential_common_point() {
    let mut generator = crate::tests::Generator(0xc0_77e7_9011);
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    let mut replacements = 0;
    let mut skips = 0;
    for _ in 0..400 {
        let history = history(&mut generator);
        let (replaced, outcome) = run(&history).await;
        let stop = (1..history.catalogs.len())
            .filter(|k| history.catalogs[*k].1 != history.catalogs[k - 1].1)
            .max()
            .unwrap_or(0);
        assert!(
            replaced.len().saturating_sub(stop) <= history.audiences.len(),
            "{replaced:?} after deletion stopped at {stop}"
        );
        replacements += replaced.len();
        if let Outcome::Complete(plan) = &outcome {
            skips += plan.deleted.len();
        }
        actual.push(output(&history, &replaced, &outcome));
        cases.push(history.case());
    }
    assert!(replacements > 0, "some histories replace a selection");
    assert!(
        skips > 0,
        "some deleted writes have no part where they are lacked"
    );
    assert_eq!(actual, crate::discovery::tests::io_runner(&cases));
}
