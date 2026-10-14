use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{ApplyOutcome, DownloadedPart, DownloadedWrite, EntryId, Replacement};
use coven_format::value::WritePositions;
use coven_foundation::id_source::{DeviceId, SequentialIds};
use coven_merge::{Audience, WriteId};

// These fixtures supply checked replay input; coverage still passes through the
// production entry-keyed persistence and transaction functions.
impl crate::Database {
    pub(crate) async fn check_boundary_fixture(
        &self,
        boundary: crate::Boundary,
    ) -> Result<EntryId, crate::DbError> {
        self.check_boundary_fixture_at(boundary, std::time::UNIX_EPOCH)
            .await
    }
    pub(crate) async fn check_boundary_fixture_at(
        &self,
        boundary: crate::Boundary,
        stored_at: std::time::SystemTime,
    ) -> Result<EntryId, crate::DbError> {
        use coven_format::{
            store_log::{StoreChange, StoreLogEntry},
            value::EntryPositions,
        };
        let log = self.store_log().await.unwrap();
        let id = boundary.entry();
        let stored_at = log
            .entries
            .iter()
            .find(|e| e.entry.position == id)
            .map_or(stored_at, |e| e.stored_at);
        let entry = if let Some(existing) = log.entries.iter().find(|e| e.entry.position == id) {
            existing.entry.clone()
        } else {
            let time = log.entries.iter().map(|e| e.entry.timestamp).max();
            let timestamp = match time {
                Some(time) => {
                    coven_merge::Timestamp::new(time.milliseconds() + 1, 0, id.device).unwrap()
                }
                None => coven_merge::Timestamp::new(1, 0, id.device).unwrap(),
            };
            StoreLogEntry {
                position: id,
                timestamp,
                author: crate::store_log::tests::keys(1).signing,
                had_read: EntryPositions(Vec::new()),
                change: match &boundary {
                    crate::Boundary::SchemaChange(v) => StoreChange::RaiseSchema {
                        version: v.number,
                        snapshot: v.snapshot.clone(),
                    },
                    crate::Boundary::Reset { snapshot, .. } => StoreChange::Reset {
                        snapshot: snapshot.clone(),
                    },
                },
            }
        };
        let mut replay = log.replay;
        replay.entries.insert(id, crate::EntryOutcome::Kept);
        if !boundary.already_in_place(&replay.state) {
            boundary.apply(&mut replay.state);
        }
        let mut checked = crate::store_log::tests::checked(entry);
        checked.stored_at = stored_at;
        self.apply_store_log::<crate::pending::tests::Record>(checked, replay, Vec::new())
            .await?;
        Ok(id)
    }
    pub(crate) async fn schema_fixture(
        &self,
        audience: Audience,
        version: u32,
    ) -> Result<crate::Boundary, crate::DbError> {
        let log = self.store_log().await.unwrap();
        if let Some(boundary) = log.boundaries().find(|b| matches!(b, crate::Boundary::SchemaChange(v) if v.number == version && v.snapshot.audience == audience)) { return Ok(boundary); }
        let device = DeviceId(98);
        let number = log
            .entries
            .iter()
            .filter(|e| e.entry.position.device == device)
            .map(|e| e.entry.position.number)
            .max()
            .unwrap_or(0)
            + 1;
        Ok(crate::Boundary::SchemaChange(crate::StoreVersion {
            number: version,
            snapshot: coven_format::store_log::SnapshotId {
                audience,
                device,
                number,
            },
            entry: EntryId { device, number },
        }))
    }
    pub(crate) async fn install_schema_fixture(
        &self,
        audience: Audience,
        version: u32,
        included: WritePositions,
    ) -> Result<bool, crate::DbError> {
        let boundary = self.schema_fixture(audience.clone(), version).await?;
        let entry = self.check_boundary_fixture(boundary).await?;
        self.inspect_writer(|sql| {
            sql.transaction(|sql| {
                let changed = crate::BoundaryCoverage { entry, included }.record_inside(sql)?;
                crate::entry_effects::ready(sql, &audience)?;
                Ok(changed)
            })
        })
    }
    pub(crate) async fn install_reset_fixture(
        &self,
        entry: EntryId,
        audience: Audience,
        included: WritePositions,
    ) -> Result<bool, crate::DbError> {
        let boundary = crate::Boundary::Reset {
            entry,
            snapshot: coven_format::store_log::SnapshotId {
                audience: audience.clone(),
                device: entry.device,
                number: entry.number,
            },
        };
        self.check_boundary_fixture(boundary).await?;
        self.inspect_writer(|sql| {
            sql.transaction(|sql| {
                let changed = crate::BoundaryCoverage { entry, included }.record_inside(sql)?;
                crate::entry_effects::ready(sql, &audience)?;
                Ok(changed)
            })
        })
    }
}

#[tokio::test]
async fn checked_boundaries_survive_selection_changes_and_restart() {
    use crate::pending::tests::Record;
    use crate::store_log::tests::{checked, keys};
    use coven_format::{
        store_log::{SnapshotId, StoreChange, StoreLogEntry},
        value::EntryPositions,
    };
    use coven_merge::Timestamp;
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    let mut replay = crate::StoreLogReplay::default();
    let mut entries = Vec::new();
    for (number, version) in [(1, 2), (2, 3), (3, 2)] {
        let position = EntryId {
            device: DeviceId(99),
            number,
        };
        let snapshot = SnapshotId {
            audience: Audience::Store,
            device: DeviceId(99),
            number,
        };
        let entry = StoreLogEntry {
            position,
            timestamp: Timestamp::new(number, 0, position.device).unwrap(),
            author: keys(1).signing,
            had_read: EntryPositions(Vec::new()),
            change: StoreChange::RaiseSchema {
                version,
                snapshot: snapshot.clone(),
            },
        };
        replay.entries.insert(position, crate::EntryOutcome::Kept);
        if number != 3 {
            replay.state.schema.insert(
                Audience::Store,
                crate::StoreVersion {
                    number: version,
                    snapshot,
                    entry: position,
                },
            );
        }
        db.apply_store_log::<Record>(checked(entry.clone()), replay.clone(), Vec::new())
            .await
            .unwrap();
        entries.push(entry);
    }
    assert_eq!(count(&db, "_coven_boundaries"), 3);
    replay.entries.insert(
        entries[1].position,
        crate::EntryOutcome::Dropped(crate::DropReason::NotAllowed),
    );
    replay.state.schema.insert(
        Audience::Store,
        crate::StoreVersion {
            number: 2,
            snapshot: SnapshotId {
                audience: Audience::Store,
                device: DeviceId(99),
                number: 3,
            },
            entry: entries[2].position,
        },
    );
    db.apply_store_log::<Record>(checked(entries[2].clone()), replay.clone(), Vec::new())
        .await
        .unwrap();
    assert_eq!(count(&db, "_coven_boundaries"), 3);
    db.close().await.unwrap();
    let db = store.schema(notes(), NOTES).await.unwrap();
    assert_eq!(db.store_log().await.unwrap().replay, replay);
    assert_eq!(count(&db, "_coven_boundaries"), 3);
    db.close().await.unwrap();
}

#[tokio::test]
async fn reset_coverage_survives_noops_reversal_and_failed_replacement() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    let id = |number| EntryId {
        device: DeviceId(99),
        number,
    };
    let snapshot = coven_format::store_log::SnapshotId {
        audience: Audience::Store,
        device: DeviceId(99),
        number: 1,
    };
    for number in [1, 2] {
        db.check_boundary_fixture(crate::Boundary::Reset {
            entry: id(number),
            snapshot: snapshot.clone(),
        })
        .await
        .unwrap();
    }
    let coverage = |number| crate::BoundaryCoverage {
        entry: id(number),
        included: WritePositions(vec![]),
    };
    let reload = |boundaries| {
        let mut reload =
            crate::SnapshotReload::<std::io::Empty, std::io::Empty>::new(vec![], vec![]);
        reload.snapshots = vec![crate::SnapshotSource::Empty(Audience::Store)];
        reload.boundaries = Some(boundaries);
        reload
    };
    assert_eq!(
        db.store_log()
            .await
            .unwrap()
            .boundaries()
            .map(|b| b.entry())
            .collect::<Vec<_>>(),
        [id(1)]
    );
    assert!(
        db.load_snapshots(reload(vec![coverage(2)])).await.is_err(),
        "kept no-op has no effective boundary"
    );
    assert!(
        db.load_snapshots(reload(vec![coverage(3)])).await.is_err(),
        "coverage cannot introduce an unchecked entry"
    );
    db.load_snapshots(reload(vec![coverage(1)])).await.unwrap();
    let retained = || {
        db.inspect_writer(|sql| {
            sql.query(
                "SELECT entry_number,included,applied FROM _coven_boundaries ORDER BY entry_number",
                [],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Option<Vec<u8>>>(1)?,
                        row.get::<_, bool>(2)?,
                    ))
                },
            )
            .unwrap()
        })
    };
    let before = retained();
    let mut changed = coverage(1);
    changed.included = WritePositions(vec![WriteId {
        device: DeviceId(55),
        number: 1,
    }]);
    assert!(db.load_snapshots(reload(vec![changed])).await.is_err());
    assert_eq!(
        retained(),
        before,
        "failed coverage must roll back application receipts"
    );
    let log = db.store_log().await.unwrap();
    let mut replay = log.replay.clone();
    replay.entries.insert(
        id(1),
        crate::EntryOutcome::Dropped(crate::DropReason::NotAllowed),
    );
    db.apply_store_log::<crate::pending::tests::Record>(log.entries[1].clone(), replay, Vec::new())
        .await
        .unwrap();
    db.load_snapshots(reload(vec![coverage(2)])).await.unwrap();
    let after = retained();
    assert_eq!(
        after[0].1, before[0].1,
        "dropped entry keeps its checked coverage"
    );
    assert!(!after[0].2);
    assert!(after[1].2);
    db.apply_store_log::<crate::pending::tests::Record>(
        log.entries[1].clone(),
        log.replay,
        Vec::new(),
    )
    .await
    .unwrap();
    db.load_snapshots(reload(vec![coverage(1)])).await.unwrap();
    assert_eq!(retained()[0], before[0]);
    db.close().await.unwrap();
    let db = store.schema(notes(), NOTES).await.unwrap();
    assert_eq!(
        db.store_log()
            .await
            .unwrap()
            .boundaries()
            .map(|b| b.entry())
            .collect::<Vec<_>>(),
        [id(1)]
    );
    let boundaries = db.inspect_writer(super::WriteBoundary::load).unwrap();
    assert_eq!(boundaries.len(), 1);
    assert_eq!(boundaries[0].boundary.entry(), id(1));
    assert_eq!(count(&db, "_coven_boundaries"), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn schema_boundaries_are_independent_for_each_audience() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    let record = records(&db).remove(0);
    let circle = Audience::Circle(coven_foundation::id_source::CircleId(
        uuid::Uuid::from_u128(1),
    ));
    let boundary = super::WriteBoundary {
        boundary: crate::Boundary::SchemaChange(crate::StoreVersion {
            number: 2,
            snapshot: coven_format::store_log::SnapshotId {
                audience: circle.clone(),
                device: DeviceId(99),
                number: 1,
            },
            entry: EntryId {
                device: DeviceId(99),
                number: 1,
            },
        }),
        included: WritePositions(Vec::new()),
    };
    assert!(boundary
        .excludes(&record.header, &record.parts[0])
        .is_none());
    let mut part = record.parts[0].clone();
    part.audience = circle.clone();
    assert!(matches!(
        boundary.excludes(&record.header, &part),
        Some(coven_format::snapshot_rows::LostWriteCause::SchemaChange(2))
    ));
    assert!(db
        .install_schema_fixture(circle, 2, WritePositions(Vec::new()))
        .await
        .unwrap());
    assert!(db
        .install_schema_fixture(Audience::Store, 2, WritePositions(Vec::new()))
        .await
        .unwrap());
    assert_eq!(count(&db, "_coven_boundaries"), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn reset_successors_can_read_post_reset_writes_from_another_device() {
    let ids = SequentialIds::new();
    let stores = [
        TestStore::with_ids(&ids),
        TestStore::with_ids(&ids),
        TestStore::with_ids(&ids),
    ];
    let a = stores[0].schema(notes(), NOTES).await.unwrap();
    let b = stores[1].schema(notes(), NOTES).await.unwrap();
    let receiver = stores[2].schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','snapshot','')")
        .await
        .unwrap();
    let initial = records(&a).remove(0);
    for db in [&b, &receiver] {
        db.apply_downloaded(initial.clone().into()).await.unwrap();
    }
    receiver
        .install_reset_fixture(
            EntryId {
                device: DeviceId(99),
                number: 1,
            },
            Audience::Store,
            WritePositions(vec![initial.header.position]),
        )
        .await
        .unwrap();
    sql(&a, "UPDATE notes SET title='post reset'")
        .await
        .unwrap();
    let first = records(&a).remove(1);
    b.apply_downloaded(first.clone().into()).await.unwrap();
    sql(&b, "UPDATE notes SET title='read post reset'")
        .await
        .unwrap();
    let second = records(&b).remove(0);
    assert!(second.header.had_read.covers(first.header.position));
    for write in [first, second] {
        assert_eq!(
            receiver.apply_downloaded(write.into()).await.unwrap(),
            ApplyOutcome::Applied
        );
    }
    assert!(receiver.lost_values().await.unwrap().is_empty());
    assert_eq!(
        receiver
            .read(
                |sql| Ok(sql.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?)
            )
            .await
            .unwrap(),
        "read post reset"
    );
    for db in [a, b, receiver] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn repeated_boundaries_keep_their_original_coverage_and_application_order() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let source = source_store.schema(notes(), NOTES).await.unwrap();
    sql(&source, "INSERT INTO notes VALUES('n','old','')")
        .await
        .unwrap();
    sql(&source, "UPDATE notes SET title='excluded'")
        .await
        .unwrap();
    let writes = records(&source);
    let entry = EntryId {
        device: DeviceId(99),
        number: 1,
    };
    for reset_first in [false, true] {
        let store = TestStore::with_ids(&ids);
        let db = store.schema(notes(), NOTES).await.unwrap();
        db.apply_downloaded(DownloadedWrite {
            header: writes[0].header.clone(),
            parts: vec![DownloadedPart::Skipped(Audience::Store)],
        })
        .await
        .unwrap();
        for reset in [reset_first, !reset_first] {
            if reset {
                assert!(db
                    .install_reset_fixture(
                        entry,
                        Audience::Store,
                        WritePositions(vec![WriteId {
                            device: DeviceId(99),
                            number: 1
                        }])
                    )
                    .await
                    .unwrap());
            } else {
                assert!(db
                    .install_schema_fixture(coven_merge::Audience::Store, 2, WritePositions(vec![]))
                    .await
                    .unwrap());
            }
        }
        db.close().await.unwrap();
        let db = store.schema(notes(), NOTES).await.unwrap();
        // A repeated effect cannot change its coverage or move it after another.
        assert!(db
            .install_reset_fixture(
                entry,
                Audience::Store,
                WritePositions(vec![writes[1].header.position])
            )
            .await
            .is_err());
        assert!(db
            .install_schema_fixture(
                coven_merge::Audience::Store,
                2,
                WritePositions(vec![writes[1].header.position])
            )
            .await
            .is_err());
        db.apply_downloaded(writes[1].clone().into()).await.unwrap();
        assert_eq!(count(&db, "_coven_boundaries"), 2);
        assert_eq!(count(&db, "notes"), 0);
        let losses = db.lost_values().await.unwrap();
        assert_eq!(losses.len(), 1);
        assert_eq!(
            losses[0].replaced_by,
            if reset_first {
                Replacement::Reset(entry)
            } else {
                Replacement::SchemaChange { version: 2 }
            }
        );
        db.close().await.unwrap();
    }
    source.close().await.unwrap();
}

mod differential {
    use crate::pending::tests::Record as PendingRecord;
    use crate::snapshot_write::tests::{frames, id as snapshot_id, stream};
    use crate::test_utils::tests::*;
    use crate::tests::TestStore;
    use crate::{BoundaryCoverage, Database, SnapshotReload};
    use coven_format::value::EntryPositions;
    use coven_foundation::id_source::SequentialIds;
    use coven_merge::{Audience, Operation, Timestamp};
    use serde_json::{json, Value};
    use std::collections::BTreeSet;
    use std::ffi::OsStr;

    async fn install_reset_entry(db: &Database) {
        // Replay selection is an input to both models. The real store-log
        // transaction supplies the incoming writes' store_log_read prerequisites.
        let entry = coven_format::store_log::StoreLogEntry {
            position: RESET,
            timestamp: Timestamp::new(100, 0, RESET.device).unwrap(),
            author: crate::store_log::tests::keys(1).signing,
            had_read: EntryPositions(Vec::new()),
            change: coven_format::store_log::StoreChange::Reset {
                snapshot: snapshot_id(Audience::Store),
            },
        };
        let mut replay = crate::StoreLogReplay::default();
        replay.entries.insert(RESET, crate::EntryOutcome::Kept);
        replay
            .state
            .resets
            .insert(Audience::Store, snapshot_id(Audience::Store));
        db.apply_store_log::<PendingRecord>(
            crate::ReplayEntry {
                entry,
                check: crate::StoreLogCheck::Allowed,
                stored_at: std::time::UNIX_EPOCH,
            },
            replay,
            Vec::new(),
        )
        .await
        .unwrap();
    }
    async fn reset_result(history: &ReloadHistory) -> Value {
        assert!(history.valid(), "invalid differential history");
        let ids = SequentialIds::new();
        let source_store = TestStore::with_ids(&ids);
        let target_store = TestStore::with_ids(&ids);
        let source = source_store.schema(tables(), SCHEMA).await.unwrap();
        let target = target_store.schema(tables(), SCHEMA).await.unwrap();
        for write in history
            .writes
            .iter()
            .filter(|w| history.covered.contains(&w.header.position))
        {
            assert_eq!(
                source.apply_downloaded(write.clone().into()).await.unwrap(),
                crate::ApplyOutcome::Applied
            );
        }
        let snapshot = frames(&source, Audience::Store).await;
        if history.reset {
            install_reset_entry(&target).await;
        }
        let boundary = BoundaryCoverage {
            entry: RESET,
            included: snapshot.prefix.writes.clone(),
        };
        let mut reload = SnapshotReload::new(
            vec![snapshot.input()],
            history.writes.iter().map(stream).collect(),
        );
        reload.boundaries = Some(if history.reset {
            vec![boundary]
        } else {
            Vec::new()
        });
        target.load_snapshots(reload).await.unwrap();
        let result = result_json(&target, &history.rows());
        let positions = target.sync_state(Vec::new()).await.unwrap().positions;
        assert!(
            history
                .writes
                .iter()
                .all(|w| positions.covers(w.header.position)),
            "ignored parts must still pass their log positions"
        );
        source.close().await.unwrap();
        target.close().await.unwrap();
        result
    }
    async fn reset_mismatch(runner: &OsStr, history: &ReloadHistory) -> bool {
        reset_result(history).await != run_lean(runner, &history.input())
    }
    async fn reduce_reset(runner: &OsStr, mut history: ReloadHistory) -> ReloadHistory {
        loop {
            let candidates = history.reductions();
            let mut smaller = None;
            for candidate in candidates {
                if candidate.valid() && reset_mismatch(runner, &candidate).await {
                    smaller = Some(candidate);
                    break;
                }
            }
            match smaller {
                Some(candidate) => history = candidate,
                None => return history,
            }
        }
    }
    #[tokio::test]
    #[ignore = "requires COVEN_MERGE_LEAN; stops at the first reset mismatch"]
    async fn reset_differential_histories() {
        let runner = std::env::var_os("COVEN_MERGE_LEAN").expect("built mergeRunner");
        for seed in 0..128 {
            let mut rng = Generator::new(seed + 30_000);
            let history = ReloadHistory::generate(&mut rng);
            for prefix in [history.writes.len(), 4 + rng.pick(20)] {
                let mut case = history.clone();
                case.writes.truncate(prefix);
                let present: BTreeSet<_> = case.writes.iter().map(|w| w.header.position).collect();
                case.covered.retain(|id| present.contains(id));
                for _ in 0..3 {
                    case.writes = rng.order(&case.writes);
                    if reset_mismatch(&runner, &case).await {
                        let reduced = reduce_reset(&runner, case).await;
                        panic!(
                            "reset differential, seed {seed}\nreduced history: {}\nRust: {}\nLean: {}",
                            reduced.input(),
                            reset_result(&reduced).await,
                            run_lean(&runner, &reduced.input())
                        );
                    }
                }
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires COVEN_MERGE_LEAN; scripts/check.sh checks the reset discrepancy"]
    async fn reset_differential_counterexample() {
        let runner = std::env::var_os("COVEN_MERGE_LEAN").expect("built mergeRunner");
        let mut write = insert(0, 0);
        let Operation::Insert(columns) = &mut write.parts[0].rows[0].change.operation else {
            unreachable!()
        };
        columns.retain(|c, _| c == "id");
        let history = ReloadHistory {
            writes: vec![write],
            covered: BTreeSet::new(),
            reset: true,
        };
        let rust = json!({"rows":[{"id":0,"gen":1,"gw":[[1,100]],"cells":[[0,100]],"lost":[]}],"frozen":[]});
        let lean = json!({"rows":[{"id":0,"gen":0,"gw":[],"cells":[],"lost":[]}],"frozen":[]});
        assert_eq!(reset_result(&history).await, rust);
        assert_eq!(run_lean(&runner, &history.input()), lean);
        assert_ne!(rust, lean);
        // No write, no reset, reading the reset, or inclusion in the snapshot each
        // removes this mismatch. The key is required by the real write format.
        let mut empty = history.clone();
        empty.writes.clear();
        assert!(!reset_mismatch(&runner, &empty).await);
        let mut no_reset = history.clone();
        no_reset.reset = false;
        assert!(!reset_mismatch(&runner, &no_reset).await);
        let mut read = history.clone();
        read.writes[0].header.store_log_read = EntryPositions(vec![RESET]);
        assert!(!reset_mismatch(&runner, &read).await);
        let mut covered = history;
        covered.covered.insert(covered.writes[0].header.position);
        assert!(!reset_mismatch(&runner, &covered).await);
    }
}
