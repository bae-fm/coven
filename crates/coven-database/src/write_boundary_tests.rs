use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{ApplyOutcome, DownloadedPart, DownloadedWrite, EntryId, Replacement};
use coven_format::value::WritePositions;
use coven_foundation::id_source::{DeviceId, SequentialIds};
use coven_merge::{Audience, WriteId};

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
    let boundary = crate::WriteBoundary::SchemaChange {
        audience: circle.clone(),
        version: 2,
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
        .apply_breaking_change(circle, 2, WritePositions(Vec::new()))
        .await
        .unwrap());
    assert!(db
        .apply_breaking_change(Audience::Store, 2, WritePositions(Vec::new()))
        .await
        .unwrap());
    assert_eq!(count(&db, "_coven_applied_boundaries"), 2);
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
        .apply_reset(
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
                    .apply_reset(
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
                    .apply_breaking_change(coven_merge::Audience::Store, 2, WritePositions(vec![]))
                    .await
                    .unwrap());
            }
        }
        db.close().await.unwrap();
        let db = store.schema(notes(), NOTES).await.unwrap();
        // A repeated effect cannot change its coverage or move it after another.
        assert!(!db
            .apply_reset(
                entry,
                Audience::Store,
                WritePositions(vec![writes[1].header.position])
            )
            .await
            .unwrap());
        assert!(!db
            .apply_breaking_change(
                coven_merge::Audience::Store,
                2,
                WritePositions(vec![writes[1].header.position])
            )
            .await
            .unwrap());
        db.apply_downloaded(writes[1].clone().into()).await.unwrap();
        assert_eq!(count(&db, "_coven_applied_boundaries"), 2);
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
    use crate::snapshot_write::tests::{frames, id as snapshot_id, stream};
    use crate::test_utils::tests::*;
    use crate::tests::TestStore;
    use crate::{Database, SnapshotReload, WriteBoundary};
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
        db.apply_store_log(
            crate::ReplayEntry {
                entry,
                check: crate::StoreLogCheck::Allowed,
            },
            replay,
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
        install_reset_entry(&target).await;
        let boundary = WriteBoundary::Reset {
            entry: RESET,
            audience: Audience::Store,
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
