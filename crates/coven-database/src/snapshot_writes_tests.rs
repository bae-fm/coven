use crate::snapshot_write::tests::{frames, id, load_one, stream};
use crate::tests::contents;
use crate::tests::TestStore;
use crate::write::tests::{notes, records, sql, NOTES};
use coven_foundation::id_source::SequentialIds;
use coven_merge::Audience;
use std::io::Cursor;

#[tokio::test]
async fn waiting_changes_and_their_missing_history_commit_in_one_reload() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let c_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    let c = c_store.schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','first','')")
        .await
        .unwrap();
    let first = records(&a).remove(0);
    for db in [&b, &c] {
        db.apply_downloaded(first.clone().into()).await.unwrap();
    }
    let snapshot = frames(&a, Audience::Store).await;
    let snapshot_prefix = snapshot.prefix.clone();
    let snapshot = snapshot.concat();
    sql(&a, "UPDATE notes SET title='second'").await.unwrap();
    let missing = records(&a).pop().unwrap();
    for db in [&b, &c] {
        db.apply_downloaded(missing.clone().into()).await.unwrap();
    }
    sql(&b, "UPDATE notes SET title='waiting'").await.unwrap();
    sql(&b, "UPDATE notes SET body='next waiting write'")
        .await
        .unwrap();
    let waiting = records(&b);
    for write in &waiting {
        c.apply_downloaded(write.clone().into()).await.unwrap();
    }
    let original = contents(&b);
    let mut query = b.subscribe(|db| {
        Ok(db.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?)
    });
    assert_eq!(query.next().await.unwrap(), "waiting");
    assert!(matches!(
        load_one(
            &b,
            id(Audience::Store),
            snapshot_prefix.clone(),
            Cursor::new(snapshot.clone())
        )
        .await,
        Err(crate::DbError::Snapshot(
            crate::SnapshotError::MissingWrites { .. }
        ))
    ));
    assert_eq!(contents(&b), original);
    assert!(!query.is_marked_for_rerun());
    b.inspect_writer(|db| db.batch("CREATE TEMP TRIGGER refuse_waiting AFTER UPDATE ON notes WHEN new.title='waiting' BEGIN SELECT RAISE(ABORT,'replay failed'); END").unwrap());
    assert!(b
        .load_snapshots(crate::SnapshotReload::new(
            vec![(
                id(Audience::Store),
                snapshot_prefix.clone(),
                Cursor::new(snapshot.clone())
            )],
            vec![stream(&missing)]
        ))
        .await
        .is_err());
    assert_eq!(contents(&b), original);
    assert!(!query.is_marked_for_rerun());
    b.inspect_writer(|db| db.batch("DROP TRIGGER refuse_waiting").unwrap());
    drop(query);
    for duplicate in [false, true] {
        let mut supplied = vec![stream(&missing)];
        if duplicate {
            supplied.push(stream(&waiting[0]));
        }
        b.load_snapshots(crate::SnapshotReload::new(
            vec![(
                id(Audience::Store),
                snapshot_prefix.clone(),
                Cursor::new(snapshot.clone()),
            )],
            supplied,
        ))
        .await
        .unwrap();
        assert_eq!(records(&b), waiting);
        assert_eq!(
            frames(&b, Audience::Store).await,
            frames(&c, Audience::Store).await
        );
    }
    for write in &waiting {
        upload(&b, write).await;
    }
    b.close().await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    assert_eq!(
        frames(&b, Audience::Store).await,
        frames(&c, Audience::Store).await
    );
    sql(&b, "UPDATE notes SET title='after reload'")
        .await
        .unwrap();
    let next = records(&b).pop().unwrap();
    assert_eq!(
        next.header.position.number,
        waiting.last().unwrap().header.position.number + 1
    );
    c.apply_downloaded(next.into()).await.unwrap();
    assert_eq!(
        frames(&b, Audience::Store).await,
        frames(&c, Audience::Store).await
    );
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

async fn upload(db: &crate::Database, record: &coven_format::write::WriteRecord) {
    crate::upload::tests::attempt(db, 17).await.unwrap();
    assert!(db.upload_succeeded(record.header.position).await.unwrap());
}

#[tokio::test]
async fn waiting_files_survive_intermediate_snapshot_and_gap_write_states() {
    use crate::file_write::tests::{attach, owned_paths, tables, SCHEMA};
    let store = TestStore::new();
    let db = store
        .schema(tables(crate::Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    attach(&db, b"original".to_vec(), true).await.unwrap();
    let snapshot = frames(&db, Audience::Store).await;
    let snapshot_prefix = snapshot.prefix.clone();
    let snapshot = snapshot.concat();
    sql(&db, "UPDATE files SET title='missing'").await.unwrap();
    let writes = records(&db);
    for write in &writes {
        upload(&db, write).await;
    }
    attach(&db, b"waiting bytes".to_vec(), false).await.unwrap();
    let waiting = records(&db).pop().unwrap();
    let paths = owned_paths(&store);
    let expected = frames(&db, Audience::Store).await;
    let before = contents(&db);
    assert!(load_one(
        &db,
        id(Audience::Store),
        snapshot_prefix.clone(),
        Cursor::new(snapshot.clone())
    )
    .await
    .is_err());
    assert_eq!(contents(&db), before);
    assert_eq!(owned_paths(&store), paths);
    db.load_snapshots(crate::SnapshotReload::new(
        vec![(
            id(Audience::Store),
            snapshot_prefix.clone(),
            Cursor::new(snapshot),
        )],
        vec![stream(&writes[1])],
    ))
    .await
    .unwrap();
    assert_eq!(frames(&db, Audience::Store).await, expected);
    assert_eq!(owned_paths(&store), paths);
    assert_eq!(std::fs::read(&paths[0]).unwrap(), b"waiting bytes");
    upload(&db, &waiting).await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn supplied_write_streams_are_bounded_and_fail_atomically() {
    use crate::{DownloadedPartStream, DownloadedWriteStream};
    use std::io::{Read, Seek, Write};
    struct BoundedFile(std::fs::File);
    impl Read for BoundedFile {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            assert!(buffer.len() <= coven_format::chunks::CHUNK_SIZE);
            let length = buffer.len().min(701);
            self.0.read(&mut buffer[..length])
        }
    }
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','before','')")
        .await
        .unwrap();
    let snapshot = frames(&a, Audience::Store).await;
    let snapshot_prefix = snapshot.prefix.clone();
    let snapshot = snapshot.concat();
    a.write(|db| {
        db.execute("UPDATE notes SET body=?1", ["x".repeat(200_000)])?;
        Ok(())
    })
    .await
    .unwrap();
    let missing = records(&a).pop().unwrap();
    b.apply_downloaded(records(&a)[0].clone().into())
        .await
        .unwrap();
    b.apply_downloaded(missing.clone().into()).await.unwrap();
    sql(&b, "UPDATE notes SET title='waiting'").await.unwrap();
    let before = contents(&b);
    let input = stream(&missing);
    let mut bytes = match input.parts.into_iter().next().unwrap() {
        DownloadedPartStream::Opened(bytes) => bytes.into_inner(),
        DownloadedPartStream::Skipped => panic!("opened test write"),
    };
    let original = bytes.clone();
    for damage in 0..3 {
        bytes = original.clone();
        match damage {
            0 => {
                bytes.truncate(bytes.len() - 1);
            }
            1 => {
                bytes.push(0);
            }
            _ => {
                bytes[0] ^= 0xff;
            }
        }
        assert!(b
            .load_snapshots(crate::SnapshotReload::new(
                vec![(
                    id(Audience::Store),
                    snapshot_prefix.clone(),
                    Cursor::new(snapshot.clone())
                )],
                vec![DownloadedWriteStream {
                    header: input.header.clone(),
                    parts: vec![DownloadedPartStream::Opened(Cursor::new(bytes.clone()))],
                }]
            ))
            .await
            .is_err());
        assert_eq!(contents(&b), before);
    }
    assert!(b
        .load_snapshots(crate::SnapshotReload::new(
            vec![(
                id(Audience::Store),
                snapshot_prefix.clone(),
                Cursor::new(snapshot.clone())
            )],
            vec![DownloadedWriteStream::<Cursor<Vec<u8>>> {
                header: input.header.clone(),
                parts: vec![DownloadedPartStream::Skipped],
            }]
        ))
        .await
        .is_err());
    assert_eq!(contents(&b), before);
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(&original).unwrap();
    file.rewind().unwrap();
    b.load_snapshots(crate::SnapshotReload::new(
        vec![(
            id(Audience::Store),
            snapshot_prefix.clone(),
            Cursor::new(snapshot),
        )],
        vec![DownloadedWriteStream {
            header: input.header,
            parts: vec![DownloadedPartStream::Opened(BoundedFile(file))],
        }],
    ))
    .await
    .unwrap();
    a.apply_downloaded(records(&b)[0].clone().into())
        .await
        .unwrap();
    assert_eq!(
        frames(&b, Audience::Store).await,
        frames(&a, Audience::Store).await
    );
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn snapshots_with_the_same_key_in_different_audiences_recompute_together() {
    use crate::{RowIdentity, SyncedTable};
    use coven_foundation::id_source::CircleId;
    let tables = || {
        vec![SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience")]
    };
    const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL, title TEXT NOT NULL)";
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let c_store = TestStore::with_ids(&ids);
    let a = a_store.schema(tables(), SCHEMA).await.unwrap();
    let b = b_store.schema(tables(), SCHEMA).await.unwrap();
    let c = c_store.schema(tables(), SCHEMA).await.unwrap();
    let circle = Audience::Circle(CircleId(uuid::Uuid::from_u128(10)));
    b_store.join_circle(&b, CircleId(uuid::Uuid::from_u128(10)));
    sql(
        &a,
        "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','store','store')",
    )
    .await
    .unwrap();
    sql(&b, "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','circle')").await.unwrap();
    let store = frames(&a, Audience::Store).await;
    let store_prefix = store.prefix.clone();
    let store = store.concat();
    let gifts = frames(&b, circle.clone()).await;
    let gifts_prefix = gifts.prefix.clone();
    let gifts = gifts.concat();
    let a_write = records(&a)[0].clone();
    let b_write = records(&b)[0].clone();
    a.apply_downloaded(b_write.clone().into()).await.unwrap();
    let original = contents(&c);
    assert!(c
        .load_snapshots(crate::SnapshotReload::new(
            vec![
                (
                    id(Audience::Store),
                    store_prefix.clone(),
                    Cursor::new(store.clone())
                ),
                (
                    id(circle.clone()),
                    gifts_prefix.clone(),
                    Cursor::new(gifts[..gifts.len() - 1].to_vec())
                )
            ],
            vec![stream(&a_write), stream(&b_write)]
        ))
        .await
        .is_err());
    assert_eq!(contents(&c), original);
    for mode in [2, 0, 1] {
        let mut snapshots = vec![
            (
                id(Audience::Store),
                store_prefix.clone(),
                Cursor::new(store.clone()),
            ),
            (
                id(circle.clone()),
                gifts_prefix.clone(),
                Cursor::new(gifts.clone()),
            ),
        ];
        match mode {
            0 => {}
            1 => snapshots.reverse(),
            _ => {
                snapshots.pop();
            }
        }
        c.load_snapshots(crate::SnapshotReload::new(
            snapshots,
            vec![stream(&b_write), stream(&a_write)],
        ))
        .await
        .unwrap();
        for audience in [Audience::Store, circle.clone()] {
            assert_eq!(
                frames(&c, audience.clone()).await,
                frames(&a, audience).await
            );
        }
    }
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn supplied_dismissals_apply_before_waiting_changes_and_validate_their_past() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let c_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    let c = c_store.schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','initial','')")
        .await
        .unwrap();
    for db in [&b, &c] {
        db.apply_downloaded(records(&a)[0].clone().into())
            .await
            .unwrap();
    }
    sql(&a, "UPDATE notes SET title='a'").await.unwrap();
    sql(&b, "UPDATE notes SET title='b'").await.unwrap();
    a.apply_downloaded(records(&b)[0].clone().into())
        .await
        .unwrap();
    b.apply_downloaded(records(&a)[1].clone().into())
        .await
        .unwrap();
    c.apply_downloaded(records(&a)[1].clone().into())
        .await
        .unwrap();
    c.apply_downloaded(records(&b)[0].clone().into())
        .await
        .unwrap();
    let snapshot = frames(&a, Audience::Store).await;
    let snapshot_prefix = snapshot.prefix.clone();
    let snapshot = snapshot.concat();
    let losses = a.lost_values().await.unwrap();
    assert_eq!(losses.len(), 1);
    a.dismiss_lost_values(&losses).await.unwrap();
    let dismissal = records(&a).pop().unwrap();
    assert_eq!(dismissal.parts[0].dismissals.len(), 1);
    for db in [&b, &c] {
        db.apply_downloaded(dismissal.clone().into()).await.unwrap();
    }
    sql(&b, "UPDATE notes SET body='waiting'").await.unwrap();
    let waiting = records(&b);
    c.apply_downloaded(waiting.last().unwrap().clone().into())
        .await
        .unwrap();
    let before = contents(&b);
    let mut invalid = stream(&dismissal);
    invalid.header.header.had_read.0.clear();
    invalid.header.header.position.number = 1;
    let invalid_position = invalid.header.header.position;
    assert!(matches!(
        b.load_snapshots(crate::SnapshotReload::new(
            vec![(
                id(Audience::Store),
                snapshot_prefix.clone(),
                Cursor::new(snapshot.clone())
            )],
            vec![invalid],
        ))
        .await,
        Err(crate::DbError::Snapshot(crate::SnapshotError::Write { write, error }))
            if write == invalid_position && matches!(*error, crate::DbError::Snapshot(crate::SnapshotError::Format(_)))
    ));
    assert_eq!(contents(&b), before);
    b.load_snapshots(crate::SnapshotReload::new(
        vec![(
            id(Audience::Store),
            snapshot_prefix.clone(),
            Cursor::new(snapshot),
        )],
        vec![stream(&dismissal)],
    ))
    .await
    .unwrap();
    assert_eq!(records(&b), waiting);
    assert!(b.lost_values().await.unwrap().is_empty());
    assert_eq!(
        frames(&b, Audience::Store).await,
        frames(&c, Audience::Store).await
    );
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

mod differential {
    use crate::snapshot_write::tests::{frames, stream};
    use crate::test_utils::tests::*;
    use crate::tests::TestStore;
    use crate::{Database, SnapshotReload, WriteBoundary};
    use coven_format::value::{Value as Scalar, WritePositions};
    use coven_format::write::{WriteDisposition, WriteRecord};
    use coven_foundation::id_source::{DeviceId, SequentialIds};
    use coven_merge::{Audience, Change, ColumnValue, History, Operation};
    use serde_json::{json, Value};
    use std::collections::{BTreeMap, BTreeSet};
    use std::ffi::OsStr;

    #[derive(Clone)]
    struct SchemaHistory {
        reload: ReloadHistory,
        raised: bool,
    }
    impl SchemaHistory {
        fn generate(rng: &mut Generator) -> Self {
            let mut reload = ReloadHistory::generate(rng);
            reload.reset = false;
            for w in &mut reload.writes {
                // Conversion/version processing supplies the model's verdict.
                // A new-schema author has reloaded the boundary; its consumed
                // excluded positions do not contribute row generations.
                w.header.schema_version = if w.header.store_log_read.covers(RESET) {
                    2
                } else {
                    1
                };
                w.header.store_log_read.0.clear();
            }
            let mut frozen = insert(400, 400);
            frozen.header.schema_version = 2;
            frozen.header.disposition = WriteDisposition::Lost(2);
            let Operation::Insert(columns) = &mut frozen.parts[0].rows[0].change.operation else {
                unreachable!()
            };
            columns.insert(
                "retired".into(),
                ColumnValue {
                    value: Scalar::Text("snapshot old column".into()),
                    parents: BTreeMap::new(),
                },
            );
            reload.covered.insert(frozen.header.position);
            reload.writes.push(frozen);
            // Empty-valued deletions still have an identity in a frozen loss.
            let mut deletion = insert(401, 0);
            deletion.header.disposition = WriteDisposition::Lost(2);
            deletion.header.had_read = WritePositions(vec![
                reload
                    .writes
                    .iter()
                    .find(|w| w.header.position.device == DeviceId(100))
                    .unwrap()
                    .header
                    .position,
            ]);
            deletion.parts[0].rows[0].change = Change {
                generation: 1,
                operation: Operation::Delete,
            };
            if rng.pick(2) == 0 {
                deletion.parts[0].rows[0]
                    .old
                    .insert("retired".into(), Scalar::Text("deleted old column".into()));
            }
            reload.writes.push(deletion);
            let mut migration = insert(402, 402);
            migration.header.schema_version = 2;
            migration.header.disposition = WriteDisposition::Migration;
            migration.parts.clear();
            reload.writes.push(migration);
            reload.writes = rng.order(&reload.writes);
            Self {
                reload,
                raised: true,
            }
        }
        fn admitted(&self, w: &WriteRecord) -> bool {
            w.header.disposition == WriteDisposition::Apply
                && (self.reload.covered.contains(&w.header.position)
                    || !self.raised
                    || w.header.schema_version >= 2)
        }
        fn valid(&self) -> bool {
            self.reload.valid()
                && self.reload.writes.iter().all(|w| {
                    !self.admitted(w)
                        || w.parts
                            .iter()
                            .flat_map(|p| &p.rows)
                            .all(|r| match &r.change.operation {
                                Operation::Insert(cols) | Operation::Update(cols) => {
                                    !cols.contains_key("retired")
                                }
                                Operation::Delete => true,
                            })
                })
                && History::new(self.reload.writes.iter().map(|w| {
                    let mut merged = merge_write(w);
                    if !self.admitted(w) {
                        merged.changes.clear();
                    }
                    merged
                }))
                .is_ok()
        }
        fn input(&self) -> Value {
            let mut input = self.reload.input();
            let rows = self.reload.rows();
            let mut schema = Vec::new();
            let mut values = Vec::new();
            let mut frozen = Vec::new();
            let mut snapshot = Vec::new();
            for w in &self.reload.writes {
                let id = write_id(w.header.position);
                let verdict = match w.header.disposition {
                    WriteDisposition::Migration => json!({"id": id, "kind": 2}),
                    WriteDisposition::Lost(version) => {
                        json!({"id": id, "kind": 1, "version": version})
                    }
                    WriteDisposition::Apply if self.raised && w.header.schema_version < 2 => {
                        json!({"id": id, "kind": 1, "version": 2})
                    }
                    WriteDisposition::Apply => json!({"id": id, "kind": 0}),
                };
                schema.push(verdict);
                if self.reload.covered.contains(&w.header.position)
                    && w.header.disposition == WriteDisposition::Apply
                {
                    snapshot.push(id);
                }
                for change in w.parts.iter().flat_map(|p| &p.rows) {
                    let row = rows.iter().position(|r| *r == change.row).unwrap();
                    let columns: Vec<_> = match &change.change.operation {
                        Operation::Insert(cols) | Operation::Update(cols) => {
                            cols.keys().map(|c| column_id(c)).collect()
                        }
                        Operation::Delete => change.old.keys().map(|c| column_id(c)).collect(),
                    };
                    values.push(json!({"write": id, "row": row, "cols": columns}));
                    if self.reload.covered.contains(&w.header.position) {
                        if let WriteDisposition::Lost(version) = w.header.disposition {
                            frozen.push(json!({"row": row, "gen": change.change.generation, "version": version, "write": id}));
                        }
                    }
                }
            }
            input["mode"] = json!("schema");
            input["schema"] = json!(schema);
            input["values"] = json!(values);
            input["snapshot"] = json!(snapshot);
            input["snapshotFrozen"] = json!(frozen);
            input
        }
    }

    async fn schema_database(store: &TestStore) -> Database {
        store
            .builder(
                tables(),
                vec![
                    crate::Migration::sql(1, "rows", SCHEMA),
                    crate::Migration::sql(
                        2,
                        "local table",
                        "CREATE TABLE local_rows(id TEXT PRIMARY KEY)",
                    ),
                ],
            )
            .open()
            .await
            .unwrap()
    }
    async fn schema_result(history: &SchemaHistory) -> Value {
        assert!(history.valid(), "invalid schema differential history");
        let ids = SequentialIds::new();
        let source_store = TestStore::with_ids(&ids);
        let target_store = TestStore::with_ids(&ids);
        let source = schema_database(&source_store).await;
        let target = schema_database(&target_store).await;
        for w in history
            .reload
            .writes
            .iter()
            .filter(|w| history.reload.covered.contains(&w.header.position))
        {
            assert_eq!(
                source.apply_downloaded(w.clone().into()).await.unwrap(),
                crate::ApplyOutcome::Applied
            );
        }
        let snapshot = frames(&source, Audience::Store).await;
        let boundary = WriteBoundary::SchemaChange {
            version: 2,
            audience: Audience::Store,
            included: snapshot.prefix.writes.clone(),
        };
        let mut reload = SnapshotReload::new(
            vec![snapshot.input()],
            history.reload.writes.iter().map(stream).collect(),
        );
        reload.boundaries = Some(if history.raised {
            vec![boundary]
        } else {
            Vec::new()
        });
        target.load_snapshots(reload).await.unwrap();
        let result = result_json(&target, &history.reload.rows());
        let positions = target.sync_state(Vec::new()).await.unwrap().positions;
        assert!(history
            .reload
            .writes
            .iter()
            .all(|w| positions.covers(w.header.position)));
        source.close().await.unwrap();
        target.close().await.unwrap();
        result
    }
    async fn mismatch(runner: &OsStr, history: &SchemaHistory) -> bool {
        schema_result(history).await != run_lean(runner, &history.input())
    }
    async fn reduce(runner: &OsStr, mut history: SchemaHistory) -> SchemaHistory {
        loop {
            let mut candidates: Vec<_> = history
                .reload
                .reductions()
                .into_iter()
                .map(|reload| SchemaHistory {
                    reload,
                    raised: history.raised,
                })
                .collect();
            if history.raised {
                candidates.push(SchemaHistory {
                    raised: false,
                    ..history.clone()
                });
            }
            for i in 0..history.reload.writes.len() {
                if matches!(
                    history.reload.writes[i].header.disposition,
                    WriteDisposition::Lost(_)
                ) {
                    let mut candidate = history.clone();
                    candidate.reload.writes[i].header.disposition = WriteDisposition::Apply;
                    candidates.push(candidate);
                }
            }
            let mut smaller = None;
            for candidate in candidates {
                if candidate.valid() && mismatch(runner, &candidate).await {
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
    #[ignore = "requires COVEN_MERGE_LEAN; scripts/check.sh supplies the runner"]
    async fn schema_differential_histories() {
        let runner = std::env::var_os("COVEN_MERGE_LEAN").expect("built mergeRunner");
        for seed in 0..128 {
            let mut rng = Generator::new(seed + 40_000);
            let history = SchemaHistory::generate(&mut rng);
            for prefix in [history.reload.writes.len(), 4 + rng.pick(20)] {
                let mut case = history.clone();
                case.reload.writes.truncate(prefix);
                let present: BTreeSet<_> = case
                    .reload
                    .writes
                    .iter()
                    .map(|w| w.header.position)
                    .collect();
                case.reload.covered.retain(|id| present.contains(id));
                for _ in 0..3 {
                    case.reload.writes = rng.order(&case.reload.writes);
                    if mismatch(&runner, &case).await {
                        let reduced = reduce(&runner, case).await;
                        panic!(
                            "schema differential, seed {seed}\nreduced history: {}\nRust: {}\nLean: {}",
                            reduced.input(),
                            schema_result(&reduced).await,
                            run_lean(&runner, &reduced.input())
                        );
                    }
                }
            }
        }
    }
}
