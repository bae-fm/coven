use super::*;
use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{Database, RowIdentity, SyncedTable};
use coven_format::snapshot::SnapshotDecoder;
use coven_format::value::Value;
use coven_foundation::id_source::{CircleId, SequentialIds};
use coven_merge::{MergeError, Timestamp, WriteOracle};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Oracle(BTreeMap<WriteId, AppliedWrite>);
impl WriteOracle for Oracle {
    fn timestamp(&self, id: WriteId) -> Option<Timestamp> {
        self.0.get(&id).map(|write| write.timestamp)
    }
    fn had_read(&self, reader: WriteId, earlier: WriteId) -> Result<bool, MergeError> {
        let write = self
            .0
            .get(&reader)
            .ok_or(MergeError::MissingWrite(reader))?;
        Ok(if reader.device == earlier.device {
            earlier.number < reader.number
        } else {
            write.had_read.covers(earlier)
        })
    }
}

pub(crate) fn id(audience: Audience) -> SnapshotId {
    SnapshotId {
        device: DeviceId(99),
        number: 1,
        audience,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotFrames {
    pub prefix: coven_format::sealed_snapshot::SnapshotObjectPrefix,
    pub frames: Vec<Vec<u8>>,
}
impl SnapshotFrames {
    pub(crate) fn concat(&self) -> Vec<u8> {
        self.frames.concat()
    }
    pub(crate) fn input(
        &self,
    ) -> (
        SnapshotId,
        coven_format::sealed_snapshot::SnapshotObjectPrefix,
        std::io::Cursor<Vec<u8>>,
    ) {
        (
            id(self.prefix.audience.clone()),
            self.prefix.clone(),
            std::io::Cursor::new(self.concat()),
        )
    }
}

pub(crate) fn prefix(
    header: &SnapshotHeader,
) -> coven_format::sealed_snapshot::SnapshotObjectPrefix {
    coven_format::sealed_snapshot::SnapshotObjectPrefix {
        audience: header.id.audience.clone(),
        key: coven_foundation::id_source::KeyId(uuid::Uuid::from_u128(1)),
        writes: header.writes.clone(),
        store_log: header.store_log.clone(),
    }
}

pub(crate) async fn frames(database: &Database, audience: Audience) -> SnapshotFrames {
    let frames = Arc::new(Mutex::new(Vec::new()));
    let metadata = Arc::new(Mutex::new(None));
    let output = frames.clone();
    let start = metadata.clone();
    database
        .write_snapshot(
            id(audience),
            move |header| {
                *start.lock().unwrap() = Some(prefix(header));
                Ok(())
            },
            move |frame| {
                output.lock().unwrap().push(frame);
                Ok::<_, std::convert::Infallible>(())
            },
        )
        .await
        .unwrap();
    SnapshotFrames {
        prefix: Arc::try_unwrap(metadata)
            .unwrap()
            .into_inner()
            .unwrap()
            .unwrap(),
        frames: Arc::try_unwrap(frames).unwrap().into_inner().unwrap(),
    }
}

pub(crate) fn decode(snapshot: &SnapshotFrames) -> (SnapshotHeader, Vec<SnapshotRecord>) {
    let frames = &snapshot.frames;
    let mut decoder = SnapshotDecoder::start(&frames[0], &snapshot.prefix).unwrap();
    let mut oracle = Oracle::default();
    let mut records = Vec::new();
    for frame in &frames[1..] {
        if let Some(record) = decoder.frame(frame, &oracle).unwrap() {
            if let SnapshotRecord::Write(write) = &record {
                oracle.0.insert(write.id, write.clone());
            }
            records.push(record);
        }
    }
    decoder.finish().unwrap();
    (decoder.header().clone(), records)
}

pub(crate) fn stream(
    record: &coven_format::write::WriteRecord,
) -> crate::DownloadedWriteStream<std::io::Cursor<Vec<u8>>> {
    let encoder = coven_format::write_stream::WriteEncoder::new(record).unwrap();
    crate::DownloadedWriteStream {
        header: encoder.header().clone(),
        parts: (0..record.parts.len())
            .map(|index| {
                let bytes = encoder
                    .part_chunks(index)
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()
                    .concat();
                crate::DownloadedPartStream::Opened(std::io::Cursor::new(bytes))
            })
            .collect(),
    }
}

pub(crate) async fn load_one<R: std::io::Read + Send + 'static>(
    database: &Database,
    expected: SnapshotId,
    prefix: coven_format::sealed_snapshot::SnapshotObjectPrefix,
    plaintext: R,
) -> Result<(), crate::DbError> {
    database
        .load_snapshots(crate::SnapshotReload::new(
            vec![(expected, prefix, plaintext)],
            Vec::<crate::DownloadedWriteStream<std::io::Cursor<Vec<u8>>>>::new(),
        ))
        .await
}

#[tokio::test]
async fn a_snapshot_round_trips_rows_history_and_excluded_changes() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let source = source_store.schema(notes(), NOTES).await.unwrap();
    let receiver = receiver_store.schema(notes(), NOTES).await.unwrap();
    sql(
        &source,
        "INSERT INTO notes VALUES('n','first','body'); INSERT INTO local_rows VALUES('private')",
    )
    .await
    .unwrap();
    sql(&source, "UPDATE notes SET title='second'")
        .await
        .unwrap();
    sql(&source, "DELETE FROM notes").await.unwrap();
    sql(&source, "INSERT INTO notes VALUES('n','third','again')")
        .await
        .unwrap();
    let writes = records(&source);
    for write in &writes {
        receiver
            .apply_downloaded(write.clone().into())
            .await
            .unwrap();
    }
    sql(&source, "UPDATE notes SET title='excluded'")
        .await
        .unwrap();
    let mut excluded = records(&source).pop().unwrap();
    excluded.header.disposition = coven_format::write::WriteDisposition::Lost(1);
    receiver
        .apply_downloaded(excluded.clone().into())
        .await
        .unwrap();
    let (header, snapshot) = decode(&frames(&receiver, Audience::Store).await);
    assert_eq!(header.schema_version, 1);
    assert_eq!(header.writes.0, [excluded.header.position]);
    assert_eq!(header.counts, [1, 5, 3, 1, 1, 0]);
    let synced = snapshot
        .iter()
        .find_map(|record| match record {
            SnapshotRecord::Synced(row) => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(synced.row.table, "notes");
    assert_eq!(synced.columns["title"].value, Value::Text("third".into()));
    let merged = snapshot
        .iter()
        .find_map(|record| match record {
            SnapshotRecord::Merge(row) => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(merged.state.generations().len(), 3);
    assert_eq!(
        merged.state.cells()["title"].write,
        writes[3].header.position
    );
    let lost = snapshot
        .iter()
        .find_map(|record| match record {
            SnapshotRecord::LostWrite(write) => Some(write),
            _ => None,
        })
        .unwrap();
    assert_eq!(lost.header, excluded.header);
    let lost_row = snapshot
        .iter()
        .find_map(|record| match record {
            SnapshotRecord::LostWriteRow(row) => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(lost_row.change, excluded.parts[0].rows[0]);
    assert_eq!(count(&source, "_coven_uploads"), 5);
    assert_eq!(count(&receiver, "_coven_uploads"), 0);
    assert_loaded_losses(&receiver, &source).await;
    source.close().await.unwrap();
    receiver.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_frames_keep_one_committed_state_while_the_writer_commits() {
    let store = TestStore::new();
    let database = store.schema(notes(), NOTES).await.unwrap();
    sql(&database, "INSERT INTO notes VALUES('n','before','body')")
        .await
        .unwrap();
    let before = decode(&frames(&database, Audience::Store).await);
    let (started, observed) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let output = Arc::new(Mutex::new(Vec::new()));
    let produced = output.clone();
    let metadata = Arc::new(Mutex::new(None));
    let metadata_output = metadata.clone();
    let clone = database.clone();
    let snapshot = tokio::spawn(async move {
        clone
            .write_snapshot(
                id(Audience::Store),
                move |header| {
                    *metadata_output.lock().unwrap() = Some(prefix(header));
                    started.send(()).unwrap();
                    wait.recv_timeout(Duration::from_secs(20)).unwrap();
                    Ok(())
                },
                move |frame| {
                    produced.lock().unwrap().push(frame);
                    Ok::<_, std::convert::Infallible>(())
                },
            )
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(20), observed)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(20),
        sql(
            &database,
            "UPDATE notes SET title='after'; INSERT INTO notes VALUES('other','new','')",
        ),
    )
    .await
    .unwrap()
    .unwrap();
    release.send(()).unwrap();
    snapshot.await.unwrap();
    let during = decode(&SnapshotFrames {
        prefix: metadata.lock().unwrap().take().unwrap(),
        frames: output.lock().unwrap().clone(),
    });
    assert_eq!(during, before);
    let after = decode(&frames(&database, Audience::Store).await);
    assert_ne!(after, before);
    assert_eq!(after.0.counts[0], 2);
    database.close().await.unwrap();
}

#[tokio::test]
async fn circle_snapshot_has_only_its_rows_and_reference_metadata() {
    let store = TestStore::new();
    let database = store.schema(vec![
        SyncedTable::new("roots", RowIdentity::SharedKey),
        SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
    ], "CREATE TABLE roots(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,root TEXT REFERENCES roots(id)); CREATE INDEX note_root ON notes(root)").await.unwrap();
    sql(&database, "INSERT INTO roots VALUES('root'); INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-4000-8000-00000000000a','root'),('00000000-0000-4000-8000-000000000002','00000000-0000-4000-8000-00000000000b','root')").await.unwrap();
    let circle = Audience::Circle(CircleId(
        uuid::Uuid::parse_str("00000000-0000-4000-8000-00000000000a").unwrap(),
    ));
    let (header, snapshot) = decode(&frames(&database, circle.clone()).await);
    assert_eq!(header.counts[0], 1);
    assert_eq!(header.counts[3], 1);
    for record in snapshot {
        match record {
            SnapshotRecord::Synced(row) => {
                assert_eq!(row.row.audience, circle);
                assert_eq!(row.row.table, "notes");
                assert_eq!(
                    row.columns["root"]
                        .parents
                        .values()
                        .next()
                        .unwrap()
                        .row
                        .audience,
                    Audience::Store
                );
            }
            SnapshotRecord::Merge(row) => assert_eq!(row.state.row().audience, circle),
            SnapshotRecord::Column(column) => assert_eq!(column.table, "notes"),
            SnapshotRecord::Write(_) => {}
            other => panic!("unexpected record {other:?}"),
        }
    }
    database.close().await.unwrap();
}

#[tokio::test]
async fn output_errors_and_panics_release_the_reader_transaction() {
    let store = TestStore::new();
    let database = store.schema(notes(), NOTES).await.unwrap();
    let refused_prefix = database
        .write_snapshot(
            id(Audience::Store),
            |_| Err("prefix refused"),
            |_| -> Result<(), &str> { panic!("no frames after prefix failure") },
        )
        .await;
    assert!(matches!(
        refused_prefix,
        Err(SnapshotWriteError::Output("prefix refused"))
    ));
    let result = database
        .write_snapshot(id(Audience::Store), |_| Ok(()), |_| Err("consumer refused"))
        .await;
    assert!(matches!(
        result,
        Err(SnapshotWriteError::Output("consumer refused"))
    ));
    let clone = database.clone();
    let task = tokio::spawn(async move {
        clone
            .write_snapshot(
                id(Audience::Store),
                |_| Ok(()),
                |_| -> Result<(), ()> { panic!("consumer panic") },
            )
            .await
    });
    assert!(task.await.unwrap_err().is_panic());
    sql(&database, "INSERT INTO notes VALUES('n','still open','')")
        .await
        .unwrap();
    assert_eq!(
        decode(&frames(&database, Audience::Store).await).0.counts[0],
        1
    );
    database.close().await.unwrap();
}

#[tokio::test]
async fn a_migration_preserves_removed_rows_and_their_concurrent_losses_in_snapshots() {
    const UNIQUE_NOTES: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL UNIQUE,body TEXT NOT NULL DEFAULT '')";
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let c_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), UNIQUE_NOTES).await.unwrap();
    let b = b_store.schema(notes(), UNIQUE_NOTES).await.unwrap();
    let c = c_store.schema(notes(), UNIQUE_NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('a','same','winner')")
        .await
        .unwrap();
    sql(&b, "INSERT INTO notes VALUES('b','same','removed')")
        .await
        .unwrap();
    let inserted = records(&b)[0].clone();
    c.apply_downloaded(inserted.clone().into()).await.unwrap();
    sql(&b, "UPDATE notes SET body='b edit'").await.unwrap();
    sql(&c, "UPDATE notes SET body='c edit'").await.unwrap();
    for write in [inserted, records(&b)[1].clone(), records(&c)[0].clone()] {
        a.apply_downloaded(write.into()).await.unwrap();
    }
    let losses = a.lost_values().await.unwrap();
    assert_eq!(losses.len(), 2);
    a.close().await.unwrap();
    let a = a_store
        .builder(
            notes(),
            vec![
                crate::Migration::sql(1, "unique notes", UNIQUE_NOTES),
                crate::Migration::sql(
                    2,
                    "rename body",
                    "ALTER TABLE notes RENAME COLUMN body TO content",
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    let retained = a.lost_values().await.unwrap();
    assert_eq!(retained.len(), losses.len());
    for (after, before) in retained.iter().zip(&losses) {
        assert_eq!(
            (&after.table, &after.key, &after.lost, &after.replaced_by),
            (
                &before.table,
                &before.key,
                &before.lost,
                &before.replaced_by
            )
        );
        assert!(matches!(after.target, crate::lost::LossTarget::Cells(_)));
    }
    let (header, snapshot) = decode(&frames(&a, Audience::Store).await);
    assert_eq!(header.schema_version, 2);
    assert_eq!(header.counts[3], 1);
    assert_eq!(header.counts[5], 2);
    let retained: Vec<_> = snapshot
        .into_iter()
        .filter_map(|record| match record {
            SnapshotRecord::RetainedLoss(loss) => Some(loss),
            _ => None,
        })
        .collect();
    assert_eq!(retained.len(), 2);
    for loss in &retained {
        assert_eq!(
            coven_format::key::decode_key(&loss.row.key).unwrap(),
            [Value::Text("b".into())]
        );
    }
    assert!(
        matches!(&retained[0].values, coven_format::retained_loss::RetainedValues::Cell { key, .. } if key.column=="body")
    );
    assert!(
        matches!(&retained[1].values, coven_format::retained_loss::RetainedValues::Row { cells, .. } if cells.contains_key("body") && !cells.contains_key("content"))
    );
    // Retired history also survives a new incarnation with the same identity.
    sql(
        &a,
        "INSERT INTO notes VALUES('b','different','new incarnation')",
    )
    .await
    .unwrap();
    let d_store = TestStore::with_ids(&ids);
    let d = d_store
        .builder(
            notes(),
            vec![
                crate::Migration::sql(1, "unique notes", UNIQUE_NOTES),
                crate::Migration::sql(
                    2,
                    "rename body",
                    "ALTER TABLE notes RENAME COLUMN body TO content",
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    // The empty device also authored its schema migration. Compare snapshots
    // only after both devices have consumed the same writes.
    for write in records(&d) {
        a.apply_downloaded(write.into()).await.unwrap();
    }
    for _ in 0..2 {
        assert_loaded_losses(&a, &d).await;
    }
    let cells: Vec<_> = a
        .lost_values()
        .await
        .unwrap()
        .into_iter()
        .filter(|loss| matches!(loss.lost, crate::Lost::Cell(_)))
        .collect();
    a.dismiss_lost_values(&cells).await.unwrap();
    assert_loaded_losses(&a, &d).await;
    assert_eq!(decode(&frames(&d, Audience::Store).await).0.counts[5], 1);
    a.dismiss_lost_values(&a.lost_values().await.unwrap())
        .await
        .unwrap();
    assert_loaded_losses(&a, &d).await;
    assert_eq!(decode(&frames(&d, Audience::Store).await).0.counts[5], 0);
    assert_eq!(count(&d, "notes"), 2);
    for database in [a, b, c, d] {
        database.close().await.unwrap();
    }
}

#[tokio::test]
async fn snapshots_keep_the_written_reference_when_the_app_reads_null_or_default() {
    for action in ["SET NULL", "SET DEFAULT"] {
        let ids = SequentialIds::new();
        let a_store = TestStore::with_ids(&ids);
        let b_store = TestStore::with_ids(&ids);
        let migrations = || {
            vec![crate::Migration::run(1, "references", move |sql| {
                sql.execute_batch(&format!("CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY COLLATE NOCASE); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT DEFAULT 'Inbox' REFERENCES parents(id) ON DELETE {action})"))?;
                Ok(())
            })]
        };
        let tables = || {
            ["parents", "children"]
                .into_iter()
                .map(|name| SyncedTable::new(name, RowIdentity::SharedKey))
                .collect()
        };
        let a = a_store
            .builder(tables(), migrations())
            .open()
            .await
            .unwrap();
        let b = b_store
            .builder(tables(), migrations())
            .open()
            .await
            .unwrap();
        sql(&a, "INSERT INTO parents VALUES('parent'),('Inbox')")
            .await
            .unwrap();
        b.apply_downloaded(records(&a).remove(0).into())
            .await
            .unwrap();
        sql(&b, "INSERT INTO children VALUES('child','PaReNt')")
            .await
            .unwrap();
        sql(&a, "DELETE FROM parents WHERE id='parent'")
            .await
            .unwrap();
        b.apply_downloaded(records(&a).remove(1).into())
            .await
            .unwrap();
        let displayed: Option<String> = b
            .read(|sql| Ok(sql.query_row("SELECT parent FROM children", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(
            displayed,
            if action == "SET NULL" {
                None
            } else {
                Some("Inbox".into())
            }
        );
        let (_, snapshot) = decode(&frames(&b, Audience::Store).await);
        let mut references = Vec::new();
        for record in snapshot {
            match record {
                SnapshotRecord::Synced(row) if row.row.table == "children" => {
                    references.push(row.columns["parent"].clone())
                }
                SnapshotRecord::Merge(row) if row.state.row().table == "children" => {
                    references.push(row.state.cells()["parent"].value.clone())
                }
                _ => {}
            }
        }
        assert_eq!(references.len(), 2);
        for written in references {
            assert_eq!(written.value, Value::Text("PaReNt".into()));
            assert_eq!(written.parents.values().next().unwrap().generation, 1);
        }
        for database in [a, b] {
            database.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn excluded_snapshots_keep_only_undismissed_cells_and_rows() {
    use coven_format::{dismissal::Dismissal, write::WriteDisposition};
    use coven_merge::Operation;
    for (operation, lost_dismissal) in ["insert", "update", "delete"]
        .into_iter()
        .flat_map(|operation| [false, true].map(|lost| (operation, lost)))
    {
        let ids = SequentialIds::new();
        let source_store = TestStore::with_ids(&ids);
        let receiver_store = TestStore::with_ids(&ids);
        let source = source_store.schema(notes(), NOTES).await.unwrap();
        let receiver = if operation == "insert" {
            receiver_store.schema(Vec::new(), "SELECT 1").await.unwrap()
        } else {
            receiver_store.schema(notes(), NOTES).await.unwrap()
        };
        sql(
            &source,
            "INSERT INTO notes VALUES('a','excluded','body'),('b','excluded too','body')",
        )
        .await
        .unwrap();
        if operation != "insert" {
            receiver
                .apply_downloaded(records(&source)[0].clone().into())
                .await
                .unwrap();
            sql(
                &source,
                if operation == "update" {
                    "UPDATE notes SET title='changed',body='changed body'"
                } else {
                    "DELETE FROM notes"
                },
            )
            .await
            .unwrap();
        }
        let mut excluded = records(&source).pop().unwrap();
        excluded.header.disposition = WriteDisposition::Lost(1);
        receiver
            .apply_downloaded(excluded.clone().into())
            .await
            .unwrap();
        sql(&source, "INSERT INTO notes VALUES('next','later','')")
            .await
            .unwrap();
        let mut dismissal = records(&source).pop().unwrap();
        dismissal.parts[0].rows.clear();
        dismissal.parts[0].dismissals = vec![Dismissal {
            row: excluded.parts[0].rows[0].row.clone(),
            column: "title".into(),
            write: excluded.header.position,
        }];
        if lost_dismissal {
            dismissal.header.disposition = WriteDisposition::Lost(1);
        }
        receiver.apply_downloaded(dismissal.into()).await.unwrap();
        let (header, snapshot) = decode(&frames(&receiver, Audience::Store).await);
        assert_eq!(header.counts[4], 1);
        let rows: Vec<_> = snapshot
            .into_iter()
            .filter_map(|r| match r {
                SnapshotRecord::LostWriteRow(row) => Some(row.change),
                _ => None,
            })
            .collect();
        assert_eq!(rows.len(), 2);
        match &rows[0].change.operation {
            Operation::Insert(values) => assert_eq!(
                values.keys().map(String::as_str).collect::<Vec<_>>(),
                ["body", "id"]
            ),
            Operation::Update(values) => {
                assert_eq!(
                    values.keys().map(String::as_str).collect::<Vec<_>>(),
                    ["body"]
                );
                assert_eq!(
                    rows[0].old.keys().map(String::as_str).collect::<Vec<_>>(),
                    ["body"]
                );
            }
            Operation::Delete => assert_eq!(
                rows[0].old.keys().map(String::as_str).collect::<Vec<_>>(),
                ["body", "id"]
            ),
        }
        assert_eq!(rows[1], excluded.parts[0].rows[1]);
        let target_store = TestStore::with_ids(&ids);
        let target = if operation == "insert" {
            target_store.schema(Vec::new(), "SELECT 1").await.unwrap()
        } else {
            target_store.schema(notes(), NOTES).await.unwrap()
        };
        assert_loaded_losses(&receiver, &target).await;
        receiver
            .dismiss_lost_values(&receiver.lost_values().await.unwrap())
            .await
            .unwrap();
        let (header, snapshot) = decode(&frames(&receiver, Audience::Store).await);
        assert_eq!(header.counts[4], 0);
        assert!(!snapshot.iter().any(|record| matches!(
            record,
            SnapshotRecord::LostWrite(_) | SnapshotRecord::LostWriteRow(_)
        )));
        assert_eq!(count(&receiver, "_coven_excluded_writes"), 0);
        assert_eq!(count(&receiver, "_coven_excluded_rows"), 0);
        assert_loaded_losses(&receiver, &target).await;
        target.close().await.unwrap();
        source.close().await.unwrap();
        receiver.close().await.unwrap();
    }
}

pub(crate) async fn assert_loaded_losses(source: &Database, target: &Database) {
    let snapshot = frames(source, Audience::Store).await;
    load_one(
        target,
        id(Audience::Store),
        snapshot.prefix.clone(),
        std::io::Cursor::new(snapshot.concat()),
    )
    .await
    .unwrap();
    assert_eq!(frames(target, Audience::Store).await, snapshot);
    let loaded = target.lost_values().await.unwrap();
    let expected = source.lost_values().await.unwrap();
    assert_eq!(loaded.len(), expected.len());
    assert!(expected.iter().all(|loss| loaded.contains(loss)));
    let key = coven_crypto::StoreKey::from_bytes(
        coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([1; 16])),
        [7; 32],
    )
    .derive();
    assert_eq!(
        target
            .sync_state(vec![(Audience::Store, key.fingerprint_hasher())])
            .await
            .unwrap()
            .fingerprints,
        source
            .sync_state(vec![(Audience::Store, key.fingerprint_hasher())])
            .await
            .unwrap()
            .fingerprints,
    );
}
