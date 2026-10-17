use super::*;
use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::write_boundary::tests::version_two;
use crate::{Database, RowIdentity, SyncedTable};
use coven_format::snapshot::SnapshotDecoder;
use coven_format::value::Value;
use coven_foundation::id_source::{CircleId, DeviceId, SequentialIds};
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
            self.0
                .get(&earlier)
                .is_some_and(|earlier| earlier.slot.0 <= write.read_through)
        })
    }
}

impl coven_format::snapshot::WriteSlots for Oracle {
    fn slot(&self, id: WriteId) -> Option<coven_merge::Slot> {
        self.0.get(&id).map(|w| w.slot)
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
        coven_foundation::id_source::MemberId,
    ) {
        (
            decode(self).0.id,
            self.prefix.clone(),
            std::io::Cursor::new(self.concat()),
            author(),
        )
    }
}

/// The member signing test snapshots: no fixture device belongs to it, so
/// every write a snapshot carries is another member's.
pub(crate) fn author() -> coven_foundation::id_source::MemberId {
    coven_foundation::id_source::MemberId(uuid::Uuid::from_bytes([0xa1; 16]))
}

pub(crate) fn prefix(
    header: &SnapshotHeader,
) -> coven_format::sealed_snapshot::SnapshotObjectPrefix {
    coven_format::sealed_snapshot::SnapshotObjectPrefix {
        audience: header.id.audience.clone(),
        key: coven_foundation::id_source::KeyId(uuid::Uuid::from_u128(1)),
        covers: header.covers,
        purpose: coven_format::sequence::Purpose::Ordinary,
    }
}

pub(crate) async fn frames(database: &Database, audience: Audience) -> SnapshotFrames {
    frames_for(database, id(audience)).await
}

pub(crate) async fn frames_for(database: &Database, id: SnapshotId) -> SnapshotFrames {
    for record in records(database) {
        database.receive(record.into()).await.unwrap();
    }
    let frames = Arc::new(Mutex::new(Vec::new()));
    let metadata = Arc::new(Mutex::new(None));
    let output = frames.clone();
    let start = metadata.clone();
    database
        .write_snapshot(
            id,
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
        signer: crate::test_utils::signer(),
        authorship: crate::test_utils::TestAuthorship::write(record),
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

/// Load one snapshot. A failure of its body is returned as the loader's own
/// verdict, after checking that the reload named this snapshot.
pub(crate) async fn load_one<R: std::io::Read + Send + 'static>(
    database: &Database,
    expected: SnapshotId,
    prefix: coven_format::sealed_snapshot::SnapshotObjectPrefix,
    plaintext: R,
) -> Result<(), crate::DbError> {
    database
        .load_snapshots::<crate::pending::tests::Record, _, std::io::Empty>(crate::SnapshotReload {
            target: prefix.covers,
            snapshots: vec![crate::SnapshotSource::Stored {
                id: expected.clone(),
                prefix,
                input: plaintext,
                author: author(),
            }],
            slots: vec![],
            state: database.store_state().await.unwrap(),
            cutoffs: vec![],
            provider: Default::default(),
            boundaries: vec![],
            operation: None,
            authorship: std::sync::Arc::new(crate::test_utils::TestAuthorship),
        })
        .await
        .map(drop)
        .map_err(|error| match error {
            crate::DbError::Snapshot(crate::SnapshotError::Body { snapshot, error }) => {
                assert_eq!(snapshot, expected);
                *error
            }
            error => error,
        })
}

#[tokio::test]
async fn a_snapshot_round_trips_rows_history_and_excluded_changes() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let source = source_store.schema(notes(), NOTES).await.unwrap();
    let receiver = version_two(&receiver_store, true).await;
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
        receiver.receive(write.clone().into()).await.unwrap();
    }
    sql(&source, "UPDATE notes SET title='excluded'")
        .await
        .unwrap();
    let excluded = records(&source).pop().unwrap();
    receiver.exclude_later_writes(2).await;
    receiver.receive(excluded.clone().into()).await.unwrap();
    let (header, snapshot) = decode(&frames(&receiver, Audience::Store).await);
    assert_eq!(header.schema_version, 2);
    assert_eq!(header.covers, 7);
    // The migration marker has no changes; five original changes retain verdicts.
    assert_eq!(&header.counts[..6], &[1, 6, 3, 1, 1, 5]);
    let verdicts: Vec<_> = snapshot
        .iter()
        .filter_map(|record| match record {
            SnapshotRecord::Authored(authored) => Some(authored.verdict.clone()),
            _ => None,
        })
        .collect();
    use coven_format::snapshot_rows::ChangeVerdict;
    assert_eq!(
        verdicts,
        [
            ChangeVerdict::Applied,
            ChangeVerdict::Applied,
            ChangeVerdict::Applied,
            ChangeVerdict::Applied,
            ChangeVerdict::Excluded(2),
        ]
    );
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
            SnapshotRecord::Loss(loss) => Some(loss),
            _ => None,
        })
        .unwrap();
    assert_eq!(lost.row, excluded.parts[0].rows[0].row);
    let coven_format::loss::LossValues::Row(cells) = &lost.values else {
        panic!("row loss")
    };
    assert_eq!(cells["title"].write, excluded.header.position);
    assert_eq!(cells["title"].value.value, Value::Text("excluded".into()));
    assert_eq!(count(&source, "_coven_uploads"), 5);
    // The receiver queues only its own migration marker.
    assert_eq!(count(&receiver, "_coven_uploads"), 1);
    let target_store = TestStore::with_ids(&ids);
    let target = version_two(&target_store, true).await;
    consume_marker(&receiver, &target).await;
    assert_loaded_losses(&receiver, &target).await;
    target.close().await.unwrap();
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
    for circle in [
        "00000000-0000-4000-8000-00000000000a",
        "00000000-0000-4000-8000-00000000000b",
    ] {
        store.join_circle(&database, CircleId(uuid::Uuid::parse_str(circle).unwrap()));
    }
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
            SnapshotRecord::Write(_) | SnapshotRecord::Claim(_) => {}
            SnapshotRecord::Authored(authored) => {
                assert_eq!(authored.change.row().audience, circle)
            }
            SnapshotRecord::Creation(creation) => assert_eq!(creation.row.audience, circle),
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
async fn a_migration_deletes_removed_rows_and_snapshots_their_losses() {
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
    a.receive(records(&a)[0].clone().into()).await.unwrap();
    sql(&b, "INSERT INTO notes VALUES('b','same','removed')")
        .await
        .unwrap();
    let inserted = records(&b)[0].clone();
    sql(&c, "INSERT INTO notes VALUES('b','same','removed')")
        .await
        .unwrap();
    sql(&b, "UPDATE notes SET body='b edit'").await.unwrap();
    sql(&c, "UPDATE notes SET body='c edit'").await.unwrap();
    for write in [
        inserted,
        records(&c)[0].clone(),
        records(&b)[1].clone(),
        records(&c)[1].clone(),
    ] {
        a.receive(write.into()).await.unwrap();
    }
    let losses = a.lost_values().await.unwrap();
    assert!(losses
        .iter()
        .any(|loss| matches!(loss.replaced_by, crate::Replacement::Write(_))));
    assert_eq!(
        losses
            .iter()
            .filter(|loss| matches!(loss.replaced_by, crate::Replacement::Rules(_)))
            .count(),
        1
    );
    a.close().await.unwrap();
    let a = a_store
        .builder(
            notes(),
            vec![
                crate::Migration::addition(1, "unique notes", UNIQUE_NOTES),
                crate::Migration::breaking(
                    2,
                    "rename body",
                    "ALTER TABLE notes RENAME COLUMN body TO content",
                    crate::OldWrites::Drop,
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    // The migration deletes the losing row at its generation: its removed-row
    // loss becomes one frozen loss with the old names. The migration write had
    // read the concurrent cell, so its delete acknowledges that loss (§17.1).
    let retained = a.lost_values().await.unwrap();
    let removed = losses
        .iter()
        .find(|l| matches!(l.replaced_by, crate::Replacement::Rules(_)))
        .unwrap();
    assert_eq!(retained.len(), 1, "the migration delete read the cell loss");
    assert_eq!(
        retained[0].replaced_by,
        crate::Replacement::SchemaChange { version: 2 }
    );
    assert_eq!(
        (&retained[0].table, &retained[0].key, &retained[0].lost),
        (&removed.table, &removed.key, &removed.lost)
    );
    assert!(matches!(
        retained[0].target,
        crate::lost::LossTarget::Cells(_)
    ));
    let (header, snapshot) = decode(&frames(&a, Audience::Store).await);
    assert_eq!(header.schema_version, 2);
    assert_eq!(
        header.counts[3], 2,
        "the deleted row keeps its merge record"
    );
    assert_eq!(header.counts[4], 1);
    let retained: Vec<_> = snapshot
        .into_iter()
        .filter_map(|record| match record {
            SnapshotRecord::Loss(loss) => Some(loss),
            _ => None,
        })
        .collect();
    assert_eq!(retained.len(), 1);
    assert_eq!(
        coven_format::key::decode_key(&retained[0].row.key).unwrap(),
        [Value::Text("b".into())]
    );
    assert!(
        matches!(&retained[0].values, coven_format::loss::LossValues::Row(cells) if cells.contains_key("body") && !cells.contains_key("content"))
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
                crate::Migration::addition(1, "unique notes", UNIQUE_NOTES),
                crate::Migration::breaking(
                    2,
                    "rename body",
                    "ALTER TABLE notes RENAME COLUMN body TO content",
                    crate::OldWrites::Drop,
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    // The empty device also authored its schema migration. Compare snapshots
    // only after both devices have consumed the same writes.
    for write in records(&d) {
        a.receive(write.into()).await.unwrap();
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
    assert_eq!(decode(&frames(&d, Audience::Store).await).0.counts[4], 1);
    a.dismiss_lost_values(&a.lost_values().await.unwrap())
        .await
        .unwrap();
    assert_loaded_losses(&a, &d).await;
    assert_eq!(decode(&frames(&d, Audience::Store).await).0.counts[4], 0);
    assert_eq!(count(&d, "notes"), 2);
    for database in [a, b, c, d] {
        database.close().await.unwrap();
    }
}

#[tokio::test]
async fn snapshots_keep_the_written_reference_when_the_app_reads_null() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let migrations = || {
        vec![crate::Migration::addition_with(
            1,
            "references",
            move |sql| {
                sql.execute_batch("CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE SET NULL)")?;
                Ok(())
            },
        )]
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
    sql(&a, "INSERT INTO parents VALUES('parent')")
        .await
        .unwrap();
    b.receive(records(&a).remove(0).into()).await.unwrap();
    sql(&b, "INSERT INTO children VALUES('child','parent')")
        .await
        .unwrap();
    sql(&a, "DELETE FROM parents WHERE id='parent'")
        .await
        .unwrap();
    b.receive(records(&a).remove(1).into()).await.unwrap();
    let displayed: Option<String> = b
        .read(|sql| Ok(sql.query_row("SELECT parent FROM children", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(displayed, None);
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
        assert_eq!(written.value, Value::Text("parent".into()));
        assert_eq!(written.parents.values().next().unwrap().generation, 1);
    }
    for database in [a, b] {
        database.close().await.unwrap();
    }
}

/// Compare snapshots after both devices have consumed the target's marker.
async fn consume_marker(source: &Database, target: &Database) {
    for write in records(target) {
        source.receive(write.into()).await.unwrap();
    }
}

#[tokio::test]
async fn excluded_snapshots_keep_only_undismissed_cells_and_rows() {
    use coven_format::dismissal::Dismissal;
    for (operation, lost_dismissal) in ["insert", "update", "delete"]
        .into_iter()
        .flat_map(|operation| [false, true].map(|lost| (operation, lost)))
    {
        let ids = SequentialIds::new();
        let source_store = TestStore::with_ids(&ids);
        let receiver_store = TestStore::with_ids(&ids);
        let source = source_store.schema(notes(), NOTES).await.unwrap();
        let receiver = version_two(&receiver_store, operation != "insert").await;
        sql(
            &source,
            "INSERT INTO notes VALUES('a','excluded','body'),('b','excluded too','body')",
        )
        .await
        .unwrap();
        if operation != "insert" {
            receiver
                .receive(records(&source)[0].clone().into())
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
        let excluded = records(&source).pop().unwrap();
        receiver.exclude_later_writes(2).await;
        receiver.receive(excluded.clone().into()).await.unwrap();
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
        // An old-schema dismissal is excluded too; a migrated one applies.
        if !lost_dismissal {
            dismissal.header.schema_version = 2;
        }
        receiver.receive(dismissal.into()).await.unwrap();
        let (header, snapshot) = decode(&frames(&receiver, Audience::Store).await);
        assert_eq!(header.counts[4], 2);
        let rows: Vec<_> = snapshot
            .into_iter()
            .filter_map(|r| match r {
                SnapshotRecord::Loss(loss) => Some(loss),
                _ => None,
            })
            .collect();
        assert_eq!(rows.len(), 2);
        let coven_format::loss::LossValues::Row(cells) = &rows[0].values else {
            panic!("row loss")
        };
        assert_eq!(
            cells.keys().map(String::as_str).collect::<Vec<_>>(),
            if operation == "update" {
                vec!["body"]
            } else {
                vec!["body", "id"]
            }
        );
        assert!(cells
            .values()
            .all(|cell| cell.write == excluded.header.position));
        assert_eq!(rows[1].row, excluded.parts[0].rows[1].row);
        let target_store = TestStore::with_ids(&ids);
        let target = version_two(&target_store, operation != "insert").await;
        consume_marker(&receiver, &target).await;
        assert_loaded_losses(&receiver, &target).await;
        receiver
            .dismiss_lost_values(&receiver.lost_values().await.unwrap())
            .await
            .unwrap();
        let (header, snapshot) = decode(&frames(&receiver, Audience::Store).await);
        assert_eq!(header.counts[4], 0);
        assert!(!snapshot
            .iter()
            .any(|record| matches!(record, SnapshotRecord::Loss(_))));
        assert_loaded_losses(&receiver, &target).await;
        target.close().await.unwrap();
        source.close().await.unwrap();
        receiver.close().await.unwrap();
    }
}

pub(crate) async fn assert_loaded_losses(source: &Database, target: &Database) {
    let snapshot = frames(source, Audience::Store).await;
    target
        .load_snapshots::<crate::pending::tests::Record, _, std::io::Cursor<Vec<u8>>>(
            crate::tests::reload(
                source,
                vec![snapshot.input()],
                vec![],
                Arc::new(crate::test_utils::TestAuthorship),
            ),
        )
        .await
        .unwrap();
    assert_eq!(frames(target, Audience::Store).await, snapshot);
    let loaded = target.lost_values().await.unwrap();
    let expected = source.lost_values().await.unwrap();
    assert_eq!(loaded.len(), expected.len());
    assert!(expected.iter().all(|loss| loaded.contains(loss)));
}

#[tokio::test]
async fn excluded_deletions_without_old_values_keep_distinct_write_identities() {
    use coven_format::loss::{LossCause, LossValues};
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let target_store = TestStore::with_ids(&ids);
    let source = source_store.schema(notes(), NOTES).await.unwrap();
    let receiver = version_two(&receiver_store, true).await;
    let target = version_two(&target_store, true).await;
    sql(&source, "INSERT INTO notes VALUES('n','value','body')")
        .await
        .unwrap();
    receiver
        .receive(records(&source)[0].clone().into())
        .await
        .unwrap();
    sql(&source, "DELETE FROM notes").await.unwrap();
    let mut deletion = records(&source).pop().unwrap();
    deletion.parts[0].rows[0].old.clear();
    receiver.exclude_later_writes(2).await;
    receiver.receive(deletion.clone().into()).await.unwrap();
    let first = deletion.header.position;
    // Excluded writes do not run merge: another deletion at the same generation
    // remains a distinct loss even when neither carries an old cell value.
    deletion.header.position.number += 1;
    deletion.header.timestamp = Timestamp::new(
        deletion.header.timestamp.milliseconds() + 1,
        0,
        deletion.header.position.device,
    )
    .unwrap();
    receiver.receive(deletion.clone().into()).await.unwrap();
    let (header, snapshot) = decode(&frames(&receiver, Audience::Store).await);
    assert_eq!(header.counts[4], 2);
    let writes: Vec<_> = snapshot
        .into_iter()
        .filter_map(|record| match record {
            SnapshotRecord::Loss(loss) => {
                assert_eq!(loss.values, LossValues::Row(BTreeMap::new()));
                let LossCause::Excluded { write, .. } = loss.cause else {
                    panic!("excluded loss")
                };
                Some(write)
            }
            _ => None,
        })
        .collect();
    assert_eq!(writes, [first, deletion.header.position]);
    consume_marker(&receiver, &target).await;
    assert_loaded_losses(&receiver, &target).await;
    for database in [source, receiver, target] {
        database.close().await.unwrap();
    }
}
