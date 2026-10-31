use super::*;
use crate::{tests::TestStore, FileColumn, RowIdentity, SyncedTable};
use coven_format::write::RowChange;

#[tokio::test]
async fn overwritten_and_deleted_coordinates_protect_their_stored_files() {
    let store = TestStore::new();
    let db = store.schema(vec![files_table()], FILES).await.unwrap();
    let file = FileId(uuid::Uuid::from_u128(7));
    for operation in [
        Operation::Update(BTreeMap::from([
            (
                "coordinates".into(),
                coven_format::test_utils::column(Value::Text("device 1".into())),
            ),
            ("hash".into(), coven_format::test_utils::column(Value::Null)),
        ])),
        Operation::Delete,
    ] {
        let mut write = coven_format::test_utils::write();
        let change = &mut write.parts[0].rows[0];
        change.row.table = "coven_files".into();
        let update = matches!(operation, Operation::Update(_));
        change.change.operation = operation;
        change.old = BTreeMap::from([(
            "coordinates".into(),
            Value::Text(format!("file 1 {file} {}", "ab".repeat(32))),
        )]);
        if update {
            change.old.insert("hash".into(), Value::Blob(vec![3; 32]));
        }
        change.sources.setters.clear();
        let encoder = coven_format::write_stream::WriteEncoder::new(&write).unwrap();
        let header =
            coven_format::write_stream::WriteHeaderFrame::decode(encoder.header_frame()).unwrap();
        let bytes: Vec<u8> = encoder
            .part_chunks(0)
            .unwrap()
            .flat_map(Result::unwrap)
            .collect();
        let references = db
            .write_file_references(
                DownloadedWriteStream {
                    signer: crate::test_utils::signer(),
                    header,
                    parts: vec![DownloadedPartStream::Opened(std::io::Cursor::new(bytes))],
                    authorship: crate::test_utils::TestAuthorship::write(&write),
                },
                BTreeSet::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            references,
            FileEvidence::Complete(BTreeSet::from([(DeviceId(1), file)]))
        );
    }
    // A skipped part proves nothing and is named by the caller with its key,
    // so the same bytes wait for that key; the stream's skipped parts must
    // agree with the ones named. A schema this version cannot interpret
    // proves no file absent either.
    let write = coven_format::test_utils::write();
    let encoder = coven_format::write_stream::WriteEncoder::new(&write).unwrap();
    let header =
        coven_format::write_stream::WriteHeaderFrame::decode(encoder.header_frame()).unwrap();
    let part = (
        write.parts[0].audience.clone(),
        coven_foundation::id_source::KeyId(uuid::Uuid::from_u128(3)),
    );
    for named in [BTreeSet::new(), BTreeSet::from([part.clone()])] {
        let skipped = db
            .write_file_references(
                DownloadedWriteStream::<std::io::Cursor<Vec<u8>>> {
                    signer: crate::test_utils::signer(),
                    header: header.clone(),
                    parts: vec![DownloadedPartStream::Skipped],
                    authorship: crate::test_utils::TestAuthorship::write(&write),
                },
                named.clone(),
            )
            .await;
        if named.is_empty() {
            assert!(matches!(
                skipped,
                Err(DbError::Snapshot(crate::SnapshotError::Inconsistent(_)))
            ));
        } else {
            assert_eq!(
                skipped.unwrap(),
                FileEvidence::Unproven(UnprovenFiles::Parts(named))
            );
        }
    }
    let mut newer = write.clone();
    newer.header.schema_version = 7;
    let encoder = coven_format::write_stream::WriteEncoder::new(&newer).unwrap();
    let header =
        coven_format::write_stream::WriteHeaderFrame::decode(encoder.header_frame()).unwrap();
    let unsupported = db
        .write_file_references(
            DownloadedWriteStream::<std::io::Cursor<Vec<u8>>> {
                signer: crate::test_utils::signer(),
                header,
                parts: vec![DownloadedPartStream::Skipped],
                authorship: crate::test_utils::TestAuthorship::write(&newer),
            },
            BTreeSet::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        unsupported,
        FileEvidence::Unproven(UnprovenFiles::Schema(7))
    );
}

pub(crate) const FILES: &str = "CREATE TABLE t(id TEXT NOT NULL PRIMARY KEY,file TEXT)";

pub(crate) fn files_table() -> SyncedTable {
    SyncedTable::new("t", RowIdentity::SharedKey).file_column(FileColumn::new("file"))
}

/// The one file's record id, which row `row` of `t` references.
const RECORD: FileId = FileId(uuid::Uuid::from_u128(
    0x0000_0000_0000_4000_8000_0000_0000_0046,
));

/// Coordinates naming `object`, stored by `device`.
pub(crate) fn stored(device: u64, object: FileId, key: &str) -> Value {
    Value::Text(format!("file {device} {object} {}", key.repeat(32)))
}

pub(crate) fn insert(table: &str, key: &str, columns: Vec<(&str, Value)>) -> RowChange {
    use coven_merge::{Audience, Change, RowId};
    RowChange {
        row: RowId {
            table: table.into(),
            key: coven_format::key::encode_key(&[Value::Text(key.into())]).unwrap(),
            audience: Audience::Store,
        },
        creator: None,
        execution: Default::default(),
        change: Change {
            generation: 0,
            operation: Operation::Insert(
                columns
                    .into_iter()
                    .map(|(name, value)| (name.into(), coven_format::test_utils::column(value)))
                    .collect(),
            ),
        },
        old: BTreeMap::new(),
        sources: coven_format::write_sources::WriteSources {
            generation: None,
            setters: BTreeMap::new(),
        },
    }
}

/// A version-1 write by `device` at `time`: the record of `RECORD` at these
/// coordinates, and the row referencing it.
pub(crate) fn write(
    device: u64,
    time: u64,
    coordinates: Value,
) -> coven_format::write::WriteRecord {
    use coven_format::write::{WriteDisposition, WriteHeader, WritePart, WriteRecord};
    use coven_merge::{Audience, Timestamp, WriteId};
    let id = WriteId {
        device: DeviceId(device),
        number: 1,
    };
    let record = RECORD.0.to_string();
    WriteRecord {
        header: WriteHeader {
            position: id,
            timestamp: Timestamp::new(time, 0, id.device).unwrap(),
            read_through: 0,
            schema_version: 1,
            disposition: WriteDisposition::Apply,
        },
        parts: vec![WritePart {
            audience: Audience::Store,
            dismissals: vec![],
            executions: vec![],
            rows: vec![
                insert(
                    "coven_files",
                    &record,
                    vec![
                        ("id", Value::Text(record.clone())),
                        (
                            "audience",
                            Value::Text(crate::write_encoding::audience_text(&Audience::Store)),
                        ),
                        ("size", Value::Integer(1)),
                        ("hash", Value::Blob(vec![3; 32])),
                        ("coordinates", coordinates),
                        ("namespace", Value::Text("files".into())),
                    ],
                ),
                insert(
                    "t",
                    "row",
                    vec![
                        ("id", Value::Text("row".into())),
                        ("file", Value::Text(record)),
                    ],
                ),
            ],
        }],
    }
}

/// Device 2 and device 3 each, unaware of the other, record `RECORD` over
/// bytes of their own: the record shows device 3's, and device 2's
/// coordinates are lost. Returns device 2's object.
async fn file_loss_fixture() -> (TestStore, Database, FileId) {
    let store = TestStore::new();
    let db = store.schema(vec![files_table()], FILES).await.unwrap();
    let file = FileId(uuid::Uuid::from_u128(7));
    for record in [
        write(2, 1, stored(2, file, "ab")),
        write(3, 2, stored(3, FileId(uuid::Uuid::from_u128(8)), "cd")),
    ] {
        assert_eq!(
            db.receive(record.into()).await.unwrap(),
            crate::ApplyOutcome::Applied
        );
    }
    assert!(!db.lost_values().await.unwrap().is_empty());
    (store, db, file)
}

/// Nothing restores coven's records, so a record's lost coordinates keep
/// nothing: the bytes they name become a candidate at once.
#[tokio::test]
async fn a_records_lost_coordinates_protect_nothing() {
    let (_store, db, file) = file_loss_fixture().await;
    let references = db.retained_files().await.unwrap().references;
    assert!(!references.contains(&(DeviceId(2), file)));
    assert!(references.contains(&(DeviceId(3), FileId(uuid::Uuid::from_u128(8)))));
    assert_eq!(
        db.file_candidates(candidates_of(DeviceId(2)))
            .await
            .unwrap(),
        vec![FileCandidate {
            device: DeviceId(2),
            file,
            protected: false,
            since: Slot(2),
        }]
    );
}

/// A candidate counts from the applied slot at which its path last lost its
/// protection: slot 2, where device 3's record replaced device 2's bytes,
/// and slot 4 once device 4 named those bytes again and device 5 replaced
/// them again, though nothing settled the candidate in between. A reload
/// skips the slots it replaces, so it counts every candidate from its
/// target (§17.10).
#[tokio::test]
async fn a_candidate_counts_from_the_slot_its_path_last_lost_its_protection() {
    let (_store, db, file) = file_loss_fixture().await;
    let candidate = |db: &Database| {
        let db = db.clone();
        async move {
            db.file_candidates(candidates_of(DeviceId(2)))
                .await
                .unwrap()
                .into_iter()
                .map(|candidate| (candidate.protected, candidate.since))
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(candidate(&db).await, [(false, Slot(2))]);
    let later = [
        write(4, 3, stored(2, file, "ab")),
        write(5, 4, stored(5, FileId(uuid::Uuid::from_u128(9)), "ef")),
        write(6, 5, stored(6, FileId(uuid::Uuid::from_u128(10)), "01")),
    ];
    db.receive(later[0].clone().into()).await.unwrap();
    assert_eq!(candidate(&db).await, [(true, Slot(2))]);
    db.receive(later[1].clone().into()).await.unwrap();
    assert_eq!(candidate(&db).await, [(false, Slot(4))]);

    let source_store = TestStore::new();
    let source = source_store
        .schema(vec![files_table()], FILES)
        .await
        .unwrap();
    for record in [
        write(2, 1, stored(2, file, "ab")),
        write(3, 2, stored(3, FileId(uuid::Uuid::from_u128(8)), "cd")),
    ]
    .into_iter()
    .chain(later)
    {
        source.receive(record.into()).await.unwrap();
    }
    let snapshot =
        crate::snapshot_write::tests::frames(&source, coven_merge::Audience::Store).await;
    crate::snapshot_write::tests::load_one(
        &db,
        crate::snapshot_write::tests::id(coven_merge::Audience::Store),
        snapshot.prefix.clone(),
        std::io::Cursor::new(snapshot.concat()),
    )
    .await
    .unwrap();
    assert_eq!(candidate(&db).await, [(false, Slot(5))]);
}

/// A write a raised schema excludes leaves its record as a frozen loss,
/// which keeps the path its coordinates name, without a displayed change.
async fn frozen_loss(
    store: &TestStore,
    db: Database,
    tables: Vec<SyncedTable>,
    change: &'static str,
) -> (Database, FileId) {
    let db = migrate(store, db, tables, change).await;
    let file = FileId(uuid::Uuid::from_u128(9));
    db.receive(write(4, 3, stored(4, file, "ef")).into())
        .await
        .unwrap();
    (db, file)
}

#[tokio::test]
async fn a_schema_loss_protects_its_file_without_a_displayed_row_change() {
    let (store, db, _) = file_loss_fixture().await;
    let (db, file) = frozen_loss(
        &store,
        db,
        vec![files_table()],
        "CREATE INDEX t_file ON t(file)",
    )
    .await;
    assert!(db
        .retained_files()
        .await
        .unwrap()
        .references
        .contains(&(DeviceId(4), file)));
}

#[tokio::test]
async fn checked_snapshots_protect_files_named_only_by_losses() {
    let (store, db, _) = file_loss_fixture().await;
    let (db, file) = frozen_loss(
        &store,
        db,
        vec![files_table()],
        "CREATE INDEX t_file ON t(file)",
    )
    .await;
    let snapshot = crate::snapshot_write::tests::frames(&db, coven_merge::Audience::Store).await;
    let (id, prefix, input, author) = snapshot.input();
    let references = crate::snapshot_write::tests::validate_one(
        &db,
        id,
        prefix,
        input,
        author,
        std::sync::Arc::new(crate::test_utils::TestAuthorship),
    )
    .await
    .unwrap();
    assert!(references.contains(&(DeviceId(4), file)));
}

#[tokio::test]
async fn frozen_losses_keep_file_paths_after_the_apps_declaration_is_removed() {
    let (store, db, _) = file_loss_fixture().await;
    let (db, file) = frozen_loss(&store, db, Vec::new(), "DROP TABLE t").await;
    assert!(db
        .retained_files()
        .await
        .unwrap()
        .references
        .contains(&(DeviceId(4), file)));
    let snapshot = crate::snapshot_write::tests::frames(&db, coven_merge::Audience::Store).await;
    let (id, prefix, input, author) = snapshot.input();
    assert!(crate::snapshot_write::tests::validate_one(
        &db,
        id,
        prefix,
        input,
        author,
        std::sync::Arc::new(crate::test_utils::TestAuthorship)
    )
    .await
    .unwrap()
    .contains(&(DeviceId(4), file)));
    // The published loss remains until the dismissal acquires a slot.
    db.dismiss_lost_values(&db.lost_values().await.unwrap())
        .await
        .unwrap();
    let row_references: i64 = db.inspect_writer(|db| {
        db.query_row(
            "SELECT count(*) FROM _coven_row_file_refs WHERE device=?1 AND file=?2",
            (DeviceId(4).0.to_be_bytes(), file.0.as_bytes()),
            |r| r.get(0),
        )
        .unwrap()
    });
    assert_eq!(row_references, 0);
    assert!(db
        .retained_files()
        .await
        .unwrap()
        .references
        .contains(&(DeviceId(4), file)));
}

/// Deleting the last row referencing the record deletes the record in the
/// same write; until that write publishes, its record of the deleted
/// coordinates keeps the path, whatever the app's schema does next.
#[tokio::test]
async fn a_queued_record_delete_keeps_its_path_after_the_apps_declaration_is_removed() {
    let (store, db, _) = file_loss_fixture().await;
    db.write(|sql| {
        sql.execute("DELETE FROM t", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let expected = (DeviceId(3), FileId(uuid::Uuid::from_u128(8)));
    let rows: i64 = db.inspect_writer(|db| {
        db.query_row("SELECT count(*) FROM coven_files", [], |r| r.get(0))
            .unwrap()
    });
    assert_eq!(rows, 0);
    assert!(db
        .retained_files()
        .await
        .unwrap()
        .references
        .contains(&expected));
    db.close().await.unwrap();
    let db = store
        .builder(
            vec![],
            vec![
                crate::Migration::addition(1, "schema", FILES),
                crate::Migration::breaking(
                    2,
                    "remove files",
                    "DROP TABLE t",
                    crate::OldWrites::Drop,
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    assert!(db
        .retained_files()
        .await
        .unwrap()
        .references
        .contains(&expected));
}

/// Reopen after the app runs breaking version 2, `change`, with an explicit
/// drop choice, under a kept raise to 2 covering what was consumed: a later
/// version-1 write is excluded (§18.6).
async fn migrate(
    store: &TestStore,
    db: Database,
    tables: Vec<SyncedTable>,
    change: &'static str,
) -> Database {
    db.close().await.unwrap();
    let db = store
        .builder(
            tables,
            vec![
                crate::Migration::addition(1, "schema", FILES),
                crate::Migration::breaking(2, "change", change, crate::OldWrites::Drop),
            ],
        )
        .open()
        .await
        .unwrap();
    db.exclude_later_writes(2).await;
    db
}

fn candidates_of(device: DeviceId) -> BTreeSet<DeviceId> {
    BTreeSet::from([device])
}

/// §17.10: a frozen loss keeps its file path protected. Dismissing it leaves
/// the counted base's protection until the dismissal is applied. Applying it
/// releases the last local reference and makes the path a candidate in the same
/// transaction, without any listing of storage.
#[tokio::test]
async fn a_frozen_loss_protects_its_file_until_its_last_local_reference_goes() {
    let (store, db, _) = file_loss_fixture().await;
    let (db, file) = frozen_loss(
        &store,
        db,
        vec![files_table()],
        "CREATE INDEX t_file ON t(file)",
    )
    .await;
    let device = DeviceId(4);
    let frozen: i64 = db.inspect_writer(|db| {
        db.query_row("SELECT count(*) FROM _coven_lost WHERE frozen=1", [], |r| {
            r.get(0)
        })
        .unwrap()
    });
    // The excluded write's two row changes, each kept whole: the record and
    // its row. The record's earlier merge losses stay live, as the migration
    // left the record as it was.
    assert_eq!(frozen, 2);
    assert!(db
        .file_candidates(candidates_of(device))
        .await
        .unwrap()
        .is_empty());
    db.dismiss_lost_values(&db.lost_values().await.unwrap())
        .await
        .unwrap();
    assert!(db
        .file_candidates(candidates_of(device))
        .await
        .unwrap()
        .is_empty());
    // The dismissal is published in slot 4, which the candidate counts from.
    crate::tests::publish(&db).await.unwrap();
    assert_eq!(
        db.file_candidates(candidates_of(device)).await.unwrap(),
        vec![FileCandidate {
            device,
            file,
            protected: false,
            since: Slot(4),
        }]
    );
}

/// A protected candidate is settled only while local data still protects it,
/// so a release racing the settlement keeps the candidate; a deleted one goes.
#[tokio::test]
async fn protected_candidates_settle_only_while_still_protected() {
    let (_store, db, _) = file_loss_fixture().await;
    let named = (DeviceId(3), FileId(uuid::Uuid::from_u128(8)));
    let orphan = (DeviceId(3), FileId(uuid::Uuid::from_u128(80)));
    db.add_file_candidates(BTreeSet::from([named, orphan]))
        .await
        .unwrap();
    assert_eq!(
        db.file_candidates(candidates_of(DeviceId(3)))
            .await
            .unwrap(),
        vec![
            FileCandidate {
                device: named.0,
                file: named.1,
                protected: true,
                since: Slot(2),
            },
            FileCandidate {
                device: orphan.0,
                file: orphan.1,
                protected: false,
                since: Slot(2),
            },
        ]
    );
    // Other uploaders' candidates are not read.
    assert!(db
        .file_candidates(candidates_of(DeviceId(5)))
        .await
        .unwrap()
        .is_empty());
    db.settle_file_candidates(vec![named, orphan], Vec::new())
        .await
        .unwrap();
    let left = db
        .file_candidates(candidates_of(DeviceId(3)))
        .await
        .unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!((left[0].device, left[0].file), orphan);
    db.settle_file_candidates(Vec::new(), vec![orphan])
        .await
        .unwrap();
    assert!(db
        .file_candidates(candidates_of(DeviceId(3)))
        .await
        .unwrap()
        .is_empty());
}

/// A listing is recorded only against a counted removal of its device; its
/// record and candidates survive reopening, and a later listing adds what
/// the device uploaded since.
#[tokio::test]
async fn a_scan_needs_a_counted_removal_and_survives_reopening() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let uploader = DeviceId(5);
    TestStore::register(&db, uploader, 1);
    let removal = crate::tests::remove_device(&db, uploader).await;
    let found = FileId(uuid::Uuid::from_u128(11));
    let later = FileId(uuid::Uuid::from_u128(12));
    assert!(!db
        .complete_file_scan(uploader, coven_merge::Slot(removal.0 + 1), [found].into())
        .await
        .unwrap());
    assert_eq!(db.file_scan(uploader).await.unwrap(), None);
    assert!(db
        .file_candidates(candidates_of(uploader))
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .complete_file_scan(uploader, removal, [found].into())
        .await
        .unwrap());
    assert_eq!(db.file_scan(uploader).await.unwrap(), Some(removal));
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(db.file_scan(uploader).await.unwrap(), Some(removal));
    assert!(db
        .complete_file_scan(uploader, removal, [found, later].into())
        .await
        .unwrap());
    assert_eq!(
        db.file_candidates(candidates_of(uploader))
            .await
            .unwrap()
            .into_iter()
            .map(|candidate| candidate.file)
            .collect::<Vec<_>>(),
        [found, later]
    );
}
