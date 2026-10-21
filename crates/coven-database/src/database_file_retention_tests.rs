use super::*;
use crate::{tests::TestStore, FileDecl, Provenance, RowIdentity, SyncedTable};

#[tokio::test]
async fn overwritten_and_deleted_old_values_protect_their_uploaded_files() {
    let store = TestStore::new();
    let db = store.schema(vec![files_table()], FILES).await.unwrap();
    let file = FileId(uuid::Uuid::from_u128(7));
    for operation in [
        Operation::Update(BTreeMap::from([(
            "x".into(),
            coven_format::test_utils::column(Value::Null),
        )])),
        Operation::Delete,
    ] {
        let mut write = coven_format::test_utils::write();
        let change = &mut write.parts[0].rows[0];
        change.change.operation = operation;
        change.old = BTreeMap::from([(
            "x".into(),
            Value::Text(format!("file 1 {file} {}", "ab".repeat(32))),
        )]);
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

const FILES: &str = "CREATE TABLE t(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,x TEXT)";

fn files_table() -> SyncedTable {
    SyncedTable::new("t", RowIdentity::SharedKey).carries_files(
        FileDecl::new("files", 1 << 30, Provenance::AppProvided).with_location_column("x"),
    )
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

async fn file_loss_fixture() -> (
    TestStore,
    Database,
    coven_format::write::WriteRecord,
    FileId,
) {
    use coven_format::write::{WriteDisposition, WriteHeader, WritePart, WriteRecord};
    use coven_merge::{Audience, Change, RowId, Timestamp, WriteId};
    let store = TestStore::new();
    let db = store.schema(vec![files_table()], FILES).await.unwrap();
    let file = FileId(uuid::Uuid::from_u128(7));
    let id = WriteId {
        device: DeviceId(2),
        number: 1,
    };
    let location = Value::Text(format!("file 2 {file} {}", "ab".repeat(32)));
    let row = RowId {
        table: "t".into(),
        key: coven_format::key::encode_key(&[Value::Text("row".into())]).unwrap(),
        audience: Audience::Store,
    };
    let mut record = WriteRecord {
        header: WriteHeader {
            position: id,
            timestamp: Timestamp::new(1, 0, id.device).unwrap(),
            read_through: 0,
            schema_version: 1,
            disposition: WriteDisposition::Apply,
        },
        parts: vec![WritePart {
            audience: Audience::Store,
            dismissals: vec![],
            executions: vec![],
            rows: vec![RowChange {
                row,
                creator: None,
                execution: Default::default(),
                change: Change {
                    generation: 0,
                    operation: Operation::Insert(BTreeMap::from([
                        (
                            "id".into(),
                            coven_format::test_utils::column(Value::Text("row".into())),
                        ),
                        (
                            "size".into(),
                            coven_format::test_utils::column(Value::Integer(1)),
                        ),
                        (
                            "hash".into(),
                            coven_format::test_utils::column(Value::Blob(vec![3; 32])),
                        ),
                        (
                            "x".into(),
                            coven_format::test_utils::column(location.clone()),
                        ),
                    ])),
                },
                old: BTreeMap::new(),
                sources: coven_format::write_sources::WriteSources {
                    generation: None,
                    setters: BTreeMap::new(),
                },
            }],
        }],
    };
    assert_eq!(
        db.receive(record.clone().into()).await.unwrap(),
        crate::ApplyOutcome::Applied
    );
    let original = record.clone();
    record.header.position = WriteId {
        device: DeviceId(3),
        number: 1,
    };
    record.header.timestamp = Timestamp::new(2, 0, DeviceId(3)).unwrap();
    let Operation::Insert(columns) = &mut record.parts[0].rows[0].change.operation else {
        panic!()
    };
    columns.insert(
        "x".into(),
        coven_format::test_utils::column(Value::Text(format!(
            "file 3 {} {}",
            FileId(uuid::Uuid::from_u128(8)),
            "cd".repeat(32)
        ))),
    );
    assert_eq!(
        db.receive(record.into()).await.unwrap(),
        crate::ApplyOutcome::Applied
    );
    assert!(!db.lost_values().await.unwrap().is_empty());
    (store, db, original, file)
}

#[tokio::test]
async fn local_losses_protect_files_after_the_downloaded_write_leaves_storage() {
    let (_store, db, record, file) = file_loss_fixture().await;
    assert!(db
        .retained_files()
        .await
        .unwrap()
        .references
        .contains(&(record.header.position.device, file)));
}

#[tokio::test]
async fn a_schema_loss_protects_its_file_without_a_displayed_row_change() {
    let (store, db, mut record, _) = file_loss_fixture().await;
    let db = migrate(&store, db, vec![files_table()], "CREATE INDEX t_x ON t(x)").await;
    let device = DeviceId(4);
    let file = FileId(uuid::Uuid::from_u128(9));
    record.header.position = coven_merge::WriteId { device, number: 1 };
    record.header.timestamp = coven_merge::Timestamp::new(3, 0, device).unwrap();
    let Operation::Insert(columns) = &mut record.parts[0].rows[0].change.operation else {
        panic!()
    };
    columns.insert(
        "x".into(),
        coven_format::test_utils::column(Value::Text(format!("file 4 {file} {}", "ef".repeat(32)))),
    );
    assert_eq!(
        db.receive(record.into()).await.unwrap(),
        crate::ApplyOutcome::Applied
    );
    assert!(db
        .retained_files()
        .await
        .unwrap()
        .references
        .contains(&(device, file)));
}

#[tokio::test]
async fn a_dismissed_file_loss_releases_its_local_protection() {
    let (_store, db, record, file) = file_loss_fixture().await;
    let expected = (record.header.position.device, file);
    assert!(db
        .retained_files()
        .await
        .unwrap()
        .references
        .contains(&expected));
    db.dismiss_lost_values(&db.lost_values().await.unwrap())
        .await
        .unwrap();
    crate::tests::publish(&db).await.unwrap();
    assert!(!db
        .retained_files()
        .await
        .unwrap()
        .references
        .contains(&expected));
}

#[tokio::test]
async fn checked_snapshots_protect_files_named_only_by_losses() {
    let (_store, db, record, file) = file_loss_fixture().await;
    let snapshot = crate::snapshot_write::tests::frames(&db, coven_merge::Audience::Store).await;
    let (id, prefix, input, author) = snapshot.input();
    let references = db
        .validate_snapshot(
            id,
            prefix,
            input,
            author,
            std::sync::Arc::new(crate::test_utils::TestAuthorship),
        )
        .await
        .unwrap();
    assert!(references.contains(&(record.header.position.device, file)));
}

#[tokio::test]
async fn frozen_losses_keep_file_paths_after_their_declaration_is_removed() {
    let (store, db, mut record, file) = file_loss_fixture().await;
    let db = migrate(&store, db, Vec::new(), "DROP TABLE t").await;
    record.header.position = coven_merge::WriteId {
        device: DeviceId(4),
        number: 1,
    };
    record.header.timestamp = coven_merge::Timestamp::new(3, 0, DeviceId(4)).unwrap();
    let Operation::Insert(columns) = &mut record.parts[0].rows[0].change.operation else {
        panic!()
    };
    columns.insert(
        "x".into(),
        coven_format::test_utils::column(Value::Text(format!("file 4 {file} {}", "ef".repeat(32)))),
    );
    db.receive(record.into()).await.unwrap();
    assert!(db
        .retained_files()
        .await
        .unwrap()
        .references
        .contains(&(DeviceId(4), file)));
    let snapshot = crate::snapshot_write::tests::frames(&db, coven_merge::Audience::Store).await;
    let (id, prefix, input, author) = snapshot.input();
    assert!(db
        .validate_snapshot(
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

#[tokio::test]
async fn queued_original_file_references_survive_removing_their_declaration() {
    let (store, db, _, _) = file_loss_fixture().await;
    db.write(|sql| {
        sql.execute("DELETE FROM t", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let expected = (DeviceId(3), FileId(uuid::Uuid::from_u128(8)));
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
                crate::Migration::addition(
                    1,
                    "files",
                    "CREATE TABLE t(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,x TEXT)",
                ),
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

fn candidates_of(device: DeviceId) -> BTreeSet<DeviceId> {
    BTreeSet::from([device])
}

/// §17.8: a frozen loss keeps its file path protected. Dismissing it leaves
/// the counted base's protection until the dismissal is applied. Applying it
/// releases the last local reference and makes the path a candidate in the same
/// transaction, without any listing of storage.
#[tokio::test]
async fn a_frozen_loss_protects_its_file_until_its_last_local_reference_goes() {
    let (store, db, mut record, file) = file_loss_fixture().await;
    let db = migrate(&store, db, vec![files_table()], "CREATE INDEX t_x ON t(x)").await;
    let device = DeviceId(4);
    record.header.position = coven_merge::WriteId { device, number: 1 };
    record.header.timestamp = coven_merge::Timestamp::new(3, 0, device).unwrap();
    let Operation::Insert(columns) = &mut record.parts[0].rows[0].change.operation else {
        panic!()
    };
    columns.insert(
        "x".into(),
        coven_format::test_utils::column(Value::Text(format!("file 4 {file} {}", "ef".repeat(32)))),
    );
    db.receive(record.into()).await.unwrap();
    let frozen: i64 = db.inspect_writer(|db| {
        db.query_row("SELECT count(*) FROM _coven_lost WHERE frozen=1", [], |r| {
            r.get(0)
        })
        .unwrap()
    });
    assert_eq!(frozen, 1);
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
    crate::tests::publish(&db).await.unwrap();
    assert_eq!(
        db.file_candidates(candidates_of(device)).await.unwrap(),
        vec![FileCandidate {
            device,
            file,
            protected: false
        }]
    );
}

/// A protected candidate is settled only while local data still protects it,
/// so a release racing the settlement keeps the candidate; a deleted one goes.
#[tokio::test]
async fn protected_candidates_settle_only_while_still_protected() {
    let (_store, db, _, _) = file_loss_fixture().await;
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
                protected: true
            },
            FileCandidate {
                device: orphan.0,
                file: orphan.1,
                protected: false
            },
        ]
    );
    // Other uploaders' candidates are not read.
    assert!(db
        .file_candidates(candidates_of(DeviceId(2)))
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

/// A completed removal scan survives reopening; restored access invalidates it.
#[tokio::test]
async fn a_scan_stands_only_while_its_removal_is_kept() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let uploader = DeviceId(5);
    TestStore::register(&db, uploader, 1);
    let removal = crate::tests::remove_device(&db, uploader).await;
    let found = FileId(uuid::Uuid::from_u128(11));
    // As replay records the account needed again, or revoked.
    let set_access = |db: &Database, intended: bool| {
        db.inspect_writer(|sql| {
            sql.internal_execute(
                "INSERT INTO _coven_provider_access(account,intended,granted) VALUES('cat@example.com',?1,0)
                 ON CONFLICT(account) DO UPDATE SET intended=excluded.intended",
                [intended],
            )
            .unwrap();
        })
    };
    let account = || BTreeSet::from(["cat@example.com".to_owned()]);
    set_access(&db, true);
    assert!(!db
        .complete_file_scan(
            uploader,
            removal,
            BTreeSet::new(),
            account(),
            BTreeSet::new()
        )
        .await
        .unwrap());
    set_access(&db, false);
    assert_eq!(db.file_scan(uploader).await.unwrap(), None);
    assert!(db
        .complete_file_scan(
            uploader,
            removal,
            BTreeSet::from([found]),
            account(),
            BTreeSet::new()
        )
        .await
        .unwrap());
    assert_eq!(db.file_scan(uploader).await.unwrap(), Some(removal));
    assert_eq!(
        db.file_candidates(candidates_of(uploader)).await.unwrap(),
        vec![FileCandidate {
            device: uploader,
            file: found,
            protected: false
        }]
    );
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(db.file_scan(uploader).await.unwrap(), Some(removal));
    // Restored access forgets the scan in that transaction; the candidates
    // it found stay. Revoking again restores no scan.
    set_access(&db, true);
    assert_eq!(db.file_scan(uploader).await.unwrap(), None);
    set_access(&db, false);
    assert_eq!(db.file_scan(uploader).await.unwrap(), None);
    assert_eq!(
        db.file_candidates(candidates_of(uploader))
            .await
            .unwrap()
            .len(),
        1
    );
}

/// Cut-off needs this device's confirmation of every recorded access: each
/// account revoked with nothing outstanding and each S3 key deleted. Nothing
/// recorded confirms nothing.
#[tokio::test]
async fn access_is_taken_away_only_with_every_confirmation() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.inspect_writer(|sql| {
        for statement in [
            "INSERT INTO _coven_provider_access(account,intended,granted) VALUES('revoked@example.com',0,0)",
            "INSERT INTO _coven_provider_access(account,intended,granted) VALUES('granted@example.com',0,1)",
            "INSERT INTO _coven_provider_access(account,intended,granted) VALUES('unknown@example.com',0,NULL)",
            "INSERT INTO _coven_access_keys(access_key_id,operation) VALUES('deleted',NULL)",
            "INSERT INTO _coven_access_keys(access_key_id,operation) VALUES('pending',7)",
        ] {
            sql.internal_execute(statement, []).unwrap();
        }
    });
    let taken = |accounts: &[&str], keys: &[&str]| {
        let db = db.clone();
        let accounts = accounts.iter().map(|a| (*a).to_owned()).collect();
        let keys = keys.iter().map(|k| (*k).to_owned()).collect();
        async move { db.access_taken_away(accounts, keys).await.unwrap() }
    };
    assert!(!taken(&[], &[]).await);
    assert!(taken(&["revoked@example.com"], &["deleted"]).await);
    assert!(!taken(&["revoked@example.com", "granted@example.com"], &[]).await);
    assert!(!taken(&["unknown@example.com"], &[]).await);
    assert!(!taken(&["never@example.com"], &[]).await);
    assert!(!taken(&[], &["deleted", "pending"]).await);
    assert!(!taken(&[], &["unrecorded"]).await);
}
