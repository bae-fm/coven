use super::*;
use crate::{tests::TestStore, CacheFill, FileDecl, Provenance, RowIdentity, SyncedTable};

#[tokio::test]
async fn overwritten_and_deleted_old_values_protect_their_uploaded_files() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("t", RowIdentity::SharedKey).carries_files(
                FileDecl::new("files", Provenance::AppProvided, CacheFill::CacheLazy)
                    .with_location_column("x"),
            )],
            "CREATE TABLE t(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,x TEXT)",
        )
        .await
        .unwrap();
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
            .write_file_references(DownloadedWriteStream {
                header,
                parts: vec![DownloadedPartStream::Opened(std::io::Cursor::new(bytes))],
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(references, BTreeSet::from([(DeviceId(1), file)]));
    }
}

async fn file_loss_fixture() -> (
    TestStore,
    Database,
    coven_format::write::WriteRecord,
    FileId,
) {
    use coven_format::{
        value::{EntryPositions, WritePositions},
        write::{WriteDisposition, WriteHeader, WritePart, WriteRecord},
    };
    use coven_merge::{Audience, Change, RowId, Timestamp, WriteId};
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("t", RowIdentity::SharedKey).carries_files(
                FileDecl::new("files", Provenance::AppProvided, CacheFill::CacheLazy)
                    .with_location_column("x"),
            )],
            "CREATE TABLE t(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,x TEXT)",
        )
        .await
        .unwrap();
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
            store_log_read: EntryPositions(vec![]),
            position: id,
            timestamp: Timestamp::new(1, 0, id.device).unwrap(),
            had_read: WritePositions(vec![]),
            schema_version: 1,
            disposition: WriteDisposition::Apply,
        },
        parts: vec![WritePart {
            audience: Audience::Store,
            dismissals: vec![],
            rows: vec![RowChange {
                row,
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
        db.apply_downloaded(record.clone().into()).await.unwrap(),
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
        db.apply_downloaded(record.into()).await.unwrap(),
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
    let (_store, db, mut record, _) = file_loss_fixture().await;
    let device = DeviceId(4);
    let file = FileId(uuid::Uuid::from_u128(9));
    record.header.position = coven_merge::WriteId { device, number: 1 };
    record.header.timestamp = coven_merge::Timestamp::new(3, 0, device).unwrap();
    record.header.disposition = coven_format::write::WriteDisposition::Lost(1);
    let Operation::Insert(columns) = &mut record.parts[0].rows[0].change.operation else {
        panic!()
    };
    columns.insert(
        "x".into(),
        coven_format::test_utils::column(Value::Text(format!("file 4 {file} {}", "ef".repeat(32)))),
    );
    assert_eq!(
        db.apply_downloaded(record.into()).await.unwrap(),
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
async fn reversible_inputs_protect_removed_file_losses_until_the_input_is_released() {
    let (store, db, record, file) = file_loss_fixture().await;
    db.inspect_writer_schema(|sql, schema| {
        sql.transaction(|sql| {
            crate::entry_inputs::original(sql, schema, &coven_merge::Audience::Store)
        })
        .unwrap()
    });
    db.write(|sql| {
        sql.execute("DELETE FROM t", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(db.lost_values().await.unwrap().is_empty());
    let expected = (record.header.position.device, file);
    assert!(db
        .retained_files()
        .await
        .unwrap()
        .references
        .contains(&expected));
    db.close().await.unwrap();
    let db = store
        .schema(
            vec![SyncedTable::new("t", RowIdentity::SharedKey).carries_files(
                FileDecl::new("files", Provenance::AppProvided, CacheFill::CacheLazy)
                    .with_location_column("x"),
            )],
            "CREATE TABLE t(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,x TEXT)",
        )
        .await
        .unwrap();
    assert!(db
        .retained_files()
        .await
        .unwrap()
        .references
        .contains(&expected));
    db.inspect_writer(|sql| {
        sql.transaction(|sql| crate::entry_inputs::forget(sql, &coven_merge::Audience::Store, None))
            .unwrap()
    });
    assert!(!db
        .retained_files()
        .await
        .unwrap()
        .references
        .contains(&expected));
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
    let (id, prefix, input) = snapshot.input();
    let references = db.validate_snapshot(id, prefix, input).await.unwrap();
    assert!(references.contains(&(record.header.position.device, file)));
}

#[tokio::test]
async fn frozen_losses_keep_file_paths_after_their_declaration_is_removed() {
    let (_store, db, mut record, file) = file_loss_fixture().await;
    record.header.position = coven_merge::WriteId {
        device: DeviceId(4),
        number: 1,
    };
    record.header.timestamp = coven_merge::Timestamp::new(3, 0, DeviceId(4)).unwrap();
    record.header.disposition = coven_format::write::WriteDisposition::Lost(1);
    record.parts[0].rows[0].row.table = "removed_files".into();
    let Operation::Insert(columns) = &mut record.parts[0].rows[0].change.operation else {
        panic!()
    };
    columns.insert(
        "x".into(),
        coven_format::test_utils::column(Value::Text(format!("file 4 {file} {}", "ef".repeat(32)))),
    );
    db.apply_downloaded(record.into()).await.unwrap();
    assert!(db
        .retained_files()
        .await
        .unwrap()
        .references
        .contains(&(DeviceId(4), file)));
    let snapshot = crate::snapshot_write::tests::frames(&db, coven_merge::Audience::Store).await;
    let (id, prefix, input) = snapshot.input();
    assert!(db
        .validate_snapshot(id, prefix, input)
        .await
        .unwrap()
        .contains(&(DeviceId(4), file)));
    db.dismiss_lost_values(&db.lost_values().await.unwrap())
        .await
        .unwrap();
    assert!(!db
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
                crate::Migration::sql(
                    1,
                    "files",
                    "CREATE TABLE t(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,x TEXT)",
                ),
                crate::Migration::sql(2, "remove files", "DROP TABLE t"),
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
