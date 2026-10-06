use super::*;
use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::Database;
use coven_format::dismissal::WriteFrame;
use coven_format::write::{WritePart, WriteRecord};
use coven_format::write_stream::{PartDecoder, WriteEncoder};

async fn candidate(db: &Database, byte: u8) -> (WriteId, Vec<u8>) {
    db.read_oldest_upload(move |upload| {
        let WaitingUpload::Plaintext {
            header,
            header_frame,
            ..
        } = upload
        else {
            panic!("already sealed")
        };
        let length = coven_format::sealed_write::sealed_length(
            header_frame.len(),
            &header
                .parts
                .iter()
                .map(|part| part.plaintext_length)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        Ok::<_, DbError>((header.header.position, vec![byte; length as usize]))
    })
    .await
    .unwrap()
    .unwrap()
}

async fn sealed(db: &Database) -> (WriteId, Vec<u8>) {
    db.read_oldest_upload(|upload| {
        let WaitingUpload::Sealed { write, bytes } = upload else {
            panic!("not sealed")
        };
        let mut all = Vec::new();
        for chunk in bytes {
            let chunk = chunk?;
            assert!(chunk.len() <= CHUNK_SIZE);
            all.extend(chunk);
        }
        Ok::<_, DbError>((write, all))
    })
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn reads_only_the_oldest_write_as_header_and_audience_streams() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    assert!(db
        .read_oldest_upload(|_| Ok::<_, DbError>(()))
        .await
        .unwrap()
        .is_none());
    db.write(|sql| {
        for i in 0..2000 {
            sql.execute(
                "INSERT INTO notes VALUES(?1,'title',?2)",
                (i.to_string(), "body".repeat(100)),
            )?;
        }
        Ok(())
    })
    .await
    .unwrap();
    let original = records(&db).remove(0);
    sql(&db, "INSERT INTO notes VALUES('later','title','body')")
        .await
        .unwrap();
    db.inspect_writer(|db| db.internal_execute("UPDATE coven_uploads SET record=x'' WHERE rowid=(SELECT max(rowid) FROM coven_uploads)", []).unwrap());
    let streamed = db
        .read_oldest_upload(|upload| {
            let WaitingUpload::Plaintext {
                header,
                header_frame,
                parts,
            } = upload
            else {
                panic!("already sealed")
            };
            assert_eq!(WriteHeaderFrame::decode(&header_frame).unwrap(), header);
            let mut decoded = Vec::new();
            for (part, bytes) in parts {
                let mut decoder = PartDecoder::new(part.clone()).unwrap();
                let mut rows = Vec::new();
                let mut dismissals = Vec::new();
                for chunk in bytes {
                    for frame in decoder.chunk(&chunk?).unwrap() {
                        match frame {
                            WriteFrame::Change(row) => rows.push(row),
                            WriteFrame::Dismissal(dismissal) => dismissals.push(dismissal),
                        }
                    }
                }
                decoder.finish().unwrap();
                decoded.push(WritePart {
                    audience: part.audience,
                    rows,
                    dismissals,
                });
            }
            Ok::<_, DbError>(WriteRecord {
                header: header.header,
                parts: decoded,
            })
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(streamed, original);
    assert_eq!(count(&db, "coven_uploads"), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn only_success_removes_a_write_and_its_seal_and_advances_the_queue() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('1','title','body')")
        .await
        .unwrap();
    sql(&db, "INSERT INTO notes VALUES('2','title','body')")
        .await
        .unwrap();
    let ids: Vec<_> = records(&db).iter().map(|r| r.header.position).collect();
    let (first, bytes) = candidate(&db, 19).await;
    assert_eq!(first, ids[0]);
    assert!(matches!(
        db.upload_succeeded(first).await,
        Err(DbError::UploadNotSealed { .. })
    ));
    assert!(matches!(
        db.keep_upload_sealed(ids[1], bytes.clone()).await,
        Err(DbError::UploadNotOldest { .. })
    ));
    assert!(matches!(
        db.keep_upload_sealed(first, vec![1]).await,
        Err(DbError::UploadLength { .. })
    ));
    assert_eq!(count(&db, "coven_upload_seals"), 0);
    assert_eq!(
        db.keep_upload_sealed(first, bytes.clone()).await.unwrap(),
        bytes
    );
    assert_eq!(db.keep_upload_sealed(first, vec![88]).await.unwrap(), bytes);
    assert_eq!(sealed(&db).await, (first, bytes.clone()));
    assert_eq!(count(&db, "coven_uploads"), 2);
    assert!(matches!(
        db.upload_succeeded(ids[1]).await,
        Err(DbError::UploadNotOldest { .. })
    ));
    db.inspect_writer(|db| db.batch("CREATE TRIGGER coven_fail AFTER DELETE ON coven_upload_seals BEGIN SELECT RAISE(ABORT,'failed removal'); END").unwrap());
    assert!(db.upload_succeeded(first).await.is_err());
    assert_eq!(sealed(&db).await, (first, bytes));
    assert_eq!(count(&db, "coven_uploads"), 2);
    db.inspect_writer(|db| db.batch("DROP TRIGGER coven_fail").unwrap());
    assert!(db.upload_succeeded(first).await.unwrap());
    assert!(!db.upload_succeeded(first).await.unwrap());
    assert_eq!(count(&db, "coven_upload_seals"), 0);
    assert_eq!(candidate(&db, 7).await.0, ids[1]);
    assert_eq!(count(&db, "notes"), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn seal_failure_rolls_back_and_competing_attempts_get_the_same_bytes() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('1','title','body')")
        .await
        .unwrap();
    let (id, bytes) = candidate(&db, 19).await;
    db.inspect_writer(|db| db.batch("CREATE TRIGGER coven_fail AFTER INSERT ON coven_upload_seals BEGIN SELECT RAISE(ABORT,'failed seal'); END").unwrap());
    assert!(db.keep_upload_sealed(id, bytes.clone()).await.is_err());
    assert_eq!(count(&db, "coven_upload_seals"), 0);
    assert_eq!(candidate(&db, 19).await, (id, bytes.clone()));
    db.inspect_writer(|db| db.batch("DROP TRIGGER coven_fail").unwrap());
    let (a, b) = tokio::join!(
        db.keep_upload_sealed(id, bytes.clone()),
        db.keep_upload_sealed(id, vec![99; bytes.len()])
    );
    let a = a.unwrap();
    assert_eq!(a, b.unwrap());
    assert_eq!(sealed(&db).await, (id, a));
    db.close().await.unwrap();
}

#[tokio::test]
async fn plaintext_and_sealed_values_fit_when_their_combined_size_exceeds_sqlites_limit() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db,"WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<100) INSERT INTO notes SELECT 'import-'||i,'Imported note','' FROM n").await.unwrap();
    let record = records(&db).remove(0);
    let plaintext = WriteEncoder::new(&record).unwrap().plaintext_length();
    let (id, bytes) = candidate(&db, 17).await;
    let maximum = bytes.len() as i32 + 24;
    assert!(plaintext + bytes.len() as u64 > maximum as u64);
    let old = db.inspect_writer(|db| db.set_value_limit(maximum));
    assert_eq!(
        db.keep_upload_sealed(id, bytes.clone()).await.unwrap(),
        bytes
    );
    db.inspect_writer(|db| db.set_value_limit(old));
    assert_eq!(sealed(&db).await, (id, bytes));
    assert_eq!(records(&db), vec![record]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn consumer_failure_and_panic_release_the_reader() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('1','title','body')")
        .await
        .unwrap();
    for _ in 0..5 {
        assert!(matches!(
            db.read_oldest_upload(|_| Err::<(), _>("sink failed")).await,
            Err(UploadReadError::Consumer("sink failed"))
        ));
        let cloned = db.clone();
        let panic = tokio::spawn(async move {
            cloned
                .read_oldest_upload(|_| -> Result<(), DbError> { panic!("sink panicked") })
                .await
        })
        .await
        .unwrap_err();
        assert!(panic.is_panic());
    }
    assert_eq!(candidate(&db, 17).await.0.number, 1);
    db.close().await.unwrap();
}

#[test]
fn a_crash_after_keeping_the_seal_resends_identical_bytes() {
    let store = TestStore::new();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let db = runtime.block_on(store.schema(notes(), NOTES)).unwrap();
    runtime
        .block_on(sql(&db, "INSERT INTO notes VALUES('1','title','body')"))
        .unwrap();
    let expected = runtime.block_on(candidate(&db, 71));
    runtime.block_on(db.close()).unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "upload::tests::crashing_upload_attempt",
            "--nocapture",
        ])
        .env("COVEN_UPLOAD_DATABASE", store.database_path())
        .env("COVEN_UPLOAD_STORE", store.id().to_string())
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(86),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let db = runtime.block_on(store.schema(notes(), NOTES)).unwrap();
    assert_eq!(runtime.block_on(sealed(&db)), expected);
    assert_eq!(count(&db, "coven_uploads"), 1);
    assert_eq!(
        runtime
            .block_on(db.keep_upload_sealed(expected.0, vec![42]))
            .unwrap(),
        expected.1
    );
    assert!(runtime.block_on(db.upload_succeeded(expected.0)).unwrap());
    assert_eq!(count(&db, "coven_uploads"), 0);
    assert_eq!(count(&db, "coven_upload_seals"), 0);
    runtime.block_on(db.close()).unwrap();
}

#[test]
fn crashing_upload_attempt() {
    let Ok(path) = std::env::var("COVEN_UPLOAD_DATABASE") else {
        return;
    };
    let path = std::path::Path::new(&path);
    let layout = coven_foundation::files::StoreLayout::new(
        path.parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_owned(),
    );
    let store = coven_foundation::id_source::StoreId(
        uuid::Uuid::parse_str(&std::env::var("COVEN_UPLOAD_STORE").unwrap()).unwrap(),
    );
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let db = runtime
        .block_on(
            crate::DatabaseBuilder::new(layout.store_dir(&store))
                .synced_tables(notes())
                .migrations(vec![crate::Migration::sql(1, "schema", NOTES)])
                .coven_migration_policy(crate::CovenMigrationPolicy::ApplyPending)
                .open(),
        )
        .unwrap();
    let (id, bytes) = runtime.block_on(candidate(&db, 71));
    runtime.block_on(db.keep_upload_sealed(id, bytes)).unwrap();
    std::process::exit(86);
}
