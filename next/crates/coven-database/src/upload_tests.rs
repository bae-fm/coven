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

pub(crate) async fn sealed(db: &Database) -> (WriteId, Vec<u8>) {
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

pub(crate) async fn attempt(db: &Database, bytes: Vec<u8>) -> Result<Option<WriteId>, DbError> {
    db.prepare_write_upload(move |_, _, _, emit| {
        for chunk in bytes.chunks(CHUNK_SIZE) {
            emit(chunk)?;
        }
        Ok(())
    })
    .await
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
        attempt(&db, vec![1]).await,
        Err(DbError::UploadLength { .. })
    ));
    assert_eq!(count(&db, "coven_upload_seals"), 0);
    assert_eq!(attempt(&db, bytes.clone()).await.unwrap(), Some(first));
    assert_eq!(attempt(&db, vec![88]).await.unwrap(), Some(first));
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
    assert!(attempt(&db, bytes.clone()).await.is_err());
    assert_eq!(count(&db, "coven_upload_seals"), 0);
    assert_eq!(candidate(&db, 19).await, (id, bytes.clone()));
    db.inspect_writer(|db| db.batch("DROP TRIGGER coven_fail").unwrap());
    let (a, b) = tokio::join!(
        attempt(&db, bytes.clone()),
        attempt(&db, vec![99; bytes.len()])
    );
    assert_eq!(a.unwrap(), Some(id));
    assert_eq!(b.unwrap(), Some(id));
    let kept = sealed(&db).await;
    assert_eq!(kept.0, id);
    assert!(kept.1 == bytes || kept.1 == vec![99; bytes.len()]);
    db.prepare_write_upload(|_, _, _, _| panic!("retry must keep the first seal"))
        .await
        .unwrap();
    assert_eq!(sealed(&db).await, kept);
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
    assert_eq!(attempt(&db, bytes.clone()).await.unwrap(), Some(id));
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
        runtime.block_on(attempt(&db, vec![42])).unwrap(),
        Some(expected.0)
    );
    assert_eq!(runtime.block_on(sealed(&db)), expected);
    assert!(runtime.block_on(db.upload_succeeded(expected.0)).unwrap());
    assert_eq!(count(&db, "coven_uploads"), 0);
    assert_eq!(count(&db, "coven_upload_seals"), 0);
    runtime.block_on(db.close()).unwrap();
}

#[tokio::test]
async fn a_panicking_sealer_rolls_back_without_poisoning_the_writer() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('1','title','body')")
        .await
        .unwrap();
    let clone = db.clone();
    assert!(tokio::spawn(async move {
        clone
            .prepare_write_upload(|_, _, _, emit| {
                emit(&[1, 2, 3])?;
                panic!("sealer panicked")
            })
            .await
    })
    .await
    .unwrap_err()
    .is_panic());
    assert_eq!(count(&db, "coven_upload_seals"), 0);
    let (id, bytes) = candidate(&db, 17).await;
    assert_eq!(attempt(&db, bytes.clone()).await.unwrap(), Some(id));
    assert_eq!(sealed(&db).await, (id, bytes));
    db.close().await.unwrap();
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
    let (_, bytes) = runtime.block_on(candidate(&db, 71));
    runtime.block_on(attempt(&db, bytes)).unwrap();
    std::process::exit(86);
}

#[test]
fn upload_completion_does_not_retain_the_object_in_sqlite_observation() {
    const CHILD: &str = "COVEN_UPLOAD_MEMORY_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "upload::tests::upload_completion_does_not_retain_the_object_in_sqlite_observation",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    // SAFETY: the isolated child has not opened SQLite on any thread yet.
    assert_eq!(
        unsafe { rusqlite::ffi::sqlite3_config(rusqlite::ffi::SQLITE_CONFIG_MEMSTATUS, 1) },
        rusqlite::ffi::SQLITE_OK
    );
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        db.write(|sql| {
            let body = "x".repeat(16 * 1024);
            for i in 0..1024 {
                sql.execute(
                    "INSERT INTO notes VALUES(?1,'title',?2)",
                    (i.to_string(), &body),
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
        let id = db
            .prepare_write_upload(|_, _, upload, emit| {
                let WaitingUpload::Plaintext {
                    header,
                    header_frame,
                    ..
                } = upload
                else {
                    panic!("plaintext")
                };
                let lengths: Vec<_> = header.parts.iter().map(|p| p.plaintext_length).collect();
                let mut remaining =
                    coven_format::sealed_write::sealed_length(header_frame.len(), &lengths)?
                        as usize;
                let chunk = vec![9; CHUNK_SIZE];
                while remaining > 0 {
                    let length = remaining.min(chunk.len());
                    emit(&chunk[..length])?;
                    remaining -= length;
                }
                Ok(())
            })
            .await
            .unwrap()
            .unwrap();
        // This subprocess runs one test, so SQLite's global high-water counter
        // measures only these queue operations and their connection caches.
        let baseline = unsafe {
            rusqlite::ffi::sqlite3_memory_highwater(1);
            rusqlite::ffi::sqlite3_memory_used()
        };
        db.keep_write_upload_session(id, vec![1; 32]).await.unwrap();
        db.upload_succeeded(id).await.unwrap();
        let peak = unsafe { rusqlite::ffi::sqlite3_memory_highwater(0) };
        assert!(baseline > 0, "SQLite memory accounting must be enabled");
        assert!(
            peak - baseline < 8 * 1024 * 1024,
            "SQLite retained {} bytes above its existing caches",
            peak - baseline
        );
        db.close().await.unwrap();
    });
}
