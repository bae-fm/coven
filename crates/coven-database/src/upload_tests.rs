use super::*;
use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::Database;
use coven_format::dismissal::WriteFrame;
use coven_format::write::{WritePart, WriteRecord};
use coven_format::write_stream::PartDecoder;

pub(crate) async fn selected(db: &Database) -> (WriteId, Option<WriteObjectPrefix>) {
    db.read_oldest_upload(|upload| Ok::<_, DbError>((upload.header.header.position, upload.keys)))
        .await
        .unwrap()
        .unwrap()
}

pub(crate) async fn attempt(db: &Database, byte: u8) -> Result<Option<WriteId>, DbError> {
    db.prepare_write_upload(move |_, _, header| {
        let key = coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([byte; 16]));
        Ok(WriteObjectPrefix {
            store_key: key,
            part_keys: vec![key; header.parts.len()],
        })
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
    db.inspect_writer(|db| db.internal_execute("UPDATE _coven_uploads SET record=x'' WHERE rowid=(SELECT max(rowid) FROM _coven_uploads)", []).unwrap());
    let streamed = db
        .read_oldest_upload(|upload| {
            let WaitingUpload {
                header,
                header_frame,
                parts,
                keys,
            } = upload;
            assert!(keys.is_none());
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
    assert_eq!(count(&db, "_coven_uploads"), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn only_success_removes_a_write_and_its_session_and_advances_the_queue() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('1','title','body')")
        .await
        .unwrap();
    sql(&db, "INSERT INTO notes VALUES('2','title','body')")
        .await
        .unwrap();
    let ids: Vec<_> = records(&db).iter().map(|r| r.header.position).collect();
    let first = ids[0];
    assert!(matches!(
        db.upload_succeeded(first).await,
        Err(DbError::UploadNotAttempted { .. })
    ));
    assert_eq!(attempt(&db, 19).await.unwrap(), Some(first));
    let fixed = selected(&db).await;
    db.keep_write_upload_session(first, vec![1, 2])
        .await
        .unwrap();
    assert!(matches!(
        db.upload_succeeded(ids[1]).await,
        Err(DbError::UploadNotOldest { .. })
    ));
    db.inspect_writer(|db| db.batch("CREATE TRIGGER _coven_fail AFTER DELETE ON _coven_uploads BEGIN SELECT RAISE(ABORT,'failed removal'); END").unwrap());
    assert!(db.upload_succeeded(first).await.is_err());
    assert_eq!(selected(&db).await, fixed);
    assert_eq!(
        db.write_upload_session(first).await.unwrap(),
        Some(vec![1, 2])
    );
    assert_eq!(count(&db, "_coven_uploads"), 2);
    db.inspect_writer(|db| db.batch("DROP TRIGGER _coven_fail").unwrap());
    assert!(db.upload_succeeded(first).await.unwrap());
    assert!(!db.upload_succeeded(first).await.unwrap());
    assert!(db.write_upload_session(first).await.unwrap().is_none());
    assert_eq!(selected(&db).await, (ids[1], None));
    assert_eq!(count(&db, "notes"), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn key_selection_rolls_back_and_competing_attempts_keep_one_choice() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('1','title','body')")
        .await
        .unwrap();
    let original = records(&db);
    let id = original[0].header.position;
    db.inspect_writer(|db| db.batch("CREATE TRIGGER _coven_fail AFTER UPDATE OF sealing_keys ON _coven_uploads BEGIN SELECT RAISE(ABORT,'failed keys'); END").unwrap());
    assert!(attempt(&db, 19).await.is_err());
    assert_eq!(selected(&db).await, (id, None));
    assert_eq!(records(&db), original);
    db.inspect_writer(|db| db.batch("DROP TRIGGER _coven_fail").unwrap());
    assert!(db
        .prepare_write_upload(|_, _, _| Err(DbError::StoreClosed))
        .await
        .is_err());
    assert_eq!(selected(&db).await, (id, None));
    assert!(db
        .prepare_write_upload(|_, _, _| Ok(WriteObjectPrefix {
            store_key: coven_foundation::id_source::KeyId(uuid::Uuid::nil()),
            part_keys: vec![],
        }))
        .await
        .is_err());
    assert_eq!(selected(&db).await, (id, None));
    let (a, b) = tokio::join!(attempt(&db, 19), attempt(&db, 99));
    assert_eq!(a.unwrap(), Some(id));
    assert_eq!(b.unwrap(), Some(id));
    let kept = selected(&db).await;
    let key = kept.1.as_ref().unwrap().store_key;
    assert!([19, 99].iter().any(|byte| key.0.as_bytes() == &[*byte; 16]));
    db.prepare_write_upload(|_, _, _| panic!("retry must keep the first keys"))
        .await
        .unwrap();
    assert_eq!(selected(&db).await, kept);
    assert_eq!(records(&db), original);
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
    assert_eq!(selected(&db).await.0.number, 1);
    db.close().await.unwrap();
}

#[test]
fn a_crash_after_recording_an_attempt_keeps_its_plaintext_and_keys() {
    let store = TestStore::new();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let db = runtime.block_on(store.schema(notes(), NOTES)).unwrap();
    runtime
        .block_on(sql(&db, "INSERT INTO notes VALUES('1','title','body')"))
        .unwrap();
    let original = records(&db);
    let expected = original[0].header.position;
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
    let kept = runtime.block_on(selected(&db));
    assert_eq!(kept.0, expected);
    assert_eq!(kept.1.as_ref().unwrap().store_key.0.as_bytes(), &[71; 16]);
    assert_eq!(records(&db), original);
    assert_eq!(count(&db, "_coven_uploads"), 1);
    assert_eq!(runtime.block_on(attempt(&db, 42)).unwrap(), Some(expected));
    assert_eq!(runtime.block_on(selected(&db)), kept);
    assert!(runtime.block_on(db.upload_succeeded(expected)).unwrap());
    assert_eq!(count(&db, "_coven_uploads"), 0);
    runtime.block_on(db.close()).unwrap();
}

#[tokio::test]
async fn a_panicking_key_selector_releases_the_writer() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('1','title','body')")
        .await
        .unwrap();
    let clone = db.clone();
    assert!(tokio::spawn(async move {
        clone
            .prepare_write_upload(|_, _, _| panic!("selector panicked"))
            .await
    })
    .await
    .unwrap_err()
    .is_panic());
    let (id, keys) = selected(&db).await;
    assert!(keys.is_none());
    assert_eq!(attempt(&db, 17).await.unwrap(), Some(id));
    assert!(selected(&db).await.1.is_some());
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
    runtime.block_on(attempt(&db, 71)).unwrap();
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
        let id = attempt(&db, 9).await.unwrap().unwrap();
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
