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
    db.prepare_upload(move |_, _, header| {
        let key = coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([byte; 16]));
        Ok(WriteObjectPrefix {
            format: coven_format::FormatVersion::CURRENT,
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
    assert!(db.read_oldest_upload(|_| Ok::<_, DbError>(())).await.unwrap().is_none());
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
    sql(&db, "INSERT INTO notes VALUES('later','title','body')").await.unwrap();
    db.inspect_writer(|db| db.internal_execute("UPDATE _coven_uploads SET record=x'' WHERE rowid=(SELECT max(rowid) FROM _coven_uploads)", []).unwrap());
    let streamed = db
        .read_oldest_upload(|upload| {
            let WaitingUpload { header, header_frame, parts, keys } = upload;
            assert!(keys.is_none());
            assert_eq!(WriteHeaderFrame::decode(&header_frame).unwrap(), header);
            let mut decoded = Vec::new();
            for (part, bytes) in parts {
                let mut decoder = PartDecoder::new(part.clone()).unwrap();
                let mut rows = Vec::new();
                let mut executions = Vec::new();
                let mut dismissals = Vec::new();
                for chunk in bytes {
                    for frame in decoder.chunk(&chunk?).unwrap() {
                        match frame {
                            WriteFrame::Change(row) => rows.push(row),
                            WriteFrame::Execution(execution) => executions.push(execution),
                            WriteFrame::Dismissal(dismissal) => dismissals.push(dismissal),
                        }
                    }
                }
                decoder.finish().unwrap();
                decoded.push(WritePart { executions, audience: part.audience, rows, dismissals });
            }
            Ok::<_, DbError>(WriteRecord { header: header.header, parts: decoded })
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(streamed, original);
    assert_eq!(count(&db, "_coven_uploads"), 2);
    db.close().await.unwrap();
}

async fn landed(db: &Database, through: WriteId) {
    db.begin_claim(crate::ClaimWork::Writes(through.number), |_| {
        Ok(crate::ClaimAttempt {
            path: coven_format::path::ObjectPath::slot(
                crate::tests::store_id(),
                1u64.try_into().unwrap(),
            ),
            bytes: Box::new(std::io::Cursor::new(vec![1, 2])),
        })
    })
    .await
    .unwrap();
    db.settle_claim(crate::ClaimRead::Kept(Some(Box::new(std::io::Cursor::new(vec![1, 2])))))
        .await
        .unwrap();
    assert!(db.finish_claim(None).await.is_err());
    for record in records(db).into_iter().filter(|r| r.header.position.number <= through.number) {
        assert_eq!(db.receive(record.into()).await.unwrap(), crate::ApplyOutcome::Applied);
    }
}

#[tokio::test]
async fn only_confirmed_claims_remove_their_batch_atomically() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('1','title','body')").await.unwrap();
    let first = records(&db)[0].header.position;
    landed(&db, first).await;
    sql(&db, "INSERT INTO notes VALUES('2','title','body')").await.unwrap();
    db.inspect_writer(|db| {
        db.fail_at("_coven_fail", "AFTER DELETE ON _coven_uploads", "failed removal")
    });
    assert!(db.finish_claim(None).await.is_err());
    assert_eq!(count(&db, "_coven_uploads"), 2);
    assert_eq!(
        db.current_claim().await.unwrap().unwrap().settlement,
        crate::ClaimSettlement::Landed
    );
    db.inspect_writer(|db| db.batch("DROP TRIGGER _coven_fail").unwrap());
    db.finish_claim(None).await.unwrap();
    assert_eq!(count(&db, "_coven_uploads"), 1);
    assert_eq!(selected(&db).await.0.number, first.number + 1);
    assert_eq!(count(&db, "notes"), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn key_selection_rolls_back_and_competing_attempts_keep_one_choice() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('1','title','body')").await.unwrap();
    let original = records(&db);
    let id = original[0].header.position;
    db.inspect_writer(|db| {
        db.fail_at("_coven_fail", "AFTER UPDATE OF sealing_keys ON _coven_uploads", "failed keys")
    });
    assert!(matches!(attempt(&db, 19).await, Err(DbError::Sqlite(_))));
    assert_eq!(selected(&db).await, (id, None));
    assert_eq!(records(&db), original);
    db.inspect_writer(|db| db.batch("DROP TRIGGER _coven_fail").unwrap());
    assert!(matches!(
        db.prepare_upload(|_, _, _| Err(DbError::StoreClosed)).await,
        Err(DbError::StoreClosed)
    ));
    assert_eq!(selected(&db).await, (id, None));
    assert!(db
        .prepare_upload(|_, _, _| Ok(WriteObjectPrefix {
            format: coven_format::FormatVersion::CURRENT,
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
    db.prepare_upload(|_, _, _| panic!("retry must keep the first keys")).await.unwrap();
    assert_eq!(selected(&db).await, kept);
    assert_eq!(records(&db), original);
    db.close().await.unwrap();
}

#[tokio::test]
async fn consumer_failure_and_panic_release_the_reader() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('1','title','body')").await.unwrap();
    for _ in 0..5 {
        assert!(matches!(
            db.read_oldest_upload(|_| Err::<(), _>("sink failed")).await,
            Err(UploadReadError::Consumer("sink failed"))
        ));
        let cloned = db.clone();
        let panic = tokio::spawn(async move {
            cloned.read_oldest_upload(|_| -> Result<(), DbError> { panic!("sink panicked") }).await
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
    runtime.block_on(sql(&db, "INSERT INTO notes VALUES('1','title','body')")).unwrap();
    let original = records(&db);
    let expected = original[0].header.position;
    runtime.block_on(db.close()).unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "upload::tests::crashing_upload_attempt", "--nocapture"])
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
    assert_eq!(kept.1.as_ref().unwrap().format, coven_format::FormatVersion::V1);
    assert_eq!(kept.1.as_ref().unwrap().store_key.0.as_bytes(), &[71; 16]);
    assert_eq!(records(&db), original);
    assert_eq!(count(&db, "_coven_uploads"), 1);
    assert_eq!(runtime.block_on(attempt(&db, 42)).unwrap(), Some(expected));
    assert_eq!(runtime.block_on(selected(&db)), kept);
    runtime.block_on(landed(&db, expected));
    runtime.block_on(db.finish_claim(None)).unwrap();
    assert_eq!(count(&db, "_coven_uploads"), 0);
    runtime.block_on(db.close()).unwrap();
}

#[tokio::test]
async fn a_panicking_key_selector_releases_the_writer() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('1','title','body')").await.unwrap();
    let clone = db.clone();
    assert!(tokio::spawn(async move {
        clone.prepare_upload(|_, _, _| panic!("selector panicked")).await
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
    let layout = coven_foundation::files::StoreDirectories::new(
        path.parent().unwrap().parent().unwrap().parent().unwrap().to_owned(),
    );
    let store = coven_foundation::id_source::StoreId(
        uuid::Uuid::parse_str(&std::env::var("COVEN_UPLOAD_STORE").unwrap()).unwrap(),
    );
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let db = runtime
        .block_on(
            crate::DatabaseBuilder::new(layout.store_dir(&store))
                .synced_tables(notes())
                .migrations(vec![crate::Migration::addition(1, "schema", NOTES)])
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
        assert_eq!(
            output.status.code(),
            Some(86),
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
                sql.execute("INSERT INTO notes VALUES(?1,'title',?2)", (i.to_string(), &body))?;
            }
            Ok(())
        })
        .await
        .unwrap();
        let id = attempt(&db, 9).await.unwrap().unwrap();
        landed(&db, id).await;
        // This subprocess runs one test, so SQLite's global high-water counter
        // measures only these queue operations and their connection caches.
        let baseline = unsafe {
            rusqlite::ffi::sqlite3_memory_highwater(1);
            rusqlite::ffi::sqlite3_memory_used()
        };
        db.finish_claim(None).await.unwrap();
        let peak = unsafe { rusqlite::ffi::sqlite3_memory_highwater(0) };
        assert!(baseline > 0, "SQLite memory accounting must be enabled");
        assert!(
            peak - baseline < 8 * 1024 * 1024,
            "SQLite retained {} bytes above its existing caches",
            peak - baseline
        );
        db.close().await.unwrap();
    });
    std::process::exit(86);
}

#[tokio::test]
async fn a_captured_batch_keeps_original_keys_and_excludes_later_commits() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    for id in ["a", "b", "later"] {
        db.write(move |sql| {
            sql.execute("INSERT INTO notes VALUES(?1,'title','body')", [id])?;
            Ok(())
        })
        .await
        .unwrap();
    }
    let through = records(&db)[1].header.position.number;
    let prefix = |header: &WriteHeaderFrame| {
        let key = coven_foundation::id_source::KeyId(uuid::Uuid::from_u128(29));
        WriteObjectPrefix {
            format: coven_format::FormatVersion::CURRENT,
            store_key: key,
            part_keys: vec![key; header.parts.len()],
        }
    };
    assert!(db
        .prepare_uploads::<DbError, _>(through, move |header| {
            if header.header.position.number == through {
                Err(DbError::TransactionEnded)
            } else {
                Ok(prefix(header))
            }
        })
        .await
        .is_err());
    assert!(selected(&db).await.1.is_none());
    let headers =
        db.prepare_uploads::<DbError, _>(through, move |header| Ok(prefix(header))).await.unwrap();
    assert_eq!(headers.len(), 2);
    db.prepare_uploads::<DbError, _>(through, |_| panic!("attempted originals retain their keys"))
        .await
        .unwrap();
    let output = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = output.clone();
    db.read_uploads::<DbError, _>(through, move |upload| {
        let mut bytes = upload.header_frame;
        for (_, chunks) in upload.parts {
            for chunk in chunks {
                bytes.extend(chunk?);
            }
        }
        observed
            .lock()
            .unwrap()
            .push((coven_format::write_stream::decode_plaintext(&bytes)?, upload.keys));
        Ok(())
    })
    .await
    .unwrap();
    let selected = std::mem::take(&mut *output.lock().unwrap());
    assert_eq!(selected.len(), 2);
    for ((record, keys), original) in selected.iter().zip(records(&db)) {
        assert_eq!(record, &original);
        assert_eq!(
            keys.as_ref().unwrap().store_key,
            coven_foundation::id_source::KeyId(uuid::Uuid::from_u128(29))
        );
    }
    assert_eq!(count(&db, "_coven_uploads"), 3);
    db.close().await.unwrap();
}
