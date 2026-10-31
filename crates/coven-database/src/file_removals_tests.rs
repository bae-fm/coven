use crate::{
    file_write::tests::{local_count, namespaces, open, owned_paths, stage, tables, SCHEMA},
    tests::TestStore,
    *,
};

/// A committed write returns its own result while a removal fails: the
/// removal stays recorded, and an app that retries on `Err` would apply its
/// change twice (§2.1, Appendix C.3). Opening reports the failure.
#[tokio::test]
async fn failed_deletions_remain_recorded_until_a_write_or_open_retries_them() {
    for reopen in [false, true] {
        let store = TestStore::new();
        let db = open(&store, Provenance::AppProvided).await;
        stage(&db, b"original".to_vec(), true).await.unwrap();
        let path = owned_paths(&store).remove(0);
        // A directory at the file's name makes removal fail on every platform.
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        // Upload completion releases the queue's copy.
        complete(&db).await.unwrap();
        // Later writes still commit and return their results.
        assert_eq!(db.write(|_| Ok(7)).await.unwrap(), 7);
        assert_eq!(local_count(&db, "files"), 1);
        assert_eq!(local_count(&db, "_coven_device_files"), 0);
        assert_eq!(local_count(&db, "_coven_file_removals"), 1);
        if reopen {
            db.close().await.unwrap();
            let error = store
                .builder(tables(), vec![Migration::addition(1, "files", SCHEMA)])
                .namespaces(namespaces(Provenance::AppProvided))
                .open()
                .await
                .err()
                .unwrap();
            assert!(matches!(
                error,
                CovenError::Database(DbError::FileCleanup { write: Ok(()), .. })
            ));
            store.assert_writer_unlocked();
        }
        std::fs::remove_dir(&path).unwrap();
        std::fs::write(&path, b"original").unwrap();
        let db = if reopen {
            open(&store, Provenance::AppProvided).await
        } else {
            db.write(|_| Ok(())).await.unwrap();
            db
        };
        assert_eq!(local_count(&db, "_coven_file_removals"), 0);
        assert!(owned_paths(&store).is_empty());
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn deleting_a_pending_record_can_fail_without_losing_the_retry() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    stage(&db, b"original".to_vec(), true).await.unwrap();
    db.inspect_writer(|sql| {
        sql.fail_at(
            "_coven_fail_cleanup",
            "BEFORE DELETE ON _coven_file_removals",
            "record stays",
        )
    });
    complete(&db).await.unwrap();
    assert_eq!(local_count(&db, "_coven_file_removals"), 1);
    assert!(owned_paths(&store).is_empty());
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER _coven_fail_cleanup").unwrap());
    db.write(|_| Ok(())).await.unwrap();
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    db.close().await.unwrap();
}

#[test]
fn process_crashes_keep_every_unclaimed_file_recorded_and_reopen_removes_it() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    for phase in ["stream", "transaction", "committed", "removed"] {
        let store = TestStore::new();
        let db = runtime.block_on(open(&store, Provenance::AppProvided));
        runtime
            .block_on(stage(&db, b"original".to_vec(), true))
            .unwrap();
        let original = owned_paths(&store).remove(0);
        runtime.block_on(db.close()).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "file_removals::tests::crashing_file_writer",
                "--nocapture",
            ])
            .env("COVEN_FILE_CRASH_PATH", store.database_path())
            .env("COVEN_FILE_CRASH_STORE", store.id().to_string())
            .env("COVEN_FILE_CRASH_PHASE", phase)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let raw = rusqlite::Connection::open(store.database_path()).unwrap();
        let pending: i64 = raw
            .query_row("SELECT count(*) FROM _coven_file_removals", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(pending, 1, "{phase}");
        // Completion released the copy in the last two phases; no row changed.
        let attached = matches!(phase, "stream" | "transaction");
        assert_eq!(
            raw.query_row("SELECT count(*) FROM files", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(original.exists(), phase != "removed");
        // This fault trigger intentionally survives the child; remove the test
        // fault before opening exercises the recorded deletion again.
        raw.execute_batch("DROP TRIGGER IF EXISTS _coven_crash")
            .unwrap();
        drop(raw);
        let db = runtime.block_on(open(&store, Provenance::AppProvided));
        assert_eq!(local_count(&db, "_coven_file_removals"), 0);
        assert_eq!(owned_paths(&store).len(), usize::from(attached));
        if attached {
            assert_eq!(std::fs::read(&original).unwrap(), b"original");
            assert_eq!(file_size(&db), 8);
        }
        runtime.block_on(db.close()).unwrap();
    }
}

#[test]
fn crashing_file_writer() {
    let Ok(path) = std::env::var("COVEN_FILE_CRASH_PATH") else {
        return;
    };
    let path = std::path::Path::new(&path);
    let layout = coven_foundation::files::StoreDirectories::new(
        path.parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_owned(),
    );
    let store = coven_foundation::id_source::StoreId(
        uuid::Uuid::parse_str(&std::env::var("COVEN_FILE_CRASH_STORE").unwrap()).unwrap(),
    );
    let phase = std::env::var("COVEN_FILE_CRASH_PHASE").unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let db = runtime
        .block_on(
            DatabaseBuilder::new(layout.store_dir(&store))
                .synced_tables(tables())
                .namespaces(namespaces(Provenance::AppProvided))
                .migrations(vec![Migration::addition(1, "files", SCHEMA)])
                .open(),
        )
        .unwrap();
    if phase == "stream" || phase == "transaction" {
        let stream = phase == "stream";
        runtime
            .block_on(db.write_with_files::<_, _, _, _, crate::DbError>(
                move |batch| {
                    let source = if stream {
                        FileSource::Stream(Box::pin(CrashingReader(false)))
                    } else {
                        b"replacement".to_vec().into()
                    };
                    Ok(batch.put_file("files", source, crate::Place::Cloud))
                },
                |sql, file| -> Result<(), DbError> {
                    sql.execute("UPDATE files SET file=?1", [file])?;
                    std::process::exit(86)
                },
            ))
            .unwrap();
    } else {
        db.inspect_writer(|sql| {
            if phase == "committed" {
                sql.crash_after_next_commit();
            } else {
                sql.crash_after("_coven_file_removals", "DELETE");
            }
        });
        runtime.block_on(complete(&db)).unwrap();
    }
    panic!("file write did not reach its crash point");
}

/// Complete the only queued upload as confirmed storage would, whether a
/// file still waits for it or not.
async fn complete(db: &Database) -> Result<(), DbError> {
    let id = db.inspect_writer(|sql| {
        sql.query_row("SELECT id FROM _coven_file_uploads", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap()
    });
    FileDatabase::new(db.clone()).complete_upload(id, ()).await
}

/// The size of the file row `7` references.
fn file_size(db: &Database) -> i64 {
    db.inspect_writer(|sql| {
        sql.query_row(
            "SELECT f.size FROM files AS r JOIN main.coven_files AS f ON f.id=r.file WHERE r.id='7'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    })
}

struct CrashingReader(bool);
impl tokio::io::AsyncRead for CrashingReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        out: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.0 {
            std::process::exit(86);
        }
        out.put_slice(b"partial");
        self.0 = true;
        std::task::Poll::Ready(Ok(()))
    }
}

/// Another device's write removing the last reference to a file this device
/// uploads deletes its record; the upload keeps its own source until its
/// attempt ends, and then switches nothing (§17.9).
#[tokio::test]
async fn a_downloaded_reference_change_keeps_the_queued_copy_until_upload() {
    let ids = coven_foundation::id_source::SequentialIds::new();
    let store = TestStore::with_ids(&ids);
    let other = TestStore::with_ids(&ids);
    let db = open(&store, Provenance::AppProvided).await;
    let peer = open(&other, Provenance::AppProvided).await;
    let file = stage(&db, b"original".to_vec(), true).await.unwrap();
    for record in crate::write::tests::records(&db) {
        db.receive(record.clone().into()).await.unwrap();
        peer.receive(record.into()).await.unwrap();
    }
    peer.write(|sql| {
        sql.execute("UPDATE files SET file=NULL", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let upload = FileDatabase::new(db.clone())
        .uploads()
        .await
        .unwrap()
        .remove(0);
    db.receive(crate::write::tests::records(&peer).remove(0).into())
        .await
        .unwrap();
    assert!(!crate::file_write::tests::has_record(&db, file));
    // No file waits for the upload, which still owns its source.
    assert!(FileDatabase::new(db.clone())
        .uploads()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(local_count(&db, "_coven_file_uploads"), 1);
    assert_eq!(owned_paths(&store).len(), 1);
    assert!(FileDatabase::new(db.clone())
        .open_upload_source(&upload)
        .await
        .unwrap()
        .is_some());
    let writes = crate::write::tests::records(&db).len();
    complete(&db).await.unwrap();
    assert_eq!(crate::write::tests::records(&db).len(), writes);
    assert_eq!(local_count(&db, "_coven_device_files"), 0);
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    assert!(owned_paths(&store).is_empty());
    db.close().await.unwrap();
    peer.close().await.unwrap();
}

#[tokio::test]
async fn a_reused_id_cannot_claim_or_delete_a_kept_file() {
    use coven_foundation::id_source::SequentialIds;
    use std::sync::Arc;
    let store = TestStore::new();
    let reopen = || {
        store
            .builder(tables(), vec![Migration::addition(1, "files", SCHEMA)])
            .namespaces(namespaces(Provenance::AppProvided))
            .id_source(Arc::new(SequentialIds::new()))
    };
    let db = reopen().open().await.unwrap();
    stage(&db, b"original".to_vec(), true).await.unwrap();
    let original = owned_paths(&store).remove(0);
    db.close().await.unwrap();
    let db = reopen().open().await.unwrap();
    assert!(matches!(
        stage(&db, b"replacement".to_vec(), false).await,
        Err(DbError::FileNameReused { name }) if name.as_str() == original.file_name().unwrap().to_str().unwrap()
    ));
    assert_eq!(std::fs::read(original).unwrap(), b"original");
    assert_eq!(file_size(&db), 8);
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_write_keeps_its_error_when_pending_deletion_also_fails() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let directory = store.database_path().parent().unwrap().join("files");
    db.inspect_writer(|sql| {
        sql.fail_at(
            "_coven_fail_cleanup",
            "BEFORE DELETE ON _coven_file_removals",
            "record stays",
        )
    });
    let error = db
        .write_with_files::<_, _, _, _, crate::DbError>(
            |batch| Ok(batch.put_file("files", b"partial".to_vec(), crate::Place::Cloud)),
            |_, _| Err::<(), _>(DbError::StoreClosed),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, DbError::FileCleanup {write: Err(error), failures} if matches!(*error, DbError::StoreClosed) && matches!(failures.as_slice(), [DbError::Sqlite(_)]))
    );
    assert_eq!(local_count(&db, "files"), 0);
    assert_eq!(local_count(&db, "_coven_file_removals"), 1);
    assert!(std::fs::read_dir(directory).unwrap().next().is_none());
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER _coven_fail_cleanup").unwrap());
    db.write(|_| Ok(())).await.unwrap();
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_rollback_cannot_release_bytes_using_uncommitted_records() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    stage(&db, b"original".to_vec(), true).await.unwrap();
    let original = owned_paths(&store).remove(0);
    db.inspect_writer(|sql| sql.refuse_transaction_end(true));
    let error = db
        .write(|sql| {
            sql.execute("DELETE FROM files", [])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(original.exists(), "the committed row still owns its bytes");
    assert!(
        matches!(error, DbError::FileCleanup {write: Err(error), ..} if matches!(*error, DbError::Rollback { .. }))
    );
    // Another attempted write must not delete from the same uncommitted state.
    assert!(matches!(
        db.write(|_| Ok(())).await,
        Err(DbError::RollbackFailed)
    ));
    assert_eq!(std::fs::read(&original).unwrap(), b"original");
    db.close().await.unwrap();
    let db = open(&store, Provenance::AppProvided).await;
    assert_eq!(file_size(&db), 8);
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    db.close().await.unwrap();
}

/// §21.2, Appendix C.3: upload completion releases the queue's copy, whose bytes go
/// only after the writer is free. While that removal stalls on the disk an
/// app write commits; once it finishes, the record goes too.
#[tokio::test]
async fn a_stalled_file_removal_holds_back_no_app_write() {
    use std::sync::{mpsc, Mutex};
    use std::time::Duration;
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    stage(&db, b"original".to_vec(), true).await.unwrap();
    let (entered, stalled) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let gate = Mutex::new(Some((entered, released)));
    db.test_gate_file_removal(move || {
        // Only the first removal, the completion's, stalls.
        let first = gate.lock().unwrap().take();
        if let Some((entered, released)) = first {
            entered.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(10)).unwrap();
        }
    })
    .await;
    let completing = tokio::spawn({
        let db = db.clone();
        async move { complete(&db).await }
    });
    tokio::task::spawn_blocking(move || stalled.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();
    assert_eq!(local_count(&db, "_coven_device_files"), 0);
    assert_eq!(local_count(&db, "_coven_file_removals"), 1);
    let written = tokio::time::timeout(
        Duration::from_secs(5),
        db.write(|sql| {
            sql.execute("UPDATE files SET title=title", [])?;
            Ok(())
        }),
    )
    .await;
    // Release the removal before judging, so a held writer fails the test
    // instead of hanging it.
    release.send(()).unwrap();
    written
        .expect("an app write waited for a stalled file removal")
        .unwrap();
    completing.await.unwrap().unwrap();
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    assert!(owned_paths(&store).is_empty());
    db.close().await.unwrap();
}

/// A removal is recorded only for bytes nothing names, and nothing names
/// bytes recorded for removal, so removing them needs no check of its own
/// (Appendix C.3). Either step that would break that fails its transaction.
#[tokio::test]
async fn a_referenced_path_cannot_be_recorded_for_removal() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    stage(&db, b"original".to_vec(), true).await.unwrap();
    let path = owned_paths(&store).remove(0);
    let name = path.file_name().unwrap().to_str().unwrap().to_owned();
    let recorded = db.inspect_writer(|sql| {
        sql.transaction(|db| {
            db.internal_execute("INSERT INTO _coven_file_removals(path) VALUES(?1)", [&name])
        })
    });
    assert!(recorded.is_err(), "{recorded:?}");
    let cached = db.inspect_writer(|sql| {
        sql.transaction(|db| {
            db.internal_execute(
                "INSERT INTO _coven_file_removals(area,path) VALUES('cache','released')",
                [],
            )?;
            db.internal_execute(
                "INSERT INTO _coven_cache(namespace,file_id,chunk,path,size,last_read,pinned)
                 VALUES('files','file',0,'released',1,1,0)",
                [],
            )
        })
    });
    assert!(cached.is_err(), "{cached:?}");
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    assert_eq!(local_count(&db, "_coven_cache"), 0);
    assert_eq!(std::fs::read(path).unwrap(), b"original");
    db.close().await.unwrap();
}
