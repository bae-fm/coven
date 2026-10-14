use super::*;
use crate::{
    file_write::tests::{attach, tables},
    tests::TestStore,
    Provenance,
};

#[tokio::test]
async fn reopen_waits_for_a_cache_reservation_after_its_database_owner_drops() {
    use crate::file_write::tests::{local_count, SCHEMA};
    use std::{future::Future, task::Poll};
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    let owner = FileDatabase::new(db.clone());
    let reservation = owner
        .reserve_cache_file(FileName::new("reserved").unwrap())
        .await
        .unwrap();
    drop(db);
    drop(owner);
    let mut reopen = Box::pin(store.schema(tables(Provenance::AppProvided), SCHEMA));
    let waiting =
        std::future::poll_fn(|cx| Poll::Ready(reopen.as_mut().poll(cx).is_pending())).await;
    drop(reservation);
    assert!(waiting, "a reservation retains work, not a live owner");
    let reopened = reopen.await.unwrap();
    assert_eq!(local_count(&reopened, "_coven_file_removals"), 0);
    reopened.close().await.unwrap();
}

#[test]
fn cancelled_cache_reservations_drop_outside_a_runtime_and_record_cleanup_failures() {
    use crate::file_write::tests::{local_count, SCHEMA};
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut panicked = Vec::new();
    for refuse_cleanup in [false, true] {
        let store = TestStore::new();
        let db = runtime
            .block_on(store.schema(tables(Provenance::AppProvided), SCHEMA))
            .unwrap();
        let owner = FileDatabase::new(db.clone());
        let name = FileName::new("reserved").unwrap();
        let reservation = runtime
            .block_on(owner.reserve_cache_file(name.clone()))
            .unwrap();
        let path = store
            .database_path()
            .parent()
            .unwrap()
            .join("cache")
            .join(name.as_str());
        if refuse_cleanup {
            std::fs::create_dir(&path).unwrap();
        } else {
            std::fs::write(&path, b"unclaimed").unwrap();
        }
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(reservation))).is_err() {
            panicked.push(refuse_cleanup);
        }
        assert_eq!(local_count(&db, "_coven_file_removals"), 1);
        runtime.block_on(db.close()).unwrap();
        let opened = runtime.block_on(store.schema(tables(Provenance::AppProvided), SCHEMA));
        let reopened = if refuse_cleanup {
            assert!(matches!(
                opened,
                Err(crate::CovenError::Database(DbError::FileCleanup { .. }))
            ));
            store.assert_writer_unlocked();
            let sql = rusqlite::Connection::open(store.database_path()).unwrap();
            assert_eq!(
                sql.query_row("SELECT count(*) FROM _coven_file_removals", [], |r| r
                    .get::<_, u32>(0))
                    .unwrap(),
                1
            );
            drop(sql);
            std::fs::remove_dir(&path).unwrap();
            runtime
                .block_on(store.schema(tables(Provenance::AppProvided), SCHEMA))
                .unwrap()
        } else {
            opened.unwrap()
        };
        assert_eq!(local_count(&reopened, "_coven_file_removals"), 0);
        assert!(!path.exists());
        runtime.block_on(reopened.close()).unwrap();
    }
    assert!(
        panicked.is_empty(),
        "cancelling cache reservations needed a runtime: {panicked:?}"
    );
}

#[tokio::test]
async fn a_where_column_trigger_cannot_remove_the_row_that_attaches_its_file() {
    let store = TestStore::new();
    let mut declarations = tables(Provenance::AppProvided);
    declarations[0] = declarations[0].clone().shared_trigger("remove_attached");
    let database=store.schema(declarations,"CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT,title TEXT); CREATE TRIGGER remove_attached AFTER UPDATE OF location ON files WHEN NOT coven_applying() AND NEW.location LIKE 'file %' BEGIN DELETE FROM files WHERE id=NEW.id; END;").await.unwrap();
    // The attaching write fixes the reference, so it is the only write that
    // sets the where-column; a trigger removing the row refuses that write.
    assert!(matches!(
        attach(&database, b"source".to_vec(), true).await,
        Err(DbError::FileRowRemoved { .. })
    ));
    assert!(matches!(
        database.file_ref("files", "7").await,
        Err(DbError::FileAbsent { .. })
    ));
    assert!(FileDatabase::new(database.clone())
        .uploads()
        .await
        .unwrap()
        .is_empty());
    database.close().await.unwrap();
}

/// The fixed reference before and after: the where-column names the path and
/// key the attaching write chose, and completion leaves every row alone.
fn fixed(
    file: &FileRef,
) -> (
    coven_foundation::id_source::DeviceId,
    coven_foundation::id_source::FileId,
) {
    (file.uploader(), file.id())
}

#[tokio::test]
async fn completion_retires_the_queue_without_writing_whatever_the_row_became() {
    use crate::file_write::tests::{local_count, owned_paths, SCHEMA};
    // Each case changes the row after attaching A, then storage confirms A.
    for change in ["none", "retarget", "delete", "edit"] {
        let store = TestStore::new();
        let db = store
            .schema(tables(Provenance::AppProvided), SCHEMA)
            .await
            .unwrap();
        let owner = FileDatabase::new(db.clone());
        attach(&db, b"first".to_vec(), true).await.unwrap();
        let first = db.file_ref("files", "7").await.unwrap();
        let first_id = owner.uploads().await.unwrap()[0].id;
        assert_eq!(
            first.reference().unwrap().device,
            first.uploader(),
            "the where-column carries the fixed reference from the start"
        );
        assert!(owner.open_local(&first).await.unwrap().is_some());
        match change {
            "retarget" => attach(&db, b"second".to_vec(), false).await.unwrap(),
            "delete" => db
                .write(|sql| {
                    sql.execute("DELETE FROM files", [])?;
                    Ok(())
                })
                .await
                .unwrap(),
            "edit" => db
                .write(|sql| {
                    sql.execute("UPDATE files SET title='renamed'", [])?;
                    Ok(())
                })
                .await
                .unwrap(),
            _ => {}
        }
        let current = db.file_ref("files", "7").await.ok();
        // The queue's source facts outlive the row's change until completion.
        assert!(owner.open_upload_source(&first).await.unwrap().is_some());
        if change == "retarget" {
            assert_eq!(owned_paths(&store).len(), 2);
        }
        let writes = db.test_queued_writes().await.unwrap().len();
        owner.complete_upload(first_id, ()).await.unwrap();
        assert!(owner.open_upload_source(&first).await.unwrap().is_none());
        assert_eq!(db.file_ref("files", "7").await.ok(), current);
        assert_eq!(db.test_queued_writes().await.unwrap().len(), writes);
        let queue = owner.uploads().await.unwrap();
        match change {
            "retarget" => {
                let second = current.clone().unwrap();
                assert_ne!(fixed(&second), fixed(&first));
                assert_eq!(queue.len(), 1);
                assert_eq!(queue[0].file, second);
                // The newer attachment keeps its own copy and hashes.
                assert!(owner.open_local(&second).await.unwrap().is_some());
                assert_eq!(local_count(&db, "_coven_device_files"), 1);
                assert_eq!(owned_paths(&store).len(), 1);
            }
            _ => {
                assert!(queue.is_empty());
                assert_eq!(local_count(&db, "_coven_device_files"), 0);
                assert_eq!(local_count(&db, "_coven_file_chunks"), 0);
                assert!(owned_paths(&store).is_empty());
            }
        }
        if change == "none" || change == "edit" {
            // Uploaded: the same FileRef stays valid and now reads remotely.
            let after = current.unwrap();
            assert_eq!(fixed(&after), fixed(&first));
            if change == "none" {
                assert_eq!(after, first);
            }
            owner.validate(&after).await.unwrap();
            assert!(owner.open_local(&after).await.unwrap().is_none());
        }
        assert!(matches!(
            owner.complete_upload(first_id, ()).await,
            Err(DbError::DamagedDatabase)
        ));
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn chunk_hashes_belong_to_the_queued_file_whatever_its_row_becomes() {
    use crate::file_write::tests::{local_count, SCHEMA};
    use coven_crypto::ContentHasher;
    use coven_format::chunks::CHUNK_SIZE;
    let hashes = |bytes: &[u8]| {
        bytes
            .chunks(CHUNK_SIZE)
            .map(|chunk| {
                let mut hash = ContentHasher::new();
                hash.update(chunk);
                hash.finish()
            })
            .collect::<Vec<_>>()
    };
    for provenance in [Provenance::UserProvided, Provenance::AppProvided] {
        for change in ["none", "retarget", "delete"] {
            let store = TestStore::new();
            let db = store
                .schema(tables(provenance.clone()), SCHEMA)
                .await
                .unwrap();
            let originals = tempfile::tempdir().unwrap();
            let attach_bytes = |bytes: Vec<u8>, insert: bool| {
                let db = db.clone();
                let path = originals.path().join(format!("{}", bytes.len()));
                let provenance = provenance.clone();
                async move {
                    match provenance {
                        Provenance::AppProvided => attach(&db, bytes, insert).await.unwrap(),
                        Provenance::UserProvided => {
                            std::fs::write(&path, &bytes).unwrap();
                            let prepared = crate::prepare_user_file(&path, |_| {}).await.unwrap();
                            let size = bytes.len() as i64;
                            db.write(move |sql| {
                                sql.execute(
                                    "INSERT INTO files(id,size) VALUES('7',?1)
                                     ON CONFLICT(id) DO UPDATE SET size=excluded.size",
                                    [size],
                                )?;
                                sql.register_user_file("files", "7", prepared)
                            })
                            .await
                            .unwrap();
                        }
                    }
                }
            };
            let first_bytes = vec![17; CHUNK_SIZE + 1];
            attach_bytes(first_bytes.clone(), true).await;
            let first = db.file_ref("files", "7").await.unwrap();
            // One hash per chunk, recorded with the queue in the attaching write.
            assert_eq!(local_count(&db, "_coven_file_chunks"), 2);
            let second_bytes = vec![18; CHUNK_SIZE * 2];
            match change {
                "retarget" => attach_bytes(second_bytes.clone(), false).await,
                "delete" => db
                    .write(|sql| {
                        sql.execute("DELETE FROM files", [])?;
                        Ok(())
                    })
                    .await
                    .unwrap(),
                _ => {}
            }
            // Crash recovery: the queue and its hashes survive reopening.
            db.close().await.unwrap();
            let db = store
                .schema(tables(provenance.clone()), SCHEMA)
                .await
                .unwrap();
            let owner = FileDatabase::new(db.clone());
            for (index, expected) in hashes(&first_bytes).into_iter().enumerate() {
                assert_eq!(
                    owner.upload_chunk_hash(&first, index as u64).await.unwrap(),
                    expected
                );
            }
            assert!(owner.open_upload_source(&first).await.unwrap().is_some());
            if change == "retarget" {
                let second = db.file_ref("files", "7").await.unwrap();
                assert_eq!(local_count(&db, "_coven_file_chunks"), 4);
                for (index, expected) in hashes(&second_bytes).into_iter().enumerate() {
                    assert_eq!(
                        owner
                            .upload_chunk_hash(&second, index as u64)
                            .await
                            .unwrap(),
                        expected
                    );
                }
            }
            let first_id = owner
                .uploads()
                .await
                .unwrap()
                .into_iter()
                .find(|upload| upload.file == first)
                .unwrap()
                .id;
            owner.complete_upload(first_id, ()).await.unwrap();
            // Completing A cannot delete B's facts.
            assert_eq!(
                local_count(&db, "_coven_file_chunks"),
                if change == "retarget" { 2 } else { 0 }
            );
            assert!(matches!(
                owner.upload_chunk_hash(&first, 0).await,
                Err(DbError::DamagedDatabase)
            ));
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn an_empty_file_queues_no_chunk_hash_and_a_missing_one_is_damage() {
    use crate::file_write::tests::{local_count, SCHEMA};
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    let owner = FileDatabase::new(db.clone());
    attach(&db, Vec::new(), true).await.unwrap();
    assert_eq!(local_count(&db, "_coven_file_chunks"), 0);
    assert_eq!(owner.uploads().await.unwrap().len(), 1);
    attach(&db, vec![5; 3], false).await.unwrap();
    let file = db.file_ref("files", "7").await.unwrap();
    assert_eq!(local_count(&db, "_coven_file_chunks"), 1);
    db.commit_writer(|sql| {
        sql.internal_execute("DELETE FROM _coven_file_chunks", [])
            .unwrap()
    });
    assert!(matches!(
        owner.upload_chunk_hash(&file, 0).await,
        Err(DbError::DamagedDatabase)
    ));
    db.close().await.unwrap();
}

#[tokio::test]
async fn attachment_and_upload_queue_commit_or_roll_back_together() {
    use crate::file_write::tests::{local_count, owned_paths, SCHEMA};
    for provenance in [Provenance::AppProvided, Provenance::UserProvided] {
        let store = TestStore::new();
        let declarations = tables(provenance.clone());
        let database = store.schema(declarations.clone(), SCHEMA).await.unwrap();
        let original = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(original.path(), b"source").unwrap();
        let owner = FileDatabase::new(database.clone());
        database.inspect_writer(|sql| {
            sql.fail_at(
                "_coven_refuse_upload",
                "BEFORE INSERT ON _coven_file_uploads",
                "queue unavailable",
            );
        });
        for rejected in [true, false] {
            let result = match provenance {
                Provenance::AppProvided => attach(&database, b"source".to_vec(), true).await,
                Provenance::UserProvided => {
                    let prepared = crate::prepare_user_file(original.path(), |_| {})
                        .await
                        .unwrap();
                    database
                        .write(move |sql| {
                            sql.insert_user_file(
                                "files",
                                "7",
                                prepared,
                                "INSERT INTO files(id,size) VALUES('7',6)",
                                &[],
                            )
                        })
                        .await
                }
            };
            if rejected {
                assert!(matches!(result, Err(DbError::Sqlite(_))));
                assert_eq!(local_count(&database, "files"), 0);
                assert_eq!(local_count(&database, "_coven_device_files"), 0);
                assert_eq!(local_count(&database, "_coven_user_files"), 0);
                assert_eq!(local_count(&database, "_coven_file_chunks"), 0);
                assert!(owned_paths(&store).is_empty());
                assert!(owner.uploads().await.unwrap().is_empty());
                database
                    .inspect_writer(|sql| sql.batch("DROP TRIGGER _coven_refuse_upload").unwrap());
            } else {
                result.unwrap();
            }
            assert_eq!(std::fs::read(original.path()).unwrap(), b"source");
        }
        let reference = database.file_ref("files", "7").await.unwrap();
        assert!(owner.open_local(&reference).await.unwrap().is_some());
        let queued = owner.uploads().await.unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].file, reference);
        let queued_id = queued[0].id;
        database
            .write(|sql| {
                sql.execute("UPDATE files SET title='renamed'", [])?;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(owner.uploads().await.unwrap().len(), 1);
        database.close().await.unwrap();
        let database = store.schema(declarations, SCHEMA).await.unwrap();
        let queued = FileDatabase::new(database.clone()).uploads().await.unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].id, queued_id);
        assert_eq!(queued[0].file, reference);
        database.close().await.unwrap();
    }
}
