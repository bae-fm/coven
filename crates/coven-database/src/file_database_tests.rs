use super::*;
use crate::{
    file_write::tests::{complete_uploads, namespaces, open, stage, tables, SCHEMA},
    tests::TestStore,
    CovenError, Migration, Namespace, Provenance, SchemaError,
};

/// A stored file of these bytes, referenced by row `7`.
async fn stored(db: &Database, bytes: Vec<u8>) -> FileRef {
    let file = stage(db, bytes, true).await.unwrap();
    complete_uploads(db).await;
    db.file_ref(file).await.unwrap()
}

#[tokio::test]
async fn reopen_waits_for_a_cache_reservation_after_its_database_owner_drops() {
    use crate::file_write::tests::local_count;
    use std::{future::Future, task::Poll};
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = stored(&db, vec![5; 3]).await;
    let owner = FileDatabase::new(db.clone());
    let reservation =
        owner.reserve_cache_file(&file, FileName::new("reserved").unwrap()).await.unwrap();
    drop(db);
    drop(owner);
    let mut reopen = Box::pin(open(&store, Provenance::AppProvided));
    let waiting =
        std::future::poll_fn(|cx| Poll::Ready(reopen.as_mut().poll(cx).is_pending())).await;
    drop(reservation);
    assert!(waiting, "a reservation retains work, not a live owner");
    let reopened = reopen.await;
    assert_eq!(local_count(&reopened, "_coven_file_removals"), 0);
    reopened.close().await.unwrap();
}

#[test]
fn cancelled_cache_reservations_drop_outside_a_runtime_and_record_cleanup_failures() {
    use crate::file_write::tests::local_count;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut panicked = Vec::new();
    for refuse_cleanup in [false, true] {
        let store = TestStore::new();
        let db = runtime.block_on(open(&store, Provenance::AppProvided));
        let file = runtime.block_on(stored(&db, vec![5; 3]));
        let owner = FileDatabase::new(db.clone());
        let name = FileName::new("reserved").unwrap();
        let reservation = runtime.block_on(owner.reserve_cache_file(&file, name.clone())).unwrap();
        let path = store.database_path().parent().unwrap().join("cache").join(name.as_str());
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
        let opened = runtime.block_on(
            store
                .builder(tables(), vec![Migration::addition(1, "files", SCHEMA)])
                .namespaces(namespaces(Provenance::AppProvided))
                .open(),
        );
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
            runtime.block_on(open(&store, Provenance::AppProvided))
        } else {
            opened.unwrap()
        };
        assert_eq!(local_count(&reopened, "_coven_file_removals"), 0);
        assert!(!path.exists());
        runtime.block_on(reopened.close()).unwrap();
    }
    assert!(panicked.is_empty(), "cancelling cache reservations needed a runtime: {panicked:?}");
}

/// Stage these bytes of either kind for row `7`, inserting or updating it.
async fn stage_kind(
    db: &Database,
    kind: &Provenance,
    originals: &std::path::Path,
    bytes: Vec<u8>,
    insert: bool,
) -> Result<coven_foundation::id_source::FileId, DbError> {
    match kind {
        Provenance::AppProvided => stage(db, bytes, insert).await,
        Provenance::UserProvided => {
            let path = originals.join(format!("{}", bytes.len()));
            std::fs::write(&path, &bytes).unwrap();
            let prepared = crate::prepare_user_file(&path, |_| {}).await.unwrap();
            db.write_with_files::<_, _, _, _, DbError>(
                move |batch| Ok(batch.put_user_file("files", crate::UserSource::Cloud(prepared))),
                move |sql, file| {
                    sql.execute(
                        "INSERT INTO files(id,file) VALUES('7',?1)
                         ON CONFLICT(id) DO UPDATE SET file=excluded.file",
                        [file],
                    )?;
                    Ok(file)
                },
            )
            .await
        }
    }
}

#[tokio::test]
async fn chunk_hashes_belong_to_the_queued_upload_whatever_its_rows_become() {
    use crate::file_write::tests::local_count;
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
            let db = open(&store, provenance.clone()).await;
            let originals = tempfile::tempdir().unwrap();
            let first_bytes = vec![17; CHUNK_SIZE + 1];
            stage_kind(&db, &provenance, originals.path(), first_bytes.clone(), true)
                .await
                .unwrap();
            let first = FileDatabase::new(db.clone()).uploads().await.unwrap().remove(0);
            // One hash per chunk, recorded with the queue in the staging write.
            assert_eq!(local_count(&db, "_coven_file_chunks"), 2);
            let second_bytes = vec![18; CHUNK_SIZE * 2];
            match change {
                "retarget" => {
                    stage_kind(&db, &provenance, originals.path(), second_bytes.clone(), false)
                        .await
                        .unwrap();
                }
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
            let db = open(&store, provenance.clone()).await;
            let owner = FileDatabase::new(db.clone());
            for (index, expected) in hashes(&first_bytes).into_iter().enumerate() {
                assert_eq!(
                    owner.upload_chunk_hash(&first.location, index as u64).await.unwrap(),
                    expected
                );
            }
            assert!(owner.open_upload_source(&first).await.unwrap().is_some());
            if change == "retarget" {
                let second = owner
                    .uploads()
                    .await
                    .unwrap()
                    .into_iter()
                    .find(|upload| upload.id != first.id)
                    .unwrap();
                assert_eq!(local_count(&db, "_coven_file_chunks"), 4);
                for (index, expected) in hashes(&second_bytes).into_iter().enumerate() {
                    assert_eq!(
                        owner.upload_chunk_hash(&second.location, index as u64).await.unwrap(),
                        expected
                    );
                }
            }
            owner.complete_upload(first.id, ()).await.unwrap();
            // Completing one upload cannot delete another's facts.
            assert_eq!(
                local_count(&db, "_coven_file_chunks"),
                if change == "retarget" { 2 } else { 0 }
            );
            assert!(matches!(
                owner.upload_chunk_hash(&first.location, 0).await,
                Err(DbError::DamagedDatabase)
            ));
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn an_empty_file_queues_no_chunk_hash_and_a_missing_one_is_damage() {
    use crate::file_write::tests::local_count;
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let owner = FileDatabase::new(db.clone());
    stage(&db, Vec::new(), true).await.unwrap();
    assert_eq!(local_count(&db, "_coven_file_chunks"), 0);
    assert_eq!(owner.uploads().await.unwrap().len(), 1);
    stage(&db, vec![5; 3], false).await.unwrap();
    // The empty file went with its last reference; its upload waits only
    // for retirement.
    let uploads = owner.uploads().await.unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!(local_count(&db, "_coven_file_chunks"), 1);
    db.commit_writer(|sql| sql.internal_execute("DELETE FROM _coven_file_chunks", []).unwrap());
    assert!(matches!(
        owner.upload_chunk_hash(&uploads[0].location, 0).await,
        Err(DbError::DamagedDatabase)
    ));
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_damaged_queue_row_fails_the_query_instead_of_emptying_it() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let owner = FileDatabase::new(db.clone());
    stage(&db, vec![5; 3], true).await.unwrap();
    assert_eq!(owner.uploads().await.unwrap().len(), 1);
    // A stored time no decoder accepts: decoding stored facts finds damage.
    db.commit_writer(|sql| {
        sql.internal_execute("UPDATE _coven_file_uploads SET queued_at=x'00'", []).unwrap()
    });
    assert!(matches!(owner.uploads().await, Err(DbError::DamagedDatabase)));
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_staged_file_and_its_upload_commit_or_roll_back_together() {
    use crate::file_write::tests::{local_count, owned_paths};
    for provenance in [Provenance::AppProvided, Provenance::UserProvided] {
        let store = TestStore::new();
        let database = open(&store, provenance.clone()).await;
        let originals = tempfile::tempdir().unwrap();
        let owner = FileDatabase::new(database.clone());
        database.inspect_writer(|sql| {
            sql.fail_at(
                "_coven_refuse_upload",
                "BEFORE INSERT ON _coven_file_uploads",
                "queue unavailable",
            );
        });
        let rejected =
            stage_kind(&database, &provenance, originals.path(), b"source".to_vec(), true).await;
        assert!(matches!(rejected, Err(DbError::Sqlite(_))), "{rejected:?}");
        assert_eq!(local_count(&database, "files"), 0);
        assert_eq!(local_count(&database, "coven_files"), 0);
        assert_eq!(local_count(&database, "_coven_device_files"), 0);
        assert_eq!(local_count(&database, "_coven_user_files"), 0);
        assert_eq!(local_count(&database, "_coven_file_chunks"), 0);
        assert!(owned_paths(&store).is_empty());
        assert!(owner.uploads().await.unwrap().is_empty());
        database.inspect_writer(|sql| sql.batch("DROP TRIGGER _coven_refuse_upload").unwrap());
        let file = stage_kind(&database, &provenance, originals.path(), b"source".to_vec(), true)
            .await
            .unwrap();
        let reference = database.file_ref(file).await.unwrap();
        assert!(owner.open_local(&reference).await.unwrap().is_some());
        let queued = owner.uploads().await.unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].files, [file]);
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
        let database = open(&store, provenance.clone()).await;
        let queued = FileDatabase::new(database.clone()).uploads().await.unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].id, queued_id);
        assert_eq!(queued[0].files, [file]);
        database.close().await.unwrap();
    }
}

/// One verified range is one cache file and one row; each chunk in it reads
/// back at its own offset. A range overlapping a published one is not
/// written, and a crash while writing one leaves bytes that opening removes
/// (Appendix C.3).
#[tokio::test]
async fn cache_ranges_publish_whole_and_never_overlap() {
    use crate::file_write::tests::local_count;
    const CHUNK: usize = 64 * 1024 + 16;
    let store = TestStore::new();
    let reopen = || open(&store, Provenance::AppProvided);
    let db = reopen().await;
    let file = stored(&db, vec![5; 2 * 64 * 1024 + 5]).await;
    let owner = FileDatabase::new(db.clone());
    let range: Vec<u8> = (0..2 * CHUNK + 21).map(|i| (i % 251) as u8).collect();
    owner.cache_range(&file, 0, 3, FileName::new("range").unwrap(), range.clone()).await.unwrap();
    assert_eq!(local_count(&db, "_coven_cache"), 1);
    assert_eq!(owner.cached(&file, 1).await.unwrap().unwrap(), range[CHUNK..2 * CHUNK]);
    assert_eq!(owner.cached(&file, 2).await.unwrap().unwrap(), range[2 * CHUNK..]);
    assert_eq!(owner.cached(&file, -1).await.unwrap(), None);
    owner
        .cache_range(
            &file,
            1,
            1,
            FileName::new("overlap").unwrap(),
            range[CHUNK..2 * CHUNK].to_vec(),
        )
        .await
        .unwrap();
    assert_eq!(local_count(&db, "_coven_cache"), 1);
    let cache = store.database_path().parent().unwrap().join("cache");
    assert!(!cache.join("overlap").exists());
    // A crash after the range's name was recorded and its bytes written.
    db.inspect_writer(|sql| {
        sql.internal_execute("INSERT INTO _coven_cache_writes(path) VALUES('interrupted')", [])
            .unwrap();
    });
    std::fs::write(cache.join("interrupted"), b"bytes").unwrap();
    db.close().await.unwrap();
    let db = reopen().await;
    assert!(!cache.join("interrupted").exists());
    assert!(cache.join("range").exists());
    assert_eq!(local_count(&db, "_coven_cache_writes"), 0);
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    db.close().await.unwrap();
}

/// The namespace total changes with its cache records in their transaction:
/// publication, eviction, a failed publication and pinning (Appendix C.3).
#[tokio::test]
async fn cache_totals_follow_their_records() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = stored(&db, vec![5; 2 * 64 * 1024]).await;
    let owner = FileDatabase::new(db.clone());
    let total = |db: &Database| {
        db.inspect_writer(|sql| {
            let recorded = super::cache::total(sql, "files").unwrap();
            let summed: i64 = sql
                .query_row(
                    "SELECT coalesce(sum(size),0) FROM _coven_cache WHERE namespace='files'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(recorded, summed);
            recorded
        })
    };
    owner.cache_range(&file, -1, 1, FileName::new("header").unwrap(), vec![1; 11]).await.unwrap();
    owner.cache_range(&file, 0, 1, FileName::new("chunk").unwrap(), vec![2; 100]).await.unwrap();
    assert_eq!(total(&db), 111);
    // A refused publication commits nothing, the total included.
    assert!(owner
        .cache_range(&file, 1, 1, FileName::new("chunk").unwrap(), vec![3; 50])
        .await
        .is_err());
    assert_eq!(total(&db), 111);
    owner.set_budget("files", 1_000).await.unwrap();
    assert_eq!(total(&db), 111);
    owner.evict(&file).await.unwrap();
    assert_eq!(total(&db), 0);
    db.close().await.unwrap();
}

/// The write deleting the last record over a stored file's bytes takes their
/// cached copies, a pinned whole file included, and their bytes go once the
/// writer is free (§17.8, Appendix C.3).
#[tokio::test]
async fn cached_bytes_go_with_the_last_record_naming_them() {
    use crate::file_write::tests::local_count;
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = stored(&db, vec![5; 2 * 64 * 1024]).await;
    let owner = FileDatabase::new(db.clone());
    owner.cache_range(&file, -1, 1, FileName::new("header").unwrap(), vec![1; 11]).await.unwrap();
    let reservation =
        owner.reserve_cache_file(&file, FileName::new("whole").unwrap()).await.unwrap();
    let cache = store.database_path().parent().unwrap().join("cache");
    std::fs::write(cache.join("whole"), b"whole file").unwrap();
    reservation.publish().await.unwrap();
    assert_eq!(local_count(&db, "_coven_cache"), 1);
    assert_eq!(local_count(&db, "coven_pins"), 1);
    assert!(cache.join("whole").exists());
    db.write(|sql| {
        sql.execute("DELETE FROM files WHERE id='7'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(local_count(&db, "_coven_cache"), 0);
    assert_eq!(local_count(&db, "coven_pins"), 0);
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    assert!(!cache.join("whole").exists());
    assert!(!cache.join("header").exists());
    db.close().await.unwrap();
}

#[cfg(unix)]
fn cache_path(store: &TestStore, name: &FileName) -> std::path::PathBuf {
    store.database_path().parent().unwrap().join("cache").join(name.as_str())
}

#[cfg(unix)]
fn fifo(path: &std::path::Path) {
    assert!(std::process::Command::new("mkfifo").arg(path).status().unwrap().success());
}

/// Let a write blocked opening the FIFO at `path` go on, reading whatever
/// it writes.
#[cfg(unix)]
async fn release_fifo_writer(path: std::path::PathBuf) {
    tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let mut fifo = std::fs::File::open(path).unwrap();
        fifo.read_to_end(&mut Vec::new()).unwrap();
    })
    .await
    .unwrap();
}

/// §21.2, Appendix C.3: a cache range's bytes are written with no writer held. A
/// FIFO at its recorded name stands in for a stalled disk: the range write
/// blocks opening it while an app write commits. Released, it is no regular
/// file, so the range publishes nothing and its name and bytes go.
#[cfg(unix)]
#[tokio::test]
async fn a_stalled_cache_range_write_holds_back_no_app_write() {
    use crate::file_write::tests::local_count;
    use std::time::Duration;
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = stored(&db, vec![5; 3]).await;
    let owner = FileDatabase::new(db.clone());
    let name = FileName::new("range").unwrap();
    let path = cache_path(&store, &name);
    fifo(&path);
    let writing = tokio::spawn({
        let name = name.clone();
        async move { owner.cache_range(&file, 0, 1, name, vec![1; 19]).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!writing.is_finished(), "the range write is stalled");
    let written = tokio::time::timeout(
        Duration::from_secs(5),
        db.write(|sql| {
            sql.execute("UPDATE files SET title='while caching'", [])?;
            Ok(())
        }),
    )
    .await;
    assert_eq!(local_count(&db, "_coven_cache_writes"), 1);
    // Release the write before judging, so a held writer fails the test
    // instead of hanging it.
    release_fifo_writer(path.clone()).await;
    written.expect("an app write waited for a stalled cache range write").unwrap();
    assert!(writing.await.unwrap().is_err());
    assert_eq!(local_count(&db, "_coven_cache"), 0);
    assert_eq!(local_count(&db, "_coven_cache_writes"), 0);
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    assert!(!path.exists());
    db.close().await.unwrap();
}

/// Appendix C.3: a read-only handle, as from another process, writes a recorded
/// range while the writer commits and removes the files its writes release.
/// That removal leaves the range being written alone: only its own writer,
/// or an open that finds it abandoned, removes it.
#[cfg(unix)]
#[tokio::test]
async fn another_handles_removals_leave_a_range_being_written() {
    use crate::file_write::tests::{local_count, owned_paths};
    use std::time::Duration;
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = stored(&db, vec![5; 3]).await;
    // A second file's upload keeps its copy until it completes.
    stage(&db, vec![6; 3], false).await.unwrap();
    let read_only = store
        .builder(tables(), vec![Migration::addition(1, "files", SCHEMA)])
        .namespaces(namespaces(Provenance::AppProvided))
        .open_read_only()
        .await
        .unwrap();
    let reader = FileDatabase::read_only(read_only.clone());
    let name = FileName::new("range").unwrap();
    let path = cache_path(&store, &name);
    fifo(&path);
    let writing = tokio::spawn({
        let name = name.clone();
        async move { reader.cache_range(&file, 0, 1, name, vec![1; 19]).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!writing.is_finished(), "the range write is stalled");
    // Completing the upload releases the queue's copy, which the writer
    // removes with every other recorded removal.
    let completed = tokio::time::timeout(Duration::from_secs(5), complete_uploads(&db)).await;
    let left = path.exists();
    release_fifo_writer(path.clone()).await;
    completed.expect("completion waited for a stalled cache range write");
    assert!(owned_paths(&store).is_empty());
    assert!(left, "the writer removed a range another handle was writing");
    assert!(writing.await.unwrap().is_err());
    assert_eq!(local_count(&db, "_coven_cache_writes"), 0);
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    assert!(!path.exists());
    read_only.close().await.unwrap();
    db.close().await.unwrap();
}

/// Appendix C.3: opening the writer while a read-only handle writes a recorded
/// range waits for that write to finish. A range still recorded as being
/// written once no handle writes it was abandoned by a crash: it becomes a
/// removal, and its bytes go.
#[cfg(unix)]
#[tokio::test]
async fn opening_waits_for_a_range_being_written_and_removes_abandoned_ones() {
    use crate::file_write::tests::local_count;
    use std::time::Duration;
    let store = TestStore::new();
    let reopen = || open(&store, Provenance::AppProvided);
    let db = reopen().await;
    let file = stored(&db, vec![5; 3]).await;
    let abandoned = cache_path(&store, &FileName::new("abandoned").unwrap());
    db.inspect_writer(|sql| {
        sql.internal_execute("INSERT INTO _coven_cache_writes(path) VALUES('abandoned')", [])
            .unwrap();
    });
    std::fs::write(&abandoned, b"partial").unwrap();
    let read_only = store
        .builder(tables(), vec![Migration::addition(1, "files", SCHEMA)])
        .namespaces(namespaces(Provenance::AppProvided))
        .open_read_only()
        .await
        .unwrap();
    let reader = FileDatabase::read_only(read_only.clone());
    let name = FileName::new("live").unwrap();
    let live = cache_path(&store, &name);
    fifo(&live);
    let writing =
        tokio::spawn(async move { reader.cache_range(&file, 0, 1, name, vec![1; 19]).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!writing.is_finished(), "the range write is stalled");
    db.close().await.unwrap();
    let mut opening = Box::pin(reopen());
    let waited = tokio::time::timeout(Duration::from_millis(300), &mut opening).await.is_err();
    release_fifo_writer(live.clone()).await;
    assert!(waited, "opening took a range another handle was writing");
    let db = opening.await;
    assert!(writing.await.unwrap().is_err());
    assert!(!abandoned.exists());
    assert!(!live.exists());
    assert_eq!(local_count(&db, "_coven_cache_writes"), 0);
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    read_only.close().await.unwrap();
    db.close().await.unwrap();
}

/// §21.2: a cached chunk's bytes are read with no writer held. A FIFO in
/// place of a published range stands in for a stalled disk: the read blocks
/// opening it while an app write commits. Released, it is no regular file,
/// so the read fails.
#[cfg(unix)]
#[tokio::test]
async fn a_stalled_cache_read_holds_back_no_app_write() {
    use crate::file_write::tests::local_count;
    use std::time::Duration;
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = stored(&db, vec![5; 3]).await;
    let owner = FileDatabase::new(db.clone());
    let name = FileName::new("range").unwrap();
    owner.cache_range(&file, 0, 1, name.clone(), vec![1; 19]).await.unwrap();
    let path = cache_path(&store, &name);
    std::fs::remove_file(&path).unwrap();
    fifo(&path);
    let reading = tokio::spawn(async move { owner.cached(&file, 0).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!reading.is_finished(), "the cache read is stalled");
    let written = tokio::time::timeout(
        Duration::from_secs(5),
        db.write(|sql| {
            sql.execute("UPDATE files SET title='while reading'", [])?;
            Ok(())
        }),
    )
    .await;
    // Release the read before judging, so a held writer fails the test
    // instead of hanging it.
    let release = path.clone();
    tokio::task::spawn_blocking(move || {
        drop(std::fs::OpenOptions::new().write(true).open(release))
    })
    .await
    .unwrap();
    written.expect("an app write waited for a stalled cache read").unwrap();
    assert!(reading.await.unwrap().is_err());
    assert_eq!(local_count(&db, "_coven_cache"), 1);
    db.close().await.unwrap();
}

/// §21.2: an upload's source is opened with no writer held. A FIFO in place of the queue's copy stands in for a
/// stalled disk: opening blocks while an app write commits. Released, it is
/// no regular file, so opening fails.
#[cfg(unix)]
#[tokio::test]
async fn a_stalled_upload_source_holds_back_no_app_write() {
    use crate::file_write::tests::owned_paths;
    use std::time::Duration;
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    stage(&db, vec![5; 3], true).await.unwrap();
    let owner = FileDatabase::new(db.clone());
    let upload = owner.uploads().await.unwrap().remove(0);
    let [path] = owned_paths(&store).try_into().unwrap();
    std::fs::remove_file(&path).unwrap();
    fifo(&path);
    let opening = tokio::spawn(async move { owner.open_upload_source(&upload).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!opening.is_finished(), "opening the source is stalled");
    let written = tokio::time::timeout(
        Duration::from_secs(5),
        db.write(|sql| {
            sql.execute("UPDATE files SET title='while uploading'", [])?;
            Ok(())
        }),
    )
    .await;
    // Release the open before judging, so a held writer fails the test
    // instead of hanging it.
    let release = path.clone();
    tokio::task::spawn_blocking(move || {
        drop(std::fs::OpenOptions::new().write(true).open(release))
    })
    .await
    .unwrap();
    written.expect("an app write waited for a stalled upload source").unwrap();
    assert!(opening.await.unwrap().is_err());
    db.close().await.unwrap();
}

/// E8: every namespace gets its budget where it is declared. A later set
/// budget survives reopening; an undeclared namespace has none.
#[tokio::test]
async fn a_namespace_has_the_budget_its_declaration_gives_until_set() {
    let store = TestStore::new();
    let open_with = |budget| {
        store
            .builder(tables(), vec![Migration::addition(1, "files", SCHEMA)])
            .namespaces(vec![Namespace::new("files", Provenance::AppProvided, budget)])
            .open()
    };
    let db = open_with(100).await.unwrap();
    let owner = FileDatabase::new(db.clone());
    assert_eq!(owner.budget("files").await.unwrap(), 100);
    assert!(matches!(
        owner.budget("other").await,
        Err(DbError::UndeclaredNamespace { namespace }) if namespace == "other"
    ));
    assert!(matches!(owner.set_budget("other", 1).await, Err(DbError::UndeclaredNamespace { .. })));
    owner.set_budget("files", 50).await.unwrap();
    db.close().await.unwrap();
    let db = open_with(100).await.unwrap();
    assert_eq!(FileDatabase::new(db.clone()).budget("files").await.unwrap(), 50);
    // The cache holds its budget: the older entry goes once the total exceeds it.
    let removed = db.inspect_writer(|sql| {
        for (path, read) in [("old", 1), ("new", 2)] {
            sql.internal_execute(
                "INSERT INTO _coven_cache(namespace,file_id,chunk,path,size,last_read,pinned)
                 VALUES('files',?1,0,?1,30,?2,0)",
                (path, read),
            )
            .unwrap();
        }
        super::cache::trim(sql, "files").unwrap()
    });
    assert_eq!(removed, ["old"]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_namespace_is_declared_once() {
    let store = TestStore::new();
    let opened = store
        .builder(tables(), vec![Migration::addition(1, "files", SCHEMA)])
        .namespaces(vec![
            Namespace::new("files", Provenance::AppProvided, 1),
            Namespace::new("files", Provenance::UserProvided, 2),
        ])
        .open()
        .await;
    assert!(matches!(
        opened,
        Err(CovenError::Database(DbError::Schema(SchemaError::DuplicateNamespace { namespace })))
            if namespace == "files"
    ));
}
