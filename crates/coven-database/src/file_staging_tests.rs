use crate::{
    file_write::tests::{local_count, open, owned_paths},
    tests::TestStore,
    *,
};
use coven_foundation::clock::HANG_GUARD;
use coven_foundation::id_source::FileId;
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};

#[test]
fn cancelled_staging_drops_outside_a_runtime_and_the_next_write_removes_its_bytes() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let store = TestStore::new();
    let db = runtime.block_on(open(&store, Provenance::AppProvided));
    let staging = runtime.block_on(async {
        let staging = super::FileStaging::new(
            db.inner.clone(),
            db.writer.file_tasks.clone().read_owned().await,
            |batch| {
                Ok::<_, DbError>(batch.put_file(
                    "files",
                    b"unclaimed".to_vec(),
                    crate::Place::Cloud,
                ))
            },
        )
        .unwrap()
        .0;
        let (staging, result) = staging.write().await.unwrap();
        let _flushed = result.unwrap();
        staging
    });
    assert_eq!(owned_paths(&store).len(), 1);
    let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(staging)));
    runtime.block_on(db.write(|_| Ok(()))).unwrap();
    let remaining = owned_paths(&store);
    runtime.block_on(db.close()).unwrap();
    assert!(dropped.is_ok(), "cancelling staging must not need a runtime");
    assert!(remaining.is_empty());
}

/// Bytes the app holds are written as a run and a stream in turn, and the
/// staged files come back in the order the batch put them, each with its
/// own size and bytes on disk.
#[tokio::test(flavor = "multi_thread")]
async fn bytes_and_streams_stage_in_the_order_the_batch_put_them() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let contents: [(&[u8], bool); 5] =
        [(b"one", false), (b"", false), (b"streamed", true), (b"four", false), (b"five!", false)];
    let (staging, ids) = super::FileStaging::new(
        db.inner.clone(),
        db.writer.file_tasks.clone().read_owned().await,
        |batch| {
            Ok::<_, DbError>(
                contents
                    .map(|(bytes, streamed)| {
                        let source = if streamed {
                            FileSource::Stream(Box::pin(std::io::Cursor::new(bytes.to_vec())))
                        } else {
                            FileSource::Bytes(bytes.to_vec())
                        };
                        batch.put_file("files", source, crate::Place::Cloud)
                    })
                    .to_vec(),
            )
        },
    )
    .unwrap();
    let (staging, flushed) = staging.write().await.unwrap();
    let _flushed = flushed.unwrap();
    let (files, _names, ()) = staging.into_staged(()).take();
    assert_eq!(files.iter().map(|file| file.id).collect::<Vec<_>>(), ids);
    assert_eq!(
        files.iter().map(|file| file.size).collect::<Vec<_>>(),
        contents.map(|(bytes, _)| bytes.len() as u64)
    );
    let mut on_disk: Vec<_> =
        owned_paths(&store).iter().map(|path| std::fs::read(path).unwrap()).collect();
    on_disk.sort();
    let mut expected: Vec<_> = contents.map(|(bytes, _)| bytes.to_vec()).to_vec();
    expected.sort();
    assert_eq!(on_disk, expected);
    db.close().await.unwrap();
}

/// The rows of `files` another connection sees committed.
fn committed_rows(store: &TestStore) -> i64 {
    rusqlite::Connection::open(store.database_path())
        .unwrap()
        .query_row("SELECT count(*) FROM files", [], |r| r.get(0))
        .unwrap()
}

/// Stage two files and reference each from a row of `files`, in one write.
async fn write_two(db: &Database, ran: Arc<AtomicBool>) -> Result<(), DbError> {
    db.staged_write::<_, _, _, _, DbError>(
        |batch| {
            Ok([b"one", b"two"]
                .map(|bytes| batch.put_file("files", bytes.to_vec(), crate::Place::Cloud)))
        },
        move |sql, [one, two]| {
            ran.store(true, Ordering::SeqCst);
            sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [one])?;
            sql.execute("INSERT INTO files(id,file) VALUES('8',?1)", [two])?;
            Ok(())
        },
    )
    .await
}

/// Stage `contents` on this device only, each referenced by a row of
/// `files` whose id is the content, in one write, returning the files.
async fn stage_rows(db: &Database, contents: [&'static str; 2]) -> [FileId; 2] {
    db.staged_write::<_, _, _, _, DbError>(
        move |batch| {
            Ok(contents
                .map(|bytes| batch.put_file("files", bytes.as_bytes().to_vec(), Place::DeviceOnly)))
        },
        move |sql, files| {
            for (id, file) in contents.iter().zip(files) {
                sql.execute("INSERT INTO files(id,file) VALUES(?1,?2)", (id, file))?;
            }
            Ok(files)
        },
    )
    .await
    .unwrap()
}

/// The folders in the store's `files` folder, each with the names of the
/// files in it.
fn staging_folders(store: &TestStore) -> Vec<Vec<String>> {
    let files = store.database_path().parent().unwrap().join("files");
    let mut folders: Vec<Vec<String>> = std::fs::read_dir(files)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_dir(), "{:?} is a folder", entry.path());
            let mut names: Vec<String> = std::fs::read_dir(entry.path())
                .unwrap()
                .map(|copy| copy.unwrap().file_name().into_string().unwrap())
                .collect();
            names.sort();
            names
        })
        .collect();
    folders.sort();
    folders
}

/// Each write's staging writes its copies into a folder of its own in the
/// store's `files` folder, and each copy reads back by its record
/// (Appendix C.3).
#[tokio::test]
async fn each_stagings_copies_land_in_a_folder_of_its_own() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let first = stage_rows(&db, ["one", "two"]).await;
    let second = stage_rows(&db, ["three", "four"]).await;
    let folders = staging_folders(&store);
    assert_eq!(folders.iter().map(Vec::len).collect::<Vec<_>>(), [2, 2], "{folders:?}");
    for (file, bytes) in first.into_iter().chain(second).zip(["one", "two", "three", "four"]) {
        let reference = db.file_ref(file).await.unwrap();
        let stream = db.open_local_file(&reference).await.unwrap().expect("its bytes are here");
        assert_eq!(stream.read_at(0, reference.size()).await.unwrap(), bytes.as_bytes());
    }
    db.close().await.unwrap();
}

/// A staging's folder stays while it holds a copy, and goes with its last
/// copy (Appendix C.3).
#[tokio::test]
async fn a_stagings_folder_goes_with_its_last_copy() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    stage_rows(&db, ["one", "two"]).await;
    assert_eq!(staging_folders(&store).iter().map(Vec::len).collect::<Vec<_>>(), [2]);
    for (id, left) in [("one", vec![1]), ("two", vec![])] {
        db.write(move |sql| {
            sql.execute("DELETE FROM files WHERE id=?1", [id])?;
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(staging_folders(&store).iter().map(Vec::len).collect::<Vec<_>>(), left);
    }
    db.close().await.unwrap();
}

/// A write's staged files reach the drive before the commit recording
/// them: the batch is flushed once, for both files, while no row naming
/// them is committed, and the write's commit follows (Appendix C.3).
#[tokio::test(flavor = "current_thread")]
async fn a_writes_staged_files_are_flushed_once_before_its_commit() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let events = Arc::new(Mutex::new(Vec::new()));
    db.test_on_file_flush(Arc::new({
        let (events, path) = (events.clone(), store.database_path());
        move || {
            let rows: i64 = rusqlite::Connection::open(&path)
                .unwrap()
                .query_row("SELECT count(*) FROM files", [], |r| r.get(0))
                .unwrap();
            events.lock().unwrap().push(("flushed", rows));
            Ok(())
        }
    }))
    .await;
    db.test_on_write_checkpoint({
        let events = events.clone();
        move |point| {
            if point == crate::test_utils::WriteCheckpoint::Committed {
                events.lock().unwrap().push(("committed", -1));
            }
        }
    })
    .await
    .unwrap();
    write_two(&db, Arc::new(AtomicBool::new(false))).await.unwrap();
    let events = events.lock().unwrap().clone();
    let flushes: Vec<_> = events.iter().filter(|(event, _)| *event == "flushed").collect();
    assert_eq!(flushes, [&("flushed", 0)], "{events:?}");
    assert_eq!(events.last(), Some(&("committed", -1)), "{events:?}");
    assert_eq!(committed_rows(&store), 2);
    db.close().await.unwrap();
}

/// The system refuses the random key of a file the write stages for the
/// cloud: the write fails, nothing commits, no upload waits, and the bytes it
/// staged are removed. The next write stages its files as usual.
#[tokio::test(flavor = "current_thread")]
async fn a_refused_file_key_fails_the_write_and_commits_nothing() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let error = db
        .staged_write::<_, _, _, _, DbError>(
            |batch| Ok(batch.put_file("files", b"one".to_vec(), crate::Place::Cloud)),
            |sql, file| {
                sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [file])?;
                coven_crypto::refuse_randomness(1);
                Ok(())
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&error, DbError::FileKey(coven_crypto::CryptoError::Unavailable(_))),
        "{error:?}"
    );
    assert_eq!(committed_rows(&store), 0);
    assert!(owned_paths(&store).is_empty());
    assert!(db.unsent().await.unwrap().is_empty());
    assert_eq!(local_count(&db, "main.coven_files"), 0, "the same transaction made its record");
    assert_eq!(local_count(&db, "coven_moves"), 0, "and its move");
    write_two(&db, Arc::new(AtomicBool::new(false))).await.unwrap();
    assert_eq!(committed_rows(&store), 2);
    db.close().await.unwrap();
}

/// A flush that fails fails the write: its SQL never runs, nothing commits,
/// and the bytes it staged are removed.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_flush_fails_the_write_and_commits_nothing() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    db.test_on_file_flush(Arc::new(|| Err(std::io::Error::other("the drive refused the flush"))))
        .await;
    let ran = Arc::new(AtomicBool::new(false));
    let error = write_two(&db, ran.clone()).await.unwrap_err();
    assert!(
        matches!(&error, DbError::Disk(_))
            && error.to_string().contains("the drive refused the flush"),
        "{error:?}"
    );
    assert!(!ran.load(Ordering::SeqCst));
    assert_eq!(committed_rows(&store), 0);
    assert!(owned_paths(&store).is_empty());
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    db.close().await.unwrap();
}

/// Staging a file takes the store's read lock for as long as the file's
/// writer lives; when the lock can't be taken, the write fails with that lock
/// error and commits nothing (§17.3).
#[tokio::test(flavor = "current_thread")]
async fn staging_that_cant_retain_the_store_fails_with_its_lock_error() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    // The readers lock file is made when first needed; a directory in its
    // place can't be opened as one.
    let directory = store.database_path().parent().unwrap().to_owned();
    let readers = directory.with_file_name(format!(".{}.readers.lock", store.id()));
    std::fs::create_dir(&readers).unwrap();
    let ran = Arc::new(AtomicBool::new(false));
    let error = write_two(&db, ran.clone()).await.unwrap_err();
    assert!(
        matches!(&error, DbError::Lock(coven_foundation::files::StoreLockError::File(_))),
        "{error:?}"
    );
    assert!(!ran.load(Ordering::SeqCst));
    assert_eq!(committed_rows(&store), 0);
    assert!(owned_paths(&store).is_empty());
    std::fs::remove_dir(&readers).unwrap();
    db.close().await.unwrap();
}

struct Started<R> {
    reader: R,
    entered: Option<tokio::sync::oneshot::Sender<()>>,
}
impl<R: AsyncRead + Unpin> AsyncRead for Started<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Some(entered) = self.entered.take() {
            entered.send(()).unwrap();
        }
        Pin::new(&mut self.reader).poll_read(cx, out)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_socket_stream_leaves_the_writer_available_and_keeps_its_pending_bytes() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (producer, consumer) = tokio::join!(
        tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept()
    );
    let mut producer = producer.unwrap();
    let (entered, reading) = tokio::sync::oneshot::channel();
    let source = FileSource::Stream(Box::pin(Started {
        reader: consumer.unwrap().0,
        entered: Some(entered),
    }));
    let writing = tokio::spawn({
        let db = db.clone();
        async move {
            db.staged_write::<_, _, _, _, crate::DbError>(
                move |batch| Ok(batch.put_file("files", source, crate::Place::Cloud)),
                |sql, file| {
                    sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [file])?;
                    Ok(file)
                },
            )
            .await
        }
    });
    reading.await.unwrap();
    let unrelated = tokio::time::timeout(HANG_GUARD, db.write(|_| Ok(()))).await;
    // Release the reader before asserting, so a failing implementation can exit.
    producer.write_all(b"socket bytes").await.unwrap();
    producer.shutdown().await.unwrap();
    let file = writing.await.unwrap().unwrap();
    assert!(unrelated.is_ok(), "waiting for file input must not hold the writer");
    assert_eq!(db.file_ref(file).await.unwrap().size(), 12);
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    assert_eq!(std::fs::read(owned_paths(&store).remove(0)).unwrap(), b"socket bytes");
    db.close().await.unwrap();
}

/// Close waits for a staging in progress, finished or cancelled, but not
/// for the staged files it returns (§17.2). Those files outlive the store
/// harmlessly: a write after close fails `StoreClosed`, dropping them
/// panics nothing, the reopened store refuses them as staged elsewhere
/// (`PreparedElsewhere`), and its opening has removed their bytes.
#[tokio::test(flavor = "current_thread")]
async fn close_waits_for_staging_and_reopen_removes_bytes_no_write_took() {
    for cancel in [false, true] {
        let store = TestStore::new();
        let db = open(&store, Provenance::AppProvided).await;
        let (mut producer, reader) = tokio::io::duplex(64);
        let (entered, reading) = tokio::sync::oneshot::channel();
        let source = FileSource::Stream(Box::pin(Started { reader, entered: Some(entered) }));
        let staging = tokio::spawn({
            let db = db.clone();
            async move {
                db.stage_files::<_, _, crate::DbError>(move |batch| {
                    Ok(batch.put_file("files", source, crate::Place::Cloud))
                })
                .await
            }
        });
        reading.await.unwrap();
        let mut closing = std::pin::pin!(db.close());
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(closing.as_mut(), cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let staged = if cancel {
            staging.abort();
            assert!(staging.await.unwrap_err().is_cancelled());
            None
        } else {
            producer.write_all(b"bytes").await.unwrap();
            producer.shutdown().await.unwrap();
            Some(staging.await.unwrap().unwrap())
        };
        tokio::time::timeout(HANG_GUARD, closing).await.unwrap().unwrap();
        assert_eq!(owned_paths(&store).len(), 1);
        let sql = rusqlite::Connection::open(store.database_path()).unwrap();
        assert_eq!(
            sql.query_row("SELECT count(*) FROM _coven_file_removals", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        drop(sql);
        let reopened = open(&store, Provenance::AppProvided).await;
        assert!(owned_paths(&store).is_empty());
        assert_eq!(local_count(&reopened, "_coven_file_removals"), 0);
        if let Some(staged) = staged {
            assert!(matches!(db.write(|_| Ok(())).await, Err(crate::DbError::StoreClosed)));
            let refused = reopened
                .write(move |sql| {
                    let file = sql.put_staged(staged)?;
                    sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [file])?;
                    Ok(())
                })
                .await;
            assert!(matches!(refused, Err(crate::DbError::PreparedElsewhere)), "{refused:?}");
        }
        reopened.close().await.unwrap();
    }
}

/// Files staged and dropped without a write keep no bytes once the next
/// write has run; files staged in one open store are refused by another's
/// write (§17.2).
#[tokio::test]
async fn staged_files_no_write_takes_go_with_the_next_write() {
    let store = TestStore::new();
    let other_store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let other = open(&other_store, Provenance::AppProvided).await;
    async fn stage(
        db: &crate::Database,
    ) -> Result<crate::StagedFiles<coven_foundation::id_source::FileId>, DbError> {
        db.stage_files(|batch| Ok(batch.put_file("files", b"photo".to_vec(), crate::Place::Cloud)))
            .await
    }
    drop(stage(&db).await.unwrap());
    assert_eq!(owned_paths(&store).len(), 1);
    db.write(|_| Ok(())).await.unwrap();
    assert!(owned_paths(&store).is_empty());
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    let staged = stage(&db).await.unwrap();
    let refused = other
        .write(move |sql| {
            let file = sql.put_staged(staged)?;
            sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [file])?;
            Ok(())
        })
        .await;
    assert!(matches!(refused, Err(crate::DbError::PreparedElsewhere)), "{refused:?}");
    db.write(|_| Ok(())).await.unwrap();
    assert!(owned_paths(&store).is_empty());
    db.close().await.unwrap();
    other.close().await.unwrap();
}

#[test]
fn a_file_reader_and_writer_can_share_one_blocking_worker() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let store = TestStore::new();
        let db = open(&store, Provenance::AppProvided).await;
        let original = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(original.path(), vec![9; 3 * 64 * 1024 + 17]).unwrap();
        let reader = tokio::fs::File::open(original.path()).await.unwrap();
        let file = tokio::time::timeout(
            HANG_GUARD,
            db.staged_write::<_, _, _, _, crate::DbError>(
                move |batch| {
                    Ok(batch.put_file(
                        "files",
                        FileSource::Stream(Box::pin(reader)),
                        crate::Place::Cloud,
                    ))
                },
                |sql, file| {
                    sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [file])?;
                    Ok(file)
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(db.file_ref(file).await.unwrap().size(), 3 * 64 * 1024 + 17);
        db.close().await.unwrap();
    });
}

/// Staged files a write's closure drops without putting them in rows go
/// free without taking the store's own lock: here a close is waiting for
/// that lock while the write holds it, and the write still finishes, as
/// does the close (§17.3).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_files_dropped_in_a_write_go_free_while_the_store_closes() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let staged = db
        .stage_files::<_, _, DbError>(|batch| {
            Ok(batch.put_file("files", b"photo".to_vec(), crate::Place::Cloud))
        })
        .await
        .unwrap();
    let (entered, entering) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let writing = tokio::spawn({
        let db = db.clone();
        async move {
            db.write(move |_sql| {
                entered.send(()).unwrap();
                released.recv().unwrap();
                drop(staged);
                Ok(())
            })
            .await
        }
    });
    tokio::task::spawn_blocking(move || entering.recv().unwrap()).await.unwrap();
    let close_waits = db.observe_close_wait();
    let closing = tokio::spawn({
        let db = db.clone();
        async move { db.close().await }
    });
    // The close reaches the store's lock and waits for the write holding it.
    tokio::time::timeout(HANG_GUARD, close_waits).await.unwrap().unwrap();
    release.send(()).unwrap();
    tokio::time::timeout(HANG_GUARD, writing).await.unwrap().unwrap().unwrap();
    tokio::time::timeout(HANG_GUARD, closing).await.unwrap().unwrap().unwrap();
}
