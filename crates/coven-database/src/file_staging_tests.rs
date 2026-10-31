use crate::{
    file_write::tests::{local_count, open, owned_paths},
    tests::TestStore,
    *,
};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
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
            db.file_tasks.clone().read_owned().await,
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
        let (staging, result) = staging.write().await;
        let _flushed = result.unwrap();
        staging
    });
    assert_eq!(owned_paths(&store).len(), 1);
    let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(staging)));
    runtime.block_on(db.write(|_| Ok(()))).unwrap();
    let remaining = owned_paths(&store);
    runtime.block_on(db.close()).unwrap();
    assert!(
        dropped.is_ok(),
        "cancelling staging must not need a runtime"
    );
    assert!(remaining.is_empty());
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
    db.write_with_files::<_, _, _, _, DbError>(
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
    write_two(&db, Arc::new(AtomicBool::new(false)))
        .await
        .unwrap();
    let events = events.lock().unwrap().clone();
    let flushes: Vec<_> = events
        .iter()
        .filter(|(event, _)| *event == "flushed")
        .collect();
    assert_eq!(flushes, [&("flushed", 0)], "{events:?}");
    assert_eq!(events.last(), Some(&("committed", -1)), "{events:?}");
    assert_eq!(committed_rows(&store), 2);
    db.close().await.unwrap();
}

/// A flush that fails fails the write: its SQL never runs, nothing commits,
/// and the bytes it staged are removed.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_flush_fails_the_write_and_commits_nothing() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    db.test_on_file_flush(Arc::new(|| {
        Err(std::io::Error::other("the drive refused the flush"))
    }))
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
            db.write_with_files::<_, _, _, _, crate::DbError>(
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
    let unrelated = tokio::time::timeout(Duration::from_secs(1), db.write(|_| Ok(()))).await;
    // Release the reader before asserting, so a failing implementation can exit.
    producer.write_all(b"socket bytes").await.unwrap();
    producer.shutdown().await.unwrap();
    let file = writing.await.unwrap().unwrap();
    assert!(
        unrelated.is_ok(),
        "waiting for file input must not hold the writer"
    );
    assert_eq!(db.file_ref(file).await.unwrap().size(), 12);
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    assert_eq!(
        std::fs::read(owned_paths(&store).remove(0)).unwrap(),
        b"socket bytes"
    );
    db.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn close_waits_for_staging_and_reopen_removes_cancelled_bytes() {
    for cancel in [false, true] {
        let store = TestStore::new();
        let db = open(&store, Provenance::AppProvided).await;
        let (mut producer, reader) = tokio::io::duplex(64);
        let (entered, reading) = tokio::sync::oneshot::channel();
        let source = FileSource::Stream(Box::pin(Started {
            reader,
            entered: Some(entered),
        }));
        let writing = tokio::spawn({
            let db = db.clone();
            async move {
                db.write_with_files::<_, _, _, _, crate::DbError>(
                    move |batch| Ok(batch.put_file("files", source, crate::Place::Cloud)),
                    |sql, file| {
                        sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [file])?;
                        Ok(())
                    },
                )
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
        if cancel {
            writing.abort();
            assert!(writing.await.unwrap_err().is_cancelled());
        } else {
            producer.write_all(b"bytes").await.unwrap();
            producer.shutdown().await.unwrap();
            writing.await.unwrap().unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), closing)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(owned_paths(&store).len(), 1);
        let sql = rusqlite::Connection::open(store.database_path()).unwrap();
        assert_eq!(
            sql.query_row("SELECT count(*) FROM _coven_file_removals", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            i64::from(cancel)
        );
        assert_eq!(
            sql.query_row("SELECT count(*) FROM files", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            i64::from(!cancel)
        );
        drop(sql);
        let reopened = open(&store, Provenance::AppProvided).await;
        assert_eq!(owned_paths(&store).len(), usize::from(!cancel));
        assert_eq!(local_count(&reopened, "_coven_file_removals"), 0);
        reopened.close().await.unwrap();
    }
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
            Duration::from_secs(5),
            db.write_with_files::<_, _, _, _, crate::DbError>(
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
