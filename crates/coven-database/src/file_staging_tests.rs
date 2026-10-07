use crate::{
    file_write::tests::{local_count, owned_paths, tables, SCHEMA},
    tests::TestStore,
    *,
};
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};

#[test]
fn cancelled_staging_drops_outside_a_runtime_and_the_next_write_removes_its_bytes() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let store = TestStore::new();
    let db = runtime
        .block_on(store.schema(tables(Provenance::AppProvided), SCHEMA))
        .unwrap();
    let staging = runtime.block_on(async {
        let staging = super::FileStaging::new(
            db.clone(),
            db.file_tasks.clone().read_owned().await,
            |batch| {
                batch.put_file("files", "7", b"unclaimed".to_vec());
                Ok::<_, DbError>(())
            },
        )
        .unwrap();
        let (staging, result) = staging.write().await;
        result.unwrap();
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
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
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
            db.write_with_files(
                move |batch| {
                    batch.put_file("files", "7", source);
                    Ok(())
                },
                |sql| {
                    sql.execute("INSERT INTO files(id) VALUES('7')", [])?;
                    Ok(())
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
    writing.await.unwrap().unwrap();
    assert!(
        unrelated.is_ok(),
        "waiting for file input must not hold the writer"
    );
    assert_eq!(
        db.file_ref("files", "7").await.unwrap().plaintext_size(),
        12
    );
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
        let db = store
            .schema(tables(Provenance::AppProvided), SCHEMA)
            .await
            .unwrap();
        let (mut producer, reader) = tokio::io::duplex(64);
        let (entered, reading) = tokio::sync::oneshot::channel();
        let source = FileSource::Stream(Box::pin(Started {
            reader,
            entered: Some(entered),
        }));
        let writing = tokio::spawn({
            let db = db.clone();
            async move {
                db.write_with_files(
                    move |batch| {
                        batch.put_file("files", "7", source);
                        Ok(())
                    },
                    |sql| {
                        sql.execute("INSERT INTO files(id) VALUES('7')", [])?;
                        Ok(())
                    },
                )
                .await
            }
        });
        reading.await.unwrap();
        let mut closing = tokio::spawn({
            let db = db.clone();
            async move { db.close().await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut closing)
                .await
                .is_err()
        );
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
        let reopened = store
            .schema(tables(Provenance::AppProvided), SCHEMA)
            .await
            .unwrap();
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
        let db = store
            .schema(tables(Provenance::AppProvided), SCHEMA)
            .await
            .unwrap();
        let original = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(original.path(), vec![9; 3 * 64 * 1024 + 17]).unwrap();
        let reader = tokio::fs::File::open(original.path()).await.unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            db.write_with_files(
                move |batch| {
                    batch.put_file("files", "7", FileSource::Stream(Box::pin(reader)));
                    Ok(())
                },
                |sql| {
                    sql.execute("INSERT INTO files(id) VALUES('7')", [])?;
                    Ok(())
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            db.file_ref("files", "7").await.unwrap().plaintext_size(),
            3 * 64 * 1024 + 17
        );
        db.close().await.unwrap();
    });
}
