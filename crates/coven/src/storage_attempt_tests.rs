//! What a file read does by why storage is not connected: wait while a
//! connection is coming, fail at once when none will, fail as the store
//! closes.
use super::*;

/// A fixture whose store holds one uploaded file no cache has seen, with sync
/// stopped.
async fn stopped_with_file() -> (Fixture, FileRef) {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    f.handle.test_hold_passes(true).await;
    let file = f
        .handle
        .test_staged_write(
            |batch| {
                let bytes: Vec<u8> = vec![7; 2048];
                Ok::<_, CovenError>(batch.put_file("covers", bytes, crate::Place::Cloud))
            },
            |sql, file: FileId| {
                sql.execute("INSERT INTO covers(id,file) VALUES('a',?1)", (&file,))?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    f.handle.retry_uploads_now().await.unwrap();
    f.handle.test_hold_passes(false).await;
    match f.handle.retry_now().await.unwrap().await {
        RetryOutcome::Synced { .. } => {}
        outcome => panic!("{outcome:?}"),
    }
    let file = f.handle.file_ref(file).await.unwrap();
    stop(&f).await;
    (f, file)
}

/// Start sync against a connector that fails, and wait until the attempt
/// has failed and backs off.
async fn backing_off(f: &Fixture) {
    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Network.with_source("offline"));
    f.handle.start_sync().await.unwrap();
    last_pass(&f.handle, |last| {
        matches!(last, Some(LastPass::Offline { .. } | LastPass::Failed { .. }))
    })
    .await;
}

/// A read asked while a connection attempt backs off waits, and reads the
/// file once the attempt connects.
#[tokio::test]
async fn a_read_during_a_connection_attempt_waits_for_it() {
    let (f, file) = stopped_with_file().await;
    backing_off(&f).await;
    let handle = f.handle.clone();
    let reading = tokio::spawn(async move { handle.read_file(&file).await });
    f.handle.test_parked().await;
    assert!(!reading.is_finished(), "the read answered while a connection was coming");
    let wait = f.handle.test_connection_retry_wait().await.expect("the attempt's wait");
    f.advance(wait.wait);
    let bytes = tokio::time::timeout(HANG_GUARD, reading).await.unwrap().unwrap();
    assert_eq!(bytes.unwrap(), vec![7; 2048]);
    f.handle.close().await.unwrap();
}

/// With sync stopped nothing is coming, so a read fails at once.
#[tokio::test]
async fn a_read_while_sync_is_stopped_fails_at_once() {
    let (f, file) = stopped_with_file().await;
    let read = tokio::time::timeout(HANG_GUARD, f.handle.read_file(&file)).await.unwrap();
    assert!(matches!(read, Err(FileReadError::NoStorage)), "{read:?}");
    f.handle.close().await.unwrap();
}

/// Closing the store ends a read waiting for a connection with the store's
/// closing.
#[tokio::test]
async fn closing_ends_a_waiting_read_with_store_closed() {
    let (f, file) = stopped_with_file().await;
    backing_off(&f).await;
    let handle = f.handle.clone();
    let reading = tokio::spawn(async move { handle.read_file(&file).await });
    f.handle.test_parked().await;
    f.handle.close().await.unwrap();
    let read = tokio::time::timeout(HANG_GUARD, reading).await.unwrap().unwrap();
    assert!(
        matches!(read, Err(FileReadError::Database(coven_database::DbError::StoreClosed))),
        "{read:?}"
    );
}

/// A read asked after the store closed fails with the store's closing.
#[tokio::test]
async fn a_read_after_close_fails_with_store_closed() {
    let (f, file) = stopped_with_file().await;
    f.handle.close().await.unwrap();
    let read = f.handle.read_file(&file).await;
    assert!(
        matches!(read, Err(FileReadError::Database(coven_database::DbError::StoreClosed))),
        "{read:?}"
    );
}

/// A caller that drops its waiting read leaves nothing: the connection
/// lands, nothing holds the file's locks, and closing returns.
#[tokio::test]
async fn a_dropped_waiting_read_leaves_nothing_behind() {
    let (f, file) = stopped_with_file().await;
    backing_off(&f).await;
    let handle = f.handle.clone();
    let reading = tokio::spawn(async move { handle.read_file(&file).await });
    f.handle.test_parked().await;
    reading.abort();
    assert!(reading.await.unwrap_err().is_cancelled());
    let wait = f.handle.test_connection_retry_wait().await.expect("the attempt's wait");
    f.advance(wait.wait);
    synced(&f.handle).await;
    tokio::time::timeout(HANG_GUARD, f.handle.close()).await.unwrap().unwrap();
}
