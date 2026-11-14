//! The file layer's storage states (§17.7): reads wait while a connection is
//! coming, fail at once when none is, and fail as the store closes.
use super::{Fixture, CHUNK};
use crate::FileReadError;
use coven_database::{DbError, Provenance};
use coven_foundation::clock::HANG_GUARD;
use coven_foundation::files::FileLease;
use std::sync::Arc;

/// A read that needs storage, asked while a connection is coming, waits for
/// the connection and then reads the file.
#[tokio::test]
async fn a_read_while_a_connection_is_coming_waits_for_it() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("waiting", vec![5; CHUNK]).await;
    f.files.set_storage(None, std::future::ready(Ok(()))).await.unwrap();
    let coming = f.files.expect_connection();
    let files = f.files.clone();
    let reading =
        tokio::spawn(async move { files.read_file(&file, &FileLease::for_test().await).await });
    while f.files.inner.storage.receiver_count() == 0 {
        tokio::task::yield_now().await;
    }
    assert!(!reading.is_finished(), "the read answered while a connection was coming");
    f.files.set_storage(Some(f.storage.clone()), std::future::ready(Ok(()))).await.unwrap();
    let bytes = tokio::time::timeout(HANG_GUARD, reading).await.unwrap().unwrap();
    assert_eq!(bytes.unwrap(), vec![5; CHUNK]);
    drop(coming);
    f.close().await;
}

/// A read the cache serves needs no storage, so it answers while a
/// connection is coming.
#[tokio::test]
async fn a_cached_read_does_not_wait_for_a_connection() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("cached", vec![6; CHUNK]).await;
    assert_eq!(
        f.files.read_file(&file, &FileLease::for_test().await).await.unwrap(),
        vec![6; CHUNK]
    );
    f.files.set_storage(None, std::future::ready(Ok(()))).await.unwrap();
    let _coming = f.files.expect_connection();
    let read =
        tokio::time::timeout(HANG_GUARD, f.files.read_file(&file, &FileLease::for_test().await))
            .await
            .unwrap();
    assert_eq!(read.unwrap(), vec![6; CHUNK]);
    f.close().await;
}

/// An attempt that ends without a connection leaves the state released: the
/// read waiting for it fails with no storage.
#[tokio::test]
async fn a_read_waiting_for_an_attempt_that_ends_without_storage_fails() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("ended", vec![7; CHUNK]).await;
    f.files.set_storage(None, std::future::ready(Ok(()))).await.unwrap();
    let coming = f.files.expect_connection();
    let files = f.files.clone();
    let reading =
        tokio::spawn(async move { files.read_file(&file, &FileLease::for_test().await).await });
    while f.files.inner.storage.receiver_count() == 0 {
        tokio::task::yield_now().await;
    }
    drop(coming);
    let read = tokio::time::timeout(HANG_GUARD, reading).await.unwrap().unwrap();
    assert!(matches!(read, Err(FileReadError::NoStorage)), "{read:?}");
    f.close().await;
}

/// Closing ends a waiting read with the store's closing, and a read after
/// the close fails the same.
#[tokio::test]
async fn closing_ends_a_waiting_read_with_store_closed() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("closing", vec![8; CHUNK]).await;
    f.files.set_storage(None, std::future::ready(Ok(()))).await.unwrap();
    let _coming = f.files.expect_connection();
    let files = f.files.clone();
    let waiting = file.clone();
    let reading =
        tokio::spawn(async move { files.read_file(&waiting, &FileLease::for_test().await).await });
    while f.files.inner.storage.receiver_count() == 0 {
        tokio::task::yield_now().await;
    }
    f.files.close().await;
    let read = tokio::time::timeout(HANG_GUARD, reading).await.unwrap().unwrap();
    assert!(matches!(read, Err(FileReadError::Database(DbError::StoreClosed))), "{read:?}");
    let after = f.files.read_file(&file, &FileLease::for_test().await).await;
    assert!(matches!(after, Err(FileReadError::Database(DbError::StoreClosed))), "{after:?}");
    f.database.close().await.unwrap();
}

/// A caller that drops its waiting read leaves nothing: no waiter on the
/// state, and the connection that lands finds no lock held.
#[tokio::test]
async fn a_dropped_waiting_read_leaves_no_waiter_and_holds_no_lock() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("dropped", vec![9; CHUNK]).await;
    f.files.set_storage(None, std::future::ready(Ok(()))).await.unwrap();
    let _coming = f.files.expect_connection();
    let files = f.files.clone();
    let reading =
        tokio::spawn(async move { files.read_file(&file, &FileLease::for_test().await).await });
    while f.files.inner.storage.receiver_count() == 0 {
        tokio::task::yield_now().await;
    }
    reading.abort();
    assert!(reading.await.unwrap_err().is_cancelled());
    assert_eq!(f.files.inner.storage.receiver_count(), 0, "a waiter outlived its read");
    tokio::time::timeout(
        HANG_GUARD,
        f.files.set_storage(Some(f.storage.clone()), std::future::ready(Ok(()))),
    )
    .await
    .unwrap()
    .unwrap();
    f.close().await;
}

/// A pin's download, holding the pins' lock, never waits for a connection: it
/// fails with no storage, as it does when none is coming.
#[tokio::test]
async fn a_pin_does_not_wait_for_a_connection() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("pinned", vec![4; CHUNK]).await;
    f.files.set_storage(None, std::future::ready(Ok(()))).await.unwrap();
    let _coming = f.files.expect_connection();
    let pin =
        tokio::time::timeout(HANG_GUARD, f.files.pin(std::slice::from_ref(&file))).await.unwrap();
    assert!(matches!(pin, Err(FileReadError::NoStorage)), "{pin:?}");
    f.close().await;
}

/// The connection number moves with every change of storage, and a failure
/// that needs a new connection is due again only then.
#[tokio::test]
async fn a_connection_wait_ends_when_the_connection_changes() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let subject = coven_database::RetrySubject::Provider;
    let inner = Arc::clone(&f.files.inner);
    assert!(inner.connection_due(subject));
    inner.wait_for_connection(subject);
    assert!(!inner.connection_due(subject));
    f.files.set_storage(None, std::future::ready(Ok(()))).await.unwrap();
    assert!(inner.connection_due(subject));
    f.close().await;
}
