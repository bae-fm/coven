use super::*;
use crate::files::{FileArea, FileName, StoreDirAccess, StoreDirectories};
use crate::id_source::{StoreId, UuidIds};
use std::sync::atomic::{AtomicUsize, Ordering};

fn store(root: &tempfile::TempDir) -> crate::files::StoreDir {
    StoreDirectories::new(root.path().to_owned(), crate::task::HeardBugs::shared())
        .create_store_dir(StoreId(uuid::Uuid::from_u128(123)), &UuidIds)
        .unwrap()
}

/// Write `bytes` as `name` in `area`, finished into `batch`.
async fn write(store: &crate::files::StoreDir, area: FileArea, name: &str, batch: &mut FlushBatch) {
    store
        .create_writer(area, &FileName::new(name).unwrap())
        .unwrap()
        .write_from(&mut b"bytes".as_slice(), |_| {}, batch)
        .await
        .unwrap();
}

/// Each folder is synced once however many files the batch made there.
/// Until the flush ends, the batch holds the store read lock of a file in
/// each folder: the flush is still part of those files' writes.
#[tokio::test]
async fn a_batch_keeps_each_folder_once_and_the_store_until_it_is_flushed() {
    let root = tempfile::tempdir().unwrap();
    let store = store(&root);
    let mut batch = FlushBatch::new(crate::task::HeardBugs::shared());
    for name in ["one", "two", "three"] {
        write(&store, FileArea::AppProvided, name, &mut batch).await;
    }
    write(&store, FileArea::Cache, "four", &mut batch).await;
    assert_eq!(batch.files, 4);
    #[cfg(unix)]
    {
        assert_eq!(batch.folders.len(), 2);
        assert!(matches!(
            store.lock_for_deletion(),
            Err(crate::files::StoreLockError::AlreadyOpen(_))
        ));
    }
    let _flushed = batch.flush().await.unwrap();
    assert!(store.lock_for_deletion().unwrap().is_some());
}

/// The files of one held folder share its single read lock: with the folder
/// let go, the store stays held until the batch they finished into flushes.
#[cfg(unix)]
#[tokio::test]
async fn files_made_in_a_held_folder_keep_the_store_until_their_batch_flushes() {
    let root = tempfile::tempdir().unwrap();
    let store = store(&root);
    let folder = store.writer_folder(FileArea::AppProvided).unwrap();
    let mut batch = FlushBatch::new(crate::task::HeardBugs::shared());
    for name in ["one", "two", "three"] {
        folder
            .create(&FileName::new(name).unwrap())
            .unwrap()
            .write_from(&mut b"bytes".as_slice(), |_| {}, &mut batch)
            .await
            .unwrap();
    }
    drop(folder);
    assert_eq!(batch.files, 3);
    assert!(matches!(store.lock_for_deletion(), Err(crate::files::StoreLockError::AlreadyOpen(_))));
    let _flushed = batch.flush().await.unwrap();
    assert!(store.lock_for_deletion().unwrap().is_some());
}

/// A flush that fails fails the batch, so the commit that would record its
/// files never gets the `Flushed` it takes. An empty batch flushes nothing.
#[tokio::test]
async fn a_failed_flush_fails_the_batch_and_an_empty_one_flushes_nothing() {
    let calls = std::sync::Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let hook: FlushHook = std::sync::Arc::new(move || {
        counted.fetch_add(1, Ordering::SeqCst);
        Err(std::io::Error::other("the drive refused the flush"))
    });
    let _flushed = FlushBatch::new(crate::task::HeardBugs::shared())
        .with_hook(Some(hook.clone()))
        .flush()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let root = tempfile::tempdir().unwrap();
    let store = store(&root);
    let mut batch = FlushBatch::new(crate::task::HeardBugs::shared()).with_hook(Some(hook));
    write(&store, FileArea::AppProvided, "one", &mut batch).await;
    let error = batch.flush().await.unwrap_err();
    assert!(error.to_string().contains("the drive refused the flush"), "{error}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
