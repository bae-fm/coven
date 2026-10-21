//! The uploading device keeps coven's own copy of a file pinned here
//! instead of downloading what it just stored (§17.6, §17.7).

use super::{Fixture, CHUNK};
use crate::files::file_error::*;
use coven_database::{FileRef, Provenance};
use coven_format::file::FILE_HEADER_LEN;
use coven_storage::{test_utils::Faults, Storage};

/// Opened responses for this file's stored object.
async fn file_reads(f: &Fixture, file: &FileRef) -> usize {
    let path = f.path(file);
    f.storage
        .reads()
        .await
        .into_iter()
        .filter(|(read, _, _)| *read == path)
        .count()
}

/// The cache copy holds the stored object's exact bytes: its header and
/// every sealed chunk.
async fn cached_as_stored(f: &Fixture, file: &FileRef) {
    let database = &f.files.inner.database;
    let mut cached = database.cached(file, -1).await.unwrap().unwrap();
    assert_eq!(cached.len(), FILE_HEADER_LEN);
    let chunks = file.plaintext_size().div_ceil(CHUNK as u64);
    for index in 0..chunks {
        cached.extend(database.cached(file, index as i64).await.unwrap().unwrap());
    }
    assert_eq!(cached, f.storage.read(&f.path(file)).await.unwrap());
}

/// Nothing is waiting to download, as the upload completed and after the
/// pin worker runs.
async fn nothing_to_download(f: &Fixture, file: &FileRef) {
    for run in [false, true] {
        if run {
            f.files.inner.keep_pins().await.unwrap();
        }
        assert!(f
            .files
            .inner
            .database
            .pin_requests()
            .await
            .unwrap()
            .is_empty());
        assert_eq!(f.files.inner.database.missing_bytes(file).await.unwrap(), 0);
    }
}

/// Pin the file while it is queued, then upload it.
async fn pinned_upload(f: &Fixture, id: &str, bytes: Vec<u8>) -> FileRef {
    let file = f.attach("files", id, bytes).await;
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    f.drain().await;
    assert!(!f.queued(&file).await);
    file
}

/// One request, or a session of parts sealed again from the copy: either
/// way the pinned copy is the stored object, byte for byte, and outlives a
/// budget of nothing without a request.
#[tokio::test]
async fn a_pin_taken_while_queued_keeps_the_uploaded_copy_pinned() {
    for size in [CHUNK / 2 + 7, CHUNK * 2 + 5, CHUNK * 3 + 19] {
        let f = Fixture::new(Provenance::AppProvided).await;
        let bytes = (0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let attached = pinned_upload(&f, "pinned", bytes.clone()).await;
        nothing_to_download(&f, &attached).await;
        assert!(f
            .database
            .rows_pinned("files", vec!["pinned".into()])
            .await
            .unwrap()[0]
            .unwrap());
        let progress = f
            .database
            .pin_progress(vec![attached.clone()])
            .await
            .unwrap();
        assert_eq!(progress.files_completed, 1);
        f.files.set_cache_budget("files", 0).await.unwrap();
        assert_eq!(f.files.read_file(&attached).await.unwrap(), bytes);
        assert_eq!(file_reads(&f, &attached).await, 0);
        cached_as_stored(&f, &attached).await;
        f.close().await;
    }
}

/// A pin keeps a file only while its row names it: a row deleted during the
/// upload leaves no pinned copy outside the budget.
#[tokio::test]
async fn a_pinned_upload_whose_row_is_gone_keeps_no_copy() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.attach("files", "gone", vec![65; CHUNK + 1]).await;
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    f.database
        .write(|sql| {
            sql.execute("DELETE FROM files WHERE id='gone'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    f.drain().await;
    assert!(!f.queued(&file).await);
    assert!(f
        .files
        .inner
        .database
        .cached(&file, -1)
        .await
        .unwrap()
        .is_none());
    assert!(!coven_database::FileDatabase::new(f.database.clone())
        .upload_keeps_copy(&file)
        .await
        .unwrap());
    f.close().await;
}

#[tokio::test]
async fn a_file_nothing_keeps_here_leaves_no_copy() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("lazy", vec![62; CHUNK + 3]).await;
    assert_eq!(
        f.files.inner.database.missing_bytes(&file).await.unwrap(),
        file.plaintext_size()
    );
    assert!(f
        .files
        .inner
        .database
        .cached(&file, -1)
        .await
        .unwrap()
        .is_none());
    f.close().await;
}

/// A user's original is never read again after its upload (§17.1), so its
/// pin is carried to the stored file, which comes from storage.
#[tokio::test]
async fn a_pinned_original_is_downloaded_after_its_upload() {
    let f = Fixture::new(Provenance::UserProvided).await;
    let file = f.original("song", &[63; CHUNK + 9]).await;
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    f.drain().await;
    assert!(!f.queued(&file).await);
    assert_eq!(
        f.files.inner.database.pin_requests().await.unwrap(),
        vec![file.clone()]
    );
    f.close().await;
}

/// Storage stored a pinned file but its reply was lost, a crash between
/// storage confirming it and the completing transaction; after reopening,
/// the retry settles the occupied path, names the file in its row exactly
/// once and keeps the copy in that transaction.
#[tokio::test]
async fn a_restart_after_a_lost_reply_still_keeps_the_copy() {
    let mut f = Fixture::new(Provenance::AppProvided).await;
    let bytes = vec![64; CHUNK / 2];
    let file = f.attach("files", "cover", bytes.clone()).await;
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    f.storage
        .set_faults(Faults {
            lose_completion_reply: true,
            ..Faults::none()
        })
        .await;
    let guard = f.files.inner.drain.lock().await;
    f.files
        .inner
        .paused
        .store(false, std::sync::atomic::Ordering::Release);
    let item = f.files.inner.database.uploads().await.unwrap().remove(0);
    assert!(matches!(
        f.files.inner.upload(&item).await,
        Err(TransferError::File(FileReadError::Storage(_)))
    ));
    f.files
        .inner
        .paused
        .store(true, std::sync::atomic::Ordering::Release);
    drop(guard);
    assert!(f.queued(&file).await);
    // Storage holds the file, but no transaction completed it: the row
    // names nothing yet.
    let writes = f.database.test_queued_writes().await.unwrap().len();
    f.reopen().await;
    f.drain().await;
    assert!(!f.queued(&file).await);
    // The retry completed it exactly once: one write names the file.
    assert_eq!(
        f.database.test_queued_writes().await.unwrap().len(),
        writes + 1
    );
    f.drain().await;
    assert_eq!(
        f.database.test_queued_writes().await.unwrap().len(),
        writes + 1
    );
    nothing_to_download(&f, &file).await;
    let settled = file_reads(&f, &file).await;
    assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
    assert_eq!(file_reads(&f, &file).await, settled);
    cached_as_stored(&f, &file).await;
    f.close().await;
}
