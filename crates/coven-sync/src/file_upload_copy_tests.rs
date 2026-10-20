//! The uploading device keeps coven's own copy of a file it must cache
//! instead of downloading what it just stored (§17.6, §17.7).

use super::{Fixture, CHUNK};
use crate::files::file_error::*;
use coven_database::{CacheFill, EagerCacheFillStatus, FileRef, Provenance};
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
/// eager worker runs.
async fn nothing_to_download(f: &Fixture, file: &FileRef) {
    for fill in [false, true] {
        if fill {
            f.files.inner.fill_eager().await.unwrap();
        }
        assert!(f
            .files
            .inner
            .database
            .eager_work()
            .await
            .unwrap()
            .is_empty());
        assert!(f
            .files
            .inner
            .database
            .pin_requests()
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            f.database.eager_cache_fill_status().await.unwrap(),
            EagerCacheFillStatus::Idle
        );
        assert_eq!(f.files.inner.database.missing_bytes(file).await.unwrap(), 0);
    }
}

#[tokio::test]
async fn an_eager_upload_keeps_its_copy_and_downloads_nothing() {
    // One request, then a session of parts sealed again from the copy.
    for size in [CHUNK / 2 + 7, CHUNK * 3 + 19] {
        let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheEager).await;
        let bytes = (0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let mut eager = f.database.subscribe_eager_cache_fill_status();
        assert_eq!(eager.next().await.unwrap(), EagerCacheFillStatus::Idle);
        let file = f.uploaded("cover", bytes.clone()).await;
        nothing_to_download(&f, &file).await;
        assert_eq!(file_reads(&f, &file).await, 0);
        assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
        assert_eq!(file_reads(&f, &file).await, 0);
        // The status never left idle while the upload completed.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), eager.next())
                .await
                .is_err()
        );
        cached_as_stored(&f, &file).await;
        // An unpinned copy is under the namespace budget like any download,
        // and evicting it requests nothing again.
        assert!(!f
            .database
            .rows_pinned("files", vec!["cover".into()])
            .await
            .unwrap()[0]
            .unwrap());
        f.files.set_cache_budget("files", 0).await.unwrap();
        assert_eq!(
            f.files.inner.database.missing_bytes(&file).await.unwrap(),
            file.plaintext_size()
        );
        assert!(f
            .files
            .inner
            .database
            .eager_work()
            .await
            .unwrap()
            .is_empty());
        f.close().await;
    }
}

#[tokio::test]
async fn a_pin_taken_while_queued_keeps_the_uploaded_copy_pinned() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let bytes = vec![61; CHUNK * 2 + 5];
    let attached = f.attach("files", "pinned", bytes.clone()).await;
    f.files.pin(std::slice::from_ref(&attached)).await.unwrap();
    f.drain().await;
    assert!(!f.queued(&attached).await);
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
    // The pinned copy outlives a budget of nothing.
    f.files.set_cache_budget("files", 0).await.unwrap();
    assert_eq!(f.files.read_file(&attached).await.unwrap(), bytes);
    assert_eq!(file_reads(&f, &attached).await, 0);
    cached_as_stored(&f, &attached).await;
    f.close().await;
}

/// A pin keeps a file only while its row names it: a row deleted during the
/// upload leaves no pinned copy outside the budget.
#[tokio::test]
async fn a_pinned_upload_whose_row_is_gone_keeps_no_copy() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
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
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
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
/// eager copy comes from storage.
#[tokio::test]
async fn an_eager_original_is_downloaded_after_its_upload() {
    let f = Fixture::new(Provenance::UserProvided, CacheFill::CacheEager).await;
    let file = f.original("song", &[63; CHUNK + 9]).await;
    f.drain().await;
    assert!(!f.queued(&file).await);
    assert_eq!(
        f.files.inner.database.eager_work().await.unwrap(),
        vec![file.clone()]
    );
    f.close().await;
}

/// Storage stored the file but its reply was lost; after reopening, the
/// retry settles the occupied path and keeps the copy in the transaction
/// that completes the upload.
#[tokio::test]
async fn a_restart_after_a_lost_reply_still_keeps_the_copy() {
    let mut f = Fixture::new(Provenance::AppProvided, CacheFill::CacheEager).await;
    let bytes = vec![64; CHUNK / 2];
    let file = f.attach("files", "cover", bytes.clone()).await;
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
    f.reopen().await;
    f.drain().await;
    assert!(!f.queued(&file).await);
    nothing_to_download(&f, &file).await;
    let settled = file_reads(&f, &file).await;
    assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
    assert_eq!(file_reads(&f, &file).await, settled);
    cached_as_stored(&f, &file).await;
    f.close().await;
}
