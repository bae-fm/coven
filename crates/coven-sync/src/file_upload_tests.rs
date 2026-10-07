use super::{Fixture, CHUNK};
use crate::files::file_error::*;
use crate::files::file_upload::*;
use coven_database::{CacheFill, FileLocation, Provenance, Uploads};
use coven_foundation::files::FileArea;
use coven_storage::{test_utils::Faults, Storage};
use coven_storage::{ObjectPath, UploadSession};
use std::time::Duration;

#[tokio::test]
async fn an_upload_reader_retains_the_store_after_its_database_closes() {
    let f = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let file = f.attach("files", "held", vec![45; CHUNK * 2]).await;
    f.enqueue(&file).await;
    let mut item = f.files.inner.database.uploads().await.unwrap().remove(0);
    f.files.inner.prepare(&mut item).await.unwrap();
    f.storage
        .set_faults(Faults {
            delay: Duration::from_secs(30),
            ..Faults::none()
        })
        .await;
    let mut requests = f.storage.subscribe_requests();
    let mut upload = Box::pin(f.files.inner.upload(&mut item));
    tokio::select! {
        result = &mut upload => panic!("upload finished before its storage request: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(10), requests.changed()) => result.unwrap().unwrap(),
    }
    f.database.close().await.unwrap();
    assert!(matches!(
        f.directory.lock_for_deletion(),
        Err(coven_foundation::files::StoreLockError::AlreadyOpen(_))
    ));
    drop(upload);
    f.files.close().await;
    f.directory
        .lock_for_deletion()
        .unwrap()
        .unwrap()
        .remove_directory()
        .unwrap();
}

#[tokio::test]
async fn a_single_request_reports_uploading_while_storage_is_pending() {
    let f = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let file = f.attach("files", "single", vec![44; 19]).await;
    f.enqueue(&file).await;
    f.storage
        .set_faults(Faults {
            delay: Duration::from_millis(200),
            ..Faults::none()
        })
        .await;
    let mut requests = f.storage.subscribe_requests();
    f.files.inner.state.lock().unwrap().paused = false;
    let files = f.files.clone();
    let task = tokio::spawn(async move { files.retry_uploads_now().await });
    tokio::time::timeout(Duration::from_secs(20), requests.changed())
        .await
        .unwrap()
        .unwrap();
    let mut uploads = f.files.subscribe_uploads();
    let phase = uploads.next().await.unwrap().files.remove(0).phase;
    task.await.unwrap().unwrap();
    f.close().await;
    assert!(
        matches!(phase, UploadPhase::Uploading { bytes_sent: 0, .. }),
        "{phase:?}"
    );
}

#[tokio::test]
async fn bounded_uploads_from_originals_and_owned_copies_publish_after_storage() {
    for original in [false, true] {
        let f = Fixture::new(
            if original {
                Provenance::UserProvided
            } else {
                Provenance::AppProvided
            },
            Uploads::WhenAsked,
            CacheFill::CacheLazy,
        )
        .await;
        let bytes = (0..8 * 1024 * 1024 + 19)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<_>>();
        let file = if original {
            f.original("large", &bytes).await
        } else {
            f.attach("files", "large", bytes.clone()).await
        };
        f.enqueue(&file).await;
        assert_ne!(
            f.database
                .file_ref("files", "large")
                .await
                .unwrap()
                .location(),
            FileLocation::Uploaded
        );
        assert!(f
            .storage
            .list(&coven_storage::ObjectPrefix::files())
            .await
            .unwrap()
            .is_empty());
        f.drain().await;
        let uploaded = f.database.file_ref("files", "large").await.unwrap();
        assert_eq!(uploaded.location(), FileLocation::Uploaded);
        let (sent, largest) = f.storage.transferred().await;
        assert!(sent > bytes.len() as u64);
        assert!(largest <= CHUNK);
        assert_eq!(f.files.read_file(&uploaded).await.unwrap(), bytes);
        f.close().await;
    }
}

#[tokio::test]
async fn fixing_bytes_survives_reopen_and_original_changes() {
    let mut f = Fixture::new(
        Provenance::UserProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let bytes = vec![93; CHUNK * 3 + 18];
    let file = f.original("recording", &bytes).await;
    f.enqueue(&file).await;
    let mut item = f.files.inner.database.uploads().await.unwrap().remove(0);
    f.files.inner.prepare(&mut item).await.unwrap();
    let fixed = item.fixed.as_ref().unwrap();
    let encrypted = f
        .directory
        .file(FileArea::AppProvided, &fixed.name)
        .read_optional()
        .unwrap()
        .unwrap();
    let (id, _) = decode_identity(fixed.identity.as_bytes()).unwrap();
    assert_eq!(f.storage.request_count(), 0);
    std::fs::write(f.root.path().join("recording"), b"a changed original").unwrap();
    f.reopen().await;
    f.drain().await;
    assert_eq!(
        f.storage
            .read(&ObjectPath::file(upload_device(&item).unwrap(), id))
            .await
            .unwrap(),
        encrypted
    );
    let uploaded = f.database.file_ref("files", "recording").await.unwrap();
    assert_eq!(f.files.read_file(&uploaded).await.unwrap(), bytes);
    f.close().await;
}

#[tokio::test]
async fn recorded_sessions_continue_or_restart_with_the_same_encrypted_bytes() {
    for expire in [false, true] {
        let mut f = Fixture::new(
            Provenance::AppProvided,
            Uploads::WhenAsked,
            CacheFill::CacheLazy,
        )
        .await;
        let file = f
            .attach("files", "recording", vec![94; CHUNK * 5 + 18])
            .await;
        f.enqueue(&file).await;
        let mut item = f.files.inner.database.uploads().await.unwrap().remove(0);
        f.files.inner.state.lock().unwrap().paused = false;
        f.storage
            .set_faults(Faults {
                lose_part_reply: true,
                ..Faults::none()
            })
            .await;
        // Hold the drain lock so the commit-driven worker cannot consume this item.
        let guard = f.files.inner.drain.lock().await;
        assert!(matches!(
            f.files.inner.upload(&mut item).await,
            Err(UploadFailure::Storage(_))
        ));
        f.files.inner.state.lock().unwrap().paused = true;
        drop(guard);
        let item = f.files.inner.database.uploads().await.unwrap().remove(0);
        let session = UploadSession::decode(item.session.as_ref().unwrap().as_bytes()).unwrap();
        assert_eq!(session.confirmed_bytes(), 0);
        let fixed = item.fixed.as_ref().unwrap();
        let encrypted = f
            .directory
            .file(FileArea::AppProvided, &fixed.name)
            .read_optional()
            .unwrap()
            .unwrap();
        let (id, _) = decode_identity(fixed.identity.as_bytes()).unwrap();
        f.reopen().await;
        f.storage
            .set_faults(Faults {
                expire_uploads: expire,
                ..Faults::none()
            })
            .await;
        let before = f.storage.transferred().await.0;
        f.drain().await;
        let after = f.storage.transferred().await.0;
        assert_eq!(
            after - before,
            encrypted.len() as u64 - if expire { 0 } else { CHUNK as u64 }
        );
        assert_eq!(
            f.storage
                .read(&ObjectPath::file(upload_device(&item).unwrap(), id))
                .await
                .unwrap(),
            encrypted
        );
        f.close().await;
    }
}

#[tokio::test]
async fn lost_completion_reply_resumes_the_published_object() {
    let mut f = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let file = f.attach("files", "one", vec![43; CHUNK * 2]).await;
    f.enqueue(&file).await;
    f.storage
        .set_faults(Faults {
            lose_completion_reply: true,
            ..Faults::none()
        })
        .await;
    let guard = f.files.inner.drain.lock().await;
    f.files.inner.state.lock().unwrap().paused = false;
    let mut item = f.files.inner.database.uploads().await.unwrap().remove(0);
    assert!(f.files.inner.upload(&mut item).await.is_err());
    f.files.inner.state.lock().unwrap().paused = true;
    drop(guard);
    assert_eq!(f.database.file_ref("files", "one").await.unwrap(), file);
    assert_eq!(
        f.storage
            .list(&coven_storage::ObjectPrefix::files())
            .await
            .unwrap()
            .len(),
        1
    );
    f.reopen().await;
    let before = f.storage.transferred().await;
    f.drain().await;
    assert_eq!(before, f.storage.transferred().await);
    assert_eq!(
        f.database
            .file_ref("files", "one")
            .await
            .unwrap()
            .location(),
        FileLocation::Uploaded
    );
    f.close().await;
}

#[tokio::test]
async fn replaced_rows_leave_a_stored_unused_copy_and_no_uploaded_write() {
    let f = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let old = f.attach("files", "one", vec![11; CHUNK * 2]).await;
    f.enqueue(&old).await;
    let mut item = f.files.inner.database.uploads().await.unwrap().remove(0);
    f.files.inner.prepare(&mut item).await.unwrap();
    let new = f.attach("files", "one", vec![12; CHUNK * 2]).await;
    let writes = f.database.test_queued_writes().await.unwrap().len();
    let guard = f.files.inner.drain.lock().await;
    f.files.inner.state.lock().unwrap().paused = false;
    assert!(!f.files.inner.upload(&mut item).await.unwrap());
    f.files.inner.state.lock().unwrap().paused = true;
    drop(guard);
    assert_eq!(f.database.file_ref("files", "one").await.unwrap(), new);
    assert_eq!(f.database.test_queued_writes().await.unwrap().len(), writes);
    let queue = f.files.inner.database.uploads().await.unwrap();
    assert_eq!(queue.len(), 1);
    assert!(queue[0].stored && queue[0].unused);
    assert_eq!(
        f.storage
            .list(&coven_storage::ObjectPrefix::files())
            .await
            .unwrap()
            .len(),
        1
    );
    f.close().await;
}

#[tokio::test]
async fn when_attached_starts_after_commit_and_checks_changed_originals() {
    let f = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAttached,
        CacheFill::CacheEager,
    )
    .await;
    f.files.set_uploads_paused(false);
    let mut uploads = f.files.subscribe_uploads();
    f.attach("files", "one", vec![14; CHUNK]).await;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let queue = uploads.next().await.unwrap();
            if queue.files.is_empty()
                && f.database
                    .file_ref("files", "one")
                    .await
                    .unwrap()
                    .location()
                    == FileLocation::Uploaded
            {
                break;
            }
        }
    })
    .await
    .unwrap();
    let file = f.database.file_ref("files", "one").await.unwrap();
    let mut eager = f.files.subscribe_eager_cache_fill_status();
    tokio::time::timeout(Duration::from_secs(20), async {
        while f.files.inner.database.missing_bytes(&file).await.unwrap() != 0 {
            eager.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    f.close().await;
    let f = Fixture::new(
        Provenance::UserProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let file = f.original("changed", b"original").await;
    f.enqueue(&file).await;
    std::fs::write(f.root.path().join("changed"), b"replacement").unwrap();
    let mut item = f.files.inner.database.uploads().await.unwrap().remove(0);
    assert!(matches!(
        f.files.inner.prepare(&mut item).await,
        Err(UploadFailure::File(FileReadError::UserFileChanged { .. }))
    ));
    assert_eq!(f.storage.request_count(), 0);
    f.close().await;
}
