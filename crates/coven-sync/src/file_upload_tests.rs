use super::{Fixture, CHUNK};
use crate::files::file_error::*;
use crate::files::file_upload::*;
use coven_database::{CacheFill, FileLocation, Provenance};
use coven_storage::{test_utils::Faults, Storage};
use coven_storage::{ObjectPath, UploadSession};
use std::time::Duration;

#[tokio::test]
async fn same_size_edits_with_restored_mtime_send_no_changed_chunks() {
    for provenance in [Provenance::UserProvided, Provenance::AppProvided] {
        // A resumed part begins inside chunk zero. Even a change in the part
        // already sent must prevent re-encrypting that chunk with its old nonce.
        for resume in [None, Some(false), Some(true)] {
            let mut f = Fixture::new(provenance.clone(), CacheFill::CacheLazy).await;
            let bytes = vec![41; CHUNK * 2];
            let file = match provenance {
                Provenance::UserProvided => f.original("changed-during-upload", &bytes).await,
                Provenance::AppProvided => {
                    f.attach("files", "changed-during-upload", bytes.clone())
                        .await
                }
            };
            let owned = f
                .root
                .path()
                .join("stores")
                .join(f.directory.id().to_string())
                .join("files");
            let paths = std::fs::read_dir(&owned)
                .unwrap()
                .map(|p| p.unwrap().path())
                .collect::<Vec<_>>();
            let path = match provenance {
                Provenance::UserProvided => {
                    assert!(paths.is_empty());
                    f.root.path().join("changed-during-upload")
                }
                Provenance::AppProvided => {
                    assert_eq!(paths.len(), 1);
                    paths[0].clone()
                }
            };

            if resume.is_some() {
                f.storage
                    .set_faults(Faults {
                        lose_part_reply: true,
                        ..Faults::none()
                    })
                    .await;
                let guard = f.files.inner.drain.lock().await;
                f.files.inner.state.lock().unwrap().paused = false;
                let mut item = f.files.inner.database.uploads().await.unwrap().remove(0);
                assert!(matches!(
                    f.files.inner.upload(&mut item).await,
                    Err(UploadFailure::Storage(_))
                ));
                f.files.inner.state.lock().unwrap().paused = true;
                drop(guard);
                let item = f.files.inner.database.uploads().await.unwrap().remove(0);
                let mut session =
                    UploadSession::decode(item.session.as_ref().unwrap().as_bytes()).unwrap();
                f.storage.resume_upload(&mut session).await.unwrap();
                assert_eq!(session.confirmed_bytes(), CHUNK as u64);
                f.reopen().await;
            }
            let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
            f.storage
                .set_faults(Faults {
                    delay: Duration::from_millis(100),
                    expire_uploads: resume == Some(true),
                    ..Faults::none()
                })
                .await;
            let before = f.storage.transferred().await.0;
            let before_requests = f.storage.request_count();
            let guard = f.files.inner.drain.lock().await;
            f.files.inner.state.lock().unwrap().paused = false;
            let mut item = f.files.inner.database.uploads().await.unwrap().remove(0);
            let mut requests = f.storage.subscribe_requests();
            let mut upload = Box::pin(f.files.inner.upload(&mut item));
            tokio::select! {
                result = &mut upload => panic!("upload ended before its first request: {result:?}"),
                result = tokio::time::timeout(Duration::from_secs(10), requests.changed()) => result.unwrap().unwrap(),
            }
            let mut changed = bytes.clone();
            changed[0] = 42;
            std::fs::write(&path, changed).unwrap();
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_times(std::fs::FileTimes::new().set_modified(modified))
                .unwrap();
            let result = upload.await;
            f.files.inner.state.lock().unwrap().paused = true;
            drop(guard);
            match provenance {
                Provenance::UserProvided => assert!(
                    matches!(
                        result,
                        Err(UploadFailure::File(FileReadError::UserFileChanged { .. }))
                    ),
                    "{result:?}"
                ),
                Provenance::AppProvided => assert!(
                    matches!(
                        result,
                        Err(UploadFailure::File(FileReadError::Integrity { .. }))
                    ),
                    "{result:?}"
                ),
            }
            assert_eq!(f.storage.transferred().await.0, before);
            // Only begin/resume (and an expired session's restart) contacted storage.
            assert_eq!(
                f.storage.request_count() - before_requests,
                if resume == Some(true) { 2 } else { 1 }
            );
            assert_eq!(std::fs::read_dir(&owned).unwrap().count(), paths.len());
            assert_eq!(
                f.database
                    .file_ref("files", "changed-during-upload")
                    .await
                    .unwrap(),
                file
            );
            f.close().await;
        }
    }
}

#[tokio::test]
async fn an_upload_reader_retains_the_store_after_its_database_closes() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    f.attach("files", "held", vec![45; CHUNK * 2]).await;

    let mut item = f.files.inner.database.uploads().await.unwrap().remove(0);
    f.files.inner.fix_identity(&mut item).await.unwrap();
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
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    f.attach("files", "single", vec![44; 19]).await;

    let (started, receiving) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_creation(coven_storage::ObjectPrefix::files(), started, resume)
        .await;
    f.files.inner.state.lock().unwrap().paused = false;
    let files = f.files.clone();
    let task = tokio::spawn(async move { files.retry_uploads_now().await });
    tokio::time::timeout(Duration::from_secs(20), receiving)
        .await
        .unwrap()
        .unwrap();
    let mut uploads = f.files.subscribe_uploads();
    let phase = uploads.next().await.unwrap().files.remove(0).phase;
    release.send(()).unwrap();
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
            CacheFill::CacheLazy,
        )
        .await;
        let bytes = (0..8 * 1024 * 1024 + 19)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<_>>();
        if original {
            f.original("large", &bytes).await
        } else {
            f.attach("files", "large", bytes.clone()).await
        };

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
async fn a_reopened_upload_refuses_a_changed_original_without_a_copy() {
    let mut f = Fixture::new(Provenance::UserProvided, CacheFill::CacheLazy).await;
    let bytes = vec![93; CHUNK * 3 + 18];
    let file = f.original("recording", &bytes).await;

    let mut item = f.files.inner.database.uploads().await.unwrap().remove(0);
    f.files.inner.fix_identity(&mut item).await.unwrap();
    let identity = item.identity.as_ref().unwrap().as_bytes().to_vec();
    assert_eq!(f.storage.request_count(), 0);
    assert_eq!(
        std::fs::read_dir(
            f.root
                .path()
                .join("stores")
                .join(f.directory.id().to_string())
                .join("files")
        )
        .unwrap()
        .count(),
        0
    );
    let path = f.root.path().join("recording");
    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    std::fs::write(&path, vec![94; bytes.len()]).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(modified))
        .unwrap();
    f.reopen().await;
    let mut item = f.files.inner.database.uploads().await.unwrap().remove(0);
    assert_eq!(item.identity.as_ref().unwrap().as_bytes(), identity);
    assert!(matches!(
        f.files.inner.upload(&mut item).await,
        Err(UploadFailure::File(FileReadError::UserFileChanged { .. }))
    ));
    assert_eq!(f.storage.request_count(), 0);
    assert_eq!(
        f.database.file_ref("files", "recording").await.unwrap(),
        file
    );
    f.close().await;
}

#[tokio::test]
async fn recorded_sessions_continue_or_restart_with_the_same_encrypted_bytes() {
    for expire in [false, true] {
        let mut f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
        f.attach("files", "recording", vec![94; CHUNK * 5 + 18])
            .await;

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
        let identity = item.identity.as_ref().unwrap().as_bytes().to_vec();
        let (id, key) = decode_identity(&identity).unwrap();
        let expected_size = 11 + (CHUNK * 5 + 18) as u64 + 6 * 16;
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
            expected_size - if expire { 0 } else { CHUNK as u64 }
        );
        let uploaded = f.database.file_ref("files", "recording").await.unwrap();
        let reference = uploaded.uploaded().unwrap().unwrap();
        assert_eq!(reference.id, id);
        assert_eq!(
            reference.key.to_secret_bytes().as_bytes(),
            key.to_secret_bytes().as_bytes()
        );
        let encrypted = f
            .storage
            .read(&ObjectPath::file(upload_device(&item).unwrap(), id))
            .await
            .unwrap();
        assert_eq!(encrypted.len() as u64, expected_size);
        assert_eq!(
            f.files.read_file(&uploaded).await.unwrap(),
            vec![94; CHUNK * 5 + 18]
        );
        f.close().await;
    }
}

#[tokio::test]
async fn lost_completion_reply_resumes_the_published_object() {
    let mut f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = f.attach("files", "one", vec![43; CHUNK * 2]).await;

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

pub(super) async fn unused_upload(f: &Fixture) {
    f.attach("files", "old", vec![33; 19]).await;

    let (started, receiving) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_creation(coven_storage::ObjectPrefix::files(), started, resume)
        .await;
    let guard = f.files.inner.drain.lock().await;
    f.files.inner.state.lock().unwrap().paused = false;
    let mut item = f.files.inner.database.uploads().await.unwrap().remove(0);
    let mut uploading = Box::pin(f.files.inner.upload(&mut item));
    tokio::select! {
        result = &mut uploading => panic!("upload ended before publication: {result:?}"),
        result = receiving => result.unwrap(),
    }
    let new = f.attach("files", "old", vec![34; 19]).await;
    let writes = f.database.test_queued_writes().await.unwrap().len();
    release.send(()).unwrap();
    assert!(!uploading.await.unwrap());
    f.files.inner.state.lock().unwrap().paused = true;
    drop(guard);
    assert_eq!(f.database.file_ref("files", "old").await.unwrap(), new);
    assert_eq!(f.database.test_queued_writes().await.unwrap().len(), writes);
    let queue = f.files.inner.database.uploads().await.unwrap();
    assert_eq!(queue.len(), 2);
    assert!(queue[0].stored && queue[0].unused);
    assert_eq!(queue[1].file, new);
    assert!(!queue[1].stored && !queue[1].unused);
}

#[tokio::test]
async fn replaced_rows_leave_a_stored_unused_copy_and_no_uploaded_write() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    unused_upload(&f).await;
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
async fn attaching_starts_upload_after_commit_and_checks_changed_originals() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheEager).await;
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
    let f = Fixture::new(Provenance::UserProvided, CacheFill::CacheLazy).await;
    f.original("changed", b"original").await;

    std::fs::write(f.root.path().join("changed"), b"replacement").unwrap();
    let mut item = f.files.inner.database.uploads().await.unwrap().remove(0);
    assert!(matches!(
        f.files.inner.upload(&mut item).await,
        Err(UploadFailure::File(FileReadError::UserFileChanged { .. }))
    ));
    assert_eq!(f.storage.request_count(), 0);
    f.close().await;
}
