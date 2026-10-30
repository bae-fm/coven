use super::{files_runner, Fixture, CHUNK};
use crate::files::file_error::*;
use coven_database::{FileLocation, FileLocationKey, FilePlace, FileRef, Provenance};
use coven_foundation::files::StoreDirAccess;
use coven_storage::{test_utils::Faults, ObjectPath, Storage, UploadSession};
use std::time::Duration;

/// A meter for an attempt the test runs itself, outside the upload worker:
/// its attempt is already over, so it tells no one.
pub(super) fn unwatched(
    f: &Fixture,
    item: &coven_database::FileUpload,
) -> coven_database::TransferMeter {
    let progress = coven_database::TransferProgress {
        bytes_transferred: 0,
        bytes_total: item.bytes_total,
    };
    f.files
        .inner
        .in_flight
        .start(
            item.location.uploader(),
            item.location.object(),
            item.files.clone(),
            progress,
        )
        .meter()
}

#[tokio::test]
async fn same_size_edits_with_restored_mtime_send_no_changed_chunks() {
    for provenance in [Provenance::UserProvided, Provenance::AppProvided] {
        // A resumed part begins inside chunk zero. Even a change in the part
        // already sent must prevent re-encrypting that chunk with its old nonce.
        for resume in [None, Some(false), Some(true)] {
            let mut f = Fixture::new(provenance.clone()).await;
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
                f.files
                    .inner
                    .paused
                    .store(false, std::sync::atomic::Ordering::Release);
                let item = f.files.inner.database.uploads().await.unwrap().remove(0);
                assert!(matches!(
                    f.files.inner.upload(&item, unwatched(&f, &item)).await,
                    Err(TransferError::File(FileReadError::Storage(_)))
                ));
                f.files
                    .inner
                    .paused
                    .store(true, std::sync::atomic::Ordering::Release);
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
            f.files
                .inner
                .paused
                .store(false, std::sync::atomic::Ordering::Release);
            let item = f.files.inner.database.uploads().await.unwrap().remove(0);
            let mut requests = f.storage.subscribe_requests();
            let mut upload = Box::pin(f.files.inner.upload(&item, unwatched(&f, &item)));
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
            f.files
                .inner
                .paused
                .store(true, std::sync::atomic::Ordering::Release);
            drop(guard);
            match provenance {
                Provenance::UserProvided => assert!(
                    matches!(
                        result,
                        Err(TransferError::File(FileReadError::UserFileChanged { .. }))
                    ),
                    "{result:?}"
                ),
                Provenance::AppProvided => assert!(
                    matches!(
                        result,
                        Err(TransferError::File(FileReadError::Integrity { .. }))
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
            assert_eq!(f.database.file_ref(file.id()).await.unwrap(), file);
            f.close().await;
        }
    }
}

#[tokio::test]
async fn an_upload_reader_retains_the_store_after_its_database_closes() {
    let f = Fixture::new(Provenance::AppProvided).await;
    f.attach("files", "held", vec![45; CHUNK * 2]).await;

    let item = f.files.inner.database.uploads().await.unwrap().remove(0);
    f.storage
        .set_faults(Faults {
            delay: Duration::from_secs(30),
            ..Faults::none()
        })
        .await;
    let mut requests = f.storage.subscribe_requests();
    let mut upload = Box::pin(f.files.inner.upload(&item, unwatched(&f, &item)));
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
async fn bounded_uploads_from_originals_and_owned_copies_publish_after_storage() {
    for original in [false, true] {
        let f = Fixture::new(if original {
            Provenance::UserProvided
        } else {
            Provenance::AppProvided
        })
        .await;
        let bytes = (0..8 * 1024 * 1024 + 19)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<_>>();
        let attached = if original {
            f.original("large", &bytes).await
        } else {
            f.attach("files", "large", bytes.clone()).await
        };
        assert!(f.queued(&attached).await);
        assert!(f
            .storage
            .list(&coven_storage::ObjectPrefix::files(f.directory.id()))
            .await
            .unwrap()
            .is_empty());
        f.drain().await;
        let uploaded = f.database.file_ref(attached.id()).await.unwrap();
        assert!(matches!(uploaded.place(), FilePlace::Cloud { .. }));
        assert!(!f.queued(&uploaded).await);
        // The stored object is at the path the staging write fixed.
        assert!(f.storage.read(&f.path(&uploaded)).await.is_ok());
        let (sent, largest) = f.storage.transferred().await;
        assert!(sent > bytes.len() as u64);
        assert!(largest <= CHUNK);
        assert_eq!(f.files.read_file(&uploaded).await.unwrap(), bytes);
        f.close().await;
    }
}

/// An original rewritten with its size and modification time restored still
/// opens, and a local read returns what's on disk. Its upload checks each
/// chunk against the hash recorded at attachment and sends none of it (§17.8).
#[tokio::test]
async fn a_reopened_upload_refuses_a_changed_original_without_a_copy() {
    let mut f = Fixture::new(Provenance::UserProvided).await;
    let bytes = vec![93; CHUNK * 3 + 18];
    let file = f.original("recording", &bytes).await;

    let item = f.files.inner.database.uploads().await.unwrap().remove(0);
    assert_eq!(item.files, [file.id()]);
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
    assert_eq!(
        f.files.read_file(&file).await.unwrap(),
        [94; CHUNK * 3 + 18]
    );
    f.files.set_uploads_paused(false).await.unwrap();
    f.files.inner.drain_uploads().await.unwrap();
    assert_eq!(
        f.upload_reason(&file).await,
        Some(crate::PendingReason::FileUnavailable(
            crate::FileMissingReason::Source(crate::FileSourceFailure::Changed)
        ))
    );
    assert_eq!(f.storage.transferred().await.0, 0);
    assert_eq!(f.database.file_ref(file.id()).await.unwrap(), file);
    f.close().await;
}

#[tokio::test]
async fn recorded_sessions_continue_or_restart_with_the_same_encrypted_bytes() {
    for expire in [false, true] {
        let mut f = Fixture::new(Provenance::AppProvided).await;
        let file = f
            .attach("files", "recording", vec![94; CHUNK * 5 + 18])
            .await;

        let item = f.files.inner.database.uploads().await.unwrap().remove(0);
        f.storage
            .set_faults(Faults {
                lose_part_reply: true,
                ..Faults::none()
            })
            .await;
        // Hold the drain lock so the commit-driven worker cannot consume this
        // item, or reset the pause it reads from the database meanwhile.
        let guard = f.files.inner.drain.lock().await;
        f.files
            .inner
            .paused
            .store(false, std::sync::atomic::Ordering::Release);
        assert!(matches!(
            f.files.inner.upload(&item, unwatched(&f, &item)).await,
            Err(TransferError::File(FileReadError::Storage(_)))
        ));
        f.files
            .inner
            .paused
            .store(true, std::sync::atomic::Ordering::Release);
        drop(guard);
        let item = f.files.inner.database.uploads().await.unwrap().remove(0);
        let session = UploadSession::decode(item.session.as_ref().unwrap().as_bytes()).unwrap();
        assert_eq!(session.confirmed_bytes(), 0);
        let coven_format::file_reference::FileReference { id, key, .. } =
            item.location.reference().unwrap();
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
        let uploaded = f.database.file_ref(file.id()).await.unwrap();
        // The stored file is at the location its upload fixed, under its key.
        let location = super::stored(&uploaded);
        assert_eq!(location, &item.location);
        let reference = location.reference().unwrap();
        assert_eq!(reference.id, id);
        assert_eq!(
            reference.key.to_secret_bytes().as_bytes(),
            key.to_secret_bytes().as_bytes()
        );
        let encrypted = f
            .storage
            .read(&ObjectPath::file(f.directory.id(), location.uploader(), id))
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
    let mut f = Fixture::new(Provenance::AppProvided).await;
    let file = f.attach("files", "one", vec![43; CHUNK * 2]).await;

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
    assert!(f
        .files
        .inner
        .upload(&item, unwatched(&f, &item))
        .await
        .is_err());
    f.files
        .inner
        .paused
        .store(true, std::sync::atomic::Ordering::Release);
    drop(guard);
    assert_eq!(f.database.file_ref(file.id()).await.unwrap(), file);
    assert_eq!(
        f.storage
            .list(&coven_storage::ObjectPrefix::files(f.directory.id()))
            .await
            .unwrap()
            .len(),
        1
    );
    f.reopen().await;
    let before = f.storage.transferred().await;
    f.drain().await;
    assert_eq!(before, f.storage.transferred().await);
    let stored = f.database.file_ref(file.id()).await.unwrap();
    assert!(matches!(stored.place(), FilePlace::Cloud { .. }));
    assert!(!f.queued(&stored).await);
    f.close().await;
}

/// Ana stages A, then points the row at B while A's bytes are in flight.
/// Nothing references A then, so that write deletes A's record. A's upload
/// completes with no file left to store, and changes nothing (§17.2).
/// Returns where A's bytes went, and B.
pub(super) async fn retargeted_upload(f: &Fixture) -> (FileLocation, FileRef) {
    let old = f.attach("files", "old", vec![33; 19]).await;
    let (started, receiving) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_creation(
            coven_storage::ObjectPrefix::files(f.directory.id()),
            started,
            resume,
        )
        .await;
    let guard = f.files.inner.drain.lock().await;
    f.files
        .inner
        .paused
        .store(false, std::sync::atomic::Ordering::Release);
    let item = f.files.inner.database.uploads().await.unwrap().remove(0);
    assert_eq!(item.files, [old.id()]);
    let mut uploading = Box::pin(f.files.inner.upload(&item, unwatched(f, &item)));
    tokio::select! {
        result = &mut uploading => panic!("upload ended before publication: {result:?}"),
        result = receiving => result.unwrap(),
    }
    let new = f.attach("files", "old", vec![34; 19]).await;
    assert_ne!(new.id(), old.id());
    assert!(matches!(
        f.database.file_ref(old.id()).await,
        Err(coven_database::DbError::FileAbsent { .. })
    ));
    let writes = f.database.test_queued_writes().await.unwrap().len();
    release.send(()).unwrap();
    assert!(uploading.await.unwrap());
    f.files
        .inner
        .paused
        .store(true, std::sync::atomic::Ordering::Release);
    drop(guard);
    assert_eq!(f.database.file_ref(new.id()).await.unwrap(), new);
    assert_eq!(f.database.test_queued_writes().await.unwrap().len(), writes);
    let queue = f.files.inner.database.uploads().await.unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].files, [new.id()]);
    (item.location, new)
}

/// The row is retargeted, then deleted, before any attempt starts: each
/// write deletes the record nothing references any more, and the worker
/// retires both uploads without sending a byte (§17.2, §17.8).
#[tokio::test]
async fn uploads_no_file_waits_for_are_retired_unsent() {
    for provenance in [Provenance::AppProvided, Provenance::UserProvided] {
        let f = Fixture::new(provenance.clone()).await;
        let old = match provenance {
            Provenance::AppProvided => f.attach("files", "old", vec![36; CHUNK + 3]).await,
            Provenance::UserProvided => f.original("old", &vec![36; CHUNK + 3]).await,
        };
        let mut locations = vec![f.location(&old).await];
        let new = match provenance {
            Provenance::AppProvided => f.attach("files", "old", vec![37; 5]).await,
            Provenance::UserProvided => {
                let path = f.root.path().join("new");
                std::fs::write(&path, [37; 5]).unwrap();
                let prepared = coven_database::prepare_user_file(&path, |_| {})
                    .await
                    .unwrap();
                let file = f
                    .database
                    .write_with_files::<_, _, _, _, coven_database::DbError>(
                        move |batch| Ok(batch.put_user_file("files", prepared)),
                        |sql, file| {
                            sql.execute("UPDATE files SET file=?1 WHERE id='old'", [file])?;
                            Ok(file)
                        },
                    )
                    .await
                    .unwrap();
                f.database.file_ref(file).await.unwrap()
            }
        };
        locations.push(f.location(&new).await);
        f.database
            .write(|sql| {
                sql.execute("DELETE FROM files", [])?;
                Ok(())
            })
            .await
            .unwrap();
        for file in [&old, &new] {
            assert!(matches!(
                f.database.file_ref(file.id()).await,
                Err(coven_database::DbError::FileAbsent { .. })
            ));
        }
        let requests = f.storage.request_count();
        f.drain().await;
        assert_eq!(f.storage.request_count(), requests);
        assert!(f
            .files
            .inner
            .database
            .all_uploads()
            .await
            .unwrap()
            .is_empty());
        for location in &locations {
            let path = ObjectPath::file(f.directory.id(), location.uploader(), location.object());
            assert!(f.storage.status(&path).await.unwrap().is_none());
        }
        f.close().await;
    }
}

#[tokio::test]
async fn completing_a_replaced_files_upload_cannot_restore_it() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let (old, new) = retargeted_upload(&f).await;
    // A's bytes are stored at their own fixed path; B still waits here.
    let objects = f
        .storage
        .list(&coven_storage::ObjectPrefix::files(f.directory.id()))
        .await
        .unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(
        objects[0].path,
        ObjectPath::file(f.directory.id(), old.uploader(), old.object())
    );
    assert_eq!(f.files.read_file(&new).await.unwrap(), vec![34; 19]);
    f.close().await;
}

#[tokio::test]
async fn deleting_a_row_during_its_upload_is_not_undone_by_completion() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.attach("files", "gone", vec![35; 19]).await;
    let (started, receiving) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_creation(
            coven_storage::ObjectPrefix::files(f.directory.id()),
            started,
            resume,
        )
        .await;
    let guard = f.files.inner.drain.lock().await;
    f.files
        .inner
        .paused
        .store(false, std::sync::atomic::Ordering::Release);
    let item = f.files.inner.database.uploads().await.unwrap().remove(0);
    let path = ObjectPath::file(
        f.directory.id(),
        item.location.uploader(),
        item.location.object(),
    );
    let mut uploading = Box::pin(f.files.inner.upload(&item, unwatched(&f, &item)));
    tokio::select! {
        result = &mut uploading => panic!("upload ended before publication: {result:?}"),
        result = receiving => result.unwrap(),
    }
    f.database
        .write(|sql| {
            sql.execute("DELETE FROM files WHERE id='gone'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let writes = f.database.test_queued_writes().await.unwrap().len();
    release.send(()).unwrap();
    assert!(uploading.await.unwrap());
    f.files
        .inner
        .paused
        .store(true, std::sync::atomic::Ordering::Release);
    drop(guard);
    assert!(matches!(
        f.database.file_ref(file.id()).await,
        Err(coven_database::DbError::FileAbsent { .. })
    ));
    assert_eq!(f.database.test_queued_writes().await.unwrap().len(), writes);
    assert!(f.files.inner.database.uploads().await.unwrap().is_empty());
    // The bytes stay until the delete is published, then go under §17.9.
    assert!(f.storage.read(&path).await.is_ok());
    f.close().await;
}

#[tokio::test]
async fn attaching_starts_upload_after_commit_and_checks_changed_originals() {
    let f = Fixture::new(Provenance::AppProvided).await;
    f.files.set_uploads_paused(false).await.unwrap();
    f.attach("files", "one", vec![14; CHUNK]).await;
    let mut uploads = f.subscribe_queue();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let queue = uploads.next().await.expect("the live query answers");
            if queue.unwrap().files.is_empty() {
                break;
            }
        }
    })
    .await
    .unwrap();
    f.close().await;
    let f = Fixture::new(Provenance::UserProvided).await;
    f.original("changed", b"original").await;

    std::fs::write(f.root.path().join("changed"), b"replacement").unwrap();
    let item = f.files.inner.database.uploads().await.unwrap().remove(0);
    assert!(matches!(
        f.files.inner.upload(&item, unwatched(&f, &item)).await,
        Err(TransferError::File(FileReadError::UserFileChanged { .. }))
    ));
    assert_eq!(f.storage.request_count(), 0);
    f.close().await;
}

/// ST `Files`: staging, row deletion and settled uploads leave the
/// references, records, queue and write count that `Files.stage`,
/// `Files.change` and `Files.finish` describe.
mod differential {
    use super::{files_runner, unwatched, Fixture};
    use crate::files::file_error::*;
    use coven_database::{FileLocation, FilePlace, FileRef, Provenance};
    use coven_foundation::id_source::FileId;
    use coven_storage::{ObjectPath, ObjectPrefix, Storage, StorageFailure};
    use serde_json::{json, Value};

    fn attach(row: u64, file: u64) -> Value {
        json!({"op": "attach", "row": row, "file": file})
    }
    fn begin(file: u64) -> Value {
        json!({"op": "begin", "file": file})
    }
    fn delete(row: u64) -> Value {
        json!({"op": "delete", "row": row})
    }
    fn finish(file: u64, remote: &str) -> Value {
        json!({"op": "finish", "file": file, "remote": remote})
    }

    /// The model's location for the n-th staged file's upload: one uploader,
    /// fresh id and key.
    fn reference(file: u64) -> Value {
        json!({"uploader": 1, "id": file, "key": file})
    }

    fn bytes(file: u64) -> Vec<u8> {
        vec![file as u8; 19]
    }

    /// Events in the runner's input shape; Rust-only fields are rewritten.
    fn model(events: &[Value]) -> Value {
        let events: Vec<Value> = events
            .iter()
            .map(|event| match event["op"].as_str().unwrap() {
                "attach" => {
                    let file = event["file"].as_u64().unwrap();
                    json!({"op": "attach", "row": event["row"], "file": file,
                       "ref": reference(file), "size": bytes(file).len(), "hash": file})
                }
                "begin" => json!({"op": "begin"}),
                "delete" => event.clone(),
                "finish" => {
                    json!({"op": "finish", "ref": reference(event["file"].as_u64().unwrap()),
                               "remote": event["remote"]})
                }
                op => panic!("unknown event {op}"),
            })
            .collect();
        json!({ "events": events })
    }

    /// The staged file's number, by its id.
    fn file_number(attached: &[FileRef], file: FileId) -> u64 {
        let index = attached
            .iter()
            .position(|known| known.id() == file)
            .expect("every referenced and recorded file comes from a staging write");
        index as u64 + 1
    }

    /// The staged file's number, by its upload's location.
    fn location_number(locations: &[FileLocation], location: &FileLocation) -> u64 {
        let index = locations
            .iter()
            .position(|known| known == location)
            .expect("every upload comes from a staging write");
        index as u64 + 1
    }

    /// Run the events against the real database, queue and upload attempt.
    async fn actual(events: &[Value]) -> Value {
        let f = Fixture::new(Provenance::AppProvided).await;
        // Hold the worker away from the queue; each finish is one attempt.
        let guard = f.files.inner.drain.lock().await;
        f.files
            .inner
            .paused
            .store(false, std::sync::atomic::Ordering::Release);
        let mut attached: Vec<FileRef> = Vec::new();
        let mut locations: Vec<FileLocation> = Vec::new();
        let mut rows: Vec<u64> = Vec::new();
        let mut held = None;
        let mut finishes = Vec::new();
        let mut deletes = Vec::new();
        let upload = |location: FileLocation| {
            let f = &f;
            async move {
                f.files
                    .inner
                    .database
                    .all_uploads()
                    .await
                    .unwrap()
                    .into_iter()
                    .find(|upload| upload.location == location)
                    .unwrap()
            }
        };
        for event in events {
            let file = event["file"].as_u64();
            match event["op"].as_str().unwrap() {
                "attach" => {
                    let row = event["row"].as_u64().unwrap();
                    let file = file.unwrap();
                    let staged = f.attach("files", &format!("r{row}"), bytes(file)).await;
                    locations.push(f.location(&staged).await);
                    attached.push(staged);
                    assert_eq!(attached.len() as u64, file);
                    if !rows.contains(&row) {
                        rows.push(row);
                    }
                }
                "delete" => {
                    let row = format!("r{}", event["row"].as_u64().unwrap());
                    let writes = f.database.test_queued_writes().await.unwrap().len();
                    f.database
                        .write(move |sql| {
                            sql.execute("DELETE FROM files WHERE id=?1", [row])?;
                            Ok(())
                        })
                        .await
                        .unwrap();
                    let changed = f.database.test_queued_writes().await.unwrap().len() != writes;
                    deletes.push(json!({ "writes_changed": changed }));
                }
                "begin" => {
                    // The attempt opens and checks its source, then waits at the
                    // provider while later events change the row.
                    let item = upload(locations[file.unwrap() as usize - 1].clone()).await;
                    let (started, receiving) = tokio::sync::oneshot::channel();
                    let (release, resume) = tokio::sync::oneshot::channel();
                    f.storage
                        .hold_next_creation(ObjectPrefix::files(f.directory.id()), started, resume)
                        .await;
                    let inner = f.files.inner.clone();
                    let meter = unwatched(&f, &item);
                    let task = tokio::spawn(async move { inner.upload(&item, meter).await });
                    receiving.await.unwrap();
                    held = Some((file.unwrap(), task, release));
                }
                "finish" => {
                    let location = locations[file.unwrap() as usize - 1].clone();
                    match event["remote"].as_str().unwrap() {
                        "stored" => {}
                        "error" => f.storage.set_online(false),
                        "other" => {
                            let path = ObjectPath::file(
                                f.directory.id(),
                                location.uploader(),
                                location.object(),
                            );
                            f.storage.create(&path, b"other").await.unwrap();
                        }
                        remote => panic!("no Rust provider state for {remote}"),
                    }
                    let writes = f.database.test_queued_writes().await.unwrap().len();
                    let result = match held.take() {
                        Some((begun, task, release)) => {
                            assert_eq!(Some(begun), file);
                            release.send(()).unwrap();
                            task.await.unwrap()
                        }
                        None => {
                            let item = upload(location).await;
                            f.files.inner.upload(&item, unwatched(&f, &item)).await
                        }
                    };
                    f.storage.set_online(true);
                    let settlement = match &result {
                        Ok(true) => "stored",
                        Err(TransferError::File(FileReadError::Storage(error)))
                            if error.failure() == StorageFailure::Network =>
                        {
                            "pending"
                        }
                        Err(TransferError::Refused(crate::Refusal::WrongIdentity { .. })) => {
                            "refused"
                        }
                        other => panic!("unexpected attempt result {other:?}"),
                    };
                    let changed = f.database.test_queued_writes().await.unwrap().len() != writes;
                    finishes.push(json!({"settlement": settlement, "writes_changed": changed}));
                }
                op => panic!("unknown event {op}"),
            }
        }
        // The file each row references.
        let mut row_files = Vec::new();
        for row in rows {
            let id = format!("r{row}");
            let named = f
                .database
                .read(move |sql| {
                    Ok(sql.query("SELECT file FROM files WHERE id=?1", [id], |r| {
                        r.get::<_, Option<FileId>>(0)
                    })?)
                })
                .await
                .unwrap()
                .pop()
                .flatten();
            let file = match named {
                Some(file) => json!(file_number(&attached, file)),
                None => Value::Null,
            };
            row_files.push(json!({"row": row, "file": file}));
        }
        // Every record this device has, with where its bytes are stored.
        let mut records = Vec::new();
        for staged in &attached {
            match f.database.file_ref(staged.id()).await {
                Ok(current) => {
                    let stored = match current.place() {
                        FilePlace::Cloud { location, .. } => {
                            reference(location_number(&locations, location))
                        }
                        FilePlace::DeviceOnly { .. } => Value::Null,
                    };
                    records.push(json!({
                        "file": file_number(&attached, staged.id()),
                        "ref": stored,
                    }));
                }
                Err(coven_database::DbError::FileAbsent { .. }) => {}
                Err(error) => panic!("reading a record failed: {error}"),
            }
        }
        let queue: Vec<Value> = f
            .files
            .inner
            .database
            .all_uploads()
            .await
            .unwrap()
            .iter()
            .map(|upload| reference(location_number(&locations, &upload.location)))
            .collect();
        f.files
            .inner
            .paused
            .store(true, std::sync::atomic::Ordering::Release);
        drop(guard);
        f.close().await;
        json!({"finishes": finishes, "deletes": deletes, "rows": row_files,
               "records": records, "queue": queue})
    }

    /// The model keeps its queue newest first; the order carries no
    /// meaning.
    fn sorted_queue(mut result: Value) -> Value {
        let queue = result["queue"].as_array_mut().unwrap();
        queue.sort_by_key(|reference| reference["id"].as_u64());
        result
    }

    #[tokio::test]
    #[ignore = "requires COVEN_FILES_LEAN; scripts/check.sh builds filesRunner"]
    async fn st_differential_files() {
        let cases = [
            vec![attach(1, 1), finish(1, "stored")],
            vec![attach(1, 1), finish(1, "error"), finish(1, "stored")],
            // Restaged, then deleted, while the first file's bytes are in flight.
            vec![
                attach(1, 1),
                begin(1),
                attach(1, 2),
                finish(1, "stored"),
                finish(2, "stored"),
            ],
            vec![attach(1, 1), begin(1), delete(1), finish(1, "stored")],
            // The queue keeps its own source facts, so the row may change
            // before an attempt opens them; an upload no file waits for any
            // more still settles.
            vec![
                attach(1, 1),
                attach(1, 2),
                delete(1),
                finish(2, "stored"),
                finish(1, "stored"),
            ],
            vec![
                attach(1, 1),
                attach(2, 2),
                finish(2, "stored"),
                begin(1),
                attach(2, 3),
                finish(1, "stored"),
            ],
            // A deleted row's upload survives a failed attempt and settles later.
            vec![
                attach(1, 1),
                delete(1),
                finish(1, "error"),
                finish(1, "stored"),
            ],
            // Deleting another row while one uploads touches neither upload.
            vec![
                attach(1, 1),
                attach(2, 2),
                begin(1),
                delete(2),
                finish(1, "stored"),
                finish(2, "stored"),
            ],
            // The row is deleted and staged again while its first file uploads:
            // each settlement retires only its own upload.
            vec![
                attach(1, 1),
                begin(1),
                delete(1),
                attach(1, 2),
                finish(1, "stored"),
                finish(2, "error"),
                finish(2, "stored"),
            ],
            // Different bytes at the fixed path never count as stored.
            vec![
                attach(1, 1),
                attach(2, 2),
                finish(2, "stored"),
                finish(1, "other"),
            ],
        ];
        let inputs: Vec<Value> = cases.iter().map(|events| model(events)).collect();
        let expected: Vec<Value> = files_runner(&inputs)
            .into_iter()
            .map(sorted_queue)
            .collect();
        let mut results = Vec::new();
        for events in &cases {
            results.push(sorted_queue(actual(events).await));
        }
        assert_eq!(results, expected);
    }
}
