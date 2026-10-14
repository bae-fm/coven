use super::{files_runner, Fixture, CHUNK};
use crate::files::file_error::*;
use crate::files::file_upload::*;
use coven_database::FileRef;
use coven_database::{CacheFill, Provenance};
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
                let item = f.files.inner.database.uploads().await.unwrap().remove(0);
                assert!(matches!(
                    f.files.inner.upload(&item).await,
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
            let item = f.files.inner.database.uploads().await.unwrap().remove(0);
            let mut requests = f.storage.subscribe_requests();
            let mut upload = Box::pin(f.files.inner.upload(&item));
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

    let item = f.files.inner.database.uploads().await.unwrap().remove(0);
    f.storage
        .set_faults(Faults {
            delay: Duration::from_secs(30),
            ..Faults::none()
        })
        .await;
    let mut requests = f.storage.subscribe_requests();
    let mut upload = Box::pin(f.files.inner.upload(&item));
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
        .hold_next_creation(
            coven_storage::ObjectPrefix::files(f.directory.id()),
            started,
            resume,
        )
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
        let uploaded = f.database.file_ref("files", "large").await.unwrap();
        assert_eq!(uploaded, attached);
        assert!(!f.queued(&uploaded).await);
        // The stored object is at the path the attaching write fixed.
        assert!(f.storage.read(&f.path(&uploaded)).await.is_ok());
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

    let item = f.files.inner.database.uploads().await.unwrap().remove(0);
    assert_eq!(item.file, file);
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
    let item = f.files.inner.database.uploads().await.unwrap().remove(0);
    assert_eq!(item.file, file);
    assert!(matches!(
        f.files.inner.upload(&item).await,
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

        let item = f.files.inner.database.uploads().await.unwrap().remove(0);
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
            f.files.inner.upload(&item).await,
            Err(UploadFailure::Storage(_))
        ));
        f.files.inner.state.lock().unwrap().paused = true;
        drop(guard);
        let item = f.files.inner.database.uploads().await.unwrap().remove(0);
        let session = UploadSession::decode(item.session.as_ref().unwrap().as_bytes()).unwrap();
        assert_eq!(session.confirmed_bytes(), 0);
        let coven_format::file_reference::FileReference { id, key, .. } =
            item.file.reference().unwrap();
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
        assert_eq!(uploaded, item.file);
        let reference = uploaded.reference().unwrap();
        assert_eq!(reference.id, id);
        assert_eq!(
            reference.key.to_secret_bytes().as_bytes(),
            key.to_secret_bytes().as_bytes()
        );
        let encrypted = f
            .storage
            .read(&ObjectPath::file(f.directory.id(), uploaded.uploader(), id))
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
    let item = f.files.inner.database.uploads().await.unwrap().remove(0);
    assert!(f.files.inner.upload(&item).await.is_err());
    f.files.inner.state.lock().unwrap().paused = true;
    drop(guard);
    assert_eq!(f.database.file_ref("files", "one").await.unwrap(), file);
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
    assert_eq!(f.database.file_ref("files", "one").await.unwrap(), file);
    assert!(!f.queued(&file).await);
    f.close().await;
}

/// Ana attaches A, then the row is pointed at B while A's bytes are in flight.
/// A's completion changes no row and cannot restore A over B (§16.1).
pub(super) async fn retargeted_upload(f: &Fixture) -> (FileRef, FileRef) {
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
    f.files.inner.state.lock().unwrap().paused = false;
    let item = f.files.inner.database.uploads().await.unwrap().remove(0);
    let mut uploading = Box::pin(f.files.inner.upload(&item));
    tokio::select! {
        result = &mut uploading => panic!("upload ended before publication: {result:?}"),
        result = receiving => result.unwrap(),
    }
    let new = f.attach("files", "old", vec![34; 19]).await;
    assert_ne!((new.uploader(), new.id()), (old.uploader(), old.id()));
    let writes = f.database.test_queued_writes().await.unwrap().len();
    release.send(()).unwrap();
    assert!(uploading.await.unwrap());
    f.files.inner.state.lock().unwrap().paused = true;
    drop(guard);
    assert_eq!(f.database.file_ref("files", "old").await.unwrap(), new);
    assert_eq!(f.database.test_queued_writes().await.unwrap().len(), writes);
    let queue = f.files.inner.database.uploads().await.unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].file, new);
    (old, new)
}

#[tokio::test]
async fn a_queued_upload_keeps_its_source_after_its_row_changes() {
    for provenance in [Provenance::AppProvided, Provenance::UserProvided] {
        let f = Fixture::new(provenance.clone(), CacheFill::CacheLazy).await;
        let old = match provenance {
            Provenance::AppProvided => f.attach("files", "old", vec![36; CHUNK + 3]).await,
            Provenance::UserProvided => f.original("old", &vec![36; CHUNK + 3]).await,
        };
        // Retarget the row, then delete it, before any attempt starts.
        let new = match provenance {
            Provenance::AppProvided => f.attach("files", "old", vec![37; 5]).await,
            Provenance::UserProvided => {
                let path = f.root.path().join("new");
                std::fs::write(&path, [37; 5]).unwrap();
                let prepared = coven_database::prepare_user_file(&path, |_| {})
                    .await
                    .unwrap();
                f.database
                    .write(move |sql| {
                        sql.execute("UPDATE files SET size=5 WHERE id='old'", [])?;
                        sql.register_user_file("files", "old", prepared)
                    })
                    .await
                    .unwrap();
                f.database.file_ref("files", "old").await.unwrap()
            }
        };
        f.database
            .write(|sql| {
                sql.execute("DELETE FROM files", [])?;
                Ok(())
            })
            .await
            .unwrap();
        f.drain().await;
        assert!(f.files.inner.database.uploads().await.unwrap().is_empty());
        // Each upload sent its own bytes to its own fixed path.
        for (file, bytes) in [(&old, vec![36; CHUNK + 3]), (&new, vec![37; 5])] {
            let path = f.path(file);
            let encrypted = f.storage.read(&path).await.unwrap();
            let header = coven_format::file::FileHeader::decode(&encrypted[..11]).unwrap();
            assert_eq!(header.size(), bytes.len() as u64);
            let key = file.reference().unwrap().key;
            let mut plain = Vec::new();
            for index in 0..header.chunk_count() {
                let chunk = header.chunk(index).unwrap();
                let start = chunk.offset as usize;
                let sealed = &encrypted[start..start + chunk.plaintext_length + 16];
                plain.extend(
                    header
                        .open_chunk(&key, path.as_str(), index, sealed)
                        .unwrap(),
                );
            }
            assert_eq!(plain, bytes);
        }
        f.close().await;
    }
}

#[tokio::test]
async fn completing_a_replaced_files_upload_cannot_restore_it() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let (old, new) = retargeted_upload(&f).await;
    // A is stored at its own fixed path; B still waits on this device.
    let objects = f
        .storage
        .list(&coven_storage::ObjectPrefix::files(f.directory.id()))
        .await
        .unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].path, f.path(&old));
    assert_eq!(f.files.read_file(&new).await.unwrap(), vec![34; 19]);
    f.close().await;
}

#[tokio::test]
async fn deleting_a_row_during_its_upload_is_not_undone_by_completion() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
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
    f.files.inner.state.lock().unwrap().paused = false;
    let item = f.files.inner.database.uploads().await.unwrap().remove(0);
    let mut uploading = Box::pin(f.files.inner.upload(&item));
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
    f.files.inner.state.lock().unwrap().paused = true;
    drop(guard);
    assert!(matches!(
        f.database.file_ref("files", "gone").await,
        Err(coven_database::DbError::FileAbsent { .. })
    ));
    assert_eq!(f.database.test_queued_writes().await.unwrap().len(), writes);
    assert!(f.files.inner.database.uploads().await.unwrap().is_empty());
    assert!(f.storage.read(&f.path(&file)).await.is_ok());
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
            if uploads.next().await.unwrap().files.is_empty() {
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
    let item = f.files.inner.database.uploads().await.unwrap().remove(0);
    assert!(matches!(
        f.files.inner.upload(&item).await,
        Err(UploadFailure::File(FileReadError::UserFileChanged { .. }))
    ));
    assert_eq!(f.storage.request_count(), 0);
    f.close().await;
}

/// ST `Files`: attachment, row deletion and settled uploads leave the rows,
/// queue and write count that `Files.attach` and `Files.finish` describe.
mod differential {
    use super::{files_runner, Fixture};
    use crate::files::file_error::*;
    use coven_database::{CacheFill, DbError, FileRef, Provenance};
    use coven_storage::{ObjectPrefix, Storage, StorageFailure};
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

    /// The model's reference for the n-th attachment: one uploader, fresh id and key.
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
                    json!({"op": "attach", "row": event["row"], "ref": reference(file),
                       "size": bytes(file).len(), "hash": file, "version": 0})
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

    fn number(attached: &[FileRef], file: &FileRef) -> u64 {
        let index = attached
            .iter()
            .position(|known| (known.uploader(), known.id()) == (file.uploader(), file.id()))
            .expect("every row and queued file comes from an attachment");
        index as u64 + 1
    }

    /// Run the events against the real database, queue and upload attempt.
    async fn actual(events: &[Value]) -> Value {
        let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
        // Hold the worker away from the queue; each finish is one attempt.
        let guard = f.files.inner.drain.lock().await;
        f.files.inner.state.lock().unwrap().paused = false;
        let mut attached: Vec<FileRef> = Vec::new();
        let mut rows: Vec<u64> = Vec::new();
        let mut held = None;
        let mut finishes = Vec::new();
        for event in events {
            let file = event["file"].as_u64();
            match event["op"].as_str().unwrap() {
                "attach" => {
                    let row = event["row"].as_u64().unwrap();
                    let file = file.unwrap();
                    attached.push(f.attach("files", &format!("r{row}"), bytes(file)).await);
                    assert_eq!(attached.len() as u64, file);
                    if !rows.contains(&row) {
                        rows.push(row);
                    }
                }
                "delete" => {
                    let row = format!("r{}", event["row"].as_u64().unwrap());
                    f.database
                        .write(move |sql| {
                            sql.execute("DELETE FROM files WHERE id=?1", [row])?;
                            Ok(())
                        })
                        .await
                        .unwrap();
                }
                "begin" => {
                    // The attempt opens and checks its source, then waits at the
                    // provider while later events change the row.
                    let target = attached[file.unwrap() as usize - 1].clone();
                    let item = f
                        .files
                        .inner
                        .database
                        .uploads()
                        .await
                        .unwrap()
                        .into_iter()
                        .find(|upload| upload.file == target)
                        .unwrap();
                    let (started, receiving) = tokio::sync::oneshot::channel();
                    let (release, resume) = tokio::sync::oneshot::channel();
                    f.storage
                        .hold_next_creation(ObjectPrefix::files(f.directory.id()), started, resume)
                        .await;
                    let inner = f.files.inner.clone();
                    let task = tokio::spawn(async move { inner.upload(&item).await });
                    receiving.await.unwrap();
                    held = Some((file.unwrap(), task, release));
                }
                "finish" => {
                    let target = attached[file.unwrap() as usize - 1].clone();
                    match event["remote"].as_str().unwrap() {
                        "stored" => {}
                        "error" => f.storage.set_online(false),
                        "other" => f.storage.create(&f.path(&target), b"other").await.unwrap(),
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
                            let item = f
                                .files
                                .inner
                                .database
                                .uploads()
                                .await
                                .unwrap()
                                .into_iter()
                                .find(|upload| upload.file == target)
                                .unwrap();
                            f.files.inner.upload(&item).await
                        }
                    };
                    f.storage.set_online(true);
                    let settlement = match &result {
                        Ok(true) => "stored",
                        Err(UploadFailure::Storage(error))
                        | Err(UploadFailure::File(FileReadError::Storage(error)))
                            if error.failure() == StorageFailure::Network =>
                        {
                            "pending"
                        }
                        Err(UploadFailure::Storage(error))
                            if error.failure() == StorageFailure::AlreadyExists =>
                        {
                            "reset"
                        }
                        other => panic!("unexpected attempt result {other:?}"),
                    };
                    let changed = f.database.test_queued_writes().await.unwrap().len() != writes;
                    finishes.push(json!({"settlement": settlement, "writes_changed": changed}));
                }
                op => panic!("unknown event {op}"),
            }
        }
        let mut row_refs = Vec::new();
        for row in rows {
            let current = match f
                .database
                .file_ref("files", format!("r{row}").as_str())
                .await
            {
                Ok(file) => reference(number(&attached, &file)),
                Err(DbError::FileAbsent { .. }) => Value::Null,
                Err(error) => panic!("{error}"),
            };
            row_refs.push(json!({"row": row, "ref": current}));
        }
        let queue: Vec<Value> = f
            .files
            .inner
            .database
            .uploads()
            .await
            .unwrap()
            .iter()
            .map(|upload| reference(number(&attached, &upload.file)))
            .collect();
        f.files.inner.state.lock().unwrap().paused = true;
        drop(guard);
        f.close().await;
        json!({"finishes": finishes, "rows": row_refs, "queue": queue})
    }

    /// The model keeps its queue newest first; the order carries no meaning.
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
            // Retargeted, then deleted, while the first file's bytes are in flight.
            vec![
                attach(1, 1),
                begin(1),
                attach(1, 2),
                finish(1, "stored"),
                finish(2, "stored"),
            ],
            vec![attach(1, 1), begin(1), delete(1), finish(1, "stored")],
            // The queue keeps its own source facts, so the row may change
            // before an attempt opens them.
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
