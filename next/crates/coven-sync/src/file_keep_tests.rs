use super::*;
use crate::{operation_data::Data, operations::Progress, SyncError};
use coven_database::{FileLocation, OperationRecord};
use std::{collections::HashMap, path::PathBuf};

async fn record(f: &Fixture, file: &FileRef) -> (OperationRecord, PathBuf) {
    let destination = f.root.path().join("kept");
    f.files
        .record_keeps(
            std::slice::from_ref(file),
            &HashMap::from([(file.id(), destination.clone())]),
        )
        .await
        .unwrap();
    (
        f.database.operations().await.unwrap().remove(0),
        destination,
    )
}
async fn step(f: &Fixture) -> Result<Progress, SyncError> {
    let record = f.database.operations().await.unwrap().remove(0);
    f.files.keep_step(&record, Data::read(&record)?).await
}
async fn uploaded(f: &Fixture, provenance: &Provenance, bytes: &[u8]) -> FileRef {
    let file = match provenance {
        Provenance::AppProvided => f.attach("files", "one", bytes.to_vec()).await,
        Provenance::UserProvided => f.original("one", bytes).await,
    };
    f.enqueue(&file).await;
    f.drain().await;
    f.database.file_ref("files", "one").await.unwrap()
}

#[tokio::test]
async fn keeping_both_kinds_resumes_after_every_step_and_changes_other_devices() {
    for provenance in [Provenance::AppProvided, Provenance::UserProvided] {
        for crash in 0..=2 {
            for bytes in [Vec::new(), vec![83; CHUNK * 3 + 7]] {
                let mut f = Fixture::new(
                    provenance.clone(),
                    Uploads::WhenAttached,
                    CacheFill::CacheLazy,
                )
                .await;
                let file = uploaded(&f, &provenance, &bytes).await;
                let (_, destination) = record(&f, &file).await;
                for _ in 0..crash {
                    assert!(matches!(step(&f).await.unwrap(), Progress::Advanced));
                }
                let before = f.database.operations().await.unwrap().remove(0);
                f.reopen().await;
                let after = f.database.operations().await.unwrap().remove(0);
                assert_eq!(before.last_step, after.last_step);
                assert_eq!(before.data, after.data);
                for _ in crash..2 {
                    assert!(matches!(step(&f).await.unwrap(), Progress::Advanced));
                }
                assert!(matches!(step(&f).await.unwrap(), Progress::Finished(_)));
                assert!(f.database.operations().await.unwrap().is_empty());
                let kept = f.database.file_ref("files", "one").await.unwrap();
                let FileLocation::OnDevice(device) = kept.location() else {
                    panic!("not kept")
                };
                assert_eq!(f.files.read_file(&kept).await.unwrap(), bytes);
                assert_eq!(kept.content_hash(), file.content_hash());
                assert!(f.files.inner.database.uploads().await.unwrap().is_empty());
                if provenance == Provenance::UserProvided {
                    assert_eq!(std::fs::read(&destination).unwrap(), bytes);
                }
                let peer = Fixture::new(
                    provenance.clone(),
                    Uploads::WhenAttached,
                    CacheFill::CacheLazy,
                )
                .await;
                for write in f.database.test_queued_writes().await.unwrap() {
                    peer.database.apply_downloaded(write.into()).await.unwrap();
                }
                let remote = peer.database.file_ref("files", "one").await.unwrap();
                assert_eq!(remote.location(), FileLocation::OnDevice(device));
                assert!(
                    matches!(peer.files.read_file(&remote).await, Err(FileReadError::OnOtherDevice { device: other, .. }) if other == device)
                );
                let path = coven_storage::ObjectPath::file(file.uploaded().unwrap().unwrap().0);
                assert!(!f.storage.read(&path).await.unwrap().is_empty());
                peer.close().await;
                f.close().await;
            }
        }
    }
}

#[tokio::test]
async fn changed_rows_permanently_abandon_downloads_and_preserve_uploads() {
    for provenance in [Provenance::AppProvided, Provenance::UserProvided] {
        let mut f =
            Fixture::new(provenance.clone(), Uploads::WhenAsked, CacheFill::CacheLazy).await;
        let file = uploaded(&f, &provenance, b"obsolete").await;
        let (record, destination) = record(&f, &file).await;
        let location = f
            .files
            .inner
            .database
            .keep_location(record.id)
            .await
            .unwrap();
        step(&f).await.unwrap();
        f.database
            .write(|sql| {
                sql.execute("DELETE FROM files WHERE id='one'", [])?;
                Ok(())
            })
            .await
            .unwrap();
        assert!(matches!(
            step(&f).await,
            Err(SyncError::Database(
                coven_database::DbError::FileRefChanged { .. }
            ))
        ));
        assert!(f
            .directory
            .download(&location)
            .unwrap()
            .open_reader()
            .is_err());
        assert!(!destination.exists());
        f.reopen().await;
        assert!(step(&f).await.is_err());
        let row = f.database.operations().await.unwrap().remove(0);
        assert!(matches!(Data::read(&row).unwrap(), Data::KeepFile(work) if work.obsolete));
        f.files
            .discard_keep(&row, Data::read(&row).unwrap())
            .await
            .unwrap();
        assert!(f.database.operations().await.unwrap().is_empty());
        assert!(!f
            .storage
            .read(&coven_storage::ObjectPath::file(
                file.uploaded().unwrap().unwrap().0
            ))
            .await
            .unwrap()
            .is_empty());
        f.close().await;
    }
}

#[tokio::test]
async fn existing_destinations_are_refused_before_recording_and_at_publication() {
    let f = Fixture::new(
        Provenance::UserProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let file = uploaded(&f, &Provenance::UserProvided, b"new").await;
    let path = f.root.path().join("occupied");
    std::fs::write(&path, b"existing").unwrap();
    assert!(matches!(
        f.files
            .record_keeps(
                std::slice::from_ref(&file),
                &HashMap::from([(file.id(), path.clone())])
            )
            .await,
        Err(SyncError::DestinationExists { .. })
    ));
    assert!(f.database.operations().await.unwrap().is_empty());
    let (row, path) = record(&f, &file).await;
    std::fs::write(&path, b"racer").unwrap();
    assert!(matches!(
        step(&f).await,
        Err(SyncError::DestinationExists { .. })
    ));
    f.files
        .discard_keep(&row, Data::read(&row).unwrap())
        .await
        .unwrap();
    assert_eq!(std::fs::read(path).unwrap(), b"racer");
    f.close().await;
}

fn operation_owner(f: &Fixture) -> crate::Operations {
    use coven_crypto::{custody::InMemoryCustody, MemberKeys, StoreKey, StoreKeyring};
    let sync = crate::StoreLogSync::new(
        f.storage.clone(),
        f.database.clone(),
        Arc::new(InMemoryCustody::new(StoreKeyring::new(
            StoreKey::generate(coven_foundation::id_source::KeyId(f.ids.new_id())).unwrap(),
        ))),
        Arc::new(InMemoryCustody::new(MemberKeys::generate().unwrap())),
        f.clock.clone(),
        f.ids.clone(),
    );
    crate::Operations::new(sync, f.files.clone())
}

#[tokio::test]
async fn acceptance_returns_offline_then_storage_reconnection_finishes_the_operation() {
    let f = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let file = f.uploaded("one", vec![84; CHUNK * 2]).await;
    let operations = operation_owner(&f);
    operations.set_storage(None).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        operations.keep_files_on_this_device(&[file], &HashMap::new()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(operations
        .report()
        .await
        .unwrap()
        .blocked_operations
        .is_empty());
    assert_eq!(f.database.operations().await.unwrap()[0].last_step, 0);
    operations.close().await.unwrap();
    f.storage
        .set_faults(coven_storage::test_utils::Faults {
            fail_next: 100,
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    let operations = operation_owner(&f);
    operations
        .set_storage(Some(f.storage.clone()))
        .await
        .unwrap();
    assert!(operations
        .report()
        .await
        .unwrap()
        .blocked_operations
        .is_empty());
    assert_eq!(f.database.operations().await.unwrap()[0].last_step, 0);
    let mut changes = f.files.inner.database.changes();
    f.storage
        .set_faults(coven_storage::test_utils::Faults::none())
        .await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            changes.next().await.unwrap();
            if matches!(
                f.database
                    .file_ref("files", "one")
                    .await
                    .unwrap()
                    .location(),
                FileLocation::OnDevice(_)
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(operations
        .report()
        .await
        .unwrap()
        .blocked_operations
        .is_empty());
    assert!(f.database.operations().await.unwrap().is_empty());
    let file = f.database.file_ref("files", "one").await.unwrap();
    assert!(matches!(file.location(), FileLocation::OnDevice(_)));
    assert_eq!(f.files.read_file(&file).await.unwrap(), vec![84; CHUNK * 2]);
    operations.close().await.unwrap();
    f.close().await;
}

#[tokio::test]
async fn corrupt_row_hash_blocks_the_operation_and_discard_removes_partial_bytes() {
    let source = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    source.uploaded("one", vec![85; CHUNK * 2]).await;
    let mut f = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    for mut write in source.database.test_queued_writes().await.unwrap() {
        for part in &mut write.parts {
            for row in &mut part.rows {
                if let coven_merge::Operation::Update(columns) = &mut row.change.operation {
                    columns.get_mut("hash").unwrap().value =
                        coven_format::value::Value::Blob(vec![0; 32]);
                }
            }
        }
        f.database.apply_downloaded(write.into()).await.unwrap();
    }
    f.storage = source.storage.clone();
    f.files.set_storage(Some(f.storage.clone()));
    let file = f.database.file_ref("files", "one").await.unwrap();
    let (row, _) = record(&f, &file).await;
    let location = f.files.inner.database.keep_location(row.id).await.unwrap();
    assert!(matches!(
        step(&f).await,
        Err(SyncError::File(FileReadError::Integrity { .. }))
    ));
    let operations = operation_owner(&f);
    let report = operations.report().await.unwrap();
    assert_eq!(report.blocked_operations.len(), 1);
    assert_eq!(report.blocked_operations[0].last_step, 0);
    assert_eq!(
        report.blocked_operations[0].kind,
        crate::OperationKind::ChangeFileLocation
    );
    assert!(operations.retry_blocked_operation(row.id).await.is_err());
    operations.close().await.unwrap();
    f.reopen().await;
    let operations = operation_owner(&f);
    assert_eq!(
        operations.report().await.unwrap().blocked_operations.len(),
        1
    );
    operations.discard_blocked_operation(row.id).await.unwrap();
    assert!(f
        .directory
        .download(&location)
        .unwrap()
        .open_reader()
        .is_err());
    assert!(f.database.operations().await.unwrap().is_empty());
    operations.close().await.unwrap();
    f.close().await;
    source.close().await;
}

#[tokio::test]
async fn bytes_changed_between_steps_cannot_be_attached() {
    for provenance in [Provenance::AppProvided, Provenance::UserProvided] {
        let f = Fixture::new(provenance.clone(), Uploads::WhenAsked, CacheFill::CacheLazy).await;
        let file = uploaded(&f, &provenance, b"checked").await;
        let (row, destination) = record(&f, &file).await;
        let location = f.files.inner.database.keep_location(row.id).await.unwrap();
        step(&f).await.unwrap();
        match &location {
            coven_foundation::files::DownloadLocation::AppProvided(name) => f
                .directory
                .file(coven_foundation::files::FileArea::AppProvided, name)
                .replace(b"changed")
                .unwrap(),
            coven_foundation::files::DownloadLocation::UserProvided { .. } => {
                std::fs::write(&destination, b"changed").unwrap()
            }
        }
        assert!(step(&f).await.is_err());
        assert_eq!(
            f.database
                .file_ref("files", "one")
                .await
                .unwrap()
                .location(),
            FileLocation::Uploaded
        );
        let row = f.database.operations().await.unwrap().remove(0);
        f.files
            .discard_keep(&row, Data::read(&row).unwrap())
            .await
            .unwrap();
        f.close().await;
    }
}

#[tokio::test]
async fn concurrent_keeps_converge_and_release_only_the_losing_owned_copy() {
    for provenance in [Provenance::AppProvided, Provenance::UserProvided] {
        let a = Fixture::new(provenance.clone(), Uploads::WhenAsked, CacheFill::CacheLazy).await;
        let file = uploaded(&a, &provenance, b"shared").await;
        let b = Fixture::new(provenance.clone(), Uploads::WhenAsked, CacheFill::CacheLazy).await;
        b.files.set_storage(Some(a.storage.clone()));
        for write in a.database.test_queued_writes().await.unwrap() {
            b.database.apply_downloaded(write.into()).await.unwrap();
        }
        let other = b.database.file_ref("files", "one").await.unwrap();
        let (ar, ap) = record(&a, &file).await;
        let (br, bp) = record(&b, &other).await;
        let al = a.files.inner.database.keep_location(ar.id).await.unwrap();
        let bl = b.files.inner.database.keep_location(br.id).await.unwrap();
        for f in [&a, &b] {
            for _ in 0..3 {
                step(f).await.unwrap();
            }
        }
        let local_a = a.database.file_ref("files", "one").await.unwrap();
        let local_b = b.database.file_ref("files", "one").await.unwrap();
        for write in b.database.test_queued_writes().await.unwrap() {
            a.database.apply_downloaded(write.into()).await.unwrap();
        }
        for write in a.database.test_queued_writes().await.unwrap() {
            b.database.apply_downloaded(write.into()).await.unwrap();
        }
        let final_a = a.database.file_ref("files", "one").await.unwrap();
        let final_b = b.database.file_ref("files", "one").await.unwrap();
        assert_eq!(final_a, final_b);
        for (f, location, local, destination) in [(&a, al, local_a, ap), (&b, bl, local_b, bp)] {
            if provenance == Provenance::UserProvided {
                assert_eq!(std::fs::read(destination).unwrap(), b"shared");
            } else {
                assert_eq!(
                    f.directory
                        .download(&location)
                        .unwrap()
                        .open_reader()
                        .is_ok(),
                    local.location() == final_a.location()
                );
            }
        }
        a.close().await;
        b.close().await;
    }
}

#[tokio::test]
async fn an_interrupted_download_restarts_its_unfinished_step() {
    let mut f = Fixture::new(
        Provenance::UserProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let bytes = vec![87; CHUNK * 40];
    let file = uploaded(&f, &Provenance::UserProvided, &bytes).await;
    let (row, destination) = record(&f, &file).await;
    f.storage
        .set_faults(coven_storage::test_utils::Faults {
            delay: Duration::from_millis(80),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    let before = f.storage.request_count();
    let mut requests = f.storage.subscribe_requests();
    let files = f.files.clone();
    let task = tokio::spawn(async move { files.keep_step(&row, Data::read(&row).unwrap()).await });
    // Header then two range requests: the first plaintext block has reached
    // the real writer before the second range is requested.
    tokio::time::timeout(Duration::from_secs(10), async {
        while f.storage.request_count() < before + 3 {
            requests.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert_eq!(f.database.operations().await.unwrap()[0].last_step, 0);
    assert!(!destination.exists());
    f.storage
        .set_faults(coven_storage::test_utils::Faults::none())
        .await;
    f.reopen().await;
    for _ in 0..3 {
        step(&f).await.unwrap();
    }
    assert_eq!(std::fs::read(destination).unwrap(), bytes);
    f.close().await;
}

#[tokio::test]
async fn a_crash_after_rename_recovers_only_the_recorded_content_without_storage() {
    for replacement in [
        None,
        Some(b"changed".as_slice()),
        Some(b"longer bytes".as_slice()),
    ] {
        let mut f = Fixture::new(
            Provenance::UserProvided,
            Uploads::WhenAsked,
            CacheFill::CacheLazy,
        )
        .await;
        let file = uploaded(&f, &Provenance::UserProvided, b"checked").await;
        let (row, destination) = record(&f, &file).await;
        let location = f.files.inner.database.keep_location(row.id).await.unwrap();
        step(&f).await.unwrap();
        let coven_foundation::files::DownloadLocation::UserProvided { name, .. } = location else {
            panic!("expected a user destination")
        };
        assert!(!destination
            .with_file_name(format!(".coven-download-{}", name.as_str()))
            .exists());
        // Retain the real publication's disk effects with the journal still at
        // its pre-publication value, as after a crash before the step commits.
        let completed = f.database.operations().await.unwrap().remove(0);
        f.database
            .advance_operation(
                Data::read(&completed)
                    .unwrap()
                    .update(&completed, 0)
                    .unwrap(),
            )
            .await
            .unwrap();
        if let Some(bytes) = replacement {
            std::fs::write(&destination, bytes).unwrap();
        }
        f.reopen().await;
        f.files.set_storage(None);
        if let Some(bytes) = replacement {
            assert!(matches!(
                step(&f).await,
                Err(SyncError::DestinationExists { .. })
            ));
            let row = f.database.operations().await.unwrap().remove(0);
            f.files
                .discard_keep(&row, Data::read(&row).unwrap())
                .await
                .unwrap();
            assert_eq!(std::fs::read(&destination).unwrap(), bytes);
        } else {
            for _ in 0..3 {
                step(&f).await.unwrap();
            }
            let kept = f.database.file_ref("files", "one").await.unwrap();
            assert!(matches!(kept.location(), FileLocation::OnDevice(_)));
            assert_eq!(f.files.read_file(&kept).await.unwrap(), b"checked");
        }
        f.close().await;
    }
}
