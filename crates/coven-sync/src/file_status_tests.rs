use super::{files_runner, Fixture, CHUNK};
use crate::files::file_status::{status, FileStatus};
use crate::{FileMissingReason, FileReadError, PendingReason, PendingRecord, PendingSubject};
use coven_database::{CacheFill, DeviceState, FileRef, Provenance};
use coven_format::pending::FileSourceFailure;
use coven_foundation::id_source::DeviceId;
use coven_storage::{Storage, StorageFailure};
use serde_json::{json, Value};

/// A second device that received the attaching write but not the bytes.
async fn receiver(a: &Fixture, file: &FileRef) -> (Fixture, FileRef) {
    let b = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    b.files
        .set_storage(Some(a.storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    for write in a.database.test_queued_writes().await.unwrap() {
        b.database
            .apply_downloaded::<PendingRecord>(write.into(), std::time::UNIX_EPOCH)
            .await
            .unwrap();
    }
    let remote = b.database.file_ref("files", "photo").await.unwrap();
    assert_eq!(
        (remote.uploader(), remote.id()),
        (file.uploader(), file.id())
    );
    (b, remote)
}

async fn report(b: &Fixture, file: &FileRef, failure: FileSourceFailure) {
    b.database
        .replace_pending_reports(vec![PendingRecord {
            subject: PendingSubject::File {
                device: file.uploader(),
                file: file.id(),
            },
            reason: PendingReason::FileUnavailable(FileMissingReason::Source(failure)),
            reported_by: file.uploader(),
        }])
        .await
        .unwrap();
}

#[tokio::test]
async fn absence_is_judged_by_the_uploaders_state_and_signed_report() {
    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = a.attach("files", "photo", vec![61; CHUNK + 9]).await;
    let (b, remote) = receiver(&a, &file).await;
    let device = file.uploader();
    // Confirmed absent, active uploader, no report: still uploading.
    assert_eq!(
        b.files.file_status(&remote).await.unwrap(),
        FileStatus::Uploading { device }
    );
    assert!(matches!(b.files.read_file(&remote).await,
        Err(FileReadError::Uploading { device: d, .. }) if d == device));
    // A's signed report that its source changed makes the file missing.
    report(&b, &remote, FileSourceFailure::Changed).await;
    let changed = FileMissingReason::Source(FileSourceFailure::Changed);
    assert_eq!(
        b.files.file_status(&remote).await.unwrap(),
        FileStatus::Missing {
            device,
            reason: changed.clone()
        }
    );
    assert!(matches!(b.files.read_file(&remote).await,
        Err(FileReadError::Missing { device: d, reason, .. }) if d == device && reason == changed));
    // The uploader's state takes precedence over its source report.
    for (state, reason) in [
        (DeviceState::Removed, FileMissingReason::DeviceRemoved),
        (DeviceState::Replaced, FileMissingReason::DeviceReplaced),
    ] {
        b.database.test_device_state(device, state).await.unwrap();
        assert_eq!(
            b.files.file_status(&remote).await.unwrap(),
            FileStatus::Missing { device, reason }
        );
    }
    // A provider failure is an error, never proof of absence.
    a.storage.set_online(false);
    assert!(matches!(b.files.file_status(&remote).await,
        Err(FileReadError::Storage(error)) if error.failure() == StorageFailure::Network));
    a.storage.set_online(true);
    // The complete object wins over an old report and the uploader's state.
    a.drain().await;
    assert_eq!(
        b.files.file_status(&remote).await.unwrap(),
        FileStatus::Available
    );
    assert_eq!(
        b.files.read_file(&remote).await.unwrap(),
        vec![61; CHUNK + 9]
    );
    b.files
        .set_storage(None, std::future::ready(Ok(())))
        .await
        .unwrap();
    assert!(matches!(
        b.files.file_status(&remote).await,
        Err(FileReadError::NoStorage)
    ));
    b.close().await;
    a.close().await;
}

#[tokio::test]
async fn an_object_of_the_wrong_size_at_the_fixed_path_is_damage_not_absence() {
    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = a.attach("files", "photo", vec![62; 40]).await;
    let (b, remote) = receiver(&a, &file).await;
    a.storage
        .create(&a.path(&file), b"truncated")
        .await
        .unwrap();
    assert!(matches!(
        b.files.file_status(&remote).await,
        Err(FileReadError::Integrity { .. })
    ));
    let read = b.files.read_file(&remote).await;
    assert!(
        matches!(read, Err(FileReadError::Integrity { .. })),
        "{read:?}"
    );
    b.close().await;
    a.close().await;
}

#[tokio::test]
async fn an_authenticated_object_with_other_content_is_present_but_unreadable() {
    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = a.uploaded("photo", vec![63; CHUNK]).await;
    let b = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    b.files
        .set_storage(Some(a.storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    // B's row names the same object but expects different content.
    for mut write in a.database.test_queued_writes().await.unwrap() {
        for part in &mut write.parts {
            for row in &mut part.rows {
                if let coven_merge::Operation::Insert(columns) = &mut row.change.operation {
                    columns.get_mut("hash").unwrap().value =
                        coven_format::value::Value::Blob(vec![0; 32]);
                }
            }
        }
        b.database
            .apply_downloaded::<PendingRecord>(write.into(), std::time::UNIX_EPOCH)
            .await
            .unwrap();
    }
    let remote = b.database.file_ref("files", "photo").await.unwrap();
    assert_eq!(remote.id(), file.id());
    assert_eq!(
        b.files.file_status(&remote).await.unwrap(),
        FileStatus::Available
    );
    assert!(matches!(
        b.files.read_file(&remote).await,
        Err(FileReadError::Integrity { .. })
    ));
    b.close().await;
    a.close().await;
}

fn state_name(state: DeviceState) -> &'static str {
    match state {
        DeviceState::Active => "active",
        DeviceState::Removed => "removed",
        DeviceState::Replaced => "replaced",
    }
}

fn failure_name(failure: FileSourceFailure) -> &'static str {
    match failure {
        FileSourceFailure::Missing => "missing",
        FileSourceFailure::Changed => "changed",
        FileSourceFailure::Integrity => "integrity",
    }
}

fn status_json(result: Result<FileStatus, ()>) -> Value {
    match result {
        Err(()) => json!({"error": true}),
        Ok(FileStatus::Available) => json!("available"),
        Ok(FileStatus::Uploading { device }) => json!({"uploading": device.0}),
        Ok(FileStatus::Missing { device, reason }) => json!({
            "missing": device.0,
            "reason": match reason {
                FileMissingReason::DeviceRemoved => "removed",
                FileMissingReason::DeviceReplaced => "replaced",
                FileMissingReason::Source(failure) => failure_name(failure),
            },
        }),
    }
}

#[test]
#[ignore = "requires COVEN_FILES_LEAN; scripts/check.sh builds filesRunner"]
fn st_differential_file_status() {
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for remote in ["present", "absent", "error"] {
        for state in [
            DeviceState::Active,
            DeviceState::Removed,
            DeviceState::Replaced,
        ] {
            for report in [
                None,
                Some(FileSourceFailure::Missing),
                Some(FileSourceFailure::Changed),
                Some(FileSourceFailure::Integrity),
            ] {
                cases.push(json!({
                    "status": true,
                    "ref": {"uploader": 3, "id": 4, "key": 4},
                    "remote": remote,
                    "device": state_name(state),
                    "report": report.map(failure_name),
                }));
                let observed = match remote {
                    "present" => Ok(true),
                    "absent" => Ok(false),
                    _ => Err(()),
                };
                actual.push(status_json(status(DeviceId(3), observed, state, report)));
            }
        }
    }
    assert_eq!(actual, files_runner(&cases));
}
