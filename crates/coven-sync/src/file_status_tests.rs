use super::{files_runner, Fixture, CHUNK};
use crate::files::file_status::{status, FileStatus};
use crate::{FileMissingReason, FileReadError, PendingReason, PendingRecord, PendingSubject};
use coven_database::{CacheFill, DeviceState, FileRef, Provenance};
use coven_format::{file::FileHeader, pending::FileSourceFailure};
use coven_foundation::id_source::DeviceId;
use coven_storage::{test_utils::MemoryRequest, Storage, StorageFailure};
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
            .apply_downloaded::<PendingRecord>(
                write.into(),
                coven_format::value::StorageTime::UNIX_EPOCH,
            )
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

/// A longer object holds every range the read asks for, but the reply reports
/// its complete size: the read is damage, as status says (§16.3).
#[tokio::test]
async fn a_longer_object_is_damage_to_status_and_reads() {
    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = a.attach("files", "photo", vec![64; CHUNK + 9]).await;
    let (b, remote) = receiver(&a, &file).await;
    a.drain().await;
    let path = a.path(&file);
    let mut longer = a.storage.read(&path).await.unwrap();
    longer.push(0);
    a.storage.delete(&path).await.unwrap();
    a.storage.create(&path, &longer).await.unwrap();
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
            .apply_downloaded::<PendingRecord>(
                write.into(),
                coven_format::value::StorageTime::UNIX_EPOCH,
            )
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

fn status_result(result: Result<FileStatus, FileReadError>) -> Value {
    match result {
        Ok(status) => status_json(Ok(status)),
        Err(FileReadError::Integrity { .. }) => json!("integrity"),
        Err(FileReadError::Storage(error)) if error.failure() == StorageFailure::Network => {
            json!({"error": true})
        }
        Err(error) => panic!("unexpected status failure {error:?}"),
    }
}

fn read_result(result: Result<Vec<u8>, FileReadError>, original: &[u8]) -> Value {
    match result {
        Ok(bytes) => {
            assert_eq!(bytes, original);
            json!("ok")
        }
        Err(FileReadError::Uploading { device, .. }) => {
            status_json(Ok(FileStatus::Uploading { device }))
        }
        Err(FileReadError::Missing { device, reason, .. }) => {
            status_json(Ok(FileStatus::Missing { device, reason }))
        }
        Err(error) => status_result(Err(error)),
    }
}

/// One observation through the real status call and whole-file read on a
/// receiving device with nothing cached, with the ranges the read requested.
async fn observe(
    a: &Fixture,
    b: &Fixture,
    remote: &FileRef,
    original: &[u8],
) -> (Value, Value, Vec<Value>) {
    b.files.evict_file(remote).await.unwrap();
    let status = status_result(b.files.file_status(remote).await);
    let before = a.storage.request_trace().len();
    let read = read_result(b.files.read_file(remote).await, original);
    let path = a.path(remote);
    let ranges = a.storage.request_trace()[before..]
        .iter()
        .filter_map(|request| match request {
            MemoryRequest::Read(read, Some(range)) if *read == path => {
                Some(json!([range.start(), range.end()]))
            }
            _ => None,
        })
        .collect();
    (status, read, ranges)
}

/// ST `Files.status` over every observation, uploader state and report, and,
/// through the real status call and read, `Files.status` and `Files.fetch`
/// for absent, failing, complete, longer and truncated objects.
#[tokio::test]
#[ignore = "requires COVEN_FILES_LEAN; scripts/check.sh builds filesRunner"]
async fn st_differential_file_status() {
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
                    "size": 1,
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

    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let original = (0..CHUNK + 9).map(|i| (i % 249) as u8).collect::<Vec<_>>();
    let file = a.attach("files", "photo", original.clone()).await;
    let (b, remote) = receiver(&a, &file).await;
    let device = file.uploader();
    let size = FileHeader::new(file.plaintext_size())
        .encrypted_size()
        .unwrap();
    let mut evidence = ("active", None);
    let mut case = |remote: &str, length: Option<u64>, evidence: (&str, Option<&str>), ranges| {
        let mut case = json!({
            "status": true,
            "ref": {"uploader": device.0, "id": 1, "key": 1},
            "size": size,
            "remote": remote,
            "device": evidence.0,
            "report": evidence.1,
            "read": ranges,
        });
        if let Some(length) = length {
            case["length"] = json!(length);
        }
        cases.push(case);
    };
    let mut record = |(status, read, ranges): (Value, Value, Vec<Value>)| {
        actual.push(json!({"status": status, "read": read}));
        ranges
    };
    // Absent: judged by the uploader's evidence.
    let ranges = record(observe(&a, &b, &remote, &original).await);
    case("absent", None, evidence, ranges);
    a.storage.set_online(false);
    let ranges = record(observe(&a, &b, &remote, &original).await);
    a.storage.set_online(true);
    case("error", None, evidence, ranges);
    report(&b, &remote, FileSourceFailure::Changed).await;
    evidence = ("active", Some("changed"));
    let ranges = record(observe(&a, &b, &remote, &original).await);
    case("absent", None, evidence, ranges);
    b.database
        .test_device_state(device, DeviceState::Removed)
        .await
        .unwrap();
    evidence = ("removed", Some("changed"));
    let ranges = record(observe(&a, &b, &remote, &original).await);
    case("absent", None, evidence, ranges);
    // Present: the complete object, then a longer one and truncated ones.
    a.drain().await;
    let path = a.path(&file);
    let stored = a.storage.read(&path).await.unwrap();
    assert_eq!(stored.len() as u64, size);
    let mut longer = stored.clone();
    longer.extend_from_slice(&[0; 5]);
    for bytes in [
        stored.clone(),
        longer,
        stored[..stored.len() - 7].to_vec(),
        stored[..5].to_vec(),
    ] {
        a.storage.delete(&path).await.unwrap();
        a.storage.create(&path, &bytes).await.unwrap();
        let ranges = record(observe(&a, &b, &remote, &original).await);
        case("present", Some(bytes.len() as u64), evidence, ranges);
    }
    assert_eq!(actual, files_runner(&cases));
    b.close().await;
    a.close().await;
}
