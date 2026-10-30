use super::{files_runner, stored as location, Fixture, CHUNK};
use crate::files::file_status::{status, FileStatus, Local};
use crate::FileSourceFailure;
use crate::{FileMissingReason, FileReadError};
use coven_database::{FileRef, Provenance};
use coven_format::file::FileHeader;
use coven_foundation::id_source::DeviceId;
use coven_storage::{test_utils::MemoryRequest, Storage, StorageFailure};
use serde_json::{json, Value};

/// A second device that received every write `a` made, including the one
/// storing its file.
async fn receiver(a: &Fixture, file: &FileRef) -> (Fixture, FileRef) {
    let b = Fixture::new(Provenance::AppProvided).await;
    b.files
        .set_storage(Some(a.storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    for write in a.database.test_queued_writes().await.unwrap() {
        crate::tests::apply_write(&b.database, write).await.unwrap();
    }
    let remote = b.database.file_ref(file.id()).await.unwrap();
    assert_eq!(&remote, file);
    (b, remote)
}

/// Storage loses a file its record says it holds: status and reads say
/// missing, lost, whoever uploaded it; nothing waits for an upload (§17.5).
#[tokio::test]
async fn a_named_file_absent_from_storage_is_lost() {
    let a = Fixture::new(Provenance::AppProvided).await;
    let file = a.uploaded("photo", vec![61; CHUNK + 9]).await;
    let (b, remote) = receiver(&a, &file).await;
    let device = location(&file).uploader();
    let path = a.path(&file);
    let stored = a.storage.read(&path).await.unwrap();
    a.storage.delete(&path).await.unwrap();
    assert_eq!(
        b.files.file_status(&remote).await.unwrap(),
        FileStatus::Missing {
            device,
            reason: FileMissingReason::Lost
        }
    );
    assert!(matches!(b.files.read_file(&remote).await,
        Err(FileReadError::Missing { device: d, .. }) if d == device));
    assert_eq!(
        b.database
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .into_iter()
            .map(|record| record.reason)
            .collect::<Vec<_>>(),
        [crate::PendingReason::FileUnavailable(
            FileMissingReason::Lost
        )]
    );
    // A provider failure is an error, never proof of absence.
    a.storage.set_online(false);
    assert!(matches!(b.files.file_status(&remote).await,
        Err(FileReadError::Storage(error)) if error.failure() == StorageFailure::Network));
    a.storage.set_online(true);
    // The complete object is available again.
    a.storage.create(&path, &stored).await.unwrap();
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

/// The staging device's own uploading file is uploading, or missing with its
/// source failure, judged with no storage at all (§17.5).
#[tokio::test]
async fn an_uploading_file_is_judged_locally() {
    let f = Fixture::new(Provenance::UserProvided).await;
    f.files.stop_background_for_counts().await;
    f.files
        .set_storage(None, std::future::ready(Ok(())))
        .await
        .unwrap();
    let file = f.original("photo", &[3; 40]).await;
    assert_eq!(
        f.files.file_status(&file).await.unwrap(),
        FileStatus::Uploading
    );
    std::fs::remove_file(f.root.path().join("photo")).unwrap();
    assert!(matches!(
        f.files.read_file(&file).await,
        Err(FileReadError::UserFileMissing { .. })
    ));
    assert_eq!(
        f.files.file_status(&file).await.unwrap(),
        FileStatus::Missing {
            device: f.location(&file).await.uploader(),
            reason: FileMissingReason::Source(FileSourceFailure::Missing)
        }
    );
    f.close().await;
}

#[tokio::test]
async fn an_object_of_the_wrong_size_at_the_fixed_path_is_damage_not_absence() {
    let a = Fixture::new(Provenance::AppProvided).await;
    let file = a.uploaded("photo", vec![62; 40]).await;
    let (b, remote) = receiver(&a, &file).await;
    a.storage.delete(&a.path(&file)).await.unwrap();
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
/// its complete size: the read is damage, as status says (§17.5).
#[tokio::test]
async fn a_longer_object_is_damage_to_status_and_reads() {
    let a = Fixture::new(Provenance::AppProvided).await;
    let file = a.uploaded("photo", vec![64; CHUNK + 9]).await;
    let (b, remote) = receiver(&a, &file).await;
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
    let a = Fixture::new(Provenance::AppProvided).await;
    let file = a.uploaded("photo", vec![63; CHUNK]).await;
    let b = Fixture::new(Provenance::AppProvided).await;
    b.files
        .set_storage(Some(a.storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    // B's record names the same object but expects different content.
    for mut write in a.database.test_queued_writes().await.unwrap() {
        for part in &mut write.parts {
            for row in &mut part.rows {
                if let coven_merge::Operation::Insert(columns)
                | coven_merge::Operation::Update(columns) = &mut row.change.operation
                {
                    // The completing write is the one storing the file.
                    if let Some(hash) = columns
                        .get_mut("hash")
                        .filter(|hash| hash.value != coven_format::value::Value::Null)
                    {
                        hash.value = coven_format::value::Value::Blob(vec![0; 32]);
                    }
                }
            }
        }
        crate::tests::apply_write(&b.database, write).await.unwrap();
    }
    let remote = b.database.file_ref(file.id()).await.unwrap();
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
        Ok(FileStatus::Uploading) => json!("uploading"),
        Ok(FileStatus::DeviceOnly { device }) => json!({"deviceOnly": device.0}),
        Ok(FileStatus::Missing { device, reason }) => json!({
            "missing": device.0,
            "reason": match reason {
                FileMissingReason::Lost => "lost",
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
        Err(FileReadError::Missing { device, .. }) => status_json(Ok(FileStatus::Missing {
            device,
            reason: FileMissingReason::Lost,
        })),
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

/// ST `Files.status` over every observation and local fact, and, through the
/// real status call and read, `Files.status` and `Files.fetch` for absent,
/// failing, complete, longer and truncated objects at a named path.
#[tokio::test]
#[ignore = "requires COVEN_FILES_LEAN; scripts/check.sh builds filesRunner"]
async fn st_differential_file_status() {
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for remote in ["present", "absent", "error"] {
        let locals: [(bool, Option<FileSourceFailure>); 5] = [
            (false, None),
            (true, None),
            (true, Some(FileSourceFailure::Missing)),
            (true, Some(FileSourceFailure::Changed)),
            (true, Some(FileSourceFailure::Integrity)),
        ];
        for (pending, report) in locals {
            cases.push(json!({
                "status": true,
                "ref": {"uploader": 3, "id": 4, "key": 4},
                "size": 1,
                "remote": remote,
                "pending": pending,
                "report": report.map(failure_name),
            }));
            let observed = match remote {
                "present" => Ok(true),
                "absent" => Ok(false),
                _ => Err(()),
            };
            let here: Local = pending.then_some(report);
            actual.push(status_json(status(DeviceId(3), || observed, here)));
        }
    }

    let a = Fixture::new(Provenance::AppProvided).await;
    let original = (0..CHUNK + 9).map(|i| (i % 249) as u8).collect::<Vec<_>>();
    let file = a.uploaded("photo", original.clone()).await;
    let (b, remote) = receiver(&a, &file).await;
    let device = location(&file).uploader();
    let size = FileHeader::new(file.size()).encrypted_size().unwrap();
    let mut case = |remote: &str, length: Option<u64>, ranges| {
        let mut case = json!({
            "status": true,
            "ref": {"uploader": device.0, "id": 1, "key": 1},
            "size": size,
            "remote": remote,
            "pending": false,
            "report": null,
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
    let path = a.path(&file);
    let stored = a.storage.read(&path).await.unwrap();
    assert_eq!(stored.len() as u64, size);
    // Absent: lost by storage.
    a.storage.delete(&path).await.unwrap();
    let ranges = record(observe(&a, &b, &remote, &original).await);
    case("absent", None, ranges);
    a.storage.set_online(false);
    let ranges = record(observe(&a, &b, &remote, &original).await);
    a.storage.set_online(true);
    case("error", None, ranges);
    // Present: the complete object, then a longer one and truncated ones.
    a.storage.create(&path, &stored).await.unwrap();
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
        case("present", Some(bytes.len() as u64), ranges);
    }
    assert_eq!(actual, files_runner(&cases));
    b.close().await;
    a.close().await;
}
