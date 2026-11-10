use super::*;
use coven_storage::StorageFailure;
use std::error::Error;

#[test]
fn cache_disk_failures_keep_their_file_error_class() {
    for local in [false, true] {
        let disk = FileError::Io {
            operation: "write cache",
            path: PathBuf::from("cache"),
            source: Box::new(std::io::Error::other("disk unavailable")),
        };
        let database = DbError::from(disk);
        let read = if local {
            FileReadError::from(LocalFileError::Database(database))
        } else {
            FileReadError::from(database)
        };
        assert!(matches!(read, FileReadError::Disk(_)));
        assert_eq!(read.source().unwrap().to_string(), "disk unavailable");
    }
}

#[test]
fn upload_failures_keep_native_storage_causes_from_reads_and_writes() {
    for failure in [
        StorageFailure::Network,
        StorageFailure::RateLimited,
        StorageFailure::MemberKeysMissing,
        StorageFailure::SessionExpired,
        StorageFailure::InvalidRange,
        StorageFailure::SingleRequestTooLarge { size: 17, limit: 16 },
    ] {
        for reading in [false, true] {
            let error = failure.with_source(std::io::Error::other("native storage cause"));
            let upload = if reading {
                TransferError::from(FileReadError::from(error))
            } else {
                TransferError::from(error)
            };
            assert!(upload.source().unwrap().downcast_ref::<std::io::Error>().is_some());
            assert!(matches!(
                &upload,
                TransferError::File(FileReadError::Storage(actual))
                    if actual.failure() == failure
            ));
        }
    }
}

/// A panic in a file read's or a device-only copy's work is the call's bug,
/// whichever error first carried it, not a disk, lock, database or storage
/// failure.
#[test]
fn a_bug_in_file_work_is_the_calls_bug() {
    use coven_foundation::files::CopyPathError;
    use coven_foundation::task::{Bug, CarriesBug};
    let bug = Bug::new("read the file", "it panicked");
    let reads = [
        FileReadError::from(FileError::Bug(bug.clone())),
        FileReadError::from(StoreLockError::Bug(bug.clone())),
        FileReadError::from(StorageError::Bug(bug.clone())),
        FileReadError::from(DbError::Bug(bug.clone())),
        FileReadError::from(LocalFileError::Bug(bug.clone())),
    ];
    for read in reads {
        assert!(matches!(&read, FileReadError::Bug(found) if *found == bug), "{read:?}");
        assert_eq!(read.bug(), Some(&bug));
    }
    let path = CopyPathError::from(FileError::Bug(bug.clone()));
    assert!(matches!(&path, CopyPathError::Bug(found) if *found == bug));
    let copies = [
        crate::DeviceOnlyError::from(path),
        crate::DeviceOnlyError::from(DbError::Bug(bug.clone())),
        crate::DeviceOnlyError::from(FileError::Bug(bug.clone())),
    ];
    for copy in copies {
        assert!(matches!(&copy, crate::DeviceOnlyError::Bug(found) if *found == bug), "{copy:?}");
        assert_eq!(copy.bug(), Some(&bug));
    }
}
