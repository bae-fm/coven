use super::*;
use coven_storage::StorageFailure;
use std::error::Error;

#[test]
fn cache_disk_failures_keep_their_file_error_class() {
    for local in [false, true] {
        let disk = DiskError::Io {
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
