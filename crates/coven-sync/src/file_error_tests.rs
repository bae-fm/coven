use super::*;
use coven_storage::StorageFailure;
use std::error::Error;

#[test]
fn upload_failures_keep_native_storage_causes_from_reads_and_writes() {
    for failure in [
        StorageFailure::Network,
        StorageFailure::RateLimited,
        StorageFailure::MemberKeysMissing,
        StorageFailure::SessionExpired,
        StorageFailure::InvalidRange,
        StorageFailure::SingleRequestTooLarge {
            size: 17,
            limit: 16,
        },
    ] {
        for reading in [false, true] {
            let error = failure.with_source(std::io::Error::other("native storage cause"));
            let upload = if reading {
                UploadFailure::from(FileReadError::from(error))
            } else {
                UploadFailure::from(error)
            };
            assert!(upload
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .is_some());
            assert!(matches!(
                &upload,
                UploadFailure::Storage(actual) | UploadFailure::File(FileReadError::Storage(actual))
                    if actual.failure() == failure
            ));
        }
    }
}
