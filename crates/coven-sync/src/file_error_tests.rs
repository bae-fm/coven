use super::*;
use std::error::Error;

#[test]
fn upload_recordings_preserve_storage_failures_from_reads_and_writes() {
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
            let recording = serde_json::to_vec(&upload.recording()).unwrap();
            let reopened = UploadFailure::Recorded(serde_json::from_slice(&recording).unwrap());
            assert!(
                matches!(reopened.recording(), RecordedUploadFailure::Storage(actual) if actual == failure)
            );
        }
    }
}
