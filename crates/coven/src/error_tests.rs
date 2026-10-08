use super::*;
use crate::{StorageFailure, SyncFailure};
use std::error::Error;

#[test]
fn setup_unlock_and_sync_preserve_the_storage_classification_and_cause() {
    for failure in [
        StorageFailure::RateLimited,
        StorageFailure::NotFound,
        StorageFailure::AlreadyExists,
        StorageFailure::Refused,
        StorageFailure::MemberKeysMissing,
    ] {
        let error =
            || SyncError::from(failure.with_source(std::io::Error::other("native storage cause")));
        let StorageSetupError::Storage(setup) = setup_error(error()) else {
            panic!("setup discarded the storage failure")
        };
        let StoreKeyUnlockError::Storage(unlock) = unlock_error(error()) else {
            panic!("unlock discarded the storage failure")
        };
        let SyncFailure::Storage(sync) = SyncFailure::from(error()) else {
            panic!("sync discarded the storage failure")
        };
        for error in [&setup, &unlock, sync.as_ref()] {
            assert_eq!(error.failure(), failure);
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<std::io::Error>()
                    .unwrap()
                    .to_string(),
                "native storage cause"
            );
        }
    }
}
