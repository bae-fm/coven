use super::*;
use crate::providers::OAuthError;
use std::error::Error;

#[test]
fn retryable_reports_only_transport_interruptions_and_throttling() {
    for failure in [StorageFailure::Network, StorageFailure::RateLimited] {
        assert!(StorageError::from(failure).retryable());
    }
    for failure in
        [StorageFailure::Conflict, StorageFailure::AlreadyExists, StorageFailure::Authentication]
    {
        assert!(!StorageError::from(failure).retryable(), "{failure:?}");
    }
}

#[test]
fn encoding_preserves_an_opaque_cause() {
    let error = StorageFailure::Encoding
        .with_source(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid encoded data"));
    assert_eq!(error.failure(), StorageFailure::Encoding);
    assert!(!error.retryable());
    assert_eq!(error.to_string(), "Encoding: invalid encoded data");
    let source = error.source().unwrap().downcast_ref::<std::io::Error>().unwrap();
    assert_eq!(source.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(source.to_string(), "invalid encoded data");
}

/// A panic in a provider request's local work is the request's bug,
/// classified `Bug`, not a file failure; sign-in carries it.
#[test]
fn a_bug_in_a_requests_file_work_is_storages_bug() {
    use coven_foundation::task::{Bug, CarriesBug};
    let bug = Bug { operation: "stage the asset", message: "it panicked".into() };
    let storage = || StorageError::from(coven_foundation::files::FileError::Bug(bug.clone()));
    assert!(matches!(storage(), StorageError::Bug(found) if found == bug));
    assert_eq!(storage().failure(), StorageFailure::Bug);
    let cleanup = StorageError::Cleanup {
        operation: Box::new(StorageFailure::Network.into()),
        cleanup: Box::new(storage()),
    };
    assert_eq!(cleanup.bug(), Some(&bug));
    assert_eq!(OAuthError::Storage(storage()).bug(), Some(&bug));
}

/// A failed cleanup keeps the original operation's failure as its own
/// classification, so a caller retries or reports the operation; it has no
/// failure of the request alone, since its cleanup left state behind (E5).
#[test]
fn a_cleanup_reports_its_operations_failure() {
    let error = StorageError::Cleanup {
        operation: Box::new(StorageFailure::AlreadyExists.into()),
        cleanup: Box::new(StorageFailure::Network.into()),
    };
    assert_eq!(error.failure(), StorageFailure::AlreadyExists);
    assert_eq!(error.own_failure(), None);
    let alone = StorageError::from(StorageFailure::AlreadyExists);
    assert_eq!(alone.own_failure(), Some(StorageFailure::AlreadyExists));
}
