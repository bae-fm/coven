use super::*;
use crate::{StorageFailure, SyncFailure};
use coven_merge::slot;
use std::error::Error;

#[test]
fn setup_preserves_sign_in_and_custody_causes() {
    let network = StorageError::Provider {
        provider: crate::CloudProvider::Dropbox,
        failure: StorageFailure::Network,
        source: Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "token endpoint")),
    };
    let setup = StorageSetupError::from(OAuthError::Storage(network));
    let StorageSetupError::OAuth(OAuthError::Storage(error)) = setup else {
        panic!("sign-in cause was lost")
    };
    assert_eq!(error.failure(), StorageFailure::Network);
    assert_eq!(
        error.source().unwrap().downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::TimedOut,
    );
    let setup = StorageSetupError::from(KeyError::Unsupported);
    assert!(matches!(setup, StorageSetupError::SecureStorage(KeyError::Unsupported)));
}

/// A panic in a storage or custody call is setup's bug; a check's storage
/// failure keeps one spelling, `Storage`.
#[test]
fn setup_classifies_a_bug_and_a_checks_storage_failure() {
    use coven_foundation::task::Bug;
    let bug = Bug { operation: "stage the asset", message: "it panicked".into() };
    let storage = || StorageError::from(FileError::Bug(bug.clone()));
    assert!(
        matches!(StorageSetupError::from(storage()), StorageSetupError::Bug(found) if found == bug)
    );
    assert!(
        matches!(StorageSetupError::from(KeyError::Bug(bug.clone())), StorageSetupError::Bug(found) if found == bug)
    );
    let check = StorageCheckError::from(StorageError::from(StorageFailure::Network));
    assert!(matches!(
        StorageSetupError::from(check),
        StorageSetupError::Storage(error) if error.failure() == StorageFailure::Network
    ));
    let versioning =
        StorageCheckError::S3VersioningUnsupported { state: crate::S3BucketVersioning::Enabled };
    assert!(matches!(
        StorageSetupError::from(versioning),
        StorageSetupError::Check(StorageCheckError::S3VersioningUnsupported { .. })
    ));
}

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
                error.source().unwrap().downcast_ref::<std::io::Error>().unwrap().to_string(),
                "native storage cause"
            );
        }
    }
}

#[test]
fn filesystem_and_settings_errors_accept_boxed_native_causes() {
    use crate::{FileError, SettingsError};
    use std::{io, path::PathBuf};

    for error in [
        FileError::Io {
            operation: "read file",
            path: PathBuf::from("settings.json"),
            source: Box::new(io::Error::from(io::ErrorKind::PermissionDenied)),
        },
        FileError::AfterReplace {
            path: PathBuf::from("settings.json"),
            source: Box::new(io::Error::from(io::ErrorKind::PermissionDenied)),
        },
        FileError::AfterRemove {
            path: PathBuf::from("settings.json"),
            source: Box::new(io::Error::from(io::ErrorKind::PermissionDenied)),
        },
    ] {
        let disk: FileError = error;
        assert_eq!(
            disk.source().unwrap().downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::PermissionDenied
        );
        let original_message = disk.to_string();
        let combined = FileError::Cleanup {
            operation: Box::new(disk),
            cleanup: Box::new(io::Error::from(io::ErrorKind::Interrupted)),
        };
        assert_eq!(combined.source().unwrap().to_string(), original_message);
        let FileError::Cleanup { operation, cleanup } = combined else { unreachable!() };
        assert_eq!(
            operation.source().unwrap().downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(cleanup.downcast_ref::<io::Error>().unwrap().kind(), io::ErrorKind::Interrupted);
    }
    let corrupt = SettingsError::Corrupt(Box::new(std::fmt::Error));
    assert!(corrupt.source().unwrap().is::<std::fmt::Error>());
}

/// A removal or a required update reaches setup and unlock by name, whether
/// the pass returned it or stopped at a record of it; anything else keeps
/// its boxed cause.
#[test]
fn setup_and_unlock_name_a_removal_and_a_required_update() {
    use crate::{PendingReason, PendingRecord, PendingSubject, RequiredUpdate};
    let update = RequiredUpdate::CovenFormat { version: 9 };
    let blocked = |reason| {
        SyncError::Pending(Box::new(PendingRecord::new(PendingSubject::Slot(slot(4)), reason)))
    };
    let removals: [fn() -> SyncError; 2] = [
        || SyncError::Removed,
        || {
            SyncError::Pending(Box::new(PendingRecord::new(
                PendingSubject::Slot(slot(4)),
                PendingReason::Removed,
            )))
        },
    ];
    for error in removals {
        assert!(matches!(unlock_error(error()), StoreKeyUnlockError::Removed));
        assert!(matches!(setup_error(error()), StorageSetupError::Removed));
    }
    for error in [SyncError::UpdateRequired(update), blocked(PendingReason::UpdateRequired(update))]
    {
        let unlock = unlock_error(error);
        assert!(
            matches!(unlock, StoreKeyUnlockError::UpdateRequired(required) if required == update),
            "{unlock:?}"
        );
    }
    assert!(matches!(
        setup_error(SyncError::UpdateRequired(update)),
        StorageSetupError::UpdateRequired(required) if required == update
    ));
    assert!(matches!(
        unlock_error(blocked(PendingReason::NoStorage)),
        StoreKeyUnlockError::Other(_)
    ));
    assert!(matches!(
        setup_error(SyncError::KeyUnavailable {
            audience: coven_merge::Audience::Store,
            key: crate::KeyId(uuid::Uuid::nil()),
        }),
        StorageSetupError::Internal(_)
    ));
}

/// A panic taking a store's lock is a bug in each error that carries the
/// lock's failure, not a disk failure.
#[test]
fn a_bug_taking_a_stores_lock_is_each_errors_bug() {
    use crate::task::{Bug, CarriesBug};
    let bug = Bug { operation: "lock the store", message: "the lock panicked".into() };
    let lock = || StoreLockError::from(FileError::Bug(bug.clone()));
    assert!(matches!(lock(), StoreLockError::Bug(found) if found == bug));
    assert!(
        matches!(crate::CovenError::from(lock()), crate::CovenError::Bug(found) if found == bug)
    );
    assert!(matches!(crate::DbError::from(lock()), crate::DbError::Bug(found) if found == bug));
    assert!(matches!(SyncError::from(lock()), SyncError::Bug(found) if found == bug));
    assert!(
        matches!(StoreDeletionError::from(lock()), StoreDeletionError::Bug(found) if found == bug)
    );
    let in_cleanup = StoreLockError::File(FileError::Cleanup {
        operation: Box::new(FileError::Bug(bug.clone())),
        cleanup: Box::new(std::io::Error::from(std::io::ErrorKind::Interrupted)),
    });
    assert_eq!(crate::CovenError::from(in_cleanup).bug(), Some(&bug));
}

/// A panic listing the stores is the listing's bug, not a disk failure.
#[test]
fn a_bug_listing_the_stores_is_the_listings_bug() {
    use crate::task::{Bug, CarriesBug};
    use crate::StoreListError;
    let bug = Bug { operation: "list stores", message: "the listing panicked".into() };
    let listing = StoreListError::from(FileError::Bug(bug.clone()));
    assert!(matches!(&listing, StoreListError::Bug(found) if *found == bug));
    assert_eq!(listing.bug(), Some(&bug));
    let shutdown = crate::task::Shutdown {
        operation: "list stores",
        outcome: crate::task::ShutdownOutcome::NotRun,
    };
    assert!(matches!(
        StoreListError::from(FileError::Shutdown(shutdown)),
        StoreListError::Shutdown(found) if found == shutdown
    ));
}

/// A bug in any error coven's own calls keep is the call's bug: creation's,
/// deletion's, bootstrap's, unlocking's and a read-only open's.
#[test]
fn a_bug_in_what_coven_calls_keep_is_the_calls_bug() {
    use crate::task::{Bug, CarriesBug};
    use crate::{BootstrapError, CovenError, IdentityError, ReadOnlyOpenError};
    use coven_foundation::files::BootstrapDirectoryError;
    let bug = Bug { operation: "create the store", message: "it panicked".into() };
    let directory = || BootstrapDirectoryError::from(FileError::Bug(bug.clone()));
    assert!(matches!(directory(), BootstrapDirectoryError::Bug(found) if found == bug));
    let locked = BootstrapDirectoryError::from(StoreLockError::Bug(bug.clone()));
    assert!(matches!(locked, BootstrapDirectoryError::Bug(found) if found == bug));
    for creation in [
        StoreCreationError::from(directory()),
        StoreCreationError::from(CovenError::Bug(bug.clone())),
        StoreCreationError::from(IdentityError::Bug(bug.clone())),
        StoreCreationError::from(SyncError::Bug(bug.clone())),
    ] {
        assert!(
            matches!(&creation, StoreCreationError::Bug(found) if *found == bug),
            "{creation:?}"
        );
    }
    for bootstrap in [
        BootstrapError::from(directory()),
        BootstrapError::from(CovenError::Bug(bug.clone())),
        BootstrapError::from(SyncError::Bug(bug.clone())),
        BootstrapError::from(KeyError::Bug(bug.clone())),
        BootstrapError::from(coven_storage::providers::OAuthError::Storage(StorageError::Bug(
            bug.clone(),
        ))),
    ] {
        assert!(matches!(&bootstrap, BootstrapError::Bug(found) if *found == bug), "{bootstrap:?}");
        assert_eq!(bootstrap.bug(), Some(&bug));
    }
    for deletion in [
        StoreDeletionError::from(SyncError::Bug(bug.clone())),
        StoreDeletionError::from(KeyError::Bug(bug.clone())),
        StoreDeletionError::from(FileError::Bug(bug.clone())),
    ] {
        assert!(
            matches!(&deletion, StoreDeletionError::Bug(found) if *found == bug),
            "{deletion:?}"
        );
    }
    for unlock in [
        StoreKeyUnlockError::from(StorageError::Bug(bug.clone())),
        StoreKeyUnlockError::from(KeyError::Bug(bug.clone())),
        unlock_error(SyncError::Storage(StorageError::Bug(bug.clone()))),
    ] {
        assert!(matches!(&unlock, StoreKeyUnlockError::Bug(found) if *found == bug), "{unlock:?}");
    }
    for open in [
        ReadOnlyOpenError::from(CovenError::Bug(bug.clone())),
        ReadOnlyOpenError::from(SyncError::Bug(bug.clone())),
    ] {
        assert!(matches!(&open, ReadOnlyOpenError::Bug(found) if *found == bug), "{open:?}");
        assert_eq!(open.bug(), Some(&bug));
    }
}

/// A storage setup or key unlock that meets a runtime shutting down reports
/// the shutdown, naming whether anything ran, and not an internal failure (E15).
#[test]
fn a_setup_or_unlock_on_a_shutting_down_runtime_reports_the_shutdown() {
    use crate::task::{Shutdown, ShutdownOutcome};
    use crate::{StorageSetupError, StoreKeyUnlockError};
    let shutdown = Shutdown { operation: "set up storage", outcome: ShutdownOutcome::NotRun };
    let setup = super::setup_error(SyncError::Shutdown(shutdown));
    assert!(
        matches!(&setup, StorageSetupError::Shutdown(found) if *found == shutdown),
        "{setup:?}"
    );
    let unlock = super::unlock_error(SyncError::Shutdown(shutdown));
    assert!(
        matches!(&unlock, StoreKeyUnlockError::Shutdown(found) if *found == shutdown),
        "{unlock:?}"
    );
}
