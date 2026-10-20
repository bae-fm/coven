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

#[test]
fn filesystem_and_settings_errors_accept_boxed_native_causes() {
    use crate::{DiskError, SettingsError};
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
        let disk: DiskError = error;
        assert_eq!(
            disk.source()
                .unwrap()
                .downcast_ref::<io::Error>()
                .unwrap()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        let original_message = disk.to_string();
        let combined = FileError::Cleanup {
            operation: Box::new(disk),
            cleanup: Box::new(io::Error::from(io::ErrorKind::Interrupted)),
        };
        assert_eq!(combined.source().unwrap().to_string(), original_message);
        let FileError::Cleanup { operation, cleanup } = combined else {
            unreachable!()
        };
        assert_eq!(
            operation
                .source()
                .unwrap()
                .downcast_ref::<io::Error>()
                .unwrap()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            cleanup.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::Interrupted
        );
    }
    let corrupt = SettingsError::Corrupt(Box::new(std::fmt::Error));
    assert!(corrupt.source().unwrap().is::<std::fmt::Error>());
}

/// A removal or a required update reaches setup and unlock by name, whether
/// the pass returned it or stopped at a record of it; anything else keeps
/// its boxed cause.
#[test]
fn setup_and_unlock_name_a_removal_and_a_required_update() {
    use crate::{PendingReason, PendingRecord, PendingSubject, RequiredUpdate, Slot};
    let update = RequiredUpdate::CovenFormat { version: 9 };
    let blocked = |reason| {
        SyncError::Pending(Box::new(PendingRecord::new(
            PendingSubject::Slot(Slot(4)),
            reason,
        )))
    };
    let removals: [fn() -> SyncError; 2] = [
        || SyncError::Removed,
        || {
            SyncError::Pending(Box::new(PendingRecord::new(
                PendingSubject::Slot(Slot(4)),
                PendingReason::Removed,
            )))
        },
    ];
    for error in removals {
        assert!(matches!(
            unlock_error(error()),
            StoreKeyUnlockError::Removed
        ));
        assert!(matches!(setup_error(error()), StorageSetupError::Removed));
    }
    for error in [
        SyncError::UpdateRequired(update),
        blocked(PendingReason::UpdateRequired(update)),
    ] {
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
        setup_error(SyncError::KeyUnavailable(crate::KeyId(uuid::Uuid::nil()))),
        StorageSetupError::Internal(_)
    ));
}
