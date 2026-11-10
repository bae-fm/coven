use super::*;
use coven_foundation::id_source::DeviceId;
use coven_merge::{MergeError, WriteId};
use coven_storage::ObjectPath;
use std::error::Error;

#[test]
fn newer_key_envelopes_require_an_update_at_the_sync_boundary() {
    for version in [coven_format::FORMAT_VERSION + 1, 0] {
        let mut bytes = coven_format::sealed_slot::SlotObjectPrefix {
            format: coven_format::FormatVersion::CURRENT,
            key: coven_foundation::id_source::KeyId(uuid::Uuid::from_u128(1)),
            kind: coven_format::sealed_slot::SlotKind::Admin,
        }
        .encode()
        .unwrap();
        bytes[1..3].copy_from_slice(&version.to_be_bytes());
        let error = coven_format::sealed_slot::SlotObjectPrefix::decode(&bytes).unwrap_err();
        let error = SyncError::from(error);
        if version == 0 {
            assert!(matches!(error, SyncError::Format(coven_format::Error::UnsupportedVersion(0))));
        } else {
            assert!(matches!(error, SyncError::UpdateRequired(_)));
        }
    }
}

#[test]
fn merge_causality_refusal_preserves_its_native_cause() {
    let write = WriteId { device: DeviceId(1), number: 2 };
    let path =
        ObjectPath::slot(coven_storage::test_utils::STORE_ID, write.number.try_into().unwrap());
    let error = crate::write_object::database_failure(
        &path,
        DbError::InvalidWrite { write, error: MergeError::CausalTimestamp(write) },
    );
    let SyncError::Damaged(object) = error else { panic!("{error:?}") };
    assert_eq!(object.failure, crate::Refusal::InvalidCausality { cause: None });
    assert_eq!(object.path, path.as_str());
    assert_eq!(
        object.failure.source().unwrap().downcast_ref::<MergeError>(),
        Some(&MergeError::CausalTimestamp(write))
    );
}

#[test]
fn object_validation_keeps_causes_and_local_failures_stay_local() {
    let write = WriteId { device: DeviceId(1), number: 1 };
    let path =
        ObjectPath::slot(coven_storage::test_utils::STORE_ID, write.number.try_into().unwrap());
    for error in [
        DbError::InvalidWrite { write, error: MergeError::GenerationParity(1) },
        DbError::Snapshot(coven_database::SnapshotError::Inconsistent("row outside snapshot")),
    ] {
        let SyncError::Damaged(object) = crate::write_object::database_failure(&path, error) else {
            panic!("expected an invalid object");
        };
        assert_eq!(object.failure, crate::Refusal::InvalidWrite { cause: None });
        assert!(object.failure.source().unwrap().is::<DbError>());
    }
    let SyncError::Damaged(object) = crate::write_object::database_failure(
        &path,
        DbError::WriteFormat(coven_format::Error::Truncated),
    ) else {
        panic!("expected malformed bytes")
    };
    assert_eq!(object.failure, crate::Refusal::Parse { cause: None });
    assert_eq!(
        object.failure.source().unwrap().downcast_ref::<coven_format::Error>(),
        Some(&coven_format::Error::Truncated)
    );
    assert!(matches!(
        crate::write_object::database_failure(&path, DbError::DamagedDatabase),
        SyncError::Database(DbError::DamagedDatabase)
    ));
}

#[test]
fn refusals_of_syncs_own_requests_fail_internally_and_keep_their_native_cause() {
    // A broken rule of coven's own is a bug; a changed operation a database
    // failure an app can't act on.
    let internal = [
        (
            DbError::Invariant { what: "a broken rule of coven's own code" },
            crate::LocalFailure::Bug,
        ),
        (DbError::OperationChanged(coven_database::OperationId(4)), crate::LocalFailure::Database),
    ];
    for (refusal, category) in internal {
        let expected = refusal.to_string();
        let error = SyncError::from(refusal);
        let SyncError::Failed { failure, source } = &error else { panic!("{error:?}") };
        assert_eq!(*failure, category);
        assert_eq!(source.downcast_ref::<DbError>().unwrap().to_string(), expected);
    }
    // Conditions an app can see stay database failures.
    for condition in [DbError::ClaimPending, DbError::ClaimNumberExhausted] {
        assert!(matches!(SyncError::from(condition), SyncError::Database(_)));
    }
}

#[test]
fn local_file_and_lock_failures_are_disk_failures_with_their_cause() {
    let store = coven_storage::test_utils::STORE_ID;
    let missing = std::path::PathBuf::from("snapshot.tmp");
    let file = || coven_foundation::files::FileError::Io {
        operation: "append",
        path: missing.clone(),
        source: Box::new(std::io::Error::from(std::io::ErrorKind::StorageFull)),
    };
    let failures = [
        (SyncError::from(file()), crate::LocalFailure::DiskFull),
        (
            SyncError::from(coven_foundation::files::ObservationError::Missing(missing.clone())),
            crate::LocalFailure::DiskOther,
        ),
        (
            SyncError::from(coven_foundation::files::StoreLockError::AlreadyOpen(store)),
            crate::LocalFailure::DiskOther,
        ),
        (
            SyncError::disk(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
            crate::LocalFailure::DiskOther,
        ),
    ];
    for (error, kind) in &failures {
        let SyncError::Failed { failure, source } = error else { panic!("{error:?}") };
        assert_eq!(failure, kind, "{error:?}");
        assert!(
            source.is::<coven_foundation::files::FileError>()
                || source.is::<coven_foundation::files::ObservationError>()
                || source.is::<coven_foundation::files::StoreLockError>()
                || source.is::<std::io::Error>(),
            "{source:?}"
        );
    }
}

/// A full disk, a refused file and anything else are three failures a person
/// answers differently, so the operating system's error decides which one a
/// disk failure is, wherever it sits in the chain of causes.
#[test]
fn a_full_disk_is_told_from_a_refused_file_and_from_any_other_disk_failure() {
    use std::io::ErrorKind as K;
    let file = |kind: K| coven_foundation::files::FileError::Io {
        operation: "write",
        path: std::path::PathBuf::from("snapshot.tmp"),
        source: Box::new(std::io::Error::from(kind)),
    };
    for (kind, expected) in [
        (K::StorageFull, crate::LocalFailure::DiskFull),
        (K::QuotaExceeded, crate::LocalFailure::DiskFull),
        (K::PermissionDenied, crate::LocalFailure::DiskDenied),
        (K::ReadOnlyFilesystem, crate::LocalFailure::DiskDenied),
        (K::NotFound, crate::LocalFailure::DiskOther),
    ] {
        assert_eq!(crate::LocalFailure::of_disk(&file(kind)), expected, "{kind:?}");
    }
    // The cause sits under a failed cleanup of the temporary file.
    let cleaned = coven_foundation::files::FileError::Cleanup {
        operation: Box::new(file(K::PermissionDenied)),
        cleanup: Box::new(std::io::Error::from(K::StorageFull)),
    };
    assert_eq!(crate::LocalFailure::of_disk(&cleaned), crate::LocalFailure::DiskDenied);
}

#[test]
fn one_failure_shared_by_several_receivers_keeps_one_cause_and_its_category() {
    let (failure, source) = SyncError::Database(DbError::DamagedDatabase).into_shared();
    assert_eq!(failure, crate::LocalFailure::Database);
    assert!(matches!(
        source.downcast_ref::<SyncError>(),
        Some(SyncError::Database(DbError::DamagedDatabase))
    ));
    // Sharing an already shared failure keeps the same cause, unwrapped.
    let (again, shared) = SyncError::Failed { failure, source: source.clone() }.into_shared();
    assert_eq!(again, failure);
    assert!(Arc::ptr_eq(&source, &shared));
    // A chained failure is categorized by the operation that failed first.
    let chained = SyncError::Cleanup {
        operation: Box::new(SyncError::SecureStorage(KeyError::StoreClosed)),
        cleanup: Box::new(SyncError::Database(DbError::StoreClosed)),
    };
    assert_eq!(chained.local_failure(), crate::LocalFailure::KeyCustody);
    assert_eq!(
        SyncError::OperationData(serde_json::from_str::<u8>("x").unwrap_err()).local_failure(),
        crate::LocalFailure::Operation
    );
}

#[test]
fn removal_and_required_updates_stop_the_pass_as_themselves() {
    let update = crate::RequiredUpdate::AppSchema { version: 3 };
    assert!(matches!(SyncFailure::from(SyncError::Removed), SyncFailure::Removed));
    assert!(matches!(
        SyncFailure::from(SyncError::UpdateRequired(update)),
        SyncFailure::UpdateRequired(found) if found == update
    ));
}

#[test]
fn stored_damage_an_invariant_and_a_remote_refusal_stay_distinct() {
    let path = coven_storage::ObjectPath::slot(
        coven_foundation::id_source::StoreId(uuid::Uuid::from_u128(1)),
        std::num::NonZeroU64::MIN,
    );
    // Decoding stored facts failed: the database is damaged.
    assert!(matches!(
        SyncError::from(DbError::DamagedDatabase),
        SyncError::Database(DbError::DamagedDatabase)
    ));
    // Coven's own rule broke: a bug keeping the rule, never damage.
    let SyncError::Failed { failure, source } =
        SyncError::from(DbError::Invariant { what: "a completed upload is queued" })
    else {
        panic!("an invariant is a local failure")
    };
    assert_eq!(failure, crate::LocalFailure::Bug);
    assert!(matches!(
        source.downcast_ref::<DbError>(),
        Some(DbError::Invariant { what: "a completed upload is queued" })
    ));
    // A received object the database refuses is that object's refusal.
    let SyncError::Damaged(object) = crate::write_object::database_failure(
        &path,
        DbError::WriteFormat(coven_format::Error::Truncated),
    ) else {
        panic!("a refused remote object")
    };
    assert_eq!(object.failure, crate::Refusal::Parse { cause: None });
}

/// A failing operation's status names its record and the error behind it,
/// not a debug dump.
#[test]
fn a_failing_operation_names_its_reason_and_cause() {
    let mut record = crate::PendingRecord::new(
        crate::PendingSubject::Operation {
            id: crate::OperationId(4),
            kind: crate::OperationKind::RetainFiles,
        },
        crate::PendingReason::Failed(crate::LocalFailure::DiskOther),
    );
    let failure = SyncFailure::Operation(Box::new(record.clone()));
    assert_eq!(
        failure.to_string(),
        "coven's own work failed its last attempt: \
         Operation { id: OperationId(4), kind: RetainFiles }: Failed(DiskOther)"
    );
    record.cause = Some("disk full".into());
    let failure = SyncFailure::Operation(Box::new(record));
    assert!(failure.to_string().ends_with(": Failed(DiskOther): disk full"), "{failure}");
}

/// Storage's and custody's bugs are sync's own bug, not a storage or
/// custody failure.
#[test]
fn storage_and_custody_bugs_are_syncs_bug() {
    use coven_foundation::task::Bug;
    let bug = Bug::new("read the object", "it panicked");
    let storage = SyncError::from(StorageError::Bug(bug.clone()));
    assert!(matches!(storage, SyncError::Bug(found) if found == bug));
    let custody = SyncError::from(KeyError::Bug(bug.clone()));
    assert!(matches!(custody, SyncError::Bug(found) if found == bug));
}

/// An earlier bug that an error repeats, a worker's or a failed handle's,
/// carries none for the caller to report: the sink, or the call it
/// happened in, heard it already. A failed handle stays a database
/// failure, as its other failures do (api.md E15).
#[test]
fn a_repeated_bug_carries_none_to_report() {
    use coven_foundation::task::{Bug, CarriesBug};
    let bug = Bug::new("the sync loop", "it panicked");
    let worker = SyncError::WorkerFailed(bug.clone());
    assert_eq!(worker.bug(), None);
    assert_eq!(worker.local_failure(), crate::LocalFailure::Bug);
    let handle = SyncError::from(DbError::StoreFailed(bug.clone()));
    assert!(
        matches!(&handle, SyncError::Database(DbError::StoreFailed(found)) if *found == bug),
        "{handle:?}"
    );
    assert_eq!(handle.bug(), None);
    assert_eq!(SyncError::from(DbError::Bug(bug.clone())).bug(), Some(&bug));
}

/// A sync step's write keeps the step's own error when its rollback or the
/// removal of its released bytes fails too: each later failure follows it,
/// and a database failure stays one database failure (§4, Appendix C.3).
#[test]
fn a_failed_writes_cleanup_follows_the_steps_own_error() {
    use coven_database::WriteFailure;
    let rolled = SyncError::Removed.with_rollback(coven_database::SqliteError::InvalidQuery);
    let SyncError::Cleanup { operation, cleanup } = rolled else {
        panic!("a rollback follows the step's error: {rolled:?}")
    };
    assert!(matches!(*operation, SyncError::Removed));
    assert!(matches!(*cleanup, SyncError::Database(_)), "{cleanup:?}");
    let cleaned = SyncError::with_cleanup(
        SyncError::Removed,
        vec![DbError::StoreClosed, DbError::RollbackFailed],
    );
    let SyncError::Cleanup { operation, cleanup } = cleaned else {
        panic!("each removal failure follows the step's error: {cleaned:?}")
    };
    assert!(matches!(*cleanup, SyncError::Database(DbError::RollbackFailed)), "{cleanup:?}");
    assert!(
        matches!(*operation, SyncError::Cleanup { ref operation, ref cleanup }
            if matches!(**operation, SyncError::Removed)
                && matches!(**cleanup, SyncError::Database(DbError::StoreClosed))),
        "{operation:?}"
    );
    let database = SyncError::with_cleanup(
        SyncError::Database(DbError::StoreClosed),
        vec![DbError::RollbackFailed],
    );
    assert!(matches!(database, SyncError::Database(DbError::FileCleanup { .. })), "{database:?}");
}

/// Write evidence a snapshot carries that doesn't authenticate, or names an
/// author who couldn't write it, is invalid evidence, which is refused (§9.6):
/// the snapshot is damaged, never a failure of the local database, whether
/// the loader reports it bare or names the snapshot whose body it read.
#[test]
fn a_snapshots_invalid_write_evidence_refuses_the_snapshot() {
    use coven_database::SnapshotError;
    let snapshot = coven_format::admin::SnapshotId {
        audience: coven_merge::Audience::Store,
        device: DeviceId(1),
        number: 1,
    };
    let path = ObjectPath::snapshot(
        coven_storage::test_utils::STORE_ID,
        coven_merge::Audience::Store,
        DeviceId(1),
        1.try_into().unwrap(),
    );
    let evidence = || {
        [
            SnapshotError::Unauthentic(coven_crypto::CryptoError::Authentication),
            SnapshotError::Unauthentic(coven_crypto::CryptoError::Signature),
            SnapshotError::Unauthorized,
        ]
    };
    let named = evidence().map(|error| {
        DbError::Snapshot(SnapshotError::Body {
            snapshot: snapshot.clone(),
            error: Box::new(error.into()),
            staged: None,
        })
    });
    for error in evidence().map(DbError::Snapshot).into_iter().chain(named) {
        let failed = crate::write_object::database_failure(&path, error);
        let SyncError::Damaged(object) = failed else { panic!("{failed:?}") };
        assert_eq!(object.failure, crate::Refusal::InvalidWrite { cause: None });
        assert_eq!(object.path, path.as_str());
        assert!(object.failure.source().unwrap().is::<DbError>());
    }
}
