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
            assert!(matches!(
                error,
                SyncError::Format(coven_format::Error::UnsupportedVersion(0))
            ));
        } else {
            assert!(matches!(error, SyncError::UpdateRequired(_)));
        }
    }
}

#[test]
fn merge_causality_refusal_preserves_its_native_cause() {
    let write = WriteId {
        device: DeviceId(1),
        number: 2,
    };
    let path = ObjectPath::slot(
        coven_storage::test_utils::STORE_ID,
        write.number.try_into().unwrap(),
    );
    let error = crate::write_object::database_failure(
        &path,
        DbError::InvalidWrite {
            write,
            error: MergeError::CausalTimestamp(write),
        },
    );
    let SyncError::Damaged(object) = error else {
        panic!("{error:?}")
    };
    assert_eq!(
        object.failure,
        crate::Refusal::InvalidCausality { cause: None }
    );
    assert_eq!(object.path, path.as_str());
    assert_eq!(
        object
            .failure
            .source()
            .unwrap()
            .downcast_ref::<MergeError>(),
        Some(&MergeError::CausalTimestamp(write))
    );
}

#[test]
fn object_validation_keeps_causes_and_local_failures_stay_local() {
    let write = WriteId {
        device: DeviceId(1),
        number: 1,
    };
    let path = ObjectPath::slot(
        coven_storage::test_utils::STORE_ID,
        write.number.try_into().unwrap(),
    );
    for error in [
        DbError::InvalidWrite {
            write,
            error: MergeError::GenerationParity(1),
        },
        DbError::Snapshot(coven_database::SnapshotError::Inconsistent(
            "row outside snapshot",
        )),
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
        object
            .failure
            .source()
            .unwrap()
            .downcast_ref::<coven_format::Error>(),
        Some(&coven_format::Error::Truncated)
    );
    assert!(matches!(
        crate::write_object::database_failure(&path, DbError::DamagedDatabase),
        SyncError::Database(DbError::DamagedDatabase)
    ));
}

#[test]
fn refusals_of_syncs_own_requests_fail_internally_and_keep_their_native_cause() {
    let internal = [
        DbError::Invariant {
            what: "a broken rule of coven's own code",
        },
        DbError::OperationChanged(coven_database::OperationId(4)),
    ];
    for refusal in internal {
        let expected = refusal.to_string();
        let error = SyncError::from(refusal);
        let SyncError::Failed {
            failure: crate::LocalFailure::Database,
            source,
        } = &error
        else {
            panic!("{error:?}")
        };
        assert_eq!(
            source.downcast_ref::<DbError>().unwrap().to_string(),
            expected
        );
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
        SyncError::from(file()),
        SyncError::from(coven_foundation::files::ObservationError::Missing(
            missing.clone(),
        )),
        SyncError::from(coven_foundation::files::StoreLockError::AlreadyOpen(store)),
        SyncError::disk(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
    ];
    for error in &failures {
        let SyncError::Failed {
            failure: crate::LocalFailure::Disk,
            source,
        } = error
        else {
            panic!("{error:?}")
        };
        assert!(
            source.is::<coven_foundation::files::FileError>()
                || source.is::<coven_foundation::files::ObservationError>()
                || source.is::<coven_foundation::files::StoreLockError>()
                || source.is::<std::io::Error>(),
            "{source:?}"
        );
    }
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
    let (again, shared) = SyncError::Failed {
        failure,
        source: source.clone(),
    }
    .into_shared();
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
    assert!(matches!(
        SyncFailure::from(SyncError::Removed),
        SyncFailure::Removed
    ));
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
    // Coven's own rule broke: a local failure keeping the rule, never damage.
    let SyncError::Failed { failure, source } = SyncError::from(DbError::Invariant {
        what: "a completed upload is queued",
    }) else {
        panic!("an invariant is a local failure")
    };
    assert_eq!(failure, crate::LocalFailure::Database);
    assert!(matches!(
        source.downcast_ref::<DbError>(),
        Some(DbError::Invariant {
            what: "a completed upload is queued"
        })
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
