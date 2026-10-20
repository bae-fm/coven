use super::*;

#[test]
fn cleanup_retains_both_causes_and_obeys_the_unfinished_causes_retry_gate() {
    assert_eq!(
        PendingReason::Rejected(crate::OperationRejection::NotAllowed).retry(),
        Retry::Never
    );
    let update = RequiredUpdate::AppSchema { version: 3 };
    for (cause, retry, before, after) in [
        (
            PendingReason::Storage {
                provider: CloudProvider::S3,
                failure: StorageFailure::Network,
                answered: false,
            },
            Retry::Automatic,
            true,
            true,
        ),
        (
            PendingReason::AdminAccessRequired,
            Retry::AppAction,
            false,
            false,
        ),
        (
            PendingReason::UpdateRequired(update),
            Retry::AfterUpdate,
            false,
            true,
        ),
        (PendingReason::Removed, Retry::Never, false, false),
    ] {
        let error = crate::SyncError::Cleanup {
            operation: Box::new(crate::SyncError::Rejected(
                crate::OperationRejection::NotAllowed,
            )),
            cleanup: Box::new(crate::SyncError::Pending(Box::new(PendingRecord {
                subject: PendingSubject::Connection,
                reason: cause.clone(),
            }))),
        };
        let record = PendingRecord {
            subject: PendingSubject::Operation {
                id: OperationId(1),
                kind: OperationKind::ReplaceAccessKey,
            },
            reason: error.pending_reason(CloudProvider::S3),
        };
        let encoded = serde_json::to_vec(&record).unwrap();
        let reopened: PendingRecord = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(reopened, record);
        assert_eq!(
            reopened.reason,
            PendingReason::Cleanup {
                operation: Box::new(PendingReason::Rejected(
                    crate::OperationRejection::NotAllowed
                )),
                cleanup: Box::new(cause),
            }
        );
        assert_eq!(reopened.retry(), retry);
        assert_eq!(reopened.can_retry(env!("CARGO_PKG_VERSION"), 2), before);
        assert_eq!(reopened.can_retry(env!("CARGO_PKG_VERSION"), 3), after);
    }
}

#[test]
fn conditional_conflicts_retry_without_becoming_throttling() {
    let record = PendingRecord {
        subject: PendingSubject::Connection,
        reason: PendingReason::Storage {
            provider: CloudProvider::S3,
            failure: StorageFailure::Conflict,
            answered: true,
        },
    };
    assert_eq!(record.retry(), Retry::Automatic);
}

/// Coven's own operation retries as its failure clears: by itself under
/// backoff, after a person acts, or never (E5).
#[test]
fn operation_failures_retry_as_they_clear_and_only_app_operations_wait_for_it() {
    let record = |subject, reason| PendingRecord { subject, reason };
    let operation = |kind| PendingSubject::Operation {
        id: OperationId(1),
        kind,
    };
    let storage = |failure| PendingReason::Storage {
        provider: CloudProvider::S3,
        failure,
        answered: true,
    };
    for (reason, coven) in [
        (PendingReason::Failed(LocalFailure::Disk), Retry::Automatic),
        (
            PendingReason::Failed(LocalFailure::Database),
            Retry::Automatic,
        ),
        (
            PendingReason::Failed(LocalFailure::KeyCustody),
            Retry::Automatic,
        ),
        (PendingReason::Failed(LocalFailure::Operation), Retry::Never),
        (storage(StorageFailure::Network), Retry::Automatic),
        (storage(StorageFailure::QuotaExceeded), Retry::Automatic),
        (storage(StorageFailure::Authentication), Retry::AppAction),
        (storage(StorageFailure::PermissionDenied), Retry::AppAction),
        (storage(StorageFailure::Refused), Retry::Never),
        (storage(StorageFailure::Protocol), Retry::Never),
    ] {
        for kind in [
            OperationKind::WriteSnapshot,
            OperationKind::RequiredReload,
            OperationKind::RotateKey,
            OperationKind::RevokeAccess,
        ] {
            assert_eq!(
                record(operation(kind), reason.clone()).retry(),
                coven,
                "{kind:?} {reason:?}"
            );
        }
        let app = match &reason {
            PendingReason::Storage {
                failure: StorageFailure::Network,
                ..
            } => Retry::Automatic,
            _ => Retry::AppAction,
        };
        for kind in [
            OperationKind::AddCircleMember,
            OperationKind::ReloadFromSnapshot,
            OperationKind::SchemaChange,
            OperationKind::Reset,
        ] {
            assert_eq!(
                record(operation(kind), reason.clone()).retry(),
                app,
                "{kind:?} {reason:?}"
            );
        }
    }
    assert_eq!(
        record(PendingSubject::Slot(Slot(1)), PendingReason::Removed).retry(),
        Retry::Never
    );
}

/// No app call asks again for a slot or a pass, so a local failure that
/// clears by itself, such as a locked keychain, retries them under the shared
/// backoff. A lasting one still waits for the app (E5).
#[test]
fn local_failures_retry_slots_and_passes_as_they_clear() {
    for subject in [PendingSubject::Slot(Slot(4)), PendingSubject::Connection] {
        for (failure, retry) in [
            (LocalFailure::Database, Retry::Automatic),
            (LocalFailure::Disk, Retry::Automatic),
            (LocalFailure::KeyCustody, Retry::Automatic),
            (LocalFailure::Crypto, Retry::AppAction),
            (LocalFailure::Operation, Retry::AppAction),
        ] {
            let record = PendingRecord {
                subject: subject.clone(),
                reason: PendingReason::Failed(failure),
            };
            assert_eq!(record.retry(), retry, "{record:?}");
            assert_eq!(
                record.can_retry(env!("CARGO_PKG_VERSION"), 1),
                retry == Retry::Automatic
            );
        }
    }
}

#[test]
fn pending_subjects_keep_their_variant_and_snapshot_order() {
    use coven_foundation::id_source::{CircleId, StoreId};
    use uuid::Uuid;

    let snapshot = |audience, device, number| {
        PendingSubject::Snapshot(SnapshotId {
            audience,
            device: DeviceId(device),
            number,
        })
    };
    let expected = vec![
        PendingSubject::Write(WriteId {
            device: DeviceId(9),
            number: 9,
        }),
        PendingSubject::Slot(Slot(1)),
        PendingSubject::KeyCopy {
            audience: Audience::Store,
            key: KeyId(Uuid::from_u128(1)),
            member: MemberId(Uuid::from_u128(1)),
            sealing: coven_crypto::SealingKey::from_bytes([1; 32]),
        },
        snapshot(Audience::Store, 1, 1),
        snapshot(Audience::Store, 1, 2),
        snapshot(Audience::Store, 2, 1),
        snapshot(Audience::Circle(CircleId(Uuid::from_u128(1))), 1, 9),
        snapshot(Audience::Circle(CircleId(Uuid::from_u128(2))), 1, 1),
        PendingSubject::File {
            device: DeviceId(1),
            file: FileId(Uuid::from_u128(1)),
        },
        PendingSubject::Operation {
            id: OperationId(1),
            kind: OperationKind::CreateStore,
        },
        PendingSubject::Retention {
            path: ObjectPath::floor(StoreId(Uuid::from_u128(1))),
        },
        PendingSubject::Audience(Audience::Store),
        PendingSubject::JoinRequest(InviteId(Uuid::from_u128(1))),
        PendingSubject::Connection,
    ];
    let mut subjects: Vec<_> = expected.iter().rev().cloned().collect();
    subjects.sort();
    assert_eq!(subjects, expected);
    let encoded = r#"{"Snapshot":["store",2,1]}"#;
    assert_eq!(serde_json::to_string(&subjects[5]).unwrap(), encoded);
    assert_eq!(
        serde_json::from_str::<PendingSubject>(encoded).unwrap(),
        subjects[5]
    );
}

/// Only a network failure no provider answered is offline (E5); its wait is
/// the one a network report ends (§6.7).
#[test]
fn only_an_unanswered_network_failure_is_offline() {
    let storage = |failure, answered| PendingReason::Storage {
        provider: CloudProvider::S3,
        failure,
        answered,
    };
    assert!(storage(StorageFailure::Network, false).offline());
    // A server's error response is an answer.
    assert!(!storage(StorageFailure::Network, true).offline());
    assert!(!storage(StorageFailure::RateLimited, true).offline());
    assert!(!storage(StorageFailure::File, false).offline());
    assert!(!PendingReason::Failed(LocalFailure::Disk).offline());
}
