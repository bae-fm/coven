use super::*;

#[test]
fn conditional_conflicts_retry_without_becoming_throttling() {
    let record = PendingRecord {
        subject: PendingSubject::Connection,
        reason: PendingReason::Storage {
            provider: CloudProvider::S3,
            failure: StorageFailure::Conflict,
        },
        reported_by: DeviceId(1),
    };
    assert_eq!(record.retry(), Retry::Automatic);
}

/// Coven's own operation retries as its failure clears: by itself under
/// backoff, after a person acts, or never (E5).
#[test]
fn operation_failures_retry_as_they_clear_and_only_app_operations_wait_for_it() {
    let record = |subject, reason| PendingRecord {
        subject,
        reason,
        reported_by: DeviceId(1),
    };
    let operation = |kind| PendingSubject::Operation {
        id: OperationId(1),
        kind,
    };
    let storage = |failure| PendingReason::Storage {
        provider: CloudProvider::S3,
        failure,
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
