use super::*;
use coven_database::DropReason;

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

#[test]
fn a_dropped_operation_waits_for_the_app_but_a_dropped_entry_or_write_is_final() {
    let record = |subject| PendingRecord {
        subject,
        reason: PendingReason::Dropped(DropReason::NotAllowed),
        reported_by: DeviceId(1),
    };
    assert_eq!(
        record(PendingSubject::Operation {
            id: OperationId(1),
            kind: OperationKind::AddCircleMember,
        })
        .retry(),
        Retry::AppAction
    );
    for subject in [
        PendingSubject::Entry(EntryId {
            device: DeviceId(1),
            number: 1,
        }),
        PendingSubject::Write(coven_merge::WriteId {
            device: DeviceId(1),
            number: 1,
        }),
    ] {
        assert_eq!(record(subject).retry(), Retry::Never);
    }
}
