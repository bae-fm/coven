use super::*;
use crate::{LocalFailure, PendingReason, PendingRecord, PendingSubject};

fn record(reason: PendingReason) -> PendingRecord {
    PendingRecord::new(PendingSubject::Connection, reason)
}

fn error(text: &str) -> Cause {
    Arc::new(std::io::Error::other(text.to_owned()))
}

const DISK: PendingReason = PendingReason::Failed(LocalFailure::Disk);
const DATABASE: PendingReason = PendingReason::Failed(LocalFailure::Database);

/// A failure logs when the subject's reason appears or changes; the same
/// reason met again on a retry only replaces the held error, and a cleared
/// subject's next failure is new.
#[test]
fn a_failure_logs_once_per_change_of_its_subjects_reason() {
    let causes = PendingCauses::default();
    let subject = PendingSubject::Connection;
    assert!(causes.failed(&subject, &DISK, Failure::Held(error("first"))));
    assert!(!causes.failed(&subject, &DISK, Failure::Held(error("again"))));
    assert!(causes.failed(&subject, &DATABASE, Failure::Held(error("other"))));
    causes.cleared(&subject, |reason| *reason == DISK);
    assert!(!causes.failed(&subject, &DATABASE, Failure::Held(error("kept"))));
    causes.cleared(&subject, |_| true);
    assert!(causes.failed(&subject, &DATABASE, Failure::Held(error("new"))));
}

/// A record carries the latest held error only while it states that
/// error's reason. An error returned to its caller is not held, and ends
/// the earlier one's.
#[test]
fn a_record_carries_the_held_error_for_its_own_reason() {
    let causes = PendingCauses::default();
    let subject = PendingSubject::Connection;
    causes.failed(&subject, &DISK, Failure::Held(error("first")));
    causes.failed(&subject, &DISK, Failure::Held(error("latest")));
    let cause = |reason: PendingReason| {
        causes.attach(vec![record(reason)])[0]
            .cause
            .as_ref()
            .map(ToString::to_string)
    };
    assert_eq!(cause(DISK).as_deref(), Some("latest"));
    assert_eq!(cause(DATABASE), None);
    let returned = std::io::Error::other("returned");
    causes.failed(&subject, &DISK, Failure::Returned(&returned));
    assert_eq!(cause(DISK), None);
    // Equality is the record's identity, not its cause.
    causes.failed(&subject, &DISK, Failure::Held(error("held")));
    assert_eq!(causes.attach(vec![record(DISK)]), vec![record(DISK)]);
}

/// An error returned for the last failure, then dropped by its receiver,
/// becomes that record's cause without logging the failure again.
#[test]
fn a_returned_error_its_receiver_drops_is_kept() {
    let causes = PendingCauses::default();
    let subject = PendingSubject::Connection;
    let returned = std::io::Error::other("returned");
    assert!(causes.failed(&subject, &DISK, Failure::Returned(&returned)));
    causes.kept(&subject, error("kept"));
    let records = causes.attach(vec![record(DISK)]);
    assert_eq!(
        records[0]
            .cause
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("kept")
    );
    // Nothing is recorded for a subject without a failure.
    let other = PendingSubject::Operation {
        id: coven_database::OperationId(9),
        kind: crate::OperationKind::RetainFiles,
    };
    causes.kept(&other, error("stray"));
    assert!(causes.attach(vec![PendingRecord::new(other, DISK)])[0]
        .cause
        .is_none());
}

/// An operation's failure ends when it advances, or when it no longer exists.
#[test]
fn an_operations_failure_ends_when_it_advances_or_ends() {
    use coven_database::OperationId;
    let causes = PendingCauses::default();
    let operation = |id| PendingSubject::Operation {
        id: OperationId(id),
        kind: crate::OperationKind::RetainFiles,
    };
    for id in [1, 2] {
        causes.failed(&operation(id), &DISK, Failure::Held(error("held")));
    }
    causes.cleared_operation(OperationId(1));
    assert!(causes.failed(&operation(1), &DISK, Failure::Held(error("new"))));
    causes.retain_operations(&[OperationId(1)].into());
    assert!(!causes.failed(&operation(1), &DISK, Failure::Held(error("again"))));
    assert!(causes.failed(&operation(2), &DISK, Failure::Held(error("new"))));
}
