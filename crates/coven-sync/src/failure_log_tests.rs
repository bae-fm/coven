use super::*;
use crate::LocalFailure;

fn error(text: &str) -> std::io::Error {
    std::io::Error::other(text.to_owned())
}

const DISK: PendingReason = PendingReason::Failed(LocalFailure::Disk);
const DATABASE: PendingReason = PendingReason::Failed(LocalFailure::Database);

/// A failure logs when the subject's reason appears or changes; the same
/// reason met again on a retry logs nothing, and a cleared subject's next
/// failure is new.
#[test]
fn a_failure_logs_once_per_change_of_its_subjects_reason() {
    let log = FailureLog::new();
    let subject = PendingSubject::Connection;
    assert!(log.failed(&subject, &DISK, &error("first")));
    assert!(!log.failed(&subject, &DISK, &error("again")));
    assert!(log.failed(&subject, &DATABASE, &error("other")));
    log.cleared(&subject, |reason| *reason == DISK);
    assert!(!log.failed(&subject, &DATABASE, &error("kept")));
    log.cleared(&subject, |_| true);
    assert!(log.failed(&subject, &DATABASE, &error("new")));
}

/// An operation's failure ends when it advances, or when it no longer exists.
#[test]
fn an_operations_failure_ends_when_it_advances_or_ends() {
    use coven_database::OperationId;
    let log = FailureLog::new();
    let operation = |id| PendingSubject::Operation {
        id: OperationId(id),
        kind: crate::OperationKind::RetainFiles,
    };
    for id in [1, 2] {
        log.failed(&operation(id), &DISK, &error("held"));
    }
    log.cleared_operation(OperationId(1));
    assert!(log.failed(&operation(1), &DISK, &error("new")));
    log.retain_operations(&[OperationId(1)].into());
    assert!(!log.failed(&operation(1), &DISK, &error("again")));
    assert!(log.failed(&operation(2), &DISK, &error("new")));
}
