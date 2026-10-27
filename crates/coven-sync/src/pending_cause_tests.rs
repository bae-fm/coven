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
    let causes = HeldCauses::default();
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
    let causes = HeldCauses::default();
    let subject = PendingSubject::Connection;
    causes.failed(&subject, &DISK, Failure::Held(error("first")));
    causes.failed(&subject, &DISK, Failure::Held(error("latest")));
    let cause = |reason: PendingReason| causes.cause(&record(reason)).map(|c| c.to_string());
    assert_eq!(cause(DISK).as_deref(), Some("latest"));
    assert_eq!(cause(DATABASE), None);
    let returned = std::io::Error::other("returned");
    causes.failed(&subject, &DISK, Failure::Returned(&returned));
    assert_eq!(cause(DISK), None);
    // Equality is the record's identity, not its cause.
    causes.failed(&subject, &DISK, Failure::Held(error("held")));
    let attached = causes.subscribe().borrow().attach(vec![record(DISK)]);
    assert_eq!(attached, vec![record(DISK)]);
}

/// An operation's failure ends when it advances, or when it no longer exists.
#[test]
fn an_operations_failure_ends_when_it_advances_or_ends() {
    use coven_database::OperationId;
    let causes = HeldCauses::default();
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

/// The causes publish again only when the cause some record would show
/// appears, changes or ends: a failure kept with no cause, or one forgotten
/// that had none, leaves every record's cause as it was.
#[test]
fn the_causes_publish_when_a_shown_cause_changes() {
    let causes = HeldCauses::default();
    let mut published = causes.subscribe();
    let subject = PendingSubject::Connection;
    let changed = |published: &mut watch::Receiver<PendingCauses>| {
        let changed = published.has_changed().unwrap();
        published.borrow_and_update();
        changed
    };
    let returned = std::io::Error::other("returned");
    causes.failed(&subject, &DISK, Failure::Returned(&returned));
    assert!(!changed(&mut published), "no record shows a returned error");
    causes.failed(&subject, &DISK, Failure::Held(error("first")));
    assert!(changed(&mut published));
    // A retry meeting the same failure holds its new error.
    causes.failed(&subject, &DISK, Failure::Held(error("again")));
    assert!(changed(&mut published));
    assert_eq!(
        published.borrow().cause(&record(DISK)).unwrap().to_string(),
        "again"
    );
    // Stopping at the same failure without its error keeps the one held.
    causes.failed(&subject, &DISK, Failure::Repeated(&returned));
    assert!(!changed(&mut published));
    causes.cleared(&subject, |reason| *reason == DATABASE);
    assert!(
        !changed(&mut published),
        "the record still states its reason"
    );
    causes.cleared(&subject, |_| true);
    assert!(changed(&mut published));
    assert!(published.borrow().cause(&record(DISK)).is_none());
    causes.failed(&subject, &DISK, Failure::Returned(&returned));
    causes.cleared(&subject, |_| true);
    assert!(
        !changed(&mut published),
        "the forgotten failure showed no cause"
    );
}
