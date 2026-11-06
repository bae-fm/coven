use super::*;
use coven_foundation::task::HeardBugs;

fn bug(message: &str) -> Bug {
    Bug { operation: "a test's work", message: message.into() }
}

/// A bug that fails the handle inside one of its calls is that call's own,
/// to return; the sink hears nothing of it (api.md E15).
#[test]
fn a_bug_inside_a_call_is_the_calls_own() {
    let bugs = HeardBugs::shared();
    let handle = CommitObserver::new(bugs.clone());
    let ((), own) = handle.calling(|| handle.fail_with_bug(bug("torn")));
    assert_eq!(own, Some(bug("torn")));
    assert!(bugs.take().is_empty());
    assert!(matches!(handle.error(), Some(DbError::StoreFailed(found)) if found == bug("torn")));
}

/// A call that hits a second bug returns the first, and the sink hears the
/// second, so neither is lost. The handle keeps the first (api.md E15).
#[test]
fn a_calls_second_bug_goes_to_the_sink() {
    let bugs = HeardBugs::shared();
    let handle = CommitObserver::new(bugs.clone());
    let ((), own) = handle.calling(|| {
        handle.fail_with_bug(bug("first"));
        handle.fail_with_bug(bug("second"));
    });
    assert_eq!(own, Some(bug("first")));
    assert_eq!(bugs.take(), [bug("second")]);
    assert!(matches!(handle.error(), Some(DbError::StoreFailed(found)) if found == bug("first")));
}

/// A call made inside another on the same handle leaves its bug to the
/// outer call, the one the app hears from; a call on another handle claims
/// none of this handle's bugs, which go to the sink (api.md E15).
#[test]
fn only_the_outer_call_on_the_handle_claims_its_bug() {
    let bugs = HeardBugs::shared();
    let handle = CommitObserver::new(bugs.clone());
    let other = CommitObserver::new(bugs.clone());
    let ((), outer) = handle.calling(|| {
        let ((), inner) = handle.calling(|| handle.fail_with_bug(bug("inner")));
        assert_eq!(inner, None);
    });
    assert_eq!(outer, Some(bug("inner")));
    let ((), own) = other.calling(|| handle.fail_with_bug(bug("elsewhere")));
    assert_eq!(own, None);
    assert_eq!(bugs.take(), [bug("elsewhere")]);
    assert!(other.error().is_none());
}
