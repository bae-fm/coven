use super::*;
use crate::tests::HeardBugs;
use crate::TableRead;
use std::convert::Infallible;

/// A handle whose host fails with text, reporting to `bugs`.
fn text_handle(bugs: &Arc<HeardBugs>) -> Handle<String> {
    Handle::recording_all(Arc::clone(bugs) as BugSinkRef)
}

fn bug(message: &str) -> Bug {
    Bug::new("a test's work", message)
}

/// A bug that fails the handle inside one of its calls is that call's own,
/// to return; the sink hears nothing of it (api.md E15).
#[test]
fn a_bug_inside_a_call_is_the_calls_own() {
    let bugs = Arc::<HeardBugs>::default();
    let handle = text_handle(&bugs);
    let ((), own) = handle.calling(|| handle.fail_with_bug(bug("torn")));
    assert_eq!(own, Some(bug("torn")));
    assert!(bugs.take().is_empty());
    assert!(
        matches!(handle.error::<Infallible>(), Some(Error::HandleFailed(found)) if found == bug("torn"))
    );
}

/// A call that hits a second bug returns the first, and the sink hears the
/// second, so neither is lost. The handle keeps the first (api.md E15).
#[test]
fn a_calls_second_bug_goes_to_the_sink() {
    let bugs = Arc::<HeardBugs>::default();
    let handle = text_handle(&bugs);
    let ((), own) = handle.calling(|| {
        handle.fail_with_bug(bug("first"));
        handle.fail_with_bug(bug("second"));
    });
    assert_eq!(own, Some(bug("first")));
    assert_eq!(bugs.take(), [bug("second")]);
    assert!(
        matches!(handle.error::<Infallible>(), Some(Error::HandleFailed(found)) if found == bug("first"))
    );
}

/// A call made inside another on the same handle leaves its bug to the
/// outer call, the one the app hears from; a call on another handle claims
/// none of this handle's bugs, which go to the sink (api.md E15).
#[test]
fn only_the_outer_call_on_the_handle_claims_its_bug() {
    let bugs = Arc::<HeardBugs>::default();
    let handle = text_handle(&bugs);
    let other = text_handle(&bugs);
    let ((), outer) = handle.calling(|| {
        let ((), inner) = handle.calling(|| handle.fail_with_bug(bug("inner")));
        assert_eq!(inner, None);
    });
    assert_eq!(outer, Some(bug("inner")));
    let ((), own) = other.calling(|| handle.fail_with_bug(bug("elsewhere")));
    assert_eq!(own, None);
    assert_eq!(bugs.take(), [bug("elsewhere")]);
    assert!(other.error::<Infallible>().is_none());
}

/// When a live query's run reads a table listed in `UnrecordedTables`, the
/// observer panics as the run finishes. A run that reads only tables not on
/// the list finishes normally.
#[test]
fn a_run_that_reads_an_unrecorded_table_panics() {
    let observer: Handle<String> =
        Handle::new(Arc::<HeardBugs>::default(), vec!["Hidden".to_owned()]);
    let read = |table: &str| {
        vec![TableRead {
            table: table.into(),
            columns: BTreeSet::new(),
            scope: RowScope::All,
            inserted_only: false,
        }]
    };
    let subscription = observer.subscribe();
    subscription.begin();
    subscription.finish(read("shown"));
    subscription.begin();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        subscription.finish(read("HIDDEN"))
    }));
    assert!(panicked.is_err());
}
