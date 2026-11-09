use super::*;

fn name(text: &str) -> OwnedFile {
    OwnedFile::Cached(coven_foundation::files::FileName::new(text).unwrap())
}

/// Releasing a name no holder reserves fails the store's handle with the
/// bug, and never panics: holders release as they drop. Released outside
/// any of the store's calls, the bug goes to the bug sink, and later calls
/// repeat it as `StoreFailed` (api.md E15).
#[test]
fn releasing_an_unreserved_name_fails_the_handle() {
    let bugs = coven_foundation::task::HeardBugs::shared();
    let handle = CommitObserver::new(bugs.clone());
    let reserved = ReservedNames::new(handle.clone());
    reserved.reserve([name("a")]);
    reserved.release([&name("a")]);
    assert!(handle.error().is_none(), "a reserved name releases cleanly");
    reserved.release([&name("a")]);
    let heard = bugs.take();
    assert!(
        matches!(handle.error(), Some(DbError::StoreFailed(bug)) if heard == [bug.clone()] && bug.operation == "release reserved file names"),
        "{:?}, heard {heard:?}",
        handle.error()
    );
    reserved.with_names(|names| assert!(names.is_empty()));
}
