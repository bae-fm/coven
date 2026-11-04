use super::*;

fn name(text: &str) -> FileName {
    FileName::new(text).unwrap()
}

/// Releasing a name no holder reserves fails the store's handle with the
/// bug, and never panics: holders release as they drop (api.md E15).
#[test]
fn releasing_an_unreserved_name_fails_the_handle() {
    let handle = CommitObserver::new();
    let reserved = ReservedNames::new(handle.clone());
    reserved.reserve([name("a")]);
    reserved.release([&name("a")]);
    assert!(handle.error().is_none(), "a reserved name releases cleanly");
    reserved.release([&name("a")]);
    assert!(
        matches!(handle.error(), Some(DbError::Bug(bug)) if bug.operation == "release reserved file names"),
        "{:?}",
        handle.error()
    );
    reserved.with_names(|names| assert!(names.is_empty()));
}
