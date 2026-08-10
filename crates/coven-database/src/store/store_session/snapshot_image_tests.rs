use super::*;

#[test]
fn staged_database_cleanup_reports_a_target_that_remains() {
    let directory = tempfile::tempdir().expect("snapshot cleanup directory");
    let path = directory.path().join("snapshot.db");
    std::fs::create_dir(&path).expect("create unremovable snapshot target");

    let error = SnapshotDatabaseImage::prepare(path.clone())
        .expect_err("an unremovable staged database must fail");

    assert!(
        matches!(
            error,
            SnapshotImageError::Cleanup {
                path: ref failed_path,
                ..
            } if *failed_path == path
        ),
        "{error}"
    );
    std::fs::remove_dir(path).expect("remove cleanup obstruction");
}

#[test]
fn staged_database_cleanup_preserves_the_operation_failure() {
    let directory = tempfile::tempdir().expect("snapshot cleanup directory");
    let path = directory.path().join("snapshot.db");
    let staged =
        SnapshotDatabaseImage::prepare(path.clone()).expect("prepare staged database image");
    std::fs::create_dir(&path).expect("create cleanup obstruction");

    let error = staged
        .finish::<()>(Err(SnapshotImageError::Projection(
            "injected operation failure".to_string(),
        )))
        .expect_err("operation and cleanup failures must both surface");

    assert!(
        matches!(
            error,
            SnapshotImageError::CleanupAfterFailure {
                path: ref failed_path,
                ref cause,
                ..
            } if *failed_path == path
                && matches!(
                    cause.as_ref(),
                    SnapshotImageError::Projection(message)
                        if message == "injected operation failure"
                )
        ),
        "{error}"
    );
    std::fs::remove_dir(path).expect("remove cleanup obstruction");
}

#[test]
fn staged_database_creation_refuses_an_existing_target() {
    let directory = tempfile::tempdir().expect("snapshot creation directory");
    let path = directory.path().join("snapshot.db");
    std::fs::write(&path, b"existing database").expect("write existing database");

    let result = SnapshotDatabaseImage::create(path.clone(), b"replacement database");

    assert!(result.is_err(), "creation must refuse an existing database");
    assert_eq!(
        std::fs::read(path).expect("read preserved database"),
        b"existing database"
    );
}
