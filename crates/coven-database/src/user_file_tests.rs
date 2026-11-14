use coven_foundation::files::FileLease;
/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_user_files SELECT 'file_' || i,CAST('path_' || i AS BLOB),i,zeroblob(13),zeroblob(32) FROM fixture;
         INSERT INTO _coven_user_chunks SELECT 'file_' || i,0,zeroblob(32) FROM fixture;",
    )
    .unwrap();
}

/// A file removed from its path is missing, and preparing it reports the path.
#[tokio::test]
async fn preparing_a_file_no_longer_at_its_path_fails_as_missing() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("original.flac");
    std::fs::write(&path, b"original").unwrap();
    super::prepare_user_file(&path, &FileLease::for_test().await, |_| {}).await.unwrap();
    std::fs::remove_file(&path).unwrap();
    let error =
        super::prepare_user_file(&path, &FileLease::for_test().await, |_| {}).await.unwrap_err();
    assert!(
        matches!(&error, crate::DbError::UserFileMissing { path: missing } if *missing == path)
    );
}

/// Reading an original holds one place of the app's budget while it reads,
/// and gives it back when the read ends, however it ends.
#[tokio::test]
async fn preparing_an_original_holds_one_place_while_it_reads() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("original.flac");
    std::fs::write(&path, vec![7u8; 200_000]).unwrap();
    let budget = coven_foundation::files::FileBudget::new(std::num::NonZeroUsize::MIN);
    let watch = budget.watch();
    let lease = budget.lease(1).await.unwrap();
    let seen = std::sync::Mutex::new(Vec::new());
    super::prepare_user_file(&path, &lease, |_| seen.lock().unwrap().push(watch.holding()))
        .await
        .unwrap();
    assert!(!seen.lock().unwrap().is_empty());
    assert!(seen.lock().unwrap().iter().all(|holding| *holding == 1));
    assert_eq!((watch.holding(), watch.most_holding()), (0, 1));
    coven_foundation::files::assert_no_open_files_under(directory.path());

    std::fs::remove_file(&path).unwrap();
    super::prepare_user_file(&path, &lease, |_| {}).await.unwrap_err();
    assert_eq!((watch.holding(), watch.most_holding()), (0, 1));
}
