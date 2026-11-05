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
    super::prepare_user_file(&path, |_| {}).await.unwrap();
    std::fs::remove_file(&path).unwrap();
    let error = super::prepare_user_file(&path, |_| {}).await.unwrap_err();
    assert!(
        matches!(&error, crate::DbError::UserFileMissing { path: missing } if *missing == path)
    );
}
