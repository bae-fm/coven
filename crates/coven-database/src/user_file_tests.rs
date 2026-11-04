/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_user_files SELECT 'file_' || i,CAST('path_' || i AS BLOB),i,zeroblob(13),zeroblob(32) FROM fixture;
         INSERT INTO _coven_user_chunks SELECT 'file_' || i,0,zeroblob(32) FROM fixture;",
    )
    .unwrap();
}
