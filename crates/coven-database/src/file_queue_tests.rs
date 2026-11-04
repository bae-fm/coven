/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_upload_pause VALUES(1);
         INSERT INTO _coven_unmetered_only VALUES(1);",
    )
    .unwrap();
}

/// The statement pausing uploads, for a test of what a transaction records.
pub(crate) const PAUSE: &str = "INSERT INTO _coven_upload_pause VALUES(1)";
