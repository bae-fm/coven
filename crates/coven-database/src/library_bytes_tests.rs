/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &sqlive::rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_capture_sizes SELECT 'snapshot-' || i || '.sealed',i FROM fixture;",
    )
    .unwrap();
}
