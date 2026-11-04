/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_table_origins SELECT i,tab,1,tab FROM fixture;
         INSERT INTO _coven_column_origins SELECT i,tab,'column_' || i,1,tab,'column_' || i FROM fixture;",
    )
    .unwrap();
}
