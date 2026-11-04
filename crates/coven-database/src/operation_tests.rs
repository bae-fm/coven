/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_held_keys SELECT CAST(printf('%032d',i) AS BLOB),zeroblob(32) FROM fixture;
         INSERT INTO _coven_write_pause VALUES(1,1);",
    )
    .unwrap();
}
