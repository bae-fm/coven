/// Fill this domain's table from the plan audit's transaction
/// (`internal_schema_scan_reasons_tests.rs`): it holds at most one row.
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch("INSERT INTO _coven_joined_invitee VALUES(1,'Carol');").unwrap();
}
