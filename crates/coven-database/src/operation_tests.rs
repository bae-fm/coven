/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_operations(id,kind,last_step,data,started_by)
           SELECT i,'kind_' || (i%32),1,x'00','app' FROM fixture;
         INSERT INTO _coven_app_operations SELECT i,'Kind' || (i%32),CASE WHEN i%2=0 THEN x'00' END FROM fixture;
         INSERT INTO _coven_held_keys SELECT CAST(printf('%032d',i) AS BLOB),zeroblob(32) FROM fixture;
         INSERT INTO _coven_write_pause VALUES(1,1);",
    )
    .unwrap();
}

/// The app's SQL can't add to the operation journal.
#[tokio::test]
async fn app_statements_cannot_write_the_journal() {
    crate::authorization::tests::refused_as_internal(
        "INSERT INTO _coven_operations(kind,last_step,data,started_by) VALUES ('x',0,x'','x')",
    )
    .await;
}
