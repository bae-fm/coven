/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &sqlive::rusqlite::Connection) {
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
    crate::app_policy::tests::refused_as_internal(
        "INSERT INTO _coven_operations(kind,last_step,data,started_by) VALUES ('x',0,x'','x')",
    )
    .await;
}

/// The follow of one kind's operations lists them oldest first, runs again
/// for a row of that kind, and leaves a row of another kind alone.
#[tokio::test]
async fn the_follow_of_one_kind_ignores_the_journal_rows_of_other_kinds() {
    use crate::tests::{next, TestStore};
    use crate::{DbError, NewOperation};
    let operation = |kind: &str| NewOperation {
        kind: kind.into(),
        data: b"opaque sync data".to_vec(),
        started_by: "coven".into(),
        pauses_writes: false,
    };
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let mut pairing = db.subscribe_operations_of_kind::<DbError>("pairing");
    assert!(next(&mut pairing).await.is_empty());
    db.start_operation(operation("invite")).await.unwrap();
    assert!(!pairing.is_marked_for_rerun());
    let first = db.start_operation(operation("pairing")).await.unwrap();
    assert!(pairing.is_marked_for_rerun());
    let rows = next(&mut pairing).await;
    assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), [first]);
    assert_eq!(rows, db.operations_of_kind("pairing").await.unwrap());
    let second = db.start_operation(operation("pairing")).await.unwrap();
    let rows = next(&mut pairing).await;
    assert_eq!(rows.iter().map(|row| row.id).collect::<Vec<_>>(), [first, second]);
    db.close().await.unwrap();
}
