/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_provider_access SELECT 'account_' || i,i%2,NULL,NULL,i,i%2 FROM fixture;",
    )
    .unwrap();
}

/// §19.3: a stored access record naming a target with no request is
/// damage, not a query error.
#[tokio::test]
async fn a_stored_access_record_with_a_target_but_no_request_is_damaged_database() {
    let store = crate::tests::TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let operation = || crate::NewOperation {
        kind: "provider-access".into(),
        data: b"account@example.com".to_vec(),
        started_by: "coven".into(),
        pauses_writes: false,
    };
    db.start_operation_with_access::<crate::pending::tests::Record>(
        operation(),
        vec![crate::ProviderAccessChange {
            account: "account@example.com".into(),
            intended: true,
            initially_granted: false,
            needs_invitation: false,
            operation: operation(),
        }],
    )
    .await
    .unwrap();
    db.inspect_writer(|sql| {
        sql.batch(
            "PRAGMA foreign_keys=OFF; PRAGMA ignore_check_constraints=ON;
             UPDATE _coven_provider_access SET operation=NULL,target=1;
             PRAGMA ignore_check_constraints=OFF; PRAGMA foreign_keys=ON;",
        )
        .unwrap()
    });
    let read = db.provider_access("account@example.com".into()).await;
    assert!(matches!(read, Err(crate::DbError::DamagedDatabase)), "{read:?}");
    db.close().await.unwrap();
}
