/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &sqlive::rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_device_reset VALUES(1,zeroblob(8),counter(1));
         INSERT INTO _coven_device_id_in_use VALUES(1,x'0000000000000001');",
    )
    .unwrap();
}

/// The follow of the device-reset notice starts with none and delivers the
/// notice once a reset records it.
#[tokio::test]
async fn the_device_reset_follow_delivers_the_recorded_notice() {
    use crate::tests::{next, TestStore};
    use crate::DbError;
    use coven_foundation::files::DeviceReset;
    use coven_foundation::id_source::DeviceId;
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let mut notice = db.subscribe_device_reset::<DbError>();
    assert_eq!(next(&mut notice).await, None);
    let reset = DeviceReset { old_device: DeviceId(1), new_device: DeviceId(2) };
    db.record_device_reset::<crate::pending::tests::Record>(reset, Vec::new(), Vec::new())
        .await
        .unwrap();
    assert_eq!(next(&mut notice).await, Some(reset));
    assert_eq!(db.device_reset().await.unwrap(), Some(reset));
    db.close().await.unwrap();
}
