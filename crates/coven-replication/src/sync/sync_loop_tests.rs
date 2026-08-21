use super::*;

fn success() -> SyncLoopSuccess {
    SyncLoopSuccess {
        last_sync_time: "2026-07-14T00:00:00Z".to_string(),
        device_count: 1,
        device_activity: Vec::new(),
        data_changed: false,
        row_changes: None,
        alerts: crate::sync::SyncLoopAlerts {
            rotation_pending: None,
            held_positions: Vec::new(),
            local_blob_cleanup_pending: false,
        },
    }
}

#[test]
fn storage_configuration_failure_is_terminal() {
    let status = storage_check_failure_status(std::sync::Arc::new(
        coven_protocol::objects::StorageError::Configuration("missing bucket".to_string()),
    ));

    assert!(matches!(status, SyncLoopStatus::Failed { .. }));
}

fn database() -> coven_database::StoreDatabase {
    let database = coven_database::Database::open(
        std::path::Path::new(":memory:"),
        Vec::new(),
        chrono::Duration::days(30),
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "status-test".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &[],
    )
    .expect("open status test database");
    coven_database::StoreDatabase::new(&database)
}

#[tokio::test]
async fn successful_cycle_projects_durable_blocked_state() {
    let database = database();
    let write_id = coven_protocol::write::WriteId::from_generated("blocked-write".to_string());
    database
        .insert_write_status_for_test(
            write_id.clone(),
            coven_protocol::write::WriteStatus::Blocked(
                coven_protocol::write::WriteBlock::MissingBlob {
                    namespace: "audio".to_string(),
                    id: "missing".to_string(),
                },
            ),
        )
        .await
        .expect("insert durable write status");
    let writes = database
        .pending_writes()
        .await
        .expect("load blocked writes");
    let blocked = current_success_status(writes, success());
    assert!(matches!(
        blocked,
        SyncLoopStatus::Blocked { writes, .. }
            if writes.len() == 1 && writes[0].write_id == write_id
    ));
    database
        .delete_write_for_test(write_id.clone())
        .await
        .expect("remove blocked projection fixture");
    assert!(
        database
            .store_write_payload_claims_for_test(&write_id)
            .await
            .expect("read deleted write payload claims")
            .is_empty(),
        "deleting the fixture must release its payload claim"
    );

    assert!(matches!(
        current_success_status(
            database
                .pending_writes()
                .await
                .expect("load synchronized writes"),
            success(),
        ),
        SyncLoopStatus::Synchronized(_)
    ));
}
