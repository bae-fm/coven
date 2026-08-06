use super::*;

fn success() -> SyncLoopSuccess {
    SyncLoopSuccess {
        last_sync_time: "2026-07-14T00:00:00Z".to_string(),
        device_count: 1,
        device_activity: Vec::new(),
        data_changed: false,
        row_changes: None,
        alerts: crate::SyncLoopAlerts {
            rotation_pending: None,
            held_positions: Vec::new(),
            asset_downloads_failed: false,
            local_blob_cleanup_pending: false,
        },
    }
}

#[test]
fn storage_configuration_failure_is_terminal() {
    let status = storage_check_failure_status(
        &coven_protocol::objects::StorageError::Configuration("missing bucket".to_string()),
    );

    assert!(matches!(status, SyncLoopStatus::Failed { .. }));
}

fn database() -> crate::database::StoreDatabase {
    let database = crate::database::Database::open(
        std::path::Path::new(":memory:"),
        Vec::new(),
        chrono::Duration::days(30),
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "status-test".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        &[],
    )
    .expect("open status test database");
    crate::database::StoreDatabase::new(&database)
}

#[tokio::test]
async fn successful_cycle_projects_durable_blocked_state() {
    let database = database();
    let write_id = crate::WriteId::from_generated("blocked-write".to_string());
    database
        .insert_write_status_for_test(
            write_id.clone(),
            crate::WriteStatus::Blocked(crate::WriteBlock::MissingBlob {
                namespace: "audio".to_string(),
                id: "missing".to_string(),
            }),
        )
        .await
        .expect("insert durable write status");
    let writes = database
        .pending_writes()
        .await
        .expect("load blocked writes");
    let blocked = current_success_status(writes, success()).expect("project blocked state");
    assert!(matches!(
        blocked,
        SyncLoopStatus::Blocked { writes, .. }
            if writes.len() == 1 && writes[0].write_id == write_id
    ));
    database
        .delete_write_for_test(write_id)
        .await
        .expect("remove blocked projection fixture");

    assert!(matches!(
        current_success_status(
            database
                .pending_writes()
                .await
                .expect("load synchronized writes"),
            success(),
        )
        .expect("project synchronized state"),
        SyncLoopStatus::Synchronized(_)
    ));
}
