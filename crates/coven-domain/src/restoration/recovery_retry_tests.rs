use super::*;

async fn restore_owner_recovery(
    fixture: &OwnerRecoveryRestoreFixture,
) -> Result<coven_foundation::config::Config, BootstrapError> {
    Box::pin(restore_from_code(
        &fixture.code,
        &fixture.tables,
        &fixture.migrations,
        coven_database::CovenMigrationPolicy::ApplyPending,
        coven_foundation::config::ExactUploadVerification::MetadataHash,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        coven_keys::custody::KeyCustody::Keyring,
        coven_keys::identity_custody::IdentityCustody::Keyring,
        coven_storage::oauth::OAuthClients::empty(),
        None,
        Some(fixture.cloudkit_ops.clone()),
        &StoreLayout::new(fixture.app.path()),
        Arc::new(SystemClock),
        Arc::new(SequentialIdProvider::new("recovery-retry-device")),
        |_status: &str| {},
        &tokio::sync::watch::channel(false).1,
    ))
    .await
}

async fn assert_restore_retry_after_atomic_create_failure(store_id: &str, failed_call: usize) {
    let fixture = Box::pin(prepare_owner_recovery_restore(store_id)).await;
    fixture
        .cloudkit_ops
        .fail_atomic_create_before_call(failed_call);

    restore_owner_recovery(&fixture)
        .await
        .expect_err("activation commit publication must be interrupted");
    assert!(
        !StoreLayout::new(fixture.app.path())
            .store_dir(&fixture.store_id)
            .exists(),
        "failed restore cleanup removes the staged local recovery state",
    );
    fixture
        .source_device
        .publish_fixture_position("history-after-published-recovery-node")
        .await;

    let config = restore_owner_recovery(&fixture)
        .await
        .expect("retry rebuilds the exact published recovery readiness");
    assert_eq!(config.store_id, fixture.store_id);
}

#[tokio::test]
async fn failed_restore_reuses_a_published_registration_after_history_advances() {
    assert_restore_retry_after_atomic_create_failure("published-recovery-registration-retry", 2)
        .await;
}

#[tokio::test]
async fn failed_restore_reuses_a_published_recovery_node_after_local_cleanup() {
    assert_restore_retry_after_atomic_create_failure("published-recovery-node-retry", 4).await;
}

#[tokio::test]
async fn failed_restore_reuses_a_published_initial_ack_after_history_advances() {
    assert_restore_retry_after_atomic_create_failure("published-recovery-ack-retry", 3).await;
}

#[tokio::test]
async fn failed_restore_reuses_a_published_activation_commit_after_history_advances() {
    assert_restore_retry_after_atomic_create_failure("published-recovery-commit-retry", 5).await;
}
