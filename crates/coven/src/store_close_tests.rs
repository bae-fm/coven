use coven_database::DbError;
use coven_keys::keys::test_keyring;
use std::sync::Arc;

async fn connect(handle: &crate::CovenHandle) -> Result<(), crate::CloudHomeSetupError> {
    handle
        .connect_sync_with_test_home(
            Arc::new(crate::InMemoryCloudHome::new()),
            coven_storage::CloudCipher::Encrypted(crate::EncryptionService::from_key([7; 32])),
        )
        .await
}

fn is_store_closed(error: &crate::CovenError) -> bool {
    matches!(error, crate::CovenError::Database(error) if matches!(**error, DbError::StoreClosed))
}

/// Closing a store with its sync loop running leaves nothing of it open: no
/// connection, no lock, no file in its directory. A clone of the handle that
/// outlives the close can neither read nor reconnect sync, and the store
/// deletes while that clone is still alive.
#[tokio::test]
async fn a_closed_store_holds_no_file_and_deletes_under_a_live_clone() {
    test_keyring::install();
    let store_id = "closed-store";
    let directory = tempfile::tempdir().expect("app directory");
    let store_dir = crate::StoreDir::new_ephemeral(directory.path().join(store_id));
    let mut config = crate::Config::with_defaults(
        store_id.to_string(),
        "device-test".to_string(),
        "Closed Store".to_string(),
    );
    config.cloud_home.provider = Some(crate::CloudProvider::S3);
    config.cloud_home.storage = crate::HomeStorage::Opaque;
    let handle = crate::Coven::builder(store_dir.clone(), config)
        .synced_tables(coven_replication::sync::test_helpers::test_synced_tables())
        .coven_migration_policy(crate::CovenMigrationPolicy::ApplyPending)
        .migrations(coven_replication::sync::test_helpers::test_migrations())
        .open()
        .expect("open store");
    handle.initialize_identity().expect("establish identity");
    connect(&handle).await.expect("connect sync");
    assert!(handle.is_syncing());
    let count_notes = |sql: crate::SqlReadContext<'_>| {
        sql.query_row("SELECT count(*) FROM notes", [], |row| row.get::<_, i64>(0))
            .map_err(crate::CovenError::from)
    };
    assert_eq!(handle.read(count_notes).await.expect("read notes"), 0);
    let clone = handle.clone();

    handle.close().await;

    let refused = clone
        .read(count_notes)
        .await
        .expect_err("a closed store serves no read");
    assert!(is_store_closed(&refused), "{refused}");
    let reconnect = connect(&clone)
        .await
        .expect_err("a closed store starts no sync");
    assert!(
        matches!(
            &reconnect,
            crate::CloudHomeSetupError::Connection(error)
                if matches!(&**error, crate::SyncError::Database(error) if matches!(**error, DbError::StoreClosed))
        ),
        "{reconnect}"
    );
    assert!(!clone.is_syncing());
    crate::assert_no_open_files_under(&store_dir);
    crate::Coven::delete_store(&store_dir, store_id, &[]).expect("delete the closed store");
    assert!(!store_dir.exists());
}

/// Dropping the last handle inside a runtime, without closing it, leaves the
/// connection threads closing their files after the drop returns. The store
/// lock stays held until they have: once it can be taken, the lock file is the
/// only file of the store still open.
#[tokio::test]
async fn a_dropped_store_frees_its_lock_only_after_its_files_close() {
    test_keyring::install();
    let directory = tempfile::tempdir().expect("app directory");
    let store_dir = crate::StoreDir::new_ephemeral(directory.path().join("dropped-store"));
    let handle = crate::Coven::builder(
        store_dir.clone(),
        crate::Config::with_defaults(
            "dropped-store".to_string(),
            "device-test".to_string(),
            "Dropped Store".to_string(),
        ),
    )
    .synced_tables(coven_replication::sync::test_helpers::test_synced_tables())
    .coven_migration_policy(crate::CovenMigrationPolicy::ApplyPending)
    .migrations(coven_replication::sync::test_helpers::test_migrations())
    .open()
    .expect("open store");
    handle
        .read(|sql| {
            sql.query_row("SELECT count(*) FROM notes", [], |row| row.get::<_, i64>(0))
                .map_err(crate::CovenError::from)
        })
        .await
        .expect("read notes");

    drop(handle);

    // The threads end on their own schedule, so the only thing to wait for is
    // the lock; what matters is what is open at the moment it frees.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let lock = loop {
        match coven_foundation::store_dir::StoreOpenGuard::acquire(&store_dir) {
            Ok(lock) => break lock,
            Err(coven_foundation::store_dir::StoreOpenGuardError::AlreadyOpen { .. }) => {
                assert!(std::time::Instant::now() < deadline, "the lock never freed");
                tokio::task::yield_now().await;
            }
            Err(error) => panic!("take the store lock: {error}"),
        }
    };
    let lock_file = store_dir
        .join(".coven-lock")
        .canonicalize()
        .expect("canonicalize lock file");
    assert_eq!(
        coven_foundation::open_files::open_files_under(&store_dir),
        vec![lock_file]
    );
    drop(lock);
}
