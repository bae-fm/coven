use super::*;

async fn assert_original_connection_uploads(f: &Fixture) {
    assert!(f.connector.clients.lock().unwrap()[0].upgrade().is_some());
    let provider = f.connector.providers.lock().unwrap()[0].clone();
    let key = provider.s3_credentials().await.unwrap();
    assert_eq!(key.access_key_id, "member-key");
    f.advance(Duration::from_secs(1));
    f.handle
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('retained','still connected')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    status(&f.handle, |state| {
        matches!(state, SyncStatus::Synced { finished_at } if *finished_at >= f.clock.now())
    })
    .await;
    assert!(f.handle.test_queued_writes().await.unwrap().is_empty());
}

#[tokio::test]
async fn failed_key_forgetting_keeps_the_running_connection() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.app.fail_next_keychain_operation();
    assert!(f.handle.forget_store_keys().await.is_err());
    assert_eq!(
        f.handle.store_key_state().unwrap(),
        StoreKeyState::Available
    );
    assert_original_connection_uploads(&f).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn key_forgetting_performs_no_fallible_custody_reads_after_removal() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.app.fail_keychain_operations_after(&[1]);
    f.handle.forget_store_keys().await.unwrap();
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    assert!(matches!(
        &*f.handle.subscribe_sync_status().borrow(),
        SyncStatus::Stopped
    ));
    f.app.fail_keychain_operations_after(&[]);
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn failed_setup_claim_keeps_the_running_connection() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    let before = f.handle.restore_code().await.unwrap();
    f.storage
        .set_faults(Faults {
            read_failures: [(
                ObjectPath::floor(f.directory.id()),
                StorageFailure::PermissionDenied,
            )]
            .into(),
            ..Faults::none()
        })
        .await;
    assert!(f.setup().await.is_err());
    f.storage.set_faults(Faults::none()).await;
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    assert_original_connection_uploads(&f).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn failed_setup_custody_commit_keeps_the_running_connection() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    let before = f.handle.restore_code().await.unwrap();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_read(ObjectPath::floor(f.directory.id()), entered, resumed)
        .await;
    let setup = f.setup();
    tokio::pin!(setup);
    tokio::select! {
        result = &mut setup => panic!("setup did not reach its claim: {result:?}"),
        _ = entering => {}
    }
    // Unlock persists its held ring; the following credential read fails.
    f.app.fail_keychain_operations_after(&[1]);
    resume.send(()).unwrap();
    assert!(matches!(
        setup.await,
        Err(StorageSetupError::SecureStorage(_))
    ));
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    assert_original_connection_uploads(&f).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn setup_key_failure_restores_the_separate_synced_values() {
    use coven_crypto::custody::{Keychain, StoreKeychain};
    let f = Fixture::new().await;
    f.handle.close().await.unwrap();
    let keychain = Keychain::in_memory("setup-rollback").unwrap();
    let scoped = StoreKeychain::new(keychain.clone(), f.directory.id());
    scoped
        .set_device_id(f.directory.settings().unwrap().device_id)
        .unwrap();
    let old_code = SecretBytes::new(b"previous synced code".to_vec());
    let old_key = SecretBytes::new(b"previous synced S3 key".to_vec());
    scoped.set_synced_restore_code(&old_code).unwrap();
    scoped.set_synced_s3_credentials(&old_key).unwrap();
    let handle = builder(
        &f.app,
        StoreLayout::new(f._root.path().into()),
        f.clock.clone(),
        f.storage.clone(),
    )
    .with_keychain(keychain)
    .identity_custody(IdentityCustody::Custom(Arc::new(
        coven_crypto::custody::KeyringCustody::new(Arc::new(
            f.app.store_keychain(f.directory.id()),
        )),
    )))
    .key_custody(KeyCustody::Custom(Arc::new(RefusePersist {
        keys: std::sync::Mutex::new(None),
        refuse: AtomicBool::new(true),
    })))
    .open(f.directory.id())
    .await
    .unwrap();
    assert!(matches!(
        handle
            .setup_s3_storage(
                f.storage.config(),
                "Device",
                "replacement".into(),
                SecretText::new("secret".into())
            )
            .await,
        Err(StorageSetupError::SecureStorage(
            KeyError::PassphraseAuthentication
        ))
    ));
    assert_eq!(
        scoped.synced_restore_code().unwrap().unwrap().as_bytes(),
        old_code.as_bytes()
    );
    assert_eq!(
        scoped.synced_s3_credentials().unwrap().unwrap().as_bytes(),
        old_key.as_bytes()
    );
    assert!(scoped.storage_credentials().unwrap().is_none());
    assert!(StorageSettings::new(f.directory.clone())
        .read()
        .unwrap()
        .is_none());
    handle.close().await.unwrap();
}

async fn assert_connected_key(f: &Fixture, id: &str, secret: &str) {
    let provider = f
        .connector
        .providers
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    let key = provider.s3_credentials().await.unwrap();
    assert_eq!(key.access_key_id, id);
    assert_eq!(key.secret_access_key.as_str(), secret);
}

#[tokio::test]
async fn connected_and_disconnected_updates_install_the_supplied_key_without_changing_the_code() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    let before = f.handle.restore_code().await.unwrap();
    let member = coven_sync::read_restore_code(&before)
        .unwrap()
        .member_keys
        .to_secret_bytes();
    let settings = StorageSettings::new(f.directory.clone());
    let location = settings.read().unwrap();
    f.handle
        .update_credentials(
            "connected".into(),
            SecretText::new("connected-secret".into()),
        )
        .await
        .unwrap();
    assert_connected_key(&f, "connected", "connected-secret").await;
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    f.handle
        .replace_access_key(
            "published".into(),
            SecretText::new("published-secret".into()),
        )
        .await
        .unwrap();
    assert_connected_key(&f, "published", "published-secret").await;
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    f.handle.disconnect_storage().await.unwrap();
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    f.handle
        .update_credentials(
            "disconnected".into(),
            SecretText::new("disconnected-secret".into()),
        )
        .await
        .unwrap();
    f.handle.start_sync().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    assert_connected_key(&f, "disconnected", "disconnected-secret").await;
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    assert_eq!(settings.read().unwrap(), location);
    assert_eq!(
        coven_sync::read_restore_code(&before)
            .unwrap()
            .member_keys
            .to_secret_bytes()
            .as_bytes(),
        member.as_bytes()
    );
    f.handle.close().await.unwrap();
    let handle = builder(
        &f.app,
        StoreLayout::new(f._root.path().into()),
        f.clock.clone(),
        f.connector.clone(),
    )
    .open(f.directory.id())
    .await
    .unwrap();
    handle.start_sync().await.unwrap();
    status(&handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    assert_connected_key(&f, "disconnected", "disconnected-secret").await;
    assert_eq!(handle.restore_code().await.unwrap(), before);
    handle.close().await.unwrap();
}

#[tokio::test]
async fn dropping_a_replacement_waiter_does_not_interrupt_key_installation_or_publication() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.stop_sync();
    status(&f.handle, |s| matches!(s, SyncStatus::Stopped)).await;
    f.handle.unlock_store_key().await.unwrap();
    let before = f.handle.restore_code().await.unwrap();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_creation(ObjectPrefix::all(f.directory.id()), entered, resumed)
        .await;
    {
        let replacement = f
            .handle
            .replace_access_key("retained".into(), SecretText::new("retained-secret".into()));
        tokio::pin!(replacement);
        tokio::select! {
            result = &mut replacement => panic!("publication was not held: {result:?}"),
            _ = entering => {}
        }
    }
    assert_connected_key(&f, "retained", "retained-secret").await;
    resume.send(()).unwrap();
    f.handle.close().await.unwrap();
    let handle = builder(
        &f.app,
        StoreLayout::new(f._root.path().into()),
        f.clock.clone(),
        f.connector.clone(),
    )
    .open(f.directory.id())
    .await
    .unwrap();
    handle.start_sync().await.unwrap();
    status(&handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    assert_connected_key(&f, "retained", "retained-secret").await;
    assert_eq!(handle.restore_code().await.unwrap(), before);
    handle.close().await.unwrap();
}

#[tokio::test]
async fn custody_failure_rolls_back_credentials_before_claiming_the_creation() {
    let f = Fixture::new().await;
    f.handle.close().await.unwrap();
    let handle = builder(
        &f.app,
        StoreLayout::new(f._root.path().into()),
        f.clock.clone(),
        f.storage.clone(),
    )
    .key_custody(KeyCustody::Custom(Arc::new(RefusePersist {
        keys: std::sync::Mutex::new(None),
        refuse: AtomicBool::new(true),
    })))
    .open(f.directory.id())
    .await
    .unwrap();
    let setup = |key: &str| {
        handle.setup_s3_storage(
            f.storage.config(),
            "Test device",
            key.into(),
            SecretText::new("secret".into()),
        )
    };
    assert!(setup("original").await.is_err());
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Locked);
    assert!(handle.restore_code().await.is_err());
    assert!(matches!(
        &*handle.subscribe_sync_status().borrow(),
        SyncStatus::Disconnected
    ));
    let first = ObjectPath::slot(f.directory.id(), 1.try_into().unwrap());
    assert!(f.storage.status(&first).await.unwrap().is_none());
    // Nothing was claimed, so another key sets the store up.
    setup("changed").await.unwrap();
    assert!(f.storage.status(&first).await.unwrap().is_some());
    let me = handle
        .members()
        .await
        .unwrap()
        .into_iter()
        .find(|member| member.is_self)
        .unwrap();
    assert_eq!(me.devices.len(), 1);
    handle.close().await.unwrap();
}
