use super::*;

#[tokio::test]
async fn setup_key_failure_restores_the_separate_synced_values() {
    use coven_crypto::custody::{Keychain, StoreKeychain};
    let f = Fixture::new().await;
    f.handle.close().await.unwrap();
    let keychain = Keychain::in_memory("setup-rollback").unwrap();
    let scoped = StoreKeychain::new(keychain.clone(), f.directory.id());
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
    .key_custody(KeyCustody::Custom(Arc::new(RefusePersist {
        keys: std::sync::Mutex::new(None),
        refuse: AtomicBool::new(true),
    })))
    .open(f.directory.id())
    .await
    .unwrap();
    assert!(handle
        .setup_s3_storage(
            f.storage.config(),
            "Device",
            "replacement".into(),
            SecretText::new("secret".into())
        )
        .await
        .is_err());
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
        .hold_next_listing(ObjectPrefix::store_logs(f.directory.id()), entered, resumed)
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
