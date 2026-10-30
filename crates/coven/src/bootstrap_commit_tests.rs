use super::*;
use coven_crypto::custody::{InMemoryCustody, KeyringCustody};
use coven_storage::S3Credentials;

fn code(store: StoreId) -> RestoreCode {
    RestoreCode {
        store,
        member: MemberId(uuid::Uuid::from_u128(0x4d)),
        member_keys: MemberKeys::generate().unwrap(),
        storage: coven_storage::RestoreStorage::location_only(StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: None,
            profile: None,
            prefix: crate::S3Prefix::parse("store").unwrap(),
        })
        .encode()
        .unwrap(),
    }
}

fn credentials() -> StorageCredentials {
    StorageCredentials::S3(s3_key())
}

fn s3_key() -> S3Credentials {
    S3Credentials {
        access_key_id: "access".into(),
        secret_access_key: SecretText::new("secret".into()),
    }
}

struct RefusedIdentity(Option<std::path::PathBuf>);
impl MemberKeyCustody for RefusedIdentity {
    fn unlock(&self) -> Result<Option<MemberIdentity>, KeyError> {
        Ok(None)
    }
    fn persist(&self, _: &MemberIdentity) -> Result<(), KeyError> {
        if let Some(path) = &self.0 {
            std::fs::remove_file(path).unwrap();
            std::fs::create_dir(path).unwrap();
        }
        Err(KeyError::ServiceNotRegistered)
    }
    fn forget(&self) -> Result<(), KeyError> {
        Ok(())
    }
    fn close(&self) {}
}

/// The bootstrap database's retry waits, as `prepare_and_load` opens them.
async fn waits(database: &Database) -> Arc<coven_sync::RetryWaits> {
    Arc::new(
        coven_sync::RetryWaits::open(
            database.clone(),
            Arc::new(coven_foundation::clock::SystemClock),
        )
        .await
        .unwrap(),
    )
}

async fn prepared(
    layout: &StoreLayout,
    id: StoreId,
    keychain: Arc<Keychain>,
    keys: KeyCustody,
    identity: IdentityCustody,
) -> (BootstrapStore, OpeningOwners, Database) {
    let pending = layout.begin_bootstrap(id, &UuidIds).await.unwrap();
    let builder = Coven::builder(layout.clone())
        .with_keychain(keychain.clone())
        .synced_tables(Vec::new())
        .migrations(Vec::new())
        .key_custody(keys)
        .identity_custody(identity);
    let database = builder
        .database(pending.directory())
        .unwrap()
        .open()
        .await
        .unwrap();
    database
        .initialize_store_name("Household".into())
        .await
        .unwrap();
    let owners = builder
        .owners(
            pending.directory(),
            Arc::new(StoreKeychain::new(keychain, id)),
        )
        .unwrap();
    (pending, owners, database)
}

#[tokio::test]
async fn custody_failure_restores_the_prior_keys_without_publishing() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let id = StoreId(UuidIds.new_id());
    let keychain = Keychain::in_memory("commit-test").unwrap();
    let scoped = Arc::new(StoreKeychain::new(keychain.clone(), id));
    let person = PersonEntries::new(keychain.clone(), id);
    let old = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
    let custody = Arc::new(KeyringCustody::<StoreKeyring>::new(scoped.clone()));
    custody.persist(&old).unwrap();
    let next = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
    let (pending, owners, database) = prepared(
        &layout,
        id,
        keychain,
        KeyCustody::Custom(custody.clone()),
        IdentityCustody::Custom(Arc::new(RefusedIdentity(None))),
    )
    .await;
    let error = publish_bootstrap(
        &pending,
        code(id),
        household(),
        credentials(),
        SignInCustody::None,
        next,
        owners,
        database.clone(),
        waits(&database).await,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        BootstrapError::SecureStorage(KeyError::ServiceNotRegistered)
    ));
    assert_eq!(
        custody
            .unlock()
            .unwrap()
            .unwrap()
            .to_secret_bytes()
            .as_bytes(),
        old.to_secret_bytes().as_bytes()
    );
    assert!(scoped.device_id().unwrap().is_none());
    assert!(scoped.storage_credentials().unwrap().is_none());
    assert!(person.restore_code().unwrap().is_none());
    assert!(person.s3_credentials().unwrap().is_none());
    assert!(StorageSettings::new(pending.directory())
        .read()
        .unwrap()
        .is_none());
    assert!(layout.stores().await.unwrap().is_empty());
    database.close().await.unwrap();
}

#[tokio::test]
async fn settings_rollback_failure_retains_both_causes_and_restores_other_custody() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let id = StoreId(UuidIds.new_id());
    let keychain = Keychain::in_memory("commit-test").unwrap();
    let custody = Arc::new(InMemoryCustody::<StoreKeyring>::empty());
    let path = root
        .path()
        .join("stores")
        .join(id.to_string())
        .join("storage.json");
    let (pending, owners, database) = prepared(
        &layout,
        id,
        keychain,
        KeyCustody::Custom(custody.clone()),
        IdentityCustody::Custom(Arc::new(RefusedIdentity(Some(path)))),
    )
    .await;
    let next = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
    let error = publish_bootstrap(
        &pending,
        code(id),
        household(),
        credentials(),
        SignInCustody::None,
        next,
        owners,
        database.clone(),
        waits(&database).await,
    )
    .await
    .unwrap_err();
    let BootstrapError::Cleanup { operation, cleanup } = error else {
        panic!("rollback failure was discarded: {error}")
    };
    assert!(matches!(
        *operation,
        BootstrapError::SecureStorage(KeyError::ServiceNotRegistered)
    ));
    assert!(matches!(
        *cleanup,
        BootstrapError::Sync(SyncError::Storage(_))
    ));
    assert!(custody.unlock().unwrap().is_none());
    assert!(layout.stores().await.unwrap().is_empty());
    database.close().await.unwrap();
}

#[tokio::test]
async fn publication_failure_rolls_back_settings_and_custody() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let id = StoreId(UuidIds.new_id());
    let keychain = Keychain::in_memory("commit-test").unwrap();
    let scoped = Arc::new(StoreKeychain::new(keychain.clone(), id));
    let person = PersonEntries::new(keychain.clone(), id);
    let (pending, owners, database) = prepared(
        &layout,
        id,
        keychain,
        KeyCustody::Keyring,
        IdentityCustody::Keyring,
    )
    .await;
    let settings = StorageSettings::new(pending.directory());
    let old_credentials = SecretBytes::new(b"device credentials before restore".to_vec());
    let old_s3_key = SecretBytes::new(b"saved S3 key before restore".to_vec());
    let old_code = SecretBytes::new(b"code before restore".to_vec());
    scoped.set_storage_credentials(&old_credentials).unwrap();
    person.set_s3_credentials(&old_s3_key).unwrap();
    person.set_restore_code(&old_code).unwrap();
    let old_location = StorageConfig::Dropbox {
        account_label: None,
        location: coven_storage::DropboxLocation::SharedFolder {
            namespace_id: "previous-location".into(),
        },
    };
    settings.commit(&old_location).unwrap();
    let marker = root
        .path()
        .join("stores")
        .join(id.to_string())
        .join(".coven-bootstrap");
    std::fs::remove_file(&marker).unwrap();
    std::fs::create_dir(&marker).unwrap();
    let next = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
    let error = publish_bootstrap(
        &pending,
        code(id),
        household(),
        credentials(),
        SignInCustody::None,
        next,
        owners,
        database.clone(),
        waits(&database).await,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        BootstrapError::Directory(BootstrapDirectoryError::File(_))
    ));
    assert!(
        StoreKeyCustody::unlock(&KeyringCustody::new(scoped.clone()))
            .unwrap()
            .is_none()
    );
    assert!(
        MemberKeyCustody::unlock(&KeyringCustody::new(scoped.clone()))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        scoped.storage_credentials().unwrap().unwrap().as_bytes(),
        old_credentials.as_bytes()
    );
    assert_eq!(
        person.restore_code().unwrap().unwrap().as_bytes(),
        old_code.as_bytes()
    );
    assert_eq!(
        person.s3_credentials().unwrap().unwrap().as_bytes(),
        old_s3_key.as_bytes()
    );
    assert!(scoped.device_id().unwrap().is_none());
    assert_eq!(settings.read().unwrap(), Some(old_location));
    assert!(layout.stores().await.unwrap().is_empty());
    database.close().await.unwrap();
}

#[tokio::test]
async fn credential_publication_failures_restore_every_prior_custody_value() {
    for after in 0..8 {
        let root = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(root.path().into());
        let id = StoreId(UuidIds.new_id());
        let keychain = Keychain::in_memory("commit-test").unwrap();
        let scoped = StoreKeychain::new(keychain.clone(), id);
        let person_entries = PersonEntries::new(keychain.clone(), id);
        let old_device = SecretBytes::new(b"old device credentials".to_vec());
        let old_key = SecretBytes::new(b"old saved S3 key".to_vec());
        let old_code = SecretBytes::new(b"old saved code".to_vec());
        scoped.set_storage_credentials(&old_device).unwrap();
        person_entries.set_s3_credentials(&old_key).unwrap();
        person_entries.set_restore_code(&old_code).unwrap();
        let (pending, owners, database) = prepared(
            &layout,
            id,
            keychain.clone(),
            KeyCustody::InMemory,
            IdentityCustody::InMemory,
        )
        .await;
        // Device id read/write, then all three credential reads and writes.
        keychain.fail_operations_after(&[after]);
        let ring = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
        let error = publish_bootstrap(
            &pending,
            code(id),
            household(),
            credentials(),
            SignInCustody::None,
            ring,
            owners,
            database.clone(),
            waits(&database).await,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                error,
                BootstrapError::SecureStorage(_)
                    | BootstrapError::Sync(SyncError::SecureStorage(_))
            ),
            "operation {after}: {error}"
        );
        assert_eq!(
            scoped.storage_credentials().unwrap().unwrap().as_bytes(),
            old_device.as_bytes()
        );
        assert_eq!(
            person_entries.s3_credentials().unwrap().unwrap().as_bytes(),
            old_key.as_bytes()
        );
        assert_eq!(
            person_entries.restore_code().unwrap().unwrap().as_bytes(),
            old_code.as_bytes()
        );
        assert!(scoped.device_id().unwrap().is_none());
        assert!(StorageSettings::new(pending.directory())
            .read()
            .unwrap()
            .is_none());
        assert!(layout.stores().await.unwrap().is_empty());
        database.close().await.unwrap();
    }
}

#[tokio::test]
async fn bootstrap_reports_saved_key_rollback_failure_with_the_publication_failure() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let id = StoreId(UuidIds.new_id());
    let keychain = Keychain::in_memory("commit-test").unwrap();
    let scoped = StoreKeychain::new(keychain.clone(), id);
    let person = PersonEntries::new(keychain.clone(), id);
    let (pending, owners, database) = prepared(
        &layout,
        id,
        keychain.clone(),
        KeyCustody::InMemory,
        IdentityCustody::InMemory,
    )
    .await;
    // Refuse the code write, then refuse rolling back the separate S3 entry.
    keychain.fail_operations_after(&[7, 1]);
    let ring = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
    let error = publish_bootstrap(
        &pending,
        code(id),
        household(),
        credentials(),
        SignInCustody::None,
        ring,
        owners,
        database.clone(),
        waits(&database).await,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, BootstrapError::Sync(SyncError::Cleanup { operation, cleanup })
        if matches!(*operation, SyncError::SecureStorage(_)) && matches!(*cleanup, SyncError::SecureStorage(_)))
    );
    assert!(person.s3_credentials().unwrap().is_some());
    assert!(scoped.storage_credentials().unwrap().is_none());
    assert!(person.restore_code().unwrap().is_none());
    assert!(scoped.device_id().unwrap().is_none());
    assert!(StorageSettings::new(pending.directory())
        .read()
        .unwrap()
        .is_none());
    assert!(layout.stores().await.unwrap().is_empty());
    database.close().await.unwrap();
}

#[tokio::test]
async fn cleanup_failure_returns_the_open_handle_with_its_session_keys() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let id = StoreId(UuidIds.new_id());
    let keychain = Keychain::in_memory("commit-test").unwrap();
    let code = code(id);
    let key = KeyId(UuidIds.new_id());
    let ring = StoreKeyring::new(StoreKey::generate(key).unwrap());
    let (pending, owners, database) = prepared(
        &layout,
        id,
        keychain,
        KeyCustody::InMemory,
        IdentityCustody::InMemory,
    )
    .await;
    let leftover = root
        .path()
        .join("stores")
        .join(id.to_string())
        .join("bootstrap.sealed");
    std::fs::create_dir(leftover).unwrap();
    let location = coven_storage::RestoreStorage::decode(code.storage.as_bytes())
        .unwrap()
        .location;
    let error = publish_bootstrap(
        &pending,
        code,
        household(),
        credentials(),
        SignInCustody::None,
        ring,
        owners,
        database.clone(),
        waits(&database).await,
    )
    .await
    .unwrap_err();
    let BootstrapError::Published { handle, .. } = error else {
        panic!("expected published handle")
    };
    assert_eq!(layout.stores().await.unwrap().len(), 1);
    assert_eq!(
        StorageSettings::new(pending.directory()).read().unwrap(),
        Some(location)
    );
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
    handle.read(|_| Ok(())).await.unwrap();
    handle.close().await.unwrap();
}

#[tokio::test]
async fn publication_returns_the_handle_without_rereading_custody() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let id = StoreId(UuidIds.new_id());
    let keychain = Keychain::in_memory("commit-test").unwrap();
    let (pending, owners, database) = prepared(
        &layout,
        id,
        keychain.clone(),
        KeyCustody::InMemory,
        IdentityCustody::InMemory,
    )
    .await;
    let waits = waits(&database).await;
    // Publication needs nine keychain operations; the next read fails.
    keychain.fail_operations_after(&[9]);
    let result = publish_bootstrap(
        &pending,
        code(id),
        household(),
        credentials(),
        SignInCustody::None,
        StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap()),
        owners,
        database,
        waits,
    )
    .await;
    let handle = match result {
        Ok(handle) | Err(BootstrapError::Published { handle, .. }) => handle,
        Err(error) => panic!("publication lost its handle: {error}"),
    };
    assert_eq!(layout.stores().await.unwrap().len(), 1);
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
    keychain.fail_operations_after(&[]);
    handle.close().await.unwrap();
}

#[tokio::test]
async fn publication_with_a_loaded_provider_does_not_suspend() {
    use std::{future::Future, task::Poll};
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let id = StoreId(UuidIds.new_id());
    let (pending, mut owners, database) = prepared(
        &layout,
        id,
        Keychain::in_memory("commit-test").unwrap(),
        KeyCustody::InMemory,
        IdentityCustody::InMemory,
    )
    .await;
    let provider = coven_storage::test_utils::MemoryStorage::builder()
        .build()
        .unwrap();
    owners.storage = Some(provider.shared_connection());
    let waits = waits(&database).await;
    let mut publishing = Box::pin(publish_bootstrap(
        &pending,
        code(id),
        household(),
        credentials(),
        SignInCustody::None,
        StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap()),
        owners,
        database,
        waits,
    ));
    let first = std::future::poll_fn(|cx| Poll::Ready(publishing.as_mut().poll(cx))).await;
    let Poll::Ready(result) = first else {
        panic!("publication suspended before returning its handle");
    };
    let handle = result.unwrap();
    assert_eq!(layout.stores().await.unwrap().len(), 1);
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
    handle.close().await.unwrap();
}

/// The name the fixture store was created with, set by slot 1.
fn household() -> coven_sync::SavedName {
    coven_sync::SavedName {
        name: "Household".into(),
        named_at: coven_merge::Slot(1),
    }
}
