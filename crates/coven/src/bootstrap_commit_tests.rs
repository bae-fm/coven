use super::*;
use coven_crypto::custody::{InMemoryCustody, KeyringCustody};
use coven_storage::S3Credentials;

fn code(store: StoreId) -> RestoreCode {
    RestoreCode {
        store,
        name: "Household".into(),
        member_keys: MemberKeys::generate().unwrap(),
        storage: RestoreStorage::S3 {
            location: StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "us-east-1".into(),
                endpoint: None,
                prefix: "store".into(),
            },
            credentials: s3_key(),
        }
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
    fn unlock(&self) -> Result<Option<MemberKeys>, KeyError> {
        Ok(None)
    }
    fn persist(&self, _: &MemberKeys) -> Result<(), KeyError> {
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

async fn prepared(
    layout: &StoreLayout,
    id: StoreId,
    keychain: Arc<Keychain>,
    keys: KeyCustody,
    identity: IdentityCustody,
) -> (BootstrapStore, OpeningOwners, Database) {
    let pending = layout.begin_bootstrap(id, "Household", &UuidIds).unwrap();
    let builder = Coven::builder(layout.clone())
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
        credentials(),
        next,
        owners,
        database.clone(),
    )
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
    assert!(scoped.synced_restore_code().unwrap().is_none());
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
        credentials(),
        next,
        owners,
        database.clone(),
    )
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
    let (pending, owners, database) = prepared(
        &layout,
        id,
        keychain,
        KeyCustody::Keyring,
        IdentityCustody::Keyring,
    )
    .await;
    let settings = StorageSettings::new(pending.directory());
    let old_location = StorageConfig::Dropbox {
        namespace_id: "previous-location".into(),
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
        credentials(),
        next,
        owners,
        database.clone(),
    )
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
    assert!(scoped.storage_credentials().unwrap().is_none());
    assert!(scoped.synced_restore_code().unwrap().is_none());
    assert!(scoped.device_id().unwrap().is_none());
    assert_eq!(settings.read().unwrap(), Some(old_location));
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
    let location = RestoreStorage::decode(code.storage.as_bytes())
        .unwrap()
        .location()
        .clone();
    let error =
        publish_bootstrap(&pending, code, credentials(), ring, owners, database).unwrap_err();
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
