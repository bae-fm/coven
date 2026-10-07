use super::*;
use coven_crypto::custody::{InMemoryCustody, KeyringCustody};
use coven_storage::S3Credentials;

fn code(store: StoreId) -> RestoreCode {
    RestoreCode {
        store,
        name: "Household".into(),
        member_keys: MemberKeys::generate().unwrap(),
        storage: RestoreStorage {
            location: StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "us-east-1".into(),
                endpoint: None,
                prefix: "store".into(),
            },
            credentials: StorageCredentials::S3(S3Credentials {
                access_key_id: "access".into(),
                secret_access_key: SecretText::new("secret".into()),
            }),
        }
        .encode()
        .unwrap(),
    }
}

struct RefusedIdentity;
impl MemberKeyCustody for RefusedIdentity {
    fn unlock(&self) -> Result<Option<MemberKeys>, KeyError> {
        Ok(None)
    }
    fn persist(&self, _: &MemberKeys) -> Result<(), KeyError> {
        Err(KeyError::ServiceNotRegistered)
    }
    fn forget(&self) -> Result<(), KeyError> {
        Ok(())
    }
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
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
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
    let scoped = StoreKeychain::new(keychain.clone(), id);
    let old = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
    let custody = Arc::new(InMemoryCustody::new(old.clone()));
    let next = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
    let (pending, owners, database) = prepared(
        &layout,
        id,
        keychain,
        KeyCustody::Custom(custody.clone()),
        IdentityCustody::Custom(Arc::new(RefusedIdentity)),
    )
    .await;
    let error = publish_bootstrap(&pending, code(id), next, owners, database.clone()).unwrap_err();
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
    assert!(layout.stores().await.unwrap().is_empty());
    database.close().await.unwrap();
}

#[tokio::test]
async fn publication_failure_rolls_back_keys_credentials_and_synced_code() {
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
    let marker = root
        .path()
        .join("stores")
        .join(id.to_string())
        .join(".coven-bootstrap");
    std::fs::remove_file(&marker).unwrap();
    std::fs::create_dir(&marker).unwrap();
    let next = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
    let error = publish_bootstrap(&pending, code(id), next, owners, database.clone()).unwrap_err();
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
    let sealed = ring.seal_app_data(key, b"retained", b"test").unwrap();
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
    let error = publish_bootstrap(&pending, code, ring, owners, database).unwrap_err();
    let BootstrapError::Published { handle, .. } = error else {
        panic!("expected published handle")
    };
    assert_eq!(layout.stores().await.unwrap().len(), 1);
    assert_eq!(handle.open_app_data(&sealed, b"test").unwrap(), b"retained");
    handle.read(|_| Ok(())).await.unwrap();
    handle.close().await.unwrap();
}
