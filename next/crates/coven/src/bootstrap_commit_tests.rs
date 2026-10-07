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

#[tokio::test]
async fn custody_failure_restores_the_prior_keys_without_publishing() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let id = StoreId(UuidIds.new_id());
    let keychain = Keychain::in_memory("commit-test").unwrap();
    let scoped = StoreKeychain::new(keychain.clone(), id);
    let pending = layout.begin_bootstrap(id, "Household", &UuidIds).unwrap();
    let old = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
    let custody = Arc::new(InMemoryCustody::new(old.clone()));
    let next = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
    let error = publish_bootstrap(
        pending,
        code(id),
        next,
        KeyCustody::Custom(custody.clone()),
        IdentityCustody::Custom(Arc::new(RefusedIdentity)),
        keychain,
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
    assert!(layout.stores().await.unwrap().is_empty());
}

#[tokio::test]
async fn publication_collision_rolls_back_keys_credentials_and_synced_code() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let id = StoreId(UuidIds.new_id());
    let keychain = Keychain::in_memory("commit-test").unwrap();
    let scoped = Arc::new(StoreKeychain::new(keychain.clone(), id));
    let pending = layout.begin_bootstrap(id, "Household", &UuidIds).unwrap();
    let existing = layout.create_store_dir(id, "Existing", &UuidIds).unwrap();
    let next = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
    let error = publish_bootstrap(
        pending,
        code(id),
        next,
        KeyCustody::Keyring,
        IdentityCustody::Keyring,
        keychain,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        BootstrapError::Directory(BootstrapDirectoryError::Create(
            coven_foundation::files::StoreCreationError::AlreadyExists(_)
        ))
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
    assert_eq!(existing.settings().unwrap().name, "Existing");
}
