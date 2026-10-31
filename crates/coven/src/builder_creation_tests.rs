use crate::*;
use coven_crypto::custody::Keychain;
use coven_foundation::files::StoreDirAccess;
use coven_storage::{test_utils::MemoryStorage, Storage};
use std::sync::{Arc, Mutex};

fn fail_publication_sync(
    pending: &coven_foundation::files::BootstrapStore,
) -> Result<(), coven_foundation::files::BootstrapDirectoryError> {
    pending.publish()?;
    Err(coven_foundation::files::DirectoryCreationError::Published {
        id: pending.directory().id(),
        source: std::io::Error::other("injected publication directory sync failure"),
    }
    .into())
}

#[tokio::test]
async fn publication_sync_failure_releases_the_store_before_returning() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let builder = || {
        app.builder(layout.clone()).synced_tables(vec![]).migrations(vec![Migration::addition(
            1,
            "initial content",
            "CREATE TABLE initial_content (value TEXT NOT NULL); \
                 INSERT INTO initial_content VALUES ('Household');",
        )])
    };
    let expected = StoreId(SequentialIds::new().new_id());
    let error = super::create(
        builder().id_source(Arc::new(SequentialIds::new())),
        "Household".into(),
        fail_publication_sync,
    )
    .await
    .unwrap_err();
    let StoreCreationError::PublishedButNotSynced { store, source } = &error else {
        panic!("{error:?}");
    };
    assert_eq!(*store, expected);
    assert!(matches!(source.as_ref(), StoreCreationError::Directory(
        coven_foundation::files::BootstrapDirectoryError::Create(
            coven_foundation::files::DirectoryCreationError::Published { id, source }
        )
    ) if *id == expected && source.to_string() == "injected publication directory sync failure"));
    let identity =
        coven_crypto::custody::KeyringCustody::new(Arc::new(app.store_keychain(expected)));
    let member = MemberKeyCustody::unlock(&identity).unwrap().unwrap();
    assert_eq!(
        layout.stores().await.unwrap(),
        [StoreInfo { id: expected, name: "Household".into(), unfinished_replacement: None }]
    );
    let reopened = builder().open(expected, |_| ()).await.unwrap();
    assert_eq!(reopened.store_id(), expected);
    let value: String = reopened
        .read(|context| {
            Ok(context.query_row("SELECT value FROM initial_content", [], |row| row.get(0))?)
        })
        .await
        .unwrap();
    assert_eq!(value, "Household");
    assert_eq!(
        MemberKeyCustody::unlock(&identity).unwrap().unwrap().keys().public_keys(),
        member.keys().public_keys()
    );
    reopened.close().await.unwrap();
    app.discard_store(&layout.store_dir(&expected)).await.unwrap();
    assert!(layout.stores().await.unwrap().is_empty());
    drop(error);
}

struct ClosingIdentity {
    keys: Mutex<Option<MemberIdentity>>,
    closing: tokio::sync::Notify,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl MemberKeyCustody for ClosingIdentity {
    fn unlock(&self) -> Result<Option<MemberIdentity>, KeyError> {
        Ok(self.keys.lock().unwrap().clone())
    }

    fn persist(&self, identity: &MemberIdentity) -> Result<(), KeyError> {
        *self.keys.lock().unwrap() = Some(identity.clone());
        Ok(())
    }

    fn forget(&self) -> Result<(), KeyError> {
        panic!("publication failure must not roll back identity custody")
    }

    fn close(&self) {
        self.closing.notify_one();
        self.release.lock().unwrap().recv_timeout(std::time::Duration::from_secs(10)).unwrap();
        self.keys.lock().unwrap().take();
    }
}

#[tokio::test]
async fn publication_sync_failure_waits_for_close_and_releases_session_custody() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let expected = StoreId(SequentialIds::new().new_id());
    let (release, wait) = std::sync::mpsc::channel();
    let identity = Arc::new(ClosingIdentity {
        keys: Mutex::new(None),
        closing: tokio::sync::Notify::new(),
        release: Mutex::new(wait),
    });
    let creation = tokio::spawn(super::create(
        app.builder(layout.clone())
            .id_source(Arc::new(SequentialIds::new()))
            .synced_tables(vec![])
            .migrations(vec![])
            .identity_custody(IdentityCustody::Custom(identity.clone())),
        "Household".into(),
        fail_publication_sync,
    ));
    tokio::time::timeout(std::time::Duration::from_secs(5), identity.closing.notified())
        .await
        .unwrap();
    assert!(!creation.is_finished(), "creation returned before custody closed");
    assert!(identity.keys.lock().unwrap().is_some());
    release.send(()).unwrap();
    let error = creation.await.unwrap().unwrap_err();
    assert!(matches!(
        &error,
        StoreCreationError::PublishedButNotSynced { store, .. } if *store == expected
    ));
    assert!(identity.keys.lock().unwrap().is_none());
    assert_eq!(Arc::strong_count(&identity), 1, "creation retained custody");
    app.discard_store(&layout.store_dir(&expected)).await.unwrap();
    assert!(layout.stores().await.unwrap().is_empty());
    drop(error);
}

#[tokio::test]
async fn creation_initializes_every_builtin_identity_custody_before_setup() {
    for custody in [
        IdentityCustody::Keyring,
        IdentityCustody::InMemory,
        IdentityCustody::Passphrase(Passphrase::new("member password".into())),
    ] {
        let persisted = !matches!(custody, IdentityCustody::InMemory);
        let passphrase = matches!(custody, IdentityCustody::Passphrase(_));
        let root = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(root.path().into());
        let app = TestCoven::new();
        let storage = Arc::new(MemoryStorage::builder().build().unwrap());
        let builder = || {
            app.builder(layout.clone())
                .synced_tables(vec![])
                .migrations(vec![])
                .storage_connector(storage.clone())
        };
        let handle = builder().identity_custody(custody).create_store("Created").await.unwrap();
        let id = handle.store_id();
        assert_eq!(
            layout.stores().await.unwrap(),
            [StoreInfo { id, name: "Created".into(), unfinished_replacement: None }]
        );
        handle
            .setup_s3_storage(
                storage.config(),
                "Phone",
                "key".into(),
                SecretText::new("secret".into()),
            )
            .await
            .unwrap();
        let code = handle.restore_code().await.unwrap();
        assert_eq!(coven_sync::read_restore_code(&code).unwrap().store, id);
        handle.close().await.unwrap();
        if persisted {
            let custody = if passphrase {
                IdentityCustody::Passphrase(Passphrase::new("member password".into()))
            } else {
                IdentityCustody::Keyring
            };
            let reopened = builder().identity_custody(custody).open(id, |_| ()).await.unwrap();
            assert_eq!(reopened.restore_code().await.unwrap(), code);
            reopened.close().await.unwrap();
        }
    }
}

struct FailAfterIdentity {
    keychain: Arc<Keychain>,
    identity: Mutex<Option<MemberIdentity>>,
    fail_cleanup: bool,
}
impl MemberKeyCustody for FailAfterIdentity {
    fn unlock(&self) -> Result<Option<MemberIdentity>, KeyError> {
        Ok(self.identity.lock().unwrap().clone())
    }
    fn persist(&self, identity: &MemberIdentity) -> Result<(), KeyError> {
        *self.identity.lock().unwrap() = Some(identity.clone());
        self.keychain.fail_next_operation();
        Ok(())
    }
    fn forget(&self) -> Result<(), KeyError> {
        if self.fail_cleanup {
            return Err(KeyError::PassphraseAuthentication);
        }
        self.identity.lock().unwrap().take();
        Ok(())
    }
    fn close(&self) {}
}

#[tokio::test]
async fn failure_after_identity_creation_never_exposes_an_incomplete_store() {
    for fail_cleanup in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(root.path().into());
        let keychain = Keychain::in_memory("creation-failure").unwrap();
        let identity = Arc::new(FailAfterIdentity {
            keychain: keychain.clone(),
            identity: Mutex::new(None),
            fail_cleanup,
        });
        let builder = || {
            Coven::builder(layout.clone())
                .with_keychain(keychain.clone())
                .synced_tables(vec![])
                .migrations(vec![])
        };
        let expected = StoreId(SequentialIds::new().new_id());
        let error = builder()
            .id_source(Arc::new(SequentialIds::new()))
            .identity_custody(IdentityCustody::Custom(identity.clone()))
            .create_store("Refused")
            .await
            .unwrap_err();
        if fail_cleanup {
            assert!(matches!(error, StoreCreationError::Cleanup { operation, cleanup }
                if matches!(*operation, StoreCreationError::Store(CovenError::Key(_)))
                    && matches!(*cleanup, StoreCreationError::Store(CovenError::Key(KeyError::PassphraseAuthentication)))));
        } else {
            assert!(matches!(error, StoreCreationError::Store(CovenError::Key(_))), "{error:?}");
            assert!(identity.identity.lock().unwrap().is_none());
        }
        assert!(layout.stores().await.unwrap().is_empty());
        assert!(!layout.store_dir(&expected).database_path().exists());
        assert!(builder().open(expected, |_| ()).await.is_err());
        assert!(coven_crypto::custody::StoreKeychain::new(keychain, expected)
            .device_id()
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn creation_does_not_replace_an_existing_custom_identity() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let keychain = Keychain::in_memory("creation-existing").unwrap();
    let existing = MemberIdentity::generate(MemberId(UuidIds.new_id())).unwrap();
    let identity = Arc::new(FailAfterIdentity {
        keychain: keychain.clone(),
        identity: Mutex::new(Some(existing.clone())),
        fail_cleanup: false,
    });
    let error = Coven::builder(layout.clone())
        .with_keychain(keychain)
        .synced_tables(vec![])
        .migrations(vec![])
        .identity_custody(IdentityCustody::Custom(identity.clone()))
        .create_store("Refused")
        .await
        .unwrap_err();
    assert!(matches!(error, StoreCreationError::Identity(IdentityError::AlreadyInitialized)));
    assert_eq!(
        identity.identity.lock().unwrap().as_ref().unwrap().keys().public_keys(),
        existing.keys().public_keys()
    );
    assert!(layout.stores().await.unwrap().is_empty());
}
