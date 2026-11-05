use crate::*;
use coven_crypto::custody::Keychain;
use coven_foundation::clock::HANG_GUARD;
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
    let app = Coven::in_memory(root.path().into());
    let builder = || {
        app.builder().synced_tables(vec![]).migrations(vec![Migration::addition(
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
    let member = Custody::<MemberIdentity>::unlock(&identity).unwrap().unwrap();
    assert_eq!(
        app.readable_stores().await,
        [StoreInfo { id: expected, name: "Household".into(), unfinished: None }]
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
        Custody::<MemberIdentity>::unlock(&identity).unwrap().unwrap().keys().public_keys(),
        member.keys().public_keys()
    );
    reopened.close().await.unwrap();
    app.discard_store(&app.store_dir(&expected)).await.unwrap();
    assert!(app.stores().await.unwrap().is_empty());
    drop(error);
}

struct ClosingIdentity {
    keys: Mutex<Option<MemberIdentity>>,
    closing: tokio::sync::Notify,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl Custody<MemberIdentity> for ClosingIdentity {
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
        self.release.lock().unwrap().recv_timeout(HANG_GUARD).unwrap();
        self.keys.lock().unwrap().take();
    }
}

#[tokio::test]
async fn publication_sync_failure_waits_for_close_and_releases_session_custody() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let expected = StoreId(SequentialIds::new().new_id());
    let (release, wait) = std::sync::mpsc::channel();
    let identity = Arc::new(ClosingIdentity {
        keys: Mutex::new(None),
        closing: tokio::sync::Notify::new(),
        release: Mutex::new(wait),
    });
    let creation = tokio::spawn(super::create(
        app.builder()
            .id_source(Arc::new(SequentialIds::new()))
            .synced_tables(vec![])
            .migrations(vec![])
            .identity_custody(CustodyChoice::Custom(identity.clone())),
        "Household".into(),
        fail_publication_sync,
    ));
    tokio::time::timeout(HANG_GUARD, identity.closing.notified()).await.unwrap();
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
    app.discard_store(&app.store_dir(&expected)).await.unwrap();
    assert!(app.stores().await.unwrap().is_empty());
    drop(error);
}

#[tokio::test]
async fn creation_initializes_every_builtin_identity_custody_before_setup() {
    for custody in [
        CustodyChoice::Keyring,
        CustodyChoice::InMemory,
        CustodyChoice::Passphrase(Passphrase::new("member password".into())),
    ] {
        let persisted = !matches!(custody, CustodyChoice::InMemory);
        let passphrase = matches!(custody, CustodyChoice::Passphrase(_));
        let root = tempfile::tempdir().unwrap();
        let app = Coven::in_memory(root.path().into());
        let storage = Arc::new(MemoryStorage::builder().build().unwrap());
        let builder = || {
            app.builder()
                .synced_tables(vec![])
                .migrations(vec![])
                .storage_connector(storage.clone())
        };
        let handle = builder().identity_custody(custody).create_store("Created").await.unwrap();
        let id = handle.store_id();
        assert_eq!(
            app.readable_stores().await,
            [StoreInfo { id, name: "Created".into(), unfinished: None }]
        );
        handle
            .setup_s3_storage(
                storage.config(),
                "Phone",
                "key".into(),
                SecretText::new("secret".into()),
                |_| {},
            )
            .await
            .unwrap();
        let code = handle.restore_code().await.unwrap();
        assert_eq!(coven_sync::read_restore_code(&code).unwrap().store, id);
        handle.close().await.unwrap();
        if persisted {
            let custody = if passphrase {
                CustodyChoice::Passphrase(Passphrase::new("member password".into()))
            } else {
                CustodyChoice::Keyring
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
impl Custody<MemberIdentity> for FailAfterIdentity {
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
        let keychain = Keychain::in_memory("creation-failure").unwrap();
        let app = Coven::with_keychain(root.path().into(), keychain.clone());
        let identity = Arc::new(FailAfterIdentity {
            keychain: keychain.clone(),
            identity: Mutex::new(None),
            fail_cleanup,
        });
        let builder = || app.builder().synced_tables(vec![]).migrations(vec![]);
        let expected = StoreId(SequentialIds::new().new_id());
        let error = builder()
            .id_source(Arc::new(SequentialIds::new()))
            .identity_custody(CustodyChoice::Custom(identity.clone()))
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
        assert!(app.stores().await.unwrap().is_empty());
        assert!(!app.store_dir(&expected).database_path().exists());
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
    let keychain = Keychain::in_memory("creation-existing").unwrap();
    let app = Coven::with_keychain(root.path().into(), keychain.clone());
    let existing = MemberIdentity::generate(MemberId(UuidIds.new_id())).unwrap();
    let identity = Arc::new(FailAfterIdentity {
        keychain: keychain.clone(),
        identity: Mutex::new(Some(existing.clone())),
        fail_cleanup: false,
    });
    let error = app
        .builder()
        .synced_tables(vec![])
        .migrations(vec![])
        .identity_custody(CustodyChoice::Custom(identity.clone()))
        .create_store("Refused")
        .await
        .unwrap_err();
    assert!(matches!(error, StoreCreationError::Identity(IdentityError::AlreadyInitialized)));
    assert_eq!(
        identity.identity.lock().unwrap().as_ref().unwrap().keys().public_keys(),
        existing.keys().public_keys()
    );
    assert!(app.stores().await.unwrap().is_empty());
}

/// Creation's publish step panics before it removes the marker. The store
/// was never published, so creation undoes its custody as after any failure
/// before publication: no store, no member identity, no device id (E15).
#[tokio::test]
async fn a_panic_before_publication_undoes_the_stores_custody() {
    let root = tempfile::tempdir().unwrap();
    let keychain = Keychain::in_memory("creation-panic").unwrap();
    let app = Coven::with_keychain(root.path().into(), keychain.clone());
    let expected = StoreId(SequentialIds::new().new_id());
    let error = super::create(
        app.builder()
            .id_source(Arc::new(SequentialIds::new()))
            .synced_tables(vec![])
            .migrations(vec![]),
        "Household".into(),
        |_pending: &coven_foundation::files::BootstrapStore| panic!("the marker is held open"),
    )
    .await
    .unwrap_err();
    let StoreCreationError::Bug(bug) = &error else { panic!("{error:?}") };
    assert_eq!(bug.message, "the marker is held open");
    assert!(app.stores().await.unwrap().is_empty());
    assert!(!app.store_dir(&expected).database_path().exists());
    let store_keychain = Arc::new(coven_crypto::custody::StoreKeychain::new(keychain, expected));
    assert!(store_keychain.device_id().unwrap().is_none());
    let identity = coven_crypto::custody::KeyringCustody::new(store_keychain);
    assert!(Custody::<MemberIdentity>::unlock(&identity).unwrap().is_none());
}

/// Creation's publish step panics after it removed the marker: the store
/// is published, so creation keeps its custody and says so, as when
/// syncing the directory fails after publication (E15).
#[tokio::test]
async fn a_panic_after_publication_keeps_the_published_store() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let builder = || app.builder().synced_tables(vec![]).migrations(vec![]);
    let expected = StoreId(SequentialIds::new().new_id());
    let error = super::create(
        builder().id_source(Arc::new(SequentialIds::new())),
        "Household".into(),
        |pending: &coven_foundation::files::BootstrapStore| {
            pending.publish()?;
            panic!("the directory's sync is lost")
        },
    )
    .await
    .unwrap_err();
    let StoreCreationError::PublishedButNotSynced { store, source } = &error else {
        panic!("{error:?}")
    };
    assert_eq!(*store, expected);
    assert!(
        matches!(source.as_ref(), StoreCreationError::Bug(bug) if bug.message == "the directory's sync is lost"),
        "{source:?}"
    );
    assert_eq!(
        app.readable_stores().await,
        [StoreInfo { id: expected, name: "Household".into(), unfinished: None }]
    );
    let reopened = builder().open(expected, |_| ()).await.unwrap();
    reopened.close().await.unwrap();
    app.discard_store(&app.store_dir(&expected)).await.unwrap();
}
