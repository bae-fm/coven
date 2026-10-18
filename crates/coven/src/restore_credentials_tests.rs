use super::*;

fn supplied_key(id: &str) -> S3Credentials {
    S3Credentials {
        access_key_id: id.into(),
        secret_access_key: SecretText::new(format!("{id}-secret")),
    }
}

fn scoped(install: &Installation, owner: &Owner) -> StoreKeychain {
    StoreKeychain::new(install.keychain.clone(), owner.directory.id())
}

pub(super) fn assert_key(store: &StoreKeychain, expected: &S3Credentials) {
    let StorageCredentials::S3(device) =
        StorageCredentials::decode(store.storage_credentials().unwrap().unwrap().as_bytes())
            .unwrap()
    else {
        panic!("S3 device key")
    };
    let synced =
        S3Credentials::decode(store.synced_s3_credentials().unwrap().unwrap().as_bytes()).unwrap();
    for key in [device, synced] {
        assert_eq!(key.access_key_id, expected.access_key_id);
        assert_eq!(
            key.secret_access_key.as_str(),
            expected.secret_access_key.as_str()
        );
    }
}

#[tokio::test]
async fn entered_and_own_device_transferred_keys_are_separate_from_the_code() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    for transferred in [false, true] {
        let install = Installation::new();
        let key = supplied_key(if transferred { "own-device" } else { "entered" });
        let supplied = if transferred {
            // An app transports the key beside the code as its own value.
            S3Credentials::decode(key.encode().unwrap().as_bytes()).unwrap()
        } else {
            key.clone()
        };
        let text = owner.code.to_text().unwrap();
        let (_, cancel) = watch::channel(false);
        let handle = restore_from_code(
            install.builder(&owner, owner.storage.clone()),
            &text,
            Some(supplied),
            "Laptop",
            |_| {},
            &cancel,
        )
        .await
        .unwrap();
        assert_eq!(handle.restore_code().await.unwrap(), text.as_str());
        assert_key(&scoped(&install, &owner), &key);
        handle.close().await.unwrap();
    }
    owner.close().await;
}

#[tokio::test]
async fn keychain_restore_uses_the_separate_key_or_the_explicit_replacement() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    for explicit in [false, true] {
        let install = Installation::new();
        let store = scoped(&install, &owner);
        store
            .set_synced_restore_code(&SecretBytes::new(owner.code.to_bytes().unwrap().to_vec()))
            .unwrap();
        let key = supplied_key("held-separately");
        store
            .set_synced_s3_credentials(&if explicit {
                // Explicit input must not try to decode a held key.
                SecretBytes::new(b"unreadable held key".to_vec())
            } else {
                key.encode().unwrap()
            })
            .unwrap();
        let (_, cancel) = watch::channel(false);
        let handle = restore_from_keychain(
            install.builder(&owner, owner.storage.clone()),
            &crate::saved_restore::list(install.keychain.clone()).unwrap()[0],
            explicit.then(|| key.clone()),
            "Laptop",
            |_| {},
            &cancel,
        )
        .await
        .unwrap()
        .unwrap();
        assert_key(&store, &key);
        assert_eq!(
            handle.restore_code().await.unwrap(),
            owner.code.to_text().unwrap().as_str()
        );
        handle.close().await.unwrap();
    }
    owner.close().await;
}

#[tokio::test]
async fn missing_invalid_and_wrong_provider_keys_fail_before_bootstrap() {
    for provider in [
        CloudProvider::S3,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
        CloudProvider::CloudKit,
    ] {
        let owner = Owner::new(provider, false).await;
        for from_keychain in [false, true] {
            let install = Installation::new();
            let store = scoped(&install, &owner);
            let text = owner.code.to_text().unwrap();
            if from_keychain {
                store
                    .set_synced_restore_code(&SecretBytes::new(
                        owner.code.to_bytes().unwrap().to_vec(),
                    ))
                    .unwrap();
            }
            let supplied = (provider != CloudProvider::S3).then(|| supplied_key("wrong-provider"));
            let (_, cancel) = watch::channel(false);
            let builder = install.builder(&owner, owner.storage.clone());
            let result = if from_keychain {
                restore_from_keychain(
                    builder,
                    &crate::saved_restore::list(install.keychain.clone()).unwrap()[0],
                    supplied,
                    "Laptop",
                    |_| panic!("bootstrap began"),
                    &cancel,
                )
                .await
            } else {
                restore_from_code(
                    builder,
                    &text,
                    supplied,
                    "Laptop",
                    |_| panic!("bootstrap began"),
                    &cancel,
                )
                .await
                .map(Some)
            };
            assert!(
                matches!(result, Err(BootstrapError::Sync(SyncError::Storage(error)))
                if error.failure() == StorageFailure::InvalidConfiguration)
            );
            assert!(!install.root.path().join("stores").exists());
            assert!(store.storage_credentials().unwrap().is_none());
        }
        if provider == CloudProvider::S3 {
            for key in [
                S3Credentials {
                    access_key_id: String::new(),
                    secret_access_key: SecretText::new("secret".into()),
                },
                S3Credentials {
                    access_key_id: "key".into(),
                    secret_access_key: SecretText::new(String::new()),
                },
            ] {
                let install = Installation::new();
                let (_, cancel) = watch::channel(false);
                assert!(
                    matches!(restore_from_code(install.builder(&owner, owner.storage.clone()),
                    &owner.code.to_text().unwrap(), Some(key), "Laptop", |_| panic!("bootstrap began"), &cancel).await,
                    Err(BootstrapError::Sync(SyncError::Storage(error))) if error.failure() == StorageFailure::InvalidConfiguration)
                );
                assert!(!install.root.path().join("stores").exists());
            }
        }
        owner.close().await;
    }
}

#[tokio::test]
async fn separately_held_key_read_failures_and_malformed_keys_are_visible_before_bootstrap() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    for refused_read in [false, true] {
        let install = Installation::new();
        let store = scoped(&install, &owner);
        store
            .set_synced_restore_code(&SecretBytes::new(owner.code.to_bytes().unwrap().to_vec()))
            .unwrap();
        store
            .set_synced_s3_credentials(&SecretBytes::new(b"bad key encoding".to_vec()))
            .unwrap();
        let saved = crate::saved_restore::list(install.keychain.clone())
            .unwrap()
            .remove(0);
        if refused_read {
            install.keychain.fail_operations_after(&[1]);
        }
        let (_, cancel) = watch::channel(false);
        let error = restore_from_keychain(
            install.builder(&owner, owner.storage.clone()),
            &saved,
            None,
            "Laptop",
            |_| panic!("bootstrap began"),
            &cancel,
        )
        .await
        .unwrap_err();
        if refused_read {
            assert!(matches!(
                error,
                BootstrapError::SecureStorage(KeyError::Keychain(_))
            ));
        } else {
            assert!(
                matches!(error, BootstrapError::Sync(SyncError::Storage(error))
                if error.failure() == StorageFailure::Encoding)
            );
        }
        assert!(!install.root.path().join("stores").exists());
        assert!(store.storage_credentials().unwrap().is_none());
    }
    owner.close().await;
}

#[tokio::test]
async fn non_s3_handles_refuse_both_credential_update_calls() {
    for provider in [
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
        CloudProvider::CloudKit,
    ] {
        let owner = Owner::new(provider, false).await;
        let before = owner.handle.restore_code().await.unwrap();
        for result in [
            owner
                .handle
                .update_credentials("key".into(), SecretText::new("secret".into()))
                .await,
            owner
                .handle
                .replace_access_key("key".into(), SecretText::new("secret".into()))
                .await,
        ] {
            assert!(
                matches!(result, Err(SyncError::Storage(error)) if error.failure() == StorageFailure::InvalidConfiguration)
            );
        }
        assert_eq!(owner.handle.restore_code().await.unwrap(), before);
        owner.close().await;
    }
}

#[tokio::test]
async fn failed_keychain_updates_restore_both_keys_and_keep_the_code() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let store = scoped(&install, &owner);
    let original = owner.s3_credentials().unwrap();
    let before = handle.restore_code().await.unwrap();
    for after in 0..5 {
        install.keychain.fail_operations_after(&[after]);
        assert!(
            matches!(
                handle
                    .update_credentials("replacement".into(), SecretText::new("secret".into()))
                    .await,
                Err(SyncError::SecureStorage(_))
            ),
            "operation {after}"
        );
        assert_key(&store, &original);
        assert_eq!(handle.restore_code().await.unwrap(), before);
    }
    for cleanup in 0..2 {
        install.keychain.fail_operations_after(&[4, cleanup]);
        assert!(
            matches!(handle.update_credentials("replacement".into(), SecretText::new("secret".into())).await,
            Err(SyncError::Cleanup { operation, cleanup }) if matches!(*operation, SyncError::SecureStorage(_)) && matches!(*cleanup, SyncError::SecureStorage(_)))
        );
        assert_eq!(handle.restore_code().await.unwrap(), before);
        handle
            .update_credentials(
                original.access_key_id.clone(),
                original.secret_access_key.clone(),
            )
            .await
            .unwrap();
        assert_key(&store, &original);
    }
    handle.close().await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn cancelled_keychain_restore_preserves_the_code_and_separately_held_key() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    // Before connecting, while loading, and after loading before publication.
    for phase in [
        (BootstrapStatus::Connecting, 1),
        (BootstrapStatus::Loading, 1),
        (BootstrapStatus::Loading, 2),
    ] {
        let install = Installation::new();
        let store = scoped(&install, &owner);
        let code = SecretBytes::new(owner.code.to_bytes().unwrap().to_vec());
        let key = owner.s3_credentials().unwrap().encode().unwrap();
        store.set_synced_restore_code(&code).unwrap();
        store.set_synced_s3_credentials(&key).unwrap();
        let saved = crate::saved_restore::list(install.keychain.clone())
            .unwrap()
            .remove(0);
        let (stop, cancel) = watch::channel(false);
        let result = restore_from_keychain(
            install.builder(&owner, owner.storage.clone()),
            &saved,
            None,
            "Laptop",
            cancel_at(&phase, &stop),
            &cancel,
        )
        .await;
        assert!(
            matches!(result, Err(BootstrapError::Cancelled)),
            "{phase:?}: {result:?}"
        );
        assert_eq!(
            store.synced_restore_code().unwrap().unwrap().as_bytes(),
            code.as_bytes()
        );
        assert_eq!(
            store.synced_s3_credentials().unwrap().unwrap().as_bytes(),
            key.as_bytes()
        );
        assert!(store.storage_credentials().unwrap().is_none());
        assert!(install.layout.stores().await.unwrap().is_empty());
    }
    owner.close().await;
}

#[tokio::test]
async fn deleting_a_restored_store_removes_the_code_and_separate_key() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.layout.store_dir(&owner.directory.id());
    let store = scoped(&install, &owner);
    assert_key(&store, &owner.s3_credentials().unwrap());
    assert!(store.synced_restore_code().unwrap().is_some());
    handle.close().await.unwrap();
    crate::coven::delete(&directory, &store).unwrap();
    install.absent(owner.directory.id()).await;
    owner.close().await;
}
