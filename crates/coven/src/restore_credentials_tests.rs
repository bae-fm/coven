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

fn person_entries(install: &Installation, owner: &Owner) -> PersonEntries {
    PersonEntries::new(install.keychain.clone(), owner.directory.id())
}

/// This device's key and the person's saved key are both `expected`.
pub(super) fn assert_key(install: &Installation, id: StoreId, expected: &S3Credentials) {
    let store = StoreKeychain::new(install.keychain.clone(), id);
    let person = PersonEntries::new(install.keychain.clone(), id);
    let StorageCredentials::S3(device) =
        StorageCredentials::decode(store.storage_credentials().unwrap().unwrap().as_bytes())
            .unwrap()
    else {
        panic!("S3 device key")
    };
    let person =
        S3Credentials::decode(person.s3_credentials().unwrap().unwrap().as_bytes()).unwrap();
    for key in [device, person] {
        assert_eq!(key.access_key_id, expected.access_key_id);
        assert_eq!(
            key.secret_access_key.as_str(),
            expected.secret_access_key.as_str()
        );
    }
}

#[tokio::test]
async fn an_entered_key_and_one_the_code_carries_both_restore_and_leave_the_code_keyless() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let text = owner.code.to_text().unwrap();
    for carried in [false, true] {
        let install = Installation::new();
        let key = supplied_key(if carried { "own-device" } else { "entered" });
        let (scanned, supplied) = if carried {
            // The person's own device put the key in the code it showed.
            let location = coven_storage::RestoreStorage::decode(owner.code.storage.as_bytes())
                .unwrap()
                .location;
            let code = RestoreCode {
                store: owner.code.store,
                member: owner.code.member,
                member_keys: owner.code.member_keys.clone(),
                storage: coven_storage::RestoreStorage {
                    location,
                    s3_key: Some(key.clone()),
                }
                .encode()
                .unwrap(),
            };
            (code.to_text().unwrap(), None)
        } else {
            (text.clone(), Some(key.clone()))
        };
        let (_, cancel) = watch::channel(false);
        let handle = restore_from_code(
            install.builder(&owner, owner.storage.clone()),
            &scanned,
            supplied,
            "Laptop",
            |_| {},
            &cancel,
        )
        .await
        .unwrap();
        // The code saved and shown again carries no key either way.
        assert_eq!(handle.restore_code().await.unwrap(), text.as_str());
        let saved = person_entries(&install, &owner)
            .restore_code()
            .unwrap()
            .unwrap();
        coven_sync::read_saved_code(owner.directory.id(), saved.as_bytes()).unwrap();
        assert_key(&install, owner.directory.id(), &key);
        handle.close().await.unwrap();
    }
    owner.close().await;
}

#[tokio::test]
async fn keychain_restore_uses_the_separate_key_or_the_explicit_replacement() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    for explicit in [false, true] {
        let install = Installation::new();
        let person = person_entries(&install, &owner);
        person
            .set_restore_code(&crate::tests::keychain_entry(&owner.code))
            .unwrap();
        let key = supplied_key("held-separately");
        person
            .set_s3_credentials(&if explicit {
                // Explicit input must not try to decode a held key.
                SecretBytes::new(b"unreadable held key".to_vec())
            } else {
                key.encode().unwrap()
            })
            .unwrap();
        let (_, cancel) = watch::channel(false);
        let handle = restore_from_saved_code(
            install.builder(&owner, owner.storage.clone()),
            &crate::tests::readable(crate::saved_restore::list(install.keychain.clone()).unwrap())
                [0],
            explicit.then(|| key.clone()),
            "Laptop",
            |_| {},
            &cancel,
        )
        .await
        .unwrap()
        .unwrap();
        assert_key(&install, owner.directory.id(), &key);
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
                person_entries(&install, &owner)
                    .set_restore_code(&crate::tests::keychain_entry(&owner.code))
                    .unwrap();
            }
            let supplied = (provider != CloudProvider::S3).then(|| supplied_key("wrong-provider"));
            let (_, cancel) = watch::channel(false);
            let builder = install.builder(&owner, owner.storage.clone());
            let result = if from_keychain {
                restore_from_saved_code(
                    builder,
                    &crate::tests::readable(
                        crate::saved_restore::list(install.keychain.clone()).unwrap(),
                    )[0],
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
        let person = person_entries(&install, &owner);
        person
            .set_restore_code(&crate::tests::keychain_entry(&owner.code))
            .unwrap();
        person
            .set_s3_credentials(&SecretBytes::new(b"bad key encoding".to_vec()))
            .unwrap();
        let saved =
            crate::tests::readable(crate::saved_restore::list(install.keychain.clone()).unwrap())
                .remove(0);
        if refused_read {
            install.keychain.fail_operations_after(&[1]);
        }
        let (_, cancel) = watch::channel(false);
        let error = restore_from_saved_code(
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
                BootstrapError::SecureStorage(KeyError::KeychainTemporarilyUnavailable(_))
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
        assert_key(&install, owner.directory.id(), &original);
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
        assert_key(&install, owner.directory.id(), &original);
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
        let person = person_entries(&install, &owner);
        let code = crate::tests::keychain_entry(&owner.code);
        let key = owner.s3_credentials().unwrap().encode().unwrap();
        person.set_restore_code(&code).unwrap();
        person.set_s3_credentials(&key).unwrap();
        let saved =
            crate::tests::readable(crate::saved_restore::list(install.keychain.clone()).unwrap())
                .remove(0);
        let (stop, cancel) = watch::channel(false);
        let result = restore_from_saved_code(
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
            person.restore_code().unwrap().unwrap().as_bytes(),
            code.as_bytes()
        );
        assert_eq!(
            person.s3_credentials().unwrap().unwrap().as_bytes(),
            key.as_bytes()
        );
        assert!(store.storage_credentials().unwrap().is_none());
        assert!(install.layout.stores().await.unwrap().is_empty());
    }
    owner.close().await;
}

#[tokio::test]
async fn deleting_a_restored_store_keeps_the_code_and_separate_key() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.layout.store_dir(&owner.directory.id());
    let store = scoped(&install, &owner);
    let person = person_entries(&install, &owner);
    assert_key(
        &install,
        owner.directory.id(),
        &owner.s3_credentials().unwrap(),
    );
    let code = person.restore_code().unwrap().unwrap();
    handle.close().await.unwrap();
    crate::coven::delete(&directory, &store).unwrap();
    assert!(install.layout.stores().await.unwrap().is_empty());
    assert!(store.device_id().unwrap().is_none());
    assert!(store.storage_credentials().unwrap().is_none());
    // The person's other devices still restore from these.
    assert_eq!(
        person.restore_code().unwrap().unwrap().as_bytes(),
        code.as_bytes()
    );
    assert_eq!(
        S3Credentials::decode(person.s3_credentials().unwrap().unwrap().as_bytes())
            .unwrap()
            .access_key_id,
        owner.s3_credentials().unwrap().access_key_id
    );
    owner.close().await;
}

/// The key inside the restore code this device shows its person's new device.
async fn transfer_key(handle: &CovenHandle) -> S3Credentials {
    let code =
        coven_sync::read_restore_code(&handle.restore_code_for_transfer().await.unwrap()).unwrap();
    coven_storage::RestoreStorage::decode(code.storage.as_bytes())
        .unwrap()
        .s3_key
        .expect("an S3 device holds a key")
}

fn assert_same_key(actual: &S3Credentials, expected: &S3Credentials) {
    assert_eq!(actual.access_key_id, expected.access_key_id);
    assert_eq!(
        actual.secret_access_key.as_str(),
        expected.secret_access_key.as_str()
    );
}

#[tokio::test]
async fn the_key_in_the_transfer_code_is_the_one_this_device_uses_now() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let phone = Installation::new();
    let handle = phone.restore(&owner).await;
    let directory = phone.layout.store_dir(&owner.directory.id());
    let code = handle.restore_code().await.unwrap();
    assert_same_key(
        &transfer_key(&handle).await,
        &owner.s3_credentials().unwrap(),
    );
    let replaced = supplied_key("replaced");
    handle
        .replace_access_key(
            replaced.access_key_id.clone(),
            replaced.secret_access_key.clone(),
        )
        .await
        .unwrap();
    assert_same_key(&transfer_key(&handle).await, &replaced);
    let updated = supplied_key("updated");
    handle
        .update_credentials(
            updated.access_key_id.clone(),
            updated.secret_access_key.clone(),
        )
        .await
        .unwrap();
    assert_same_key(&transfer_key(&handle).await, &updated);
    handle.close().await.unwrap();
    let handle = phone.handle(directory, &owner).await;
    assert_same_key(&transfer_key(&handle).await, &updated);
    // The code to write down is unchanged and carries no key.
    assert_eq!(handle.restore_code().await.unwrap(), code);
    assert!(!decode_code_info(&code).unwrap().metadata().carries_s3_key);
    let shown = handle.restore_code_for_transfer().await.unwrap();
    let info = decode_code_info(&shown).unwrap();
    assert!(info.metadata().carries_s3_key);
    assert_eq!(info, {
        let mut written = decode_code_info(&code).unwrap();
        match &mut written {
            CodeInfo::Restore(metadata) => metadata.carries_s3_key = true,
            CodeInfo::Invite(_) => panic!("a restore code"),
        }
        written
    });

    // The laptop scans the one code, and takes no second key beside it.
    let laptop = Installation::new();
    let (_, cancel) = watch::channel(false);
    let twice = restore_from_code(
        laptop.builder(&owner, owner.storage.clone()),
        &shown,
        Some(supplied_key("another")),
        "Laptop",
        |_| {},
        &cancel,
    )
    .await;
    assert!(matches!(
        twice,
        Err(BootstrapError::Sync(SyncError::Storage(error)))
            if error.failure() == StorageFailure::InvalidConfiguration
    ));
    let restored = restore_from_code(
        laptop.builder(&owner, owner.storage.clone()),
        &shown,
        None,
        "Laptop",
        |_| {},
        &cancel,
    )
    .await
    .unwrap();
    assert_key(&laptop, owner.directory.id(), &updated);
    restored.close().await.unwrap();
    handle.close().await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn other_providers_transfer_the_code_to_write_down() {
    for provider in [
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
        CloudProvider::CloudKit,
    ] {
        let owner = Owner::new(provider, false).await;
        let shown = owner.handle.restore_code_for_transfer().await.unwrap();
        assert_eq!(shown, owner.handle.restore_code().await.unwrap());
        assert!(!decode_code_info(&shown).unwrap().metadata().carries_s3_key);
        owner.close().await;
    }
}

#[tokio::test]
async fn a_locked_keychain_is_reported_rather_than_an_absent_key() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    install.keychain.fail_operations_after(&[0]);
    assert!(matches!(
        handle.restore_code_for_transfer().await,
        Err(SyncError::SecureStorage(
            KeyError::KeychainTemporarilyUnavailable(_)
        ))
    ));
    assert_same_key(
        &transfer_key(&handle).await,
        &owner.s3_credentials().unwrap(),
    );
    handle.close().await.unwrap();
    owner.close().await;
}
