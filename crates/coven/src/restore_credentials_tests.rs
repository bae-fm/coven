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
        assert_eq!(key.secret_access_key.as_str(), expected.secret_access_key.as_str());
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
                storage: coven_storage::RestoreStorage { location, s3_key: Some(key.clone()) }
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
        let saved = person_entries(&install, &owner).restore_code().unwrap().unwrap();
        coven_sync::read_saved_code(owner.directory.id(), saved.as_bytes()).unwrap();
        assert_key(&install, owner.directory.id(), &key);
        handle.close().await.unwrap();
    }
    owner.close().await;
}

/// Ana's laptop scans the code her phone showed, with her key inside, and the
/// app passes a key she typed as well. The laptop would hold two keys and no
/// way to tell which she meant, so the restore is refused before anything
/// changes (§13.1).
#[tokio::test]
async fn a_key_entered_beside_a_code_that_carries_one_is_refused() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let location =
        coven_storage::RestoreStorage::decode(owner.code.storage.as_bytes()).unwrap().location;
    let code = RestoreCode {
        store: owner.code.store,
        member: owner.code.member,
        member_keys: owner.code.member_keys.clone(),
        storage: coven_storage::RestoreStorage {
            location,
            s3_key: Some(supplied_key("own-device")),
        }
        .encode()
        .unwrap(),
    };
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let result = restore_from_code(
        install.builder(&owner, owner.storage.clone()),
        &code.to_text().unwrap(),
        Some(supplied_key("entered")),
        "Laptop",
        |_| panic!("bootstrap began"),
        &cancel,
    )
    .await;
    assert!(
        matches!(&result, Err(BootstrapError::Sync(SyncError::Storage(error)))
            if error.failure() == StorageFailure::InvalidConfiguration),
        "{:?}",
        result.map(drop)
    );
    assert!(!install.root.path().join("stores").exists());
    assert!(scoped(&install, &owner).storage_credentials().unwrap().is_none());
    let person = person_entries(&install, &owner);
    assert!(person.restore_code().unwrap().is_none());
    assert!(person.s3_credentials().unwrap().is_none());
    owner.close().await;
}

#[tokio::test]
async fn keychain_restore_uses_the_separate_key_or_the_explicit_replacement() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    for explicit in [false, true] {
        let install = Installation::new();
        let person = person_entries(&install, &owner);
        person.set_restore_code(&crate::tests::keychain_entry(&owner.code)).unwrap();
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
        assert_eq!(handle.restore_code().await.unwrap(), owner.code.to_text().unwrap().as_str());
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
            assert!(matches!(result, Err(BootstrapError::Sync(SyncError::Storage(error)))
                if error.failure() == StorageFailure::InvalidConfiguration));
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
                assert!(matches!(restore_from_code(install.builder(&owner, owner.storage.clone()),
                    &owner.code.to_text().unwrap(), Some(key), "Laptop", |_| panic!("bootstrap began"), &cancel).await,
                    Err(BootstrapError::Sync(SyncError::Storage(error))) if error.failure() == StorageFailure::InvalidConfiguration));
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
        person.set_restore_code(&crate::tests::keychain_entry(&owner.code)).unwrap();
        person.set_s3_credentials(&SecretBytes::new(b"bad key encoding".to_vec())).unwrap();
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
            assert!(matches!(error, BootstrapError::Sync(SyncError::Storage(error))
                if error.failure() == StorageFailure::Encoding));
        }
        assert!(!install.root.path().join("stores").exists());
        assert!(store.storage_credentials().unwrap().is_none());
    }
    owner.close().await;
}

#[tokio::test]
async fn non_s3_handles_refuse_a_new_access_key() {
    for provider in [CloudProvider::Dropbox, CloudProvider::OneDrive, CloudProvider::CloudKit] {
        let owner = Owner::new(provider, false).await;
        let before = owner.handle.restore_code().await.unwrap();
        let result = owner
            .handle
            .use_access_key(
                "key".into(),
                SecretText::new("secret".into()),
                &crate::tests::uncancelled(),
            )
            .await;
        assert!(
            matches!(result, Err(UseAccessKeyError::Sync(SyncError::Storage(error))) if error.failure() == StorageFailure::InvalidConfiguration)
        );
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
                    .use_access_key(
                        "replacement".into(),
                        SecretText::new("secret".into()),
                        &crate::tests::uncancelled()
                    )
                    .await,
                Err(UseAccessKeyError::Sync(SyncError::SecureStorage(_)))
            ),
            "operation {after}"
        );
        assert_key(&install, owner.directory.id(), &original);
        assert_eq!(handle.restore_code().await.unwrap(), before);
    }
    for cleanup in 0..2 {
        install.keychain.fail_operations_after(&[4, cleanup]);
        assert!(
            matches!(handle.use_access_key("replacement".into(), SecretText::new("secret".into()), &crate::tests::uncancelled()).await,
            Err(UseAccessKeyError::Sync(SyncError::Cleanup { operation, cleanup })) if matches!(*operation, SyncError::SecureStorage(_)) && matches!(*cleanup, SyncError::SecureStorage(_)))
        );
        assert_eq!(handle.restore_code().await.unwrap(), before);
        handle
            .use_access_key(
                original.access_key_id.clone(),
                original.secret_access_key.clone(),
                &crate::tests::uncancelled(),
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
    for phase in [Phase::Connecting, Phase::Loading, Phase::Loaded] {
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
            cancel_at(phase, &stop),
            &cancel,
        )
        .await;
        assert!(matches!(result, Err(BootstrapError::Cancelled)), "{phase:?}: {result:?}");
        assert_eq!(person.restore_code().unwrap().unwrap().as_bytes(), code.as_bytes());
        assert_eq!(person.s3_credentials().unwrap().unwrap().as_bytes(), key.as_bytes());
        assert!(store.storage_credentials().unwrap().is_none());
        assert!(install.app.stores().await.unwrap().is_empty());
    }
    owner.close().await;
}

#[tokio::test]
async fn deleting_a_restored_store_keeps_the_code_and_separate_key() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.app.store_dir(&owner.directory.id());
    let store = scoped(&install, &owner);
    let person = person_entries(&install, &owner);
    assert_key(&install, owner.directory.id(), &owner.s3_credentials().unwrap());
    let code = person.restore_code().unwrap().unwrap();
    handle.close().await.unwrap();
    crate::coven::delete::<StoreDeletionError>(&directory, &store).unwrap();
    assert!(install.app.stores().await.unwrap().is_empty());
    assert!(store.device_id().unwrap().is_none());
    assert!(store.storage_credentials().unwrap().is_none());
    // The person's other devices still restore from these.
    assert_eq!(person.restore_code().unwrap().unwrap().as_bytes(), code.as_bytes());
    assert_eq!(
        S3Credentials::decode(person.s3_credentials().unwrap().unwrap().as_bytes())
            .unwrap()
            .access_key_id,
        owner.s3_credentials().unwrap().access_key_id
    );
    owner.close().await;
}

/// The path of the slot after the last one stored.
async fn next_slot(owner: &Owner) -> ObjectPath {
    let last = crate::tests::stored(&owner.storage, owner.directory.id(), |path| {
        path.slot_number().is_some()
    })
    .await
    .iter()
    .filter_map(|object| object.path.slot_number())
    .map(|slot| slot.get())
    .max()
    .unwrap();
    ObjectPath::slot(owner.directory.id(), std::num::NonZeroU64::new(last + 1).unwrap())
}

/// Storage refuses the create of `slot`, as it would a claim it refused.
async fn refuse_claim(owner: &Owner, slot: ObjectPath) {
    owner
        .storage
        .set_faults(Faults {
            create_failures: [(slot, (StorageFailure::PermissionDenied, 403))].into(),
            ..Faults::none()
        })
        .await;
}

/// This member's access as `handle`'s store records it.
async fn recorded(handle: &CovenHandle) -> Option<MemberAccess> {
    handle.members().await.unwrap().into_iter().find(|member| member.is_self).unwrap().access
}

fn s3_access(id: &str) -> Option<MemberAccess> {
    Some(MemberAccess::S3AccessKey { access_key_id: id.into() })
}

/// Whether the key's check failed with `failure`.
fn check_failed(
    result: &Result<AccessKeyUsed, UseAccessKeyError>,
    failure: StorageFailure,
) -> bool {
    matches!(result, Err(UseAccessKeyError::Check(
        StorageCheckError::Storage(error) | StorageCheckError::ProviderCheck { source: error, .. },
    )) if error.failure() == failure)
}

async fn use_key(
    handle: &CovenHandle,
    key: &S3Credentials,
) -> Result<AccessKeyUsed, UseAccessKeyError> {
    handle
        .use_access_key(
            key.access_key_id.clone(),
            key.secret_access_key.clone(),
            &crate::tests::uncancelled(),
        )
        .await
}

/// One pass of `handle`'s at the clock's current time.
async fn synced_now(handle: &CovenHandle) {
    handle.start_sync().await.unwrap();
    match handle.retry_now().await.unwrap().await {
        RetryOutcome::Synced { .. } => {}
        outcome => panic!("the pass: {outcome:?}"),
    }
}

/// Ana mistypes her new key's secret, and S3 storage refuses it: the call
/// fails with the check, and her laptop keeps the key it had, the record and
/// an empty journal, and goes on syncing with the old key (§14.3).
#[tokio::test]
async fn a_key_storage_refuses_changes_nothing() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.app.store_dir(&owner.directory.id());
    let original = owner.s3_credentials().unwrap();
    let slot = next_slot(&owner).await;
    owner.storage.refuse_s3_key("mistyped").await;
    let refused = use_key(&handle, &supplied_key("mistyped")).await;
    assert!(check_failed(&refused, StorageFailure::Authentication), "{refused:?}");
    assert_key(&install, directory.id(), &original);
    assert_eq!(recorded(&handle).await, s3_access(&original.access_key_id));
    assert!(handle.pending().await.unwrap().is_empty());
    synced_now(&handle).await;
    assert_eq!(next_slot(&owner).await, slot);
    handle.close().await.unwrap();
    owner.close().await;
}

/// Ben's laptop lost its key, and he enters one S3 storage refuses: the call
/// fails with the check and saves nothing (§14.3).
#[tokio::test]
async fn a_signed_out_device_saves_no_key_storage_refuses() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.app.store_dir(&owner.directory.id());
    handle.close().await.unwrap();
    install.app.lose_storage_credentials(directory.id()).unwrap();
    let handle =
        install.builder(&owner, owner.storage.clone()).open(directory.id(), |_| ()).await.unwrap();
    owner.storage.refuse_s3_key("mistyped").await;
    let refused = use_key(&handle, &supplied_key("mistyped")).await;
    assert!(check_failed(&refused, StorageFailure::Authentication), "{refused:?}");
    assert!(scoped(&install, &owner).storage_credentials().unwrap().is_none());
    let person = person_entries(&install, &owner).s3_credentials().unwrap().unwrap();
    assert_eq!(S3Credentials::decode(person.as_bytes()).unwrap().access_key_id, "owner-key");
    assert!(handle.pending().await.unwrap().is_empty());
    handle.close().await.unwrap();
    owner.close().await;
}

/// A key that may read the store but not write it fails the provider check's
/// first write, and nothing changes (§20.8).
#[tokio::test]
async fn a_key_that_cannot_write_fails_its_check() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.app.store_dir(&owner.directory.id());
    let original = owner.s3_credentials().unwrap();
    owner.storage.refuse_s3_key_writes("read-only").await;
    let refused = use_key(&handle, &supplied_key("read-only")).await;
    assert!(
        matches!(&refused, Err(UseAccessKeyError::Check(StorageCheckError::ProviderCheck {
            check: StorageCheck::Create, source,
        })) if source.failure() == StorageFailure::PermissionDenied),
        "{refused:?}"
    );
    assert_key(&install, directory.id(), &original);
    assert_eq!(recorded(&handle).await, s3_access(&original.access_key_id));
    assert!(handle.pending().await.unwrap().is_empty());
    handle.close().await.unwrap();
    owner.close().await;
}

/// The app cancels while the key's check runs: the check stops after its
/// step in flight, deletes its test object and fails `Cancelled`, and
/// nothing changes (§14.3).
#[tokio::test]
async fn a_cancel_during_the_check_changes_nothing() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.app.store_dir(&owner.directory.id());
    let original = owner.s3_credentials().unwrap();
    let files = ObjectPrefix::files(owner.directory.id());
    let (started, starting) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    owner.storage.hold_next_creation(files.clone(), started, resumed).await;
    let (cancel, cancelled) = watch::channel(false);
    let key = supplied_key("replacement");
    let call = handle.use_access_key(key.access_key_id, key.secret_access_key, &cancelled);
    let cancelling = async {
        starting.await.unwrap();
        cancel.send(true).unwrap();
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(call, cancelling);
    assert!(matches!(result, Err(UseAccessKeyError::Cancelled)), "{result:?}");
    assert!(crate::tests::stored(&owner.storage, owner.directory.id(), |path| files
        .contains(path))
    .await
    .is_empty());
    assert_key(&install, directory.id(), &original);
    assert_eq!(recorded(&handle).await, s3_access(&original.access_key_id));
    assert!(handle.pending().await.unwrap().is_empty());
    handle.close().await.unwrap();
    owner.close().await;
}

/// A key storage accepts replaces the recorded one: the call says which key
/// the record named, the record names the new key, and this device's entry
/// and the person's hold it (§14.3).
#[tokio::test]
async fn a_good_key_replaces_the_recorded_one() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.app.store_dir(&owner.directory.id());
    let key = supplied_key("replacement");
    let used = use_key(&handle, &key).await.unwrap();
    assert_eq!(used.replaced.as_deref(), Some("owner-key"));
    assert_eq!(recorded(&handle).await, s3_access("replacement"));
    assert_key(&install, directory.id(), &key);
    assert!(handle.pending().await.unwrap().is_empty());
    handle.close().await.unwrap();
    owner.close().await;
}

/// The provider check's requests that S3 storage refuses for rate limiting
/// are sent again within the call's minute, and the key is used (§20.8).
#[tokio::test]
async fn a_rate_limited_check_is_waited_out() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    owner.stop_sync().await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    owner
        .storage
        .set_faults(Faults {
            fail_next: 2,
            failure: StorageFailure::RateLimited,
            response: Some(429),
            ..Faults::none()
        })
        .await;
    let started = owner.clock.elapsed();
    let key = supplied_key("replacement");
    let call = use_key(&handle, &key);
    tokio::pin!(call);
    let mut seen = owner.clock.sleeps_begun();
    let used = loop {
        tokio::select! {
            result = &mut call => break result,
            (begun, deadline) = owner.clock.sleep_begun_after(seen) => {
                seen = begun;
                owner.clock.advance(deadline.saturating_sub(owner.clock.elapsed()));
            }
        }
    };
    assert_eq!(used.unwrap().replaced.as_deref(), Some("owner-key"));
    assert!(owner.clock.elapsed() >= started + Duration::from_secs(1 + 2));
    assert_eq!(recorded(&handle).await, s3_access("replacement"));
    handle.close().await.unwrap();
    owner.close().await;
}

/// Ana's first new key passed its check, and its claim was refused; then
/// she deleted it in the console. Her next key passes its check, the first
/// key's entry ends without claiming, and the record ends on the next key:
/// a refused key never blocks a later one (§14.3).
#[tokio::test]
async fn a_refused_keys_entry_never_blocks_a_later_key() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.app.store_dir(&owner.directory.id());
    refuse_claim(&owner, next_slot(&owner).await).await;
    let first = supplied_key("first");
    let used = use_key(&handle, &first).await.unwrap();
    assert_eq!(used.replaced.as_deref(), Some("owner-key"));
    assert_key(&install, directory.id(), &first);
    assert_eq!(recorded(&handle).await, s3_access("owner-key"));
    assert!(!handle.pending().await.unwrap().is_empty());
    owner.storage.set_faults(Faults::none()).await;
    owner.storage.refuse_s3_key("first").await;
    let second = supplied_key("second");
    let used = use_key(&handle, &second).await.unwrap();
    assert_eq!(used.replaced.as_deref(), Some("owner-key"));
    assert_eq!(recorded(&handle).await, s3_access("second"));
    assert_key(&install, directory.id(), &second);
    assert!(handle.pending().await.unwrap().is_empty());
    handle.close().await.unwrap();
    owner.close().await;
}

/// The key passed its check and was taken, and the network dropped as the
/// entry's claim went out: the call succeeds, the entry stays journaled, and
/// the next pass claims it with the key (§14.3).
#[tokio::test]
async fn a_claim_that_fails_after_the_check_waits_for_a_pass() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.app.store_dir(&owner.directory.id());
    let slot = next_slot(&owner).await;
    // The network drops as the claim goes out.
    owner
        .storage
        .set_faults(Faults {
            create_failures: [(slot.clone(), (StorageFailure::Network, 503))].into(),
            ..Faults::none()
        })
        .await;
    let replacement = supplied_key("replacement");
    let used = use_key(&handle, &replacement).await.unwrap();
    assert_eq!(used.replaced.as_deref(), Some("owner-key"));
    assert_key(&install, directory.id(), &replacement);
    assert_eq!(recorded(&handle).await, s3_access("owner-key"));
    assert!(!handle.pending().await.unwrap().is_empty());
    owner.storage.set_faults(Faults::none()).await;
    owner.clock.advance(Duration::from_secs(600));
    synced_now(&handle).await;
    assert_eq!(recorded(&handle).await, s3_access("replacement"));
    assert!(handle.pending().await.unwrap().is_empty());
    assert_ne!(next_slot(&owner).await, slot);
    handle.close().await.unwrap();
    owner.close().await;
}

/// Calling again with the key whose entry's claim was refused runs that
/// entry now rather than journaling a second one, and finishes it once
/// (§14.3, E9).
#[tokio::test]
async fn retrying_a_refused_key_finishes_it_once() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.app.store_dir(&owner.directory.id());
    let slots = || async {
        crate::tests::stored(&owner.storage, owner.directory.id(), |path| {
            path.slot_number().is_some()
        })
        .await
        .len()
    };
    let before = slots().await;
    refuse_claim(&owner, next_slot(&owner).await).await;
    let replacement = supplied_key("replacement");
    use_key(&handle, &replacement).await.unwrap();
    assert_key(&install, directory.id(), &replacement);
    handle.close().await.unwrap();
    owner.storage.set_faults(Faults::none()).await;

    let handle = install.handle(directory.clone(), &owner).await;
    use_key(&handle, &replacement).await.unwrap();
    assert_key(&install, directory.id(), &replacement);
    assert_eq!(recorded(&handle).await, s3_access("replacement"));
    assert!(handle.pending().await.unwrap().is_empty());
    owner.sync().await;
    assert_eq!(slots().await, before + 1);
    handle.close().await.unwrap();
    owner.close().await;
}

/// Calling with the key the member's record already names journals nothing,
/// claims no slot and names no replaced key (§14.3).
#[tokio::test]
async fn the_key_the_record_names_journals_no_entry() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.app.store_dir(&owner.directory.id());
    let slot = || next_slot(&owner);
    let key = supplied_key("fresh");
    assert_eq!(use_key(&handle, &key).await.unwrap().replaced.as_deref(), Some("owner-key"));
    let recorded = slot().await;
    assert_eq!(use_key(&handle, &key).await.unwrap().replaced, None);
    assert_eq!(slot().await, recorded);
    assert!(handle.pending().await.unwrap().is_empty());
    assert_key(&install, directory.id(), &key);
    handle.close().await.unwrap();
    owner.close().await;
}

/// A provider's cooldown holds a retried call as it holds every request:
/// the key's check sends nothing and fails `RateLimited`, and the entry an
/// earlier call journaled stays for a pass to finish once the cooldown ends
/// (§6.7, §14.3).
#[tokio::test]
async fn a_cooldown_holds_a_retried_key() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    owner.stop_sync().await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.app.store_dir(&owner.directory.id());
    refuse_claim(&owner, next_slot(&owner).await).await;
    let replacement = supplied_key("replacement");
    use_key(&handle, &replacement).await.unwrap();
    // The provider answers the next request with 429 and Retry-After.
    owner
        .storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            retry_after: Some(200),
            ..Faults::none()
        })
        .await;
    let answered = use_key(&handle, &replacement).await;
    assert!(check_failed(&answered, StorageFailure::RateLimited), "{answered:?}");
    handle.test_parked().await;
    let requests = owner.storage.request_count();
    let held = use_key(&handle, &replacement).await;
    assert!(check_failed(&held, StorageFailure::RateLimited), "{held:?}");
    handle.test_parked().await;
    assert_eq!(owner.storage.request_count(), requests, "the retry sent a request");
    assert_key(&install, directory.id(), &replacement);
    assert!(!handle.pending().await.unwrap().is_empty());
    handle.close().await.unwrap();
    owner.close().await;
}
