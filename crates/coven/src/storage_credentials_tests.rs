use super::*;
use coven_crypto::custody::PersonEntries;
use coven_foundation::files::StoreDirAccess;

async fn assert_original_connection_uploads(f: &Fixture) {
    assert!(f.connector.clients.lock().unwrap()[0].upgrade().is_some());
    let provider = f.connector.providers.lock().unwrap()[0].clone();
    let key = provider.s3_credentials().await.unwrap();
    assert_eq!(key.access_key_id, "member-key");
    f.advance(Duration::from_secs(1));
    f.handle
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('retained','still connected')", [])?;
            Ok::<_, CovenError>(())
        })
        .await
        .unwrap();
    synced_since(&f.handle, f.clock.now()).await;
    assert!(f.handle.test_queued_writes().await.unwrap().is_empty());
}

#[tokio::test]
async fn failed_key_forgetting_keeps_the_running_connection() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    f.app.fail_next_keychain_operation();
    assert!(f.handle.forget_store_keys().await.is_err());
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Available);
    assert_original_connection_uploads(&f).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn key_forgetting_performs_no_fallible_custody_reads_after_removal() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    f.app.fail_keychain_operations_after(&[1]);
    f.handle.forget_store_keys().await.unwrap();
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Locked);
    // Releasing the connection recorded no failure.
    assert_eq!(f.handle.test_last_pass().latest().await, Some(LastPass::Synced));
    f.app.fail_keychain_operations_after(&[]);
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn failed_setup_claim_keeps_the_running_connection() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    let before = f.handle.restore_code().await.unwrap();
    f.storage
        .set_faults(Faults {
            read_failures: [(
                ObjectPath::floor(f.directory.id()),
                StorageFailure::PermissionDenied,
            )]
            .into(),
            ..Faults::none()
        })
        .await;
    assert!(f.setup().await.is_err());
    f.storage.set_faults(Faults::none()).await;
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    assert_original_connection_uploads(&f).await;
    f.handle.close().await.unwrap();
}

/// Setup's claim panics while the running connection's passes are held:
/// setup fails with the bug, and the passes run again, as after any failed
/// setup (E15).
#[tokio::test]
async fn a_setup_whose_claim_panics_lets_the_held_passes_run_again() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    f.storage
        .set_faults(Faults {
            read_panics: [ObjectPath::floor(f.directory.id())].into(),
            ..Faults::none()
        })
        .await;
    let failed = f.setup().await.unwrap_err();
    f.storage.set_faults(Faults::none()).await;
    assert!(matches!(&failed, StorageSetupError::Bug(_)), "{failed:?}");
    assert_original_connection_uploads(&f).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn failed_setup_custody_commit_keeps_the_running_connection() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    let before = f.handle.restore_code().await.unwrap();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage.hold_next_read(ObjectPath::floor(f.directory.id()), entered, resumed).await;
    let setup = f.setup();
    tokio::pin!(setup);
    tokio::select! {
        result = &mut setup => panic!("setup did not reach its claim: {result:?}"),
        _ = entering => {}
    }
    // Unlock persists its held ring; the following credential read fails.
    f.app.fail_keychain_operations_after(&[1]);
    resume.send(()).unwrap();
    assert!(matches!(setup.await, Err(StorageSetupError::SecureStorage(_))));
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    assert_original_connection_uploads(&f).await;
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn setup_key_failure_restores_the_separate_saved_values() {
    use coven_crypto::custody::{Keychain, StoreKeychain};
    let f = Fixture::new().await;
    f.handle.close().await.unwrap();
    let keychain = Keychain::in_memory("setup-rollback").unwrap();
    let scoped = StoreKeychain::new(keychain.clone(), f.directory.id());
    let person = PersonEntries::new(keychain.clone(), f.directory.id());
    scoped.set_device_id(f.directory.settings().unwrap().device_id).unwrap();
    let old_code = SecretBytes::new(b"previous saved code".to_vec());
    let old_key = SecretBytes::new(b"previous saved S3 key".to_vec());
    person.set_restore_code(&old_code).unwrap();
    person.set_s3_credentials(&old_key).unwrap();
    let app = Coven::with_keychain(f._root.path().into(), keychain);
    let handle = builder(&app, f.clock.clone(), f.storage.clone())
        .identity_custody(CustodyChoice::Custom(Arc::new(
            coven_crypto::custody::KeyringCustody::new(Arc::new(
                f.app.store_keychain(f.directory.id()),
            )),
        )))
        .key_custody(CustodyChoice::Custom(Arc::new(RefusePersist {
            keys: std::sync::Mutex::new(None),
            refuse: AtomicBool::new(true),
        })))
        .open(f.directory.id(), |_| ())
        .await
        .unwrap();
    assert!(matches!(
        handle
            .setup_s3_storage(
                f.storage.config(),
                "Device",
                "replacement".into(),
                SecretText::new("secret".into()),
                |_| {}
            )
            .await,
        Err(StorageSetupError::SecureStorage(KeyError::PassphraseAuthentication))
    ));
    assert_eq!(person.restore_code().unwrap().unwrap().as_bytes(), old_code.as_bytes());
    assert_eq!(person.s3_credentials().unwrap().unwrap().as_bytes(), old_key.as_bytes());
    assert!(scoped.storage_credentials().unwrap().is_none());
    assert!(coven_storage::recorded_config(&f.directory).unwrap().is_none());
    handle.close().await.unwrap();
}

async fn assert_connected_key(f: &Fixture, id: &str, secret: &str) {
    let provider = f.connector.providers.lock().unwrap().last().unwrap().clone();
    let key = provider.s3_credentials().await.unwrap();
    assert_eq!(key.access_key_id, id);
    assert_eq!(key.secret_access_key.as_str(), secret);
}

#[tokio::test]
async fn connected_and_signed_out_updates_install_the_supplied_key_without_changing_the_code() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    let before = f.handle.restore_code().await.unwrap();
    let member = coven_sync::read_restore_code(&before).unwrap().member_keys.to_secret_bytes();
    let location = coven_storage::recorded_config(&f.directory).unwrap();
    f.handle
        .update_credentials("connected".into(), SecretText::new("connected-secret".into()))
        .await
        .unwrap();
    assert_connected_key(&f, "connected", "connected-secret").await;
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    f.handle
        .replace_access_key("published".into(), SecretText::new("published-secret".into()))
        .await
        .unwrap();
    assert_connected_key(&f, "published", "published-secret").await;
    assert_eq!(f.handle.restore_code().await.unwrap(), before);
    f.handle.close().await.unwrap();
    // The device's key is gone, as when the person deletes the keychain entry.
    f.app.lose_storage_credentials(f.directory.id()).unwrap();
    let reopen =
        || builder(&f.app, f.clock.clone(), f.connector.clone()).open(f.directory.id(), |_| ());
    let handle = reopen().await.unwrap();
    assert_eq!(
        handle.test_sync_status().latest().await.state,
        SyncState::SignedOut { provider: CloudProvider::S3, cause: None }
    );
    assert_eq!(handle.restore_code().await.unwrap(), before);
    handle
        .update_credentials("signed-out".into(), SecretText::new("signed-out-secret".into()))
        .await
        .unwrap();
    f.advance(Duration::from_secs(1));
    handle.start_sync().await.unwrap();
    synced_since(&handle, f.clock.now()).await;
    assert_connected_key(&f, "signed-out", "signed-out-secret").await;
    assert_eq!(handle.restore_code().await.unwrap(), before);
    assert_eq!(coven_storage::recorded_config(&f.directory).unwrap(), location);
    assert_eq!(
        coven_sync::read_restore_code(&before).unwrap().member_keys.to_secret_bytes().as_bytes(),
        member.as_bytes()
    );
    handle.close().await.unwrap();
    let handle = reopen().await.unwrap();
    f.advance(Duration::from_secs(1));
    handle.start_sync().await.unwrap();
    synced_since(&handle, f.clock.now()).await;
    assert_connected_key(&f, "signed-out", "signed-out-secret").await;
    assert_eq!(handle.restore_code().await.unwrap(), before);
    handle.close().await.unwrap();
}

/// A dropped replacement's caller doesn't stop it: its record publishes
/// while the connection keeps the old key, and custody connects with the
/// new one once history records it (§14.3).
#[tokio::test]
async fn dropping_a_replacement_waiter_does_not_interrupt_key_installation_or_publication() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    stop(&f).await;
    f.handle.unlock_store_key().await.unwrap();
    let before = f.handle.restore_code().await.unwrap();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage.hold_next_creation(ObjectPrefix::all(f.directory.id()), entered, resumed).await;
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
    // The record publishes with the key history records now.
    let provider = f.connector.providers.lock().unwrap().last().unwrap().clone();
    assert_eq!(provider.s3_credentials().await.unwrap().access_key_id, "member-key");
    resume.send(()).unwrap();
    f.handle.close().await.unwrap();
    let handle = builder(&f.app, f.clock.clone(), f.connector.clone())
        .open(f.directory.id(), |_| ())
        .await
        .unwrap();
    f.advance(Duration::from_secs(1));
    handle.start_sync().await.unwrap();
    synced_since(&handle, f.clock.now()).await;
    assert_connected_key(&f, "retained", "retained-secret").await;
    assert_eq!(handle.restore_code().await.unwrap(), before);
    handle.close().await.unwrap();
}

/// A key replacement whose caller stopped waiting holds the store's
/// credential owner until it finishes: stopping sync closes the connection
/// only after it, so the new key is in custody and its id recorded.
#[tokio::test]
async fn a_dropped_key_replacement_finishes_before_its_connection_closes() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    stop(&f).await;
    f.handle.unlock_store_key().await.unwrap();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage.hold_next_creation(ObjectPrefix::all(f.directory.id()), entered, resumed).await;
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
    let stopping = crate::tests::stopping(&f.handle);
    // Stopping's first write says sync isn't running; then it waits.
    f.handle.test_retry_effect().wait_for(|effect| *effect == RetryEffect::NotSyncing).await;
    assert!(!stopping.is_finished());
    resume.send(()).unwrap();
    stopping.await.unwrap().unwrap();
    assert_connected_key(&f, "retained", "retained-secret").await;
    let me = f.handle.members().await.unwrap().into_iter().find(|member| member.is_self).unwrap();
    assert_eq!(me.access, MemberAccess::S3AccessKey { access_key_id: "retained".into() });
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn custody_failure_rolls_back_credentials_before_claiming_the_creation() {
    let f = Fixture::new().await;
    f.handle.close().await.unwrap();
    let handle = builder(&f.app, f.clock.clone(), f.storage.clone())
        .key_custody(CustodyChoice::Custom(Arc::new(RefusePersist {
            keys: std::sync::Mutex::new(None),
            refuse: AtomicBool::new(true),
        })))
        .open(f.directory.id(), |_| ())
        .await
        .unwrap();
    let setup = |key: &str| {
        handle.setup_s3_storage(
            f.storage.config(),
            "Test device",
            key.into(),
            SecretText::new("secret".into()),
            |_| {},
        )
    };
    assert!(setup("original").await.is_err());
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Locked);
    assert!(handle.restore_code().await.is_err());
    assert_eq!(handle.test_sync_status().latest().await.state, SyncState::NotSynced);
    let first = ObjectPath::slot(f.directory.id(), 1.try_into().unwrap());
    assert!(f.storage.status(&first).await.unwrap().is_none());
    // Nothing was claimed, so another key sets the store up.
    setup("changed").await.unwrap();
    assert!(f.storage.status(&first).await.unwrap().is_some());
    let me = handle.members().await.unwrap().into_iter().find(|member| member.is_self).unwrap();
    assert_eq!(me.devices.len(), 1);
    handle.close().await.unwrap();
}

/// Store key custody whose forgetting panics, as a bug in an app's custody
/// would.
struct PanicsForgetting(std::sync::Mutex<Option<StoreKeyring>>);

impl Custody<StoreKeyring> for PanicsForgetting {
    fn unlock(&self) -> Result<Option<StoreKeyring>, KeyError> {
        Ok(self.0.lock().unwrap().clone())
    }
    fn persist(&self, keys: &StoreKeyring) -> Result<(), KeyError> {
        *self.0.lock().unwrap() = Some(keys.clone());
        Ok(())
    }
    fn forget(&self) -> Result<(), KeyError> {
        panic!("the key's entry is locked")
    }
    fn close(&self) {}
}

/// Forgetting the store keys panics in custody: the call fails with the
/// bug as `KeyError::Bug`, not as custody that is unavailable (E15).
#[tokio::test]
async fn a_panicking_key_forget_fails_with_its_bug() {
    let f = Fixture::new().await;
    f.handle.close().await.unwrap();
    let handle = builder(&f.app, f.clock.clone(), f.storage.clone())
        .key_custody(CustodyChoice::Custom(Arc::new(PanicsForgetting(std::sync::Mutex::new(None)))))
        .open(f.directory.id(), |_| ())
        .await
        .unwrap();
    handle
        .setup_s3_storage(
            f.storage.config(),
            "Test device",
            "member-key".into(),
            SecretText::new("secret".into()),
            |_| {},
        )
        .await
        .unwrap();
    let failed = handle.forget_store_keys().await.unwrap_err();
    let KeyError::Bug(bug) = &failed else { panic!("the call heard no bug: {failed:?}") };
    assert_eq!(bug.message, "the key's entry is locked");
    handle.close().await.unwrap();
}

/// Ben's phone is killed while it replaces its S3 key: once the new key is in
/// custody, or once its id is journaled. Reopened and synced, the phone uses
/// the key its member's record names, whichever that is: custody never holds
/// a key the history doesn't name (§14.3).
#[tokio::test]
async fn a_key_replacement_cut_at_any_step_leaves_custody_on_the_recorded_key() {
    for at in [coven_sync::ReplaceStop::Custody, coven_sync::ReplaceStop::Journal] {
        let f = Fixture::new().await;
        f.setup().await.unwrap();
        synced(&f.handle).await;
        f.handle.test_replace_crash(at);
        f.handle
            .replace_access_key("replacement".into(), SecretText::new("replacement-secret".into()))
            .await
            .unwrap_err();
        f.handle.close().await.unwrap();
        let handle = builder(&f.app, f.clock.clone(), f.connector.clone())
            .open(f.directory.id(), |_| ())
            .await
            .unwrap();
        f.advance(Duration::from_secs(1));
        handle.start_sync().await.unwrap();
        synced_since(&handle, f.clock.now()).await;
        let provider = f.connector.providers.lock().unwrap().last().unwrap().clone();
        let used = provider.s3_credentials().await.unwrap().access_key_id;
        let me = handle.members().await.unwrap().into_iter().find(|member| member.is_self).unwrap();
        assert_eq!(me.access, MemberAccess::S3AccessKey { access_key_id: used }, "{at:?}");
        handle.close().await.unwrap();
    }
}

/// Unlocking names what failed: no storage before any setup, and custody
/// that fails while unlocking; once custody answers, unlocking opens the
/// keys (E5).
#[tokio::test]
async fn unlocking_names_what_failed() {
    let f = Fixture::new().await;
    assert!(matches!(f.handle.unlock_store_key().await, Err(StoreKeyUnlockError::NoStorage)));
    f.setup().await.unwrap();
    synced(&f.handle).await;
    f.handle.forget_store_keys().await.unwrap();
    f.app.fail_next_keychain_operation();
    let refused = f.handle.unlock_store_key().await;
    assert!(matches!(refused, Err(StoreKeyUnlockError::SecureStorage(_))), "{refused:?}");
    f.handle.unlock_store_key().await.unwrap();
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Available);
    f.handle.close().await.unwrap();
}

/// Member-key custody that counts how often it is unlocked.
struct CountedIdentity {
    inner: coven_crypto::custody::KeyringCustody<coven_crypto::MemberIdentity>,
    unlocks: std::sync::atomic::AtomicUsize,
}

impl coven_crypto::custody::Custody<coven_crypto::MemberIdentity> for CountedIdentity {
    fn unlock(&self) -> Result<Option<coven_crypto::MemberIdentity>, KeyError> {
        self.unlocks.fetch_add(1, Ordering::SeqCst);
        self.inner.unlock()
    }
    fn persist(&self, keys: &coven_crypto::MemberIdentity) -> Result<(), KeyError> {
        self.inner.persist(keys)
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.inner.forget()
    }
    fn close(&self) {
        self.inner.close()
    }
}

/// Unlocking opens the store keys with the member keys held since the store
/// opened, without unlocking their custody again, and connects without
/// syncing: a retry is answered `NotSyncing` and sends nothing until sync
/// starts (E5).
#[tokio::test]
async fn unlocking_reads_no_member_key_custody_and_starts_no_sync() {
    let f = Fixture::new().await;
    f.handle.close().await.unwrap();
    let identity = Arc::new(CountedIdentity {
        inner: coven_crypto::custody::KeyringCustody::new(Arc::new(
            f.app.store_keychain(f.directory.id()),
        )),
        unlocks: Default::default(),
    });
    let handle = builder(&f.app, f.clock.clone(), f.storage.clone())
        .identity_custody(CustodyChoice::Custom(identity.clone()))
        .open(f.directory.id(), |_| ())
        .await
        .unwrap();
    let opened = identity.unlocks.load(Ordering::SeqCst);
    handle
        .setup_s3_storage(
            f.storage.config(),
            "Test device",
            "member-key".into(),
            SecretText::new("secret".into()),
            |_| {},
        )
        .await
        .unwrap();
    synced(&handle).await;
    handle.stop_sync().await.unwrap();
    handle.forget_store_keys().await.unwrap();
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Locked);

    let unlocked = handle.unlock_store_key().await.unwrap();
    assert_eq!(unlocked.key_state, StoreKeyState::Available);
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
    assert_eq!(identity.unlocks.load(Ordering::SeqCst), opened);
    assert_eq!(handle.test_retry_effect().latest().await, RetryEffect::NotSyncing);
    let requests = f.storage.request_count();
    let answer = handle.retry_now().await.unwrap().await;
    assert!(matches!(answer, RetryOutcome::NotSyncing), "{answer:?}");
    assert_eq!(f.storage.request_count(), requests, "no pass runs until sync starts");

    f.advance(Duration::from_secs(1));
    handle.start_sync().await.unwrap();
    synced_since(&handle, f.clock.now()).await;
    handle.close().await.unwrap();
}

/// The store key state is read from the handle's session alone: it reads no
/// custody, so a custody failure armed before it is still there for the next
/// call that reads custody (E5).
#[tokio::test]
async fn the_store_key_state_reads_no_custody() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    f.app.fail_next_keychain_operation();
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Available);
    assert!(f.handle.forget_store_keys().await.is_err());
    assert_eq!(f.handle.store_key_state().unwrap(), StoreKeyState::Available);
    f.handle.close().await.unwrap();
}

/// Reconnect Ana's store with the S3 key `key`, as the app does when she
/// enters another key, recording the steps setup reports.
async fn reconnect_with(
    f: &Fixture,
    key: &str,
) -> (Result<StorageSetup, StorageSetupError>, Vec<StorageSetupStep>) {
    let steps = std::sync::Mutex::new(Vec::new());
    let result = f
        .handle
        .setup_s3_storage(
            f.storage.config(),
            "Test device",
            key.into(),
            SecretText::new(format!("{key} secret")),
            |progress| steps.lock().unwrap().push(progress.step),
        )
        .await;
    let steps = steps.into_inner().unwrap();
    (result, steps)
}

/// The access key custody holds for the store's connection.
fn custody_key(f: &Fixture) -> String {
    let kept = f.app.store_keychain(f.directory.id()).storage_credentials().unwrap().unwrap();
    let StorageCredentials::S3(keys) = StorageCredentials::decode(kept.as_bytes()).unwrap() else {
        panic!("S3 credentials");
    };
    keys.access_key_id
}

/// A reconnect reaches the provider with the key it was just given, not the
/// one it holds: refused there, it changes nothing, and the store keeps its
/// connection, code and credentials; accepted, its check ran on that key
/// (E5).
#[tokio::test]
async fn a_reconnect_checks_the_provider_with_the_newly_supplied_key() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    stop(&f).await;
    let code = f.handle.restore_code().await.unwrap();
    let location = coven_storage::recorded_config(&f.directory).unwrap();

    *f.connector.failure.lock().unwrap() = Some(StorageFailure::Authentication.into());
    let (refused, _) = reconnect_with(&f, "replacement").await;
    assert!(refused.is_err(), "{refused:?}");
    assert_eq!(f.handle.restore_code().await.unwrap(), code);
    assert_eq!(coven_storage::recorded_config(&f.directory).unwrap(), location);
    assert_eq!(custody_key(&f), "member-key");

    let connections = f.connector.credentials.lock().unwrap().len();
    reconnect_with(&f, "replacement").await.0.unwrap();
    let checked = f.connector.credentials.lock().unwrap()[connections].clone();
    let StorageCredentials::S3(keys) = checked else { panic!("S3 credentials") };
    assert_eq!(keys.access_key_id, "replacement");
    assert_eq!(custody_key(&f), "replacement");
    f.handle.close().await.unwrap();
}

/// A reconnect with another key records the member's new access in a
/// `Set access` entry before it commits the key: when that claim fails, the
/// store keeps its old key and code; when it lands, the member's access is
/// the new key's id (E5).
#[tokio::test]
async fn a_reconnect_with_another_key_records_set_access_before_committing_it() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    stop(&f).await;
    let code = f.handle.restore_code().await.unwrap();
    let next = f
        .storage
        .list(&ObjectPrefix::all(f.directory.id()))
        .await
        .unwrap()
        .iter()
        .filter_map(|object| object.path.slot_number())
        .max()
        .unwrap()
        .0
        + 1;
    f.storage
        .set_faults(Faults {
            create_failures: [(
                ObjectPath::slot(f.directory.id(), next.try_into().unwrap()),
                (StorageFailure::PermissionDenied, 403),
            )]
            .into(),
            ..Faults::none()
        })
        .await;
    let (failed, steps) = reconnect_with(&f, "new-key").await;
    assert!(failed.is_err(), "{failed:?}");
    assert!(steps.contains(&StorageSetupStep::RecordingAccess), "{steps:?}");
    assert_eq!(custody_key(&f), "member-key");
    assert_eq!(f.handle.restore_code().await.unwrap(), code);

    // Past its point of no return, the setup finishes once sync starts and
    // the app retries the refused entry.
    f.storage.set_faults(Faults::none()).await;
    let pending = f.handle.pending().await.unwrap();
    let set_access = |subject: &coven_sync::PendingSubject| {
        matches!(
            subject,
            coven_sync::PendingSubject::Operation { kind: OperationKind::SetAccess, .. }
        )
    };
    let Some(coven_sync::PendingSubject::Operation { id, .. }) =
        pending.iter().map(|record| record.subject.clone()).find(set_access)
    else {
        panic!("the Set access entry waits: {pending:?}");
    };
    f.advance(Duration::from_secs(1));
    f.handle.start_sync().await.unwrap();
    f.handle.retry_pending_operation(id).await.unwrap();
    synced_since(&f.handle, f.clock.now()).await;
    let members = f.handle.members().await.unwrap();
    let ana = members.iter().find(|member| member.is_self).unwrap();
    assert_eq!(
        ana.access,
        coven_format::MemberAccess::S3AccessKey { access_key_id: "new-key".into() }
    );
    assert_eq!(custody_key(&f), "new-key");
    f.handle.close().await.unwrap();
}
