use super::*;
use crate::builder::Continuing;
use coven_crypto::custody::KeyringCustody;
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
impl Custody<MemberIdentity> for RefusedIdentity {
    fn unlock(&self) -> Result<Option<MemberIdentity>, KeyError> {
        Ok(None)
    }
    fn persist(&self, _: &MemberIdentity) -> Result<(), KeyError> {
        if let Some(path) = &self.0 {
            std::fs::remove_file(path).unwrap();
            std::fs::create_dir(path).unwrap();
        }
        Err(KeyError::Unsupported)
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
    app: &Coven,
    id: StoreId,
    keychain: Arc<Keychain>,
    keys: CustodyChoice<StoreKeyring>,
    identity: CustodyChoice<MemberIdentity>,
) -> (BootstrapStore, OpeningOwners, Database) {
    prepared_continuing(app, id, keychain, keys, identity, Continuing::Fresh).await
}

async fn prepared_continuing(
    app: &Coven,
    id: StoreId,
    keychain: Arc<Keychain>,
    keys: CustodyChoice<StoreKeyring>,
    identity: CustodyChoice<MemberIdentity>,
    continuing: Continuing,
) -> (BootstrapStore, OpeningOwners, Database) {
    let pending = app.directories().begin_bootstrap(id, &UuidIds).await.unwrap();
    let builder = app
        .builder()
        .synced_tables(Vec::new())
        .migrations(Vec::new())
        .key_custody(keys)
        .identity_custody(identity);
    let database = builder.database(pending.directory()).unwrap().open().await.unwrap();
    database.initialize_store_name("Household".into()).await.unwrap();
    let owners = builder
        .owners(pending.directory(), Arc::new(StoreKeychain::new(keychain, id)), continuing)
        .unwrap();
    (pending, owners, database)
}

/// The owners a later call makes for the same installation, with these
/// custody choices.
fn owners_again(
    app: &Coven,
    pending: &BootstrapStore,
    keychain: Arc<Keychain>,
    keys: CustodyChoice<StoreKeyring>,
    identity: CustodyChoice<MemberIdentity>,
) -> OpeningOwners {
    let id = pending.directory().id();
    app.builder()
        .synced_tables(Vec::new())
        .migrations(Vec::new())
        .key_custody(keys)
        .identity_custody(identity)
        .owners(pending.directory(), Arc::new(StoreKeychain::new(keychain, id)), Continuing::Fresh)
        .unwrap()
}

/// The custody a whole commit of `code` leaves for the store, beside the
/// installation's `device`: its location, keys, identity, device id and
/// credentials, and the person's restore code and S3 key.
fn committed(
    keychain: &Arc<Keychain>,
    pending: &BootstrapStore,
    code: &RestoreCode,
    ring: &StoreKeyring,
    device: DeviceId,
) {
    let id = pending.directory().id();
    let scoped = Arc::new(StoreKeychain::new(keychain.clone(), id));
    let person = PersonEntries::new(keychain.clone(), id);
    let location = coven_storage::RestoreStorage::decode(code.storage.as_bytes()).unwrap().location;
    assert_eq!(coven_storage::recorded_config(&pending.directory()).unwrap(), Some(location));
    assert_eq!(
        Custody::<StoreKeyring>::unlock(&KeyringCustody::new(scoped.clone()))
            .unwrap()
            .unwrap()
            .to_secret_bytes()
            .as_bytes(),
        ring.to_secret_bytes().as_bytes()
    );
    let identity =
        Custody::<MemberIdentity>::unlock(&KeyringCustody::new(scoped.clone())).unwrap().unwrap();
    assert_eq!(identity.member(), code.member);
    assert_eq!(scoped.device_id().unwrap(), Some(device));
    assert_eq!(
        scoped.storage_credentials().unwrap().unwrap().as_bytes(),
        credentials().encode().unwrap().as_bytes()
    );
    assert_eq!(
        person.s3_credentials().unwrap().unwrap().as_bytes(),
        s3_key().encode().unwrap().as_bytes()
    );
    let (saved, _) =
        coven_sync::read_saved_code(id, person.restore_code().unwrap().unwrap().as_bytes())
            .unwrap();
    assert_eq!(saved.code.member, code.member);
}

/// The member's keys can't be kept, after the location and the store keys
/// were written. The commit runs past its call's point of no return, so it
/// keeps what it wrote and publishes nothing; the next call commits it all
/// again and publishes (§13.4, §11.6).
#[tokio::test]
async fn a_failed_custody_step_keeps_what_it_wrote_and_the_next_commit_finishes() {
    let root = tempfile::tempdir().unwrap();
    let id = StoreId(UuidIds.new_id());
    let keychain = Keychain::in_memory("commit-test").unwrap();
    let app = Coven::with_keychain(root.path().into(), keychain.clone());
    let scoped = Arc::new(StoreKeychain::new(keychain.clone(), id));
    let old = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
    KeyringCustody::<StoreKeyring>::new(scoped.clone()).persist(&old).unwrap();
    let next = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
    let (pending, owners, database) = prepared(
        &app,
        id,
        keychain.clone(),
        CustodyChoice::Keyring,
        CustodyChoice::Custom(Arc::new(RefusedIdentity(None))),
    )
    .await;
    let code = code(id);
    let error = commit(
        &pending.directory(),
        &code,
        &household(),
        &credentials(),
        &SignInCustody::None,
        &next,
        &owners,
        &pending,
    )
    .unwrap_err();
    assert!(matches!(error, BootstrapError::SecureStorage(KeyError::Unsupported)), "{error:?}");
    assert_eq!(
        Custody::<StoreKeyring>::unlock(&KeyringCustody::new(scoped.clone()))
            .unwrap()
            .unwrap()
            .to_secret_bytes()
            .as_bytes(),
        next.to_secret_bytes().as_bytes()
    );
    assert!(scoped.device_id().unwrap().is_none());
    assert!(app.stores().await.unwrap().is_empty());
    let again = owners_again(
        &app,
        &pending,
        keychain.clone(),
        CustodyChoice::Keyring,
        CustodyChoice::Keyring,
    );
    let published = commit(
        &pending.directory(),
        &code,
        &household(),
        &credentials(),
        &SignInCustody::None,
        &next,
        &again,
        &pending,
    );
    assert!(matches!(published, Ok(None)), "{published:?}");
    committed(&keychain, &pending, &code, &next, again.device);
    assert_eq!(app.stores().await.unwrap().len(), 1);
    database.close().await.unwrap();
}

/// Publication fails after every custody value was written. The values
/// stay, so the store's custody is whole when the call that resumes the
/// installation publishes it; what the device held before is not put back
/// (§13.4).
#[tokio::test]
async fn a_failed_publication_keeps_the_committed_custody() {
    let root = tempfile::tempdir().unwrap();
    let id = StoreId(UuidIds.new_id());
    let keychain = Keychain::in_memory("commit-test").unwrap();
    let app = Coven::with_keychain(root.path().into(), keychain.clone());
    let scoped = Arc::new(StoreKeychain::new(keychain.clone(), id));
    let person = PersonEntries::new(keychain.clone(), id);
    let (pending, owners, database) =
        prepared(&app, id, keychain.clone(), CustodyChoice::Keyring, CustodyChoice::Keyring).await;
    scoped
        .set_storage_credentials(&SecretBytes::new(b"device credentials before".to_vec()))
        .unwrap();
    person.set_s3_credentials(&SecretBytes::new(b"saved S3 key before".to_vec())).unwrap();
    person.set_restore_code(&SecretBytes::new(b"code before".to_vec())).unwrap();
    let marker = root.path().join("stores").join(id.to_string()).join(".coven-bootstrap");
    std::fs::remove_file(&marker).unwrap();
    std::fs::create_dir(&marker).unwrap();
    let code = code(id);
    let ring = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
    let error = commit(
        &pending.directory(),
        &code,
        &household(),
        &credentials(),
        &SignInCustody::None,
        &ring,
        &owners,
        &pending,
    )
    .unwrap_err();
    assert!(matches!(error, BootstrapError::Directory(BootstrapDirectoryError::File(_))));
    committed(&keychain, &pending, &code, &ring, owners.device);
    assert!(app.stores().await.unwrap().is_empty());
    database.close().await.unwrap();
}

/// The keychain refuses one of the commit's operations, whichever it is.
/// The commit keeps what it wrote, and run again it finishes, leaving the
/// same custody as a commit nothing interrupted (§13.4).
#[tokio::test]
async fn a_commit_refused_at_any_keychain_operation_finishes_when_run_again() {
    for after in 0..9 {
        let root = tempfile::tempdir().unwrap();
        let id = StoreId(UuidIds.new_id());
        let keychain = Keychain::in_memory("commit-test").unwrap();
        let app = Coven::with_keychain(root.path().into(), keychain.clone());
        let scoped = StoreKeychain::new(keychain.clone(), id);
        let person = PersonEntries::new(keychain.clone(), id);
        scoped
            .set_storage_credentials(&SecretBytes::new(b"old device credentials".to_vec()))
            .unwrap();
        person.set_s3_credentials(&SecretBytes::new(b"old saved S3 key".to_vec())).unwrap();
        person.set_restore_code(&SecretBytes::new(b"old saved code".to_vec())).unwrap();
        let (pending, owners, database) =
            prepared(&app, id, keychain.clone(), CustodyChoice::Keyring, CustodyChoice::Keyring)
                .await;
        let code = code(id);
        let ring = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
        let run = || {
            commit(
                &pending.directory(),
                &code,
                &household(),
                &credentials(),
                &SignInCustody::None,
                &ring,
                &owners,
                &pending,
            )
        };
        keychain.fail_operations_after(&[after]);
        let error = run().unwrap_err();
        assert!(
            matches!(
                error,
                BootstrapError::SecureStorage(_)
                    | BootstrapError::Sync(SyncError::SecureStorage(_))
            ),
            "operation {after}: {error}"
        );
        assert!(app.stores().await.unwrap().is_empty());
        keychain.fail_operations_after(&[]);
        assert!(matches!(run(), Ok(None)), "operation {after}");
        committed(&keychain, &pending, &code, &ring, owners.device);
        assert_eq!(app.stores().await.unwrap().len(), 1);
        database.close().await.unwrap();
    }
}

#[tokio::test]
async fn cleanup_failure_returns_the_open_handle_with_its_session_keys() {
    let root = tempfile::tempdir().unwrap();
    let id = StoreId(UuidIds.new_id());
    let keychain = Keychain::in_memory("commit-test").unwrap();
    let app = Coven::with_keychain(root.path().into(), keychain.clone());
    let code = code(id);
    let key = KeyId(UuidIds.new_id());
    let ring = StoreKeyring::new(StoreKey::generate(key).unwrap());
    let (pending, owners, database) =
        prepared(&app, id, keychain, CustodyChoice::InMemory, CustodyChoice::InMemory).await;
    let leftover = root.path().join("stores").join(id.to_string()).join("bootstrap.sealed");
    std::fs::create_dir(leftover).unwrap();
    let location = coven_storage::RestoreStorage::decode(code.storage.as_bytes()).unwrap().location;
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
    assert_eq!(app.stores().await.unwrap().len(), 1);
    assert_eq!(coven_storage::recorded_config(&pending.directory()).unwrap(), Some(location));
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
    handle.read(|_| Ok::<_, CovenError>(())).await.unwrap();
    handle.close().await.unwrap();
}

#[tokio::test]
async fn publication_returns_the_handle_without_rereading_custody() {
    let root = tempfile::tempdir().unwrap();
    let id = StoreId(UuidIds.new_id());
    let keychain = Keychain::in_memory("commit-test").unwrap();
    let app = Coven::with_keychain(root.path().into(), keychain.clone());
    let (pending, owners, database) =
        prepared(&app, id, keychain.clone(), CustodyChoice::InMemory, CustodyChoice::InMemory)
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
    assert_eq!(app.stores().await.unwrap().len(), 1);
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
    keychain.fail_operations_after(&[]);
    handle.close().await.unwrap();
}

#[tokio::test]
async fn publication_with_a_loaded_provider_does_not_suspend() {
    use std::{future::Future, task::Poll};
    let root = tempfile::tempdir().unwrap();
    let keychain = Keychain::in_memory("commit-test").unwrap();
    let app = Coven::with_keychain(root.path().into(), keychain.clone());
    let id = StoreId(UuidIds.new_id());
    let provider = coven_storage::test_utils::MemoryStorage::builder().build().unwrap();
    let continuing = Continuing::Loaded {
        state: Arc::new(coven_sync::SequenceState::new()),
        storage: provider.shared_connection(),
        credentials: None,
    };
    let (pending, owners, database) = prepared_continuing(
        &app,
        id,
        keychain,
        CustodyChoice::InMemory,
        CustodyChoice::InMemory,
        continuing,
    )
    .await;
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
    assert_eq!(app.stores().await.unwrap().len(), 1);
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
    handle.close().await.unwrap();
}

/// The name the fixture store was created with, set by slot 1.
fn household() -> coven_sync::SavedName {
    coven_sync::SavedName { name: "Household".into(), named_at: coven_merge::Slot(1) }
}

/// Identity custody that panics as it persists, as a bug in an app's
/// custody would.
struct PanickingIdentity;
impl Custody<MemberIdentity> for PanickingIdentity {
    fn unlock(&self) -> Result<Option<MemberIdentity>, KeyError> {
        Ok(None)
    }
    fn persist(&self, _: &MemberIdentity) -> Result<(), KeyError> {
        panic!("the identity's entry is half written")
    }
    fn forget(&self) -> Result<(), KeyError> {
        Ok(())
    }
    fn close(&self) {}
}

/// A custody step that panics fails the commit with the bug, and what the
/// commit wrote before it stays, as after any failure before publication
/// (§13.4, E15).
#[tokio::test]
async fn a_panicking_custody_step_keeps_what_the_commit_wrote() {
    let root = tempfile::tempdir().unwrap();
    let id = StoreId(UuidIds.new_id());
    let keychain = Keychain::in_memory("commit-test").unwrap();
    let app = Coven::with_keychain(root.path().into(), keychain.clone());
    let scoped = Arc::new(StoreKeychain::new(keychain.clone(), id));
    let (pending, owners, database) = prepared(
        &app,
        id,
        keychain,
        CustodyChoice::Keyring,
        CustodyChoice::Custom(Arc::new(PanickingIdentity)),
    )
    .await;
    let next = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
    let error = publish_bootstrap(
        &pending,
        code(id),
        household(),
        credentials(),
        SignInCustody::None,
        next.clone(),
        owners,
        database.clone(),
        waits(&database).await,
    )
    .await
    .unwrap_err();
    let BootstrapError::Bug(bug) = &error else { panic!("{error:?}") };
    assert_eq!(bug.message, "the identity's entry is half written");
    assert_eq!(
        Custody::<StoreKeyring>::unlock(&KeyringCustody::new(scoped.clone()))
            .unwrap()
            .unwrap()
            .to_secret_bytes()
            .as_bytes(),
        next.to_secret_bytes().as_bytes()
    );
    assert!(coven_storage::recorded_config(&pending.directory()).unwrap().is_some());
    assert!(scoped.device_id().unwrap().is_none());
    assert!(app.stores().await.unwrap().is_empty());
    database.close().await.unwrap();
}

/// Publishes with the bootstrap's own step, then panics where `after` says:
/// before the marker goes, or after.
struct PanickingPublication<'a> {
    pending: &'a BootstrapStore,
    after: bool,
}
impl Publish for PanickingPublication<'_> {
    fn publish(&self) -> Result<(), BootstrapDirectoryError> {
        if self.after {
            self.pending.publish()?;
        }
        panic!("the marker's removal is lost")
    }
    fn published(&self) -> Result<bool, coven_foundation::files::FileError> {
        self.pending.is_published()
    }
}

/// Publication panics. Whether the marker went says how far it got: a
/// published store reports the bug as the failure after publication, and an
/// unpublished one fails with it. Either keeps its custody (E15, §13.4).
#[tokio::test]
async fn a_panicking_publication_settles_by_the_marker() {
    for after in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let id = StoreId(UuidIds.new_id());
        let keychain = Keychain::in_memory("commit-test").unwrap();
        let app = Coven::with_keychain(root.path().into(), keychain.clone());
        let (pending, owners, database) =
            prepared(&app, id, keychain.clone(), CustodyChoice::Keyring, CustodyChoice::Keyring)
                .await;
        let ring = StoreKeyring::new(StoreKey::generate(KeyId(UuidIds.new_id())).unwrap());
        let code = code(id);
        let committing = commit(
            &pending.directory(),
            &code,
            &household(),
            &credentials(),
            &SignInCustody::None,
            &ring,
            &owners,
            &PanickingPublication { pending: &pending, after },
        );
        match committing {
            Ok(Some(BootstrapError::Bug(bug))) if after => {
                assert_eq!(bug.message, "the marker's removal is lost");
            }
            Err(BootstrapError::Bug(bug)) if !after => {
                assert_eq!(bug.message, "the marker's removal is lost");
            }
            other => panic!("after {after}: {other:?}"),
        }
        committed(&keychain, &pending, &code, &ring, owners.device);
        assert_eq!(app.stores().await.unwrap().len(), usize::from(after));
        database.close().await.unwrap();
    }
}
