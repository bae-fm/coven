use crate::*;
use coven_foundation::clock::HANG_GUARD;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

fn builder(app: &Coven) -> CovenBuilder {
    app.builder().synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)]).migrations(
        vec![Migration::addition(
            1,
            "notes",
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL)",
        )],
    )
}

#[tokio::test]
async fn reopening_waits_after_the_last_handle_drops_or_close_starts() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    for close in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let app = Coven::in_memory(root.path().into());
        let directory = app.create_closed_store("Reopen", Arc::new(UuidIds)).await.unwrap();
        let handle = builder(&app).open(directory.id(), |_| ()).await.unwrap();
        let clone = handle.clone();
        drop(handle);
        assert!(matches!(
            builder(&app).open(directory.id(), |_| ()).await,
            Err(CovenError::Lock(StoreLockError::AlreadyOpen(_)))
        ));
        let (entered, entry) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let mut read = Box::pin(clone.read(move |_| {
            entered.send(()).unwrap();
            released.recv_timeout(HANG_GUARD).unwrap();
            Ok::<_, CovenError>(())
        }));
        tokio::select! {
            result = &mut read => panic!("held read returned: {result:?}"),
            result = entry => result.unwrap(),
        }
        drop(read);
        if close {
            let mut closing = Box::pin(clone.close());
            poll_fn(|cx| {
                assert!(closing.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(closing);
        }
        drop(clone);
        let mut reopen = Box::pin(builder(&app).open(directory.id(), |_| ()));
        poll_fn(|cx| {
            assert!(reopen.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        release.send(()).unwrap();
        let reopened = reopen.await.unwrap();
        reopened.read(|_| Ok::<_, CovenError>(())).await.unwrap();
        reopened.close().await.unwrap();
    }
}

#[tokio::test]
async fn kept_keys_survive_reopen_and_forgetting_preserves_identity() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    let ids = Arc::new(UuidIds);
    let directory = app.create_closed_store("keys", ids.clone()).await.unwrap();
    let keys = StoreKeyring::new(StoreKey::generate(KeyId(ids.new_id())).unwrap());
    app.keep_store_keys(&directory, &keys).unwrap();
    let handle = builder(&app).open(directory.id(), |_| ()).await.unwrap();
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);

    handle.set_host_secret("token", "secret").unwrap();
    handle.close().await.unwrap();
    let handle = builder(&app).open(directory.id(), |_| ()).await.unwrap();
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
    app.fail_next_keychain_operation();
    assert!(handle.forget_store_keys().await.is_err());
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
    handle.forget_store_keys().await.unwrap();
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Locked);
    assert!(Custody::<MemberIdentity>::unlock(&coven_crypto::custody::KeyringCustody::new(
        Arc::new(app.store_keychain(directory.id()))
    ))
    .unwrap()
    .is_some());
    assert_eq!(handle.host_secret("token").unwrap().as_deref(), Some("secret"));
    handle.close().await.unwrap();
    app.keep_store_keys(&directory, &keys).unwrap();
    app.discard_store(&directory).await.unwrap();
    // Deletion removes custody even when no directory remains to open.
    app.fail_next_keychain_operation();
    assert!(app.discard_store(&directory).await.is_err());
    app.discard_store(&directory).await.unwrap();
}

#[derive(Default)]
struct Keys {
    keys: Mutex<Option<StoreKeyring>>,
    unlocks: AtomicUsize,
    closes: AtomicUsize,
    refuse: AtomicBool,
}
impl Custody<StoreKeyring> for Keys {
    fn unlock(&self) -> Result<Option<StoreKeyring>, KeyError> {
        self.unlocks.fetch_add(1, Ordering::SeqCst);
        if self.refuse.load(Ordering::SeqCst) {
            return Err(KeyError::PassphraseAuthentication);
        }
        Ok(self.keys.lock().unwrap().clone())
    }
    fn persist(&self, keys: &StoreKeyring) -> Result<(), KeyError> {
        *self.keys.lock().unwrap() = Some(keys.clone());
        Ok(())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.keys.lock().unwrap().take();
        Ok(())
    }
    fn close(&self) {
        self.closes.fetch_add(1, Ordering::SeqCst);
        self.keys.lock().unwrap().take();
    }
}

#[derive(Default)]
struct Identity {
    keys: Mutex<Option<MemberIdentity>>,
    unlocks: AtomicUsize,
    closes: AtomicUsize,
    refuse: AtomicBool,
}
impl Custody<MemberIdentity> for Identity {
    fn unlock(&self) -> Result<Option<MemberIdentity>, KeyError> {
        self.unlocks.fetch_add(1, Ordering::SeqCst);
        if self.refuse.load(Ordering::SeqCst) {
            return Err(KeyError::PassphraseAuthentication);
        }
        Ok(self.keys.lock().unwrap().clone())
    }
    fn persist(&self, keys: &MemberIdentity) -> Result<(), KeyError> {
        *self.keys.lock().unwrap() = Some(keys.clone());
        Ok(())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.keys.lock().unwrap().take();
        Ok(())
    }
    fn close(&self) {
        self.closes.fetch_add(1, Ordering::SeqCst);
        self.keys.lock().unwrap().take();
    }
}

#[tokio::test]
async fn creation_unlocks_once_and_sql_callback_failures_roll_back() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    let keys = Arc::new(Keys::default());
    let identity = Arc::new(Identity::default());
    let handle = builder(&app)
        .key_custody(CustodyChoice::Custom(keys.clone()))
        .identity_custody(CustodyChoice::Custom(identity.clone()))
        .create_store("session")
        .await
        .unwrap();
    let directory = app.store_dir(&handle.store_id());
    let failed: CovenResult<()> = handle
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('a','not committed')", [])?;
            Err(KeyError::PassphraseAuthentication.into())
        })
        .await;
    assert!(matches!(failed, Err(CovenError::Key(KeyError::PassphraseAuthentication))));
    assert_eq!(
        handle
            .read(|sql| Ok::<_, CovenError>(sql.query_row(
                "SELECT count(*) FROM notes",
                [],
                |r| r.get::<_, i64>(0)
            )?))
            .await
            .unwrap(),
        0
    );
    assert!(handle.pending().await.unwrap().is_empty());
    let reader = builder(&app)
        .key_custody(CustodyChoice::Custom(keys.clone()))
        .identity_custody(CustodyChoice::Custom(identity.clone()))
        .open_read_only(directory.id())
        .await
        .unwrap();
    reader.read(|_| Ok::<_, CovenError>(())).await.unwrap();
    reader.close().await.unwrap();
    assert!(matches!(
        reader.read(|_| Ok(())).await,
        Err(CovenError::Database(DbError::StoreClosed))
    ));
    assert_eq!(keys.unlocks.load(Ordering::SeqCst), 1);
    assert_eq!(identity.unlocks.load(Ordering::SeqCst), 1);
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Locked);
    assert_eq!(keys.unlocks.load(Ordering::SeqCst), 1);
    let failed: CovenResult<()> = handle
        .test_staged_write(
            |_| Err::<(), _>(KeyError::PassphraseHeader.into()),
            |_, ()| panic!("failed build cannot run SQL"),
        )
        .await;
    assert!(matches!(failed, Err(CovenError::Key(KeyError::PassphraseHeader))));
    handle.close().await.unwrap();
}

#[tokio::test]
async fn cancelling_the_close_caller_still_closes_all_clones() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    let directory = app.create_closed_store("closing", Arc::new(UuidIds)).await.unwrap();
    let handle = builder(&app).open(directory.id(), |_| ()).await.unwrap();
    let mut live = handle.subscribe(|_| Ok(()));
    live.next().await.expect("the live query answers").unwrap();
    // Polling starts closing; dropping only the caller must not cancel it.
    {
        let close = handle.close();
        tokio::pin!(close);
        poll_fn(|cx| {
            assert!(close.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    assert!(matches!(
        tokio::time::timeout(HANG_GUARD, live.next())
            .await
            .unwrap()
            .expect("the live query answers"),
        Err(CovenError::Database(DbError::StoreClosed))
    ));
    assert!(matches!(
        handle.read(|_| Ok(())).await,
        Err(CovenError::Database(DbError::StoreClosed))
    ));
    assert!(matches!(handle.host_secret("token"), Err(KeyError::StoreClosed)));
    let reopened = builder(&app).open(directory.id(), |_| ()).await.unwrap();
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn closed_storage_calls_keep_their_public_errors_on_every_clone() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    let directory = app.create_closed_store("closed", Arc::new(UuidIds)).await.unwrap();
    let original = builder(&app).open(directory.id(), |_| ()).await.unwrap();
    let handle = original.clone();
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Locked);
    original.close().await.unwrap();
    assert!(matches!(handle.store_key_state(), Err(KeyError::StoreClosed)));
    assert!(matches!(
        handle
            .setup_s3_storage(
                StorageConfig::S3 {
                    bucket: "bucket".into(),
                    region: "region".into(),
                    endpoint: None,
                    profile: None,
                },
                "Device",
                "member".into(),
                SecretText::new("secret".into()),
                |_| {}
            )
            .await,
        Err(StorageSetupError::SecureStorage(KeyError::StoreClosed))
    ));
    assert!(matches!(
        handle
            .setup_oauth_storage(
                StorageConfig::Dropbox {
                    account_label: None,
                    location: coven_storage::DropboxLocation::SharedFolder {
                        namespace_id: "folder".into()
                    },
                    folder_label: None
                },
                "Device",
                |_| {}
            )
            .await,
        Err(StorageSetupError::SecureStorage(KeyError::StoreClosed))
    ));
    assert!(matches!(
        handle.setup_cloudkit_storage("container", "Device", |_| {}).await,
        Err(StorageSetupError::SecureStorage(KeyError::StoreClosed))
    ));
    assert!(matches!(
        handle
            .authenticate(coven_storage::OAuthAccess::Dropbox(
                coven_storage::DropboxAccess::FullDropbox
            ))
            .await,
        Err(StorageSetupError::SecureStorage(KeyError::StoreClosed))
    ));
    assert!(matches!(
        handle.unlock_store_key().await,
        Err(StoreKeyUnlockError::SecureStorage(KeyError::StoreClosed))
    ));
    assert!(matches!(handle.forget_store_keys().await, Err(KeyError::StoreClosed)));
}

/// Members and circles come from the committed replay: a handle with no
/// storage, connection or unlocked key reads them without the operation
/// worker, and each subscription starts with the value its call returns. A
/// store not yet created in storage has no members (E4, E9, E12).
#[tokio::test]
async fn disconnected_member_and_circle_reads_match_their_subscriptions() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    let directory = app.create_closed_store("members", Arc::new(UuidIds)).await.unwrap();
    let handle = builder(&app).open(directory.id(), |_| ()).await.unwrap();
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Locked);
    let members = handle.members().await.unwrap();
    assert!(members.is_empty());
    assert_eq!(
        handle.subscribe_members().next().await.expect("the live query answers").unwrap(),
        members
    );
    assert!(handle.circles().list().await.unwrap().is_empty());
    assert!(handle
        .circles()
        .subscribe_list()
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        .is_empty());
    handle.close().await.unwrap();
    assert!(matches!(handle.members().await, Err(CovenError::Database(DbError::StoreClosed))));
}

#[tokio::test]
async fn closing_waits_for_setup_after_its_caller_is_cancelled() {
    tokio::time::timeout(HANG_GUARD, async {
        use coven_storage::test_utils::MemoryStorage;
        let root = tempfile::tempdir().unwrap();
        let app = Coven::in_memory(root.path().to_owned());
        let directory = app.create_closed_store("setup", Arc::new(UuidIds)).await.unwrap();
        let config = StorageConfig::S3 {
            bucket: "bucket".into(), region: "region".into(), endpoint: None, profile: None,
        };
        let storage = Arc::new(MemoryStorage::builder().location(config.clone()).clock(Arc::new(SystemClock)).build().unwrap());
        let handle = builder(&app).storage_connector(storage.clone()).open(directory.id(), |_| ()).await.unwrap();

        let (entered, entering) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        storage.hold_next_listing(ObjectPrefix::all(directory.id()), entered, resumed).await;
        {
            let setup = handle.setup_s3_storage(config.clone(), "Device", "member".into(), SecretText::new("secret".into()), |_| {});
            tokio::pin!(setup);
            tokio::select! {
                result = &mut setup => panic!("setup finished while its provider check was held: {result:?}"),
                result = entering => result.unwrap(),
            }
        }
        let waiting = handle.test_call_waits();
        let close = handle.close();
        tokio::pin!(close);
        tokio::select! {
            result = &mut close => panic!("close finished before setup: {result:?}"),
            () = waiting => {},
        }
        resume.send(()).unwrap();
        close.await.unwrap();
        assert_eq!(coven_storage::recorded_config(&directory).unwrap(), Some(config));
        // Creation claimed slot 1 and the creator's key copy at slot 2.
        assert_eq!(crate::tests::stored(&storage, directory.id(), |path| path.slot_number().is_some()).await.len(), 2);
        assert!(matches!(handle.store_key_state(), Err(KeyError::StoreClosed)));
    }).await.expect("cancelled setup finishes before close");
}

#[tokio::test]
async fn workers_share_keys_across_stop_forget_and_reacquisition_until_close() {
    use coven_storage::{test_utils::MemoryStorage, Storage};
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let connector = crate::tests::Connector::new(storage.clone());
    let keys = Arc::new(Keys::default());
    let identity = Arc::new(Identity::default());
    let handle = builder(&app)
        .key_custody(CustodyChoice::Custom(keys.clone()))
        .identity_custody(CustodyChoice::Custom(identity.clone()))
        .storage_connector(connector.clone())
        .create_store("session")
        .await
        .unwrap();
    let member = identity.keys.lock().unwrap().as_ref().unwrap().member();
    handle
        .setup_s3_storage(
            storage.config(),
            "Owner",
            "owner-key".into(),
            SecretText::new("secret".into()),
            |_| {},
        )
        .await
        .unwrap();
    let clone = handle.clone();
    let circle = crate::tests::circle(&clone, "Shared").await;
    assert!(keys.keys.lock().unwrap().as_ref().unwrap().circle_key_ids(circle).next().is_some());
    let code = handle.restore_code().await.unwrap();
    for _ in 0..2 {
        // Stopping releases the provider's client; the keys stay.
        connector.stop(&handle).await;
        assert_eq!(clone.store_key_state().unwrap(), StoreKeyState::Available);
        assert_eq!(handle.restore_code().await.unwrap(), code);
        handle.start_sync().await.unwrap();
    }
    handle.forget_store_keys().await.unwrap();
    assert!(keys.keys.lock().unwrap().is_none());
    assert_eq!(clone.store_key_state().unwrap(), StoreKeyState::Locked);
    assert_eq!(identity.keys.lock().unwrap().as_ref().unwrap().member(), member);
    assert_eq!(handle.restore_code().await.unwrap(), code);
    handle.unlock_store_key().await.unwrap();
    assert_eq!(clone.store_key_state().unwrap(), StoreKeyState::Available);
    assert!(keys.keys.lock().unwrap().as_ref().unwrap().circle_key_ids(circle).next().is_some());
    assert_eq!(keys.unlocks.load(Ordering::SeqCst), 1);
    assert_eq!(identity.unlocks.load(Ordering::SeqCst), 1);
    handle.close().await.unwrap();
    assert!(keys.keys.lock().unwrap().is_none());
    assert!(identity.keys.lock().unwrap().is_none());
    assert_eq!(keys.closes.load(Ordering::SeqCst), 1);
    assert_eq!(identity.closes.load(Ordering::SeqCst), 1);
    assert!(matches!(clone.store_key_state(), Err(KeyError::StoreClosed)));
    assert!(matches!(clone.close().await, Err(SyncError::Database(DbError::StoreClosed))));
    assert_eq!(keys.closes.load(Ordering::SeqCst), 1);
    assert_eq!(identity.closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_identity_unlock_releases_the_opened_store_session() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let directory = app.create_closed_store("session", Arc::new(UuidIds)).await.unwrap();
    let keys = Arc::new(Keys::default());
    let identity = Arc::new(Identity::default());
    identity.refuse.store(true, Ordering::SeqCst);
    assert!(matches!(
        builder(&app)
            .key_custody(CustodyChoice::Custom(keys.clone()))
            .identity_custody(CustodyChoice::Custom(identity.clone()))
            .open(directory.id(), |_| ())
            .await,
        Err(CovenError::Key(KeyError::PassphraseAuthentication))
    ));
    assert_eq!(keys.unlocks.load(Ordering::SeqCst), 1);
    assert_eq!(identity.unlocks.load(Ordering::SeqCst), 1);
    assert_eq!(keys.closes.load(Ordering::SeqCst), 1);
    assert_eq!(identity.closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn migration_failure_precedes_custody_unlock() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let directory = app.create_closed_store("session", Arc::new(UuidIds)).await.unwrap();
    let keys = Arc::new(Keys::default());
    let identity = Arc::new(Identity::default());
    keys.refuse.store(true, Ordering::SeqCst);
    let error = builder(&app)
        .migrations(vec![Migration::addition(1, "invalid", "INVALID SQL")])
        .key_custody(CustodyChoice::Custom(keys.clone()))
        .identity_custody(CustodyChoice::Custom(identity.clone()))
        .open(directory.id(), |_| ())
        .await
        .unwrap_err();
    assert!(matches!(error, CovenError::Migration(_)), "{error:?}");
    assert_eq!(keys.unlocks.load(Ordering::SeqCst), 0);
    assert_eq!(identity.unlocks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn closing_erases_custody_once_and_fails_later_calls_on_every_clone() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let ids = Arc::new(UuidIds);
    let keys = Arc::new(Keys {
        keys: Mutex::new(Some(StoreKeyring::new(StoreKey::generate(KeyId(ids.new_id())).unwrap()))),
        ..Keys::default()
    });
    let identity = Arc::new(Identity::default());
    let handle = builder(&app)
        .key_custody(CustodyChoice::Custom(keys.clone()))
        .identity_custody(CustodyChoice::Custom(identity.clone()))
        .create_store("session")
        .await
        .unwrap();
    let clone = handle.clone();
    // Without storage there is no loop to stop; stopping changes nothing.
    handle.stop_sync().await.unwrap();
    handle.close().await.unwrap();
    assert_eq!(keys.closes.load(Ordering::SeqCst), 1);
    assert_eq!(identity.closes.load(Ordering::SeqCst), 1);
    assert!(keys.keys.lock().unwrap().is_none());
    assert!(identity.keys.lock().unwrap().is_none());
    assert!(matches!(
        clone.read(|_| Ok(())).await,
        Err(CovenError::Database(DbError::StoreClosed))
    ));
    assert!(matches!(clone.store_key_state(), Err(KeyError::StoreClosed)));
}

#[test]
fn row_keys_preserve_column_order_and_sql_storage_classes() {
    use types::Value;
    assert_eq!(RowKey::from("track").values(), &[Value::Text("track".into())]);
    assert_eq!(
        RowKey::from(("album".to_owned(), 7_i64)).values(),
        &[Value::Text("album".into()), Value::Integer(7)]
    );
    let values = vec![
        Value::Blob(vec![0, 255]),
        Value::Integer(i64::MIN),
        Value::Real(1.5),
        Value::Text("track\0title".into()),
        Value::Null,
    ];
    assert_eq!(RowKey::from(values.clone()).values(), values.as_slice());
    assert!(RowKey::from(Vec::<Value>::new()).values().is_empty());
}

fn retry(reason: PendingReason) -> Retry {
    PendingRecord::new(PendingSubject::Connection, reason).retry()
}

#[test]
fn missing_prerequisites_retry_automatically() {
    let path = ObjectPath::floor(coven_storage::test_utils::STORE_ID);
    for prerequisite in [Prerequisite::SequenceCatchUp, Prerequisite::Reload(Audience::Store)] {
        assert_eq!(retry(PendingReason::Waits(prerequisite)), Retry::Automatic);
    }
    for reason in [
        PendingReason::Missing { path },
        PendingReason::KeyUnavailable {
            audience: Audience::Store,
            key: KeyId(uuid::Uuid::from_u128(1)),
        },
        PendingReason::ProviderPending {
            action: ProviderAccessAction::Grant,
            account: "member@example.org".into(),
        },
        PendingReason::ProviderPending {
            action: ProviderAccessAction::Revoke,
            account: "member@example.org".into(),
        },
        PendingReason::AccountInUse { account: "shared@example.org".into() },
    ] {
        assert_eq!(retry(reason.clone()), Retry::Automatic, "{reason:?}");
    }
}

#[test]
fn storage_pending_retry_distinguishes_transient_failures_from_app_actions() {
    for (expected, failures) in [
        (
            Retry::Automatic,
            vec![
                StorageFailure::Network,
                StorageFailure::Conflict,
                StorageFailure::RateLimited,
                StorageFailure::NotFound,
                StorageFailure::SessionExpired,
            ],
        ),
        (
            Retry::AppAction,
            vec![
                StorageFailure::ProviderUnavailable,
                StorageFailure::AccessNotRegistered { required: OAuthAccess::OneDrive },
                StorageFailure::AccessNotGranted { required: OAuthAccess::OneDrive },
                StorageFailure::DropboxAppFolderNotShareable,
                StorageFailure::Authentication,
                StorageFailure::PermissionDenied,
                StorageFailure::AlreadyExists,
                StorageFailure::ContainerNotFound,
                StorageFailure::RegionMismatch,
                StorageFailure::QuotaExceeded,
                StorageFailure::InvalidConfiguration,
                StorageFailure::Refused,
                StorageFailure::Protocol,
                StorageFailure::InvalidPath,
                StorageFailure::InvalidRange,
                StorageFailure::NotStoreOwner,
                StorageFailure::InviteeIsStoreOwner,
                StorageFailure::AccountIdUnavailable,
                StorageFailure::InvitationMismatch,
                StorageFailure::SessionMismatch,
                StorageFailure::InvalidPart,
                StorageFailure::SingleRequestTooLarge { size: 2, limit: 1 },
                StorageFailure::Encoding,
                StorageFailure::File,
                StorageFailure::MemberKeysMissing,
            ],
        ),
    ] {
        for failure in failures {
            assert_eq!(
                retry(PendingReason::Storage {
                    provider: CloudProvider::S3,
                    failure,
                    answered: true,
                }),
                expected,
                "{failure:?}"
            );
        }
    }
}

#[test]
fn immutable_refusals_and_required_versions_wait_for_an_update() {
    for refusal in [
        Refusal::Decryption { cause: None },
        Refusal::Signature { cause: None },
        Refusal::Parse { cause: None },
        Refusal::InvalidWrite { cause: None },
        Refusal::NotAuthorized,
        Refusal::InvalidCausality { cause: None },
        Refusal::WrongIdentity { cause: None },
        Refusal::ContentHash,
    ] {
        assert_eq!(retry(PendingReason::Refused(refusal)), Retry::AfterUpdate);
    }
    for update in [
        RequiredUpdate::AppSchema { version: u32::MAX },
        RequiredUpdate::CovenFormat { version: u16::MAX },
    ] {
        assert_eq!(retry(PendingReason::UpdateRequired(update)), Retry::AfterUpdate);
    }
}

#[test]
fn access_and_lasting_local_failures_require_app_action() {
    for reason in [
        PendingReason::NoStorage,
        PendingReason::AccessRemains {
            account: "member@example.org".into(),
            shares: vec![RetainedAccess {
                provider_id: "parent-folder".into(),
                reason: RetainedAccessReason::Inherited,
            }],
        },
        PendingReason::FileUnavailable(FileMissingReason::Lost),
        PendingReason::FileUnavailable(FileMissingReason::Source(FileSourceFailure::Missing)),
        PendingReason::FileUnavailable(FileMissingReason::Source(FileSourceFailure::Changed)),
        PendingReason::FileUnavailable(FileMissingReason::Source(FileSourceFailure::Integrity)),
        PendingReason::Failed(LocalFailure::Crypto),
        PendingReason::Failed(LocalFailure::Operation),
        PendingReason::Paused,
    ] {
        assert_eq!(retry(reason.clone()), Retry::AppAction, "{reason:?}");
    }
    // A pass that fails on a local failure that clears by itself, such as a
    // locked keychain, retries on its own: no app call asks for it again.
    for failure in [LocalFailure::Database, LocalFailure::Disk, LocalFailure::KeyCustody] {
        assert_eq!(retry(PendingReason::Failed(failure)), Retry::Automatic);
    }
    assert_eq!(retry(PendingReason::Removed), Retry::Never);
    // This installation never sends again with a retired pair; restoring
    // with the current keys is a new installation (§13.1).
    assert_eq!(
        retry(PendingReason::MemberKeysReplaced { member: MemberId(uuid::Uuid::from_u128(3)) }),
        Retry::Never
    );
}

#[test]
fn cut_off_writes_are_final_and_rejected_operations_wait_for_the_app() {
    for cause in
        [WriteCutoff::DeviceRemoved, WriteCutoff::MemberRemoved, WriteCutoff::MemberKeysReplaced]
    {
        let record = PendingRecord::new(
            PendingSubject::Slot(Slot(7)),
            PendingReason::WritesRefused { cutoff: Slot(5), cause },
        );
        assert_eq!(record.retry(), Retry::Never, "{record:?}");
    }
    // A refused administrative operation records its local category; an
    // app's operation waits for the app, coven's own never clears.
    for (kind, expected) in
        [(OperationKind::CreateCircle, Retry::AppAction), (OperationKind::RotateKey, Retry::Never)]
    {
        let record = PendingRecord::new(
            PendingSubject::Operation { id: OperationId(3), kind },
            PendingReason::Failed(LocalFailure::Operation),
        );
        assert_eq!(record.retry(), expected, "{record:?}");
    }
}

mod store_rename {
    use super::*;
    use crate::tests::{landed, Network};

    async fn name(query: &mut LiveQuery<String, crate::CovenError>) -> String {
        tokio::time::timeout(HANG_GUARD, query.next())
            .await
            .unwrap()
            .expect("the live query answers")
            .unwrap()
    }

    #[tokio::test]
    async fn renames_follow_authority_and_publish_the_committed_name() {
        let network = Network::new(2).await;
        let owner = &network.devices[0].handle;
        let member = &network.devices[1].handle;
        let mut owner_names = owner.subscribe_store_name();
        let mut member_names = member.subscribe_store_name();
        assert_eq!(name(&mut owner_names).await, "Household");
        assert_eq!(name(&mut member_names).await, "Household");
        let restore = owner.restore_code().await.unwrap();
        let app = &network.devices[0].app;
        let person = crate::tests::saved_entry(app, owner.store_id()).unwrap();
        assert_eq!(person.name, "Household");
        assert!(matches!(
            member.rename_store("Forbidden").await,
            Err(SyncError::Rejected(OperationRejection::NotAllowed))
        ));
        assert_eq!(member.store_name().await.unwrap(), "Household");
        owner.rename_store("家庭 recipes").await.unwrap();
        assert_eq!(name(&mut owner_names).await, "家庭 recipes");
        network.sync(1).await;
        assert_eq!(name(&mut member_names).await, "家庭 recipes");
        for device in &network.devices {
            assert_eq!(device.handle.store_name().await.unwrap(), "家庭 recipes");
            assert_eq!(
                device.app.readable_stores().await,
                vec![StoreInfo {
                    id: device.store, name: "家庭 recipes".into(), unfinished: None
                }]
            );
        }
        assert_eq!(owner.restore_code().await.unwrap(), restore);
        // The saved code keeps its bytes and takes the new name.
        let renamed = crate::tests::saved_entry(app, owner.store_id()).unwrap();
        assert_eq!(renamed.name, "家庭 recipes");
        assert_eq!(renamed.code.to_bytes().unwrap(), person.code.to_bytes().unwrap());
        assert!(matches!(decode_code_info(&restore).unwrap(), CodeInfo::Restore(_)));
        let reader =
            network.reopen(&network.devices[0]).open_read_only(owner.store_id()).await.unwrap();
        assert_eq!(reader.store_name().await.unwrap(), "家庭 recipes");
        reader.close().await.unwrap();

        let member_id = member.members().await.unwrap().into_iter().find(|m| m.is_self).unwrap().id;
        landed(owner, owner.set_member_role(&member_id, MemberRole::Admin).await.unwrap()).await;
        // Each write checks against the slots its device has applied.
        network.sync(1).await;
        landed(member, member.rename_store("Latest name").await.unwrap()).await;
        network.sync(0).await;
        landed(owner, owner.set_member_role(&member_id, MemberRole::Member).await.unwrap()).await;
        network.sync(1).await;
        assert!(matches!(
            member.rename_store("After demotion").await,
            Err(SyncError::Rejected(OperationRejection::NotAllowed))
        ));
        assert_eq!(owner.store_name().await.unwrap(), "Latest name");
        network.devices[0].stop_sync().await;
        owner.unlock_store_key().await.unwrap();
        // A name the store already has journals nothing.
        let through = owner.applied_slot().await;
        assert_eq!(owner.rename_store("Latest name").await.unwrap(), None);
        assert_eq!(owner.applied_slot().await, through);
        assert!(owner.rename_store("").await.is_err());
        assert_eq!(owner.store_name().await.unwrap(), "Latest name");
        network.close().await;
    }
}

mod mutual_removal {
    use super::*;
    use crate::tests::{landed, outcome, Network};

    /// Ana and Ben, both admins, each journal the other's removal while
    /// offline. Ana's claims its slot first; Ben's device applies it, stops
    /// for good, and his call ends `Stopped` without landing (§14.1). No
    /// removal of Ana ever enters history (§10.2).
    #[tokio::test]
    async fn admins_removing_each_other_leave_the_first_removal_only() {
        let network = Network::new(2).await;
        let (ana, ben) = (&network.devices[0], &network.devices[1]);
        let own_id = |handle: &CovenHandle| {
            let handle = handle.clone();
            async move { handle.members().await.unwrap().into_iter().find(|m| m.is_self).unwrap().id }
        };
        let (ana_id, ben_id) = (own_id(&ana.handle).await, own_id(&ben.handle).await);
        landed(&ana.handle, ana.handle.set_member_role(&ben_id, MemberRole::Admin).await.unwrap())
            .await;
        network.sync(1).await;

        ana.stop_sync().await;
        ben.stop_sync().await;
        let against_ben = ana.handle.remove_member(&ben_id).await.unwrap();
        let against_ana = ben.handle.remove_member(&ana_id).await.unwrap();

        ana.handle.start_sync().await.unwrap();
        let removal = landed(&ana.handle, against_ben).await;
        ben.handle.start_sync().await.unwrap();
        network.clock.set(network.clock.now() + std::time::Duration::from_secs(1));
        let _ = ben.handle.retry_now().await.unwrap().await;
        let ended = outcome(&ben.handle, against_ana).await;
        assert_eq!(ended, OperationOutcome::Stopped(coven_database::DeviceStop::Removed));

        // Ben's device stopped at the removal; Ana's goes on and no slot of
        // Ben's follows it.
        network.pass(0).await;
        let settled = ana.handle.applied_slot().await;
        network.pass(0).await;
        assert_eq!(ana.handle.applied_slot().await, settled);
        assert_eq!(ben.handle.applied_slot().await, removal.0);
        for device in [ana, ben] {
            let members = device.handle.members().await.unwrap();
            let ids: Vec<_> = members.iter().map(|m| m.id).collect();
            assert_eq!(ids, [ana_id], "{members:?}");
        }
        network.close().await;
    }
}

/// Closing a read-only handle releases its storage client, while the app
/// still holds the handle (E1).
#[tokio::test]
async fn closing_a_read_only_handle_releases_its_storage_client() {
    let network = crate::tests::Network::new(1).await;
    let device = &network.devices[0];
    device.stop_sync().await;
    let reader = network.reopen(device).open_read_only(device.store).await.unwrap();
    reader.close().await.unwrap();
    device.connector.clients.released().await;
    drop(reader);
    network.close().await;
}

/// The circles a member is in, a circle's members and their live queries
/// follow the applied slots: a member sees only their own circles in
/// circle-id order, a circle's members who are still in the store in
/// member-id order, and each subscription reports an addition, a removal
/// from the store and a deletion (E12).
#[tokio::test]
async fn circle_lists_and_members_follow_applied_slots_live() {
    use crate::tests::Network;
    let network = Network::new(3).await;
    let ana = &network.devices[0].handle;
    let member = |index: usize| {
        let handle = network.devices[index].handle.clone();
        async move { handle.members().await.unwrap().into_iter().find(|m| m.is_self).unwrap().id }
    };
    let (ben, carol) = (member(1).await, member(2).await);
    let gifts = crate::tests::circle(ana, "Gifts").await;
    let garden = crate::tests::circle(ana, "Garden").await;
    let bens = crate::tests::circle(&network.devices[1].handle, "Ben's own").await;
    let mut listed = ana.circles().subscribe_list();
    let mut gifts_members = ana.circles().subscribe_members(gifts);
    assert_eq!(listed.next().await.expect("answers").unwrap().len(), 2);
    assert_eq!(gifts_members.next().await.expect("answers").unwrap().len(), 1);
    for who in [&ben, &carol] {
        crate::tests::landed(ana, ana.circles().add_member(gifts, who).await.unwrap()).await;
    }
    network.sync(0).await;
    let circles = ana.circles().list().await.unwrap();
    let mut expected = vec![gifts, garden];
    expected.sort();
    assert_eq!(
        circles.iter().map(|c| c.id).collect::<Vec<_>>(),
        expected,
        "Ben's own circle is not Ana's"
    );
    assert!(!circles.iter().any(|c| c.id == bens));
    let members = ana.circles().members(gifts).await.unwrap();
    let mut ids = vec![member(0).await, ben, carol];
    ids.sort();
    assert_eq!(members.iter().map(|m| m.member).collect::<Vec<_>>(), ids);
    assert_eq!(members.iter().filter(|m| m.is_self).count(), 1);
    let live = loop {
        let value = gifts_members.next().await.expect("answers").unwrap();
        if value.len() == 3 {
            break value;
        }
    };
    assert_eq!(live, members);
    // Removing Carol from the store takes her out of the circle's members.
    crate::tests::landed(ana, ana.remove_member(&carol).await.unwrap()).await;
    network.sync(0).await;
    let live = loop {
        let value = gifts_members.next().await.expect("answers").unwrap();
        if value.len() == 2 {
            break value;
        }
    };
    assert!(!live.iter().any(|m| m.member == carol));
    assert_eq!(ana.circles().members(gifts).await.unwrap(), live);
    // Deleting a circle takes it out of the list.
    crate::tests::landed(ana, ana.circles().delete(garden).await.unwrap()).await;
    let live = loop {
        let value = listed.next().await.expect("answers").unwrap();
        if value.iter().all(|c| c.id != garden) {
            break value;
        }
    };
    assert_eq!(live.iter().map(|c| c.id).collect::<Vec<_>>(), [gifts]);
    network.close().await;
}

/// Ana resets Gifts from her copy. A Gifts write Ben queued before he read
/// the reset counts on neither device; one he makes after reading it does.
/// The store's rows are untouched (§19.4, E12).
#[tokio::test]
async fn a_circle_reset_keeps_only_the_snapshot_and_writes_that_read_it() {
    use crate::tests::Network;
    async fn quiet(network: &Network) {
        for _ in 0..3 {
            for index in 0..network.devices.len() {
                network.sync(index).await;
            }
        }
    }
    async fn held(handle: &CovenHandle) -> (Vec<String>, i64) {
        handle
            .read(|sql| {
                Ok::<_, CovenError>((
                    sql.query("SELECT id FROM circle_notes ORDER BY id", [], |r| r.get(0))?,
                    sql.query_row("SELECT count(*) FROM parents", [], |r| r.get(0))?,
                ))
            })
            .await
            .unwrap()
    }
    let network = Network::new(2).await;
    let (ana, ben) = (&network.devices[0].handle, &network.devices[1].handle);
    let ben_id = ben.members().await.unwrap().into_iter().find(|m| m.is_self).unwrap().id;
    let gifts = crate::tests::circle(ana, "Gifts").await;
    crate::tests::landed(ana, ana.circles().add_member(gifts, &ben_id).await.unwrap()).await;
    ana.write(|sql| {
        sql.execute("INSERT INTO parents(id,label,value) VALUES('store','kept',1)", [])?;
        Ok::<_, CovenError>(())
    })
    .await
    .unwrap();
    quiet(&network).await;
    let note = |id: &'static str| {
        move |sql: crate::CovenWrite<'_>| {
            sql.execute(
                "INSERT INTO circle_notes(id,value,audience) VALUES(?1,1,?2)",
                (id, gifts.to_string()),
            )?;
            Ok::<_, CovenError>(())
        }
    };
    ben.write(note("00000000-0000-7000-8000-000000000100")).await.unwrap();
    ana.circles().reset(gifts).await.unwrap();
    quiet(&network).await;
    for index in 0..2 {
        let (notes, parents) = held(&network.devices[index].handle).await;
        assert!(notes.is_empty(), "device {index} dropped the queued write");
        assert_eq!(parents, 1, "the store's row stays");
    }
    ben.write(note("00000000-0000-7000-8000-000000000101")).await.unwrap();
    quiet(&network).await;
    for index in 0..2 {
        let (notes, parents) = held(&network.devices[index].handle).await;
        assert_eq!(notes, ["00000000-0000-7000-8000-000000000101"], "device {index}");
        assert_eq!(parents, 1);
    }
    network.close().await;
}
