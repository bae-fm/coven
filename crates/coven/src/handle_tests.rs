use crate::*;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

fn builder(app: &TestCoven, layout: StoreLayout) -> CovenBuilder {
    app.builder(layout)
        .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
        .migrations(vec![Migration::addition(
            1,
            "notes",
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL)",
        )])
}

#[tokio::test]
async fn reopening_waits_after_the_last_handle_drops_or_close_starts() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    for close in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(root.path().into());
        let app = TestCoven::new();
        let directory = app
            .create_closed_store(&layout, "Reopen", Arc::new(UuidIds))
            .await
            .unwrap();
        let handle = builder(&app, layout.clone())
            .open(directory.id())
            .await
            .unwrap();
        let clone = handle.clone();
        drop(handle);
        assert!(matches!(
            builder(&app, layout.clone()).open(directory.id()).await,
            Err(CovenError::Lock(StoreLockError::AlreadyOpen(_)))
        ));
        let (entered, entry) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let mut read = Box::pin(clone.read(move |_| {
            entered.send(()).unwrap();
            released
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
            Ok(())
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
        let mut reopen = Box::pin(builder(&app, layout.clone()).open(directory.id()));
        poll_fn(|cx| {
            assert!(reopen.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        release.send(()).unwrap();
        let reopened = reopen.await.unwrap();
        reopened.read(|_| Ok(())).await.unwrap();
        reopened.close().await.unwrap();
    }
}

#[tokio::test]
async fn kept_keys_survive_reopen_and_forgetting_preserves_identity() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let ids = Arc::new(UuidIds);
    let directory = app
        .create_closed_store(&layout, "keys", ids.clone())
        .await
        .unwrap();
    let keys = StoreKeyring::new(StoreKey::generate(KeyId(ids.new_id())).unwrap());
    app.keep_store_keys(&directory, &keys).unwrap();
    let handle = builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);

    handle.set_host_secret("token", "secret").unwrap();
    handle.close().await.unwrap();
    let handle = builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
    app.fail_next_keychain_operation();
    assert!(handle.forget_store_keys().await.is_err());
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
    handle.forget_store_keys().await.unwrap();
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Locked);
    assert!(
        MemberKeyCustody::unlock(&coven_crypto::custody::KeyringCustody::new(Arc::new(
            app.store_keychain(directory.id())
        )))
        .unwrap()
        .is_some()
    );
    assert_eq!(
        handle.host_secret("token").unwrap().as_deref(),
        Some("secret")
    );
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
impl StoreKeyCustody for Keys {
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
impl MemberKeyCustody for Identity {
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
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let keys = Arc::new(Keys::default());
    let identity = Arc::new(Identity::default());
    let handle = builder(&app, layout.clone())
        .key_custody(KeyCustody::Custom(keys.clone()))
        .identity_custody(IdentityCustody::Custom(identity.clone()))
        .create_store("session")
        .await
        .unwrap();
    let directory = layout.store_dir(&handle.store_id());
    let failed: CovenResult<()> = handle
        .write(|sql| {
            sql.execute("INSERT INTO notes VALUES('a','not committed')", [])?;
            Err(KeyError::PassphraseAuthentication.into())
        })
        .await;
    assert!(matches!(
        failed,
        Err(CovenError::Key(KeyError::PassphraseAuthentication))
    ));
    assert_eq!(
        handle
            .read(
                |sql| Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?)
            )
            .await
            .unwrap(),
        0
    );
    assert!(handle.pending().await.unwrap().is_empty());
    let reader = builder(&app, layout.clone())
        .key_custody(KeyCustody::Custom(keys.clone()))
        .identity_custody(IdentityCustody::Custom(identity.clone()))
        .open_read_only(directory.id())
        .await
        .unwrap();
    reader.read(|_| Ok(())).await.unwrap();
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
        .write_with_files(
            |_| Err(KeyError::PassphraseHeader.into()),
            |_| panic!("failed build cannot run SQL"),
        )
        .await;
    assert!(matches!(
        failed,
        Err(CovenError::Key(KeyError::PassphraseHeader))
    ));
    handle.close().await.unwrap();
}

#[tokio::test]
async fn cancelling_the_close_caller_still_closes_all_clones() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let directory = app
        .create_closed_store(&layout, "closing", Arc::new(UuidIds))
        .await
        .unwrap();
    let handle = builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    let mut live = handle.subscribe(|_| Ok(()));
    live.next().await.unwrap();
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
        tokio::time::timeout(std::time::Duration::from_secs(5), live.next())
            .await
            .unwrap(),
        Err(CovenError::Database(DbError::StoreClosed))
    ));
    assert!(matches!(
        handle.read(|_| Ok(())).await,
        Err(CovenError::Database(DbError::StoreClosed))
    ));
    assert!(matches!(
        handle.host_secret("token"),
        Err(KeyError::StoreClosed)
    ));
    let reopened = builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn closed_storage_calls_keep_their_public_errors_on_every_clone() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let directory = app
        .create_closed_store(&layout, "closed", Arc::new(UuidIds))
        .await
        .unwrap();
    let original = builder(&app, layout).open(directory.id()).await.unwrap();
    let handle = original.clone();
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Locked);
    original.close().await.unwrap();
    assert!(matches!(
        handle.store_key_state(),
        Err(KeyError::StoreClosed)
    ));
    assert!(matches!(
        handle
            .setup_s3_storage(
                StorageConfig::S3 {
                    bucket: "bucket".into(),
                    region: "region".into(),
                    prefix: "store".into(),
                    endpoint: None,
                    profile: None,
                },
                "Device",
                "member".into(),
                SecretText::new("secret".into())
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
                    }
                },
                "Device"
            )
            .await,
        Err(StorageSetupError::SecureStorage(KeyError::StoreClosed))
    ));
    assert!(matches!(
        handle.setup_cloudkit_storage("container", "Device").await,
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
    assert!(matches!(
        handle.disconnect_storage().await,
        Err(SyncError::SecureStorage(KeyError::StoreClosed))
    ));
    assert!(matches!(
        handle.forget_store_keys().await,
        Err(KeyError::StoreClosed)
    ));
}

/// Members and circles come from the committed replay: a handle with no
/// storage, connection or unlocked key reads them without the operation
/// worker, and each subscription starts with the value its call returns. A
/// store not yet created in storage has no members (E4, E9, E12).
#[tokio::test]
async fn disconnected_member_and_circle_reads_match_their_subscriptions() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let directory = app
        .create_closed_store(&layout, "members", Arc::new(UuidIds))
        .await
        .unwrap();
    let handle = builder(&app, layout).open(directory.id()).await.unwrap();
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Locked);
    let members = handle.members().await.unwrap();
    assert!(members.is_empty());
    assert_eq!(handle.subscribe_members().next().await.unwrap(), members);
    assert!(handle.circles().list().await.unwrap().is_empty());
    assert!(handle
        .circles()
        .subscribe_list()
        .next()
        .await
        .unwrap()
        .is_empty());
    handle.close().await.unwrap();
    assert!(matches!(
        handle.members().await,
        Err(CovenError::Database(DbError::StoreClosed))
    ));
}

#[tokio::test]
async fn closing_waits_for_setup_after_its_caller_is_cancelled() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        use coven_storage::{test_utils::MemoryStorage, StorageSettings};
        let root = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(root.path().to_owned());
        let app = TestCoven::new();
        let directory = app.create_closed_store(&layout, "setup", Arc::new(UuidIds)).await.unwrap();
        let config = StorageConfig::S3 {
            bucket: "bucket".into(), region: "region".into(), prefix: "store".into(), endpoint: None, profile: None,
        };
        let storage = Arc::new(MemoryStorage::builder().location(config.clone()).clock(Arc::new(SystemClock)).build().unwrap());
        let handle = builder(&app, layout).storage_connector(storage.clone()).open(directory.id()).await.unwrap();

        let (entered, entering) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        storage.hold_next_listing(ObjectPrefix::all(directory.id()), entered, resumed).await;
        {
            let setup = handle.setup_s3_storage(config.clone(), "Device", "member".into(), SecretText::new("secret".into()));
            tokio::pin!(setup);
            tokio::select! {
                result = &mut setup => panic!("setup finished while its provider check was held: {result:?}"),
                result = entering => result.unwrap(),
            }
        }
        let close = handle.close();
        tokio::pin!(close);
        tokio::select! {
            result = &mut close => panic!("close finished before setup: {result:?}"),
            _ = tokio::task::yield_now() => {},
        }
        resume.send(()).unwrap();
        close.await.unwrap();
        assert_eq!(StorageSettings::new(directory.clone()).read().unwrap(), Some(config));
        // Creation claimed slot 1 and the creator's key copy at slot 2.
        assert_eq!(crate::tests::stored(&storage, directory.id(), |path| path.slot_number().is_some()).await.len(), 2);
        assert!(matches!(handle.store_key_state(), Err(KeyError::StoreClosed)));
    }).await.expect("cancelled setup finishes before close");
}

#[tokio::test]
async fn workers_share_keys_across_stop_forget_and_reacquisition_until_close() {
    use coven_storage::{test_utils::MemoryStorage, Storage};
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let keys = Arc::new(Keys::default());
    let identity = Arc::new(Identity::default());
    let handle = builder(&app, layout.clone())
        .key_custody(KeyCustody::Custom(keys.clone()))
        .identity_custody(IdentityCustody::Custom(identity.clone()))
        .storage_connector(storage.clone())
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
        )
        .await
        .unwrap();
    let clone = handle.clone();
    let circle = clone.circles().create("Shared").await.unwrap();
    assert!(keys
        .keys
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .circle_key_ids(circle)
        .next()
        .is_some());
    let code = handle.restore_code().await.unwrap();
    for _ in 0..2 {
        let mut status = handle.subscribe_sync_status();
        handle.stop_sync();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            status.wait_for(|s| matches!(s, SyncStatus::Stopped)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(clone.store_key_state().unwrap(), StoreKeyState::Available);
        assert_eq!(handle.restore_code().await.unwrap(), code);
        handle.start_sync().await.unwrap();
    }
    handle.forget_store_keys().await.unwrap();
    assert!(keys.keys.lock().unwrap().is_none());
    assert_eq!(clone.store_key_state().unwrap(), StoreKeyState::Locked);
    assert_eq!(
        identity.keys.lock().unwrap().as_ref().unwrap().member(),
        member
    );
    assert_eq!(handle.restore_code().await.unwrap(), code);
    handle.unlock_store_key().await.unwrap();
    assert_eq!(clone.store_key_state().unwrap(), StoreKeyState::Available);
    assert!(keys
        .keys
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .circle_key_ids(circle)
        .next()
        .is_some());
    assert_eq!(keys.unlocks.load(Ordering::SeqCst), 1);
    assert_eq!(identity.unlocks.load(Ordering::SeqCst), 1);
    handle.close().await.unwrap();
    assert!(keys.keys.lock().unwrap().is_none());
    assert!(identity.keys.lock().unwrap().is_none());
    assert_eq!(keys.closes.load(Ordering::SeqCst), 1);
    assert_eq!(identity.closes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        clone.store_key_state(),
        Err(KeyError::StoreClosed)
    ));
    assert!(matches!(
        clone.close().await,
        Err(SyncError::Database(DbError::StoreClosed))
    ));
    assert_eq!(keys.closes.load(Ordering::SeqCst), 1);
    assert_eq!(identity.closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_identity_unlock_releases_the_opened_store_session() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let directory = app
        .create_closed_store(&layout, "session", Arc::new(UuidIds))
        .await
        .unwrap();
    let keys = Arc::new(Keys::default());
    let identity = Arc::new(Identity::default());
    identity.refuse.store(true, Ordering::SeqCst);
    assert!(matches!(
        builder(&app, layout)
            .key_custody(KeyCustody::Custom(keys.clone()))
            .identity_custody(IdentityCustody::Custom(identity.clone()))
            .open(directory.id())
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
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let directory = app
        .create_closed_store(&layout, "session", Arc::new(UuidIds))
        .await
        .unwrap();
    let keys = Arc::new(Keys::default());
    let identity = Arc::new(Identity::default());
    keys.refuse.store(true, Ordering::SeqCst);
    let error = builder(&app, layout)
        .migrations(vec![Migration::addition(1, "invalid", "INVALID SQL")])
        .key_custody(KeyCustody::Custom(keys.clone()))
        .identity_custody(IdentityCustody::Custom(identity.clone()))
        .open(directory.id())
        .await
        .unwrap_err();
    assert!(matches!(error, CovenError::Migration(_)), "{error:?}");
    assert_eq!(keys.unlocks.load(Ordering::SeqCst), 0);
    assert_eq!(identity.unlocks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn closing_erases_custody_once_and_fails_later_calls_on_every_clone() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let ids = Arc::new(UuidIds);
    let keys = Arc::new(Keys {
        keys: Mutex::new(Some(StoreKeyring::new(
            StoreKey::generate(KeyId(ids.new_id())).unwrap(),
        ))),
        ..Keys::default()
    });
    let identity = Arc::new(Identity::default());
    let handle = builder(&app, layout.clone())
        .key_custody(KeyCustody::Custom(keys.clone()))
        .identity_custody(IdentityCustody::Custom(identity.clone()))
        .create_store("session")
        .await
        .unwrap();
    let clone = handle.clone();
    // Without storage there is no loop to stop; stopping changes nothing.
    handle.stop_sync();
    handle.close().await.unwrap();
    assert_eq!(keys.closes.load(Ordering::SeqCst), 1);
    assert_eq!(identity.closes.load(Ordering::SeqCst), 1);
    assert!(keys.keys.lock().unwrap().is_none());
    assert!(identity.keys.lock().unwrap().is_none());
    assert!(matches!(
        clone.read(|_| Ok(())).await,
        Err(CovenError::Database(DbError::StoreClosed))
    ));
    assert!(matches!(
        clone.store_key_state(),
        Err(KeyError::StoreClosed)
    ));
}

#[test]
fn row_keys_preserve_column_order_and_sql_storage_classes() {
    use types::Value;
    assert_eq!(
        RowKey::from("track").values(),
        &[Value::Text("track".into())]
    );
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
    PendingRecord {
        subject: PendingSubject::Connection,
        reason,
    }
    .retry()
}

#[test]
fn missing_prerequisites_retry_automatically() {
    let device = DeviceId(1);
    let path = ObjectPath::floor(coven_storage::test_utils::STORE_ID);
    for prerequisite in [
        Prerequisite::SequenceCatchUp,
        Prerequisite::Object(path.clone()),
        Prerequisite::DeviceRegistration(device),
        Prerequisite::SchemaRaise {
            audience: Audience::Store,
            version: 2,
        },
        Prerequisite::Reload(Audience::Store),
        Prerequisite::Snapshot(SnapshotId {
            audience: Audience::Store,
            device,
            number: 1,
        }),
    ] {
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
        PendingReason::PendingOwner {
            account: "owner@example.org".into(),
        },
        PendingReason::AccountInUse {
            account: "shared@example.org".into(),
        },
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
                StorageFailure::AccessNotRegistered {
                    required: OAuthAccess::OneDrive,
                },
                StorageFailure::AccessNotGranted {
                    required: OAuthAccess::OneDrive,
                },
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
        assert_eq!(
            retry(PendingReason::UpdateRequired(update)),
            Retry::AfterUpdate
        );
    }
}

#[test]
fn local_and_access_failures_require_app_action() {
    for reason in [
        PendingReason::NoStorage,
        PendingReason::AccessRemains {
            account: "member@example.org".into(),
            shares: vec![RetainedAccess {
                provider_id: "parent-folder".into(),
                reason: RetainedAccessReason::Inherited,
            }],
        },
        PendingReason::DeleteAccessKey(AccessKeyToDelete {
            access_key_id: "expired-invite-key".into(),
            principal: coven_format::S3Principal::Console,
            member: None,
        }),
        PendingReason::FileUnavailable(FileMissingReason::DeviceRemoved),
        PendingReason::FileUnavailable(FileMissingReason::DeviceReplaced),
        PendingReason::FileUnavailable(FileMissingReason::Source(FileSourceFailure::Missing)),
        PendingReason::FileUnavailable(FileMissingReason::Source(FileSourceFailure::Changed)),
        PendingReason::FileUnavailable(FileMissingReason::Source(FileSourceFailure::Integrity)),
        PendingReason::Failed(LocalFailure::Database),
        PendingReason::Failed(LocalFailure::Disk),
        PendingReason::Failed(LocalFailure::KeyCustody),
        PendingReason::Failed(LocalFailure::Crypto),
        PendingReason::Failed(LocalFailure::Operation),
        PendingReason::Paused,
    ] {
        assert_eq!(retry(reason.clone()), Retry::AppAction, "{reason:?}");
    }
    assert_eq!(retry(PendingReason::Removed), Retry::Never);
    // This installation never sends again with a retired pair; restoring
    // with the current keys is a new installation (§13.1).
    assert_eq!(
        retry(PendingReason::MemberKeysReplaced {
            member: MemberId(uuid::Uuid::from_u128(3)),
        }),
        Retry::Never
    );
}

#[test]
fn cut_off_writes_are_final_and_rejected_operations_wait_for_the_app() {
    for cause in [
        WriteCutoff::DeviceRemoved,
        WriteCutoff::MemberRemoved,
        WriteCutoff::MemberKeysReplaced,
    ] {
        let record = PendingRecord {
            subject: PendingSubject::Slot(Slot(7)),
            reason: PendingReason::WritesRefused {
                cutoff: Slot(5),
                cause,
            },
        };
        assert_eq!(record.retry(), Retry::Never, "{record:?}");
    }
    // A refused administrative operation records its local category; an
    // app's operation waits for the app, coven's own never clears.
    for (kind, expected) in [
        (OperationKind::CreateCircle, Retry::AppAction),
        (OperationKind::RotateKey, Retry::Never),
    ] {
        let record = PendingRecord {
            subject: PendingSubject::Operation {
                id: OperationId(3),
                kind,
            },
            reason: PendingReason::Failed(LocalFailure::Operation),
        };
        assert_eq!(record.retry(), expected, "{record:?}");
    }
}

mod store_rename {
    use super::*;
    use crate::tests::Network;
    use std::time::Duration;

    async fn name(query: &mut LiveQuery<String>) -> String {
        tokio::time::timeout(Duration::from_secs(10), query.next())
            .await
            .unwrap()
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
        let keychain = network.devices[0].app.synced_keychain(owner.store_id());
        let synced = keychain.restore_code().unwrap().unwrap();
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
                device.layout.stores().await.unwrap(),
                vec![StoreInfo {
                    id: device.store,
                    name: "家庭 recipes".into(),
                }]
            );
        }
        assert_eq!(owner.restore_code().await.unwrap(), restore);
        assert_eq!(
            keychain.restore_code().unwrap().unwrap().as_bytes(),
            synced.as_bytes()
        );
        assert!(matches!(
            decode_code_info(&restore).unwrap(),
            CodeInfo::Restore(_)
        ));
        let reader = network
            .reopen(&network.devices[0])
            .open_read_only(owner.store_id())
            .await
            .unwrap();
        assert_eq!(reader.store_name().await.unwrap(), "家庭 recipes");
        reader.close().await.unwrap();

        let member_id = member
            .members()
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.is_self)
            .unwrap()
            .id;
        owner
            .set_member_role(&member_id, MemberRole::Admin)
            .await
            .unwrap();
        member.rename_store("Latest name").await.unwrap();
        owner
            .set_member_role(&member_id, MemberRole::Member)
            .await
            .unwrap();
        assert!(matches!(
            member.rename_store("After demotion").await,
            Err(SyncError::Rejected(OperationRejection::NotAllowed))
        ));
        assert_eq!(owner.store_name().await.unwrap(), "Latest name");
        owner.stop_sync();
        owner
            .subscribe_sync_status()
            .wait_for(|s| matches!(s, SyncStatus::Stopped))
            .await
            .unwrap();
        owner.unlock_store_key().await.unwrap();
        let through = owner.applied_slot().await;
        owner.rename_store("Latest name").await.unwrap();
        assert_eq!(owner.applied_slot().await, through);
        assert!(owner.rename_store("").await.is_err());
        assert_eq!(owner.store_name().await.unwrap(), "Latest name");
        network.close().await;
    }
}
