use crate::*;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

fn builder(app: &TestCoven, layout: StoreLayout) -> CovenBuilder {
    app.builder(layout)
        .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
        .migrations(vec![Migration::sql(
            1,
            "notes",
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL)",
        )])
}

#[tokio::test]
async fn kept_keys_survive_reopen_and_forgetting_preserves_identity() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let ids = Arc::new(UuidIds);
    let directory = app
        .create_store(&layout, "keys", ids.clone())
        .await
        .unwrap();
    let keys = StoreKeyring::new(StoreKey::generate(KeyId(ids.new_id())).unwrap());
    app.keep_store_keys(&directory, &keys).unwrap();
    let handle = builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    assert_eq!(handle.store_key_state().unwrap(), StoreKeyState::Available);
    handle.initialize_identity().unwrap();
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
    assert!(matches!(
        handle.initialize_identity(),
        Err(IdentityError::AlreadyInitialized)
    ));
    assert_eq!(
        handle.host_secret("token").unwrap().as_deref(),
        Some("secret")
    );
    handle.close().await.unwrap();
    app.keep_store_keys(&directory, &keys).unwrap();
    app.delete_store(&directory).await.unwrap();
    // Deletion removes custody even when no directory remains to open.
    app.fail_next_keychain_operation();
    assert!(app.delete_store(&directory).await.is_err());
    app.delete_store(&directory).await.unwrap();
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
    keys: Mutex<Option<MemberKeys>>,
    unlocks: AtomicUsize,
    closes: AtomicUsize,
    refuse: AtomicBool,
}
impl MemberKeyCustody for Identity {
    fn unlock(&self) -> Result<Option<MemberKeys>, KeyError> {
        self.unlocks.fetch_add(1, Ordering::SeqCst);
        if self.refuse.load(Ordering::SeqCst) {
            return Err(KeyError::PassphraseAuthentication);
        }
        Ok(self.keys.lock().unwrap().clone())
    }
    fn persist(&self, keys: &MemberKeys) -> Result<(), KeyError> {
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
async fn opening_unlocks_once_and_sql_callback_failures_roll_back() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let directory = app
        .create_store(&layout, "session", Arc::new(UuidIds))
        .await
        .unwrap();
    let keys = Arc::new(Keys::default());
    let identity = Arc::new(Identity::default());
    let handle = builder(&app, layout.clone())
        .key_custody(KeyCustody::Custom(keys.clone()))
        .identity_custody(IdentityCustody::Custom(identity.clone()))
        .open(directory.id())
        .await
        .unwrap();
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
    assert!(handle.access_keys_to_delete().await.unwrap().is_empty());
    assert!(handle.pending_operations().await.unwrap().is_empty());
    let reader = builder(&app, layout)
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
    handle.initialize_identity().unwrap();
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
        .create_store(&layout, "closing", Arc::new(UuidIds))
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
        .create_store(&layout, "closed", Arc::new(UuidIds))
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
                    namespace_id: "folder".into()
                },
                "Device"
            )
            .await,
        Err(StorageSetupError::SecureStorage(KeyError::StoreClosed))
    ));
    assert!(matches!(
        handle
            .setup_cloudkit_storage(
                StorageConfig::CloudKit {
                    container: "container".into(),
                    owner: "owner".into(),
                    zone: "zone".into(),
                },
                "Device"
            )
            .await,
        Err(StorageSetupError::SecureStorage(KeyError::StoreClosed))
    ));
    assert!(matches!(
        handle.authenticate(CloudProvider::Dropbox).await,
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

#[tokio::test]
async fn closing_waits_for_setup_after_its_caller_is_cancelled() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        use coven_storage::{test_utils::MemoryStorage, Storage, StorageSettings};
        let root = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(root.path().to_owned());
        let app = TestCoven::new();
        let directory = app.create_store(&layout, "setup", Arc::new(UuidIds)).await.unwrap();
        let config = StorageConfig::S3 {
            bucket: "bucket".into(), region: "region".into(), prefix: "store".into(), endpoint: None,
        };
        let storage = Arc::new(MemoryStorage::builder().location(config.clone()).clock(Arc::new(SystemClock)).build().unwrap());
        let handle = builder(&app, layout).storage_connector(storage.clone()).open(directory.id()).await.unwrap();
        handle.initialize_identity().unwrap();
        let (entered, entering) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        storage.hold_next_listing(ObjectPrefix::all(), entered, resumed).await;
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
        assert_eq!(StorageSettings::new(directory).read().unwrap(), Some(config));
        assert_eq!(storage.list(&ObjectPrefix::store_logs()).await.unwrap().len(), 1);
        assert!(matches!(handle.store_key_state(), Err(KeyError::StoreClosed)));
    }).await.expect("cancelled setup finishes before close");
}

#[tokio::test]
async fn workers_share_keys_across_stop_forget_and_reacquisition_until_close() {
    use coven_storage::{test_utils::MemoryStorage, Storage};
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let directory = app
        .create_store(&layout, "session", Arc::new(UuidIds))
        .await
        .unwrap();
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let keys = Arc::new(Keys::default());
    let identity = Arc::new(Identity::default());
    let handle = builder(&app, layout)
        .key_custody(KeyCustody::Custom(keys.clone()))
        .identity_custody(IdentityCustody::Custom(identity.clone()))
        .storage_connector(storage.clone())
        .open(directory.id())
        .await
        .unwrap();
    let member = handle.initialize_identity().unwrap();
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
        identity.keys.lock().unwrap().as_ref().unwrap().member_id(),
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
    assert!(matches!(clone.close().await, Err(DbError::StoreClosed)));
    assert_eq!(keys.closes.load(Ordering::SeqCst), 1);
    assert_eq!(identity.closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_identity_unlock_releases_the_opened_store_session() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let directory = app
        .create_store(&layout, "session", Arc::new(UuidIds))
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
        .create_store(&layout, "session", Arc::new(UuidIds))
        .await
        .unwrap();
    let keys = Arc::new(Keys::default());
    let identity = Arc::new(Identity::default());
    keys.refuse.store(true, Ordering::SeqCst);
    let error = builder(&app, layout)
        .migrations(vec![Migration::sql(1, "invalid", "INVALID SQL")])
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
async fn closing_erases_custody_even_when_releasing_a_worker_fails() {
    use std::{
        future::Future,
        pin::Pin,
        time::{Duration, SystemTime},
    };

    struct HeldClock;
    impl Clock for HeldClock {
        fn now(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_000)
        }
        fn sleep(&self, _: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(std::future::pending())
        }
    }

    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let ids = Arc::new(UuidIds);
    let directory = app
        .create_store(&layout, "closing", ids.clone())
        .await
        .unwrap();
    let keys = Arc::new(Keys {
        keys: Mutex::new(Some(StoreKeyring::new(
            StoreKey::generate(KeyId(ids.new_id())).unwrap(),
        ))),
        ..Keys::default()
    });
    let identity = Arc::new(Identity::default());
    let handle = builder(&app, layout)
        .clock(Arc::new(HeldClock))
        .key_custody(KeyCustody::Custom(keys.clone()))
        .identity_custody(IdentityCustody::Custom(identity.clone()))
        .open(directory.id())
        .await
        .unwrap();
    handle.initialize_identity().unwrap();
    handle.pending_operations().await.unwrap();
    // Hold timer-driven work so closing encounters the damaged journal row.
    handle
        .database
        .start_operation(coven_database::NewOperation {
            kind: "create-circle".into(),
            data: b"malformed operation".to_vec(),
            started_by: "create_circle".into(),
        })
        .await
        .unwrap();
    let clone = handle.clone();
    assert!(matches!(
        handle.close().await,
        Err(DbError::OperationWorker(_))
    ));
    assert_eq!(keys.closes.load(Ordering::SeqCst), 1);
    assert_eq!(identity.closes.load(Ordering::SeqCst), 1);
    assert!(keys.keys.lock().unwrap().is_none());
    assert!(identity.keys.lock().unwrap().is_none());
    assert!(matches!(
        clone.read(|_| Ok(())).await,
        Err(CovenError::Database(DbError::StoreClosed))
    ));
}
