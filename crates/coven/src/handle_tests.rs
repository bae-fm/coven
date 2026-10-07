use crate::*;
use coven_database::DatabaseBuilder;
use coven_format::{
    store_log::{MemberPublicKeys, MemberRole, StoreChange, StoreLogEntry},
    value::EntryPositions,
};
use coven_merge::Timestamp;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
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
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
}

#[tokio::test]
async fn kept_keys_open_old_values_across_reopen_and_forgetting_preserves_identity() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let ids = Arc::new(UuidIds);
    let directory = app
        .create_store(&layout, "keys", ids.clone())
        .await
        .unwrap();
    let old = KeyId(ids.new_id());
    let mut keys = StoreKeyring::new(StoreKey::generate(old).unwrap());
    let sealed = keys
        .seal_app_data(old, b"private note", b"notes/a")
        .unwrap();
    keys.insert_store_key(StoreKey::generate(KeyId(ids.new_id())).unwrap())
        .unwrap();
    app.keep_store_keys(&directory, &keys).unwrap();
    let handle = builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    assert!(matches!(
        handle.seal_app_data(b"new", b"notes/a").await,
        Err(CovenError::Seal(SealError::NoCurrentStoreKey))
    ));
    handle.initialize_identity().unwrap();
    handle.set_host_secret("token", "secret").unwrap();
    assert_eq!(
        handle.open_app_data(&sealed, b"notes/a").unwrap(),
        b"private note"
    );
    assert!(matches!(
        handle.open_app_data(&sealed, b"notes/b"),
        Err(SealError::Crypto(CryptoError::Authentication))
    ));
    handle.close().await.unwrap();
    let handle = builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    let reader = builder(&app, layout.clone())
        .open_read_only(directory.id())
        .await
        .unwrap();
    assert_eq!(
        reader.open_app_data(&sealed, b"notes/a").unwrap(),
        b"private note"
    );
    app.fail_next_keychain_operation();
    assert!(handle.forget_store_keys().await.is_err());
    assert_eq!(
        handle.open_app_data(&sealed, b"notes/a").unwrap(),
        b"private note"
    );
    handle.forget_store_keys().await.unwrap();
    assert!(matches!(
        handle.open_app_data(&sealed, b"notes/a"),
        Err(SealError::NoStoreKeys)
    ));
    assert!(matches!(
        reader.open_app_data(&sealed, b"notes/a"),
        Err(SealError::NoStoreKeys)
    ));
    assert!(matches!(
        handle.initialize_identity(),
        Err(IdentityError::AlreadyInitialized)
    ));
    assert_eq!(
        handle.host_secret("token").unwrap().as_deref(),
        Some("secret")
    );
    reader.close().await.unwrap();
    assert!(matches!(
        reader.open_app_data(&sealed, b"notes/a"),
        Err(SealError::Custody(KeyError::StoreClosed))
    ));
    handle.close().await.unwrap();
    app.keep_store_keys(&directory, &keys).unwrap();
    app.delete_store(&directory, &["token"]).await.unwrap();
    // Deletion removes custody even when no directory remains to open.
    app.fail_next_keychain_operation();
    assert!(app.delete_store(&directory, &[]).await.is_err());
    app.delete_store(&directory, &[]).await.unwrap();
}

struct Keys {
    keys: Mutex<Option<StoreKeyring>>,
    unlocks: AtomicUsize,
}
impl StoreKeyCustody for Keys {
    fn unlock(&self) -> Result<Option<StoreKeyring>, KeyError> {
        self.unlocks.fetch_add(1, Ordering::SeqCst);
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
}

struct Identity {
    keys: Mutex<Option<MemberKeys>>,
    unlocks: AtomicUsize,
}
impl MemberKeyCustody for Identity {
    fn unlock(&self) -> Result<Option<MemberKeys>, KeyError> {
        self.unlocks.fetch_add(1, Ordering::SeqCst);
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
}

#[tokio::test]
async fn opening_and_sql_do_not_unlock_custody_and_callback_failures_roll_back() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let directory = app
        .create_store(&layout, "lazy", Arc::new(UuidIds))
        .await
        .unwrap();
    let keys = Arc::new(Keys {
        keys: Mutex::new(None),
        unlocks: AtomicUsize::new(0),
    });
    let identity = Arc::new(Identity {
        keys: Mutex::new(None),
        unlocks: AtomicUsize::new(0),
    });
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
    assert_eq!(keys.unlocks.load(Ordering::SeqCst), 0);
    assert_eq!(identity.unlocks.load(Ordering::SeqCst), 0);
    handle.initialize_identity().unwrap();
    assert_eq!(identity.unlocks.load(Ordering::SeqCst), 1);
    assert!(matches!(
        handle.open_app_data(b"", b"notes/a"),
        Err(SealError::NoStoreKeys)
    ));
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

fn key_builder(app: &TestCoven, layout: StoreLayout) -> CovenBuilder {
    app.builder(layout.clone())
        .synced_tables(Vec::new())
        .migrations(Vec::new())
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
}

// Populate the input through the public replay and database APIs, then let the
// app open the store. No test writes coven's tables or computes replay results.
async fn apply_entry(
    directory: &StoreDir,
    author: &MemberPublicKeys,
    number: u64,
    change: StoreChange,
) {
    let database = DatabaseBuilder::new(directory.clone())
        .synced_tables(Vec::new())
        .migrations(Vec::new())
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
        .open()
        .await
        .unwrap();
    let device = directory.settings().unwrap().device_id;
    let entry = StoreLogEntry {
        position: EntryId { device, number },
        timestamp: Timestamp::new(number, 0, device).unwrap(),
        author: author.signing.clone(),
        had_read: EntryPositions(Vec::new()),
        change,
    };
    let (entry, replay) = coven_sync::replay_entry(&database.store_log().await.unwrap(), entry);
    database.apply_store_log(entry, replay).await.unwrap();
    database.close().await.unwrap();
}

fn member() -> MemberPublicKeys {
    let keys = MemberKeys::generate().unwrap();
    MemberPublicKeys {
        signing: keys.member_id(),
        sealing: keys.sealing_public_key(),
    }
}

#[tokio::test]
async fn app_data_uses_the_replayed_key_and_opens_both_generations() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let directory = app
        .create_store(&layout, "keys", Arc::new(UuidIds))
        .await
        .unwrap();
    // The new key sorts before the old one; neither UUID order nor custody
    // insertion order determines which key seals new data.
    let old = KeyId(uuid::Uuid::from_u128(99));
    let new = KeyId(uuid::Uuid::from_u128(1));
    let old_keys = StoreKeyring::new(StoreKey::generate(old).unwrap());
    let new_key = StoreKey::generate(new).unwrap();
    let new_keys = StoreKeyring::new(new_key.clone());
    let mut keys = old_keys.clone();
    keys.insert_store_key(new_key).unwrap();
    app.keep_store_keys(&directory, &keys).unwrap();
    let handle = key_builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    assert!(matches!(
        handle.seal_app_data(b"unselected", b"notes/a").await,
        Err(CovenError::Seal(SealError::NoCurrentStoreKey))
    ));
    handle.close().await.unwrap();

    let admin = member();
    apply_entry(
        &directory,
        &admin,
        1,
        StoreChange::CreateStore {
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "fixture-access-key".into(),
            },
            store: directory.id(),
            name: "keys".into(),
            admin: admin.clone(),
            key: old,
            device_name: "phone".into(),
        },
    )
    .await;
    let handle = key_builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    let sealed_old = handle
        .seal_app_data(b"old value", b"notes/a")
        .await
        .unwrap();
    assert_eq!(
        old_keys.open_app_data(&sealed_old, b"notes/a").unwrap(),
        b"old value"
    );
    handle.close().await.unwrap();

    let removed = member();
    apply_entry(
        &directory,
        &admin,
        2,
        StoreChange::AddMember {
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "fixture-access-key".into(),
            },
            keys: removed.clone(),
            role: MemberRole::Member,
        },
    )
    .await;
    apply_entry(
        &directory,
        &admin,
        3,
        StoreChange::RemoveMember {
            member: removed.signing.clone(),
            key: new,
            circle_keys: Vec::new(),
        },
    )
    .await;
    // This entry is already in place, so the key it names never becomes current.
    let ignored = KeyId(uuid::Uuid::from_u128(200));
    keys.insert_store_key(StoreKey::generate(ignored).unwrap())
        .unwrap();
    apply_entry(
        &directory,
        &admin,
        4,
        StoreChange::RemoveMember {
            member: removed.signing,
            key: ignored,
            circle_keys: Vec::new(),
        },
    )
    .await;
    app.keep_store_keys(&directory, &keys).unwrap();
    let handle = key_builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    let sealed_new = handle
        .seal_app_data(b"new value", b"notes/a")
        .await
        .unwrap();
    assert_eq!(
        new_keys.open_app_data(&sealed_new, b"notes/a").unwrap(),
        b"new value"
    );
    for (sealed, plaintext) in [(&sealed_old, b"old value"), (&sealed_new, b"new value")] {
        assert_eq!(handle.open_app_data(sealed, b"notes/a").unwrap(), plaintext);
        assert!(matches!(
            handle.open_app_data(sealed, b"notes/b"),
            Err(SealError::Crypto(CryptoError::Authentication))
        ));
    }
    let reader = key_builder(&app, layout.clone())
        .open_read_only(directory.id())
        .await
        .unwrap();
    assert_eq!(
        reader.open_app_data(&sealed_old, b"notes/a").unwrap(),
        b"old value"
    );
    assert_eq!(
        reader.open_app_data(&sealed_new, b"notes/a").unwrap(),
        b"new value"
    );
    reader.close().await.unwrap();

    app.fail_next_keychain_operation();
    assert!(matches!(
        handle.seal_app_data(b"unavailable", b"notes/a").await,
        Err(CovenError::Seal(SealError::Custody(_)))
    ));
    app.keep_store_keys(&directory, &old_keys).unwrap();
    assert!(matches!(
        handle.seal_app_data(b"missing", b"notes/a").await,
        Err(CovenError::Seal(SealError::Key(MaterialError::UnknownStoreKey(key)))) if key == new
    ));
    handle.forget_store_keys().await.unwrap();
    assert!(matches!(
        handle.seal_app_data(b"forgotten", b"notes/a").await,
        Err(CovenError::Seal(SealError::NoStoreKeys))
    ));
    app.keep_store_keys(&directory, &keys).unwrap();
    let sealed = handle.seal_app_data(b"restored", b"notes/a").await.unwrap();
    assert_eq!(
        new_keys.open_app_data(&sealed, b"notes/a").unwrap(),
        b"restored"
    );
    handle.close().await.unwrap();
    assert!(matches!(
        handle.seal_app_data(b"closed", b"notes/a").await,
        Err(CovenError::Database(DbError::StoreClosed))
    ));
}
