use crate::*;
use coven_foundation::files::{StoreDirAccess, StoreFile};
use coven_storage::{test_utils::MemoryStorage, Storage};
use std::sync::Arc;

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
async fn application_lifecycle_two_stores_lock_read_only_live_query_and_reopen() {
    set_keyring_service("com.example.notes").unwrap();
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let ids: IdSourceRef = Arc::new(SequentialIds::new());
    let first = app
        .create_closed_store(&layout, "Household", ids.clone())
        .await
        .unwrap();
    let second = app.create_closed_store(&layout, "Work", ids).await.unwrap();
    assert_eq!(
        layout
            .stores()
            .await
            .unwrap()
            .iter()
            .map(|s| &s.name)
            .collect::<Vec<_>>(),
        [&"Household", &"Work"]
    );
    let a = builder(&app, layout.clone())
        .open(first.id())
        .await
        .unwrap();
    let b = builder(&app, layout.clone())
        .open(second.id())
        .await
        .unwrap();
    assert!(matches!(
        builder(&app, layout.clone()).open(first.id()).await,
        Err(CovenError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    assert!(matches!(
        app.discard_store(&first).await,
        Err(StoreDeletionError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    let reader = builder(&app, layout.clone())
        .open_read_only(first.id())
        .await
        .unwrap();
    let mut live = a.subscribe(|sql| {
        Ok(
            sql.query("SELECT title FROM notes ORDER BY title", [], |r| {
                r.get::<_, String>(0)
            })?,
        )
    });
    assert!(live.next().await.unwrap().is_empty());
    a.write(|sql| {
        sql.execute("INSERT INTO notes VALUES(?1,?2)", ("a", "Paint colors"))?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(live.next().await.unwrap(), ["Paint colors"]);
    let titles = reader
        .read(|sql| Ok(sql.query("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?))
        .process(|mut titles| {
            titles.sort();
            Ok(titles)
        })
        .await
        .unwrap();
    assert_eq!(titles, ["Paint colors"]);
    assert_eq!(
        b.read(|sql| Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?))
            .await
            .unwrap(),
        0
    );
    assert!(a.lost_values().await.unwrap().is_empty());
    let mut losses = a.subscribe_lost_values();
    assert!(losses.next().await.unwrap().is_empty());
    a.dismiss_lost_values(&[]).await.unwrap();
    let mut by_id = a.subscribe_reconfigurable("a".to_owned(), |id, sql| {
        Ok(sql.query("SELECT title FROM notes WHERE id=?1", [id], |r| {
            r.get::<_, String>(0)
        })?)
    });
    let requests = by_id.requests();
    assert_eq!(by_id.next().await.result.unwrap(), ["Paint colors"]);
    let revision = requests.set("missing".into()).unwrap();
    let event = by_id.next().await;
    assert_eq!(event.revision, revision);
    assert!(event.result.unwrap().is_empty());
    let clone = a.clone();
    a.close().await.unwrap();
    assert!(matches!(
        clone.read(|_| Ok(())).await,
        Err(CovenError::Database(DbError::StoreClosed))
    ));
    assert!(matches!(
        clone.host_secret("token"),
        Err(KeyError::StoreClosed)
    ));
    assert!(matches!(
        live.next().await,
        Err(CovenError::Database(DbError::StoreClosed))
    ));
    assert!(matches!(
        app.discard_store(&first).await,
        Err(StoreDeletionError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    reader.close().await.unwrap();
    let a = builder(&app, layout.clone())
        .open(first.id())
        .await
        .unwrap();
    assert_eq!(
        a.read(|sql| Ok(sql.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?))
            .await
            .unwrap(),
        "Paint colors"
    );
    a.close().await.unwrap();
    b.close().await.unwrap();
    app.discard_store(&first).await.unwrap();
    app.discard_store(&first).await.unwrap();
    assert_eq!(
        layout.stores().await.unwrap(),
        vec![StoreInfo {
            id: second.id(),
            name: "Work".into()
        }]
    );
}

#[tokio::test]
async fn host_secrets_identity_and_failed_deletion_survive_reopen() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let directory = app
        .create_closed_store(&layout, "private", Arc::new(UuidIds))
        .await
        .unwrap();
    let key_id = KeyId(UuidIds.new_id());
    let keys = StoreKeyring::new(StoreKey::generate(key_id).unwrap());
    app.keep_store_keys(&directory, &keys).unwrap();
    let handle = builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    let member = identity(&app, directory.id()).member();
    handle.set_host_secret("token", "the app's token").unwrap();
    handle.set_host_secret("device-id", "app secret").unwrap();
    handle.set_host_secret("discarded", "discard me").unwrap();
    handle.delete_host_secret("discarded").unwrap();
    handle.close().await.unwrap();
    let handle = builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    assert_eq!(identity(&app, directory.id()).member(), member);
    assert_eq!(
        handle.host_secret("token").unwrap().as_deref(),
        Some("the app's token")
    );
    handle.close().await.unwrap();
    std::fs::write(directory.database_path(), b"damaged database").unwrap();
    app.fail_next_keychain_operation();
    assert!(matches!(
        app.discard_store(&directory).await,
        Err(StoreDeletionError::Key(_))
    ));
    assert!(
        matches!(layout.stores().await, Err(StoreLayoutError::Store {
        store, source: CovenError::Database(DbError::DamagedDatabase),
    }) if store == directory.id())
    );
    assert_eq!(directory.settings().unwrap().id, directory.id());
    app.discard_store(&directory).await.unwrap();
    assert!(layout.stores().await.unwrap().is_empty());
    // Recreate exactly this store id to prove deletion removed all its keychain entries.
    struct SameStore(std::sync::Mutex<Option<StoreId>>, UuidIds);
    impl IdSource for SameStore {
        fn new_id(&self) -> uuid::Uuid {
            match self.0.lock().unwrap().take() {
                Some(id) => id.0,
                None => self.1.new_id(),
            }
        }
    }
    let directory = app
        .create_closed_store(
            &layout,
            "replacement",
            Arc::new(SameStore(
                std::sync::Mutex::new(Some(directory.id())),
                UuidIds,
            )),
        )
        .await
        .unwrap();
    let replacement = builder(&app, layout.clone())
        .open(directory.id())
        .await
        .unwrap();
    for name in ["token", "device-id", "discarded"] {
        assert_eq!(replacement.host_secret(name).unwrap(), None);
    }
    assert_eq!(
        replacement.store_key_state().unwrap(),
        StoreKeyState::Locked
    );
    assert_ne!(identity(&app, directory.id()).member(), member);
    replacement.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_device_identity_prevents_publication() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    app.fail_next_keychain_operation();
    assert!(matches!(
        app.create_closed_store(&layout, "refused", Arc::new(UuidIds))
            .await,
        Err(StoreCreationError::Store(CovenError::Key(_)))
    ));
    assert!(layout.stores().await.unwrap().is_empty());
}

#[tokio::test]
/// A local migration never converts the queue: the waiting write keeps its
/// original schema and the callback serves only received late writes (§18.6).
async fn a_breaking_migration_keeps_the_waiting_write_as_authored() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let directory = app
        .create_closed_store(&layout, "migrations", Arc::new(UuidIds))
        .await
        .unwrap();
    let first = || {
        Migration::addition(
            1,
            "initial",
            "CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL)",
        )
    };
    let table = || vec![SyncedTable::new("attachments", RowIdentity::SharedKey)];
    let handle = app
        .builder(layout.clone())
        .synced_tables(table())
        .migrations(vec![first()])
        .open(directory.id())
        .await
        .unwrap();
    handle
        .write(|sql| {
            sql.execute("INSERT INTO attachments VALUES('a','Swatches')", [])?;
            Ok(())
        })
        .await
        .unwrap();
    handle.close().await.unwrap();
    let converted = Arc::new(std::sync::Mutex::new(Vec::new()));
    let output = converted.clone();
    let migration = Migration::breaking(
        2,
        "rename_attachment_title",
        "ALTER TABLE attachments RENAME COLUMN title TO name",
        crate::OldWrites::Convert(std::sync::Arc::new(move |change| {
            if change.table == "attachments" {
                change.rename_column("title", "name");
            }
            output.lock().unwrap().push(change.clone());
            Ok(())
        })),
    );
    let handle = app
        .builder(layout.clone())
        .synced_tables(table())
        .migrations(vec![first(), migration])
        .open(directory.id())
        .await
        .unwrap();
    assert!(converted.lock().unwrap().is_empty());
    assert_eq!(
        handle
            .read(
                |sql| Ok(sql.query_row("SELECT name FROM attachments", [], |r| r
                    .get::<_, String>(0))?)
            )
            .await
            .unwrap(),
        "Swatches"
    );
    handle.close().await.unwrap();
}

fn identity(app: &TestCoven, id: StoreId) -> MemberIdentity {
    MemberKeyCustody::unlock(&coven_crypto::custody::KeyringCustody::new(Arc::new(
        app.store_keychain(id),
    )))
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn saved_location_survives_disconnect_and_reopen_without_custody_reads() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let builder = || {
        app.builder(layout.clone())
            .synced_tables(vec![])
            .migrations(vec![])
            .storage_connector(storage.clone())
    };
    let handle = builder().create_store("Household").await.unwrap();
    let directory = layout.store_dir(&handle.store_id());
    assert_eq!(handle.storage_config().await.unwrap(), None);
    assert_eq!(Coven::storage_config(&directory).await.unwrap(), None);
    let setup = handle
        .setup_s3_storage(
            storage.config(),
            "Ana's phone",
            "key".into(),
            SecretText::new("secret".into()),
        )
        .await
        .unwrap();
    let expected = Some(setup.connection.storage);
    assert_eq!(handle.storage_config().await.unwrap(), expected);
    let mut wrong = storage.config();
    let StorageConfig::S3 { bucket, .. } = &mut wrong else {
        unreachable!()
    };
    *bucket = "another-bucket".into();
    assert!(matches!(
        handle
            .setup_s3_storage(
                wrong,
                "Ana's phone",
                "key".into(),
                SecretText::new("secret".into())
            )
            .await,
        Err(StorageSetupError::LocationChangeUnsupported)
    ));
    assert_eq!(handle.storage_config().await.unwrap(), expected);
    handle.disconnect_storage().await.unwrap();
    assert_eq!(handle.storage_config().await.unwrap(), expected);
    handle.close().await.unwrap();
    assert!(matches!(
        handle.storage_config().await,
        Err(SyncError::SecureStorage(KeyError::StoreClosed))
    ));
    let reopened = builder().open(directory.id()).await.unwrap();
    assert_eq!(reopened.storage_config().await.unwrap(), expected);
    reopened.close().await.unwrap();
    // Neither a damaged database nor unavailable custody prevents showing the location.
    std::fs::write(directory.database_path(), b"damaged").unwrap();
    app.fail_next_keychain_operation();
    assert_eq!(Coven::storage_config(&directory).await.unwrap(), expected);
    assert!(matches!(
        app.discard_store(&directory).await,
        Err(StoreDeletionError::Key(_))
    ));
    directory
        .owned_file(StoreFile::StorageSettings)
        .replace(b"malformed")
        .unwrap();
    assert_eq!(
        Coven::storage_config(&directory)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Encoding
    );
    app.discard_store(&directory).await.unwrap();
}

#[tokio::test]
async fn device_id_is_available_before_setup_and_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let handle = builder(&app, layout.clone())
        .create_store("Household")
        .await
        .unwrap();
    let store = handle.store_id();
    let device = handle.device_id();
    assert_eq!(handle.clone().device_id(), device);
    assert!(handle.members().await.unwrap().is_empty());
    handle.close().await.unwrap();
    assert_eq!(handle.device_id(), device);
    let handle = builder(&app, layout).open(store).await.unwrap();
    assert_eq!(handle.device_id(), device);
    handle.close().await.unwrap();
}
