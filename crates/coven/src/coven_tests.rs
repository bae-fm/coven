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
        .open(first.id(), |_| ())
        .await
        .unwrap();
    let b = builder(&app, layout.clone())
        .open(second.id(), |_| ())
        .await
        .unwrap();
    assert!(matches!(
        builder(&app, layout.clone()).open(first.id(), |_| ()).await,
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
    assert!(live
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        .is_empty());
    a.write(|sql| {
        sql.execute("INSERT INTO notes VALUES(?1,?2)", ("a", "Paint colors"))?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        ["Paint colors"]
    );
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
    assert!(losses
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        .is_empty());
    a.dismiss_lost_values(&[]).await.unwrap();
    let mut by_id = a.subscribe_reconfigurable("a".to_owned(), |id, sql| {
        Ok(sql.query("SELECT title FROM notes WHERE id=?1", [id], |r| {
            r.get::<_, String>(0)
        })?)
    });
    let requests = by_id.requests();
    assert_eq!(
        by_id
            .next()
            .await
            .expect("the live query answers")
            .result
            .unwrap(),
        ["Paint colors"]
    );
    let revision = requests.set("missing".into()).unwrap();
    let event = by_id.next().await.expect("the live query answers");
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
        live.next().await.expect("the live query answers"),
        Err(CovenError::Database(DbError::StoreClosed))
    ));
    assert!(matches!(
        app.discard_store(&first).await,
        Err(StoreDeletionError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    reader.close().await.unwrap();
    let a = builder(&app, layout.clone())
        .open(first.id(), |_| ())
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
            name: "Work".into(),
            unfinished_replacement: None,
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
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    let member = identity(&app, directory.id()).member();
    handle.set_host_secret("token", "the app's token").unwrap();
    handle.set_host_secret("device-id", "app secret").unwrap();
    handle.set_host_secret("discarded", "discard me").unwrap();
    handle.delete_host_secret("discarded").unwrap();
    handle.close().await.unwrap();
    let handle = builder(&app, layout.clone())
        .open(directory.id(), |_| ())
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
        .open(directory.id(), |_| ())
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
        .open(directory.id(), |_| ())
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
        .open(directory.id(), |_| ())
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
    let reopened = builder().open(directory.id(), |_| ()).await.unwrap();
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
    let handle = builder(&app, layout).open(store, |_| ()).await.unwrap();
    assert_eq!(handle.device_id(), device);
    handle.close().await.unwrap();
}

/// Whether this device still keeps any entry of its own for `store`: its
/// device id, member keys, store keys or provider credentials.
fn device_entries_remain(app: &TestCoven, store: StoreId) -> bool {
    use coven_crypto::custody::KeyringCustody;
    let keychain = Arc::new(app.store_keychain(store));
    keychain.device_id().unwrap().is_some()
        || keychain.storage_credentials().unwrap().is_some()
        || MemberKeyCustody::unlock(&KeyringCustody::<MemberIdentity>::new(keychain.clone()))
            .unwrap()
            .is_some()
        || StoreKeyCustody::unlock(&KeyringCustody::<StoreKeyring>::new(keychain))
            .unwrap()
            .is_some()
}

/// The person's saved restore code and S3 key for `store`.
fn person_entries(app: &TestCoven, store: StoreId) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let person = app.person_entries(store);
    let bytes = |value: Option<SecretBytes>| value.map(|value| value.as_bytes().to_vec());
    (
        bytes(person.restore_code().unwrap()),
        bytes(person.s3_credentials().unwrap()),
    )
}

/// Carol's laptop stopped right after the swap of a reset or restore, and
/// the app discards the store instead of finishing it. A first attempt fails
/// partway through the keychain and leaves the store listed as it was; the
/// retry removes both installations, the record, the hidden directory and
/// every device-only entry, which may already name the new device. It never
/// touches the person's saved restore code and S3 key (§13.1, §19.3).
#[tokio::test]
async fn discarding_a_store_whose_replacement_stopped_after_its_swap_removes_both_copies() {
    for by in [ReplacedBy::Reset, ReplacedBy::Restore] {
        let mut network = crate::tests::Network::new(2).await;
        let (carol, interrupted) = match by {
            ReplacedBy::Reset => {
                let carol = network.devices.remove(1);
                carol.handle.close().await.unwrap();
                let interrupted = network
                    .reopen(&carol)
                    .interrupt_after_swap()
                    .reset_device(carol.store, "Carol's laptop")
                    .await;
                (carol, interrupted)
            }
            ReplacedBy::Restore => {
                let replaced = network.replace_keys_elsewhere().await;
                let (_, cancel) = tokio::sync::watch::channel(false);
                let interrupted = network
                    .reopen(&replaced.carol)
                    .interrupt_after_swap()
                    .restore_device(
                        replaced.carol.store,
                        &replaced.code,
                        Some(S3Credentials {
                            access_key_id: "member-1".into(),
                            secret_access_key: SecretText::new("secret".into()),
                        }),
                        "Carol's laptop",
                        |_| {},
                        &cancel,
                    )
                    .await;
                (replaced.carol, interrupted)
            }
        };
        assert!(matches!(interrupted, Err(BootstrapError::Cancelled)));
        let store = carol.store;
        let directory = carol.layout.store_dir(&store);
        let record = directory.unfinished_replacement().unwrap().unwrap();
        assert_eq!(record.by, by);
        // As if the keychain already named the new device when it stopped.
        carol
            .app
            .store_keychain(store)
            .set_device_id(record.reset.new_device)
            .unwrap();
        carol
            .app
            .person_entries(store)
            .set_s3_credentials(&SecretBytes::new(b"Carol's S3 key".to_vec()))
            .unwrap();
        let person = person_entries(&carol.app, store);
        assert!(person.0.is_some());
        let path = directory.database_path().parent().unwrap().to_owned();
        // The old installation waits in its archive beside the new one.
        assert!(path.join(".coven-replacement").exists());
        assert!(path
            .join(format!("discarded-device-{}", record.reset.old_device.0))
            .exists());
        // Stopped after removing the store keys, before the member keys.
        carol.app.fail_keychain_operations_after(&[3]);
        assert!(matches!(
            carol.app.discard_store(&directory).await,
            Err(StoreDeletionError::Key(_))
        ));
        assert_eq!(
            carol.layout.stores().await.unwrap()[0].unfinished_replacement,
            Some(by)
        );
        assert_eq!(directory.unfinished_replacement().unwrap(), Some(record));
        assert!(device_entries_remain(&carol.app, store));
        // Discarding reaches no person entry: refusing them changes nothing.
        carol.app.refuse_person_entry_operations(true);
        carol.app.discard_store(&directory).await.unwrap();
        carol.app.refuse_person_entry_operations(false);
        assert!(!path.exists());
        assert!(carol.layout.stores().await.unwrap().is_empty());
        assert!(!device_entries_remain(&carol.app, store));
        assert_eq!(person_entries(&carol.app, store), person);
        carol.app.discard_store(&directory).await.unwrap();
        network.close().await;
    }
}
