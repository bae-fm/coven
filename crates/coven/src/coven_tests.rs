use crate::*;
use coven_foundation::clock::HANG_GUARD;
use coven_foundation::files::{StoreDirAccess, StoreFile};
use coven_storage::{test_utils::MemoryStorage, Storage};
use std::sync::Arc;

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
async fn application_lifecycle_two_stores_lock_read_only_live_query_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    let ids: IdSourceRef = Arc::new(SequentialIds::new());
    let first = app.create_closed_store("Household", ids.clone()).await.unwrap();
    let second = app.create_closed_store("Work", ids).await.unwrap();
    assert_eq!(
        app.readable_stores().await.iter().map(|s| &s.name).collect::<Vec<_>>(),
        [&"Household", &"Work"]
    );
    let a = builder(&app).open(first.id(), |_| ()).await.unwrap();
    let b = builder(&app).open(second.id(), |_| ()).await.unwrap();
    assert!(matches!(
        builder(&app).open(first.id(), |_| ()).await,
        Err(CovenError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    assert!(matches!(
        app.discard_store(&first).await,
        Err(StoreDeletionError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    let reader = builder(&app).open_read_only(first.id()).await.unwrap();
    let mut live = a.subscribe(|sql| {
        Ok(sql.query("SELECT title FROM notes ORDER BY title", [], |r| r.get::<_, String>(0))?)
    });
    assert!(live.next().await.expect("the live query answers").unwrap().is_empty());
    a.write(|sql| {
        sql.execute("INSERT INTO notes VALUES(?1,?2)", ("a", "Paint colors"))?;
        Ok::<_, CovenError>(())
    })
    .await
    .unwrap();
    assert_eq!(live.next().await.expect("the live query answers").unwrap(), ["Paint colors"]);
    let titles = reader
        .read(|sql| {
            Ok::<_, CovenError>(
                sql.query("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?,
            )
        })
        .process(|mut titles| {
            titles.sort();
            Ok(titles)
        })
        .await
        .unwrap();
    assert_eq!(titles, ["Paint colors"]);
    assert_eq!(
        b.read(|sql| Ok::<_, CovenError>(sql.query_row(
            "SELECT count(*) FROM notes",
            [],
            |r| r.get::<_, i64>(0)
        )?))
        .await
        .unwrap(),
        0
    );
    assert!(a.lost_values().await.unwrap().is_empty());
    let mut losses = a.subscribe_lost_values();
    assert!(losses.next().await.expect("the live query answers").unwrap().is_empty());
    a.dismiss_lost_values(&[]).await.unwrap();
    let mut by_id: crate::ReconfigurableLiveQuery<_, _, crate::CovenError> = a
        .subscribe_reconfigurable("a".to_owned(), |id, sql| {
            Ok(sql.query("SELECT title FROM notes WHERE id=?1", [id], |r| r.get::<_, String>(0))?)
        });
    let requests = by_id.requests();
    assert_eq!(
        by_id.next().await.expect("the live query answers").result.unwrap(),
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
    assert!(matches!(clone.host_secret("token"), Err(KeyError::StoreClosed)));
    assert!(matches!(
        live.next().await.expect("the live query answers"),
        Err(CovenError::Database(DbError::StoreClosed))
    ));
    assert!(matches!(
        app.discard_store(&first).await,
        Err(StoreDeletionError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    reader.close().await.unwrap();
    let a = builder(&app).open(first.id(), |_| ()).await.unwrap();
    assert_eq!(
        a.read(|sql| Ok::<_, CovenError>(
            sql.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?
        ))
        .await
        .unwrap(),
        "Paint colors"
    );
    a.close().await.unwrap();
    b.close().await.unwrap();
    app.discard_store(&first).await.unwrap();
    app.discard_store(&first).await.unwrap();
    assert_eq!(
        app.readable_stores().await,
        vec![StoreInfo { id: second.id(), name: "Work".into(), unfinished: None }]
    );
}

#[tokio::test]
async fn host_secrets_identity_and_failed_deletion_survive_reopen() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    let directory = app.create_closed_store("private", Arc::new(UuidIds)).await.unwrap();
    let key_id = KeyId(UuidIds.new_id());
    let keys = StoreKeyring::new(StoreKey::generate(key_id).unwrap());
    app.keep_store_keys(&directory, &keys).unwrap();
    let handle = builder(&app).open(directory.id(), |_| ()).await.unwrap();
    let member = identity(&app, directory.id()).member();
    handle.set_host_secret("token", "the app's token").unwrap();
    handle.set_host_secret("device-id", "app secret").unwrap();
    handle.set_host_secret("discarded", "discard me").unwrap();
    handle.delete_host_secret("discarded").unwrap();
    handle.close().await.unwrap();
    let handle = builder(&app).open(directory.id(), |_| ()).await.unwrap();
    assert_eq!(identity(&app, directory.id()).member(), member);
    assert_eq!(handle.host_secret("token").unwrap().as_deref(), Some("the app's token"));
    handle.close().await.unwrap();
    std::fs::write(directory.database_path(), b"damaged database").unwrap();
    assert!(matches!(
        app.stores().await.unwrap().as_slice(),
        [ListedStore::Unreadable { id, reason: CovenError::Database(DbError::DamagedDatabase) }]
            if *id == directory.id()
    ));
    app.fail_next_keychain_operation();
    assert!(matches!(app.discard_store(&directory).await, Err(StoreDeletionError::Key(_))));
    app.discard_store(&directory).await.unwrap();
    assert!(app.stores().await.unwrap().is_empty());
    // Recreate exactly this store id to prove deletion removed all its keychain entries.
    let directory = app
        .create_closed_store(
            "replacement",
            Arc::new(SameStore(std::sync::Mutex::new(Some(directory.id())))),
        )
        .await
        .unwrap();
    let replacement = builder(&app).open(directory.id(), |_| ()).await.unwrap();
    for name in ["token", "device-id", "discarded"] {
        assert_eq!(replacement.host_secret(name).unwrap(), None);
    }
    assert_eq!(replacement.store_key_state().unwrap(), StoreKeyState::Locked);
    assert_ne!(identity(&app, directory.id()).member(), member);
    replacement.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_device_identity_prevents_publication() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    app.fail_next_keychain_operation();
    assert!(matches!(
        app.create_closed_store("refused", Arc::new(UuidIds)).await,
        Err(StoreCreationError::Store(CovenError::Key(_)))
    ));
    assert!(app.stores().await.unwrap().is_empty());
}

#[tokio::test]
/// A local migration never converts the queue: the waiting write keeps its
/// original schema and the callback serves only received late writes (§18.6).
async fn a_breaking_migration_keeps_the_waiting_write_as_authored() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    let directory = app.create_closed_store("migrations", Arc::new(UuidIds)).await.unwrap();
    let first = || {
        Migration::addition(
            1,
            "initial",
            "CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL)",
        )
    };
    let table = || vec![SyncedTable::new("attachments", RowIdentity::SharedKey)];
    let handle = app
        .builder()
        .synced_tables(table())
        .migrations(vec![first()])
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    handle
        .write(|sql| {
            sql.execute("INSERT INTO attachments VALUES('a','Swatches')", [])?;
            Ok::<_, CovenError>(())
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
        .builder()
        .synced_tables(table())
        .migrations(vec![first(), migration])
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    assert!(converted.lock().unwrap().is_empty());
    assert_eq!(
        handle
            .read(|sql| Ok::<_, CovenError>(sql.query_row(
                "SELECT name FROM attachments",
                [],
                |r| r.get::<_, String>(0)
            )?))
            .await
            .unwrap(),
        "Swatches"
    );
    handle.close().await.unwrap();
}

fn identity(app: &Coven, id: StoreId) -> MemberIdentity {
    Custody::<MemberIdentity>::unlock(&coven_crypto::custody::KeyringCustody::new(Arc::new(
        app.store_keychain(id),
    )))
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn saved_location_survives_lost_credentials_and_reopen_without_custody_reads() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let builder = || {
        app.builder().synced_tables(vec![]).migrations(vec![]).storage_connector(storage.clone())
    };
    let handle = builder().create_store("Household").await.unwrap();
    let directory = app.store_dir(&handle.store_id());
    assert_eq!(handle.storage_config().await.unwrap(), None);
    assert_eq!(Coven::storage_config(&directory).await.unwrap(), None);
    let setup = handle
        .setup_s3_storage(
            storage.config(),
            "Ana's phone",
            "key".into(),
            SecretText::new("secret".into()),
            |_| {},
        )
        .await
        .unwrap();
    let expected = Some(setup.connection.storage);
    assert_eq!(handle.storage_config().await.unwrap(), expected);
    let mut wrong = storage.config();
    let StorageConfig::S3 { bucket, .. } = &mut wrong else { unreachable!() };
    *bucket = "another-bucket".into();
    assert!(matches!(
        handle
            .setup_s3_storage(
                wrong,
                "Ana's phone",
                "key".into(),
                SecretText::new("secret".into()),
                |_| {}
            )
            .await,
        Err(StorageSetupError::LocationChangeUnsupported)
    ));
    assert_eq!(handle.storage_config().await.unwrap(), expected);
    handle.close().await.unwrap();
    assert!(matches!(
        handle.storage_config().await,
        Err(SyncError::SecureStorage(KeyError::StoreClosed))
    ));
    app.lose_storage_credentials(directory.id()).unwrap();
    let reopened = builder().open(directory.id(), |_| ()).await.unwrap();
    assert_eq!(reopened.storage_config().await.unwrap(), expected);
    reopened.close().await.unwrap();
    // Neither a damaged database nor unavailable custody prevents showing the location.
    std::fs::write(directory.database_path(), b"damaged").unwrap();
    app.fail_next_keychain_operation();
    assert_eq!(Coven::storage_config(&directory).await.unwrap(), expected);
    app.fail_keychain_operations_after(&[]);
    directory.owned_file(StoreFile::StorageSettings).replace(b"malformed").unwrap();
    assert!(matches!(
        Coven::storage_config(&directory).await,
        Err(SyncError::Storage(error)) if error.failure() == StorageFailure::Encoding
    ));
    app.discard_store(&directory).await.unwrap();
}

#[tokio::test]
async fn device_id_is_available_before_setup_and_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let handle = builder(&app).create_store("Household").await.unwrap();
    let store = handle.store_id();
    let device = handle.device_id();
    assert_eq!(handle.clone().device_id(), device);
    assert!(handle.members().await.unwrap().is_empty());
    handle.close().await.unwrap();
    assert_eq!(handle.device_id(), device);
    let handle = builder(&app).open(store, |_| ()).await.unwrap();
    assert_eq!(handle.device_id(), device);
    handle.close().await.unwrap();
}

/// Whether this device still keeps any entry of its own for `store`: its
/// device id, member keys, store keys or provider credentials.
fn device_entries_remain(app: &Coven, store: StoreId) -> bool {
    use coven_crypto::custody::KeyringCustody;
    let keychain = Arc::new(app.store_keychain(store));
    keychain.device_id().unwrap().is_some()
        || keychain.storage_credentials().unwrap().is_some()
        || Custody::<MemberIdentity>::unlock(&KeyringCustody::<MemberIdentity>::new(
            keychain.clone(),
        ))
        .unwrap()
        .is_some()
        || Custody::<StoreKeyring>::unlock(&KeyringCustody::<StoreKeyring>::new(keychain))
            .unwrap()
            .is_some()
}

/// The person's saved restore code and S3 key for `store`.
fn person_entries(app: &Coven, store: StoreId) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let person = app.person_entries(store);
    let bytes = |value: Option<SecretBytes>| value.map(|value| value.as_bytes().to_vec());
    (bytes(person.restore_code().unwrap()), bytes(person.s3_credentials().unwrap()))
}

/// Carol's laptop stopped right after the swap of a reset or restore, and
/// the app discards the store instead of finishing it. A first attempt fails
/// partway through the keychain, the store already hidden; the next listing
/// finishes it, removing both installations, the record, the hidden
/// directory and every device-only entry, which may already name the new
/// device. It never touches the person's saved restore code and S3 key
/// (§13.1, §19.3).
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
                    .reset_device(carol.store, "Carol's laptop", |_| ())
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
                        |_| Ok(()),
                        |_| {},
                        &cancel,
                    )
                    .await;
                (replaced.carol, interrupted)
            }
        };
        assert!(matches!(interrupted, Err(BootstrapError::Cancelled)));
        let store = carol.store;
        let directory = carol.app.store_dir(&store);
        let record = directory.unfinished_replacement().unwrap().unwrap();
        assert_eq!(record.by, by);
        // As if the keychain already named the new device when it stopped.
        carol.app.store_keychain(store).set_device_id(record.reset.new_device).unwrap();
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
        assert!(path.join(format!("discarded-device-{}", record.reset.old_device.0)).exists());
        // Stopped after removing the store keys, before the member keys:
        // the store is hidden already, and the next listing finishes it.
        carol.app.fail_keychain_operations_after(&[3]);
        assert!(matches!(
            carol.app.discard_store(&directory).await,
            Err(StoreDeletionError::Key(_))
        ));
        assert!(!path.exists());
        assert!(device_entries_remain(&carol.app, store));
        // Discarding reaches no person entry: refusing them changes nothing.
        carol.app.refuse_person_entry_operations(true);
        assert!(carol.app.stores().await.unwrap().is_empty());
        carol.app.refuse_person_entry_operations(false);
        assert!(!path.exists());
        assert!(carol.app.stores().await.unwrap().is_empty());
        assert!(!device_entries_remain(&carol.app, store));
        assert_eq!(person_entries(&carol.app, store), person);
        carol.app.discard_store(&directory).await.unwrap();
        network.close().await;
    }
}

/// Discarding a store whose last handle was dropped, or whose close has
/// begun, while blocking work still holds its lock waits for that close to
/// finish instead of refusing the store as open, as reopening does.
#[tokio::test]
async fn discarding_waits_after_the_last_handle_drops_or_close_starts() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    for close in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let app = Coven::in_memory(root.path().into());
        let directory = app.create_closed_store("Discard", Arc::new(UuidIds)).await.unwrap();
        let handle = builder(&app).open(directory.id(), |_| ()).await.unwrap();
        let (entered, entry) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let mut read = Box::pin(handle.read(move |_| {
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
            let mut closing = Box::pin(handle.close());
            poll_fn(|cx| {
                assert!(closing.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(closing);
        }
        drop(handle);
        let mut discard = Box::pin(app.discard_store(&directory));
        poll_fn(|cx| {
            assert!(discard.as_mut().poll(cx).is_pending(), "discarding refused a closing store");
            Poll::Ready(())
        })
        .await;
        release.send(()).unwrap();
        discard.await.unwrap();
        assert!(app.stores().await.unwrap().is_empty());
    }
}

/// A location read whose runtime shut down is a shutdown, not a file failure (E15).
#[test]
fn a_closed_stores_location_read_after_its_runtime_shut_down_is_a_shutdown() {
    use coven_foundation::task::{Shutdown, ShutdownOutcome};
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let handle = runtime.handle().clone();
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let directory = runtime
        .block_on(app.create_closed_store("Household", Arc::new(SequentialIds::new())))
        .unwrap();
    drop(runtime);
    let outlived = tokio::runtime::Builder::new_current_thread().build().unwrap();
    outlived.block_on(async {
        let _entered = handle.enter();
        assert!(matches!(
            Coven::storage_config(&directory).await,
            Err(SyncError::Shutdown(Shutdown {
                operation: "read the storage settings",
                outcome: ShutdownOutcome::NotRun,
            }))
        ));
    });
}

/// Listing the stores when the folder that holds them can't be read fails
/// with the file failure, not as an empty list (E1).
#[tokio::test]
async fn listing_stores_whose_folder_cant_be_read_fails_with_its_file_error() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    std::fs::write(root.path().join("stores"), b"not a folder").unwrap();
    assert!(matches!(
        app.stores().await,
        Err(StoreListError::File(FileError::Io { operation: "list store directories", .. }))
    ));
}

/// Calls on a runtime that shut down report it, naming whether anything ran,
/// and leave the stores as they were (E15). Opening's first blocking step is
/// taking the store's lock, so that is where its shutdown shows.
#[test]
fn calls_on_a_runtime_that_shut_down_are_shutdowns() {
    use coven_foundation::task::{Shutdown, ShutdownOutcome};
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let handle = runtime.handle().clone();
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let directory = runtime
        .block_on(app.create_closed_store("Household", Arc::new(SequentialIds::new())))
        .unwrap();
    drop(runtime);
    let outlived = tokio::runtime::Builder::new_current_thread().build().unwrap();
    outlived.block_on(async {
        let _entered = handle.enter();
        let opened = builder(&app).open(directory.id(), |_| ()).await;
        assert!(
            matches!(
                &opened,
                Err(CovenError::Lock(StoreLockError::File(FileError::Shutdown(Shutdown {
                    outcome: ShutdownOutcome::NotRun,
                    ..
                }))))
            ),
            "{:?}",
            opened.map(drop)
        );
        let created = builder(&app).create_store("Work").await;
        assert!(
            matches!(created, Err(StoreCreationError::Shutdown(Shutdown { .. }))),
            "{:?}",
            created.map(drop)
        );
        let discarded = app.discard_store(&directory).await;
        assert!(
            matches!(
                discarded,
                Err(StoreDeletionError::Shutdown(Shutdown {
                    outcome: ShutdownOutcome::NotRun,
                    ..
                }))
            ),
            "{discarded:?}"
        );
    });
    let listed = tokio::runtime::Builder::new_current_thread().build().unwrap();
    listed.block_on(async {
        let stores = app.stores().await.unwrap();
        assert!(
            matches!(stores.as_slice(), [ListedStore::Readable(info)] if info.id == directory.id())
        );
    });
}

#[tokio::test]
async fn a_store_whose_database_is_damaged_is_listed_as_unreadable_beside_the_others() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    let ids: IdSourceRef = Arc::new(SequentialIds::new());
    let good = app.create_closed_store("Household", ids.clone()).await.unwrap();
    let damaged = app.create_closed_store("Work", ids).await.unwrap();
    std::fs::write(damaged.database_path(), b"damaged database").unwrap();
    let listed = app.stores().await.unwrap();
    assert!(matches!(
        listed.as_slice(),
        [
            ListedStore::Readable(StoreInfo { id: first, name, unfinished: None }),
            ListedStore::Unreadable {
                id: second,
                reason: CovenError::Database(DbError::DamagedDatabase),
            },
        ] if *first == good.id() && name == "Household" && *second == damaged.id()
    ));
    assert_eq!(listed.iter().map(ListedStore::id).collect::<Vec<_>>(), [good.id(), damaged.id()]);
}

/// The entries in an app's stores directory whose names contain `store`.
fn files_named_for(app_dir: &std::path::Path, store: StoreId) -> Vec<String> {
    std::fs::read_dir(app_dir.join("stores"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.contains(&store.to_string()))
        .collect()
}

/// Creates the store with the id `store`, once.
struct SameStore(std::sync::Mutex<Option<StoreId>>);
impl IdSource for SameStore {
    fn new_id(&self) -> uuid::Uuid {
        self.0.lock().unwrap().take().map_or_else(|| UuidIds.new_id(), |id| id.0)
    }
}

/// Ana discards Household on her laptop, and the keychain refuses the first
/// of its entries' deletions. The deletion's first step has hidden the store
/// already: no listing shows a store whose entries may be gone, and the next
/// listing finishes the deletion, leaving nothing named for Household, so it
/// can be made again with the same id (§19.3, E1).
#[tokio::test]
async fn a_discard_that_stops_part_way_is_hidden_and_the_listing_finishes_it() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    let directory = app.create_closed_store("Household", Arc::new(UuidIds)).await.unwrap();
    let store = directory.id();
    assert!(device_entries_remain(&app, store));
    app.fail_next_keychain_operation();
    assert!(matches!(app.discard_store(&directory).await, Err(StoreDeletionError::Key(_))));
    assert!(device_entries_remain(&app, store));
    assert!(app.stores().await.unwrap().is_empty());
    assert!(!device_entries_remain(&app, store));
    assert_eq!(files_named_for(root.path(), store), Vec::<String>::new());
    let again = app
        .create_closed_store("Household", Arc::new(SameStore(std::sync::Mutex::new(Some(store)))))
        .await
        .unwrap();
    assert_eq!(again.id(), store);
}

/// Ana discards Household, and removing its directory fails once its
/// entries are gone, as a crash there would leave it: the directory under
/// its deletion name and the lock files beside it. Nothing would ever remove
/// them, and making Household again with its id would be refused for good.
/// The next listing finishes the deletion (§19.3, E1).
#[cfg(unix)]
#[tokio::test]
async fn a_discard_stopped_after_its_entries_is_finished_by_the_listing() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    let directory = app.create_closed_store("Household", Arc::new(UuidIds)).await.unwrap();
    let store = directory.id();
    let files = directory.area_path(coven_foundation::files::FileArea::AppProvided);
    std::fs::write(files.join("kept"), b"bytes").unwrap();
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o555)).unwrap();
    assert!(matches!(app.discard_store(&directory).await, Err(StoreDeletionError::File(_))));
    assert!(!device_entries_remain(&app, store));
    let deleting = root.path().join("stores").join(format!(".coven-delete-{store}"));
    std::fs::set_permissions(deleting.join("files"), std::fs::Permissions::from_mode(0o755))
        .unwrap();
    assert!(!files_named_for(root.path(), store).is_empty());
    assert!(app.stores().await.unwrap().is_empty());
    assert_eq!(files_named_for(root.path(), store), Vec::<String>::new());
    let again = app
        .create_closed_store("Household", Arc::new(SameStore(std::sync::Mutex::new(Some(store)))))
        .await
        .unwrap();
    assert_eq!(again.id(), store);
}

/// A read-only handle's `close` dropped after its first poll has already
/// made every clone refuse calls with `StoreClosed`, and its closing goes on
/// as its own task (E1, E15).
#[tokio::test]
async fn cancelling_a_read_only_close_still_closes_every_clone() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    let directory = app.create_closed_store("Household", Arc::new(UuidIds)).await.unwrap();
    builder(&app).open(directory.id(), |_| ()).await.unwrap().close().await.unwrap();
    let reader = builder(&app).open_read_only(directory.id()).await.unwrap();
    let clone = reader.clone();
    {
        let close = reader.close();
        tokio::pin!(close);
        poll_fn(|cx| {
            assert!(close.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    for handle in [&reader, &clone] {
        let read = tokio::time::timeout(HANG_GUARD, handle.read(|_| Ok(()))).await.unwrap();
        assert!(matches!(read, Err(CovenError::Database(DbError::StoreClosed))), "{read:?}");
    }
}

/// A read-only close dropped after its first poll still closes every
/// clone's connections and releases the readers' lock, and a deletion in
/// this process waits for that release instead of refusing (E1, E15).
#[tokio::test]
async fn a_dropped_read_only_close_releases_its_connections_and_readers_lock() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().to_owned());
    let directory = app.create_closed_store("Household", Arc::new(UuidIds)).await.unwrap();
    builder(&app).open(directory.id(), |_| ()).await.unwrap().close().await.unwrap();
    let reader = builder(&app).open_read_only(directory.id()).await.unwrap();
    let _clone = reader.clone();
    assert!(matches!(directory.lock_for_deletion(), Err(StoreLockError::AlreadyOpen(_))));
    assert!(!crate::open_files_under(root.path()).is_empty());
    {
        let close = reader.close();
        tokio::pin!(close);
        poll_fn(|cx| {
            assert!(close.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    tokio::time::timeout(HANG_GUARD, directory.closed())
        .await
        .expect("the dropped close finishes")
        .unwrap();
    crate::assert_no_open_files_under(root.path());
    assert!(directory.lock_for_deletion().unwrap().is_some());
}

/// Ana's app lists the saved restore codes, and the keychain's read panics.
/// A caller still waiting gets the bug as its error; one that dropped the
/// call is gone, so the app's bug sink hears it instead. Discarding a store
/// does the same (E15).
#[tokio::test]
async fn a_coven_calls_panic_reaches_the_apps_sink_once_its_caller_stops_waiting() {
    let root = tempfile::tempdir().unwrap();
    let keychain = coven_crypto::custody::Keychain::in_memory("coven-test").unwrap();
    let bugs = crate::task::HeardBugs::shared();
    let app = Coven::with_keychain_reporting_to(root.path().into(), keychain.clone(), bugs.clone());
    keychain.panic_next_operation();
    let answered = app.saved_restore_codes().await;
    assert!(
        matches!(&answered, Err(BootstrapError::Bug(bug)) if bug.operation == "list saved restore codes"),
        "{answered:?}"
    );
    keychain.panic_next_operation();
    keychain.with_operations_held(|| dropped_at_first_poll(app.saved_restore_codes()));
    let bug = tokio::time::timeout(HANG_GUARD, bugs.next()).await.unwrap();
    assert_eq!(
        (bug.operation, bug.message.as_str()),
        ("list saved restore codes", "the in-memory keychain's operation panicked")
    );
    let store = app.create_closed_store("Household", Arc::new(SequentialIds::new())).await.unwrap();
    keychain.panic_next_operation();
    keychain.with_operations_held(|| dropped_at_first_poll(app.discard_store(&store)));
    let bug = tokio::time::timeout(HANG_GUARD, bugs.next()).await.unwrap();
    assert_eq!(bug.operation, "delete the store");
}

/// Poll `call` once, which starts the work it runs apart from its caller,
/// then drop it, as an app does when the screen that asked goes away. The
/// caller holds what the work waits on, so the work can't answer first.
fn dropped_at_first_poll<T: std::fmt::Debug>(call: impl std::future::Future<Output = T>) {
    use std::task::{Context, Poll, Waker};
    let mut call = std::pin::pin!(call);
    if let Poll::Ready(answer) = call.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        panic!("the call answered at its first poll: {answer:?}");
    }
}

/// A closed store's sync status (E1, E5): read under the writer lock, it is
/// the state opening shows before sync starts, with the last synced time and
/// the unsent work, and an open handle refuses it.
mod closed_sync_status {
    use crate::tests::{stage, Device, Network};
    use crate::*;
    use coven_foundation::clock::HANG_GUARD;
    use coven_foundation::files::StoreDirAccess;

    fn tables() -> Vec<SyncedTable> {
        vec![
            SyncedTable::new("photos", RowIdentity::SharedKey).file_column(FileColumn::new("file")),
            SyncedTable::new("notes", RowIdentity::SharedKey),
        ]
    }

    const SCHEMA: &str = "CREATE TABLE photos(id TEXT NOT NULL PRIMARY KEY, file TEXT);
        CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);";

    /// Ana's Household store on Dropbox, open on her laptop and synced.
    async fn household() -> Network {
        Network::with_schema(
            CloudProvider::Dropbox,
            tables(),
            vec![Migration::addition(1, "household", SCHEMA)],
        )
        .await
    }

    /// Close device 0's handle, change what `while_closed` changes, read the
    /// closed store's status, then reopen it without starting sync: the closed
    /// read and what that opening shows.
    async fn closed_then_reopened(
        network: &mut Network,
        while_closed: impl FnOnce(&Device),
    ) -> (ClosedSyncStatus, SyncStatus) {
        let device = &network.devices[0];
        device.handle.close().await.unwrap();
        while_closed(device);
        let closed =
            device.app.closed_sync_status(&device.app.store_dir(&device.store)).await.unwrap();
        let reopened = network.reopen(device).open(device.store, |_| ()).await.unwrap();
        let opened = reopened.sync_status().await.unwrap();
        network.devices[0].handle = reopened;
        (closed, opened)
    }

    /// Ana's laptop synced the Household store and she closed it. Her store
    /// list reads it closed: synced, at the time the open handle last showed,
    /// with nothing unsent, as the store shows the moment it opens again.
    #[tokio::test]
    async fn a_synced_store_reads_closed_as_it_opens() {
        let mut network = household().await;
        let before = network.devices[0].handle.sync_status().await.unwrap();
        assert_eq!(before.state, SyncState::Synced);
        assert!(before.last_synced.is_some(), "setup and its passes synced");
        let (closed, opened) = closed_then_reopened(&mut network, |_| ()).await;
        assert_eq!(closed.sync, before);
        assert_eq!(closed.sync, opened);
        assert!(closed.unsent.is_empty(), "{:?}", closed.unsent);
        network.close().await;
    }

    /// Ana stops sync, writes a note and closes the store before it goes
    /// out. Closed, the note waits with sync not running, never synced, as it
    /// does once the store opens again. She takes a photo and closes the
    /// store again: closed, it reads the unsent work the open handle counted,
    /// and still waits.
    #[tokio::test]
    async fn a_closed_store_reads_its_unsent_work() {
        let mut network = household().await;
        let ana = &network.devices[0];
        let waiting = SyncStatus {
            state: SyncState::WaitingToSync(SyncWait::NotSyncing),
            last_synced: ana.handle.sync_status().await.unwrap().last_synced,
        };
        ana.stop_sync().await;
        ana.handle
            .write(|sql| {
                sql.execute("INSERT INTO notes(id,title) VALUES('milk','Buy milk')", [])?;
                Ok::<_, CovenError>(())
            })
            .await
            .unwrap();
        let (closed, opened) = closed_then_reopened(&mut network, |_| ()).await;
        assert_eq!(closed.unsent.files, 0, "{:?}", closed.unsent);
        assert_eq!(closed.sync, waiting);
        assert_eq!(opened, waiting);
        let ana = &network.devices[0];
        stage(ana, "photos", "beach", vec![7; 4096]).await;
        let unsent = ana.handle.unsent_work().await.unwrap();
        assert_eq!(unsent.files, 1, "{unsent:?}");
        assert!(unsent.writes >= 1, "{unsent:?}");
        let (closed, opened) = closed_then_reopened(&mut network, |_| ()).await;
        assert_eq!(closed.unsent, unsent);
        assert_eq!(closed.sync, waiting);
        assert_eq!(closed.sync, opened);
        network.close().await;
    }

    /// Ana's Dropbox sign-in leaves her laptop's keychain while the Household
    /// store is closed. Read closed, it is signed out with no cause, as it is
    /// the moment it opens again, and keeps its last synced time.
    #[tokio::test]
    async fn a_closed_store_without_credentials_reads_signed_out() {
        let mut network = household().await;
        let synced = network.devices[0].handle.sync_status().await.unwrap().last_synced;
        let (closed, opened) = closed_then_reopened(&mut network, |device| {
            device.app.lose_storage_credentials(device.store).unwrap();
        })
        .await;
        assert_eq!(
            closed.sync,
            SyncStatus {
                state: SyncState::SignedOut { provider: CloudProvider::Dropbox, cause: None },
                last_synced: synced,
            }
        );
        assert_eq!(closed.sync, opened);
        network.close().await;
    }

    /// Ana's laptop is restored from a backup, which keeps no device-only
    /// keychain entries. Its copy of the Household store doesn't open, so
    /// its closed read fails as opening does, with `DeviceKeyMissing`; the
    /// laptop it was copied from still reads it, never synced (§19.3).
    #[tokio::test]
    async fn a_copy_without_its_device_entry_is_refused() {
        let app_dir = tempfile::tempdir().unwrap();
        let app = Coven::in_memory(app_dir.path().into());
        let store =
            app.create_closed_store("Household", std::sync::Arc::new(UuidIds)).await.unwrap().id();
        let read = app.closed_sync_status(&app.store_dir(&store)).await.unwrap();
        assert_eq!(read.sync, SyncStatus { state: SyncState::NotSynced, last_synced: None });
        let restored = Coven::in_memory(app_dir.path().into());
        let refused = restored.closed_sync_status(&restored.store_dir(&store)).await;
        assert!(
            matches!(refused, Err(CovenError::DeviceKeyMissing { store: missing }) if missing == store),
            "{refused:?}"
        );
        let opened = super::builder(&restored).open(store, |_| ()).await;
        assert!(
            matches!(opened, Err(CovenError::DeviceKeyMissing { store: missing }) if missing == store),
            "{opened:?}"
        );
    }

    /// While a handle holds the Household store open, its writer lock shows a
    /// sync the closed read can't see, so the read is refused and the app asks
    /// that handle instead.
    #[tokio::test]
    async fn an_open_store_refuses_the_closed_read() {
        let network = household().await;
        let ana = &network.devices[0];
        let refused = ana.app.closed_sync_status(&ana.app.store_dir(&ana.store)).await;
        assert!(
            matches!(refused, Err(CovenError::Lock(StoreLockError::AlreadyOpen(store))) if store == ana.store),
            "{refused:?}"
        );
        network.close().await;
    }

    /// Ana's store list reads the closed Household store as she opens it. The
    /// read holds the writer lock, registered as closing, so her open waits
    /// for it at that lock rather than failing, and opens once the read ends
    /// (E1).
    #[tokio::test]
    async fn an_open_waits_for_a_closed_read() {
        let app_dir = tempfile::tempdir().unwrap();
        let app = Coven::in_memory(app_dir.path().into());
        let directory =
            app.create_closed_store("Household", std::sync::Arc::new(UuidIds)).await.unwrap();
        let store = directory.id();
        let writer = coven_foundation::files::writer_lock(
            directory.database_path().parent().unwrap(),
            store,
        );
        // Its first poll registers the read's lock, before any await; the
        // read then goes no further until it is polled again.
        let store_dir = app.store_dir(&store);
        let mut read = std::pin::pin!(app.closed_sync_status(&store_dir));
        tokio::select! {
            biased;
            answer = &mut read => panic!("the read answered at its first poll: {answer:?}"),
            () = std::future::ready(()) => {}
        }
        let waits = coven_foundation::files::when_reached(&writer);
        let mut open = std::pin::pin!(super::builder(&app).open(store, |_| ()));
        tokio::select! {
            opened = &mut open => panic!("the open didn't wait for the read: {opened:?}"),
            reached = tokio::time::timeout(HANG_GUARD, waits) => {
                reached.expect("the open waits at the writer lock").unwrap()
            }
        }
        let read = tokio::time::timeout(HANG_GUARD, read).await.unwrap().unwrap();
        assert_eq!(read.sync, SyncStatus { state: SyncState::NotSynced, last_synced: None });
        let handle = tokio::time::timeout(HANG_GUARD, open).await.unwrap().unwrap();
        handle.close().await.unwrap();
    }
}
