use crate::*;
use coven_crypto::custody::{KeyringCustody, StoreKeychain};
use coven_database::DatabaseBuilder;
use coven_foundation::files::StoreDirAccess;
use coven_storage::{test_utils::MemoryStorage, Storage};
use std::sync::Arc;
use std::{future::Future, task::Poll, time::UNIX_EPOCH};

fn tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("attachments", RowIdentity::SharedKey)
        .file_column(FileColumn::new("file"))]
}

fn namespaces() -> Vec<Namespace> {
    vec![Namespace::new(
        "attachments",
        Provenance::AppProvided,
        1 << 30,
    )]
}

fn migrations() -> Vec<Migration> {
    vec![Migration::addition(
        1,
        "attachments",
        "CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,file TEXT)",
    )]
}

/// Opening tells the app each step it runs, on the task that awaits it, so
/// the callback needn't be sendable: Ana's app adds its first table to the
/// Household store, and the open that runs that migration says so last.
#[tokio::test]
async fn opening_reports_its_steps_to_the_app() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let directory = app
        .create_closed_store(&layout, "Household", Arc::new(UuidIds))
        .await
        .unwrap();
    let seen = std::cell::RefCell::new(Vec::new());
    let handle = app
        .builder(layout.clone())
        .synced_tables(tables())
        .namespaces(namespaces())
        .migrations(migrations())
        .open(directory.id(), |progress| seen.borrow_mut().push(progress))
        .await
        .unwrap();
    handle.close().await.unwrap();
    let seen = seen.into_inner();
    assert_eq!(
        seen.last(),
        Some(&Progress {
            step: OpenStep::Migrating,
            done: 1,
            total: 1
        }),
        "{seen:?}"
    );
}

#[tokio::test]
async fn app_reopening_resumes_operations_and_files_using_one_storage_capability() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let ids = Arc::new(UuidIds);
    let directory = app
        .create_closed_store(&layout, "Household", ids.clone())
        .await
        .unwrap();
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    let storage = Arc::new(
        MemoryStorage::builder()
            .clock(clock.clone())
            .transfer_limits(65536, 65536)
            .build()
            .unwrap(),
    );
    let member = MemberIdentity::generate(MemberId(uuid::Uuid::from_u128(0x4d))).unwrap();
    let scoped = Arc::new(StoreKeychain::new(
        app.builder(layout.clone()).keychain().unwrap(),
        directory.id(),
    ));
    let identity = Arc::new(KeyringCustody::<MemberIdentity>::new(scoped.clone()));
    identity.persist(&member).unwrap();
    let keys = Arc::new(KeyringCustody::<StoreKeyring>::new(scoped));
    let open = || {
        app.builder(layout.clone())
            .synced_tables(tables())
            .namespaces(namespaces())
            .migrations(migrations())
            .key_custody(KeyCustody::Custom(keys.clone()))
            .identity_custody(IdentityCustody::Custom(identity.clone()))
            .clock(clock.clone())
            .storage_connector(storage.clone())
    };
    let handle = open().open(directory.id(), |_| ()).await.unwrap();
    handle
        .setup_s3_storage(
            storage.config(),
            "Owner",
            "owner-key".into(),
            SecretText::new("secret".into()),
        )
        .await
        .unwrap();
    handle.close().await.unwrap();
    let handle = open().open(directory.id(), |_| ()).await.unwrap();
    handle.set_uploads_paused(true).await.unwrap();
    let staged = handle
        .write_with_files(
            |batch| Ok(batch.put_file("attachments", vec![42; 200_000], crate::Place::Cloud)),
            |sql, file| {
                sql.execute(
                    "INSERT INTO attachments(id,file) VALUES('shared',?1)",
                    [file],
                )?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    let local = handle.file_ref(staged).await.unwrap();
    let queued = handle
        .read(|sql| {
            Ok(sql.query("SELECT file FROM coven_uploads", [], |r| {
                r.get::<_, FileId>(0)
            })?)
        })
        .await
        .unwrap();
    assert_eq!(queued, [local.id()]);
    let members = handle.members().await.unwrap();
    assert_eq!(members.len(), 1);
    assert!(members[0].is_self);
    assert!(matches!(
        handle.circles().create("Disconnected").await,
        Err(SyncError::NoStorage)
    ));
    handle.unlock_store_key().await.unwrap();
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    storage
        .hold_next_creation(ObjectPrefix::all(directory.id()), started, held)
        .await;
    let circles = handle.circles();
    let mut waiting = Box::pin(circles.create("Interrupted"));
    std::future::poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    observed.await.unwrap();
    storage.set_online(false);
    resume.send(()).unwrap();
    // The circle's operation stays journaled after its offline attempt.
    assert!(waiting.await.is_err());
    handle.close().await.unwrap();
    storage.set_online(true);
    let handle = open().open(directory.id(), |_| ()).await.unwrap();
    handle.unlock_store_key().await.unwrap();
    // The pause setting survived reopening; the app resumes uploads.
    assert!(handle.uploads().await.unwrap().paused);
    handle.set_uploads_paused(false).await.unwrap();
    let mut uploads = handle.subscribe(|sql| {
        Ok(
            sql.query_row("SELECT count(*) FROM coven_uploads", [], |r| {
                r.get::<_, i64>(0)
            })?,
        )
    });
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while uploads
            .next()
            .await
            .expect("the live query answers")
            .unwrap()
            != 0
        {}
    })
    .await
    .unwrap();
    // No attempt left a record about its file.
    assert!(handle
        .pending()
        .await
        .unwrap()
        .iter()
        .all(|record| !matches!(
            record.subject,
            PendingSubject::Upload { .. } | PendingSubject::Download { .. }
        )));
    // Upload completion switched the file to the cloud under the same id,
    // so the reference taken before upload reads it from the cache or
    // storage.
    assert_eq!(handle.read_file(&local).await.unwrap(), vec![42; 200_000]);
    let file = handle.file_ref(staged).await.unwrap();
    let FilePlace::Cloud { location, .. } = file.place() else {
        panic!("the upload stored the file");
    };
    let path = ObjectPath::file(directory.id(), location.uploader(), location.object());
    let reads = storage.reads().await.len();
    let stream = handle.open_file_stream(&file).await.unwrap();
    assert_eq!(stream.read_at(70_000, 37).await.unwrap(), vec![42; 37]);
    assert!(storage.reads().await[reads..]
        .iter()
        .all(|(read, _, _)| read != &path));
    assert!(matches!(
        handle.retry_pending_operation(OperationId(-1)).await,
        Err(OperationError::NotPending(OperationId(-1)))
    ));
    // A pass resumes the interrupted circle's journaled operation.
    let mut status = handle.subscribe_sync_status();
    status.borrow_and_update();
    handle.start_sync().await.unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        status.wait_for(|s| matches!(s, SyncStatus::Synced { .. })),
    )
    .await
    .unwrap()
    .unwrap();
    let circle = handle.circles().list().await.unwrap().remove(0);
    assert_eq!(circle.name, "Interrupted");
    handle.circles().rename(circle.id, "Family").await.unwrap();
    assert!(handle.circles().members(circle.id).await.unwrap()[0].is_self);
    let outsider = MemberId(uuid::Uuid::from_u128(0x0e));
    assert!(matches!(
        handle.circles().add_member(circle.id, &outsider).await,
        Err(SyncError::Rejected(OperationRejection::TargetGone(EntryTarget::Member(id)))) if id == outsider
    ));
    let invite = handle
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
                name: "Carol".into(),
            },
            InviteAccess::S3AccessKey {
                access_key_id: "invited-key".into(),
                secret_access_key: SecretText::new("invited-secret".into()),
            },
            crate::InviteDelivery::Sent {
                lifetime: crate::InviteLifetime::Day,
            },
        )
        .await
        .unwrap();
    let invites = handle.subscribe_invites();
    assert_eq!(invites.borrow().len(), 1);
    handle.cancel_invite(&invite.id).await.unwrap();
    assert!(invites.borrow().is_empty());
    assert!(matches!(
        handle
            .set_member_role(&member.member(), MemberRole::Member)
            .await,
        Err(SyncError::Rejected(OperationRejection::NoAdminLeft))
    ));
    assert!(keys
        .unlock()
        .unwrap()
        .unwrap()
        .circle_key_ids(circle.id)
        .next()
        .is_some());
    handle.circles().delete(circle.id).await.unwrap();
    assert!(handle.circles().list().await.unwrap().is_empty());
    // A deleted circle has no members left to list.
    assert!(handle
        .circles()
        .members(circle.id)
        .await
        .unwrap()
        .is_empty());
    handle.close().await.unwrap();
    assert!(matches!(
        handle.circles().list().await,
        Err(CovenError::Database(DbError::StoreClosed))
    ));

    let updated = || {
        let mut result = migrations();
        result.push(Migration::breaking(
            2,
            "files",
            "CREATE INDEX attachment_files ON attachments(file)",
            crate::OldWrites::Convert(std::sync::Arc::new(|_| Ok(()))),
        ));
        result
    };
    // The app migrates offline. Opening again resumes the journal committed
    // with that migration, without an app call to publish its schema.
    let handle = open()
        .migrations(updated())
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    handle.members().await.unwrap();
    handle.close().await.unwrap();
    let database = || {
        DatabaseBuilder::new(directory.clone())
            .synced_tables(tables())
            .migrations(updated())
            .clock(clock.clone())
    };
    let db = database().open().await.unwrap();
    let pending = db.operations().await.unwrap();
    let migration = pending.iter().find(|r| r.kind == "schema-raise").unwrap();
    assert_eq!(migration.last_step, 1);
    assert!(db
        .pending::<PendingRecord>()
        .await
        .unwrap()
        .iter()
        .all(|r| matches!(r.reason, PendingReason::NoStorage | PendingReason::Waits(_))));
    db.close().await.unwrap();
    let handle = open()
        .migrations(updated())
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    handle.unlock_store_key().await.unwrap();
    // A sync pass uploads the captured old prefix and marker, then the raise
    // snapshot and entry publish (§11.5).
    handle.start_sync().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while handle
            .store_state()
            .await
            .schema
            .get(&coven_merge::Audience::Store)
            .is_none_or(|version| version.number != 2)
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    handle.close().await.unwrap();
    let db = database().open().await.unwrap();
    assert_eq!(
        db.store_state().await.unwrap().schema[&coven_merge::Audience::Store].number,
        2
    );
    assert!(db.operations().await.unwrap().is_empty());
    db.close().await.unwrap();
}

mod recovery {
    use super::*;

    fn recovery_tables() -> Vec<SyncedTable> {
        vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
    }
    fn recovery_migrations() -> Vec<Migration> {
        vec![Migration::addition(1, "notes", "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL); CREATE TABLE scratch(value TEXT)")]
    }

    #[tokio::test]
    async fn a_damaged_database_resets_from_setup_credentials_after_checking_them() {
        for config in [
            StorageConfig::S3 {
                bucket: "test".into(),
                region: "us-east-1".into(),
                endpoint: None,
                profile: None,
                prefix: crate::S3Prefix::parse("recovery").unwrap(),
            },
            StorageConfig::Dropbox {
                account_label: None,
                location: coven_storage::DropboxLocation::SharedFolder {
                    namespace_id: "recovery".into(),
                },
            },
            StorageConfig::CloudKit {
                container: "test".into(),
                owner: "owner".into(),
                zone: "recovery".into(),
            },
        ] {
            let root = tempfile::tempdir().unwrap();
            let layout = StoreLayout::new(root.path().into());
            let app = TestCoven::new();
            let directory = app
                .create_closed_store(&layout, "Recovery", Arc::new(UuidIds))
                .await
                .unwrap();
            let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
            let sign_in = crate::authentication::SignIn::new(clock.clone()).await;
            let storage = Arc::new(
                MemoryStorage::builder()
                    .location(config.clone())
                    .clock(clock.clone())
                    .transfer_limits(65536, 65536)
                    .build()
                    .unwrap(),
            );
            let builder = || {
                sign_in.configure(
                    app.builder(layout.clone())
                        .synced_tables(recovery_tables())
                        .migrations(recovery_migrations())
                        .clock(clock.clone())
                        .storage_connector(storage.clone()),
                )
            };
            let handle = builder().open(directory.id(), |_| ()).await.unwrap();

            let setup = match config.provider() {
                CloudProvider::S3 => handle
                    .setup_s3_storage(
                        config.clone(),
                        "Original",
                        "owner".into(),
                        SecretText::new("secret".into()),
                    )
                    .await
                    .unwrap(),
                CloudProvider::CloudKit => handle
                    .setup_cloudkit_storage("test", "Original")
                    .await
                    .unwrap(),
                _ => {
                    handle
                        .authenticate(config.oauth_access().unwrap())
                        .await
                        .unwrap();
                    handle
                        .setup_oauth_storage(config.clone(), "Original")
                        .await
                        .unwrap()
                }
            };
            assert_eq!(
                setup.restore_code.as_str(),
                handle.restore_code().await.unwrap()
            );
            let code = coven_sync::read_restore_code(setup.restore_code.as_str()).unwrap();
            assert_eq!(code.store, directory.id());
            assert_eq!(
                coven_storage::RestoreStorage::decode(code.storage.as_bytes())
                    .unwrap()
                    .location,
                setup.connection.storage
            );
            let mut status = handle.subscribe_sync_status();
            handle.stop_sync();
            status
                .wait_for(|s| matches!(s, SyncStatus::Stopped))
                .await
                .unwrap();
            handle
                .write(|sql| {
                    sql.execute("INSERT INTO notes VALUES('saved','from storage')", [])?;
                    Ok(())
                })
                .await
                .unwrap();
            handle.start_sync().await.unwrap();
            tokio::time::timeout(
                std::time::Duration::from_secs(20),
                status.wait_for(|s| matches!(s, SyncStatus::Synced { .. })),
            )
            .await
            .unwrap()
            .unwrap();
            handle.stop_sync();
            status
                .wait_for(|s| matches!(s, SyncStatus::Stopped))
                .await
                .unwrap();
            handle
                .write(|sql| {
                    sql.execute("INSERT INTO notes VALUES('unsent','local only')", [])?;
                    Ok(())
                })
                .await
                .unwrap();
            handle.close().await.unwrap();
            let damaged = b"damaged SQLite";
            std::fs::write(directory.database_path(), damaged).unwrap();
            // The provider now rejects the saved token; recovery refreshes it
            // once and publishes the replacement with the new device.
            storage.reject_access_token("access-0").await;
            let scoped = coven_crypto::custody::StoreKeychain::new(
                builder().keychain().unwrap(),
                directory.id(),
            );
            let credentials = scoped.storage_credentials().unwrap().unwrap();
            scoped
                .set_storage_credentials(&SecretBytes::new(b"malformed credentials".to_vec()))
                .unwrap();
            assert!(matches!(
                builder().open(directory.id(), |_| ()).await,
                Err(CovenError::Database(DbError::DamagedDatabase))
            ));
            assert!(matches!(
                builder()
                    .reset_device(directory.id(), "Ana’s recovered laptop")
                    .await,
                Err(BootstrapError::Sync(SyncError::Storage(error)))
                    if error.failure() == StorageFailure::Encoding
            ));
            assert_eq!(std::fs::read(directory.database_path()).unwrap(), damaged);
            scoped.set_storage_credentials(&credentials).unwrap();
            storage.set_online(false);
            let error = builder()
                .reset_device(directory.id(), "Ana’s recovered laptop")
                .await
                .unwrap_err();
            assert!(
                matches!(
                    &error,
                    BootstrapError::Sync(SyncError::Storage(error)) if error.failure() == StorageFailure::Network
                ),
                "{error:?}"
            );
            // Failed checks move nothing and decide no reset.
            assert_eq!(std::fs::read(directory.database_path()).unwrap(), damaged);
            assert_eq!(directory.unfinished_replacement().unwrap(), None);
            let old_device = directory.settings().unwrap().device_id;
            storage.set_online(true);
            let recovered = builder()
                .reset_device(directory.id(), "Ana’s recovered laptop")
                .await
                .unwrap();
            let reset = recovered.device_reset().await.unwrap().unwrap();
            assert_eq!(reset.old_device, old_device);
            assert_ne!(reset.new_device, old_device);
            // The damaged files stay aside for inspection, never as live state.
            let archive = directory
                .database_path()
                .parent()
                .unwrap()
                .join(format!("discarded-device-{}", old_device.0));
            assert_eq!(std::fs::read(archive.join("store.db")).unwrap(), damaged);
            let title: String = recovered
                .read(|sql| {
                    Ok(
                        sql.query_row("SELECT title FROM notes WHERE id='saved'", [], |row| {
                            row.get(0)
                        })?,
                    )
                })
                .await
                .unwrap();
            assert_eq!(title, "from storage");
            // Nothing unsent is salvaged from the damaged copy.
            let unsent: i64 = recovered
                .read(|sql| {
                    Ok(sql.query_row(
                        "SELECT count(*) FROM notes WHERE id='unsent'",
                        [],
                        |row| row.get(0),
                    )?)
                })
                .await
                .unwrap();
            assert_eq!(unsent, 0);
            if config.provider() == CloudProvider::Dropbox {
                assert_eq!(
                    sign_in.presentation_count(),
                    1,
                    "recovery never opens sign-in UI"
                );
                assert_eq!(
                    sign_in.request_count(),
                    2,
                    "recovery refreshes the rejected token once"
                );
                let coven_storage::StorageCredentials::OAuth(tokens) =
                    coven_storage::StorageCredentials::decode(
                        scoped.storage_credentials().unwrap().unwrap().as_bytes(),
                    )
                    .unwrap()
                else {
                    panic!("OAuth credentials")
                };
                assert_eq!(tokens.access_token.as_str(), "access-1");
            }
            assert!(matches!(
                *recovered.subscribe_sync_status().borrow(),
                SyncStatus::Stopped
            ));
            recovered.close().await.unwrap();
            let database = DatabaseBuilder::new(directory.clone())
                .synced_tables(recovery_tables())
                .migrations(recovery_migrations())
                .open()
                .await
                .unwrap();
            let device = directory.settings().unwrap().device_id;
            assert_eq!(
                database.store_state().await.unwrap().devices[&device].name,
                "Ana’s recovered laptop"
            );
            database.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn creation_identity_matches_settings_before_and_after_reopening() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let directory = app
        .create_closed_store(&layout, "Recipes", Arc::new(UuidIds))
        .await
        .unwrap();
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let builder = || {
        app.builder(layout.clone())
            .synced_tables(vec![])
            .migrations(vec![])
            .storage_connector(storage.clone())
    };
    // Directory settings identify the store before creation is published.
    let handle = builder().open(directory.id(), |_| ()).await.unwrap();
    handle.close().await.unwrap();
    let handle = builder().open(directory.id(), |_| ()).await.unwrap();
    handle
        .setup_s3_storage(
            storage.config(),
            "Phone",
            "owner".into(),
            SecretText::new("secret".into()),
        )
        .await
        .unwrap();
    handle.close().await.unwrap();
    let handle = builder().open(directory.id(), |_| ()).await.unwrap();
    handle.close().await.unwrap();

    let reader = builder().open_read_only(directory.id()).await.unwrap();
    reader.close().await.unwrap();
    let handle = builder().open(directory.id(), |_| ()).await.unwrap();
    handle.unlock_store_key().await.unwrap();
    handle.rename_store("Renamed recipes").await.unwrap();
    handle.close().await.unwrap();
    let handle = builder().open(directory.id(), |_| ()).await.unwrap();
    assert_eq!(handle.store_name().await.unwrap(), "Renamed recipes");
    handle
        .setup_s3_storage(
            storage.config(),
            "Phone",
            "owner".into(),
            SecretText::new("secret".into()),
        )
        .await
        .unwrap();
    handle.close().await.unwrap();
    let reader = builder().open_read_only(directory.id()).await.unwrap();
    assert_eq!(reader.store_name().await.unwrap(), "Renamed recipes");
    reader.close().await.unwrap();
}

#[tokio::test]
async fn the_apps_sqlite_settings_reach_both_handles_from_creation() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let settings = SqliteSettings {
        cache_size: Some(CacheSize::Kibibytes(65536)),
        wal_autocheckpoint: Some(WalAutocheckpoint::Off),
        page_size: Some(PageSize::Bytes8192),
        auto_vacuum: Some(AutoVacuum::Incremental),
        ..SqliteSettings::default()
    };
    let builder = || {
        app.builder(layout.clone())
            .synced_tables(vec![])
            .migrations(vec![])
            .sqlite_settings(settings.clone())
    };
    let handle = builder().create_store("Recipes").await.unwrap();
    let chosen = |effect: SqliteSettingsInEffect| {
        (
            effect.cache_size,
            effect.wal_autocheckpoint,
            effect.page_size,
            effect.auto_vacuum,
        )
    };
    let expected = (
        CacheSize::Kibibytes(65536),
        WalAutocheckpoint::Off,
        PageSize::Bytes8192,
        AutoVacuum::Incremental,
    );
    assert_eq!(chosen(handle.sqlite_settings().await.unwrap()), expected);
    let checkpoint = handle.checkpoint(CheckpointMode::Truncate).await.unwrap();
    assert!(!checkpoint.busy);
    handle
        .incremental_vacuum(IncrementalVacuum::All)
        .await
        .unwrap();
    let store = handle.store_id();
    let reader = builder().open_read_only(store).await.unwrap();
    assert_eq!(chosen(reader.sqlite_settings().await.unwrap()), expected);
    reader.close().await.unwrap();
    handle.close().await.unwrap();
}

#[tokio::test]
async fn the_apps_collations_reach_both_handles_from_creation() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let builder = || {
        app.builder(layout.clone())
            .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
            .migrations(vec![Migration::addition(
                1,
                "notes",
                "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL); CREATE INDEX notes_reversed ON notes(title COLLATE reversed)",
            )])
            .collation("reversed", 1, |left: &str, right: &str| right.cmp(left))
    };
    let handle = builder().create_store("Recipes").await.unwrap();
    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO notes VALUES ('1','apples'),('2','cherries'),('3','bananas')",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let in_order = "SELECT title FROM notes ORDER BY title COLLATE reversed";
    let expected = ["cherries", "bananas", "apples"];
    let read = handle
        .read(|sql| Ok(sql.query(in_order, [], |row| row.get::<_, String>(0))?))
        .await
        .unwrap();
    assert_eq!(read, expected);
    let reader = builder().open_read_only(handle.store_id()).await.unwrap();
    let read = reader
        .read(|sql| Ok(sql.query(in_order, [], |row| row.get::<_, String>(0))?))
        .await
        .unwrap();
    assert_eq!(read, expected);
    reader.close().await.unwrap();
    handle.close().await.unwrap();
}

#[tokio::test]
async fn the_apps_functions_reach_both_handles_from_creation() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let builder = || {
        app.builder(layout.clone())
            .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
            .migrations(vec![Migration::addition(
                1,
                "notes",
                "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL); CREATE INDEX notes_folded ON notes(fold(title))",
            )])
            .function("fold", 1, Determinism::Deterministic { version: 1 }, |arguments| {
                Ok(arguments.get::<String>(0)?.to_lowercase())
            })
    };
    let handle = builder().create_store("Recipes").await.unwrap();
    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO notes VALUES ('1','École'),('2','ecole'),('3','ÉCOLE')",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let matching = "SELECT title FROM notes WHERE fold(title) = 'école' ORDER BY id";
    let expected = ["École", "ÉCOLE"];
    let read = handle
        .read(|sql| Ok(sql.query(matching, [], |row| row.get::<_, String>(0))?))
        .await
        .unwrap();
    assert_eq!(read, expected);
    let reader = builder().open_read_only(handle.store_id()).await.unwrap();
    let read = reader
        .read(|sql| Ok(sql.query(matching, [], |row| row.get::<_, String>(0))?))
        .await
        .unwrap();
    assert_eq!(read, expected);
    reader.close().await.unwrap();
    handle.close().await.unwrap();
}

/// Every folder and file under `folder`, the folder included, with its
/// protection class.
#[cfg(target_vendor = "apple")]
fn protection_classes(
    folder: &std::path::Path,
) -> Vec<(std::path::PathBuf, Option<FileProtection>)> {
    let mut found = vec![(folder.to_owned(), FileProtection::of(folder).unwrap())];
    for entry in std::fs::read_dir(folder).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            found.extend(protection_classes(&path));
        } else {
            found.push((path.clone(), FileProtection::of(&path).unwrap()));
        }
    }
    found
}

/// The class reaches the database, its WAL and shared memory, coven's folders
/// and settings: when the store is created, and when a later open chooses
/// another (E1). On macOS, whose APFS volumes keep the class, a new file's
/// default is `CompleteUntilFirstUserAuthentication`, so this uses the two
/// classes that differ from it.
#[cfg(target_vendor = "apple")]
#[tokio::test]
async fn the_chosen_protection_class_reaches_every_file_of_the_store() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let builder = |class| {
        app.builder(layout.clone())
            .synced_tables(tables())
            .namespaces(namespaces())
            .migrations(migrations())
            .file_protection(class)
    };
    let assert_store = |store: &std::path::Path, class| {
        let found = protection_classes(store);
        for name in ["store.db", "store.db-wal", "store.db-shm", "files", "cache"] {
            assert!(
                found.iter().any(|(path, _)| path.ends_with(name)),
                "{name} is missing: {found:?}"
            );
        }
        for (path, found) in found {
            assert_eq!(found, Some(class), "{}", path.display());
        }
    };
    let handle = builder(FileProtection::CompleteUnlessOpen)
        .create_store("Household")
        .await
        .unwrap();
    let id = handle.store_id();
    let store = root.path().join("stores").join(id.to_string());
    let write = |handle: CovenHandle, row: &'static str| async move {
        handle
            .write(move |sql| {
                sql.execute("INSERT INTO attachments(id) VALUES(?1)", [row])?;
                Ok(())
            })
            .await
            .unwrap();
        handle
    };
    let handle = write(handle, "a").await;
    assert_store(&store, FileProtection::CompleteUnlessOpen);
    handle.close().await.unwrap();
    let handle = write(
        builder(FileProtection::Complete)
            .open(id, |_| ())
            .await
            .unwrap(),
        "b",
    )
    .await;
    assert_store(&store, FileProtection::Complete);
    handle.close().await.unwrap();
}
