use crate::*;
use coven_crypto::custody::{KeyringCustody, StoreKeychain};
use coven_database::DatabaseBuilder;
use coven_storage::{test_utils::MemoryStorage, Storage};
use std::sync::Arc;
use std::{future::Future, task::Poll, time::UNIX_EPOCH};

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("attachments", RowIdentity::SharedKey).carries_files(FileDecl::new(
            "attachments",
            1 << 30,
            Provenance::AppProvided,
            CacheFill::CacheLazy,
        )),
    ]
}

fn migrations() -> Vec<Migration> {
    vec![Migration::sql(
        1,
        "attachments",
        "CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT)",
    )]
}

#[tokio::test]
async fn app_reopening_resumes_operations_and_files_using_one_storage_capability() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let ids = Arc::new(UuidIds);
    let directory = app
        .create_store(&layout, "Household", ids.clone())
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
            .migrations(migrations())
            .key_custody(KeyCustody::Custom(keys.clone()))
            .identity_custody(IdentityCustody::Custom(identity.clone()))
            .clock(clock.clone())
            .storage_connector(storage.clone())
    };
    let handle = open().open(directory.id()).await.unwrap();
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
    let handle = open().open(directory.id()).await.unwrap();
    handle.set_uploads_paused(true).await.unwrap();
    handle
        .write_with_files(
            |batch| {
                batch.put_file("attachments", "shared", vec![42; 200_000]);
                Ok(())
            },
            |sql| {
                sql.execute("INSERT INTO attachments(id) VALUES('shared')", [])?;
                Ok(())
            },
        )
        .await
        .unwrap();
    let local = handle.file_ref("attachments", "shared").await.unwrap();
    let queued = handle.subscribe_uploads().next().await.unwrap();
    assert_eq!(queued.files.len(), 1);
    assert_eq!(queued.files[0].file, local);
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
        .hold_next_creation(ObjectPrefix::keys(directory.id()), started, held)
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
    handle.barrier().await.unwrap();
    drop(waiting);
    handle.close().await.unwrap();
    storage.set_online(true);
    let handle = open().open(directory.id()).await.unwrap();
    handle.unlock_store_key().await.unwrap();
    // The pause setting survived reopening; the app resumes uploads.
    assert!(handle.uploads().await.unwrap().paused);
    handle.set_uploads_paused(false).await.unwrap();
    let mut uploads = handle.subscribe_uploads();
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let queue = uploads.next().await.unwrap();
            if queue.files.is_empty() {
                break;
            }
        }
    })
    .await
    .unwrap();
    // No attempt left a record about its file.
    assert!(handle
        .pending()
        .await
        .unwrap()
        .iter()
        .all(|record| !matches!(record.subject, PendingSubject::File { .. })));
    // Upload completion changes no row, so the reference taken before upload
    // still names the file and now reads it from the cache or storage.
    let file = handle.file_ref("attachments", "shared").await.unwrap();
    assert_eq!(file, local);
    assert_eq!(handle.read_file(&file).await.unwrap(), vec![42; 200_000]);
    let path = ObjectPath::file(directory.id(), file.uploader(), file.id());
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
    handle.barrier().await.unwrap();
    let circle = handle.circles().list().await.unwrap().remove(0);
    assert_eq!(circle.name, "Interrupted");
    handle.circles().rename(circle.id, "Family").await.unwrap();
    assert!(handle.circles().members(circle.id).await.unwrap()[0].is_self);
    let outsider = MemberId(uuid::Uuid::from_u128(0x0e));
    assert!(matches!(
        handle.circles().add_member(circle.id, &outsider).await,
        Err(SyncError::Rejected(coven_database::DropReason::TargetGone(coven_database::EntryTarget::Member(id)))) if id == outsider
    ));
    let invite = handle
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
            },
            InviteAccess::S3AccessKey {
                access_key_id: "invited-key".into(),
                secret_access_key: SecretText::new("invited-secret".into()),
            },
        )
        .await
        .unwrap();
    let joins = handle.subscribe_join_requests();
    assert!(joins.borrow().is_empty());
    handle.cancel_invite(&invite.id).await.unwrap();
    handle
        .confirm_access_key_deleted("invited-key")
        .await
        .unwrap();
    assert!(matches!(
        handle
            .set_member_role(&member.member(), MemberRole::Member)
            .await,
        Err(SyncError::Rejected(coven_database::DropReason::NoAdminLeft))
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
        result.push(
            Migration::sql(
                2,
                "sizes",
                "CREATE INDEX attachment_sizes ON attachments(size)",
            )
            .writes(|_| Ok(())),
        );
        result
    };
    // The app migrates offline. Opening again resumes the journal committed
    // with that migration, without an app call to publish its schema.
    let handle = open()
        .migrations(updated())
        .open(directory.id())
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
    let migration = pending.iter().find(|r| r.kind == "migrate-schema").unwrap();
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
        .open(directory.id())
        .await
        .unwrap();
    handle.unlock_store_key().await.unwrap();
    // A sync pass uploads the captured old prefix and marker, then the raise
    // snapshot and entry publish (§18.1).
    let mut pending = handle.subscribe_pending();
    handle.start_sync().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let records = pending.next().await.unwrap();
            if !records.iter().any(|r| {
                matches!(
                    r.subject,
                    coven_sync::PendingSubject::Operation {
                        kind: coven_sync::OperationKind::SchemaChange,
                        ..
                    }
                )
            }) {
                break;
            }
        }
    })
    .await
    .unwrap();
    handle.close().await.unwrap();
    let db = database().open().await.unwrap();
    assert_eq!(
        db.store_log().await.unwrap().replay.state.schema[&coven_merge::Audience::Store].number,
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
        vec![Migration::sql(1, "notes", "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL); CREATE TABLE scratch(value TEXT)")]
    }

    #[tokio::test]
    async fn a_damaged_database_resets_from_setup_credentials_after_checking_them() {
        for config in [
            StorageConfig::S3 {
                bucket: "test".into(),
                region: "us-east-1".into(),
                endpoint: None,
                profile: None,
                prefix: "recovery".into(),
            },
            StorageConfig::Dropbox {
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
                .create_store(&layout, "Recovery", Arc::new(UuidIds))
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
            let handle = builder().open(directory.id()).await.unwrap();
            handle.initialize_identity().unwrap();
            match config.provider() {
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
                builder().open(directory.id()).await,
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
            assert_eq!(directory.device_reset().unwrap(), None);
            let old_device = directory.settings().unwrap().device_id;
            storage.set_online(true);
            let recovered = builder()
                .reset_device(directory.id(), "Ana’s recovered laptop")
                .await
                .unwrap();
            let reset = recovered.device_reset().await.unwrap().unwrap();
            assert_eq!(reset.old_device, old_device);
            assert_eq!(reset.reason, DeviceResetReason::DatabaseDamage);
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
            let local = database.local_store_log().await.unwrap();
            assert_eq!(
                local.log.replay.state.devices[&local.device].name,
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
        .create_store(&layout, "Recipes", Arc::new(UuidIds))
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
    let handle = builder().open(directory.id()).await.unwrap();
    handle.close().await.unwrap();
    let handle = builder().open(directory.id()).await.unwrap();
    handle.initialize_identity().unwrap();
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
    let handle = builder().open(directory.id()).await.unwrap();
    handle.close().await.unwrap();

    let reader = builder().open_read_only(directory.id()).await.unwrap();
    reader.close().await.unwrap();
    let mut settings = directory.settings().unwrap();
    settings.name = "Conflicting name".into();
    let path = directory
        .database_path()
        .parent()
        .unwrap()
        .join("settings.json");
    std::fs::write(&path, serde_json::to_vec(&settings).unwrap()).unwrap();
    assert!(
        matches!(
            builder().open(directory.id()).await,
            Err(CovenError::Database(DbError::StoreSettingsMismatch { .. }))
        ),
        "opening must reject a name that disagrees with checked creation"
    );
    assert!(
        matches!(
            builder().open_read_only(directory.id()).await,
            Err(ReadOnlyOpenError::Local(CovenError::Database(
                DbError::StoreSettingsMismatch { .. }
            )))
        ),
        "read-only opening must check the same identity"
    );
}
