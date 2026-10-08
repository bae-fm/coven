use crate::*;
use coven_crypto::custody::{InMemoryCustody, StoreKeyCustody};
use coven_database::DatabaseBuilder;
use coven_format::store_log::StoreChange;
use coven_storage::{test_utils::MemoryStorage, Storage};
use coven_sync::StoreLogSync;
use std::sync::Arc;
use std::{future::Future, task::Poll, time::UNIX_EPOCH};

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("attachments", RowIdentity::SharedKey).carries_files(FileDecl::new(
            "attachments",
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
        MemoryStorage::new(
            StorageConfig::S3 {
                bucket: "test".into(),
                region: "us-east-1".into(),
                endpoint: None,
                prefix: "household".into(),
            },
            clock.clone(),
        )
        .unwrap()
        .with_transfer_limits(65536, 65536)
        .unwrap(),
    );
    let member = MemberKeys::generate().unwrap();
    let identity = Arc::new(InMemoryCustody::new(member.clone()));
    let keys = Arc::new(InMemoryCustody::<StoreKeyring>::empty());
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
    handle.set_uploads_paused(true);
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
    let members = handle.get_members().await.unwrap();
    assert_eq!(members.len(), 1);
    assert!(members[0].is_self);
    let circles = handle.circles();
    let mut waiting = Box::pin(circles.create("While offline"));
    std::future::poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    handle.get_members().await.unwrap(); // The preceding call's intent has committed.
    drop(waiting);
    handle.close().await.unwrap();
    let handle = open().open(directory.id()).await.unwrap();
    handle.unlock_store_key().await.unwrap();
    let mut uploads = handle.subscribe_uploads();
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let queue = uploads.next().await.unwrap();
            if queue.files.is_empty() {
                break;
            }
            assert!(queue.files.iter().all(|file| file.last_failure.is_none()));
        }
    })
    .await
    .unwrap();
    let file = handle.file_ref("attachments", "shared").await.unwrap();
    assert_eq!(file.location(), FileLocation::Uploaded);
    assert_eq!(handle.read_file(&file).await.unwrap(), vec![42; 200_000]);
    let uploaded = file.uploaded().unwrap().unwrap();
    let path = ObjectPath::file(uploaded.device, uploaded.id);
    let reads = storage.reads().await.len();
    let stream = handle.open_file_stream(&file).await.unwrap();
    assert_eq!(stream.read_at(70_000, 37).await.unwrap(), vec![42; 37]);
    assert!(storage.reads().await[reads..]
        .iter()
        .all(|(read, _, _)| read != &path));
    assert!(matches!(
        handle.read_file(&local).await,
        Err(FileReadError::Database(DbError::FileRefChanged { .. }))
    ));
    assert!(matches!(
        handle.retry_blocked_operation(OperationId(-1)).await,
        Err(OperationError::NotBlocked(OperationId(-1)))
    ));
    handle.get_members().await.unwrap();
    let circle = handle.circles().list().await.unwrap().remove(0);
    assert_eq!(circle.name, "While offline");
    handle.circles().rename(circle.id, "Family").await.unwrap();
    assert!(handle.circles().members(circle.id).await.unwrap()[0].is_self);
    let outsider = MemberKeys::generate().unwrap().member_id();
    assert!(matches!(
        handle.circles().add_member(circle.id, &outsider).await,
        Err(SyncError::NotStoreMember(id)) if id == outsider
    ));
    let invite = handle
        .create_invite(
            MemberRole::Member,
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
            .set_member_role(&member.member_id(), MemberRole::Member)
            .await,
        Err(SyncError::LastAdmin)
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
    assert!(matches!(
        handle.circles().members(circle.id).await,
        Err(SyncError::CircleDeleted(id)) if id == circle.id
    ));
    handle.close().await.unwrap();
    assert!(matches!(
        handle.circles().list().await,
        Err(SyncError::Database(DbError::StoreClosed))
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
    handle.get_members().await.unwrap();
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
    assert!(pending.iter().all(|r| r.failure.is_none()));
    db.close().await.unwrap();
    let handle = open()
        .migrations(updated())
        .open(directory.id())
        .await
        .unwrap();
    handle.unlock_store_key().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(20), handle.get_members())
        .await
        .unwrap()
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
    async fn recovery_reconnects_from_setup_credentials() {
        for config in [
            StorageConfig::S3 {
                bucket: "test".into(),
                region: "us-east-1".into(),
                endpoint: None,
                prefix: "recovery".into(),
            },
            StorageConfig::Dropbox {
                namespace_id: "recovery".into(),
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
                MemoryStorage::new(config.clone(), clock.clone())
                    .unwrap()
                    .with_transfer_limits(65536, 65536)
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
                    .setup_cloudkit_storage(config.clone(), "Original")
                    .await
                    .unwrap(),
                provider => {
                    handle.authenticate(provider).await.unwrap();
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
            handle.close().await.unwrap();
            let damaged = b"damaged SQLite";
            std::fs::write(directory.database_path(), damaged).unwrap();
            clock.set(UNIX_EPOCH + std::time::Duration::from_secs(3600));
            let scoped = coven_crypto::custody::StoreKeychain::new(
                builder().keychain().unwrap(),
                directory.id(),
            );
            let credentials = scoped.storage_credentials().unwrap().unwrap();
            scoped
                .set_storage_credentials(&SecretBytes::new(b"malformed credentials".to_vec()))
                .unwrap();
            assert!(matches!(
                builder()
                    .open_reloading(directory.id(), "Ana’s recovered laptop")
                    .await,
                Err(RecoveryError::Sync(SyncError::Storage(error)))
                    if error.failure() == StorageFailure::Encoding
            ));
            assert_eq!(std::fs::read(directory.database_path()).unwrap(), damaged);
            scoped.set_storage_credentials(&credentials).unwrap();
            storage.set_online(false);
            let error = builder()
                .open_reloading(directory.id(), "Ana’s recovered laptop")
                .await
                .unwrap_err();
            assert!(
                matches!(
                    &error,
                    RecoveryError::Sync(SyncError::Storage(error)) if error.failure() == StorageFailure::Network
                ),
                "{error:?}"
            );
            assert_eq!(std::fs::read(directory.database_path()).unwrap(), damaged);
            storage.set_online(true);
            let recovered = builder()
                .open_reloading(directory.id(), "Ana’s recovered laptop")
                .await
                .unwrap();
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
            if config.provider() == CloudProvider::Dropbox {
                assert_eq!(
                    sign_in.presentation_count(),
                    1,
                    "recovery never opens sign-in UI"
                );
                assert_eq!(
                    sign_in.request_count(),
                    2,
                    "recovery refreshes the saved token once"
                );
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

    #[tokio::test]
    async fn damaged_database_requires_explicit_reload_and_preserves_readable_waiting_writes() {
        for readable in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let layout = StoreLayout::new(root.path().into());
            let app = TestCoven::new();
            let ids = Arc::new(UuidIds);
            let directory = app
                .create_store(&layout, "Recovery", ids.clone())
                .await
                .unwrap();
            let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
            let storage = Arc::new(
                MemoryStorage::new(
                    StorageConfig::S3 {
                        bucket: "test".into(),
                        region: "us-east-1".into(),
                        endpoint: None,
                        prefix: "recovery".into(),
                    },
                    clock.clone(),
                )
                .unwrap(),
            );
            let identity = Arc::new(InMemoryCustody::new(MemberKeys::generate().unwrap()));
            let keys = Arc::new(InMemoryCustody::<StoreKeyring>::empty());
            let builder = || {
                app.builder(layout.clone())
                    .synced_tables(recovery_tables())
                    .migrations(recovery_migrations())
                    .clock(clock.clone())
                    .key_custody(KeyCustody::Custom(keys.clone()))
                    .identity_custody(IdentityCustody::Custom(identity.clone()))
                    .storage_connector(storage.clone())
            };
            let handle = builder().open(directory.id()).await.unwrap();
            handle
                .setup_s3_storage(
                    storage.config(),
                    "Original",
                    "owner".into(),
                    SecretText::new("secret".into()),
                )
                .await
                .unwrap();
            handle.close().await.unwrap();
            let db = DatabaseBuilder::new(directory.clone())
                .synced_tables(recovery_tables())
                .migrations(recovery_migrations())
                .clock(clock.clone())
                .open()
                .await
                .unwrap();
            let mut sync = StoreLogSync::new(
                storage.clone(),
                db.clone(),
                keys.clone(),
                identity.clone(),
                clock.clone(),
                ids.clone(),
                directory.clone(),
            );
            db.write(|sql| {
                sql.execute("INSERT INTO notes VALUES('saved','snapshot')", [])?;
                Ok(())
            })
            .await
            .unwrap();
            let mut writes = coven_sync::DeviceLogSync::new(
                storage.clone(),
                db.clone(),
                keys.clone(),
                identity.clone(),
            );
            writes.upload_writes().await.unwrap();
            sync.write_snapshot(Audience::Store).await.unwrap();
            storage
                .set_faults(coven_storage::test_utils::Faults {
                    fail_next: 1,
                    ..coven_storage::test_utils::Faults::none()
                })
                .await;
            assert!(matches!(
                sync.make_and_upload_entry(StoreChange::AddDevice {
                    device: DeviceId(77),
                    name: "Waiting entry".into(),
                })
                .await,
                Err(SyncError::Storage(_))
            ));
            db.write(|sql| {
                sql.execute("INSERT INTO notes VALUES('waiting','offline')", [])?;
                Ok(())
            })
            .await
            .unwrap();
            let files = coven_sync::Files::new(
                coven_database::FileDatabase::new(db.clone()),
                directory.clone(),
                None,
                clock.clone(),
                ids.clone(),
                coven_sync::TransferLimits::default(),
            );
            let operations = coven_sync::Operations::new(
                StoreLogSync::disconnected(
                    db.clone(),
                    keys.clone(),
                    identity.clone(),
                    clock.clone(),
                    ids.clone(),
                    directory.clone(),
                ),
                files,
                coven_sync::DeviceLogSync::disconnected(db.clone(), keys.clone(), identity.clone()),
                clock.clone(),
            );
            let mut pending = Box::pin(operations.create_circle("Waiting operation"));
            std::future::poll_fn(|cx| {
                assert!(pending.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            operations.get_members().await.unwrap();
            drop(pending);
            operations.close().await.unwrap();
            let old_device = directory.settings().unwrap().device_id;
            let scratch: i64 = db
                .read(|sql| {
                    Ok(sql.query_row(
                        "SELECT rootpage FROM sqlite_schema WHERE name='scratch'",
                        [],
                        |r| r.get(0),
                    )?)
                })
                .await
                .unwrap();
            db.close().await.unwrap();
            drop(sync);
            drop(writes);
            let path = directory.database_path();
            let mut damaged = std::fs::read(&path).unwrap();
            if readable {
                let size = u16::from_be_bytes([damaged[16], damaged[17]]) as usize;
                damaged[(scratch as usize - 1) * size] = 0xff;
            } else {
                damaged[..16].fill(0xff);
            }
            std::fs::write(&path, &damaged).unwrap();
            assert!(matches!(
                builder().open(directory.id()).await,
                Err(CovenError::Database(DbError::DamagedDatabase))
            ));
            let settings = coven_storage::StorageSettings::new(directory.clone());
            settings.remove().unwrap();
            assert!(matches!(
                builder()
                    .open_reloading(directory.id(), "Ana’s recovered laptop")
                    .await,
                Err(RecoveryError::Sync(SyncError::NoStorage))
            ));
            assert_eq!(std::fs::read(&path).unwrap(), damaged);
            settings.commit(&storage.config()).unwrap();
            let ring = keys.unlock().unwrap().unwrap();
            keys.forget().unwrap();
            assert!(matches!(
                builder()
                    .open_reloading(directory.id(), "Ana’s recovered laptop")
                    .await,
                Err(RecoveryError::NoStoreKeys)
            ));
            assert_eq!(std::fs::read(&path).unwrap(), damaged);
            keys.persist(&ring).unwrap();
            if readable {
                let snapshot = storage
                    .list(&ObjectPrefix::snapshots())
                    .await
                    .unwrap()
                    .remove(0)
                    .path;
                let snapshot_bytes = storage.read(&snapshot).await.unwrap();
                let write = storage
                    .list(&ObjectPrefix::device_logs())
                    .await
                    .unwrap()
                    .remove(0)
                    .path;
                let write_bytes = storage.read(&write).await.unwrap();
                storage
                    .corrupt_byte(&snapshot, snapshot_bytes.len() - 1)
                    .await
                    .unwrap();
                storage.delete(&write).await.unwrap();
                assert!(builder()
                    .open_reloading(directory.id(), "Ana’s recovered laptop")
                    .await
                    .is_err());
                assert!(matches!(
                    builder().open(directory.id()).await,
                    Err(CovenError::Lock(StoreLockError::RecoveryPending(_)))
                ));
                assert!(matches!(
                    builder().open_read_only(directory.id()).await,
                    Err(ReadOnlyOpenError::Local(CovenError::Lock(
                        StoreLockError::RecoveryPending(_)
                    )))
                ));
                storage.delete(&snapshot).await.unwrap();
                storage
                    .create_once(&snapshot, &snapshot_bytes)
                    .await
                    .unwrap();
                storage.create_once(&write, &write_bytes).await.unwrap();
            }
            let recovered = builder()
                .open_reloading(directory.id(), "Ana’s recovered laptop")
                .await
                .unwrap();
            let rows = recovered
                .read(|sql| {
                    Ok(sql.query("SELECT id FROM notes ORDER BY id", [], |r| {
                        r.get::<_, String>(0)
                    })?)
                })
                .await
                .unwrap();
            assert_eq!(
                rows,
                if readable {
                    vec!["saved", "waiting"]
                } else {
                    vec!["saved"]
                }
            );
            if readable {
                recovered.get_members().await.unwrap();
                assert!(recovered
                    .get_members()
                    .await
                    .unwrap()
                    .iter()
                    .flat_map(|member| &member.devices)
                    .any(|device| *device == DeviceId(77)));
                assert_eq!(
                    recovered.circles().list().await.unwrap()[0].name,
                    "Waiting operation"
                );
            }
            assert_ne!(directory.settings().unwrap().device_id, old_device);
            let archives: Vec<_> = std::fs::read_dir(path.parent().unwrap())
                .unwrap()
                .map(|e| e.unwrap().path())
                .filter(|p| {
                    p.file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .starts_with("damaged-database-")
                })
                .collect();
            assert_eq!(archives.len(), 1);
            assert_eq!(
                std::fs::read(archives[0].join("store.db")).unwrap(),
                damaged
            );
            recovered.close().await.unwrap();
            let db = DatabaseBuilder::new(directory.clone())
                .synced_tables(recovery_tables())
                .migrations(recovery_migrations())
                .clock(clock.clone())
                .open()
                .await
                .unwrap();
            let mut writes = coven_sync::DeviceLogSync::new(
                storage.clone(),
                db.clone(),
                keys.clone(),
                identity.clone(),
            );
            writes.upload_writes().await.unwrap();
            assert_eq!(
                storage
                    .list(&ObjectPrefix::device_logs())
                    .await
                    .unwrap()
                    .len(),
                if readable { 2 } else { 1 }
            );
            db.close().await.unwrap();
        }
    }
}
