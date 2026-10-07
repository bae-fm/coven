use super::*;
use coven_crypto::custody::KeyringCustody;
use coven_database::{Database, FileDatabase};
use coven_format::{
    store_log::{MemberPublicKeys, StoreChange},
    MemberAccess,
};
use coven_storage::{
    test_utils::{Faults, MemoryStorage},
    ObjectPath, ObjectPrefix, S3Credentials, StorageFailure,
};
use coven_sync::{DeviceLogSync, Files, Operations};
use std::time::UNIX_EPOCH;

fn tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
}

fn migrations() -> Vec<Migration> {
    vec![Migration::sql(
        1,
        "notes",
        "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL, body BLOB NOT NULL)",
    )]
}

fn tokens(value: &str) -> OAuthTokens {
    OAuthTokens {
        access_token: SecretText::new(value.into()),
        refresh_token: Some(SecretText::new("refresh".into())),
        expires_at: None,
    }
}

struct Owner {
    _root: tempfile::TempDir,
    directory: StoreDir,
    db: Database,
    member: MemberKeys,
    keys: Arc<dyn StoreKeyCustody>,
    operations: Operations,
    files: Files,
    storage: Arc<MemoryStorage>,
    clock: Arc<FixedClock>,
    code: RestoreCode,
}

impl Owner {
    async fn new(provider: CloudProvider, snapshot: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(root.path().into());
        let ids = Arc::new(UuidIds);
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
        let app = TestCoven::new();
        let directory = app
            .create_store(&layout, "Household", ids.clone())
            .await
            .unwrap();
        let config = match provider {
            CloudProvider::S3 => StorageConfig::S3 {
                bucket: "test".into(),
                region: "us-east-1".into(),
                endpoint: None,
                prefix: "household".into(),
            },
            CloudProvider::GoogleDrive => StorageConfig::GoogleDrive {
                folder_id: "folder".into(),
            },
            CloudProvider::Dropbox => StorageConfig::Dropbox {
                namespace_id: "folder".into(),
            },
            CloudProvider::OneDrive => StorageConfig::OneDrive {
                drive_id: "drive".into(),
                folder_id: "folder".into(),
            },
            CloudProvider::CloudKit => StorageConfig::CloudKit {
                container: "container".into(),
                owner: "owner".into(),
                zone: "zone".into(),
            },
        };
        let storage = Arc::new(
            MemoryStorage::new(config.clone(), clock.clone())
                .unwrap()
                .with_transfer_limits(65536, 65536)
                .unwrap(),
        );
        let member = MemberKeys::generate().unwrap();
        let identity = Arc::new(InMemoryCustody::new(member.clone()));
        let keys: Arc<dyn StoreKeyCustody> = Arc::new(InMemoryCustody::<StoreKeyring>::empty());
        let db = DatabaseBuilder::new(directory.clone())
            .synced_tables(tables())
            .migrations(migrations())
            .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
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
        let access = if provider == CloudProvider::S3 {
            MemberAccess::S3AccessKey {
                access_key_id: "owner-key".into(),
            }
        } else {
            MemberAccess::ProviderAccount("owner@example.com".into())
        };
        sync.make_and_upload_entry(StoreChange::CreateStore {
            store: directory.id(),
            name: "Household".into(),
            admin: MemberPublicKeys {
                signing: member.member_id(),
                sealing: member.sealing_public_key(),
            },
            access,
            device_name: "Owner".into(),
            key: KeyId(ids.new_id()),
        })
        .await
        .unwrap();
        db.write(move |sql| {
            sql.execute(
                "INSERT INTO notes VALUES('before',?1)",
                coven_database::params![vec![42_u8; if snapshot { 1_100_000 } else { 37 }]],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        let mut writes = DeviceLogSync::new(storage.clone(), db.clone(), keys.clone(), identity);
        writes.upload_writes().await.unwrap();
        if snapshot {
            sync.write_snapshots().await.unwrap();
            assert_eq!(
                storage
                    .list(&ObjectPrefix::snapshots())
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }
        db.write(|sql| {
            sql.execute(
                "INSERT INTO notes VALUES('after',?1)",
                coven_database::params![vec![17_u8; 91]],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        writes.upload_writes().await.unwrap();
        let credentials = match provider {
            CloudProvider::S3 => StorageCredentials::S3(S3Credentials {
                access_key_id: "owner-key".into(),
                secret_access_key: SecretText::new("owner-secret".into()),
            }),
            CloudProvider::CloudKit => StorageCredentials::CloudKit,
            _ => StorageCredentials::OAuth(tokens("owner-token")),
        };
        let code = RestoreCode {
            store: directory.id(),
            name: "Household".into(),
            member_keys: member.clone(),
            storage: RestoreStorage {
                location: config,
                credentials,
            }
            .encode()
            .unwrap(),
        };
        let files = Files::new(
            FileDatabase::new(db.clone()),
            directory.clone(),
            Some(storage.clone()),
            clock.clone(),
            ids,
        );
        let operations = Operations::new(sync, files.clone());
        Self {
            _root: root,
            directory,
            db,
            member,
            keys,
            operations,
            files,
            storage,
            clock,
            code,
        }
    }

    async fn invite(&self) -> Invite {
        let access = if self.storage.config().provider() == CloudProvider::S3 {
            InviteAccess::S3AccessKey {
                access_key_id: "invited-key".into(),
                secret_access_key: SecretText::new("invited-secret".into()),
            }
        } else {
            InviteAccess::ProviderAccount {
                email: "join@example.com".into(),
            }
        };
        self.operations
            .create_invite(MemberRole::Member, access)
            .await
            .unwrap()
    }

    fn recipient(&self) -> Arc<MemoryStorage> {
        if self.storage.config().provider() == CloudProvider::S3 {
            self.storage.clone()
        } else {
            Arc::new(MemoryStorage::for_recipient(&self.storage, "join@example.com").unwrap())
        }
    }

    async fn close(self) {
        self.operations.close().await.unwrap();
        self.files.close().await;
        self.db.close().await.unwrap();
    }
}

struct Installation {
    root: tempfile::TempDir,
    layout: StoreLayout,
    keychain: Arc<Keychain>,
}

impl Installation {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(root.path().into());
        Self {
            root,
            layout,
            keychain: Keychain::in_memory("bootstrap-test").unwrap(),
        }
    }

    async fn run(
        &self,
        owner: &Owner,
        request: BootstrapRequest,
        storage: Arc<MemoryStorage>,
        cancel: &watch::Receiver<bool>,
        status: impl Fn(&str),
    ) -> Result<Option<StoreDir>, BootstrapError> {
        bootstrap_device(
            request,
            &tables(),
            &migrations(),
            CovenMigrationPolicy::ApplyPending,
            KeyCustody::Keyring,
            IdentityCustody::Keyring,
            Some(tokens("joining-token")),
            &self.layout,
            Arc::new(OAuthClients::new(None, None, None, owner.clock.clone())),
            None,
            owner.clock.clone(),
            Arc::new(UuidIds),
            status,
            cancel,
            self.keychain.clone(),
            Some(storage),
        )
        .await
    }

    fn join_request(invite: &Invite) -> BootstrapRequest {
        BootstrapRequest::Join {
            code: coven_sync::read_invite_code(&invite.code).unwrap(),
            name: "New phone".into(),
        }
    }

    async fn absent(&self, id: StoreId) {
        assert!(self.layout.stores().await.unwrap().is_empty());
        let scoped = Arc::new(StoreKeychain::new(self.keychain.clone(), id));
        assert!(
            StoreKeyCustody::unlock(&KeyringCustody::new(scoped.clone()))
                .unwrap()
                .is_none()
        );
        assert!(
            MemberKeyCustody::unlock(&KeyringCustody::new(scoped.clone()))
                .unwrap()
                .is_none()
        );
        assert!(scoped.device_id().unwrap().is_none());
        assert!(scoped.storage_credentials().unwrap().is_none());
        assert!(scoped.synced_restore_code().unwrap().is_none());
    }

    async fn handle(&self, directory: StoreDir, owner: &Owner) -> CovenHandle {
        Coven::builder(directory)
            .with_keychain(self.keychain.clone())
            .synced_tables(tables())
            .migrations(migrations())
            .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
            .clock(owner.clock.clone())
            .storage(owner.storage.clone())
            .open()
            .await
            .unwrap()
    }
}

async fn next_request(requests: &mut watch::Receiver<Vec<JoinRequest>>) -> JoinRequest {
    loop {
        if let Some(request) = requests.borrow_and_update().first() {
            return request.clone();
        }
        requests.changed().await.unwrap();
    }
}

async fn rows(db: &Database) -> Vec<(String, Vec<u8>)> {
    db.read(|sql| {
        Ok(sql.query("SELECT id,body FROM notes ORDER BY id", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?)
    })
    .await
    .unwrap()
}

async fn fingerprint(
    db: &Database,
    keys: &dyn StoreKeyCustody,
) -> Vec<(Audience, coven_crypto::Fingerprint)> {
    let ring = keys.unlock().unwrap().unwrap();
    let key = db
        .local_store_log()
        .await
        .unwrap()
        .log
        .replay
        .state
        .store
        .unwrap()
        .key;
    db.sync_state(vec![(
        Audience::Store,
        ring.store_key(key).unwrap().derive().fingerprint_hasher(),
    )])
    .await
    .unwrap()
    .fingerprints
}

#[tokio::test]
async fn restore_loads_snapshots_and_later_writes_with_identical_fingerprints() {
    let owner = Owner::new(CloudProvider::S3, true).await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let directory = install
        .run(
            &owner,
            BootstrapRequest::Restore(
                coven_sync::read_restore_code(&owner.code.to_text().unwrap()).unwrap(),
            ),
            owner.storage.clone(),
            &cancel,
            |_| {},
        )
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        directory.settings().unwrap().device_id,
        owner.directory.settings().unwrap().device_id
    );
    let db = DatabaseBuilder::new(directory.clone())
        .synced_tables(tables())
        .migrations(migrations())
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
        .open()
        .await
        .unwrap();
    let scoped = Arc::new(StoreKeychain::new(install.keychain.clone(), directory.id()));
    let keys = KeyringCustody::<StoreKeyring>::new(scoped.clone());
    assert_eq!(rows(&db).await, rows(&owner.db).await);
    assert_eq!(
        fingerprint(&db, &keys).await,
        fingerprint(&owner.db, owner.keys.as_ref()).await
    );
    let log = db.local_store_log().await.unwrap();
    assert_eq!(
        log.log.replay.state.devices[&log.device].member,
        owner.member.member_id()
    );
    assert!(directory
        .owned_file(StoreFile::Bootstrap)
        .read_optional()
        .unwrap()
        .is_none());
    db.close().await.unwrap();
    let handle = install.handle(directory, &owner).await;
    assert_eq!(
        handle.restore_code().await.unwrap(),
        owner.code.to_text().unwrap().as_str()
    );
    let synced = keychain_code(&install.keychain).unwrap().unwrap();
    assert_eq!(
        synced.to_text().unwrap().as_str(),
        handle.restore_code().await.unwrap()
    );
    handle.close().await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn join_accepts_provider_access_and_approval_then_loads_the_store() {
    for provider in [
        CloudProvider::S3,
        CloudProvider::GoogleDrive,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
        CloudProvider::CloudKit,
    ] {
        let owner = Owner::new(provider, false).await;
        let invite = owner.invite().await;
        let install = Installation::new();
        let (_, cancel) = watch::channel(false);
        let mut requests = owner.operations.subscribe_join_requests();
        let joining = install.run(
            &owner,
            Installation::join_request(&invite),
            owner.recipient(),
            &cancel,
            |_| {},
        );
        let approve = async {
            let request = next_request(&mut requests).await;
            assert_eq!(request.device_name, "New phone");
            install.absent(owner.directory.id()).await;
            owner
                .operations
                .approve_join_request(&request)
                .await
                .unwrap();
            request.member
        };
        let (result, member) = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::join!(joining, approve)
        })
        .await
        .unwrap();
        let directory = result.unwrap().unwrap();
        let handle = install.handle(directory, &owner).await;
        let restored =
            coven_sync::read_restore_code(&handle.restore_code().await.unwrap()).unwrap();
        assert_eq!(restored.member_keys.member_id(), member);
        assert_ne!(member, owner.member.member_id());
        assert_eq!(
            handle
                .read(|sql| Ok(
                    sql.query("SELECT id,body FROM notes ORDER BY id", [], |r| Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Vec<u8>>(1)?
                    )))?
                ))
                .await
                .unwrap(),
            rows(&owner.db).await
        );
        assert_eq!(
            handle
                .get_members()
                .await
                .unwrap()
                .iter()
                .find(|m| m.is_self)
                .unwrap()
                .devices
                .len(),
            1
        );
        handle.close().await.unwrap();
        owner.close().await;
    }
}

#[tokio::test]
async fn restart_while_waiting_reuses_the_member_device_and_request_bytes() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let mut requests = owner.operations.subscribe_join_requests();
    let mut joining = Box::pin(install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    ));
    let request = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::select! { result = &mut joining => panic!("join returned before approval: {result:?}"), request = next_request(&mut requests) => request }
    }).await.unwrap();
    let bytes = owner
        .storage
        .read(&ObjectPath::join_request(invite.id))
        .await
        .unwrap();
    drop(joining);
    install.absent(owner.directory.id()).await;
    let pending = install
        .layout
        .begin_bootstrap(owner.directory.id(), "Household", &UuidIds)
        .unwrap();
    let device = pending.directory().settings().unwrap().device_id;
    drop(pending);
    let resumed = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let approve = async {
        assert_eq!(
            owner
                .storage
                .read(&ObjectPath::join_request(invite.id))
                .await
                .unwrap(),
            bytes
        );
        owner
            .operations
            .approve_join_request(&request)
            .await
            .unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(resumed, approve)
    })
    .await
    .unwrap();
    let directory = result.unwrap().unwrap();
    assert_eq!(directory.settings().unwrap().device_id, device);
    assert_eq!(
        keychain_code(&install.keychain)
            .unwrap()
            .unwrap()
            .member_keys
            .member_id(),
        request.member
    );
    owner.close().await;
}

#[tokio::test]
async fn declined_and_expired_requests_finish_without_committing_custody() {
    for expiry in [false, true] {
        let owner = Owner::new(CloudProvider::S3, false).await;
        let invite = owner.invite().await;
        let install = Installation::new();
        let (_, cancel) = watch::channel(false);
        let mut requests = owner.operations.subscribe_join_requests();
        let joining = install.run(
            &owner,
            Installation::join_request(&invite),
            owner.recipient(),
            &cancel,
            |_| {},
        );
        let settle = async {
            let request = next_request(&mut requests).await;
            if expiry {
                owner.clock.set(invite.expires_at);
                owner.operations.report().await.unwrap();
            } else {
                owner
                    .operations
                    .decline_join_request(&request)
                    .await
                    .unwrap();
            }
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::join!(joining, settle)
        })
        .await
        .unwrap();
        assert!(result.unwrap().is_none());
        install.absent(owner.directory.id()).await;
        assert!(!install
            .root
            .path()
            .join("stores/.coven-bootstrap")
            .join(owner.directory.id().to_string())
            .exists());
        owner.close().await;
    }
}

#[tokio::test]
async fn permission_revocation_while_joining_keeps_the_provider_failure() {
    let owner = Owner::new(CloudProvider::GoogleDrive, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let mut requests = owner.operations.subscribe_join_requests();
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let revoke = async {
        let request = next_request(&mut requests).await;
        owner
            .operations
            .decline_join_request(&request)
            .await
            .unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(joining, revoke)
    })
    .await
    .unwrap();
    assert!(
        matches!(result, Err(BootstrapError::Sync(SyncError::Storage(error))) if error.failure() == StorageFailure::PermissionDenied)
    );
    install.absent(owner.directory.id()).await;
    owner.close().await;
}

#[tokio::test]
async fn cancellation_while_waiting_removes_every_local_installation_value() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (stop, cancel) = watch::channel(false);
    let mut requests = owner.operations.subscribe_join_requests();
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let cancel_join = async {
        next_request(&mut requests).await;
        stop.send(true).unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(joining, cancel_join)
    })
    .await
    .unwrap();
    assert!(matches!(result, Err(BootstrapError::Cancelled)));
    install.absent(owner.directory.id()).await;
    assert!(!install
        .root
        .path()
        .join("stores/.coven-bootstrap")
        .join(owner.directory.id().to_string())
        .exists());
    owner.operations.cancel_invite(&invite.id).await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn cancellation_during_restore_and_before_publication_keeps_final_custody_empty() {
    for phase in [
        "Preparing this device",
        "Opening member keys and the store log",
        "Keeping keys and credentials",
    ] {
        let owner = Owner::new(CloudProvider::S3, false).await;
        let install = Installation::new();
        let (stop, cancel) = watch::channel(false);
        let result = install
            .run(
                &owner,
                BootstrapRequest::Restore(
                    coven_sync::read_restore_code(&owner.code.to_text().unwrap()).unwrap(),
                ),
                owner.storage.clone(),
                &cancel,
                |status| {
                    if status == phase {
                        stop.send(true).unwrap();
                    }
                },
            )
            .await;
        assert!(
            matches!(result, Err(BootstrapError::Cancelled)),
            "{phase}: {result:?}"
        );
        install.absent(owner.directory.id()).await;
        assert!(!install
            .root
            .path()
            .join("stores/.coven-bootstrap")
            .join(owner.directory.id().to_string())
            .exists());
        owner.close().await;
    }
}

#[tokio::test]
async fn storage_failure_retains_staging_for_retry_without_final_keys() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    owner
        .storage
        .set_faults(Faults {
            fail_next: 1,
            ..Faults::none()
        })
        .await;
    assert!(matches!(
        install
            .run(
                &owner,
                BootstrapRequest::Restore(
                    coven_sync::read_restore_code(&owner.code.to_text().unwrap()).unwrap()
                ),
                owner.storage.clone(),
                &cancel,
                |_| {}
            )
            .await,
        Err(BootstrapError::Sync(SyncError::Storage(_)))
    ));
    install.absent(owner.directory.id()).await;
    assert!(install
        .run(
            &owner,
            BootstrapRequest::Restore(
                coven_sync::read_restore_code(&owner.code.to_text().unwrap()).unwrap()
            ),
            owner.storage.clone(),
            &cancel,
            |_| {}
        )
        .await
        .unwrap()
        .is_some());
    owner.close().await;
}

#[tokio::test]
async fn restore_codes_track_s3_keys_and_oauth_credentials_in_synced_custody() {
    for provider in [CloudProvider::S3, CloudProvider::GoogleDrive] {
        let owner = Owner::new(provider, false).await;
        let install = Installation::new();
        let (_, cancel) = watch::channel(false);
        let directory = install
            .run(
                &owner,
                BootstrapRequest::Restore(
                    coven_sync::read_restore_code(&owner.code.to_text().unwrap()).unwrap(),
                ),
                owner.storage.clone(),
                &cancel,
                |_| {},
            )
            .await
            .unwrap()
            .unwrap();
        let handle = install.handle(directory.clone(), &owner).await;
        let before = handle.restore_code().await.unwrap();
        let info = decode_code_info(&before).unwrap();
        assert_eq!(info.kind, CodeKind::Restore);
        assert_eq!(info.store_id, directory.id());
        assert_eq!(info.cloud_provider, provider);
        assert_eq!(info.needs_oauth, provider != CloudProvider::S3);
        let mut updated = coven_sync::read_restore_code(&before).unwrap();
        let mut data = RestoreStorage::decode(updated.storage.as_bytes()).unwrap();
        data.credentials = if provider == CloudProvider::S3 {
            StorageCredentials::S3(S3Credentials {
                access_key_id: "replacement".into(),
                secret_access_key: SecretText::new("new-secret".into()),
            })
        } else {
            StorageCredentials::OAuth(tokens("replacement"))
        };
        updated.storage = data.encode().unwrap();
        let after = updated.to_text().unwrap();
        if provider == CloudProvider::S3 {
            assert_eq!(
                handle
                    .replace_access_key("replacement".into(), SecretText::new("new-secret".into()))
                    .await
                    .unwrap(),
                after.as_str()
            );
            assert_eq!(
                owner.storage.s3_access_key_id().await.as_deref(),
                Some("replacement")
            );
        } else {
            handle.update_credentials(&after).await.unwrap();
        }
        assert_ne!(before, after.as_str());
        assert_eq!(handle.restore_code().await.unwrap(), after.as_str());
        assert_eq!(
            keychain_code(&install.keychain)
                .unwrap()
                .unwrap()
                .to_text()
                .unwrap()
                .as_str(),
            after.as_str()
        );
        let mut wrong_member = coven_sync::read_restore_code(&after).unwrap();
        wrong_member.member_keys = MemberKeys::generate().unwrap();
        assert!(matches!(
            handle
                .update_credentials(&wrong_member.to_text().unwrap())
                .await,
            Err(SyncError::WrongMember { .. })
        ));
        let mut wrong_store = coven_sync::read_restore_code(&after).unwrap();
        wrong_store.store = StoreId(UuidIds.new_id());
        assert!(matches!(
            handle
                .update_credentials(&wrong_store.to_text().unwrap())
                .await,
            Err(SyncError::WrongStore { .. })
        ));
        assert_eq!(handle.restore_code().await.unwrap(), after.as_str());
        handle.close().await.unwrap();
        let handle = Coven::builder(directory)
            .with_keychain(install.keychain.clone())
            .synced_tables(tables())
            .migrations(migrations())
            .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
            .clock(owner.clock.clone())
            .open()
            .await
            .unwrap();
        assert_eq!(handle.restore_code().await.unwrap(), after.as_str());
        handle.update_credentials(&before).await.unwrap();
        assert_eq!(handle.restore_code().await.unwrap(), before);
        handle.close().await.unwrap();
        assert!(matches!(
            handle.restore_code().await,
            Err(SyncError::Database(DbError::StoreClosed))
        ));
        owner.close().await;
    }
}

#[tokio::test]
async fn keychain_discovery_refuses_ambiguous_or_mismatched_store_ids() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    assert!(keychain_code(&install.keychain).unwrap().is_none());
    let scoped = StoreKeychain::new(install.keychain.clone(), owner.directory.id());
    scoped
        .set_synced_restore_code(&SecretBytes::new(owner.code.to_bytes().unwrap().to_vec()))
        .unwrap();
    assert_eq!(
        keychain_code(&install.keychain).unwrap().unwrap().store,
        owner.directory.id()
    );
    let other = StoreKeychain::new(install.keychain.clone(), StoreId(UuidIds.new_id()));
    other
        .set_synced_restore_code(&SecretBytes::new(owner.code.to_bytes().unwrap().to_vec()))
        .unwrap();
    assert!(
        matches!(keychain_code(&install.keychain), Err(BootstrapError::MultipleStores(ids)) if ids.len() == 2)
    );
    scoped.delete_synced_restore_code().unwrap();
    assert!(matches!(
        keychain_code(&install.keychain),
        Err(BootstrapError::Code(CodeError::Invalid))
    ));
    owner.close().await;
}

#[tokio::test]
async fn a_device_removed_during_loading_receives_the_provider_refusal() {
    let owner = Owner::new(CloudProvider::GoogleDrive, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let mut requests = owner.operations.subscribe_join_requests();
    let (listed, listing) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    owner
        .storage
        .hold_next_listing(ObjectPrefix::device_logs(), listed, resumed)
        .await;
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let remove = async {
        let request = next_request(&mut requests).await;
        owner
            .operations
            .approve_join_request(&request)
            .await
            .unwrap();
        listing.await.unwrap();
        owner.operations.sync_store_log().await.unwrap();
        let members = owner.operations.get_members().await.unwrap();
        let devices = &members
            .iter()
            .find(|m| m.id == request.member)
            .unwrap()
            .devices;
        assert_eq!(devices.len(), 1);
        assert!(matches!(
            owner.operations.remove_device(devices[0]).await.unwrap(),
            ProviderSignOut::RemoveAppAccess {
                provider: CloudProvider::GoogleDrive
            }
        ));
        // Cut off this account in the fake to model provider refusal on the removed device.
        owner
            .storage
            .revoke_access(&MemberAccess::ProviderAccount("join@example.com".into()))
            .await
            .unwrap();
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(joining, remove)
    })
    .await
    .unwrap();
    assert!(
        matches!(result, Err(BootstrapError::Sync(SyncError::Storage(error))) if error.failure() == StorageFailure::PermissionDenied)
    );
    install.absent(owner.directory.id()).await;
    owner.close().await;
}

#[tokio::test]
async fn approval_during_a_log_listing_is_not_mistaken_for_decline() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let mut requests = owner.operations.subscribe_join_requests();
    let (listed, listing) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    owner
        .storage
        .hold_next_listing(ObjectPrefix::store_logs(), listed, resumed)
        .await;
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let approve = async {
        listing.await.unwrap();
        let request = next_request(&mut requests).await;
        owner
            .operations
            .approve_join_request(&request)
            .await
            .unwrap();
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(joining, approve)
    })
    .await
    .unwrap();
    assert!(
        result.unwrap().is_some(),
        "approval deleted the request after this device took its log listing"
    );
    owner.close().await;
}
