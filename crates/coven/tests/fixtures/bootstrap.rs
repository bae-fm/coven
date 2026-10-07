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

// These integration fixtures control timestamps while approval polling uses runtime time.
fn clock_with_runtime_waits(time: &Arc<FixedClock>) -> ClockRef {
    let time = time.clone();
    Arc::new(coven_foundation::clock::ClosureClock(move || time.now()))
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
            TransferLimits::default(),
        );
        let operations = Operations::new(sync, files.clone(), writes, clock_with_runtime_waits(&clock));
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

    fn builder(&self, owner: &Owner, storage: Arc<MemoryStorage>) -> CovenBuilder {
        Coven::builder(self.layout.clone())
            .with_keychain(self.keychain.clone())
            .synced_tables(tables())
            .migrations(migrations())
            .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
            .clock(clock_with_runtime_waits(&owner.clock))
            .storage(storage.clone())
            .storage_connector(storage)
    }

    async fn run(
        &self,
        owner: &Owner,
        request: BootstrapRequest,
        storage: Arc<MemoryStorage>,
        cancel: &watch::Receiver<bool>,
        status: impl Fn(&str),
    ) -> Result<Option<CovenHandle>, BootstrapError> {
        bootstrap_device(
            self.builder(owner, storage),
            request,
            Some(tokens("joining-token")),
            status,
            cancel,
        )
        .await
    }

    async fn restore(&self, owner: &Owner) -> CovenHandle {
        let (_, cancel) = watch::channel(false);
        restore_from_code(
            self.builder(owner, owner.storage.clone()),
            &owner.code.to_text().unwrap(),
            "Ana’s laptop",
            Some(tokens("joining-token")),
            |_| {},
            &cancel,
        )
        .await
        .unwrap()
    }

    fn restore_request(owner: &Owner) -> BootstrapRequest {
        BootstrapRequest::Restore {
            code: coven_sync::read_restore_code(&owner.code.to_text().unwrap()).unwrap(),
            name: "Ana’s laptop".into(),
        }
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
        Coven::builder(self.layout.clone())
            .with_keychain(self.keychain.clone())
            .synced_tables(tables())
            .migrations(migrations())
            .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
            .clock(clock_with_runtime_waits(&owner.clock))
            .storage(owner.storage.clone())
            .open(directory.id())
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
