use super::*;
pub(super) use crate::tests::{join_and_approve, join_with_response, next_join_request};

pub(super) fn tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
}

pub(super) fn migrations() -> Vec<Migration> {
    vec![Migration::sql(
        1,
        "notes",
        "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL, body BLOB NOT NULL)",
    )]
}

#[cfg(test)]
pub(super) struct Owner {
    network: crate::tests::Network,
    pub(super) directory: StoreDir,
    pub(super) handle: CovenHandle,
    pub(super) member: MemberKeys,
    pub(super) storage: Arc<MemoryStorage>,
    pub(super) clock: Arc<FixedClock>,
    pub(super) code: RestoreCode,
}

impl Owner {
    pub(super) async fn new(provider: CloudProvider, snapshot: bool) -> Self {
        let network = crate::tests::Network::with_schema(provider, tables(), migrations()).await;
        let device = &network.devices[0];
        let handle = device.handle.clone();
        let directory = device.layout.store_dir(&device.store);
        let storage = device.storage.clone();
        let clock = network.clock.clone();
        let code = coven_sync::read_restore_code(&handle.restore_code().await.unwrap()).unwrap();
        let member = code.member_keys.clone();
        handle
            .write(move |sql| {
                sql.execute(
                    "INSERT INTO notes VALUES('before',?1)",
                    coven_database::params![vec![42_u8; if snapshot { 1_100_000 } else { 37 }]],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        network.sync(0).await;
        if snapshot {
            assert_eq!(
                storage
                    .list(&ObjectPrefix::snapshots())
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }
        handle
            .write(|sql| {
                sql.execute(
                    "INSERT INTO notes VALUES('after',?1)",
                    coven_database::params![vec![17_u8; 91]],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        network.sync(0).await;
        // Keep operation calls connected without a background sync pass racing
        // the joining device for held requests and injected failures.
        handle.unlock_store_key().await.unwrap();
        Self {
            network,
            directory,
            handle,
            member,
            storage,
            clock,
            code,
        }
    }

    pub(super) async fn sync(&self) {
        self.handle.start_sync().await.unwrap();
        self.network.sync(0).await;
        self.handle.unlock_store_key().await.unwrap();
    }

    pub(super) async fn rows(&self) -> Vec<(String, Vec<u8>)> {
        self.handle
            .read(|sql| {
                Ok(sql.query("SELECT id,body FROM notes ORDER BY id", [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })?)
            })
            .await
            .unwrap()
    }

    pub(super) async fn fingerprints(&self) -> Vec<(Audience, coven_crypto::Fingerprint)> {
        crate::tests::posted(&self.network.devices[0])
            .await
            .fingerprints
            .into_iter()
            .map(|f| (f.audience, f.bytes))
            .collect()
    }

    pub(super) async fn invite(&self) -> Invite {
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
        self.handle
            .create_invite(MemberRole::Member, access)
            .await
            .unwrap()
    }

    pub(super) fn recipient(&self) -> Arc<MemoryStorage> {
        if self.storage.config().provider() == CloudProvider::S3 {
            self.storage.clone()
        } else {
            Arc::new(MemoryStorage::for_recipient(&self.storage, "join@example.com").unwrap())
        }
    }

    pub(super) async fn close(self) {
        self.network.close().await;
    }
}

#[cfg(test)]
pub(super) struct Installation {
    pub(super) root: tempfile::TempDir,
    pub(super) layout: StoreLayout,
    pub(super) keychain: Arc<Keychain>,
}

impl Installation {
    pub(super) fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let layout = StoreLayout::new(root.path().into());
        Self {
            root,
            layout,
            keychain: Keychain::in_memory("bootstrap-test").unwrap(),
        }
    }

    pub(super) fn builder(&self, owner: &Owner, storage: Arc<MemoryStorage>) -> CovenBuilder {
        Coven::builder(self.layout.clone())
            .with_keychain(self.keychain.clone())
            .synced_tables(tables())
            .migrations(migrations())
            .clock(Arc::new(crate::tests::PollingClock(owner.clock.clone())))
            .storage_connector(storage)
    }

    pub(super) async fn authenticated_builder(
        &self,
        owner: &Owner,
        storage: Arc<MemoryStorage>,
    ) -> CovenBuilder {
        let mut builder = self.builder(owner, storage);
        let provider = owner.storage.config().provider();
        if matches!(
            provider,
            CloudProvider::GoogleDrive | CloudProvider::Dropbox | CloudProvider::OneDrive
        ) {
            let sign_in = crate::authentication::SignIn::new(owner.clock.clone()).await;
            builder = sign_in.configure(builder);
            builder.authenticate(provider).await.unwrap();
        }
        builder
    }

    pub(super) async fn run(
        &self,
        owner: &Owner,
        request: BootstrapRequest,
        storage: Arc<MemoryStorage>,
        cancel: &watch::Receiver<bool>,
        status: impl Fn(&str),
    ) -> Result<Option<CovenHandle>, BootstrapError> {
        bootstrap_device(
            self.authenticated_builder(owner, storage).await,
            request,
            status,
            cancel,
        )
        .await
    }

    pub(super) async fn restore(&self, owner: &Owner) -> CovenHandle {
        let (_, cancel) = watch::channel(false);
        restore_from_code(
            self.authenticated_builder(owner, owner.storage.clone())
                .await,
            &owner.code.to_text().unwrap(),
            "Ana’s laptop",
            |_| {},
            &cancel,
        )
        .await
        .unwrap()
    }

    pub(super) fn restore_request(owner: &Owner) -> BootstrapRequest {
        BootstrapRequest::Restore {
            code: coven_sync::read_restore_code(&owner.code.to_text().unwrap()).unwrap(),
            name: "Ana’s laptop".into(),
        }
    }

    pub(super) fn join_request(invite: &Invite) -> BootstrapRequest {
        BootstrapRequest::Join {
            code: coven_sync::read_invite_code(&invite.code).unwrap(),
            name: "New phone".into(),
        }
    }

    pub(super) async fn absent(&self, id: StoreId) {
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

    pub(super) async fn handle(&self, directory: StoreDir, owner: &Owner) -> CovenHandle {
        let handle = self
            .builder(owner, owner.storage.clone())
            .open(directory.id())
            .await
            .unwrap();
        handle.unlock_store_key().await.unwrap();
        handle
    }
}

pub(super) async fn rows(db: &Database) -> Vec<(String, Vec<u8>)> {
    db.read(|sql| {
        Ok(sql.query("SELECT id,body FROM notes ORDER BY id", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?)
    })
    .await
    .unwrap()
}

pub(super) async fn fingerprint(
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
