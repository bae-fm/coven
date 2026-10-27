use super::*;
pub(super) use crate::tests::{join_and_approve, join_with_response, next_join_request};

pub(super) fn tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
}

pub(super) fn migrations() -> Vec<Migration> {
    vec![Migration::addition(
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
    pub(super) member: MemberIdentity,
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
        let member = MemberIdentity::new(code.member, code.member_keys.clone());
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
                crate::tests::snapshots(&storage, directory.id())
                    .await
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
        // Passes read join requests and approve them; the loop keeps running.
        handle.start_sync().await.unwrap();
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
    }

    /// One pass, which may leave operations pending.
    pub(super) async fn pass(&self) {
        self.handle.start_sync().await.unwrap();
        self.network.pass(0).await;
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

    /// The last slot the owner applied.
    pub(super) async fn applied(&self) -> u64 {
        self.handle.applied_slot().await
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
            .create_invite(
                InviteTarget::NewMember {
                    role: MemberRole::Member,
                    name: "Carol".into(),
                },
                access,
                InviteDelivery::Sent {
                    lifetime: InviteLifetime::Day,
                },
            )
            .await
            .unwrap()
    }

    pub(super) fn s3_credentials(&self) -> Option<S3Credentials> {
        (self.storage.config().provider() == CloudProvider::S3).then(|| S3Credentials {
            access_key_id: "owner-key".into(),
            secret_access_key: SecretText::new("secret".into()),
        })
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
            .clock(Arc::new(crate::tests::PollingClock::new(
                owner.clock.clone(),
            )))
            .storage_connector(storage)
    }

    pub(super) async fn authenticated_builder(
        &self,
        owner: &Owner,
        storage: Arc<MemoryStorage>,
    ) -> CovenBuilder {
        let mut builder = self.builder(owner, storage);
        let provider = owner.storage.config().provider();
        if matches!(provider, CloudProvider::Dropbox | CloudProvider::OneDrive) {
            let sign_in = crate::authentication::SignIn::new(owner.clock.clone()).await;
            builder = sign_in.configure(builder);
            builder
                .authenticate(owner.storage.config().oauth_access().unwrap())
                .await
                .unwrap();
        }
        builder
    }

    pub(super) async fn run(
        &self,
        owner: &Owner,
        request: BootstrapRequest,
        storage: Arc<MemoryStorage>,
        cancel: &watch::Receiver<bool>,
        status: impl Fn(BootstrapStatus),
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
            owner.s3_credentials(),
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
            data: ConnectionCredentials {
                location: owner.storage.config(),
                credentials: StorageCredentials::S3(
                    owner.s3_credentials().expect("S3 restore fixture"),
                ),
            },
            sign_in: None,
            name: "Ana’s laptop".into(),
        }
    }

    pub(super) fn join_request(invite: &Invite) -> BootstrapRequest {
        BootstrapRequest::Join {
            code: coven_sync::read_invite_code(&invite.code).unwrap(),
            name: DeviceName::Given("New phone".into()),
            // Nobody asks this join to check now.
            check: watch::channel(0).1,
        }
    }

    /// Nothing of the store is listed or in custody.
    pub(super) async fn absent(&self, id: StoreId) {
        assert!(self.only_kept_sign_in(id).await.is_none());
    }

    /// Nothing of the store is listed or in custody but the provider sign-in
    /// a waiting join keeps, which this returns (§13.2).
    pub(super) async fn only_kept_sign_in(
        &self,
        id: StoreId,
    ) -> Option<coven_storage::OAuthTokens> {
        assert!(self.layout.stores().await.unwrap().is_empty());
        let scoped = Arc::new(StoreKeychain::new(self.keychain.clone(), id));
        let synced = SyncedKeychain::new(self.keychain.clone(), id);
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
        assert!(synced.restore_code().unwrap().is_none());
        assert!(synced.s3_credentials().unwrap().is_none());
        self.device_tokens(id)
    }

    /// The OAuth sign-in in the store's device credentials.
    pub(super) fn device_tokens(&self, id: StoreId) -> Option<coven_storage::OAuthTokens> {
        let bytes = StoreKeychain::new(self.keychain.clone(), id)
            .storage_credentials()
            .unwrap()?;
        match StorageCredentials::decode(bytes.as_bytes()).unwrap() {
            StorageCredentials::OAuth(tokens) => Some(tokens),
            _ => panic!("the device credentials hold no OAuth sign-in"),
        }
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

/// A status callback that cancels at the `phase.1`-th report of `phase.0`.
pub(super) fn cancel_at<'a>(
    phase: &'a (BootstrapStatus, usize),
    stop: &'a watch::Sender<bool>,
) -> impl Fn(BootstrapStatus) + 'a {
    let seen = std::cell::Cell::new(0);
    move |status| {
        if status == phase.0 {
            seen.set(seen.get() + 1);
            if seen.get() == phase.1 {
                stop.send(true).unwrap();
            }
        }
    }
}

/// Start Carol's join with a sent invite and stop it while it waits, as when
/// the app is closed.
pub(super) async fn waiting_join(owner: &Owner, install: &Installation, invite: &Invite) {
    waiting_join_with(
        install
            .authenticated_builder(owner, owner.recipient())
            .await,
        invite,
    )
    .await;
}

/// Start Carol's join through `builder` and stop it while it waits.
pub(super) async fn waiting_join_with(builder: CovenBuilder, invite: &Invite) {
    let (waiting, mut waits) = watch::channel(false);
    let control = JoinControl::new();
    let joining = join_with_invite(
        builder,
        &invite.code,
        "Carol's phone",
        |status| {
            if matches!(status, BootstrapStatus::WaitingForApproval { .. }) {
                waiting.send_replace(true);
            }
        },
        &control,
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        tokio::select! {
            result = joining => panic!("the join ended while waiting: {result:?}"),
            changed = waits.wait_for(|waiting| *waiting) => drop(changed.unwrap()),
        }
    })
    .await
    .unwrap();
}

pub(super) fn saved_code(install: &Installation, store: StoreId) -> Option<SecretText> {
    StoreKeychain::new(install.keychain.clone(), store)
        .invite_code()
        .unwrap()
}
