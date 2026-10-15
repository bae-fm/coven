use super::*;
use crate::*;
use axum::{extract::State, Json, Router};
use coven_crypto::custody::{Keychain, StoreKeychain};
use coven_storage::{test_utils::MemoryStorage, Storage, StorageCredentials};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

pub(crate) struct Presenter {
    pub(crate) cancel: AtomicBool,
    pub(crate) calls: AtomicUsize,
    requests: std::sync::Mutex<Vec<std::collections::BTreeMap<String, String>>>,
}
#[async_trait::async_trait]
impl OAuthPresenter for Presenter {
    fn redirect_uri(&self, _: CloudProvider) -> &str {
        "coven-app://sign-in"
    }
    async fn present(&self, authorization_url: &str) -> Result<SecretText, OAuthError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.cancel.load(Ordering::SeqCst) {
            return Err(OAuthError::Cancelled);
        }
        let url = url::Url::parse(authorization_url).unwrap();
        let params: std::collections::BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(params["redirect_uri"], "coven-app://sign-in");
        self.requests.lock().unwrap().push(params.clone());
        Ok(SecretText::new(format!(
            "coven-app://sign-in?code=app-code&state={}",
            params["state"]
        )))
    }
}

struct TokenServerState {
    requests: AtomicUsize,
    scope: std::sync::Mutex<Option<String>>,
}

pub(crate) struct SignIn {
    endpoint: String,
    clock: ClockRef,
    presenter: Arc<Presenter>,
    server: Arc<TokenServerState>,
    task: tokio::task::JoinHandle<()>,
}
impl SignIn {
    pub(crate) async fn new(clock: ClockRef) -> Self {
        let server = Arc::new(TokenServerState {
            requests: AtomicUsize::new(0),
            scope: std::sync::Mutex::new(None),
        });
        let router = Router::new().fallback(|State(server): State<Arc<TokenServerState>>| async move {
            let n = server.requests.fetch_add(1, Ordering::SeqCst);
            let mut reply = serde_json::json!({"access_token": format!("access-{n}"), "refresh_token": format!("refresh-{n}"), "token_type": "Bearer", "expires_in": 3600});
            if let Some(scope) = &*server.scope.lock().unwrap() { reply["scope"] = serde_json::json!(scope); }
            Json(reply)
        }).with_state(server.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            endpoint,
            clock,
            presenter: Arc::new(Presenter {
                cancel: AtomicBool::new(false),
                calls: AtomicUsize::new(0),
                requests: std::sync::Mutex::new(Vec::new()),
            }),
            server,
            task,
        }
    }
    pub(crate) fn configure(&self, builder: CovenBuilder) -> CovenBuilder {
        builder
            .oauth_clients(self.clients())
            .oauth_presenter(self.presenter.clone())
    }
    pub(crate) fn presentation_count(&self) -> usize {
        self.presenter.calls.load(Ordering::SeqCst)
    }
    pub(crate) fn request_count(&self) -> usize {
        self.server.requests.load(Ordering::SeqCst)
    }
    pub(crate) fn clients(&self) -> OAuthClients {
        self.clients_with_google_access(GoogleDriveAccess::Full)
    }
    fn clients_with_google_access(&self, registered: GoogleDriveAccess) -> OAuthClients {
        OAuthClients::new(
            Some(GoogleDriveClient::new("google".into(), registered)),
            Some(DropboxClients::Both {
                app_folder_key: "dropbox-folder".into(),
                full_dropbox_key: "dropbox".into(),
            }),
            Some("onedrive".into()),
            self.clock.clone(),
        )
        .with_token_endpoint(self.endpoint.clone())
    }
}
impl Drop for SignIn {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn builder(
    layout: StoreLayout,
    keychain: Arc<Keychain>,
    clock: ClockRef,
    storage: Arc<MemoryStorage>,
    sign_in: &SignIn,
) -> CovenBuilder {
    Coven::builder(layout)
        .with_keychain(keychain)
        .synced_tables(Vec::new())
        .migrations(Vec::new())
        .clock(clock)
        .oauth_clients(sign_in.clients())
        .oauth_presenter(sign_in.presenter.clone())
        .storage_connector(storage)
}

#[tokio::test]
async fn handle_sign_in_setup_refresh_and_reopen_keep_tokens_in_coven() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let directory = app
        .create_store(&layout, "Account", Arc::new(UuidIds))
        .await
        .unwrap();
    let keychain = Keychain::in_memory("oauth-custody").unwrap();
    // The same installation: its device-only custody holds the directory's id.
    StoreKeychain::new(keychain.clone(), directory.id())
        .set_device_id(directory.settings().unwrap().device_id)
        .unwrap();
    let scoped = StoreKeychain::new(keychain.clone(), directory.id());
    let clock = Arc::new(FixedClock::new(std::time::SystemTime::UNIX_EPOCH));
    let sign_in = SignIn::new(clock.clone()).await;
    let storage = Arc::new(
        MemoryStorage::builder()
            .provider(coven_storage::CloudProvider::Dropbox)
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let handle = builder(
        layout.clone(),
        keychain.clone(),
        clock.clone(),
        storage.clone(),
        &sign_in,
    )
    .open(directory.id())
    .await
    .unwrap();
    handle.initialize_identity().unwrap();
    assert!(matches!(
        handle.setup_oauth_storage(storage.config(), "Laptop").await,
        Err(StorageSetupError::OAuth(OAuthError::Reauthorize(
            CloudProvider::Dropbox
        )))
    ));
    assert_eq!(sign_in.presenter.calls.load(Ordering::SeqCst), 0);
    handle
        .authenticate(coven_storage::OAuthAccess::Dropbox(
            coven_storage::DropboxAccess::FullDropbox,
        ))
        .await
        .unwrap();
    assert!(scoped.storage_credentials().unwrap().is_none());
    // A failed or cancelled replacement preserves the prior successful sign-in.
    sign_in.presenter.cancel.store(true, Ordering::SeqCst);
    assert!(matches!(
        handle
            .authenticate(coven_storage::OAuthAccess::Dropbox(
                coven_storage::DropboxAccess::FullDropbox
            ))
            .await,
        Err(StorageSetupError::OAuth(OAuthError::Cancelled))
    ));
    sign_in.presenter.cancel.store(false, Ordering::SeqCst);
    storage.set_online(false);
    assert!(handle
        .setup_oauth_storage(storage.config(), "Laptop")
        .await
        .is_err());
    assert!(scoped.storage_credentials().unwrap().is_none());
    storage.set_online(true);
    handle
        .setup_oauth_storage(storage.config(), "Laptop")
        .await
        .unwrap();
    let bytes = scoped.storage_credentials().unwrap().unwrap();
    let StorageCredentials::OAuth(tokens) = StorageCredentials::decode(bytes.as_bytes()).unwrap()
    else {
        panic!("OAuth credentials")
    };
    assert_eq!(tokens.access_token.as_str(), "access-0");
    let code = handle.restore_code().await.unwrap();
    let decoded = coven_sync::read_restore_code(&code).unwrap();
    assert!(matches!(
        StorageConfig::decode(decoded.storage.as_bytes()).unwrap(),
        StorageConfig::Dropbox { .. }
    ));
    assert!(matches!(
        handle.update_credentials("s3-id".into(), SecretText::new("s3-secret".into())).await,
        Err(SyncError::Storage(error)) if error.failure() == StorageFailure::InvalidConfiguration
    ));
    handle.close().await.unwrap();
    scoped.delete_synced_restore_code().unwrap();
    let held_s3_key = SecretBytes::new(b"separately held key".to_vec());
    scoped.set_synced_s3_credentials(&held_s3_key).unwrap();
    // A clock set a year ahead does not expire the token; the provider's
    // rejection does, and the next request refreshes it once.
    clock.set(clock.now() + std::time::Duration::from_secs(365 * 24 * 3600));
    let handle = builder(layout, keychain, clock, storage.clone(), &sign_in)
        .open(directory.id())
        .await
        .unwrap();
    handle.unlock_store_key().await.unwrap();
    assert_eq!(sign_in.request_count(), 1, "a token is used until rejected");
    storage.reject_access_token("access-0").await;
    handle.unlock_store_key().await.unwrap();
    let StorageCredentials::OAuth(tokens) =
        StorageCredentials::decode(scoped.storage_credentials().unwrap().unwrap().as_bytes())
            .unwrap()
    else {
        panic!("OAuth credentials")
    };
    assert_eq!(tokens.access_token.as_str(), "access-1");
    assert!(scoped.synced_restore_code().unwrap().is_none());
    assert_eq!(
        scoped.synced_s3_credentials().unwrap().unwrap().as_bytes(),
        held_s3_key.as_bytes()
    );
    assert_eq!(
        handle.restore_code().await.unwrap(),
        code,
        "refresh does not change the restore code"
    );
    assert_eq!(sign_in.request_count(), 2);
    assert_eq!(sign_in.presenter.calls.load(Ordering::SeqCst), 2);
    handle.disconnect_storage().await.unwrap();
    assert!(scoped.storage_credentials().unwrap().is_none());
    assert!(matches!(
        handle.setup_oauth_storage(storage.config(), "Laptop").await,
        Err(StorageSetupError::OAuth(OAuthError::Reauthorize(
            CloudProvider::Dropbox
        )))
    ));
    handle.close().await.unwrap();
    assert!(matches!(
        handle
            .authenticate(coven_storage::OAuthAccess::Dropbox(
                coven_storage::DropboxAccess::FullDropbox
            ))
            .await,
        Err(StorageSetupError::SecureStorage(KeyError::StoreClosed))
    ));
}

#[tokio::test]
async fn builder_sign_in_is_provider_bound_and_refreshes_when_setup_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let directory = app
        .create_store(&layout, "Account", Arc::new(UuidIds))
        .await
        .unwrap();
    let keychain = Keychain::in_memory("oauth-builder").unwrap();
    // The same installation: its device-only custody holds the directory's id.
    StoreKeychain::new(keychain.clone(), directory.id())
        .set_device_id(directory.settings().unwrap().device_id)
        .unwrap();
    let clock = Arc::new(FixedClock::new(std::time::SystemTime::UNIX_EPOCH));
    let sign_in = SignIn::new(clock.clone()).await;
    let storage = Arc::new(
        MemoryStorage::builder()
            .provider(coven_storage::CloudProvider::Dropbox)
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let mut builder = builder(
        layout,
        keychain.clone(),
        clock.clone(),
        storage.clone(),
        &sign_in,
    );
    builder
        .authenticate(coven_storage::OAuthAccess::GoogleDrive(
            coven_storage::GoogleDriveAccess::File,
        ))
        .await
        .unwrap();
    let handle = builder.open(directory.id()).await.unwrap();
    handle.initialize_identity().unwrap();
    assert!(matches!(
        handle.setup_oauth_storage(storage.config(), "Laptop").await,
        Err(StorageSetupError::OAuth(OAuthError::Reauthorize(
            CloudProvider::Dropbox
        )))
    ));
    handle
        .authenticate(coven_storage::OAuthAccess::Dropbox(
            coven_storage::DropboxAccess::FullDropbox,
        ))
        .await
        .unwrap();
    storage.reject_access_token("access-1").await;
    handle
        .setup_oauth_storage(storage.config(), "Laptop")
        .await
        .unwrap();
    assert_eq!(sign_in.request_count(), 3);
    assert_eq!(sign_in.presenter.calls.load(Ordering::SeqCst), 2);
    // Setup commits the session's refreshed sign-in, not the rejected one.
    let StorageCredentials::OAuth(tokens) = StorageCredentials::decode(
        StoreKeychain::new(keychain, directory.id())
            .storage_credentials()
            .unwrap()
            .unwrap()
            .as_bytes(),
    )
    .unwrap() else {
        panic!("OAuth credentials")
    };
    assert_eq!(tokens.access_token.as_str(), "access-2");
    handle.close().await.unwrap();
}

mod provider_access_tests {
    use super::*;
    use coven_storage::providers::StorageConnector;

    const FILE: OAuthAccess = OAuthAccess::GoogleDrive(GoogleDriveAccess::File);
    const FULL: OAuthAccess = OAuthAccess::GoogleDrive(GoogleDriveAccess::Full);

    struct Fixture {
        _root: tempfile::TempDir,
        layout: StoreLayout,
        directory: StoreDir,
        keychain: Arc<Keychain>,
        clock: ClockRef,
        sign_in: SignIn,
        storage: Arc<MemoryStorage>,
    }
    impl Fixture {
        async fn new(location: StorageConfig) -> Self {
            let root = tempfile::tempdir().unwrap();
            let layout = StoreLayout::new(root.path().into());
            let directory = TestCoven::new()
                .create_store(&layout, "Account", Arc::new(UuidIds))
                .await
                .unwrap();
            let keychain = Keychain::in_memory("provider-access").unwrap();
            StoreKeychain::new(keychain.clone(), directory.id())
                .set_device_id(directory.settings().unwrap().device_id)
                .unwrap();
            let clock: ClockRef = Arc::new(FixedClock::new(std::time::UNIX_EPOCH));
            let sign_in = SignIn::new(clock.clone()).await;
            let storage = Arc::new(
                MemoryStorage::builder()
                    .location(location)
                    .clock(clock.clone())
                    .build()
                    .unwrap(),
            );
            Self {
                _root: root,
                layout,
                directory,
                keychain,
                clock,
                sign_in,
                storage,
            }
        }
        fn builder(&self) -> CovenBuilder {
            builder(
                self.layout.clone(),
                self.keychain.clone(),
                self.clock.clone(),
                self.storage.clone(),
                &self.sign_in,
            )
        }
        async fn setup(&self, builder: CovenBuilder) -> CovenHandle {
            let handle = builder.open(self.directory.id()).await.unwrap();
            handle.initialize_identity().unwrap();
            handle
                .authenticate(self.storage.config().oauth_access().unwrap())
                .await
                .unwrap();
            handle
                .setup_oauth_storage(self.storage.config(), "Laptop")
                .await
                .unwrap();
            handle
        }
        fn credentials(&self) -> SecretBytes {
            StoreKeychain::new(self.keychain.clone(), self.directory.id())
                .storage_credentials()
                .unwrap()
                .unwrap()
        }
    }
    fn member() -> InviteAccess {
        InviteAccess::ProviderAccount {
            email: "member@example.test".into(),
        }
    }
    fn google() -> StorageConfig {
        StorageConfig::GoogleDrive {
            folder_id: "folder".into(),
        }
    }
    fn assert_access(bytes: &SecretBytes, expected: OAuthAccess) {
        let StorageCredentials::OAuth(tokens) =
            StorageCredentials::decode(bytes.as_bytes()).unwrap()
        else {
            panic!("OAuth credentials expected")
        };
        assert_eq!(tokens.access, expected);
    }

    #[tokio::test]
    async fn unregistered_drive_and_app_folder_invites_fail_before_provider_requests() {
        for (location, expected) in [
            (
                google(),
                StorageFailure::AccessNotRegistered { required: FULL },
            ),
            (
                StorageConfig::Dropbox {
                    location: DropboxLocation::AppFolder,
                },
                StorageFailure::DropboxAppFolderNotShareable,
            ),
        ] {
            let fixture = Fixture::new(location).await;
            let handle = fixture
                .setup(
                    fixture.builder().oauth_clients(
                        fixture
                            .sign_in
                            .clients_with_google_access(GoogleDriveAccess::File),
                    ),
                )
                .await;
            let before = fixture.storage.request_count();
            let credentials = fixture.credentials();
            assert!(
                matches!(handle.create_invite(InviteTarget::NewMember { role: MemberRole::Member }, member()).await, Err(SyncError::Storage(error)) if error.failure() == expected)
            );
            assert_eq!(fixture.storage.request_count(), before);
            assert_eq!(fixture.sign_in.presentation_count(), 1);
            assert_eq!(fixture.credentials().as_bytes(), credentials.as_bytes());
            handle.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn first_google_invite_upgrades_once_and_reopen_keeps_the_grant() {
        let fixture = Fixture::new(google()).await;
        let handle = fixture.setup(fixture.builder()).await;
        assert_access(&fixture.credentials(), FILE);
        let code = handle.restore_code().await.unwrap();
        let invite = handle
            .create_invite(
                InviteTarget::NewMember {
                    role: MemberRole::Member,
                },
                member(),
            )
            .await
            .unwrap();
        assert_access(&fixture.credentials(), FULL);
        assert_eq!(
            decode_code_info(&invite.code).unwrap().oauth_access,
            Some(FULL)
        );
        assert_eq!(fixture.sign_in.presentation_count(), 2);
        assert_eq!(handle.restore_code().await.unwrap(), code);
        {
            let requests = fixture.sign_in.presenter.requests.lock().unwrap();
            assert_eq!(
                requests[0]["scope"],
                "https://www.googleapis.com/auth/drive.file"
            );
            assert_eq!(
                requests[1]["scope"],
                "https://www.googleapis.com/auth/drive"
            );
            assert_eq!(requests[1]["include_granted_scopes"], "true");
            assert_eq!(requests[0]["client_id"], requests[1]["client_id"]);
        }
        handle.close().await.unwrap();
        let handle = fixture
            .builder()
            .open(fixture.directory.id())
            .await
            .unwrap();
        handle.start_sync().await.unwrap();
        // The first pass connects; calls needing storage follow it.
        let mut status = handle.subscribe_sync_status();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            status.wait_for(|s| matches!(s, SyncStatus::Synced { .. })),
        )
        .await
        .unwrap()
        .unwrap();
        handle
            .create_invite(
                InviteTarget::NewMember {
                    role: MemberRole::Member,
                },
                InviteAccess::ProviderAccount {
                    email: "other@example.test".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(fixture.sign_in.presentation_count(), 2);
        handle.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_and_insufficient_google_upgrades_preserve_credentials_and_objects() {
        let fixture = Fixture::new(google()).await;
        let handle = fixture.setup(fixture.builder()).await;
        let credentials = fixture.credentials();
        let prefix = coven_storage::ObjectPrefix::all(fixture.directory.id());
        let objects = fixture.storage.list(&prefix).await.unwrap();
        fixture
            .sign_in
            .presenter
            .cancel
            .store(true, Ordering::SeqCst);
        let Err(SyncError::Storage(coven_storage::StorageError::Provider { source, .. })) = handle
            .create_invite(
                InviteTarget::NewMember {
                    role: MemberRole::Member,
                },
                member(),
            )
            .await
        else {
            panic!("cancelled upgrade expected")
        };
        assert!(matches!(
            source.downcast_ref::<OAuthError>(),
            Some(OAuthError::Cancelled)
        ));
        fixture
            .sign_in
            .presenter
            .cancel
            .store(false, Ordering::SeqCst);
        *fixture.sign_in.server.scope.lock().unwrap() =
            Some("https://www.googleapis.com/auth/drive.file".into());
        assert!(
            matches!(handle.create_invite(InviteTarget::NewMember { role: MemberRole::Member }, member()).await, Err(SyncError::Storage(error)) if error.failure() == StorageFailure::AccessNotGranted { required: FULL })
        );
        assert_eq!(fixture.credentials().as_bytes(), credentials.as_bytes());
        assert_eq!(fixture.storage.list(&prefix).await.unwrap(), objects);
        *fixture.sign_in.server.scope.lock().unwrap() = None;
        handle
            .create_invite(
                InviteTarget::NewMember {
                    role: MemberRole::Member,
                },
                member(),
            )
            .await
            .unwrap();
        assert_access(&fixture.credentials(), FULL);
        handle.close().await.unwrap();
    }

    struct ChangedAccount(Arc<MemoryStorage>);
    #[async_trait::async_trait]
    impl StorageConnector for ChangedAccount {
        async fn connect(
            &self,
            config: StorageConfig,
            auth: coven_storage::ProviderAuth,
            device: DeviceId,
            publications: Arc<dyn coven_storage::PublicationGate>,
            cooldown: Arc<dyn coven_storage::Cooldown>,
        ) -> Result<Arc<coven_storage::StorageConnection>, coven_storage::StorageError> {
            if matches!(auth.credentials(), StorageCredentials::OAuth(tokens) if tokens.access == FULL)
            {
                MemoryStorage::for_recipient(&self.0, "another@example.test")?
                    .connect(config, auth, device, publications, cooldown)
                    .await
            } else {
                self.0
                    .connect(config, auth, device, publications, cooldown)
                    .await
            }
        }
    }

    #[tokio::test]
    async fn google_upgrade_refuses_another_account_before_committing_credentials() {
        let fixture = Fixture::new(google()).await;
        // This account can reach the folder but must not replace the owner's identity.
        fixture
            .storage
            .grant_access("another@example.test")
            .await
            .unwrap();
        let handle = fixture
            .setup(
                fixture
                    .builder()
                    .storage_connector(Arc::new(ChangedAccount(fixture.storage.clone()))),
            )
            .await;
        let credentials = fixture.credentials();
        assert!(
            matches!(handle.create_invite(InviteTarget::NewMember { role: MemberRole::Member }, member()).await, Err(SyncError::Storage(error)) if error.failure() == StorageFailure::AccountMismatch)
        );
        assert_eq!(fixture.credentials().as_bytes(), credentials.as_bytes());
        handle.close().await.unwrap();
    }
}
