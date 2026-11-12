use super::*;
use crate::*;
use axum::{extract::State, Json, Router};
use coven_crypto::custody::{Keychain, PersonEntries, StoreKeychain};
use coven_storage::{test_utils::MemoryStorage, Storage, StorageCredentials};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

pub(crate) struct Presenter {
    pub(crate) cancel: AtomicBool,
    pub(crate) calls: AtomicUsize,
    requests: std::sync::Mutex<Vec<std::collections::BTreeMap<String, String>>>,
    /// Holds the next presentation.
    held: Held,
}

/// A step a test holds: it says it has been reached, then waits until its
/// future is dropped.
type Held = tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>;

async fn hold_if_asked(held: &Held) {
    if let Some(reached) = held.lock().await.take() {
        reached.send(()).unwrap();
        std::future::pending::<()>().await;
    }
}

#[async_trait::async_trait]
impl OAuthPresenter for Presenter {
    fn redirect_uri(&self, _: CloudProvider) -> &str {
        "coven-app://sign-in"
    }
    async fn present(&self, authorization_url: &str) -> Result<SecretText, OAuthError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        hold_if_asked(&self.held).await;
        if self.cancel.load(Ordering::SeqCst) {
            return Err(OAuthError::Cancelled);
        }
        let url = url::Url::parse(authorization_url).unwrap();
        let params: std::collections::BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(params["redirect_uri"], "coven-app://sign-in");
        self.requests.lock().unwrap().push(params.clone());
        Ok(SecretText::new(format!("coven-app://sign-in?code=app-code&state={}", params["state"])))
    }
}

struct TokenServerState {
    requests: AtomicUsize,
    /// Holds the next token reply.
    held: Held,
}

pub(crate) struct SignIn {
    endpoint: String,
    presenter: Arc<Presenter>,
    server: Arc<TokenServerState>,
    task: tokio::task::JoinHandle<()>,
}
impl SignIn {
    pub(crate) async fn new() -> Self {
        let server =
            Arc::new(TokenServerState { requests: AtomicUsize::new(0), held: Held::default() });
        let router = Router::new().fallback(|State(server): State<Arc<TokenServerState>>| async move {
            let n = server.requests.fetch_add(1, Ordering::SeqCst);
            hold_if_asked(&server.held).await;
            Json(serde_json::json!({"access_token": format!("access-{n}"), "refresh_token": format!("refresh-{n}"), "token_type": "Bearer", "expires_in": 3600}))
        }).with_state(server.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            endpoint,
            presenter: Arc::new(Presenter {
                cancel: AtomicBool::new(false),
                calls: AtomicUsize::new(0),
                requests: std::sync::Mutex::new(Vec::new()),
                held: Held::default(),
            }),
            server,
            task,
        }
    }
    pub(crate) fn configure(&self, builder: CovenBuilder) -> CovenBuilder {
        builder.oauth_clients(self.clients()).oauth_presenter(self.presenter.clone())
    }
    pub(crate) fn presentation_count(&self) -> usize {
        self.presenter.calls.load(Ordering::SeqCst)
    }
    pub(crate) fn request_count(&self) -> usize {
        self.server.requests.load(Ordering::SeqCst)
    }
    pub(crate) fn clients(&self) -> OAuthClients {
        OAuthClients::new(
            Some(DropboxClients::Both {
                app_folder_key: "dropbox-folder".into(),
                full_dropbox_key: "dropbox".into(),
            }),
            Some("onedrive".into()),
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
    app: &Coven,
    clock: ClockRef,
    storage: Arc<MemoryStorage>,
    sign_in: &SignIn,
) -> CovenBuilder {
    app.builder()
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
    let keychain = Keychain::in_memory("oauth-custody").unwrap();
    let app = Coven::with_keychain(root.path().into(), keychain.clone());
    let clock = Arc::new(FixedClock::new(std::time::SystemTime::UNIX_EPOCH));
    let sign_in = SignIn::new().await;
    let storage = Arc::new(
        MemoryStorage::builder()
            .provider(coven_storage::CloudProvider::Dropbox)
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let handle = builder(&app, clock.clone(), storage.clone(), &sign_in)
        .create_store("Account")
        .await
        .unwrap();
    let directory = app.store_dir(&handle.store_id());
    let scoped = StoreKeychain::new(keychain.clone(), directory.id());
    let person = PersonEntries::new(keychain.clone(), directory.id());

    assert!(matches!(
        handle.setup_oauth_storage(storage.config(), "Laptop", |_| {}).await,
        Err(StorageSetupError::OAuth(OAuthError::Reauthorize(CloudProvider::Dropbox)))
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
    assert!(handle.setup_oauth_storage(storage.config(), "Laptop", |_| {}).await.is_err());
    assert!(scoped.storage_credentials().unwrap().is_none());
    storage.set_online(true);
    handle.setup_oauth_storage(storage.config(), "Laptop", |_| {}).await.unwrap();
    let bytes = scoped.storage_credentials().unwrap().unwrap();
    let StorageCredentials::OAuth(tokens) = StorageCredentials::decode(bytes.as_bytes()).unwrap()
    else {
        panic!("OAuth credentials")
    };
    assert_eq!(tokens.access_token.as_str(), "access-0");
    let code = handle.restore_code().await.unwrap();
    let decoded = coven_sync::read_restore_code(&code).unwrap();
    assert!(matches!(
        coven_storage::RestoreStorage::decode(decoded.storage.as_bytes()).unwrap().location,
        StorageConfig::Dropbox { .. }
    ));
    assert!(matches!(
        handle.use_access_key("s3-id".into(), SecretText::new("s3-secret".into())).await,
        Err(SyncError::Storage(error)) if error.failure() == StorageFailure::InvalidConfiguration
    ));
    handle.close().await.unwrap();
    person.delete_restore_code().unwrap();
    let held_s3_key = SecretBytes::new(b"separately held key".to_vec());
    person.set_s3_credentials(&held_s3_key).unwrap();
    // A clock set a year ahead does not expire the token; the provider's
    // rejection does, and the next request refreshes it once.
    clock.set(clock.now() + std::time::Duration::from_secs(365 * 24 * 3600));
    let handle = builder(&app, clock.clone(), storage.clone(), &sign_in)
        .open(directory.id(), |_| ())
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
    assert!(person.restore_code().unwrap().is_none());
    assert_eq!(person.s3_credentials().unwrap().unwrap().as_bytes(), held_s3_key.as_bytes());
    assert_eq!(
        handle.restore_code().await.unwrap(),
        code,
        "refresh does not change the restore code"
    );
    assert_eq!(sign_in.request_count(), 2);
    assert_eq!(sign_in.presenter.calls.load(Ordering::SeqCst), 2);
    handle.close().await.unwrap();
    // The device's tokens are gone, as when the person deletes the keychain
    // entry: connecting the recorded location again needs a new sign-in.
    scoped.delete_storage_credentials().unwrap();
    let handle =
        builder(&app, clock, storage.clone(), &sign_in).open(directory.id(), |_| ()).await.unwrap();
    assert!(matches!(
        handle.setup_oauth_storage(storage.config(), "Laptop", |_| {}).await,
        Err(StorageSetupError::OAuth(OAuthError::Reauthorize(CloudProvider::Dropbox)))
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
    let keychain = Keychain::in_memory("oauth-builder").unwrap();
    let app = Coven::with_keychain(root.path().into(), keychain.clone());
    let clock = Arc::new(FixedClock::new(std::time::SystemTime::UNIX_EPOCH));
    let sign_in = SignIn::new().await;
    let storage = Arc::new(
        MemoryStorage::builder()
            .provider(coven_storage::CloudProvider::Dropbox)
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let mut builder = builder(&app, clock.clone(), storage.clone(), &sign_in);
    builder.authenticate(coven_storage::OAuthAccess::OneDrive).await.unwrap();
    let handle = builder.create_store("Account").await.unwrap();
    let store = handle.store_id();

    assert!(matches!(
        handle.setup_oauth_storage(storage.config(), "Laptop", |_| {}).await,
        Err(StorageSetupError::OAuth(OAuthError::Reauthorize(CloudProvider::Dropbox)))
    ));
    handle
        .authenticate(coven_storage::OAuthAccess::Dropbox(
            coven_storage::DropboxAccess::FullDropbox,
        ))
        .await
        .unwrap();
    storage.reject_access_token("access-1").await;
    handle.setup_oauth_storage(storage.config(), "Laptop", |_| {}).await.unwrap();
    assert_eq!(sign_in.request_count(), 3);
    assert_eq!(sign_in.presenter.calls.load(Ordering::SeqCst), 2);
    // Setup commits the session's refreshed sign-in, not the rejected one.
    let StorageCredentials::OAuth(tokens) = StorageCredentials::decode(
        StoreKeychain::new(keychain, store).storage_credentials().unwrap().unwrap().as_bytes(),
    )
    .unwrap() else {
        panic!("OAuth credentials")
    };
    assert_eq!(tokens.access_token.as_str(), "access-2");
    handle.close().await.unwrap();
}

/// Coven ships no sign-in clients: a builder the app gave none refuses to
/// sign in, without presenting anything (E1).
#[tokio::test]
async fn without_the_apps_clients_sign_in_is_unavailable() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let sign_in = SignIn::new().await;
    let mut builder = app.builder().oauth_presenter(sign_in.presenter.clone());
    assert!(matches!(
        builder.authenticate(coven_storage::OAuthAccess::OneDrive).await,
        Err(OAuthError::Unavailable(CloudProvider::OneDrive))
    ));
    assert_eq!(sign_in.presentation_count(), 0);
    assert_eq!(sign_in.request_count(), 0);
}

/// A presenter that reports it was asked and never answers.
struct Unanswered {
    asked: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl OAuthPresenter for Unanswered {
    fn redirect_uri(&self, _: CloudProvider) -> &str {
        "coven-app://sign-in"
    }
    async fn present(&self, _: &str) -> Result<SecretText, OAuthError> {
        self.asked.notify_one();
        std::future::pending().await
    }
}

/// A sign-in that is cancelled by the presenter, or dropped while waiting on
/// it, leaves the builder's earlier sign-in in place (E1).
#[tokio::test]
async fn a_failed_or_dropped_sign_in_keeps_the_prior_one() {
    let root = tempfile::tempdir().unwrap();
    let keychain = Keychain::in_memory("oauth-prior").unwrap();
    let app = Coven::with_keychain(root.path().into(), keychain.clone());
    let clock = Arc::new(FixedClock::new(std::time::SystemTime::UNIX_EPOCH));
    let sign_in = SignIn::new().await;
    let storage = Arc::new(
        MemoryStorage::builder()
            .provider(coven_storage::CloudProvider::OneDrive)
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let mut builder = builder(&app, clock.clone(), storage.clone(), &sign_in);
    builder.authenticate(coven_storage::OAuthAccess::OneDrive).await.unwrap();
    assert_eq!(sign_in.presentation_count(), 1);

    sign_in.presenter.cancel.store(true, Ordering::SeqCst);
    assert!(matches!(
        builder.authenticate(coven_storage::OAuthAccess::OneDrive).await,
        Err(OAuthError::Cancelled)
    ));
    sign_in.presenter.cancel.store(false, Ordering::SeqCst);
    assert_eq!(sign_in.presentation_count(), 2);

    // The app drops the sign-in while the sheet is up.
    let unanswered = Arc::new(Unanswered { asked: tokio::sync::Notify::new() });
    let mut builder = builder.oauth_presenter(unanswered.clone());
    tokio::select! {
        _ = builder.authenticate(coven_storage::OAuthAccess::OneDrive) => {
            panic!("a sign-in nobody answered returned")
        }
        () = unanswered.asked.notified() => {}
    }
    let builder = builder.oauth_presenter(sign_in.presenter.clone());

    // The first sign-in is what setup commits: no new presentation, and the
    // tokens of the one successful exchange.
    let handle = builder.create_store("Account").await.unwrap();
    handle.setup_oauth_storage(storage.config(), "Laptop", |_| {}).await.unwrap();
    assert_eq!(sign_in.presentation_count(), 2);
    let StorageCredentials::OAuth(tokens) = StorageCredentials::decode(
        StoreKeychain::new(keychain, handle.store_id())
            .storage_credentials()
            .unwrap()
            .unwrap()
            .as_bytes(),
    )
    .unwrap() else {
        panic!("OAuth credentials")
    };
    assert_eq!(tokens.access_token.as_str(), "access-0");
    handle.close().await.unwrap();
}

mod provider_access_tests {
    use super::*;

    struct Fixture {
        _root: tempfile::TempDir,
        app: Coven,
        directory: StoreDir,
        keychain: Arc<Keychain>,
        clock: ClockRef,
        sign_in: SignIn,
        storage: Arc<MemoryStorage>,
    }
    impl Fixture {
        async fn new(location: StorageConfig) -> Self {
            let root = tempfile::tempdir().unwrap();
            let keychain = Keychain::in_memory("provider-access").unwrap();
            let app = Coven::with_keychain(root.path().into(), keychain.clone());
            let clock: ClockRef = Arc::new(FixedClock::new(std::time::UNIX_EPOCH));
            let sign_in = SignIn::new().await;
            let storage = Arc::new(
                MemoryStorage::builder().location(location).clock(clock.clone()).build().unwrap(),
            );
            let handle = builder(&app, clock.clone(), storage.clone(), &sign_in)
                .create_store("Account")
                .await
                .unwrap();
            let directory = app.store_dir(&handle.store_id());
            handle.close().await.unwrap();
            Self { _root: root, app, directory, keychain, clock, sign_in, storage }
        }
        fn builder(&self) -> CovenBuilder {
            builder(&self.app, self.clock.clone(), self.storage.clone(), &self.sign_in)
        }
        async fn setup(&self, builder: CovenBuilder) -> CovenHandle {
            let handle = builder.open(self.directory.id(), |_| ()).await.unwrap();

            handle.authenticate(self.storage.config().oauth_access().unwrap()).await.unwrap();
            handle.setup_oauth_storage(self.storage.config(), "Laptop", |_| {}).await.unwrap();
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
        InviteAccess::ProviderAccount { email: "member@example.test".into() }
    }
    #[tokio::test]
    async fn app_folder_invites_fail_before_provider_requests() {
        let fixture = Fixture::new(StorageConfig::Dropbox {
            account_label: None,
            location: DropboxLocation::AppFolder,
            folder_label: None,
        })
        .await;
        let handle = fixture.setup(fixture.builder()).await;
        // Setup starts sync; its first pass's requests come first.
        handle.test_parked().await;
        let before = fixture.storage.request_count();
        let credentials = fixture.credentials();
        assert!(
            matches!(handle.create_invite(InviteTarget::NewMember { role: MemberRole::Member }, member(), crate::InviteLifetime::SevenDays, "Ana".into()).await, Err(SyncError::Storage(error)) if error.failure() == StorageFailure::DropboxAppFolderNotShareable)
        );
        assert_eq!(fixture.storage.request_count(), before);
        assert_eq!(fixture.sign_in.presentation_count(), 1);
        assert_eq!(fixture.credentials().as_bytes(), credentials.as_bytes());
        handle.close().await.unwrap();
    }

    /// A store set up in a Dropbox app folder stays there: setting it up
    /// again at the shared-folder access level is refused before any
    /// request, and its settings and restore code stay as they were (§20.2,
    /// §20.3).
    #[tokio::test]
    async fn reconnecting_a_dropbox_store_with_the_other_access_level_is_refused() {
        let fixture = Fixture::new(StorageConfig::Dropbox {
            account_label: None,
            location: DropboxLocation::AppFolder,
            folder_label: None,
        })
        .await;
        let handle = fixture.setup(fixture.builder()).await;
        let code = handle.restore_code().await.unwrap();
        let recorded = coven_storage::recorded_config(&fixture.directory).unwrap();
        assert!(recorded.is_some());
        // Setup starts sync; its first pass's requests come first.
        handle.test_parked().await;
        let before = fixture.storage.request_count();
        let shared = StorageConfig::Dropbox {
            account_label: None,
            location: DropboxLocation::SharedFolder { namespace_id: "other-level".into() },
            folder_label: None,
        };
        let error = handle.setup_oauth_storage(shared, "Laptop", |_| {}).await.unwrap_err();
        assert_eq!(error.to_string(), "storage location change is unsupported");
        assert_eq!(fixture.storage.request_count(), before);
        assert_eq!(handle.restore_code().await.unwrap(), code);
        assert_eq!(coven_storage::recorded_config(&fixture.directory).unwrap(), recorded);
        handle.close().await.unwrap();
    }
}

/// Dropping `authenticate` while the provider's page is presented, or while
/// its token reply is awaited, ends sign-in at once: no tokens are
/// committed, the call lock is free for the next call, and the earlier
/// sign-in is still the one setup commits (§20.2).
#[tokio::test]
async fn dropping_sign_in_while_presenting_or_exchanging_cancels_it_and_keeps_the_prior_sign_in() {
    let root = tempfile::tempdir().unwrap();
    let keychain = Keychain::in_memory("oauth-cancel").unwrap();
    let app = Coven::with_keychain(root.path().into(), keychain.clone());
    let clock = Arc::new(FixedClock::new(std::time::SystemTime::UNIX_EPOCH));
    let sign_in = SignIn::new().await;
    let storage = Arc::new(
        MemoryStorage::builder()
            .provider(coven_storage::CloudProvider::Dropbox)
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let handle =
        builder(&app, clock, storage.clone(), &sign_in).create_store("Account").await.unwrap();
    let scoped = StoreKeychain::new(keychain, app.store_dir(&handle.store_id()).id());
    let access = || coven_storage::OAuthAccess::Dropbox(coven_storage::DropboxAccess::FullDropbox);
    handle.authenticate(access()).await.unwrap();
    assert_eq!(sign_in.request_count(), 1);

    let (reached, presenting) = tokio::sync::oneshot::channel();
    *sign_in.presenter.held.lock().await = Some(reached);
    let mut replacing = Box::pin(handle.authenticate(access()));
    tokio::select! {
        biased;
        _ = &mut replacing => panic!("sign-in finished while its page was held"),
        reached = presenting => reached.unwrap(),
    }
    drop(replacing);
    assert_eq!(sign_in.request_count(), 1, "no token was asked for while presenting");

    let (reached, exchanging) = tokio::sync::oneshot::channel();
    *sign_in.server.held.lock().await = Some(reached);
    let mut replacing = Box::pin(handle.authenticate(access()));
    tokio::select! {
        biased;
        _ = &mut replacing => panic!("sign-in finished while its token reply was held"),
        reached = exchanging => reached.unwrap(),
    }
    drop(replacing);
    assert_eq!(sign_in.request_count(), 2, "the held exchange was the second token request");
    assert!(scoped.storage_credentials().unwrap().is_none(), "a dropped sign-in commits nothing");

    // The call lock is free, and the sign-in setup commits is the first one.
    handle.setup_oauth_storage(storage.config(), "Laptop", |_| {}).await.unwrap();
    let StorageCredentials::OAuth(tokens) =
        StorageCredentials::decode(scoped.storage_credentials().unwrap().unwrap().as_bytes())
            .unwrap()
    else {
        panic!("OAuth credentials")
    };
    assert_eq!(tokens.access_token.as_str(), "access-0");
    handle.close().await.unwrap();
}
