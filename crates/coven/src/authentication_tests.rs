use super::*;
use crate::*;
use axum::{extract::State, Json, Router};
use coven_crypto::custody::{Keychain, StoreKeychain};
use coven_storage::{test_utils::MemoryStorage, RestoreStorage, Storage};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

pub(crate) struct Presenter {
    pub(crate) cancel: AtomicBool,
    pub(crate) calls: AtomicUsize,
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
        Ok(SecretText::new(format!(
            "coven-app://sign-in?code=app-code&state={}",
            params["state"]
        )))
    }
}

pub(crate) struct SignIn {
    endpoint: String,
    clock: ClockRef,
    presenter: Arc<Presenter>,
    requests: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl SignIn {
    pub(crate) async fn new(clock: ClockRef) -> Self {
        let requests = Arc::new(AtomicUsize::new(0));
        let router = Router::new().fallback(|State(requests): State<Arc<AtomicUsize>>| async move {
            let n = requests.fetch_add(1, Ordering::SeqCst);
            Json(serde_json::json!({"access_token": format!("access-{n}"), "refresh_token": format!("refresh-{n}"), "token_type": "Bearer", "expires_in": 3600}))
        }).with_state(requests.clone());
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
            }),
            requests,
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
        self.requests.load(Ordering::SeqCst)
    }
    pub(crate) fn clients(&self) -> OAuthClients {
        OAuthClients::new(
            Some("google".into()),
            Some("dropbox".into()),
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
    let scoped = StoreKeychain::new(keychain.clone(), directory.id());
    let clock = Arc::new(FixedClock::new(std::time::SystemTime::UNIX_EPOCH));
    let sign_in = SignIn::new(clock.clone()).await;
    let storage = Arc::new(
        MemoryStorage::new(
            StorageConfig::Dropbox {
                namespace_id: "folder".into(),
            },
            clock.clone(),
        )
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
    handle.authenticate(CloudProvider::Dropbox).await.unwrap();
    assert!(scoped.storage_credentials().unwrap().is_none());
    // A failed or cancelled replacement preserves the prior successful sign-in.
    sign_in.presenter.cancel.store(true, Ordering::SeqCst);
    assert!(matches!(
        handle.authenticate(CloudProvider::Dropbox).await,
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
        RestoreStorage::decode(decoded.storage.as_bytes()).unwrap(),
        RestoreStorage::Account(_)
    ));
    assert!(matches!(
        handle.update_credentials(&code).await,
        Err(SyncError::Storage(error)) if error.failure() == StorageFailure::InvalidConfiguration
    ));
    handle.close().await.unwrap();
    scoped.delete_synced_restore_code().unwrap();
    clock.set(clock.now() + std::time::Duration::from_secs(3600));
    let handle = builder(layout, keychain, clock, storage.clone(), &sign_in)
        .open(directory.id())
        .await
        .unwrap();
    handle.start_sync().await.unwrap();
    let StorageCredentials::OAuth(tokens) =
        StorageCredentials::decode(scoped.storage_credentials().unwrap().unwrap().as_bytes())
            .unwrap()
    else {
        panic!("OAuth credentials")
    };
    assert_eq!(tokens.access_token.as_str(), "access-1");
    assert!(scoped.synced_restore_code().unwrap().is_none());
    assert_eq!(
        handle.restore_code().await.unwrap(),
        code,
        "refresh does not change the restore code"
    );
    assert_eq!(sign_in.requests.load(Ordering::SeqCst), 2);
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
        handle.authenticate(CloudProvider::Dropbox).await,
        Err(StorageSetupError::SecureStorage(KeyError::StoreClosed))
    ));
}

#[tokio::test]
async fn builder_sign_in_is_provider_bound_and_refreshes_before_setup() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let directory = app
        .create_store(&layout, "Account", Arc::new(UuidIds))
        .await
        .unwrap();
    let keychain = Keychain::in_memory("oauth-builder").unwrap();
    let clock = Arc::new(FixedClock::new(std::time::SystemTime::UNIX_EPOCH));
    let sign_in = SignIn::new(clock.clone()).await;
    let storage = Arc::new(
        MemoryStorage::new(
            StorageConfig::Dropbox {
                namespace_id: "folder".into(),
            },
            clock.clone(),
        )
        .unwrap(),
    );
    let mut builder = builder(layout, keychain, clock.clone(), storage.clone(), &sign_in);
    builder
        .authenticate(CloudProvider::GoogleDrive)
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
    handle.authenticate(CloudProvider::Dropbox).await.unwrap();
    clock.set(clock.now() + std::time::Duration::from_secs(3600));
    handle
        .setup_oauth_storage(storage.config(), "Laptop")
        .await
        .unwrap();
    assert_eq!(sign_in.requests.load(Ordering::SeqCst), 3);
    assert_eq!(sign_in.presenter.calls.load(Ordering::SeqCst), 2);
    handle.close().await.unwrap();
}
