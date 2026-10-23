use super::*;
use coven_crypto::{custody::Keychain, SecretBytes, SecretText};
use coven_foundation::{clock::FixedClock, id_source::StoreId};
use coven_storage::OAuthAccess;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A token endpoint answering `access-N`; it holds each reply until released
/// when gated.
struct TokenServer {
    url: String,
    requests: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
    task: tokio::task::JoinHandle<()>,
}
impl TokenServer {
    async fn new(gated: bool) -> Self {
        let requests = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(if gated {
            0
        } else {
            tokio::sync::Semaphore::MAX_PERMITS
        }));
        let (count, notify, gate) = (requests.clone(), entered.clone(), release.clone());
        let router = axum::Router::new().fallback(move || {
            let (count, notify, gate) = (count.clone(), notify.clone(), gate.clone());
            async move {
                let n = count.fetch_add(1, Ordering::SeqCst);
                notify.notify_one();
                gate.acquire().await.unwrap().forget();
                axum::Json(serde_json::json!({
                    "access_token": format!("access-{n}"),
                    "token_type": "Bearer",
                    "expires_in": 3600,
                }))
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Self {
            url,
            requests,
            entered,
            release,
            task,
        }
    }
    fn clients(&self) -> OAuthClients {
        OAuthClients::new(
            None,
            Some("onedrive".into()),
            Arc::new(FixedClock::new(std::time::UNIX_EPOCH)),
        )
        .with_token_endpoint(self.url.clone())
    }
    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}
impl Drop for TokenServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn tokens(token: &str) -> OAuthTokens {
    OAuthTokens {
        client_id: "onedrive".into(),
        access: OAuthAccess::OneDrive,
        access_token: SecretText::new(token.into()),
        refresh_token: Some(SecretText::new("refresh".into())),
    }
}

fn custody(
    server: &TokenServer,
    held: Option<&str>,
) -> (Arc<Keychain>, Arc<StoreKeychain>, Arc<DeviceCredentials>) {
    let keychain = Keychain::in_memory("sign-in").unwrap();
    let id = store_id();
    let store = Arc::new(StoreKeychain::new(keychain.clone(), id));
    if let Some(token) = held {
        store
            .set_storage_credentials(&StorageCredentials::OAuth(tokens(token)).encode().unwrap())
            .unwrap();
    }
    let device = Arc::new(DeviceCredentials::new(
        store.clone(),
        Arc::new(SyncedKeychain::new(keychain.clone(), id)),
        Some(server.clients()),
    ));
    (keychain, store, device)
}

fn store_id() -> StoreId {
    StoreId(uuid::Uuid::from_u128(7))
}

fn held(store: &StoreKeychain) -> String {
    let bytes = store.storage_credentials().unwrap().unwrap();
    let StorageCredentials::OAuth(tokens) = StorageCredentials::decode(bytes.as_bytes()).unwrap()
    else {
        panic!("OAuth credentials")
    };
    tokens.access_token.as_str().to_owned()
}

fn commit(keychain: &CredentialKeychains, tokens: &OAuthTokens) -> Result<(), SyncError> {
    commit_credentials(keychain, &StorageCredentials::OAuth(tokens.clone()), None).map(drop)
}

#[tokio::test]
async fn device_refresh_commits_before_returning_and_touches_neither_synced_entry() {
    let server = TokenServer::new(false).await;
    let (keychain, store, device) = custody(&server, Some("token"));
    let synced = SyncedKeychain::new(keychain, store_id());
    let code = SecretBytes::new(b"restore code".to_vec());
    synced.set_restore_code(&code).unwrap();
    let replacement = device.replace(&tokens("token")).await.unwrap();
    assert_eq!(replacement.access_token.as_str(), "access-0");
    assert_eq!(
        replacement.refresh_token.unwrap().as_str(),
        "refresh",
        "an unrotated refresh token is retained"
    );
    assert_eq!(held(&store), "access-0");
    assert_eq!(
        synced.restore_code().unwrap().unwrap().as_bytes(),
        code.as_bytes()
    );
    // Another connection rejected the same token: custody already moved on.
    let again = device.replace(&tokens("token")).await.unwrap();
    assert_eq!(again.access_token.as_str(), "access-0");
    assert_eq!(server.requests(), 1);
}

#[tokio::test]
async fn a_sign_in_committed_during_a_refresh_wins_over_it() {
    let server = TokenServer::new(true).await;
    let (_, store, device) = custody(&server, Some("token"));
    let refresh = tokio::spawn({
        let device = device.clone();
        async move { device.replace(&tokens("token")).await }
    });
    server.entered.notified().await;
    device
        .write(|keychain| commit(keychain, &tokens("upgraded")))
        .unwrap();
    server.release.add_permits(1);
    let replacement = refresh.await.unwrap().unwrap();
    assert_eq!(replacement.access_token.as_str(), "upgraded");
    assert_eq!(held(&store), "upgraded");
}

#[tokio::test]
async fn a_refresh_custody_cannot_keep_is_returned_and_changes_nothing() {
    let server = TokenServer::new(false).await;
    let (keychain, store, device) = custody(&server, Some("token"));
    // The two reads before the refresh and the two before the commit succeed;
    // the commit's write is refused.
    keychain.fail_operations_after(&[3]);
    let error = device.replace(&tokens("token")).await.err().unwrap();
    assert_eq!(error.failure(), StorageFailure::File);
    keychain.fail_operations_after(&[]);
    assert_eq!(held(&store), "token");
}

#[tokio::test]
async fn a_session_sign_in_refreshes_in_the_session_until_published_then_in_device_custody() {
    let server = TokenServer::new(false).await;
    let (_, store, device) = custody(&server, None);
    let session = SessionSignIn::new(tokens("token"), Some(server.clients()));
    let refreshed = session.replace(&tokens("token")).await.unwrap();
    assert_eq!(refreshed.access_token.as_str(), "access-0");
    assert!(store.storage_credentials().unwrap().is_none());
    session.publish(&device, commit).unwrap();
    assert_eq!(held(&store), "access-0");
    let replacement = session.replace(&refreshed).await.unwrap();
    assert_eq!(replacement.access_token.as_str(), "access-1");
    assert_eq!(held(&store), "access-1");
    assert_eq!(session.tokens().unwrap().access_token.as_str(), "access-1");
    session.unpublish();
    assert_eq!(session.tokens().unwrap().access_token.as_str(), "access-0");
}

#[tokio::test]
async fn publication_during_a_session_refresh_moves_the_replacement_to_device_custody() {
    let server = TokenServer::new(true).await;
    let (_, store, device) = custody(&server, None);
    let session = SessionSignIn::new(tokens("token"), Some(server.clients()));
    let refresh = tokio::spawn({
        let session = session.clone();
        async move { session.replace(&tokens("token")).await }
    });
    server.entered.notified().await;
    session.publish(&device, commit).unwrap();
    assert_eq!(held(&store), "token");
    server.release.add_permits(1);
    let replacement = refresh.await.unwrap().unwrap();
    // One rejection, one refresh: its replacement is committed in device custody.
    assert_eq!(replacement.access_token.as_str(), "access-0");
    assert_eq!(held(&store), "access-0");
    assert_eq!(server.requests(), 1);
}
