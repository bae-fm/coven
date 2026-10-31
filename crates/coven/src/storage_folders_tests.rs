use super::*;
use crate::authentication::SignIn;
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, Response, Uri},
    Router,
};
use coven_storage::{
    providers::{ProviderConnector, StorageConnector},
    test_utils::MemoryStorage,
};
use serde_json::{json, Value};
use std::collections::VecDeque;

struct Reply {
    method: &'static str,
    path: String,
    body: Option<Value>,
    value: Value,
    status: u16,
    token: usize,
    retry: Option<u64>,
}
impl Reply {
    fn new(method: &'static str, path: &str, body: Option<Value>, value: Value) -> Self {
        Self { method, path: path.into(), body, value, status: 200, token: 0, retry: None }
    }
    fn status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }
    fn token(mut self, token: usize) -> Self {
        self.token = token;
        self
    }
    fn retry(mut self, seconds: u64) -> Self {
        self.retry = Some(seconds);
        self
    }
}
struct Fake {
    url: String,
    replies: Arc<std::sync::Mutex<VecDeque<Reply>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Fake {
    async fn new() -> Self {
        let replies = Arc::new(std::sync::Mutex::new(VecDeque::<Reply>::new()));
        let router = Router::new()
            .fallback(
                |State(replies): State<Arc<std::sync::Mutex<VecDeque<Reply>>>>,
                 method: Method,
                 uri: Uri,
                 headers: HeaderMap,
                 bytes: Bytes| async move {
                    let reply =
                        replies.lock().unwrap().pop_front().expect("unexpected provider request");
                    assert_eq!(method.as_str(), reply.method);
                    assert_eq!(uri.path_and_query().unwrap().as_str(), reply.path);
                    assert_eq!(headers["authorization"], format!("Bearer access-{}", reply.token));
                    assert!(!headers.contains_key("Dropbox-API-Path-Root"));
                    match reply.body {
                        Some(body) => {
                            assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), body)
                        }
                        None => assert!(bytes.is_empty()),
                    }
                    let mut response = Response::builder()
                        .status(reply.status)
                        .header("content-type", "application/json");
                    if let Some(seconds) = reply.retry {
                        response = response.header("Retry-After", seconds.to_string());
                    }
                    response.body(Body::from(reply.value.to_string())).unwrap()
                },
            )
            .with_state(replies.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { url, replies, task }
    }
    fn expect(&self, reply: Reply) {
        self.replies.lock().unwrap().push_back(reply);
    }
    fn consumed(&self) {
        assert!(self.replies.lock().unwrap().is_empty(), "provider requests were not sent");
    }
}
impl Drop for Fake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Connector {
    native: ProviderConnector,
    storage: Arc<MemoryStorage>,
}
#[async_trait::async_trait]
impl StorageConnector for Connector {
    async fn account_label(
        &self,
        auth: ProviderAuth,
        cooldown: Arc<dyn coven_storage::Cooldown>,
    ) -> Result<String, StorageError> {
        self.native.account_label(auth, cooldown).await
    }
    /// Read the signed-in OneDrive root.
    async fn onedrive_root(
        &self,
        auth: ProviderAuth,
        cooldown: Arc<dyn coven_storage::Cooldown>,
    ) -> Result<StorageFolder, StorageError> {
        self.native.onedrive_root(auth, cooldown).await
    }
    /// Read all immediate OneDrive child folders.
    async fn onedrive_folders(
        &self,
        parent: &StorageConfig,
        auth: ProviderAuth,
        cooldown: Arc<dyn coven_storage::Cooldown>,
    ) -> Result<Vec<StorageFolder>, StorageError> {
        self.native.onedrive_folders(parent, auth, cooldown).await
    }
    /// Create a OneDrive folder without renaming or replacing an occupied name.
    async fn create_onedrive_folder(
        &self,
        parent: &StorageConfig,
        name: &str,
        auth: ProviderAuth,
        cooldown: Arc<dyn coven_storage::Cooldown>,
    ) -> Result<StorageFolder, StorageError> {
        self.native.create_onedrive_folder(parent, name, auth, cooldown).await
    }
    /// Read all shared Dropbox namespaces.
    async fn dropbox_shared_folders(
        &self,
        auth: ProviderAuth,
        cooldown: Arc<dyn coven_storage::Cooldown>,
    ) -> Result<Vec<StorageFolder>, StorageError> {
        self.native.dropbox_shared_folders(auth, cooldown).await
    }
    async fn connect(
        &self,
        config: StorageConfig,
        auth: ProviderAuth,
        publications: Arc<dyn PublicationGate>,
        cooldown: Arc<dyn coven_storage::Cooldown>,
        transfers: Arc<coven_storage::TransferBudget>,
        setup: Option<Arc<coven_storage::SetupWaits>>,
    ) -> Result<Arc<StorageConnection>, StorageError> {
        self.storage.connect(config, auth, publications, cooldown, transfers, setup).await
    }
}
struct Fixture {
    _root: tempfile::TempDir,
    app: TestCoven,
    handle: CovenHandle,
    sign_in: SignIn,
    fake: Fake,
}
impl Fixture {
    async fn new(location: StorageConfig) -> Self {
        let root = tempfile::tempdir().unwrap();
        let app = TestCoven::new();
        let clock = Arc::new(FixedClock::new(std::time::UNIX_EPOCH));
        let sign_in = SignIn::new(clock.clone()).await;
        let fake = Fake::new().await;
        let storage = Arc::new(
            MemoryStorage::builder().location(location).clock(clock.clone()).build().unwrap(),
        );
        let native = ProviderConnector::new(clock.clone(), Arc::new(SequentialIds::new()))
            .with_folder_api(fake.url.clone());
        let handle = sign_in
            .configure(
                app.builder(StoreLayout::new(root.path().into()))
                    .clock(clock)
                    .synced_tables(vec![])
                    .migrations(vec![])
                    .storage_connector(Arc::new(Connector { native, storage })),
            )
            .create_store("Household")
            .await
            .unwrap();
        Self { _root: root, app, handle, sign_in, fake }
    }
    fn no_credentials(&self) {
        assert!(self
            .app
            .store_keychain(self.handle.store_id())
            .storage_credentials()
            .unwrap()
            .is_none());
    }
}
fn drive(folder: &str) -> StorageConfig {
    StorageConfig::OneDrive {
        account_label: None,
        drive_id: "drive".into(),
        folder_id: folder.into(),
    }
}
fn folder(id: &str, name: &str) -> Value {
    json!({"id":id,"name":name,"folder":{},"parentReference":{"driveId":"drive"}})
}
fn dropbox(namespace: &str) -> StorageConfig {
    StorageConfig::Dropbox {
        account_label: None,
        location: DropboxLocation::SharedFolder { namespace_id: namespace.into() },
    }
}
fn failure(error: StorageSetupError) -> StorageError {
    match error {
        StorageSetupError::Storage(error) => error,
        other => panic!("storage failure expected: {other:?}"),
    }
}

#[tokio::test]
async fn onedrive_root_pages_and_creation_return_setup_locations_without_publishing_sign_in() {
    let f = Fixture::new(drive("household")).await;
    assert!(matches!(
        f.handle.onedrive_root().await,
        Err(StorageSetupError::OAuth(OAuthError::Reauthorize(CloudProvider::OneDrive)))
    ));
    f.handle.authenticate(OAuthAccess::OneDrive).await.unwrap();
    f.fake.expect(Reply::new("GET", "/me/drive/root", None, folder("root", "Files")));
    let root = f.handle.onedrive_root().await.unwrap();
    assert_eq!(root, StorageFolder { name: "Files".into(), storage: drive("root") });
    f.fake.expect(Reply::new(
        "GET",
        "/drives/drive/items/root/children",
        None,
        json!({
            "value":[folder("garden", "Garden"), {"id":"photo","name":"Photo","file":{}}],
            "@odata.nextLink":format!("{}/next?page=2", f.fake.url)
        }),
    ));
    f.fake.expect(Reply::new(
        "GET",
        "/next?page=2",
        None,
        json!({"value":[folder("gifts", "Gifts")]}),
    ));
    let found = f.handle.onedrive_folders(&root.storage).await.unwrap();
    assert_eq!(
        found,
        vec![
            StorageFolder { name: "Garden".into(), storage: drive("garden") },
            StorageFolder { name: "Gifts".into(), storage: drive("gifts") }
        ]
    );
    f.fake.expect(Reply::new(
        "POST",
        "/drives/drive/items/root/children",
        Some(json!({
            "name":"Household", "folder":{}, "@microsoft.graph.conflictBehavior":"fail"
        })),
        folder("household", "Household"),
    ));
    let created = f.handle.create_onedrive_folder(&root.storage, "Household").await.unwrap();
    assert_eq!(created.storage, drive("household"));
    assert_eq!(created.name, "Household");
    f.no_credentials();
    assert_eq!(f.handle.storage_config().await.unwrap(), None);
    f.fake.expect(account_reply(CloudProvider::OneDrive, "ana@example.com", 0));
    let setup =
        f.handle.setup_oauth_storage(created.storage.clone(), "Ana's laptop").await.unwrap();
    assert_eq!(setup.connection.storage, labelled(created.storage, "ana@example.com"));
    f.fake.consumed();
    f.handle.close().await.unwrap();
    assert!(matches!(
        f.handle.onedrive_root().await,
        Err(StorageSetupError::SecureStorage(KeyError::StoreClosed))
    ));
}

#[tokio::test]
async fn dropbox_namespaces_follow_cursors_and_refresh_in_the_same_sign_in_used_by_setup() {
    let f = Fixture::new(dropbox("123")).await;
    f.handle.authenticate(OAuthAccess::Dropbox(DropboxAccess::FullDropbox)).await.unwrap();
    f.fake.expect(
        Reply::new(
            "POST",
            "/sharing/list_folders",
            Some(json!({})),
            json!({"error": {".tag":"expired_access_token"}}),
        )
        .status(401),
    );
    f.fake.expect(
        Reply::new(
            "POST",
            "/sharing/list_folders",
            Some(json!({})),
            json!({
                "entries":[{"name":"Household","shared_folder_id":"123"}], "cursor":"next"
            }),
        )
        .token(1),
    );
    f.fake.expect(
        Reply::new(
            "POST",
            "/sharing/list_folders/continue",
            Some(json!({"cursor":"next"})),
            json!({
                "entries":[{"name":"Garden","shared_folder_id":"456"}]
            }),
        )
        .token(1),
    );
    let folders = f.handle.dropbox_shared_folders().await.unwrap();
    assert_eq!(
        folders,
        vec![
            StorageFolder { name: "Household".into(), storage: dropbox("123") },
            StorageFolder { name: "Garden".into(), storage: dropbox("456") }
        ]
    );
    assert_eq!(f.sign_in.request_count(), 2);
    f.no_credentials();
    assert_eq!(f.handle.storage_config().await.unwrap(), None);
    f.fake.expect(account_reply(CloudProvider::Dropbox, "ana@example.com", 1));
    let setup =
        f.handle.setup_oauth_storage(folders[0].storage.clone(), "Ana's laptop").await.unwrap();
    assert_eq!(setup.connection.storage, labelled(dropbox("123"), "ana@example.com"));
    let bytes = f.app.store_keychain(f.handle.store_id()).storage_credentials().unwrap().unwrap();
    let StorageCredentials::OAuth(tokens) = StorageCredentials::decode(bytes.as_bytes()).unwrap()
    else {
        panic!("OAuth credentials");
    };
    assert_eq!(tokens.access_token.as_str(), "access-1");
    f.fake.consumed();
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn folder_selection_refuses_missing_wrong_and_app_folder_sign_in_before_requests() {
    let f = Fixture::new(dropbox("123")).await;
    assert!(matches!(
        f.handle.dropbox_shared_folders().await,
        Err(StorageSetupError::OAuth(OAuthError::Reauthorize(CloudProvider::Dropbox)))
    ));
    f.handle.authenticate(OAuthAccess::OneDrive).await.unwrap();
    assert!(matches!(
        f.handle.dropbox_shared_folders().await,
        Err(StorageSetupError::OAuth(OAuthError::Reauthorize(CloudProvider::Dropbox)))
    ));
    assert_eq!(
        failure(f.handle.onedrive_folders(&dropbox("123")).await.unwrap_err()).failure(),
        StorageFailure::InvalidConfiguration
    );
    f.handle.authenticate(OAuthAccess::Dropbox(DropboxAccess::AppFolder)).await.unwrap();
    assert_eq!(
        failure(f.handle.dropbox_shared_folders().await.unwrap_err()).failure(),
        StorageFailure::DropboxAppFolderNotShareable
    );
    f.no_credentials();
    f.fake.consumed();
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn creation_conflicts_keep_the_native_cause_and_never_rename_or_replace() {
    let f = Fixture::new(drive("household")).await;
    f.handle.authenticate(OAuthAccess::OneDrive).await.unwrap();
    f.fake.expect(
        Reply::new(
            "POST",
            "/drives/drive/items/root/children",
            Some(json!({
                "name":"Household", "folder":{}, "@microsoft.graph.conflictBehavior":"fail"
            })),
            json!({"error":{"code":"nameAlreadyExists"}}),
        )
        .status(409),
    );
    let error =
        failure(f.handle.create_onedrive_folder(&drive("root"), "Household").await.unwrap_err());
    assert_eq!(error.failure(), StorageFailure::AlreadyExists);
    let StorageError::Provider { source, .. } = error else {
        panic!("native response");
    };
    assert_eq!(source.downcast_ref::<ProviderResponse>().unwrap().status(), 409);
    f.fake.consumed();
    f.no_credentials();
    assert_eq!(f.handle.storage_config().await.unwrap(), None);
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn incomplete_or_cyclic_listings_fail_without_returning_a_partial_choice() {
    let f = Fixture::new(drive("household")).await;
    f.handle.authenticate(OAuthAccess::OneDrive).await.unwrap();
    let path = "/drives/drive/items/root/children";
    for link in [format!("{}{path}", f.fake.url), "http://127.0.0.1:1/steal-token".into()] {
        f.fake.expect(Reply::new(
            "GET",
            path,
            None,
            json!({"value":[folder("one","One")],"@odata.nextLink":link}),
        ));
        assert_eq!(
            failure(f.handle.onedrive_folders(&drive("root")).await.unwrap_err()).failure(),
            StorageFailure::Protocol
        );
    }
    for invalid in [
        json!({"id":" ","name":"Invalid id","folder":{}}),
        json!({"name":"Missing id","folder":{}}),
        json!({"id":"both","name":"Both","folder":{},"file":{}}),
        json!({"id":"bad","name":"Bad","folder":false}),
    ] {
        f.fake.expect(Reply::new("GET", path, None, json!({"value":[invalid]})));
        assert_eq!(
            failure(f.handle.onedrive_folders(&drive("root")).await.unwrap_err()).failure(),
            StorageFailure::Protocol
        );
    }
    f.handle.authenticate(OAuthAccess::Dropbox(DropboxAccess::FullDropbox)).await.unwrap();
    let first = Reply::new(
        "POST",
        "/sharing/list_folders",
        Some(json!({})),
        json!({"entries":[{"name":"One","shared_folder_id":"1"}],"cursor":"repeat"}),
    )
    .token(1);
    f.fake.expect(first);
    f.fake.expect(
        Reply::new(
            "POST",
            "/sharing/list_folders/continue",
            Some(json!({"cursor":"repeat"})),
            json!({"entries":[],"cursor":"repeat"}),
        )
        .token(1),
    );
    assert_eq!(
        failure(f.handle.dropbox_shared_folders().await.unwrap_err()).failure(),
        StorageFailure::Protocol
    );
    for invalid in
        [json!({"entries":[{"name":"Missing namespace"}]}), json!({"entries":[],"cursor":false})]
    {
        f.fake
            .expect(Reply::new("POST", "/sharing/list_folders", Some(json!({})), invalid).token(1));
        assert_eq!(
            failure(f.handle.dropbox_shared_folders().await.unwrap_err()).failure(),
            StorageFailure::Protocol
        );
    }
    f.fake.consumed();
    f.no_credentials();
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn remote_onedrive_folders_use_the_target_drive_and_item_ids() {
    let f = Fixture::new(StorageConfig::OneDrive {
        account_label: None,
        drive_id: "shared-drive".into(),
        folder_id: "shared-item".into(),
    })
    .await;
    f.handle.authenticate(OAuthAccess::OneDrive).await.unwrap();
    f.fake.expect(Reply::new(
        "GET",
        "/drives/drive/items/root/children",
        None,
        json!({"value":[{
            "id":"local-shortcut", "name":"Household", "remoteItem":{
                "id":"shared-item", "folder":{}, "parentReference":{"driveId":"shared-drive"}
            }
        }]}),
    ));
    let folders = f.handle.onedrive_folders(&drive("root")).await.unwrap();
    assert_eq!(
        folders,
        vec![StorageFolder {
            name: "Household".into(),
            storage: StorageConfig::OneDrive {
                account_label: None,
                drive_id: "shared-drive".into(),
                folder_id: "shared-item".into()
            }
        }]
    );
    f.fake.expect(account_reply(CloudProvider::OneDrive, "ana@example.com", 0));
    f.handle.setup_oauth_storage(folders[0].storage.clone(), "Ana's laptop").await.unwrap();
    f.fake.consumed();
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn a_later_provider_page_failure_returns_the_error_instead_of_partial_folders() {
    let f = Fixture::new(drive("household")).await;
    f.handle.authenticate(OAuthAccess::OneDrive).await.unwrap();
    f.fake.expect(Reply::new(
        "GET",
        "/drives/drive/items/root/children",
        None,
        json!({"value":[folder("one","One")], "@odata.nextLink":format!("{}/next",f.fake.url)}),
    ));
    f.fake.expect(
        Reply::new("GET", "/next", None, json!({"error":{"code":"accessDenied"}})).status(403),
    );
    assert_eq!(
        failure(f.handle.onedrive_folders(&drive("root")).await.unwrap_err()).failure(),
        StorageFailure::PermissionDenied
    );
    f.fake.consumed();
    f.handle.close().await.unwrap();
}

#[tokio::test]
async fn provider_cooldown_applies_to_folder_selection_before_another_request() {
    let f = Fixture::new(drive("household")).await;
    f.handle.authenticate(OAuthAccess::OneDrive).await.unwrap();
    f.fake.expect(
        Reply::new("GET", "/me/drive/root", None, json!({"error":{"code":"activityLimitReached"}}))
            .status(429)
            .retry(60),
    );
    assert_eq!(
        failure(f.handle.onedrive_root().await.unwrap_err()).failure(),
        StorageFailure::RateLimited
    );
    let error = failure(f.handle.onedrive_root().await.unwrap_err());
    assert!(std::error::Error::source(&error).unwrap().is::<coven_storage::CoolingDown>());
    f.fake.consumed();
    f.handle.close().await.unwrap();
}

fn labelled(mut location: StorageConfig, label: &str) -> StorageConfig {
    match &mut location {
        StorageConfig::Dropbox { account_label, .. }
        | StorageConfig::OneDrive { account_label, .. } => *account_label = Some(label.into()),
        _ => panic!("OAuth location"),
    }
    location
}

fn account_reply(provider: CloudProvider, label: &str, token: usize) -> Reply {
    match provider {
        CloudProvider::Dropbox => Reply::new(
            "POST",
            "/users/get_current_account",
            Some(Value::Null),
            json!({"email":label}),
        ),
        CloudProvider::OneDrive => Reply::new(
            "GET",
            "/me?%24select=mail%2CuserPrincipalName",
            None,
            json!({"mail":label,"userPrincipalName":"principal@example.com"}),
        ),
        _ => panic!("OAuth provider"),
    }
    .token(token)
}

#[tokio::test]
async fn setup_records_the_account_and_signing_in_again_refreshes_only_the_label() {
    for location in [
        dropbox("123"),
        StorageConfig::Dropbox { location: DropboxLocation::AppFolder, account_label: None },
        drive("household"),
    ] {
        let f = Fixture::new(location.clone()).await;
        let access = location.oauth_access().unwrap();
        let mut previous_code = None;
        for (token, label) in ["ana@example.com", "ana@work.example"].into_iter().enumerate() {
            f.handle.authenticate(access).await.unwrap();
            f.fake.expect(account_reply(location.provider(), label, token));
            let setup = f
                .handle
                .setup_oauth_storage(
                    labelled(location.clone(), "untrusted@example.com"),
                    "Ana's laptop",
                )
                .await
                .unwrap();
            let expected = labelled(location.clone(), label);
            assert_eq!(setup.connection.storage, expected);
            assert_eq!(f.handle.storage_config().await.unwrap(), Some(expected.clone()));
            let code = f.handle.restore_code().await.unwrap();
            let decoded = coven_sync::read_restore_code(&code).unwrap();
            assert_eq!(
                coven_storage::RestoreStorage::decode(decoded.storage.as_bytes()).unwrap().location,
                expected
            );
            assert_eq!(
                crate::tests::readable(f.app.saved_restore_codes().await.unwrap())[0].location,
                expected
            );
            // Display metadata cannot replace the account used for membership.
            let state = f.handle.store_state().await;
            assert_eq!(
                state.members[&decoded.member].access,
                coven_format::MemberAccess::ProviderAccount("owner@example.test".into())
            );
            if let Some(previous) = previous_code {
                assert_ne!(code, previous);
            }
            previous_code = Some(code);
        }
        // A different folder remains a location change, regardless of its label.
        let elsewhere = match &location {
            StorageConfig::Dropbox { .. } => dropbox("other"),
            StorageConfig::OneDrive { .. } => drive("other"),
            _ => unreachable!(),
        };
        assert!(matches!(
            f.handle.setup_oauth_storage(elsewhere, "Laptop").await,
            Err(StorageSetupError::LocationChangeUnsupported)
        ));
        f.fake.consumed();
        f.handle.close().await.unwrap();
        let directory = StoreLayout::new(f._root.path().into()).store_dir(&f.handle.store_id());
        assert_eq!(
            Coven::storage_config(&directory).await.unwrap(),
            Some(labelled(location, "ana@work.example"))
        );
    }
}

#[tokio::test]
async fn account_lookup_failure_retains_committed_location_credentials_and_code() {
    for location in [dropbox("123"), drive("household")] {
        let f = Fixture::new(location.clone()).await;
        let access = location.oauth_access().unwrap();
        f.handle.authenticate(access).await.unwrap();
        f.fake.expect(account_reply(location.provider(), "ana@example.com", 0));
        f.handle.setup_oauth_storage(location.clone(), "Laptop").await.unwrap();
        let code = f.handle.restore_code().await.unwrap();
        let keychain = f.app.store_keychain(f.handle.store_id());
        let credentials = keychain.storage_credentials().unwrap().unwrap();
        let saved = crate::tests::readable(f.app.saved_restore_codes().await.unwrap());
        f.handle.authenticate(access).await.unwrap();
        f.fake.expect(account_reply(location.provider(), "ignored", 1).status(503));
        assert_eq!(
            failure(f.handle.setup_oauth_storage(location.clone(), "Laptop").await.unwrap_err())
                .failure(),
            StorageFailure::Network
        );
        assert_eq!(f.handle.restore_code().await.unwrap(), code);
        assert_eq!(crate::tests::readable(f.app.saved_restore_codes().await.unwrap()), saved);
        assert_eq!(
            keychain.storage_credentials().unwrap().unwrap().as_bytes(),
            credentials.as_bytes()
        );
        f.fake.expect(account_reply(location.provider(), "ana@work.example", 1));
        f.handle.setup_oauth_storage(location, "Laptop").await.unwrap();
        f.fake.consumed();
        f.handle.close().await.unwrap();
    }
}

#[tokio::test]
async fn onedrive_uses_the_principal_name_when_mail_is_absent() {
    for value in [
        json!({"mail":null,"userPrincipalName":"ANA@example.com"}),
        json!({"userPrincipalName":"ANA@example.com"}),
    ] {
        let f = Fixture::new(drive("household")).await;
        f.handle.authenticate(OAuthAccess::OneDrive).await.unwrap();
        let mut reply = account_reply(CloudProvider::OneDrive, "ignored", 0);
        reply.value = value;
        f.fake.expect(reply);
        let setup = f.handle.setup_oauth_storage(drive("household"), "Laptop").await.unwrap();
        assert_eq!(setup.connection.storage, labelled(drive("household"), "ana@example.com"));
        f.fake.consumed();
        f.handle.close().await.unwrap();
    }
}

#[tokio::test]
async fn malformed_accounts_fail_before_setup_publishes_anything() {
    for (location, value) in [
        (dropbox("123"), json!({})),
        (dropbox("123"), json!({"email":""})),
        (drive("household"), json!({"mail":42,"userPrincipalName":"ana@example.com"})),
        (drive("household"), json!({"mail":null})),
    ] {
        let f = Fixture::new(location.clone()).await;
        f.handle.authenticate(location.oauth_access().unwrap()).await.unwrap();
        let mut reply = account_reply(location.provider(), "ignored", 0);
        reply.value = value;
        f.fake.expect(reply);
        assert_eq!(
            failure(f.handle.setup_oauth_storage(location, "Laptop").await.unwrap_err()).failure(),
            StorageFailure::Protocol
        );
        assert_eq!(f.handle.storage_config().await.unwrap(), None);
        assert!(crate::tests::readable(f.app.saved_restore_codes().await.unwrap()).is_empty());
        f.no_credentials();
        f.fake.consumed();
        f.handle.close().await.unwrap();
    }
}

#[tokio::test]
async fn refreshing_during_account_lookup_commits_the_replacement_tokens() {
    for location in [dropbox("123"), drive("household")] {
        for reconnect in [false, true] {
            let f = Fixture::new(location.clone()).await;
            f.handle.authenticate(location.oauth_access().unwrap()).await.unwrap();
            if reconnect {
                f.fake.expect(account_reply(location.provider(), "ana@example.com", 0));
                f.handle.setup_oauth_storage(location.clone(), "Laptop").await.unwrap();
            }
            f.fake.expect(account_reply(location.provider(), "ignored", 0).status(401));
            f.fake.expect(account_reply(location.provider(), "ana@work.example", 1));
            let setup = f.handle.setup_oauth_storage(location.clone(), "Laptop").await.unwrap();
            assert_eq!(setup.connection.storage, labelled(location.clone(), "ana@work.example"));
            let bytes =
                f.app.store_keychain(f.handle.store_id()).storage_credentials().unwrap().unwrap();
            let coven_storage::StorageCredentials::OAuth(tokens) =
                coven_storage::StorageCredentials::decode(bytes.as_bytes()).unwrap()
            else {
                panic!("OAuth credentials")
            };
            assert_eq!(tokens.access_token.as_str(), "access-1");
            assert_eq!(f.sign_in.request_count(), 2);
            f.fake.consumed();
            f.handle.close().await.unwrap();
        }
    }
}
