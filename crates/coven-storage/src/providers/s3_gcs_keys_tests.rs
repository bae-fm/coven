//! GCS minting against an in-process JSON API and XML API. The XML side lets
//! an HMAC key reach only the managed folders bound to its service account.
use super::*;
use crate::providers::tests::{query, response, TestServer};
use crate::{AccessKeyAdmin, AdminCredential, S3Profile, S3ProfileName};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method as HttpMethod, Response, Uri},
    Router,
};
use coven_foundation::{clock::Clock, id_source::UuidIds};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use url::Url;

#[derive(Default)]
struct Remote {
    uniform: bool,
    accounts: Vec<String>,
    /// Service account email to its description.
    descriptions: BTreeMap<String, String>,
    folders: Vec<String>,
    /// Folder name to (etag, role bindings).
    policies: BTreeMap<String, (u32, Vec<String>)>,
    /// Access id to (service account, state, secret).
    hmac: BTreeMap<String, (String, String, String)>,
    objects: BTreeMap<String, Vec<u8>>,
    next: u32,
    /// The next HMAC creation answers with a server error after creating.
    lose_key_reply: bool,
    /// Bindings refused as naming an account Google does not yet see.
    unseen_binds: u32,
    /// HMAC creations refused as naming an account Google does not yet see.
    unseen_key_creates: u32,
    /// The pauses the minter's clock took; none of them waits.
    pauses: Vec<Duration>,
    bearer_seen: bool,
}

/// A clock whose pauses return at once, recorded in the remote's state.
struct Pauses(Arc<Mutex<Remote>>);

impl Clock for Pauses {
    fn now(&self) -> std::time::SystemTime {
        std::time::SystemTime::now()
    }
    fn elapsed(&self) -> Duration {
        self.0.lock().unwrap().pauses.iter().sum()
    }
    fn sleep(
        &self,
        duration: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.0.lock().unwrap().pauses.push(duration);
        Box::pin(std::future::ready(()))
    }
}

fn json(value: serde_json::Value) -> Response<Body> {
    response(200, value.to_string())
}

fn decode(segment: &str) -> String {
    url::form_urlencoded::parse(format!("x={segment}").as_bytes())
        .next()
        .unwrap()
        .1
        .into_owned()
}

fn api(
    state: &mut Remote,
    method: HttpMethod,
    uri: &Uri,
    headers: &HeaderMap,
    body: &Bytes,
) -> Response<Body> {
    state.bearer_seen |= headers["authorization"] == "Bearer admin-token";
    // Google refuses a POST without a length.
    if method == HttpMethod::POST && !headers.contains_key("content-length") {
        return response(411, "{}");
    }
    let path = uri.path();
    let q = query(uri);
    let body: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
    if path == "/storage/v1/b/bucket" {
        return json(serde_json::json!({
            "projectNumber": "123",
            "iamConfiguration": {"uniformBucketLevelAccess": {"enabled": state.uniform}},
        }));
    }
    if path == "/projects/v3/projects/123" {
        return json(serde_json::json!({"projectId": "household"}));
    }
    if path == "/iam/v1/projects/household/serviceAccounts" && method == HttpMethod::GET {
        // One account per page, so a listing must follow every page.
        let index: usize = q.get("pageToken").map_or(0, |token| token.parse().unwrap());
        let Some(email) = state.accounts.get(index) else {
            return json(serde_json::json!({}));
        };
        let mut page = serde_json::json!({"accounts": [{
            "email": email,
            "description": state.descriptions.get(email),
        }]});
        if index + 1 < state.accounts.len() {
            page["nextPageToken"] = serde_json::json!((index + 1).to_string());
        }
        return json(page);
    }
    if path == "/iam/v1/projects/household/serviceAccounts" {
        let email = format!(
            "{}@household.iam.gserviceaccount.com",
            body["accountId"].as_str().unwrap()
        );
        if state.accounts.contains(&email) {
            return response(409, "{}");
        }
        state.accounts.push(email.clone());
        if let Some(description) = body["serviceAccount"]["description"].as_str() {
            state
                .descriptions
                .insert(email.clone(), description.to_owned());
        }
        return json(serde_json::json!({"email": email}));
    }
    if let Some(email) = path.strip_prefix("/iam/v1/projects/household/serviceAccounts/") {
        let email = decode(email);
        if method == HttpMethod::GET {
            return if state.accounts.contains(&email) {
                json(serde_json::json!({"email": email}))
            } else {
                response(404, "{}")
            };
        }
        let before = state.accounts.len();
        state.accounts.retain(|account| *account != email);
        return if state.accounts.len() == before {
            response(404, "{}")
        } else {
            json(serde_json::json!({}))
        };
    }
    if path == "/storage/v1/b/bucket/managedFolders" {
        let name = body["name"].as_str().unwrap().to_owned();
        if state.folders.contains(&name) {
            return response(409, "{}");
        }
        state.folders.push(name.clone());
        state.policies.insert(name, (1, Vec::new()));
        return json(serde_json::json!({}));
    }
    if let Some(rest) = path.strip_prefix("/storage/v1/b/bucket/managedFolders/") {
        let folder = decode(rest.strip_suffix("/iam").unwrap());
        let Some((etag, members)) = state.policies.get_mut(&folder) else {
            return response(404, "{}");
        };
        if method == HttpMethod::GET {
            let bindings: Vec<_> = if members.is_empty() {
                vec![
                    serde_json::json!({"role": "roles/storage.legacyBucketReader", "members": ["projectViewer:household"]}),
                ]
            } else {
                vec![
                    serde_json::json!({"role": "roles/storage.legacyBucketReader", "members": ["projectViewer:household"]}),
                    serde_json::json!({"role": "roles/storage.objectUser", "members": members.clone()}),
                ]
            };
            return json(serde_json::json!({"etag": etag.to_string(), "bindings": bindings}));
        }
        if body["etag"] != serde_json::json!(etag.to_string()) {
            return response(412, "{}");
        }
        if state.unseen_binds > 0 {
            state.unseen_binds -= 1;
            let message =
                "Service account coven-1@household.iam.gserviceaccount.com does not exist.";
            return response(
                400,
                serde_json::json!({"error": {"code": 400, "message": message, "errors": [{"message": message, "reason": "invalid"}]}}).to_string(),
            );
        }
        let (etag, members) = state.policies.get_mut(&folder).unwrap();
        *etag += 1;
        *members = body["bindings"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|binding| binding["role"] == "roles/storage.objectUser")
            .flat_map(|binding| binding["members"].as_array().unwrap().clone())
            .map(|member| member.as_str().unwrap().to_owned())
            .collect();
        return json(body);
    }
    if path == "/storage/v1/projects/household/hmacKeys" {
        let email = q["serviceAccountEmail"].clone();
        // Google answers for an account it does not know with 404.
        if method == HttpMethod::GET && !state.accounts.contains(&email) {
            return response(404, "{}");
        }
        if method == HttpMethod::GET {
            let items: Vec<_> = state
                .hmac
                .iter()
                .filter(|(_, (owner, ..))| *owner == email)
                .map(|(id, (_, status, _))| serde_json::json!({"accessId": id, "state": status}))
                .collect();
            return json(serde_json::json!({"items": items}));
        }
        if !state.accounts.contains(&email) {
            return response(404, "{}");
        }
        if state.unseen_key_creates > 0 {
            state.unseen_key_creates -= 1;
            return response(404, "{}");
        }
        state.next += 1;
        let id = format!("GOOG1{}", state.next);
        let secret = format!("hmac-secret-{}", state.next);
        state
            .hmac
            .insert(id.clone(), (email, "ACTIVE".into(), secret.clone()));
        if std::mem::take(&mut state.lose_key_reply) {
            return response(503, "{}");
        }
        return json(serde_json::json!({"metadata": {"accessId": id}, "secret": secret}));
    }
    if let Some(id) = path.strip_prefix("/storage/v1/projects/household/hmacKeys/") {
        let Some(key) = state.hmac.get_mut(id) else {
            return response(404, "{}");
        };
        // A deleted key stays readable and refuses changes, as Google's does.
        if method == HttpMethod::GET {
            return json(
                serde_json::json!({"accessId": id, "state": key.1, "serviceAccountEmail": key.0}),
            );
        }
        if key.1 == "DELETED" {
            return response(400, "{}");
        }
        if method == HttpMethod::PUT {
            key.1 = body["state"].as_str().unwrap().to_owned();
            return json(serde_json::json!({}));
        }
        if key.1 != "INACTIVE" {
            return response(400, "{}");
        }
        key.1 = "DELETED".into();
        return json(serde_json::json!({}));
    }
    response(404, "{}")
}

fn xml_error(status: u16, code: &str) -> Response<Body> {
    response(status, format!("<Error><Code>{code}</Code></Error>"))
}

fn xml(
    state: &mut Remote,
    method: HttpMethod,
    uri: &Uri,
    headers: &HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let sent = headers["authorization"].to_str().unwrap();
    assert!(
        sent.starts_with("GOOG4-HMAC-SHA256 "),
        "GCS keys sign with Google HMAC"
    );
    let id = sent
        .split("Credential=")
        .nth(1)
        .unwrap()
        .split('/')
        .next()
        .unwrap();
    let Some((email, status, _)) = state.hmac.get(id) else {
        return xml_error(403, "InvalidAccessKeyId");
    };
    if status != "ACTIVE" {
        return xml_error(403, "InvalidAccessKeyId");
    }
    let member = format!("serviceAccount:{email}");
    let folders: Vec<String> = state
        .policies
        .iter()
        .filter(|(_, (_, members))| members.contains(&member))
        .map(|(folder, _)| folder.clone())
        .collect();
    let object = uri
        .path()
        .strip_prefix("/bucket")
        .unwrap()
        .trim_start_matches('/')
        .to_owned();
    let q = query(uri);
    let reach = if object.is_empty() {
        q.get("prefix").cloned().unwrap_or_default()
    } else {
        object.clone()
    };
    if !folders.iter().any(|folder| reach.starts_with(folder)) {
        return xml_error(403, "AccessDenied");
    }
    if object.is_empty() && q.contains_key("uploads") {
        return response(200, "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>");
    }
    if object.is_empty() {
        return response(200, "<ListBucketResult><Name>bucket</Name><IsTruncated>false</IsTruncated></ListBucketResult>");
    }
    match method {
        HttpMethod::PUT => {
            if headers
                .get("x-goog-if-generation-match")
                .is_some_and(|v| v == "0")
                && state.objects.contains_key(&object)
            {
                return xml_error(412, "PreconditionFailed");
            }
            state.objects.insert(object, body.to_vec());
            Response::builder()
                .header("x-goog-generation", "1")
                .body(Body::empty())
                .unwrap()
        }
        HttpMethod::GET => match state.objects.get(&object) {
            Some(bytes) => Response::builder()
                .header("x-goog-generation", "1")
                .header("last-modified", "Wed, 14 Oct 2026 00:00:00 GMT")
                .header("content-length", bytes.len())
                .body(Body::from(bytes.clone()))
                .unwrap(),
            None => xml_error(404, "NoSuchKey"),
        },
        HttpMethod::DELETE => {
            state.objects.remove(&object);
            response(204, Vec::new())
        }
        _ => xml_error(400, "InvalidRequest"),
    }
}

async fn serve(
    State(state): State<Arc<Mutex<Remote>>>,
    method: HttpMethod,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let mut state = state.lock().unwrap();
    if uri.path().starts_with("/bucket") {
        xml(&mut state, method, &uri, &headers, body)
    } else {
        api(&mut state, method, &uri, &headers, &body)
    }
}

async fn fake(uniform: bool) -> (Arc<Mutex<Remote>>, TestServer, Arc<dyn AccessKeyAdmin>) {
    let state = Arc::new(Mutex::new(Remote {
        uniform,
        ..Remote::default()
    }));
    let server = TestServer::new(Router::new().fallback(serve).with_state(state.clone())).await;
    let url = Url::parse(&server.url).unwrap();
    let mut endpoints = minting::AdminEndpoints::production();
    endpoints.gcs_storage = url.join("/storage/v1/").unwrap();
    endpoints.gcs_iam = url.join("/iam/v1/").unwrap();
    endpoints.gcs_projects = url.join("/projects/v3/").unwrap();
    let admin = minting::key_admin(
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "auto".into(),
            endpoint: Some(url),
            profile: Some(S3Profile::Named(S3ProfileName::Gcs)),
            prefix: "base".into(),
        },
        AdminCredential::BearerToken(SecretText::new("admin-token".into())),
        endpoints,
        Arc::new(Pauses(state.clone())),
        Arc::new(UuidIds),
    )
    .unwrap();
    (state, server, admin)
}

fn scope() -> MintScope {
    MintScope::new(
        StoreId(uuid::Uuid::from_u128(7)),
        "member-id",
        coven_foundation::id_source::DeviceId(3),
        uuid::Uuid::from_u128(0x99),
    )
}

#[tokio::test]
async fn a_service_account_hmac_key_reaches_only_its_managed_folder() {
    let (state, _server, admin) = fake(true).await;
    let scope = scope();
    let minted = admin.mint(&scope).await.unwrap();
    let folder = format!("base/{}/", scope.store);
    assert_eq!(
        minted.principal,
        S3Principal::GcsServiceAccount {
            project: "household".into(),
            email: format!("{}@household.iam.gserviceaccount.com", scope.name),
            managed_folder: folder.clone(),
        }
    );
    admin.probe(&scope, &minted.credentials).await.unwrap();
    let id = minted.credentials.access_key_id.clone();
    assert!(admin.key_exists(&id, &minted.principal).await.unwrap());
    // Minting deletes no key, even one whose secret was lost.
    state.lock().unwrap().lose_key_reply = true;
    assert!(admin.mint(&scope).await.is_err());
    let again = admin.mint(&scope).await.unwrap();
    assert_eq!(state.lock().unwrap().hmac.len(), 3);
    admin
        .revoke(&again.credentials.access_key_id, &again.principal)
        .await
        .unwrap();
    assert!(!admin
        .key_exists(&again.credentials.access_key_id, &again.principal)
        .await
        .unwrap());
    // Revoking again finds the key deleted and the account gone.
    admin
        .revoke(&again.credentials.access_key_id, &again.principal)
        .await
        .unwrap();
    let state = state.lock().unwrap();
    assert!(state.accounts.is_empty() && state.hmac.values().all(|(_, key, _)| key == "DELETED"));
    assert_eq!(
        state.folders,
        std::slice::from_ref(&folder),
        "the store's folder stays"
    );
    assert!(state.policies[&folder].1.is_empty());
    assert!(state.bearer_seen);
}

#[tokio::test]
async fn buckets_without_uniform_access_are_refused_and_rollback_removes_the_account() {
    let (state, _server, admin) = fake(false).await;
    assert_eq!(
        admin.mint(&scope()).await.unwrap_err().failure(),
        StorageFailure::InvalidConfiguration
    );
    assert!(state.lock().unwrap().accounts.is_empty());
    state.lock().unwrap().uniform = true;
    admin.mint(&scope()).await.unwrap();
    admin.roll_back(&scope()).await.unwrap();
    let state = state.lock().unwrap();
    assert!(state.accounts.is_empty() && state.hmac.values().all(|(_, key, _)| key == "DELETED"));
}

/// A store's listing reads every page of the project's service accounts and
/// returns the HMAC keys of those whose description names this store, with
/// the minting device (§13).
#[tokio::test]
async fn a_store_listing_returns_its_accounts_keys_with_their_minting_device() {
    let (state, _server, admin) = fake(true).await;
    let ours = scope();
    let elsewhere = MintScope::new(
        StoreId(uuid::Uuid::from_u128(8 << 100)),
        "member-id",
        coven_foundation::id_source::DeviceId(3),
        uuid::Uuid::from_u128(0x98 << 100),
    );
    let mine = admin.mint(&ours).await.unwrap();
    admin.mint(&elsewhere).await.unwrap();
    {
        let mut state = state.lock().unwrap();
        // An account coven did not make, holding a key.
        state
            .accounts
            .push("someone@household.iam.gserviceaccount.com".into());
        state.hmac.insert(
            "GOOG1other".into(),
            (
                "someone@household.iam.gserviceaccount.com".into(),
                "ACTIVE".into(),
                "secret".into(),
            ),
        );
    }
    let listed = admin.list_keys(ours.store).await.unwrap();
    assert_eq!(
        listed,
        [crate::ListedKey {
            access_key_id: mine.credentials.access_key_id.clone(),
            principal: mine.principal.clone(),
            device: coven_foundation::id_source::DeviceId(3),
        }]
    );
    assert_eq!(
        state.lock().unwrap().descriptions
            [&format!("{}@household.iam.gserviceaccount.com", ours.name)],
        ours.label()
    );
}

/// Google may refuse a binding or an HMAC key for a service account it made
/// moments ago; minting is repeated each second for up to 60 seconds, then
/// fails, and the repeats make one account and one key.
#[tokio::test]
async fn minting_waits_for_google_to_see_a_new_account() {
    let (state, _server, admin) = fake(true).await;
    state.lock().unwrap().unseen_binds = 3;
    state.lock().unwrap().unseen_key_creates = 2;
    admin.mint(&scope()).await.unwrap();
    {
        let state = state.lock().unwrap();
        assert_eq!(state.pauses, [Duration::from_secs(1); 5]);
        assert_eq!((state.accounts.len(), state.hmac.len()), (1, 1));
    }
    admin.roll_back(&scope()).await.unwrap();
    let (state, _server, admin) = fake(true).await;
    state.lock().unwrap().unseen_binds = 61;
    assert_eq!(
        admin.mint(&scope()).await.unwrap_err().failure(),
        StorageFailure::NotFound
    );
    assert_eq!(state.lock().unwrap().pauses.len(), 60);
}
