//! R2 minting against an in-process Cloudflare API. Minting, revocation and
//! reads use only that API; the location names the account in its host.
use super::*;
use crate::providers::tests::{response, TestServer};
use crate::{AccessKeyAdmin, AdminCredential, S3Profile, S3ProfileName};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method as HttpMethod, Response, Uri},
    Router,
};
use coven_foundation::id_source::StoreId;
use coven_foundation::{clock::SystemClock, id_source::UuidIds};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use url::Url;

#[derive(Default)]
struct Remote {
    /// Token id to (name, resource, permission group, value).
    tokens: BTreeMap<String, (String, String, String, String)>,
    next: u32,
    lose_create_reply: bool,
    bearer_seen: bool,
}

fn result(value: serde_json::Value) -> Response<Body> {
    response(
        200,
        serde_json::json!({"success": true, "errors": [], "result": value}).to_string(),
    )
}

async fn serve(
    State(state): State<Arc<Mutex<Remote>>>,
    method: HttpMethod,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let mut state = state.lock().unwrap();
    state.bearer_seen |= headers["authorization"] == "Bearer admin-token";
    let Some(path) = uri.path().strip_prefix("/client/v4/accounts/acct/tokens") else {
        return response(404, "{}");
    };
    match (method, path) {
        (HttpMethod::GET, "") => result(
            state
                .tokens
                .iter()
                .map(|(id, (name, ..))| serde_json::json!({"id": id, "name": name}))
                .collect(),
        ),
        (HttpMethod::GET, "/permission_groups") => result(serde_json::json!([
            {"id": "pg-read", "name": "Workers R2 Storage Bucket Item Read"},
            {"id": "pg-write", "name": "Workers R2 Storage Bucket Item Write"},
        ])),
        (HttpMethod::POST, "") => {
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let policy = &body["policies"][0];
            let resource = policy["resources"]
                .as_object()
                .unwrap()
                .keys()
                .next()
                .unwrap()
                .clone();
            state.next += 1;
            let id = format!("token-{}", state.next);
            let value = format!("token-value-{}", state.next);
            state.tokens.insert(
                id.clone(),
                (
                    body["name"].as_str().unwrap().to_owned(),
                    resource,
                    policy["permission_groups"][0]["id"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                    value.clone(),
                ),
            );
            if std::mem::take(&mut state.lose_create_reply) {
                return response(502, "{}");
            }
            result(serde_json::json!({"id": id, "value": value}))
        }
        (method, id) => {
            let id = id.trim_start_matches('/');
            if !state.tokens.contains_key(id) {
                return response(404, serde_json::json!({"success": false}).to_string());
            }
            if method == HttpMethod::DELETE {
                state.tokens.remove(id);
                return result(serde_json::json!({"id": id}));
            }
            result(serde_json::json!({"id": id, "status": "active"}))
        }
    }
}

async fn fake() -> (Arc<Mutex<Remote>>, TestServer, Arc<dyn AccessKeyAdmin>) {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(serve).with_state(state.clone())).await;
    let mut endpoints = minting::AdminEndpoints::production();
    endpoints.cloudflare = Url::parse(&server.url)
        .unwrap()
        .join("/client/v4/")
        .unwrap();
    let admin = minting::key_admin(
        StorageConfig::S3 {
            bucket: "household".into(),
            region: "auto".into(),
            endpoint: Some(Url::parse("https://acct.eu.r2.cloudflarestorage.com").unwrap()),
            profile: Some(S3Profile::Named(S3ProfileName::R2)),
            prefix: String::new(),
        },
        AdminCredential::BearerToken(SecretText::new("admin-token".into())),
        endpoints,
        Arc::new(SystemClock),
        Arc::new(UuidIds),
    )
    .unwrap();
    (state, server, admin)
}

#[tokio::test]
async fn a_token_reaches_the_stores_own_bucket_and_its_hash_is_the_secret() {
    let (state, _server, admin) = fake().await;
    let scope = MintScope::new(
        StoreId(uuid::Uuid::from_u128(3)),
        "member-id",
        coven_foundation::id_source::DeviceId(3),
        uuid::Uuid::from_u128(0x77),
    );
    state.lock().unwrap().lose_create_reply = true;
    assert!(admin.mint(&scope).await.is_err());
    let minted = admin.mint(&scope).await.unwrap();
    {
        let state = state.lock().unwrap();
        assert_eq!(
            state.tokens.len(),
            1,
            "the token whose value was lost is gone"
        );
        let (name, resource, group, value) = &state.tokens[&minted.credentials.access_key_id];
        assert_eq!(*name, scope.label());
        assert_eq!(resource, "com.cloudflare.edge.r2.bucket.acct_eu_household");
        assert_eq!(group, "pg-write");
        assert_eq!(
            minted.credentials.secret_access_key.as_str(),
            hex::encode(coven_crypto::s3_request_hash(value.as_bytes()))
        );
        assert!(state.bearer_seen);
    }
    assert_eq!(
        minted.principal,
        S3Principal::R2Token {
            account: "acct".into()
        }
    );
    let id = minted.credentials.access_key_id.clone();
    assert!(admin.key_exists(&id, &minted.principal).await.unwrap());
    admin.revoke(&id, &minted.principal).await.unwrap();
    admin.revoke(&id, &minted.principal).await.unwrap();
    assert!(!admin.key_exists(&id, &minted.principal).await.unwrap());
    admin.mint(&scope).await.unwrap();
    admin.roll_back(&scope).await.unwrap();
    assert!(state.lock().unwrap().tokens.is_empty());
}

#[test]
fn the_endpoint_names_the_account_and_jurisdiction() {
    let config = |host: &str| StorageConfig::S3 {
        bucket: "b".into(),
        region: "auto".into(),
        endpoint: Some(Url::parse(&format!("https://{host}")).unwrap()),
        profile: None,
        prefix: String::new(),
    };
    assert_eq!(
        account(&config("acct.r2.cloudflarestorage.com")).unwrap(),
        ("acct".into(), "default".into())
    );
    assert_eq!(
        account(&config("acct.eu.r2.cloudflarestorage.com")).unwrap(),
        ("acct".into(), "eu".into())
    );
    assert!(account(&config("example.com")).is_err());
}

/// An R2-like bucket's objects, and whether the token may list buckets.
type Bucket = Arc<Mutex<(BTreeMap<String, Vec<u8>>, bool)>>;

/// An R2-like bucket: the token reaches every object in it; listing the
/// account's buckets is refused unless allowed.
async fn bucket(
    State(state): State<Bucket>,
    method: HttpMethod,
    uri: Uri,
    body: Bytes,
) -> Response<Body> {
    let mut state = state.lock().unwrap();
    if uri.path() == "/" {
        return if state.1 {
            response(
                200,
                "<ListAllMyBucketsResult><Buckets></Buckets></ListAllMyBucketsResult>",
            )
        } else {
            response(403, "<Error><Code>AccessDenied</Code></Error>")
        };
    }
    let key = uri
        .path()
        .trim_start_matches("/household")
        .trim_start_matches('/')
        .to_owned();
    if key.is_empty() {
        return response(200, "<ListBucketResult><Name>household</Name><IsTruncated>false</IsTruncated></ListBucketResult>");
    }
    match method {
        HttpMethod::PUT if state.0.contains_key(&key) => {
            response(412, "<Error><Code>PreconditionFailed</Code></Error>")
        }
        HttpMethod::PUT => {
            state.0.insert(key, body.to_vec());
            Response::builder()
                .header("etag", "\"e\"")
                .body(Body::empty())
                .unwrap()
        }
        HttpMethod::GET => Response::builder()
            .header("etag", "\"e\"")
            .header("last-modified", "Wed, 14 Oct 2026 00:00:00 GMT")
            .body(Body::from(state.0[&key].clone()))
            .unwrap(),
        _ => {
            state.0.remove(&key);
            response(204, Vec::new())
        }
    }
}

#[tokio::test]
async fn a_bucket_scoped_key_must_not_list_other_buckets() {
    for lists_buckets in [false, true] {
        let state: Bucket = Arc::new(Mutex::new((BTreeMap::new(), lists_buckets)));
        let server =
            TestServer::new(Router::new().fallback(bucket).with_state(state.clone())).await;
        let admin = minting::key_admin(
            StorageConfig::S3 {
                bucket: "household".into(),
                region: "auto".into(),
                endpoint: Some(Url::parse(&server.url).unwrap()),
                profile: Some(S3Profile::Named(S3ProfileName::R2)),
                prefix: String::new(),
            },
            AdminCredential::BearerToken(SecretText::new("admin-token".into())),
            minting::AdminEndpoints::production(),
            Arc::new(SystemClock),
            Arc::new(UuidIds),
        )
        .unwrap();
        let key = S3Credentials {
            access_key_id: "token-1".into(),
            secret_access_key: SecretText::new("hash".into()),
        };
        let scope = MintScope::new(
            StoreId(uuid::Uuid::from_u128(3)),
            "member-id",
            coven_foundation::id_source::DeviceId(3),
            uuid::Uuid::from_u128(0x77),
        );
        let checked = admin.probe(&scope, &key).await;
        if lists_buckets {
            assert_eq!(
                checked.unwrap_err().failure(),
                StorageFailure::KeyBeyondStore
            );
        } else {
            checked.unwrap();
        }
        assert!(state.lock().unwrap().0.is_empty());
    }
}

/// A store's listing returns the account tokens whose name coven made for this
/// store, with the minting device the name carries (§13).
#[tokio::test]
async fn a_store_listing_returns_its_tokens_with_their_minting_device() {
    let (state, _server, admin) = fake().await;
    let ours = MintScope::new(
        StoreId(uuid::Uuid::from_u128(3 << 100)),
        "member-id",
        coven_foundation::id_source::DeviceId(0x0abc),
        uuid::Uuid::from_u128(0x77),
    );
    let elsewhere = MintScope::new(
        StoreId(uuid::Uuid::from_u128(6 << 100)),
        "member-id",
        coven_foundation::id_source::DeviceId(0x0abc),
        uuid::Uuid::from_u128(0x78),
    );
    let mine = admin.mint(&ours).await.unwrap();
    admin.mint(&elsewhere).await.unwrap();
    state.lock().unwrap().tokens.insert(
        "someone-else".into(),
        (
            "deploy token".into(),
            "resource".into(),
            "pg-write".into(),
            "value".into(),
        ),
    );
    assert_eq!(
        admin.list_keys(ours.store).await.unwrap(),
        [crate::ListedKey {
            access_key_id: mine.credentials.access_key_id.clone(),
            principal: S3Principal::R2Token {
                account: "acct".into()
            },
            device: coven_foundation::id_source::DeviceId(0x0abc),
        }]
    );
}
