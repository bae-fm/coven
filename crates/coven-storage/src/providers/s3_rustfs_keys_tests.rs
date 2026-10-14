//! RustFS minting against an in-process admin API and S3 API. The S3 side
//! lets a service account reach what its inline policy allows.
use super::*;
use crate::providers::s3_admin_query::{authorization, SignedRequest};
use crate::providers::tests::{query, response, TestServer};
use crate::{AdminCredential, S3Profile, S3ProfileName};
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
    /// User to (secret, policy).
    users: BTreeMap<String, (String, Option<String>)>,
    /// Access key to (parent, name, secret, policy prefix).
    accounts: BTreeMap<String, (String, String, String, String)>,
    objects: BTreeMap<String, Vec<u8>>,
    next: u32,
    lose_create_reply: bool,
    /// Answer every delete as RustFS answers one of an absent account.
    refuse_delete: bool,
    bad_signatures: u32,
}

fn admin_api(
    state: &mut Remote,
    method: HttpMethod,
    uri: &Uri,
    headers: &HeaderMap,
    body: &Bytes,
) -> Response<Body> {
    let url = Url::parse(&format!(
        "http://{}{}",
        headers["host"].to_str().unwrap(),
        uri
    ))
    .unwrap();
    let signed: Vec<(&str, String)> = ["host", "x-amz-content-sha256", "x-amz-date"]
        .into_iter()
        .map(|name| (name, headers[name].to_str().unwrap().to_owned()))
        .collect();
    let expected = authorization(
        "root",
        "root-secret",
        &SignedRequest {
            method: method.as_str(),
            url: &url,
            region: "us-east-1",
            service: "s3",
        },
        headers["x-amz-date"].to_str().unwrap(),
        &signed,
        body,
    );
    if headers["authorization"] != expected.as_str() {
        state.bad_signatures += 1;
        return response(403, "{}");
    }
    let q = query(uri);
    let body: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
    match uri.path().strip_prefix("/rustfs/admin/v3/").unwrap() {
        "user-info" => match state.users.get(&q["accessKey"]) {
            Some(_) => response(200, "{}"),
            None => response(404, "{}"),
        },
        "add-user" => {
            state.users.insert(
                q["accessKey"].clone(),
                (body["secretKey"].as_str().unwrap().to_owned(), None),
            );
            response(200, "")
        }
        "set-user-or-group-policy" => {
            state.users.get_mut(&q["userOrGroup"]).unwrap().1 = Some(q["policyName"].clone());
            response(200, "")
        }
        "list-service-accounts" => {
            let accounts: Vec<_> = state
                .accounts
                .iter()
                .filter(|(_, (parent, ..))| *parent == q["user"])
                .map(|(key, (_, name, ..))| serde_json::json!({"accessKey": key, "name": name}))
                .collect();
            response(200, serde_json::json!({"accounts": accounts}).to_string())
        }
        "add-service-account" => {
            let parent = body["targetUser"].as_str().unwrap().to_owned();
            assert!(state.users.contains_key(&parent));
            state.next += 1;
            let key = format!("SA{}", state.next);
            let secret = format!("sa-secret-{}", state.next);
            let prefix = body["policy"]["Statement"][1]["Condition"]["StringLike"]["s3:prefix"]
                .as_str()
                .unwrap()
                .trim_end_matches('*')
                .to_owned();
            state.accounts.insert(
                key.clone(),
                (
                    parent,
                    body["name"].as_str().unwrap().to_owned(),
                    secret.clone(),
                    prefix,
                ),
            );
            if std::mem::take(&mut state.lose_create_reply) {
                return response(500, "{}");
            }
            response(
                200,
                serde_json::json!({"credentials": {"accessKey": key, "secretKey": secret}})
                    .to_string(),
            )
        }
        // RustFS answers a delete of an absent account with HTTP 400.
        "delete-service-account" if state.refuse_delete => s3_error(400, "InvalidRequest"),
        "delete-service-account" => match state.accounts.remove(&q["accessKey"]) {
            Some(_) => response(204, ""),
            None => s3_error(400, "InvalidRequest"),
        },
        "info-service-account" => match state.accounts.get(&q["accessKey"]) {
            Some(_) => response(200, "{}"),
            None => response(404, "{}"),
        },
        _ => response(404, "{}"),
    }
}

fn s3_error(status: u16, code: &str) -> Response<Body> {
    response(status, format!("<Error><Code>{code}</Code></Error>"))
}

fn s3(
    state: &mut Remote,
    method: HttpMethod,
    uri: &Uri,
    headers: &HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let sent = headers["authorization"].to_str().unwrap();
    let key = sent
        .split("Credential=")
        .nth(1)
        .unwrap()
        .split('/')
        .next()
        .unwrap();
    let Some((.., prefix)) = state.accounts.get(key).cloned() else {
        return s3_error(403, "InvalidAccessKeyId");
    };
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
    if !reach.starts_with(&prefix) {
        return s3_error(403, "AccessDenied");
    }
    if object.is_empty() {
        return response(200, "<ListBucketResult><Name>bucket</Name><IsTruncated>false</IsTruncated></ListBucketResult>");
    }
    match method {
        HttpMethod::PUT => {
            if headers.contains_key("if-none-match") && state.objects.contains_key(&object) {
                return s3_error(412, "PreconditionFailed");
            }
            state.objects.insert(object, body.to_vec());
            Response::builder()
                .header("etag", "\"e\"")
                .body(Body::empty())
                .unwrap()
        }
        HttpMethod::GET => match state.objects.get(&object) {
            Some(bytes) => Response::builder()
                .header("etag", "\"e\"")
                .header("last-modified", "Wed, 14 Oct 2026 00:00:00 GMT")
                .header("content-length", bytes.len())
                .body(Body::from(bytes.clone()))
                .unwrap(),
            None => s3_error(404, "NoSuchKey"),
        },
        HttpMethod::DELETE => {
            state.objects.remove(&object);
            response(204, Vec::new())
        }
        _ => s3_error(400, "InvalidRequest"),
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
    if uri.path().starts_with("/rustfs/admin/") {
        admin_api(&mut state, method, &uri, &headers, &body)
    } else {
        s3(&mut state, method, &uri, &headers, body)
    }
}

#[tokio::test]
async fn service_accounts_share_one_parent_and_reach_only_the_store() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(serve).with_state(state.clone())).await;
    let admin = minting::key_admin(
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some(Url::parse(&server.url).unwrap()),
            profile: Some(S3Profile::Named(S3ProfileName::RustFs)),
            prefix: "base".into(),
        },
        AdminCredential::AccessKey {
            access_key_id: "root".into(),
            secret_access_key: SecretText::new("root-secret".into()),
            session_token: None,
        },
        minting::AdminEndpoints::production(),
        Arc::new(SystemClock),
        Arc::new(UuidIds),
    )
    .unwrap();
    let scope = MintScope::new(
        StoreId(uuid::Uuid::from_u128(4)),
        "member-id",
        uuid::Uuid::from_u128(0x55),
    );
    state.lock().unwrap().lose_create_reply = true;
    assert!(admin.mint(&scope).await.is_err());
    let minted = admin.mint(&scope).await.unwrap();
    assert_eq!(
        minted.principal,
        S3Principal::RustFsServiceAccount {
            parent: "coven".into()
        }
    );
    {
        let state = state.lock().unwrap();
        assert_eq!(
            state.accounts.len(),
            1,
            "the account with a lost secret is gone"
        );
        assert_eq!(state.users["coven"].1.as_deref(), Some("readwrite"));
        assert_ne!(state.users["coven"].0, "", "the parent's secret is random");
    }
    admin.probe(&scope, &minted.credentials).await.unwrap();
    let key = minted.credentials.access_key_id.clone();
    assert!(admin.key_exists(&key, &minted.principal).await.unwrap());
    // A refused delete of a present account is not mistaken for absence.
    state.lock().unwrap().refuse_delete = true;
    assert_eq!(
        admin
            .revoke(&key, &minted.principal)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Refused
    );
    assert!(admin.key_exists(&key, &minted.principal).await.unwrap());
    state.lock().unwrap().refuse_delete = false;
    admin.revoke(&key, &minted.principal).await.unwrap();
    admin.revoke(&key, &minted.principal).await.unwrap();
    assert!(!admin.key_exists(&key, &minted.principal).await.unwrap());
    // Minting again keeps the parent; rolling back removes only the account.
    admin.mint(&scope).await.unwrap();
    admin.roll_back(&scope).await.unwrap();
    let state = state.lock().unwrap();
    assert!(state.accounts.is_empty());
    assert!(state.users.contains_key("coven"));
    assert_eq!(state.bad_signatures, 0);
}
