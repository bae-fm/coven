//! IAM minting against an in-process IAM and S3 server. The S3 side enforces
//! each minted user's policy prefix, so the member-side check sees what a
//! scoped key may and may not reach.
use super::*;
use crate::providers::s3_admin_query::{authorization, SignedRequest};
use crate::providers::tests::{query, response, TestServer};
use crate::{AccessKeyAdmin, AdminCredential, S3Profile, S3ProfileName};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, Response, Uri},
    Router,
};
use coven_foundation::{clock::SystemClock, id_source::UuidIds};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use url::Url;

const ADMIN_ID: &str = "admin-key";
const ADMIN_SECRET: &str = "admin-secret";

#[derive(Default)]
struct User {
    path: Option<String>,
    tags: Vec<(String, String)>,
    inline: Option<(String, String)>,
    attached: Vec<String>,
    keys: Vec<(String, String)>,
}

#[derive(Default)]
struct Remote {
    users: BTreeMap<String, User>,
    /// ARN to (name, document).
    policies: BTreeMap<String, (String, String)>,
    objects: BTreeMap<String, Vec<u8>>,
    next: u32,
    actions: Vec<String>,
    /// CreateAccessKey creates its key but answers with a server error.
    lose_key_reply: bool,
    /// New keys are refused this many times before they work.
    propagation: u32,
    /// Every key may list the whole bucket.
    broad: bool,
    /// Signed admin requests whose recomputed signature differed.
    bad_signatures: u32,
}

fn iam_error(status: u16, code: &str) -> Response<Body> {
    response(
        status,
        format!("<ErrorResponse><Error><Type>Sender</Type><Code>{code}</Code><Message>m</Message></Error></ErrorResponse>"),
    )
}

fn reply(action: &str, inner: String) -> Response<Body> {
    response(
        200,
        format!("<{action}Response><{action}Result>{inner}</{action}Result></{action}Response>"),
    )
}

/// The prefix a key's user may list, from its inline or attached policy.
fn allowed(state: &Remote, key: &str) -> Option<String> {
    let user = state
        .users
        .values()
        .find(|user| user.keys.iter().any(|(id, _)| id == key))?;
    let document = match &user.inline {
        Some((_, document)) => document.clone(),
        None => state.policies.get(user.attached.first()?)?.1.clone(),
    };
    let policy: serde_json::Value = serde_json::from_str(&document).unwrap();
    let prefix = policy["Statement"][1]["Condition"]["StringLike"]["s3:prefix"]
        .as_str()
        .unwrap();
    Some(prefix.trim_end_matches('*').to_owned())
}

fn admin(state: &mut Remote, uri: &Uri, headers: &HeaderMap, body: &Bytes) -> Response<Body> {
    let form: BTreeMap<String, String> = url::form_urlencoded::parse(body).into_owned().collect();
    let endpoint = Url::parse(&format!(
        "http://{}{}",
        headers["host"].to_str().unwrap(),
        uri.path()
    ))
    .unwrap();
    let sent = headers["authorization"].to_str().unwrap();
    let region = sent.split('/').nth(2).unwrap();
    let signed: Vec<(&str, String)> = ["content-type", "host", "x-amz-date"]
        .into_iter()
        .map(|name| (name, headers[name].to_str().unwrap().to_owned()))
        .collect();
    let expected = authorization(
        ADMIN_ID,
        ADMIN_SECRET,
        &SignedRequest {
            method: "POST",
            url: &endpoint,
            region,
            service: "iam",
        },
        headers["x-amz-date"].to_str().unwrap(),
        &signed,
        body,
    );
    if sent != expected {
        state.bad_signatures += 1;
        return iam_error(403, "SignatureDoesNotMatch");
    }
    let action = form["Action"].clone();
    state.actions.push(action.clone());
    let user_name = form.get("UserName").cloned().unwrap_or_default();
    match action.as_str() {
        "CreateUser" => {
            if state.users.contains_key(&user_name) {
                return iam_error(409, "EntityAlreadyExists");
            }
            let tags = (1..)
                .map_while(|n| {
                    Some((
                        form.get(&format!("Tags.member.{n}.Key"))?.clone(),
                        form[&format!("Tags.member.{n}.Value")].clone(),
                    ))
                })
                .collect();
            state.users.insert(
                user_name,
                User {
                    path: form.get("Path").cloned(),
                    tags,
                    ..User::default()
                },
            );
            reply(&action, String::new())
        }
        "PutUserPolicy" => {
            let Some(user) = state.users.get_mut(&user_name) else {
                return iam_error(404, "NoSuchEntity");
            };
            user.inline = Some((form["PolicyName"].clone(), form["PolicyDocument"].clone()));
            reply(&action, String::new())
        }
        "CreatePolicy" => {
            let arn = format!("arn:aws:iam:::policy/{}", form["PolicyName"]);
            if state.policies.contains_key(&arn) {
                return iam_error(409, "EntityAlreadyExists");
            }
            state.policies.insert(
                arn.clone(),
                (form["PolicyName"].clone(), form["PolicyDocument"].clone()),
            );
            reply(&action, format!("<Policy><Arn>{arn}</Arn></Policy>"))
        }
        "ListPolicies" => {
            let members: String = state
                .policies
                .iter()
                .map(|(arn, (name, _))| {
                    format!("<member><PolicyName>{name}</PolicyName><Arn>{arn}</Arn></member>")
                })
                .collect();
            reply(
                &action,
                format!("<IsTruncated>false</IsTruncated><Policies>{members}</Policies>"),
            )
        }
        "AttachUserPolicy" => {
            let arn = form["PolicyArn"].clone();
            if !state.policies.contains_key(&arn) {
                return iam_error(404, "NoSuchEntity");
            }
            let Some(user) = state.users.get_mut(&user_name) else {
                return iam_error(404, "NoSuchEntity");
            };
            if !user.attached.contains(&arn) {
                user.attached.push(arn);
            }
            reply(&action, String::new())
        }
        "ListAccessKeys" => {
            let Some(user) = state.users.get(&user_name) else {
                return iam_error(404, "NoSuchEntity");
            };
            let members: String = user
                .keys
                .iter()
                .map(|(id, _)| format!("<member><AccessKeyId>{id}</AccessKeyId></member>"))
                .collect();
            reply(
                &action,
                format!("<IsTruncated>false</IsTruncated><AccessKeyMetadata>{members}</AccessKeyMetadata>"),
            )
        }
        "CreateAccessKey" => {
            state.next += 1;
            let id = format!("member-key-{}", state.next);
            let secret = format!("member+secret/{}&", state.next);
            let Some(user) = state.users.get_mut(&user_name) else {
                return iam_error(404, "NoSuchEntity");
            };
            if user.keys.len() >= 2 {
                return iam_error(409, "LimitExceeded");
            }
            user.keys.push((id.clone(), secret.clone()));
            if std::mem::take(&mut state.lose_key_reply) {
                return iam_error(500, "ServiceFailure");
            }
            let secret = secret.replace('&', "&amp;");
            reply(
                &action,
                format!("<AccessKey><AccessKeyId>{id}</AccessKeyId><SecretAccessKey>{secret}</SecretAccessKey></AccessKey>"),
            )
        }
        "DeleteAccessKey" => {
            let Some(user) = state.users.get_mut(&user_name) else {
                return iam_error(404, "NoSuchEntity");
            };
            let before = user.keys.len();
            user.keys.retain(|(id, _)| *id != form["AccessKeyId"]);
            if user.keys.len() == before {
                return iam_error(404, "NoSuchEntity");
            }
            reply(&action, String::new())
        }
        "DeleteUserPolicy" => {
            let Some(user) = state.users.get_mut(&user_name) else {
                return iam_error(404, "NoSuchEntity");
            };
            if user.inline.take().is_none() {
                return iam_error(404, "NoSuchEntity");
            }
            reply(&action, String::new())
        }
        "DetachUserPolicy" => {
            let Some(user) = state.users.get_mut(&user_name) else {
                return iam_error(404, "NoSuchEntity");
            };
            user.attached.retain(|arn| *arn != form["PolicyArn"]);
            reply(&action, String::new())
        }
        "DeletePolicy" => match state.policies.remove(&form["PolicyArn"]) {
            Some(_) => reply(&action, String::new()),
            None => iam_error(404, "NoSuchEntity"),
        },
        "DeleteUser" => {
            let Some(user) = state.users.get(&user_name) else {
                return iam_error(404, "NoSuchEntity");
            };
            if !user.keys.is_empty() || user.inline.is_some() || !user.attached.is_empty() {
                return iam_error(409, "DeleteConflict");
            }
            state.users.remove(&user_name);
            reply(&action, String::new())
        }
        _ => iam_error(400, "InvalidAction"),
    }
}

fn s3(
    state: &mut Remote,
    method: Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let sent = headers["authorization"].to_str().unwrap();
    let key_id = sent
        .split("Credential=")
        .nth(1)
        .unwrap()
        .split('/')
        .next()
        .unwrap()
        .to_owned();
    if state.propagation > 0 {
        state.propagation -= 1;
        return iam_error(403, "InvalidAccessKeyId");
    }
    let Some(prefix) = allowed(state, &key_id) else {
        return iam_error(403, "InvalidAccessKeyId");
    };
    let q = query(uri);
    let object = uri
        .path()
        .strip_prefix("/bucket")
        .unwrap()
        .trim_start_matches('/')
        .to_owned();
    let reach = if object.is_empty() {
        q.get("prefix").cloned().unwrap_or_default()
    } else {
        object.clone()
    };
    if !state.broad && !reach.starts_with(&prefix) {
        return iam_error(403, "AccessDenied");
    }
    if object.is_empty() {
        let contents: String = state
            .objects
            .keys()
            .filter(|key| key.starts_with(&reach))
            .map(|key| format!("<Contents><Key>{key}</Key><Size>1</Size><LastModified>2026-10-14T00:00:00.000Z</LastModified><ETag>\"e\"</ETag></Contents>"))
            .collect();
        return response(200, format!("<ListBucketResult><Name>bucket</Name><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"));
    }
    match method {
        Method::PUT => {
            if headers.contains_key("if-none-match") && state.objects.contains_key(&object) {
                return iam_error(412, "PreconditionFailed");
            }
            state.objects.insert(object, body.to_vec());
            Response::builder()
                .header("etag", "\"e\"")
                .body(Body::empty())
                .unwrap()
        }
        Method::GET => match state.objects.get(&object) {
            Some(bytes) => Response::builder()
                .header("etag", "\"e\"")
                .header("last-modified", "Wed, 14 Oct 2026 00:00:00 GMT")
                .header("content-length", bytes.len())
                .body(Body::from(bytes.clone()))
                .unwrap(),
            None => iam_error(404, "NoSuchKey"),
        },
        Method::DELETE => {
            state.objects.remove(&object);
            response(204, Vec::new())
        }
        _ => iam_error(400, "InvalidRequest"),
    }
}

async fn serve(
    State(state): State<Arc<Mutex<Remote>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let mut state = state.lock().unwrap();
    if uri.path().starts_with("/bucket") {
        s3(&mut state, method, &uri, &headers, body)
    } else {
        admin(&mut state, &uri, &headers, &body)
    }
}

struct Fake {
    state: Arc<Mutex<Remote>>,
    server: TestServer,
}

impl Fake {
    async fn new() -> Self {
        let state = Arc::new(Mutex::new(Remote::default()));
        let server = TestServer::new(Router::new().fallback(serve).with_state(state.clone())).await;
        Self { state, server }
    }

    fn admin(&self, name: S3ProfileName) -> Arc<dyn AccessKeyAdmin> {
        let url = Url::parse(&self.server.url).unwrap();
        minting::key_admin(
            StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "us-east-1".into(),
                endpoint: Some(url.clone()),
                profile: Some(S3Profile::Named(name)),
                prefix: "base".into(),
            },
            AdminCredential::AccessKey {
                access_key_id: ADMIN_ID.into(),
                secret_access_key: SecretText::new(ADMIN_SECRET.into()),
                session_token: None,
            },
            minting::AdminEndpoints {
                aws_iam: url.join("/iam/").unwrap(),
                ..minting::AdminEndpoints::production()
            },
            Arc::new(SystemClock),
            Arc::new(UuidIds),
        )
        .unwrap()
    }
}

fn scope() -> MintScope {
    MintScope::new(
        coven_foundation::id_source::StoreId(uuid::Uuid::from_u128(7)),
        "member-id",
        uuid::Uuid::from_u128(0x1234),
    )
}

#[tokio::test]
async fn seaweedfs_attaches_a_managed_prefix_policy_and_revokes_everything() {
    let fake = Fake::new().await;
    let admin = fake.admin(S3ProfileName::SeaweedFs);
    let scope = scope();
    let minted = admin.mint(&scope).await.unwrap();
    let S3Principal::IamUser { user, policy } = &minted.principal else {
        panic!("IAM principal")
    };
    assert_eq!(*user, scope.name);
    let IamPolicy::Managed { arn } = policy else {
        panic!("managed policy")
    };
    assert_eq!(
        minted.credentials.secret_access_key.as_str(),
        "member+secret/1&"
    );
    {
        let state = fake.state.lock().unwrap();
        let remote = &state.users[user];
        assert_eq!(remote.attached, std::slice::from_ref(arn));
        assert!(remote.inline.is_none() && remote.path.is_none());
        let document = &state.policies[arn].1;
        assert!(document.contains(&format!("base/{}/*", scope.store)));
        assert!(!document.contains("Deny"));
    }
    admin.probe(&scope, &minted.credentials).await.unwrap();
    assert!(fake.state.lock().unwrap().objects.is_empty());
    let id = &minted.credentials.access_key_id;
    assert!(admin.key_exists(id, &minted.principal).await.unwrap());
    admin.revoke(id, &minted.principal).await.unwrap();
    assert!(!admin.key_exists(id, &minted.principal).await.unwrap());
    admin.revoke(id, &minted.principal).await.unwrap();
    let state = fake.state.lock().unwrap();
    assert!(state.users.is_empty() && state.policies.is_empty());
    assert_eq!(state.bad_signatures, 0);
}

#[tokio::test]
async fn aws_puts_an_inline_policy_on_a_tagged_user_at_the_iam_endpoint() {
    let fake = Fake::new().await;
    let admin = fake.admin(S3ProfileName::Aws);
    let scope = scope();
    let minted = admin.mint(&scope).await.unwrap();
    assert_eq!(
        minted.principal,
        S3Principal::IamUser {
            user: scope.name.clone(),
            policy: IamPolicy::Inline {
                name: "coven-member".into()
            }
        }
    );
    {
        let state = fake.state.lock().unwrap();
        let user = &state.users[&scope.name];
        assert_eq!(user.path.as_deref(), Some("/coven/"));
        assert_eq!(
            user.tags,
            [
                ("coven-store".into(), scope.store.to_string()),
                ("coven-member".into(), "member-id".into())
            ]
        );
    }
    admin.probe(&scope, &minted.credentials).await.unwrap();
    admin.roll_back(&scope).await.unwrap();
    assert!(fake.state.lock().unwrap().users.is_empty());
    assert!(!admin
        .key_exists(&minted.credentials.access_key_id, &minted.principal)
        .await
        .unwrap());
}

#[tokio::test]
async fn a_retry_replaces_keys_whose_secret_was_lost() {
    let fake = Fake::new().await;
    let admin = fake.admin(S3ProfileName::Aws);
    let scope = scope();
    fake.state.lock().unwrap().lose_key_reply = true;
    assert_eq!(
        admin.mint(&scope).await.err().unwrap().failure(),
        StorageFailure::Network
    );
    assert_eq!(fake.state.lock().unwrap().users[&scope.name].keys.len(), 1);
    // The user already exists; the lost key is deleted before a new one.
    let minted = admin.mint(&scope).await.unwrap();
    let again = admin.mint(&scope).await.unwrap();
    let state = fake.state.lock().unwrap();
    let keys = &state.users[&scope.name].keys;
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].0, again.credentials.access_key_id);
    assert_ne!(
        minted.credentials.access_key_id,
        again.credentials.access_key_id
    );
}

#[tokio::test]
async fn the_member_check_waits_for_propagation_and_refuses_a_broad_policy() {
    let fake = Fake::new().await;
    let admin = fake.admin(S3ProfileName::SeaweedFs);
    let scope = scope();
    let minted = admin.mint(&scope).await.unwrap();
    fake.state.lock().unwrap().propagation = 2;
    admin.probe(&scope, &minted.credentials).await.unwrap();
    fake.state.lock().unwrap().broad = true;
    assert_eq!(
        admin
            .probe(&scope, &minted.credentials)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::KeyBeyondStore
    );
}

#[tokio::test]
async fn console_keys_and_other_principals_are_not_deleted_through_iam() {
    let fake = Fake::new().await;
    let admin = fake.admin(S3ProfileName::Aws);
    for principal in [
        S3Principal::Console,
        S3Principal::R2Token {
            account: "account".into(),
        },
    ] {
        assert_eq!(
            admin.revoke("key", &principal).await.unwrap_err().failure(),
            StorageFailure::InvalidConfiguration
        );
    }
    assert!(fake.state.lock().unwrap().actions.is_empty());
}
