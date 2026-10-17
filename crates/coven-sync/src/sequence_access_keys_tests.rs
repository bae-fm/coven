use super::{NewOperation, ReplaceKeyWork, Step, REPLACE_ACCESS_KEY};
use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::admin::StoreChange;
use coven_storage::Storage;
use std::sync::Arc;

// §14.3 and E9: a delivery is authenticated by the applied entry recording its key.
#[tokio::test]
async fn a_delivery_uses_the_signing_key_at_its_recorded_slot() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let replacement = ana
        .sync
        .replace_member_access_key(ben.identity.member(), Arc::new(group.storage.key_admin()))
        .await
        .unwrap();
    ana.sync.replace_member_keys().await.unwrap();
    ben.sync.pass().await.unwrap();
    let delivered = ben.sync.delivered_access_key(Some("ben")).await.unwrap();
    assert_eq!(
        delivered.map(|key| key.access_key_id),
        Some(replacement.key.credentials.access_key_id)
    );
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn an_admin_replacement_delivers_records_and_leaves_the_console_key_owed() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let admin = Arc::new(group.storage.key_admin());
    let replaced = ana
        .sync
        .replace_member_access_key(ben.identity.member(), admin.clone())
        .await
        .unwrap();
    let OldKeyDeletion::Console { operation } = replaced.old_key else {
        panic!("a console key is deleted in the console");
    };
    assert_eq!(admin.calls(), ["mint", "probe"]);
    let deletion: crate::sequence_sync::access_keys::Deletion = serde_json::from_slice(
        &ana.sync
            .database
            .operation(operation)
            .await
            .unwrap()
            .unwrap()
            .data,
    )
    .unwrap();
    assert_eq!(deletion.key.access_key_id, "ben");
    ben.sync.pass().await.unwrap();
    let delivered = ben
        .sync
        .delivered_access_key(Some("ben"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        delivered.access_key_id,
        replaced.key.credentials.access_key_id
    );
    assert!(ben
        .sync
        .delivered_access_key(Some(&delivered.access_key_id))
        .await
        .unwrap()
        .is_none());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_replacement_invalidated_by_contention_undoes_its_key_and_delivery() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &carol).await;
    carol.sync.pass().await.unwrap();
    let admin = Arc::new(group.storage.key_admin());
    let (delivery_started, delivery_observed) = tokio::sync::oneshot::channel();
    let (delivery_resume, delivery_held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(
            coven_storage::ObjectPrefix::all(group.store),
            delivery_started,
            delivery_held,
        )
        .await;
    let replace = ana
        .sync
        .replace_member_access_key(ben.identity.member(), admin.clone());
    let remove = async {
        delivery_observed.await.unwrap();
        let (claim_started, claim_observed) = tokio::sync::oneshot::channel();
        let (claim_resume, claim_held) = tokio::sync::oneshot::channel();
        group
            .storage
            .hold_next_creation(
                coven_storage::ObjectPrefix::all(group.store),
                claim_started,
                claim_held,
            )
            .await;
        delivery_resume.send(()).unwrap();
        claim_observed.await.unwrap();
        carol
            .sync
            .administer(StoreChange::RemoveMember {
                member: ben.identity.member(),
            })
            .await
            .unwrap();
        claim_resume.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(replace, remove);
    assert!(
        matches!(
            &result,
            Err(SyncError::Rejected(OperationRejection::TargetGone(_)))
        ),
        "{result:?}"
    );
    assert_eq!(admin.calls(), ["mint", "probe", "revoke", "key_exists"]);
    let deliveries = group
        .storage
        .list(&coven_storage::ObjectPrefix::access_keys(
            group.store,
            &ben.identity.member(),
        ))
        .await
        .unwrap();
    assert!(deliveries.is_empty(), "undo deletes its delivery");
    assert!(ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .iter()
        .all(|record| record.kind != "replace-access-key"));
    for device in [&ana, &ben, &carol] {
        device.sync.database.close().await.unwrap();
    }
}

use coven_database::Database;
use coven_foundation::id_source::StoreId;
use coven_storage::{
    test_utils::MemoryKeyAdmin, AccessKeyAdmin, MintScope, S3Credentials, S3MemberKey,
    StorageError, StorageFailure, StoreListing, UnkeyedPrincipal,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

struct InterruptedMint {
    database: Database,
    provider: MemoryKeyAdmin,
    kind: &'static str,
    fail_undo: AtomicBool,
    observations: Mutex<Vec<(MintScope, MintScope)>>,
}

#[async_trait::async_trait]
impl AccessKeyAdmin for InterruptedMint {
    async fn mint(&self, scope: &MintScope) -> Result<S3MemberKey, StorageError> {
        let record = self
            .database
            .operations()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.kind == self.kind)
            .unwrap();
        let data: serde_json::Value = serde_json::from_slice(&record.data).unwrap();
        let recorded = if self.kind == "invite" {
            &data["access"]["Minting"]
        } else {
            &data["scope"]
        };
        self.observations.lock().unwrap().push((
            scope.clone(),
            serde_json::from_value(recorded.clone()).unwrap(),
        ));
        self.provider.mint(scope).await
    }
    async fn probe(&self, scope: &MintScope, key: &S3Credentials) -> Result<(), StorageError> {
        self.provider.probe(scope, key).await
    }
    async fn roll_back(&self, scope: &MintScope) -> Result<(), StorageError> {
        if self.fail_undo.swap(false, Ordering::SeqCst) {
            return Err(StorageFailure::PermissionDenied.into());
        }
        self.provider.roll_back(scope).await
    }
    async fn revoke(
        &self,
        key: &str,
        principal: &coven_format::S3Principal,
    ) -> Result<(), StorageError> {
        self.provider.revoke(key, principal).await
    }
    async fn key_exists(
        &self,
        key: &str,
        principal: &coven_format::S3Principal,
    ) -> Result<bool, StorageError> {
        self.provider.key_exists(key, principal).await
    }
    async fn remove_unkeyed(
        &self,
        store: StoreId,
        principal: &UnkeyedPrincipal,
    ) -> Result<(), StorageError> {
        self.provider.remove_unkeyed(store, principal).await
    }
    async fn list_keys(&self, store: StoreId) -> Result<StoreListing, StorageError> {
        self.provider.list_keys(store).await
    }
}

// §14.3, §11: record the exact artifact before creation; retry undoes that name.
#[tokio::test]
async fn uncertain_mints_are_recorded_before_creation_and_undone_after_reopening() {
    for kind in ["invite", "replace-access-key"] {
        for keyless in [false, true] {
            interrupted_mint(kind, false, keyless).await;
        }
    }
}

// §13.2, §11: cancellation cannot discard an uncertain grant.
#[tokio::test]
async fn cancelling_an_uncertain_invite_keeps_its_name_until_undo_succeeds() {
    for keyless in [false, true] {
        interrupted_mint("invite", true, keyless).await;
    }
}

async fn interrupted_mint(kind: &'static str, cancel: bool, keyless: bool) {
    let group = Group::new();
    let mut ana = group.device(1).await;
    Arc::get_mut(&mut ana.sync).unwrap().ids = Arc::new(coven_foundation::id_source::UuidIds);
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let provider = group.storage.key_admin();
    if keyless {
        provider.stop_next_mint_before_key();
    } else {
        provider.fail_next_mint();
    }
    let admin = Arc::new(InterruptedMint {
        database: ana.sync.database.clone(),
        provider: provider.clone(),
        kind,
        fail_undo: AtomicBool::new(true),
        observations: Mutex::new(Vec::new()),
    });
    let result = if kind == "invite" {
        ana.sync
            .create_invite(
                InviteTarget::NewMember {
                    role: coven_format::admin::MemberRole::Member,
                },
                InviteAccess::S3AdminAccess(admin.clone()),
            )
            .await
            .map(drop)
    } else {
        ana.sync
            .replace_member_access_key(ben.identity.member(), admin.clone())
            .await
            .map(drop)
    };
    assert!(
        matches!(result, Err(SyncError::Cleanup { .. })),
        "{result:?}"
    );
    let (minted, recorded) = admin.observations.lock().unwrap()[0].clone();
    assert_eq!(
        minted, recorded,
        "the provider must receive the journaled name"
    );
    assert_eq!(provider.users(), std::slice::from_ref(&minted.key_name));
    let record = ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == kind)
        .unwrap();
    let mut ana = group.reopen(ana).await;
    Arc::get_mut(&mut ana.sync).unwrap().ids = Arc::new(coven_foundation::id_source::UuidIds);
    if cancel {
        let data: serde_json::Value = serde_json::from_slice(&record.data).unwrap();
        let invite = serde_json::from_value(data["id"].clone()).unwrap();
        assert!(matches!(
            ana.sync.cancel_invite(invite).await,
            Err(SyncError::Pending(_))
        ));
        assert!(ana
            .sync
            .database
            .operation(record.id)
            .await
            .unwrap()
            .is_some());
    }
    let resumed = Arc::new(InterruptedMint {
        database: ana.sync.database.clone(),
        provider: provider.clone(),
        kind,
        fail_undo: AtomicBool::new(false),
        observations: Mutex::new(Vec::new()),
    });
    ana.sync
        .retry_with_admin_access(record.id, resumed.clone())
        .await
        .unwrap();
    assert!(
        !provider.users().contains(&minted.key_name),
        "undo must delete the recorded principal"
    );
    if cancel {
        assert!(provider.users().is_empty());
        assert!(ana
            .sync
            .database
            .operation(record.id)
            .await
            .unwrap()
            .is_none());
        assert!(resumed.observations.lock().unwrap().is_empty());
    } else {
        let observations = resumed.observations.lock().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].0, observations[0].1);
        assert_ne!(
            observations[0].0, minted,
            "a retry mints under a fresh name"
        );
        assert_eq!(provider.users(), [observations[0].0.key_name.clone()]);
    }
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

use axum::{
    body::{Body, Bytes},
    extract::{Query, State},
    http::{Method, Response, Uri},
    Router,
};
use coven_foundation::id_source::DeviceId;
use coven_storage::{AdminCredential, S3Profile, S3ProfileName, StorageConfig};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Principal {
    description: String,
    keys: Vec<String>,
    policy: bool,
}

#[derive(Default)]
struct Remote {
    principals: BTreeMap<String, Principal>,
    bindings: BTreeMap<String, Value>,
    fail_delete: bool,
    fail_list: bool,
    deleted: Vec<String>,
}

fn response(status: u16, body: impl Into<String>) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::from(body.into()))
        .unwrap()
}

fn iam(remote: &mut Remote, form: BTreeMap<String, String>) -> Response<Body> {
    let action = form["Action"].as_str();
    let failure = |code| response(403, format!("<Error><Code>{code}</Code></Error>"));
    if remote.fail_list && action == "ListUsers" {
        return failure("AccessDenied");
    }
    let body = match action {
        "ListUsers" => remote
            .principals
            .keys()
            .map(|name| format!("<member><UserName>{name}</UserName></member>"))
            .collect(),
        "ListAccessKeys" => {
            let Some(principal) = remote.principals.get(&form["UserName"]) else {
                return response(404, "<Error><Code>NoSuchEntity</Code></Error>");
            };
            principal
                .keys
                .iter()
                .map(|key| format!("<member><AccessKeyId>{key}</AccessKeyId></member>"))
                .collect()
        }
        "DeleteUserPolicy" => {
            if let Some(principal) = remote.principals.get_mut(&form["UserName"]) {
                principal.policy = false;
            }
            String::new()
        }
        "DeleteUser" => {
            if remote.fail_delete {
                return failure("AccessDenied");
            }
            let name = &form["UserName"];
            if let Some(principal) = remote.principals.get(name) {
                assert!(principal.keys.is_empty() && !principal.policy);
            }
            remote.principals.remove(name);
            remote.deleted.push(name.clone());
            String::new()
        }
        _ => panic!("unexpected IAM action: {action}"),
    };
    let body = match action {
        "ListUsers" => format!("<Users>{body}</Users>"),
        "ListAccessKeys" => format!("<AccessKeyMetadata>{body}</AccessKeyMetadata>"),
        _ => body,
    };
    response(200, format!("<{action}Response><{action}Result>{body}<IsTruncated>false</IsTruncated></{action}Result></{action}Response>"))
}

fn gcs(
    remote: &mut Remote,
    method: Method,
    uri: Uri,
    query: BTreeMap<String, String>,
    body: Bytes,
) -> Response<Body> {
    let reply = |value: Value| response(200, value.to_string());
    let path = uri.path();
    match path {
        "/b/bucket" => {
            return reply(
                json!({"projectNumber":"123", "iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true}}}),
            )
        }
        "/projects/123" => return reply(json!({"projectId":"project"})),
        "/projects/project/serviceAccounts" => {
            if remote.fail_list {
                return response(403, "{}");
            }
            // One account per page exercises completion before any deletion.
            let index = query
                .get("pageToken")
                .map_or(0, |token| token.parse::<usize>().unwrap());
            let mut page = json!({"accounts": []});
            if let Some((email, principal)) = remote.principals.iter().nth(index) {
                page["accounts"] = json!([{"email":email, "description":principal.description}]);
                if index + 1 < remote.principals.len() {
                    page["nextPageToken"] = json!((index + 1).to_string());
                }
            }
            return reply(page);
        }
        "/projects/project/hmacKeys" => {
            let principal = &remote.principals[&query["serviceAccountEmail"]];
            return reply(
                json!({"items":principal.keys.iter().map(|key| json!({"accessId":key,"state":"ACTIVE"})).collect::<Vec<_>>()}),
            );
        }
        _ => (),
    }
    if path.ends_with("/iam") {
        if method == Method::PUT {
            remote
                .bindings
                .insert(path.into(), serde_json::from_slice(&body).unwrap());
        }
        return reply(remote.bindings[path].clone());
    }
    if path.starts_with("/projects/project/serviceAccounts/") {
        let name = uri.path().rsplit('/').next().unwrap().replace("%40", "@");
        if method == Method::GET {
            return if remote.principals.contains_key(&name) {
                reply(json!({"email":name,"description":remote.principals[&name].description}))
            } else {
                response(404, "{}")
            };
        }
        assert_eq!(method, Method::DELETE);
        if remote.fail_delete {
            return response(403, "{}");
        }
        if let Some(principal) = remote.principals.get(&name) {
            assert!(principal.keys.is_empty());
        }
        remote.principals.remove(&name);
        remote.deleted.push(name);
        return reply(json!({}));
    }
    panic!("unexpected GCS request: {method} {uri}");
}

async fn serve(
    State(remote): State<Arc<Mutex<Remote>>>,
    method: Method,
    uri: Uri,
    Query(query): Query<BTreeMap<String, String>>,
    body: Bytes,
) -> Response<Body> {
    gcs(&mut remote.lock().unwrap(), method, uri, query, body)
}

async fn serve_iam(
    State(remote): State<Arc<Mutex<Remote>>>,
    axum::Form(form): axum::Form<BTreeMap<String, String>>,
) -> Response<Body> {
    iam(&mut remote.lock().unwrap(), form)
}

struct Fake {
    remote: Arc<Mutex<Remote>>,
    task: tokio::task::JoinHandle<()>,
    admin: Arc<dyn AccessKeyAdmin>,
    profile: S3ProfileName,
}

impl Drop for Fake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Fake {
    async fn new(group: &Group, profile: S3ProfileName) -> Self {
        let remote = Arc::new(Mutex::new(Remote::default()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "auto".into(),
            endpoint: Some(
                format!("http://{}/", listener.local_addr().unwrap())
                    .parse()
                    .unwrap(),
            ),
            profile: Some(S3Profile::Named(profile)),
            prefix: "base".into(),
        };
        let StorageConfig::S3 {
            endpoint: Some(endpoint),
            ..
        } = &config
        else {
            unreachable!()
        };
        let endpoint = endpoint.clone();
        let router = Router::new()
            .route("/", axum::routing::post(serve_iam))
            .fallback(serve)
            .with_state(remote.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let admin = coven_storage::providers::s3_key_admin_at(
            config,
            if profile == S3ProfileName::Gcs {
                AdminCredential::BearerToken(coven_crypto::SecretText::new("admin-token".into()))
            } else {
                AdminCredential::AccessKey {
                    access_key_id: "admin-key".into(),
                    secret_access_key: coven_crypto::SecretText::new("admin-secret".into()),
                    session_token: None,
                    iam_endpoint: None,
                }
            },
            endpoint,
            group.clock.clone(),
            group.ids.clone(),
        )
        .unwrap();
        Self {
            remote,
            task,
            admin,
            profile,
        }
    }

    fn principal(&self, scope: &MintScope, keys: Vec<String>) -> String {
        let name = if self.profile == S3ProfileName::Gcs {
            format!("{}@project.iam.gserviceaccount.com", scope.name)
        } else {
            scope.key_name.clone()
        };
        self.remote.lock().unwrap().principals.insert(
            name.clone(),
            Principal {
                description: scope.label(),
                keys,
                policy: true,
            },
        );
        name
    }

    fn bind(&self, group: &Group) {
        let mut remote = self.remote.lock().unwrap();
        let members = remote
            .principals
            .keys()
            .map(|name| format!("serviceAccount:{name}"))
            .collect::<Vec<_>>();
        for (path, role) in [
            (
                format!("/b/bucket/managedFolders/base%2F{}%2F/iam", group.store),
                "roles/storage.objectUser",
            ),
            ("/b/bucket/iam".into(), "roles/storage.bucketViewer"),
        ] {
            remote.bindings.insert(
                path,
                json!({"etag":"1","bindings":[{"role":role,"members":members}]}),
            );
        }
    }
}

#[tokio::test]
async fn keyless_iam_users_wait_for_applied_retirement_and_failed_deletes_surface() {
    reclaim(S3ProfileName::Aws).await;
}

#[tokio::test]
async fn keyless_gcs_accounts_wait_for_applied_retirement_and_failed_deletes_surface() {
    reclaim(S3ProfileName::Gcs).await;
}

async fn reclaim(profile: S3ProfileName) {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &carol).await;
    let fake = Fake::new(&group, profile).await;
    let scope = |device, attempt: u128| {
        MintScope::new(
            group.store,
            "member",
            device,
            uuid::Uuid::from_u128(attempt << 100),
        )
    };
    let retired = fake.principal(&scope(ben.sync.device, 1), vec![]);
    let active = fake.principal(&scope(ana.sync.device, 2), vec![]);
    let unknown = fake.principal(&scope(DeviceId(u64::MAX), 3), vec![]);
    let keyed = fake.principal(&scope(ben.sync.device, 4), vec!["ben".into()]);
    let mut unrelated_scope = scope(ben.sync.device, 5);
    unrelated_scope.name = "unrelated".into();
    unrelated_scope.key_name = "unrelated".into();
    let unrelated = fake.principal(&unrelated_scope, vec![]);
    let foreign_scope = MintScope::new(
        coven_foundation::id_source::StoreId(uuid::Uuid::from_u128(9 << 100)),
        "member",
        ben.sync.device,
        uuid::Uuid::from_u128(6 << 100),
    );
    let foreign = fake.principal(&foreign_scope, vec![]);
    fake.bind(&group);
    let before = fake.remote.lock().unwrap().principals.clone();
    let operation = ana
        .database()
        .start_operation(NewOperation {
            kind: REPLACE_ACCESS_KEY.into(),
            data: serde_json::to_vec(&ReplaceKeyWork {
                member: ana.identity.member(),
                attempt: uuid::Uuid::from_u128(7 << 100),
                scope: scope(ana.sync.device, 7),
                old: crate::sequence_sync::tests::access("ana"),
                step: Step::Minting,
            })
            .unwrap(),
            started_by: "test".into(),
            pauses_writes: false,
        })
        .await
        .unwrap();
    ana.sync
        .admin_access
        .lock()
        .unwrap()
        .insert(operation, fake.admin.clone());
    // Another device has claimed the removal; Ana has not applied it.
    carol
        .sync
        .administer(StoreChange::RemoveDevice {
            device: ben.sync.device,
        })
        .await
        .unwrap();
    {
        let mut session = ana.sync.session.lock().await;
        ana.sync
            .sweep_stray_access_keys(&mut session, &ana.identity)
            .await
            .unwrap();
    }
    assert_eq!(fake.remote.lock().unwrap().principals, before);
    fake.remote.lock().unwrap().fail_list = true;
    let failed = ana.sync.pass().await;
    assert!(
        matches!(failed, Err(SyncError::Storage(ref error)) if error.failure() == StorageFailure::PermissionDenied),
        "{profile:?}: {failed:?}"
    );
    assert_eq!(fake.remote.lock().unwrap().principals, before);
    assert!(ana.sync.session.lock().await.swept.is_none());
    fake.remote.lock().unwrap().fail_list = false;
    fake.remote.lock().unwrap().fail_delete = true;
    let failed = ana.sync.pass().await;
    assert!(
        matches!(failed, Err(SyncError::Storage(ref error)) if error.failure() == StorageFailure::PermissionDenied),
        "{profile:?}: {failed:?}"
    );
    assert!(ana.sync.session.lock().await.swept.is_none());
    assert!(fake
        .remote
        .lock()
        .unwrap()
        .principals
        .contains_key(&retired));
    fake.remote.lock().unwrap().fail_delete = false;
    ana.sync.pass().await.unwrap();
    {
        let remote = fake.remote.lock().unwrap();
        assert!(!remote.principals.contains_key(&retired), "{profile:?}");
        for name in [&active, &unknown, &unrelated, &foreign, &keyed] {
            assert_eq!(remote.principals[name], before[name], "{profile:?}: {name}");
        }
        assert_eq!(remote.deleted, std::slice::from_ref(&retired));
        if profile == S3ProfileName::Gcs {
            for binding in remote.bindings.values() {
                let members = binding["bindings"][0]["members"].as_array().unwrap();
                assert!(!members.contains(&json!(format!("serviceAccount:{retired}"))));
                for name in [&active, &unknown, &unrelated, &foreign, &keyed] {
                    assert!(members.contains(&json!(format!("serviceAccount:{name}"))));
                }
            }
        }
    }
    assert!(ana
        .database()
        .operations()
        .await
        .unwrap()
        .iter()
        .all(|op| op.kind != "delete-access-key"));
    ana.database().close().await.unwrap();
    ben.database().close().await.unwrap();
    carol.database().close().await.unwrap();
}
