use super::*;
use crate::providers::tests::{json as reply, read, response, session, TestServer};
use crate::test_utils::STORE_ID;
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Response, Uri},
    Router,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

#[cfg(not(any(target_os = "ios", target_os = "android")))]
#[path = "dropbox_live.rs"]
mod live;

struct RemovalJob {
    member: String,
    statuses: std::collections::VecDeque<Value>,
}
#[derive(Default)]
struct BufferedUpload {
    bytes: Vec<u8>,
    closed: bool,
}
#[derive(Default)]
pub(super) struct Remote {
    app_folder: bool,
    requests: usize,
    non_owner: bool,
    pub(super) missing_namespace: bool,
    mounted: BTreeSet<String>,
    mount_requests: usize,
    lose_mount_reply: bool,
    inherited_members: BTreeMap<String, String>,
    invitees: Vec<Value>,
    groups: Vec<Value>,
    lose_remove_reply: bool,
    removal_job: Option<RemovalJob>,
    pub(super) objects: BTreeMap<String, Vec<u8>>,
    revisions: BTreeMap<String, u64>,
    uploads: BTreeMap<String, BufferedUpload>,
    next_upload: u64,
    fail_reply: bool,
    close_requests: Vec<u64>,
    members: BTreeMap<String, String>,
    sharing_mutations: Vec<String>,
    refuse_share: bool,
    lose_update_reply: bool,
    range_reads: usize,
    pub(super) folders: BTreeSet<String>,
    list_prefix: String,
    list_recursive: bool,
}
pub(super) fn file_metadata(path: &str, bytes: &[u8], state: &Remote) -> Value {
    json!({".tag":"file", "path_lower":path, "size":bytes.len(), "id":format!("id:{path}"),
        "rev":state.revisions.get(path).copied().unwrap_or(1).to_string(),
        "server_modified":"2026-10-06T00:00:00Z", "client_modified":"2000-01-01T00:00:00Z"})
}
pub(super) async fn endpoint(
    State(state): State<Arc<Mutex<Remote>>>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let token = headers["authorization"].to_str().unwrap();
    let recipient = token.strip_prefix("Bearer recipient:");
    assert!(token == "Bearer token" || recipient.is_some());
    let mut state = state.lock().unwrap();
    state.requests += 1;
    if !state.app_folder && uri.path().starts_with("/2/files/") {
        assert_eq!(
            headers["dropbox-api-path-root"],
            r#"{".tag":"namespace_id","namespace_id":"namespace"}"#
        );
    } else {
        assert!(!headers.contains_key("dropbox-api-path-root"));
    }
    let arg: Value = match headers.get("dropbox-api-arg") {
        Some(arg) => serde_json::from_str(arg.to_str().unwrap()).unwrap(),
        None => serde_json::from_slice(&body).unwrap(),
    };
    let path = arg["path"].as_str().unwrap_or("").to_owned();
    let error = |summary: &str| {
        response(
            409,
            json!({"error_summary":summary,"error":{".tag":"path","path":{".tag":"not_found"}}})
                .to_string(),
        )
    };
    if let Some(email) = recipient {
        if !state.members.contains_key(email)
            || (uri.path().starts_with("/2/files/") && !state.mounted.contains(email))
        {
            return response(
                409,
                json!({"error":{".tag":"access_error","access_error":{".tag":"not_a_member"}}})
                    .to_string(),
            );
        }
    }
    if state.app_folder {
        assert!(!uri.path().starts_with("/2/sharing/"));
        if let Some(path) = arg["path"].as_str() {
            assert!(path.starts_with(&format!("/{STORE_ID}/")) || path == format!("/{STORE_ID}"));
        }
    }
    match uri.path() {
        "/2/users/get_current_account" => reply(json!({"email":"owner@example.test"})),
        "/2/sharing/mount_folder" => {
            assert_eq!(arg["shared_folder_id"], "namespace");
            state.mount_requests += 1;
            let email = recipient.expect("mount runs under recipient account");
            if !state.mounted.insert(email.into()) {
                return response(409, json!({"error":{".tag":"already_mounted"}}).to_string());
            }
            if std::mem::replace(&mut state.lose_mount_reply, false) {
                return response(503, "{}");
            }
            reply(json!({"shared_folder_id":"namespace"}))
        }
        "/2/sharing/get_folder_metadata" => {
            assert_eq!(arg["shared_folder_id"], "namespace");
            if state.missing_namespace {
                return response(409, json!({"error":{".tag":"shared_folder_access_error","shared_folder_access_error":{".tag":"no_permission"}}}).to_string());
            }
            reply(
                json!({"shared_folder_id":"namespace", "access_type":{".tag":if state.non_owner || recipient.is_some() {"editor"} else {"owner"}}}),
            )
        }
        "/2/files/upload" => {
            if body.len() > 150 * 1024 * 1024 {
                return response(413, "single request limit exceeded");
            }
            if arg["mode"] == "add" && state.objects.contains_key(&path) {
                return error("path/conflict/file/...");
            }
            if arg["mode"][".tag"] == "update" {
                let current = state.revisions.get(&path).map(u64::to_string);
                if !state.objects.contains_key(&path)
                    || current.as_deref() != arg["mode"]["update"].as_str()
                {
                    return error("path/conflict/file/...");
                }
            } else {
                assert_eq!(arg["mode"], "add");
            }
            assert_eq!(arg["autorename"], false);
            assert_eq!(arg["strict_conflict"], true);
            *state.revisions.entry(path.clone()).or_insert(0) += 1;
            state.objects.insert(path.clone(), body.to_vec());
            if std::mem::take(&mut state.fail_reply) {
                return response(503, "{}");
            }
            reply(file_metadata(&path, &body, &state))
        }
        "/2/files/download" => {
            if headers.contains_key("range") {
                state.range_reads += 1;
            }
            match state.objects.get(&path) {
                Some(bytes) => {
                    let mut response = read(bytes, &headers);
                    response.headers_mut().insert(
                        "dropbox-api-result",
                        file_metadata(&path, bytes, &state)
                            .to_string()
                            .parse()
                            .unwrap(),
                    );
                    response
                }
                None => error("path/not_found/..."),
            }
        }
        "/2/files/list_folder" | "/2/files/list_folder/continue" => {
            if uri.path() == "/2/files/list_folder" {
                assert!(!path.is_empty());
                state.list_prefix = format!("{path}/");
                state.list_recursive = arg["recursive"].as_bool().unwrap();
            }
            let start = arg["cursor"]
                .as_str()
                .map(|s| s.parse::<usize>().unwrap())
                .unwrap_or(0);
            let mut all: Vec<_> = state
                .objects
                .iter()
                .map(|(path, bytes)| file_metadata(path, bytes, &state))
                .chain(
                    state
                        .folders
                        .iter()
                        .map(|path| json!({".tag":"folder","path_lower":path})),
                )
                .filter(|entry| {
                    entry["path_lower"]
                        .as_str()
                        .unwrap()
                        .starts_with(&state.list_prefix)
                })
                .collect();
            if !state.list_recursive {
                // A non-recursive listing returns each immediate child once.
                let prefix = state.list_prefix.clone();
                let mut children = std::collections::BTreeMap::new();
                for entry in all {
                    let path = entry["path_lower"].as_str().unwrap();
                    match path[prefix.len()..].split_once('/') {
                        Some((folder, _)) => {
                            let folder = format!("{prefix}{folder}");
                            children.insert(
                                folder.clone(),
                                json!({".tag":"folder","path_lower":folder}),
                            );
                        }
                        None => {
                            children.entry(path.to_owned()).or_insert(entry);
                        }
                    }
                }
                all = children.into_values().collect();
            }
            if uri.path() == "/2/files/list_folder" && all.is_empty() {
                return error("path/not_found/...");
            }
            let entries: Vec<_> = all.iter().skip(start).take(1).collect();
            reply(
                json!({"entries":entries,"has_more":all.len()>start+1,"cursor":(start+1).to_string()}),
            )
        }
        "/2/files/delete_v2" => match state.objects.remove(&path) {
            Some(_) => reply(json!({"metadata":{"path_lower":path}})),
            None => error("path_lookup/not_found/..."),
        },
        "/2/files/get_metadata" => match state.objects.get(&path) {
            Some(bytes) => reply(file_metadata(&path, bytes, &state)),
            None => error("path/not_found/..."),
        },
        "/2/files/upload_session/start" => {
            state.next_upload += 1;
            let id = state.next_upload.to_string();
            state.uploads.insert(id.clone(), BufferedUpload::default());
            reply(json!({"session_id":id}))
        }
        "/2/files/upload_session/append_v2" => {
            let id = arg["cursor"]["session_id"].as_str().unwrap();
            if arg["close"] == true {
                state
                    .close_requests
                    .push(arg["cursor"]["offset"].as_u64().unwrap());
            }
            let Some(upload) = state.uploads.get_mut(id) else {
                return response(
                    409,
                    json!({"error_summary":"not_found/...","error":{".tag":"not_found"}})
                        .to_string(),
                );
            };
            if upload.closed {
                return response(
                    409,
                    json!({"error_summary":"closed/...","error":{".tag":"closed"}}).to_string(),
                );
            }
            if arg["cursor"]["offset"].as_u64() != Some(upload.bytes.len() as u64) {
                return response(409,json!({"error_summary":"incorrect_offset/...","error":{".tag":"incorrect_offset","correct_offset":upload.bytes.len()}}).to_string());
            }
            upload.bytes.extend_from_slice(&body);
            if arg["close"] == true {
                upload.closed = true;
            }
            if std::mem::replace(&mut state.fail_reply, false) {
                return response(503, "{}");
            }
            reply(Value::Null)
        }
        "/2/files/upload_session/finish" => {
            let path = arg["commit"]["path"].as_str().unwrap().to_owned();
            if state.objects.contains_key(&path) {
                return error("path/conflict/file/...");
            }
            let id = arg["cursor"]["session_id"].as_str().unwrap();
            let Some(upload) = state.uploads.get(id) else {
                return response(
                    409,
                    json!({"error":{".tag":"lookup_failed","lookup_failed":{".tag":"not_found"}}})
                        .to_string(),
                );
            };
            assert_eq!(
                arg["cursor"]["offset"].as_u64(),
                Some(upload.bytes.len() as u64)
            );
            let bytes = state.uploads.remove(id).unwrap().bytes;
            let size = bytes.len();
            state.objects.insert(path.clone(), bytes);
            if std::mem::replace(&mut state.fail_reply, false) {
                return response(503, "{}");
            }
            reply(json!({"path_lower":path,"size":size,"id":"id:file"}))
        }
        "/2/sharing/list_folder_members" | "/2/sharing/list_folder_members/continue" => {
            assert!(arg.get("include_inherited").is_none());
            let (inherited, start) = if let Some(cursor) = arg["cursor"].as_str() {
                let (kind, offset) = cursor.split_once(':').unwrap();
                (kind == "parent", offset.parse::<usize>().unwrap())
            } else {
                (arg.get("path").is_some(), 0)
            };
            if arg.get("path").is_some() {
                assert_eq!(arg["path"], "ns:namespace");
            }
            let members = if inherited {
                &state.inherited_members
            } else {
                &state.members
            };
            let all: Vec<_> = members.iter().map(|(email, role)| (
                "users", json!({"user":{"email":email,"account_id":format!("dbid:{email}")},"access_type":{".tag":role},"is_inherited":state.inherited_members.contains_key(email)})
            )).chain(state.invitees.iter().filter(|_| !inherited).cloned().map(|m| ("invitees",m)))
                .chain(state.groups.iter().filter(|_| !inherited).cloned().map(|m| ("groups",m))).collect();
            let mut value = json!({"users":[],"invitees":[],"groups":[]});
            if let Some((kind, member)) = all.get(start) {
                value[*kind] = json!([member]);
            }
            if start + 1 < all.len() {
                value["cursor"] = json!(format!(
                    "{}:{}",
                    if inherited { "parent" } else { "direct" },
                    start + 1
                ));
            }
            reply(value)
        }
        "/2/sharing/add_folder_member" => {
            state.sharing_mutations.push("add".into());
            if state.refuse_share {
                return response(403, r#"{"error_summary":"access_denied/..."}"#);
            }
            state.members.insert(
                arg["members"][0]["member"]["email"]
                    .as_str()
                    .unwrap()
                    .into(),
                arg["members"][0]["access_level"][".tag"]
                    .as_str()
                    .unwrap()
                    .into(),
            );
            reply(Value::Null)
        }
        "/2/sharing/update_folder_member" => {
            state.sharing_mutations.push("update".into());
            if state.refuse_share {
                return response(403, r#"{"error_summary":"access_denied/..."}"#);
            }
            assert_eq!(arg["shared_folder_id"], "namespace");
            let role = arg["access_level"][".tag"].as_str().unwrap();
            assert_eq!(arg["member"][".tag"], "dropbox_id");
            let email = arg["member"]["dropbox_id"]
                .as_str()
                .unwrap()
                .strip_prefix("dbid:")
                .unwrap();
            *state.members.get_mut(email).unwrap() = role.into();
            if std::mem::replace(&mut state.lose_update_reply, false) {
                return response(503, "{}");
            }
            reply(json!({"access_level":{".tag":role}}))
        }
        "/2/sharing/remove_folder_member" => {
            state.sharing_mutations.push("remove".into());
            let email = match arg["member"][".tag"].as_str().unwrap() {
                "email" => arg["member"]["email"].as_str().unwrap(),
                "dropbox_id" => arg["member"]["dropbox_id"]
                    .as_str()
                    .unwrap()
                    .strip_prefix("dbid:")
                    .unwrap(),
                other => panic!("unexpected selector {other}"),
            };
            if state.members.get(email).is_some_and(|role| role == "owner") {
                return response(409, json!({"error":{".tag":"folder_owner"}}).to_string());
            }
            if let Some(job) = &state.removal_job {
                assert_eq!(email, job.member);
            } else {
                let complete = match state.inherited_members.get(email) {
                    Some(role) => {
                        json!({".tag":"complete","access_level":{".tag":role},"access_details":[{"shared_folder_id":"parent-folder","folder_name":"Parent","path":"/parent","permissions":[]}]})
                    }
                    None => json!({".tag":"complete"}),
                };
                state.removal_job = Some(RemovalJob {
                    member: email.into(),
                    statuses: [complete].into(),
                });
            }
            if std::mem::replace(&mut state.lose_remove_reply, false) {
                state.members.remove(email);
                return response(503, "{}");
            }
            reply(json!({".tag":"async_job_id","async_job_id":"removal"}))
        }
        "/2/sharing/check_remove_member_job_status" => {
            assert_eq!(arg["async_job_id"], "removal");
            let job = state.removal_job.as_mut().unwrap();
            let status = if job.statuses.len() > 1 {
                job.statuses.pop_front().unwrap()
            } else {
                job.statuses.front().unwrap().clone()
            };
            let member = job.member.clone();
            if status[".tag"] == "complete" {
                state.members.remove(&member);
                state
                    .invitees
                    .retain(|m| m["invitee"]["email"] != member && m["user"]["email"] != member);
            }
            reply(status)
        }
        other => panic!("unexpected Dropbox request {other}"),
    }
}

#[tokio::test]
async fn lost_completion_requires_byte_verification() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::file(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        coven_foundation::id_source::FileId(uuid::Uuid::from_bytes([0xcc; 16])),
    );
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    let recorded = upload.encode().unwrap();
    state.lock().unwrap().fail_reply = true;
    assert!(storage.finish_upload(&mut upload).await.is_err());
    let storage = provider(&server.url);
    let mut upload = UploadSession::decode(recorded.as_bytes()).unwrap();
    storage.resume_upload(&mut upload).await.unwrap();
    assert!(!upload.is_complete());
    assert_eq!(upload.confirmed_bytes(), 0);
    assert!(matches!(
        storage.upload_part(&mut upload, b"else").await,
        Err(error) if error.failure() == StorageFailure::AlreadyExists
    ));
    assert_eq!(upload.confirmed_bytes(), 0);
    let mut upload = UploadSession::decode(upload.encode().unwrap().as_bytes()).unwrap();
    storage.resume_upload(&mut upload).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    storage.finish_upload(&mut upload).await.unwrap();
    assert!(upload.is_complete());
    assert!(state.lock().unwrap().uploads.is_empty());
}
pub(super) fn provider(url: &str) -> StorageConnection<DropboxStorage> {
    let mut storage = DropboxStorage::new(
        StorageConfig::Dropbox {
            location: crate::DropboxLocation::SharedFolder {
                namespace_id: "namespace".into(),
            },
        },
        session(PROVIDER),
    )
    .unwrap();
    storage.api = format!("{url}/2");
    storage.content = storage.api.clone();
    storage.removal_poll_unit = std::time::Duration::from_millis(1);
    StorageConnection::from_provider(Arc::new(storage))
}
#[tokio::test]
async fn missing_session_and_destination_is_expired() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let error = crate::test_utils::Conformance::new(Arc::new(storage), STORE_ID)
        .expired_session(async {
            state.lock().unwrap().uploads.clear();
        })
        .await;
    assert!(std::error::Error::source(&error).is_some());
}
#[tokio::test]
async fn conformance_ranges_pagination_and_account_sharing() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = Arc::new(provider(&server.url));
    crate::test_utils::Conformance::new(storage.clone(), STORE_ID)
        .run()
        .await
        .unwrap();
    assert_eq!(state.lock().unwrap().range_reads, 6);
    assert!(matches!(
        storage.grant_access("member@example.com").await.unwrap(),
        AccessGrant::Granted { .. }
    ));
    storage.grant_access("member@example.com").await.unwrap();
    storage.revoke_access("member@example.com").await.unwrap();
    storage.revoke_access("member@example.com").await.unwrap();
    assert!(state.lock().unwrap().members.is_empty());
}
#[tokio::test]
async fn resume_queries_the_stored_offset_after_lost_part_reply() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    crate::test_utils::Conformance::new(Arc::new(storage), STORE_ID)
        .lost_part_reply(8 * 1024 * 1024, async {
            state.lock().unwrap().fail_reply = true;
        })
        .await;
}

#[tokio::test]
async fn refreshed_tokens_reach_the_same_adapter() {
    crate::providers::tests::assert_token_refresh(
        |url| Arc::new(provider(url)),
        json!({"entries":[],"has_more":false}),
    )
    .await;
}

#[tokio::test]
async fn abort_retries_a_lost_close_and_accepts_an_expired_session() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    let upload = storage.begin_upload(&path, 4).await.unwrap();
    state.lock().unwrap().fail_reply = true;
    assert_eq!(
        storage.abort_upload(&upload).await.unwrap_err().failure(),
        StorageFailure::Network
    );
    assert!(state.lock().unwrap().uploads[storage.provider.id(&upload).unwrap()].closed);
    storage.abort_upload(&upload).await.unwrap();
    state.lock().unwrap().uploads.clear();
    storage.abort_upload(&upload).await.unwrap();
    assert!(state.lock().unwrap().objects.is_empty());
}

#[tokio::test]
async fn abort_uses_the_remote_offset_after_a_lost_part_reply() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    state.lock().unwrap().fail_reply = true;
    assert_eq!(
        storage
            .upload_part(&mut upload, b"data")
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Network
    );
    assert_eq!(upload.confirmed_bytes(), 0);
    storage.abort_upload(&upload).await.unwrap();
    assert_eq!(state.lock().unwrap().close_requests, [0, 4]);
    assert!(state.lock().unwrap().uploads[storage.provider.id(&upload).unwrap()].closed);
    storage.abort_upload(&upload).await.unwrap();
    assert!(state.lock().unwrap().objects.is_empty());
}

#[tokio::test]
async fn abort_does_not_remove_an_upload_published_before_a_lost_reply() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    state.lock().unwrap().fail_reply = true;
    assert!(storage.finish_upload(&mut upload).await.is_err());
    storage.abort_upload(&upload).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
}

#[tokio::test]
async fn create_uploads_an_oversized_write_in_parts() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let mut storage = provider(&server.url);
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    assert_eq!(storage.single_request_limit(), 150 * 1024 * 1024);
    Arc::get_mut(&mut storage.provider)
        .unwrap()
        .single_request_limit = 4;
    let mut bytes = vec![0x7b; storage.single_request_limit() as usize];
    storage.create(&path, &bytes).await.unwrap();
    assert!(state.lock().unwrap().uploads.is_empty());
    assert_eq!(state.lock().unwrap().next_upload, 0);
    storage.delete(&path).await.unwrap();
    bytes.push(8);
    storage.create(&path, &bytes).await.unwrap();
    assert_eq!(state.lock().unwrap().objects[&path.absolute()], bytes);
    assert!(state.lock().unwrap().uploads.is_empty());
    assert_eq!(state.lock().unwrap().next_upload, 1);
}

#[tokio::test]
async fn listing_retains_server_time_and_size_across_pages_and_retries() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    let storage = provider(&server.url);
    let time = crate::providers::http::timestamp(
        &serde_json::json!({"time":"2026-10-06T00:00:00Z"}),
        "time",
    )
    .unwrap();
    crate::test_utils::Conformance::new(Arc::new(storage), STORE_ID)
        .listing_times(time, async {})
        .await;
}

#[path = "dropbox_access_tests.rs"]
mod access_tests;

#[tokio::test]
async fn recipient_join_mounts_the_invited_namespace_and_retries_a_lost_reply() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let owner = provider(&server.url);
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    owner.create(&path, b"first").await.unwrap();
    let AccessGrant::Granted { invitation } = owner.grant_access("member").await.unwrap() else {
        panic!()
    };
    let invitation = StorageInvitation::decode(invitation.encode().unwrap().as_bytes()).unwrap();
    let recipient = provider(&server.url);
    recipient
        .set_oauth_tokens(OAuthTokens {
            client_id: "dropbox".into(),
            access: crate::OAuthAccess::Dropbox(crate::DropboxAccess::FullDropbox),
            access_token: SecretText::new("recipient:member".into()),
            refresh_token: None,
        })
        .await
        .unwrap();
    assert_eq!(
        recipient.read(&path).await.unwrap_err().failure(),
        StorageFailure::PermissionDenied
    );
    let elsewhere = StorageInvitation::for_account(StorageConfig::Dropbox {
        location: crate::DropboxLocation::SharedFolder {
            namespace_id: "elsewhere".into(),
        },
    })
    .unwrap();
    assert!(matches!(
        recipient.join(STORE_ID, &elsewhere).await,
        Err(error) if error.failure() == StorageFailure::InvitationMismatch
    ));
    assert_eq!(state.lock().unwrap().mount_requests, 0);
    state.lock().unwrap().lose_mount_reply = true;
    assert_eq!(
        recipient
            .join(STORE_ID, &invitation)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Network
    );
    recipient.join(STORE_ID, &invitation).await.unwrap();
    assert_eq!(recipient.read(&path).await.unwrap(), b"first");
    assert_eq!(state.lock().unwrap().mount_requests, 2);
    owner.revoke_access("member").await.unwrap();
    for error in [
        recipient.read(&path).await.unwrap_err(),
        recipient.join(STORE_ID, &invitation).await.unwrap_err(),
    ] {
        assert_eq!(error.failure(), StorageFailure::PermissionDenied);
        let StorageError::Provider { source, .. } = error else {
            panic!()
        };
        assert!(source.downcast_ref::<http::ProviderResponse>().is_some());
    }
}

#[tokio::test]
async fn permission_failures_reach_every_object_and_upload_caller() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let denied = Arc::new(AtomicBool::new(false));
    let failures = denied.clone();
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(
        move |uri: Uri, headers: HeaderMap, body: Bytes| {
            let denied = failures.clone();
            let state = state.clone();
            async move {
                if denied.load(Ordering::SeqCst) {
                    response(403, "native-refusal")
                } else {
                    endpoint(State(state), uri, headers, body).await
                }
            }
        },
    ))
    .await;
    let errors = crate::test_utils::Conformance::new(Arc::new(provider(&server.url)), STORE_ID)
        .permission_failures(async {
            denied.store(true, Ordering::SeqCst);
        })
        .await;
    assert!(errors
        .iter()
        .all(|error| matches!(error, StorageError::Provider { .. })));
}

#[tokio::test]
async fn missing_namespace_and_revoked_membership_keep_the_native_cause() {
    for (tag, expected) in [
        ("invalid_root", StorageFailure::ContainerNotFound),
        ("invalid_namespace_id", StorageFailure::ContainerNotFound),
        ("not_a_member", StorageFailure::PermissionDenied),
    ] {
        let body = json!({"error":{".tag":tag}}).to_string();
        let source = body.clone();
        let server = TestServer::new(Router::new().fallback(move || {
            let body = body.clone();
            async move { response(409, body) }
        }))
        .await;
        let storage = provider(&server.url);
        let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
        for error in [
            storage
                .list(&ObjectPrefix::all(STORE_ID))
                .await
                .err()
                .unwrap(),
            storage.read(&path).await.err().unwrap(),
            storage.delete(&path).await.err().unwrap(),
            storage.create(&path, b"first").await.err().unwrap(),
            storage.grant_access("member").await.err().unwrap(),
        ] {
            assert_eq!(error.failure(), expected);
            let StorageError::Provider { source: native, .. } = error else {
                panic!("native error lost")
            };
            assert_eq!(
                native
                    .downcast_ref::<crate::providers::ProviderResponse>()
                    .unwrap()
                    .body(),
                source.as_bytes()
            );
        }
    }
}

#[tokio::test]
async fn listings_refuse_missing_paging_state_and_empty_or_repeated_cursors() {
    for page in [
        json!({"entries":[]}),
        json!({"entries":[],"has_more":true,"cursor":""}),
        json!({"entries":[],"has_more":true,"cursor":"repeat"}),
    ] {
        let server = TestServer::new(Router::new().fallback(move || {
            let page = page.clone();
            async move { reply(page) }
        }))
        .await;
        assert_eq!(
            provider(&server.url)
                .list(&ObjectPrefix::all(STORE_ID))
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::Protocol
        );
    }
}

#[tokio::test]
async fn stale_abort_cannot_close_another_native_session() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let first = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    let second = ObjectPath::slot(STORE_ID, 2.try_into().unwrap());
    let mut old = storage.begin_upload(&first, 4).await.unwrap();
    let mut next = storage.begin_upload(&second, 4).await.unwrap();
    assert_ne!(
        storage.provider.id(&old).unwrap(),
        storage.provider.id(&next).unwrap()
    );
    storage.upload_part(&mut old, b"old!").await.unwrap();
    storage.upload_part(&mut next, b"next").await.unwrap();
    storage.abort_upload(&old).await.unwrap();
    storage.abort_upload(&old).await.unwrap();
    storage.finish_upload(&mut next).await.unwrap();
    storage.abort_upload(&old).await.unwrap();
    assert_eq!(storage.read(&second).await.unwrap(), b"next");
    assert_eq!(
        storage.read(&first).await.unwrap_err().failure(),
        StorageFailure::NotFound
    );
}
#[tokio::test]
async fn throttling_is_one_request_then_a_cooldown_for_the_whole_connection() {
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = hits.clone();
    let body = r#"{"error_summary":"too_many_requests/..","error":{"retry_after":600}}"#;
    let server = TestServer::new(Router::new().fallback(move || {
        counted.fetch_add(1, SeqCst);
        async move { response(429, body) }
    }))
    .await;
    let cooldown = Arc::new(crate::cooldown::tests::RecordingCooldown::default());
    let publications = Arc::new(crate::test_utils::UnrestrictedPublications);
    let storage = StorageConnection::with_gate(
        provider(&server.url).provider,
        publications,
        cooldown.clone(),
    );
    let prefix = crate::ObjectPrefix::all(STORE_ID);
    let rate_limited = |error: StorageError| error.failure() == StorageFailure::RateLimited;
    assert!(rate_limited(storage.list(&prefix).await.unwrap_err()));
    // The HTTP client retries nothing; the cooldown then sends nothing.
    assert_eq!(
        *cooldown.recorded.lock().unwrap(),
        [std::time::Duration::from_secs(600)]
    );
    assert!(rate_limited(storage.list(&prefix).await.unwrap_err()));
    assert_eq!(hits.load(SeqCst), 1);
}

mod sequence {
    use super::*;

    #[tokio::test]
    async fn floor_replacement_conflicts_and_lost_reply_preserve_the_accepted_revision() {
        let state = Arc::new(Mutex::new(Remote::default()));
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = provider(&server.url);
        let path = ObjectPath::floor(crate::test_utils::STORE_ID);
        storage.create(&path, b"floor one").await.unwrap();
        let first = storage.status(&path).await.unwrap().unwrap();
        storage.replace(&first, b"floor two").await.unwrap();
        assert_eq!(
            storage
                .replace(&first, b"stale floor")
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::Conflict
        );
        assert_eq!(storage.read(&path).await.unwrap(), b"floor two");
        let second = storage.status(&path).await.unwrap().unwrap();
        state.lock().unwrap().fail_reply = true;
        assert_eq!(
            storage
                .replace(&second, b"floor three")
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::Network
        );
        assert_eq!(
            storage
                .replace(&second, b"stale retry")
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::Conflict
        );
        let third = storage.read_object(&path, None).await.unwrap();
        assert_eq!(third.bytes, b"floor three");
        assert_ne!(second.revision, third.metadata.revision);
        assert_ne!(second.replace_token, third.metadata.replace_token);
    }

    #[tokio::test]
    async fn slot_create_once_refuses_equal_and_different_bytes_without_replacement() {
        let state = Arc::new(Mutex::new(Remote::default()));
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = provider(&server.url);
        let path = ObjectPath::slot(crate::test_utils::STORE_ID, std::num::NonZeroU64::MIN);
        storage.create(&path, b"claim one").await.unwrap();
        let first = storage.status(&path).await.unwrap().unwrap();
        for bytes in [b"claim one", b"claim two"] {
            assert_eq!(
                storage.create(&path, bytes).await.unwrap_err().failure(),
                StorageFailure::AlreadyExists
            );
            assert_eq!(storage.read(&path).await.unwrap(), b"claim one");
            assert_eq!(storage.status(&path).await.unwrap().unwrap(), first);
        }
    }
}
