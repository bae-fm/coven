use super::*;
use crate::providers::tests::{json as reply, read, response, session, TestServer};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Response, Uri},
    Router,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
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
struct Remote {
    non_owner: bool,
    mounted: BTreeSet<String>,
    mount_requests: usize,
    lose_mount_reply: bool,
    inherited_members: BTreeMap<String, String>,
    invitees: Vec<Value>,
    groups: Vec<Value>,
    lose_remove_reply: bool,
    removal_job: Option<RemovalJob>,
    objects: BTreeMap<String, Vec<u8>>,
    uploads: BTreeMap<String, BufferedUpload>,
    next_upload: u64,
    fail_reply: bool,
    close_requests: Vec<u64>,
    members: BTreeMap<String, String>,
    sharing_mutations: Vec<String>,
    refuse_share: bool,
    lose_update_reply: bool,
    range_reads: usize,
    folders: BTreeSet<String>,
}
async fn endpoint(
    State(state): State<Arc<Mutex<Remote>>>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let token = headers["authorization"].to_str().unwrap();
    let recipient = token.strip_prefix("Bearer recipient:");
    assert!(token == "Bearer token" || recipient.is_some());
    if uri.path().starts_with("/2/files/") {
        assert_eq!(
            headers["dropbox-api-path-root"],
            r#"{".tag":"namespace_id","namespace_id":"namespace"}"#
        );
    } else {
        assert!(!headers.contains_key("dropbox-api-path-root"));
    }
    let mut state = state.lock().unwrap();
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
    match uri.path() {
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
            assert_eq!(arg["autorename"], false);
            assert_eq!(arg["strict_conflict"], true);
            state.objects.insert(path.clone(), body.to_vec());
            reply(json!({"path_lower":path,"size":body.len(),"id":"id:file"}))
        }
        "/2/files/download" => {
            if headers.contains_key("range") {
                state.range_reads += 1;
            }
            match state.objects.get(&path) {
                Some(bytes) => read(bytes, &headers),
                None => error("path/not_found/..."),
            }
        }
        "/2/files/list_folder" | "/2/files/list_folder/continue" => {
            let start = arg["cursor"]
                .as_str()
                .map(|s| s.parse::<usize>().unwrap())
                .unwrap_or(0);
            let all: Vec<_> = state
                .objects
                .iter()
                .map(|(path, bytes)| json!({".tag":"file","path_lower":path,"size":bytes.len(),"server_modified":"2026-10-06T00:00:00Z","client_modified":"2000-01-01T00:00:00Z"}))
                .chain(
                    state
                        .folders
                        .iter()
                        .map(|path| json!({".tag":"folder","path_lower":path})),
                )
                .collect();
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
            Some(bytes) => reply(
                json!({"path_lower":path,"size":bytes.len(),"server_modified":"2026-10-06T00:00:00Z","client_modified":"2000-01-01T00:00:00Z"}),
            ),
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
                        json!({"access_level":{".tag":role},"access_details":[{"shared_folder_id":"parent-folder","folder_name":"Parent","path":"/parent","permissions":[]}]})
                    }
                    None => json!({}),
                };
                state.removal_job = Some(RemovalJob {
                    member: email.into(),
                    statuses: [json!({".tag":"complete","complete":complete})].into(),
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
    let path = ObjectPath::file(coven_foundation::id_source::FileId(uuid::Uuid::from_bytes(
        [0xcc; 16],
    )));
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
        Err(StorageError::AlreadyExists)
    ));
    assert_eq!(upload.confirmed_bytes(), 0);
    let mut upload = UploadSession::decode(upload.encode().unwrap().as_bytes()).unwrap();
    storage.resume_upload(&mut upload).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    storage.finish_upload(&mut upload).await.unwrap();
    assert!(upload.is_complete());
    assert!(state.lock().unwrap().uploads.is_empty());
}
fn provider(url: &str) -> DropboxStorage {
    let mut storage = DropboxStorage::new(
        StorageConfig::Dropbox {
            namespace_id: "namespace".into(),
        },
        session(PROVIDER),
    )
    .unwrap();
    storage.api = format!("{url}/2");
    storage.content = storage.api.clone();
    storage
}
#[tokio::test]
async fn missing_session_and_destination_is_expired() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::file(coven_foundation::id_source::FileId(uuid::Uuid::from_bytes(
        [0xff; 16],
    )));
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    state.lock().unwrap().uploads.clear();
    assert!(matches!(
        storage.resume_upload(&mut upload).await,
        Err(StorageError::SessionExpired)
    ));
    let replacement = storage.restart_upload(&upload).await.unwrap();
    assert_eq!(replacement.path(), &path);
    assert_eq!(replacement.total_bytes(), b"data".len() as u64);
    assert_eq!(replacement.confirmed_bytes(), 0);
    let recorded = replacement.encode().unwrap();
    let mut replacement = UploadSession::decode(recorded.as_bytes()).unwrap();
    storage.resume_upload(&mut replacement).await.unwrap();
    storage
        .upload_part(&mut replacement, b"data")
        .await
        .unwrap();
    storage.finish_upload(&mut replacement).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
}
#[tokio::test]
async fn conformance_ranges_pagination_and_account_sharing() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = Arc::new(provider(&server.url));
    crate::test_utils::Conformance::new(storage.clone())
        .run()
        .await
        .unwrap();
    assert_eq!(state.lock().unwrap().range_reads, 7);
    assert!(matches!(
        storage.grant_access("member@example.com").await.unwrap(),
        AccessGrant::Granted { .. }
    ));
    storage.grant_access("member@example.com").await.unwrap();
    storage
        .revoke_access(&MemberAccess::ProviderAccount("member@example.com".into()))
        .await
        .unwrap();
    storage
        .revoke_access(&MemberAccess::ProviderAccount("member@example.com".into()))
        .await
        .unwrap();
    assert!(state.lock().unwrap().members.is_empty());
}
#[tokio::test]
async fn resume_queries_the_stored_offset_after_lost_part_reply() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::file(coven_foundation::id_source::FileId(uuid::Uuid::from_bytes(
        [0xbb; 16],
    )));
    let mut upload = storage
        .begin_upload(&path, 8 * 1024 * 1024 + 1)
        .await
        .unwrap();
    let recorded = upload.encode().unwrap();
    state.lock().unwrap().fail_reply = true;
    assert!(storage
        .upload_part(&mut upload, &vec![42; 8 * 1024 * 1024])
        .await
        .is_err());
    drop(upload);
    let storage = provider(&server.url);
    let mut upload = UploadSession::decode(recorded.as_bytes()).unwrap();
    storage.resume_upload(&mut upload).await.unwrap();
    assert_eq!(upload.confirmed, 8 * 1024 * 1024);
    storage.upload_part(&mut upload, b"z").await.unwrap();
    storage.finish_upload(&mut upload).await.unwrap();
    storage.finish_upload(&mut upload).await.unwrap();
    assert_eq!(
        storage
            .read_range(
                &path,
                ByteRange::new(8 * 1024 * 1024, 8 * 1024 * 1024 + 1).unwrap()
            )
            .await
            .unwrap(),
        b"z"
    );
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
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let upload = storage.begin_upload(&path, 4).await.unwrap();
    state.lock().unwrap().fail_reply = true;
    assert_eq!(
        storage.abort_upload(&upload).await.unwrap_err().failure(),
        StorageFailure::Network
    );
    assert!(state.lock().unwrap().uploads[storage.id(&upload).unwrap()].closed);
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
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
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
    assert!(state.lock().unwrap().uploads[storage.id(&upload).unwrap()].closed);
    storage.abort_upload(&upload).await.unwrap();
    assert!(state.lock().unwrap().objects.is_empty());
}

#[tokio::test]
async fn abort_does_not_remove_an_upload_published_before_a_lost_reply() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
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
    let storage = provider(&server.url);
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    assert_eq!(storage.single_request_limit(), 150 * 1024 * 1024);
    let mut bytes = vec![0x7b; storage.single_request_limit() as usize];
    storage.create(&path, &bytes).await.unwrap();
    assert!(state.lock().unwrap().uploads.is_empty());
    storage.delete(&path).await.unwrap();
    bytes.push(8);
    storage.create(&path, &bytes).await.unwrap();
    assert_eq!(state.lock().unwrap().objects[&path.absolute()], bytes);
    assert!(state.lock().unwrap().uploads.is_empty());
}

#[tokio::test]
async fn setup_refuses_unrelated_empty_folders_and_accepts_its_own_parents() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let first = ObjectPath::store_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    for folder in [
        "/vacation",
        "/devices/031",
        "/files/00000000-0000-0000-0000-000000000000",
    ] {
        state.lock().unwrap().folders = [folder.into()].into();
        assert_eq!(
            storage.setup(&first, b"first").await.unwrap_err().failure(),
            StorageSetupFailure::LocationOccupied
        );
        assert!(state.lock().unwrap().objects.is_empty());
    }
    state.lock().unwrap().folders = ["/store-log".into(), "/store-log/31".into()].into();
    storage.setup(&first, b"first").await.unwrap();
    storage.setup(&first, b"first").await.unwrap();
}

#[tokio::test]
async fn listing_retains_server_time_and_size_across_pages_and_retries() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    let storage = provider(&server.url);
    let first = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let second = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(32),
        std::num::NonZeroU64::MIN,
    );
    storage.create_once(&first, b"first").await.unwrap();
    storage.create_once(&second, b"second").await.unwrap();
    let time = crate::providers::http::timestamp(
        &serde_json::json!({"time":"2026-10-06T00:00:00Z"}),
        "time",
    )
    .unwrap();
    let expected = vec![
        StoredObject {
            path: first.clone(),
            size: 5,
            stored_at: time,
        },
        StoredObject {
            path: second,
            size: 6,
            stored_at: time,
        },
    ];
    assert_eq!(
        storage.list(&ObjectPrefix::device_logs()).await.unwrap(),
        expected
    );
    storage.create_once(&first, b"first").await.unwrap();
    assert_eq!(storage.list(&ObjectPrefix::all()).await.unwrap(), expected);
}

#[tokio::test]
async fn asynchronous_removal_keeps_the_native_failure_and_previous_access() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.grant_access("member").await.unwrap();
    for (failed, expected) in [
        (
            json!({".tag":"no_permission"}),
            StorageFailure::PermissionDenied,
        ),
        (
            json!({".tag":"access_error","access_error":{".tag":"not_a_member"}}),
            StorageFailure::PermissionDenied,
        ),
        (json!({".tag":"team_folder"}), StorageFailure::Refused),
    ] {
        let body = json!({".tag":"failed","failed":failed});
        state.lock().unwrap().removal_job = Some(RemovalJob {
            member: "member".into(),
            statuses: [body.clone()].into(),
        });
        let error = storage
            .revoke_access(&MemberAccess::ProviderAccount("member".into()))
            .await
            .err()
            .unwrap();
        assert_eq!(error.failure(), expected);
        let StorageError::Provider { source, .. } = error else {
            panic!("native removal failure discarded")
        };
        let response = source.downcast_ref::<http::ProviderResponse>().unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            serde_json::from_slice::<Value>(response.body()).unwrap(),
            body
        );
        assert!(state.lock().unwrap().members.contains_key("member"));
    }
}

#[tokio::test]
async fn asynchronous_removal_waits_for_publication_and_retries_after_completion() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.grant_access("member").await.unwrap();
    state.lock().unwrap().removal_job = Some(RemovalJob {
        member: "member".into(),
        statuses: [
            json!({".tag":"in_progress"}),
            json!({".tag":"complete", "complete":{}}),
        ]
        .into(),
    });
    storage
        .revoke_access(&MemberAccess::ProviderAccount("member".into()))
        .await
        .unwrap();
    storage
        .revoke_access(&MemberAccess::ProviderAccount("member".into()))
        .await
        .unwrap();
    assert!(!state.lock().unwrap().members.contains_key("member"));
    assert_eq!(state.lock().unwrap().sharing_mutations, ["add", "remove"]);
}

#[tokio::test]
async fn asynchronous_removal_times_out_without_hiding_remaining_access() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.grant_access("member").await.unwrap();
    state.lock().unwrap().removal_job = Some(RemovalJob {
        member: "member".into(),
        statuses: [json!({".tag":"in_progress"})].into(),
    });
    let error = storage
        .revoke_access(&MemberAccess::ProviderAccount("member".into()))
        .await
        .err()
        .unwrap();
    assert_eq!(error.failure(), StorageFailure::Network);
    let StorageError::Provider { source, .. } = error else {
        panic!("timeout cause discarded")
    };
    assert_eq!(
        source.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::TimedOut
    );
    assert!(state.lock().unwrap().members.contains_key("member"));
    state.lock().unwrap().removal_job.as_mut().unwrap().statuses =
        [json!({".tag":"complete", "complete":{}})].into();
    storage
        .revoke_access(&MemberAccess::ProviderAccount("member".into()))
        .await
        .unwrap();
    assert!(!state.lock().unwrap().members.contains_key("member"));
}

#[path = "dropbox_access_tests.rs"]
mod access_tests;

#[tokio::test]
async fn recipient_join_mounts_the_invited_namespace_and_retries_a_lost_reply() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let owner = provider(&server.url);
    let path = ObjectPath::store_log(
        coven_foundation::id_source::DeviceId(1),
        std::num::NonZeroU64::MIN,
    );
    owner.create(&path, b"first").await.unwrap();
    let AccessGrant::Granted { invitation } = owner.grant_access("member").await.unwrap() else {
        panic!()
    };
    let invitation = StorageInvitation::decode(invitation.encode().unwrap().as_bytes()).unwrap();
    let recipient = provider(&server.url);
    recipient
        .set_oauth_tokens(OAuthTokens {
            access_token: SecretText::new("recipient:member".into()),
            refresh_token: None,
            expires_at: None,
        })
        .await
        .unwrap();
    assert_eq!(
        recipient.read(&path).await.unwrap_err().failure(),
        StorageFailure::PermissionDenied
    );
    let elsewhere = StorageInvitation::for_account(StorageConfig::Dropbox {
        namespace_id: "elsewhere".into(),
    })
    .unwrap();
    assert!(matches!(
        recipient.join(&elsewhere).await,
        Err(StorageError::InvitationMismatch)
    ));
    assert_eq!(state.lock().unwrap().mount_requests, 0);
    state.lock().unwrap().lose_mount_reply = true;
    assert_eq!(
        recipient.join(&invitation).await.unwrap_err().failure(),
        StorageFailure::Network
    );
    recipient.join(&invitation).await.unwrap();
    assert_eq!(recipient.read(&path).await.unwrap(), b"first");
    assert_eq!(state.lock().unwrap().mount_requests, 2);
    owner
        .revoke_access(&MemberAccess::ProviderAccount("member".into()))
        .await
        .unwrap();
    for error in [
        recipient.read(&path).await.unwrap_err(),
        recipient.join(&invitation).await.unwrap_err(),
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
    crate::providers::tests::assert_permission_failures(Arc::new(provider(&server.url)), || {
        denied.store(true, Ordering::SeqCst)
    })
    .await;
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
        let path = ObjectPath::store_log(
            coven_foundation::id_source::DeviceId(31),
            std::num::NonZeroU64::MIN,
        );
        for error in [
            storage.list(&ObjectPrefix::all()).await.err().unwrap(),
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
                .list(&ObjectPrefix::all())
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
    let first = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let second = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(32),
        std::num::NonZeroU64::MIN,
    );
    let mut old = storage.begin_upload(&first, 4).await.unwrap();
    let mut next = storage.begin_upload(&second, 4).await.unwrap();
    assert_ne!(storage.id(&old).unwrap(), storage.id(&next).unwrap());
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
