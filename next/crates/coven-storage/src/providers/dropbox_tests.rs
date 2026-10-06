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
#[derive(Default)]
struct Remote {
    objects: BTreeMap<String, Vec<u8>>,
    upload: Vec<u8>,
    fail_reply: bool,
    closed: bool,
    missing: bool,
    close_requests: Vec<u64>,
    members: BTreeSet<String>,
    range_reads: usize,
}
async fn endpoint(
    State(state): State<Arc<Mutex<Remote>>>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    assert_eq!(headers["authorization"], "Bearer token");
    assert_eq!(
        headers["dropbox-api-path-root"],
        r#"{".tag":"namespace_id","namespace_id":"namespace"}"#
    );
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
    match uri.path() {
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
            let all: Vec<_> = state.objects.iter().collect();
            let entries: Vec<_> = all
                .iter()
                .skip(start)
                .take(1)
                .map(|(path, bytes)| json!({".tag":"file","path_lower":path,"size":bytes.len()}))
                .collect();
            reply(
                json!({"entries":entries,"has_more":all.len()>start+1,"cursor":(start+1).to_string()}),
            )
        }
        "/2/files/delete_v2" => match state.objects.remove(&path) {
            Some(_) => reply(json!({"metadata":{"path_lower":path}})),
            None => error("path_lookup/not_found/..."),
        },
        "/2/files/get_metadata" => match state.objects.get(&path) {
            Some(bytes) => reply(json!({"path_lower":path,"size":bytes.len()})),
            None => error("path/not_found/..."),
        },
        "/2/files/upload_session/start" => {
            state.upload.clear();
            state.closed = false;
            reply(json!({"session_id":"session"}))
        }
        "/2/files/upload_session/append_v2" => {
            assert_eq!(arg["cursor"]["session_id"], "session");
            if arg["close"] == true {
                state
                    .close_requests
                    .push(arg["cursor"]["offset"].as_u64().unwrap());
            }
            if state.missing {
                return response(
                    409,
                    json!({"error_summary":"not_found/...","error":{".tag":"not_found"}})
                        .to_string(),
                );
            }
            if state.closed {
                return response(
                    409,
                    json!({"error_summary":"closed/...","error":{".tag":"closed"}}).to_string(),
                );
            }
            if arg["cursor"]["offset"].as_u64() != Some(state.upload.len() as u64) {
                return response(409,json!({"error_summary":"incorrect_offset/...","error":{".tag":"incorrect_offset","correct_offset":state.upload.len()}}).to_string());
            }
            state.upload.extend_from_slice(&body);
            if arg["close"] == true {
                state.closed = true;
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
            let bytes = state.upload.clone();
            let size = bytes.len();
            state.objects.insert(path.clone(), bytes);
            state.closed = true;
            if std::mem::replace(&mut state.fail_reply, false) {
                return response(503, "{}");
            }
            reply(json!({"path_lower":path,"size":size,"id":"id:file"}))
        }
        "/2/sharing/list_folder_members" => reply(
            json!({"users":state.members.iter().map(|email|json!({"user":{"email":email},"access_type":{".tag":"editor"}})).collect::<Vec<_>>(),"invitees":[]}),
        ),
        "/2/sharing/add_folder_member" => {
            state.members.insert(
                arg["members"][0]["member"]["email"]
                    .as_str()
                    .unwrap()
                    .into(),
            );
            reply(Value::Null)
        }
        "/2/sharing/remove_folder_member" => {
            state
                .members
                .remove(arg["member"]["email"].as_str().unwrap());
            reply(json!({".tag":"complete"}))
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
    assert_eq!(state.lock().unwrap().upload, b"data");
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
    state.lock().unwrap().closed = true;
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
    assert_eq!(state.lock().unwrap().range_reads, 5);
    assert!(matches!(
        storage.grant_access("member@example.com").await.unwrap(),
        AccessGrant::Granted
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
    assert!(state.lock().unwrap().closed);
    storage.abort_upload(&upload).await.unwrap();
    state.lock().unwrap().missing = true;
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
    assert!(state.lock().unwrap().closed);
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
    assert!(state.lock().unwrap().upload.is_empty());
    storage.delete(&path).await.unwrap();
    bytes.push(8);
    storage.create(&path, &bytes).await.unwrap();
    assert_eq!(state.lock().unwrap().objects[&path.absolute()], bytes);
    assert!(state.lock().unwrap().closed);
}
