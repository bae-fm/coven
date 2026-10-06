use super::*;
use crate::providers::tests::{json as reply, query, read, response, session, TestServer};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, Response, Uri},
    Router,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
#[derive(Default)]
struct Remote {
    next: u64,
    files: BTreeMap<String, (Value, Vec<u8>)>,
    uploads: BTreeMap<String, (Value, Vec<u8>, usize)>,
    fail_reply: bool,
    partial: Option<usize>,
    permissions: BTreeMap<String, String>,
    lose_delete_reply: bool,
}
fn metadata(mut value: Value, size: usize) -> Value {
    value["size"] = json!(size.to_string());
    value["capabilities"] = json!({"canDelete":true,"canRemoveMyDriveParent":true});
    value["ownedByMe"] = json!(true);
    value["createdTime"] = json!("2026-10-06T00:00:00Z");
    value
}
async fn endpoint(
    State(state): State<Arc<Mutex<Remote>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    respond(&state, &method, &uri, &headers, &body)
}

fn respond(
    state: &Mutex<Remote>,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &Bytes,
) -> Response<Body> {
    assert_eq!(headers["authorization"], "Bearer token");
    let mut state = state.lock().unwrap();
    let q = query(uri);
    let parts: Vec<_> = uri.path().trim_start_matches('/').split('/').collect();
    let value: Value = if body.is_empty() {
        Value::Null
    } else if headers
        .get("content-type")
        .is_some_and(|v| v.to_str().unwrap().starts_with("application/json"))
    {
        serde_json::from_slice(body).unwrap()
    } else {
        Value::Null
    };
    if parts == ["drive", "files", "generateIds"] {
        state.next += 1;
        return reply(json!({"ids":[format!("id{}",state.next)]}));
    }
    if parts == ["upload", "files"] {
        assert_eq!(method, Method::POST);
        let id = value["id"].as_str().unwrap().to_owned();
        let total = headers["x-upload-content-length"]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        state.uploads.insert(id.clone(), (value, Vec::new(), total));
        return Response::builder()
            .header(
                "location",
                format!("http://{}/session/{id}", headers["host"].to_str().unwrap()),
            )
            .body(Body::empty())
            .unwrap();
    }
    if parts.first() == Some(&"session") {
        let id = parts[1].to_owned();
        if method == Method::DELETE {
            state.uploads.remove(&id);
            return response(204, Vec::new());
        }
        if let Some((metadata, _)) = state.files.get(&id) {
            return reply(metadata.clone());
        }
        let range = headers["content-range"].to_str().unwrap();
        let partial = state.partial.take();
        let Some((metadata, bytes, total)) = state.uploads.get_mut(&id) else {
            return response(404, "{}");
        };
        if !range.starts_with("bytes */") {
            let start: usize = range
                .strip_prefix("bytes ")
                .unwrap()
                .split('-')
                .next()
                .unwrap()
                .parse()
                .unwrap();
            assert_eq!(start, bytes.len());
            bytes.extend_from_slice(&body[..partial.unwrap_or(body.len())]);
        }
        let size = bytes.len();
        let complete = size == *total;
        if complete {
            let (value, bytes, _) = state.uploads.remove(&id).unwrap();
            let value = super::tests::metadata(value, bytes.len());
            state.files.insert(id, (value.clone(), bytes));
            return reply(value);
        }
        let _ = metadata;
        if !range.starts_with("bytes */") && std::mem::replace(&mut state.fail_reply, false) {
            return response(503, "{}");
        }
        let mut response = Response::builder().status(308);
        if size > 0 {
            response = response.header("range", format!("bytes=0-{}", size - 1));
        }
        return response.body(Body::empty()).unwrap();
    }
    if parts.len() >= 4 && parts[2] == "folder" && parts[3] == "permissions" {
        return match *method {
            Method::GET => reply(
                json!({"permissions":state.permissions.iter().map(|(email,role)|json!({"id":email,"type":"user","emailAddress":email,"role":role})).collect::<Vec<_>>()}),
            ),
            Method::POST => {
                state.permissions.insert(
                    value["emailAddress"].as_str().unwrap().into(),
                    value["role"].as_str().unwrap().into(),
                );
                reply(json!({"id":"permission"}))
            }
            Method::PATCH => {
                state
                    .permissions
                    .insert(parts[4].into(), value["role"].as_str().unwrap().into());
                reply(json!({}))
            }
            Method::DELETE => {
                state.permissions.remove(parts[4]);
                response(204, Vec::new())
            }
            _ => panic!(),
        };
    }
    if parts == ["drive", "files"] {
        if method == Method::POST {
            state.next += 1;
            let id = format!("id{}", state.next);
            let mut value = value;
            value["id"] = json!(id);
            let value = metadata(value, 0);
            state.files.insert(id, (value.clone(), Vec::new()));
            return reply(value);
        }
        let filter = q["q"]
            .split("name = '")
            .nth(1)
            .map(|s| s.split('\'').next().unwrap());
        let files: Vec<_> = state
            .files
            .values()
            .filter(|(v, _)| {
                v["parents"].as_array().unwrap().contains(&json!("folder"))
                    && filter.is_none_or(|name| v["name"] == name)
            })
            .map(|(v, _)| v.clone())
            .collect();
        let start = q
            .get("pageToken")
            .map(|s| s.parse::<usize>().unwrap())
            .unwrap_or(0);
        let mut page = json!({"files":files.iter().skip(start).take(1).collect::<Vec<_>>()});
        if files.len() > start + 1 {
            page["nextPageToken"] = json!((start + 1).to_string());
        }
        return reply(page);
    }
    if parts.len() == 3 && parts[1] == "files" {
        let id = parts[2];
        if method == Method::DELETE {
            state.files.remove(id);
            if std::mem::replace(&mut state.lose_delete_reply, false) {
                return response(503, "{}");
            }
            return response(204, Vec::new());
        }
        let Some((value, bytes)) = state.files.get_mut(id) else {
            return response(404, "{}");
        };
        if method == Method::PATCH {
            if q.contains_key("removeParents") {
                value["parents"] = json!([]);
            } else {
                *bytes = body.to_vec();
                value["size"] = json!(bytes.len().to_string());
            }
            return reply(value.clone());
        }
        if q.get("alt").map(String::as_str) == Some("media") {
            return read(bytes, headers);
        }
        return reply(value.clone());
    }
    panic!("unexpected Drive request {method} {uri}")
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
}
fn provider(url: &str) -> GoogleDriveStorage {
    let mut storage = GoogleDriveStorage::new(
        StorageConfig::GoogleDrive {
            folder_id: "folder".into(),
        },
        DeviceId(31),
        session(PROVIDER),
    )
    .unwrap();
    storage.api = format!("{url}/drive");
    storage.upload_api = format!("{url}/upload");
    storage
}
#[tokio::test]
async fn conformance_and_account_sharing() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = Arc::new(provider(&server.url));
    crate::test_utils::Conformance::new(storage.clone())
        .run()
        .await
        .unwrap();
    storage.grant_access("member").await.unwrap();
    storage
        .revoke_access(&MemberAccess::ProviderAccount("member".into()))
        .await
        .unwrap();
    assert!(state.lock().unwrap().permissions.is_empty());
}
#[tokio::test]
async fn session_reopens_and_only_uploader_deletes() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::file(coven_foundation::id_source::FileId(uuid::Uuid::from_bytes(
        [0xcc; 16],
    )));
    let mut upload = storage
        .begin_upload(&path, 8 * 1024 * 1024 + 1)
        .await
        .unwrap();
    let recorded = upload.encode().unwrap();
    state.lock().unwrap().fail_reply = true;
    assert!(storage
        .upload_part(&mut upload, &vec![1; 8 * 1024 * 1024])
        .await
        .is_err());
    drop(upload);
    let storage = provider(&server.url);
    let mut upload = UploadSession::decode(recorded.as_bytes()).unwrap();
    storage.resume_upload(&mut upload).await.unwrap();
    assert_eq!(upload.confirmed, 8 * 1024 * 1024);
    let before_final = upload.encode().unwrap();
    storage.upload_part(&mut upload, b"z").await.unwrap();
    let mut lost_reply = UploadSession::decode(before_final.as_bytes()).unwrap();
    storage.resume_upload(&mut lost_reply).await.unwrap();
    assert!(lost_reply.is_complete());
    let id = state.lock().unwrap().files.keys().next().unwrap().clone();
    state.lock().unwrap().files.get_mut(&id).unwrap().0["ownedByMe"] = json!(false);
    storage.delete(&path).await.unwrap();
    assert!(storage.list(&ObjectPrefix::all()).await.unwrap().is_empty());
    assert_eq!(
        state.lock().unwrap().files[&id].1.len(),
        8 * 1024 * 1024 + 1
    );
}
#[tokio::test]
async fn two_uploads_cannot_publish_different_bytes_at_one_path() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    let first = provider(&server.url);
    let second = provider(&server.url);
    let path = ObjectPath::file(coven_foundation::id_source::FileId(uuid::Uuid::from_bytes(
        [0xdd; 16],
    )));
    let mut a = first.begin_upload(&path, 1).await.unwrap();
    let mut b = second.begin_upload(&path, 1).await.unwrap();
    first.upload_part(&mut a, b"a").await.unwrap();
    assert!(second.upload_part(&mut b, b"b").await.is_err());
    assert_eq!(first.read(&path).await.unwrap(), b"a");
}

#[tokio::test]
async fn interrupted_part_continues_at_the_confirmed_byte() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::file(coven_foundation::id_source::FileId(uuid::Uuid::from_bytes(
        [0xee; 16],
    )));
    let total = 8 * 1024 * 1024 + 1;
    let mut upload = storage.begin_upload(&path, total).await.unwrap();
    let recorded = upload.encode().unwrap();
    state.lock().unwrap().partial = Some(4 * 1024 * 1024);
    assert!(storage
        .upload_part(&mut upload, &vec![7; 8 * 1024 * 1024])
        .await
        .is_err());
    let mut upload = UploadSession::decode(recorded.as_bytes()).unwrap();
    storage.resume_upload(&mut upload).await.unwrap();
    assert_eq!(upload.confirmed_bytes(), 4 * 1024 * 1024);
    storage
        .upload_part(&mut upload, &vec![7; 4 * 1024 * 1024])
        .await
        .unwrap();
    storage.upload_part(&mut upload, b"z").await.unwrap();
    storage.finish_upload(&mut upload).await.unwrap();
    assert_eq!(
        storage
            .read_range(&path, ByteRange::new(total - 2, total).unwrap())
            .await
            .unwrap(),
        [7, b'z']
    );
}

#[tokio::test]
async fn refreshed_tokens_reach_the_same_adapter() {
    crate::providers::tests::assert_token_refresh(
        |url| Arc::new(provider(url)),
        json!({"files":[]}),
    )
    .await;
}

fn duplicate(state: &mut Remote, path: &ObjectPath, id: &str, created: &str, device: u64) {
    let mut value = metadata(
        json!({"id":id,"name":path.as_str(),"parents":["folder"],"properties":{"covenDevice":device.to_string()}}),
        4,
    );
    value["createdTime"] = json!(created);
    state.files.insert(id.into(), (value, b"data".to_vec()));
}

#[tokio::test]
async fn retry_keeps_earliest_own_copy_and_leaves_other_devices_copies() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let path = ObjectPath::store_log(DeviceId(31), std::num::NonZeroU64::MIN);
    {
        let mut state = state.lock().unwrap();
        duplicate(&mut state, &path, "z-first", "2026-10-06T00:00:00Z", 31);
        duplicate(&mut state, &path, "a-later", "2026-10-06T00:00:01Z", 31);
        duplicate(
            &mut state,
            &path,
            "other-device",
            "2026-10-06T00:00:02Z",
            32,
        );
    }
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.create_once(&path, b"data").await.unwrap();
    assert_eq!(
        state
            .lock()
            .unwrap()
            .files
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        ["other-device", "z-first"]
    );
    assert!(state.lock().unwrap().uploads.is_empty());
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
}

#[tokio::test]
async fn duplicate_time_ties_use_ids_after_normalizing_timezones() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let path = ObjectPath::store_log(DeviceId(31), std::num::NonZeroU64::MIN);
    {
        let mut state = state.lock().unwrap();
        duplicate(
            &mut state,
            &path,
            "b-first",
            "2026-10-06T01:00:00+01:00",
            31,
        );
        duplicate(&mut state, &path, "a-first", "2026-10-06T00:00:00Z", 31);
        duplicate(&mut state, &path, "0-later", "2026-10-06T00:00:00.001Z", 31);
    }
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.create_once(&path, b"data").await.unwrap();
    assert_eq!(
        state
            .lock()
            .unwrap()
            .files
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        ["a-first"]
    );
}

#[tokio::test]
async fn duplicate_cleanup_failure_is_reported_and_the_retry_finishes() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let path = ObjectPath::store_log(DeviceId(31), std::num::NonZeroU64::MIN);
    {
        let mut state = state.lock().unwrap();
        duplicate(&mut state, &path, "z-first", "2026-10-06T00:00:00Z", 31);
        duplicate(&mut state, &path, "a-later", "2026-10-06T00:00:01Z", 31);
        duplicate(&mut state, &path, "b-later", "2026-10-06T00:00:02Z", 31);
        state.lose_delete_reply = true;
    }
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    assert_eq!(
        storage
            .create_once(&path, b"data")
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Network
    );
    assert!(state.lock().unwrap().files.contains_key("z-first"));
    storage.create_once(&path, b"data").await.unwrap();
    assert_eq!(
        state
            .lock()
            .unwrap()
            .files
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        ["z-first"]
    );
}
