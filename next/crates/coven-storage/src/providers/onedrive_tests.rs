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
    dirs: BTreeMap<String, String>,
    files: BTreeMap<String, Vec<u8>>,
    uploads: BTreeMap<String, (String, Vec<u8>)>,
    next: u64,
    fail_reply: bool,
    members: BTreeSet<String>,
}
fn file(path: &str, bytes: &[u8], host: &str) -> Value {
    json!({"id":format!("file:{path}"),"name":path.rsplit('/').next().unwrap(),"size":bytes.len(),"file":{},"@microsoft.graph.downloadUrl":format!("http://{host}/download/{path}")})
}
async fn endpoint(
    State(state): State<Arc<Mutex<Remote>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let mut state = state.lock().unwrap();
    let q = query(&uri);
    let host = headers["host"].to_str().unwrap();
    if let Some(path) = uri.path().strip_prefix("/download/") {
        assert!(headers.get("authorization").is_none());
        return match state.files.get(path) {
            Some(bytes) => read(bytes, &headers),
            None => response(404, "{}"),
        };
    }
    if let Some(id) = uri.path().strip_prefix("/session/") {
        assert!(headers.get("authorization").is_none());
        if method == Method::DELETE {
            state.uploads.remove(id);
            return response(204, Vec::new());
        }
        let Some((path, bytes)) = state.uploads.get_mut(id) else {
            return response(404, "{}");
        };
        if method == Method::GET {
            return reply(json!({"nextExpectedRanges":[format!("{}-",bytes.len())]}));
        }
        let range = headers["content-range"]
            .to_str()
            .unwrap()
            .strip_prefix("bytes ")
            .unwrap();
        let (bounds, total) = range.split_once('/').unwrap();
        let start: usize = bounds.split('-').next().unwrap().parse().unwrap();
        let total: usize = total.parse().unwrap();
        assert_eq!(start, bytes.len());
        bytes.extend_from_slice(&body);
        let path = path.clone();
        let size = bytes.len();
        if size == total {
            if state.files.contains_key(&path) {
                return response(
                    409,
                    json!({"error":{"code":"nameAlreadyExists"}}).to_string(),
                );
            }
            let (_, bytes) = state.uploads.remove(id).unwrap();
            let value = file(&path, &bytes, host);
            state.files.insert(path, bytes);
            if std::mem::replace(&mut state.fail_reply, false) {
                return response(503, "{}");
            }
            return reply(value);
        }
        if std::mem::replace(&mut state.fail_reply, false) {
            return response(503, "{}");
        }
        return response(
            202,
            json!({"nextExpectedRanges":[format!("{size}-")]}).to_string(),
        );
    }
    assert_eq!(headers["authorization"], "Bearer token");
    let tail = uri
        .path()
        .strip_prefix("/graph/drives/drive/items/")
        .unwrap();
    let value: Value = if body.is_empty()
        || headers
            .get("content-type")
            .is_none_or(|v| !v.to_str().unwrap().starts_with("application/json"))
    {
        Value::Null
    } else {
        serde_json::from_slice(&body).unwrap()
    };
    if let Some((parent, tail)) = tail.split_once(":/") {
        let (relative, suffix) = tail.rsplit_once(':').unwrap();
        let base = state.dirs.get(parent).unwrap();
        let path = if base.is_empty() {
            relative.to_owned()
        } else {
            format!("{base}/{relative}")
        };
        if suffix == "/createUploadSession" {
            assert_eq!(value["item"]["@microsoft.graph.conflictBehavior"], "fail");
            state.next += 1;
            let id = state.next.to_string();
            state.uploads.insert(id.clone(), (path, Vec::new()));
            return reply(json!({"uploadUrl":format!("http://{host}/session/{id}")}));
        }
        if method == Method::DELETE {
            state.files.remove(&path);
            return response(204, Vec::new());
        }
        if method == Method::PUT {
            state.files.insert(path.clone(), body.to_vec());
            return reply(file(&path, &body, host));
        }
        if let Some((id, _)) = state.dirs.iter().find(|(_, p)| *p == &path) {
            return reply(json!({"id":id,"folder":{},"name":relative}));
        }
        return match state.files.get(&path) {
            Some(bytes) => reply(file(&path, bytes, host)),
            None => response(404, json!({"error":{"code":"itemNotFound"}}).to_string()),
        };
    }
    let (id, suffix) = tail.split_once('/').unwrap();
    if suffix == "children" {
        let base = state.dirs.get(id).unwrap().clone();
        if method == Method::POST {
            let name = value["name"].as_str().unwrap();
            let path = if base.is_empty() {
                name.to_owned()
            } else {
                format!("{base}/{name}")
            };
            if state.dirs.values().any(|p| p == &path) {
                return response(
                    409,
                    json!({"error":{"code":"nameAlreadyExists"}}).to_string(),
                );
            }
            state.next += 1;
            let id = format!("dir{}", state.next);
            state.dirs.insert(id.clone(), path);
            return reply(json!({"id":id,"name":name,"folder":{}}));
        }
        let prefix = if base.is_empty() {
            String::new()
        } else {
            format!("{base}/")
        };
        let mut entries = Vec::new();
        for (id, path) in &state.dirs {
            if let Some(name) = path.strip_prefix(&prefix) {
                if !name.is_empty() && !name.contains('/') {
                    entries.push(json!({"id":id,"name":name,"folder":{}}));
                }
            }
        }
        for (path, bytes) in &state.files {
            if let Some(name) = path.strip_prefix(&prefix) {
                if !name.contains('/') {
                    entries.push(file(path, bytes, host));
                }
            }
        }
        let start = q
            .get("page")
            .map(|n| n.parse::<usize>().unwrap())
            .unwrap_or(0);
        let mut result = json!({"value":entries.iter().skip(start).take(1).collect::<Vec<_>>()});
        if entries.len() > start + 1 {
            result["@odata.nextLink"] =
                json!(format!("http://{host}{}?page={}", uri.path(), start + 1));
        }
        return reply(result);
    }
    match suffix {
        "permissions" => reply(
            json!({"value":state.members.iter().map(|email|json!({"id":email,"invitation":{"email":email},"roles":["write"]})).collect::<Vec<_>>()}),
        ),
        "invite" => {
            state
                .members
                .insert(value["recipients"][0]["email"].as_str().unwrap().into());
            reply(json!({"value":[]}))
        }
        _ if suffix.starts_with("permissions/") => {
            state
                .members
                .remove(suffix.strip_prefix("permissions/").unwrap());
            response(204, Vec::new())
        }
        _ => panic!("unexpected OneDrive request {method} {uri}"),
    }
}

#[tokio::test]
async fn lost_completion_requires_byte_verification() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::file(&coven_crypto::StoredFileName::from_bytes([0xff; 32]));
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    let recorded = upload.encode().unwrap();
    state.lock().unwrap().fail_reply = true;
    assert!(storage.upload_part(&mut upload, b"data").await.is_err());
    let storage = provider(&server.url);
    let mut upload = UploadSession::decode(recorded.as_bytes()).unwrap();
    storage.resume_upload(&mut upload).await.unwrap();
    assert!(!upload.is_complete());
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
}
fn provider(url: &str) -> OneDriveStorage {
    let mut storage = OneDriveStorage::new(
        StorageConfig::OneDrive {
            drive_id: "drive".into(),
            folder_id: "root".into(),
        },
        session(PROVIDER),
    )
    .unwrap();
    storage.api = format!("{url}/graph");
    storage
}
fn remote() -> Arc<Mutex<Remote>> {
    let mut remote = Remote::default();
    remote.dirs.insert("root".into(), String::new());
    Arc::new(Mutex::new(remote))
}
#[tokio::test]
async fn missing_session_and_destination_is_expired() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::file(&coven_crypto::StoredFileName::from_bytes([0xff; 32]));
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    state.lock().unwrap().uploads.clear();
    assert!(matches!(
        storage.resume_upload(&mut upload).await,
        Err(StorageError::SessionExpired)
    ));
}
#[tokio::test]
async fn conformance_hierarchy_pagination_and_sharing() {
    let state = remote();
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
    assert!(state.lock().unwrap().members.is_empty());
}
#[tokio::test]
async fn resume_uses_provider_progress_and_keeps_bearer_off_transfer_urls() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::file(&coven_crypto::StoredFileName::from_bytes([0xee; 32]));
    let part_size = 24 * 320 * 1024;
    let mut upload = storage.begin_upload(&path, part_size + 1).await.unwrap();
    let recorded = upload.encode().unwrap();
    state.lock().unwrap().fail_reply = true;
    assert!(storage
        .upload_part(&mut upload, &vec![1; part_size as usize])
        .await
        .is_err());
    drop(upload);
    let storage = provider(&server.url);
    let mut upload = UploadSession::decode(recorded.as_bytes()).unwrap();
    storage.resume_upload(&mut upload).await.unwrap();
    assert_eq!(upload.confirmed, part_size);
    storage.upload_part(&mut upload, b"z").await.unwrap();
    storage.finish_upload(&mut upload).await.unwrap();
    assert_eq!(
        storage
            .read_range(&path, ByteRange::new(part_size, part_size + 1).unwrap())
            .await
            .unwrap(),
        b"z"
    );
}
