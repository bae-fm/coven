use super::*;
use crate::providers::tests::{json as reply, query, read, response, session, TestServer};
use crate::test_utils::STORE_ID;
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, Response, Uri},
    Router,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

#[path = "onedrive_live.rs"]
mod live;

#[tokio::test]
async fn completed_parts_wait_for_publication_and_temporaries_are_not_listed() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::device_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    assert!(!upload.is_complete());
    assert_eq!(upload.confirmed_bytes(), 4);
    assert!(storage.status(&path).await.unwrap().is_none());
    assert!(storage
        .list(&ObjectPrefix::all(STORE_ID))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        storage.read(&path).await.unwrap_err().failure(),
        StorageFailure::NotFound
    );
    storage.finish_upload(&mut upload).await.unwrap();
    assert!(upload.is_complete());
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
    assert_eq!(state.lock().unwrap().files.len(), 1);
}

#[derive(Default)]
pub(super) struct Remote {
    pub(super) requests: Vec<(Method, String)>,
    pub(super) dirs: BTreeMap<String, String>,
    pub(super) files: BTreeMap<String, Vec<u8>>,
    revisions: BTreeMap<String, u64>,
    replace_during_read: bool,
    uploads: BTreeMap<String, (String, Vec<u8>)>,
    next: u64,
    non_owner: bool,
    redeemed: BTreeSet<String>,
    redeem_requests: Vec<String>,
    wrong_share_destination: bool,
    lose_redeem_reply: bool,
    single_uploads: usize,
    session_starts: usize,
    fail_reply: bool,
    copies: usize,
    copied: BTreeSet<String>,
    fail_copy_reply: bool,
    refuse_copy: bool,
    fail_monitor: bool,
    monitor_replies: std::collections::VecDeque<(u16, Value)>,
    refuse_temporary_delete: bool,
    lose_temporary_delete_reply: bool,
    expected_ranges: Option<Value>,
    /// Refuse the next fragment with an invalid range, as for a bad request.
    invalid_range: bool,
    /// Another party locks the next copy's source.
    lock_copy: bool,
    members: BTreeSet<String>,
    permissions: BTreeMap<String, Value>,
    deleted_permissions: Vec<String>,
    lose_permission_reply: bool,
}
fn file(path: &str, bytes: &[u8], host: &str, state: &Remote) -> Value {
    let modified = if state.copied.contains(path) {
        "2026-10-06T00:10:00Z"
    } else {
        "2026-10-06T00:00:00Z"
    };
    json!({"id":format!("file:{path}"),"name":path.rsplit('/').next().unwrap(),"size":bytes.len(),"file":{},"eTag":state.revisions[path].to_string(),"lastModifiedDateTime":modified,"createdDateTime":"2026-10-06T00:00:00Z","fileSystemInfo":{"createdDateTime":"2000-01-01T00:00:00Z"},"@microsoft.graph.downloadUrl":format!("http://{host}/download/{path}")})
}
pub(super) async fn endpoint(
    State(state): State<Arc<Mutex<Remote>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let mut state = state.lock().unwrap();
    state.requests.push((method.clone(), uri.to_string()));
    let q = query(&uri);
    let host = headers["host"].to_str().unwrap();
    if uri.path().starts_with("/copy-monitor/") {
        assert!(headers.get("authorization").is_none());
        if let Some((status, body)) = state.monitor_replies.pop_front() {
            return response(status, body.to_string());
        }
        if std::mem::take(&mut state.fail_monitor) {
            return response(503, "{}");
        }
        return reply(json!({"status":"completed"}));
    }
    if let Some(path) = uri.path().strip_prefix("/download/") {
        assert!(headers.get("authorization").is_none());
        if std::mem::take(&mut state.replace_during_read) {
            *state.revisions.get_mut(path).unwrap() += 1;
        }
        return match state.files.get(path) {
            Some(bytes) => read(bytes, &headers),
            None => response(404, "{}"),
        };
    }
    if let Some(id) = uri.path().strip_prefix("/session/") {
        assert!(headers.get("authorization").is_none());
        if method == Method::DELETE {
            let status = if let Some((path, _)) = state.uploads.remove(id) {
                state.files.remove(&path);
                204
            } else {
                404
            };
            return response(status, Vec::new());
        }
        if method == Method::GET && state.uploads.contains_key(id) {
            if let Some(ranges) = &state.expected_ranges {
                return reply(json!({"nextExpectedRanges": ranges}));
            }
        }
        let invalid_range = std::mem::take(&mut state.invalid_range);
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
        if invalid_range {
            return response(416, json!({"error":{"code":"invalidRange"}}).to_string());
        }
        if start < bytes.len() {
            return response(
                416,
                json!({"error":{"code":"invalidRange","innererror":{"code":"fragmentOverlap"}}})
                    .to_string(),
            );
        }
        assert_eq!(start, bytes.len());
        bytes.extend_from_slice(&body);
        let path = path.clone();
        let size = bytes.len();
        let partial = bytes.clone();
        if size == total {
            let (_, bytes) = state.uploads.remove(id).unwrap();
            *state.revisions.entry(path.clone()).or_insert(0) += 1;
            let value = file(&path, &bytes, host, &state);
            state.files.insert(path, bytes);
            if std::mem::replace(&mut state.fail_reply, false) {
                return response(503, "{}");
            }
            return reply(value);
        }
        state.revisions.entry(path.clone()).or_insert(1);
        state.files.insert(path, partial);
        if std::mem::replace(&mut state.fail_reply, false) {
            return response(503, "{}");
        }
        return response(
            202,
            json!({"nextExpectedRanges":[format!("{size}-")]}).to_string(),
        );
    }
    let token = headers["authorization"].to_str().unwrap();
    let recipient = token.strip_prefix("Bearer recipient:");
    assert!(token == "Bearer token" || recipient.is_some());
    if let Some(share) = uri.path().strip_prefix("/graph/shares/") {
        let email = share
            .strip_prefix("share-")
            .unwrap()
            .strip_suffix("/driveItem")
            .unwrap();
        assert_eq!(recipient, Some(email));
        if !state.members.contains(email) {
            return response(403, json!({"error":{"code":"accessDenied"}}).to_string());
        }
        let prefer = headers["prefer"].to_str().unwrap();
        state.redeem_requests.push(prefer.into());
        if prefer == "redeemSharingLink" {
            state.redeemed.insert(email.into());
            if std::mem::replace(&mut state.lose_redeem_reply, false) {
                return response(503, "{}");
            }
        } else {
            assert_eq!(prefer, "redeemSharingLinkIfNecessary");
        }
        return reply(
            json!({"id":if state.wrong_share_destination {"other"} else {"root"},"folder":{},"parentReference":{"driveId":"drive"}}),
        );
    }
    if let Some(email) = recipient {
        if !state.members.contains(email) || !state.redeemed.contains(email) {
            return response(403, json!({"error":{"code":"accessDenied"}}).to_string());
        }
    }
    if uri.path() == "/graph/me/drive" {
        return reply(
            json!({"id": if state.non_owner || recipient.is_some() { "other" } else { "drive" }}),
        );
    }
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
    if let Some(source) = tail.strip_suffix("/copy") {
        assert_eq!(method, Method::POST);
        assert_eq!(q["@microsoft.graph.conflictBehavior"], "fail");
        assert_eq!(value["parentReference"]["driveId"], "drive");
        if std::mem::take(&mut state.lock_copy) {
            let mut locked = response(423, json!({"error":{"code":"resourceLocked"}}).to_string());
            locked
                .headers_mut()
                .insert("retry-after", "30".parse().unwrap());
            return locked;
        }
        state.copies += 1;
        if state.refuse_copy {
            return response(403, json!({"error":{"code":"accessDenied"}}).to_string());
        }
        let source = url::form_urlencoded::parse(source.as_bytes())
            .next()
            .unwrap()
            .0;
        let source = source.strip_prefix("file:").unwrap();
        let parent = value["parentReference"]["id"].as_str().unwrap();
        let Some(parent) = state.dirs.get(parent) else {
            return response(404, json!({"error":{"code":"itemNotFound"}}).to_string());
        };
        let path = format!("{parent}/{}", value["name"].as_str().unwrap());
        if state.files.contains_key(&path) {
            return response(
                409,
                json!({"error":{"code":"nameAlreadyExists"}}).to_string(),
            );
        }
        let Some(bytes) = state.files.get(source).cloned() else {
            return response(404, json!({"error":{"code":"itemNotFound"}}).to_string());
        };
        let failed = state
            .monitor_replies
            .iter()
            .any(|(_, value)| matches!(value["status"].as_str(), Some("failed" | "deleteFailed")));
        if !failed {
            state.files.insert(path.clone(), bytes);
            state.revisions.insert(path.clone(), 1);
            state.copied.insert(path);
        }
        if std::mem::take(&mut state.fail_copy_reply) {
            return response(503, "{}");
        }
        return Response::builder()
            .status(202)
            .header(
                "location",
                format!("http://{host}/copy-monitor/{}", state.copies),
            )
            .body(Body::empty())
            .unwrap();
    }
    if let Some((parent, tail)) = tail.split_once(":/") {
        let (relative, suffix) = tail.rsplit_once(':').unwrap();
        let Some(base) = state.dirs.get(parent) else {
            return response(404, json!({"error":{"code":"itemNotFound"}}).to_string());
        };
        let path = if base.is_empty() {
            relative.to_owned()
        } else {
            format!("{base}/{relative}")
        };
        if method == Method::PUT || method == Method::POST {
            let parent = path.rsplit_once('/').unwrap().0;
            if !state.dirs.values().any(|directory| directory == parent) {
                return response(404, json!({"error":{"code":"itemNotFound"}}).to_string());
            }
        }
        if suffix == "/children" && method == Method::GET {
            let Some(base) = state.dirs.values().find(|p| *p == &path).cloned() else {
                return response(404, json!({"error":{"code":"itemNotFound"}}).to_string());
            };
            return children(&state, &base, host, &q, uri.path());
        }
        if suffix == "/createUploadSession" {
            state.session_starts += 1;
            assert_eq!(value["item"]["@microsoft.graph.conflictBehavior"], "fail");
            if state.files.contains_key(&path) {
                return response(
                    409,
                    json!({"error":{"code":"nameAlreadyExists"}}).to_string(),
                );
            }
            state.next += 1;
            let id = state.next.to_string();
            state.uploads.insert(id.clone(), (path, Vec::new()));
            return reply(json!({"uploadUrl":format!("http://{host}/session/{id}")}));
        }
        if method == Method::DELETE {
            if path.contains("/.coven-uploads/") && state.refuse_temporary_delete {
                return response(403, json!({"error":{"code":"accessDenied"}}).to_string());
            }
            state.files.remove(&path);
            if path.contains("/.coven-uploads/")
                && std::mem::take(&mut state.lose_temporary_delete_reply)
            {
                return response(503, "{}");
            }
            return response(204, Vec::new());
        }
        if method == Method::PUT {
            assert_eq!(headers["content-type"], "application/octet-stream");
            state.single_uploads += 1;
            if q.get("@microsoft.graph.conflictBehavior")
                .map(String::as_str)
                == Some("fail")
            {
                assert_eq!(headers["if-none-match"], "*");
                if state.files.contains_key(&path) {
                    return response(
                        409,
                        json!({"error":{"code":"nameAlreadyExists"}}).to_string(),
                    );
                }
            }
            *state.revisions.entry(path.clone()).or_insert(0) += 1;
            state.files.insert(path.clone(), body.to_vec());
            return reply(file(&path, &body, host, &state));
        }
        if let Some((id, _)) = state.dirs.iter().find(|(_, p)| *p == &path) {
            return reply(json!({"id":id,"folder":{},"name":relative}));
        }
        return match state.files.get(&path) {
            Some(bytes) => reply(file(&path, bytes, host, &state)),
            None => response(404, json!({"error":{"code":"itemNotFound"}}).to_string()),
        };
    }
    if tail == "root" {
        if !state.dirs.contains_key("root") {
            return response(404, json!({"error":{"code":"itemNotFound"}}).to_string());
        }
        return reply(json!({"id":"root","folder":{}}));
    }
    let (id, suffix) = tail.split_once('/').unwrap();
    if suffix == "children" {
        let Some(base) = state.dirs.get(id).cloned() else {
            return response(404, json!({"error":{"code":"itemNotFound"}}).to_string());
        };
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
        return children(&state, &base, host, &q, uri.path());
    }
    match suffix {
        "permissions" => {
            let entries: Vec<_> = state
                .members
                .iter()
                .map(|email| json!({"id":email,"invitation":{"email":email},"roles":["write"],"shareId":format!("share-{email}")}))
                .chain(state.permissions.values().cloned())
                .collect();
            let start = q
                .get("page")
                .map(|n| n.parse::<usize>().unwrap())
                .unwrap_or(0);
            let mut value = json!({"value":entries.iter().skip(start).take(1).collect::<Vec<_>>()});
            if entries.len() > start + 1 {
                value["@odata.nextLink"] =
                    json!(format!("http://{host}{}?page={}", uri.path(), start + 1));
            }
            reply(value)
        }
        "invite" => {
            state
                .members
                .insert(value["recipients"][0]["email"].as_str().unwrap().into());
            reply(json!({"value":[]}))
        }
        _ if suffix.starts_with("permissions/") => {
            assert_eq!(method, Method::DELETE);
            let id = suffix.strip_prefix("permissions/").unwrap();
            state.deleted_permissions.push(id.into());
            state.members.remove(id);
            state.permissions.remove(id);
            if std::mem::replace(&mut state.lose_permission_reply, false) {
                return response(503, "{}");
            }
            response(204, Vec::new())
        }
        _ => panic!("unexpected OneDrive request {method} {uri}"),
    }
}

pub(super) fn provider(url: &str) -> StorageConnection<OneDriveStorage> {
    let mut storage = OneDriveStorage::new(
        StorageConfig::OneDrive {
            drive_id: "drive".into(),
            folder_id: "root".into(),
        },
        session(PROVIDER),
        Arc::new(coven_foundation::id_source::SequentialIds::new()),
    )
    .unwrap();
    storage.api = format!("{url}/graph");
    StorageConnection::from_provider(Arc::new(storage))
}
pub(super) fn remote() -> Arc<Mutex<Remote>> {
    let mut remote = Remote::default();
    remote.dirs.insert("root".into(), String::new());
    Arc::new(Mutex::new(remote))
}
#[tokio::test]
async fn content_uploads_declare_their_length_including_empty_objects() {
    let state = remote();
    let lengths = Arc::new(Mutex::new(Vec::new()));
    let received = lengths.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let state = state.clone();
            let received = received.clone();
            async move {
                if method == Method::PUT {
                    received
                        .lock()
                        .unwrap()
                        .push((body.len(), headers.get("content-length").cloned()));
                }
                endpoint(State(state), method, uri, headers, body).await
            }
        },
    ))
    .await;
    let storage = provider(&server.url);
    let immutable = ObjectPath::store_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let mutable = ObjectPath::positions(STORE_ID, coven_foundation::id_source::DeviceId(31));
    for bytes in [b"".as_slice(), b"data"] {
        storage.create(&immutable, bytes).await.unwrap();
        storage.delete(&immutable).await.unwrap();
        storage.replace(&mutable, bytes).await.unwrap();
    }
    let lengths = lengths.lock().unwrap();
    assert_eq!(lengths.len(), 4);
    for (length, header) in lengths.iter() {
        assert_eq!(
            header.as_ref().map(|value| value.to_str().unwrap()),
            Some(length.to_string().as_str())
        );
    }
}
#[tokio::test]
async fn missing_session_and_destination_is_expired() {
    let state = remote();
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
async fn conformance_hierarchy_pagination_and_sharing() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = Arc::new(provider(&server.url));
    crate::test_utils::Conformance::new(storage.clone(), STORE_ID)
        .run()
        .await
        .unwrap();
    storage.grant_access("member").await.unwrap();
    storage.revoke_access("member").await.unwrap();
    assert!(state.lock().unwrap().members.is_empty());
}
#[tokio::test]
async fn resume_uses_provider_progress_and_keeps_bearer_off_transfer_urls() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    crate::test_utils::Conformance::new(Arc::new(storage), STORE_ID)
        .lost_part_reply(24 * 320 * 1024, async {
            state.lock().unwrap().fail_reply = true;
        })
        .await;
}

#[tokio::test]
async fn resume_accepts_bounded_and_multiple_missing_ranges() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::device_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(1),
        std::num::NonZeroU64::MIN,
    );
    for ranges in [json!(["5-9"]), json!(["9-", "5-7"]), json!(["5-"])] {
        let mut upload = storage.begin_upload(&path, 12).await.unwrap();
        state.lock().unwrap().expected_ranges = Some(ranges);
        storage.resume_upload(&mut upload).await.unwrap();
        assert_eq!(upload.confirmed_bytes(), 5);
        assert!(!upload.is_complete());
    }
}

#[tokio::test]
async fn malformed_missing_ranges_keep_the_response_and_recorded_progress() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::device_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(1),
        std::num::NonZeroU64::MIN,
    );
    for ranges in [
        json!([]),
        json!("5-"),
        json!([3]),
        json!(["no-range"]),
        json!(["4-"]),
        json!(["12-"]),
        json!(["5-12"]),
        json!(["8-7"]),
        json!(["18446744073709551616-"]),
    ] {
        let mut upload = storage.begin_upload(&path, 12).await.unwrap();
        upload.confirmed = 5;
        state.lock().unwrap().expected_ranges = Some(ranges.clone());
        let error = storage.resume_upload(&mut upload).await.unwrap_err();
        assert_eq!(error.failure(), StorageFailure::Protocol);
        let StorageError::Provider { source, .. } = error else {
            panic!("response discarded")
        };
        let response = source.downcast_ref::<http::ProviderResponse>().unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            serde_json::from_slice::<Value>(response.body()).unwrap()["nextExpectedRanges"],
            ranges
        );
        assert_eq!(upload.confirmed_bytes(), 5);
    }
}

#[tokio::test]
async fn abort_retries_after_cancellation_and_preserves_published_objects() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::device_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let upload = storage.begin_upload(&path, 4).await.unwrap();
    storage.abort_upload(&upload).await.unwrap();
    storage.abort_upload(&upload).await.unwrap();
    assert!(state.lock().unwrap().uploads.is_empty());
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    let recorded = upload.encode().unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    storage.finish_upload(&mut upload).await.unwrap();
    let unconfirmed = UploadSession::decode(recorded.as_bytes()).unwrap();
    storage.abort_upload(&unconfirmed).await.unwrap();
    storage.abort_upload(&unconfirmed).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
}

#[tokio::test]
async fn create_switches_to_a_session_above_the_content_limit() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let mut storage = provider(&server.url);
    assert_eq!(storage.single_request_limit(), 250_000_000);
    Arc::get_mut(&mut storage.provider)
        .unwrap()
        .single_request_limit = 4;
    let path = ObjectPath::device_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let mut bytes = vec![7; storage.single_request_limit() as usize];
    storage.create(&path, &bytes).await.unwrap();
    assert_eq!(state.lock().unwrap().single_uploads, 1);
    assert_eq!(state.lock().unwrap().session_starts, 0);
    storage.delete(&path).await.unwrap();
    bytes.push(8);
    storage.create(&path, &bytes).await.unwrap();
    assert_eq!(state.lock().unwrap().single_uploads, 1);
    assert_eq!(state.lock().unwrap().session_starts, 1);
    assert_eq!(state.lock().unwrap().files[path.as_str()], bytes);
}

#[tokio::test]
async fn sharing_requires_the_store_owners_account() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.grant_access("kept@example.test").await.unwrap();
    state.lock().unwrap().non_owner = true;
    crate::test_utils::Conformance::new(Arc::new(storage), STORE_ID)
        .owner_only_sharing()
        .await;
    assert_eq!(
        state
            .lock()
            .unwrap()
            .members
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        ["kept@example.test"]
    );
}

#[tokio::test]
async fn shared_location_ignores_unrelated_folders() {
    let state = remote();
    state
        .lock()
        .unwrap()
        .dirs
        .insert("unrelated".into(), "vacation".into());
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    assert!(storage
        .list(&ObjectPrefix::all(STORE_ID))
        .await
        .unwrap()
        .is_empty());
    crate::test_utils::Conformance::new(Arc::new(storage), STORE_ID)
        .shared_location(coven_foundation::id_source::StoreId(
            uuid::Uuid::from_bytes([0x20; 16]),
        ))
        .await;
    assert_eq!(state.lock().unwrap().dirs["unrelated"], "vacation");
}

#[tokio::test]
async fn listing_retains_server_time_and_size_across_pages_and_retries() {
    let state = remote();
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

#[tokio::test]
async fn recipient_join_redeems_only_the_invited_destination_and_keeps_native_refusals() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let owner = provider(&server.url);
    let path = ObjectPath::store_log(
        STORE_ID,
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
            client_id: "onedrive".into(),
            access: crate::OAuthAccess::OneDrive,
            access_token: SecretText::new("recipient:member".into()),
            refresh_token: None,
        })
        .await
        .unwrap();
    assert_eq!(
        recipient.read(&path).await.unwrap_err().failure(),
        StorageFailure::PermissionDenied
    );
    state.lock().unwrap().wrong_share_destination = true;
    assert!(matches!(
        recipient.join(STORE_ID, &invitation).await,
        Err(error) if error.failure() == StorageFailure::InvitationMismatch
    ));
    assert!(state.lock().unwrap().redeemed.is_empty());
    state.lock().unwrap().wrong_share_destination = false;
    state.lock().unwrap().lose_redeem_reply = true;
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
    assert_eq!(
        state.lock().unwrap().redeem_requests,
        [
            "redeemSharingLinkIfNecessary",
            "redeemSharingLinkIfNecessary",
            "redeemSharingLink",
            "redeemSharingLinkIfNecessary",
            "redeemSharingLink"
        ]
    );
    owner.revoke_access("member").await.unwrap();
    assert_eq!(
        recipient.read(&path).await.unwrap_err().failure(),
        StorageFailure::PermissionDenied
    );
    let error = recipient.join(STORE_ID, &invitation).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::PermissionDenied);
    let StorageError::Provider { source, .. } = error else {
        panic!()
    };
    assert!(source.downcast_ref::<http::ProviderResponse>().is_some());
}

#[path = "onedrive_access_tests.rs"]
mod access_tests;

#[tokio::test]
async fn missing_container_is_not_an_empty_store_or_a_missing_object() {
    for (status, expected) in [
        (404, StorageFailure::ContainerNotFound),
        (403, StorageFailure::PermissionDenied),
    ] {
        let server = TestServer::new(Router::new().fallback(move |uri: Uri| async move {
            if uri.path().ends_with("/root") {
                response(status, "native-folder-cause")
            } else if uri.path().ends_with("/files") {
                reply(json!({"value":[]}))
            } else {
                response(status, "native-folder-cause")
            }
        }))
        .await;
        let storage = provider(&server.url);
        let path = ObjectPath::store_log(
            STORE_ID,
            coven_foundation::id_source::DeviceId(31),
            std::num::NonZeroU64::MIN,
        );
        for error in [
            storage
                .list(&ObjectPrefix::all(STORE_ID))
                .await
                .err()
                .unwrap(),
            storage.read(&path).await.err().unwrap(),
            storage.delete(&path).await.err().unwrap(),
            storage.create(&path, b"first").await.err().unwrap(),
        ] {
            assert_eq!(error.failure(), expected);
            assert!(matches!(error, StorageError::Provider { .. }));
        }
    }
    let server = TestServer::new(Router::new().fallback(|uri: Uri| async move {
        if uri.path().ends_with("/root") {
            reply(json!({"id":"root","mimeType":"application/vnd.google-apps.folder","folder":{}}))
        } else if uri.path().ends_with("/files") {
            reply(json!({"value":[]}))
        } else {
            response(404, "missing-object")
        }
    }))
    .await;
    let storage = provider(&server.url);
    let path = ObjectPath::store_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    assert_eq!(
        storage.read(&path).await.unwrap_err().failure(),
        StorageFailure::NotFound
    );
    storage.delete(&path).await.unwrap();
}

#[tokio::test]
async fn permission_failures_reach_every_object_and_upload_caller() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let denied = Arc::new(AtomicBool::new(false));
    let failures = denied.clone();
    let state = remote();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let denied = failures.clone();
            let state = state.clone();
            async move {
                if denied.load(Ordering::SeqCst) {
                    response(403, "native-refusal")
                } else {
                    endpoint(State(state), method, uri, headers, body).await
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
    fn native_refusal(error: &StorageError) {
        match error {
            StorageError::Provider { source, .. } => {
                let response = source.downcast_ref::<http::ProviderResponse>().unwrap();
                assert_eq!(response.status(), 403);
                assert_eq!(response.body(), b"native-refusal");
            }
            StorageError::Cleanup { operation, cleanup } => {
                native_refusal(operation);
                native_refusal(cleanup);
            }
            _ => panic!("native permission failure was discarded"),
        }
    }
    assert!(errors
        .iter()
        .any(|error| matches!(error, StorageError::Cleanup { .. })));
    for error in &errors {
        native_refusal(error);
    }
}

#[tokio::test]
async fn listing_refuses_hostile_links_repeated_pages_and_folder_cycles() {
    for variant in 0..5 {
        let server=TestServer::new(Router::new().fallback(move |uri:Uri,headers:HeaderMap| async move {
            if uri.path().ends_with("/root") || uri.path().ends_with(':') {return reply(json!({"id":"root","folder":{}}));}
            let page=match variant {
                0=>json!({"value":[],"@odata.nextLink":"https://another-account.invalid/list"}),
                1=>json!({"value":[],"@odata.nextLink":format!("http://{}/graph/drives/drive/items/root/children",headers["host"].to_str().unwrap())}),
                2=>json!({"value":[{"id":"root","name":"devices","folder":{}}]}),
                3=>json!({"value":[{"id":"both","name":"devices","folder":{},"file":{}}]}),
                _=>json!({"value":[{"id":"malformed","name":"devices","folder":true}]}),
            };
            reply(page)
        })).await;
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
async fn listing_refuses_native_items_that_are_not_object_files() {
    let server = TestServer::new(Router::new().fallback(|uri: Uri| async move {
        if uri.path().ends_with("/root") || uri.path().ends_with(':') {
            reply(json!({"id":"root","folder":{}}))
        } else {
            reply(json!({"value":[{"id":"native","name":"notebook","package":{"type":"oneNote"}}]}))
        }
    }))
    .await;
    assert!(
        matches!(provider(&server.url).list(&ObjectPrefix::all(STORE_ID)).await,
        Err(error) if error.failure() == StorageFailure::InvalidPath)
    );
}

#[tokio::test]
async fn replacement_during_download_does_not_return_mismatched_metadata() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::positions(STORE_ID, coven_foundation::id_source::DeviceId(31));
    storage.replace(&path, b"first").await.unwrap();
    state.lock().unwrap().replace_during_read = true;
    assert_eq!(
        storage
            .read_object(&path, None)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Protocol
    );
}

#[path = "onedrive_upload_tests.rs"]
mod upload_tests;

/// One page of a folder's immediate children, addressed by id or by path.
fn children(
    state: &Remote,
    base: &str,
    host: &str,
    q: &std::collections::BTreeMap<String, String>,
    request_path: &str,
) -> Response<Body> {
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
                entries.push(file(path, bytes, host, state));
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
            json!(format!("http://{host}{request_path}?page={}", start + 1));
    }
    reply(result)
}
