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
    non_owner: bool,
    redeemed: BTreeSet<String>,
    redeem_requests: Vec<String>,
    wrong_share_destination: bool,
    lose_redeem_reply: bool,
    single_uploads: usize,
    session_starts: usize,
    fail_reply: bool,
    expected_ranges: Option<Value>,
    members: BTreeSet<String>,
    permissions: BTreeMap<String, Value>,
    deleted_permissions: Vec<String>,
    lose_permission_reply: bool,
}
fn file(path: &str, bytes: &[u8], host: &str) -> Value {
    json!({"id":format!("file:{path}"),"name":path.rsplit('/').next().unwrap(),"size":bytes.len(),"file":{},"createdDateTime":"2026-10-06T00:00:00Z","fileSystemInfo":{"createdDateTime":"2000-01-01T00:00:00Z"},"@microsoft.graph.downloadUrl":format!("http://{host}/download/{path}")})
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
            let status = if state.uploads.remove(id).is_some() {
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
    if let Some((parent, tail)) = tail.split_once(":/") {
        let (relative, suffix) = tail.rsplit_once(':').unwrap();
        let base = state.dirs.get(parent).unwrap();
        let path = if base.is_empty() {
            relative.to_owned()
        } else {
            format!("{base}/{relative}")
        };
        if suffix == "/createUploadSession" {
            state.session_starts += 1;
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
    if tail == "root" {
        return reply(json!({"id":"root","folder":{}}));
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

#[tokio::test]
async fn lost_completion_requires_byte_verification() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::file(coven_foundation::id_source::FileId(uuid::Uuid::from_bytes(
        [0xff; 16],
    )));
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
    let path = ObjectPath::file(coven_foundation::id_source::FileId(uuid::Uuid::from_bytes(
        [0xee; 16],
    )));
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

#[tokio::test]
async fn resume_accepts_bounded_and_multiple_missing_ranges() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::store_log(
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
    let path = ObjectPath::store_log(
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
async fn refreshed_tokens_reach_the_same_adapter() {
    crate::providers::tests::assert_token_refresh(
        |url| Arc::new(provider(url)),
        json!({"value":[],"id":"root","folder":{}}),
    )
    .await;
}

#[tokio::test]
async fn abort_retries_after_cancellation_and_preserves_published_objects() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::device_log(
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
    let unconfirmed = UploadSession::decode(recorded.as_bytes()).unwrap();
    storage.abort_upload(&unconfirmed).await.unwrap();
    storage.abort_upload(&unconfirmed).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
}

#[tokio::test]
async fn create_switches_to_a_session_above_the_content_limit() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    assert_eq!(storage.single_request_limit(), 250 * 1024 * 1024);
    let path = ObjectPath::device_log(
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
    for error in [
        storage
            .grant_access("new@example.test")
            .await
            .err()
            .unwrap(),
        storage
            .revoke_access(&MemberAccess::ProviderAccount("kept@example.test".into()))
            .await
            .err()
            .unwrap(),
    ] {
        assert!(matches!(error, StorageError::NotStoreOwner));
        assert_eq!(error.failure(), StorageFailure::PermissionDenied);
    }
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
async fn setup_refuses_an_unrelated_empty_folder() {
    let state = remote();
    state
        .lock()
        .unwrap()
        .dirs
        .insert("unrelated".into(), "vacation".into());
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let first = ObjectPath::store_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    assert_eq!(
        storage.setup(&first, b"first").await.unwrap_err().failure(),
        StorageSetupFailure::LocationOccupied
    );
    assert!(state.lock().unwrap().files.is_empty());
}

#[tokio::test]
async fn listing_retains_server_time_and_size_across_pages_and_retries() {
    let state = remote();
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
async fn recipient_join_redeems_only_the_invited_destination_and_keeps_native_refusals() {
    let state = remote();
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
    state.lock().unwrap().wrong_share_destination = true;
    assert!(matches!(
        recipient.join(&invitation).await,
        Err(StorageError::InvitationMismatch)
    ));
    assert!(state.lock().unwrap().redeemed.is_empty());
    state.lock().unwrap().wrong_share_destination = false;
    state.lock().unwrap().lose_redeem_reply = true;
    assert_eq!(
        recipient.join(&invitation).await.unwrap_err().failure(),
        StorageFailure::Network
    );
    recipient.join(&invitation).await.unwrap();
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
    owner
        .revoke_access(&MemberAccess::ProviderAccount("member".into()))
        .await
        .unwrap();
    assert_eq!(
        recipient.read(&path).await.unwrap_err().failure(),
        StorageFailure::PermissionDenied
    );
    let error = recipient.join(&invitation).await.unwrap_err();
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
            coven_foundation::id_source::DeviceId(31),
            std::num::NonZeroU64::MIN,
        );
        for error in [
            storage.list(&ObjectPrefix::all()).await.err().unwrap(),
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
    crate::providers::tests::assert_permission_failures(Arc::new(provider(&server.url)), || {
        denied.store(true, Ordering::SeqCst)
    })
    .await;
}

#[tokio::test]
async fn listing_refuses_hostile_links_repeated_pages_and_folder_cycles() {
    for variant in 0..5 {
        let server=TestServer::new(Router::new().fallback(move |uri:Uri,headers:HeaderMap| async move {
            if uri.path().ends_with("/root") {return reply(json!({"id":"root","folder":{}}));}
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
                .list(&ObjectPrefix::all())
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::Protocol
        );
    }
}

#[tokio::test]
async fn setup_refuses_native_items_that_are_not_object_files() {
    let server = TestServer::new(Router::new().fallback(|uri: Uri| async move {
        if uri.path().ends_with("/root") {
            reply(json!({"id":"root","folder":{}}))
        } else {
            reply(json!({"value":[{"id":"native","name":"notebook","package":{"type":"oneNote"}}]}))
        }
    }))
    .await;
    let path = ObjectPath::store_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    assert_eq!(
        provider(&server.url)
            .setup(&path, b"first")
            .await
            .unwrap_err()
            .failure(),
        StorageSetupFailure::LocationOccupied
    );
}
