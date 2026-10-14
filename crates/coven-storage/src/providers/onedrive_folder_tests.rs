use super::*;
use crate::providers::onedrive::tests::{endpoint, provider, remote, Remote};
use crate::providers::tests::{json as reply, response, TestServer};
use crate::test_utils::STORE_ID;
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, Method, Uri},
    Router,
};
use coven_foundation::id_source::DeviceId;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

fn log(number: u64) -> ObjectPath {
    ObjectPath::store_log(STORE_ID, DeviceId(31), NonZeroU64::new(number).unwrap())
}

fn device_log(number: u64) -> ObjectPath {
    ObjectPath::device_log(STORE_ID, DeviceId(31), NonZeroU64::new(number).unwrap())
}

fn take_requests(state: &Arc<Mutex<Remote>>) -> Vec<(Method, String)> {
    std::mem::take(&mut state.lock().unwrap().requests)
}

fn folder_id(state: &Arc<Mutex<Remote>>, path: &str) -> String {
    state
        .lock()
        .unwrap()
        .dirs
        .iter()
        .find(|(_, directory)| directory.as_str() == path)
        .unwrap()
        .0
        .clone()
}

fn remove_folder(state: &Arc<Mutex<Remote>>, path: &str) {
    let mut state = state.lock().unwrap();
    let prefix = format!("{path}/");
    state
        .dirs
        .retain(|_, directory| directory != path && !directory.starts_with(&prefix));
    state.files.retain(|file, _| !file.starts_with(&prefix));
}

#[tokio::test]
async fn warm_create_and_read_do_not_resolve_parents_again() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.create(&log(1), b"first").await.unwrap();
    take_requests(&state);
    storage.create(&log(2), b"second").await.unwrap();
    assert_eq!(take_requests(&state), [(Method::PUT, format!(
        "/graph/drives/drive/items/root:/{STORE_ID}/store-log/31/2:/content?@microsoft.graph.conflictBehavior=fail"
    ))]);
    for _ in 0..2 {
        assert_eq!(storage.read(&log(1)).await.unwrap(), b"first");
        assert_eq!(
            take_requests(&state),
            [
                (
                    Method::GET,
                    format!("/graph/drives/drive/items/root:/{STORE_ID}/store-log/31/1:")
                ),
                (Method::GET, format!("/download/{STORE_ID}/store-log/31/1")),
            ]
        );
    }
    let positions = ObjectPath::positions(STORE_ID, DeviceId(31));
    storage.replace(&positions, b"first").await.unwrap();
    take_requests(&state);
    storage.replace(&positions, b"second").await.unwrap();
    assert_eq!(
        take_requests(&state),
        [(
            Method::PUT,
            format!("/graph/drives/drive/items/root:/{STORE_ID}/positions/31:/content")
        )]
    );
}

#[tokio::test]
async fn scoped_listing_counts_only_requested_folder_pages() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    for number in 1..=3 {
        storage.create(&log(number), b"entry").await.unwrap();
    }
    storage
        .replace(&ObjectPath::positions(STORE_ID, DeviceId(31)), b"positions")
        .await
        .unwrap();
    storage
        .create(
            &ObjectPath::file(
                STORE_ID,
                DeviceId(32),
                coven_foundation::id_source::FileId(uuid::Uuid::from_bytes([3; 16])),
            ),
            b"unrelated file",
        )
        .await
        .unwrap();
    // Invalid contents of an unrelated directory must never enter this listing.
    state
        .lock()
        .unwrap()
        .dirs
        .insert("unrelated".into(), format!("{STORE_ID}/files/not-a-device"));
    let folder = folder_id(&state, &format!("{STORE_ID}/store-log/31"));
    take_requests(&state);
    for _ in 0..2 {
        let objects = storage
            .list(&ObjectPrefix::store_log(STORE_ID, DeviceId(31)))
            .await
            .unwrap();
        assert_eq!(
            objects
                .into_iter()
                .map(|object| object.path)
                .collect::<Vec<_>>(),
            [log(1), log(2), log(3)]
        );
        assert_eq!(
            take_requests(&state),
            [
                (
                    Method::GET,
                    format!("/graph/drives/drive/items/{folder}/children")
                ),
                (
                    Method::GET,
                    format!("/graph/drives/drive/items/{folder}/children?page=1")
                ),
                (
                    Method::GET,
                    format!("/graph/drives/drive/items/{folder}/children?page=2")
                ),
            ]
        );
    }
}

#[tokio::test]
async fn exact_name_misses_do_not_list_or_cache_absence() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.create(&log(1), b"entry").await.unwrap();
    take_requests(&state);
    for _ in 0..2 {
        assert!(storage.status(&log(2)).await.unwrap().is_none());
        assert_eq!(
            take_requests(&state),
            [
                (
                    Method::GET,
                    format!("/graph/drives/drive/items/root:/{STORE_ID}/store-log/31/2:")
                ),
                (Method::GET, "/graph/drives/drive/items/root".into()),
            ]
        );
    }
    provider(&server.url)
        .create(&log(2), b"arrived")
        .await
        .unwrap();
    assert_eq!(storage.read(&log(2)).await.unwrap(), b"arrived");
}

#[tokio::test]
async fn disappeared_cached_folder_fails_listing_then_resolves_again() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.create(&log(1), b"entry").await.unwrap();
    let prefix = ObjectPrefix::store_log(STORE_ID, DeviceId(31));
    let folder = folder_id(&state, prefix.as_str().trim_end_matches('/'));
    remove_folder(&state, &format!("{STORE_ID}/store-log"));
    take_requests(&state);
    assert_eq!(
        storage.list(&prefix).await.unwrap_err().failure(),
        StorageFailure::NotFound
    );
    assert_eq!(
        take_requests(&state),
        [
            (
                Method::GET,
                format!("/graph/drives/drive/items/{folder}/children")
            ),
            (Method::GET, "/graph/drives/drive/items/root".into()),
        ]
    );
    assert!(storage.list(&prefix).await.unwrap().is_empty());
    provider(&server.url)
        .create(&log(2), b"new folder")
        .await
        .unwrap();
    assert_eq!(storage.list(&prefix).await.unwrap()[0].path, log(2));
    take_requests(&state);
    storage.create(&log(3), b"cached again").await.unwrap();
    assert_eq!(take_requests(&state).len(), 1);
}

#[tokio::test]
async fn listed_folders_are_reused_for_creates() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    provider(&server.url)
        .create(&log(1), b"existing")
        .await
        .unwrap();
    let storage = provider(&server.url);
    let prefix = ObjectPrefix::store_logs(STORE_ID);
    assert_eq!(storage.list(&prefix).await.unwrap()[0].path, log(1));
    take_requests(&state);
    storage.create(&log(2), b"new").await.unwrap();
    assert_eq!(take_requests(&state).len(), 1);
    let folder = folder_id(&state, prefix.as_str().trim_end_matches('/'));
    take_requests(&state);
    storage.list(&prefix).await.unwrap();
    let requests = take_requests(&state);
    assert_eq!(
        requests[0],
        (
            Method::GET,
            format!("/graph/drives/drive/items/{folder}/children")
        )
    );
    assert_eq!(requests.len(), 3); // One writer folder, two entry pages.
}

#[tokio::test]
async fn failed_page_does_not_publish_partial_folder_discovery() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let fail_page = Arc::new(AtomicBool::new(false));
    let state = remote();
    let server = TestServer::new(Router::new().fallback({
        let state = state.clone();
        let fail_page = fail_page.clone();
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let state = state.clone();
            let fail_page = fail_page.clone();
            async move {
                if uri.query() == Some("page=1") && fail_page.load(Ordering::SeqCst) {
                    response(503, "failed-page")
                } else {
                    endpoint(State(state), method, uri, headers, body).await
                }
            }
        }
    }))
    .await;
    let seed = provider(&server.url);
    seed.create(&log(1), b"existing").await.unwrap();
    seed.create(
        &ObjectPath::store_log(STORE_ID, DeviceId(32), NonZeroU64::MIN),
        b"other",
    )
    .await
    .unwrap();
    let storage = provider(&server.url);
    fail_page.store(true, Ordering::SeqCst);
    assert_eq!(
        storage
            .list(&ObjectPrefix::store_logs(STORE_ID))
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Network
    );
    fail_page.store(false, Ordering::SeqCst);
    take_requests(&state);
    // A folder from the failed listing must be resolved before this write.
    storage.create(&log(2), b"new").await.unwrap();
    assert!(take_requests(&state)
        .iter()
        .any(|(method, _)| *method == Method::POST));
    assert_eq!(
        storage
            .list(&ObjectPrefix::store_logs(STORE_ID))
            .await
            .unwrap()
            .len(),
        3
    );
}

#[tokio::test]
async fn disappeared_parent_fails_write_and_retry_recreates_it() {
    for replace in [false, true] {
        let state = remote();
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = provider(&server.url);
        let path = if replace {
            ObjectPath::positions(STORE_ID, DeviceId(31))
        } else {
            log(1)
        };
        storage.create(&path, b"before").await.unwrap();
        remove_folder(&state, &STORE_ID.to_string());
        let result = if replace {
            storage.replace(&path, b"after").await
        } else {
            storage.create(&path, b"after").await
        };
        assert_eq!(result.unwrap_err().failure(), StorageFailure::NotFound);
        if replace {
            storage.replace(&path, b"after").await.unwrap();
        } else {
            storage.create(&path, b"after").await.unwrap();
        }
        assert_eq!(storage.read(&path).await.unwrap(), b"after");
    }
}

#[tokio::test]
async fn missing_configured_folder_is_not_cached_absence() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.create(&log(1), b"entry").await.unwrap();
    remove_folder(&state, &STORE_ID.to_string());
    state.lock().unwrap().dirs.remove("root");
    for error in [
        storage
            .list(&ObjectPrefix::store_logs(STORE_ID))
            .await
            .unwrap_err(),
        storage.status(&log(1)).await.unwrap_err(),
        storage.create(&log(2), b"entry").await.unwrap_err(),
    ] {
        assert_eq!(error.failure(), StorageFailure::ContainerNotFound);
    }
}

#[tokio::test]
async fn concurrent_creates_share_parent_resolution() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let one = log(1);
    let two = log(2);
    let (first, second) = tokio::join!(storage.create(&one, b"one"), storage.create(&two, b"two"));
    first.unwrap();
    second.unwrap();
    let requests = take_requests(&state);
    assert_eq!(
        requests
            .iter()
            .filter(|(method, _)| *method == Method::POST)
            .count(),
        3
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(method, _)| *method == Method::GET)
            .count(),
        1
    );
}

#[tokio::test]
async fn missing_prefix_uses_exact_lookup_and_can_appear_later() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.create(&log(1), b"entry").await.unwrap();
    take_requests(&state);
    assert!(storage
        .list(&ObjectPrefix::positions(STORE_ID))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        take_requests(&state),
        [
            (
                Method::GET,
                format!("/graph/drives/drive/items/root:/{STORE_ID}/positions:")
            ),
            (Method::GET, "/graph/drives/drive/items/root".into()),
        ]
    );
    let positions = ObjectPath::positions(STORE_ID, DeviceId(31));
    provider(&server.url)
        .replace(&positions, b"arrived")
        .await
        .unwrap();
    assert_eq!(
        storage
            .list(&ObjectPrefix::positions(STORE_ID))
            .await
            .unwrap()[0]
            .path,
        positions
    );
}

#[tokio::test]
async fn warm_upload_begin_reuses_the_reserved_folder() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let first = storage.begin_upload(&device_log(1), 4).await.unwrap();
    take_requests(&state);
    let second = storage.begin_upload(&device_log(2), 4).await.unwrap();
    let requests = take_requests(&state);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].0, Method::POST);
    assert!(requests[0].1.ends_with("/createUploadSession"));
    take_requests(&state);
    assert!(storage
        .list(&ObjectPrefix::all(STORE_ID))
        .await
        .unwrap()
        .is_empty());
    let temporary = folder_id(&state, &format!("{STORE_ID}/.coven-uploads"));
    assert!(!take_requests(&state)
        .iter()
        .any(|(_, uri)| uri.contains(&format!("/{temporary}/children"))));
    storage.abort_upload(&first).await.unwrap();
    storage.abort_upload(&second).await.unwrap();
}

#[tokio::test]
async fn refreshed_tokens_reach_cached_folder_requests() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let server = TestServer::new(Router::new().fallback({
        let received = received.clone();
        move |headers: HeaderMap| {
            let received = received.clone();
            async move {
                received
                    .lock()
                    .unwrap()
                    .push(headers["authorization"].to_str().unwrap().to_owned());
                reply(json!({"value":[], "id":"root", "folder":{}}))
            }
        }
    }))
    .await;
    let storage = provider(&server.url);
    let prefix = ObjectPrefix::all(STORE_ID);
    storage.list(&prefix).await.unwrap();
    let initial = std::mem::take(&mut *received.lock().unwrap());
    assert_eq!(initial, ["Bearer token"; 3]);
    storage
        .set_oauth_tokens(OAuthTokens {
            client_id: "test-client".into(),
            access: crate::OAuthAccess::OneDrive,
            access_token: SecretText::new("refreshed".into()),
            refresh_token: None,
        })
        .await
        .unwrap();
    storage.list(&prefix).await.unwrap();
    assert_eq!(*received.lock().unwrap(), ["Bearer refreshed"]);
}

#[tokio::test]
async fn disappeared_upload_folders_are_invalidated_before_retry() {
    let state = remote();
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let first = storage.begin_upload(&device_log(1), 4).await.unwrap();
    storage.abort_upload(&first).await.unwrap();
    remove_folder(&state, &format!("{STORE_ID}/.coven-uploads"));
    assert_eq!(
        storage
            .begin_upload(&device_log(1), 4)
            .await
            .err()
            .unwrap()
            .failure(),
        StorageFailure::NotFound
    );
    let mut upload = storage.begin_upload(&device_log(1), 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    storage
        .create(&device_log(2), b"seed parent")
        .await
        .unwrap();
    remove_folder(&state, &format!("{STORE_ID}/devices"));
    assert_eq!(
        storage
            .finish_upload(&mut upload)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::NotFound
    );
    storage.abort_upload(&upload).await.unwrap();
    let mut upload = storage.begin_upload(&device_log(1), 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    storage.finish_upload(&mut upload).await.unwrap();
    assert_eq!(storage.read(&device_log(1)).await.unwrap(), b"data");
}
