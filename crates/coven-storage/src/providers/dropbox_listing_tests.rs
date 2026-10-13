use super::*;
use crate::providers::dropbox::tests::{endpoint, file_metadata, provider, Remote};
use crate::providers::tests::{json as reply, response, session, TestServer};
use crate::test_utils::STORE_ID;
use crate::*;
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, Uri},
    Router,
};
use coven_foundation::id_source::{CircleId, DeviceId, FileId, InviteId, StoreId};
use coven_merge::Audience;
use serde_json::Value;
use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

struct ListingServer {
    server: TestServer,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
}

impl ListingServer {
    async fn new(remote: Arc<Mutex<Remote>>) -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let received = requests.clone();
        let server = TestServer::new(Router::new().fallback(
            move |uri: Uri, headers: HeaderMap, body: Bytes| {
                let remote = remote.clone();
                let received = received.clone();
                async move {
                    received.lock().unwrap().push((
                        uri.path().to_owned(),
                        serde_json::from_slice(&body).unwrap(),
                    ));
                    endpoint(State(remote), uri, headers, body).await
                }
            },
        ))
        .await;
        Self { server, requests }
    }

    fn take_requests(&self) -> Vec<(String, Value)> {
        std::mem::take(&mut *self.requests.lock().unwrap())
    }
}

fn list_request(folder: &str) -> (String, Value) {
    (
        "/2/files/list_folder".into(),
        json!({"path":folder,"recursive":true,"include_deleted":false,"limit":2000}),
    )
}

#[tokio::test]
async fn folder_pages_keep_the_selected_root() {
    for location in [
        DropboxLocation::AppFolder,
        DropboxLocation::SharedFolder {
            namespace_id: "namespace".into(),
        },
        DropboxLocation::SharedFolder {
            namespace_id: "other-namespace".into(),
        },
    ] {
        let expected_root = match &location {
            DropboxLocation::AppFolder => None,
            DropboxLocation::SharedFolder { namespace_id } => {
                Some(json!({".tag":"namespace_id","namespace_id":namespace_id}))
            }
        };
        let store = StoreId(uuid::Uuid::from_bytes([0xab; 16]));
        let path = ObjectPath::store_log(store, DeviceId(31), 1.try_into().unwrap());
        let item = file_metadata(&path.absolute(), b"entry", &Remote::default());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let received = requests.clone();
        let handler = move |uri: Uri, headers: HeaderMap, body: Bytes| {
            let received = received.clone();
            let item = item.clone();
            let expected_root = expected_root.clone();
            async move {
                let root = headers
                    .get("dropbox-api-path-root")
                    .map(|header| serde_json::from_str::<Value>(header.to_str().unwrap()).unwrap());
                assert_eq!(root, expected_root);
                received.lock().unwrap().push((
                    uri.path().to_owned(),
                    serde_json::from_slice::<Value>(&body).unwrap(),
                ));
                if uri.path() == "/2/files/list_folder" {
                    reply(json!({
                        "entries":[{".tag":"folder","path_lower":format!("/{store}/store-log/31")}],
                        "has_more":true,
                        "cursor":"opaque",
                    }))
                } else {
                    reply(json!({"entries":[item],"has_more":false}))
                }
            }
        };
        let server = TestServer::new(Router::new().fallback(handler)).await;
        let mut storage = DropboxStorage::new(
            StorageConfig::Dropbox { location },
            session(CloudProvider::Dropbox),
        )
        .unwrap();
        storage.api = format!("{}/2", server.url);
        let storage = StorageConnection::from_provider(Arc::new(storage));
        let listed = storage
            .list(&ObjectPrefix::store_logs(store))
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].path, path);
        assert_eq!(
            *requests.lock().unwrap(),
            [
                list_request(&format!("/{store}/store-log")),
                (
                    "/2/files/list_folder/continue".into(),
                    json!({"cursor":"opaque"})
                ),
            ]
        );
    }
}

#[tokio::test]
async fn every_prefix_starts_at_its_folder_and_keeps_descendants() {
    let device = DeviceId(31);
    let other = DeviceId(32);
    let circle = Audience::Circle(CircleId(uuid::Uuid::from_bytes([0xcc; 16])));
    let paths = [
        ObjectPath::device_log(STORE_ID, device, 1.try_into().unwrap()),
        ObjectPath::device_log(STORE_ID, other, 1.try_into().unwrap()),
        ObjectPath::store_log(STORE_ID, device, 1.try_into().unwrap()),
        ObjectPath::store_log(STORE_ID, other, 1.try_into().unwrap()),
        ObjectPath::snapshot(STORE_ID, Audience::Store, device, 1.try_into().unwrap()),
        ObjectPath::snapshot(STORE_ID, Audience::Store, other, 1.try_into().unwrap()),
        ObjectPath::snapshot(STORE_ID, circle.clone(), device, 1.try_into().unwrap()),
        ObjectPath::positions(STORE_ID, device),
        ObjectPath::key_copy(STORE_ID, device, 1.try_into().unwrap()),
        ObjectPath::file(STORE_ID, device, FileId(uuid::Uuid::from_bytes([0xff; 16]))),
        ObjectPath::join_request(STORE_ID, InviteId(uuid::Uuid::from_bytes([0xaa; 16]))),
    ];
    let remote = Arc::new(Mutex::new(Remote::default()));
    for path in &paths {
        remote
            .lock()
            .unwrap()
            .objects
            .insert(path.absolute(), b"data".to_vec());
    }
    let server = ListingServer::new(remote).await;
    let storage = provider(&server.server.url);
    for prefix in [
        ObjectPrefix::all(STORE_ID),
        ObjectPrefix::device_logs(STORE_ID),
        ObjectPrefix::device_log(STORE_ID, device),
        ObjectPrefix::store_logs(STORE_ID),
        ObjectPrefix::store_log(STORE_ID, device),
        ObjectPrefix::snapshots(STORE_ID),
        ObjectPrefix::audience_snapshots(STORE_ID, &Audience::Store),
        ObjectPrefix::audience_snapshots(STORE_ID, &circle),
        ObjectPrefix::positions(STORE_ID),
        ObjectPrefix::keys(STORE_ID),
        ObjectPrefix::files(STORE_ID),
        ObjectPrefix::join_requests(STORE_ID),
    ] {
        let listed = storage.list(&prefix).await.unwrap();
        let expected: BTreeSet<_> = paths
            .iter()
            .filter(|path| prefix.contains(path))
            .cloned()
            .collect();
        assert_eq!(
            listed
                .iter()
                .map(|object| object.path.clone())
                .collect::<BTreeSet<_>>(),
            expected
        );
        assert!(listed.iter().all(|object| object.size == 4));
        let mut requests = vec![list_request(&format!(
            "/{}",
            prefix.as_str().trim_end_matches('/')
        ))];
        for page in 1..expected.len() {
            requests.push((
                "/2/files/list_folder/continue".into(),
                json!({"cursor":page.to_string()}),
            ));
        }
        assert_eq!(server.take_requests(), requests);
    }
}

#[tokio::test]
async fn store_log_listing_never_enumerates_unrelated_file_history() {
    let remote = Arc::new(Mutex::new(Remote::default()));
    let entry = ObjectPath::store_log(STORE_ID, DeviceId(31), 1.try_into().unwrap());
    remote
        .lock()
        .unwrap()
        .objects
        .insert(entry.absolute(), b"entry".to_vec());
    let server = ListingServer::new(remote.clone()).await;
    let storage = provider(&server.server.url);
    for count in [0, 2001] {
        {
            let mut state = remote.lock().unwrap();
            for index in 0..count {
                let path =
                    ObjectPath::file(STORE_ID, DeviceId(31), FileId(uuid::Uuid::from_u128(index)));
                state.objects.insert(path.absolute(), b"file".to_vec());
            }
            state.objects.insert(
                format!(
                    "/{}/positions/9",
                    StoreId(uuid::Uuid::from_bytes([0xbb; 16]))
                ),
                b"other store".to_vec(),
            );
            state.folders.insert("/unrelated".into());
        }
        let listed = storage
            .list(&ObjectPrefix::store_logs(STORE_ID))
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].path, entry);
        assert_eq!(
            server.take_requests(),
            [list_request(&format!("/{STORE_ID}/store-log"))]
        );
    }
}

#[tokio::test]
async fn absent_requested_folder_checks_namespace_without_listing_siblings() {
    let remote = Arc::new(Mutex::new(Remote::default()));
    remote.lock().unwrap().objects.insert(
        ObjectPath::positions(STORE_ID, DeviceId(31)).absolute(),
        b"positions".to_vec(),
    );
    let server = ListingServer::new(remote.clone()).await;
    let storage = provider(&server.server.url);
    let prefix = ObjectPrefix::store_log(STORE_ID, DeviceId(31));
    assert!(storage.list(&prefix).await.unwrap().is_empty());
    assert_eq!(
        server.take_requests(),
        [
            list_request(&format!("/{STORE_ID}/store-log/31")),
            (
                "/2/sharing/get_folder_metadata".into(),
                json!({"shared_folder_id":"namespace"})
            ),
        ]
    );
    remote.lock().unwrap().missing_namespace = true;
    assert_eq!(
        storage.list(&prefix).await.unwrap_err().failure(),
        StorageFailure::PermissionDenied
    );
}

#[tokio::test]
async fn scoped_listing_refuses_an_object_outside_the_requested_folder() {
    let path = ObjectPath::positions(STORE_ID, DeviceId(31));
    let item = file_metadata(&path.absolute(), b"positions", &Remote::default());
    let server = TestServer::new(Router::new().fallback(move || {
        let item = item.clone();
        async move { reply(json!({"entries":[item],"has_more":false})) }
    }))
    .await;
    assert_eq!(
        provider(&server.url)
            .list(&ObjectPrefix::store_logs(STORE_ID))
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Protocol
    );
}

#[tokio::test]
async fn empty_folder_succeeds_without_a_namespace_lookup() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let received = requests.clone();
    let server = TestServer::new(Router::new().fallback(move |uri: Uri, body: Bytes| {
        let received = received.clone();
        async move {
            received.lock().unwrap().push((
                uri.path().to_owned(),
                serde_json::from_slice::<Value>(&body).unwrap(),
            ));
            reply(json!({"entries":[],"has_more":false}))
        }
    }))
    .await;
    assert!(provider(&server.url)
        .list(&ObjectPrefix::positions(STORE_ID))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        *requests.lock().unwrap(),
        [list_request(&format!("/{STORE_ID}/positions"))]
    );
}

#[tokio::test]
async fn failed_continuation_is_not_an_empty_folder_or_partial_success() {
    for (status, body, failure) in [
        (
            409,
            json!({"error":{".tag":"path","path":{".tag":"not_found"}}}),
            StorageFailure::NotFound,
        ),
        (
            403,
            json!({"error":{".tag":"no_permission"}}),
            StorageFailure::PermissionDenied,
        ),
        (503, json!({}), StorageFailure::Network),
    ] {
        let path = ObjectPath::positions(STORE_ID, DeviceId(31));
        let item = file_metadata(&path.absolute(), b"positions", &Remote::default());
        let replies = Arc::new(Mutex::new(VecDeque::from([
            (
                200,
                json!({"entries":[item.clone()],"has_more":true,"cursor":"next"}),
            ),
            (status, body.clone()),
            (200, json!({"entries":[item],"has_more":false})),
        ])));
        let pending = replies.clone();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let received = requests.clone();
        let server = TestServer::new(Router::new().fallback(move |uri: Uri, body: Bytes| {
            let pending = pending.clone();
            let received = received.clone();
            async move {
                received.lock().unwrap().push((
                    uri.path().to_owned(),
                    serde_json::from_slice::<Value>(&body).unwrap(),
                ));
                let (status, body) = pending
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("unexpected request");
                response(status, body.to_string())
            }
        }))
        .await;
        let storage = provider(&server.url);
        let prefix = ObjectPrefix::positions(STORE_ID);
        let error = storage.list(&prefix).await.unwrap_err();
        assert_eq!(error.failure(), failure);
        let StorageError::Provider { source, .. } = error else {
            panic!("native cause missing")
        };
        let original = source.downcast_ref::<http::ProviderResponse>().unwrap();
        assert_eq!(original.status(), status);
        assert_eq!(
            serde_json::from_slice::<Value>(original.body()).unwrap(),
            body
        );
        let listed = storage.list(&prefix).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].path, path);
        assert!(replies.lock().unwrap().is_empty());
        assert_eq!(
            *requests.lock().unwrap(),
            [
                list_request(&format!("/{STORE_ID}/positions")),
                (
                    "/2/files/list_folder/continue".into(),
                    json!({"cursor":"next"})
                ),
                list_request(&format!("/{STORE_ID}/positions")),
            ]
        );
    }
}
