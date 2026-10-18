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
        let path = ObjectPath::file(store, DeviceId(31), FileId(uuid::Uuid::from_u128(1)));
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
                        "entries":[{".tag":"folder","path_lower":format!("/{store}/files/31")}],
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
        let listed = storage.list(&ObjectPrefix::files(store)).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].path, path);
        assert_eq!(
            *requests.lock().unwrap(),
            [
                list_request(&format!("/{store}/files")),
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
        ObjectPath::slot(STORE_ID, 1.try_into().unwrap()),
        ObjectPath::snapshot(STORE_ID, Audience::Store, device, 1.try_into().unwrap()),
        ObjectPath::snapshot(STORE_ID, circle, other, 1.try_into().unwrap()),
        ObjectPath::floor(STORE_ID),
        ObjectPath::file(STORE_ID, device, FileId(uuid::Uuid::from_u128(1))),
        ObjectPath::file(STORE_ID, other, FileId(uuid::Uuid::from_u128(1))),
        ObjectPath::join_request(STORE_ID, InviteId(uuid::Uuid::from_u128(1))),
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
        ObjectPrefix::files(STORE_ID),
        ObjectPrefix::device_files(STORE_ID, device),
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
async fn absent_requested_folder_checks_namespace_without_listing_siblings() {
    let remote = Arc::new(Mutex::new(Remote::default()));
    remote
        .lock()
        .unwrap()
        .objects
        .insert(ObjectPath::floor(STORE_ID).absolute(), b"floor".to_vec());
    let server = ListingServer::new(remote.clone()).await;
    let storage = provider(&server.server.url);
    let prefix = ObjectPrefix::device_files(STORE_ID, DeviceId(31));
    assert!(storage.list(&prefix).await.unwrap().is_empty());
    assert_eq!(
        server.take_requests(),
        [
            list_request(&format!("/{STORE_ID}/files/31")),
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
    let path = ObjectPath::floor(STORE_ID);
    let item = file_metadata(&path.absolute(), b"floor", &Remote::default());
    let server = TestServer::new(Router::new().fallback(move || {
        let item = item.clone();
        async move { reply(json!({"entries":[item],"has_more":false})) }
    }))
    .await;
    assert_eq!(
        provider(&server.url)
            .list(&ObjectPrefix::files(STORE_ID))
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
        .list(&ObjectPrefix::join_requests(STORE_ID))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        *requests.lock().unwrap(),
        [list_request(&format!("/{STORE_ID}/join-requests"))]
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
        let path = ObjectPath::join_request(STORE_ID, InviteId(uuid::Uuid::from_u128(1)));
        let item = file_metadata(&path.absolute(), b"join request", &Remote::default());
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
        let prefix = ObjectPrefix::join_requests(STORE_ID);
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
                list_request(&format!("/{STORE_ID}/join-requests")),
                (
                    "/2/files/list_folder/continue".into(),
                    json!({"cursor":"next"})
                ),
                list_request(&format!("/{STORE_ID}/join-requests")),
            ]
        );
    }
}

#[tokio::test]
async fn listings_skip_stray_files_and_folders_beside_shared_stores() {
    let state = Arc::new(Mutex::new(Remote::default()));
    state.lock().unwrap().folders.insert("/vacation".into());
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = Arc::new(provider(&server.url));
    assert!(storage
        .list(&ObjectPrefix::all(STORE_ID))
        .await
        .unwrap()
        .is_empty());
    state.lock().unwrap().missing_namespace = true;
    assert!(matches!(storage.list(&ObjectPrefix::all(STORE_ID)).await,
        Err(error) if error.failure() == StorageFailure::PermissionDenied));
    state.lock().unwrap().missing_namespace = false;
    crate::test_utils::Conformance::new(storage.clone(), STORE_ID)
        .shared_location(coven_foundation::id_source::StoreId(
            uuid::Uuid::from_bytes([0x20; 16]),
        ))
        .await;
    // Stray files and folders, including names close to a coven path, are
    // outside the layout (D10); the store's own objects still list.
    let write = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    let entry = ObjectPath::slot(STORE_ID, 2.try_into().unwrap());
    storage.create(&write, b"write").await.unwrap();
    storage.create(&entry, b"entry").await.unwrap();
    {
        let mut remote = state.lock().unwrap();
        for folder in ["files/031", "seq/new folder", "photos"] {
            remote.folders.insert(format!("/{STORE_ID}/{folder}"));
        }
        for stray in [
            "notes.txt",
            "seq/01",
            "seq/+1",
            "photos/1",
            "seq/readme",
            "seq/1/extra",
        ] {
            let path = format!("/{STORE_ID}/{stray}");
            remote.objects.insert(path, b"stray".to_vec());
        }
    }
    let listed = storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap();
    assert_eq!(
        listed.iter().map(|object| &object.path).collect::<Vec<_>>(),
        [&write, &entry]
    );
}

#[tokio::test]
async fn a_failed_page_fails_the_listing_rather_than_ending_it() {
    let server = TestServer::new(Router::new().fallback(|uri: Uri| async move {
        if uri.path().ends_with("/continue") {
            response(503, "{}")
        } else {
            reply(json!({"entries":[],"has_more":true,"cursor":"next"}))
        }
    }))
    .await;
    let storage = provider(&server.url);
    let result = storage.list(&ObjectPrefix::all(STORE_ID)).await;
    assert_eq!(result.unwrap_err().failure(), StorageFailure::Network);
}
