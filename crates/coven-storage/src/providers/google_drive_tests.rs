use super::*;
use crate::providers::tests::google_drive::{endpoint, metadata, respond, Remote};
use crate::providers::tests::{json as reply, response, session, TestServer};
use crate::test_utils::STORE_ID;
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, Response, Uri},
    Router,
};
use std::sync::{Arc, Mutex};

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
fn provider(url: &str) -> StorageConnection<GoogleDriveStorage> {
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
    StorageConnection::from_provider(Arc::new(storage))
}

#[tokio::test]
async fn session_reopens_and_only_uploader_deletes() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let storage = Arc::new(storage);
    crate::test_utils::Conformance::new(storage.clone(), STORE_ID)
        .lost_part_reply(8 * 1024 * 1024, async {
            state.lock().unwrap().fail_reply = true;
        })
        .await;
    let path = ObjectPath::device_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    let mut upload = storage.begin_upload(&path, 1).await.unwrap();
    let before_final = upload.encode().unwrap();
    storage.upload_part(&mut upload, b"z").await.unwrap();
    let mut lost_reply = UploadSession::decode(before_final.as_bytes()).unwrap();
    storage.resume_upload(&mut lost_reply).await.unwrap();
    assert!(lost_reply.is_complete());
    let id = state.lock().unwrap().files.keys().next().unwrap().clone();
    state.lock().unwrap().files.get_mut(&id).unwrap().0["ownedByMe"] = json!(false);
    storage.delete(&path).await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::all(STORE_ID))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(state.lock().unwrap().files[&id].1.len(), 1);
}
#[tokio::test]
async fn two_uploads_cannot_publish_different_bytes_at_one_path() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    let first = provider(&server.url);
    let second = provider(&server.url);
    let path = ObjectPath::file(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        coven_foundation::id_source::FileId(uuid::Uuid::from_bytes([0xdd; 16])),
    );
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
    let path = ObjectPath::file(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        coven_foundation::id_source::FileId(uuid::Uuid::from_bytes([0xee; 16])),
    );
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
        json!({"files":[],"id":"folder","mimeType":"application/vnd.google-apps.folder"}),
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
    let path = ObjectPath::store_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
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
    let path = ObjectPath::store_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
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
    let path = ObjectPath::store_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
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

#[tokio::test]
async fn first_positions_write_uses_one_content_request() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::positions(STORE_ID, DeviceId(31));
    let data = b"encrypted\0\xff\r\n--coven-upload-0\r\n--coven-upload-1\r\n";
    storage.replace(&path, data).await.unwrap();
    assert_eq!(state.lock().unwrap().single_uploads, 1);
    assert!(state.lock().unwrap().uploads.is_empty());
    assert_eq!(storage.read(&path).await.unwrap(), data);
    storage.replace(&path, b"next").await.unwrap();
    assert_eq!(state.lock().unwrap().single_uploads, 1);
    assert_eq!(storage.read(&path).await.unwrap(), b"next");
}

#[tokio::test]
async fn abort_retries_after_cancellation_and_preserves_published_objects() {
    let state = Arc::new(Mutex::new(Remote::default()));
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
    let unconfirmed = UploadSession::decode(recorded.as_bytes()).unwrap();
    storage.abort_upload(&unconfirmed).await.unwrap();
    storage.abort_upload(&unconfirmed).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
}

#[tokio::test]
async fn create_switches_to_a_session_above_the_multipart_request_limit() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    assert_eq!(storage.single_request_limit(), 5_000_000);
    let path = ObjectPath::device_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let mut bytes = vec![7; storage.single_request_limit() as usize];
    storage.create(&path, &bytes).await.unwrap();
    assert_eq!(state.lock().unwrap().single_uploads, 1);
    storage.delete(&path).await.unwrap();
    bytes.push(8);
    storage.create(&path, &bytes).await.unwrap();
    assert_eq!(state.lock().unwrap().single_uploads, 1);
    assert_eq!(storage.read(&path).await.unwrap(), bytes);
}

#[tokio::test]
async fn listing_refuses_a_folder_even_when_its_name_is_an_object_path() {
    let state = Arc::new(Mutex::new(Remote::default()));
    state.lock().unwrap().files.insert("directory".into(), (metadata(json!({"id":"directory","name":"10101010-1010-1010-1010-101010101010/devices/31/1","mimeType":"application/vnd.google-apps.folder","parents":["folder"]}), 0), Vec::new()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    assert_eq!(state.lock().unwrap().files.len(), 1);
    assert!(matches!(
        storage.list(&ObjectPrefix::all(STORE_ID)).await,
        Err(error) if error.failure() == StorageFailure::InvalidPath
    ));
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

#[tokio::test]
async fn listing_uses_the_same_copy_as_reads_and_refuses_missing_metadata() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let path = ObjectPath::store_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    {
        let mut remote = state.lock().unwrap();
        duplicate(&mut remote, &path, "z-first", "2026-10-06T00:00:00Z", 31);
        duplicate(&mut remote, &path, "a-later", "2026-10-06T00:00:01Z", 31);
        remote.files.get_mut("a-later").unwrap().0["size"] = json!("9");
    }
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    assert_eq!(
        storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap()[0].size,
        4
    );
    state.lock().unwrap().files.get_mut("a-later").unwrap().0["createdTime"] =
        json!("2026-10-06T00:00:00Z");
    assert_eq!(
        storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap()[0].size,
        9
    );
    for (field, invalid, failure) in [
        ("size", json!(null), StorageFailure::Protocol),
        ("size", json!("-1"), StorageFailure::Protocol),
        ("createdTime", json!(null), StorageFailure::Protocol),
        ("createdTime", json!("not-time"), StorageFailure::Encoding),
    ] {
        let old = state.lock().unwrap().files["a-later"].0[field].clone();
        state.lock().unwrap().files.get_mut("a-later").unwrap().0[field] = invalid;
        assert_eq!(
            storage
                .list(&ObjectPrefix::all(STORE_ID))
                .await
                .unwrap_err()
                .failure(),
            failure
        );
        state.lock().unwrap().files.get_mut("a-later").unwrap().0[field] = old;
    }
}

#[path = "google_drive_access_tests.rs"]
mod access_tests;

#[tokio::test]
async fn missing_container_is_not_an_empty_store_or_a_missing_object() {
    for (status, expected) in [
        (404, StorageFailure::ContainerNotFound),
        (403, StorageFailure::PermissionDenied),
    ] {
        let server = TestServer::new(Router::new().fallback(move |uri: Uri| async move {
            if uri.path().ends_with("/folder") {
                response(status, "native-folder-cause")
            } else if uri.path().ends_with("/files") {
                reply(json!({"files":[]}))
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
        if uri.path().ends_with("/folder") {
            reply(
                json!({"id":"folder","mimeType":"application/vnd.google-apps.folder","folder":{}}),
            )
        } else if uri.path().ends_with("/files") {
            reply(json!({"files":[]}))
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
    let state = Arc::new(Mutex::new(Remote::default()));
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
    assert!(errors
        .iter()
        .all(|error| matches!(error, StorageError::Provider { .. })));
}

#[tokio::test]
async fn incomplete_search_and_invalid_pages_never_describe_an_empty_store() {
    for page in [
        json!({"files":[],"incompleteSearch":true}),
        json!({"files":[],"incompleteSearch":"false"}),
        json!({"files":[],"nextPageToken":""}),
        json!({"files":[],"nextPageToken":"repeat"}),
    ] {
        let server = TestServer::new(Router::new().fallback(move |uri: Uri| {
            let page = page.clone();
            async move {
                if uri.path().ends_with("/folder") {
                    reply(json!({"id":"folder","mimeType":"application/vnd.google-apps.folder"}))
                } else {
                    reply(page)
                }
            }
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
async fn listing_refuses_native_documents_and_shortcuts_named_like_objects() {
    for kind in [
        "application/vnd.google-apps.document",
        "application/vnd.google-apps.shortcut",
    ] {
        let state = Arc::new(Mutex::new(Remote::default()));
        state.lock().unwrap().files.insert("native".into(),(metadata(json!({"id":"native","name":"10101010-1010-1010-1010-101010101010/devices/31/1","mimeType":kind,"parents":["folder"]}),0),Vec::new()));
        let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
        assert!(matches!(
            provider(&server.url).list(&ObjectPrefix::all(STORE_ID)).await,
            Err(error) if error.failure() == StorageFailure::InvalidPath
        ));
    }
}

#[tokio::test]
async fn upload_start_refuses_empty_ids_and_cross_origin_session_urls() {
    for empty_id in [true, false] {
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = starts.clone();
        let server =
            TestServer::new(Router::new().fallback(move |uri: Uri, headers: HeaderMap| {
                let counted = counted.clone();
                async move {
                    match uri.path() {
                        "/drive/files/folder" => reply(
                            json!({"id":"folder","mimeType":"application/vnd.google-apps.folder"}),
                        ),
                        "/drive/files" => reply(json!({"files":[]})),
                        "/drive/files/generateIds" => {
                            reply(json!({"ids":[if empty_id {""}else{"generated"}]}))
                        }
                        "/upload/files" => {
                            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            Response::builder()
                                .header(
                                    "Location",
                                    if empty_id {
                                        format!(
                                            "http://{}/session",
                                            headers["host"].to_str().unwrap()
                                        )
                                    } else {
                                        "https://unrelated.invalid/session".into()
                                    },
                                )
                                .body(Body::empty())
                                .unwrap()
                        }
                        other => panic!("unexpected request {other}"),
                    }
                }
            }))
            .await;
        let path = ObjectPath::device_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
        assert_eq!(
            provider(&server.url)
                .begin_upload(&path, 4)
                .await
                .err()
                .unwrap()
                .failure(),
            StorageFailure::Protocol
        );
        assert_eq!(
            starts.load(std::sync::atomic::Ordering::SeqCst),
            usize::from(!empty_id)
        );
    }
}

/// Live Drive checks. Run with `COVEN_LIVE_DRIVE_ENV=<file> cargo test -p
/// coven-storage --all-features --lib drive_live -- --ignored --nocapture`.
#[cfg(not(any(target_os = "ios", target_os = "android")))]
#[path = "google_drive_live.rs"]
mod live;
