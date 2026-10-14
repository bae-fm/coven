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

#[path = "google_drive_metadata_tests.rs"]
mod metadata_tests;

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
    provider_for_device(url, DeviceId(31))
}
fn provider_for_device(url: &str, device: DeviceId) -> StorageConnection<GoogleDriveStorage> {
    let mut storage = GoogleDriveStorage::new(
        StorageConfig::GoogleDrive {
            folder_id: "folder".into(),
        },
        device,
        session(PROVIDER),
    )
    .unwrap();
    storage.api = format!("{url}/drive");
    storage.upload_api = format!("{url}/upload");
    StorageConnection::from_provider(Arc::new(storage))
}

fn tokens(access: GoogleDriveAccess) -> OAuthTokens {
    OAuthTokens {
        client_id: "test-client".into(),
        access: OAuthAccess::GoogleDrive(access),
        access_token: coven_crypto::SecretText::new("token".into()),
        refresh_token: None,
    }
}

#[tokio::test]
async fn known_object_status_and_reads_do_not_depend_on_listings() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::positions(STORE_ID, DeviceId(31));
    storage.replace(&path, b"first").await.unwrap();
    let before = storage.status(&path).await.unwrap().unwrap();
    state.lock().unwrap().hide_listings = true;
    assert!(storage
        .list(&ObjectPrefix::all(STORE_ID))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(storage.status(&path).await.unwrap(), Some(before));
    storage.replace(&path, b"other").await.unwrap();
    assert_eq!(
        storage.read_object(&path, None).await.unwrap().bytes,
        b"other"
    );
    assert_eq!(state.lock().unwrap().object_ids().len(), 1);
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
    let id = state
        .lock()
        .unwrap()
        .object_ids()
        .into_iter()
        .next()
        .unwrap();
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
        |url| {
            let storage = provider(url);
            storage.provider.root_checked.set(()).unwrap();
            Arc::new(storage)
        },
        json!({"files":[],"id":"folder","mimeType":"application/vnd.google-apps.folder"}),
    )
    .await;
}

fn duplicate(state: &mut Remote, path: &ObjectPath, id: &str, created: &str, device: u64) {
    let parent = state.directory(path.as_str().rsplit_once('/').unwrap().0);
    let mut value = metadata(
        json!({"id":id,"name":path.file_name(),"parents":[parent],"properties":{"covenDevice":device.to_string()}}),
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
    assert_eq!(
        storage.create(&path, b"data").await.unwrap_err().failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(
        state.lock().unwrap().object_ids(),
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
    assert_eq!(
        storage.create(&path, b"data").await.unwrap_err().failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(state.lock().unwrap().object_ids(), ["a-first"]);
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
        storage.create(&path, b"data").await.unwrap_err().failure(),
        StorageFailure::Network
    );
    assert!(state.lock().unwrap().files.contains_key("z-first"));
    assert_eq!(
        storage.create(&path, b"data").await.unwrap_err().failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(state.lock().unwrap().object_ids(), ["z-first"]);
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
async fn listing_refuses_a_folder_at_an_object_path() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let parent = state
        .lock()
        .unwrap()
        .directory(&format!("{STORE_ID}/devices/31"));
    state.lock().unwrap().files.insert("directory".into(), (metadata(json!({"id":"directory","name":"1","mimeType":"application/vnd.google-apps.folder","parents":[parent]}), 0), Vec::new()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
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
        ("createdTime", json!("not-time"), StorageFailure::Protocol),
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
async fn folder_visibility_errors_are_not_an_empty_store_or_a_missing_object() {
    for (access, status, expected) in [
        (
            GoogleDriveAccess::File,
            404,
            StorageFailure::GoogleDriveFolderNotVisible,
        ),
        (
            GoogleDriveAccess::File,
            403,
            StorageFailure::PermissionDenied,
        ),
        (
            GoogleDriveAccess::Full,
            404,
            StorageFailure::ContainerNotFound,
        ),
        (
            GoogleDriveAccess::Full,
            403,
            StorageFailure::PermissionDenied,
        ),
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
        storage.set_oauth_tokens(tokens(access)).await.unwrap();
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
        let parent = state
            .lock()
            .unwrap()
            .directory(&format!("{STORE_ID}/devices/31"));
        state.lock().unwrap().files.insert(
            "native".into(),
            (
                metadata(
                    json!({"id":"native","name":"1","mimeType":kind,"parents":[parent]}),
                    0,
                ),
                Vec::new(),
            ),
        );
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
        let storage = provider(&server.url);
        storage.provider.folder_ids.lock().await.extend([
            (
                STORE_ID.to_string(),
                std::collections::BTreeSet::from([(
                    coven_format::value::StorageTime::new(0, 0).unwrap(),
                    "store".into(),
                )]),
            ),
            (
                format!("{STORE_ID}/devices"),
                std::collections::BTreeSet::from([(
                    coven_format::value::StorageTime::new(0, 0).unwrap(),
                    "devices".into(),
                )]),
            ),
            (
                format!("{STORE_ID}/devices/31"),
                std::collections::BTreeSet::from([(
                    coven_format::value::StorageTime::new(0, 0).unwrap(),
                    "writer".into(),
                )]),
            ),
        ]);
        assert_eq!(
            storage
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

#[tokio::test]
async fn replacement_on_a_new_connection_removes_its_own_duplicate_copies() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let path = ObjectPath::positions(STORE_ID, DeviceId(31));
    {
        let mut remote = state.lock().unwrap();
        duplicate(&mut remote, &path, "first", "2026-10-06T00:00:00Z", 31);
        duplicate(&mut remote, &path, "later", "2026-10-06T00:00:01Z", 31);
    }
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage.replace(&path, b"next").await.unwrap();
    assert_eq!(state.lock().unwrap().object_ids(), ["first"]);
    assert_eq!(storage.read(&path).await.unwrap(), b"next");
}

#[tokio::test]
async fn failed_duplicate_cleanup_does_not_leave_a_provisional_replacement_id() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let path = ObjectPath::positions(STORE_ID, DeviceId(31));
    let remote = state.clone();
    let destination = path.clone();
    let reject_delete = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let remote = remote.clone();
            let path = destination.clone();
            let reject = reject_delete.clone();
            async move {
                if method == Method::DELETE
                    && reject.swap(false, std::sync::atomic::Ordering::SeqCst)
                {
                    return response(503, "duplicate deletion failed");
                }
                let reply = respond(&remote, &method, &uri, &headers, &body);
                if method == Method::POST && uri.path() == "/upload/files" {
                    duplicate(
                        &mut remote.lock().unwrap(),
                        &path,
                        "earlier",
                        "2026-10-05T00:00:00Z",
                        31,
                    );
                }
                reply
            }
        },
    ))
    .await;
    let storage = provider(&server.url);
    assert_eq!(
        storage.replace(&path, b"next").await.unwrap_err().failure(),
        StorageFailure::Network
    );
    assert!(!storage.provider.object_ids.lock().await.contains_key(&path));
    storage.replace(&path, b"next").await.unwrap();
    assert_eq!(state.lock().unwrap().object_ids(), ["earlier"]);
    assert_eq!(storage.read(&path).await.unwrap(), b"next");
}

#[tokio::test]
async fn upload_confirmation_uses_its_returned_id_when_search_omits_the_publication() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let remote = state.clone();
    let server = TestServer::new(Router::new().fallback(
        move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
            let remote = remote.clone();
            async move {
                if crate::providers::tests::query(&uri)
                    .get("q")
                    .is_some_and(|q| q.contains("name = '1'"))
                {
                    reply(json!({"files":[]}))
                } else {
                    respond(&remote, &method, &uri, &headers, &body)
                }
            }
        },
    ))
    .await;
    let storage = provider(&server.url);
    let path = ObjectPath::store_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    storage.create(&path, b"data").await.unwrap();
    assert_eq!(
        storage.create(&path, b"data").await.unwrap_err().failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
    assert_eq!(state.lock().unwrap().object_ids().len(), 1);
    storage.delete(&path).await.unwrap();
    assert!(state.lock().unwrap().object_ids().is_empty());
}

/// Live Drive checks. Run with `COVEN_LIVE_DRIVE_ENV=<file> cargo test -p
/// coven-storage --all-features --lib google_drive::tests::live -- --ignored --nocapture`.
#[cfg(not(any(target_os = "ios", target_os = "android")))]
#[path = "google_drive_live.rs"]
mod live;

#[tokio::test]
async fn narrow_drive_access_works_until_folder_sharing_requires_full_access() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    storage
        .set_oauth_tokens(tokens(GoogleDriveAccess::File))
        .await
        .unwrap();
    let path = ObjectPath::device_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    storage.create(&path, b"personal").await.unwrap();
    let before = storage.status(&path).await.unwrap().unwrap();
    assert_eq!(
        storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap(),
        std::slice::from_ref(&before)
    );
    let required = OAuthAccess::GoogleDrive(GoogleDriveAccess::Full);
    assert!(
        matches!(storage.grant_access("member@example.test").await, Err(error) if error.failure() == StorageFailure::AccessNotGranted { required })
    );
    assert!(state.lock().unwrap().permission_mutations.is_empty());
    state.lock().unwrap().shared = true;
    assert!(
        matches!(storage.list(&ObjectPrefix::all(STORE_ID)).await, Err(error) if error.failure() == StorageFailure::AccessNotGranted { required })
    );
    storage
        .set_oauth_tokens(tokens(GoogleDriveAccess::Full))
        .await
        .unwrap();
    assert_eq!(
        storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap(),
        std::slice::from_ref(&before)
    );
    assert_eq!(
        storage.read_object(&path, None).await.unwrap().metadata,
        before
    );
    assert_eq!(storage.read(&path).await.unwrap(), b"personal");
    storage.grant_access("member@example.test").await.unwrap();
    assert_eq!(state.lock().unwrap().permission_mutations.len(), 1);
}

#[tokio::test]
async fn narrow_drive_access_requires_full_access_for_a_shared_drive_folder() {
    let server = TestServer::new(Router::new().fallback(|uri: Uri| async move {
        assert_eq!(uri.path(), "/drive/files/folder");
        reply(json!({
            "id": "folder",
            "mimeType": "application/vnd.google-apps.folder",
            "driveId": "shared-drive"
        }))
    }))
    .await;
    let storage = provider(&server.url);
    storage
        .set_oauth_tokens(tokens(GoogleDriveAccess::File))
        .await
        .unwrap();
    assert_eq!(
        storage
            .list(&ObjectPrefix::all(STORE_ID))
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::AccessNotGranted {
            required: OAuthAccess::GoogleDrive(GoogleDriveAccess::Full)
        }
    );
}

#[tokio::test]
async fn folder_visibility_uses_the_grant_sent_with_the_request() {
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let server = TestServer::new(Router::new().fallback({
        let started = started.clone();
        let release = release.clone();
        move |uri: Uri| {
            let started = started.clone();
            let release = release.clone();
            async move {
                assert_eq!(uri.path(), "/drive/files/folder");
                started.notify_one();
                release.notified().await;
                response(404, "native-folder-cause")
            }
        }
    }))
    .await;
    let storage = Arc::new(provider(&server.url));
    storage
        .set_oauth_tokens(tokens(GoogleDriveAccess::File))
        .await
        .unwrap();
    let listing = tokio::spawn({
        let storage = storage.clone();
        async move { storage.list(&ObjectPrefix::all(STORE_ID)).await }
    });
    started.notified().await;
    storage
        .set_oauth_tokens(tokens(GoogleDriveAccess::Full))
        .await
        .unwrap();
    release.notify_one();
    assert_eq!(
        listing.await.unwrap().unwrap_err().failure(),
        StorageFailure::GoogleDriveFolderNotVisible
    );
}
