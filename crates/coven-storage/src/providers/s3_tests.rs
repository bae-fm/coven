use super::*;
use crate::providers::tests::{query, read, response, TestServer};
use crate::{check_provider, StorageCheck};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, Response, Uri},
    Router,
};
use coven_crypto::SecretText;
use coven_foundation::{clock::FixedClock, id_source::SequentialIds};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::SystemTime,
};
struct PendingUpload {
    key: String,
    token: String,
    parts: BTreeMap<u32, Vec<u8>>,
}
#[derive(Default)]
struct Remote {
    objects: BTreeMap<String, Vec<u8>>,
    uploads: BTreeMap<String, PendingUpload>,
    next_upload: u64,
    part_pages: usize,
    metadata: BTreeMap<String, String>,
    fail_part_reply: bool,
    lose_create_reply: bool,
    refuse_delete: bool,
    deletions: usize,
    fail_completion_reply: bool,
    setup_barrier: Option<Arc<tokio::sync::Barrier>>,
    broken_check: Option<StorageCheck>,
    forced_error: Option<(&'static str, u16)>,
}
async fn endpoint(
    State(state): State<Arc<Mutex<Remote>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let barrier = if query(&uri).contains_key("list-type") {
        let remote = state.lock().unwrap();
        if remote.objects.is_empty() {
            remote.setup_barrier.clone()
        } else {
            None
        }
    } else {
        None
    };
    let reply = respond(&state, method, uri, headers, body);
    if let Some(barrier) = barrier {
        barrier.wait().await;
    }
    reply
}
fn respond(
    state: &Mutex<Remote>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let mut state = state.lock().unwrap();
    if let Some((code, status)) = state.forced_error {
        return response(
            status,
            format!("<Error><Code>{code}</Code><Message>credential-echo</Message></Error>"),
        );
    }
    let q = query(&uri);
    let key = uri.path().strip_prefix("/bucket/").unwrap_or("").to_owned();
    if q.contains_key("uploads") {
        state.next_upload += 1;
        let id = state.next_upload.to_string();
        state.uploads.insert(
            id.clone(),
            PendingUpload {
                key,
                token: headers["x-amz-meta-coven-upload"].to_str().unwrap().into(),
                parts: BTreeMap::new(),
            },
        );
        return response(200,format!("<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"));
    }
    if let Some(id) = q.get("uploadId") {
        let Some(upload) = state.uploads.get(id) else {
            return response(404, "<Error><Code>NoSuchUpload</Code></Error>");
        };
        assert_eq!(upload.key, key);
        if method == Method::PUT {
            let number = q["partNumber"].parse().unwrap();
            state
                .uploads
                .get_mut(id)
                .unwrap()
                .parts
                .insert(number, body.to_vec());
            if std::mem::replace(&mut state.fail_part_reply, false) {
                return response(503, "<Error><Code>ServiceUnavailable</Code></Error>");
            }
            return Response::builder()
                .header("etag", format!("\"part-{number}\""))
                .body(Body::empty())
                .unwrap();
        }
        if method == Method::GET {
            state.part_pages += 1;
            let after = q
                .get("part-number-marker")
                .map(|value| value.parse::<u32>().unwrap())
                .unwrap_or(0);
            let parts = &state.uploads[id].parts;
            let remaining = parts
                .iter()
                .filter(|(number, _)| **number > after)
                .collect::<Vec<_>>();
            let (part, next) = match remaining.first() {
                Some((n,b)) => (format!("<Part><PartNumber>{n}</PartNumber><ETag>\"part-{n}\"</ETag><Size>{}</Size></Part>",b.len()), if remaining.len() > 1 {format!("<IsTruncated>true</IsTruncated><NextPartNumberMarker>{n}</NextPartNumberMarker>")} else {"<IsTruncated>false</IsTruncated>".into()}),
                None => (String::new(), "<IsTruncated>false</IsTruncated>".into()),
            };
            return response(
                200,
                format!("<ListPartsResult>{next}{part}</ListPartsResult>"),
            );
        }
        if method == Method::POST {
            assert_eq!(headers["if-none-match"], "*");
            if state.objects.contains_key(&key) {
                return response(412, "<Error><Code>PreconditionFailed</Code></Error>");
            }
            let upload = state.uploads.remove(id).unwrap();
            state.metadata.insert(key.clone(), upload.token);
            state
                .objects
                .insert(key, upload.parts.into_values().flatten().collect());
            if std::mem::replace(&mut state.fail_completion_reply, false) {
                return response(503, "<Error><Code>ServiceUnavailable</Code></Error>");
            }
            return response(200,"<CompleteMultipartUploadResult><ETag>\"complete\"</ETag></CompleteMultipartUploadResult>");
        }
        assert_eq!(method, Method::DELETE);
        state.uploads.remove(id);
        return response(204, Vec::new());
    }
    if q.contains_key("list-type") {
        if state.broken_check == Some(StorageCheck::List) {
            return response(
                200,
                "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>",
            );
        }
        let prefix = &q["prefix"];
        let after = q.get("continuation-token");
        let entries: Vec<_> = state
            .objects
            .keys()
            .filter(|k| k.starts_with(prefix) && after.is_none_or(|after| *k > after))
            .collect();
        let next = if entries.len() > 1 {
            format!(
                "<IsTruncated>true</IsTruncated><NextContinuationToken>{}</NextContinuationToken>",
                entries[0]
            )
        } else {
            "<IsTruncated>false</IsTruncated>".into()
        };
        let entry = entries
            .first()
            .map(|key| format!("<Contents><Key>{key}</Key><Size>{}</Size><LastModified>2026-10-06T00:00:00Z</LastModified></Contents>",state.objects[*key].len()))
            .unwrap_or_default();
        return response(
            200,
            format!("<ListBucketResult>{entry}{next}</ListBucketResult>"),
        );
    }
    match method {
        Method::PUT => {
            if state.broken_check == Some(StorageCheck::Create) {
                return response(403, "<Error><Code>AccessDenied</Code></Error>");
            }
            if headers.get("if-none-match").is_some()
                && state.objects.contains_key(&key)
                && state.broken_check != Some(StorageCheck::CreateOnce)
            {
                return response(412, "<Error><Code>PreconditionFailed</Code></Error>");
            }
            state.objects.insert(key, body.to_vec());
            if std::mem::replace(&mut state.lose_create_reply, false) {
                return response(503, "<Error><Code>ServiceUnavailable</Code></Error>");
            }
            response(200, Vec::new())
        }
        Method::GET => match state.objects.get(&key) {
            Some(bytes) if state.broken_check == Some(StorageCheck::Read) => {
                let mut damaged = bytes.clone();
                damaged[0] ^= 1;
                read(&damaged, &headers)
            }
            Some(bytes) if state.broken_check == Some(StorageCheck::ReadRange) => {
                response(200, bytes.clone())
            }
            Some(bytes) => read(bytes, &headers),
            None => response(404, "<Error><Code>NoSuchKey</Code></Error>"),
        },
        Method::HEAD => match state.objects.get(&key) {
            Some(bytes) => Response::builder()
                .header("content-length", bytes.len())
                .header(
                    "x-amz-meta-coven-upload",
                    state
                        .metadata
                        .get(&key)
                        .map(String::as_str)
                        .unwrap_or("ordinary"),
                )
                .body(Body::empty())
                .unwrap(),
            None => response(404, Vec::new()),
        },
        Method::DELETE => {
            state.deletions += 1;
            if state.refuse_delete {
                return response(403, "<Error><Code>AccessDenied</Code></Error>");
            }
            if state.broken_check != Some(StorageCheck::Delete) {
                state.objects.remove(&key);
            }
            response(204, Vec::new())
        }
        _ => panic!("unexpected S3 request: {uri}"),
    }
}
fn provider(url: &str) -> S3Storage {
    S3Storage::new(
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some(url.parse().unwrap()),
            prefix: "store".into(),
        },
        S3Credentials {
            access_key_id: "access".into(),
            secret_access_key: SecretText::new("secret".into()),
        },
        Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
        Arc::new(SequentialIds::new()),
    )
    .unwrap()
}
#[tokio::test]
async fn real_s3_client_conforms_with_pagination() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    crate::test_utils::Conformance::new(Arc::new(provider(&server.url)))
        .run()
        .await
        .unwrap();
}
#[tokio::test]
async fn multipart_recovers_lost_part_and_completion_replies() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    assert_eq!(storage.single_request_limit(), 5 * 1024 * 1024 * 1024);
    let path = ObjectPath::file(
        coven_foundation::id_source::DeviceId(31),
        coven_foundation::id_source::FileId(uuid::Uuid::from_bytes([0xaa; 16])),
    );
    let mut session = storage
        .begin_upload(&path, 8 * 1024 * 1024 + 1)
        .await
        .unwrap();
    let recorded = session.encode().unwrap();
    state.lock().unwrap().fail_part_reply = true;
    assert!(storage
        .upload_part(&mut session, &vec![7; 8 * 1024 * 1024])
        .await
        .is_err());
    drop(session);
    let mut session = UploadSession::decode(recorded.as_bytes()).unwrap();
    let reopened = provider(&server.url);
    reopened.resume_upload(&mut session).await.unwrap();
    assert_eq!(session.confirmed, 8 * 1024 * 1024);
    reopened.upload_part(&mut session, b"z").await.unwrap();
    let last = session.encode().unwrap();
    reopened.finish_upload(&mut session).await.unwrap();
    let mut session = UploadSession::decode(last.as_bytes()).unwrap();
    reopened.resume_upload(&mut session).await.unwrap();
    assert!(session.is_complete());
    assert_eq!(
        reopened
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
async fn errors_and_manual_key_instructions() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    for (code, status, failure) in [
        ("InvalidAccessKeyId", 403, StorageFailure::Authentication),
        ("AccessDenied", 403, StorageFailure::PermissionDenied),
        ("NoSuchBucket", 404, StorageFailure::ContainerNotFound),
        ("NoSuchUpload", 404, StorageFailure::SessionExpired),
        ("InvalidRange", 416, StorageFailure::InvalidRange),
        ("PermanentRedirect", 301, StorageFailure::RegionMismatch),
        ("QuotaExceeded", 403, StorageFailure::QuotaExceeded),
        ("SlowDown", 503, StorageFailure::RateLimited),
        ("ServiceUnavailable", 503, StorageFailure::Network),
    ] {
        state.lock().unwrap().forced_error = Some((code, status));
        let error = storage.list(&ObjectPrefix::all()).await.unwrap_err();
        assert_eq!(error.failure(), failure);
        assert!(!format!("{error:?} {error}").contains("credential-echo"));
    }
    assert!(matches!(
        storage.grant_access("member").await.unwrap(),
        AccessGrant::CreateAccessKey
    ));
    let MemberRemoval::DeleteAccessKey { access_key_id } = storage
        .revoke_access(&MemberAccess::S3AccessKey {
            access_key_id: "member-key".into(),
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(access_key_id, "member-key");
    assert_eq!(storage.sign_out(), ProviderSignOut::ReplaceAccessKey);
}

#[tokio::test]
async fn multipart_sizes_parts_for_the_promised_object() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    let storage = provider(&server.url);
    let path = ObjectPath::file(
        coven_foundation::id_source::DeviceId(31),
        coven_foundation::id_source::FileId(uuid::Uuid::from_bytes([0xdd; 16])),
    );
    let total = 1024u64.pow(4);
    let upload = storage.begin_upload(&path, total).await.unwrap();
    assert!(total.div_ceil(upload.part_size() as u64) <= 10_000);
    assert!(upload.part_size() >= 5 * 1024 * 1024);
    storage.abort_upload(&upload).await.unwrap();
}

#[tokio::test]
async fn setup_refuses_an_unrelated_object_in_the_location() {
    let state = Arc::new(Mutex::new(Remote::default()));
    state
        .lock()
        .unwrap()
        .objects
        .insert("store/unrelated".into(), b"data".to_vec());
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::store_log(
        coven_foundation::id_source::DeviceId(1),
        std::num::NonZeroU64::MIN,
    );
    assert!(matches!(
        storage.setup(&path, b"first entry").await.unwrap_err(),
        StorageSetupError::LocationOccupied
    ));
    assert_eq!(state.lock().unwrap().objects.len(), 1);
}

#[tokio::test]
async fn missing_session_and_destination_is_expired() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::file(
        coven_foundation::id_source::DeviceId(31),
        coven_foundation::id_source::FileId(uuid::Uuid::from_bytes([0xff; 16])),
    );
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    state.lock().unwrap().uploads.clear();
    let error = storage.resume_upload(&mut upload).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::SessionExpired);
    assert!(std::error::Error::source(&error).is_some());
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
async fn abort_retries_and_preserves_an_object_after_a_lost_completion_reply() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let upload = storage.begin_upload(&path, 4).await.unwrap();
    storage.abort_upload(&upload).await.unwrap();
    storage.abort_upload(&upload).await.unwrap();
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    state.lock().unwrap().fail_completion_reply = true;
    assert!(storage.finish_upload(&mut upload).await.is_err());
    storage.abort_upload(&upload).await.unwrap();
    storage.abort_upload(&upload).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
}

#[tokio::test]
async fn simultaneous_setups_expose_both_first_entries_for_sync() {
    use coven_foundation::id_source::DeviceId;
    let state = Arc::new(Mutex::new(Remote::default()));
    state.lock().unwrap().setup_barrier = Some(Arc::new(tokio::sync::Barrier::new(2)));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let first = ObjectPath::store_log(DeviceId(31), std::num::NonZeroU64::MIN);
    let other = ObjectPath::store_log(DeviceId(32), std::num::NonZeroU64::MIN);
    let (left, right) = tokio::join!(
        storage.setup(&first, b"first store"),
        storage.setup(&other, b"second store")
    );
    assert_eq!(left.unwrap(), storage.config());
    assert_eq!(right.unwrap(), storage.config());
    assert_eq!(
        storage
            .list(&ObjectPrefix::store_logs())
            .await
            .unwrap()
            .into_iter()
            .map(|object| object.path)
            .collect::<Vec<_>>(),
        [first, other]
    );
    assert_eq!(state.lock().unwrap().objects.len(), 2);
}

#[tokio::test]
async fn provider_check_cleans_up_after_a_lost_create_reply_and_retains_both_failures() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    state.lock().unwrap().lose_create_reply = true;
    assert!(
        matches!(check_provider(&storage, &path, b"sealed test bytes")
            .await
            .unwrap_err(), StorageSetupError::ProviderCheck { check: StorageCheck::Create, source } if source.failure() == StorageFailure::Network)
    );
    assert!(state.lock().unwrap().objects.is_empty());
    assert_eq!(state.lock().unwrap().deletions, 1);
    state.lock().unwrap().lose_create_reply = true;
    state.lock().unwrap().refuse_delete = true;
    let StorageSetupError::ProviderCheck {
        check: StorageCheck::Create,
        source: StorageError::Cleanup { operation, cleanup },
    } = check_provider(&storage, &path, b"sealed test bytes")
        .await
        .unwrap_err()
    else {
        panic!("both failures must reach the caller")
    };
    assert_eq!(operation.failure(), StorageFailure::Network);
    assert_eq!(cleanup.failure(), StorageFailure::PermissionDenied);
    assert!(matches!(*operation, StorageError::Provider { .. }));
    assert!(matches!(*cleanup, StorageError::Provider { .. }));
    state.lock().unwrap().refuse_delete = false;
    storage.delete(&path).await.unwrap();
    check_provider(&storage, &path, b"sealed test bytes")
        .await
        .unwrap();
    assert!(state.lock().unwrap().objects.is_empty());
    storage.create(&path, b"preexisting").await.unwrap();
    let deletes = state.lock().unwrap().deletions;
    assert!(
        matches!(check_provider(&storage, &path, b"sealed test bytes")
            .await
            .unwrap_err(), StorageSetupError::ProviderCheck { check: StorageCheck::Create, source } if source.failure() == StorageFailure::AlreadyExists)
    );
    assert_eq!(state.lock().unwrap().deletions, deletes);
    assert_eq!(storage.read(&path).await.unwrap(), b"preexisting");
}

#[tokio::test]
async fn listing_retains_server_time_and_size_across_pages_and_retries() {
    let state = Arc::new(Mutex::new(Remote::default()));
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
async fn multipart_publication_and_abort_are_bound_to_the_native_session() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let mut first = storage.begin_upload(&path, 4).await.unwrap();
    let mut other = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut first, b"data").await.unwrap();
    storage.upload_part(&mut other, b"else").await.unwrap();
    state.lock().unwrap().fail_completion_reply = true;
    assert_eq!(
        storage
            .finish_upload(&mut first)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Network
    );
    storage.resume_upload(&mut first).await.unwrap();
    assert!(first.is_complete());
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
    assert!(matches!(
        storage.finish_upload(&mut other).await,
        Err(error) if error.failure() == StorageFailure::AlreadyExists
    ));
    storage.abort_upload(&other).await.unwrap();
    storage.abort_upload(&other).await.unwrap();
    assert!(matches!(
        storage.resume_upload(&mut other).await,
        Err(error) if error.failure() == StorageFailure::AlreadyExists
    ));
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
    assert!(state.lock().unwrap().uploads.is_empty());
}

#[tokio::test]
async fn resumed_multipart_parts_follow_every_page_before_advancing() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let mut upload = storage
        .begin_upload(&path, 8 * 1024 * 1024 + 1)
        .await
        .unwrap();
    let mut recorded = upload.clone();
    storage
        .upload_part(&mut upload, &vec![1; 8 * 1024 * 1024])
        .await
        .unwrap();
    storage.upload_part(&mut upload, b"z").await.unwrap();
    storage.resume_upload(&mut recorded).await.unwrap();
    assert_eq!(state.lock().unwrap().part_pages, 2);
    assert_eq!(recorded.confirmed_bytes(), recorded.total_bytes());
    storage.finish_upload(&mut recorded).await.unwrap();
}

#[tokio::test]
async fn interrupted_ranged_body_retains_the_sdk_cause() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        assert!(socket.read(&mut request).await.unwrap() > 0);
        socket.write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Length: 4\r\nContent-Range: bytes 3-6/8\r\nConnection: close\r\n\r\nx").await.unwrap();
        socket.shutdown().await.unwrap();
    });
    let storage = provider(&format!("http://{address}"));
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let error = storage
        .read_range(&path, ByteRange::new(3, 7).unwrap())
        .await
        .unwrap_err();
    assert_eq!(error.failure(), StorageFailure::Network);
    assert!(matches!(error, StorageError::Provider { .. }));
    server.await.unwrap();
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
                    response(
                        403,
                        "<Error><Code>AccessDenied</Code><Message>native-refusal</Message></Error>",
                    )
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
async fn listing_requires_an_explicit_end_or_a_fresh_nonempty_cursor() {
    for tail in [
        "",
        "<IsTruncated>true</IsTruncated>",
        "<IsTruncated>true</IsTruncated><NextContinuationToken></NextContinuationToken>",
        "<IsTruncated>true</IsTruncated><NextContinuationToken>repeat</NextContinuationToken>",
    ] {
        let server = TestServer::new(Router::new().fallback(move || async move {
            response(200, format!("<ListBucketResult>{tail}</ListBucketResult>"))
        }))
        .await;
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
async fn multipart_recovery_refuses_incomplete_pages_and_empty_etags_without_advancing() {
    for tail in ["", "<IsTruncated>true</IsTruncated>", "<IsTruncated>true</IsTruncated><NextPartNumberMarker></NextPartNumberMarker>", "<IsTruncated>true</IsTruncated><NextPartNumberMarker>1</NextPartNumberMarker>", "<IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber><Size>4</Size><ETag></ETag></Part>"] {
        let server=TestServer::new(Router::new().fallback(move |method:Method,uri:Uri|async move {
            if query(&uri).contains_key("uploads") {response(200,"<InitiateMultipartUploadResult><UploadId>session</UploadId></InitiateMultipartUploadResult>")}
            else if method==Method::PUT {Response::builder().header("etag","").body(Body::empty()).unwrap()}
            else {response(200,format!("<ListPartsResult>{tail}</ListPartsResult>"))}
        })).await;
        let storage=provider(&server.url);
        let path=ObjectPath::device_log(coven_foundation::id_source::DeviceId(31),std::num::NonZeroU64::MIN);
        let mut upload=storage.begin_upload(&path,4).await.unwrap();
        assert_eq!(storage.upload_part(&mut upload,b"data").await.unwrap_err().failure(),StorageFailure::Protocol);
        assert_eq!(upload.confirmed_bytes(),0);
        assert_eq!(storage.resume_upload(&mut upload).await.unwrap_err().failure(),StorageFailure::Protocol);
        assert_eq!(upload.confirmed_bytes(),0);
        UploadSession::decode(upload.encode().unwrap().as_bytes()).unwrap();
    }
}

#[tokio::test]
async fn recovered_parts_cannot_exceed_the_recordings_part_size() {
    let total = 8 * 1024 * 1024 + 1;
    let server=TestServer::new(Router::new().fallback(move |uri:Uri|async move {
        if query(&uri).contains_key("uploads") {response(200,"<InitiateMultipartUploadResult><UploadId>session</UploadId></InitiateMultipartUploadResult>")}
        else {response(200,format!("<ListPartsResult><IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber><Size>{total}</Size><ETag>etag</ETag></Part></ListPartsResult>"))}
    })).await;
    let storage = provider(&server.url);
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let mut upload = storage.begin_upload(&path, total).await.unwrap();
    assert_eq!(
        storage
            .resume_upload(&mut upload)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Protocol
    );
    assert_eq!(upload.confirmed_bytes(), 0);
    UploadSession::decode(upload.encode().unwrap().as_bytes()).unwrap();
}

#[tokio::test]
async fn replacement_key_signs_the_next_request_on_the_existing_client() {
    let signatures = Arc::new(Mutex::new(Vec::new()));
    let captured = signatures.clone();
    let server = TestServer::new(Router::new().fallback(move |headers: HeaderMap| {
        captured
            .lock()
            .unwrap()
            .push(headers["authorization"].to_str().unwrap().to_owned());
        async {
            response(
                200,
                "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>",
            )
        }
    }))
    .await;
    let storage = provider(&server.url);
    storage.list(&ObjectPrefix::all()).await.unwrap();
    storage
        .set_s3_credentials(S3Credentials {
            access_key_id: "replacement".into(),
            secret_access_key: SecretText::new("replacement-secret".into()),
        })
        .await
        .unwrap();
    storage.list(&ObjectPrefix::all()).await.unwrap();
    let signatures = signatures.lock().unwrap();
    assert!(signatures[0].contains("Credential=access/"));
    assert!(signatures[1].contains("Credential=replacement/"));
}

#[tokio::test]
async fn provider_check_names_each_failed_operation_and_removes_its_object() {
    for check in [
        StorageCheck::Create,
        StorageCheck::CreateOnce,
        StorageCheck::Read,
        StorageCheck::ReadRange,
        StorageCheck::List,
        StorageCheck::Delete,
    ] {
        let state = Arc::new(Mutex::new(Remote::default()));
        let server =
            TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
        let storage = provider(&server.url);
        let path = ObjectPath::file(
            coven_foundation::id_source::DeviceId(31),
            coven_foundation::id_source::FileId(uuid::Uuid::from_bytes([0xab; 16])),
        );
        state.lock().unwrap().broken_check = Some(check);
        let error = check_provider(&storage, &path, b"sealed test bytes")
            .await
            .unwrap_err();
        assert!(
            matches!(error, StorageSetupError::ProviderCheck { check: actual, source } if actual == check && source.failure() == if check == StorageCheck::Create {
                StorageFailure::PermissionDenied
            } else {
                StorageFailure::Protocol
            })
        );
        assert_eq!(
            state.lock().unwrap().objects.len(),
            usize::from(check == StorageCheck::Delete)
        );
        assert_eq!(state.lock().unwrap().deletions, 1);
    }
}

#[tokio::test]
async fn provider_check_preserves_unrelated_contents_and_reports_occupied() {
    let state = Arc::new(Mutex::new(Remote::default()));
    state
        .lock()
        .unwrap()
        .objects
        .insert("store/unrelated".into(), b"kept".to_vec());
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::file(
        coven_foundation::id_source::DeviceId(31),
        coven_foundation::id_source::FileId(uuid::Uuid::from_bytes([0xab; 16])),
    );
    assert!(matches!(
        check_provider(&storage, &path, b"sealed test bytes")
            .await
            .unwrap_err(),
        StorageSetupError::LocationOccupied
    ));
    assert_eq!(
        state.lock().unwrap().objects,
        BTreeMap::from([("store/unrelated".into(), b"kept".to_vec())])
    );
}
