use super::*;
use crate::providers::tests::{query, read, response, TestServer};
use crate::test_utils::STORE_ID;
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
    gcs: bool,
    response_headers: BTreeMap<String, String>,
    fail_copy_reply: bool,
    refuse_copy: bool,
    copies: usize,
    refuse_temporary_delete: bool,
    lose_temporary_delete_reply: bool,
    objects: BTreeMap<String, Vec<u8>>,
    revisions: BTreeMap<String, String>,
    generation: u64,
    uploads: BTreeMap<String, PendingUpload>,
    next_upload: u64,
    part_pages: usize,
    metadata: BTreeMap<String, String>,
    fail_part_reply: bool,
    lose_create_reply: bool,
    refuse_delete: bool,
    deletions: usize,
    fail_completion_reply: bool,
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
    respond(&state, method, uri, headers, body)
}
fn respond(
    state: &Mutex<Remote>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let mut state = state.lock().unwrap();
    if state.gcs {
        assert!(headers["authorization"]
            .to_str()
            .unwrap()
            .starts_with("GOOG4-HMAC-SHA256 "));
        assert!(!headers
            .keys()
            .any(|name| name.as_str().starts_with("x-amz-")));
    }
    if let Some((code, status)) = state.forced_error {
        return response(
            status,
            format!("<Error><Code>{code}</Code><Message>credential-echo</Message></Error>"),
        );
    }
    if method == Method::HEAD && uri.path() == "/bucket/" {
        return response(if state.gcs { 403 } else { 200 }, Vec::new());
    }
    let q = query(&uri);
    let key = uri.path().strip_prefix("/bucket/").unwrap_or("").to_owned();
    if q.contains_key("uploads") {
        let token = headers[if state.gcs {
            "x-goog-meta-coven-upload"
        } else {
            "x-amz-meta-coven-upload"
        }]
        .to_str()
        .unwrap()
        .to_owned();
        state.next_upload += 1;
        let id = state.next_upload.to_string();
        state.uploads.insert(
            id.clone(),
            PendingUpload {
                key,
                token,
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
            if state.gcs {
                assert!(!headers.contains_key("x-goog-if-generation-match"));
                assert!(!headers.contains_key("if-none-match"));
            } else {
                assert_eq!(headers["if-none-match"], "*");
            }
            if !state.gcs && state.objects.contains_key(&key) {
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
    if let Some(source) = headers.get("x-goog-copy-source") {
        assert!(state.gcs);
        assert_eq!(method, Method::PUT);
        assert_eq!(headers["x-goog-if-generation-match"], "0");
        state.copies += 1;
        if state.refuse_copy {
            return response(403, "<Error><Code>AccessDenied</Code></Error>");
        }
        if state.objects.contains_key(&key) {
            return response(412, "<Error><Code>PreconditionFailed</Code></Error>");
        }
        let source = source.to_str().unwrap().strip_prefix("/bucket/").unwrap();
        let bytes = state.objects[source].clone();
        let token = state.metadata[source].clone();
        state.objects.insert(key.clone(), bytes);
        state.metadata.insert(key, token);
        if std::mem::replace(&mut state.fail_copy_reply, false) {
            return response(503, "<Error><Code>ServiceUnavailable</Code></Error>");
        }
        return response(200, "<CopyObjectResult><ETag>\"copied\"</ETag><LastModified>2026-10-06T00:00:02Z</LastModified></CopyObjectResult>");
    }
    if q.contains_key("list-type") {
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
        let listed_time = if state.gcs {
            "2026-10-06T00:00:00.751Z"
        } else {
            "2026-10-06T00:00:00Z"
        };
        let entry = entries
            .first()
            .map(|key| {
                let size = state.objects[*key].len() + usize::from(state.broken_check == Some(StorageCheck::List));
                format!("<Contents><Key>{key}</Key><Size>{size}</Size><LastModified>{listed_time}</LastModified><ETag>etag</ETag></Contents>")
            })
            .unwrap_or_default();
        return response(
            200,
            format!("<ListBucketResult>{entry}{next}</ListBucketResult>"),
        );
    }
    let result = match method {
        Method::PUT => {
            if state.broken_check == Some(StorageCheck::Create) {
                return response(403, "<Error><Code>AccessDenied</Code></Error>");
            }
            let create = if state.gcs {
                headers
                    .get("x-goog-if-generation-match")
                    .is_some_and(|value| value == "0")
            } else {
                headers.get("if-none-match").is_some()
            };
            if create
                && state.objects.contains_key(&key)
                && state.broken_check != Some(StorageCheck::CreateOnce)
            {
                return response(412, "<Error><Code>PreconditionFailed</Code></Error>");
            }
            state.generation += 1;
            let revision = if state.gcs {
                state.generation.to_string()
            } else {
                headers
                    .get("x-amz-meta-coven-revision")
                    .map(|value| value.to_str().unwrap().to_owned())
                    .unwrap_or_else(|| "immutable".into())
            };
            state.revisions.insert(key.clone(), revision);
            state.objects.insert(key.clone(), body.to_vec());
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
            Some(bytes) => {
                let reply = read(bytes, &headers);
                if reply.status() == 416 {
                    response(416, "<Error><Code>InvalidRange</Code></Error>")
                } else {
                    reply
                }
            }
            None => response(404, "<Error><Code>NoSuchKey</Code></Error>"),
        },
        Method::HEAD => match state.objects.get(&key) {
            Some(bytes) => Response::builder()
                .header("content-length", bytes.len())
                .header(
                    if state.gcs {
                        "x-goog-meta-coven-upload"
                    } else {
                        "x-amz-meta-coven-upload"
                    },
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
            if state.refuse_delete
                || (state.refuse_temporary_delete && key.contains("/.coven-uploads/"))
            {
                return response(403, "<Error><Code>AccessDenied</Code></Error>");
            }
            if state.broken_check != Some(StorageCheck::Delete) {
                let removed = state.objects.remove(&key);
                if state.gcs && removed.is_none() {
                    return response(404, "<Error><Code>NoSuchKey</Code></Error>");
                }
                if key.contains("/.coven-uploads/")
                    && std::mem::replace(&mut state.lose_temporary_delete_reply, false)
                {
                    return response(503, "<Error><Code>ServiceUnavailable</Code></Error>");
                }
            }
            response(204, Vec::new())
        }
        _ => panic!("unexpected S3 request: {uri}"),
    };
    let mut result = result;
    if result.status().is_success() && matches!(method, Method::GET | Method::HEAD) {
        result.headers_mut().insert(
            "last-modified",
            "Tue, 06 Oct 2026 00:00:00 GMT".parse().unwrap(),
        );
        result.headers_mut().insert("etag", "etag".parse().unwrap());
        if let Some(revision) = state.revisions.get(&key) {
            result.headers_mut().insert(
                if state.gcs {
                    "x-goog-generation"
                } else {
                    "x-amz-meta-coven-revision"
                },
                revision.parse().unwrap(),
            );
        }
    }
    if result.status().is_success() && matches!(method, Method::GET | Method::HEAD) {
        for (name, value) in &state.response_headers {
            result.headers_mut().insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
    }
    result
}

#[path = "s3_metadata_tests.rs"]
mod metadata_tests;
use gcs::gcs_provider;
#[path = "s3_copy_upload_tests.rs"]
mod gcs;
fn provider(url: &str) -> StorageConnection<S3Storage> {
    let storage = S3Storage::new(
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some(url.parse().unwrap()),
            profile: None,
            prefix: "store".into(),
        },
        S3Credentials {
            access_key_id: "access".into(),
            secret_access_key: SecretText::new("secret".into()),
        },
        Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
        Arc::new(SequentialIds::new()),
    )
    .unwrap();
    StorageConnection::from_provider(Arc::new(storage))
}
#[tokio::test]
async fn real_s3_client_conforms_with_pagination() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    crate::test_utils::Conformance::new(Arc::new(provider(&server.url)), STORE_ID)
        .run()
        .await
        .unwrap();
}
#[tokio::test]
async fn multipart_recovers_a_lost_part_reply() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    assert_eq!(storage.single_request_limit(), 5 * 1024 * 1024 * 1024);
    crate::test_utils::Conformance::new(Arc::new(storage), STORE_ID)
        .lost_part_reply(8 * 1024 * 1024, async {
            state.lock().unwrap().fail_part_reply = true;
        })
        .await;
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
        let error = storage
            .list(&ObjectPrefix::all(STORE_ID))
            .await
            .unwrap_err();
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
}

#[tokio::test]
async fn multipart_sizes_parts_for_the_promised_object() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state)).await;
    let storage = provider(&server.url);
    let path = ObjectPath::file(
        STORE_ID,
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
async fn shared_location_ignores_unrelated_objects() {
    let state = Arc::new(Mutex::new(Remote::default()));
    state
        .lock()
        .unwrap()
        .objects
        .insert("store/unrelated".into(), b"data".to_vec());
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    crate::test_utils::Conformance::new(Arc::new(storage), STORE_ID)
        .shared_location(coven_foundation::id_source::StoreId(
            uuid::Uuid::from_bytes([0x20; 16]),
        ))
        .await;
    assert_eq!(state.lock().unwrap().objects.len(), 1);
}

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

#[tokio::test]
async fn abort_retries_and_preserves_an_object_after_a_lost_completion_reply() {
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
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    state.lock().unwrap().fail_completion_reply = true;
    assert!(storage.finish_upload(&mut upload).await.is_err());
    storage.abort_upload(&upload).await.unwrap();
    storage.abort_upload(&upload).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"data");
}

#[tokio::test]
async fn provider_check_cleans_up_after_a_lost_create_reply_and_retains_both_failures() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::device_log(
        STORE_ID,
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
async fn duplicate_listing_entries_merge_or_fail_before_fetching_another_page() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let path = ObjectPath::device_log(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    for (size, expected_requests) in [(4, 2), (5, 1)] {
        let requests = Arc::new(AtomicUsize::new(0));
        let received = requests.clone();
        let key = path.under("store");
        let server = TestServer::new(Router::new().fallback(move || {
            let page = received.fetch_add(1, Ordering::SeqCst);
            let key = key.clone();
            async move {
                let contents = if page == 0 {
                    format!("<IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><Contents><Key>{key}</Key><Size>4</Size><LastModified>2026-10-06T00:00:00Z</LastModified><ETag>etag</ETag></Contents><Contents><Key>{key}</Key><Size>{size}</Size><LastModified>2026-10-06T00:00:00Z</LastModified><ETag>etag</ETag></Contents>")
                } else {
                    "<IsTruncated>false</IsTruncated>".into()
                };
                response(200, format!("<ListBucketResult>{contents}</ListBucketResult>"))
            }
        })).await;
        let result = provider(&server.url)
            .list(&ObjectPrefix::all(STORE_ID))
            .await;
        if size == 4 {
            let listed = result.unwrap();
            assert_eq!(listed.len(), 1);
            assert_eq!(listed[0].path, path);
            assert_eq!(listed[0].size, 4);
        } else {
            assert_eq!(result.unwrap_err().failure(), StorageFailure::Protocol);
        }
        assert_eq!(requests.load(Ordering::SeqCst), expected_requests);
    }
}

#[tokio::test]
async fn multipart_publication_and_abort_are_bound_to_the_native_session() {
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::device_log(
        STORE_ID,
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
        STORE_ID,
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
        socket.write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Length: 4\r\nContent-Range: bytes 3-6/8\r\nLast-Modified: Tue, 06 Oct 2026 00:00:00 GMT\r\nETag: etag\r\nConnection: close\r\n\r\nx").await.unwrap();
        socket.shutdown().await.unwrap();
    });
    let storage = provider(&format!("http://{address}"));
    let path = ObjectPath::device_log(
        STORE_ID,
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
                .list(&ObjectPrefix::all(STORE_ID))
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
        let path=ObjectPath::device_log(STORE_ID, coven_foundation::id_source::DeviceId(31),std::num::NonZeroU64::MIN);
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
        STORE_ID,
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
    storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap();
    storage
        .set_s3_credentials(S3Credentials {
            access_key_id: "replacement".into(),
            secret_access_key: SecretText::new("replacement-secret".into()),
        })
        .await
        .unwrap();
    storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap();
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
            STORE_ID,
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
async fn provider_check_preserves_unrelated_contents() {
    let state = Arc::new(Mutex::new(Remote::default()));
    state
        .lock()
        .unwrap()
        .objects
        .insert("store/unrelated".into(), b"kept".to_vec());
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let path = ObjectPath::file(
        STORE_ID,
        coven_foundation::id_source::DeviceId(31),
        coven_foundation::id_source::FileId(uuid::Uuid::from_bytes([0xab; 16])),
    );
    check_provider(&storage, &path, b"sealed test bytes")
        .await
        .unwrap();
    assert_eq!(
        state.lock().unwrap().objects,
        BTreeMap::from([("store/unrelated".into(), b"kept".to_vec())])
    );
}

#[path = "s3_error_tests.rs"]
mod errors;

#[path = "s3_restart_tests.rs"]
mod restart;

#[cfg(not(any(target_os = "ios", target_os = "android")))]
#[path = "s3_live.rs"]
mod live;
#[cfg(not(any(target_os = "ios", target_os = "android")))]
#[path = "s3_live_tests.rs"]
mod live_tests;

#[path = "s3_profile_tests.rs"]
mod profile_tests;
