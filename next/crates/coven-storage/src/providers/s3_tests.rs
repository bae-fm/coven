use super::*;
use crate::providers::tests::{query, read, response, TestServer};
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
#[derive(Default)]
struct Remote {
    objects: BTreeMap<String, Vec<u8>>,
    uploads: BTreeMap<String, BTreeMap<u32, Vec<u8>>>,
    metadata: BTreeMap<String, String>,
    fail_part_reply: bool,
    forced_error: Option<(&'static str, u16)>,
}
async fn endpoint(
    State(state): State<Arc<Mutex<Remote>>>,
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
        state.uploads.insert(key.clone(), BTreeMap::new());
        state.metadata.insert(
            key.clone(),
            headers["x-amz-meta-coven-upload"].to_str().unwrap().into(),
        );
        return response(200,"<InitiateMultipartUploadResult><UploadId>upload</UploadId></InitiateMultipartUploadResult>");
    }
    if q.contains_key("uploadId") {
        if method == Method::PUT {
            let number = q["partNumber"].parse().unwrap();
            state
                .uploads
                .get_mut(&key)
                .unwrap()
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
            let Some(parts) = state.uploads.get(&key) else {
                return response(404, "<Error><Code>NoSuchUpload</Code></Error>");
            };
            let parts=parts.iter().map(|(n,b)|format!("<Part><PartNumber>{n}</PartNumber><ETag>\"part-{n}\"</ETag><Size>{}</Size></Part>",b.len())).collect::<String>();
            return response(
                200,
                format!(
                    "<ListPartsResult><IsTruncated>false</IsTruncated>{parts}</ListPartsResult>"
                ),
            );
        }
        if method == Method::POST {
            assert_eq!(headers["if-none-match"], "*");
            if state.objects.contains_key(&key) {
                return response(412, "<Error><Code>PreconditionFailed</Code></Error>");
            }
            let parts = state.uploads.remove(&key).unwrap();
            state
                .objects
                .insert(key, parts.into_values().flatten().collect());
            return response(200,"<CompleteMultipartUploadResult><ETag>\"complete\"</ETag></CompleteMultipartUploadResult>");
        }
        state.uploads.remove(&key);
        return response(204, Vec::new());
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
        let entry = entries
            .first()
            .map(|key| format!("<Contents><Key>{key}</Key></Contents>"))
            .unwrap_or_default();
        return response(
            200,
            format!("<ListBucketResult>{entry}{next}</ListBucketResult>"),
        );
    }
    match method {
        Method::PUT => {
            if headers.get("if-none-match").is_some() && state.objects.contains_key(&key) {
                return response(412, "<Error><Code>PreconditionFailed</Code></Error>");
            }
            state.objects.insert(key, body.to_vec());
            response(200, Vec::new())
        }
        Method::GET => match state.objects.get(&key) {
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
            state.objects.remove(&key);
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
    let path = ObjectPath::file(coven_foundation::id_source::FileId(uuid::Uuid::from_bytes(
        [0xaa; 16],
    )));
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
    let path = ObjectPath::file(coven_foundation::id_source::FileId(uuid::Uuid::from_bytes(
        [0xdd; 16],
    )));
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
    assert_eq!(
        storage
            .setup(&path, b"first entry")
            .await
            .unwrap_err()
            .failure(),
        StorageSetupFailure::LocationOccupied
    );
    assert_eq!(state.lock().unwrap().objects.len(), 1);
}

#[tokio::test]
async fn missing_session_and_destination_is_expired() {
    let state = Arc::new(Mutex::new(Remote::default()));
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
}
