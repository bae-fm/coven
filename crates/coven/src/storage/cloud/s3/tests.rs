use super::*;
use axum::body::Body;
use axum::extract::State;
use axum::http::header::{CONTENT_LENGTH, CONTENT_RANGE, IF_NONE_MATCH, RANGE};
use axum::http::{HeaderMap, Method, Response, StatusCode, Uri};
use axum::Router;
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct FailingBodyReader {
    emitted: bool,
}

#[async_trait]
impl coven_foundation::local_file::PlaintextChunkReader for FailingBodyReader {
    type Error = crate::storage::local_file::PlaintextChunkError;

    async fn next_chunk(
        &mut self,
        _max: usize,
    ) -> Result<Vec<u8>, crate::storage::local_file::PlaintextChunkError> {
        if !self.emitted {
            self.emitted = true;
            return Ok(vec![7; MULTIPART_PART_SIZE]);
        }
        Err(crate::storage::local_file::PlaintextChunkError::Local(
            "injected body failure".to_string(),
        ))
    }
}

#[test]
fn full_key_prepends_prefix() {
    let key = apply_prefix(Some("libs/abc"), "objects/dev1.json");
    assert_eq!(key, "libs/abc/objects/dev1.json");
}

#[test]
fn full_key_no_prefix() {
    let key = apply_prefix(None, "objects/dev1.json");
    assert_eq!(key, "objects/dev1.json");
}

#[test]
fn normalized_prefix_drops_trailing_slash() {
    let prefix = normalize_prefix(Some("libs/abc/".to_string()));
    let key = apply_prefix(prefix.as_deref(), "objects/dev1.json");
    assert_eq!(key, "libs/abc/objects/dev1.json");
}

#[derive(Clone)]
struct FakeRangeObject {
    bucket: String,
    key: String,
    range_body: Vec<u8>,
    object_len: u64,
    whole_object_crc32c: &'static str,
}

async fn fake_s3_range_endpoint(
    State(object): State<Arc<FakeRangeObject>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response<Body> {
    let expected_path = format!("/{}/{}", object.bucket, object.key);
    let range = headers.get(RANGE).and_then(|v| v.to_str().ok());

    if method != Method::GET || uri.path() != expected_path || range != Some("bytes=0-23") {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from(format!(
                "unexpected request: method={method}, path={}, range={range:?}",
                uri.path()
            )))
            .expect("build bad-request response");
    }

    Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(CONTENT_RANGE, format!("bytes 0-23/{}", object.object_len))
        .header(CONTENT_LENGTH, object.range_body.len().to_string())
        .header("x-amz-checksum-crc32c", object.whole_object_crc32c)
        .body(Body::from(object.range_body.clone()))
        .expect("build fake range response")
}

/// A test home against a fake endpoint with the fixture credentials every
/// fake-server test uses; bucket, region, key prefix, and exact-slot
/// capability are the axes tests vary.
async fn test_home(
    bucket: String,
    region: &str,
    endpoint: String,
    prefix: Option<String>,
    exact_slots: Option<crate::CustomS3ExactSlots>,
) -> S3CloudHome {
    open_cloud_home(
        bucket,
        region.to_string(),
        Some(endpoint),
        "access-key".to_string(),
        "secret-key".to_string(),
        prefix,
        exact_slots,
    )
    .await
    .expect("construct test S3CloudHome")
}

/// A test home in `us-east-1`, unprefixed, whose exact slots ride standard
/// conditional requests.
async fn standard_test_home(bucket: String, endpoint: String) -> S3CloudHome {
    test_home(
        bucket,
        "us-east-1",
        endpoint,
        None,
        Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
    )
    .await
}

/// Bind, serve, and wire graceful shutdown for a fake S3 server — the
/// scaffolding every fake endpoint shares; each test supplies its Router.
async fn spawn_fake_s3(app: Router) -> (String, tokio::sync::oneshot::Sender<()>) {
    crate::storage::cloud::test_server::spawn_test_server(
        app.layer(axum::extract::DefaultBodyLimit::disable()),
    )
    .await
}

async fn spawn_fake_s3_endpoint(
    object: FakeRangeObject,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    spawn_fake_s3(
        Router::new()
            .fallback(fake_s3_range_endpoint)
            .with_state(Arc::new(object)),
    )
    .await
}

#[derive(Clone)]
struct FakeFullBodyObject {
    bucket: String,
    key: String,
    full_body: Vec<u8>,
}

/// A fake S3 that ignores `Range` and answers every GET with 200 and the
/// whole object — the provider-ignores-range failure the range verification
/// must catch.
async fn fake_s3_full_body_endpoint(
    State(object): State<Arc<FakeFullBodyObject>>,
    method: Method,
    uri: Uri,
) -> Response<Body> {
    let expected_path = format!("/{}/{}", object.bucket, object.key);
    if method != Method::GET || uri.path() != expected_path {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from(format!(
                "unexpected request: method={method}, path={}",
                uri.path()
            )))
            .expect("build bad-request response");
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_LENGTH, object.full_body.len().to_string())
        .body(Body::from(object.full_body.clone()))
        .expect("build full-body response")
}

async fn spawn_fake_s3_full_body_endpoint(
    object: FakeFullBodyObject,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    spawn_fake_s3(
        Router::new()
            .fallback(fake_s3_full_body_endpoint)
            .with_state(Arc::new(object)),
    )
    .await
}

#[derive(Clone)]
struct FakePausedBodyObject {
    bucket: String,
    key: String,
    first: Vec<u8>,
    second: Vec<u8>,
    first_sent: Arc<tokio::sync::Notify>,
    release_second: Arc<tokio::sync::Notify>,
}

async fn fake_s3_paused_body_endpoint(
    State(object): State<FakePausedBodyObject>,
    method: Method,
    uri: Uri,
) -> Response<Body> {
    let expected_path = format!("/{}/{}", object.bucket, object.key);
    if method != Method::GET || uri.path() != expected_path {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from("unexpected paused-body request"))
            .expect("build bad-request response");
    }

    let total_len = object.first.len() + object.second.len();
    let stream = futures_util::stream::unfold((0u8, object), |(stage, object)| async move {
        match stage {
            0 => {
                object.first_sent.notify_one();
                let first = object.first.clone();
                Some((Ok::<Bytes, std::io::Error>(Bytes::from(first)), (1, object)))
            }
            1 => {
                object.release_second.notified().await;
                let second = object.second.clone();
                Some((Ok(Bytes::from(second)), (2, object)))
            }
            _ => None,
        }
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_LENGTH, total_len.to_string())
        .body(Body::from_stream(stream))
        .expect("build paused-body response")
}

async fn spawn_fake_s3_paused_body_endpoint(
    object: FakePausedBodyObject,
) -> (String, tokio::sync::oneshot::Sender<()>) {
    spawn_fake_s3(
        Router::new()
            .fallback(fake_s3_paused_body_endpoint)
            .with_state(object),
    )
    .await
}

async fn atomic_temp_paths(directory: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut entries = tokio::fs::read_dir(directory)
        .await
        .expect("read destination directory");
    let mut temps = Vec::new();
    while let Some(entry) = entries.next_entry().await.expect("read destination entry") {
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(coven_foundation::local_file::TEMP_BLOB_PREFIX)
        {
            temps.push(entry.path());
        }
    }
    temps
}

#[derive(Clone)]
struct FakeListState {
    bucket: String,
    request_count: Arc<AtomicUsize>,
}

async fn fake_s3_truncated_list_endpoint(
    State(state): State<FakeListState>,
    method: Method,
    uri: Uri,
) -> Response<Body> {
    state.request_count.fetch_add(1, Ordering::SeqCst);

    let expected_path = format!("/{}/", state.bucket);
    if method != Method::GET || uri.path() != expected_path {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from(format!(
                "unexpected request: method={method}, path={}, query={:?}",
                uri.path(),
                uri.query()
            )))
            .expect("build bad-request response");
    }

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>coven-s3-list-test</Name>
  <Prefix>objects/</Prefix>
  <KeyCount>1</KeyCount>
  <IsTruncated>true</IsTruncated>
  <Contents>
    <Key>objects/dev1.json</Key>
    <Size>10</Size>
  </Contents>
</ListBucketResult>"#,
        ))
        .expect("build fake list response")
}

async fn spawn_fake_s3_truncated_list_endpoint(
    bucket: String,
) -> (String, tokio::sync::oneshot::Sender<()>, Arc<AtomicUsize>) {
    let request_count = Arc::new(AtomicUsize::new(0));
    let state = FakeListState {
        bucket,
        request_count: request_count.clone(),
    };
    let (endpoint, shutdown_tx) = spawn_fake_s3(
        Router::new()
            .fallback(fake_s3_truncated_list_endpoint)
            .with_state(state),
    )
    .await;
    (endpoint, shutdown_tx, request_count)
}

async fn fake_s3_two_page_list_endpoint(
    State(state): State<FakeListState>,
    method: Method,
    uri: Uri,
) -> Response<Body> {
    let request = state.request_count.fetch_add(1, Ordering::SeqCst);
    if method != Method::GET || uri.path() != format!("/{}/", state.bucket) {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from("unexpected list request"))
            .expect("build response");
    }
    let (key, truncated, token) = match request {
        0 => (
            "objects/copy-a",
            true,
            "<NextContinuationToken>page-2</NextContinuationToken>",
        ),
        1 if uri
            .query()
            .is_some_and(|query| query.contains("continuation-token=page-2")) =>
        {
            ("objects/copy-b", false, "")
        }
        _ => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!("unexpected list page: {uri}")))
                .expect("build response");
        }
    };
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                 <Name>{}</Name><Prefix>objects/</Prefix><KeyCount>1</KeyCount>\
                 <IsTruncated>{truncated}</IsTruncated>{token}\
                 <Contents><Key>{key}</Key><Size>10</Size></Contents>\
                 </ListBucketResult>",
            state.bucket
        )))
        .expect("build response")
}

#[tokio::test]
async fn listing_exhausts_every_page() {
    let requests = Arc::new(AtomicUsize::new(0));
    let bucket = "immutable-list-test".to_string();
    let (endpoint, shutdown) = spawn_fake_s3(
        Router::new()
            .fallback(fake_s3_two_page_list_endpoint)
            .with_state(FakeListState {
                bucket: bucket.clone(),
                request_count: requests.clone(),
            }),
    )
    .await;
    let home = standard_test_home(bucket, endpoint).await;

    let listing = home.list("objects/").await.expect("list objects");

    assert_eq!(requests.load(Ordering::SeqCst), 2);
    assert_eq!(
        listing,
        vec!["objects/copy-a".to_string(), "objects/copy-b".to_string()]
    );
    shutdown.send(()).expect("shut down fake S3");
}

#[derive(Clone)]
struct FakeWriteState {
    bucket: String,
    conditional_headers: Arc<std::sync::Mutex<Vec<Option<String>>>>,
}

async fn fake_s3_write_endpoint(
    State(state): State<FakeWriteState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response<Body> {
    if method != Method::PUT || !uri.path().starts_with(&format!("/{}/", state.bucket)) {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from("unexpected write request"))
            .expect("build response");
    }
    state
        .conditional_headers
        .lock()
        .expect("lock headers")
        .push(
            headers
                .get(IF_NONE_MATCH)
                .map(|value| value.to_str().expect("If-None-Match header is UTF-8"))
                .map(str::to_string),
        );
    Response::builder()
        .status(StatusCode::OK)
        .header("etag", "\"write-etag\"")
        .body(Body::empty())
        .expect("build response")
}

#[tokio::test]
async fn immutable_append_is_create_only_but_generic_put_remains_mutable() {
    let headers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let bucket = "immutable-write-test".to_string();
    let (endpoint, shutdown) = spawn_fake_s3(
        Router::new()
            .fallback(fake_s3_write_endpoint)
            .with_state(FakeWriteState {
                bucket: bucket.clone(),
                conditional_headers: headers.clone(),
            }),
    )
    .await;
    let home = standard_test_home(bucket, endpoint).await;

    home.put_object("mutable", b"first".to_vec())
        .await
        .expect("generic mutable put");
    let slot = ObjectSlot::logical("immutable/copy".to_string()).unwrap();
    ExactSlotStorage::create_at(
        &home,
        &slot,
        crate::storage::cloud::BlobBody::from_bytes(b"second".to_vec()),
        &crate::storage::cloud::no_progress(),
    )
    .await
    .expect("immutable append");

    assert_eq!(
        *headers.lock().expect("lock headers"),
        vec![None, Some("*".to_string())]
    );
    assert!(home.exact_slots);
    shutdown.send(()).expect("shut down fake S3");
}

#[derive(Clone)]
struct FakeMultipartState {
    bucket: String,
    completion_headers: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    next_upload: Arc<AtomicUsize>,
    uploaded_parts: Arc<std::sync::Mutex<Vec<(String, usize)>>>,
    aborted_uploads: Arc<std::sync::Mutex<Vec<String>>>,
}

async fn fake_s3_multipart_endpoint(
    State(state): State<FakeMultipartState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let path_ok = uri.path().starts_with(&format!("/{}/", state.bucket));
    if method == Method::POST && path_ok && uri.query().is_some_and(|query| query == "uploads") {
        let upload_number = state.next_upload.fetch_add(1, Ordering::SeqCst) + 1;
        return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .body(Body::from(format!(
                    "<InitiateMultipartUploadResult><Bucket>{}</Bucket><Key>object</Key><UploadId>upload-{upload_number}</UploadId></InitiateMultipartUploadResult>", state.bucket
                )))
                .expect("build create response");
    }
    if method == Method::PUT && path_ok {
        let query = uri.query().expect("multipart part query");
        let upload_id = query
            .split('&')
            .find_map(|part| part.strip_prefix("uploadId="))
            .expect("multipart part uploadId")
            .to_string();
        let declared_length: usize = headers
            .get(CONTENT_LENGTH)
            .expect("multipart part Content-Length")
            .to_str()
            .expect("multipart part Content-Length is UTF-8")
            .parse()
            .expect("multipart part Content-Length is an integer");
        assert_eq!(declared_length, body.len());
        state
            .uploaded_parts
            .lock()
            .expect("lock parts")
            .push((upload_id, body.len()));
        return Response::builder()
            .status(StatusCode::OK)
            .header("etag", "part-etag")
            .body(Body::empty())
            .expect("build part response");
    }
    if method == Method::POST
        && path_ok
        && uri
            .query()
            .is_some_and(|query| query.contains("uploadId=upload-"))
    {
        state.completion_headers.lock().expect("lock headers").push(
            headers
                .get(IF_NONE_MATCH)
                .map(|value| value.to_str().expect("If-None-Match header is UTF-8"))
                .map(str::to_string),
        );
        let collision = uri
            .query()
            .is_some_and(|query| query.contains("uploadId=upload-2"));
        return Response::builder()
                .status(if collision {
                    StatusCode::PRECONDITION_FAILED
                } else {
                    StatusCode::OK
                })
                .header("content-type", "application/xml")
                .body(Body::from(if collision {
                    "<Error><Code>PreconditionFailed</Code><Message>exists</Message></Error>"
                } else {
                    "<CompleteMultipartUploadResult><ETag>\"etag\"</ETag></CompleteMultipartUploadResult>"
                }))
                .expect("build complete response");
    }
    if method == Method::DELETE && path_ok {
        let upload_id = uri
            .query()
            .expect("multipart abort query")
            .split('&')
            .find_map(|part| part.strip_prefix("uploadId="))
            .expect("multipart abort uploadId")
            .to_string();
        state
            .aborted_uploads
            .lock()
            .expect("lock aborts")
            .push(upload_id);
        return Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .expect("build abort response");
    }
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Body::from(format!(
            "unexpected multipart request: {method} {uri}"
        )))
        .expect("build response")
}

#[tokio::test]
async fn public_immutable_append_streams_parts_and_completes_create_only() {
    let headers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let parts = Arc::new(std::sync::Mutex::new(Vec::new()));
    let aborts = Arc::new(std::sync::Mutex::new(Vec::new()));
    let bucket = "immutable-multipart-test".to_string();
    let (endpoint, shutdown) = spawn_fake_s3(
        Router::new()
            .fallback(fake_s3_multipart_endpoint)
            .with_state(FakeMultipartState {
                bucket: bucket.clone(),
                completion_headers: headers.clone(),
                next_upload: Arc::new(AtomicUsize::new(0)),
                uploaded_parts: parts.clone(),
                aborted_uploads: aborts.clone(),
            }),
    )
    .await;
    let home = standard_test_home(bucket, endpoint).await;

    let slot = ObjectSlot::logical("immutable".to_string()).unwrap();
    ExactSlotStorage::create_at(
        &home,
        &slot,
        BlobBody::from_bytes(vec![9; MULTIPART_THRESHOLD + 1]),
        &crate::storage::cloud::no_progress(),
    )
    .await
    .expect("append immutable multipart object");
    let collision = ExactSlotStorage::create_at(
        &home,
        &slot,
        BlobBody::from_bytes(vec![8; MULTIPART_THRESHOLD + 1]),
        &crate::storage::cloud::no_progress(),
    )
    .await
    .expect_err("second immutable append must collide");

    assert!(
        matches!(collision, CloudHomeError::AlreadyExists(_)),
        "{collision}"
    );
    assert_eq!(
        *headers.lock().expect("lock headers"),
        vec![Some("*".to_string()), Some("*".to_string())]
    );
    let parts = parts.lock().expect("lock parts");
    for upload_id in ["upload-1", "upload-2"] {
        let lengths: Vec<_> = parts
            .iter()
            .filter_map(|(id, length)| (id == upload_id).then_some(*length))
            .collect();
        assert!(lengths.contains(&MULTIPART_PART_SIZE), "{parts:?}");
        assert!(lengths.contains(&1), "{parts:?}");
        assert!(
            lengths
                .iter()
                .all(|length| matches!(*length, MULTIPART_PART_SIZE | 1)),
            "{parts:?}"
        );
    }
    assert_eq!(
        *aborts.lock().expect("lock aborts"),
        vec!["upload-2".to_string()]
    );
    shutdown.send(()).expect("shut down fake S3");
}

async fn fake_s3_body_failure_endpoint(
    State((bucket, remaining_abort_failures)): State<(String, Arc<AtomicUsize>)>,
    method: Method,
    uri: Uri,
    _body: Bytes,
) -> Response<Body> {
    let path_ok = uri.path().starts_with(&format!("/{bucket}/"));
    if method == Method::POST && path_ok && uri.query() == Some("uploads") {
        return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .body(Body::from(format!(
                    "<InitiateMultipartUploadResult><Bucket>{bucket}</Bucket><Key>object</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>"
                )))
                .expect("build create response");
    }
    if method == Method::PUT && path_ok {
        return Response::builder()
            .status(StatusCode::OK)
            .header("etag", "part-1")
            .body(Body::empty())
            .expect("build part response");
    }
    if method == Method::DELETE && path_ok {
        if remaining_abort_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("injected abort failure"))
                .expect("build abort failure response");
        }
        return Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .expect("build abort response");
    }
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Body::from(format!("unexpected request: {method} {uri}")))
        .expect("build unexpected response")
}

async fn fake_s3_cancel_success_endpoint(
    State((bucket, abort_seen)): State<(String, Arc<std::sync::atomic::AtomicBool>)>,
    method: Method,
    uri: Uri,
) -> Response<Body> {
    let path_ok = uri.path().starts_with(&format!("/{bucket}/"));
    if method == Method::POST && path_ok && uri.query() == Some("uploads") {
        return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .body(Body::from(format!(
                    "<InitiateMultipartUploadResult><Bucket>{bucket}</Bucket><Key>object</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>"
                )))
                .expect("build create response");
    }
    if method == Method::DELETE && path_ok {
        abort_seen.store(true, Ordering::SeqCst);
        return Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .expect("build abort response");
    }
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Body::from(format!("unexpected request: {method} {uri}")))
        .expect("build unexpected response")
}

#[tokio::test]
async fn multipart_sink_retains_the_s3_runtime_through_abort() {
    let bucket = "immutable-cancel-success-test".to_string();
    let abort_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (endpoint, shutdown) = spawn_fake_s3(
        Router::new()
            .fallback(fake_s3_cancel_success_endpoint)
            .with_state((bucket.clone(), abort_seen.clone())),
    )
    .await;
    let home = standard_test_home(bucket, endpoint).await;
    let sink = home
        .open_multipart_sink("immutable/cancelled", MultipartCompletion::CreateOnly)
        .await
        .unwrap();

    drop(home);
    drop(sink);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !abort_seen.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("multipart owner must abort after its command channel closes");
    shutdown.send(()).expect("shut down fake S3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn immutable_append_reports_body_and_multipart_abort_failures() {
    let bucket = "immutable-body-failure-test".to_string();
    let abort_failures = Arc::new(AtomicUsize::new(1));
    let (endpoint, shutdown) = spawn_fake_s3(
        Router::new()
            .fallback(fake_s3_body_failure_endpoint)
            .with_state((bucket.clone(), abort_failures.clone())),
    )
    .await;
    let home = standard_test_home(bucket, endpoint).await;
    let reader = crate::storage::local_file::PlaintextReader::from_test_reader(FailingBodyReader {
        emitted: false,
    });
    let body = BlobBody::from_test_reader((MULTIPART_PART_SIZE + 1) as u64, reader);

    let slot = ObjectSlot::logical("immutable/body-failure".to_string()).unwrap();
    let error =
        ExactSlotStorage::create_at(&home, &slot, body, &crate::storage::cloud::no_progress())
            .await
            .expect_err("body failure must abort synchronously");

    assert!(
        matches!(error, CloudHomeError::CleanupFailed { .. }),
        "{error}"
    );
    assert!(
        error.to_string().contains("injected body failure"),
        "{error}"
    );
    assert!(error.to_string().contains("abort multipart"), "{error}");
    assert_eq!(abort_failures.load(Ordering::SeqCst), 0);
    shutdown.send(()).expect("shut down fake S3");
}

#[tokio::test]
async fn exact_operations_reject_an_opaque_s3_locator() {
    let home = standard_test_home(
        "exact-locator-test".to_string(),
        "http://127.0.0.1:9".to_string(),
    )
    .await;
    let slot = ObjectSlot::opaque("protocol/copy".to_string(), "protocol/other".to_string())
        .expect("build opaque S3 locator");

    let read_error = ExactSlotStorage::read_at(&home, &slot)
        .await
        .expect_err("opaque S3 read must fail");
    assert!(read_error.to_string().contains("must use its logical key"));
    let destination = std::env::temp_dir().join("coven-mismatched-s3-locator");
    let file_error = ExactSlotStorage::read_at_to_file(&home, &slot, &destination)
        .await
        .expect_err("opaque S3 file read must fail");
    assert!(file_error.to_string().contains("must use its logical key"));
    let delete_error = ExactSlotStorage::delete_at(&home, &slot)
        .await
        .expect_err("opaque S3 delete must fail");
    assert!(delete_error
        .to_string()
        .contains("must use its logical key"));
}

/// A slot whose physical locator this device built wrong is a fault in the
/// caller, not a transient failure of the network. Retrying it re-runs the
/// same deterministic rejection forever.
#[tokio::test]
async fn an_opaque_s3_locator_is_not_retryable() {
    let home = standard_test_home(
        "retryability-test".to_string(),
        "http://127.0.0.1:9".to_string(),
    )
    .await;
    let slot = ObjectSlot::opaque("protocol/copy".to_string(), "protocol/other".to_string())
        .expect("build opaque S3 locator");

    let error = ExactSlotStorage::read_at(&home, &slot)
        .await
        .expect_err("opaque S3 read must fail");

    assert!(
        !error.is_retryable(),
        "a malformed slot must not be retried: {error}"
    );
}

#[tokio::test]
async fn provider_binding_canonicalizes_the_custom_origin_and_hashes_the_access_key_id() {
    use coven_protocol::objects::{ProviderPrincipalId, S3EndpointBinding, StoreProviderBinding};

    let home = test_home(
        "bucket-a".to_string(),
        "us-east-1",
        "https://objects.example:443".to_string(),
        Some("stores/a/".to_string()),
        Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
    )
    .await;

    let binding = ExactSlotStorage::provider_binding(&home)
        .await
        .expect("resolve S3 provider binding");

    assert_eq!(
        binding.store,
        StoreProviderBinding::S3 {
            endpoint: S3EndpointBinding::Custom {
                origin: "https://objects.example".to_string(),
            },
            region: "us-east-1".to_string(),
            bucket: "bucket-a".to_string(),
            key_prefix: Some("stores/a".to_string()),
        }
    );
    assert_eq!(
        binding.device.principal,
        ProviderPrincipalId::CustomS3Credential {
            access_key_id_hash: s3_access_key_id_hash("access-key"),
        }
    );
}

#[tokio::test]
async fn provider_binding_rejects_a_custom_endpoint_with_a_base_path() {
    let home = standard_test_home(
        "bucket-a".to_string(),
        "https://objects.example/s3".to_string(),
    )
    .await;

    let error = ExactSlotStorage::provider_binding(&home)
        .await
        .expect_err("a non-origin custom endpoint cannot be signed as an origin");

    assert!(error.to_string().contains("origin"), "{error}");
}

#[test]
fn sts_transport_failure_remains_retryable_transport() {
    let error = sts_request_error("offline");
    assert!(matches!(error, CloudHomeError::Transport(_)));
    assert!(error.is_retryable());
}

#[test]
fn sts_identity_accepts_stable_aws_principals_and_rejects_federated_users() {
    use coven_protocol::objects::AwsPrincipal;

    assert_eq!(
        aws_caller_identity(
            "123456789012",
            "arn:aws:iam::123456789012:user/path/alice",
            "AIDAEXAMPLE"
        )
        .unwrap(),
        (
            "aws".to_string(),
            AwsPrincipal::User {
                arn: "arn:aws:iam::123456789012:user/path/alice".to_string(),
                user_id: "AIDAEXAMPLE".to_string(),
            }
        )
    );
    assert_eq!(
        aws_caller_identity(
            "123456789012",
            "arn:aws:sts::123456789012:assumed-role/path/role/session",
            "AROAEXAMPLE:session"
        )
        .unwrap()
        .1,
        AwsPrincipal::Role {
            role_id: "AROAEXAMPLE".to_string()
        }
    );
    assert!(aws_caller_identity(
        "123456789012",
        "arn:aws:sts::123456789012:federated-user/alice",
        "123456789012:alice"
    )
    .is_err());
}

#[test]
fn cancellation_abort_failure_does_not_terminate_the_process() {
    const CHILD: &str = "COVEN_S3_CANCEL_ABORT_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let bucket = "immutable-cancel-failure-test".to_string();
            let abort_failures = Arc::new(AtomicUsize::new(1));
            let (endpoint, shutdown) = spawn_fake_s3(
                Router::new()
                    .fallback(fake_s3_body_failure_endpoint)
                    .with_state((bucket.clone(), abort_failures.clone())),
            )
            .await;
            let home = standard_test_home(bucket, endpoint).await;
            let sink = home
                .open_multipart_sink("immutable/cancelled", MultipartCompletion::CreateOnly)
                .await
                .unwrap();
            drop(sink);
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while abort_failures.load(Ordering::SeqCst) != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("multipart owner must finish the abort request");
            shutdown.send(()).expect("shut down fake S3");
        });
        std::process::exit(0);
    }

    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("cancellation_abort_failure_does_not_terminate_the_process")
        .arg("--nocapture")
        .env(CHILD, "1")
        .status()
        .expect("run S3 cancellation sabotage subprocess");
    assert!(
        status.success(),
        "multipart abort failure terminated the subprocess: {status}"
    );
}

#[tokio::test]
async fn read_range_accepts_s3_compatible_full_object_checksum_header() {
    let range_body = b"abcdefghijklmnopqrstuvwx".to_vec();
    let key = "storage/audio-object".to_string();
    let bucket = "coven-s3-compatible-test".to_string();
    let (endpoint, shutdown) = spawn_fake_s3_endpoint(FakeRangeObject {
        bucket: bucket.clone(),
        key: key.clone(),
        range_body: range_body.clone(),
        object_len: 96,
        whole_object_crc32c: "sNqCyA==",
    })
    .await;

    let home = test_home(bucket, "us-central1", endpoint, None, None).await;

    let bytes = home
        .read_range(&key, 0, range_body.len() as u64)
        .await
        .expect("read range");

    assert_eq!(bytes, range_body);
    shutdown.send(()).expect("shut down fake S3");
}

/// A provider that ignores `Range` and answers a ranged GET with 200 and the
/// whole object returns offset-0 bytes where a mid-file range was asked for —
/// silent corruption on a plaintext home. The range read must reject the
/// mismatched body, not return the wrong bytes.
#[tokio::test]
async fn read_range_rejects_full_object_200_response() {
    let full_body = b"0123456789abcdefghijklmnopqrstuvwxyz".to_vec();
    let key = "storage/audio-object".to_string();
    let bucket = "coven-s3-ignores-range".to_string();
    let (endpoint, shutdown) = spawn_fake_s3_full_body_endpoint(FakeFullBodyObject {
        bucket: bucket.clone(),
        key: key.clone(),
        full_body,
    })
    .await;

    let home = test_home(bucket, "us-east-1", endpoint, None, None).await;

    let err = home
        .read_range(&key, 8, 16)
        .await
        .expect_err("a 200 full-object response to a range request must error");
    assert!(matches!(err, CloudHomeError::Transport(_)), "got {err:?}");
    shutdown.send(()).expect("shut down fake S3");
}

#[tokio::test]
async fn exact_read_streams_object_to_file() {
    let full_body = b"0123456789abcdefghijklmnopqrstuvwxyz".to_vec();
    let key = "storage/audio-object".to_string();
    let bucket = "coven-s3-appended-read".to_string();
    let (endpoint, shutdown) = spawn_fake_s3_full_body_endpoint(FakeFullBodyObject {
        bucket: bucket.clone(),
        key: key.clone(),
        full_body: full_body.clone(),
    })
    .await;

    let home = test_home(bucket, "us-east-1", endpoint, None, None).await;
    let tmp = tempfile::tempdir().expect("temp dir");
    let destination = tmp.path().join("object.bin");
    let slot = ObjectSlot::logical(key).unwrap();

    ExactSlotStorage::read_at_to_file(&home, &slot, &destination)
        .await
        .expect("stream exact object");

    assert_eq!(
        tokio::fs::read(&destination)
            .await
            .expect("read destination"),
        full_body
    );
    shutdown.send(()).expect("shut down fake S3");
}

#[tokio::test]
async fn canceling_exact_read_cannot_rename_over_destination_later() {
    let key = "storage/cancel-object".to_string();
    let bucket = "coven-s3-cancel-read".to_string();
    let first = b"partial".to_vec();
    let first_sent = Arc::new(tokio::sync::Notify::new());
    let release_second = Arc::new(tokio::sync::Notify::new());
    let (endpoint, shutdown) = spawn_fake_s3_paused_body_endpoint(FakePausedBodyObject {
        bucket: bucket.clone(),
        key: key.clone(),
        first: first.clone(),
        second: b" remainder".to_vec(),
        first_sent: first_sent.clone(),
        release_second: release_second.clone(),
    })
    .await;

    let home = test_home(bucket, "us-east-1", endpoint, None, None).await;
    let tmp = tempfile::tempdir().expect("temp dir");
    let destination = tmp.path().join("object.bin");
    tokio::fs::write(&destination, b"committed")
        .await
        .expect("seed destination");
    let slot = ObjectSlot::logical(key).unwrap();
    let read_destination = destination.clone();
    let read = tokio::spawn(async move {
        ExactSlotStorage::read_at_to_file(&home, &slot, &read_destination).await
    });
    first_sent.notified().await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if atomic_temp_paths(tmp.path()).await.iter().any(|path| {
                std::fs::metadata(path).is_ok_and(|metadata| metadata.len() == first.len() as u64)
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first response chunk was written to the temp file");

    read.abort();
    assert!(read.await.expect_err("read task canceled").is_cancelled());
    release_second.notify_waiters();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if atomic_temp_paths(tmp.path()).await.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("canceled S3 task removed its temp file");

    assert_eq!(
        tokio::fs::read(&destination)
            .await
            .expect("read destination"),
        b"committed"
    );
    shutdown.send(()).expect("shut down fake S3");
}

#[tokio::test]
async fn list_errors_when_truncated_response_has_no_continuation_token() {
    let bucket = "coven-s3-list-test".to_string();
    let (endpoint, shutdown, request_count) =
        spawn_fake_s3_truncated_list_endpoint(bucket.clone()).await;

    let home = test_home(bucket, "us-central1", endpoint, None, None).await;

    let result = tokio::time::timeout(std::time::Duration::from_secs(1), home.list("objects/"))
        .await
        .expect("list should return instead of refetching the first page");
    let err = result.expect_err("truncated response without token must fail");
    let msg = err.to_string();

    assert!(
        msg.contains("truncated") && msg.contains("continuation token"),
        "unexpected error: {msg}"
    );
    assert_eq!(
        request_count.load(Ordering::SeqCst),
        1,
        "malformed page must not be refetched"
    );
    let _ = shutdown.send(());
}

#[tokio::test]
async fn s3_revoke_access_reports_unsupported() {
    let home = test_home(
        "bucket".to_string(),
        "us-east-1",
        "http://127.0.0.1:9".to_string(),
        None,
        None,
    )
    .await;

    let outcome = home
        .set_access(CloudAccessState::Absent {
            member_pubkey: "member-pubkey".to_string(),
            provider_account_email: None,
        })
        .await
        .expect("S3 revoke_access must not error so member removal completes");

    assert_eq!(
            outcome,
            CloudAccessOutcome::Absent(RevokeOutcome::Unsupported),
            "S3 hands out one static bucket credential that cannot be withdrawn per member, so it reports Unsupported rather than claiming a revocation it did not perform",
        );
}

// ── probe() against a real S3 endpoint ──────────────────────────────
//
// These tests require a minio (or any S3-compatible server) reachable at
// `COVEN_TEST_S3_URL` (default http://localhost:19000) with credentials
// `COVEN_TEST_S3_KEY` / `COVEN_TEST_S3_SECRET` (default minioadmin / minioadmin).
// Marked `#[ignore]` so `cargo test` skips them; run with
// `cargo test -- --ignored` when an endpoint is available.

/// Read a test env var with a default fallback. `NotPresent` silently uses
/// the default (the intended path); `NotUnicode` panics so a misconfigured
/// env var fails loudly instead of silently substituting bytes-as-default.
fn test_env(name: &str, default: &str) -> String {
    match std::env::var(name) {
        Ok(s) => s,
        Err(std::env::VarError::NotPresent) => default.to_string(),
        Err(std::env::VarError::NotUnicode(raw)) => {
            panic!("test env var {name} is non-utf8: {raw:?}");
        }
    }
}

struct TestCreds {
    endpoint: String,
    access_key: String,
    secret_key: String,
}

impl TestCreds {
    fn from_env() -> Self {
        Self {
            endpoint: test_env("COVEN_TEST_S3_URL", "http://localhost:19000"),
            access_key: test_env("COVEN_TEST_S3_KEY", "coventest"),
            secret_key: test_env("COVEN_TEST_S3_SECRET", "coventestpass"),
        }
    }
}

fn required_test_env(name: &str) -> String {
    match std::env::var(name) {
        Ok(s) => s,
        Err(std::env::VarError::NotPresent) => {
            panic!("test env var {name} must be set for this test");
        }
        Err(std::env::VarError::NotUnicode(raw)) => {
            panic!("test env var {name} is non-utf8: {raw:?}");
        }
    }
}

fn optional_test_env(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(s) => Some(s),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(raw)) => {
            panic!("test env var {name} is non-utf8: {raw:?}");
        }
    }
}

struct ExistingS3ObjectEnv {
    bucket: String,
    region: String,
    endpoint: String,
    key: String,
    access_key: String,
    secret_key: String,
}

impl ExistingS3ObjectEnv {
    fn from_env() -> Option<Self> {
        let names = [
            "COVEN_TEST_S3_BUCKET",
            "COVEN_TEST_S3_REGION",
            "COVEN_TEST_S3_URL",
            "COVEN_TEST_S3_EXISTING_KEY",
            "COVEN_TEST_S3_KEY",
            "COVEN_TEST_S3_SECRET",
        ];
        let mut values = Vec::with_capacity(names.len());
        let mut missing = Vec::new();
        for name in names {
            match optional_test_env(name) {
                Some(value) => values.push(value),
                None => missing.push(name),
            }
        }
        if !missing.is_empty() {
            eprintln!(
                "skipping live S3 object test; unset env vars: {}",
                missing.join(", ")
            );
            return None;
        }
        let [bucket, region, endpoint, key, access_key, secret_key]: [String; 6] =
            values.try_into().expect("collected every live S3 env var");
        Some(Self {
            bucket,
            region,
            endpoint,
            key,
            access_key,
            secret_key,
        })
    }
}

#[tokio::test]
#[ignore]
async fn read_range_succeeds_against_existing_s3_object() {
    let creds = TestCreds::from_env();
    let bucket = required_test_env("COVEN_TEST_S3_BUCKET");
    let region = test_env("COVEN_TEST_S3_REGION", "us-east-1");
    let key = required_test_env("COVEN_TEST_S3_EXISTING_KEY");
    let start: u64 = test_env("COVEN_TEST_S3_RANGE_START", "0")
        .parse()
        .expect("COVEN_TEST_S3_RANGE_START must be a u64");
    let end: u64 = test_env("COVEN_TEST_S3_RANGE_END", "24")
        .parse()
        .expect("COVEN_TEST_S3_RANGE_END must be a u64");

    let home = open_cloud_home(
        bucket,
        region,
        Some(creds.endpoint),
        creds.access_key,
        creds.secret_key,
        None,
        None,
    )
    .await
    .expect("construct S3CloudHome");

    eprintln!("reading {key} range {start}..{end}");
    let bytes = home
        .read_range(&key, start, end)
        .await
        .unwrap_or_else(|e| panic!("read_range failed: {e:?}"));

    assert_eq!(bytes.len() as u64, end - start);
}

/// Proves an `S3CloudHome`'s AWS calls run end to end on its retained
/// runtime—a different runtime than the `Client` was built on—against a real bucket:
/// connection establishment, TLS, and body streaming, not just "it compiles".
/// If the aws connector bound to the build-time runtime's reactor this would
/// panic with "no reactor running" or hang; a real byte vec back proves the
/// spawn-on-other-runtime path works.
///
/// Reads an existing object from a real S3-compatible bucket. Coordinates
/// come from `COVEN_TEST_S3_BUCKET`, `COVEN_TEST_S3_REGION`,
/// `COVEN_TEST_S3_URL`, and `COVEN_TEST_S3_EXISTING_KEY`.
#[tokio::test]
#[ignore]
async fn s3_big_stack_reads_real_bytes_from_existing_object() {
    let Some(env) = ExistingS3ObjectEnv::from_env() else {
        return;
    };

    let home = open_cloud_home(
        env.bucket,
        env.region,
        Some(env.endpoint),
        env.access_key,
        env.secret_key,
        None,
        None,
    )
    .await
    .expect("construct S3CloudHome");

    let whole = home
        .read(&env.key)
        .await
        .unwrap_or_else(|e| panic!("read({}) failed: {e:?}", env.key));
    assert!(
        !whole.is_empty(),
        "expected non-empty object at {}",
        env.key
    );
    eprintln!("read {} bytes from {}", whole.len(), env.key);

    let n = whole.len().min(16) as u64;
    let head = home
        .read_range(&env.key, 0, n)
        .await
        .unwrap_or_else(|e| panic!("read_range({}, 0..{n}) failed: {e:?}", env.key));
    assert_eq!(
        head.as_slice(),
        &whole[..n as usize],
        "range bytes must match the object's prefix"
    );
    eprintln!("read_range first {n} bytes match the full read");
}

#[tokio::test]
#[ignore]
async fn probe_succeeds_against_existing_bucket() {
    let creds = TestCreds::from_env();
    let bucket = format!("coven-probe-ok-{}", uuid::Uuid::new_v4());
    let home = open_cloud_home(
        bucket,
        "us-east-1".to_string(),
        Some(creds.endpoint),
        creds.access_key,
        creds.secret_key,
        None,
        None,
    )
    .await
    .expect("construct S3CloudHome");
    home.provision_test_bucket().await;
    home.probe().await.expect("probe should succeed");
}

#[tokio::test]
#[ignore]
async fn probe_fails_for_missing_bucket() {
    let creds = TestCreds::from_env();
    let bucket = format!("coven-probe-missing-{}", uuid::Uuid::new_v4());
    let home = open_cloud_home(
        bucket.clone(),
        "us-east-1".to_string(),
        Some(creds.endpoint),
        creds.access_key,
        creds.secret_key,
        None,
        None,
    )
    .await
    .expect("construct S3CloudHome");
    // Deliberately do NOT create the bucket.
    let err = home
        .probe()
        .await
        .expect_err("probe should fail for a missing bucket");
    let msg = format!("{err}");
    assert!(
        msg.contains("does not exist") || msg.contains("NoSuchBucket") || msg.contains("404"),
        "expected missing-bucket error, got: {msg}",
    );
}

#[tokio::test]
#[ignore]
async fn probe_fails_for_bad_secret_key() {
    let creds = TestCreds::from_env();
    let bucket = format!("coven-probe-badkey-{}", uuid::Uuid::new_v4());
    // Provision the bucket with the good creds so the only difference is the bad secret.
    let good = open_cloud_home(
        bucket.clone(),
        "us-east-1".to_string(),
        Some(creds.endpoint.clone()),
        creds.access_key.clone(),
        creds.secret_key,
        None,
        None,
    )
    .await
    .expect("construct good S3CloudHome");
    good.provision_test_bucket().await;

    let bad = open_cloud_home(
        bucket,
        "us-east-1".to_string(),
        Some(creds.endpoint),
        creds.access_key,
        "wrong-secret".to_string(),
        None,
        None,
    )
    .await
    .expect("construct bad S3CloudHome");
    let err = bad
        .probe()
        .await
        .expect_err("probe should fail for bad credentials");
    let msg = format!("{err}");
    assert!(
        msg.contains("rejected") || msg.contains("403") || msg.contains("SignatureDoesNotMatch"),
        "expected credentials error, got: {msg}",
    );
}
