use super::*;
use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{Request, Response, StatusCode};
use axum::Router;
use std::sync::{Arc, Mutex};

use crate::keys::StoreKeys;
use crate::oauth::OAuthTokens;

fn home() -> GoogleDriveCloudHome {
    let config = crate::oauth::OAuthClients::for_tests()
        .config_for(coven_foundation::config::CloudProvider::GoogleDrive)
        .expect("Google Drive test client");
    let session = OAuthSession::new(
        OAuthTokens {
            access_token: "test".to_string(),
            refresh_token: None,
            expires_at: None,
        },
        StoreKeys::bind("test".to_string()),
        Arc::new(coven_foundation::clock::SystemClock),
        config,
        "Google Drive",
    );
    GoogleDriveCloudHome::new("folder123".to_string(), session)
}

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: String,
    path: String,
    query: Option<String>,
    body: Vec<u8>,
}

async fn immutable_copy_endpoint(
    State(requests): State<Arc<Mutex<Vec<RecordedRequest>>>>,
    request: Request<Body>,
) -> Response<Body> {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let query = request.uri().query().map(str::to_string);
    let body = to_bytes(request.into_body(), usize::MAX)
        .await
        .expect("read request body")
        .to_vec();
    requests
        .lock()
        .expect("lock requests")
        .push(RecordedRequest {
            method: method.clone(),
            path: path.clone(),
            query: query.clone(),
            body,
        });

    if method == "GET" && path == "/files/generateIds" {
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"ids":["generated-id"]}"#))
            .expect("build generated id response");
    }
    if method == "POST"
        && path == "/files"
        && query
            .as_deref()
            .is_some_and(|query| query.contains("uploadType=multipart"))
    {
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"id":"ignored-response-id"}"#))
            .expect("build append response");
    }
    if method == "GET"
        && path == "/files/generated-id"
        && query
            .as_deref()
            .is_some_and(|query| query.contains("fields="))
    {
        return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"id":"generated-id","name":"{}","parents":["folder123"],"trashed":false,"appProperties":{{"covenLogicalKey":"protocol/copy"}}}}"#,
                    encode_key("protocol/copy"),
                )))
                .expect("build metadata response");
    }
    if method == "GET" && path == "/files/generated-id" {
        return Response::builder()
            .status(StatusCode::OK)
            .body(Body::from("copy-bytes"))
            .expect("build read response");
    }
    if method == "DELETE" && path == "/files/generated-id" {
        return Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .expect("build delete response");
    }
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from(format!(
            "unexpected request: {method} {path} {query:?}"
        )))
        .expect("build unexpected response")
}

async fn immutable_copy_test_home() -> (
    GoogleDriveCloudHome,
    Arc<Mutex<Vec<RecordedRequest>>>,
    tokio::sync::oneshot::Sender<()>,
) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .fallback(immutable_copy_endpoint)
        .with_state(requests.clone());
    let (endpoint, shutdown_tx) = crate::storage::cloud::test_server::spawn_test_server(app).await;
    (
        home()
            .with_endpoints(endpoint.clone(), endpoint)
            .with_ids(Arc::new(
                coven_foundation::id_provider::SequentialIdProvider::new("drive-create"),
            )),
        requests,
        shutdown_tx,
    )
}

#[tokio::test]
async fn immutable_copy_uses_preallocated_id_for_create_read_and_delete() {
    let (home, requests, shutdown) = immutable_copy_test_home().await;
    let slot = home
        .allocate_slot("protocol/copy")
        .await
        .expect("allocate Drive slot");
    home.create_at(
        &slot,
        BlobBody::from_bytes(b"copy-bytes".to_vec()),
        &crate::storage::cloud::no_progress(),
    )
    .await
    .expect("create exact Drive object");
    assert_eq!(
        slot,
        ObjectSlot::opaque("protocol/copy".to_string(), "generated-id".to_string())
            .expect("opaque Drive slot")
    );
    assert_eq!(
        home.read_at(&slot).await.expect("read Drive copy"),
        b"copy-bytes"
    );
    home.delete_at(&slot).await.expect("delete Drive copy");

    let requests = requests.lock().expect("lock requests");
    assert_eq!(requests.len(), 6, "{requests:?}");
    assert_eq!(requests[0].path, "/files/generateIds");
    let upload = String::from_utf8(requests[1].body.clone()).expect("multipart body is UTF-8");
    assert_eq!(requests[1].method, "POST");
    assert!(requests[1]
        .query
        .as_deref()
        .is_some_and(|query| query.contains("uploadType=multipart")));
    assert!(upload.contains(r#""id":"generated-id""#), "{upload}");
    assert!(
        upload.contains(r#""covenLogicalKey":"protocol/copy""#),
        "{upload}"
    );
    assert!(!upload.contains(CREATE_TOKEN_PROPERTY), "{upload}");
    assert!(upload.contains(&encode_key("protocol/copy")), "{upload}");
    assert!(upload.contains("copy-bytes"), "{upload}");
    assert_eq!(requests[2].method, "GET");
    assert_eq!(requests[2].path, "/files/generated-id");
    assert!(requests[2]
        .query
        .as_deref()
        .is_some_and(|query| query.contains("fields=")));
    assert_eq!(requests[3].method, "GET");
    assert_eq!(requests[3].path, "/files/generated-id");
    assert_eq!(requests[4].method, "GET");
    assert_eq!(requests[4].path, "/files/generated-id");
    assert_eq!(requests[5].method, "DELETE");
    assert_eq!(requests[5].path, "/files/generated-id");
    for request in requests.iter().skip(1) {
        let query = request.query.as_deref().expect("Drive file request query");
        assert!(query.contains("supportsAllDrives=true"), "{request:?}");
    }
    drop(requests);
    shutdown.send(()).expect("shut down Drive endpoint");
}

#[tokio::test]
async fn exact_operations_reject_a_drive_id_bound_to_another_logical_key() {
    let (home, requests, shutdown) = immutable_copy_test_home().await;
    let slot = ObjectSlot::opaque("protocol/other".to_string(), "generated-id".to_string())
        .expect("build mismatched Drive slot");

    let read_error = home
        .read_at(&slot)
        .await
        .expect_err("mismatched Drive read must fail");
    assert!(
        read_error.to_string().contains("does not identify"),
        "{read_error}"
    );
    let delete_error = home
        .delete_at(&slot)
        .await
        .expect_err("mismatched Drive delete must fail");
    assert!(
        delete_error.to_string().contains("does not identify"),
        "{delete_error}"
    );

    let requests = requests.lock().expect("lock requests");
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert!(requests.iter().all(|request| request.method == "GET"));
    drop(requests);
    shutdown.send(()).expect("shut down Drive endpoint");
}

async fn malformed_location_endpoint() -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(
            reqwest::header::LOCATION,
            reqwest::header::HeaderValue::from_bytes(&[0xff])
                .expect("build non-UTF-8 Location header"),
        )
        .body(Body::empty())
        .expect("build malformed Location response")
}

#[tokio::test]
async fn resumable_create_rejects_a_non_utf8_location_header() {
    let (endpoint, shutdown) = crate::storage::cloud::test_server::spawn_test_server(
        Router::new().fallback(malformed_location_endpoint),
    )
    .await;
    let home = home().with_endpoints(endpoint.clone(), endpoint);
    let attempt = DriveAppendAttempt {
        file_id: "generated-id".to_string(),
        create_token: "create-token".to_string(),
    };

    let error = home
        .open_resumable_create_session("protocol/copy", &attempt)
        .await
        .expect_err("non-UTF-8 Location must fail");

    assert!(
        error.to_string().contains("invalid Location header"),
        "{error}"
    );
    shutdown.send(()).expect("shut down test endpoint");
}

struct FailingMultipartBodyReader {
    emitted: bool,
}

#[async_trait]
impl coven_foundation::local_file::PlaintextChunkReader for FailingMultipartBodyReader {
    type Error = crate::storage::local_file::PlaintextChunkError;

    async fn next_chunk(
        &mut self,
        _max: usize,
    ) -> Result<Vec<u8>, crate::storage::local_file::PlaintextChunkError> {
        if !self.emitted {
            self.emitted = true;
            return Ok(vec![7; GDRIVE_CHUNK_SIZE]);
        }
        Err(crate::storage::local_file::PlaintextChunkError::Local(
            "injected Drive body failure".to_string(),
        ))
    }
}

struct EarlyEofMultipartBodyReader {
    emitted: bool,
}

#[async_trait]
impl coven_foundation::local_file::PlaintextChunkReader for EarlyEofMultipartBodyReader {
    type Error = crate::storage::local_file::PlaintextChunkError;

    async fn next_chunk(
        &mut self,
        _max: usize,
    ) -> Result<Vec<u8>, crate::storage::local_file::PlaintextChunkError> {
        if self.emitted {
            return Ok(Vec::new());
        }
        self.emitted = true;
        Ok(vec![7; GDRIVE_CHUNK_SIZE])
    }
}

struct PausedMultipartBodyReader {
    emitted: bool,
    waiting: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl coven_foundation::local_file::PlaintextChunkReader for PausedMultipartBodyReader {
    type Error = crate::storage::local_file::PlaintextChunkError;

    async fn next_chunk(
        &mut self,
        _max: usize,
    ) -> Result<Vec<u8>, crate::storage::local_file::PlaintextChunkError> {
        if !self.emitted {
            self.emitted = true;
            return Ok(vec![7; GDRIVE_CHUNK_SIZE]);
        }
        self.waiting.notify_one();
        std::future::pending().await
    }
}

#[derive(Clone)]
struct MutableCreateEndpointState {
    endpoint: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    deletes: Arc<std::sync::atomic::AtomicUsize>,
    first_delete_status: StatusCode,
}

async fn mutable_create_endpoint(
    State(state): State<MutableCreateEndpointState>,
    request: Request<Body>,
) -> Response<Body> {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let query = request.uri().query().map(str::to_string);
    let body = to_bytes(request.into_body(), usize::MAX)
        .await
        .expect("read request body")
        .to_vec();
    state
        .requests
        .lock()
        .expect("lock requests")
        .push(RecordedRequest {
            method: method.clone(),
            path: path.clone(),
            query: query.clone(),
            body,
        });
    if method == "GET" && path == "/files" {
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"files":[]}"#))
            .expect("build absent file response");
    }
    if method == "GET" && path == "/files/generateIds" {
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"ids":["generated-id"]}"#))
            .expect("build generated id response");
    }
    if method == "POST"
        && path == "/files"
        && query
            .as_deref()
            .is_some_and(|query| query.contains("uploadType=resumable"))
    {
        return Response::builder()
            .status(StatusCode::OK)
            .header(
                reqwest::header::LOCATION,
                format!("{}/session", state.endpoint),
            )
            .body(Body::empty())
            .expect("build resumable session response");
    }
    if method == "PUT" && path == "/session" {
        return Response::builder()
            .status(StatusCode::PERMANENT_REDIRECT)
            .body(Body::empty())
            .expect("build incomplete upload response");
    }
    if method == "DELETE" && path == "/session" {
        let delete_index = state
            .deletes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        return Response::builder()
            .status(if delete_index == 0 {
                state.first_delete_status
            } else {
                StatusCode::NO_CONTENT
            })
            .body(Body::empty())
            .expect("build session cancellation response");
    }
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from(format!(
            "unexpected request: {method} {path} {query:?}"
        )))
        .expect("build unexpected response")
}

async fn mutable_create_test_home() -> (
    GoogleDriveCloudHome,
    Arc<Mutex<Vec<RecordedRequest>>>,
    Arc<std::sync::atomic::AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    mutable_create_test_home_with_delete_status(StatusCode::NO_CONTENT).await
}

async fn mutable_create_test_home_with_delete_status(
    first_delete_status: StatusCode,
) -> (
    GoogleDriveCloudHome,
    Arc<Mutex<Vec<RecordedRequest>>>,
    Arc<std::sync::atomic::AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind Drive endpoint");
    let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let deletes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let state = MutableCreateEndpointState {
        endpoint: endpoint.clone(),
        requests: requests.clone(),
        deletes: deletes.clone(),
        first_delete_status,
    };
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .fallback(mutable_create_endpoint)
                .with_state(state),
        )
        .await
        .expect("Drive endpoint failed");
    });
    (
        home()
            .with_endpoints(endpoint.clone(), endpoint)
            .with_ids(Arc::new(
                coven_foundation::id_provider::SequentialIdProvider::new("drive-create"),
            )),
        requests,
        deletes,
        server,
    )
}

#[derive(Clone, Copy)]
struct CreateEndpointBehavior {
    metadata_status: StatusCode,
    metadata_body: &'static str,
    rollback_lookup_status: StatusCode,
    upload_status: StatusCode,
    delete_status: StatusCode,
}

#[derive(Clone)]
struct CreateEndpointState {
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    create_token: Arc<Mutex<Option<String>>>,
    behavior: CreateEndpointBehavior,
}

async fn create_endpoint(
    State(state): State<CreateEndpointState>,
    request: Request<Body>,
) -> Response<Body> {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let query = request.uri().query().map(str::to_string);
    let body = to_bytes(request.into_body(), usize::MAX)
        .await
        .expect("read request body")
        .to_vec();
    state
        .requests
        .lock()
        .expect("lock requests")
        .push(RecordedRequest {
            method: method.clone(),
            path: path.clone(),
            query,
            body: body.clone(),
        });

    match (method.as_str(), path.as_str()) {
        ("GET", "/files") => {
            let token = state
                .create_token
                .lock()
                .expect("lock create token")
                .clone();
            if token.is_some() && !state.behavior.rollback_lookup_status.is_success() {
                return Response::builder()
                    .status(state.behavior.rollback_lookup_status)
                    .body(Body::from("lookup failed"))
                    .expect("build lookup failure");
            }
            let response = match token {
                Some(token) => serde_json::json!({
                    "files": [{
                        "id": "created-file-id",
                        "appProperties": {
                            (CREATE_TOKEN_PROPERTY): token,
                        },
                    }],
                }),
                None => serde_json::json!({"files": []}),
            };
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(response.to_string()))
                .expect("build file list response")
        }
        ("POST", "/files") if state.behavior.metadata_status.is_success() => {
            let metadata: serde_json::Value =
                serde_json::from_slice(&body).expect("parse create metadata");
            let token = metadata["appProperties"][CREATE_TOKEN_PROPERTY]
                .as_str()
                .expect("create token")
                .to_string();
            *state.create_token.lock().expect("lock create token") = Some(token);
            Response::builder()
                .status(state.behavior.metadata_status)
                .header("content-type", "application/json")
                .body(Body::from(state.behavior.metadata_body))
                .expect("build metadata response")
        }
        ("POST", "/files") => Response::builder()
            .status(state.behavior.metadata_status)
            .body(Body::from("metadata failed"))
            .expect("build metadata failure"),
        ("PATCH", "/files/created-file-id") => Response::builder()
            .status(state.behavior.upload_status)
            .body(Body::from("media upload failed"))
            .expect("build media response"),
        ("DELETE", "/files/created-file-id") => Response::builder()
            .status(state.behavior.delete_status)
            .body(Body::from("delete failed"))
            .expect("build delete response"),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from(format!("unexpected request: {method} {path}")))
            .expect("build unexpected response"),
    }
}

async fn create_test_home(
    behavior: CreateEndpointBehavior,
) -> (
    GoogleDriveCloudHome,
    Arc<Mutex<Vec<RecordedRequest>>>,
    tokio::sync::oneshot::Sender<()>,
) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let state = CreateEndpointState {
        requests: requests.clone(),
        create_token: Arc::new(Mutex::new(None)),
        behavior,
    };
    let (endpoint, shutdown) = crate::storage::cloud::test_server::spawn_test_server(
        Router::new().fallback(create_endpoint).with_state(state),
    )
    .await;
    (
        home().with_endpoints(endpoint.clone(), endpoint),
        requests,
        shutdown,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_mutable_multipart_body_failure_cancels_the_create_session() {
    let (home, requests, deletes, server) = mutable_create_test_home().await;
    let reader =
        crate::storage::local_file::PlaintextReader::from_test_reader(FailingMultipartBodyReader {
            emitted: false,
        });
    let body = BlobBody::from_test_reader((GDRIVE_CHUNK_SIZE + 1) as u64, reader);

    let error = home
        .write("mutable/copy", body, &crate::storage::cloud::no_progress())
        .await
        .expect_err("Drive body failure must cancel its create session");

    assert!(
        error.to_string().contains("injected Drive body failure"),
        "{error}"
    );
    assert_eq!(deletes.load(std::sync::atomic::Ordering::SeqCst), 1);
    let requests = requests.lock().expect("lock requests");
    let file_posts: Vec<_> = requests
        .iter()
        .filter(|request| request.method == "POST" && request.path == "/files")
        .collect();
    assert_eq!(file_posts.len(), 1, "{requests:?}");
    assert!(file_posts[0]
        .query
        .as_deref()
        .is_some_and(|query| query.contains("uploadType=resumable")));
    drop(requests);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_mutable_multipart_early_eof_cancels_the_create_session() {
    let (home, _requests, deletes, server) = mutable_create_test_home().await;
    let reader = crate::storage::local_file::PlaintextReader::from_test_reader(
        EarlyEofMultipartBodyReader { emitted: false },
    );
    let body = BlobBody::from_test_reader((GDRIVE_CHUNK_SIZE + 1) as u64, reader);

    let error = home
        .write("mutable/short", body, &crate::storage::cloud::no_progress())
        .await
        .expect_err("Drive must reject an incomplete upload body");

    assert!(error.to_string().contains("ended after"), "{error}");
    assert_eq!(deletes.load(std::sync::atomic::Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drive_treats_499_as_successful_session_cancellation() {
    let (home, _requests, deletes, server) = mutable_create_test_home_with_delete_status(
        StatusCode::from_u16(499).expect("valid Drive cancellation status"),
    )
    .await;
    let reader =
        crate::storage::local_file::PlaintextReader::from_test_reader(FailingMultipartBodyReader {
            emitted: false,
        });
    let body = BlobBody::from_test_reader((GDRIVE_CHUNK_SIZE + 1) as u64, reader);

    let error = home
        .write(
            "mutable/cancel-499",
            body,
            &crate::storage::cloud::no_progress(),
        )
        .await
        .expect_err("the body failure must surface");

    assert!(
        error.to_string().contains("injected Drive body failure"),
        "{error}"
    );
    assert!(error.cleanup_causes().is_none(), "{error}");
    assert_eq!(deletes.load(std::sync::atomic::Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceling_absent_mutable_multipart_cancels_the_create_session() {
    let (home, _requests, deletes, server) = mutable_create_test_home().await;
    let waiting = Arc::new(tokio::sync::Notify::new());
    let reader =
        crate::storage::local_file::PlaintextReader::from_test_reader(PausedMultipartBodyReader {
            emitted: false,
            waiting: waiting.clone(),
        });
    let body = BlobBody::from_test_reader((GDRIVE_CHUNK_SIZE + 1) as u64, reader);
    let write = tokio::spawn(async move {
        home.write("mutable/copy", body, &crate::storage::cloud::no_progress())
            .await
    });
    waiting.notified().await;

    write.abort();
    assert!(write.await.expect_err("write task canceled").is_cancelled());
    assert_eq!(deletes.load(std::sync::atomic::Ordering::SeqCst), 1);
    server.abort();
}

async fn repeated_file_identity_page_endpoint(request: Request<Body>) -> Response<Body> {
    let path = request.uri().path();
    let body = if path == "/files" {
        r#"{"files":[],"nextPageToken":"same"}"#
    } else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from(format!("unexpected path: {path}")))
            .expect("build unexpected response");
    };
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .expect("build repeated page response")
}

#[tokio::test]
async fn file_identity_listing_rejects_a_repeated_page_token() {
    let (endpoint, shutdown) = crate::storage::cloud::test_server::spawn_test_server(
        Router::new().fallback(repeated_file_identity_page_endpoint),
    )
    .await;
    let home = home().with_endpoints(endpoint.clone(), endpoint);

    let error = home
        .list_file_identities(&encode_key("protocol/copy"))
        .await
        .expect_err("repeated identity page token must fail");

    assert!(error.to_string().contains("repeated"), "{error}");
    shutdown.send(()).expect("shut down test endpoint");
}

async fn shared_drive_listing_endpoint(
    State(requests): State<Arc<Mutex<Vec<RecordedRequest>>>>,
    request: Request<Body>,
) -> Response<Body> {
    let path = request.uri().path().to_string();
    let query = request.uri().query().map(str::to_string);
    requests
        .lock()
        .expect("lock requests")
        .push(RecordedRequest {
            method: request.method().to_string(),
            path: path.clone(),
            query,
            body: Vec::new(),
        });
    let body = match path.as_str() {
        "/files" => r#"{"files":[]}"#,
        _ => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(format!("unexpected path: {path}")))
                .expect("build unexpected response")
        }
    };
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .expect("build listing response")
}

#[tokio::test]
async fn exact_slot_identity_lookup_includes_shared_drives() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (endpoint, shutdown) = crate::storage::cloud::test_server::spawn_test_server(
        Router::new()
            .fallback(shared_drive_listing_endpoint)
            .with_state(requests.clone()),
    )
    .await;
    let home = home().with_endpoints(endpoint.clone(), endpoint);

    home.list_file_identities(&encode_key("protocol/copy"))
        .await
        .expect("look up shared Drive exact-slot identity");

    let requests = requests.lock().expect("lock requests");
    assert_eq!(requests.len(), 1, "{requests:?}");
    let query = requests[0].query.as_deref().expect("listing query");
    assert!(query.contains("supportsAllDrives=true"), "{query}");
    assert!(query.contains("includeItemsFromAllDrives=true"), "{query}");
    assert!(!query.contains("restrictToMyDrive"), "{query}");
    drop(requests);
    shutdown.send(()).expect("shut down test endpoint");
}

async fn generated_id_collision_endpoint(
    State(requests): State<Arc<Mutex<Vec<String>>>>,
    request: Request<Body>,
) -> Response<Body> {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    requests
        .lock()
        .expect("lock requests")
        .push(format!("{method} {path}"));
    match (method.as_str(), path.as_str()) {
        ("GET", "/files/generateIds") => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"ids":["generated-id"]}"#))
            .expect("build generated id response"),
        ("POST", "/files") => Response::builder()
            .status(StatusCode::CONFLICT)
            .body(Body::from("generated id already exists"))
            .expect("build collision response"),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("unexpected request"))
            .expect("build unexpected response"),
    }
}

#[tokio::test]
async fn generated_id_collision_preserves_the_pre_existing_file() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (endpoint, shutdown) = crate::storage::cloud::test_server::spawn_test_server(
        Router::new()
            .fallback(generated_id_collision_endpoint)
            .with_state(requests.clone()),
    )
    .await;
    let home = home().with_endpoints(endpoint.clone(), endpoint);

    let slot = home
        .allocate_slot("protocol/collision")
        .await
        .expect("allocate collision slot");
    let error = home
        .create_at(
            &slot,
            BlobBody::from_bytes(b"new bytes".to_vec()),
            &crate::storage::cloud::no_progress(),
        )
        .await
        .expect_err("generated-id collision must fail");

    assert!(matches!(error, CloudHomeError::AlreadyExists(_)), "{error}");
    assert!(
        !requests
            .lock()
            .expect("lock requests")
            .iter()
            .any(|request| request.starts_with("DELETE ")),
        "collision deleted a pre-existing file"
    );
    shutdown.send(()).expect("shut down test endpoint");
}

#[derive(Clone, Default)]
struct AmbiguousCreateState {
    committed: Arc<Mutex<bool>>,
    requests: Arc<Mutex<Vec<String>>>,
}

async fn ambiguous_create_endpoint(
    State(state): State<AmbiguousCreateState>,
    request: Request<Body>,
) -> Response<Body> {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let body = to_bytes(request.into_body(), usize::MAX)
        .await
        .expect("read request body");
    state
        .requests
        .lock()
        .expect("lock requests")
        .push(format!("{method} {path}"));
    match (method.as_str(), path.as_str()) {
        ("GET", "/files/generateIds") => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"ids":["generated-id"]}"#))
            .expect("build generated id response"),
        ("POST", "/files") => {
            let body = String::from_utf8(body.to_vec()).expect("multipart body is UTF-8");
            assert!(body.contains(r#""id":"generated-id""#), "{body}");
            assert!(
                body.contains(r#""covenLogicalKey":"protocol/ambiguous""#),
                "{body}"
            );
            assert!(!body.contains(CREATE_TOKEN_PROPERTY), "{body}");
            *state.committed.lock().expect("lock commit state") = true;
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("response lost after commit"))
                .expect("build ambiguous response")
        }
        ("GET", "/files/generated-id") => {
            assert!(*state.committed.lock().expect("lock commit state"));
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "id": "generated-id",
                        "name": encode_key("protocol/ambiguous"),
                        "parents": ["folder123"],
                        "trashed": false,
                        "appProperties": {
                            (LOGICAL_KEY_PROPERTY): "protocol/ambiguous",
                        },
                    })
                    .to_string(),
                ))
                .expect("build owned metadata response")
        }
        ("DELETE", "/files/generated-id") => Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .expect("build delete response"),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("unexpected request"))
            .expect("build unexpected response"),
    }
}

#[tokio::test]
async fn ambiguous_exact_create_preserves_the_logical_key_matched_commit() {
    let state = AmbiguousCreateState::default();
    let (endpoint, shutdown) = crate::storage::cloud::test_server::spawn_test_server(
        Router::new()
            .fallback(ambiguous_create_endpoint)
            .with_state(state.clone()),
    )
    .await;
    let home = home().with_endpoints(endpoint.clone(), endpoint);

    let slot = home
        .allocate_slot("protocol/ambiguous")
        .await
        .expect("allocate ambiguous slot");
    home.create_at(
        &slot,
        BlobBody::from_bytes(b"committed bytes".to_vec()),
        &crate::storage::cloud::no_progress(),
    )
    .await
    .expect("logical-key-matched commit resolves ambiguous create");

    assert_eq!(
        slot,
        ObjectSlot::opaque("protocol/ambiguous".to_string(), "generated-id".to_string(),)
            .expect("opaque Drive slot")
    );
    assert!(
        !state
            .requests
            .lock()
            .expect("lock requests")
            .iter()
            .any(|request| request.starts_with("DELETE ")),
        "ambiguous committed file was deleted"
    );
    shutdown.send(()).expect("shut down test endpoint");
}

#[test]
fn parse_google_api_error_reason_extracts_storage_quota() {
    let body = r#"{"error":{"code":403,"message":"quota","errors":[{"domain":"usageLimits","reason":"storageQuotaExceeded","message":"full"}]}}"#;
    assert_eq!(
        parse_google_api_error_reason(body).as_deref(),
        Some("storageQuotaExceeded"),
    );
}

#[test]
fn parse_google_api_error_reason_returns_none_for_non_drive_body() {
    assert!(parse_google_api_error_reason("<html>500</html>").is_none());
    assert!(parse_google_api_error_reason("{}").is_none());
    assert!(parse_google_api_error_reason(r#"{"error":"flat"}"#).is_none());
}

#[test]
fn find_file_query_escapes_encoded_name_and_folder() {
    let query = find_file_query("folder'1", "encoded'name");

    assert!(query.contains("'folder\\'1' in parents"));
    assert!(query.contains("name = 'encoded\\'name'"));
    assert!(!query.contains("encoded'name"));
}

#[test]
fn find_created_file_query_escapes_name_and_create_token() {
    let query = find_created_file_query("folder-id", "object'1", r"token\2");

    assert!(query.contains("name = 'object\\'1'"));
    assert!(query.contains(r"value='token\\2'"));
}

#[test]
fn list_file_query_escapes_encoded_prefix() {
    let query = list_file_query("folder-id", "artist's-live/");

    assert!(query.contains("name contains '61727469737427732d6c6976652f'"));
    assert!(!query.contains("artist's-live"));
}

#[test]
fn drive_permissions_next_page_url_appends_encoded_page_token() {
    let page = serde_json::json!({"nextPageToken": "tok/en+1"});

    assert_eq!(
            drive_permissions_next_page_url(
                "https://www.googleapis.com/drive/v3/files/folder/permissions?fields=permissions(id,emailAddress),nextPageToken",
                &page,
            )
            .expect("encode next page")
            .as_deref(),
            Some("https://www.googleapis.com/drive/v3/files/folder/permissions?fields=permissions(id,emailAddress),nextPageToken&pageToken=tok%2Fen%2B1")
        );
}

#[test]
fn parse_drive_file_identities_requires_create_tokens() {
    let page = serde_json::json!({
        "files": [
            {"id": "file-a", "appProperties": {"covenCreateToken": "token-b"}},
            {"id": "file-b", "appProperties": {"other": "ignored"}}
        ]
    });

    let error = parse_drive_file_identities(&page)
        .expect_err("a Drive file without its create token must fail");
    assert!(error.to_string().contains("file-b"), "{error}");
}

#[test]
fn select_drive_file_uses_deterministic_create_token_tiebreak() {
    let files = vec![
        DriveFileIdentity {
            id: "local-loser".to_string(),
            create_token: "token-z".to_string(),
        },
        DriveFileIdentity {
            id: "peer-winner".to_string(),
            create_token: "token-a".to_string(),
        },
    ];

    assert_eq!(
        select_drive_file(&files).map(|file| file.id.as_str()),
        Some("peer-winner")
    );
}

#[test]
fn parse_drive_file_identities_rejects_missing_ids() {
    let page = serde_json::json!({
        "files": [
            {"appProperties": {"covenCreateToken": "token-a"}}
        ]
    });

    assert!(parse_drive_file_identities(&page).is_err());
}

#[test]
fn parse_list_page_skips_malformed_flat_names() {
    let valid = encode_key("objects/dev1/1.enc");
    let other_prefix = encode_key("snapshots/dev1.json.enc");
    let body = format!(
        r#"{{"files":[{{"name":"{valid}"}},{{"name":"not-hex"}},{{"name":"{other_prefix}"}}]}}"#
    );

    let page = home()
        .parse_list_page(&body, "objects/")
        .expect("parse list page");

    assert_eq!(page.keys, vec!["objects/dev1/1.enc"]);
}

#[test]
fn folder_search_query_escapes_folder_name() {
    let query = folder_search_query("your-app - artist's live");

    assert!(query.contains("name = 'your-app - artist\\'s live'"));
    assert!(query.contains("mimeType = 'application/vnd.google-apps.folder'"));
}

#[test]
fn classify_write_error_quota_message_names_provider_and_recovery() {
    let body = r#"{"error":{"code":403,"errors":[{"reason":"storageQuotaExceeded"}]}}"#;
    let err = classify_write_error(reqwest::StatusCode::FORBIDDEN, body, "k", "create");
    let msg = err.to_string();
    assert!(
        msg.contains("Google Drive storage is full"),
        "missing provider+state: {msg}",
    );
    assert!(
        msg.contains("Free up space"),
        "missing recovery step: {msg}"
    );
}

#[test]
fn classify_write_error_keeps_raw_for_non_quota_errors() {
    let body = r#"{"error":{"code":500,"message":"server error"}}"#;
    let err = classify_write_error(
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        body,
        "blobs/aa/bb/cc",
        "create",
    );
    let msg = err.to_string();
    assert!(msg.contains("HTTP 500"), "missing HTTP status: {msg}");
    assert!(msg.contains("blobs/aa/bb/cc"), "missing key: {msg}");
    assert!(
        !msg.contains("storage is full"),
        "should not match the quota message: {msg}",
    );
}

#[test]
fn parse_create_file_id_extracts_id() {
    assert_eq!(
        parse_create_file_id(r#"{"id":"drive-file-1"}"#, "objects/a")
            .expect("parse created file id"),
        "drive-file-1",
    );
}

#[test]
fn parse_create_file_id_errors_when_id_is_missing() {
    let err = parse_create_file_id(r#"{"name":"encoded-object-name"}"#, "objects/a")
        .expect_err("missing created file id");
    let msg = err.to_string();

    assert!(
        msg.contains("create objects/a"),
        "missing create path: {msg}"
    );
    assert!(msg.contains("missing id"), "missing id reason: {msg}");
}

#[test]
fn generated_append_id_requires_one_nonempty_id() {
    assert_eq!(
        parse_generated_file_id(&serde_json::json!({"ids": ["drive-id-1"]}), "objects/a")
            .expect("parse generated file id"),
        "drive-id-1",
    );
    assert!(parse_generated_file_id(&serde_json::json!({"ids": []}), "objects/a").is_err());
    assert!(parse_generated_file_id(
        &serde_json::json!({"ids": ["drive-id-1", "drive-id-2"]}),
        "objects/a"
    )
    .is_err());
    assert!(parse_generated_file_id(&serde_json::json!({"ids": [""]}), "objects/a").is_err());
}

#[test]
fn create_file_metadata_body_carries_rollback_token() {
    let body = create_file_metadata_body("encoded-object-name", "folder-1", "create-token-1");
    let json: serde_json::Value = serde_json::from_str(&body).expect("metadata json");

    assert_eq!(json["name"].as_str(), Some("encoded-object-name"));
    assert_eq!(json["parents"][0].as_str(), Some("folder-1"));
    assert_eq!(
        json["appProperties"][CREATE_TOKEN_PROPERTY].as_str(),
        Some("create-token-1"),
    );
}

#[tokio::test]
async fn create_metadata_id_error_rolls_back_token_matched_file() {
    let (home, requests, shutdown) = create_test_home(CreateEndpointBehavior {
        metadata_status: StatusCode::OK,
        metadata_body: r#"{"name":"encoded-object-name"}"#,
        rollback_lookup_status: StatusCode::OK,
        upload_status: StatusCode::OK,
        delete_status: StatusCode::NO_CONTENT,
    })
    .await;
    let err = home
        .put_object("objects/a", b"media".to_vec())
        .await
        .expect_err("create id error");
    let msg = err.to_string();

    assert!(
        requests
            .lock()
            .expect("lock requests")
            .iter()
            .any(|request| {
                request.method == "DELETE" && request.path == "/files/created-file-id"
            }),
        "token-matched created file was not deleted"
    );
    assert!(
        msg.contains("response missing id"),
        "missing create id failure: {msg}"
    );
    shutdown.send(()).expect("shut down test endpoint");
}

#[tokio::test]
async fn create_metadata_id_error_reports_token_lookup_failure() {
    let (home, requests, shutdown) = create_test_home(CreateEndpointBehavior {
        metadata_status: StatusCode::OK,
        metadata_body: r#"{"name":"encoded-object-name"}"#,
        rollback_lookup_status: StatusCode::BAD_REQUEST,
        upload_status: StatusCode::OK,
        delete_status: StatusCode::NO_CONTENT,
    })
    .await;
    let err = home
        .put_object("objects/a", b"media".to_vec())
        .await
        .expect_err("lookup failure");
    let msg = err.to_string();

    assert!(
        !requests
            .lock()
            .expect("lock requests")
            .iter()
            .any(|request| request.method == "DELETE"),
        "delete ran without a resolved created file"
    );
    assert!(
        msg.contains("response missing id"),
        "missing create id failure: {msg}"
    );
    assert!(
        msg.contains("lookup failed"),
        "missing lookup failure: {msg}"
    );
    shutdown.send(()).expect("shut down test endpoint");
}

#[tokio::test]
async fn create_metadata_id_error_reports_token_delete_failure() {
    let (home, requests, shutdown) = create_test_home(CreateEndpointBehavior {
        metadata_status: StatusCode::OK,
        metadata_body: r#"{"name":"encoded-object-name"}"#,
        rollback_lookup_status: StatusCode::OK,
        upload_status: StatusCode::OK,
        delete_status: StatusCode::BAD_REQUEST,
    })
    .await;
    let err = home
        .put_object("objects/a", b"media".to_vec())
        .await
        .expect_err("delete failure");
    let msg = err.to_string();

    assert!(
        requests
            .lock()
            .expect("lock requests")
            .iter()
            .any(|request| {
                request.method == "DELETE" && request.path == "/files/created-file-id"
            }),
        "rollback delete was not attempted"
    );
    assert!(
        msg.contains("response missing id"),
        "missing create id failure: {msg}"
    );
    assert!(
        msg.contains("delete failed"),
        "missing delete failure: {msg}"
    );
    shutdown.send(()).expect("shut down test endpoint");
}

#[tokio::test]
async fn create_file_with_media_does_not_delete_on_metadata_failure() {
    let (home, requests, shutdown) = create_test_home(CreateEndpointBehavior {
        metadata_status: StatusCode::BAD_REQUEST,
        metadata_body: r#"{"id":"created-file-id"}"#,
        rollback_lookup_status: StatusCode::OK,
        upload_status: StatusCode::OK,
        delete_status: StatusCode::NO_CONTENT,
    })
    .await;
    let err = home
        .put_object("objects/a", b"media".to_vec())
        .await
        .expect_err("metadata failure");
    let msg = err.to_string();

    assert!(
        !requests
            .lock()
            .expect("lock requests")
            .iter()
            .any(|request| request.method == "DELETE"),
        "delete ran without a created file id"
    );
    assert!(
        msg.contains("metadata failed"),
        "missing metadata failure: {msg}"
    );
    shutdown.send(()).expect("shut down test endpoint");
}

#[tokio::test]
async fn create_file_with_media_deletes_created_id_on_media_failure() {
    let (home, requests, shutdown) = create_test_home(CreateEndpointBehavior {
        metadata_status: StatusCode::OK,
        metadata_body: r#"{"id":"created-file-id"}"#,
        rollback_lookup_status: StatusCode::OK,
        upload_status: StatusCode::BAD_REQUEST,
        delete_status: StatusCode::NO_CONTENT,
    })
    .await;
    let err = home
        .put_object("objects/a", b"media".to_vec())
        .await
        .expect_err("media failure");
    let msg = err.to_string();

    assert!(
        requests
            .lock()
            .expect("lock requests")
            .iter()
            .any(|request| request.method == "DELETE" && request.path == "/files/created-file-id"),
        "created file was not deleted"
    );
    assert!(
        msg.contains("media upload failed"),
        "missing media failure: {msg}"
    );
    shutdown.send(()).expect("shut down test endpoint");
}

#[tokio::test]
async fn create_file_with_media_reports_upload_and_rollback_failures() {
    let (home, _requests, shutdown) = create_test_home(CreateEndpointBehavior {
        metadata_status: StatusCode::OK,
        metadata_body: r#"{"id":"created-file-id"}"#,
        rollback_lookup_status: StatusCode::OK,
        upload_status: StatusCode::BAD_REQUEST,
        delete_status: StatusCode::BAD_REQUEST,
    })
    .await;
    let err = home
        .put_object("objects/a", b"media".to_vec())
        .await
        .expect_err("upload and rollback error");
    let msg = err.to_string();

    assert!(
        msg.contains("media upload failed"),
        "missing upload failure: {msg}"
    );
    assert!(
        msg.contains("delete failed"),
        "missing delete failure: {msg}"
    );
    shutdown.send(()).expect("shut down test endpoint");
}
