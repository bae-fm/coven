use super::*;
use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{Request, Response, StatusCode};
use axum::Router;
use std::sync::{Arc, Mutex};

use crate::oauth::OAuthTokens;
use coven_foundation::config::ExactUploadVerification;
use coven_keys::keys::StoreKeys;

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
        coven_keys::keys::CloudHomeCredentialsOwner::new(StoreKeys::bind("test".to_string()))
            .current(),
        Arc::new(coven_foundation::clock::SystemClock),
        config,
        "Google Drive",
    );
    GoogleDriveCloudHome::new(
        "folder123".to_string(),
        session,
        ExactUploadVerification::MetadataHash,
    )
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
                    r#"{{"id":"generated-id","name":"{}","parents":["folder123"],"trashed":false,"size":"10","md5Checksum":"2f4c3c1992f3016909827d43b8267ae4","appProperties":{{"covenLogicalKey":"protocol/copy"}}}}"#,
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
    let (endpoint, shutdown_tx) = crate::cloud::test_server::spawn_test_server(app).await;
    (
        home().with_endpoints(endpoint.clone(), endpoint),
        requests,
        shutdown_tx,
    )
}

#[derive(Clone)]
struct ExactReadEndpointState {
    body: Vec<u8>,
    /// How many bytes the response declares. A cut body declares the whole
    /// object and delivers less.
    declared: usize,
}

async fn exact_read_endpoint(
    State(state): State<ExactReadEndpointState>,
    request: Request<Body>,
) -> Response<Body> {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let query = request.uri().query().map(str::to_string);

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
                r#"{{"id":"generated-id","name":"{}","parents":["folder123"],"trashed":false,"size":"{}","md5Checksum":"2f4c3c1992f3016909827d43b8267ae4","appProperties":{{"covenLogicalKey":"protocol/copy"}}}}"#,
                encode_key("protocol/copy"),
                state.declared,
            )))
            .expect("build metadata response");
    }
    if method == "GET" && path == "/files/generated-id" {
        let body = if state.body.len() == state.declared {
            Body::from(state.body.clone())
        } else {
            crate::cloud::test_server::cut_body(state.body.clone())
        };
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-length", state.declared.to_string())
            .body(body)
            .expect("build read response");
    }
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from(format!(
            "unexpected request: {method} {path} {query:?}"
        )))
        .expect("build unexpected response")
}

async fn exact_read_test_home(
    body: Vec<u8>,
    declared: usize,
) -> (GoogleDriveCloudHome, tokio::sync::oneshot::Sender<()>) {
    let (endpoint, shutdown) = crate::cloud::test_server::spawn_test_server(
        Router::new()
            .fallback(exact_read_endpoint)
            .with_state(ExactReadEndpointState { body, declared }),
    )
    .await;
    (home().with_endpoints(endpoint.clone(), endpoint), shutdown)
}

fn drive_slot() -> ObjectSlot {
    ObjectSlot::opaque("protocol/copy".to_string(), "generated-id".to_string())
        .expect("valid Drive slot")
}

#[tokio::test]
async fn exact_stream_serves_the_whole_drive_body() {
    let body = b"drive object bytes".to_vec();
    let (home, shutdown) = exact_read_test_home(body.clone(), body.len()).await;

    let mut stream = ExactSlotStorage::open_stream_at(&home, &drive_slot())
        .await
        .expect("open the Drive exact stream");
    let mut received = Vec::new();
    while let Some(part) = futures_util::StreamExt::next(&mut stream).await {
        received.extend_from_slice(&part.expect("Drive body part"));
    }

    assert_eq!(received, body);
    shutdown.send(()).expect("shut down Drive endpoint");
}

/// A body that stops mid-stream ends the stream with an error. It must not
/// look like an object that simply finished early.
#[tokio::test]
async fn a_cut_drive_body_ends_its_stream_with_an_error() {
    let (home, shutdown) = exact_read_test_home(b"drive ".to_vec(), 18).await;

    let mut stream = ExactSlotStorage::open_stream_at(&home, &drive_slot())
        .await
        .expect("open the Drive exact stream");
    let mut received = Vec::new();
    let error = loop {
        match futures_util::StreamExt::next(&mut stream).await {
            Some(Ok(part)) => received.extend_from_slice(&part),
            Some(Err(error)) => break error,
            None => panic!("a cut body must not end the stream cleanly"),
        }
    };

    assert_eq!(received, b"drive ");
    assert!(matches!(error, CloudHomeError::Backend { .. }), "{error}");
    shutdown.send(()).expect("shut down Drive endpoint");
}

#[tokio::test]
async fn immutable_copy_uses_preallocated_id_for_create_read_and_delete() {
    let (home, requests, shutdown) = immutable_copy_test_home().await;
    let slot = home
        .allocate_slot("protocol/copy")
        .await
        .expect("allocate Drive slot");
    crate::cloud::create_exact_bytes(&home, &slot, b"copy-bytes", &crate::cloud::no_progress())
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
    assert_eq!(requests.len(), 7, "{requests:?}");
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
    assert!(!upload.contains("covenCreateToken"), "{upload}");
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
    assert!(requests[4]
        .query
        .as_deref()
        .is_some_and(|query| query.contains("alt=media")));
    assert_eq!(requests[5].method, "GET");
    assert_eq!(requests[5].path, "/files/generated-id");
    assert_eq!(requests[6].method, "DELETE");
    assert_eq!(requests[6].path, "/files/generated-id");
    for request in requests.iter().skip(1) {
        let query = request.query.as_deref().expect("Drive file request query");
        assert!(query.contains("supportsAllDrives=true"), "{request:?}");
    }
    drop(requests);
    shutdown.send(()).expect("shut down Drive endpoint");
}

/// Two Drive files can carry the same name and the same `covenLogicalKey` —
/// Drive does not enforce unique names, and two devices creating the same
/// logical key each allocate their own id. An exact read is told which file it
/// means, so each slot serves its own file's bytes and neither read searches
/// for a filename or elects a winner between them.
async fn duplicate_name_endpoint(
    State(requests): State<Arc<Mutex<Vec<RecordedRequest>>>>,
    request: Request<Body>,
) -> Response<Body> {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let query = request.uri().query().map(str::to_string);
    requests
        .lock()
        .expect("lock requests")
        .push(RecordedRequest {
            method: method.clone(),
            path: path.clone(),
            query: query.clone(),
            body: Vec::new(),
        });
    let Some(file_id) = path.strip_prefix("/files/") else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from(format!("unexpected path: {path}")))
            .expect("build unexpected response");
    };
    if query
        .as_deref()
        .is_some_and(|query| query.contains("fields="))
    {
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "id": file_id,
                    "name": encode_key("protocol/twin"),
                    "parents": ["folder123"],
                    "trashed": false,
                    "size": "9",
                    "md5Checksum": "00000000000000000000000000000000",
                    "appProperties": { LOGICAL_KEY_PROPERTY: "protocol/twin" },
                })
                .to_string(),
            ))
            .expect("build metadata response");
    }
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(format!("bytes-{file_id}")))
        .expect("build read response")
}

#[tokio::test]
async fn duplicate_drive_names_are_read_by_their_own_exact_ids() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (endpoint, shutdown) = crate::cloud::test_server::spawn_test_server(
        Router::new()
            .fallback(duplicate_name_endpoint)
            .with_state(requests.clone()),
    )
    .await;
    let home = home().with_endpoints(endpoint.clone(), endpoint);
    let first = ObjectSlot::opaque("protocol/twin".to_string(), "file-a".to_string())
        .expect("first twin slot");
    let second = ObjectSlot::opaque("protocol/twin".to_string(), "file-b".to_string())
        .expect("second twin slot");

    assert_eq!(
        home.read_at(&first).await.expect("read twin a"),
        b"bytes-file-a"
    );
    assert_eq!(
        home.read_at(&second).await.expect("read twin b"),
        b"bytes-file-b"
    );

    let requests = requests.lock().expect("lock requests");
    assert!(
        requests
            .iter()
            .all(|request| request.path.starts_with("/files/file-")),
        "a read searched for a filename instead of naming its file: {requests:?}",
    );
    drop(requests);
    shutdown.send(()).expect("shut down test endpoint");
}

/// A resumable create writes the same file metadata a bounded one does. No
/// create token is stamped, because nothing reads one: the slot's id is the
/// object's identity and exact inspection checks the id, name, parent and
/// logical key against it.
async fn resumable_create_metadata_endpoint(
    State(bodies): State<Arc<Mutex<Vec<String>>>>,
    request: Request<Body>,
) -> Response<Body> {
    let path = request.uri().path().to_string();
    let body = to_bytes(request.into_body(), usize::MAX)
        .await
        .expect("read request body");
    if path == "/files" {
        bodies
            .lock()
            .expect("lock bodies")
            .push(String::from_utf8(body.to_vec()).expect("session body is UTF-8"));
        return Response::builder()
            .status(StatusCode::OK)
            .header(reqwest::header::LOCATION, "https://upload.invalid/session")
            .body(Body::empty())
            .expect("build session response");
    }
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from(format!("unexpected path: {path}")))
        .expect("build unexpected response")
}

#[tokio::test]
async fn a_resumable_create_session_carries_no_create_token() {
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let (endpoint, shutdown) = crate::cloud::test_server::spawn_test_server(
        Router::new()
            .fallback(resumable_create_metadata_endpoint)
            .with_state(bodies.clone()),
    )
    .await;
    let home = home().with_endpoints(endpoint.clone(), endpoint);
    let slot = ObjectSlot::opaque("protocol/large".to_string(), "generated-id".to_string())
        .expect("opaque Drive slot");

    home.open_resumable_create_session(&slot)
        .await
        .expect("open a resumable create session");

    let bodies = bodies.lock().expect("lock bodies");
    let body = bodies.first().expect("the session request was recorded");
    assert!(body.contains(r#""id":"generated-id""#), "{body}");
    assert!(
        body.contains(&format!(r#""name":"{}""#, encode_key("protocol/large"))),
        "{body}"
    );
    assert!(
        body.contains(r#""covenLogicalKey":"protocol/large""#),
        "{body}"
    );
    assert!(!body.contains("covenCreateToken"), "{body}");
    drop(bodies);
    shutdown.send(()).expect("shut down test endpoint");
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
    let (endpoint, shutdown) = crate::cloud::test_server::spawn_test_server(
        Router::new().fallback(malformed_location_endpoint),
    )
    .await;
    let home = home().with_endpoints(endpoint.clone(), endpoint);
    let slot = ObjectSlot::opaque("protocol/copy".to_string(), "generated-id".to_string())
        .expect("opaque Drive slot");

    let error = home
        .open_resumable_create_session(&slot)
        .await
        .expect_err("non-UTF-8 Location must fail");

    assert!(matches!(
        error,
        CloudHomeError::Backend {
            kind: coven_protocol::objects::StorageBackendFailure::Transport,
            operation,
            source,
        }
            if operation == "read append resumable create Location header for protocol/copy"
                && source.is::<reqwest::header::ToStrError>()
    ));
    shutdown.send(()).expect("shut down test endpoint");
}

async fn repeated_listing_page_endpoint(request: Request<Body>) -> Response<Body> {
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

/// A listing that keeps handing back the same page token is not making
/// progress; it must fail rather than loop or report a partial result as whole.
#[tokio::test]
async fn prefix_listing_rejects_a_repeated_page_token() {
    let (endpoint, shutdown) = crate::cloud::test_server::spawn_test_server(
        Router::new().fallback(repeated_listing_page_endpoint),
    )
    .await;
    let home = home().with_endpoints(endpoint.clone(), endpoint);

    let error = home
        .list_slots("protocol/")
        .await
        .expect_err("repeated listing page token must fail");

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

/// A store folder can live on a shared drive, and Drive hides those from a
/// listing unless it is asked for them.
#[tokio::test]
async fn prefix_listing_includes_shared_drives() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (endpoint, shutdown) = crate::cloud::test_server::spawn_test_server(
        Router::new()
            .fallback(shared_drive_listing_endpoint)
            .with_state(requests.clone()),
    )
    .await;
    let home = home().with_endpoints(endpoint.clone(), endpoint);

    home.list_slots("protocol/")
        .await
        .expect("list a shared Drive folder");

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
        ("GET", "/files/generated-id") => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "id": "generated-id",
                    "name": encode_key("protocol/collision"),
                    "parents": ["folder123"],
                    "trashed": false,
                    "size": "8",
                    "md5Checksum": "00000000000000000000000000000000",
                    "appProperties": {
                        (LOGICAL_KEY_PROPERTY): "protocol/collision",
                    },
                })
                .to_string(),
            ))
            .expect("build collision metadata response"),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("unexpected request"))
            .expect("build unexpected response"),
    }
}

#[tokio::test]
async fn generated_id_collision_preserves_the_pre_existing_file() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (endpoint, shutdown) = crate::cloud::test_server::spawn_test_server(
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
    let error =
        crate::cloud::create_exact_bytes(&home, &slot, b"new bytes", &crate::cloud::no_progress())
            .await
            .expect_err("generated-id collision must fail");

    assert!(matches!(error, CloudHomeError::SlotCollision(_)), "{error}");
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
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
}

async fn ambiguous_create_endpoint(
    State(state): State<AmbiguousCreateState>,
    request: Request<Body>,
) -> Response<Body> {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let query = request.uri().query().map(str::to_string);
    let body = to_bytes(request.into_body(), usize::MAX)
        .await
        .expect("read request body");
    state
        .requests
        .lock()
        .expect("lock requests")
        .push(RecordedRequest {
            method: method.clone(),
            path: path.clone(),
            query,
            body: body.to_vec(),
        });
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
            assert!(!body.contains("covenCreateToken"), "{body}");
            let mut committed = state.committed.lock().expect("lock commit state");
            if *committed {
                Response::builder()
                    .status(StatusCode::CONFLICT)
                    .body(Body::from("allocated id already committed"))
                    .expect("build occupied response")
            } else {
                *committed = true;
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header(reqwest::header::RETRY_AFTER, "0")
                    .body(Body::from("response lost after commit"))
                    .expect("build ambiguous response")
            }
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
                    "size": "15",
                    "md5Checksum": "321235422fa8fa518e07c432c452473c",
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
    let (endpoint, shutdown) = crate::cloud::test_server::spawn_test_server(
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
    crate::cloud::create_exact_bytes(
        &home,
        &slot,
        b"committed bytes",
        &crate::cloud::no_progress(),
    )
    .await
    .expect("logical-key-matched commit resolves ambiguous create");

    assert_eq!(
        state
            .requests
            .lock()
            .expect("lock requests")
            .iter()
            .filter(|request| request.method == "POST")
            .count(),
        2,
        "the lost response is retried once and observes the committed id",
    );

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
            .any(|request| request.method == "DELETE"),
        "ambiguous committed file was deleted"
    );
    assert!(
        !state
            .requests
            .lock()
            .expect("lock requests")
            .iter()
            .any(|request| request
                .query
                .as_deref()
                .is_some_and(|query| query.contains("alt=media"))),
        "ambiguous create settlement downloaded the object body"
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
fn list_file_query_escapes_the_folder_id() {
    let query = list_file_query("folder'1", "protocol/");

    assert!(query.contains("'folder\\'1' in parents"), "{query}");
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
fn parse_list_page_skips_malformed_flat_names() {
    let valid = encode_key("objects/dev1/1.enc");
    let other_prefix = encode_key("snapshots/dev1.json.enc");
    let body = format!(
        r#"{{"files":[{{"id":"file-valid","name":"{valid}"}},{{"id":"file-junk","name":"not-hex"}},{{"id":"file-other","name":"{other_prefix}"}}]}}"#
    );

    let page = home()
        .parse_list_page(&body, "objects/")
        .expect("parse list page");

    assert_eq!(
        page.slots,
        vec![
            ObjectSlot::opaque("objects/dev1/1.enc".to_string(), "file-valid".to_string())
                .expect("opaque slot")
        ]
    );
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
