use super::*;
use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{Request, Response, StatusCode};
use axum::Router;
use std::sync::{Arc, Mutex};

use crate::oauth::OAuthTokens;
use coven_foundation::config::ExactUploadVerification;
use coven_keys::keys::StoreKeys;

#[test]
fn onedrive_does_not_accept_drive_cancellation_status() {
    let drive_canceled = StatusCode::from_u16(499).expect("valid Drive cancellation status");
    assert!(!onedrive_upload_cancellation_succeeded(drive_canceled));
}

fn home() -> OneDriveCloudHome {
    let config = crate::oauth::OAuthClients::for_tests()
        .config_for(coven_foundation::config::CloudProvider::OneDrive)
        .expect("OneDrive test client");
    let session = OAuthSession::new(
        OAuthTokens {
            access_token: "test".to_string(),
            refresh_token: None,
            expires_at: None,
        },
        StoreKeys::bind("test".to_string()),
        Arc::new(coven_foundation::clock::SystemClock),
        config,
        "OneDrive",
    );
    OneDriveCloudHome::new(
        "drive123".to_string(),
        "folder456".to_string(),
        session,
        ExactUploadVerification::MetadataHash,
    )
}

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: String,
    path: String,
    body: Vec<u8>,
}

#[derive(Clone)]
struct ExactCreateEndpointState {
    endpoint: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
}

async fn exact_create_endpoint(
    State(state): State<ExactCreateEndpointState>,
    request: Request<Body>,
) -> Response<Body> {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
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
            body,
        });

    if method == "POST" && path.ends_with("/createUploadSession") {
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(format!(
                r#"{{"uploadUrl":"{}/upload/session"}}"#,
                state.endpoint
            )))
            .expect("build session response");
    }
    if method == "PUT" && path == "/upload/session" {
        return Response::builder()
            .status(StatusCode::ACCEPTED)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"nextExpectedRanges":[]}"#))
            .expect("build deferred upload response");
    }
    if method == "PUT" && path.contains("/items/folder456:/") {
        return Response::builder()
            .status(StatusCode::CREATED)
            .header("content-type", "application/json")
            .body(Body::from(r#"{"id":"item-1"}"#))
            .expect("build commit response");
    }
    if method == "GET" && path.contains("/items/folder456:/") {
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "id": "item-1",
                    "name": encode_key("protocol/copy"),
                    "parentReference": { "id": "folder456" },
                    "file": {
                        "hashes": {
                            "sha1Hash": "994b62b0e47abf4768a374def3bf8b963eab4abd",
                        },
                    },
                    "size": 10,
                })
                .to_string(),
            ))
            .expect("build metadata response");
    }
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from(format!("unexpected request: {method} {path}")))
        .expect("build unexpected response")
}

async fn exact_create_test_home() -> (
    OneDriveCloudHome,
    Arc<Mutex<Vec<RecordedRequest>>>,
    tokio::sync::oneshot::Sender<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind OneDrive endpoint");
    let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let state = ExactCreateEndpointState {
        endpoint: endpoint.clone(),
        requests: requests.clone(),
    };
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .fallback(exact_create_endpoint)
                .with_state(state),
        )
        .with_graceful_shutdown(async {
            shutdown_rx
                .await
                .expect("receive OneDrive endpoint shutdown");
        })
        .await
        .expect("OneDrive endpoint failed");
    });
    (home().with_graph_api(endpoint), requests, shutdown_tx)
}

#[tokio::test]
async fn exact_create_defers_publication_then_commits_the_destination() {
    let (home, requests, shutdown) = exact_create_test_home().await;
    let slot = home
        .allocate_slot("protocol/copy")
        .await
        .expect("allocate OneDrive slot");
    crate::cloud::create_exact_bytes(&home, &slot, b"copy-bytes", &crate::cloud::no_progress())
        .await
        .expect("create exact OneDrive object");
    assert_eq!(
        slot,
        ObjectSlot::logical("protocol/copy".to_string()).expect("logical OneDrive slot")
    );

    let requests = requests.lock().expect("lock requests");
    assert_eq!(requests.len(), 4, "{requests:?}");
    let session: serde_json::Value =
        serde_json::from_slice(&requests[0].body).expect("parse session body");
    assert_eq!(requests[0].method, "POST");
    assert!(requests[0].path.ends_with("/createUploadSession"));
    assert_eq!(session["item"]["@microsoft.graph.conflictBehavior"], "fail");
    assert_eq!(session["deferCommit"], true);
    assert_eq!(requests[1].path, "/upload/session");
    assert_eq!(requests[1].body, b"copy-bytes");
    let commit: serde_json::Value =
        serde_json::from_slice(&requests[2].body).expect("parse commit body");
    assert_eq!(requests[2].method, "PUT");
    assert_eq!(commit["name"], encode_key("protocol/copy"));
    assert_eq!(commit["@microsoft.graph.conflictBehavior"], "fail");
    assert!(commit["@microsoft.graph.sourceUrl"]
        .as_str()
        .is_some_and(|url| url.ends_with("/upload/session")));
    assert_eq!(requests[3].method, "GET");
    drop(requests);
    shutdown.send(()).expect("shut down OneDrive endpoint");
}

#[tokio::test]
async fn exact_operations_reject_an_opaque_onedrive_locator() {
    let home = home();
    let slot = ObjectSlot::opaque("protocol/other".to_string(), "item-1".to_string())
        .expect("build opaque OneDrive slot");

    let read_error = home
        .read_at(&slot)
        .await
        .expect_err("opaque OneDrive read must fail");
    assert!(
        read_error.to_string().contains("must use its logical key"),
        "{read_error}"
    );
    let delete_error = home
        .delete_at(&slot)
        .await
        .expect_err("opaque OneDrive delete must fail");
    assert!(
        delete_error
            .to_string()
            .contains("must use its logical key"),
        "{delete_error}"
    );
}

async fn ambiguous_commit_endpoint(
    State(endpoint): State<String>,
    request: Request<Body>,
) -> Response<Body> {
    let method = request.method().as_str();
    let path = request.uri().path();
    if method == "POST" && path.ends_with("/createUploadSession") {
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(format!(
                r#"{{"uploadUrl":"{endpoint}/upload/session"}}"#
            )))
            .expect("build upload session response");
    }
    if method == "PUT" && path == "/upload/session" {
        return Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Body::from(r#"{"nextExpectedRanges":[]}"#))
            .expect("build upload response");
    }
    if method == "PUT" && path.contains("/items/folder456:/") {
        let stream = futures_util::stream::iter([
            Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from_static(b"{")),
            Err(std::io::Error::other("commit response interrupted")),
        ]);
        return Response::builder()
            .status(StatusCode::CREATED)
            .body(Body::from_stream(stream))
            .expect("build interrupted commit response");
    }
    if method == "GET" && path.contains("/items/folder456:/") {
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "id": "pre-existing-item",
                    "name": encode_key("protocol/copy"),
                    "parentReference": { "id": "folder456" },
                    "file": {
                        "hashes": {
                            "sha1Hash": "0000000000000000000000000000000000000000",
                        },
                    },
                    "size": 10,
                })
                .to_string(),
            ))
            .expect("build occupant response");
    }
    if method == "DELETE" && path == "/upload/session" {
        return Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .expect("build cancellation response");
    }
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from(format!("unexpected request: {method} {path}")))
        .expect("build unexpected response")
}

#[tokio::test]
async fn ambiguous_commit_does_not_adopt_the_current_path_occupant() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind OneDrive endpoint");
    let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
    let app = Router::new()
        .fallback(ambiguous_commit_endpoint)
        .with_state(endpoint.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("OneDrive endpoint failed");
    });
    let home = home().with_graph_api(endpoint);

    let slot = home
        .allocate_slot("protocol/copy")
        .await
        .expect("allocate OneDrive slot");
    let error =
        crate::cloud::create_exact_bytes(&home, &slot, b"copy-bytes", &crate::cloud::no_progress())
            .await
            .expect_err("ambiguous commit must remain unresolved");

    assert!(matches!(error, CloudHomeError::SlotCollision(_)), "{error}");
    server.abort();
}

#[test]
fn item_path_url_encodes_key() {
    assert_eq!(
            home().item_path_url("objects/dev1/42.enc"),
            "https://graph.microsoft.com/v1.0/drives/drive123/items/folder456:/6f626a656374732f646576312f34322e656e63:"
        );
}

#[test]
fn children_url_format() {
    assert_eq!(
        home().children_url(),
        "https://graph.microsoft.com/v1.0/drives/drive123/items/folder456/children"
    );
}

#[test]
fn parse_list_page_skips_malformed_flat_names() {
    let valid = encode_key("objects/dev1/1.enc");
    let malformed = format!("{}zz", encode_key("objects/"));
    let body = format!(
        r#"{{"value":[{{"name":"{valid}"}},{{"name":"{malformed}"}},{{"name":"{}"}}]}}"#,
        encode_key("snapshots/dev1.json.enc"),
    );

    let page = home()
        .parse_list_page(&body, "objects/")
        .expect("parse list page");

    assert_eq!(page.keys, vec!["objects/dev1/1.enc"]);
}

#[test]
fn oauth_config_uses_consumers_endpoint() {
    let config = crate::oauth::OAuthClients::for_tests()
        .config_for(coven_foundation::config::CloudProvider::OneDrive)
        .expect("build OneDrive oauth config");
    assert!(config.auth_url.contains("/consumers/"));
    assert!(config.token_url.contains("/consumers/"));
    assert!(config.scopes.contains(&"Files.ReadWrite".to_string()));
    assert!(config.scopes.contains(&"offline_access".to_string()));
}

#[test]
fn parse_onedrive_error_code_extracts_quota_limit_reached() {
    let body = r#"{"error":{"code":"quotaLimitReached","message":"Insufficient Storage"}}"#;
    assert_eq!(
        parse_onedrive_error_code(body).as_deref(),
        Some("quotaLimitReached"),
    );
}

#[test]
fn classify_write_error_quota_message_names_provider_and_recovery() {
    let body = r#"{"error":{"code":"quotaLimitReached"}}"#;
    let err = classify_write_error(reqwest::StatusCode::INSUFFICIENT_STORAGE, body, "objects/1");
    let msg = err.to_string();
    assert!(msg.contains("OneDrive storage is full"), "{msg}");
    assert!(msg.contains("Free up space"), "{msg}");
}

#[test]
fn classify_write_error_keeps_raw_for_non_quota_errors() {
    let body = r#"{"error":{"code":"itemNotFound","message":"..."}}"#;
    let err = classify_write_error(reqwest::StatusCode::NOT_FOUND, body, "objects/dev1/1.enc");
    let msg = err.to_string();
    assert!(msg.contains("HTTP 404"), "{msg}");
    assert!(msg.contains("objects/dev1/1.enc"), "{msg}");
    assert!(!msg.contains("storage is full"), "{msg}");
}
