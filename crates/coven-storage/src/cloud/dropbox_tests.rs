use super::*;
use crate::cloud::PartSink;

use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{Request, Response, StatusCode};
use axum::Router;
use std::sync::{Arc, Mutex};

use crate::oauth::OAuthTokens;
use coven_keys::keys::StoreKeys;

fn home() -> DropboxCloudHome {
    home_with_folder("/Apps/your-app/my-store")
}

fn home_with_folder(folder_path: &str) -> DropboxCloudHome {
    let config = crate::oauth::OAuthClients::for_tests()
        .config_for(coven_foundation::config::CloudProvider::Dropbox)
        .expect("Dropbox test client");
    let session = OAuthSession::new(
        OAuthTokens {
            access_token: String::new(),
            refresh_token: None,
            expires_at: None,
        },
        StoreKeys::bind("test".to_string()),
        Arc::new(coven_foundation::clock::SystemClock),
        config,
        "Dropbox",
    );
    DropboxCloudHome::new(folder_path.to_string(), session)
}

#[derive(Clone, Debug)]
struct RecordedRequest {
    path: String,
    api_arg: Option<String>,
    body: Vec<u8>,
}

/// Record one incoming request's path, `Dropbox-API-Arg` header, and body
/// into the shared log, returning the path for the endpoint to dispatch on.
async fn record_request(
    requests: &Arc<Mutex<Vec<RecordedRequest>>>,
    request: Request<Body>,
) -> String {
    let path = request.uri().path().to_string();
    let api_arg = request.headers().get("Dropbox-API-Arg").map(|value| {
        value
            .to_str()
            .expect("Dropbox-API-Arg is UTF-8")
            .to_string()
    });
    let body = to_bytes(request.into_body(), usize::MAX)
        .await
        .expect("read request body")
        .to_vec();
    requests
        .lock()
        .expect("lock requests")
        .push(RecordedRequest {
            path: path.clone(),
            api_arg,
            body,
        });
    path
}

async fn immutable_copy_endpoint(
    State(requests): State<Arc<Mutex<Vec<RecordedRequest>>>>,
    request: Request<Body>,
) -> Response<Body> {
    let path = record_request(&requests, request).await;

    match path.as_str() {
        "/sharing/share_folder" => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{".tag":"complete","shared_folder_id":"namespace:copy"}"#,
            ))
            .expect("build share response"),
        "/files/upload" => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"id":"id:copy","path_display":"/protocol/copy"}"#,
            ))
            .expect("build upload response"),
        "/files/download" => Response::builder()
            .status(StatusCode::OK)
            .header(
                "Dropbox-API-Result",
                serde_json::json!({
                    ".tag": "file",
                    "id": "id:copy",
                    "path_display": "/protocol/copy",
                })
                .to_string(),
            )
            .body(Body::from("copy-bytes"))
            .expect("build download response"),
        "/files/get_metadata" => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    ".tag": "file",
                    "id": "id:copy",
                    "path_display": "/protocol/copy",
                })
                .to_string(),
            ))
            .expect("build metadata response"),
        "/files/delete_v2" => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .expect("build delete response"),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from(format!("unexpected path: {path}")))
            .expect("build unexpected response"),
    }
}

async fn immutable_copy_test_home() -> (
    DropboxCloudHome,
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

#[tokio::test]
async fn exact_slot_requests_use_create_only_and_the_logical_path() {
    let (home, requests, shutdown) = immutable_copy_test_home().await;
    let slot = ExactSlotStorage::allocate_slot(&home, "protocol/copy")
        .await
        .expect("allocate Dropbox slot");
    ExactSlotStorage::create_at(
        &home,
        &slot,
        BlobBody::from_bytes(b"copy-bytes".to_vec()),
        &crate::cloud::no_progress(),
    )
    .await
    .expect("create Dropbox slot");
    assert_eq!(
        ExactSlotStorage::read_at(&home, &slot)
            .await
            .expect("read exact slot"),
        b"copy-bytes"
    );
    ExactSlotStorage::delete_at(&home, &slot)
        .await
        .expect("delete exact slot");

    let requests = requests.lock().expect("lock requests");
    assert_eq!(requests.len(), 5, "{requests:?}");
    let upload_arg: serde_json::Value = serde_json::from_str(
        requests[1]
            .api_arg
            .as_deref()
            .expect("upload carries Dropbox-API-Arg"),
    )
    .expect("parse upload arg");
    assert_eq!(requests[0].path, "/sharing/share_folder");
    assert_eq!(requests[1].path, "/files/upload");
    assert_eq!(upload_arg["mode"][".tag"], "add");
    assert_eq!(upload_arg["autorename"], false);
    assert_eq!(upload_arg["strict_conflict"], true);
    assert_eq!(requests[1].body, b"copy-bytes");
    let read_arg: serde_json::Value = serde_json::from_str(
        requests[2]
            .api_arg
            .as_deref()
            .expect("read carries Dropbox-API-Arg"),
    )
    .expect("parse read arg");
    assert_eq!(read_arg["path"], "/protocol/copy");
    let metadata_body: serde_json::Value =
        serde_json::from_slice(&requests[3].body).expect("parse metadata body");
    assert_eq!(requests[3].path, "/files/get_metadata");
    assert_eq!(metadata_body["path"], "/protocol/copy");
    let delete_body: serde_json::Value =
        serde_json::from_slice(&requests[4].body).expect("parse delete body");
    assert_eq!(delete_body["path"], "/protocol/copy");
    drop(requests);
    shutdown.send(()).expect("shut down Dropbox endpoint");
}

#[tokio::test]
async fn exact_operations_reject_an_opaque_dropbox_locator() {
    let (home, requests, shutdown) = immutable_copy_test_home().await;
    let slot = ObjectSlot::opaque("protocol/other".to_string(), "id:copy".to_string())
        .expect("build opaque Dropbox locator");

    let read_error = ExactSlotStorage::read_at(&home, &slot)
        .await
        .expect_err("opaque Dropbox read must fail");
    assert!(
        read_error.to_string().contains("must use its logical key"),
        "{read_error}"
    );
    let delete_error = ExactSlotStorage::delete_at(&home, &slot)
        .await
        .expect_err("opaque Dropbox delete must fail");
    assert!(
        delete_error
            .to_string()
            .contains("must use its logical key"),
        "{delete_error}"
    );

    let requests = requests.lock().expect("lock requests");
    assert!(requests.is_empty(), "{requests:?}");
    drop(requests);
    shutdown.send(()).expect("shut down Dropbox endpoint");
}

async fn binding_and_close_endpoint(
    State(requests): State<Arc<Mutex<Vec<RecordedRequest>>>>,
    request: Request<Body>,
) -> Response<Body> {
    let path = record_request(&requests, request).await;
    let body = match path.as_str() {
        "/files/get_metadata" => serde_json::json!({
            ".tag": "folder",
            "id": "id:store-folder",
            "path_lower": "/apps/your-app/my-store",
        })
        .to_string(),
        "/users/get_current_account" => serde_json::json!({
            "account_id": "dbid:account-a",
        })
        .to_string(),
        "/sharing/share_folder" => serde_json::json!({
            ".tag": "complete",
            "shared_folder_id": "namespace:store-folder",
        })
        .to_string(),
        "/files/upload_session/append_v2" => "{}".to_string(),
        _ => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(format!("unexpected path: {path}")))
                .unwrap()
        }
    };
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

async fn binding_and_close_test_home() -> (
    DropboxCloudHome,
    Arc<Mutex<Vec<RecordedRequest>>>,
    tokio::sync::oneshot::Sender<()>,
) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .fallback(binding_and_close_endpoint)
        .with_state(requests.clone());
    let (endpoint, shutdown_tx) = crate::cloud::test_server::spawn_test_server(app).await;
    (
        home().with_endpoints(endpoint.clone(), endpoint),
        requests,
        shutdown_tx,
    )
}

#[tokio::test]
async fn provider_binding_uses_the_folder_identity_and_current_account() {
    use coven_protocol::objects::{ProviderPrincipalId, StoreProviderBinding};
    let (home, requests, shutdown) = binding_and_close_test_home().await;

    let binding = ExactSlotStorage::provider_binding(&home)
        .await
        .expect("resolve Dropbox binding");

    assert_eq!(
        binding.store,
        StoreProviderBinding::Dropbox {
            namespace_id: "namespace:store-folder".to_string(),
        }
    );
    assert_eq!(
        binding.device.principal,
        ProviderPrincipalId::Dropbox {
            account_id: "dbid:account-a".to_string(),
        }
    );
    assert_eq!(
        requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| request.path.as_str())
            .collect::<Vec<_>>(),
        [
            "/files/get_metadata",
            "/sharing/share_folder",
            "/users/get_current_account"
        ]
    );
    shutdown.send(()).expect("shut down Dropbox endpoint");
}

#[tokio::test]
async fn abort_closes_the_upload_session_at_the_confirmed_offset() {
    let (home, requests, shutdown) = binding_and_close_test_home().await;
    let mut sink = DropboxSessionSink {
        home: &home,
        session_id: "session-a".to_string(),
        key: "blob-a".to_string(),
        completion: DropboxSessionCompletion::Overwrite,
        confirmed_offset: 17,
        settled: false,
    };

    PartSink::abort(&mut sink)
        .await
        .expect("close Dropbox session");

    let requests = requests.lock().unwrap();
    let close = requests.last().expect("close request");
    assert_eq!(close.path, "/files/upload_session/append_v2");
    assert!(close.body.is_empty());
    let arg: serde_json::Value = serde_json::from_str(close.api_arg.as_deref().unwrap()).unwrap();
    assert_eq!(arg["cursor"]["session_id"], "session-a");
    assert_eq!(arg["cursor"]["offset"], 17);
    assert_eq!(arg["close"], true);
    assert!(sink.settled);
    drop(requests);
    shutdown.send(()).expect("shut down Dropbox endpoint");
}

async fn repeated_cursor_endpoint(request: Request<Body>) -> Response<Body> {
    let body = match request.uri().path() {
        "/sharing/share_folder" => r#"{".tag":"complete","shared_folder_id":"namespace:list"}"#,
        "/files/list_folder" | "/files/list_folder/continue" => {
            r#"{"entries":[],"cursor":"same","has_more":true}"#
        }
        path => {
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
        .expect("build repeated cursor response")
}

#[tokio::test]
async fn authoritative_listing_rejects_a_repeated_cursor() {
    let (endpoint, shutdown) = crate::cloud::test_server::spawn_test_server(
        Router::new().fallback(repeated_cursor_endpoint),
    )
    .await;
    let home = home().with_endpoints(endpoint.clone(), endpoint);

    let result = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        home.list("protocol/"),
    )
    .await
    .expect("listing must terminate on a repeated cursor")
    .expect_err("repeated cursor must refuse authoritative coverage");

    assert!(result.to_string().contains("repeated"), "{result}");
    shutdown.send(()).expect("shut down test endpoint");
}

/// `join_info()` (what `set_access` returns to a newly-added member) must
/// carry the folder *path* files are read/written under — the same value
/// passed to `DropboxCloudHome::new` — not Dropbox's own sharing-API
/// `shared_folder_id`, which is an unrelated member-management handle a
/// joiner's `full_path` could never resolve against.
#[test]
fn join_info_carries_the_folder_path() {
    let home = home_with_folder("/Apps/your-app/my-store");
    match home.join_info() {
        CloudHomeJoinInfo::Dropbox { folder_path } => {
            assert_eq!(folder_path, "/Apps/your-app/my-store");
        }
        other => panic!("expected Dropbox join info, got {other:?}"),
    }
}

#[test]
fn dropbox_api_arg_escapes_non_ascii_for_headers() {
    let path = "/Apps/your-app/Folderé/Object🧪.enc";
    let arg = dropbox_api_arg(&serde_json::json!({ "path": path }));

    assert!(arg.is_ascii(), "Dropbox-API-Arg must be ASCII: {arg}");
    assert!(arg.contains(r"\u00e9"), "missing BMP escape: {arg}");
    assert!(
        arg.contains(r"\ud83e\uddea"),
        "missing surrogate-pair escape: {arg}",
    );
    assert!(reqwest::header::HeaderValue::from_str(&arg).is_ok());

    let parsed: serde_json::Value = serde_json::from_str(&arg).expect("parse escaped JSON");
    assert_eq!(parsed["path"].as_str(), Some(path));
}

#[test]
fn oauth_config_uses_dropbox_urls() {
    let config = crate::oauth::OAuthClients::for_tests()
        .config_for(coven_foundation::config::CloudProvider::Dropbox)
        .expect("build Dropbox oauth config");
    assert_eq!(config.auth_url, "https://www.dropbox.com/oauth2/authorize");
    assert_eq!(config.token_url, "https://api.dropboxapi.com/oauth2/token");
    assert!(config.client_secret.is_none());
    assert!(config.scopes.is_empty());
}

#[test]
fn parse_dropbox_error_summary_extracts_insufficient_space() {
    let body = r#"{"error_summary":"path/insufficient_space/.tag","error":{".tag":"path"}}"#;
    assert_eq!(
        parse_dropbox_error_summary(body).as_deref(),
        Some("path/insufficient_space/.tag"),
    );
}

#[test]
fn classify_write_error_quota_message_names_provider_and_recovery() {
    let body = r#"{"error_summary":"path/insufficient_space/..","error":{}}"#;
    let err = classify_write_error(reqwest::StatusCode::INSUFFICIENT_STORAGE, body, "objects/1");
    let msg = err.to_string();
    assert!(msg.contains("Dropbox storage is full"), "{msg}");
    assert!(msg.contains("Free up space"), "{msg}");
}

#[test]
fn classify_write_error_preserves_file_conflict_as_already_exists() {
    let body = r#"{"error_summary":"path/conflict/file","error":{}}"#;
    let err = classify_write_error(reqwest::StatusCode::CONFLICT, body, "objects/dev1/1.enc");
    assert!(matches!(err, CloudHomeError::AlreadyExists(ref key) if key == "objects/dev1/1.enc"));
    assert!(!err.is_retryable());
}

#[test]
fn revoke_error_accepts_only_not_a_member_as_absent() {
    let absent = r#"{
            "error_summary": "member_error/not_a_member/...",
            "error": {
                ".tag": "member_error",
                "member_error": { ".tag": "not_a_member" }
            }
        }"#;
    assert!(dropbox_revoke_error_is_already_absent(absent));

    let ambiguous = r#"{
            "error_summary": "member_error/invalid_dropbox_id/...",
            "error": {
                ".tag": "member_error",
                "member_error": { ".tag": "invalid_dropbox_id" }
            }
        }"#;
    assert!(!dropbox_revoke_error_is_already_absent(ambiguous));
}

#[test]
fn revoke_launch_response_requires_completion_or_polling() {
    assert!(matches!(
        parse_dropbox_revoke_launch(r#"{".tag":"complete"}"#).expect("parse complete launch"),
        DropboxRevokeLaunch::Complete,
    ));

    match parse_dropbox_revoke_launch(r#"{".tag":"async_job_id","async_job_id":"job-123"}"#)
        .expect("parse async launch")
    {
        DropboxRevokeLaunch::AsyncJob(job_id) => assert_eq!(job_id, "job-123"),
        DropboxRevokeLaunch::Complete => panic!("async launch must carry the job id"),
    }

    assert!(parse_dropbox_revoke_launch(r#"{".tag":"async_job_id"}"#).is_err(),);
}

#[test]
fn parse_list_page_rejects_has_more_without_cursor() {
    let body = r#"{"entries":[],"has_more":true}"#;
    match home().parse_list_page(body, "") {
        Ok(_) => panic!("has_more without cursor must fail"),
        Err(err) => assert!(err.to_string().contains("cursor"), "{err}"),
    }
}

#[test]
fn parse_list_page_preserves_non_ascii_namespace_relative_key() {
    assert_list_page_preserves_namespace_relative_key(
        "/Apps/your-app/Folderé",
        "/objects/dev1/Foldéré.enc",
        "objects/dev1/Foldéré.enc",
    );
}

#[test]
fn parse_list_page_does_not_case_fold_namespace_relative_keys() {
    assert_list_page_preserves_namespace_relative_key(
        "/Apps/your-app/İlib",
        "/objects/dev1/İlib.enc",
        "objects/dev1/İlib.enc",
    );
}

fn assert_list_page_preserves_namespace_relative_key(
    folder_path: &str,
    path_display: &str,
    expected_key: &str,
) {
    let home = home_with_folder(folder_path);
    let body = serde_json::json!({
        "entries": [{
            ".tag": "file",
            "path_display": path_display,
        }],
        "has_more": false,
    })
    .to_string();
    let page = home
        .parse_list_page(&body, "objects/")
        .expect("parse list page");

    assert_eq!(page.keys, vec![expected_key]);
}
