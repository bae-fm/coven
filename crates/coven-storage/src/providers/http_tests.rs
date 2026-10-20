use super::*;
use crate::providers::tests::{response, TestServer};
use axum::Router;

/// GET `url` through the providers' client, as a OneDrive call.
async fn get(url: &str) -> Result<reqwest::Response, StorageError> {
    let client = crate::providers::http_client::HttpClient::new(std::sync::Arc::new(
        coven_foundation::clock::SystemClock,
    ))
    .unwrap();
    client
        .send(
            CloudProvider::OneDrive,
            client.request(reqwest::Method::GET, url),
        )
        .await
}

#[test]
fn malformed_provider_timestamps_are_protocol_failures_with_the_parse_cause() {
    use std::error::Error;

    fn check_cause<E: Error + 'static>(error: &StorageError, expected: E) {
        let cause = error.source().unwrap().downcast_ref::<E>().unwrap();
        assert_eq!(cause.to_string(), expected.to_string());
    }

    for value in ["not-a-timestamp", "2026-99-06T00:00:00Z"] {
        let error = timestamp(&serde_json::json!({"stored_at": value}), "stored_at").unwrap_err();
        assert_eq!(error.failure(), StorageFailure::Protocol);
        let cause = aws_sdk_s3::primitives::DateTime::from_str(
            value,
            aws_sdk_s3::primitives::DateTimeFormat::DateTimeWithOffset,
        )
        .unwrap_err();
        check_cause(&error, cause);
    }
}

#[test]
fn oauth_error_codes_determine_the_action_before_http_status() {
    // S3 classifies the Google token endpoint's replies to GCS admin sign-in.
    for provider in [
        CloudProvider::S3,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
    ] {
        for (code, expected) in [
            ("invalid_grant", StorageFailure::Authentication),
            ("invalid_client", StorageFailure::InvalidConfiguration),
            ("unauthorized_client", StorageFailure::InvalidConfiguration),
            ("deleted_client", StorageFailure::InvalidConfiguration),
            ("invalid_scope", StorageFailure::InvalidConfiguration),
            ("invalid_resource", StorageFailure::InvalidConfiguration),
            ("invalid_request", StorageFailure::InvalidConfiguration),
            (
                "unsupported_grant_type",
                StorageFailure::InvalidConfiguration,
            ),
            (
                "unsupported_response_type",
                StorageFailure::InvalidConfiguration,
            ),
            ("interaction_required", StorageFailure::Authentication),
            ("consent_required", StorageFailure::Authentication),
            ("temporarily_unavailable", StorageFailure::Network),
            ("server_error", StorageFailure::Network),
            ("access_denied", StorageFailure::PermissionDenied),
        ] {
            let body = serde_json::json!({"error": code}).to_string();
            for status in [400, 401, 403] {
                assert_eq!(
                    classify(provider, status, body.as_bytes()),
                    expected,
                    "{provider:?}: {status}, {code}"
                );
            }
        }
        for (status, expected) in [
            (400, StorageFailure::Refused),
            (429, StorageFailure::RateLimited),
            (503, StorageFailure::Network),
        ] {
            assert_eq!(
                classify(provider, status, br#"{"error":"unrecognized_error"}"#),
                expected
            );
        }
    }
}

#[test]
fn typed_provider_error_mapping() {
    let cases = [
        (
            CloudProvider::Dropbox,
            416,
            serde_json::Value::Null,
            StorageFailure::InvalidRange,
        ),
        (
            CloudProvider::Dropbox,
            409,
            serde_json::json!({"error_summary":"path/insufficient_space/..."}),
            StorageFailure::QuotaExceeded,
        ),
        (
            CloudProvider::Dropbox,
            409,
            serde_json::json!({"error_summary":"path/not_found/..."}),
            StorageFailure::NotFound,
        ),
        (
            CloudProvider::Dropbox,
            409,
            serde_json::json!({"error_summary":"path/not_folder/..."}),
            StorageFailure::NotFound,
        ),
        (
            CloudProvider::Dropbox,
            409,
            serde_json::json!({"error_summary":"path/no_permission/..."}),
            StorageFailure::PermissionDenied,
        ),
        (
            CloudProvider::OneDrive,
            507,
            serde_json::json!({"error":{"code":"quotaLimitReached"}}),
            StorageFailure::QuotaExceeded,
        ),
        (
            CloudProvider::OneDrive,
            401,
            serde_json::json!({"error":{"code":"InvalidAuthenticationToken"}}),
            StorageFailure::Authentication,
        ),
        (
            CloudProvider::OneDrive,
            403,
            serde_json::json!({"error":{"code":"accessDenied"}}),
            StorageFailure::PermissionDenied,
        ),
        (
            CloudProvider::OneDrive,
            404,
            serde_json::json!({"error":{"code":"itemNotFound"}}),
            StorageFailure::NotFound,
        ),
    ];
    for (provider, status, body, expected) in cases {
        assert_eq!(
            classify(provider, status, &serde_json::to_vec(&body).unwrap()),
            expected
        );
    }
    assert_eq!(
        classify(CloudProvider::Dropbox, 503, b"upstream unavailable"),
        StorageFailure::Network
    );
}
#[tokio::test]
async fn ranged_reads_refuse_ignored_ranges_and_wrong_offsets() {
    let server =
        TestServer::new(Router::new().fallback(|| async { response(200, "abcdefgh") })).await;
    let response = get(&server.url).await.unwrap();
    assert!(matches!(
        read_response(
            CloudProvider::Dropbox,
            response,
            Some(ByteRange::new(3, 7).unwrap())
        )
        .await,
        Err(error) if error.failure() == StorageFailure::Protocol
    ));
    assert!(validate_content_range("bytes 0-3/8", ByteRange::new(3, 7).unwrap()).is_err());
    assert!(validate_content_range("bytes 3-6/6", ByteRange::new(3, 7).unwrap()).is_err());
}
#[tokio::test]
async fn network_errors_are_distinct_and_response_bodies_do_not_print() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let error = get(&format!("http://{address}")).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::Network);
    assert!(error.retryable());
    assert!(!error.answered(), "a refused connection is no answer");
    // A server error is classified Network, but the provider answered (E5).
    let outage = ProviderResponse::new(503, reqwest::header::HeaderMap::new(), Vec::new())
        .into_error(CloudProvider::OneDrive);
    assert_eq!(outage.failure(), StorageFailure::Network);
    assert!(outage.answered());
    let source = ProviderResponse {
        status: 403,
        headers: reqwest::header::HeaderMap::new(),
        body: SecretBytes::new(b"secret echoed by provider".to_vec()),
    };
    assert!(!format!("{source:?} {source}").contains("secret"));
}

#[test]
fn dropbox_protocol_states_are_not_create_conflicts() {
    for (tag, expected) in [
        ("closed", StorageFailure::Refused),
        ("incorrect_offset", StorageFailure::Refused),
        ("too_many_write_operations", StorageFailure::RateLimited),
        ("too_many_requests", StorageFailure::RateLimited),
        ("rate_limit", StorageFailure::RateLimited),
        (
            "access_error/no_permission",
            StorageFailure::PermissionDenied,
        ),
    ] {
        let body = serde_json::json!({"error_summary":format!("{tag}/...")}).to_string();
        assert_eq!(
            classify(CloudProvider::Dropbox, 409, body.as_bytes()),
            expected,
            "{tag}"
        );
    }
    assert_eq!(classify(CloudProvider::Dropbox, 200, br#"{".tag":"failed","failed":{".tag":"access_error","access_error":{".tag":"no_permission"}}}"#), StorageFailure::PermissionDenied);
}

#[test]
fn dropbox_native_failures_in_tags_and_summary_prefixes() {
    use serde_json::json;

    let cases = [
        (
            409,
            "path/no_write_permission",
            json!({".tag":"path","path":{"reason":{".tag":"no_write_permission"},"upload_session_id":"session"}}),
            StorageFailure::PermissionDenied,
        ),
        (
            409,
            "path/upload_traffic_limit_reached",
            json!({".tag":"path","path":{".tag":"upload_traffic_limit_reached"}}),
            StorageFailure::QuotaExceeded,
        ),
        (
            409,
            "path/team_folder_insufficient_space",
            json!({".tag":"path","path":{".tag":"team_folder_insufficient_space"}}),
            StorageFailure::QuotaExceeded,
        ),
        (
            409,
            "path/member_folder_insufficient_space",
            json!({".tag":"path","path":{".tag":"member_folder_insufficient_space"}}),
            StorageFailure::QuotaExceeded,
        ),
        (
            409,
            "path/insufficient_space",
            json!({".tag":"path","path":{".tag":"insufficient_space"}}),
            StorageFailure::QuotaExceeded,
        ),
        (
            403,
            "invalid_account_type/feature",
            json!({".tag":"invalid_account_type","invalid_account_type":{".tag":"feature"}}),
            StorageFailure::QuotaExceeded,
        ),
        (
            403,
            "invalid_account_type/endpoint",
            json!({".tag":"invalid_account_type","invalid_account_type":{".tag":"endpoint"}}),
            StorageFailure::PermissionDenied,
        ),
        (
            401,
            "user_suspended",
            json!({".tag":"user_suspended"}),
            StorageFailure::PermissionDenied,
        ),
        (
            401,
            "missing_scope",
            json!({".tag":"missing_scope","required_scope":"files.content.write"}),
            StorageFailure::InvalidConfiguration,
        ),
        (
            401,
            "expired_access_token",
            json!({".tag":"expired_access_token"}),
            StorageFailure::Authentication,
        ),
        (
            401,
            "invalid_access_token",
            json!({".tag":"invalid_access_token"}),
            StorageFailure::Authentication,
        ),
        (
            409,
            "rate_limit",
            json!({".tag":"rate_limit"}),
            StorageFailure::RateLimited,
        ),
        (
            409,
            "path/too_many_write_operations",
            json!({".tag":"path","path":{".tag":"too_many_write_operations"}}),
            StorageFailure::RateLimited,
        ),
        (
            429,
            "too_many_requests",
            json!({"reason":{".tag":"too_many_requests"},"retry_after":37}),
            StorageFailure::RateLimited,
        ),
    ];
    for (status, summary, error, expected) in cases {
        let mut bodies = vec![json!({"error":error})];
        for suffix in ["", ".", "/...", "/detail/..."] {
            bodies.push(json!({"error_summary":format!("{summary}{suffix}")}));
        }
        for body in bodies {
            assert_eq!(
                classify(CloudProvider::Dropbox, status, body.to_string().as_bytes()),
                expected,
                "HTTP {status}: {body}"
            );
        }
    }
}

#[test]
fn dropbox_category_precedence_preserves_specific_failures() {
    use serde_json::json;

    for (status, body, expected) in [
        (
            409,
            json!({"error_summary":"path/conflict/file/...","error":{".tag":"path","path":{".tag":"no_write_permission"}}}),
            StorageFailure::PermissionDenied,
        ),
        (
            409,
            json!({"error_summary":"path/conflict/file/...","error":{".tag":"path","path":{".tag":"team_folder_insufficient_space"}}}),
            StorageFailure::QuotaExceeded,
        ),
        (
            401,
            json!({"error_summary":"expired_access_token/...","error":{".tag":"missing_scope","required_scope":"files.content.write"}}),
            StorageFailure::InvalidConfiguration,
        ),
        (
            409,
            json!({"error_summary":"path/no_write_permission/...","error":{".tag":"rate_limit"}}),
            StorageFailure::RateLimited,
        ),
        (
            200,
            json!({".tag":"failed","failed":{".tag":"remove_folder_member_error","remove_folder_member_error":{".tag":"access_error","access_error":{".tag":"no_permission"}}}}),
            StorageFailure::PermissionDenied,
        ),
        (
            409,
            json!({"error_summary":"path/conflict/file/..."}),
            StorageFailure::AlreadyExists,
        ),
        (
            409,
            json!({"error_summary":"path/no_write_permission_extra/..."}),
            StorageFailure::Refused,
        ),
        (
            409,
            json!({"error_summary":"path/unknown_error/..."}),
            StorageFailure::Refused,
        ),
        (
            403,
            json!({"error_summary":"invalid_account_type/feature_extra/..."}),
            StorageFailure::PermissionDenied,
        ),
        (
            403,
            json!({"error":{".tag":"invalid_account_type","invalid_account_type":{".tag":"endpoint"},"detail":{".tag":"feature"}}}),
            StorageFailure::PermissionDenied,
        ),
    ] {
        assert_eq!(
            classify(CloudProvider::Dropbox, status, body.to_string().as_bytes()),
            expected,
            "HTTP {status}: {body}"
        );
    }
}

#[tokio::test]
async fn dropbox_native_classification_preserves_provider_wait_and_cause() {
    use serde_json::json;

    for (status, body, expected, retryable) in [
        (
            409,
            json!({"error_summary":"rate_limit/...","error":{".tag":"rate_limit"},"retry_after":86400}),
            StorageFailure::RateLimited,
            true,
        ),
        (
            429,
            json!({"error":{"reason":{".tag":"too_many_requests"},"retry_after":37}}),
            StorageFailure::RateLimited,
            true,
        ),
        (
            409,
            json!({"error":{".tag":"path","path":{".tag":"upload_traffic_limit_reached"}},"retry_after":86400}),
            StorageFailure::QuotaExceeded,
            false,
        ),
        (
            403,
            json!({"error":{".tag":"invalid_account_type","invalid_account_type":{".tag":"feature"}},"retry_after":86400}),
            StorageFailure::QuotaExceeded,
            false,
        ),
        (
            401,
            json!({"error":{".tag":"user_suspended"}}),
            StorageFailure::PermissionDenied,
            false,
        ),
        (
            401,
            json!({"error":{".tag":"missing_scope","required_scope":"files.content.write"}}),
            StorageFailure::InvalidConfiguration,
            false,
        ),
    ] {
        let body = body.to_string();
        let reply = body.clone();
        let server = TestServer::new(Router::new().fallback(move || {
            let reply = reply.clone();
            async move {
                axum::http::Response::builder()
                    .status(status)
                    .header("Retry-After", "86400")
                    .header("X-Dropbox-Request-Id", "secret-request-id")
                    .body(axum::body::Body::from(reply))
                    .unwrap()
            }
        }))
        .await;
        let response = get(&server.url).await.unwrap();
        let error = checked(CloudProvider::Dropbox, response).await.unwrap_err();
        assert_eq!(error.failure(), expected, "{body}");
        assert_eq!(error.retryable(), retryable, "{body}");
        assert!(!format!("{error} {error:?}").contains("secret-request-id"));
        let StorageError::Provider {
            provider, source, ..
        } = error
        else {
            panic!("native response discarded")
        };
        assert_eq!(provider, CloudProvider::Dropbox);
        let response = source.downcast_ref::<ProviderResponse>().unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["retry-after"], "86400");
        assert_eq!(
            response.headers()["x-dropbox-request-id"],
            "secret-request-id"
        );
        assert_eq!(response.body(), body.as_bytes());
    }
}

#[test]
fn malformed_range_replies_are_provider_errors() {
    for header in ["bytes 0-3/8", "bytes 3-6/6", "bytes 3-6/*", "bytes 3-6/bad"] {
        assert_eq!(
            validate_content_range(header, ByteRange::new(3, 7).unwrap())
                .unwrap_err()
                .failure(),
            StorageFailure::Protocol,
            "{header}"
        );
    }
}

#[tokio::test]
async fn malformed_success_keeps_the_original_response() {
    let server = TestServer::new(
        Router::new().fallback(|| async { response(200, "not-json credential-echo") }),
    )
    .await;
    let response = get(&server.url).await.unwrap();
    let error = json(CloudProvider::OneDrive, response).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::Protocol);
    assert!(!format!("{error} {error:?}").contains("credential-echo"));
    let StorageError::Provider { source, .. } = error else {
        panic!("native response discarded")
    };
    let response = source.downcast_ref::<ProviderResponse>().unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.body(), b"not-json credential-echo");
}

#[tokio::test]
async fn throttling_retains_headers_without_printing_them() {
    for provider in [CloudProvider::Dropbox, CloudProvider::OneDrive] {
        let server = TestServer::new(Router::new().fallback(|| async {
            axum::http::Response::builder()
                .status(429)
                .header("Retry-After", "37")
                .header("X-Request-Id", "secret-request-id")
                .body(axum::body::Body::from("secret-native-cause"))
                .unwrap()
        }))
        .await;
        let response = get(&server.url).await.unwrap();
        let error = checked(provider, response).await.unwrap_err();
        assert_eq!(error.failure(), StorageFailure::RateLimited);
        assert!(error.retryable());
        assert!(!format!("{error} {error:?}").contains("secret"));
        let StorageError::Provider { source, .. } = error else {
            panic!("native response discarded")
        };
        let response = source.downcast_ref::<ProviderResponse>().unwrap();
        assert_eq!(response.headers()["retry-after"], "37");
        assert_eq!(response.headers()["x-request-id"], "secret-request-id");
        assert_eq!(response.body(), b"secret-native-cause");
    }
}

#[tokio::test]
async fn interrupted_bodies_keep_the_transport_failure() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for provider in [CloudProvider::Dropbox, CloudProvider::OneDrive] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            socket.write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Length: 4\r\nContent-Range: bytes 3-6/8\r\nConnection: close\r\n\r\nx").await.unwrap();
            socket.shutdown().await.unwrap();
        });
        let response = get(&format!("http://{address}")).await.unwrap();
        let error = read_response(provider, response, Some(ByteRange::new(3, 7).unwrap()))
            .await
            .unwrap_err();
        assert_eq!(error.failure(), StorageFailure::Network);
        assert!(
            !error.answered(),
            "an interrupted body is no usable response"
        );
        let StorageError::Provider { source, .. } = error else {
            panic!("transport cause discarded")
        };
        assert!(source.downcast_ref::<reqwest::Error>().is_some());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn ranged_body_must_match_the_entire_requested_interval() {
    for body in ["abc", "abcde"] {
        let server = TestServer::new(Router::new().fallback(move || async move {
            axum::http::Response::builder()
                .status(206)
                .header("Content-Range", "bytes 3-6/8")
                .body(axum::body::Body::from(body))
                .unwrap()
        }))
        .await;
        let response = get(&server.url).await.unwrap();
        assert_eq!(
            read_response(
                CloudProvider::Dropbox,
                response,
                Some(ByteRange::new(3, 7).unwrap())
            )
            .await
            .unwrap_err()
            .failure(),
            StorageFailure::Protocol
        );
    }
}

#[test]
fn a_valid_provider_clamp_identifies_a_range_past_eof() {
    assert!(matches!(
        validate_content_range("bytes 3-5/6", ByteRange::new(3, 7).unwrap()),
        Err(error) if error.failure() == StorageFailure::InvalidRange
    ));
    for header in [
        "bytes 3-4/6",
        "bytes 2-5/6",
        "bytes 3-6/6",
        "bytes 5-3/6",
        "bytes 3-x/6",
    ] {
        assert_eq!(
            validate_content_range(header, ByteRange::new(3, 7).unwrap())
                .unwrap_err()
                .failure(),
            StorageFailure::Protocol
        );
    }
}

async fn read_response(
    provider: CloudProvider,
    response: reqwest::Response,
    range: Option<ByteRange>,
) -> Result<Vec<u8>, StorageError> {
    let metadata = crate::StoredObject {
        path: crate::ObjectPath::floor(crate::test_utils::STORE_ID),
        size: 8,
        stored_at: coven_format::value::StorageTime::new(0, 0).unwrap(),
        time_resolution: std::time::Duration::from_secs(1),
        object_id: "object".into(),
        revision: "1".into(),
        replace_token: "1".into(),
        checksum: None,
    };
    Ok(
        crate::providers::http_body::stream(provider, response, metadata, range)
            .await?
            .collect(8)
            .await?
            .bytes,
    )
}

struct ControlledPublication(std::sync::atomic::AtomicBool);
impl crate::PublicationGate for ControlledPublication {
    fn check(&self, _: &crate::ObjectPath) -> Result<(), StorageError> {
        if self.0.load(std::sync::atomic::Ordering::SeqCst) {
            Ok(())
        } else {
            Err(StorageFailure::CatchUpRequired.into())
        }
    }
}

#[tokio::test]
async fn publication_is_rechecked_after_waiting_for_tokens() {
    use std::{
        future::Future,
        sync::{atomic::Ordering, Arc},
        task::Poll,
    };
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = requests.clone();
    let server = TestServer::new(Router::new().fallback(move || {
        let observed = observed.clone();
        async move {
            observed.fetch_add(1, Ordering::SeqCst);
            response(200, "published")
        }
    }))
    .await;
    let session = crate::providers::tests::session(CloudProvider::Dropbox);
    let gate = Arc::new(ControlledPublication(std::sync::atomic::AtomicBool::new(
        true,
    )));
    let publication = crate::PublicationRequest::new(
        gate.clone(),
        crate::ObjectPath::floor(crate::test_utils::STORE_ID),
    );
    let tokens = session.tokens.hold().await;
    let mut request = Box::pin(session.publish(
        &publication,
        Method::POST,
        &server.url,
        &[],
        Body::Empty,
        true,
    ));
    std::future::poll_fn(|cx| {
        assert!(request.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    gate.0.store(false, Ordering::SeqCst);
    drop(tokens);
    assert_eq!(
        request.await.unwrap_err().failure(),
        StorageFailure::CatchUpRequired
    );
    assert_eq!(requests.load(Ordering::SeqCst), 0);
    gate.0.store(true, Ordering::SeqCst);
    session
        .publish(
            &publication,
            Method::POST,
            &server.url,
            &[],
            Body::Empty,
            true,
        )
        .await
        .unwrap();
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

/// A OneDrive endpoint that rejects every token in `rejected` with HTTP 401 and
/// records each request's token and body.
async fn rejecting_server(
    rejected: &'static [&'static str],
    body: &'static str,
) -> (TestServer, Arc<std::sync::Mutex<Vec<(String, Vec<u8>)>>>) {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = seen.clone();
    let server = TestServer::new(Router::new().fallback(
        move |headers: axum::http::HeaderMap, bytes: axum::body::Bytes| {
            let observed = observed.clone();
            async move {
                let token = headers["authorization"].to_str().unwrap().to_owned();
                observed
                    .lock()
                    .unwrap()
                    .push((token.clone(), bytes.to_vec()));
                if rejected.iter().any(|t| token == format!("Bearer {t}")) {
                    response(401, body)
                } else {
                    response(200, "ok")
                }
            }
        },
    ))
    .await;
    (server, seen)
}

fn session_with(
    provider: CloudProvider,
    custody: Arc<crate::token_custody::tests::CountingCustody>,
) -> OAuthSession {
    OAuthSession::new(
        provider,
        OAuthTokens {
            client_id: "test-client".into(),
            access: crate::providers::tests::full_access(provider),
            access_token: coven_crypto::SecretText::new("token".into()),
            refresh_token: None,
        },
        custody,
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
        crate::providers::tests::http_client(),
    )
    .unwrap()
}

const AUTH_ERROR: &str = r#"{"error":{"code":"InvalidAuthenticationToken"}}"#;

#[tokio::test]
async fn a_rejected_token_refreshes_once_and_retries_the_same_request_once() {
    let custody = crate::token_custody::tests::CountingCustody::new(false);
    let session = session_with(CloudProvider::OneDrive, custody.clone());
    let (server, seen) = rejecting_server(&["token"], AUTH_ERROR).await;
    let reply = session
        .send(
            Method::PUT,
            &server.url,
            &[],
            Body::Bytes(b"part".to_vec()),
            true,
        )
        .await
        .unwrap();
    assert_eq!(reply.status(), 200);
    session
        .send(Method::GET, &server.url, &[], Body::Empty, true)
        .await
        .unwrap();
    assert_eq!(custody.count(), 1);
    let seen = seen.lock().unwrap();
    let tokens: Vec<_> = seen.iter().map(|(token, _)| token.as_str()).collect();
    assert_eq!(
        tokens,
        ["Bearer token", "Bearer refreshed-0", "Bearer refreshed-0"]
    );
    assert_eq!(seen[0].1, b"part");
    assert_eq!(seen[1].1, b"part", "the retry resends the same body");
}

#[tokio::test]
async fn a_second_rejection_reaches_the_caller_without_another_refresh() {
    let custody = crate::token_custody::tests::CountingCustody::new(false);
    let session = session_with(CloudProvider::OneDrive, custody.clone());
    let (server, seen) = rejecting_server(&["token", "refreshed-0"], AUTH_ERROR).await;
    let reply = session
        .send(Method::GET, &server.url, &[], Body::Empty, true)
        .await
        .unwrap();
    assert_eq!(
        checked(CloudProvider::OneDrive, reply)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Authentication
    );
    assert_eq!(custody.count(), 1);
    assert_eq!(seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn concurrent_rejected_requests_share_one_refresh() {
    let custody = crate::token_custody::tests::CountingCustody::new(false);
    let session = session_with(CloudProvider::OneDrive, custody.clone());
    let (server, _) = rejecting_server(&["token"], AUTH_ERROR).await;
    let send = || session.send(Method::GET, &server.url, &[], Body::Empty, true);
    let (a, b, c, d) = tokio::join!(send(), send(), send(), send());
    for reply in [a, b, c, d] {
        assert_eq!(reply.unwrap().status(), 200);
    }
    assert_eq!(custody.count(), 1);
}

#[tokio::test]
async fn rejections_a_refresh_cannot_cure_do_not_refresh() {
    let custody = crate::token_custody::tests::CountingCustody::new(false);
    let session = session_with(CloudProvider::Dropbox, custody.clone());
    let (server, seen) = rejecting_server(
        &["token"],
        r#"{"error_summary":"missing_scope/..","error":{".tag":"missing_scope","required_scope":"files.content.write"}}"#,
    )
    .await;
    let error = session
        .send(Method::POST, &server.url, &[], Body::Empty, true)
        .await
        .unwrap_err();
    assert_eq!(error.failure(), StorageFailure::InvalidConfiguration);
    assert_eq!(custody.count(), 0);
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_refresh_its_custody_cannot_commit_fails_the_request_without_retrying() {
    let custody = crate::token_custody::tests::CountingCustody::new(true);
    let session = session_with(CloudProvider::OneDrive, custody.clone());
    let (server, seen) = rejecting_server(&["token"], AUTH_ERROR).await;
    let error = session
        .send(Method::GET, &server.url, &[], Body::Empty, true)
        .await
        .unwrap_err();
    assert_eq!(error.failure(), StorageFailure::File);
    assert_eq!(seen.lock().unwrap().len(), 1);
    // Nothing was installed: the next request still sends the rejected token.
    session
        .send(Method::GET, &server.url, &[], Body::Empty, true)
        .await
        .unwrap_err();
    assert!(seen
        .lock()
        .unwrap()
        .iter()
        .all(|(token, _)| token == "Bearer token"));
}
