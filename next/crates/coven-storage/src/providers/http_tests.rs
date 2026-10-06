use super::*;
use crate::providers::tests::{response, TestServer};
use axum::Router;
#[test]
fn typed_provider_error_mapping() {
    let cases = [
        (
            CloudProvider::GoogleDrive,
            403,
            serde_json::json!({"error":{"errors":[{"reason":"storageQuotaExceeded"}]}}),
            StorageFailure::QuotaExceeded,
        ),
        (
            CloudProvider::GoogleDrive,
            403,
            serde_json::json!({"error":{"errors":[{"reason":"insufficientFilePermissions"}]}}),
            StorageFailure::PermissionDenied,
        ),
        (
            CloudProvider::GoogleDrive,
            401,
            serde_json::json!({"error":{"errors":[{"reason":"authError"}]}}),
            StorageFailure::Authentication,
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
    let response = client().unwrap().get(&server.url).send().await.unwrap();
    assert!(matches!(
        bytes(
            CloudProvider::Dropbox,
            response,
            Some(ByteRange::new(3, 7).unwrap())
        )
        .await,
        Err(StorageError::Protocol(_))
    ));
    assert!(validate_content_range("bytes 0-3/8", ByteRange::new(3, 7).unwrap()).is_err());
    assert!(validate_content_range("bytes 3-6/6", ByteRange::new(3, 7).unwrap()).is_err());
}
#[tokio::test]
async fn network_errors_are_distinct_and_response_bodies_do_not_print() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let error = client()
        .unwrap()
        .get(format!("http://{address}"))
        .send()
        .await
        .unwrap_err();
    let error = transport(CloudProvider::OneDrive, error);
    assert_eq!(error.failure(), StorageFailure::Network);
    assert!(error.retryable());
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
    let response = client().unwrap().get(&server.url).send().await.unwrap();
    let error = json(CloudProvider::GoogleDrive, response)
        .await
        .unwrap_err();
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
    for provider in [
        CloudProvider::GoogleDrive,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
    ] {
        let server = TestServer::new(Router::new().fallback(|| async {
            axum::http::Response::builder()
                .status(429)
                .header("Retry-After", "37")
                .header("X-Request-Id", "secret-request-id")
                .body(axum::body::Body::from("secret-native-cause"))
                .unwrap()
        }))
        .await;
        let response = client().unwrap().get(&server.url).send().await.unwrap();
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
    for provider in [
        CloudProvider::GoogleDrive,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            socket.write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Length: 4\r\nContent-Range: bytes 3-6/8\r\nConnection: close\r\n\r\nx").await.unwrap();
            socket.shutdown().await.unwrap();
        });
        let response = client()
            .unwrap()
            .get(format!("http://{address}"))
            .send()
            .await
            .unwrap();
        let error = bytes(provider, response, Some(ByteRange::new(3, 7).unwrap()))
            .await
            .unwrap_err();
        assert_eq!(error.failure(), StorageFailure::Network);
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
        let response = client().unwrap().get(&server.url).send().await.unwrap();
        assert_eq!(
            bytes(
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
