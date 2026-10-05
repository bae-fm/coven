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
        body: SecretBytes::new(b"secret echoed by provider".to_vec()),
    };
    assert!(!format!("{source:?} {source}").contains("secret"));
}
