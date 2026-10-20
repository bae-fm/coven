use super::*;
use aws_sdk_s3::{
    error::{ConnectorError, SdkError},
    operation::{get_object::GetObjectError, list_objects_v2::ListObjectsV2Error},
};
use std::error::Error;

pub(super) fn sdk_error<E: Error + Send + Sync + 'static>(error: &StorageError) -> &SdkError<E> {
    assert!(matches!(
        error,
        StorageError::Provider {
            provider: CloudProvider::S3,
            ..
        }
    ));
    error
        .source()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref()
        .unwrap()
}

#[tokio::test]
async fn timeout_and_storage_full_responses_preserve_their_classification_and_cause() {
    use StorageFailure::{
        Conflict, Network, PermissionDenied, QuotaExceeded, RateLimited, Refused,
    };
    let cases = [
        ("RequestTimeout", 400, Network, true),
        ("ConditionalRequestConflict", 409, Conflict, false),
        ("ConcurrentModification", 409, Conflict, false),
        ("OperationAborted", 409, Conflict, false),
        ("OverQuota", 403, QuotaExceeded, false),
        ("QuotaExceeded", 403, QuotaExceeded, false),
        ("InsufficientStorage", 507, QuotaExceeded, false),
        ("UnknownStorageError", 507, QuotaExceeded, false),
        ("UnknownProviderError", 400, Refused, false),
        ("AccessDenied", 403, PermissionDenied, false),
        ("SlowDown", 503, RateLimited, true),
        ("ServiceUnavailable", 503, Network, true),
    ];
    let state = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(state.clone())).await;
    let storage = provider(&server.url);
    let mut actual = Vec::new();
    for (code, status, _, _) in cases {
        state.lock().unwrap().forced_error = Some((code, status));
        let error = storage
            .list(&ObjectPrefix::all(STORE_ID))
            .await
            .unwrap_err();
        let SdkError::ServiceError(native) = sdk_error::<ListObjectsV2Error>(&error) else {
            panic!("expected the native service error");
        };
        assert_eq!(native.err().code(), Some(code));
        assert_eq!(native.err().message(), Some("credential-echo"));
        assert_eq!(native.raw().status().as_u16(), status);
        assert!(!format!("{error:?} {error}").contains("credential-echo"));
        assert!(error
            .to_string()
            .contains(&format!("HTTP {status} ({code})")));
        actual.push((code, status, error.failure(), error.retryable()));
    }
    assert_eq!(actual, cases);
}

#[tokio::test]
async fn status_only_responses_distinguish_quota_from_transient_failures() {
    for (status, failure, retryable) in [
        (507, StorageFailure::QuotaExceeded, false),
        (408, StorageFailure::Network, true),
        (503, StorageFailure::Network, true),
        (403, StorageFailure::PermissionDenied, false),
    ] {
        let server = TestServer::new(
            Router::new().fallback(move || async move { response(status, Vec::new()) }),
        )
        .await;
        let error = provider(&server.url)
            .list(&ObjectPrefix::all(STORE_ID))
            .await
            .unwrap_err();
        assert_eq!(error.failure(), failure, "status {status}");
        assert_eq!(error.retryable(), retryable);
        assert!(error.answered(), "status {status} is the provider's answer");
        assert_eq!(
            sdk_error::<ListObjectsV2Error>(&error)
                .raw_response()
                .unwrap()
                .status()
                .as_u16(),
            status
        );
    }
}

#[tokio::test]
async fn archived_object_is_a_refusal_with_its_native_archive_state() {
    let server = TestServer::new(Router::new().fallback(|| async {
        response(403, "<Error><Code>InvalidObjectState</Code><Message>credential-echo</Message><StorageClass>DEEP_ARCHIVE</StorageClass><AccessTier>ARCHIVE_ACCESS</AccessTier></Error>")
    })).await;
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    let error = provider(&server.url).read(&path).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::Refused);
    assert!(!error.retryable());
    let SdkError::ServiceError(native) = sdk_error::<GetObjectError>(&error) else {
        panic!("expected the native service error");
    };
    let GetObjectError::InvalidObjectState(archive) = native.err() else {
        panic!("expected the native archive state");
    };
    assert_eq!(
        archive.storage_class().map(|value| value.as_str()),
        Some("DEEP_ARCHIVE")
    );
    assert_eq!(
        archive.access_tier().map(|value| value.as_str()),
        Some("ARCHIVE_ACCESS")
    );
    assert_eq!(archive.message(), Some("credential-echo"));
    assert!(!format!("{error:?} {error}").contains("credential-echo"));
}

#[tokio::test]
async fn malformed_sdk_responses_are_protocol_failures_with_the_original_bytes() {
    for status in [200, 403, 500] {
        let body = "<invalid credential-echo";
        let server =
            TestServer::new(Router::new().fallback(move || async move { response(status, body) }))
                .await;
        let error = provider(&server.url)
            .list(&ObjectPrefix::all(STORE_ID))
            .await
            .unwrap_err();
        assert_eq!(error.failure(), StorageFailure::Protocol, "status {status}");
        assert!(!error.retryable());
        let native = sdk_error::<ListObjectsV2Error>(&error);
        assert!(native.source().is_some());
        let raw = native.raw_response().unwrap();
        assert_eq!(raw.status().as_u16(), status);
        assert_eq!(raw.body().bytes().unwrap(), body.as_bytes());
        assert!(!format!("{error:?} {error}").contains("credential-echo"));
    }
}

#[tokio::test]
async fn sdk_response_error_is_protocol_failure() {
    let server = TestServer::new(
        Router::new()
            .fallback(|| async { response(500, "<Error><Code>InternalError</Code></Error>") }),
    )
    .await;
    let storage = provider(&server.url);
    let SdkError::ServiceError(native) = storage
        .provider
        .client
        .list_objects_v2()
        .bucket("bucket")
        .send()
        .await
        .unwrap_err()
    else {
        panic!("expected the native service error");
    };
    let error = s3_error(SdkError::<ListObjectsV2Error>::response_error(
        "credential-echo",
        native.into_raw(),
    ));
    assert_eq!(error.failure(), StorageFailure::Protocol);
    assert!(!error.retryable());
    let native = sdk_error::<ListObjectsV2Error>(&error);
    assert!(matches!(native, SdkError::ResponseError(_)));
    assert_eq!(native.source().unwrap().to_string(), "credential-echo");
    assert_eq!(native.raw_response().unwrap().status().as_u16(), 500);
    assert!(!format!("{error:?} {error}").contains("credential-echo"));
}

#[tokio::test]
async fn sdk_request_construction_is_invalid_configuration() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let requests = Arc::new(AtomicUsize::new(0));
    let received = requests.clone();
    let server = TestServer::new(Router::new().fallback(move || {
        received.fetch_add(1, Ordering::SeqCst);
        async {
            response(
                200,
                "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>",
            )
        }
    }))
    .await;
    let storage = provider(&server.url);
    let native = storage
        .provider
        .client
        .list_objects_v2()
        .send()
        .await
        .unwrap_err();
    assert!(matches!(native, SdkError::ConstructionFailure(_)));
    assert_eq!(requests.load(Ordering::SeqCst), 0);
    let error = s3_error(native);
    assert_eq!(error.failure(), StorageFailure::InvalidConfiguration);
    assert!(!error.retryable());
    assert!(!error.answered());
    assert!(matches!(
        sdk_error::<ListObjectsV2Error>(&error),
        SdkError::ConstructionFailure(_)
    ));
    assert!(sdk_error::<ListObjectsV2Error>(&error).source().is_some());
}

/// A transport failure names its cause, with any URL cut to its scheme and
/// host: a signed URL's query, a path and credentials in it never print.
#[test]
fn sdk_transport_failures_remain_retryable_and_name_their_redacted_causes() {
    let cause = "error sending request for url \
        (https://ana:hunter2@bucket.example:9000/store/key?X-Amz-Signature=deadbeef)";
    for native in [
        SdkError::<ListObjectsV2Error>::timeout_error(cause),
        SdkError::dispatch_failure(ConnectorError::io(cause.into())),
        SdkError::dispatch_failure(ConnectorError::timeout(cause.into())),
    ] {
        let error = s3_error(native);
        assert_eq!(error.failure(), StorageFailure::Network);
        assert!(error.retryable());
        assert!(!error.answered());
        assert!(sdk_error::<ListObjectsV2Error>(&error).source().is_some());
        for text in [error.to_string(), format!("{error:?}")] {
            assert!(
                text.contains("error sending request for url (https://bucket.example:9000/…)"),
                "{text}"
            );
            for hidden in ["deadbeef", "Signature", "hunter2", "ana", "/store/key"] {
                assert!(!text.contains(hidden), "{hidden} in {text}");
            }
        }
    }
}

/// A certificate the device does not trust fails the request with a message
/// naming the certificate, and nothing of the request but its host.
#[tokio::test]
async fn a_tls_failure_names_the_certificate() {
    use tokio_rustls::rustls::{
        pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
        ServerConfig,
    };
    let certificate = CertificateDer::from_pem_slice(UNTRUSTED_CERTIFICATE.as_bytes()).unwrap();
    let key = PrivateKeyDer::from_pem_slice(UNTRUSTED_KEY.as_bytes()).unwrap();
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate], key)
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            // The client refuses the certificate, so every handshake fails.
            let _ = acceptor.accept(socket).await;
        }
    });
    let storage = provider(&format!("https://{address}"));
    let error = storage
        .status(&ObjectPath::floor(STORE_ID))
        .await
        .unwrap_err();
    assert_eq!(error.failure(), StorageFailure::Network);
    let text = error.to_string();
    assert!(text.contains("certificate"), "{text}");
    assert!(text.contains(&format!("https://{address}/…")), "{text}");
    assert!(!text.contains(&STORE_ID.to_string()), "{text}");
    assert!(!text.contains("secret"), "{text}");
}

/// A self-signed certificate for 127.0.0.1 that no platform trusts.
const UNTRUSTED_CERTIFICATE: &str = "-----BEGIN CERTIFICATE-----
MIIBcjCCARegAwIBAgIUE+fnYR+qkNOKpgMgown0dxy7oBUwCgYIKoZIzj0EAwIw
FTETMBEGA1UEAwwKY292ZW4tdGVzdDAgFw0yNjEwMjAxNzE5MjZaGA8yMTI2MDky
NjE3MTkyNlowFTETMBEGA1UEAwwKY292ZW4tdGVzdDBZMBMGByqGSM49AgEGCCqG
SM49AwEHA0IABA5TrFuO/qRmQLK5eLgIgcwssnHGJsBXi/byXFcxu9y6YtSwBVHs
qK2VZYmNic8bKNE1zOAn3FfTv13ziard7LCjQzBBMB0GA1UdDgQWBBRAOPRh/K+w
ujjxi5i91ZmodDp38TAPBgNVHRMBAf8EBTADAQH/MA8GA1UdEQQIMAaHBH8AAAEw
CgYIKoZIzj0EAwIDSQAwRgIhAIjJLRA89a+nau3kLWVwW6oGWpGZBUQeztMhos4G
wk3nAiEAgMAgCUji019YTa7LZPnPY3zY/+c5WbFpHTU1yvP0A6I=
-----END CERTIFICATE-----
";
const UNTRUSTED_KEY: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgUdyqLLEZT0KXW3gf
sDHHoTdd31SoiUSKvZdBlUqXrOChRANCAAQOU6xbjv6kZkCyuXi4CIHMLLJxxibA
V4v28lxXMbvcumLUsAVR7KitlWWJjYnPGyjRNczgJ9xX079d84mq3eyw
-----END PRIVATE KEY-----
";

#[tokio::test]
async fn throttled_responses_retain_retry_after_seconds_dates_and_missing_basis() {
    use crate::RetryAfter;
    use std::time::Duration;
    for (retry, date, expected) in [
        ("37", None, RetryAfter::Wait(Duration::from_secs(37))),
        (
            "Wed, 21 Oct 2026 07:38:00 GMT",
            Some("Wed, 21 Oct 2026 07:28:00 GMT"),
            RetryAfter::Wait(Duration::from_secs(600)),
        ),
        ("not-a-date", None, RetryAfter::NoTimeBasis),
    ] {
        let server = TestServer::new(Router::new().fallback(move || async move {
            let mut reply = response(429, "<Error><Code>SlowDown</Code></Error>");
            reply
                .headers_mut()
                .insert("retry-after", retry.parse().unwrap());
            if let Some(date) = date {
                reply.headers_mut().insert("date", date.parse().unwrap());
            }
            reply
        }))
        .await;
        let error = provider(&server.url)
            .list(&ObjectPrefix::all(STORE_ID))
            .await
            .unwrap_err();
        assert_eq!(error.failure(), StorageFailure::RateLimited);
        assert_eq!(error.retry_after(), Some(expected));
    }
}
