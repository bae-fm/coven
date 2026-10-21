use super::*;
use crate::{S3Profile, S3ProfileName, SecretText};
use axum::{
    http::{Method, Uri},
    Router,
};
use std::sync::Mutex;

fn location(endpoint: &str, prefix: &str) -> (StorageConfig, S3Credentials) {
    (
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some(endpoint.parse().unwrap()),
            // AWS keys can be limited to a prefix, so the location may name one.
            profile: Some(S3Profile::Named(S3ProfileName::Aws)),
            prefix: prefix.into(),
        },
        S3Credentials {
            access_key_id: "access".into(),
            secret_access_key: SecretText::new("secret".into()),
        },
    )
}

fn reply(body: &'static str) -> axum::response::Response {
    axum::response::Response::builder()
        .status(200)
        .body(axum::body::Body::from(body))
        .unwrap()
}

#[tokio::test]
async fn the_prefix_is_emptied_through_the_production_connector() {
    // One object under the prefix until it is deleted, and no uploads.
    let deleted = Arc::new(Mutex::new(Vec::new()));
    let seen = deleted.clone();
    let router = Router::new().fallback(move |method: Method, uri: Uri| {
        let seen = seen.clone();
        async move {
            let query = uri.query().unwrap_or_default();
            if method == Method::DELETE {
                seen.lock().unwrap().push(uri.path().to_owned());
                return axum::response::Response::builder()
                    .status(204)
                    .body(axum::body::Body::empty())
                    .unwrap();
            }
            assert!(query.contains("prefix=dev%2F"), "{query}");
            if query.contains("uploads") {
                return reply("<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>");
            }
            if seen.lock().unwrap().is_empty() {
                reply("<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>dev/0f8e/floor</Key></Contents></ListBucketResult>")
            } else {
                reply("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>")
            }
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let (location, credentials) = location(&endpoint, "dev");

    let emptied = empty_s3_prefix(location, credentials).await.unwrap();

    assert_eq!(
        emptied,
        EmptiedS3Prefix {
            objects: 1,
            uploads: 0
        }
    );
    assert_eq!(*deleted.lock().unwrap(), ["/bucket/dev/0f8e/floor"]);
    server.abort();
}

#[tokio::test]
async fn a_location_without_a_base_prefix_is_refused() {
    // Nothing listens here: the refusal comes before any request.
    let (location, credentials) = location("http://127.0.0.1:9", "");

    let error = empty_s3_prefix(location, credentials).await.unwrap_err();

    assert!(
        matches!(error, EmptyS3PrefixError::WholeBucket),
        "{error:?}"
    );
}
