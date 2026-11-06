use super::*;
use crate::{S3Profile, S3ProfileName, SecretText};
use axum::{
    http::{Method, Uri},
    Router,
};
use std::sync::Mutex;

fn location(endpoint: &str) -> (StorageConfig, S3Credentials) {
    (
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some(endpoint.parse().unwrap()),
            profile: Some(S3Profile::Named(S3ProfileName::Aws)),
        },
        S3Credentials {
            access_key_id: "access".into(),
            secret_access_key: SecretText::new("secret".into()),
        },
    )
}

fn reply(body: String) -> axum::response::Response {
    axum::response::Response::builder().status(200).body(axum::body::Body::from(body)).unwrap()
}

const STORE: &str = "0f8e0f8e-0f8e-4f8e-8f8e-0f8e0f8e0f8e";

/// A bucket holding `root` at its root, as the listing with delimiter `/`
/// shows it, and `keys` whole; it records each deletion, after which the
/// key is gone.
async fn serve(root: Vec<String>, keys: Vec<String>) -> (String, Arc<Mutex<Vec<String>>>) {
    let deleted = Arc::new(Mutex::new(Vec::new()));
    let seen = deleted.clone();
    let router = Router::new().fallback(move |method: Method, uri: Uri| {
        let (seen, root, keys) = (seen.clone(), root.clone(), keys.clone());
        async move {
            let query = uri.query().unwrap_or_default();
            if method == Method::DELETE {
                seen.lock().unwrap().push(uri.path().trim_start_matches("/bucket/").to_owned());
                return axum::response::Response::builder()
                    .status(204)
                    .body(axum::body::Body::empty())
                    .unwrap();
            }
            if query.contains("uploads") {
                return reply("<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>".into());
            }
            let entries: String = if query.contains("delimiter=%2F") {
                root.iter()
                    .map(|entry| match entry.strip_suffix('/') {
                        Some(_) => format!("<CommonPrefixes><Prefix>{entry}</Prefix></CommonPrefixes>"),
                        None => format!("<Contents><Key>{entry}</Key></Contents>"),
                    })
                    .collect()
            } else {
                let deleted = seen.lock().unwrap();
                keys.iter()
                    .filter(|key| !deleted.contains(key))
                    .map(|key| format!("<Contents><Key>{key}</Key></Contents>"))
                    .collect()
            };
            reply(format!("<ListBucketResult><IsTruncated>false</IsTruncated>{entries}</ListBucketResult>"))
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (endpoint, deleted)
}

#[tokio::test]
async fn a_bucket_holding_only_stores_is_emptied_through_the_production_connector() {
    let keys = vec!["coven-bucket".to_owned(), format!("{STORE}/floor")];
    let root = vec!["coven-bucket".to_owned(), format!("{STORE}/")];
    let (endpoint, deleted) = serve(root, keys.clone()).await;
    let (location, credentials) = location(&endpoint);

    let emptied = empty_s3_bucket(location, credentials).await.unwrap();

    assert_eq!(emptied, EmptiedS3Bucket { objects: 2, uploads: 0 });
    assert_eq!(*deleted.lock().unwrap(), keys);
}

#[tokio::test]
async fn a_bucket_holding_other_files_is_refused_before_anything_is_removed() {
    for root in [vec!["notes.txt"], vec!["music/"], vec!["coven-bucket", "0f8e/"]] {
        let root: Vec<String> = root.into_iter().map(str::to_owned).collect();
        let (endpoint, deleted) = serve(root.clone(), vec!["notes.txt".to_owned()]).await;
        let (location, credentials) = location(&endpoint);

        let error = empty_s3_bucket(location, credentials).await.unwrap_err();

        assert!(matches!(error, EmptyS3BucketError::OtherFiles), "{root:?}: {error:?}");
        assert!(deleted.lock().unwrap().is_empty(), "{root:?}");
    }
}
