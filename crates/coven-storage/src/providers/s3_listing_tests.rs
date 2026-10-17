use super::*;
use crate::providers::tests::{query, response, TestServer};
use crate::test_utils::STORE_ID;
use axum::{http::Uri, Router};
use coven_foundation::{clock::FixedClock, id_source::SequentialIds};
use std::sync::Arc;

fn storage(url: &str) -> S3Storage {
    S3Storage::new(
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some(url.parse().unwrap()),
            profile: Some(crate::S3Profile::Custom(profile::PLAIN)),
            prefix: "root".into(),
        },
        S3Credentials {
            access_key_id: "access".into(),
            secret_access_key: coven_crypto::SecretText::new("secret".into()),
        },
        Arc::new(FixedClock::new(std::time::UNIX_EPOCH)),
        Arc::new(SequentialIds::new()),
        std::sync::Arc::new(crate::NoCooldown),
    )
    .unwrap()
}

#[tokio::test]
async fn listings_skip_stray_keys_and_folders_but_not_keys_outside_the_request() {
    let server = TestServer::new(Router::new().fallback(|uri: Uri| async move {
        let q = query(&uri);
        let object = |key: &str| {
            format!("<Contents><Key>{key}</Key><Size>5</Size><LastModified>2026-10-06T00:00:00.000Z</LastModified><ETag>\"e\"</ETag></Contents>")
        };
        let body = if q["prefix"] == format!("root/{STORE_ID}/files/") {
            object(&format!("root/{STORE_ID}/seq/2"))
        } else {
            ["seq/1", "seq/01", "seq/1/extra", "Notes.txt", "photos/1", ".coven-uploads/not-a-uuid", ".coven-check/run/marker", "seq/2"]
            .map(|name| object(&format!("root/{STORE_ID}/{name}"))).concat()
        };
        response(200, format!("<ListBucketResult>{body}<IsTruncated>false</IsTruncated></ListBucketResult>"))
    }))
    .await;
    let storage = StorageConnection::from_provider(Arc::new(storage(&server.url)));
    let listed = storage.list(&ObjectPrefix::all(STORE_ID)).await.unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|object| object.path.as_str().to_owned())
            .collect::<Vec<_>>(),
        [format!("{STORE_ID}/seq/1"), format!("{STORE_ID}/seq/2")]
    );
    // A key outside the requested prefix is the service's error, not a stray.
    assert_eq!(
        storage
            .list(&ObjectPrefix::files(STORE_ID))
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Protocol
    );
}

#[tokio::test]
async fn a_failed_page_fails_the_listing_rather_than_ending_it() {
    let server = TestServer::new(Router::new().fallback(|uri: Uri| async move {
        if query(&uri).contains_key("continuation-token") {
            return response(503, "<Error><Code>ServiceUnavailable</Code></Error>");
        }
        response(200, format!("<ListBucketResult><Contents><Key>root/{STORE_ID}/seq/1</Key><Size>5</Size><LastModified>2026-10-06T00:00:00.000Z</LastModified><ETag>\"e\"</ETag></Contents><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken></ListBucketResult>"))
    }))
    .await;
    let storage = StorageConnection::from_provider(Arc::new(storage(&server.url)));
    assert_eq!(
        storage
            .list(&ObjectPrefix::all(STORE_ID))
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Network
    );
}
