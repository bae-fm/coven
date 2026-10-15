use super::*;
use crate::providers::tests::{query, response, TestServer};
use crate::test_utils::STORE_ID;
use axum::{http::Uri, Router};
use coven_foundation::{
    clock::FixedClock,
    id_source::{DeviceId, SequentialIds},
};
use std::sync::Arc;

#[tokio::test]
async fn writer_discovery_uses_delimiters_and_each_native_page() {
    for protocol in [S3Listing::V1, S3Listing::V2] {
        let server = TestServer::new(Router::new().fallback(move |uri: Uri| async move {
            let q = query(&uri);
            assert_eq!(q["prefix"], format!("root/{STORE_ID}/store-log/"));
            assert_eq!(q["delimiter"], "/");
            let marker = if protocol == S3Listing::V1 { "marker" } else { "continuation-token" };
            let next = if protocol == S3Listing::V1 { "NextMarker" } else { "NextContinuationToken" };
            let (writer, tail) = if q.contains_key(marker) {
                assert_eq!(q[marker], "next");
                (32, "<IsTruncated>false</IsTruncated>".to_owned())
            } else { (31, format!("<IsTruncated>true</IsTruncated><{next}>next</{next}>")) };
            response(200, format!("<ListBucketResult><CommonPrefixes><Prefix>root/{STORE_ID}/store-log/{writer}/</Prefix></CommonPrefixes>{tail}</ListBucketResult>"))
        })).await;
        let mut settings = profile::PLAIN;
        settings.listing = protocol;
        let storage = S3Storage::new(
            StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "us-east-1".into(),
                endpoint: Some(server.url.parse().unwrap()),
                profile: Some(crate::S3Profile::Custom(settings)),
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
        .unwrap();
        assert_eq!(
            storage.list_writers(STORE_ID).await.unwrap(),
            [DeviceId(31), DeviceId(32)]
        );
    }
}

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
        let body = if q.contains_key("delimiter") {
            [
                format!("<CommonPrefixes><Prefix>root/{STORE_ID}/store-log/32/</Prefix></CommonPrefixes>"),
                format!("<CommonPrefixes><Prefix>root/{STORE_ID}/store-log/New folder/</Prefix></CommonPrefixes>"),
                object(&format!("root/{STORE_ID}/store-log/")),
                object(&format!("root/{STORE_ID}/store-log/readme")),
            ]
            .concat()
        } else if q["prefix"] == format!("root/{STORE_ID}/devices/") {
            object(&format!("root/{STORE_ID}/store-log/32/1"))
        } else {
            [
                "devices/31/1",
                "devices/31/01",
                "devices/31/1/extra",
                "Notes.txt",
                "photos/1",
                ".coven-uploads/not-a-uuid",
                ".coven-check/run/marker",
                "store-log/32/1",
            ]
            .map(|stray| object(&format!("root/{STORE_ID}/{stray}")))
            .concat()
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
        [
            format!("{STORE_ID}/devices/31/1"),
            format!("{STORE_ID}/store-log/32/1")
        ]
    );
    assert_eq!(
        storage.list_writers(STORE_ID).await.unwrap(),
        [DeviceId(32)]
    );
    // A key outside the requested prefix is the service's error, not a stray.
    assert_eq!(
        storage
            .list(&ObjectPrefix::device_logs(STORE_ID))
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
        response(200, format!("<ListBucketResult><Contents><Key>root/{STORE_ID}/devices/31/1</Key><Size>5</Size><LastModified>2026-10-06T00:00:00.000Z</LastModified><ETag>\"e\"</ETag></Contents><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken></ListBucketResult>"))
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
