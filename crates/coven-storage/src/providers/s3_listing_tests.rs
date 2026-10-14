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
        )
        .unwrap();
        assert_eq!(
            storage.list_writers(STORE_ID).await.unwrap(),
            [DeviceId(31), DeviceId(32)]
        );
    }
}
