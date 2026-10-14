use super::*;
use crate::providers::tests::{query, response, TestServer};
use axum::{
    http::{HeaderMap, Method, Uri},
    Router,
};
use std::{collections::BTreeSet, sync::Mutex};

#[tokio::test]
async fn cleanup_removes_every_version_and_marker_across_pages_in_both_xml_dialects() {
    for google in [false, true] {
        let deleted = Arc::new(Mutex::new(BTreeSet::new()));
        let requests = deleted.clone();
        let server = TestServer::new(Router::new().fallback(move |method: Method, uri: Uri, headers: HeaderMap| {
            let deleted = requests.clone();
            async move {
                let q = query(&uri);
                if method == Method::DELETE {
                    assert_eq!(uri.path(), "/bucket/run/object");
                    deleted.lock().unwrap().insert(q["versionId"].clone());
                    return response(204, "");
                }
                assert!(q.contains_key("versions"));
                assert_eq!(q["prefix"], "run/");
                assert_eq!(headers.get("x-goog-interop-list-objects-format").map(|v| v.to_str().unwrap()), google.then_some("enabled"));
                let body = if q.contains_key("max-keys") {
                    assert_eq!(*deleted.lock().unwrap(), BTreeSet::from(["v1".to_owned(), "v2".to_owned()]));
                    "<IsTruncated>false</IsTruncated>"
                } else if q.contains_key("key-marker") {
                    assert_eq!(q["key-marker"], "run/object");
                    assert_eq!(q["version-id-marker"], "v1");
                    "<IsTruncated>false</IsTruncated><DeleteMarker><Key>run/object</Key><VersionId>v2</VersionId></DeleteMarker>"
                } else {
                    "<IsTruncated>true</IsTruncated><NextKeyMarker>run/object</NextKeyMarker><NextVersionIdMarker>v1</NextVersionIdMarker><Version><Key>run/object</Key><VersionId>v1</VersionId></Version>"
                };
                response(200, format!("<ListVersionsResult>{body}</ListVersionsResult>"))
            }
        })).await;
        let settings = if google {
            profile::CataloguedProfile::named(S3ProfileName::Gcs).settings
        } else {
            profile::PLAIN
        };
        let storage = profile_provider(&server.url, settings);
        cleanup_versions(&storage.provider, "run/").await.unwrap();
    }
}

#[tokio::test]
async fn cleanup_refuses_to_delete_a_version_outside_its_namespace() {
    let server = TestServer::new(Router::new().fallback(|method: Method| async move {
        assert_eq!(method, Method::GET);
        response(200, "<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>another-run/object</Key><VersionId>v1</VersionId></Version></ListVersionsResult>")
    })).await;
    let storage = profile_provider(&server.url, profile::PLAIN);
    assert_eq!(
        cleanup_versions(&storage.provider, "run/")
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Protocol
    );
}
