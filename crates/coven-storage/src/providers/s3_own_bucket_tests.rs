use super::*;
use crate::providers::tests::{query, response, TestServer};
use crate::providers::{ProviderConnector, StorageConnector};
use crate::test_utils::STORE_ID;
use crate::{S3Profile, S3ProfileName};
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, Response, Uri},
    Router,
};
use coven_crypto::SecretText;
use coven_foundation::{clock::FixedClock, id_source::SequentialIds};
use std::{
    sync::{Arc, Mutex},
    time::SystemTime,
};

const OTHER: StoreId = StoreId(uuid::Uuid::from_bytes([0x20; 16]));

#[derive(Default)]
struct Bucket {
    /// The bucket root's listing, one page each: a folder ends with `/`.
    pages: Vec<Vec<String>>,
    /// Refuse Google HMAC, as a service speaking only SigV4 does.
    refuse_google: bool,
    requests: Vec<Uri>,
}

async fn serve(
    State(bucket): State<Arc<Mutex<Bucket>>>,
    uri: Uri,
    headers: HeaderMap,
) -> Response<Body> {
    let mut bucket = bucket.lock().unwrap();
    bucket.requests.push(uri.clone());
    let authorization = headers["authorization"].to_str().unwrap();
    if bucket.refuse_google && authorization.starts_with("GOOG4") {
        return response(403, "<Error><Code>AccessDenied</Code></Error>");
    }
    assert_eq!(uri.path(), "/bucket/");
    let q = query(&uri);
    assert_eq!(q.get("prefix").map(String::as_str).unwrap_or(""), "");
    assert_eq!(q["delimiter"], "/");
    let v2 = q.contains_key("list-type");
    let page = q
        .get(if v2 { "continuation-token" } else { "marker" })
        .map(|marker| marker.parse::<usize>().unwrap())
        .unwrap_or(0);
    let next = match (page + 1 < bucket.pages.len(), v2) {
        (false, _) => "<IsTruncated>false</IsTruncated>".to_owned(),
        (true, true) => format!(
            "<IsTruncated>true</IsTruncated><NextContinuationToken>{}</NextContinuationToken>",
            page + 1
        ),
        (true, false) => format!(
            "<IsTruncated>true</IsTruncated><NextMarker>{}</NextMarker>",
            page + 1
        ),
    };
    let entries: String = bucket
        .pages
        .get(page)
        .into_iter()
        .flatten()
        .map(|entry| {
            if entry.ends_with('/') {
                format!("<CommonPrefixes><Prefix>{entry}</Prefix></CommonPrefixes>")
            } else {
                format!("<Contents><Key>{entry}</Key><Size>1</Size><ETag>etag</ETag><LastModified>2026-10-06T00:00:00Z</LastModified></Contents>")
            }
        })
        .collect();
    response(
        200,
        format!("<ListBucketResult>{next}{entries}</ListBucketResult>"),
    )
}

struct Fixture {
    bucket: Arc<Mutex<Bucket>>,
    server: TestServer,
}
impl Fixture {
    async fn new(pages: Vec<Vec<String>>) -> Self {
        let bucket = Arc::new(Mutex::new(Bucket {
            pages,
            ..Bucket::default()
        }));
        let server =
            TestServer::new(Router::new().fallback(serve).with_state(bucket.clone())).await;
        Self { bucket, server }
    }
    async fn check(&self, profile: Option<S3Profile>, prefix: &str) -> Result<(), StorageError> {
        ProviderConnector::new(
            Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
            Arc::new(SequentialIds::new()),
        )
        .check_new_s3_store(
            StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "auto".into(),
                endpoint: Some(self.server.url.parse().unwrap()),
                profile,
                prefix: prefix.into(),
            },
            S3Credentials {
                access_key_id: "access".into(),
                secret_access_key: SecretText::new("secret".into()),
            },
            STORE_ID,
        )
        .await
    }
    fn requests(&self) -> Vec<Uri> {
        self.bucket.lock().unwrap().requests.clone()
    }
}

fn own() -> String {
    ObjectPrefix::all(STORE_ID).as_str().to_owned()
}
fn other() -> String {
    ObjectPrefix::all(OTHER).as_str().to_owned()
}
fn refused(result: Result<(), StorageError>, reason: OwnBucketReason) -> bool {
    matches!(result, Err(error) if error.failure() == StorageFailure::OwnBucketRequired { reason })
}
const R2: Option<S3Profile> = Some(S3Profile::Named(S3ProfileName::R2));

#[tokio::test]
async fn an_r2_bucket_holding_only_this_store_admits_its_creation() {
    for pages in [
        vec![],
        vec![vec![own()]],
        vec![vec![own()], vec![]],
        vec![vec!["coven-bucket".into()], vec![own()]],
    ] {
        let fixture = Fixture::new(pages.clone()).await;
        fixture.check(R2, "").await.unwrap();
        // R2's profile lists with V2, following every page.
        let requests = fixture.requests();
        assert_eq!(requests.len(), pages.len().max(1));
        assert!(requests
            .iter()
            .all(|uri| query(uri).get("list-type").map(String::as_str) == Some("2")));
    }
}

#[tokio::test]
async fn an_r2_bucket_holding_anything_else_refuses_creation_before_any_write() {
    for pages in [
        vec![vec![other()]],
        vec![vec![own(), other()]],
        vec![vec!["notes.txt".into()]],
        vec![vec!["coven-bucket/".into()]],
        vec![vec!["Coven-bucket".into()]],
        vec![vec![own()], vec![other()]],
    ] {
        let fixture = Fixture::new(pages.clone()).await;
        assert!(refused(
            fixture.check(R2, "").await,
            OwnBucketReason::Occupied
        ));
        assert_eq!(fixture.requests().len(), pages.len());
    }
}

#[tokio::test]
async fn a_base_prefix_is_refused_before_any_request_where_keys_reach_the_whole_bucket() {
    let fixture = Fixture::new(vec![vec![own()]]).await;
    for profile in [R2, Some(S3Profile::Custom(profile::PLAIN)), None] {
        assert!(refused(
            fixture.check(profile, "store").await,
            OwnBucketReason::Prefix
        ));
    }
    assert!(fixture.requests().is_empty());
}

#[tokio::test]
async fn prefix_scoped_profiles_share_a_bucket_without_a_check() {
    let fixture = Fixture::new(vec![vec![other(), "notes.txt".into()]]).await;
    for name in [
        S3ProfileName::Aws,
        S3ProfileName::Gcs,
        S3ProfileName::RustFs,
        S3ProfileName::SeaweedFs,
    ] {
        for prefix in ["", "store"] {
            fixture
                .check(Some(S3Profile::Named(name)), prefix)
                .await
                .unwrap();
        }
    }
    assert!(fixture.requests().is_empty());
}

#[tokio::test]
async fn an_undetected_endpoint_is_checked_with_the_signer_it_accepts() {
    for (pages, occupied) in [(vec![vec![own()]], false), (vec![vec![other()]], true)] {
        let fixture = Fixture::new(pages).await;
        fixture.bucket.lock().unwrap().refuse_google = true;
        let result = fixture.check(None, "").await;
        if occupied {
            assert!(refused(result, OwnBucketReason::Occupied));
        } else {
            result.unwrap();
        }
        // Google HMAC is refused, then SigV4 lists with V1, as detection's
        // read-only signing probe does.
        let requests = fixture.requests();
        assert_eq!(requests.len(), 2);
        assert!(!query(&requests[1]).contains_key("list-type"));
    }
}

#[tokio::test]
async fn a_bucket_refusing_every_signer_reports_the_plain_signers_failure() {
    // Google HMAC is denied; SigV4 finds no bucket.
    let server = TestServer::new(Router::new().fallback(|headers: HeaderMap| async move {
        if headers["authorization"]
            .to_str()
            .unwrap()
            .starts_with("GOOG4")
        {
            response(403, "<Error><Code>AccessDenied</Code></Error>")
        } else {
            response(404, "<Error><Code>NoSuchBucket</Code></Error>")
        }
    }))
    .await;
    let fixture = Fixture {
        bucket: Arc::default(),
        server,
    };
    let error = fixture.check(None, "").await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::ContainerNotFound);
}
