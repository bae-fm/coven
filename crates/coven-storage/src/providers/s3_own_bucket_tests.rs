use super::*;
use crate::providers::s3::tests::test_server::{
    publish, Remote, Service, RUSTFS, SEAWEEDFS, VERSITY_GW,
};
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
    collections::BTreeMap,
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
        (true, false) => {
            format!("<IsTruncated>true</IsTruncated><NextMarker>{}</NextMarker>", page + 1)
        }
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
    response(200, format!("<ListBucketResult>{next}{entries}</ListBucketResult>"))
}

struct Fixture {
    bucket: Arc<Mutex<Bucket>>,
    server: TestServer,
}
impl Fixture {
    async fn new(pages: Vec<Vec<String>>) -> Self {
        let bucket = Arc::new(Mutex::new(Bucket { pages, ..Bucket::default() }));
        let server =
            TestServer::new(Router::new().fallback(serve).with_state(bucket.clone())).await;
        Self { bucket, server }
    }
    async fn check(&self, profile: Option<S3Profile>) -> Result<(), StorageError> {
        ProviderConnector::new(
            Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
            Arc::new(SequentialIds::new()),
        )
        .check_new_store(
            StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "auto".into(),
                endpoint: Some(self.server.url.parse().unwrap()),
                profile,
            },
            crate::ProviderAuth::S3(S3Credentials {
                access_key_id: "access".into(),
                secret_access_key: SecretText::new("secret".into()),
            }),
            STORE_ID,
            &crate::SetupWaits::default(),
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
fn refused(result: Result<(), StorageError>) -> bool {
    matches!(result, Err(error) if error.failure() == StorageFailure::OwnLocationRequired)
}
const R2: Option<S3Profile> = Some(S3Profile::Named(S3ProfileName::R2));

#[tokio::test]
async fn a_bucket_holding_only_this_store_admits_its_creation() {
    for pages in [
        vec![],
        vec![vec![own()]],
        vec![vec![own()], vec![]],
        vec![vec!["coven-location".into()], vec![own()]],
    ] {
        let fixture = Fixture::new(pages.clone()).await;
        fixture.check(R2).await.unwrap();
        // R2's profile lists with V2, following every page.
        let requests = fixture.requests();
        assert_eq!(requests.len(), pages.len().max(1));
        assert!(requests
            .iter()
            .all(|uri| query(uri).get("list-type").map(String::as_str) == Some("2")));
    }
}

#[tokio::test]
async fn a_bucket_holding_anything_else_refuses_creation_before_any_write() {
    for pages in [
        vec![vec![other()]],
        vec![vec![own(), other()]],
        vec![vec!["notes.txt".into()]],
        vec![vec!["coven-location/".into()]],
        vec![vec!["Coven-bucket".into()]],
        vec![vec![own()], vec![other()]],
    ] {
        let fixture = Fixture::new(pages.clone()).await;
        assert!(refused(fixture.check(R2).await));
        assert_eq!(fixture.requests().len(), pages.len());
    }
}

#[tokio::test]
async fn an_undetected_endpoint_is_checked_with_the_signer_it_accepts() {
    for (pages, occupied) in [(vec![vec![own()]], false), (vec![vec![other()]], true)] {
        let fixture = Fixture::new(pages).await;
        fixture.bucket.lock().unwrap().refuse_google = true;
        let result = fixture.check(None).await;
        if occupied {
            assert!(refused(result));
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
        if headers["authorization"].to_str().unwrap().starts_with("GOOG4") {
            response(403, "<Error><Code>AccessDenied</Code></Error>")
        } else {
            response(404, "<Error><Code>NoSuchBucket</Code></Error>")
        }
    }))
    .await;
    let fixture = Fixture { bucket: Arc::default(), server };
    let error = fixture.check(None).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::ContainerNotFound);
}

/// The services whose keys could once be limited to a folder, each with its
/// catalogued profile: their buckets are a store's own like any other's.
const SERVICES: [(Service, S3ProfileName); 3] = [
    (RUSTFS, S3ProfileName::RustFs),
    (SEAWEEDFS, S3ProfileName::SeaweedFs),
    (VERSITY_GW, S3ProfileName::VersityGw),
];

/// A bucket on the shared in-process server, holding `keys`.
struct Served {
    state: Arc<Mutex<Remote>>,
    server: TestServer,
    profile: S3ProfileName,
}
impl Served {
    async fn new((service, profile): (Service, S3ProfileName), keys: &[String]) -> Self {
        let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
        let mut remote = Remote::new(service, clock);
        for key in keys {
            publish(&mut remote, key.clone(), b"held".to_vec(), 0);
        }
        remote.written.clear();
        let state = Arc::new(Mutex::new(remote));
        let server = TestServer::new(
            Router::new()
                .fallback(crate::providers::s3::tests::test_server::serve)
                .with_state(state.clone()),
        )
        .await;
        Self { state, server, profile }
    }
    fn location(&self) -> StorageConfig {
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some(self.server.url.parse().unwrap()),
            profile: Some(S3Profile::Named(self.profile)),
        }
    }
    /// A new store's check of the bucket, as setup runs it.
    async fn check_new(&self) -> Result<(), StorageError> {
        ProviderConnector::new(
            Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
            Arc::new(SequentialIds::new()),
        )
        .check_new_store(
            self.location(),
            crate::ProviderAuth::S3(credentials()),
            STORE_ID,
            &crate::SetupWaits::default(),
        )
        .await
    }
    /// The store's connection to the bucket.
    fn connection(&self) -> crate::StorageConnection<S3Storage> {
        crate::StorageConnection::from_provider(Arc::new(
            S3Storage::new(
                self.location(),
                credentials(),
                Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH)),
                Arc::new(SequentialIds::new()),
                Arc::new(crate::NoCooldown),
                crate::providers::tests::http_client(),
            )
            .unwrap(),
        ))
    }
    fn objects(&self) -> BTreeMap<String, Vec<u8>> {
        let state = self.state.lock().unwrap();
        state.objects.iter().map(|(key, object)| (key.clone(), object.bytes.clone())).collect()
    }
}

fn credentials() -> S3Credentials {
    S3Credentials {
        access_key_id: "access".into(),
        secret_access_key: SecretText::new("secret".into()),
    }
}

#[tokio::test]
async fn setup_refuses_a_bucket_holding_a_foreign_object() {
    for service in SERVICES {
        for foreign in [
            "notes.txt".to_owned(),
            "music/song.flac".to_owned(),
            ObjectPath::floor(OTHER).as_str().to_owned(),
        ] {
            let served = Served::new(service, std::slice::from_ref(&foreign)).await;
            assert!(refused(served.check_new().await), "{:?} holding {foreign}", served.profile);
            assert!(served.state.lock().unwrap().written.is_empty());
        }
    }
}

#[tokio::test]
async fn setup_accepts_an_empty_bucket_and_claims_it() {
    for service in SERVICES {
        let served = Served::new(service, &[]).await;
        served.check_new().await.unwrap();
        served.connection().claim_location(STORE_ID).await.unwrap();
        assert_eq!(
            served.objects(),
            BTreeMap::from([("coven-location".to_owned(), STORE_ID.to_string().into_bytes())]),
            "{:?}",
            served.profile
        );
    }
}

/// The store's objects, a detection run's probes in its folder and its
/// marker are its own: a reconnect reads the marker alone and passes, and
/// so does a retried creation.
#[tokio::test]
async fn a_librarys_own_objects_dont_block_its_reconnect() {
    let own = [
        "coven-location".to_owned(),
        ObjectPath::floor(STORE_ID).as_str().to_owned(),
        ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN).as_str().to_owned(),
        format!("{STORE_ID}/.coven-check/{}/marker-before", uuid::Uuid::from_bytes([7; 16])),
    ];
    for service in SERVICES {
        let served = Served::new(service, &own).await;
        served.state.lock().unwrap().objects.get_mut("coven-location").unwrap().bytes =
            STORE_ID.to_string().into_bytes();
        let connection = served.connection();
        connection.check_location_claim(STORE_ID).await.unwrap();
        assert_eq!(
            served.state.lock().unwrap().requests,
            [(axum::http::Method::GET, "/bucket/coven-location".to_owned())],
            "{:?}",
            served.profile
        );
        let held = served.objects();
        served.check_new().await.unwrap();
        connection.claim_location(STORE_ID).await.unwrap();
        assert_eq!(served.objects(), held, "{:?}", served.profile);
    }
}
