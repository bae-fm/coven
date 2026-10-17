use crate::providers::s3::{error, profile, S3Storage};
use crate::providers::tests::{response, TestServer};
use crate::test_utils::STORE_ID;
use crate::*;
use async_trait::async_trait;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, Response, Uri},
    Router,
};
use coven_crypto::SecretText;
use coven_foundation::{clock::FixedClock, id_source::SequentialIds};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

/// The service's own clock: it dates every response and judges signatures by it.
const SERVER: u64 = 1_800_000_000;
const SKEWED: &str = "<Error><Code>RequestTimeTooSkewed</Code><Message>The difference between the request time and the current time is too large.</Message></Error>";

#[derive(Default)]
struct Remote {
    /// Refuse every signature, whatever its date.
    refuse_always: bool,
    /// Refuse HEAD with a bodyless 403, as S3 must.
    head: bool,
    /// Each request's signing date and credential.
    seen: Vec<(Method, u64, String)>,
    objects: BTreeMap<String, Vec<u8>>,
    /// Runs once, at the first request.
    first: Option<Box<dyn FnOnce() + Send>>,
}

fn signed_at(headers: &HeaderMap) -> u64 {
    let stamp = headers
        .get("x-amz-date")
        .or_else(|| headers.get("x-goog-date"))
        .unwrap()
        .to_str()
        .unwrap();
    let rfc3339 = format!(
        "{}-{}-{}T{}:{}:{}Z",
        &stamp[0..4],
        &stamp[4..6],
        &stamp[6..8],
        &stamp[9..11],
        &stamp[11..13],
        &stamp[13..15]
    );
    DateTime::from_str(&rfc3339, DateTimeFormat::DateTime)
        .unwrap()
        .secs() as u64
}

async fn endpoint(
    State(state): State<Arc<Mutex<Remote>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let mut remote = state.lock().unwrap();
    let signed = signed_at(&headers);
    let credential = headers["authorization"]
        .to_str()
        .unwrap()
        .split("Credential=")
        .nth(1)
        .and_then(|rest| rest.split('/').next())
        .unwrap_or_default()
        .to_owned();
    remote.seen.push((method.clone(), signed, credential));
    if let Some(first) = remote.first.take() {
        first();
    }
    let key = uri.path().strip_prefix("/bucket/").unwrap_or("").to_owned();
    let mut reply = if remote.refuse_always || signed.abs_diff(SERVER) > 15 * 60 {
        response(403, if method == Method::HEAD { "" } else { SKEWED })
    } else {
        match method {
            Method::PUT => {
                remote.objects.insert(key, body.to_vec());
                response(200, Vec::new())
            }
            Method::HEAD if remote.head => response(403, Vec::new()),
            Method::HEAD => match remote.objects.get(&key) {
                Some(bytes) => {
                    let mut reply = response(200, Vec::new());
                    reply
                        .headers_mut()
                        .insert("content-length", bytes.len().into());
                    reply
                }
                None => response(404, Vec::new()),
            },
            _ => panic!("unexpected request {method} {uri}"),
        }
    };
    let date = DateTime::from_secs(SERVER as i64)
        .fmt(DateTimeFormat::HttpDate)
        .unwrap();
    reply.headers_mut().insert("date", date.parse().unwrap());
    if reply.status().is_success() {
        reply.headers_mut().insert(
            "last-modified",
            "Tue, 06 Oct 2026 00:00:00 GMT".parse().unwrap(),
        );
        reply.headers_mut().insert("etag", "etag".parse().unwrap());
    }
    reply
}

struct Fixture {
    remote: Arc<Mutex<Remote>>,
    storage: StorageConnection<S3Storage>,
    _server: TestServer,
}

/// A device clock a day behind the service's.
async fn fixture(
    signing: S3Signing,
    cooldown: Arc<dyn Cooldown>,
    publications: Arc<dyn PublicationGate>,
) -> Fixture {
    let remote = Arc::new(Mutex::new(Remote::default()));
    let server = TestServer::new(Router::new().fallback(endpoint).with_state(remote.clone())).await;
    let device = SystemTime::UNIX_EPOCH + Duration::from_secs(SERVER - 24 * 60 * 60);
    let storage = S3Storage::with_settings(
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some(server.url.parse().unwrap()),
            profile: None,
            prefix: String::new(),
        },
        S3Credentials {
            access_key_id: "access".into(),
            secret_access_key: SecretText::new("secret".into()),
        },
        Arc::new(FixedClock::new(device)),
        Arc::new(SequentialIds::new()),
        cooldown.clone(),
        S3Settings {
            signing,
            checksums: S3Checksums::Absent,
            request_checksums: false,
            ..profile::PLAIN
        },
    )
    .unwrap();
    Fixture {
        remote,
        storage: StorageConnection::with_gate(Arc::new(storage), publications, cooldown),
        _server: server,
    }
}

fn path() -> ObjectPath {
    numbered(1)
}

fn numbered(number: u64) -> ObjectPath {
    ObjectPath::slot(STORE_ID, std::num::NonZeroU64::new(number).unwrap())
}

fn open() -> Arc<dyn PublicationGate> {
    Arc::new(crate::test_utils::UnrestrictedPublications)
}

#[tokio::test]
async fn a_refused_clock_retries_once_with_the_service_offset_and_keeps_it() {
    for signing in [S3Signing::AwsSigV4, S3Signing::GoogleHmac] {
        let f = fixture(signing, Arc::new(NoCooldown), open()).await;
        f.storage.create(&path(), b"entry").await.unwrap();
        f.storage.create(&numbered(2), b"next").await.unwrap();
        let seen = f.remote.lock().unwrap().seen.clone();
        let dates: Vec<_> = seen.iter().map(|(_, date, _)| *date).collect();
        // One refusal, its corrected retry, then the next request signs correctly at once.
        assert_eq!(
            dates,
            [SERVER - 24 * 60 * 60, SERVER, SERVER],
            "{signing:?}"
        );
        assert_eq!(
            f.remote.lock().unwrap().objects.len(),
            2,
            "{signing:?}: both creates published"
        );
    }
}

#[tokio::test]
async fn a_second_refusal_is_clock_skew_with_the_provider_cause_and_the_same_key() {
    let f = fixture(S3Signing::AwsSigV4, Arc::new(NoCooldown), open()).await;
    f.remote.lock().unwrap().refuse_always = true;
    let error = f.storage.create(&path(), b"entry").await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::ClockSkew);
    assert_eq!(error::native_status(&error), Some(403));
    let StorageError::Provider { source, .. } = &error else {
        panic!("provider cause discarded: {error:?}")
    };
    let sdk = std::error::Error::source(source.as_ref()).unwrap();
    assert!(sdk
        .downcast_ref::<aws_sdk_s3::error::SdkError<
            aws_sdk_s3::operation::put_object::PutObjectError,
            aws_smithy_runtime_api::client::orchestrator::HttpResponse,
        >>()
        .is_some_and(|sdk| sdk.code() == Some("RequestTimeTooSkewed")));
    let seen = f.remote.lock().unwrap().seen.clone();
    assert_eq!(seen.len(), 2, "one retry");
    // The device clock was at fault, not the key: both attempts used the same one.
    assert!(seen.iter().all(|(_, _, key)| key == "access"));
}

#[tokio::test]
async fn heads_bodyless_refusal_is_judged_by_the_services_date() {
    let f = fixture(S3Signing::AwsSigV4, Arc::new(NoCooldown), open()).await;
    f.remote
        .lock()
        .unwrap()
        .objects
        .insert(path().as_str().into(), b"entry".to_vec());
    assert_eq!(f.storage.status(&path()).await.unwrap().unwrap().size, 5);
    assert_eq!(f.remote.lock().unwrap().seen.len(), 2);
    // With the offset adopted, a bodyless 403 is a refusal of the key.
    f.remote.lock().unwrap().head = true;
    assert_eq!(
        f.storage.status(&path()).await.unwrap_err().failure(),
        StorageFailure::PermissionDenied
    );
    assert_eq!(f.remote.lock().unwrap().seen.len(), 3, "no retry");
}

/// A cooldown armed by another consumer while the refused attempt was out.
#[derive(Default)]
struct Armed(std::sync::atomic::AtomicBool);
#[async_trait]
impl Cooldown for Armed {
    fn remaining(&self) -> Option<Duration> {
        self.0
            .load(std::sync::atomic::Ordering::SeqCst)
            .then_some(Duration::from_secs(30))
    }
    async fn record(&self, _: Duration) -> Result<(), StorageError> {
        Ok(())
    }
}

/// Publication authority that lapses while the refused attempt was out.
#[derive(Default)]
struct Lapsing(std::sync::atomic::AtomicBool);
impl PublicationGate for Lapsing {
    fn check(&self, _: &ObjectPath) -> Result<(), StorageError> {
        if self.0.load(std::sync::atomic::Ordering::SeqCst) {
            Err(StorageFailure::CatchUpRequired.into())
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn the_retry_waits_for_the_cooldown_and_rechecks_publication_authority() {
    let cooldown = Arc::new(Armed::default());
    let f = fixture(S3Signing::AwsSigV4, cooldown.clone(), open()).await;
    let armed = cooldown.clone();
    f.remote.lock().unwrap().first = Some(Box::new(move || {
        armed.0.store(true, std::sync::atomic::Ordering::SeqCst)
    }));
    let error = f.storage.create(&path(), b"entry").await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::RateLimited);
    assert!(std::iter::successors(
        Some(&error as &(dyn std::error::Error + 'static)),
        |cause| cause.source()
    )
    .any(|cause| cause.is::<CoolingDown>()));
    assert_eq!(f.remote.lock().unwrap().seen.len(), 1);
    assert!(
        !error.answered(),
        "the error reports the withheld retry, which no response answered"
    );

    let gate = Arc::new(Lapsing::default());
    let f = fixture(S3Signing::AwsSigV4, Arc::new(NoCooldown), gate.clone()).await;
    let lapse = gate.clone();
    f.remote.lock().unwrap().first = Some(Box::new(move || {
        lapse.0.store(true, std::sync::atomic::Ordering::SeqCst)
    }));
    assert_eq!(
        f.storage
            .create(&path(), b"entry")
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::CatchUpRequired
    );
    assert_eq!(f.remote.lock().unwrap().seen.len(), 1);
}
