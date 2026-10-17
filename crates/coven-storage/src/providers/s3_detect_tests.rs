//! Detection against an in-process S3 server configured as each known
//! behavior. Times are real server seconds, so each run waits about a second.
use super::*;
use crate::providers::tests::{query, response, TestServer};
use crate::{S3Profile, S3ProfileName, StorageCheck, StorageSetupError};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, Response, Uri},
    Router,
};
use coven_foundation::{clock::SystemClock, id_source::UuidIds};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Checksum {
    None,
    Scoped,
    Unscoped,
    GoogleHash,
}

/// One S3 service's native behavior; conditions it does not enforce are ignored.
#[derive(Clone, Copy, Debug)]
struct Service {
    signers: &'static [S3Signing],
    single_put: Option<S3SinglePut>,
    completion_enforced: bool,
    start_timestamp: bool,
    partial_visible: bool,
    copy: Option<S3CopyCondition>,
    checksum: Checksum,
    generation: bool,
    v2: bool,
}
const AWS: Service = Service {
    signers: &[S3Signing::AwsSigV4],
    single_put: Some(S3SinglePut::IfNoneMatch),
    completion_enforced: true,
    start_timestamp: true,
    partial_visible: false,
    copy: Some(S3CopyCondition::IfNoneMatch),
    checksum: Checksum::Scoped,
    generation: false,
    v2: true,
};
const GCS: Service = Service {
    signers: &[S3Signing::GoogleHmac, S3Signing::AwsSigV4],
    single_put: Some(S3SinglePut::GenerationMatch),
    completion_enforced: false,
    start_timestamp: false,
    copy: Some(S3CopyCondition::GenerationMatch),
    checksum: Checksum::GoogleHash,
    generation: true,
    ..AWS
};
const R2: Service = Service {
    start_timestamp: false,
    copy: Some(S3CopyCondition::CloudflareIfNoneMatch),
    checksum: Checksum::Unscoped,
    ..AWS
};
const RUSTFS: Service = Service {
    start_timestamp: false,
    copy: None,
    checksum: Checksum::None,
    ..AWS
};
/// Enforces conditional completion one request at a time, and the copy
/// condition. Its racing completions both succeeding is invisible to these
/// sequential probes.
const SEAWEEDFS: Service = Service {
    copy: Some(S3CopyCondition::IfNoneMatch),
    ..RUSTFS
};
/// Enforces conditional completion and the copy condition, dates multipart
/// objects by completion and states checksum scope: the plain S3 settings.
const VERSITY_GW: Service = Service {
    start_timestamp: false,
    ..AWS
};
/// Accepts writes to an occupied name whatever condition is sent, and dates
/// multipart objects by initiation.
const OVERWRITES: Service = Service {
    single_put: None,
    completion_enforced: false,
    copy: None,
    checksum: Checksum::None,
    ..AWS
};

struct Object {
    bytes: Vec<u8>,
    modified: u64,
    etag: u64,
}
struct Upload {
    key: String,
    started: u64,
    parts: BTreeMap<u32, Vec<u8>>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LostReply {
    Put,
    Copy,
    Initiate,
    Complete,
}

struct Remote {
    lost_reply: Option<LostReply>,
    hidden: BTreeSet<String>,
    versioning: &'static str,
    versioning_failure: Option<u16>,
    service: Service,
    objects: BTreeMap<String, Object>,
    uploads: BTreeMap<String, Upload>,
    /// Every key ever written, to prove detection stays in its namespace.
    written: Vec<String>,
    next: u64,
    fail_copies: bool,
    /// The one second every publication is dated, when the server's clock
    /// stands still.
    frozen: Option<u64>,
    /// The status a V2 listing with `max-keys=0` answers instead of a page.
    empty_v2_listing: Option<u16>,
    incomplete_v1_listing: bool,
    bucket_heads: usize,
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
fn date(secs: u64, format: aws_sdk_s3::primitives::DateTimeFormat) -> String {
    aws_sdk_s3::primitives::DateTime::from_secs(secs as i64)
        .fmt(format)
        .unwrap()
}
fn error(status: u16, code: &str) -> Response<Body> {
    response(status, format!("<Error><Code>{code}</Code></Error>"))
}
/// GoogleHmac moves x-amz-* request headers to x-goog-*.
fn header<'a>(headers: &'a HeaderMap, suffix: &str) -> Option<&'a str> {
    headers
        .get(format!("x-amz-{suffix}"))
        .or(headers.get(format!("x-goog-{suffix}")))
        .map(|value| value.to_str().unwrap())
}
fn sent(headers: &HeaderMap, (name, value): (&str, &str)) -> bool {
    headers.get(name).is_some_and(|sent| sent == value)
}

async fn serve(
    State(state): State<Arc<Mutex<Remote>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let mut state = state.lock().unwrap();
    let service = state.service;
    let google = headers["authorization"]
        .to_str()
        .unwrap()
        .starts_with("GOOG4-HMAC-SHA256 ");
    let signer = if google {
        S3Signing::GoogleHmac
    } else {
        S3Signing::AwsSigV4
    };
    if !service.signers.contains(&signer) {
        return error(400, "InvalidArgument");
    }
    let q = query(&uri);
    let key = uri
        .path()
        .strip_prefix("/bucket")
        .unwrap()
        .trim_start_matches('/')
        .to_owned();
    if key.is_empty() {
        if q.contains_key("uploads") {
            let prefix = q.get("prefix").expect("scoped multipart listing");
            let contents: String = state
                .uploads
                .iter()
                .filter(|(_, upload)| upload.key.starts_with(prefix))
                .map(|(id, upload)| {
                    format!(
                        "<Upload><Key>{}</Key><UploadId>{id}</UploadId></Upload>",
                        upload.key
                    )
                })
                .collect();
            return response(200, format!("<ListMultipartUploadsResult><IsTruncated>false</IsTruncated>{contents}</ListMultipartUploadsResult>"));
        }
        if q.contains_key("versioning") {
            if let Some(status) = state.versioning_failure {
                return error(
                    status,
                    match status {
                        403 => "AccessDenied",
                        501 => "NotImplemented",
                        _ => "ServiceUnavailable",
                    },
                );
            }
            return response(
                200,
                format!(
                    "<VersioningConfiguration>{}</VersioningConfiguration>",
                    state.versioning
                ),
            );
        }
        return match method {
            Method::HEAD => {
                state.bucket_heads += 1;
                response(403, Vec::new())
            }
            _ => list(&state, &q),
        };
    }
    if q.contains_key("uploads") {
        state.next += 1;
        let id = state.next.to_string();
        let upload = Upload {
            key,
            started: now(),
            parts: BTreeMap::new(),
        };
        state.uploads.insert(id.clone(), upload);
        if state.lost_reply == Some(LostReply::Initiate) {
            return error(503, "ServiceUnavailable");
        }
        return response(
            200,
            format!("<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"),
        );
    }
    if let Some(id) = q.get("uploadId") {
        if !state.uploads.contains_key(id) {
            return error(404, "NoSuchUpload");
        }
        return match method {
            Method::PUT => {
                let number = q["partNumber"].parse().unwrap();
                let upload = state.uploads.get_mut(id).unwrap();
                upload.parts.insert(number, body.to_vec());
                Response::builder()
                    .header("etag", format!("\"part-{number}\""))
                    .body(Body::empty())
                    .unwrap()
            }
            Method::POST => {
                if service.completion_enforced
                    && headers.contains_key("if-none-match")
                    && state.objects.contains_key(&key)
                {
                    return error(412, "PreconditionFailed");
                }
                let upload = state.uploads.remove(id).unwrap();
                let modified = if service.start_timestamp {
                    upload.started
                } else {
                    now()
                };
                let bytes = upload.parts.into_values().flatten().collect();
                publish(&mut state, key.clone(), bytes, modified);
                if state.lost_reply == Some(LostReply::Complete) {
                    state.hidden.insert(key);
                    return error(503, "ServiceUnavailable");
                }
                response(
                    200,
                    "<CompleteMultipartUploadResult><ETag>\"complete\"</ETag></CompleteMultipartUploadResult>",
                )
            }
            _ => {
                state.uploads.remove(id);
                response(204, Vec::new())
            }
        };
    }
    if let Some(source) = header(&headers, "copy-source") {
        if state.fail_copies {
            return error(500, "InternalError");
        }
        let enforced = service
            .copy
            .is_some_and(|condition| sent(&headers, condition.header()));
        if enforced && state.objects.contains_key(&key) {
            return error(412, "PreconditionFailed");
        }
        let source = source.strip_prefix("/bucket/").unwrap();
        let bytes = state.objects[source].bytes.clone();
        publish(&mut state, key.clone(), bytes, now());
        if state.lost_reply == Some(LostReply::Copy) {
            state.hidden.insert(key);
            return error(503, "ServiceUnavailable");
        }
        return response(
            200,
            "<CopyObjectResult><ETag>\"copied\"</ETag></CopyObjectResult>",
        );
    }
    match method {
        Method::PUT => {
            let enforced = service
                .single_put
                .is_some_and(|condition| sent(&headers, condition.header()));
            if enforced && state.objects.contains_key(&key) {
                return error(412, "PreconditionFailed");
            }
            publish(&mut state, key.clone(), body.to_vec(), now());
            if state.lost_reply == Some(LostReply::Put) {
                state.hidden.insert(key);
                return error(503, "ServiceUnavailable");
            }
            response(200, Vec::new())
        }
        Method::DELETE => {
            state.objects.remove(&key);
            response(204, Vec::new())
        }
        _ => object(&state, &key, method == Method::HEAD, &headers),
    }
}

fn publish(state: &mut Remote, key: String, bytes: Vec<u8>, modified: u64) {
    let modified = state.frozen.unwrap_or(modified);
    state.next += 1;
    let etag = state.next;
    state.written.push(key.clone());
    state.objects.insert(
        key,
        Object {
            bytes,
            modified,
            etag,
        },
    );
}

fn object(state: &Remote, key: &str, head: bool, headers: &HeaderMap) -> Response<Body> {
    let pending = state.uploads.values().find(|upload| upload.key == key);
    let (bytes, modified, etag) = match (state.objects.get(key), pending) {
        (Some(object), _) => (object.bytes.clone(), object.modified, object.etag),
        (None, Some(upload)) if state.service.partial_visible => (
            upload.parts.values().flatten().copied().collect(),
            upload.started,
            0,
        ),
        _ if head => return response(404, Vec::new()),
        _ => return error(404, "NoSuchKey"),
    };
    let mut reply = Response::builder()
        .header("content-length", bytes.len())
        .header(
            "last-modified",
            date(modified, aws_sdk_s3::primitives::DateTimeFormat::HttpDate),
        )
        .header("etag", format!("\"{etag}\""));
    if state.service.generation {
        reply = reply.header("x-goog-generation", etag);
    }
    // Checksums only on HEAD: no body for the SDK to validate.
    let requested = header(headers, "checksum-mode") == Some("ENABLED");
    if head {
        reply = match state.service.checksum {
            Checksum::Scoped if requested => reply
                .header("x-amz-checksum-crc32", "AAAAAA==")
                .header("x-amz-checksum-type", "FULL_OBJECT"),
            Checksum::Unscoped if requested => reply.header("x-amz-checksum-crc32", "AAAAAA=="),
            Checksum::GoogleHash => reply.header("x-goog-hash", "crc32c=AAAAAA=="),
            _ => reply,
        };
    }
    let body = if head {
        Body::empty()
    } else {
        Body::from(bytes)
    };
    reply.body(body).unwrap()
}

fn list(state: &Remote, q: &BTreeMap<String, String>) -> Response<Body> {
    let v2 = q.get("list-type").is_some_and(|value| value == "2");
    if v2 && !state.service.v2 {
        return error(501, "NotImplemented");
    }
    if let Some(status) = state.empty_v2_listing {
        if v2 && q.get("max-keys").is_some_and(|value| value == "0") {
            let code = if status == 501 {
                "NotImplemented"
            } else {
                "ServiceUnavailable"
            };
            return error(status, code);
        }
    }
    let prefix = q.get("prefix").map(String::as_str).unwrap_or("");
    if !prefix.starts_with(&format!("{STORE}/")) {
        return error(403, "AccessDenied");
    }
    let after = q.get(if v2 { "continuation-token" } else { "marker" });
    let limit: usize = q
        .get("max-keys")
        .map(|value| value.parse().unwrap())
        .unwrap_or(1000);
    let entries: Vec<_> = state
        .objects
        .iter()
        .filter(|_| v2 || !state.incomplete_v1_listing)
        .filter(|(key, _)| {
            !state.hidden.contains(*key)
                && key.starts_with(prefix)
                && after.is_none_or(|after| *key > after)
        })
        .collect();
    let truncated = limit > 0 && entries.len() > limit;
    let page = &entries[..limit.min(entries.len())];
    let contents: String = page
        .iter()
        .map(|(key, object)| {
            format!(
                "<Contents><Key>{key}</Key><Size>{}</Size><LastModified>{}</LastModified><ETag>\"{}\"</ETag></Contents>",
                object.bytes.len(),
                date(object.modified, aws_sdk_s3::primitives::DateTimeFormat::DateTime),
                object.etag
            )
        })
        .collect();
    let next = match (truncated && v2, page.last()) {
        (true, Some((key, _))) => format!("<NextContinuationToken>{key}</NextContinuationToken>"),
        _ => String::new(),
    };
    response(
        200,
        format!("<ListBucketResult><IsTruncated>{truncated}</IsTruncated>{next}{contents}</ListBucketResult>"),
    )
}

const STORE: StoreId = StoreId(uuid::Uuid::from_bytes([0x44; 16]));

fn credentials() -> S3Credentials {
    S3Credentials {
        access_key_id: "access".into(),
        secret_access_key: SecretText::new("secret".into()),
    }
}

struct Fixture {
    state: Arc<Mutex<Remote>>,
    server: TestServer,
}
impl Fixture {
    async fn new(service: Service) -> Self {
        let state = Arc::new(Mutex::new(Remote {
            lost_reply: None,
            hidden: BTreeSet::new(),
            versioning: "",
            versioning_failure: None,
            service,
            objects: BTreeMap::new(),
            uploads: BTreeMap::new(),
            written: Vec::new(),
            next: 0,
            fail_copies: false,
            frozen: None,
            empty_v2_listing: None,
            incomplete_v1_listing: false,
            bucket_heads: 0,
        }));
        let server = TestServer::new(Router::new().fallback(serve).with_state(state.clone())).await;
        Self { state, server }
    }
    fn location(&self, profile: Option<S3Profile>) -> StorageConfig {
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some(self.server.url.parse().unwrap()),
            profile,
            prefix: String::new(),
        }
    }
    async fn detect(&self, profile: Option<S3Profile>) -> Result<S3Detection, StorageError> {
        self.detect_with(profile, Arc::new(SystemClock)).await
    }
    async fn detect_with(
        &self,
        profile: Option<S3Profile>,
        clock: ClockRef,
    ) -> Result<S3Detection, StorageError> {
        detect(
            self.location(profile),
            credentials(),
            STORE,
            clock,
            Arc::new(UuidIds),
        )
        .await
    }
    /// Detection left nothing behind and wrote only under one run's prefix.
    fn assert_clean(&self) {
        let state = self.state.lock().unwrap();
        assert!(state.objects.is_empty(), "probe objects remain");
        assert!(state.uploads.is_empty(), "probe uploads remain");
        assert_eq!(
            state.bucket_heads, 0,
            "detection uses only prefix-scoped access"
        );
        let namespace = format!("{STORE}/.coven-check/");
        let runs: std::collections::BTreeSet<_> = state
            .written
            .iter()
            .map(|key| {
                let run = key
                    .strip_prefix(&namespace)
                    .expect("probe outside namespace");
                run.split_once('/').unwrap().0.to_owned()
            })
            .collect();
        assert_eq!(runs.len(), 1);
    }
}

#[tokio::test]
async fn versioned_uncertified_service_is_refused_even_when_sequential_checks_pass() {
    let fixture = Fixture::new(RUSTFS).await;
    fixture.state.lock().unwrap().versioning = "<Status>Enabled</Status>";
    let detection = fixture.detect(None).await.unwrap();
    detection.check(&detection.settings).unwrap();
    for profile in [
        None,
        Some(S3Profile::Named(S3ProfileName::CephRgw)),
        Some(S3Profile::Custom(detection.settings)),
    ] {
        assert!(matches!(
            detection.resolve(fixture.location(profile)),
            Err(StorageSetupError::S3VersioningUnsupported {
                state: S3BucketVersioning::Enabled
            })
        ));
    }
    fixture.assert_clean();
}

#[tokio::test]
async fn unknown_versioning_refuses_setup_and_outages_stop_detection_before_writes() {
    for status in [403, 501, 503] {
        let fixture = Fixture::new(RUSTFS).await;
        fixture.state.lock().unwrap().versioning_failure = Some(status);
        let result = fixture.detect(None).await;
        if status == 503 {
            assert_eq!(result.unwrap_err().failure(), StorageFailure::Network);
            assert!(fixture.state.lock().unwrap().written.is_empty());
        } else {
            let detection = result.unwrap();
            assert!(detection.versioning.is_err());
            assert!(matches!(
                detection.resolve(fixture.location(None)),
                Err(StorageSetupError::S3VersioningUnknown { .. })
            ));
            fixture.assert_clean();
        }
    }
}

fn refused_check(result: Result<StorageConfig, StorageSetupError>) -> StorageCheck {
    match result {
        Err(StorageSetupError::ProviderCheck { check, source }) => {
            assert_eq!(source.failure(), StorageFailure::Protocol);
            check
        }
        other => panic!("expected a refused check, got {other:?}"),
    }
}

#[tokio::test]
async fn detection_reproduces_each_catalogued_entry() {
    for (service, name) in [
        (AWS, S3ProfileName::Aws),
        (GCS, S3ProfileName::Gcs),
        (R2, S3ProfileName::R2),
        (RUSTFS, S3ProfileName::RustFs),
        (SEAWEEDFS, S3ProfileName::SeaweedFs),
        (RUSTFS, S3ProfileName::CephRgw),
        (VERSITY_GW, S3ProfileName::VersityGw),
    ] {
        let fixture = Fixture::new(service).await;
        let detection = fixture
            .detect(None)
            .await
            .unwrap_or_else(|error| panic!("{name:?}: {error:?}"));
        let entry = profile::CataloguedProfile::named(name).settings;
        // Detection prefers enforced conditional completion; only the
        // certification races showed SeaweedFS's racing completions, and a
        // racing PUT and copy, both succeed, so its entry publishes every
        // object by copy instead (§20.6).
        let detected = match name {
            S3ProfileName::SeaweedFs => S3Settings {
                copy_publication: S3CopyPublication::Never,
                ..entry
            },
            _ => entry,
        };
        assert_eq!(detection.settings, detected, "{name:?}");
        assert_eq!(
            detection.resolve(fixture.location(None)).unwrap(),
            fixture.location(Some(S3Profile::Custom(detected)))
        );
        // The named profile passes the same safety check at its own server.
        let named = fixture.location(Some(S3Profile::Named(name)));
        assert_eq!(detection.resolve(named.clone()).unwrap(), named);
        fixture.assert_clean();
    }
}

#[tokio::test]
async fn a_service_that_overwrites_occupied_names_is_refused_with_create_once() {
    let fixture = Fixture::new(OVERWRITES).await;
    let detection = fixture.detect(None).await.unwrap();
    for probe in SINGLE_PUTS
        .map(S3Probe::SinglePut)
        .into_iter()
        .chain(COPY_CONDITIONS.map(S3Probe::CopyPublication))
        .chain([S3Probe::ConditionalCompletion, S3Probe::CompletionTimestamp])
    {
        assert!(!detection.held(probe), "{probe:?}");
    }
    assert_eq!(
        refused_check(detection.resolve(fixture.location(None))),
        StorageCheck::CreateOnce
    );
    // No catalogued or custom setting makes it safe either.
    for profile in [
        S3Profile::Named(S3ProfileName::RustFs),
        S3Profile::Named(S3ProfileName::Aws),
        S3Profile::Custom(S3Settings {
            single_put: S3SinglePut::GenerationMatch,
            ..detection.settings
        }),
    ] {
        assert_eq!(
            refused_check(detection.resolve(fixture.location(Some(profile)))),
            StorageCheck::CreateOnce
        );
    }
    fixture.assert_clean();
}

#[tokio::test]
async fn overrides_contradicting_observations_are_refused_by_the_check_they_break() {
    let fixture = Fixture::new(Service {
        v2: false,
        ..RUSTFS
    })
    .await;
    let detection = fixture.detect(None).await.unwrap();
    assert_eq!(detection.settings.listing, S3Listing::V1);
    let detected = detection.settings;
    for (settings, check) in [
        (
            S3Settings {
                copy_publication: S3CopyPublication::Multipart(S3CopyCondition::GenerationMatch),
                ..detected
            },
            StorageCheck::MultipartCreateOnce,
        ),
        (
            S3Settings {
                copy_publication: S3CopyPublication::Every(S3CopyCondition::GenerationMatch),
                ..detected
            },
            StorageCheck::MultipartCreateOnce,
        ),
        (
            S3Settings {
                multipart_timestamp: S3MultipartTimestamp::Start,
                ..detected
            },
            StorageCheck::PublicationTime,
        ),
        (
            S3Settings {
                listing: S3Listing::V2,
                ..detected
            },
            StorageCheck::Behavior,
        ),
        (
            S3Settings {
                signing: S3Signing::GoogleHmac,
                ..detected
            },
            StorageCheck::Behavior,
        ),
        (
            S3Settings {
                response_headers: S3ResponseHeaders::Google,
                ..detected
            },
            StorageCheck::Behavior,
        ),
    ] {
        let location = fixture.location(Some(S3Profile::Custom(settings)));
        assert_eq!(
            refused_check(detection.resolve(location)),
            check,
            "{settings:?}"
        );
    }
    fixture.assert_clean();
}

#[tokio::test]
async fn a_start_timestamp_service_without_a_conditional_copy_fails_publication_time() {
    let fixture = Fixture::new(Service { copy: None, ..AWS }).await;
    let detection = fixture.detect(None).await.unwrap();
    assert!(detection.held(S3Probe::ConditionalCompletion));
    assert!(!detection.held(S3Probe::CompletionTimestamp));
    assert_eq!(
        refused_check(detection.resolve(fixture.location(None))),
        StorageCheck::PublicationTime
    );
    fixture.assert_clean();
}

#[tokio::test]
async fn a_visible_unfinished_upload_rules_out_conditional_completion() {
    let fixture = Fixture::new(Service {
        partial_visible: true,
        ..R2
    })
    .await;
    let detection = fixture.detect(None).await.unwrap();
    assert!(!detection.held(S3Probe::PartialUploadHidden));
    assert_eq!(
        detection.settings.copy_publication,
        S3CopyPublication::Multipart(S3CopyCondition::CloudflareIfNoneMatch)
    );
    detection.check(&detection.settings).unwrap();
    let named = fixture.location(Some(S3Profile::Named(S3ProfileName::R2)));
    assert_eq!(
        refused_check(detection.resolve(named)),
        StorageCheck::MultipartCreateOnce
    );
    fixture.assert_clean();
}

#[tokio::test]
async fn a_refused_zero_entry_listing_fails_the_behavior_check_and_an_outage_fails_detection() {
    let fixture = Fixture::new(RUSTFS).await;
    fixture.state.lock().unwrap().empty_v2_listing = Some(501);
    let detection = fixture.detect(None).await.unwrap();
    assert!(!detection.held(S3Probe::BucketList));
    assert!(detection.held(S3Probe::Listing(S3Listing::V2)));
    assert_eq!(
        refused_check(detection.resolve(fixture.location(None))),
        StorageCheck::Behavior
    );
    fixture.assert_clean();
    fixture.state.lock().unwrap().empty_v2_listing = Some(503);
    let error = fixture.detect(None).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::Network);
    let state = fixture.state.lock().unwrap();
    assert!(state.objects.is_empty() && state.uploads.is_empty());
}

#[tokio::test]
async fn a_failed_probe_still_deletes_every_probe_object_and_upload() {
    let fixture = Fixture::new(RUSTFS).await;
    fixture.state.lock().unwrap().fail_copies = true;
    let error = fixture.detect(None).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::Network);
    fixture.assert_clean();
}

#[tokio::test]
async fn listings_exclude_detection_probes_and_malformed_ones() {
    let fixture = Fixture::new(RUSTFS).await;
    let storage = StorageConnection::from_provider(Arc::new(
        S3Storage::new(
            fixture.location(Some(S3Profile::Named(S3ProfileName::RustFs))),
            credentials(),
            Arc::new(SystemClock),
            Arc::new(UuidIds),
            std::sync::Arc::new(crate::NoCooldown),
        )
        .unwrap(),
    ));
    let run = uuid::Uuid::from_bytes([7; 16]);
    let probe = format!("{STORE}/.coven-check/{run}/marker-before");
    publish(
        &mut fixture.state.lock().unwrap(),
        probe,
        b"left".to_vec(),
        now(),
    );
    let all = ObjectPrefix::all(STORE);
    assert!(storage.list(&all).await.unwrap().is_empty());
    let malformed = format!("{STORE}/.coven-check/not-a-run/marker");
    publish(
        &mut fixture.state.lock().unwrap(),
        malformed,
        b"left".to_vec(),
        now(),
    );
    assert!(storage.list(&all).await.unwrap().is_empty());
}

#[tokio::test]
async fn v2_is_unsupported_on_501_but_an_outage_fails_detection() {
    let fixture = Fixture::new(Service {
        v2: false,
        ..RUSTFS
    })
    .await;
    let detection = fixture.detect(None).await.unwrap();
    assert!(!detection.held(S3Probe::Listing(S3Listing::V2)));
    assert!(detection.held(S3Probe::Listing(S3Listing::V1)));
    let outage = TestServer::new(Router::new().fallback(|uri: Uri| async move {
        if query(&uri).contains_key("versioning") {
            response(200, "<VersioningConfiguration/>")
        } else if query(&uri).contains_key("list-type") {
            error(503, "ServiceUnavailable")
        } else {
            response(
                200,
                "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>",
            )
        }
    }))
    .await;
    let failed = detect(
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some(outage.url.parse().unwrap()),
            profile: None,
            prefix: String::new(),
        },
        credentials(),
        STORE,
        Arc::new(SystemClock),
        Arc::new(UuidIds),
    )
    .await
    .unwrap_err();
    assert_eq!(failed.failure(), StorageFailure::Network);
}

/// System time that records each pause instead of waiting it.
#[derive(Default)]
struct Pauses(Mutex<Vec<std::time::Duration>>);
impl coven_foundation::clock::Clock for Pauses {
    fn now(&self) -> std::time::SystemTime {
        std::time::SystemTime::now()
    }
    fn elapsed(&self) -> std::time::Duration {
        self.0.lock().unwrap().iter().sum()
    }
    fn sleep(
        &self,
        duration: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.0.lock().unwrap().push(duration);
        Box::pin(std::future::ready(()))
    }
}

#[tokio::test]
async fn complete_v2_does_not_pass_when_v1_never_completes() {
    let fixture = Fixture::new(RUSTFS).await;
    fixture.state.lock().unwrap().incomplete_v1_listing = true;
    let clock = Arc::new(Pauses::default());
    let storage = S3Storage::with_settings(
        fixture.location(None),
        credentials(),
        clock.clone(),
        Arc::new(UuidIds),
        Arc::new(crate::NoCooldown),
        profile::PLAIN,
    )
    .unwrap();
    let mut probes = Probes {
        s: &storage,
        prefix: namespace(&storage, STORE, uuid::Uuid::from_bytes([7; 16])),
        clock: clock.clone(),
        results: Vec::new(),
        written: BTreeSet::new(),
        intended: BTreeSet::new(),
        uploads: Vec::new(),
    };
    probes
        .put(&probes.key("published"), b"visible", None)
        .await
        .unwrap();
    probes.listings(true).await.unwrap();
    assert_eq!(clock.0.lock().unwrap().len(), RETRIES as usize);
    for protocol in [S3Listing::V1, S3Listing::V2] {
        let observation = probes
            .results
            .iter()
            .find(|observation| observation.probe == S3Probe::Listing(protocol))
            .unwrap();
        assert!(!observation.held, "{protocol:?}");
    }
    probes.cleanup().await.unwrap();
    fixture.assert_clean();
}

/// A marker the server never dates later fails detection after the 1 second
/// wait and 40 pauses of 250 ms between its attempts (Appendix E.7).
#[tokio::test]
async fn a_marker_that_never_advances_fails_after_forty_pauses() {
    let fixture = Fixture::new(AWS).await;
    fixture.state.lock().unwrap().frozen = Some(now());
    let pauses = Arc::new(Pauses::default());
    let error = fixture.detect_with(None, pauses.clone()).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::Protocol);
    let mut expected = vec![std::time::Duration::from_secs(1)];
    expected.extend([std::time::Duration::from_millis(250); 40]);
    assert_eq!(*pauses.0.lock().unwrap(), expected);
    fixture.assert_clean();
}

async fn lost_probe_reply(failure: LostReply) {
    let fixture = Fixture::new(RUSTFS).await;
    fixture.state.lock().unwrap().lost_reply = Some(failure);
    assert!(fixture.detect(None).await.is_err(), "{failure:?}");
    let state = fixture.state.lock().unwrap();
    assert!(
        state.objects.is_empty(),
        "{failure:?}: objects left after lost reply"
    );
    assert!(
        state.uploads.is_empty(),
        "{failure:?}: uploads left after lost reply"
    );
}

#[tokio::test]
async fn a_lost_probe_put_reply_leaves_no_object() {
    lost_probe_reply(LostReply::Put).await;
}

#[tokio::test]
async fn a_lost_probe_copy_reply_leaves_no_object() {
    lost_probe_reply(LostReply::Copy).await;
}

#[tokio::test]
async fn a_lost_probe_initiation_reply_leaves_no_native_upload() {
    lost_probe_reply(LostReply::Initiate).await;
}

#[tokio::test]
async fn a_lost_probe_completion_reply_leaves_no_object() {
    lost_probe_reply(LostReply::Complete).await;
}
