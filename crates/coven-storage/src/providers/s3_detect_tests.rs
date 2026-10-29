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

#[path = "s3_detect_server.rs"]
mod server;
use server::*;

/// The server's clock: real seconds, as a service dates its objects.
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
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
            throttle: 0,
            lagging_listings: 0,
            listings: 0,
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
            prefix: crate::S3Prefix::default(),
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
            crate::providers::tests::http_client(),
            &crate::SetupWaits::default(),
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
            crate::providers::tests::http_client(),
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
            prefix: crate::S3Prefix::default(),
        },
        credentials(),
        STORE,
        Arc::new(SystemClock),
        Arc::new(UuidIds),
        crate::providers::tests::http_client(),
        &crate::SetupWaits::default(),
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
        crate::providers::tests::http_client(),
    )
    .unwrap();
    let mut probes = Probes {
        s: &storage,
        prefix: namespace(&storage, STORE, uuid::Uuid::from_bytes([7; 16])),
        clock: clock.clone(),
        waits: &crate::SetupWaits::default(),
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

/// System time that records each pause and waits it.
#[derive(Default)]
struct WaitedPauses(Mutex<Vec<std::time::Duration>>);
impl coven_foundation::clock::Clock for WaitedPauses {
    fn now(&self) -> std::time::SystemTime {
        std::time::SystemTime::now()
    }
    fn elapsed(&self) -> std::time::Duration {
        SystemClock.elapsed()
    }
    fn sleep(
        &self,
        duration: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.0.lock().unwrap().push(duration);
        Box::pin(tokio::time::sleep(duration))
    }
}

/// The two signing probes each draw a 429 first; neither is judged by it.
#[tokio::test]
async fn rate_limited_probes_are_sent_again_and_detection_passes() {
    let fixture = Fixture::new(AWS).await;
    fixture.state.lock().unwrap().throttle = 2;
    let pauses = Arc::new(WaitedPauses::default());
    let detection = fixture.detect_with(None, pauses.clone()).await.unwrap();
    assert_eq!(
        detection.settings,
        profile::CataloguedProfile::named(S3ProfileName::Aws).settings
    );
    assert_eq!(
        pauses.0.lock().unwrap()[..2],
        [1, 2].map(std::time::Duration::from_secs)
    );
    fixture.assert_clean();
}

#[tokio::test]
async fn steady_rate_limiting_fails_detection_once_the_setup_minute_is_spent() {
    let fixture = Fixture::new(AWS).await;
    fixture.state.lock().unwrap().throttle = usize::MAX;
    let pauses = Arc::new(Pauses::default());
    let error = fixture.detect_with(None, pauses.clone()).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::RateLimited);
    // 1 + 2 + 4 + 8 + 16 seconds; the next, 32, would pass the minute.
    assert_eq!(
        *pauses.0.lock().unwrap(),
        [1, 2, 4, 8, 16].map(std::time::Duration::from_secs)
    );
    assert!(fixture.state.lock().unwrap().objects.is_empty());
}

/// While the listing lags, each poll reads one page; only a page showing
/// every probe object is followed by the walk one key per page.
#[tokio::test]
async fn a_lagging_listing_is_polled_one_page_at_a_time() {
    let fixture = Fixture::new(AWS).await;
    let clock = Arc::new(Pauses::default());
    let storage = S3Storage::with_settings(
        fixture.location(None),
        credentials(),
        clock.clone(),
        Arc::new(UuidIds),
        Arc::new(crate::NoCooldown),
        profile::PLAIN,
        crate::providers::tests::http_client(),
    )
    .unwrap();
    let mut probes = Probes {
        s: &storage,
        prefix: namespace(&storage, STORE, uuid::Uuid::from_bytes([8; 16])),
        clock: clock.clone(),
        waits: &crate::SetupWaits::default(),
        results: Vec::new(),
        written: BTreeSet::new(),
        intended: BTreeSet::new(),
        uploads: Vec::new(),
    };
    for index in 0..5 {
        let key = probes.key(&format!("probe-{index}"));
        probes.put(&key, b"probe", None).await.unwrap();
    }
    {
        let mut state = fixture.state.lock().unwrap();
        state.lagging_listings = 3;
        state.listings = 0;
    }
    let listed = probes.visible(S3Listing::V1).await.unwrap().unwrap();
    assert_eq!(listed.len(), 5);
    assert_eq!(
        *clock.0.lock().unwrap(),
        [std::time::Duration::from_millis(250); 3]
    );
    // Three lagging pages and one complete page, then five pages of one key.
    assert_eq!(fixture.state.lock().unwrap().listings, 4 + 5);
    probes.cleanup().await.unwrap();
    fixture.assert_clean();
}
