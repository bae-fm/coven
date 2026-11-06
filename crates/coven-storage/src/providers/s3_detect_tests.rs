//! Detection against an in-process S3 server configured as each known
//! behavior. Times are the test clock's seconds, which detection's waits move.
use super::*;
use crate::providers::s3::tests::test_server::*;
use crate::providers::tests::{query, response, TestServer};
use crate::{S3Profile, S3ProfileName, StorageCheck, StorageCheckError};
use axum::{http::Uri, Router};
use coven_foundation::{
    clock::{FixedClock, PollingClock},
    id_source::{IdSource, UuidIds},
};
use std::sync::{Arc, Mutex};

/// Where the test's clock starts.
const START: std::time::Duration = std::time::Duration::from_secs(1_790_000_000);

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
    /// Detection's clock: it passes each wait at once, moving the server's
    /// time by it, and records the waits.
    clock: Arc<PollingClock>,
}
impl Fixture {
    async fn new(service: Service) -> Self {
        let fixed = Arc::new(FixedClock::new(std::time::UNIX_EPOCH + START));
        let state = Arc::new(Mutex::new(Remote::new(service, fixed.clone())));
        let server = TestServer::new(Router::new().fallback(serve).with_state(state.clone())).await;
        Self { state, server, clock: Arc::new(PollingClock::new(fixed)) }
    }
    fn location(&self, profile: Option<S3Profile>) -> StorageConfig {
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some(self.server.url.parse().unwrap()),
            profile,
        }
    }
    async fn detect(&self, profile: Option<S3Profile>) -> Result<S3Detection, StorageError> {
        let runs = crate::S3DetectionRuns { run: UuidIds.new_id(), earlier: Vec::new() };
        self.detect_runs(profile, &runs).await.detection
    }
    async fn detect_runs(
        &self,
        profile: Option<S3Profile>,
        runs: &crate::S3DetectionRuns,
    ) -> crate::S3DetectionRun {
        detect(
            self.location(profile),
            credentials(),
            STORE,
            runs,
            self.clock.clone(),
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
        assert_eq!(state.bucket_heads, 0, "detection sends no bucket request");
        let store = format!("{STORE}/");
        assert!(
            state.listed.iter().all(|prefix| prefix.starts_with(&store)),
            "a listing outside the store's folder: {:?}",
            state.listed
        );
        let namespace = format!("{STORE}/.coven-check/");
        let runs: std::collections::BTreeSet<_> = state
            .written
            .iter()
            .map(|key| {
                let run = key.strip_prefix(&namespace).expect("probe outside namespace");
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
            Err(StorageCheckError::S3VersioningUnsupported { state: S3BucketVersioning::Enabled })
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
                Err(StorageCheckError::S3VersioningUnknown { .. })
            ));
            fixture.assert_clean();
        }
    }
}

fn refused_check(result: Result<StorageConfig, StorageCheckError>) -> StorageCheck {
    match result {
        Err(StorageCheckError::ProviderCheck { check, source }) => {
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
        let detection =
            fixture.detect(None).await.unwrap_or_else(|error| panic!("{name:?}: {error:?}"));
        let entry = profile::CataloguedProfile::named(name).settings;
        // Detection prefers enforced conditional completion; only the
        // certification races showed SeaweedFS's racing completions, and a
        // racing PUT and copy, both succeed, so its entry publishes every
        // object by copy instead (§20.6).
        let detected = match name {
            S3ProfileName::SeaweedFs => {
                S3Settings { copy_publication: S3CopyPublication::Never, ..entry }
            }
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
    assert_eq!(refused_check(detection.resolve(fixture.location(None))), StorageCheck::CreateOnce);
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
    let fixture = Fixture::new(Service { v2: false, ..RUSTFS }).await;
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
            S3Settings { multipart_timestamp: S3MultipartTimestamp::Start, ..detected },
            StorageCheck::PublicationTime,
        ),
        (S3Settings { listing: S3Listing::V2, ..detected }, StorageCheck::Behavior),
        (S3Settings { signing: S3Signing::GoogleHmac, ..detected }, StorageCheck::Behavior),
        (
            S3Settings { response_headers: S3ResponseHeaders::Google, ..detected },
            StorageCheck::Behavior,
        ),
    ] {
        let location = fixture.location(Some(S3Profile::Custom(settings)));
        assert_eq!(refused_check(detection.resolve(location)), check, "{settings:?}");
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
    let fixture = Fixture::new(Service { partial_visible: true, ..R2 }).await;
    let detection = fixture.detect(None).await.unwrap();
    assert!(!detection.held(S3Probe::PartialUploadHidden));
    assert_eq!(
        detection.settings.copy_publication,
        S3CopyPublication::Multipart(S3CopyCondition::CloudflareIfNoneMatch)
    );
    detection.check(&detection.settings).unwrap();
    let named = fixture.location(Some(S3Profile::Named(S3ProfileName::R2)));
    assert_eq!(refused_check(detection.resolve(named)), StorageCheck::MultipartCreateOnce);
    fixture.assert_clean();
}

#[tokio::test]
async fn a_refused_zero_entry_listing_fails_the_behavior_check_and_an_outage_fails_detection() {
    let fixture = Fixture::new(RUSTFS).await;
    fixture.state.lock().unwrap().empty_v2_listing = Some(501);
    let detection = fixture.detect(None).await.unwrap();
    assert!(!detection.held(S3Probe::BucketList));
    assert!(detection.held(S3Probe::Listing(S3Listing::V2)));
    assert_eq!(refused_check(detection.resolve(fixture.location(None))), StorageCheck::Behavior);
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

/// Detection's cleanup is part of it: probe objects it cannot delete fail
/// the call, keeping the probe's own failure beside the delete's when both
/// happened (§20.7, E.7).
#[tokio::test]
async fn a_failed_probe_cleanup_fails_detection_keeping_both_causes() {
    let failing = Fixture::new(RUSTFS).await;
    {
        let mut state = failing.state.lock().unwrap();
        state.fail_copies = true;
        state.fail_deletes = true;
    }
    match failing.detect(None).await {
        Err(StorageError::Cleanup { operation, cleanup }) => {
            assert_eq!(operation.failure(), StorageFailure::Network);
            assert_eq!(cleanup.failure(), StorageFailure::PermissionDenied);
        }
        other => panic!("both causes are kept: {:?}", other.map(|_| ())),
    }

    let passing = Fixture::new(RUSTFS).await;
    passing.state.lock().unwrap().fail_deletes = true;
    let runs = crate::S3DetectionRuns { run: UuidIds.new_id(), earlier: Vec::new() };
    let run = passing.detect_runs(None, &runs).await;
    let error = run.detection.map(|_| ()).unwrap_err();
    assert_eq!(error.failure(), StorageFailure::PermissionDenied);
    assert!(run.cleaned.is_empty(), "a run whose cleanup failed isn't confirmed gone");
}

/// Kill a detection run while it uploads its multipart probe's part, as a
/// crash would: its unfinished upload and its probes stay behind.
async fn killed_run(fixture: &Fixture) -> uuid::Uuid {
    let stalled = Arc::new(tokio::sync::Notify::new());
    fixture.state.lock().unwrap().stall_parts = Some(stalled.clone());
    let killed = crate::S3DetectionRuns { run: UuidIds.new_id(), earlier: Vec::new() };
    tokio::select! {
        run = fixture.detect_runs(None, &killed) => panic!("the part upload never answers: {run:?}"),
        () = stalled.notified() => {}
    }
    let mut state = fixture.state.lock().unwrap();
    assert_eq!(state.uploads.len(), 1, "the killed run's upload is left");
    assert!(!state.objects.is_empty(), "the killed run's probes are left");
    state.stall_parts = None;
    killed.run
}

/// Ana's setup is killed while detection uploads a part: that run's
/// unfinished upload, which her provider keeps billing, and its probes
/// stay. The next detection, handed that run as an earlier one, aborts the
/// upload and deletes the probes, and says so (§20.7).
#[tokio::test]
async fn the_next_detection_removes_what_a_killed_run_left() {
    let fixture = Fixture::new(RUSTFS).await;
    let killed = killed_run(&fixture).await;
    let next = crate::S3DetectionRuns { run: UuidIds.new_id(), earlier: vec![killed] };
    let run = fixture.detect_runs(None, &next).await;
    run.detection.unwrap();
    assert_eq!(run.cleaned, [next.run, killed]);
    let state = fixture.state.lock().unwrap();
    assert!(state.uploads.is_empty(), "the killed run's upload is aborted");
    assert!(state.objects.is_empty(), "the killed run's probes are deleted");
}

/// The store's connection to the bucket removes what a killed run left
/// there, as listing the run's prefix finds it, and removing it again finds
/// nothing (§20.7).
#[tokio::test]
async fn a_connection_removes_what_a_killed_run_left() {
    let fixture = Fixture::new(RUSTFS).await;
    let killed = killed_run(&fixture).await;
    let storage = StorageConnection::from_provider(Arc::new(
        S3Storage::new(
            fixture.location(Some(S3Profile::Named(S3ProfileName::RustFs))),
            credentials(),
            fixture.clock.clone(),
            Arc::new(UuidIds),
            std::sync::Arc::new(crate::NoCooldown),
            crate::providers::tests::http_client(),
        )
        .unwrap(),
    ));
    storage.end_detection_run(STORE, killed).await.unwrap();
    {
        let state = fixture.state.lock().unwrap();
        assert!(state.uploads.is_empty(), "the killed run's upload is aborted");
        assert!(state.objects.is_empty(), "the killed run's probes are deleted");
    }
    storage.end_detection_run(STORE, killed).await.unwrap();
}

/// An earlier run whose leftovers can't be removed fails detection and
/// isn't confirmed gone; a later detection that can remove them confirms
/// both runs (§20.7).
#[tokio::test]
async fn an_earlier_run_is_confirmed_gone_only_once_its_leftovers_are() {
    let fixture = Fixture::new(RUSTFS).await;
    let killed = killed_run(&fixture).await;
    fixture.state.lock().unwrap().fail_deletes = true;
    let refused = crate::S3DetectionRuns { run: UuidIds.new_id(), earlier: vec![killed] };
    let run = fixture.detect_runs(None, &refused).await;
    assert_eq!(run.detection.unwrap_err().failure(), StorageFailure::PermissionDenied);
    assert!(run.cleaned.is_empty(), "{:?}", run.cleaned);
    fixture.state.lock().unwrap().fail_deletes = false;
    let last = crate::S3DetectionRuns { run: UuidIds.new_id(), earlier: vec![killed, refused.run] };
    let run = fixture.detect_runs(None, &last).await;
    run.detection.unwrap();
    assert_eq!(run.cleaned, [last.run, killed, refused.run]);
    let state = fixture.state.lock().unwrap();
    assert!(state.uploads.is_empty() && state.objects.is_empty());
}

/// Detection that fails before its first write leaves its own run with
/// nothing to remove, and removes no earlier run's leftovers (§20.7).
#[tokio::test]
async fn a_detection_failing_before_its_first_write_confirms_only_its_own_run() {
    let fixture = Fixture::new(RUSTFS).await;
    let killed = killed_run(&fixture).await;
    fixture.state.lock().unwrap().versioning_failure = Some(503);
    let runs = crate::S3DetectionRuns { run: UuidIds.new_id(), earlier: vec![killed] };
    let run = fixture.detect_runs(None, &runs).await;
    assert_eq!(run.detection.unwrap_err().failure(), StorageFailure::Network);
    assert_eq!(run.cleaned, [runs.run]);
    assert_eq!(fixture.state.lock().unwrap().uploads.len(), 1);
}

/// A probe's recorded outcome names no key, endpoint or object: what a
/// person sees of detection holds only fixed labels, statuses and times
/// (§20.7).
#[tokio::test]
async fn probe_results_name_no_key_endpoint_or_object() {
    for service in [AWS, GCS, R2, RUSTFS, SEAWEEDFS, VERSITY_GW, OVERWRITES] {
        let fixture = Fixture::new(service).await;
        let detection = detect(
            fixture.location(None),
            S3Credentials {
                access_key_id: "AKIADISTINCTIVE7".into(),
                secret_access_key: SecretText::new("secretDistinctive9".into()),
            },
            STORE,
            &crate::S3DetectionRuns { run: UuidIds.new_id(), earlier: Vec::new() },
            fixture.clock.clone(),
            Arc::new(UuidIds),
            crate::providers::tests::http_client(),
            &crate::SetupWaits::default(),
        )
        .await
        .detection
        .unwrap();
        let address = fixture.server.url.trim_start_matches("http://").to_owned();
        assert!(!detection.results.is_empty());
        let mut probes = BTreeSet::new();
        for result in &detection.results {
            assert!(probes.insert(format!("{:?}", result.probe)), "{:?} ran twice", result.probe);
            for forbidden in [
                "AKIADISTINCTIVE7",
                "secretDistinctive9",
                address.as_str(),
                "bucket",
                ".coven-check",
                "probe",
                &STORE.to_string(),
            ] {
                assert!(
                    !result.detail.contains(forbidden),
                    "{:?} detail {:?} names {forbidden}",
                    result.probe,
                    result.detail
                );
            }
        }
    }
}

/// Detection pauses for a later server second only while it waits for the
/// timestamp marker: one second, then at most forty quarter-second pauses,
/// and a server whose listings never lag needs no other pause (§20.7, E.7).
#[tokio::test]
async fn detection_pauses_only_for_the_timestamp_marker() {
    let fixture = Fixture::new(AWS).await;
    fixture.detect(None).await.unwrap();
    let pauses = fixture.clock.waited();
    let first = std::time::Duration::from_secs(1);
    let retry = std::time::Duration::from_millis(250);
    assert_eq!(pauses.first(), Some(&first));
    let mut run = 0;
    for pause in &pauses {
        if *pause == first {
            run = 0;
        } else {
            assert_eq!(*pause, retry, "{pauses:?}");
            run += 1;
            assert!(run <= 40, "{pauses:?}");
        }
    }
    fixture.assert_clean();
}

#[tokio::test]
async fn listings_exclude_detection_probes_and_malformed_ones() {
    let fixture = Fixture::new(RUSTFS).await;
    let storage = StorageConnection::from_provider(Arc::new(
        S3Storage::new(
            fixture.location(Some(S3Profile::Named(S3ProfileName::RustFs))),
            credentials(),
            fixture.clock.clone(),
            Arc::new(UuidIds),
            std::sync::Arc::new(crate::NoCooldown),
            crate::providers::tests::http_client(),
        )
        .unwrap(),
    ));
    let run = uuid::Uuid::from_bytes([7; 16]);
    let probe = format!("{STORE}/.coven-check/{run}/marker-before");
    {
        let mut state = fixture.state.lock().unwrap();
        let modified = seconds(&state);
        publish(&mut state, probe, b"left".to_vec(), modified);
    }
    let all = ObjectPrefix::all(STORE);
    assert!(storage.list(&all).await.unwrap().is_empty());
    let malformed = format!("{STORE}/.coven-check/not-a-run/marker");
    {
        let mut state = fixture.state.lock().unwrap();
        let modified = seconds(&state);
        publish(&mut state, malformed, b"left".to_vec(), modified);
    }
    assert!(storage.list(&all).await.unwrap().is_empty());
}

#[tokio::test]
async fn v2_is_unsupported_on_501_but_an_outage_fails_detection() {
    let fixture = Fixture::new(Service { v2: false, ..RUSTFS }).await;
    let detection = fixture.detect(None).await.unwrap();
    assert!(!detection.held(S3Probe::Listing(S3Listing::V2)));
    assert!(detection.held(S3Probe::Listing(S3Listing::V1)));
    let outage = TestServer::new(Router::new().fallback(|uri: Uri| async move {
        if query(&uri).contains_key("versioning") {
            response(200, "<VersioningConfiguration/>")
        } else if query(&uri).contains_key("list-type") {
            error(503, "ServiceUnavailable")
        } else {
            response(200, "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>")
        }
    }))
    .await;
    let failed = detect(
        StorageConfig::S3 {
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some(outage.url.parse().unwrap()),
            profile: None,
        },
        credentials(),
        STORE,
        &crate::S3DetectionRuns { run: UuidIds.new_id(), earlier: Vec::new() },
        fixture.clock.clone(),
        Arc::new(UuidIds),
        crate::providers::tests::http_client(),
        &crate::SetupWaits::default(),
    )
    .await
    .detection
    .unwrap_err();
    assert_eq!(failed.failure(), StorageFailure::Network);
}

#[tokio::test]
async fn complete_v2_does_not_pass_when_v1_never_completes() {
    let fixture = Fixture::new(RUSTFS).await;
    fixture.state.lock().unwrap().incomplete_v1_listing = true;
    let clock = fixture.clock.clone();
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
        prefix: namespace(STORE, uuid::Uuid::from_bytes([7; 16])),
        clock: clock.clone(),
        waits: &crate::SetupWaits::default(),
        results: Vec::new(),
        written: BTreeSet::new(),
        intended: BTreeSet::new(),
        uploads: Vec::new(),
    };
    probes.put(&probes.key("published"), b"visible", None).await.unwrap();
    probes.listings(true).await.unwrap();
    assert_eq!(clock.waited().len(), RETRIES as usize);
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
    {
        let mut state = fixture.state.lock().unwrap();
        state.frozen = Some(seconds(&state));
    }
    let error = fixture.detect(None).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::Protocol);
    let mut expected = vec![std::time::Duration::from_secs(1)];
    expected.extend([std::time::Duration::from_millis(250); 40]);
    assert_eq!(fixture.clock.waited(), expected);
    fixture.assert_clean();
}

async fn lost_probe_reply(failure: LostReply) {
    let fixture = Fixture::new(RUSTFS).await;
    fixture.state.lock().unwrap().lost_reply = Some(failure);
    assert!(fixture.detect(None).await.is_err(), "{failure:?}");
    let state = fixture.state.lock().unwrap();
    assert!(state.objects.is_empty(), "{failure:?}: objects left after lost reply");
    assert!(state.uploads.is_empty(), "{failure:?}: uploads left after lost reply");
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

/// The two signing probes each draw a 429 first; neither is judged by it.
#[tokio::test]
async fn rate_limited_probes_are_sent_again_and_detection_passes() {
    let fixture = Fixture::new(AWS).await;
    fixture.state.lock().unwrap().throttle = 2;
    let detection = fixture.detect(None).await.unwrap();
    assert_eq!(detection.settings, profile::CataloguedProfile::named(S3ProfileName::Aws).settings);
    assert_eq!(fixture.clock.waited()[..2], [1, 2].map(std::time::Duration::from_secs));
    fixture.assert_clean();
}

#[tokio::test]
async fn steady_rate_limiting_fails_detection_once_the_setup_minute_is_spent() {
    let fixture = Fixture::new(AWS).await;
    fixture.state.lock().unwrap().throttle = usize::MAX;
    let error = fixture.detect(None).await.unwrap_err();
    assert_eq!(error.failure(), StorageFailure::RateLimited);
    // 1 + 2 + 4 + 8 + 16 seconds; the next, 32, would pass the minute.
    assert_eq!(fixture.clock.waited(), [1, 2, 4, 8, 16].map(std::time::Duration::from_secs));
    assert!(fixture.state.lock().unwrap().objects.is_empty());
}

/// While the listing lags, each poll reads one page; only a page showing
/// every probe object is followed by the walk one key per page.
#[tokio::test]
async fn a_lagging_listing_is_polled_one_page_at_a_time() {
    let fixture = Fixture::new(AWS).await;
    let clock = fixture.clock.clone();
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
        prefix: namespace(STORE, uuid::Uuid::from_bytes([8; 16])),
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
    assert_eq!(*clock.waited(), [std::time::Duration::from_millis(250); 3]);
    // Three lagging pages and one complete page, then five pages of one key.
    assert_eq!(fixture.state.lock().unwrap().listings, 4 + 5);
    probes.cleanup().await.unwrap();
    fixture.assert_clean();
}
