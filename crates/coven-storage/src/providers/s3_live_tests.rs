//! Production detection feeds a candidate; adapter checks certify it.
//! Only fixed labels, behavior enums, timestamps and durations enter output.
use super::*;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use coven_foundation::id_source::{DeviceId, StoreId};
use profile::*;
use std::{
    future::Future,
    time::{Duration, Instant},
};
use tokio::{sync::Barrier, task::JoinSet};

/// Version 4 records bucket versioning and requires overlapping conditional
/// races. Version 5 races a named profile's own publication, rather than the
/// detected settings, which cannot see that a race needs copy publication.
/// Version 6 observes whether a publishing copy's reply describes the object
/// as a HEAD after it does; that evidence never decides the verdict.
pub(super) const SUITE_VERSION: u32 = 6;

#[path = "s3_copy_reply_live.rs"]
mod copy_reply;
#[path = "s3_race.rs"]
mod races;

#[tokio::test]
#[ignore = "COVEN_LIVE_S3_ENV and a certified profile; creates and deletes only this run's store objects"]
async fn s3_live_sequence() {
    use crate::providers::tests::sequence_live;
    let clock = coven_foundation::clock::SystemClock;
    let settings = live::LiveS3::load();
    let StorageConfig::S3 {
        endpoint, profile, ..
    } = &settings.config
    else {
        unreachable!()
    };
    let Selected::Catalogued(selected) =
        profile::select(endpoint.as_ref(), profile.as_ref()).unwrap()
    else {
        panic!("sequence evidence requires a catalogued profile");
    };
    assert!(
        matches!(selected.certification, Certification::Passed { .. }),
        "profile must be certified"
    );
    let connected = settings.connect(Some(selected.settings));
    let before = connected
        .provider
        .bucket_versioning()
        .await
        .expect("read bucket versioning before sequence writes");
    versioning::check(endpoint.as_ref(), profile.as_ref(), &Ok(before)).unwrap();
    println!("sequence profile={:?} versioning={before:?}", selected.name);
    // Native replies establish exclusivity without adapter recovery hiding an
    // extra accepted publication. Every case owns a fresh store for cleanup.
    for (race, width) in [
        (races::Race::Put, 2),
        (races::Race::Put, 8),
        (races::Race::Multipart, 2),
        (races::Race::Multipart, 8),
        (races::Race::Mixed, 2),
        (races::Race::Replace, 2),
    ] {
        assert!(
            case(
                &settings,
                &["sequence_native_race"],
                selected.settings,
                move |s, store| run_race(s, store, race, width)
            )
            .await
            .is_some(),
            "sequence native race failed: {race:?} width={width}"
        );
    }
    let writers: Vec<Arc<dyn Storage>> = (0..8)
        .map(|_| Arc::new(settings.connect(Some(selected.settings))) as Arc<dyn Storage>)
        .collect();
    assert!(
        case(
            &settings,
            &["sequence_floor_and_lost_replies"],
            selected.settings,
            move |_, store| async move {
                sequence_live::lost_replies(
                    &clock,
                    writers[0].as_ref(),
                    writers[1].as_ref(),
                    store,
                )
                .await?;
                sequence_live::floor(&clock, &writers, store, sequence_live::AllRefused::Fails)
                    .await
            }
        )
        .await
        .is_some(),
        "sequence floor or reply settlement failed"
    );
    let after = connected.provider.bucket_versioning().await.unwrap();
    assert_eq!(before, after, "bucket versioning changed during the run");
    println!("sequence S3 PASS cleanup=PASS");
}

fn path(store: StoreId, number: u64) -> ObjectPath {
    ObjectPath::slot(store, number.try_into().unwrap())
}
fn require(ok: bool, reason: &'static str) -> Result<(), StorageError> {
    if ok {
        Ok(())
    } else {
        Err(StorageFailure::Protocol.with_source(reason))
    }
}
async fn head(s: &S3Storage, key: &str) -> Result<HeadObjectOutput, StorageError> {
    s.client
        .head_object()
        .bucket(&s.bucket)
        .key(key)
        .send()
        .await
        .map_err(s3_error)
}
/// Print every observation, the derived settings, the setup safety verdict
/// for an uncatalogued location, and the catalogued entries they reproduce.
fn report(detection: &crate::S3Detection, config: &StorageConfig) {
    match &detection.versioning {
        Ok(state) => println!("bucket versioning={state:?}"),
        Err(error) => println!("bucket versioning=Unknown cause={error}"),
    }
    for observation in &detection.results {
        println!(
            "detect {:?} held={} {}",
            observation.probe, observation.held, observation.detail
        );
    }
    println!("detected {:?}", detection.settings);
    match detection.resolve(config.clone()) {
        Ok(_) => println!("setup_safety_check PASS"),
        Err(error) => println!("setup_safety_check refused: {error}"),
    }
    let matching: Vec<_> = PROFILES
        .iter()
        .filter(|profile| profile.settings == detection.settings)
        .map(|profile| profile.name)
        .collect();
    println!("catalogued entries reproduced: {matching:?}");
    for profile in PROFILES {
        if profile.settings != detection.settings {
            let differing = differences(&profile.settings, &detection.settings);
            println!("differs from {:?}: {differing}", profile.name);
        }
    }
}
fn differences(entry: &S3Settings, detected: &S3Settings) -> String {
    let mut fields = Vec::new();
    macro_rules! compare {
        ($($field:ident),*) => {$(
            if entry.$field != detected.$field {
                fields.push(format!("{}={:?}/{:?}", stringify!($field), entry.$field, detected.$field));
            }
        )*};
    }
    compare!(
        signing,
        single_put,
        copy_publication,
        multipart_timestamp,
        checksums,
        listing,
        revision,
        response_headers,
        request_checksums
    );
    fields.join(" ")
}

#[tokio::test]
#[ignore = "COVEN_LIVE_S3_ENV; creates and deletes live probe objects and multipart uploads"]
async fn s3_detection() {
    let settings = live::LiveS3::load();
    let detection = settings.detect().await.expect("detection");
    report(&detection, &settings.config);
}

/// Suite 6's copy reply evidence alone, for the location's own profile.
#[tokio::test]
#[ignore = "COVEN_LIVE_S3_ENV and a catalogued or named profile; creates and deletes only this run's store objects"]
async fn s3_copy_reply() {
    let settings = live::LiveS3::load();
    let StorageConfig::S3 {
        endpoint, profile, ..
    } = &settings.config
    else {
        unreachable!()
    };
    let selected = profile::settings(endpoint.as_ref(), profile.as_ref())
        .expect("valid live location")
        .expect("copy reply evidence names its profile");
    copy_reply::report(&settings, selected).await;
}

/// Three installations' setups of one store claim its first slot at once
/// (§10.3, D10), each through its own connection: one claim wins and the others
/// are refused, in each of 20 fresh stores.
#[tokio::test]
#[ignore = "COVEN_LIVE_S3_ENV; creates and deletes live first slots"]
async fn s3_first_slot_race() {
    let settings = live::LiveS3::load();
    let StorageConfig::S3 {
        endpoint, profile, ..
    } = &settings.config
    else {
        unreachable!()
    };
    let selected = match profile::settings(endpoint.as_ref(), profile.as_ref())
        .expect("valid live location")
    {
        Some(selected) => selected,
        None => settings.detect().await.expect("detection").settings,
    };
    for _ in 0..20 {
        let rivals: [Arc<dyn Storage>; 2] = [
            Arc::new(settings.connect(Some(selected))),
            Arc::new(settings.connect(Some(selected))),
        ];
        let raced = case(
            &settings,
            &["first_slot_race"],
            selected,
            |s, store| async move {
                let [second, third] = rivals;
                crate::test_utils::Conformance::new(s.clone(), store)
                    .creation_race([s, second, third])
                    .await
            },
        )
        .await;
        assert!(raced.is_some(), "first slot race failed");
    }
}

#[tokio::test]
#[ignore = "COVEN_LIVE_S3_ENV; reads bucket versioning without writing objects"]
async fn s3_bucket_versioning() {
    let settings = live::LiveS3::load();
    let StorageConfig::S3 {
        endpoint, profile, ..
    } = &settings.config
    else {
        unreachable!()
    };
    // An uncatalogued endpoint naming no profile has no settings before
    // detection; the versioning read needs only the plain S3 signer.
    let selected = profile::settings(endpoint.as_ref(), profile.as_ref())
        .expect("valid live location")
        .unwrap_or(profile::PLAIN);
    let s = settings.connect(Some(selected));
    let versioning = s.provider.bucket_versioning().await.map_err(Arc::new);
    match &versioning {
        Ok(state) => println!("bucket versioning={state:?}"),
        Err(error) => println!(
            "bucket versioning=Unknown failure={:?} status={:?}",
            error.failure(),
            error::native_status(error)
        ),
    }
    match versioning::check(endpoint.as_ref(), profile.as_ref(), &versioning) {
        Ok(()) => println!("setup_versioning_check PASS"),
        Err(error) => println!("setup_versioning_check refused: {error}"),
    }
    if let Err(error) = versioning {
        assert!(
            versioning::unknown(&error),
            "versioning read failed: {error}"
        );
    }
}

/// Each independent case gets its own namespace. Unwinding still reaches cleanup.
async fn case<T, F, Fut>(
    settings: &live::LiveS3,
    requirements: &[&str],
    profile: S3Settings,
    work: F,
) -> Option<T>
where
    F: FnOnce(Arc<StorageConnection<S3Storage>>, StoreId) -> Fut,
    Fut: Future<Output = Result<T, StorageError>>,
{
    let label = format!("§20 {}", requirements.join(", "));
    let value = in_namespace(settings, &label, profile, work).await;
    for requirement in requirements {
        println!(
            "§20 {requirement} {} cleanup=PASS",
            if value.is_some() { "PASS" } else { "FAIL" }
        );
    }
    value
}
/// Run `work` in a fresh store namespace and remove everything it wrote,
/// printing a failure under `label`. A failed cleanup stops the suite.
async fn in_namespace<T, F, Fut>(
    settings: &live::LiveS3,
    label: &str,
    profile: S3Settings,
    work: F,
) -> Option<T>
where
    F: FnOnce(Arc<StorageConnection<S3Storage>>, StoreId) -> Fut,
    Fut: Future<Output = Result<T, StorageError>>,
{
    let store = StoreId(uuid::Uuid::new_v4());
    let storage = Arc::new(settings.connect(Some(profile)));
    use futures_util::FutureExt;
    let result = std::panic::AssertUnwindSafe(work(storage.clone(), store))
        .catch_unwind()
        .await;
    let cleanup = live_cleanup::cleanup(&storage.provider, store).await;
    let value = match result {
        Ok(Ok(value)) => Some(value),
        Ok(Err(error)) => {
            println!("{label} cause={error}");
            None
        }
        Err(_) => {
            println!("{label} assertion_failed");
            None
        }
    };
    if let Err(error) = cleanup {
        println!("{label} cleanup FAIL cause={error}");
        panic!("certification could not delete its live objects/uploads");
    }
    value
}
async fn publication(
    s: Arc<StorageConnection<S3Storage>>,
    store: StoreId,
) -> Result<(), StorageError> {
    let marker = ObjectPath::floor(store);
    s.create(&marker, b"initial").await?;
    for multipart in [false, true] {
        let target = path(store, if multipart { 2 } else { 1 });
        let mut pending = if multipart {
            Some(s.begin_upload(&target, 4).await?)
        } else {
            None
        };
        if let Some(upload) = &mut pending {
            s.upload_part(upload, b"data").await?;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        s.replace(
            &s.status(&marker).await?.ok_or(StorageFailure::NotFound)?,
            b"before",
        )
        .await?;
        let before = s.status(&marker).await?.ok_or(StorageFailure::NotFound)?;
        if let Some(upload) = &mut pending {
            s.finish_upload(upload).await?;
        } else {
            s.create(&target, b"data").await?;
        }
        s.replace(&before, b"after").await?;
        let after = s.status(&marker).await?.ok_or(StorageFailure::NotFound)?;
        let published = s.status(&target).await?.ok_or(StorageFailure::NotFound)?;
        println!(
            "publication multipart={multipart} before={:?} published={:?} after={:?}",
            before.stored_at, published.stored_at, after.stored_at
        );
        require(
            published.stored_at >= before.stored_at && published.stored_at <= after.stored_at,
            "publication outside completion markers",
        )?;
    }
    Ok(())
}
async fn replacements(
    s: Arc<StorageConnection<S3Storage>>,
    store: StoreId,
) -> Result<(), StorageError> {
    for target in [ObjectPath::floor(store)] {
        s.create(&target, b"first").await?;
        let mut previous = s.status(&target).await?.ok_or(StorageFailure::NotFound)?;
        for data in [b"other", b"first"] {
            s.replace(&previous, data).await?;
            let next = s.read_object(&target, None).await?;
            let changed = next.metadata.revision != previous.revision;
            println!(
                "replacement changed={changed} same_size={} same_tick={}",
                next.metadata.size == previous.size,
                next.metadata.stored_at == previous.stored_at
            );
            require(
                changed && next.bytes == data,
                "replacement revision or bytes disagree",
            )?;
            previous = next.metadata;
        }
    }
    Ok(())
}
async fn listing_lag(
    s: Arc<StorageConnection<S3Storage>>,
    store: StoreId,
) -> Result<(), StorageError> {
    use crate::providers::tests::listing_lag::{observe, report, TRIALS};
    use tokio::time::Instant;
    let mut samples = Vec::new();
    for trial in 1..=TRIALS {
        let target = ObjectPath::file(
            store,
            DeviceId(31),
            coven_foundation::id_source::FileId(uuid::Uuid::from_u128(trial.into())),
        );
        s.create(&target, b"listing visibility").await?;
        let replied = Instant::now();
        let stored_at = detect::time(&head(&s.provider, &s.provider.prefix.key(&target)).await?)?;
        let all = ObjectPrefix::files(store);
        let object = observe("S3_object", trial, replied, stored_at, || async {
            Ok(s.list(&all)
                .await?
                .iter()
                .any(|object| object.path == target))
        })
        .await;
        samples.push(object);
        if trial > 1 {
            s.delete(&target).await?;
        }
    }
    report("S3_object", &samples);
    Ok(())
}

async fn run_race(
    s: Arc<StorageConnection<S3Storage>>,
    store: StoreId,
    race: races::Race,
    width: usize,
) -> Result<(), StorageError> {
    use crate::providers::tests::sequence_live;
    let clock = coven_foundation::clock::SystemClock;
    let trials = if width == 8 { 50 } else { 200 };
    if matches!(race, races::Race::Replace) {
        sequence_live::retry(&clock, async || {
            s.create(&ObjectPath::floor(store), b"initial revision")
                .await
        })
        .await?;
    }
    let tally = sequence_live::rounds(
        &clock,
        &format!("{race:?} writers={width}"),
        trials,
        async |trial, attempt| {
            run_race_round(&clock, s.clone(), store, race, width, trial, attempt).await
        },
    )
    .await?;
    println!("race SUMMARY {race:?} writers={width} trials={trials} overlapping={trials} successes={trials} throttled={} violations=0", tally.throttled);
    Ok(())
}

async fn run_race_round(
    clock: &dyn coven_foundation::clock::Clock,
    s: Arc<StorageConnection<S3Storage>>,
    store: StoreId,
    race: races::Race,
    width: usize,
    trial: u64,
    attempt: u64,
) -> Result<crate::providers::tests::sequence_live::Round, StorageError> {
    use crate::providers::tests::sequence_live;
    use races::{fire, judge, prepare, single, Race, Reply, Write};
    let target = if matches!(race, Race::Replace) {
        ObjectPath::floor(store)
    } else {
        path(store, attempt)
    };
    let previous = if matches!(race, Race::Replace) {
        Some(s.read_object(&target, None).await?)
    } else {
        None
    };
    let payloads: Vec<_> = (0..width)
        .map(|writer| {
            let mut bytes = format!("trial={trial} attempt={attempt} writer={writer} ")
                .repeat(4096)
                .into_bytes();
            bytes.truncate(64 * 1024);
            bytes
        })
        .collect();
    let mut prepared = Vec::new();
    // Preparation failures retry the whole round with a fresh destination;
    // this case's namespace records every temporary and upload for cleanup.
    for (writer, bytes) in payloads.iter().enumerate() {
        prepared.push(match &previous {
            Some(previous) => Write::Put {
                header: match s.provider.settings.revision {
                    S3Revision::ETag => "if-match",
                    S3Revision::Generation => "x-goog-if-generation-match",
                },
                value: previous.metadata.replace_token.clone(),
            },
            None if matches!(race, Race::Multipart)
                || matches!(race, Race::Mixed) && writer == 1 =>
            {
                prepare(&s, &target, bytes).await?
            }
            None => single(&s.provider, &target, bytes).await?,
        });
    }
    let barrier = Arc::new(Barrier::new(width + 1));
    let origin = Instant::now();
    let mut tasks = JoinSet::new();
    for (writer, write) in prepared.into_iter().enumerate() {
        let s = s.clone();
        let target = target.clone();
        let bytes = payloads[writer].clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            let start = origin.elapsed();
            let result = fire(&s.provider, &target, bytes, write).await;
            Reply {
                writer,
                start,
                end: origin.elapsed(),
                result,
            }
        });
    }
    barrier.wait().await;
    let mut joined = Vec::new();
    while let Some(reply) = tasks.join_next().await {
        joined.push(reply);
    }
    // Await every worker before a failed request can reach cleanup.
    let mut replies = joined
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| StorageFailure::Protocol.with_source(error))?;
    replies.sort_by_key(|reply| reply.writer);
    for reply in &replies {
        println!("race {race:?} writers={width} trial={trial} attempt={attempt} writer={} start_us={} end_us={} outcome={}",
            reply.writer, reply.start.as_micros(), reply.end.as_micros(), detect::outcome(&reply.result));
    }
    sequence_live::wait_for_writers(clock, &replies).await?;
    let stored = sequence_live::observed(clock, s.as_ref(), &target).await?;
    if let Some(previous) = &previous {
        if replies.iter().any(|reply| reply.result.is_ok()) {
            require(
                stored
                    .as_ref()
                    .is_some_and(|s| s.metadata.revision != previous.metadata.revision),
                "floor winner retained the previous revision",
            )?;
        } else {
            require(
                stored.as_ref() == Some(previous),
                "refused floor writers changed its revision or bytes",
            )?;
        }
    }
    let round = judge(
        replies,
        &payloads,
        stored.as_ref().map(|s| s.bytes.as_slice()),
        previous.as_ref().map(|p| p.bytes.as_slice()),
        sequence_live::AllRefused::Fails,
    )?;
    println!("race {race:?} writers={width} trial={trial} attempt={attempt} overlap=true successes=1 pass=true");
    Ok(round)
}

#[tokio::test]
#[ignore = "COVEN_LIVE_S3_ENV; creates and deletes live objects and multipart uploads"]
async fn s3_certification() {
    let settings = live::LiveS3::load();
    let detection = match settings.detect().await {
        Ok(detection) => detection,
        Err(error) => {
            println!("§20 native_behavior_detection cause={error}");
            println!("§20 native_behavior_detection FAIL");
            panic!("S3 certification could not detect the service");
        }
    };
    report(&detection, &settings.config);
    // A named profile is certified with its own settings, which the safety
    // check below compares with the results; otherwise the detected ones.
    let StorageConfig::S3 { profile: named, .. } = &settings.config else {
        unreachable!()
    };
    let profile = match named {
        Some(S3Profile::Named(name)) => CataloguedProfile::named(*name).settings,
        _ => detection.settings,
    };
    println!("candidate {profile:?}");
    let mut passed = detection.check(&profile).is_ok();
    println!(
        "§20 native_behavior_detection {}",
        if passed { "PASS" } else { "FAIL" }
    );
    assert!(
        detection.versioning.is_ok(),
        "certification requires a known bucket versioning state; see detection output"
    );
    // Certification can establish new evidence even when setup refuses the
    // bucket's versioning state. The setup verdict above remains separate.
    for (race, width, label) in [
        (races::Race::Put, 2, "concurrent_put_2"),
        (races::Race::Put, 8, "concurrent_put_8"),
        (
            races::Race::Multipart,
            2,
            "concurrent_multipart_publication_2",
        ),
        (
            races::Race::Multipart,
            8,
            "concurrent_multipart_publication_8",
        ),
        (
            races::Race::Mixed,
            2,
            "concurrent_put_and_multipart_publication",
        ),
        (
            races::Race::Replace,
            2,
            "concurrent_replace_current_revision",
        ),
    ] {
        passed &= case(&settings, &[label], profile, move |s, store| {
            run_race(s, store, race, width)
        })
        .await
        .is_some();
    }
    passed &= case(
        &settings,
        &[
            "create_once_single_put",
            "whole_object_reads",
            "ranged_reads",
            "prefix_listing",
            "exact_status",
            "deletion",
            "setup_provider_check",
        ],
        profile,
        |s, store| async move {
            conformance(s, store).await;
            Ok(())
        },
    )
    .await
    .is_some();
    passed &= case(
        &settings,
        &[
            "create_once_multipart",
            "partial_uploads_hidden",
            "multipart_resume",
        ],
        profile,
        |s, store| async move {
            multipart(s, store).await;
            Ok(())
        },
    )
    .await
    .is_some();
    passed &= case(
        &settings,
        &[
            "single_put_timestamp_equals_completion",
            "multipart_timestamp_equals_completion",
        ],
        profile,
        publication,
    )
    .await
    .is_some();
    passed &= case(
        &settings,
        &["revision_changes_on_replace"],
        profile,
        replacements,
    )
    .await
    .is_some();
    passed &= case(&settings, &["listing_lag"], profile, listing_lag)
        .await
        .is_some();
    // Separate evidence: a copy reply that falls short leaves status to HEAD.
    copy_reply::report(&settings, profile).await;
    let after = settings
        .connect(Some(profile))
        .provider
        .bucket_versioning()
        .await;
    match after {
        Ok(state) => {
            let stable = detection
                .versioning
                .as_ref()
                .is_ok_and(|before| *before == state);
            println!("bucket versioning_after={state:?} stable={stable}");
            passed &= stable;
        }
        Err(error) => {
            println!("bucket versioning_after=Unknown cause={error}");
            passed = false;
        }
    }
    let date = aws_sdk_s3::primitives::DateTime::from(SystemTime::now())
        .fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)
        .unwrap();
    print_profile(
        profile,
        &settings.config,
        detection.versioning.as_ref().unwrap(),
        if passed { Some(&date[..10]) } else { None },
    );
    println!(
        "certification suite={SUITE_VERSION} {}",
        if passed { "PASS" } else { "FAIL" }
    );
    assert!(passed, "S3 certification failed; see §20 lines above");
}

fn print_profile(
    p: S3Settings,
    config: &StorageConfig,
    versioning: &S3BucketVersioning,
    date: Option<&str>,
) {
    let StorageConfig::S3 {
        endpoint, profile, ..
    } = config
    else {
        unreachable!()
    };
    // Only the hostname is source data, never a URL's credentials, path or query.
    let selected = profile::select(endpoint.as_ref(), profile.as_ref()).unwrap();
    let key_scope = selected.key_scope();
    let (name, endpoint) = match selected {
        Selected::Catalogued(known) => (
            format!("{:?}", known.name),
            match known.endpoint {
                Some(endpoint) => format!("Some(EndpointMatch::{endpoint:?})"),
                None => "None".into(),
            },
        ),
        Selected::Custom(_) | Selected::Undetected => ("Uncatalogued".into(), "None".into()),
    };
    let certification = match date {
        Some(date) => format!(
            "{:?}",
            Certification::Passed {
                date,
                suite: SUITE_VERSION
            }
        ),
        None => "Uncertified".into(),
    };
    let versioned = if *versioning == S3BucketVersioning::Enabled {
        certification.as_str()
    } else {
        "Uncertified"
    };
    let copy_publication = match p.copy_publication {
        S3CopyPublication::Never => "S3CopyPublication::Never".into(),
        S3CopyPublication::Multipart(condition) => {
            format!("S3CopyPublication::Multipart(S3CopyCondition::{condition:?})")
        }
        S3CopyPublication::Every(condition) => {
            format!("S3CopyPublication::Every(S3CopyCondition::{condition:?})")
        }
    };
    println!(
        "CataloguedProfile {{\n    name: S3ProfileName::{name},\n    endpoint: {endpoint},\n    settings: S3Settings {{\n        signing: S3Signing::{:?},\n        single_put: S3SinglePut::{:?},\n        copy_publication: {copy_publication},\n        multipart_timestamp: S3MultipartTimestamp::{:?},\n        checksums: S3Checksums::{:?},\n        listing: S3Listing::{:?},\n        revision: S3Revision::{:?},\n        response_headers: S3ResponseHeaders::{:?},\n        request_checksums: {},\n    }},\n    key_scope: S3KeyScope::{key_scope:?},\n    versioned_buckets: Certification::{versioned},\n    certification: Certification::{certification},\n}},",
        p.signing, p.single_put, p.multipart_timestamp, p.checksums, p.listing,
        p.revision, p.response_headers, p.request_checksums,
    );
}

pub(super) async fn conformance(connected: Arc<StorageConnection<S3Storage>>, store: StoreId) {
    crate::test_utils::Conformance::new(connected.clone(), store)
        .run()
        .await
        .unwrap();
    let path = ObjectPath::slot(store, std::num::NonZeroU64::MIN);
    check_provider(connected.as_ref(), &path, b"sealed live provider check")
        .await
        .unwrap();
}

pub(super) async fn multipart(connected: Arc<StorageConnection<S3Storage>>, store: StoreId) {
    let path = ObjectPath::slot(store, std::num::NonZeroU64::MIN);
    // Cross both the lowered one-request limit and the native multipart
    // minimum part size. The adapter uses two parts: 8 MiB and one byte.
    let bytes = vec![42; 8 * 1024 * 1024 + 1];
    connected.create(&path, &bytes).await.unwrap();
    let listed = connected.list(&ObjectPrefix::all(store)).await.unwrap();
    assert_eq!(listed.len(), 1);
    let first_time = listed[0].stored_at;
    let error = connected
        .create(&path, &vec![7; bytes.len()])
        .await
        .unwrap_err();
    assert_eq!(error.failure(), StorageFailure::AlreadyExists);
    assert_eq!(connected.read(&path).await.unwrap(), bytes);
    assert_eq!(
        connected.list(&ObjectPrefix::all(store)).await.unwrap()[0].stored_at,
        first_time
    );
    connected.delete(&path).await.unwrap();
    // The deleted name passes the provider check again. That check replaces
    // its test object in one request, so it takes setup's small sealed bytes.
    check_provider(connected.as_ref(), &path, b"sealed live provider check")
        .await
        .unwrap();

    let mut session = connected
        .begin_upload(&path, bytes.len() as u64)
        .await
        .unwrap();
    let recording = session.encode().unwrap();
    let part_size = session.part_size();
    connected
        .upload_part(&mut session, &bytes[..part_size])
        .await
        .unwrap();
    assert_eq!(
        connected.status(&path).await.unwrap(),
        None,
        "unfinished multipart upload is absent from exact status"
    );
    assert_eq!(
        connected.read(&path).await.unwrap_err().failure(),
        StorageFailure::NotFound
    );
    let mut resumed = UploadSession::decode(recording.as_bytes()).unwrap();
    let copy = connected
        .provider
        .settings
        .copy_publication
        .multipart()
        .is_some();
    if copy {
        // A private staging name identifies the initiation this journal owns.
        connected.resume_upload(&mut resumed).await.unwrap();
        assert_eq!(resumed.confirmed_bytes(), 0);
    } else {
        // A shared slot cannot identify which unfinished initiation is ours.
        assert_eq!(
            connected
                .resume_upload(&mut resumed)
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::SessionIdentityUnknown
        );
        resumed = UploadSession::decode(session.encode().unwrap().as_bytes()).unwrap();
        connected.resume_upload(&mut resumed).await.unwrap();
        assert_eq!(resumed.confirmed_bytes(), part_size as u64);
    }
    let confirmed = resumed.confirmed_bytes() as usize;
    for part in bytes[confirmed..].chunks(part_size) {
        connected.upload_part(&mut resumed, part).await.unwrap();
    }
    if copy {
        // Assemble without publishing, as when the completion reply is lost.
        let SessionState::S3 { id, parts, .. } = &resumed.state else {
            panic!("S3 session");
        };
        let temporary = connected.provider.session_key(&resumed).unwrap();
        connected
            .provider
            .client
            .complete_multipart_upload()
            .bucket(&connected.provider.bucket)
            .key(&temporary)
            .upload_id(id.as_str())
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(
                        parts
                            .iter()
                            .map(|part| {
                                CompletedPart::builder()
                                    .part_number(part.number)
                                    .e_tag(&part.etag)
                                    .build()
                            })
                            .collect(),
                    ))
                    .build(),
            )
            .send()
            .await
            .map_err(s3_error)
            .unwrap();
        let before = connected
            .provider
            .client
            .head_object()
            .bucket(&connected.provider.bucket)
            .key(&temporary)
            .send()
            .await
            .map_err(s3_error)
            .unwrap();
        let assembled_at =
            crate::providers::http::storage_time(*before.last_modified().unwrap()).unwrap();
        assert!(
            connected
                .list(&ObjectPrefix::all(store))
                .await
                .unwrap()
                .is_empty(),
            "assembled temporary must remain outside published listings"
        );
        assert_eq!(connected.status(&path).await.unwrap(), None);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        connected.finish_upload(&mut resumed).await.unwrap();
        let published = connected.list(&ObjectPrefix::all(store)).await.unwrap();
        assert_eq!(published.len(), 1);
        assert!(published[0].stored_at > assembled_at);
        assert_eq!(connected.read(&path).await.unwrap(), bytes);
        assert_eq!(
            connected
                .provider
                .client
                .head_object()
                .bucket(&connected.provider.bucket)
                .key(&temporary)
                .send()
                .await
                .map_err(s3_error)
                .unwrap_err()
                .failure(),
            StorageFailure::NotFound
        );
        let mut stale = UploadSession::decode(recording.as_bytes()).unwrap();
        connected.resume_upload(&mut stale).await.unwrap();
        assert!(stale.is_complete());
    } else {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let marker = ObjectPath::floor(store);
        connected
            .create(&marker, b"before completion")
            .await
            .unwrap();
        let before = connected.status(&marker).await.unwrap().unwrap();
        connected.finish_upload(&mut resumed).await.unwrap();
        let published = connected.status(&path).await.unwrap().unwrap();
        assert!(published.stored_at >= before.stored_at, "S3 multipart Last-Modified reports initiation rather than complete publication: published={:?}, before completion={:?}", published.stored_at, before.stored_at);
    }
    connected.finish_upload(&mut resumed).await.unwrap();
}
