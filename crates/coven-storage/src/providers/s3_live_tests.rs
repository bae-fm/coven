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
pub(super) const SUITE_VERSION: u32 = 5;

#[path = "s3_race.rs"]
mod races;

fn path(store: StoreId, number: u64) -> ObjectPath {
    ObjectPath::device_log(store, DeviceId(91), number.try_into().unwrap())
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
async fn folders(s: &S3Storage, prefix: &str) -> Result<Vec<String>, StorageError> {
    let mut folders = Vec::new();
    let mut marker = None;
    let mut pages = Pagination::new();
    loop {
        let page = s
            .list_page(s.settings.listing, prefix, Some("/"), marker, Some(1))
            .await?;
        for folder in page.folders {
            folders.push(metadata::required(folder.prefix(), "missing folder")?.into());
        }
        let Some(next) = page.next else {
            return Ok(folders);
        };
        pages.check(&next)?;
        marker = Some(next);
    }
}

/// Print every observation, the derived settings, the setup safety verdict
/// for an uncatalogued location, and the catalogued entries they reproduce.
fn report(detection: &crate::S3Detection, config: &StorageConfig) {
    match &detection.versioning {
        Ok(state) => println!("bucket versioning={state:?}"),
        Err(error) => println!("bucket versioning=Unknown cause={error}"),
    }
    for observation in &detection.observations {
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

/// Three installations' setups of one store claim its creation marker at once
/// (§10, D10), each through its own connection: one claim wins and the others
/// are refused, in each of 20 fresh stores.
#[tokio::test]
#[ignore = "COVEN_LIVE_S3_ENV; creates and deletes live creation markers"]
async fn s3_creation_marker_race() {
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
            &["creation_marker_race"],
            selected,
            |s, store| async move {
                let [second, third] = rivals;
                crate::test_utils::Conformance::new(s.clone(), store)
                    .creation_race([s, second, third])
                    .await
            },
        )
        .await;
        assert!(raced.is_some(), "creation marker race failed");
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
async fn case<T: Send + 'static, F, Fut>(
    settings: &live::LiveS3,
    requirements: &[&str],
    profile: S3Settings,
    work: F,
) -> Option<T>
where
    F: FnOnce(Arc<StorageConnection<S3Storage>>, StoreId) -> Fut,
    Fut: Future<Output = Result<T, StorageError>> + Send + 'static,
{
    let label = requirements.join(", ");
    let store = StoreId(uuid::Uuid::new_v4());
    let storage = Arc::new(settings.connect(Some(profile)));
    let result = tokio::spawn(work(storage.clone(), store)).await;
    let cleanup = live_cleanup::cleanup(&storage.provider, store).await;
    let value = match result {
        Ok(Ok(value)) => Some(value),
        Ok(Err(error)) => {
            println!("§4 {label} cause={error}");
            None
        }
        Err(_) => {
            println!("§4 {label} assertion_failed");
            None
        }
    };
    if let Err(error) = cleanup {
        println!("§4 {label} cleanup FAIL cause={error}");
        panic!("certification could not delete its live objects/uploads");
    }
    for requirement in requirements {
        println!(
            "§4 {requirement} {} cleanup=PASS",
            if value.is_some() { "PASS" } else { "FAIL" }
        );
    }
    value
}
async fn publication(
    s: Arc<StorageConnection<S3Storage>>,
    store: StoreId,
) -> Result<(), StorageError> {
    let marker = ObjectPath::clock(store, DeviceId(91));
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
        s.replace(&marker, b"before").await?;
        let before = s.status(&marker).await?.ok_or(StorageFailure::NotFound)?;
        if let Some(upload) = &mut pending {
            s.finish_upload(upload).await?;
        } else {
            s.create(&target, b"data").await?;
        }
        s.replace(&marker, b"after").await?;
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
    for target in [
        ObjectPath::positions(store, DeviceId(91)),
        ObjectPath::clock(store, DeviceId(91)),
    ] {
        s.replace(&target, b"first").await?;
        let mut previous = s.status(&target).await?.ok_or(StorageFailure::NotFound)?;
        for data in [b"other", b"first"] {
            s.replace(&target, data).await?;
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
        let target = ObjectPath::store_log(store, DeviceId(trial), std::num::NonZeroU64::MIN);
        s.create(&target, b"listing visibility").await?;
        let replied = Instant::now();
        let stored_at = detect::time(&head(&s.provider, &target.under(&s.provider.prefix)).await?)?;
        let all = ObjectPrefix::store_logs(store);
        let full = all.under(&s.provider.prefix);
        let folder = ObjectPrefix::store_log(store, DeviceId(trial)).under(&s.provider.prefix);
        let (object, discovery) = tokio::join!(
            observe("S3_object", trial, replied, stored_at, || async {
                Ok(s.list(&all)
                    .await?
                    .iter()
                    .any(|object| object.path == target))
            }),
            observe(
                "S3_new_device_folder",
                trial,
                replied,
                stored_at,
                || async { Ok(folders(&s.provider, &full).await?.contains(&folder)) }
            ),
        );
        report("S3_new_device_folder", &[discovery]);
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
    use races::{fire, judge, prepare, single, Race, Reply, Write};
    let trials = if width == 8 { 50 } else { 200 };
    let mut overlapping = 0;
    let mut violations = 0;
    let mut successes = 0;
    for trial in 1..=trials {
        let target = if matches!(race, Race::Replace) {
            ObjectPath::positions(store, DeviceId(trial))
        } else {
            path(store, trial)
        };
        let replacement = if matches!(race, Race::Replace) {
            s.replace(&target, b"initial revision").await?;
            let head = head(&s.provider, &target.under(&s.provider.prefix)).await?;
            Some(match s.provider.settings.revision {
                S3Revision::ETag => (
                    "if-match",
                    metadata::required(head.e_tag(), "missing current ETag")?.to_owned(),
                ),
                S3Revision::Generation => (
                    "x-goog-if-generation-match",
                    metadata::required(head.version_id(), "missing current generation")?.to_owned(),
                ),
            })
        } else {
            None
        };
        let payloads: Vec<_> = (0..width)
            .map(|writer| {
                let mut bytes = format!("trial={trial} writer={writer} ")
                    .repeat(4096)
                    .into_bytes();
                bytes.truncate(64 * 1024);
                bytes
            })
            .collect();
        let mut prepared = Vec::new();
        // Each writer publishes as the profile does: a single-request create
        // is a copy too where the profile publishes every object by copy.
        for (writer, bytes) in payloads.iter().enumerate() {
            prepared.push(match &replacement {
                Some((header, value)) => Write::Put {
                    header,
                    value: value.clone(),
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
        // Even a failed worker must not let cleanup race the other requests.
        let mut replies = joined
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| StorageFailure::Protocol.with_source(error))?;
        replies.sort_by_key(|reply| reply.writer);
        let overlap = replies.iter().map(|reply| reply.start).max().unwrap()
            < replies.iter().map(|reply| reply.end).min().unwrap();
        overlapping += usize::from(overlap);
        let accepted = replies.iter().filter(|reply| reply.result.is_ok()).count();
        successes += accepted;
        let stored = s.read(&target).await?;
        let passed = judge(&replies, &payloads, &stored);
        violations += usize::from(!passed);
        for reply in &replies {
            println!("race {race:?} writers={width} trial={trial} writer={} start_us={} end_us={} outcome={}",
                reply.writer, reply.start.as_micros(), reply.end.as_micros(), detect::outcome(&reply.result));
        }
        println!("race {race:?} writers={width} trial={trial} overlap={overlap} successes={accepted} pass={passed}");
    }
    println!("race SUMMARY {race:?} writers={width} trials={trials} overlapping={overlapping} successes={successes} violations={violations}");
    require(
        violations == 0,
        "concurrent conditional publication violated exclusivity, bytes or overlap",
    )
}

#[tokio::test]
#[ignore = "COVEN_LIVE_S3_ENV; creates and deletes live objects and multipart uploads"]
async fn s3_certification() {
    let settings = live::LiveS3::load();
    let detection = match settings.detect().await {
        Ok(detection) => detection,
        Err(error) => {
            println!("§4 native_behavior_detection cause={error}");
            println!("§4 native_behavior_detection FAIL");
            panic!("S3 certification could not detect the service");
        }
    };
    report(&detection, &settings.config);
    // A named profile is certified with its own settings, which the safety
    // check below compares with the observations; otherwise the detected ones.
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
        "§4 native_behavior_detection {}",
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
    passed &= case(
        &settings,
        &["listing_lag", "immediate_device_folders"],
        profile,
        listing_lag,
    )
    .await
    .is_some();
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
    assert!(passed, "S3 certification failed; see §4 lines above");
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
    let path = ObjectPath::device_log(
        store,
        coven_foundation::id_source::DeviceId(41),
        std::num::NonZeroU64::MIN,
    );
    check_provider(connected.as_ref(), &path, b"sealed live provider check")
        .await
        .unwrap();
}

pub(super) async fn multipart(connected: Arc<StorageConnection<S3Storage>>, store: StoreId) {
    let path = ObjectPath::device_log(
        store,
        coven_foundation::id_source::DeviceId(41),
        std::num::NonZeroU64::MIN,
    );
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
    check_provider(connected.as_ref(), &path, &bytes)
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
    connected.resume_upload(&mut resumed).await.unwrap();
    let copy = connected
        .provider
        .settings
        .copy_publication
        .multipart()
        .is_some();
    // Every session is recorded before its upload exists, so a recording
    // older than its first part aborts that upload and starts over (§4).
    assert_eq!(resumed.confirmed_bytes(), 0);
    for part in bytes.chunks(part_size) {
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
        let marker = ObjectPath::clock(store, coven_foundation::id_source::DeviceId(41));
        connected
            .replace(&marker, b"before completion")
            .await
            .unwrap();
        let before = connected.status(&marker).await.unwrap().unwrap();
        connected.finish_upload(&mut resumed).await.unwrap();
        let published = connected.status(&path).await.unwrap().unwrap();
        assert!(published.stored_at >= before.stored_at, "S3 multipart Last-Modified reports initiation rather than complete publication: published={:?}, before completion={:?}", published.stored_at, before.stored_at);
    }
    connected.finish_upload(&mut resumed).await.unwrap();
}
