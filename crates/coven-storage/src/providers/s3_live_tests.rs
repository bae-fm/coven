//! Production detection feeds a candidate; adapter checks certify it.
//! Only fixed labels, behavior enums, timestamps and durations enter output.
use super::*;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use coven_foundation::id_source::{DeviceId, StoreId};
use profile::*;
use std::{future::Future, time::Duration};

/// Version 3 detects with the production probes, including AWS's copy
/// condition, and applies the setup safety check (§4).
pub(super) const SUITE_VERSION: u32 = 3;

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
        multipart,
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
    let cleanup = live::cleanup(&storage.provider, store).await;
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

#[tokio::test]
#[ignore = "COVEN_LIVE_S3_ENV; creates and deletes live objects and multipart uploads"]
async fn s3_certification() {
    let settings = live::LiveS3::load();
    let detection = settings.detect().await;
    let candidate = match &detection {
        Ok(detection) => {
            report(detection, &settings.config);
            let safe = detection.check(&detection.settings);
            println!(
                "§4 native_behavior_detection {}",
                if safe.is_ok() { "PASS" } else { "FAIL" }
            );
            safe.is_ok().then_some(detection.settings)
        }
        Err(error) => {
            println!("§4 native_behavior_detection cause={error}");
            println!("§4 native_behavior_detection FAIL");
            None
        }
    };
    let profile = candidate
        .or(detection.as_ref().ok().map(|detection| detection.settings))
        .unwrap_or(PLAIN);
    let mut passed = candidate.is_some();
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
    let date = aws_sdk_s3::primitives::DateTime::from(SystemTime::now())
        .fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)
        .unwrap();
    print_profile(
        profile,
        &settings.config,
        if passed { Some(&date[..10]) } else { None },
    );
    println!(
        "certification suite={SUITE_VERSION} {}",
        if passed { "PASS" } else { "FAIL" }
    );
    assert!(passed, "S3 certification failed; see §4 lines above");
}

fn print_profile(p: S3Settings, config: &StorageConfig, date: Option<&str>) {
    let StorageConfig::S3 { endpoint, .. } = config else {
        unreachable!()
    };
    // Only the hostname is source data, never a URL's credentials, path or query.
    let (name, endpoint) = match endpoint.as_ref().map(CataloguedProfile::hosting) {
        Some(Some(known)) => (
            format!("{:?}", known.name),
            format!("Some(EndpointMatch::{:?})", known.endpoint.unwrap()),
        ),
        Some(None) => ("Uncatalogued".into(), "None".into()),
        None => (
            "Aws".into(),
            "Some(EndpointMatch::HostSuffix(\".amazonaws.com\"))".into(),
        ),
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
    println!("CataloguedProfile {{\n    name: S3ProfileName::{name},\n    endpoint: {endpoint},\n    settings: {p:#?},\n    certification: Certification::{certification},\n}},");
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
    assert_eq!(resumed.confirmed_bytes(), session.part_size() as u64);
    connected
        .upload_part(&mut resumed, &bytes[session.part_size()..])
        .await
        .unwrap();
    if connected.provider.settings.multipart.uses_copy() {
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
        let assembled_at: SystemTime = (*before.last_modified().unwrap()).try_into().unwrap();
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
