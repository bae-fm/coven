//! Native probes feed a candidate profile; adapter checks certify that candidate.
//! Only fixed labels, behavior enums, timestamps and durations enter output.
use super::*;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use coven_foundation::id_source::{DeviceId, StoreId};
use profile::*;
use std::{future::Future, time::Duration};

pub(super) const SUITE_VERSION: u32 = 1;

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
fn absent<T>(result: Result<T, StorageError>) -> Result<bool, StorageError> {
    match result {
        Ok(_) => Ok(false),
        Err(error) if error.failure() == StorageFailure::NotFound => Ok(true),
        Err(error) => Err(error),
    }
}
fn refused<T>(result: Result<T, StorageError>) -> Result<bool, StorageError> {
    match result {
        Ok(_) => Ok(false),
        Err(error) if error.failure() == StorageFailure::AlreadyExists => Ok(true),
        Err(error) if error.failure() == StorageFailure::Refused => Ok(false),
        Err(error) => Err(error),
    }
}
fn outcome<T>(result: &Result<T, StorageError>) -> String {
    use std::error::Error;
    let Err(error) = result else {
        return "accepted".into();
    };
    let cause = error.source().and_then(|source| source.source());
    macro_rules! status {
        ($($operation:ident :: $native:ident),*) => {$(
            if let Some(native) = cause.and_then(|cause| cause.downcast_ref::<aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::$operation::$native>>()) {
                if let Some(response) = native.raw_response() {
                    return format!("{:?} HTTP {}", error.failure(), response.status().as_u16());
                }
            }
        )*};
    }
    status!(
        put_object::PutObjectError,
        complete_multipart_upload::CompleteMultipartUploadError,
        copy_object::CopyObjectError,
        head_object::HeadObjectError,
        get_object::GetObjectError,
        list_objects::ListObjectsError
    );
    format!("{:?}", error.failure())
}
fn time(head: &HeadObjectOutput) -> Result<SystemTime, StorageError> {
    (*head.last_modified().ok_or(StorageFailure::Protocol)?)
        .try_into()
        .map_err(|e| StorageFailure::Protocol.with_source(e))
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
async fn put(
    s: &S3Storage,
    key: &str,
    bytes: &[u8],
    condition: Option<SinglePut>,
) -> Result<(), StorageError> {
    s.client
        .put_object()
        .bucket(&s.bucket)
        .key(key)
        .body(ByteStream::from(bytes.to_vec()))
        .customize()
        .mutate_request(move |request| {
            if let Some(condition) = condition {
                let (name, value) = condition.header();
                request.headers_mut().insert(name, value);
            }
        })
        .send()
        .await
        .map_err(s3_error)?;
    Ok(())
}
async fn bytes(s: &S3Storage, key: &str) -> Result<Vec<u8>, StorageError> {
    let reply = s
        .client
        .get_object()
        .bucket(&s.bucket)
        .key(key)
        .send()
        .await
        .map_err(s3_error)?;
    Ok(reply
        .body
        .collect()
        .await
        .map_err(|_| StorageFailure::Network)?
        .into_bytes()
        .to_vec())
}
struct NativeUpload {
    key: String,
    id: String,
    parts: Vec<CompletedPart>,
}
async fn start(s: &S3Storage, key: String) -> Result<NativeUpload, StorageError> {
    let reply = s
        .client
        .create_multipart_upload()
        .bucket(&s.bucket)
        .key(&key)
        .metadata("coven-upload", "certification")
        .send()
        .await
        .map_err(s3_error)?;
    Ok(NativeUpload {
        key,
        id: metadata::required(reply.upload_id(), "missing upload id")?.into(),
        parts: Vec::new(),
    })
}
async fn part(s: &S3Storage, upload: &mut NativeUpload, bytes: &[u8]) -> Result<(), StorageError> {
    let number = i32::try_from(upload.parts.len() + 1).unwrap();
    let reply = s
        .client
        .upload_part()
        .bucket(&s.bucket)
        .key(&upload.key)
        .upload_id(&upload.id)
        .part_number(number)
        .body(ByteStream::from(bytes.to_vec()))
        .send()
        .await
        .map_err(s3_error)?;
    upload.parts.push(
        CompletedPart::builder()
            .part_number(number)
            .e_tag(metadata::required(reply.e_tag(), "missing part etag")?)
            .build(),
    );
    Ok(())
}
async fn complete(
    s: &S3Storage,
    upload: &NativeUpload,
    conditional: bool,
) -> Result<(), StorageError> {
    s.client
        .complete_multipart_upload()
        .bucket(&s.bucket)
        .key(&upload.key)
        .upload_id(&upload.id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(upload.parts.clone()))
                .build(),
        )
        .set_if_none_match(conditional.then(|| "*".into()))
        .send()
        .await
        .map_err(s3_error)?;
    Ok(())
}
async fn keys(
    s: &S3Storage,
    protocol: Listing,
    prefix: &str,
    delimiter: Option<&str>,
) -> Result<(Vec<String>, Vec<String>), StorageError> {
    let mut objects = Vec::new();
    let mut folders = Vec::new();
    let mut marker = None;
    let mut pages = Pagination::new();
    loop {
        let page = s
            .list_page(protocol, prefix, delimiter, marker, Some(1))
            .await?;
        for object in page.contents {
            objects.push(metadata::required(object.key(), "missing key")?.into());
        }
        for folder in page.folders {
            folders.push(metadata::required(folder.prefix(), "missing folder")?.into());
        }
        let Some(next) = page.next else {
            break;
        };
        pages.check(&next)?;
        marker = Some(next);
    }
    Ok((objects, folders))
}

async fn single_put(s: &S3Storage, store: StoreId) -> Result<SinglePut, StorageError> {
    let mut detected = None;
    for (i, condition) in [SinglePut::IfNoneMatch, SinglePut::GenerationMatch]
        .into_iter()
        .enumerate()
    {
        let key = path(store, 10 + i as u64).under(&s.prefix);
        let first = put(s, &key, b"original", Some(condition)).await;
        if let Err(error) = &first {
            if error.failure() != StorageFailure::Refused {
                first?;
                unreachable!();
            }
            println!("native single_put {condition:?} empty={}", outcome(&first));
            continue;
        }
        let before = head(s, &key).await?;
        let taken = put(s, &key, b"replaced", Some(condition)).await;
        let preserved =
            bytes(s, &key).await? == b"original" && time(&head(s, &key).await?)? == time(&before)?;
        println!(
            "native single_put {condition:?} empty=accepted taken={} preserved={preserved}",
            outcome(&taken)
        );
        if refused(taken)? && preserved && detected.is_none() {
            detected = Some(condition);
        }
    }
    detected.ok_or(StorageFailure::Protocol.with_source("no create-once PUT behavior passed"))
}
async fn multipart_native(
    s: &S3Storage,
    store: StoreId,
    listing: Listing,
) -> Result<(bool, MultipartTimestamp), StorageError> {
    let key = path(store, 20).under(&s.prefix);
    let before_key = path(store, 21).under(&s.prefix);
    let after_key = path(store, 22).under(&s.prefix);
    put(s, &before_key, b"before initiation", None).await?;
    let start_before = time(&head(s, &before_key).await?)?;
    let mut upload = start(s, key.clone()).await?;
    put(s, &after_key, b"after initiation", None).await?;
    let start_after = time(&head(s, &after_key).await?)?;
    let mut expected = vec![42; 8 * 1024 * 1024];
    part(s, &mut upload, &expected).await?;
    expected.push(b'z');
    let unfinished_head = head(s, &key).await;
    let unfinished_get = bytes(s, &key).await;
    let listed = keys(s, listing, &ObjectPrefix::all(store).under(&s.prefix), None)
        .await?
        .0
        .contains(&key);
    println!(
        "native partial_upload HEAD={} GET={} listed={listed}",
        outcome(&unfinished_head),
        outcome(&unfinished_get)
    );
    let head_absent = absent(unfinished_head)?;
    let get_absent = absent(unfinished_get)?;
    let hidden = head_absent && get_absent && !listed;
    println!(
        "§4 native partial_uploads_hidden {}",
        if hidden { "PASS" } else { "FAIL" }
    );
    require(hidden, "partial upload is visible")?;
    part(s, &mut upload, b"z").await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    put(s, &before_key, b"before completion", None).await?;
    let completion_before = time(&head(s, &before_key).await?)?;
    let first = complete(s, &upload, true).await;
    let accepted_empty = first.is_ok();
    println!("native conditional_completion empty={}", outcome(&first));
    if let Err(error) = first {
        if error.failure() != StorageFailure::Refused {
            return Err(error);
        }
        complete(s, &upload, false).await?;
    }
    put(s, &after_key, b"after completion", None).await?;
    let completion_after = time(&head(s, &after_key).await?)?;
    let published = time(&head(s, &key).await?)?;
    let timestamp = if published >= completion_before && published <= completion_after {
        MultipartTimestamp::Completion
    } else if published >= start_before && published <= start_after && published < completion_before
    {
        MultipartTimestamp::Start
    } else {
        return Err(
            StorageFailure::Protocol.with_source("multipart timestamp outside marker brackets")
        );
    };
    println!("native multipart_timestamp={timestamp:?} start_before={start_before:?} start_after={start_after:?} completion_before={completion_before:?} completion_after={completion_after:?} published={published:?}");
    let before = head(s, &key).await?;
    let mut taken = start(s, key.clone()).await?;
    part(s, &mut taken, b"replacement").await?;
    let result = complete(s, &taken, true).await;
    let preserved = head(s, &key).await?.e_tag() == before.e_tag()
        && time(&head(s, &key).await?)? == time(&before)?
        && bytes(s, &key).await? == expected;
    let result_label = outcome(&result);
    let condition_refused = refused(result)?;
    let conditional = accepted_empty && condition_refused && preserved;
    println!("native conditional_completion taken={result_label} preserved={preserved} enforced={conditional}");
    Ok((conditional, timestamp))
}
async fn copy_condition(s: &S3Storage, store: StoreId) -> Result<Multipart, StorageError> {
    let source = path(store, 30).under(&s.prefix);
    put(s, &source, b"original", None).await?;
    let source_time = time(&head(s, &source).await?)?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut detected = None;
    for (index, (header, value)) in [
        ("x-goog-if-generation-match", "0"),
        ("cf-copy-destination-if-none-match", "*"),
    ]
    .into_iter()
    .enumerate()
    {
        let key = path(store, 31 + index as u64).under(&s.prefix);
        let copy = || {
            s.client
                .copy_object()
                .bucket(&s.bucket)
                .key(&key)
                .copy_source(format!("/{}/{source}", s.bucket))
                .customize()
                .mutate_request(move |request| {
                    request.headers_mut().insert(header, value);
                })
                .send()
        };
        let first = copy().await.map_err(s3_error);
        if let Err(error) = &first {
            if error.failure() != StorageFailure::Refused {
                first?;
                unreachable!();
            }
            println!("native copy header={header} empty={}", outcome(&first));
            continue;
        }
        let before = head(s, &key).await?;
        let publication_time = time(&before)? > source_time;
        put(s, &source, b"replacement", None).await?;
        let second = copy().await.map_err(s3_error);
        let preserved =
            bytes(s, &key).await? == b"original" && time(&head(s, &key).await?)? == time(&before)?;
        println!("native copy header={header} empty=accepted taken={} preserved={preserved} later_than_source={publication_time}", outcome(&second));
        if refused(second)? && preserved && publication_time && detected.is_none() {
            detected = Some(Multipart::Copy { header, value });
        }
        put(s, &source, b"original", None).await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    detected.ok_or(StorageFailure::Protocol.with_source("no conditional copy behavior passed"))
}

#[path = "s3_checksum_headers_tests.rs"]
mod headers;

async fn detect(
    s: Arc<StorageConnection<S3Storage>>,
    store: StoreId,
) -> Result<S3Profile, StorageError> {
    let s = &s.provider;
    let mut profile = s.profile;
    profile.certification = Certification::Uncertified;
    let prefix = ObjectPrefix::all(store).under(&s.prefix);
    profile.listing = native_listing(s, &prefix).await?;
    profile.single_put = single_put(s, store).await?;
    let (conditional, timestamp) = multipart_native(s, store, profile.listing).await?;
    profile.multipart_timestamp = timestamp;
    profile.multipart = if conditional && timestamp == MultipartTimestamp::Completion {
        Multipart::ConditionalCompletion
    } else {
        copy_condition(s, store).await?
    };
    let prefix = ObjectPrefix::all(store).under(&s.prefix);
    let v1 = keys(s, Listing::V1, &prefix, None).await?;
    if profile.listing == Listing::V2 {
        let v2 = keys(s, Listing::V2, &prefix, None).await?;
        require(v1 == v2, "V2 differs from V1")?;
    }
    println!(
        "native listing={:?} objects={} pages_forced_at=1",
        profile.listing,
        v1.0.len()
    );
    headers::detect(s, store, &mut profile).await?;
    Ok(profile)
}

/// Each independent case gets its own namespace. Unwinding still reaches cleanup.
async fn case<T: Send + 'static, F, Fut>(
    settings: &live::LiveS3,
    requirements: &[&str],
    profile: S3Profile,
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
        let stored_at = time(&head(&s.provider, &target.under(&s.provider.prefix)).await?)?;
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
                || async {
                    Ok(
                        keys(&s.provider, s.provider.profile.listing, &full, Some("/"))
                            .await?
                            .1
                            .contains(&folder),
                    )
                }
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
    let configured = settings.connect(None).provider.profile;
    // Probe authentication with read-only requests before creating any object.
    let mut authenticated = None;
    for signing in [Signing::GoogleHmac, Signing::AwsSigV4] {
        let mut candidate = configured;
        candidate.signing = signing;
        let connection = settings.connect(Some(candidate));
        let prefix =
            ObjectPrefix::all(StoreId(uuid::Uuid::new_v4())).under(&connection.provider.prefix);
        let result = connection
            .provider
            .list_page(Listing::V1, &prefix, None, None, Some(0))
            .await;
        println!("native signing={signing:?} result={}", outcome(&result));
        if result.is_ok() && authenticated.is_none() {
            authenticated = Some(candidate);
        }
    }
    let authenticated =
        authenticated.expect("no implemented signer accepted the supplied credentials");
    let candidate = case(
        &settings,
        &["native_behavior_detection"],
        authenticated,
        detect,
    )
    .await;
    let profile = candidate.unwrap_or(authenticated);
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
    if let Some(profile) = candidate {
        let date = aws_sdk_s3::primitives::DateTime::from(SystemTime::now())
            .fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)
            .unwrap();
        print_profile(
            profile,
            &settings.config,
            if passed { Some(&date[..10]) } else { None },
        );
    }
    println!(
        "certification suite={SUITE_VERSION} {}",
        if passed { "PASS" } else { "FAIL" }
    );
    assert!(passed, "S3 certification failed; see §4 lines above");
}

fn print_profile(p: S3Profile, config: &StorageConfig, date: Option<&str>) {
    let StorageConfig::S3 { endpoint, .. } = config else {
        unreachable!()
    };
    // Only the hostname is source data, never a URL's credentials, path or query.
    let (name, endpoint) = match endpoint.as_ref().and_then(url::Url::host_str) {
        Some(host) if !p.endpoint.matches(host) => ("Uncatalogued", format!("Host({host:?})")),
        _ => (p.name, format!("{:?}", p.endpoint)),
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
    println!("S3Profile {{\n    name: {name:?},\n    endpoint: EndpointMatch::{endpoint},\n    signing: Signing::{:?},\n    single_put: SinglePut::{:?},\n    multipart: Multipart::{:?},\n    multipart_timestamp: MultipartTimestamp::{:?},\n    checksums: Checksums::{:?},\n    listing: Listing::{:?},\n    revision: Revision::{:?},\n    bucket_check: BucketCheck::{:?},\n    response_headers: ResponseHeaders::{:?},\n    request_checksums: {},\n    certification: Certification::{certification},\n}},", p.signing, p.single_put, p.multipart, p.multipart_timestamp, p.checksums, p.listing, p.revision, p.bucket_check, p.response_headers, p.request_checksums);
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
    if connected.provider.profile.multipart.uses_copy() {
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

async fn native_listing(s: &S3Storage, prefix: &str) -> Result<Listing, StorageError> {
    use aws_sdk_s3::{error::SdkError, operation::list_objects_v2::ListObjectsV2Error};
    use std::error::Error;
    match s.list_page(Listing::V2, prefix, None, None, Some(1)).await {
        Ok(_) => Ok(Listing::V2),
        Err(error) => {
            // A capability probe distinguishes HTTP 501 from transient 5xx
            // failures; normal storage operations keep their error policy.
            let unimplemented = error
                .source()
                .and_then(|source| source.source())
                .and_then(|source| source.downcast_ref::<SdkError<ListObjectsV2Error>>())
                .and_then(|native| native.raw_response())
                .is_some_and(|response| response.status().as_u16() == 501);
            if error.failure() == StorageFailure::Refused || unimplemented {
                Ok(Listing::V1)
            } else {
                Err(error)
            }
        }
    }
}

#[tokio::test]
async fn certification_detects_v1_when_v2_is_not_implemented() {
    let server = TestServer::new(
        Router::new()
            .fallback(|| async { response(501, "<Error><Code>NotImplemented</Code></Error>") }),
    )
    .await;
    assert_eq!(
        native_listing(&provider(&server.url).provider, "store/")
            .await
            .unwrap(),
        Listing::V1
    );
}

#[tokio::test]
async fn certification_does_not_treat_a_listing_outage_as_v1_only() {
    let server =
        TestServer::new(Router::new().fallback(|| async {
            response(503, "<Error><Code>ServiceUnavailable</Code></Error>")
        }))
        .await;
    assert_eq!(
        native_listing(&provider(&server.url).provider, "store/")
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Network
    );
}
