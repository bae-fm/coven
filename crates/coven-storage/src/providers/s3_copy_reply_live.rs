#![cfg(test)]

//! Whether a publishing copy's reply describes the object exactly as a HEAD
//! right after it does (suite 6). Each trial sends the requests the adapter's
//! copy publication sends: stage a temporary carrying the session marker, then
//! one `CopyObject` with the profile's destination condition. Only fixed
//! labels, timestamps and comparison outcomes enter output.
use super::*;
use aws_sdk_s3::operation::copy_object::CopyObjectOutput;
use coven_foundation::clock::Clock;

/// Single-request trials, spread across a second by `SPACING`.
const SINGLE_TRIALS: u64 = 20;
/// Multipart trials, each a two-part upload (8 MiB and one byte).
const MULTIPART_TRIALS: u64 = 5;
/// Between trials, so the copies' fractional times cover the whole second.
const SPACING: std::time::Duration = std::time::Duration::from_millis(50);

/// The two verdicts, kept apart: the time question, and whether every field
/// a status answer takes from HEAD came back in the copy reply as well.
struct Verdict {
    time: bool,
    metadata: bool,
    /// Replies whose time had a fraction, and those whose fraction was half
    /// a second or more. A HEAD date that rounds the copy's time differs from
    /// one that truncates it only in the upper half.
    fractional: u64,
    upper_half: u64,
}

/// Observe in a fresh store and print both verdicts; a profile without copy
/// publication has no copy reply to observe.
pub(super) async fn report(settings: &live::LiveS3, profile: S3Settings) {
    if profile.copy_publication.multipart().is_none() {
        println!("copy_reply publication=none");
        return;
    }
    let verdict = in_namespace(settings, "copy_reply", profile, observe).await;
    if let Some(verdict) = &verdict {
        println!(
            "copy_reply SUMMARY fractional={} upper_half={}",
            verdict.fractional, verdict.upper_half
        );
    }
    let shown = |held: Option<bool>| match held {
        Some(true) => "PASS",
        Some(false) => "FAIL",
        None => "FAIL (not observed)",
    };
    println!(
        "copy_reply_time_equals_status {} cleanup=PASS",
        shown(
            verdict
                .as_ref()
                .map(|verdict| verdict.time && (verdict.fractional == 0 || verdict.upper_half > 0))
        )
    );
    println!(
        "copy_reply_metadata_equals_status {} cleanup=PASS",
        shown(verdict.as_ref().map(|verdict| verdict.metadata))
    );
}

/// Each publication the profile makes by copy: single-request creates on a
/// profile publishing every object by copy (a slot, and the floor, whose
/// revision may come from the copied marker), and multipart uploads.
async fn observe(
    s: Arc<StorageConnection<S3Storage>>,
    store: StoreId,
) -> Result<Verdict, StorageError> {
    let mut verdict = Verdict {
        time: true,
        metadata: true,
        fractional: 0,
        upper_half: 0,
    };
    let settings = s.provider.settings;
    let clock = coven_foundation::clock::SystemClock;
    if let Some(condition) = settings.copy_publication.single_request() {
        for trial in 1..=SINGLE_TRIALS {
            let target = path(store, trial);
            let bytes = format!("copy reply trial={trial}").into_bytes();
            let observed = single(&s.provider, &target, &bytes, condition).await?;
            verdict.add(compare("single", trial, &observed));
            clock.sleep(SPACING).await;
        }
        let floor = ObjectPath::floor(store);
        let observed = single(&s.provider, &floor, b"copy reply floor", condition).await?;
        verdict.add(compare("floor", 1, &observed));
    }
    if let Some(condition) = settings.copy_publication.multipart() {
        for trial in 1..=MULTIPART_TRIALS {
            let target = path(store, SINGLE_TRIALS + trial);
            let observed = multipart(&s, &target, trial, condition).await?;
            verdict.add(compare("multipart", trial, &observed));
            clock.sleep(SPACING).await;
        }
    }
    Ok(verdict)
}

impl Verdict {
    fn add(&mut self, trial: Verdict) {
        self.time &= trial.time;
        self.metadata &= trial.metadata;
        self.fractional += trial.fractional;
        self.upper_half += trial.upper_half;
    }
}

/// A copy's reply and the HEAD sent after it, with the marker the copied
/// temporary carried, and whether it carried that marker as its revision too.
struct Observed {
    reply: CopyObjectOutput,
    head: HeadObjectOutput,
    token: SecretText,
    revision_marker: bool,
}

/// Stage `bytes` at the create's temporary, as `publish_by_copy` does, and copy it.
async fn single(
    s: &S3Storage,
    target: &ObjectPath,
    bytes: &[u8],
    condition: S3CopyCondition,
) -> Result<Observed, StorageError> {
    let token = SecretText::new(s.ids.new_id().to_string());
    let temporary = s.create_temporary_key(target, bytes)?;
    let mut put = s
        .client
        .put_object()
        .bucket(&s.bucket)
        .key(&temporary)
        .body(ByteStream::from(bytes.to_vec()))
        .metadata("coven-upload", token.as_str());
    if target.is_replaceable() {
        put = put.metadata("coven-revision", token.as_str());
    }
    put.send().await.map_err(s3_error)?;
    let revision_marker = target.is_replaceable();
    publish(s, &temporary, target, condition, token, revision_marker).await
}

/// Assemble a two-part upload at its session's temporary, as `finish_upload`
/// does before its copy, and copy it.
async fn multipart(
    s: &StorageConnection<S3Storage>,
    target: &ObjectPath,
    trial: u64,
    condition: S3CopyCondition,
) -> Result<Observed, StorageError> {
    let mut bytes = format!("copy reply multipart trial={trial} ")
        .repeat(1 + 8 * 1024 * 1024 / 32)
        .into_bytes();
    bytes.truncate(8 * 1024 * 1024 + 1);
    let mut session = s.begin_upload(target, bytes.len() as u64).await?;
    for part in bytes.chunks(session.part_size()) {
        s.upload_part(&mut session, part).await?;
    }
    let SessionState::S3 { token, .. } = &session.state else {
        return Err(StorageFailure::SessionMismatch.into());
    };
    let temporary = s.provider.session_key(&session)?;
    races::multipart_request(&s.provider, &session)?
        .send()
        .await
        .map_err(s3_error)?;
    // Initiation stores the session token as both markers.
    publish(
        &s.provider,
        &temporary,
        target,
        condition,
        token.clone(),
        true,
    )
    .await
}

/// The publishing copy, the HEAD right after it, then the temporary's deletion.
async fn publish(
    s: &S3Storage,
    temporary: &str,
    target: &ObjectPath,
    condition: S3CopyCondition,
    token: SecretText,
    revision_marker: bool,
) -> Result<Observed, StorageError> {
    let reply = s
        .copy(
            None,
            temporary,
            Destination::Object(target),
            condition,
            None,
        )
        .await
        .map_err(s3_error)?;
    // The request status sends: checksum mode where the profile asks for it.
    let mut request = s
        .client
        .head_object()
        .bucket(&s.bucket)
        .key(s.prefix.key(target));
    if s.settings.request_checksums {
        request = request.checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled);
    }
    let head = request.send().await.map_err(s3_error)?;
    s.delete_key(temporary).await?;
    Ok(Observed {
        reply,
        head,
        token,
        revision_marker,
    })
}

/// Every checksum field and its stated scope, as status reads them.
type Checksums<'a> = [Option<&'a str>; 6];

fn checksums_of_reply(reply: &CopyObjectOutput) -> Checksums<'_> {
    let result = reply.copy_object_result();
    [
        result
            .and_then(|r| r.checksum_type())
            .map(|kind| kind.as_str()),
        result.and_then(|r| r.checksum_crc32()),
        result.and_then(|r| r.checksum_crc32_c()),
        result.and_then(|r| r.checksum_crc64_nvme()),
        result.and_then(|r| r.checksum_sha1()),
        result.and_then(|r| r.checksum_sha256()),
    ]
}

fn checksums_of_head(head: &HeadObjectOutput) -> Checksums<'_> {
    [
        head.checksum_type().map(|kind| kind.as_str()),
        head.checksum_crc32(),
        head.checksum_crc32_c(),
        head.checksum_crc64_nvme(),
        head.checksum_sha1(),
        head.checksum_sha256(),
    ]
}

/// A native version, where "null" means none, as revisions read it.
fn version(id: Option<&str>) -> Option<&str> {
    id.filter(|id| *id != "null")
}

/// One trial's comparison, printed, with its two verdicts.
fn compare(publication: &str, trial: u64, observed: &Observed) -> Verdict {
    let Observed {
        reply,
        head,
        token,
        revision_marker,
    } = observed;
    let reply_time = reply
        .copy_object_result()
        .and_then(|result| result.last_modified());
    // HEAD's HTTP date has whole seconds; status keeps them as they are.
    let head_time = detect::time(head).ok();
    // The copy's XML time may carry a fraction. A listing's time is read at
    // whole seconds so it equals HEAD's (§20.5); the copy's is read the same way.
    let reply_seconds = reply_time.and_then(|time| {
        crate::providers::http::storage_time(aws_sdk_s3::primitives::DateTime::from_secs(
            time.secs(),
        ))
        .ok()
    });
    let time = reply_seconds.is_some() && reply_seconds == head_time;
    let etag = reply.copy_object_result().and_then(|result| result.e_tag());
    let etag_equal = etag.is_some() && etag == head.e_tag();
    let version_equal = version(reply.version_id()) == version(head.version_id());
    let checksum_equal = checksums_of_reply(reply) == checksums_of_head(head);
    // The copy carries the temporary's markers: the session marker that
    // settles a lost reply, and the revision a floor without a native version
    // takes from it.
    let marker = |name: &str| {
        head.metadata()
            .and_then(|metadata| metadata.get(name))
            .map(String::as_str)
    };
    let marker_equal = marker("coven-upload") == Some(token.as_str())
        && (!revision_marker || marker("coven-revision") == Some(token.as_str()));
    println!(
        "copy_reply publication={publication} trial={trial} reply_time={} head_time={} time_equal={time} etag_equal={etag_equal} version={} checksum={} marker_equal={marker_equal}",
        reply_time.map_or("absent".into(), |time| format!(
            "{}.{:09}",
            time.secs(),
            time.subsec_nanos()
        )),
        head_time.map_or("absent".into(), |time| format!(
            "{}.{:09}",
            time.seconds(),
            time.nanos()
        )),
        match (version(reply.version_id()), version_equal) {
            (_, false) => "differs",
            (Some(_), true) => "equal",
            (None, true) => "none",
        },
        match (checksums_of_head(head) == [None; 6], checksum_equal) {
            (_, false) => "differs",
            (true, true) => "none",
            (false, true) => "equal",
        },
    );
    let fraction = reply_time.map_or(0, |time| time.subsec_nanos());
    Verdict {
        time,
        metadata: time && etag_equal && version_equal && checksum_equal && marker_equal,
        fractional: u64::from(fraction > 0),
        upper_half: u64::from(fraction >= 500_000_000),
    }
}
