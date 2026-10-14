//! S3 detection (§4): native probes against the user's bucket, under the
//! store's reserved `<store>/.coven-check/<run>/` prefix, deleted afterwards.
//! Only fixed labels, status codes and server times enter observations.
use super::*;
use crate::s3_detection::{COPY_CONDITIONS, SIGNERS, SINGLE_PUTS};
use crate::{S3Detection, S3Observation, S3Probe};
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use coven_foundation::id_source::StoreId;
use std::{
    collections::BTreeSet,
    time::{Duration, SystemTime},
};

const PROBES: &str = ".coven-check";
/// `Last-Modified` has whole seconds: the earliest a later marker can differ.
const FIRST_PAUSE: Duration = Duration::from_secs(1);
const RETRY_PAUSE: Duration = Duration::from_millis(250);
/// 10 seconds of pauses, beyond the worst listing lag measured (5.5 seconds, §4).
const RETRIES: u32 = 40;

#[path = "s3_detect_headers.rs"]
mod headers;

/// Probe the location's service with these credentials, starting from the
/// location's own settings, or the plain S3 settings where detection must
/// supply them. Cleanup runs even after a failed probe; both causes survive.
pub(crate) async fn detect(
    config: StorageConfig,
    credentials: S3Credentials,
    store: StoreId,
    clock: ClockRef,
    ids: IdSourceRef,
) -> Result<S3Detection, StorageError> {
    let StorageConfig::S3 {
        endpoint, profile, ..
    } = &config
    else {
        return Err(StorageFailure::InvalidConfiguration.with_source("expected S3 configuration"));
    };
    let base = profile::settings(endpoint.as_ref(), profile.as_ref())?.unwrap_or(profile::PLAIN);
    let run = ids.new_id();
    let mut observations = Vec::new();
    let mut chosen = None;
    let mut refusal = None;
    for signing in SIGNERS {
        let candidate = S3Storage::with_settings(
            config.clone(),
            credentials.clone(),
            clock.clone(),
            ids.clone(),
            S3Settings { signing, ..base },
        )?;
        let namespace = namespace(&candidate, store, run);
        let result = candidate
            .list_page(S3Listing::V1, &namespace, None, None, Some(0))
            .await;
        observations.push(observation(
            S3Probe::Signing(signing),
            result.is_ok(),
            outcome(&result),
        ));
        match result {
            Ok(_) if chosen.is_none() || signing == base.signing => chosen = Some(candidate),
            Ok(_) => {}
            Err(error) if signing == base.signing || refusal.is_none() => refusal = Some(error),
            Err(_) => {}
        }
    }
    let Some(storage) = chosen else {
        return Err(refusal.unwrap_or(StorageFailure::Authentication.into()));
    };
    let mut probes = Probes {
        prefix: namespace(&storage, store, run),
        s: &storage,
        clock,
        observations,
        written: BTreeSet::new(),
        uploads: Vec::new(),
    };
    let result = probes.run().await;
    let cleanup = probes.cleanup().await;
    crate::transfer::with_cleanup(result, cleanup)?;
    Ok(S3Detection::derive(probes.observations, base))
}

fn namespace(storage: &S3Storage, store: StoreId, run: uuid::Uuid) -> String {
    format!(
        "{}{PROBES}/{run}/",
        ObjectPrefix::all(store).under(&storage.prefix)
    )
}

/// Whether a listed key, relative to the location prefix, is a detection probe.
/// Probes never reach published-object listings, even after a killed process.
pub(super) fn is_probe(relative: &str, prefix: &ObjectPrefix) -> Result<bool, StorageError> {
    let directory = format!("{}{PROBES}/", ObjectPrefix::all(prefix.store()).as_str());
    let Some(rest) = relative.strip_prefix(&directory) else {
        return Ok(false);
    };
    if !relative.starts_with(prefix.as_str()) {
        return Err(StorageFailure::Protocol.with_source("S3 listed a probe outside prefix"));
    }
    let run = rest.split_once('/').map(|(run, _)| run).unwrap_or(rest);
    let parsed = uuid::Uuid::parse_str(run)
        .map_err(|source| StorageFailure::Protocol.with_source(source))?;
    if parsed.to_string() != run {
        return Err(StorageFailure::Protocol.with_source("S3 listed a malformed detection probe"));
    }
    Ok(true)
}

fn observation(probe: S3Probe, held: bool, detail: impl Into<String>) -> S3Observation {
    S3Observation {
        probe,
        held,
        detail: detail.into(),
    }
}

/// A fixed-label outcome: the classification and native status, never a body.
fn outcome<T>(result: &Result<T, StorageError>) -> String {
    match result {
        Ok(_) => "accepted".into(),
        Err(error) => match error::native_status(error) {
            Some(status) => format!("{:?} HTTP {status}", error.failure()),
            None => format!("{:?}", error.failure()),
        },
    }
}

/// The service does not implement this request or header: a refusal or 501.
fn unsupported(error: &StorageError) -> bool {
    error.failure() == StorageFailure::Refused || error::native_status(error) == Some(501)
}

/// Ok(true) for a create-once refusal, Ok(false) for an accepted write or an
/// unsupported condition; any other failure stops detection.
fn refused<T>(result: Result<T, StorageError>) -> Result<bool, StorageError> {
    match result {
        Ok(_) => Ok(false),
        Err(error) if error.failure() == StorageFailure::AlreadyExists => Ok(true),
        Err(error) if unsupported(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

fn absent<T>(result: Result<T, StorageError>) -> Result<bool, StorageError> {
    match result {
        Ok(_) => Ok(false),
        Err(error) if error.failure() == StorageFailure::NotFound => Ok(true),
        Err(error) => Err(error),
    }
}

fn seconds(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default()
}

struct Probes<'a> {
    s: &'a S3Storage,
    prefix: String,
    clock: ClockRef,
    observations: Vec<S3Observation>,
    /// Every key a probe published, for deletion and listing visibility.
    written: BTreeSet<String>,
    uploads: Vec<(String, String)>,
}

impl Probes<'_> {
    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }
    fn observe(&mut self, probe: S3Probe, held: bool, detail: impl Into<String>) {
        self.observations.push(observation(probe, held, detail));
    }

    async fn run(&mut self) -> Result<(), StorageError> {
        let v2_accepted = self.v2_accepted().await?;
        // Written before multipart initiation, so every copy after the
        // multipart pause is dated a later second than its source.
        let source = self.key("copy-source");
        self.put(&source, b"original", None).await?;
        let source_time = time(&self.head(&source).await?)?;
        self.single_puts().await?;
        let listing = if v2_accepted {
            S3Listing::V2
        } else {
            S3Listing::V1
        };
        self.multipart(listing).await?;
        self.copies(&source, source_time).await?;
        let checksum = self.key("checksum");
        self.put(&checksum, b"checksum observation", None).await?;
        let multipart = self.key("multipart");
        headers::observe(self, &[checksum, multipart]).await?;
        self.bucket_check(listing).await?;
        self.listings(v2_accepted).await
    }

    async fn v2_accepted(&mut self) -> Result<bool, StorageError> {
        // A capability probe distinguishes a refusal or HTTP 501 from transient
        // failures; normal storage operations keep their error policy.
        let result = self
            .s
            .list_page(S3Listing::V2, &self.prefix, None, None, Some(1))
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(error) if unsupported(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn single_puts(&mut self) -> Result<(), StorageError> {
        for (index, condition) in SINGLE_PUTS.into_iter().enumerate() {
            let key = self.key(&format!("single-put-{index}"));
            let first = self.put(&key, b"original", Some(condition.header())).await;
            if let Err(error) = first {
                if !unsupported(&error) {
                    return Err(error);
                }
                let detail = format!("empty={}", outcome::<()>(&Err(error)));
                self.observe(S3Probe::SinglePut(condition), false, detail);
                continue;
            }
            let before = time(&self.head(&key).await?)?;
            let taken = self.put(&key, b"replaced", Some(condition.header())).await;
            let preserved =
                self.bytes(&key).await? == b"original" && time(&self.head(&key).await?)? == before;
            let detail = format!(
                "empty=accepted taken={} preserved={preserved}",
                outcome(&taken)
            );
            let held = refused(taken)? && preserved;
            self.observe(S3Probe::SinglePut(condition), held, detail);
        }
        Ok(())
    }

    async fn multipart(&mut self, listing: S3Listing) -> Result<(), StorageError> {
        let key = self.key("multipart");
        let before_key = self.key("marker-before");
        let after_key = self.key("marker-after");
        self.put(&before_key, b"before initiation", None).await?;
        let start_before = time(&self.head(&before_key).await?)?;
        let mut upload = self.start(&key).await?;
        self.put(&after_key, b"after initiation", None).await?;
        let start_after = time(&self.head(&after_key).await?)?;
        let expected = b"multipart probe".to_vec();
        self.part(&mut upload, &expected).await?;
        let unfinished_head = self.head(&key).await;
        let unfinished_get = self.bytes(&key).await;
        let listed = self
            .s
            .list_page(listing, &self.prefix, None, None, None)
            .await?
            .contents
            .iter()
            .any(|object| object.key() == Some(key.as_str()));
        let detail = format!(
            "HEAD={} GET={} listed={listed}",
            outcome(&unfinished_head),
            outcome(&unfinished_get)
        );
        let hidden = absent(unfinished_head)? && absent(unfinished_get)? && !listed;
        self.observe(S3Probe::PartialUploadHidden, hidden, detail);
        let completion_before = self.marker_after(&before_key, start_after).await?;
        let first = self.complete(&upload, true).await;
        let empty = outcome(&first);
        let accepted_empty = first.is_ok();
        if let Err(error) = first {
            if !unsupported(&error) {
                return Err(error);
            }
            self.complete(&upload, false).await?;
        }
        self.written.insert(key.clone());
        self.put(&after_key, b"after completion", None).await?;
        let completion_after = time(&self.head(&after_key).await?)?;
        let published = self.head(&key).await?;
        let published_at = time(&published)?;
        let completion = if (completion_before..=completion_after).contains(&published_at) {
            true
        } else if (start_before..=start_after).contains(&published_at)
            && published_at < completion_before
        {
            false
        } else {
            return Err(
                StorageFailure::Protocol.with_source("multipart timestamp outside marker brackets")
            );
        };
        let detail = format!(
            "start={}..{} completion={}..{} published={}",
            seconds(start_before),
            seconds(start_after),
            seconds(completion_before),
            seconds(completion_after),
            seconds(published_at)
        );
        self.observe(S3Probe::CompletionTimestamp, completion, detail);
        let mut taken = self.start(&key).await?;
        self.part(&mut taken, b"replacement").await?;
        let result = self.complete(&taken, true).await;
        let after = self.head(&key).await?;
        let preserved = after.e_tag() == published.e_tag()
            && time(&after)? == published_at
            && self.bytes(&key).await? == expected;
        let detail = format!(
            "empty={empty} taken={} preserved={preserved}",
            outcome(&result)
        );
        let held = accepted_empty && refused(result)? && preserved;
        self.observe(S3Probe::ConditionalCompletion, held, detail);
        Ok(())
    }

    /// Rewrite a marker until the server dates it a later second than `after`.
    async fn marker_after(
        &mut self,
        key: &str,
        after: SystemTime,
    ) -> Result<SystemTime, StorageError> {
        self.clock.sleep(FIRST_PAUSE).await;
        for _ in 0..=RETRIES {
            self.put(key, b"later marker", None).await?;
            let marked = time(&self.head(key).await?)?;
            if marked > after {
                return Ok(marked);
            }
            self.clock.sleep(RETRY_PAUSE).await;
        }
        Err(StorageFailure::Protocol.with_source("server time did not advance"))
    }

    async fn copies(&mut self, source: &str, source_time: SystemTime) -> Result<(), StorageError> {
        let mut dated = None;
        for (index, condition) in COPY_CONDITIONS.into_iter().enumerate() {
            let key = self.key(&format!("copy-{index}"));
            let first = self.copy(source, &key, condition).await;
            if let Err(error) = first {
                if !unsupported(&error) {
                    return Err(error);
                }
                let detail = format!("empty={}", outcome::<()>(&Err(error)));
                self.observe(S3Probe::CopyPublication(condition), false, detail);
                continue;
            }
            let before = time(&self.head(&key).await?)?;
            dated.get_or_insert((before, condition));
            self.put(source, b"replacement", None).await?;
            let taken = self.copy(source, &key, condition).await;
            let preserved =
                self.bytes(&key).await? == b"original" && time(&self.head(&key).await?)? == before;
            let detail = format!(
                "empty=accepted taken={} preserved={preserved}",
                outcome(&taken)
            );
            let held = refused(taken)? && preserved;
            self.observe(S3Probe::CopyPublication(condition), held, detail);
            self.put(source, b"original", None).await?;
        }
        let (held, detail) = match dated {
            Some((copied, condition)) => (
                copied > source_time,
                format!(
                    "condition={condition:?} source={} copy={}",
                    seconds(source_time),
                    seconds(copied)
                ),
            ),
            None => (false, "no copy accepted".into()),
        };
        self.observe(S3Probe::CopyTimestamp, held, detail);
        Ok(())
    }

    /// As status does after a bodyless 404: a zero-entry listing at a missing
    /// object's key, inside the store's prefix. A refused listing is observed
    /// for the setup check; any other failure stops detection.
    async fn bucket_check(&mut self, listing: S3Listing) -> Result<(), StorageError> {
        let list = self
            .s
            .list_page(listing, &self.key("absent"), None, None, Some(0))
            .await;
        let detail = outcome(&list);
        match list {
            Ok(_) => self.observe(S3Probe::BucketList, true, detail),
            Err(error) if unsupported(&error) => self.observe(S3Probe::BucketList, false, detail),
            Err(error) => return Err(error),
        }
        Ok(())
    }

    /// Each accepted protocol must list every probe object, across pages
    /// forced at one key; V2 must also agree with V1.
    async fn listings(&mut self, v2_accepted: bool) -> Result<(), StorageError> {
        let v1 = self.visible(S3Listing::V1).await?;
        let detail = |listed: &Option<BTreeSet<String>>| match listed {
            Some(keys) => format!("objects={} pages_forced_at=1", keys.len()),
            None => format!("incomplete after {RETRIES} polls"),
        };
        self.observe(S3Probe::Listing(S3Listing::V1), v1.is_some(), detail(&v1));
        if !v2_accepted {
            self.observe(S3Probe::Listing(S3Listing::V2), false, "refused");
            return Ok(());
        }
        let v2 = self.visible(S3Listing::V2).await?;
        let agrees = v1.is_none() || v1 == v2;
        let held = v2.is_some() && agrees;
        let detail = format!("{} agrees_with_v1={agrees}", detail(&v2));
        self.observe(S3Probe::Listing(S3Listing::V2), held, detail);
        Ok(())
    }

    /// Poll until a listing shows every published probe, `RETRIES` pauses at most.
    async fn visible(&self, protocol: S3Listing) -> Result<Option<BTreeSet<String>>, StorageError> {
        for _ in 0..RETRIES {
            let listed = self.keys(protocol).await?;
            if listed.is_superset(&self.written) {
                return Ok(Some(listed));
            }
            self.clock.sleep(RETRY_PAUSE).await;
        }
        let listed = self.keys(protocol).await?;
        Ok(listed.is_superset(&self.written).then_some(listed))
    }

    async fn keys(&self, protocol: S3Listing) -> Result<BTreeSet<String>, StorageError> {
        let mut keys = BTreeSet::new();
        let mut marker = None;
        let mut pages = Pagination::new();
        loop {
            let page = self
                .s
                .list_page(protocol, &self.prefix, None, marker, Some(1))
                .await?;
            for object in page.contents {
                keys.insert(metadata::required(object.key(), "S3 list entry omitted key")?.into());
            }
            let Some(next) = page.next else {
                return Ok(keys);
            };
            pages.check(&next)?;
            marker = Some(next);
        }
    }

    /// Abort every started upload and delete every probe, tracked or listed.
    async fn cleanup(&mut self) -> Result<(), StorageError> {
        let mut result = Ok(());
        for (key, id) in std::mem::take(&mut self.uploads) {
            let aborted = match self
                .s
                .client
                .abort_multipart_upload()
                .bucket(&self.s.bucket)
                .key(key)
                .upload_id(id)
                .send()
                .await
            {
                Ok(_) => Ok(()),
                Err(error) if error.code() == Some("NoSuchUpload") => Ok(()),
                Err(error) => Err(s3_error(error)),
            };
            result = crate::transfer::with_cleanup(result, aborted);
        }
        let mut keys = std::mem::take(&mut self.written);
        match self.keys(S3Listing::V1).await {
            Ok(listed) => keys.extend(listed),
            Err(error) => result = crate::transfer::with_cleanup(result, Err(error)),
        }
        for key in keys {
            debug_assert!(key.starts_with(&self.prefix));
            result = crate::transfer::with_cleanup(result, self.s.delete_key(&key).await);
        }
        result
    }

    async fn head(&self, key: &str) -> Result<HeadObjectOutput, StorageError> {
        self.s
            .client
            .head_object()
            .bucket(&self.s.bucket)
            .key(key)
            .send()
            .await
            .map_err(s3_error)
    }

    async fn put(
        &mut self,
        key: &str,
        bytes: &[u8],
        condition: Option<(&'static str, &'static str)>,
    ) -> Result<(), StorageError> {
        self.s
            .client
            .put_object()
            .bucket(&self.s.bucket)
            .key(key)
            .body(ByteStream::from(bytes.to_vec()))
            .customize()
            .mutate_request(move |request| {
                if let Some((name, value)) = condition {
                    request.headers_mut().insert(name, value);
                }
            })
            .send()
            .await
            .map_err(s3_error)?;
        self.written.insert(key.into());
        Ok(())
    }

    async fn copy(
        &mut self,
        source: &str,
        key: &str,
        condition: S3CopyCondition,
    ) -> Result<(), StorageError> {
        let (name, value) = condition.header();
        self.s
            .client
            .copy_object()
            .bucket(&self.s.bucket)
            .key(key)
            .copy_source(format!("/{}/{source}", self.s.bucket))
            .customize()
            .mutate_request(move |request| {
                request.headers_mut().insert(name, value);
            })
            .send()
            .await
            .map_err(s3_error)?;
        self.written.insert(key.into());
        Ok(())
    }

    async fn bytes(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let reply = self
            .s
            .client
            .get_object()
            .bucket(&self.s.bucket)
            .key(key)
            .send()
            .await
            .map_err(s3_error)?;
        Ok(reply
            .body
            .collect()
            .await
            .map_err(|source| StorageError::Provider {
                provider: CloudProvider::S3,
                failure: StorageFailure::Network,
                source: Box::new(source),
            })?
            .into_bytes()
            .to_vec())
    }

    async fn start(&mut self, key: &str) -> Result<NativeUpload, StorageError> {
        let reply = self
            .s
            .client
            .create_multipart_upload()
            .bucket(&self.s.bucket)
            .key(key)
            .send()
            .await
            .map_err(s3_error)?;
        let id: String = metadata::required(reply.upload_id(), "S3 omitted upload id")?.into();
        self.uploads.push((key.into(), id.clone()));
        Ok(NativeUpload {
            key: key.into(),
            id,
            parts: Vec::new(),
        })
    }

    async fn part(&self, upload: &mut NativeUpload, bytes: &[u8]) -> Result<(), StorageError> {
        let number = i32::try_from(upload.parts.len() + 1)
            .map_err(|error| StorageFailure::InvalidPart.with_source(error))?;
        let reply = self
            .s
            .client
            .upload_part()
            .bucket(&self.s.bucket)
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
                .e_tag(metadata::required(reply.e_tag(), "S3 omitted part ETag")?)
                .build(),
        );
        Ok(())
    }

    async fn complete(&self, upload: &NativeUpload, conditional: bool) -> Result<(), StorageError> {
        self.s
            .client
            .complete_multipart_upload()
            .bucket(&self.s.bucket)
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
}

struct NativeUpload {
    key: String,
    id: String,
    parts: Vec<CompletedPart>,
}

pub(super) fn time(head: &HeadObjectOutput) -> Result<SystemTime, StorageError> {
    (*head
        .last_modified()
        .ok_or(StorageFailure::Protocol.with_source("S3 omitted storage time"))?)
    .try_into()
    .map_err(|error| StorageFailure::Protocol.with_source(error))
}

#[cfg(test)]
#[path = "s3_detect_tests.rs"]
mod tests;
