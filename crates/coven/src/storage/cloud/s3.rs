//! S3-backed `CloudHome` implementation.
//!
//! Wraps `aws-sdk-s3` to provide raw storage operations against any
//! S3-compatible endpoint.

use async_trait::async_trait;
use aws_config::stalled_stream_protection::StalledStreamProtectionConfig;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::config::ResponseChecksumValidation;
use aws_sdk_s3::Client;
use tracing::warn;

use super::s3_common::{
    apply_prefix, is_not_found_code, normalize_prefix, probe_error, strip_listed_key_prefix,
};
use super::{
    range_header, CloudAccessGrant, CloudAccessRevoke, CloudHome, CloudHomeError,
    CloudHomeJoinInfo, CloudObjectState, CloudObjectVersion, ConditionalDelete,
};

/// A coven-owned tokio runtime whose worker threads have a large stack, used to
/// run every aws-sdk call.
///
/// The reason it exists: aws-sdk-s3's endpoint resolver
/// (`DefaultResolver::resolve_endpoint`, a giant generated function) descends
/// deep *synchronously in one poll*. When a `CloudHome` S3 call is awaited on a
/// foreign UI executor thread (Swift's cooperative pool / Kotlin's dispatcher,
/// ~0.5 MiB stack) the descent overflows that stack → SIGBUS. Spawning every aws
/// interaction onto a worker of this runtime guarantees the descent always runs
/// on a big stack, no matter who awaits the `CloudHome` method — so the host no
/// longer has to know which calls are "deep" and hand-wrap them.
fn s3_runtime() -> &'static tokio::runtime::Runtime {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(16 * 1024 * 1024) // aws endpoint resolver needs >> a UI thread's ~0.5 MiB
            .thread_name("coven-s3")
            .enable_all() // io + time: the aws connector/sleep need both
            .build()
            .expect("build coven S3 runtime")
    })
}

/// Run an S3 interaction on the big-stack runtime, flattening the `JoinError`.
/// The future must be `Send + 'static` (owned args, a cloned `Client`).
async fn on_s3_rt<T: Send + 'static>(
    fut: impl std::future::Future<Output = Result<T, CloudHomeError>> + Send + 'static,
) -> Result<T, CloudHomeError> {
    match s3_runtime().spawn(fut).await {
        Ok(r) => r,
        Err(e) => Err(CloudHomeError::Storage(format!("S3 task aborted: {e}"))),
    }
}

/// S3-backed cloud home.
pub struct S3CloudHome {
    client: Client,
    bucket: String,
    region: String,
    endpoint: Option<String>,
    access_key: String,
    secret_key: String,
    key_prefix: Option<String>,
}

impl S3CloudHome {
    pub async fn new(
        bucket: String,
        region: String,
        endpoint: Option<String>,
        access_key: String,
        secret_key: String,
        key_prefix: Option<String>,
    ) -> Result<Self, CloudHomeError> {
        let credentials =
            Credentials::new(&access_key, &secret_key, None, None, "coven-cloud-home");

        // aws-config has default-features disabled, so the SDK won't auto-bundle
        // an HTTP client. Plug in the rustls-ring smithy client explicitly.
        let http_client = aws_smithy_http_client::Builder::new()
            .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
            ))
            .build_https();

        let mut builder = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region.clone()))
            .credentials_provider(credentials)
            .http_client(http_client)
            // The SDK default stalled-stream protection aborts any body
            // transfer that stays under 1 B/s for 5 seconds — on slow or
            // briefly-stalling links that kills large legitimate downloads
            // (a pinned release's full-object GETs) with "minimum throughput
            // was specified at 1 B/s, but throughput of 0 B/s was observed".
            // Keep the protection (a truly dead stream should still error;
            // uploads retry from the durable outbox) but give real-world
            // stalls a 60-second grace window.
            .stalled_stream_protection(
                StalledStreamProtectionConfig::enabled()
                    .grace_period(std::time::Duration::from_secs(60))
                    .build(),
            );

        if let Some(ref ep) = endpoint {
            builder = builder.endpoint_url(ep.trim_end_matches('/'));
        }

        let aws_config = builder.load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
            .force_path_style(true)
            // Coven's S3 backend is intentionally S3-compatible, not AWS-only.
            //
            // The AWS SDK default is ResponseChecksumValidation::WhenSupported.
            // For GetObject that default mutates the request to checksum-mode=ENABLED,
            // then validates any returned x-amz-checksum-* header against the response
            // body. That is correct for AWS S3's modeled checksum behavior, but it is
            // not a portable integrity layer for S3-compatible providers.
            //
            // Google Cloud Storage's S3-compatible API returns
            // x-amz-checksum-crc32c on ranged GetObject responses with the checksum of
            // the whole object. A Range: bytes=0-23 response legitimately contains only
            // those 24 bytes, so validating that partial body against the full-object
            // checksum fails with a checksum mismatch before playback can read the
            // encrypted nonce header.
            //
            // Do not use provider checksum headers as coven's generic byte-integrity
            // contract. Managed encrypted blobs are authenticated by their AEAD tags
            // during decrypt; plaintext cloud integrity needs coven-owned metadata or
            // chunk hashes, not provider-specific response-header semantics.
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .build();
        let client = Client::from_conf(s3_config);

        Ok(S3CloudHome {
            client,
            bucket,
            region,
            endpoint,
            access_key,
            secret_key,
            // Normalize once here (trim trailing slash, drop empty), so neither
            // full_key nor list re-trims — the same normalization the wasm backend
            // does, now shared.
            key_prefix: normalize_prefix(key_prefix),
        })
    }

    /// Prepend the key prefix (if configured) to produce the full S3 object key.
    fn full_key(&self, key: &str) -> String {
        apply_prefix(self.key_prefix.as_deref(), key)
    }
}

/// A [`PartSink`] over an open S3 multipart upload: each `send_part` is one
/// `upload_part` whose ETag is kept, and `finish` is `complete_multipart_upload`.
/// On any part or completion failure the upload is aborted (best-effort) so the
/// bucket isn't left holding orphaned parts that accrue storage charges.
struct S3PartSink {
    client: Client,
    bucket: String,
    /// The prefixed object key (also used in error messages).
    key: String,
    upload_id: String,
    completed: Vec<aws_sdk_s3::types::CompletedPart>,
    next_part_number: i32,
}

impl S3PartSink {
    /// Best-effort abort so a failed upload leaves no orphaned parts.
    async fn abort(&self) {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let upload_id = self.upload_id.clone();
        let result = on_s3_rt(async move {
            client
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await
                .map_err(|e| CloudHomeError::Storage(format!("{e}")))?;
            Ok(())
        })
        .await;
        if let Err(e) = result {
            warn!("Failed to abort multipart upload for {}: {e}", self.key);
        }
    }
}

#[async_trait]
impl super::PartSink for S3PartSink {
    fn part_size(&self) -> usize {
        MULTIPART_PART_SIZE
    }

    async fn send_part(
        &mut self,
        part: bytes::Bytes,
        _offset: u64,
        _is_last: bool,
    ) -> Result<(), CloudHomeError> {
        let part_number = self.next_part_number;
        self.next_part_number += 1;
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let upload_id = self.upload_id.clone();
        let result = on_s3_rt(async move {
            client
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(part.into())
                .send()
                .await
                .map_err(|e| {
                    CloudHomeError::Storage(format!("multipart part {part_number} {key}: {e}"))
                })
        })
        .await;
        match result {
            Ok(p) => {
                self.completed.push(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(part_number)
                        .set_e_tag(p.e_tag().map(str::to_string))
                        .build(),
                );
                Ok(())
            }
            Err(e) => {
                self.abort().await;
                Err(e)
            }
        }
    }

    async fn finish(mut self: Box<Self>) -> Result<(), CloudHomeError> {
        let completed_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(std::mem::take(&mut self.completed)))
            .build();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let upload_id = self.upload_id.clone();
        let result = on_s3_rt(async move {
            client
                .complete_multipart_upload()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .multipart_upload(completed_upload)
                .send()
                .await
                .map_err(|e| CloudHomeError::Storage(format!("multipart complete {key}: {e}")))?;
            Ok(())
        })
        .await;
        if let Err(e) = result {
            self.abort().await;
            return Err(e);
        }
        Ok(())
    }
}

/// Files at or below this size go up as a single PUT; larger files use a
/// multipart upload so progress advances per part. The threshold equals the
/// part size, so the smallest multipart upload is two parts.
const MULTIPART_THRESHOLD: usize = 8 * 1024 * 1024;

/// Multipart part size. S3 requires every part except the last to be at least
/// 5 MiB; 8 MiB keeps the part count (and request count) reasonable for large
/// audio files while still giving several progress ticks.
const MULTIPART_PART_SIZE: usize = 8 * 1024 * 1024;

fn body_read_error<E>(context: &str, key: &str, err: E) -> CloudHomeError
where
    E: std::error::Error + std::fmt::Debug,
{
    let mut msg = format!("{context} for {key}: {err}");
    let mut source = err.source();
    while let Some(err) = source {
        msg.push_str(&format!("; caused by: {err}"));
        source = err.source();
    }
    CloudHomeError::Storage(msg)
}

/// Map a GetObject failure to a `CloudHomeError`, surfacing the S3 error code and
/// message (e.g. `AccessDenied`, `PermanentRedirect`, `SignatureDoesNotMatch`)
/// rather than the opaque "service error". `NoSuchKey` becomes `NotFound`;
/// non-service failures (timeouts, connection errors) fall back to their own
/// description. Generic over the response type so it serves both `read` and
/// `read_range` without naming the smithy HTTP type.
fn get_object_error<R>(
    key: &str,
    err: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError, R>,
) -> CloudHomeError {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    match err.code() {
        Some("NoSuchKey") => CloudHomeError::NotFound(key.to_string()),
        Some(code) => CloudHomeError::Storage(match err.message() {
            Some(msg) => format!("get {key}: S3 {code}: {msg}"),
            None => format!("get {key}: S3 {code} (no message provided)"),
        }),
        // Not a service error (timeout / connection / dispatch) — its own
        // Display carries the detail.
        None => CloudHomeError::Storage(format!("get {key}: {err}")),
    }
}

/// Map a PutObject failure to a `CloudHomeError`. The common failure modes
/// each name the cause and the recovery the user can take:
///
/// - `AccessDenied` — bucket policy or IAM rejects writes. User fixes via
///   sync settings.
/// - `NoSuchBucket` — bucket was renamed/deleted out from under us.
/// - `OverQuota` / `QuotaExceeded` — non-AWS S3 providers (Backblaze, MinIO)
///   signal quota exhaustion through these codes.
///
/// Other service errors keep the raw code + message so they're debuggable;
/// transport failures surface their own Display.
fn put_object_error(
    key: &str,
    err: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
) -> CloudHomeError {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    match err.code() {
        Some("AccessDenied") => CloudHomeError::Storage(
            "Your S3 credentials don't have permission to write to this bucket. Check the access policy in sync settings."
                .to_string(),
        ),
        Some("NoSuchBucket") => CloudHomeError::Storage(
            "The S3 bucket no longer exists. Check the bucket name in sync settings.".to_string(),
        ),
        Some("OverQuota" | "QuotaExceeded") => CloudHomeError::Storage(
            "Your S3 storage quota is exceeded. Free up space or expand the quota.".to_string(),
        ),
        Some(code) => CloudHomeError::Storage(match err.message() {
            Some(msg) => format!("put {key}: S3 {code}: {msg}"),
            None => format!("put {key}: S3 {code} (no message provided)"),
        }),
        None => CloudHomeError::Storage(format!("put {key}: {err}")),
    }
}

#[async_trait]
impl CloudHome for S3CloudHome {
    /// HeadBucket — cheap auth + existence check, no listing cost.
    async fn probe(&self) -> Result<(), CloudHomeError> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        on_s3_rt(async move {
            use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};

            match client.head_bucket().bucket(&bucket).send().await {
                Ok(_) => Ok(()),
                Err(SdkError::ServiceError(svc)) => {
                    let status = svc.raw().status().as_u16();
                    let code: Option<String> = svc.err().code().map(str::to_string);
                    // The shared 404→missing / 403→creds-rejected classification both
                    // S3 backends use.
                    Err(probe_error(status, code.as_deref(), &bucket))
                }
                Err(e) => Err(CloudHomeError::Storage(format!("S3 probe failed: {e}"))),
            }
        })
        .await
    }

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let full = self.full_key(key);
        let key = key.to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        on_s3_rt(async move {
            client
                .put_object()
                .bucket(&bucket)
                .key(&full)
                .body(data.into())
                .send()
                .await
                .map_err(|e| put_object_error(&key, e))?;
            Ok(())
        })
        .await
    }

    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        _total_len: u64,
    ) -> Result<super::BoxPartSink<'a>, CloudHomeError> {
        let full = self.full_key(key);
        // Only the aws interaction goes on the big-stack runtime; the `S3PartSink`
        // is built here so its borrow of `&self` stays out of the spawned
        // `'static` future (the sink owns its own `client.clone()`).
        let upload_id = {
            let key = key.to_string();
            let full = full.clone();
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            on_s3_rt(async move {
                let create = client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(&full)
                    .send()
                    .await
                    .map_err(|e| CloudHomeError::Storage(format!("multipart create {key}: {e}")))?;
                create
                    .upload_id()
                    .ok_or_else(|| {
                        CloudHomeError::Storage(format!(
                            "multipart create {key}: no upload id returned"
                        ))
                    })
                    .map(str::to_string)
            })
            .await?
        };
        Ok(Box::new(S3PartSink {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            key: full,
            upload_id,
            completed: Vec::new(),
            next_part_number: 1,
        }))
    }

    fn multipart_threshold(&self) -> u64 {
        MULTIPART_THRESHOLD as u64
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
        let full = self.full_key(key);
        let key = key.to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        // The body `collect()` runs inside the spawn too: streaming the response
        // drives the same aws connector that needs the big stack.
        on_s3_rt(async move {
            let resp = client
                .get_object()
                .bucket(&bucket)
                .key(&full)
                .send()
                .await
                .map_err(|e| get_object_error(&key, e))?;

            let bytes = resp
                .body
                .collect()
                .await
                .map_err(|e| body_read_error("read body", &key, e))?
                .into_bytes()
                .to_vec();

            Ok(bytes)
        })
        .await
    }

    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError> {
        let full = self.full_key(key);
        let range = range_header(start, end);
        let key = key.to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        on_s3_rt(async move {
            let resp = client
                .get_object()
                .bucket(&bucket)
                .key(&full)
                .range(range)
                .send()
                .await
                .map_err(|e| get_object_error(&key, e))?;

            let bytes = resp
                .body
                .collect()
                .await
                .map_err(|e| body_read_error("read range body", &key, e))?
                .into_bytes()
                .to_vec();

            Ok(bytes)
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
        let full_prefix = self.full_key(prefix);
        let key_prefix = self.key_prefix.clone();
        let prefix = prefix.to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        // The whole continuation loop is one spawned task: every page's `send`
        // runs on the big-stack runtime.
        on_s3_rt(async move {
            let mut keys = Vec::new();
            let mut continuation_token: Option<String> = None;

            loop {
                let mut req = client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&full_prefix);

                if let Some(token) = continuation_token.take() {
                    req = req.continuation_token(token);
                }

                let resp = req
                    .send()
                    .await
                    .map_err(|e| CloudHomeError::Storage(format!("list {prefix}: {e}")))?;

                for obj in resp.contents() {
                    let Some(key) = obj.key() else {
                        warn!("list {prefix}: S3 returned an object with no key; skipping it");
                        continue;
                    };
                    let Some(stripped) =
                        strip_listed_key_prefix(key_prefix.as_deref(), &full_prefix, key)
                    else {
                        warn!(
                            "list {prefix}: key {key} is outside the configured S3 prefix {:?}; \
                             skipping it",
                            key_prefix
                        );
                        continue;
                    };
                    keys.push(stripped.to_string());
                }

                if resp.is_truncated() == Some(true) {
                    continuation_token = resp.next_continuation_token().map(|s| s.to_string());
                } else {
                    break;
                }
            }

            Ok(keys)
        })
        .await
    }

    async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
        let full = self.full_key(key);
        let key = key.to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        on_s3_rt(async move {
            use aws_sdk_s3::error::ProvideErrorMetadata;
            if let Err(e) = client
                .delete_object()
                .bucket(&bucket)
                .key(&full)
                .send()
                .await
            {
                // Delete is idempotent: AWS S3 returns 204 for an already-absent key,
                // but GCS's S3 XML API returns 404 `NoSuchKey`. A missing object is not
                // a failure — `cancel_tombstone` deletes the tombstone after every
                // upload and relies on the no-tombstone case being a no-op — so swallow
                // not-found (the shared rule both S3 backends apply) and surface only
                // real errors.
                if !is_not_found_code(e.code()) {
                    return Err(CloudHomeError::Storage(format!("delete {key}: {e}")));
                }
            }
            Ok(())
        })
        .await
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
        let full = self.full_key(key);
        let key = key.to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        on_s3_rt(async move {
            use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
            match client.head_object().bucket(&bucket).key(&full).send().await {
                Ok(_) => Ok(true),
                // Apply the shared not-found rule (NoSuchKey/NotFound, or a raw 404)
                // off the modeled error code and status, not a Display-string match.
                Err(e) => {
                    let status = match &e {
                        SdkError::ServiceError(svc) => Some(svc.raw().status().as_u16()),
                        _ => None,
                    };
                    if is_not_found_code(e.code()) || status == Some(404) {
                        Ok(false)
                    } else {
                        Err(CloudHomeError::Storage(format!("head {key}: {e}")))
                    }
                }
            }
        })
        .await
    }

    async fn object_state(&self, key: &str) -> Result<CloudObjectState, CloudHomeError> {
        let full = self.full_key(key);
        let key = key.to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        on_s3_rt(async move {
            use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
            match client.head_object().bucket(&bucket).key(&full).send().await {
                Ok(resp) => match resp.e_tag() {
                    Some(etag) => Ok(CloudObjectState::Present(CloudObjectVersion::new(etag))),
                    None => Ok(CloudObjectState::VersionUnavailable),
                },
                Err(e) => {
                    let status = match &e {
                        SdkError::ServiceError(svc) => Some(svc.raw().status().as_u16()),
                        _ => None,
                    };
                    if is_not_found_code(e.code()) || status == Some(404) {
                        Ok(CloudObjectState::Absent)
                    } else {
                        Err(CloudHomeError::Storage(format!("head {key}: {e}")))
                    }
                }
            }
        })
        .await
    }

    async fn delete_if_version(
        &self,
        key: &str,
        version: &CloudObjectVersion,
    ) -> Result<ConditionalDelete, CloudHomeError> {
        let full = self.full_key(key);
        let key = key.to_string();
        let etag = version.as_str().to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        on_s3_rt(async move {
            use aws_sdk_s3::error::ProvideErrorMetadata;
            if let Err(e) = client
                .delete_object()
                .bucket(&bucket)
                .key(&full)
                .if_match(etag)
                .send()
                .await
            {
                match e.code() {
                    Some("PreconditionFailed") => return Ok(ConditionalDelete::Changed),
                    Some(code) if is_not_found_code(Some(code)) => {
                        return Ok(ConditionalDelete::NotFound);
                    }
                    _ => return Err(CloudHomeError::Storage(format!("delete {key}: {e}"))),
                }
            }
            Ok(ConditionalDelete::Deleted)
        })
        .await
    }

    async fn grant_access(
        &self,
        _grant: CloudAccessGrant,
    ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
        Ok(CloudHomeJoinInfo::S3 {
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            key_prefix: self.key_prefix.clone(),
        })
    }

    async fn revoke_access(&self, _revoke: CloudAccessRevoke) -> Result<(), CloudHomeError> {
        Err(CloudHomeError::Storage(
            "S3 member removal requires rotating the shared bucket credentials; this backend cannot revoke one member from a shared access key"
                .to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::State;
    use axum::http::header::{CONTENT_LENGTH, CONTENT_RANGE, RANGE};
    use axum::http::{HeaderMap, Method, Response, StatusCode, Uri};
    use axum::Router;
    use std::sync::Arc;

    #[test]
    fn full_key_prepends_prefix() {
        let key = apply_prefix(Some("libs/abc"), "heads/dev1.json");
        assert_eq!(key, "libs/abc/heads/dev1.json");
    }

    #[test]
    fn full_key_no_prefix() {
        let key = apply_prefix(None, "heads/dev1.json");
        assert_eq!(key, "heads/dev1.json");
    }

    #[test]
    fn normalized_prefix_drops_trailing_slash() {
        let prefix = normalize_prefix(Some("libs/abc/".to_string()));
        let key = apply_prefix(prefix.as_deref(), "heads/dev1.json");
        assert_eq!(key, "libs/abc/heads/dev1.json");
    }

    #[derive(Clone)]
    struct FakeRangeObject {
        bucket: String,
        key: String,
        range_body: Vec<u8>,
        object_len: u64,
        whole_object_crc32c: &'static str,
    }

    async fn fake_s3_range_endpoint(
        State(object): State<Arc<FakeRangeObject>>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
    ) -> Response<Body> {
        let expected_path = format!("/{}/{}", object.bucket, object.key);
        let range = headers.get(RANGE).and_then(|v| v.to_str().ok());

        if method != Method::GET || uri.path() != expected_path || range != Some("bytes=0-23") {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!(
                    "unexpected request: method={method}, path={}, range={range:?}",
                    uri.path()
                )))
                .expect("build bad-request response");
        }

        Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(CONTENT_RANGE, format!("bytes 0-23/{}", object.object_len))
            .header(CONTENT_LENGTH, object.range_body.len().to_string())
            .header("x-amz-checksum-crc32c", object.whole_object_crc32c)
            .body(Body::from(object.range_body.clone()))
            .expect("build fake range response")
    }

    async fn spawn_fake_s3_endpoint(
        object: FakeRangeObject,
    ) -> (String, tokio::sync::oneshot::Sender<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake S3 endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let app = Router::new()
            .fallback(fake_s3_range_endpoint)
            .with_state(Arc::new(object));

        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("fake S3 endpoint failed");
        });

        (endpoint, shutdown_tx)
    }

    #[tokio::test]
    async fn read_range_accepts_s3_compatible_full_object_checksum_header() {
        let range_body = b"abcdefghijklmnopqrstuvwx".to_vec();
        let key = "storage/audio-object".to_string();
        let bucket = "coven-s3-compatible-test".to_string();
        let (endpoint, shutdown) = spawn_fake_s3_endpoint(FakeRangeObject {
            bucket: bucket.clone(),
            key: key.clone(),
            range_body: range_body.clone(),
            object_len: 96,
            whole_object_crc32c: "sNqCyA==",
        })
        .await;

        if !loopback_connects(&endpoint).await {
            return;
        }

        let home = S3CloudHome::new(
            bucket,
            "us-central1".to_string(),
            Some(endpoint),
            "access-key".to_string(),
            "secret-key".to_string(),
            None,
        )
        .await
        .expect("construct S3CloudHome");

        let bytes = home
            .read_range(&key, 0, range_body.len() as u64)
            .await
            .expect("read range");

        assert_eq!(bytes, range_body);
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn s3_revoke_reports_unsupported_not_success() {
        let home = S3CloudHome::new(
            "bucket".to_string(),
            "us-east-1".to_string(),
            Some("http://127.0.0.1:9".to_string()),
            "access-key".to_string(),
            "secret-key".to_string(),
            None,
        )
        .await
        .expect("construct S3CloudHome");

        let result = home
            .revoke_access(CloudAccessRevoke {
                member_pubkey: "member-pubkey".to_string(),
                provider_account_email: None,
            })
            .await;

        assert!(
            result.is_err(),
            "S3 removal must not report success without rotating bucket credentials",
        );
    }

    async fn loopback_connects(endpoint: &str) -> bool {
        let Some(addr) = endpoint.strip_prefix("http://") else {
            panic!("fake S3 endpoint must be an http URL, got {endpoint}");
        };
        match tokio::net::TcpStream::connect(addr).await {
            Ok(_) => true,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::PermissionDenied
                ) =>
            {
                tracing::debug!(
                    "skipping fake S3 endpoint test: loopback connect unavailable: {e}"
                );
                false
            }
            Err(e) => panic!("fake S3 endpoint is not reachable at {endpoint}: {e}"),
        }
    }

    // ── probe() against a real S3 endpoint ──────────────────────────────
    //
    // These tests require a minio (or any S3-compatible server) reachable at
    // `COVEN_TEST_S3_URL` (default http://localhost:19000) with credentials
    // `COVEN_TEST_S3_KEY` / `COVEN_TEST_S3_SECRET` (default minioadmin / minioadmin).
    // Marked `#[ignore]` so `cargo test` skips them; run with
    // `cargo test -- --ignored` when an endpoint is available.

    /// Read a test env var with a default fallback. `NotPresent` silently uses
    /// the default (the intended path); `NotUnicode` panics so a misconfigured
    /// env var fails loudly instead of silently substituting bytes-as-default.
    fn test_env(name: &str, default: &str) -> String {
        match std::env::var(name) {
            Ok(s) => s,
            Err(std::env::VarError::NotPresent) => default.to_string(),
            Err(std::env::VarError::NotUnicode(raw)) => {
                panic!("test env var {name} is non-utf8: {raw:?}");
            }
        }
    }

    struct TestCreds {
        endpoint: String,
        access_key: String,
        secret_key: String,
    }

    fn test_creds() -> TestCreds {
        TestCreds {
            endpoint: test_env("COVEN_TEST_S3_URL", "http://localhost:19000"),
            access_key: test_env("COVEN_TEST_S3_KEY", "coventest"),
            secret_key: test_env("COVEN_TEST_S3_SECRET", "coventestpass"),
        }
    }

    fn required_test_env(name: &str) -> String {
        match std::env::var(name) {
            Ok(s) => s,
            Err(std::env::VarError::NotPresent) => {
                panic!("test env var {name} must be set for this test");
            }
            Err(std::env::VarError::NotUnicode(raw)) => {
                panic!("test env var {name} is non-utf8: {raw:?}");
            }
        }
    }

    fn optional_test_env(name: &str) -> Option<String> {
        match std::env::var(name) {
            Ok(s) => Some(s),
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(raw)) => {
                panic!("test env var {name} is non-utf8: {raw:?}");
            }
        }
    }

    struct ExistingS3ObjectEnv {
        bucket: String,
        region: String,
        endpoint: String,
        key: String,
        access_key: String,
        secret_key: String,
    }

    fn existing_s3_object_env() -> Option<ExistingS3ObjectEnv> {
        let names = [
            "COVEN_TEST_S3_BUCKET",
            "COVEN_TEST_S3_REGION",
            "COVEN_TEST_S3_URL",
            "COVEN_TEST_S3_EXISTING_KEY",
            "COVEN_TEST_S3_KEY",
            "COVEN_TEST_S3_SECRET",
        ];
        let mut values = Vec::with_capacity(names.len());
        let mut missing = Vec::new();
        for name in names {
            match optional_test_env(name) {
                Some(value) => values.push(value),
                None => missing.push(name),
            }
        }
        if !missing.is_empty() {
            eprintln!(
                "skipping live S3 object test; unset env vars: {}",
                missing.join(", ")
            );
            return None;
        }
        let [bucket, region, endpoint, key, access_key, secret_key]: [String; 6] =
            values.try_into().expect("collected every live S3 env var");
        Some(ExistingS3ObjectEnv {
            bucket,
            region,
            endpoint,
            key,
            access_key,
            secret_key,
        })
    }

    #[tokio::test]
    #[ignore]
    async fn read_range_succeeds_against_existing_s3_object() {
        let creds = test_creds();
        let bucket = required_test_env("COVEN_TEST_S3_BUCKET");
        let region = test_env("COVEN_TEST_S3_REGION", "us-east-1");
        let key = required_test_env("COVEN_TEST_S3_EXISTING_KEY");
        let start: u64 = test_env("COVEN_TEST_S3_RANGE_START", "0")
            .parse()
            .expect("COVEN_TEST_S3_RANGE_START must be a u64");
        let end: u64 = test_env("COVEN_TEST_S3_RANGE_END", "24")
            .parse()
            .expect("COVEN_TEST_S3_RANGE_END must be a u64");

        let home = S3CloudHome::new(
            bucket,
            region,
            Some(creds.endpoint),
            creds.access_key,
            creds.secret_key,
            None,
        )
        .await
        .expect("construct S3CloudHome");

        eprintln!("reading {key} range {start}..{end}");
        let bytes = home
            .read_range(&key, start, end)
            .await
            .unwrap_or_else(|e| panic!("read_range failed: {e:?}"));

        assert_eq!(bytes.len() as u64, end - start);
    }

    /// Proves an `S3CloudHome`'s aws calls run end to end on `s3_runtime` — a
    /// different runtime than the `Client` was built on — against a real bucket:
    /// connection establishment, TLS, and body streaming, not just "it compiles".
    /// If the aws connector bound to the build-time runtime's reactor this would
    /// panic with "no reactor running" or hang; a real byte vec back proves the
    /// spawn-on-other-runtime path works.
    ///
    /// Reads an existing object from a real S3-compatible bucket. Coordinates
    /// come from `COVEN_TEST_S3_BUCKET`, `COVEN_TEST_S3_REGION`,
    /// `COVEN_TEST_S3_URL`, and `COVEN_TEST_S3_EXISTING_KEY`.
    #[tokio::test]
    #[ignore]
    async fn s3_big_stack_reads_real_bytes_from_existing_object() {
        let Some(env) = existing_s3_object_env() else {
            return;
        };

        let home = S3CloudHome::new(
            env.bucket,
            env.region,
            Some(env.endpoint),
            env.access_key,
            env.secret_key,
            None,
        )
        .await
        .expect("construct S3CloudHome");

        let whole = home
            .read(&env.key)
            .await
            .unwrap_or_else(|e| panic!("read({}) failed: {e:?}", env.key));
        assert!(
            !whole.is_empty(),
            "expected non-empty object at {}",
            env.key
        );
        eprintln!("read {} bytes from {}", whole.len(), env.key);

        let n = whole.len().min(16) as u64;
        let head = home
            .read_range(&env.key, 0, n)
            .await
            .unwrap_or_else(|e| panic!("read_range({}, 0..{n}) failed: {e:?}", env.key));
        assert_eq!(
            head.as_slice(),
            &whole[..n as usize],
            "range bytes must match the object's prefix"
        );
        eprintln!("read_range first {n} bytes match the full read");
    }

    /// Provision the bucket configured on `home`.
    async fn provision_test_bucket(home: &S3CloudHome) {
        home.client
            .create_bucket()
            .bucket(&home.bucket)
            .send()
            .await
            .expect("create test bucket");
    }

    #[tokio::test]
    #[ignore]
    async fn probe_succeeds_against_existing_bucket() {
        let creds = test_creds();
        let bucket = format!("coven-probe-ok-{}", uuid::Uuid::new_v4());
        let home = S3CloudHome::new(
            bucket,
            "us-east-1".to_string(),
            Some(creds.endpoint),
            creds.access_key,
            creds.secret_key,
            None,
        )
        .await
        .expect("construct S3CloudHome");
        provision_test_bucket(&home).await;
        home.probe().await.expect("probe should succeed");
    }

    #[tokio::test]
    #[ignore]
    async fn probe_fails_for_missing_bucket() {
        let creds = test_creds();
        let bucket = format!("coven-probe-missing-{}", uuid::Uuid::new_v4());
        let home = S3CloudHome::new(
            bucket.clone(),
            "us-east-1".to_string(),
            Some(creds.endpoint),
            creds.access_key,
            creds.secret_key,
            None,
        )
        .await
        .expect("construct S3CloudHome");
        // Deliberately do NOT create the bucket.
        let err = home
            .probe()
            .await
            .expect_err("probe should fail for a missing bucket");
        let msg = format!("{err}");
        assert!(
            msg.contains("does not exist") || msg.contains("NoSuchBucket") || msg.contains("404"),
            "expected missing-bucket error, got: {msg}",
        );
    }

    #[tokio::test]
    #[ignore]
    async fn probe_fails_for_bad_secret_key() {
        let creds = test_creds();
        let bucket = format!("coven-probe-badkey-{}", uuid::Uuid::new_v4());
        // Provision the bucket with the good creds so the only difference is the bad secret.
        let good = S3CloudHome::new(
            bucket.clone(),
            "us-east-1".to_string(),
            Some(creds.endpoint.clone()),
            creds.access_key.clone(),
            creds.secret_key,
            None,
        )
        .await
        .expect("construct good S3CloudHome");
        provision_test_bucket(&good).await;

        let bad = S3CloudHome::new(
            bucket,
            "us-east-1".to_string(),
            Some(creds.endpoint),
            creds.access_key,
            "wrong-secret".to_string(),
            None,
        )
        .await
        .expect("construct bad S3CloudHome");
        let err = bad
            .probe()
            .await
            .expect_err("probe should fail for bad credentials");
        let msg = format!("{err}");
        assert!(
            msg.contains("rejected")
                || msg.contains("403")
                || msg.contains("SignatureDoesNotMatch"),
            "expected credentials error, got: {msg}",
        );
    }
}
