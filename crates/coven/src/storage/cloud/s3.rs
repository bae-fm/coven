//! S3-backed `CloudHome` implementation.
//!
//! Wraps `aws-sdk-s3` to provide raw storage operations against any
//! S3-compatible endpoint.

mod runtime;

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
    combine_cleanup_failure, range_header, BlobBody, CloudAccessOutcome, CloudAccessState,
    CloudHome, CloudHomeError, CloudHomeJoinInfo, ExactSlotStorage, MultipartUpload, ObjectSlot,
    RevokeOutcome, UploadProgress,
};
use runtime::S3Runtime;

/// S3-backed cloud home.
#[derive(Clone)]
pub struct S3CloudHome {
    runtime: S3Runtime,
    client: Client,
    sts_client: Option<aws_sdk_sts::Client>,
    bucket: String,
    region: String,
    endpoint: Option<String>,
    access_key: String,
    secret_key: String,
    key_prefix: Option<String>,
    exact_slots: bool,
}

fn create_only_put_failed(
    error: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
) -> bool {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    matches!(
        error.code(),
        Some("PreconditionFailed" | "ConditionalRequestConflict")
    )
}

impl S3CloudHome {
    #[allow(clippy::too_many_arguments)]
    async fn new(
        runtime: S3Runtime,
        bucket: String,
        region: String,
        endpoint: Option<String>,
        access_key: String,
        secret_key: String,
        key_prefix: Option<String>,
        custom_exact_slots: Option<crate::config::CustomS3ExactSlots>,
    ) -> Result<Self, CloudHomeError> {
        let exact_slots = endpoint.is_none()
            || custom_exact_slots
                == Some(crate::config::CustomS3ExactSlots::StandardConditionalRequests);
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
        let sts_client = endpoint
            .is_none()
            .then(|| aws_sdk_sts::Client::new(&aws_config));

        Ok(S3CloudHome {
            runtime,
            client,
            sts_client,
            bucket,
            region,
            endpoint,
            access_key,
            secret_key,
            // Normalize once here (trim trailing slash, drop empty), so neither
            // full_key nor list re-trims it.
            key_prefix: normalize_prefix(key_prefix),
            exact_slots,
        })
    }

    /// Prepend the key prefix (if configured) to produce the full S3 object key.
    fn full_key(&self, key: &str) -> String {
        apply_prefix(self.key_prefix.as_deref(), key)
    }

    async fn open_multipart_sink(
        &self,
        key: &str,
        completion: MultipartCompletion,
    ) -> Result<Box<S3PartSink>, CloudHomeError> {
        let full = self.full_key(key);
        let upload_id = {
            let key = key.to_string();
            let full = full.clone();
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            self.runtime
                .run(async move {
                    let create = client
                        .create_multipart_upload()
                        .bucket(&bucket)
                        .key(&full)
                        .send()
                        .await
                        .map_err(|error| {
                            CloudHomeError::Transport(format!("multipart create {key}: {error}"))
                        })?;
                    create
                        .upload_id()
                        .ok_or_else(|| {
                            CloudHomeError::Transport(format!(
                                "multipart create {key}: no upload id returned"
                            ))
                        })
                        .map(str::to_string)
                })
                .await?
        };
        let (commands, receiver) = tokio::sync::mpsc::channel(1);
        let owner = S3MultipartOwner {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            key: full,
            logical_key: key.to_string(),
            upload_id,
            completed: Vec::new(),
            next_part_number: 1,
            completion,
        };
        Ok(Box::new(S3PartSink {
            commands: Some(commands),
            owner: Some(self.runtime.spawn(owner.run(receiver))),
        }))
    }

    async fn put_create_only(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let full = self.full_key(key);
        let logical_key = key.to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime
            .run(async move {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(&full)
                    .if_none_match("*")
                    .body(data.into())
                    .send()
                    .await
                    .map_err(|error| {
                        if create_only_put_failed(&error) {
                            CloudHomeError::AlreadyExists(logical_key.clone())
                        } else {
                            put_object_error(&logical_key, error)
                        }
                    })?;
                Ok(())
            })
            .await
    }

    async fn append_create_only(
        &self,
        key: &str,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        if body.len() <= self.multipart_threshold() {
            let data = body.collect().await?;
            let length = data.len() as u64;
            self.put_create_only(key, data).await?;
            progress(length);
            return Ok(());
        }
        let sink = self
            .open_multipart_sink(key, MultipartCompletion::CreateOnly)
            .await?;
        MultipartUpload::new(key, body, sink, progress).run().await
    }

    #[cfg(test)]
    async fn provision_test_bucket(&self) {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime
            .run(async move {
                client
                    .create_bucket()
                    .bucket(&bucket)
                    .send()
                    .await
                    .map_err(|error| {
                        CloudHomeError::Transport(format!(
                            "create test S3 bucket {bucket}: {error}"
                        ))
                    })?;
                Ok(())
            })
            .await
            .expect("create test bucket");
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn open_cloud_home(
    bucket: String,
    region: String,
    endpoint: Option<String>,
    access_key: String,
    secret_key: String,
    key_prefix: Option<String>,
    custom_exact_slots: Option<crate::config::CustomS3ExactSlots>,
) -> Result<S3CloudHome, CloudHomeError> {
    S3CloudHome::new(
        S3Runtime::new()?,
        bucket,
        region,
        endpoint,
        access_key,
        secret_key,
        key_prefix,
        custom_exact_slots,
    )
    .await
}

/// A [`PartSink`] over an open S3 multipart upload: each `send_part` is one
/// `upload_part` whose ETag is kept, and `finish` is `complete_multipart_upload`.
/// The owner task holds the multipart state and waits for every S3 request. On
/// normal completion or failure the caller joins it; cancellation closes the
/// command channel and the owner waits for abort without blocking `Drop`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum MultipartCompletion {
    Mutable,
    CreateOnly,
}

struct S3PartSink {
    commands: Option<tokio::sync::mpsc::Sender<S3MultipartCommand>>,
    owner: Option<tokio::task::JoinHandle<Result<(), CloudHomeError>>>,
}

enum S3MultipartCommand {
    SendPart {
        part: bytes::Bytes,
        response: tokio::sync::oneshot::Sender<Result<(), CloudHomeError>>,
    },
    Abort,
    Finish,
}

struct S3MultipartOwner {
    client: Client,
    bucket: String,
    /// The prefixed object key (also used in error messages).
    key: String,
    logical_key: String,
    upload_id: String,
    completed: Vec<aws_sdk_s3::types::CompletedPart>,
    next_part_number: i32,
    completion: MultipartCompletion,
}

impl S3MultipartOwner {
    async fn run(
        mut self,
        mut commands: tokio::sync::mpsc::Receiver<S3MultipartCommand>,
    ) -> Result<(), CloudHomeError> {
        while let Some(command) = commands.recv().await {
            match command {
                S3MultipartCommand::SendPart { part, response } => {
                    let result = self.send_part(part).await;
                    if response.send(result).is_err() {
                        return self.abort().await;
                    }
                }
                S3MultipartCommand::Abort => return self.abort().await,
                S3MultipartCommand::Finish => return self.finish().await,
            }
        }
        self.abort().await
    }

    async fn send_part(&mut self, part: bytes::Bytes) -> Result<(), CloudHomeError> {
        let part_number = self.next_part_number;
        self.next_part_number += 1;
        let uploaded = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .part_number(part_number)
            .body(part.into())
            .send()
            .await
            .map_err(|error| {
                CloudHomeError::Transport(format!(
                    "multipart part {part_number} {}: {error}",
                    self.key
                ))
            })?;
        self.completed.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(part_number)
                .set_e_tag(uploaded.e_tag().map(str::to_string))
                .build(),
        );
        Ok(())
    }

    async fn abort(&mut self) -> Result<(), CloudHomeError> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .send()
            .await
            .map_err(|error| {
                CloudHomeError::Transport(format!("abort multipart {}: {error}", self.key))
            })?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), CloudHomeError> {
        let completed_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(std::mem::take(&mut self.completed)))
            .build();
        let request = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .multipart_upload(completed_upload);
        let request = match self.completion {
            MultipartCompletion::Mutable => request,
            MultipartCompletion::CreateOnly => request.if_none_match("*"),
        };
        let operation = request.send().await.map(|_| ()).map_err(|error| {
            use aws_sdk_s3::error::ProvideErrorMetadata;
            if self.completion == MultipartCompletion::CreateOnly
                && matches!(
                    error.code(),
                    Some("PreconditionFailed" | "ConditionalRequestConflict")
                )
            {
                CloudHomeError::AlreadyExists(self.logical_key.clone())
            } else {
                CloudHomeError::Transport(format!("multipart complete {}: {error}", self.key))
            }
        });
        match operation {
            Ok(()) => Ok(()),
            Err(operation) => {
                let cleanup = self.abort().await;
                Err(combine_cleanup_failure(operation, cleanup))
            }
        }
    }
}

impl Drop for S3PartSink {
    fn drop(&mut self) {
        self.commands.take();
    }
}

impl S3PartSink {
    async fn settle(&mut self, command: S3MultipartCommand) -> Result<(), CloudHomeError> {
        let commands = self.commands.take().ok_or_else(|| {
            CloudHomeError::Transport("S3 multipart upload is already settled".to_string())
        })?;
        let send_result = commands.send(command).await;
        drop(commands);
        let owner = self
            .owner
            .take()
            .ok_or_else(|| CloudHomeError::Transport("S3 multipart owner is absent".to_string()))?;
        let result = owner.await.map_err(|error| {
            CloudHomeError::Transport(format!("S3 multipart owner task failed: {error}"))
        })?;
        match (send_result, result) {
            (Ok(()), result) => result,
            (Err(_), Err(error)) => Err(error),
            (Err(_), Ok(())) => Err(CloudHomeError::Transport(
                "S3 multipart owner stopped before receiving its terminal command".to_string(),
            )),
        }
    }
}

fn s3_access_key_id_hash(access_key_id: &str) -> crate::protocol::store_commit::ObjectHash {
    const DOMAIN: &[u8] = b"coven.s3-access-key-id.v1\0";
    let mut material = Vec::with_capacity(DOMAIN.len() + access_key_id.len());
    material.extend_from_slice(DOMAIN);
    material.extend_from_slice(access_key_id.as_bytes());
    crate::protocol::store_commit::ObjectHash::digest(&material)
}

fn aws_caller_identity(
    account_id: &str,
    arn: &str,
    user_id: &str,
) -> Result<(String, crate::storage::AwsPrincipal), CloudHomeError> {
    use crate::storage::AwsPrincipal;

    if account_id.len() != 12 || !account_id.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(CloudHomeError::Configuration(
            "STS GetCallerIdentity returned a malformed AWS account id".to_string(),
        ));
    }
    let fields: Vec<_> = arn.splitn(6, ':').collect();
    if fields.len() != 6
        || fields[0] != "arn"
        || fields[1].is_empty()
        || !fields[3].is_empty()
        || fields[4] != account_id
    {
        return Err(CloudHomeError::Configuration(
            "STS GetCallerIdentity returned an unrecognized caller ARN".to_string(),
        ));
    }
    let principal = match (fields[2], fields[5]) {
        ("iam", "root") if user_id == account_id => AwsPrincipal::Root,
        ("iam", resource) if resource.starts_with("user/") && !user_id.is_empty() => {
            AwsPrincipal::User {
                arn: arn.to_string(),
                user_id: user_id.to_string(),
            }
        }
        ("sts", resource) if resource.starts_with("assumed-role/") => {
            let (role_id, session) = user_id.split_once(':').ok_or_else(|| {
                CloudHomeError::Configuration(
                    "STS assumed-role caller has no stable role-id prefix".to_string(),
                )
            })?;
            if role_id.is_empty() || session.is_empty() {
                return Err(CloudHomeError::Configuration(
                    "STS assumed-role caller has a malformed user id".to_string(),
                ));
            }
            AwsPrincipal::Role {
                role_id: role_id.to_string(),
            }
        }
        _ => {
            return Err(CloudHomeError::Configuration(
                "STS caller must be the account root, an IAM user, or an assumed role".to_string(),
            ));
        }
    };
    Ok((fields[1].to_string(), principal))
}

fn sts_request_error(error: impl std::fmt::Display) -> CloudHomeError {
    CloudHomeError::Transport(format!("STS GetCallerIdentity failed: {error}"))
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
        let commands = self.commands.as_ref().ok_or_else(|| {
            CloudHomeError::Transport("S3 multipart upload is already settled".to_string())
        })?;
        let (response, result) = tokio::sync::oneshot::channel();
        commands
            .send(S3MultipartCommand::SendPart { part, response })
            .await
            .map_err(|_| {
                CloudHomeError::Transport(
                    "S3 multipart owner stopped before part upload".to_string(),
                )
            })?;
        result.await.map_err(|_| {
            CloudHomeError::Transport("S3 multipart owner stopped during part upload".to_string())
        })?
    }

    async fn abort(&mut self) -> Result<(), CloudHomeError> {
        if self.commands.is_none() {
            return Ok(());
        }
        self.settle(S3MultipartCommand::Abort).await
    }

    async fn finish(mut self: Box<Self>) -> Result<(), CloudHomeError> {
        self.settle(S3MultipartCommand::Finish).await
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
    CloudHomeError::Transport(msg)
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
        Some(code) => CloudHomeError::Transport(match err.message() {
            Some(msg) => format!("get {key}: S3 {code}: {msg}"),
            None => format!("get {key}: S3 {code} (no message provided)"),
        }),
        // Not a service error (timeout / connection / dispatch) — its own
        // Display carries the detail.
        None => CloudHomeError::Transport(format!("get {key}: {err}")),
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
        Some("AccessDenied") => CloudHomeError::Configuration(
            "Your S3 credentials don't have permission to write to this bucket. Check the access policy in sync settings."
                .to_string(),
        ),
        Some("NoSuchBucket") => CloudHomeError::Configuration(
            "The S3 bucket no longer exists. Check the bucket name in sync settings.".to_string(),
        ),
        Some("OverQuota" | "QuotaExceeded") => CloudHomeError::Configuration(
            "Your S3 storage quota is exceeded. Free up space or expand the quota.".to_string(),
        ),
        Some(code) => CloudHomeError::Transport(match err.message() {
            Some(msg) => format!("put {key}: S3 {code}: {msg}"),
            None => format!("put {key}: S3 {code} (no message provided)"),
        }),
        None => CloudHomeError::Transport(format!("put {key}: {err}")),
    }
}

impl S3CloudHome {
    /// HeadBucket — cheap auth + existence check, no listing cost.
    async fn create_at_slot(
        &self,
        slot: &ObjectSlot,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        slot.require_logical_key_for("S3")?;
        self.append_create_only(slot.logical_key(), body, progress)
            .await
    }

    async fn read_exact_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        slot.require_logical_key_for("S3")?;
        let full = self.full_key(slot.logical_key());
        let key = slot.logical_key().to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let destination = destination.to_path_buf();
        self.runtime
            .run_file_read(async move {
                let response = client
                    .get_object()
                    .bucket(&bucket)
                    .key(&full)
                    .send()
                    .await
                    .map_err(|error| get_object_error(&key, error))?;
                let stream = futures_util::stream::unfold(
                    (response.body, key),
                    |(mut body, key)| async move {
                        body.next().await.map(|result| {
                            let result = result.map_err(|error| {
                                body_read_error("read appended body", &key, error)
                            });
                            (result, (body, key))
                        })
                    },
                );
                super::write_cloud_object_stream(&destination, Box::pin(stream)).await?;
                Ok::<(), super::CloudFileReadError>(())
            })
            .await
    }
}

#[async_trait]
impl CloudHome for S3CloudHome {
    fn exact_slot_storage(
        self: std::sync::Arc<Self>,
    ) -> Option<std::sync::Arc<dyn ExactSlotStorage>> {
        self.exact_slots.then_some(self)
    }

    async fn probe(&self) -> Result<(), CloudHomeError> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime
            .run(async move {
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
                    Err(e) => Err(CloudHomeError::Transport(format!("S3 probe failed: {e}"))),
                }
            })
            .await
    }

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let full = self.full_key(key);
        let key = key.to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime
            .run(async move {
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
        Ok(self
            .open_multipart_sink(key, MultipartCompletion::Mutable)
            .await?)
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
        self.runtime
            .run(async move {
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
        self.runtime
            .run(async move {
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

                // A ranged GET is honored only with 206 Partial Content; a 200 means
                // the provider ignored `Range` and returned the whole object from
                // byte 0. The aws-sdk `GetObjectOutput` doesn't surface the raw HTTP
                // status, so verify the equivalent invariant the reqwest transports
                // check by status: the body must be exactly the requested byte count
                // (the `CloudHome` contract never reads past the object's end).
                let expected = end - start;
                if bytes.len() as u64 != expected {
                    return Err(CloudHomeError::Transport(format!(
                        "read range {key}: expected {expected} bytes for range {start}..{end}, \
                     got {} — the provider likely ignored Range and returned the whole object",
                        bytes.len()
                    )));
                }

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
        self.runtime
            .run(async move {
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
                        .map_err(|e| CloudHomeError::Transport(format!("list {prefix}: {e}")))?;

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
                        let token = resp.next_continuation_token().ok_or_else(|| {
                            CloudHomeError::Transport(format!(
                                "list {prefix}: S3 truncated but returned no continuation token"
                            ))
                        })?;
                        continuation_token = Some(token.to_string());
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
        self.runtime
            .run(async move {
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
                    // a failure. Exact cleanup operations are retried after uncertain
                    // outcomes, so deleting an already-absent object must succeed. Swallow
                    // not-found and surface only real errors.
                    if !is_not_found_code(e.code()) {
                        return Err(CloudHomeError::Transport(format!("delete {key}: {e}")));
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
        self.runtime
            .run(async move {
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
                            Err(CloudHomeError::Transport(format!("head {key}: {e}")))
                        }
                    }
                }
            })
            .await
    }

    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError> {
        Ok(match desired {
            CloudAccessState::Present { .. } => {
                CloudAccessOutcome::Present(CloudHomeJoinInfo::S3 {
                    bucket: self.bucket.clone(),
                    region: self.region.clone(),
                    endpoint: self.endpoint.clone(),
                    access_key: self.access_key.clone(),
                    secret_key: self.secret_key.clone(),
                    key_prefix: self.key_prefix.clone(),
                })
            }
            CloudAccessState::Absent { .. } => {
                CloudAccessOutcome::Absent(RevokeOutcome::Unsupported)
            }
        })
    }
}

#[async_trait]
impl ExactSlotStorage for S3CloudHome {
    async fn provider_binding(
        &self,
    ) -> Result<crate::storage::ResolvedProviderBinding, CloudHomeError> {
        use crate::storage::{
            ProviderDeviceBinding, ProviderPrincipalId, ResolvedProviderBinding, S3EndpointBinding,
            StoreProviderBinding,
        };

        if self.bucket.is_empty() || self.region.is_empty() || self.access_key.is_empty() {
            return Err(CloudHomeError::Configuration(
                "S3 provider binding requires a bucket, region, and access-key id".to_string(),
            ));
        }
        let (endpoint, principal) = match self.endpoint.as_deref() {
            None => {
                let client = self.sts_client.clone().ok_or_else(|| {
                    CloudHomeError::Configuration("AWS S3 adapter has no STS client".to_string())
                })?;
                let identity = self
                    .runtime
                    .run(async move {
                        client
                            .get_caller_identity()
                            .send()
                            .await
                            .map_err(sts_request_error)
                    })
                    .await?;
                let account = identity.account().ok_or_else(|| {
                    CloudHomeError::Configuration(
                        "STS GetCallerIdentity returned no account id".to_string(),
                    )
                })?;
                let arn = identity.arn().ok_or_else(|| {
                    CloudHomeError::Configuration(
                        "STS GetCallerIdentity returned no caller ARN".to_string(),
                    )
                })?;
                let user_id = identity.user_id().ok_or_else(|| {
                    CloudHomeError::Configuration(
                        "STS GetCallerIdentity returned no user id".to_string(),
                    )
                })?;
                let (partition, principal) = aws_caller_identity(account, arn, user_id)?;
                (
                    S3EndpointBinding::Aws { partition },
                    ProviderPrincipalId::Aws {
                        account_id: account.to_string(),
                        principal,
                    },
                )
            }
            Some(endpoint) => (
                S3EndpointBinding::Custom {
                    origin: crate::protocol::provider::canonical_custom_s3_origin(endpoint)
                        .map_err(|error| CloudHomeError::Configuration(error.to_string()))?,
                },
                ProviderPrincipalId::CustomS3Credential {
                    access_key_id_hash: s3_access_key_id_hash(&self.access_key),
                },
            ),
        };
        let binding = ResolvedProviderBinding {
            store: StoreProviderBinding::S3 {
                endpoint,
                region: self.region.to_ascii_lowercase(),
                bucket: self.bucket.clone(),
                key_prefix: self.key_prefix.clone(),
            },
            device: ProviderDeviceBinding { principal },
        };
        binding
            .validate()
            .map_err(|error| CloudHomeError::Configuration(error.to_string()))?;
        Ok(binding)
    }

    async fn create_at(
        &self,
        slot: &ObjectSlot,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        S3CloudHome::create_at_slot(self, slot, body, progress).await
    }
    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        slot.require_logical_key_for("S3")?;
        S3CloudHome::read(self, slot.logical_key()).await
    }
    async fn read_range_at(
        &self,
        slot: &ObjectSlot,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, CloudHomeError> {
        slot.require_logical_key_for("S3")?;
        S3CloudHome::read_range(self, slot.logical_key(), start, end).await
    }
    async fn read_at_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        S3CloudHome::read_exact_to_file(self, slot, destination).await
    }
    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        slot.require_logical_key_for("S3")?;
        S3CloudHome::delete(self, slot.logical_key()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::State;
    use axum::http::header::{CONTENT_LENGTH, CONTENT_RANGE, IF_NONE_MATCH, RANGE};
    use axum::http::{HeaderMap, Method, Response, StatusCode, Uri};
    use axum::Router;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct FailingBodyReader {
        emitted: bool,
    }

    #[async_trait]
    impl crate::storage::local_file::PlaintextChunkReader for FailingBodyReader {
        async fn next_chunk(
            &mut self,
            _max: usize,
        ) -> Result<Vec<u8>, crate::storage::local_file::PlaintextChunkError> {
            if !self.emitted {
                self.emitted = true;
                return Ok(vec![7; MULTIPART_PART_SIZE]);
            }
            Err(crate::storage::local_file::PlaintextChunkError::Local(
                "injected body failure".to_string(),
            ))
        }
    }

    #[test]
    fn full_key_prepends_prefix() {
        let key = apply_prefix(Some("libs/abc"), "objects/dev1.json");
        assert_eq!(key, "libs/abc/objects/dev1.json");
    }

    #[test]
    fn full_key_no_prefix() {
        let key = apply_prefix(None, "objects/dev1.json");
        assert_eq!(key, "objects/dev1.json");
    }

    #[test]
    fn normalized_prefix_drops_trailing_slash() {
        let prefix = normalize_prefix(Some("libs/abc/".to_string()));
        let key = apply_prefix(prefix.as_deref(), "objects/dev1.json");
        assert_eq!(key, "libs/abc/objects/dev1.json");
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

    /// A test home against a fake endpoint with the fixture credentials every
    /// fake-server test uses; bucket, region, key prefix, and exact-slot
    /// capability are the axes tests vary.
    async fn test_home(
        bucket: String,
        region: &str,
        endpoint: String,
        prefix: Option<String>,
        exact_slots: Option<crate::CustomS3ExactSlots>,
    ) -> S3CloudHome {
        open_cloud_home(
            bucket,
            region.to_string(),
            Some(endpoint),
            "access-key".to_string(),
            "secret-key".to_string(),
            prefix,
            exact_slots,
        )
        .await
        .expect("construct test S3CloudHome")
    }

    /// Bind, serve, and wire graceful shutdown for a fake S3 server — the
    /// scaffolding every fake endpoint shares; each test supplies its Router.
    async fn spawn_fake_s3(app: Router) -> (String, tokio::sync::oneshot::Sender<()>) {
        crate::storage::cloud::test_server::spawn_test_server(
            app.layer(axum::extract::DefaultBodyLimit::disable()),
        )
        .await
    }

    async fn spawn_fake_s3_endpoint(
        object: FakeRangeObject,
    ) -> (String, tokio::sync::oneshot::Sender<()>) {
        spawn_fake_s3(
            Router::new()
                .fallback(fake_s3_range_endpoint)
                .with_state(Arc::new(object)),
        )
        .await
    }

    #[derive(Clone)]
    struct FakeFullBodyObject {
        bucket: String,
        key: String,
        full_body: Vec<u8>,
    }

    /// A fake S3 that ignores `Range` and answers every GET with 200 and the
    /// whole object — the provider-ignores-range failure the range verification
    /// must catch.
    async fn fake_s3_full_body_endpoint(
        State(object): State<Arc<FakeFullBodyObject>>,
        method: Method,
        uri: Uri,
    ) -> Response<Body> {
        let expected_path = format!("/{}/{}", object.bucket, object.key);
        if method != Method::GET || uri.path() != expected_path {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!(
                    "unexpected request: method={method}, path={}",
                    uri.path()
                )))
                .expect("build bad-request response");
        }

        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_LENGTH, object.full_body.len().to_string())
            .body(Body::from(object.full_body.clone()))
            .expect("build full-body response")
    }

    async fn spawn_fake_s3_full_body_endpoint(
        object: FakeFullBodyObject,
    ) -> (String, tokio::sync::oneshot::Sender<()>) {
        spawn_fake_s3(
            Router::new()
                .fallback(fake_s3_full_body_endpoint)
                .with_state(Arc::new(object)),
        )
        .await
    }

    #[derive(Clone)]
    struct FakePausedBodyObject {
        bucket: String,
        key: String,
        first: Vec<u8>,
        second: Vec<u8>,
        first_sent: Arc<tokio::sync::Notify>,
        release_second: Arc<tokio::sync::Notify>,
    }

    async fn fake_s3_paused_body_endpoint(
        State(object): State<FakePausedBodyObject>,
        method: Method,
        uri: Uri,
    ) -> Response<Body> {
        let expected_path = format!("/{}/{}", object.bucket, object.key);
        if method != Method::GET || uri.path() != expected_path {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("unexpected paused-body request"))
                .expect("build bad-request response");
        }

        let total_len = object.first.len() + object.second.len();
        let stream = futures_util::stream::unfold((0u8, object), |(stage, object)| async move {
            match stage {
                0 => {
                    object.first_sent.notify_one();
                    let first = object.first.clone();
                    Some((Ok::<Bytes, std::io::Error>(Bytes::from(first)), (1, object)))
                }
                1 => {
                    object.release_second.notified().await;
                    let second = object.second.clone();
                    Some((Ok(Bytes::from(second)), (2, object)))
                }
                _ => None,
            }
        });
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_LENGTH, total_len.to_string())
            .body(Body::from_stream(stream))
            .expect("build paused-body response")
    }

    async fn spawn_fake_s3_paused_body_endpoint(
        object: FakePausedBodyObject,
    ) -> (String, tokio::sync::oneshot::Sender<()>) {
        spawn_fake_s3(
            Router::new()
                .fallback(fake_s3_paused_body_endpoint)
                .with_state(object),
        )
        .await
    }

    async fn atomic_temp_paths(directory: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut entries = tokio::fs::read_dir(directory)
            .await
            .expect("read destination directory");
        let mut temps = Vec::new();
        while let Some(entry) = entries.next_entry().await.expect("read destination entry") {
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with(crate::storage::local_file::TEMP_BLOB_PREFIX)
            {
                temps.push(entry.path());
            }
        }
        temps
    }

    #[derive(Clone)]
    struct FakeListState {
        bucket: String,
        request_count: Arc<AtomicUsize>,
    }

    async fn fake_s3_truncated_list_endpoint(
        State(state): State<FakeListState>,
        method: Method,
        uri: Uri,
    ) -> Response<Body> {
        state.request_count.fetch_add(1, Ordering::SeqCst);

        let expected_path = format!("/{}/", state.bucket);
        if method != Method::GET || uri.path() != expected_path {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!(
                    "unexpected request: method={method}, path={}, query={:?}",
                    uri.path(),
                    uri.query()
                )))
                .expect("build bad-request response");
        }

        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/xml")
            .body(Body::from(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>coven-s3-list-test</Name>
  <Prefix>objects/</Prefix>
  <KeyCount>1</KeyCount>
  <IsTruncated>true</IsTruncated>
  <Contents>
    <Key>objects/dev1.json</Key>
    <Size>10</Size>
  </Contents>
</ListBucketResult>"#,
            ))
            .expect("build fake list response")
    }

    async fn spawn_fake_s3_truncated_list_endpoint(
        bucket: String,
    ) -> (String, tokio::sync::oneshot::Sender<()>, Arc<AtomicUsize>) {
        let request_count = Arc::new(AtomicUsize::new(0));
        let state = FakeListState {
            bucket,
            request_count: request_count.clone(),
        };
        let (endpoint, shutdown_tx) = spawn_fake_s3(
            Router::new()
                .fallback(fake_s3_truncated_list_endpoint)
                .with_state(state),
        )
        .await;
        (endpoint, shutdown_tx, request_count)
    }

    async fn fake_s3_two_page_list_endpoint(
        State(state): State<FakeListState>,
        method: Method,
        uri: Uri,
    ) -> Response<Body> {
        let request = state.request_count.fetch_add(1, Ordering::SeqCst);
        if method != Method::GET || uri.path() != format!("/{}/", state.bucket) {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("unexpected list request"))
                .expect("build response");
        }
        let (key, truncated, token) = match request {
            0 => (
                "objects/copy-a",
                true,
                "<NextContinuationToken>page-2</NextContinuationToken>",
            ),
            1 if uri
                .query()
                .is_some_and(|query| query.contains("continuation-token=page-2")) =>
            {
                ("objects/copy-b", false, "")
            }
            _ => {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from(format!("unexpected list page: {uri}")))
                    .expect("build response");
            }
        };
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/xml")
            .body(Body::from(format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                 <Name>{}</Name><Prefix>objects/</Prefix><KeyCount>1</KeyCount>\
                 <IsTruncated>{truncated}</IsTruncated>{token}\
                 <Contents><Key>{key}</Key><Size>10</Size></Contents>\
                 </ListBucketResult>",
                state.bucket
            )))
            .expect("build response")
    }

    #[tokio::test]
    async fn listing_exhausts_every_page() {
        let requests = Arc::new(AtomicUsize::new(0));
        let bucket = "immutable-list-test".to_string();
        let (endpoint, shutdown) = spawn_fake_s3(
            Router::new()
                .fallback(fake_s3_two_page_list_endpoint)
                .with_state(FakeListState {
                    bucket: bucket.clone(),
                    request_count: requests.clone(),
                }),
        )
        .await;
        let home = test_home(
            bucket,
            "us-east-1",
            endpoint,
            None,
            Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
        )
        .await;

        let listing = home.list("objects/").await.expect("list objects");

        assert_eq!(requests.load(Ordering::SeqCst), 2);
        assert_eq!(
            listing,
            vec!["objects/copy-a".to_string(), "objects/copy-b".to_string()]
        );
        shutdown.send(()).expect("shut down fake S3");
    }

    #[derive(Clone)]
    struct FakeWriteState {
        bucket: String,
        conditional_headers: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    }

    async fn fake_s3_write_endpoint(
        State(state): State<FakeWriteState>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
    ) -> Response<Body> {
        if method != Method::PUT || !uri.path().starts_with(&format!("/{}/", state.bucket)) {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("unexpected write request"))
                .expect("build response");
        }
        state
            .conditional_headers
            .lock()
            .expect("lock headers")
            .push(
                headers
                    .get(IF_NONE_MATCH)
                    .map(|value| value.to_str().expect("If-None-Match header is UTF-8"))
                    .map(str::to_string),
            );
        Response::builder()
            .status(StatusCode::OK)
            .header("etag", "\"write-etag\"")
            .body(Body::empty())
            .expect("build response")
    }

    #[tokio::test]
    async fn immutable_append_is_create_only_but_generic_put_remains_mutable() {
        let headers = Arc::new(std::sync::Mutex::new(Vec::new()));
        let bucket = "immutable-write-test".to_string();
        let (endpoint, shutdown) =
            spawn_fake_s3(Router::new().fallback(fake_s3_write_endpoint).with_state(
                FakeWriteState {
                    bucket: bucket.clone(),
                    conditional_headers: headers.clone(),
                },
            ))
            .await;
        let home = test_home(
            bucket,
            "us-east-1",
            endpoint,
            None,
            Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
        )
        .await;

        home.put_object("mutable", b"first".to_vec())
            .await
            .expect("generic mutable put");
        let slot = ObjectSlot::logical("immutable/copy".to_string()).unwrap();
        ExactSlotStorage::create_at(
            &home,
            &slot,
            crate::storage::cloud::BlobBody::from_bytes(b"second".to_vec()),
            &crate::storage::cloud::no_progress(),
        )
        .await
        .expect("immutable append");

        assert_eq!(
            *headers.lock().expect("lock headers"),
            vec![None, Some("*".to_string())]
        );
        assert!(home.exact_slots);
        shutdown.send(()).expect("shut down fake S3");
    }

    #[derive(Clone)]
    struct FakeMultipartState {
        bucket: String,
        completion_headers: Arc<std::sync::Mutex<Vec<Option<String>>>>,
        next_upload: Arc<AtomicUsize>,
        uploaded_parts: Arc<std::sync::Mutex<Vec<(String, usize)>>>,
        aborted_uploads: Arc<std::sync::Mutex<Vec<String>>>,
    }

    async fn fake_s3_multipart_endpoint(
        State(state): State<FakeMultipartState>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response<Body> {
        let path_ok = uri.path().starts_with(&format!("/{}/", state.bucket));
        if method == Method::POST && path_ok && uri.query().is_some_and(|query| query == "uploads")
        {
            let upload_number = state.next_upload.fetch_add(1, Ordering::SeqCst) + 1;
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .body(Body::from(format!(
                    "<InitiateMultipartUploadResult><Bucket>{}</Bucket><Key>object</Key><UploadId>upload-{upload_number}</UploadId></InitiateMultipartUploadResult>", state.bucket
                )))
                .expect("build create response");
        }
        if method == Method::PUT && path_ok {
            let query = uri.query().expect("multipart part query");
            let upload_id = query
                .split('&')
                .find_map(|part| part.strip_prefix("uploadId="))
                .expect("multipart part uploadId")
                .to_string();
            let declared_length: usize = headers
                .get(CONTENT_LENGTH)
                .expect("multipart part Content-Length")
                .to_str()
                .expect("multipart part Content-Length is UTF-8")
                .parse()
                .expect("multipart part Content-Length is an integer");
            assert_eq!(declared_length, body.len());
            state
                .uploaded_parts
                .lock()
                .expect("lock parts")
                .push((upload_id, body.len()));
            return Response::builder()
                .status(StatusCode::OK)
                .header("etag", "part-etag")
                .body(Body::empty())
                .expect("build part response");
        }
        if method == Method::POST
            && path_ok
            && uri
                .query()
                .is_some_and(|query| query.contains("uploadId=upload-"))
        {
            state.completion_headers.lock().expect("lock headers").push(
                headers
                    .get(IF_NONE_MATCH)
                    .map(|value| value.to_str().expect("If-None-Match header is UTF-8"))
                    .map(str::to_string),
            );
            let collision = uri
                .query()
                .is_some_and(|query| query.contains("uploadId=upload-2"));
            return Response::builder()
                .status(if collision {
                    StatusCode::PRECONDITION_FAILED
                } else {
                    StatusCode::OK
                })
                .header("content-type", "application/xml")
                .body(Body::from(if collision {
                    "<Error><Code>PreconditionFailed</Code><Message>exists</Message></Error>"
                } else {
                    "<CompleteMultipartUploadResult><ETag>\"etag\"</ETag></CompleteMultipartUploadResult>"
                }))
                .expect("build complete response");
        }
        if method == Method::DELETE && path_ok {
            let upload_id = uri
                .query()
                .expect("multipart abort query")
                .split('&')
                .find_map(|part| part.strip_prefix("uploadId="))
                .expect("multipart abort uploadId")
                .to_string();
            state
                .aborted_uploads
                .lock()
                .expect("lock aborts")
                .push(upload_id);
            return Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .expect("build abort response");
        }
        Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from(format!(
                "unexpected multipart request: {method} {uri}"
            )))
            .expect("build response")
    }

    #[tokio::test]
    async fn public_immutable_append_streams_parts_and_completes_create_only() {
        let headers = Arc::new(std::sync::Mutex::new(Vec::new()));
        let parts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let aborts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let bucket = "immutable-multipart-test".to_string();
        let (endpoint, shutdown) = spawn_fake_s3(
            Router::new()
                .fallback(fake_s3_multipart_endpoint)
                .with_state(FakeMultipartState {
                    bucket: bucket.clone(),
                    completion_headers: headers.clone(),
                    next_upload: Arc::new(AtomicUsize::new(0)),
                    uploaded_parts: parts.clone(),
                    aborted_uploads: aborts.clone(),
                }),
        )
        .await;
        let home = test_home(
            bucket,
            "us-east-1",
            endpoint,
            None,
            Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
        )
        .await;

        let slot = ObjectSlot::logical("immutable".to_string()).unwrap();
        ExactSlotStorage::create_at(
            &home,
            &slot,
            BlobBody::from_bytes(vec![9; MULTIPART_THRESHOLD + 1]),
            &crate::storage::cloud::no_progress(),
        )
        .await
        .expect("append immutable multipart object");
        let collision = ExactSlotStorage::create_at(
            &home,
            &slot,
            BlobBody::from_bytes(vec![8; MULTIPART_THRESHOLD + 1]),
            &crate::storage::cloud::no_progress(),
        )
        .await
        .expect_err("second immutable append must collide");

        assert!(
            matches!(collision, CloudHomeError::AlreadyExists(_)),
            "{collision}"
        );
        assert_eq!(
            *headers.lock().expect("lock headers"),
            vec![Some("*".to_string()), Some("*".to_string())]
        );
        let parts = parts.lock().expect("lock parts");
        for upload_id in ["upload-1", "upload-2"] {
            let lengths: Vec<_> = parts
                .iter()
                .filter_map(|(id, length)| (id == upload_id).then_some(*length))
                .collect();
            assert!(lengths.contains(&MULTIPART_PART_SIZE), "{parts:?}");
            assert!(lengths.contains(&1), "{parts:?}");
            assert!(
                lengths
                    .iter()
                    .all(|length| matches!(*length, MULTIPART_PART_SIZE | 1)),
                "{parts:?}"
            );
        }
        assert_eq!(
            *aborts.lock().expect("lock aborts"),
            vec!["upload-2".to_string()]
        );
        shutdown.send(()).expect("shut down fake S3");
    }

    async fn fake_s3_body_failure_endpoint(
        State((bucket, remaining_abort_failures)): State<(String, Arc<AtomicUsize>)>,
        method: Method,
        uri: Uri,
        _body: Bytes,
    ) -> Response<Body> {
        let path_ok = uri.path().starts_with(&format!("/{bucket}/"));
        if method == Method::POST && path_ok && uri.query() == Some("uploads") {
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .body(Body::from(format!(
                    "<InitiateMultipartUploadResult><Bucket>{bucket}</Bucket><Key>object</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>"
                )))
                .expect("build create response");
        }
        if method == Method::PUT && path_ok {
            return Response::builder()
                .status(StatusCode::OK)
                .header("etag", "part-1")
                .body(Body::empty())
                .expect("build part response");
        }
        if method == Method::DELETE && path_ok {
            if remaining_abort_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from("injected abort failure"))
                    .expect("build abort failure response");
            }
            return Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .expect("build abort response");
        }
        Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from(format!("unexpected request: {method} {uri}")))
            .expect("build unexpected response")
    }

    async fn fake_s3_cancel_success_endpoint(
        State((bucket, abort_seen)): State<(String, Arc<std::sync::atomic::AtomicBool>)>,
        method: Method,
        uri: Uri,
    ) -> Response<Body> {
        let path_ok = uri.path().starts_with(&format!("/{bucket}/"));
        if method == Method::POST && path_ok && uri.query() == Some("uploads") {
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .body(Body::from(format!(
                    "<InitiateMultipartUploadResult><Bucket>{bucket}</Bucket><Key>object</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>"
                )))
                .expect("build create response");
        }
        if method == Method::DELETE && path_ok {
            abort_seen.store(true, Ordering::SeqCst);
            return Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .expect("build abort response");
        }
        Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from(format!("unexpected request: {method} {uri}")))
            .expect("build unexpected response")
    }

    #[tokio::test]
    async fn multipart_sink_retains_the_s3_runtime_through_abort() {
        let bucket = "immutable-cancel-success-test".to_string();
        let abort_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (endpoint, shutdown) = spawn_fake_s3(
            Router::new()
                .fallback(fake_s3_cancel_success_endpoint)
                .with_state((bucket.clone(), abort_seen.clone())),
        )
        .await;
        let home = test_home(
            bucket,
            "us-east-1",
            endpoint,
            None,
            Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
        )
        .await;
        let sink = home
            .open_multipart_sink("immutable/cancelled", MultipartCompletion::CreateOnly)
            .await
            .unwrap();

        drop(home);
        drop(sink);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !abort_seen.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("multipart owner must abort after its command channel closes");
        shutdown.send(()).expect("shut down fake S3");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn immutable_append_reports_body_and_multipart_abort_failures() {
        let bucket = "immutable-body-failure-test".to_string();
        let abort_failures = Arc::new(AtomicUsize::new(1));
        let (endpoint, shutdown) = spawn_fake_s3(
            Router::new()
                .fallback(fake_s3_body_failure_endpoint)
                .with_state((bucket.clone(), abort_failures.clone())),
        )
        .await;
        let home = test_home(
            bucket,
            "us-east-1",
            endpoint,
            None,
            Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
        )
        .await;
        let reader =
            crate::storage::local_file::PlaintextReader::from_test_reader(FailingBodyReader {
                emitted: false,
            });
        let body = BlobBody::from_test_reader((MULTIPART_PART_SIZE + 1) as u64, reader);

        let slot = ObjectSlot::logical("immutable/body-failure".to_string()).unwrap();
        let error =
            ExactSlotStorage::create_at(&home, &slot, body, &crate::storage::cloud::no_progress())
                .await
                .expect_err("body failure must abort synchronously");

        assert!(
            matches!(error, CloudHomeError::CleanupFailed { .. }),
            "{error}"
        );
        assert!(
            error.to_string().contains("injected body failure"),
            "{error}"
        );
        assert!(error.to_string().contains("abort multipart"), "{error}");
        assert_eq!(abort_failures.load(Ordering::SeqCst), 0);
        shutdown.send(()).expect("shut down fake S3");
    }

    #[tokio::test]
    async fn exact_operations_reject_an_opaque_s3_locator() {
        let home = test_home(
            "exact-locator-test".to_string(),
            "us-east-1",
            "http://127.0.0.1:9".to_string(),
            None,
            Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
        )
        .await;
        let slot = ObjectSlot::opaque("protocol/copy".to_string(), "protocol/other".to_string())
            .expect("build opaque S3 locator");

        let read_error = ExactSlotStorage::read_at(&home, &slot)
            .await
            .expect_err("opaque S3 read must fail");
        assert!(read_error.to_string().contains("must use its logical key"));
        let destination = std::env::temp_dir().join("coven-mismatched-s3-locator");
        let file_error = ExactSlotStorage::read_at_to_file(&home, &slot, &destination)
            .await
            .expect_err("opaque S3 file read must fail");
        assert!(file_error.to_string().contains("must use its logical key"));
        let delete_error = ExactSlotStorage::delete_at(&home, &slot)
            .await
            .expect_err("opaque S3 delete must fail");
        assert!(delete_error
            .to_string()
            .contains("must use its logical key"));
    }

    /// A slot whose physical locator this device built wrong is a fault in the
    /// caller, not a transient failure of the network. Retrying it re-runs the
    /// same deterministic rejection forever.
    #[tokio::test]
    async fn an_opaque_s3_locator_is_not_retryable() {
        let home = test_home(
            "retryability-test".to_string(),
            "us-east-1",
            "http://127.0.0.1:9".to_string(),
            None,
            Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
        )
        .await;
        let slot = ObjectSlot::opaque("protocol/copy".to_string(), "protocol/other".to_string())
            .expect("build opaque S3 locator");

        let error = ExactSlotStorage::read_at(&home, &slot)
            .await
            .expect_err("opaque S3 read must fail");

        assert!(
            !error.is_retryable(),
            "a malformed slot must not be retried: {error}"
        );
    }

    #[tokio::test]
    async fn provider_binding_canonicalizes_the_custom_origin_and_hashes_the_access_key_id() {
        use crate::storage::{ProviderPrincipalId, S3EndpointBinding, StoreProviderBinding};

        let home = test_home(
            "bucket-a".to_string(),
            "us-east-1",
            "https://objects.example:443".to_string(),
            Some("stores/a/".to_string()),
            Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
        )
        .await;

        let binding = ExactSlotStorage::provider_binding(&home)
            .await
            .expect("resolve S3 provider binding");

        assert_eq!(
            binding.store,
            StoreProviderBinding::S3 {
                endpoint: S3EndpointBinding::Custom {
                    origin: "https://objects.example".to_string(),
                },
                region: "us-east-1".to_string(),
                bucket: "bucket-a".to_string(),
                key_prefix: Some("stores/a".to_string()),
            }
        );
        assert_eq!(
            binding.device.principal,
            ProviderPrincipalId::CustomS3Credential {
                access_key_id_hash: s3_access_key_id_hash("access-key"),
            }
        );
    }

    #[tokio::test]
    async fn provider_binding_rejects_a_custom_endpoint_with_a_base_path() {
        let home = test_home(
            "bucket-a".to_string(),
            "us-east-1",
            "https://objects.example/s3".to_string(),
            None,
            Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
        )
        .await;

        let error = ExactSlotStorage::provider_binding(&home)
            .await
            .expect_err("a non-origin custom endpoint cannot be signed as an origin");

        assert!(error.to_string().contains("origin"), "{error}");
    }

    #[test]
    fn sts_transport_failure_remains_retryable_transport() {
        let error = sts_request_error("offline");
        assert!(matches!(error, CloudHomeError::Transport(_)));
        assert!(error.is_retryable());
    }

    #[test]
    fn sts_identity_accepts_stable_aws_principals_and_rejects_federated_users() {
        use crate::storage::AwsPrincipal;

        assert_eq!(
            aws_caller_identity(
                "123456789012",
                "arn:aws:iam::123456789012:user/path/alice",
                "AIDAEXAMPLE"
            )
            .unwrap(),
            (
                "aws".to_string(),
                AwsPrincipal::User {
                    arn: "arn:aws:iam::123456789012:user/path/alice".to_string(),
                    user_id: "AIDAEXAMPLE".to_string(),
                }
            )
        );
        assert_eq!(
            aws_caller_identity(
                "123456789012",
                "arn:aws:sts::123456789012:assumed-role/path/role/session",
                "AROAEXAMPLE:session"
            )
            .unwrap()
            .1,
            AwsPrincipal::Role {
                role_id: "AROAEXAMPLE".to_string()
            }
        );
        assert!(aws_caller_identity(
            "123456789012",
            "arn:aws:sts::123456789012:federated-user/alice",
            "123456789012:alice"
        )
        .is_err());
    }

    #[test]
    fn cancellation_abort_failure_does_not_terminate_the_process() {
        const CHILD: &str = "COVEN_S3_CANCEL_ABORT_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async {
                let bucket = "immutable-cancel-failure-test".to_string();
                let abort_failures = Arc::new(AtomicUsize::new(1));
                let (endpoint, shutdown) = spawn_fake_s3(
                    Router::new()
                        .fallback(fake_s3_body_failure_endpoint)
                        .with_state((bucket.clone(), abort_failures.clone())),
                )
                .await;
                let home = test_home(
                    bucket,
                    "us-east-1",
                    endpoint,
                    None,
                    Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
                )
                .await;
                let sink = home
                    .open_multipart_sink("immutable/cancelled", MultipartCompletion::CreateOnly)
                    .await
                    .unwrap();
                drop(sink);
                tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    while abort_failures.load(Ordering::SeqCst) != 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("multipart owner must finish the abort request");
                shutdown.send(()).expect("shut down fake S3");
            });
            std::process::exit(0);
        }

        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("cancellation_abort_failure_does_not_terminate_the_process")
            .arg("--nocapture")
            .env(CHILD, "1")
            .status()
            .expect("run S3 cancellation sabotage subprocess");
        assert!(
            status.success(),
            "multipart abort failure terminated the subprocess: {status}"
        );
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

        let home = test_home(bucket, "us-central1", endpoint, None, None).await;

        let bytes = home
            .read_range(&key, 0, range_body.len() as u64)
            .await
            .expect("read range");

        assert_eq!(bytes, range_body);
        shutdown.send(()).expect("shut down fake S3");
    }

    /// A provider that ignores `Range` and answers a ranged GET with 200 and the
    /// whole object returns offset-0 bytes where a mid-file range was asked for —
    /// silent corruption on a plaintext home. The range read must reject the
    /// mismatched body, not return the wrong bytes.
    #[tokio::test]
    async fn read_range_rejects_full_object_200_response() {
        let full_body = b"0123456789abcdefghijklmnopqrstuvwxyz".to_vec();
        let key = "storage/audio-object".to_string();
        let bucket = "coven-s3-ignores-range".to_string();
        let (endpoint, shutdown) = spawn_fake_s3_full_body_endpoint(FakeFullBodyObject {
            bucket: bucket.clone(),
            key: key.clone(),
            full_body,
        })
        .await;

        let home = test_home(bucket, "us-east-1", endpoint, None, None).await;

        let err = home
            .read_range(&key, 8, 16)
            .await
            .expect_err("a 200 full-object response to a range request must error");
        assert!(matches!(err, CloudHomeError::Transport(_)), "got {err:?}");
        shutdown.send(()).expect("shut down fake S3");
    }

    #[tokio::test]
    async fn exact_read_streams_object_to_file() {
        let full_body = b"0123456789abcdefghijklmnopqrstuvwxyz".to_vec();
        let key = "storage/audio-object".to_string();
        let bucket = "coven-s3-appended-read".to_string();
        let (endpoint, shutdown) = spawn_fake_s3_full_body_endpoint(FakeFullBodyObject {
            bucket: bucket.clone(),
            key: key.clone(),
            full_body: full_body.clone(),
        })
        .await;

        let home = test_home(bucket, "us-east-1", endpoint, None, None).await;
        let tmp = tempfile::tempdir().expect("temp dir");
        let destination = tmp.path().join("object.bin");
        let slot = ObjectSlot::logical(key).unwrap();

        ExactSlotStorage::read_at_to_file(&home, &slot, &destination)
            .await
            .expect("stream exact object");

        assert_eq!(
            tokio::fs::read(&destination)
                .await
                .expect("read destination"),
            full_body
        );
        shutdown.send(()).expect("shut down fake S3");
    }

    #[tokio::test]
    async fn canceling_exact_read_cannot_rename_over_destination_later() {
        let key = "storage/cancel-object".to_string();
        let bucket = "coven-s3-cancel-read".to_string();
        let first = b"partial".to_vec();
        let first_sent = Arc::new(tokio::sync::Notify::new());
        let release_second = Arc::new(tokio::sync::Notify::new());
        let (endpoint, shutdown) = spawn_fake_s3_paused_body_endpoint(FakePausedBodyObject {
            bucket: bucket.clone(),
            key: key.clone(),
            first: first.clone(),
            second: b" remainder".to_vec(),
            first_sent: first_sent.clone(),
            release_second: release_second.clone(),
        })
        .await;

        let home = test_home(bucket, "us-east-1", endpoint, None, None).await;
        let tmp = tempfile::tempdir().expect("temp dir");
        let destination = tmp.path().join("object.bin");
        tokio::fs::write(&destination, b"committed")
            .await
            .expect("seed destination");
        let slot = ObjectSlot::logical(key).unwrap();
        let read_destination = destination.clone();
        let read = tokio::spawn(async move {
            ExactSlotStorage::read_at_to_file(&home, &slot, &read_destination).await
        });
        first_sent.notified().await;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if atomic_temp_paths(tmp.path()).await.iter().any(|path| {
                    std::fs::metadata(path)
                        .is_ok_and(|metadata| metadata.len() == first.len() as u64)
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first response chunk was written to the temp file");

        read.abort();
        assert!(read.await.expect_err("read task canceled").is_cancelled());
        release_second.notify_waiters();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if atomic_temp_paths(tmp.path()).await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("canceled S3 task removed its temp file");

        assert_eq!(
            tokio::fs::read(&destination)
                .await
                .expect("read destination"),
            b"committed"
        );
        shutdown.send(()).expect("shut down fake S3");
    }

    #[tokio::test]
    async fn list_errors_when_truncated_response_has_no_continuation_token() {
        let bucket = "coven-s3-list-test".to_string();
        let (endpoint, shutdown, request_count) =
            spawn_fake_s3_truncated_list_endpoint(bucket.clone()).await;

        let home = test_home(bucket, "us-central1", endpoint, None, None).await;

        let result = tokio::time::timeout(std::time::Duration::from_secs(1), home.list("objects/"))
            .await
            .expect("list should return instead of refetching the first page");
        let err = result.expect_err("truncated response without token must fail");
        let msg = err.to_string();

        assert!(
            msg.contains("truncated") && msg.contains("continuation token"),
            "unexpected error: {msg}"
        );
        assert_eq!(
            request_count.load(Ordering::SeqCst),
            1,
            "malformed page must not be refetched"
        );
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn s3_revoke_access_reports_unsupported() {
        let home = test_home(
            "bucket".to_string(),
            "us-east-1",
            "http://127.0.0.1:9".to_string(),
            None,
            None,
        )
        .await;

        let outcome = home
            .set_access(CloudAccessState::Absent {
                member_pubkey: "member-pubkey".to_string(),
                provider_account_email: None,
            })
            .await
            .expect("S3 revoke_access must not error so member removal completes");

        assert_eq!(
            outcome,
            CloudAccessOutcome::Absent(RevokeOutcome::Unsupported),
            "S3 hands out one static bucket credential that cannot be withdrawn per member, so it reports Unsupported rather than claiming a revocation it did not perform",
        );
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

    impl TestCreds {
        fn from_env() -> Self {
            Self {
                endpoint: test_env("COVEN_TEST_S3_URL", "http://localhost:19000"),
                access_key: test_env("COVEN_TEST_S3_KEY", "coventest"),
                secret_key: test_env("COVEN_TEST_S3_SECRET", "coventestpass"),
            }
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

    impl ExistingS3ObjectEnv {
        fn from_env() -> Option<Self> {
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
            Some(Self {
                bucket,
                region,
                endpoint,
                key,
                access_key,
                secret_key,
            })
        }
    }

    #[tokio::test]
    #[ignore]
    async fn read_range_succeeds_against_existing_s3_object() {
        let creds = TestCreds::from_env();
        let bucket = required_test_env("COVEN_TEST_S3_BUCKET");
        let region = test_env("COVEN_TEST_S3_REGION", "us-east-1");
        let key = required_test_env("COVEN_TEST_S3_EXISTING_KEY");
        let start: u64 = test_env("COVEN_TEST_S3_RANGE_START", "0")
            .parse()
            .expect("COVEN_TEST_S3_RANGE_START must be a u64");
        let end: u64 = test_env("COVEN_TEST_S3_RANGE_END", "24")
            .parse()
            .expect("COVEN_TEST_S3_RANGE_END must be a u64");

        let home = open_cloud_home(
            bucket,
            region,
            Some(creds.endpoint),
            creds.access_key,
            creds.secret_key,
            None,
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

    /// Proves an `S3CloudHome`'s AWS calls run end to end on its retained
    /// runtime—a different runtime than the `Client` was built on—against a real bucket:
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
        let Some(env) = ExistingS3ObjectEnv::from_env() else {
            return;
        };

        let home = open_cloud_home(
            env.bucket,
            env.region,
            Some(env.endpoint),
            env.access_key,
            env.secret_key,
            None,
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

    #[tokio::test]
    #[ignore]
    async fn probe_succeeds_against_existing_bucket() {
        let creds = TestCreds::from_env();
        let bucket = format!("coven-probe-ok-{}", uuid::Uuid::new_v4());
        let home = open_cloud_home(
            bucket,
            "us-east-1".to_string(),
            Some(creds.endpoint),
            creds.access_key,
            creds.secret_key,
            None,
            None,
        )
        .await
        .expect("construct S3CloudHome");
        home.provision_test_bucket().await;
        home.probe().await.expect("probe should succeed");
    }

    #[tokio::test]
    #[ignore]
    async fn probe_fails_for_missing_bucket() {
        let creds = TestCreds::from_env();
        let bucket = format!("coven-probe-missing-{}", uuid::Uuid::new_v4());
        let home = open_cloud_home(
            bucket.clone(),
            "us-east-1".to_string(),
            Some(creds.endpoint),
            creds.access_key,
            creds.secret_key,
            None,
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
        let creds = TestCreds::from_env();
        let bucket = format!("coven-probe-badkey-{}", uuid::Uuid::new_v4());
        // Provision the bucket with the good creds so the only difference is the bad secret.
        let good = open_cloud_home(
            bucket.clone(),
            "us-east-1".to_string(),
            Some(creds.endpoint.clone()),
            creds.access_key.clone(),
            creds.secret_key,
            None,
            None,
        )
        .await
        .expect("construct good S3CloudHome");
        good.provision_test_bucket().await;

        let bad = open_cloud_home(
            bucket,
            "us-east-1".to_string(),
            Some(creds.endpoint),
            creds.access_key,
            "wrong-secret".to_string(),
            None,
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
