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
    CloudHome, CloudHomeError, CloudHomeJoinInfo, ExactSlotStorage, MultipartUpload, RevokeOutcome,
    UploadProgress,
};
use coven_foundation::id_provider::{IdRef, UuidProvider};
use coven_protocol::objects::ObjectSlot;
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
    exact_upload_verification: coven_foundation::config::ExactUploadVerification,
    ids: IdRef,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct S3ExactMetadata {
    size: u64,
    sha256: String,
}

fn sha256_base64(hash: coven_protocol::store_commit::ObjectHash) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(hash.as_bytes())
}

fn sha256_bytes_base64(bytes: &[u8]) -> String {
    use base64::Engine as _;
    use sha2::{Digest, Sha256};
    base64::engine::general_purpose::STANDARD.encode(Sha256::digest(bytes))
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

fn checksum_put_failed(
    error: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
) -> bool {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    matches!(
        error.code(),
        Some("BadDigest" | "InvalidDigest" | "XAmzContentSHA256Mismatch")
    )
}

enum S3CreateOnlyPutError {
    AlreadyExists(String),
    ChecksumRejected(CloudHomeError),
    Other(CloudHomeError),
}

impl S3CreateOnlyPutError {
    fn into_cloud_error(self) -> CloudHomeError {
        match self {
            Self::AlreadyExists(key) => CloudHomeError::AlreadyExists(key),
            Self::ChecksumRejected(error) => error,
            Self::Other(error) => error,
        }
    }
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
        exact_upload_verification: coven_foundation::config::ExactUploadVerification,
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
            exact_upload_verification,
            ids: std::sync::Arc::new(UuidProvider),
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
        exact_sha256: Option<String>,
    ) -> Result<Box<S3PartSink>, CloudHomeError> {
        let full = self.full_key(key);
        let uses_checksum = exact_sha256.is_some();
        let upload_id = {
            let key = key.to_string();
            let full = full.clone();
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            self.runtime
                .run(move || async move {
                    let mut request = client.create_multipart_upload().bucket(&bucket).key(&full);
                    if uses_checksum {
                        request = request
                            .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Sha256)
                            .checksum_type(aws_sdk_s3::types::ChecksumType::FullObject);
                    }
                    let create = request.send().await.map_err(|error| {
                        CloudHomeError::transport(format!("multipart create {key}"), error)
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
            exact_sha256,
        };
        Ok(Box::new(S3PartSink {
            commands: Some(commands),
            owner: Some(self.runtime.spawn(move || owner.run(receiver))),
        }))
    }

    async fn put_create_only_raw(
        &self,
        key: &str,
        data: Vec<u8>,
        checksum_sha256: Option<String>,
    ) -> Result<(), S3CreateOnlyPutError> {
        let full = self.full_key(key);
        let logical_key = key.to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime
            .spawn(move || async move {
                let mut request = client
                    .put_object()
                    .bucket(&bucket)
                    .key(&full)
                    .if_none_match("*")
                    .body(data.into());
                if let Some(checksum) = checksum_sha256 {
                    request = request.checksum_sha256(checksum);
                }
                request.send().await.map_err(|error| {
                    if create_only_put_failed(&error) {
                        S3CreateOnlyPutError::AlreadyExists(logical_key.clone())
                    } else if checksum_put_failed(&error) {
                        S3CreateOnlyPutError::ChecksumRejected(CloudHomeError::transport(
                            format!("S3 rejected the SHA-256 request checksum for {logical_key}"),
                            error,
                        ))
                    } else {
                        S3CreateOnlyPutError::Other(put_object_error(&logical_key, error))
                    }
                })?;
                Ok(())
            })
            .await
            .map_err(|error| {
                S3CreateOnlyPutError::Other(CloudHomeError::transport("run S3 task", error))
            })?
    }

    async fn put_create_only(
        &self,
        key: &str,
        data: Vec<u8>,
        checksum_sha256: Option<String>,
    ) -> Result<(), CloudHomeError> {
        self.put_create_only_raw(key, data, checksum_sha256)
            .await
            .map_err(S3CreateOnlyPutError::into_cloud_error)
    }

    async fn append_create_only(
        &self,
        key: &str,
        body: BlobBody,
        exact_sha256: Option<String>,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        if body.len() <= self.multipart_threshold() {
            let data = body.collect().await?;
            let length = data.len() as u64;
            self.put_create_only(key, data, exact_sha256).await?;
            progress(length);
            return Ok(());
        }
        let sink = self
            .open_multipart_sink(key, MultipartCompletion::CreateOnly, exact_sha256)
            .await?;
        MultipartUpload::new(key, body, sink, progress).run().await
    }

    /// HeadBucket — cheap auth + existence check, no listing cost.
    async fn create_at_slot(
        &self,
        slot: &ObjectSlot,
        body: BlobBody,
        exact_sha256: Option<String>,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        slot.require_logical_key_for("S3")
            .map_err(CloudHomeError::from)?;
        self.append_create_only(slot.logical_key(), body, exact_sha256, progress)
            .await
    }

    async fn read_exact_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
    ) -> Result<(), super::CloudFileReadError> {
        slot.require_logical_key_for("S3")
            .map_err(|error| super::CloudFileReadError::Source(CloudHomeError::from(error)))?;
        let full = self.full_key(slot.logical_key());
        let key = slot.logical_key().to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let destination = destination.to_path_buf();
        self.runtime
            .run_file_read(move || async move {
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

    async fn exact_metadata(&self, slot: &ObjectSlot) -> Result<S3ExactMetadata, CloudHomeError> {
        slot.require_logical_key_for("S3")?;
        let full = self.full_key(slot.logical_key());
        let key = slot.logical_key().to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime
            .run(move || async move {
                use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
                let response = client
                    .head_object()
                    .bucket(&bucket)
                    .key(&full)
                    .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
                    .send()
                    .await
                    .map_err(|error| {
                        let status = match &error {
                            SdkError::ServiceError(service) => {
                                Some(service.raw().status().as_u16())
                            }
                            _ => None,
                        };
                        if is_not_found_code(error.code()) || status == Some(404) {
                            CloudHomeError::NotFound(key.clone())
                        } else {
                            CloudHomeError::transport(format!("head exact S3 object {key}"), error)
                        }
                    })?;
                let size = response
                    .content_length()
                    .and_then(|size| u64::try_from(size).ok())
                    .ok_or_else(|| {
                        CloudHomeError::Transport(format!(
                            "head exact S3 object {key}: missing content length"
                        ))
                    })?;
                let sha256 = response
                    .checksum_sha256()
                    .filter(|checksum| !checksum.is_empty())
                    .ok_or_else(|| {
                        CloudHomeError::Transport(format!(
                            "head exact S3 object {key}: missing SHA-256 checksum"
                        ))
                    })?
                    .to_string();
                Ok(S3ExactMetadata { size, sha256 })
            })
            .await
    }

    async fn verify_exact_upload(
        &self,
        upload: &super::ExactUpload<'_>,
        created_response_was_observed: bool,
    ) -> Result<(), CloudHomeError> {
        use coven_foundation::config::ExactUploadVerification;

        match self.exact_upload_verification {
            ExactUploadVerification::UploadChecksum if created_response_was_observed => Ok(()),
            ExactUploadVerification::UploadChecksum | ExactUploadVerification::MetadataHash => {
                let metadata = self.exact_metadata(upload.object().slot()).await?;
                if metadata.size != upload.object().stored_size()
                    || metadata.sha256 != sha256_base64(upload.object().stored_hash())
                {
                    return Err(CloudHomeError::SlotCollision(
                        upload.object().slot().logical_key().to_string(),
                    ));
                }
                Ok(())
            }
            ExactUploadVerification::Readback => {
                let bytes = self.read_at(upload.object().slot()).await?;
                upload.verify_stored_bytes(&bytes)
            }
            ExactUploadVerification::Unchecked => {
                super::exact_upload::accept_unchecked_create_response(
                    created_response_was_observed,
                    upload.object(),
                )
            }
        }
    }

    async fn probe_exact_slots(&self) -> Result<(), CloudHomeError> {
        use coven_foundation::config::ExactUploadVerification;

        let suffix = self.ids.new_id();
        let key = format!("__coven_probe__/exact-{suffix}");
        let bad_key = format!("__coven_probe__/bad-checksum-{suffix}");
        let bytes = b"coven exact-slot checksum probe".to_vec();
        let checksum = sha256_bytes_base64(&bytes);
        let sends_checksum = matches!(
            self.exact_upload_verification,
            ExactUploadVerification::UploadChecksum | ExactUploadVerification::MetadataHash
        );
        let operation = async {
            self.put_create_only(
                &key,
                bytes.clone(),
                sends_checksum.then(|| checksum.clone()),
            )
            .await?;

            match self
                .put_create_only(
                    &key,
                    bytes.clone(),
                    sends_checksum.then(|| checksum.clone()),
                )
                .await
            {
                Err(CloudHomeError::AlreadyExists(_)) => {}
                Ok(()) => {
                    return Err(CloudHomeError::Configuration(
                        "S3 endpoint did not enforce If-None-Match on exact-slot creation"
                            .to_string(),
                    ));
                }
                Err(error) => return Err(error),
            }

            match self.exact_upload_verification {
                ExactUploadVerification::UploadChecksum => {
                    let wrong = sha256_bytes_base64(b"different bytes");
                    match self
                        .put_create_only_raw(&bad_key, bytes.clone(), Some(wrong))
                        .await
                    {
                        Err(S3CreateOnlyPutError::ChecksumRejected(_)) => {}
                        Ok(()) => {
                            return Err(CloudHomeError::Configuration(
                                "S3 endpoint accepted an object whose SHA-256 request checksum was wrong"
                                    .to_string(),
                            ));
                        }
                        Err(error) => return Err(error.into_cloud_error()),
                    }
                }
                ExactUploadVerification::MetadataHash => {
                    let slot = ObjectSlot::logical(key.clone())?;
                    let metadata = self.exact_metadata(&slot).await?;
                    if metadata.size != bytes.len() as u64 || metadata.sha256 != checksum {
                        return Err(CloudHomeError::Configuration(
                            "S3 endpoint did not return the uploaded SHA-256 through HeadObject"
                                .to_string(),
                        ));
                    }
                }
                ExactUploadVerification::Readback => {
                    if self.read(&key).await? != bytes {
                        return Err(CloudHomeError::Configuration(
                            "S3 exact-slot readback returned different bytes".to_string(),
                        ));
                    }
                }
                ExactUploadVerification::Unchecked => {}
            }
            Ok(())
        }
        .await;

        let cleanup_key = self.delete(&key).await;
        let cleanup_bad = self.delete(&bad_key).await;
        let cleanup = match (cleanup_key, cleanup_bad) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(first), Err(second)) => Err(CloudHomeError::CleanupFailed {
                operation: Box::new(first),
                cleanup: Box::new(second),
            }),
        };
        match (operation, cleanup) {
            (Ok(()), cleanup) => cleanup,
            (Err(operation), Ok(())) => Err(operation),
            (Err(operation), Err(cleanup)) => Err(combine_cleanup_failure(operation, Err(cleanup))),
        }
    }

    #[cfg(test)]
    async fn provision_test_bucket(&self) {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime
            .run(move || async move {
                client
                    .create_bucket()
                    .bucket(&bucket)
                    .send()
                    .await
                    .map_err(|error| {
                        CloudHomeError::transport(format!("create test S3 bucket {bucket}"), error)
                    })?;
                Ok(())
            })
            .await
            .expect("create test bucket");
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn open_cloud_home(
    bucket: String,
    region: String,
    endpoint: Option<String>,
    access_key: String,
    secret_key: String,
    key_prefix: Option<String>,
    exact_upload_verification: coven_foundation::config::ExactUploadVerification,
) -> Result<S3CloudHome, CloudHomeError> {
    let runtime = S3Runtime::new()?;
    let home_runtime = runtime.clone();
    runtime
        .run(move || {
            S3CloudHome::new(
                home_runtime,
                bucket,
                region,
                endpoint,
                access_key,
                secret_key,
                key_prefix,
                exact_upload_verification,
            )
        })
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
    exact_sha256: Option<String>,
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
        let part_sha256 = self
            .exact_sha256
            .as_ref()
            .map(|_| sha256_bytes_base64(&part));
        let mut request = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .part_number(part_number)
            .body(part.into());
        if let Some(checksum) = part_sha256.as_ref() {
            request = request.checksum_sha256(checksum);
        }
        let uploaded = request.send().await.map_err(|error| {
            CloudHomeError::transport(
                format!("upload multipart part {part_number} for {}", self.key),
                error,
            )
        })?;
        let mut completed = aws_sdk_s3::types::CompletedPart::builder()
            .part_number(part_number)
            .set_e_tag(uploaded.e_tag().map(str::to_string));
        if let Some(checksum) = part_sha256 {
            completed = completed.checksum_sha256(checksum);
        }
        self.completed.push(completed.build());
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
                CloudHomeError::transport(format!("abort multipart {}", self.key), error)
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
        let mut request = match self.completion {
            MultipartCompletion::Mutable => request,
            MultipartCompletion::CreateOnly => request.if_none_match("*"),
        };
        if let Some(checksum) = self.exact_sha256.as_ref() {
            request = request
                .checksum_sha256(checksum)
                .checksum_type(aws_sdk_s3::types::ChecksumType::FullObject);
        }
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
                CloudHomeError::transport(format!("complete multipart {}", self.key), error)
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
            CloudHomeError::transport("S3 multipart owner task failed".to_string(), error)
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

fn s3_access_key_id_hash(access_key_id: &str) -> coven_protocol::store_commit::ObjectHash {
    const DOMAIN: &[u8] = b"coven.s3-access-key-id.v1\0";
    let mut material = Vec::with_capacity(DOMAIN.len() + access_key_id.len());
    material.extend_from_slice(DOMAIN);
    material.extend_from_slice(access_key_id.as_bytes());
    coven_protocol::store_commit::ObjectHash::digest(&material)
}

fn aws_caller_identity(
    account_id: &str,
    arn: &str,
    user_id: &str,
) -> Result<(String, coven_protocol::objects::AwsPrincipal), CloudHomeError> {
    use coven_protocol::objects::AwsPrincipal;

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

fn sts_request_error(error: impl std::error::Error + Send + Sync + 'static) -> CloudHomeError {
    CloudHomeError::transport("request STS caller identity", error)
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
    E: std::error::Error + Send + Sync + 'static,
{
    CloudHomeError::transport(format!("{context} for {key}"), err)
}

/// Map a GetObject failure to a `CloudHomeError`, surfacing the S3 error code and
/// message (e.g. `AccessDenied`, `PermanentRedirect`, `SignatureDoesNotMatch`)
/// rather than the opaque "service error". `NoSuchKey` becomes `NotFound`;
/// non-service failures (timeouts, connection errors) fall back to their own
/// description. Generic over the response type so it serves both `read` and
/// `read_range` without naming the smithy HTTP type.
fn get_object_error<R: std::fmt::Debug + Send + Sync + 'static>(
    key: &str,
    err: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError, R>,
) -> CloudHomeError {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    match err.code() {
        Some("NoSuchKey") => CloudHomeError::NotFound(key.to_string()),
        Some(code) => CloudHomeError::transport(
            match err.message() {
                Some(msg) => format!("get {key}: S3 {code}: {msg}"),
                None => format!("get {key}: S3 {code} (no message provided)"),
            },
            err,
        ),
        // Not a service error (timeout / connection / dispatch) — its own
        // Display carries the detail.
        None => CloudHomeError::transport(format!("get {key}"), err),
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
        Some("AccessDenied") => CloudHomeError::configuration(
            "write S3 object; credentials do not permit writing to this bucket",
            err,
        ),
        Some("NoSuchBucket") => {
            CloudHomeError::configuration("write S3 object; bucket does not exist", err)
        }
        Some("OverQuota" | "QuotaExceeded") => {
            CloudHomeError::configuration("write S3 object; storage quota is exceeded", err)
        }
        Some(code) => CloudHomeError::transport(
            match err.message() {
                Some(msg) => format!("put {key}: S3 {code}: {msg}"),
                None => format!("put {key}: S3 {code} (no message provided)"),
            },
            err,
        ),
        None => CloudHomeError::transport(format!("put {key}"), err),
    }
}

#[async_trait]
impl CloudHome for S3CloudHome {
    async fn probe(&self) -> Result<(), CloudHomeError> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime
            .run(move || async move {
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
                    Err(e) => Err(CloudHomeError::transport("probe S3", e)),
                }
            })
            .await?;
        self.probe_exact_slots().await
    }

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let full = self.full_key(key);
        let key = key.to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime
            .run(move || async move {
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
            .open_multipart_sink(key, MultipartCompletion::Mutable, None)
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
            .run(move || async move {
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
            .run(move || async move {
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
            .run(move || async move {
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
                        .map_err(|e| CloudHomeError::transport(format!("list {prefix}"), e))?;

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
            .run(move || async move {
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
                        return Err(CloudHomeError::transport(format!("delete {key}"), e));
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
            .run(move || async move {
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
                            Err(CloudHomeError::transport(format!("head {key}"), e))
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
    ) -> Result<coven_protocol::objects::ResolvedProviderBinding, CloudHomeError> {
        use coven_protocol::objects::{
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
                    .run(move || async move {
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
                    origin: coven_protocol::provider::canonical_custom_s3_origin(endpoint)
                        .map_err(|error| {
                            CloudHomeError::configuration("validate custom S3 endpoint", error)
                        })?,
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
        binding.validate().map_err(|error| {
            CloudHomeError::configuration("validate S3 provider binding", error)
        })?;
        Ok(binding)
    }

    async fn create_at(
        &self,
        upload: &super::ExactUpload<'_>,
        progress: &UploadProgress<'_>,
    ) -> Result<super::ExactCreateOutcome, CloudHomeError> {
        let checksum = matches!(
            self.exact_upload_verification,
            coven_foundation::config::ExactUploadVerification::UploadChecksum
                | coven_foundation::config::ExactUploadVerification::MetadataHash
        )
        .then(|| sha256_base64(upload.object().stored_hash()));
        let operation = S3CloudHome::create_at_slot(
            self,
            upload.object().slot(),
            upload.body().await?,
            checksum,
            progress,
        )
        .await;
        super::exact_upload::settle_exact_create(operation, |observed| {
            self.verify_exact_upload(upload, observed)
        })
        .await
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
mod tests;
