//! S3-backed `CloudHome` implementation.
//!
//! Wraps `aws-sdk-s3` to provide raw storage operations against any
//! S3-compatible endpoint.

use async_trait::async_trait;
use aws_config::stalled_stream_protection::StalledStreamProtectionConfig;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::config::{RequestChecksumCalculation, ResponseChecksumValidation};
use aws_sdk_s3::error::ProvideErrorMetadata as _;
use aws_sdk_s3::Client;
use std::fmt;
use tracing::warn;

mod google_cloud_storage;

use google_cloud_storage::{GoogleCloudStorageXml, GoogleUploadSource};

use super::runtime::CloudRuntime;
use super::s3_common::{
    apply_prefix, is_not_found_code, normalize_prefix, strip_listed_key_prefix,
};
use super::{
    combine_cleanup_failure, range_header, BlobBody, CloudAccessOutcome, CloudAccessState,
    CloudHome, CloudHomeError, CloudHomeJoinInfo, ExactSlotStorage, MultipartUpload, RevokeOutcome,
    UploadControl,
};
use coven_foundation::id_provider::{IdRef, UuidProvider};
use coven_protocol::objects::{ObjectSlot, StorageBackendFailure};

/// S3-backed cloud home.
#[derive(Clone)]
pub struct S3CloudHome {
    runtime: CloudRuntime,
    client: Client,
    sts_client: Option<aws_sdk_sts::Client>,
    bucket: String,
    region: String,
    endpoint: Option<String>,
    access_key: String,
    secret_key: String,
    key_prefix: Option<String>,
    google_xml: Option<GoogleCloudStorageXml>,
    clock: coven_foundation::clock::ClockRef,
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
    let status = match error {
        aws_sdk_s3::error::SdkError::ServiceError(service) => Some(service.raw().status().as_u16()),
        _ => None,
    };
    status == Some(412)
        || matches!(
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
        runtime: CloudRuntime,
        bucket: String,
        region: String,
        endpoint: Option<String>,
        access_key: String,
        secret_key: String,
        key_prefix: Option<String>,
        exact_upload_verification: coven_foundation::config::ExactUploadVerification,
        clock: coven_foundation::clock::ClockRef,
    ) -> Result<Self, CloudHomeError> {
        for (name, value) in [
            ("bucket", bucket.as_str()),
            ("region", region.as_str()),
            ("access key", access_key.as_str()),
            ("secret key", secret_key.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(CloudHomeError::Configuration(format!(
                    "S3 {name} must not be empty"
                )));
            }
        }
        let endpoint = endpoint
            .map(|endpoint| {
                coven_protocol::provider::canonical_custom_s3_origin(&endpoint).map_err(|error| {
                    CloudHomeError::configuration("validate custom S3 endpoint", error)
                })
            })
            .transpose()?;
        let google_xml = GoogleCloudStorageXml::for_endpoint(endpoint.as_deref())?;
        let credentials =
            Credentials::new(&access_key, &secret_key, None, None, "coven-cloud-home");

        // aws-config has default-features disabled, so the SDK needs an
        // explicit HTTP client. Coven uses reqwest for every provider; its
        // rustls backend delegates certificate decisions to the host platform,
        // including Android's initialized TrustManager.
        let http_client = smithy_transport_reqwest::ReqwestHttpClient::new();

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
            builder = builder.endpoint_url(ep);
        }

        let aws_config = builder.load().await;
        let s3_builder = aws_sdk_s3::config::Builder::from(&aws_config)
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
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired);
        let s3_config = s3_builder.build();
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
            google_xml,
            clock,
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
                .run_cloud(move || async move {
                    let mut request = client.create_multipart_upload().bucket(&bucket).key(&full);
                    if uses_checksum {
                        request = request
                            .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Sha256)
                            .checksum_type(aws_sdk_s3::types::ChecksumType::FullObject);
                    }
                    let create = request.send().await.map_err(|error| {
                        s3_operation_error(format!("multipart create {key}"), error)
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
            owner: Some(
                self.runtime
                    .spawn(move || owner.run(receiver))
                    .map_err(|error| {
                        CloudHomeError::transport("start S3 multipart owner", error)
                    })?,
            ),
        }))
    }

    async fn put_create_only_raw(
        &self,
        key: &str,
        data: Vec<u8>,
        checksum_sha256: Option<String>,
        control: UploadControl,
    ) -> Result<(), S3CreateOnlyPutError> {
        let full = self.full_key(key);
        let logical_key = key.to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let google_xml = self.google_xml.clone();
        let endpoint = self.endpoint.clone();
        let region = self.region.clone();
        let access_key = self.access_key.clone();
        let secret_key = self.secret_key.clone();
        let now = self.clock.now();
        self.runtime
            .run(move || async move {
                if let Some(google_xml) = google_xml {
                    let endpoint = endpoint.ok_or_else(|| {
                        S3CreateOnlyPutError::Other(CloudHomeError::Configuration(
                            "Google Cloud Storage XML endpoint is absent".to_string(),
                        ))
                    })?;
                    let size = data.len() as u64;
                    let payload_hash = hex::encode(
                        coven_protocol::store_commit::ObjectHash::digest(&data).as_bytes(),
                    );
                    return google_xml
                        .create_only(
                            &endpoint,
                            &bucket,
                            &region,
                            &access_key,
                            &secret_key,
                            &full,
                            GoogleUploadSource::Bytes(data),
                            size,
                            &payload_hash,
                            now,
                            control,
                        )
                        .await
                        .map_err(|error| match error {
                            CloudHomeError::AlreadyExists(_) => {
                                S3CreateOnlyPutError::AlreadyExists(logical_key)
                            }
                            error => S3CreateOnlyPutError::Other(error),
                        });
                }
                let content_length = i64::try_from(data.len()).map_err(|_| {
                    S3CreateOnlyPutError::Other(CloudHomeError::Transport(format!(
                        "object {logical_key} exceeds S3's content-length range"
                    )))
                })?;
                let body =
                    reqwest::Body::wrap_stream(control.stream_part(bytes::Bytes::from(data), 0));
                let body = aws_sdk_s3::primitives::ByteStream::new(
                    aws_sdk_s3::primitives::SdkBody::from_body_1_x(body),
                );
                let mut request = client
                    .put_object()
                    .bucket(&bucket)
                    .key(&full)
                    .content_length(content_length)
                    .body(body);
                if let Some(checksum) = checksum_sha256 {
                    request = request.checksum_sha256(checksum);
                }
                let result = request.if_none_match("*").send().await;
                result.map_err(|error| {
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
                S3CreateOnlyPutError::Other(CloudHomeError::transport(
                    "run S3 create-only operation",
                    error,
                ))
            })?
    }

    async fn put_google_exact_create_only(
        &self,
        key: &str,
        source: GoogleUploadSource,
        size: u64,
        payload_hash: String,
        control: UploadControl,
    ) -> Result<(), CloudHomeError> {
        let full = self.full_key(key);
        let google_xml = self.google_xml.clone().ok_or_else(|| {
            CloudHomeError::Configuration(
                "Google Cloud Storage exact creator is absent".to_string(),
            )
        })?;
        let endpoint = self.endpoint.clone().ok_or_else(|| {
            CloudHomeError::Configuration("Google Cloud Storage endpoint is absent".to_string())
        })?;
        let bucket = self.bucket.clone();
        let region = self.region.clone();
        let access_key = self.access_key.clone();
        let secret_key = self.secret_key.clone();
        let now = self.clock.now();
        self.runtime
            .run(move || async move {
                google_xml
                    .create_only(
                        &endpoint,
                        &bucket,
                        &region,
                        &access_key,
                        &secret_key,
                        &full,
                        source,
                        size,
                        &payload_hash,
                        now,
                        control,
                    )
                    .await
            })
            .await
            .map_err(|error| CloudHomeError::transport("run S3 create-only operation", error))?
    }

    async fn put_create_only(
        &self,
        key: &str,
        data: Vec<u8>,
        checksum_sha256: Option<String>,
    ) -> Result<(), CloudHomeError> {
        self.put_create_only_raw(
            key,
            data,
            checksum_sha256,
            UploadControl::running(super::no_progress()),
        )
        .await
        .map_err(S3CreateOnlyPutError::into_cloud_error)
    }

    async fn append_create_only(
        &self,
        key: &str,
        body: BlobBody,
        exact_sha256: Option<String>,
        control: &UploadControl,
    ) -> Result<(), CloudHomeError> {
        if body.len() <= self.multipart_threshold() {
            let data = body.collect().await?;
            return self
                .put_create_only_raw(key, data, exact_sha256, control.clone())
                .await
                .map_err(S3CreateOnlyPutError::into_cloud_error);
        }
        let sink = self
            .open_multipart_sink(key, MultipartCompletion::CreateOnly, exact_sha256)
            .await?;
        MultipartUpload::new(key, body, sink, control).run().await
    }

    async fn create_at_slot(
        &self,
        slot: &ObjectSlot,
        body: BlobBody,
        exact_sha256: Option<String>,
        control: &UploadControl,
    ) -> Result<(), CloudHomeError> {
        slot.require_logical_key_for("S3")
            .map_err(CloudHomeError::from)?;
        self.append_create_only(slot.logical_key(), body, exact_sha256, control)
            .await
    }

    async fn read_exact_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &std::path::Path,
        progress: super::DownloadProgress,
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
                super::write_cloud_object_stream(&destination, Box::pin(stream), progress).await?;
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
            .run_cloud(move || async move {
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
                            s3_operation_error(format!("head exact S3 object {key}"), error)
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

        if self.google_xml.is_some() {
            if created_response_was_observed {
                return Ok(());
            }
            let bytes = self.read_at(upload.object().slot()).await?;
            return upload.verify_stored_bytes(&bytes);
        }

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

    /// Verify the object capabilities this home uses: conditional creation,
    /// readback or checksum verification, and deletion. This deliberately does
    /// not read bucket metadata.
    async fn probe_exact_slots(&self) -> Result<(), CloudHomeError> {
        use coven_foundation::config::ExactUploadVerification;

        let suffix = self.ids.new_id();
        let key = format!("__coven_probe__/exact-{suffix}");
        let bad_key = format!("__coven_probe__/bad-checksum-{suffix}");
        let bytes = b"coven exact-slot checksum probe".to_vec();
        let checksum = sha256_bytes_base64(&bytes);
        let sends_checksum = self.google_xml.is_none()
            && matches!(
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
                        "S3 endpoint did not enforce atomic exact-slot creation".to_string(),
                    ));
                }
                Err(error) => return Err(error),
            }

            if self.read(&key).await? != bytes {
                return Err(CloudHomeError::Configuration(
                    "S3 exact-slot readback returned different bytes".to_string(),
                ));
            }
            let listed = self.list("__coven_probe__/").await?;
            if !listed.iter().any(|listed_key| listed_key == &key) {
                return Err(CloudHomeError::Configuration(
                    "S3 listing did not return the exact-slot probe object".to_string(),
                ));
            }

            if self.google_xml.is_none() {
                match self.exact_upload_verification {
                    ExactUploadVerification::UploadChecksum => {
                    let wrong = sha256_bytes_base64(b"different bytes");
                    match self
                        .put_create_only_raw(
                            &bad_key,
                            bytes.clone(),
                            Some(wrong),
                            UploadControl::running(super::no_progress()),
                        )
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
                    ExactUploadVerification::Readback | ExactUploadVerification::Unchecked => {}
                }
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
            .run_cloud(move || async move {
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
pub(crate) async fn open_cloud_home(
    runtime: CloudRuntime,
    bucket: String,
    region: String,
    endpoint: Option<String>,
    access_key: String,
    secret_key: String,
    key_prefix: Option<String>,
    exact_upload_verification: coven_foundation::config::ExactUploadVerification,
    clock: coven_foundation::clock::ClockRef,
) -> Result<S3CloudHome, CloudHomeError> {
    let home_runtime = runtime.clone();
    runtime
        .run_cloud(move || {
            S3CloudHome::new(
                home_runtime,
                bucket,
                region,
                endpoint,
                access_key,
                secret_key,
                key_prefix,
                exact_upload_verification,
                clock,
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
        offset: u64,
        control: UploadControl,
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
                S3MultipartCommand::SendPart {
                    part,
                    offset,
                    control,
                    response,
                } => {
                    let result = self.send_part(part, offset, control).await;
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

    async fn send_part(
        &mut self,
        part: bytes::Bytes,
        offset: u64,
        control: UploadControl,
    ) -> Result<(), CloudHomeError> {
        let part_number = self.next_part_number;
        self.next_part_number += 1;
        let part_sha256 = self
            .exact_sha256
            .as_ref()
            .map(|_| sha256_bytes_base64(&part));
        let content_length = i64::try_from(part.len()).map_err(|_| {
            CloudHomeError::Transport(format!(
                "multipart part {part_number} for {} exceeds S3's content-length range",
                self.key
            ))
        })?;
        let request_body = reqwest::Body::wrap_stream(control.stream_part(part, offset));
        let body = aws_sdk_s3::primitives::ByteStream::new(
            aws_sdk_s3::primitives::SdkBody::from_body_1_x(request_body),
        );
        let mut request = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .part_number(part_number)
            .content_length(content_length)
            .body(body);
        if let Some(checksum) = part_sha256.as_ref() {
            request = request.checksum_sha256(checksum);
        }
        let uploaded = request.send().await.map_err(|error| {
            s3_operation_error(
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
            .map_err(|error| s3_operation_error(format!("abort multipart {}", self.key), error))?;
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
                s3_operation_error(format!("complete multipart {}", self.key), error)
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

mod provider_identity;
use provider_identity::*;

#[async_trait]
impl super::PartSink for S3PartSink {
    fn part_size(&self) -> usize {
        MULTIPART_PART_SIZE
    }

    async fn send_part(
        &mut self,
        part: bytes::Bytes,
        offset: u64,
        _is_last: bool,
        control: &UploadControl,
    ) -> Result<(), CloudHomeError> {
        let commands = self.commands.as_ref().ok_or_else(|| {
            CloudHomeError::Transport("S3 multipart upload is already settled".to_string())
        })?;
        let (response, result) = tokio::sync::oneshot::channel();
        commands
            .send(S3MultipartCommand::SendPart {
                part,
                offset,
                control: control.clone(),
                response,
            })
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

fn s3_backend_failure(code: Option<&str>, status: Option<u16>) -> StorageBackendFailure {
    fn from_status(status: Option<u16>) -> StorageBackendFailure {
        match status {
            Some(401) => StorageBackendFailure::Authentication,
            Some(403) => StorageBackendFailure::PermissionDenied,
            Some(404) => StorageBackendFailure::ContainerNotFound,
            Some(429 | 500..=599) | None => StorageBackendFailure::Transport,
            Some(_) => StorageBackendFailure::Configuration,
        }
    }

    match code {
        Some(
            "InvalidAccessKeyId"
            | "InvalidAccessKey"
            | "InvalidClientTokenId"
            | "SignatureDoesNotMatch"
            | "IncompleteSignature"
            | "MissingAuthenticationToken"
            | "UnrecognizedClientException"
            | "InvalidToken"
            | "ExpiredToken"
            | "TokenRefreshRequired",
        ) => StorageBackendFailure::Authentication,
        Some("AccessDenied" | "AllAccessDisabled" | "AccountProblem") => {
            StorageBackendFailure::PermissionDenied
        }
        Some("NoSuchBucket") => StorageBackendFailure::ContainerNotFound,
        Some(
            "PermanentRedirect"
            | "AuthorizationHeaderMalformed"
            | "IncorrectEndpoint"
            | "IllegalLocationConstraintException",
        ) => StorageBackendFailure::RegionMismatch,
        Some("OverQuota" | "QuotaExceeded" | "InsufficientStorage") => {
            StorageBackendFailure::QuotaExceeded
        }
        Some(
            "InternalError"
            | "RequestTimeout"
            | "RequestTimeoutException"
            | "ServiceUnavailable"
            | "SlowDown"
            | "Throttling"
            | "ThrottlingException"
            | "RequestLimitExceeded",
        ) => StorageBackendFailure::Transport,
        Some(_) | None => from_status(status),
    }
}

fn s3_operation_error<E>(
    operation: impl Into<String>,
    error: aws_sdk_s3::error::SdkError<E>,
) -> CloudHomeError
where
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let status = match &error {
        aws_sdk_s3::error::SdkError::ServiceError(service) => Some(service.raw().status().as_u16()),
        _ => None,
    };
    let kind = s3_backend_failure(error.code(), status);
    CloudHomeError::backend(kind, operation, S3SdkError(error))
}

#[derive(Debug)]
struct S3SdkError<E>(aws_sdk_s3::error::SdkError<E>);

impl<E> fmt::Display for S3SdkError<E>
where
    E: std::error::Error + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        aws_sdk_s3::error::DisplayErrorContext(&self.0).fmt(formatter)
    }
}

impl<E> std::error::Error for S3SdkError<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// Map a GetObject failure to a `CloudHomeError`, surfacing the S3 error code and
/// message (e.g. `AccessDenied`, `PermanentRedirect`, `SignatureDoesNotMatch`)
/// rather than the opaque "service error". `NoSuchKey` becomes `NotFound`;
/// non-service failures (timeouts, connection errors) fall back to their own
/// description. Generic over the response type so it serves both `read` and
/// `read_range` without naming the smithy HTTP type.
fn get_object_error(
    key: &str,
    err: aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
) -> CloudHomeError {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    match err.code() {
        Some("NoSuchKey") => CloudHomeError::NotFound(key.to_string()),
        Some(code) => s3_operation_error(
            match err.message() {
                Some(msg) => format!("get {key}: S3 {code}: {msg}"),
                None => format!("get {key}: S3 {code} (no message provided)"),
            },
            err,
        ),
        // Not a service error (timeout / connection / dispatch) — its own
        // Display carries the detail.
        None => s3_operation_error(format!("get {key}"), err),
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
        Some(code) => s3_operation_error(
            match err.message() {
                Some(msg) => format!("put {key}: S3 {code}: {msg}"),
                None => format!("put {key}: S3 {code} (no message provided)"),
            },
            err,
        ),
        None => s3_operation_error(format!("put {key}"), err),
    }
}

#[async_trait]
impl CloudHome for S3CloudHome {
    async fn probe(&self) -> Result<(), CloudHomeError> {
        self.probe_exact_slots().await
    }

    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
        let full = self.full_key(key);
        let key = key.to_string();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime
            .run_cloud(move || async move {
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
            .run_cloud(move || async move {
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
            .run_cloud(move || async move {
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
        // runs on the retained cloud runtime.
        self.runtime
            .run_cloud(move || async move {
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
                        .map_err(|error| s3_operation_error(format!("list {prefix}"), error))?;

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
            .run_cloud(move || async move {
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
                        return Err(s3_operation_error(format!("delete {key}"), e));
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
            .run_cloud(move || async move {
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
                            Err(s3_operation_error(format!("head {key}"), e))
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

mod exact;

#[cfg(test)]
mod tests;
