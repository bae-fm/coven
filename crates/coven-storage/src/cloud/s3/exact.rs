use super::*;

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
                    .run_cloud(move || async move {
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
        upload: &crate::cloud::ExactUpload<'_>,
        control: &UploadControl,
    ) -> Result<crate::cloud::ExactCreateOutcome, CloudHomeError> {
        let checksum = matches!(
            self.exact_upload_verification,
            coven_foundation::config::ExactUploadVerification::UploadChecksum
                | coven_foundation::config::ExactUploadVerification::MetadataHash
        )
        .then(|| sha256_base64(upload.object().stored_hash()));
        let operation = match self.google_xml.is_none() {
            true => {
                S3CloudHome::create_at_slot(
                    self,
                    upload.object().slot(),
                    upload.body().await?,
                    checksum,
                    control,
                )
                .await
            }
            false => {
                upload.object().slot().require_logical_key_for("S3")?;
                let source = match upload.source() {
                    crate::cloud::ExactUploadSource::Bytes(bytes) => {
                        GoogleUploadSource::Bytes(bytes.to_vec())
                    }
                    crate::cloud::ExactUploadSource::File(path) => {
                        GoogleUploadSource::File(path.to_path_buf())
                    }
                };
                let result = self
                    .put_google_exact_create_only(
                        upload.object().slot().logical_key(),
                        source,
                        upload.object().stored_size(),
                        hex::encode(upload.object().stored_hash().as_bytes()),
                        control.clone(),
                    )
                    .await;
                if result.is_ok() {
                    // The body already reported every chunk it handed over; this
                    // settles the count on the exact stored size once the
                    // provider has acknowledged the whole object.
                    control.report(upload.object().stored_size());
                }
                result
            }
        };
        crate::cloud::exact_upload::settle_exact_create(operation, |observed| {
            self.verify_exact_upload(upload, observed)
        })
        .await
    }
    async fn list_slots(&self, prefix: &str) -> Result<Vec<ObjectSlot>, CloudHomeError> {
        crate::cloud::logical_slots(CloudHome::list(self, prefix).await?)
    }
    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError> {
        slot.require_logical_key_for("S3")?;
        S3CloudHome::read(self, slot.logical_key()).await
    }
    async fn read_versioned_at(
        &self,
        slot: &ObjectSlot,
    ) -> Result<crate::cloud::CloudVersionedObject, CloudHomeError> {
        slot.require_logical_key_for("S3")?;
        let key = slot.logical_key().to_string();
        if let Some(google_xml) = self.google_xml.clone() {
            let endpoint = self.endpoint.clone().ok_or_else(|| {
                CloudHomeError::Configuration("Google Cloud Storage endpoint is absent".to_string())
            })?;
            let bucket = self.bucket.clone();
            let region = self.region.clone();
            let access_key = self.access_key.clone();
            let secret_key = self.secret_key.clone();
            let full = self.full_key(&key);
            let now = self.clock.now();
            return self
                .runtime
                .run(move || async move {
                    google_xml
                        .read_versioned(
                            &endpoint,
                            &bucket,
                            &region,
                            &access_key,
                            &secret_key,
                            &full,
                            now,
                        )
                        .await
                })
                .await
                .map_err(|error| {
                    CloudHomeError::transport("run Google Cloud Storage versioned read", error)
                })?;
        }
        let full = self.full_key(&key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.runtime
            .run_cloud(move || async move {
                let response = client
                    .get_object()
                    .bucket(&bucket)
                    .key(full)
                    .send()
                    .await
                    .map_err(|error| get_object_error(&key, error))?;
                let version = response.e_tag().ok_or_else(|| {
                    CloudHomeError::Transport(format!("read versioned {key}: S3 returned no ETag"))
                })?;
                let version = crate::cloud::CloudObjectVersion::from_provider(version.to_string())?;
                let bytes = response
                    .body
                    .collect()
                    .await
                    .map_err(|error| body_read_error("read versioned body", &key, error))?
                    .into_bytes()
                    .to_vec();
                Ok(crate::cloud::CloudVersionedObject { bytes, version })
            })
            .await
    }
    async fn replace_at_if_version(
        &self,
        slot: &ObjectSlot,
        expected: &crate::cloud::CloudObjectVersion,
        bytes: Vec<u8>,
    ) -> Result<crate::cloud::ConditionalWriteOutcome, CloudHomeError> {
        slot.require_logical_key_for("S3")?;
        let key = slot.logical_key().to_string();
        if let Some(google_xml) = self.google_xml.clone() {
            let endpoint = self.endpoint.clone().ok_or_else(|| {
                CloudHomeError::Configuration("Google Cloud Storage endpoint is absent".to_string())
            })?;
            let bucket = self.bucket.clone();
            let region = self.region.clone();
            let access_key = self.access_key.clone();
            let secret_key = self.secret_key.clone();
            let full = self.full_key(&key);
            let expected = expected.clone();
            let now = self.clock.now();
            return self
                .runtime
                .run(move || async move {
                    google_xml
                        .replace_if_generation(
                            &endpoint,
                            &bucket,
                            &region,
                            &access_key,
                            &secret_key,
                            &full,
                            &expected,
                            bytes,
                            now,
                        )
                        .await
                })
                .await
                .map_err(|error| {
                    CloudHomeError::transport(
                        "run Google Cloud Storage conditional replacement",
                        error,
                    )
                })?;
        }
        let full = self.full_key(&key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let expected = expected.as_provider().to_string();
        self.runtime
            .run_cloud(move || async move {
                let result = client
                    .put_object()
                    .bucket(&bucket)
                    .key(full)
                    .if_match(expected)
                    .body(bytes.into())
                    .send()
                    .await;
                let response = match result {
                    Ok(response) => response,
                    Err(error) if create_only_put_failed(&error) => {
                        return Ok(crate::cloud::ConditionalWriteOutcome::VersionChanged)
                    }
                    Err(error) => return Err(put_object_error(&key, error)),
                };
                let version = response.e_tag().ok_or_else(|| {
                    CloudHomeError::Transport(format!(
                        "replace versioned {key}: S3 returned no ETag"
                    ))
                })?;
                Ok(crate::cloud::ConditionalWriteOutcome::Replaced(
                    crate::cloud::CloudObjectVersion::from_provider(version.to_string())?,
                ))
            })
            .await
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
        progress: crate::cloud::DownloadProgress,
    ) -> Result<(), crate::cloud::CloudFileReadError> {
        S3CloudHome::read_exact_to_file(self, slot, destination, progress).await
    }
    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        slot.require_logical_key_for("S3")?;
        S3CloudHome::delete(self, slot.logical_key()).await
    }
}
