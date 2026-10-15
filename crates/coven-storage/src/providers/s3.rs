use super::pagination::Pagination;
use crate::session::{S3Part, SessionState};
use crate::*;
use async_trait::async_trait;
use aws_sdk_s3::{
    config::{Region, RequestChecksumCalculation, ResponseChecksumValidation},
    error::ProvideErrorMetadata,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use coven_crypto::SecretText;
use coven_foundation::{clock::ClockRef, id_source::IdSourceRef};
use std::fmt;

#[path = "s3_clock.rs"]
mod clock;
#[path = "s3_error.rs"]
mod error;
#[path = "s3_publication.rs"]
mod publication;
pub(crate) use error::response_status;
use error::s3_error;
#[path = "s3_profile.rs"]
mod profile;
#[path = "s3_versioning.rs"]
mod versioning;
pub(crate) use profile::{settings as s3_settings, S3KeyScope};
pub(crate) use versioning::check as check_s3_versioning;
#[path = "s3_checksum_headers.rs"]
mod checksum_headers;
#[path = "s3_copy_upload.rs"]
mod copy_upload;
#[path = "s3_detect.rs"]
mod detect;
#[path = "s3_gcs_signing.rs"]
mod gcs_signing;
#[path = "s3_listing.rs"]
mod listing;
#[path = "s3_metadata.rs"]
mod metadata;
#[path = "s3_read.rs"]
mod read;
#[path = "s3_restart.rs"]
mod restart;
pub(crate) use detect::detect;
#[path = "s3_gcs_keys.rs"]
mod gcs_keys;
#[path = "s3_iam.rs"]
mod iam;
#[path = "s3_member_probe.rs"]
mod member_probe;
#[path = "s3_minting.rs"]
pub(crate) mod minting;
pub(crate) use minting::member_policy;
#[path = "s3_own_bucket.rs"]
mod own_bucket;
#[path = "s3_r2_keys.rs"]
mod r2_keys;
#[path = "s3_rustfs_keys.rs"]
mod rustfs_keys;
pub(crate) use own_bucket::check_new_store;

/// The finest scope a member key has at this location, after refusing a
/// profile or plain-http endpoint that contradicts the endpoint (§4).
pub(crate) fn s3_key_scope(
    endpoint: Option<&url::Url>,
    profile: Option<&crate::S3Profile>,
) -> Result<S3KeyScope, StorageError> {
    profile::select(endpoint, profile).map(profile::Selected::key_scope)
}

/// S3 and compatible endpoints, using owner-provisioned buckets and manually supplied keys.
pub(crate) struct S3Storage {
    client: Client,
    credentials: SigningCredentials,
    config: StorageConfig,
    bucket: String,
    prefix: String,
    ids: IdSourceRef,
    settings: S3Settings,
    #[cfg(test)]
    single_request_limit: u64,
}
#[derive(Clone)]
struct SigningCredentials(std::sync::Arc<std::sync::RwLock<aws_credential_types::Credentials>>);
impl fmt::Debug for SigningCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SigningCredentials([REDACTED])")
    }
}
impl aws_credential_types::provider::ProvideCredentials for SigningCredentials {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::ready(Ok(self
            .0
            .read()
            .expect("S3 credentials lock poisoned")
            .clone()))
    }
}

impl S3Storage {
    /// Construct at a composition root. No credential discovery, IAM calls, key
    /// creation or revocation occurs. The SDK computes protocol checksums itself.
    /// Every request, a clock retry included, waits for `cooldown` (§4).
    pub(crate) fn new(
        config: StorageConfig,
        credentials: S3Credentials,
        clock: ClockRef,
        ids: IdSourceRef,
        cooldown: std::sync::Arc<dyn Cooldown>,
    ) -> Result<Self, StorageError> {
        let StorageConfig::S3 {
            endpoint, profile, ..
        } = &config
        else {
            return Err(
                StorageFailure::InvalidConfiguration.with_source("expected S3 configuration")
            );
        };
        let settings = profile::settings(endpoint.as_ref(), profile.as_ref())?.ok_or(
            StorageFailure::InvalidConfiguration
                .with_source("an uncatalogued S3 endpoint needs settings detected at setup"),
        )?;
        Self::with_settings(config, credentials, clock, ids, cooldown, settings)
    }
    /// Connect with explicit settings; detection probes candidates this way.
    fn with_settings(
        config: StorageConfig,
        credentials: S3Credentials,
        clock: ClockRef,
        ids: IdSourceRef,
        cooldown: std::sync::Arc<dyn Cooldown>,
        settings: S3Settings,
    ) -> Result<Self, StorageError> {
        let StorageConfig::S3 {
            bucket,
            region,
            endpoint,
            prefix,
            ..
        } = &config
        else {
            return Err(
                StorageFailure::InvalidConfiguration.with_source("expected S3 configuration")
            );
        };
        if credentials.access_key_id.is_empty() || credentials.secret_access_key.as_str().is_empty()
        {
            return Err(StorageFailure::InvalidConfiguration.with_source("empty S3 key"));
        }
        let credentials = SigningCredentials(std::sync::Arc::new(std::sync::RwLock::new(
            aws_credentials(credentials),
        )));
        let signing = clock::SigningClock::new(clock);
        let mut builder = aws_sdk_s3::config::Builder::new()
            .behavior_version_latest()
            .region(Region::new(region.clone()))
            .credentials_provider(credentials.clone())
            .identity_cache(aws_sdk_s3::config::IdentityCache::no_cache())
            .time_source(signing.clone())
            // Coven corrects the signing clock itself, for both signers (§4).
            .disable_clock_skew_correction(true)
            .interceptor(clock::ClockSkew::new(signing, cooldown))
            .retry_classifier(clock::ClockRetry)
            .retry_config(clock::retry_config())
            .force_path_style(true)
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired);
        if settings.signing == S3Signing::GoogleHmac {
            builder = builder.push_auth_scheme(gcs_signing::GcsSigning {
                region: region.clone(),
            });
        }
        if settings.response_headers == S3ResponseHeaders::Google {
            builder = builder.interceptor(gcs_signing::GcsResponseHeaders);
        }
        if settings.checksums == S3Checksums::UnstatedIgnored {
            builder = builder.interceptor(checksum_headers::UnstatedChecksumHeaders);
        }
        if let Some(endpoint) = endpoint {
            builder = builder.endpoint_url(endpoint.as_str());
        }
        let client = Client::from_conf(builder.build());
        Ok(Self {
            client,
            credentials,
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            config,
            ids,
            settings,
            #[cfg(test)]
            single_request_limit: 5 * 1024 * 1024 * 1024,
        })
    }
    async fn put(
        &self,
        publication: &PublicationRequest,
        path: &ObjectPath,
        bytes: &[u8],
        create: bool,
    ) -> Result<(), StorageError> {
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(path.under(&self.prefix))
            .body(ByteStream::from(bytes.to_vec()));
        if path.is_replaceable() && self.settings.revision == S3Revision::ETag {
            request = request.metadata("coven-revision", self.ids.new_id().to_string());
        }
        let condition = self.settings.single_put;
        request
            .customize()
            .mutate_request(move |request| {
                if create {
                    let (name, value) = condition.header();
                    request.headers_mut().insert(name, value);
                }
            })
            .interceptor(publication.clone())
            .send()
            .await
            .map_err(s3_error)?;
        Ok(())
    }
    fn upload_id<'a>(&self, session: &'a UploadSession) -> Result<&'a str, StorageError> {
        match &session.state {
            SessionState::S3 { id, .. } | SessionState::S3Restart { id, .. } => Ok(id.as_str()),
            _ => Err(StorageFailure::SessionMismatch.into()),
        }
    }
    async fn completed(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.confirm_published(session).await?;
        session.confirmed = session.total;
        session.state = SessionState::Complete;
        Ok(())
    }
    async fn confirm_published(&self, session: &UploadSession) -> Result<(), StorageError> {
        let (SessionState::S3 { token, .. } | SessionState::S3Planned { token }) = &session.state
        else {
            return Err(StorageFailure::SessionMismatch.into());
        };
        self.confirm_marker(&session.path, session.total, token)
            .await
    }
    /// Whether `path` holds the object this publication's marker names: its
    /// `coven-upload` token and size. Another object is `AlreadyExists`; an
    /// absent one `SessionExpired`.
    async fn confirm_marker(
        &self,
        path: &ObjectPath,
        total: u64,
        token: &SecretText,
    ) -> Result<(), StorageError> {
        let response = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(path.under(&self.prefix))
            .send()
            .await
            .map_err(s3_error);
        let response = match response {
            Ok(response) => response,
            Err(error) if error.failure() == StorageFailure::NotFound => {
                return Err(StorageFailure::SessionExpired.with_source(error))
            }
            Err(error) => return Err(error),
        };
        if response
            .content_length()
            .and_then(|size| u64::try_from(size).ok())
            != Some(total)
            || response
                .metadata()
                .and_then(|metadata| metadata.get("coven-upload"))
                .map(String::as_str)
                != Some(token.as_str())
        {
            return Err(StorageFailure::AlreadyExists.into());
        }
        Ok(())
    }
    async fn delete_key(&self, key: &str) -> Result<(), StorageError> {
        match self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if matches!(error.code(), Some("NoSuchKey" | "NotFound")) => {
                tracing::debug!("S3 object already absent");
                Ok(())
            }
            Err(error) => Err(s3_error(error)),
        }
    }
}

#[async_trait]
impl ProviderOps for S3Storage {
    async fn set_s3_credentials(&self, credentials: S3Credentials) -> Result<(), StorageError> {
        if credentials.access_key_id.is_empty() || credentials.secret_access_key.as_str().is_empty()
        {
            return Err(StorageFailure::InvalidConfiguration.with_source("empty S3 key"));
        }
        *self
            .credentials
            .0
            .write()
            .expect("S3 credentials lock poisoned") = aws_credentials(credentials);
        Ok(())
    }
    fn config(&self) -> StorageConfig {
        self.config.clone()
    }
    fn single_request_limit(&self) -> u64 {
        #[cfg(test)]
        {
            self.single_request_limit
        }
        #[cfg(not(test))]
        {
            5 * 1024 * 1024 * 1024
        }
    }
    async fn create(
        &self,
        publication: &PublicationRequest,
        path: &ObjectPath,
        bytes: &[u8],
    ) -> Result<Option<StoredObject>, StorageError> {
        // A PUT or copy reply carries no publication time.
        match self.settings.copy_publication.single_request() {
            Some(condition) => {
                self.create_by_copy(publication, path, bytes, condition)
                    .await?
            }
            None => self.put(publication, path, bytes, true).await?,
        }
        Ok(None)
    }
    async fn replace(
        &self,
        publication: &PublicationRequest,
        path: &ObjectPath,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        self.put(publication, path, bytes, false).await
    }
    async fn status(&self, path: &ObjectPath) -> Result<Option<StoredObject>, StorageError> {
        let mut request = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(path.under(&self.prefix));
        if self.settings.request_checksums {
            request = request.checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled);
        }
        match request.send().await.map_err(s3_error) {
            Ok(response) => Ok(Some(metadata::head(self, path, &response)?)),
            Err(error) if error.failure() == StorageFailure::NotFound => {
                // HEAD's 404 has no body: distinguish the object from its bucket.
                // A member key's list permission is scoped to this store's prefix;
                // HEAD Bucket sends no prefix and needs an unconditioned grant.
                // The zero-entry listing checks the bucket. Object absence comes
                // from the exact HEAD, never from the listing.
                self.list_page(
                    self.settings.listing,
                    &path.under(&self.prefix),
                    None,
                    None,
                    Some(0),
                )
                .await
                .map_err(super::http::container_error)?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
    async fn stream_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<ObjectStream<'_>, StorageError> {
        self.get(path, range).await
    }
    async fn list(&self, listing: &mut ObjectListing) -> Result<(), StorageError> {
        let prefix = listing.prefix().clone();
        let full = prefix.under(&self.prefix);
        let root = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        };
        let mut marker = None;
        let mut seen = Pagination::new();
        loop {
            let response = self
                .list_page(self.settings.listing, &full, None, marker, None)
                .await?;
            if !response.folders.is_empty() {
                return Err(
                    StorageFailure::Protocol.with_source("S3 returned folders without a delimiter")
                );
            }
            for object in &response.contents {
                let key = object
                    .key()
                    .ok_or(StorageFailure::Protocol.with_source("S3 list entry omitted key"))?;
                let relative = key
                    .strip_prefix(&root)
                    .ok_or(StorageFailure::Protocol.with_source("S3 listed another location"))?;
                // Upload temporaries and detection probes are outside D10 too.
                let Some(path) = listing.object_path(relative)? else {
                    continue;
                };
                let size = object
                    .size()
                    .and_then(|size| u64::try_from(size).ok())
                    .ok_or(StorageFailure::Protocol.with_source("S3 omitted object size"))?;
                let stored_at = object
                    .last_modified()
                    .ok_or(StorageFailure::Protocol.with_source("S3 omitted storage time"))?;
                // HEAD and GET use whole-second HTTP dates. GCS's XML listings
                // include fractions; use the same precision for every observation
                // so listing an immutable object cannot change its publication time.
                let stored_at = crate::providers::http::storage_time(
                    aws_sdk_s3::primitives::DateTime::from_secs(stored_at.secs()),
                )?;
                if path.is_replaceable() {
                    // ListObjectsV2 has neither native versions nor user metadata.
                    // Exact status supplies the replacement revision; an intervening
                    // deletion is absence, while a failed lookup fails the listing.
                    if let Some(current) = self.status(&path).await? {
                        listing.insert(current)?;
                    } else {
                        tracing::debug!(
                            path = path.as_str(),
                            "S3 listed object was deleted before exact status"
                        );
                    }
                } else {
                    listing.insert(StoredObject {
                        object_id: key.into(),
                        revision: metadata::required(object.e_tag(), "S3 omitted ETag")?.into(),
                        path,
                        size,
                        stored_at,
                        time_resolution: std::time::Duration::from_secs(1),
                        checksum: None,
                    })?;
                }
            }
            let Some(next) = response.next else {
                break;
            };
            seen.check(&next)?;
            marker = Some(next);
        }
        Ok(())
    }
    async fn list_writers(
        &self,
        store: coven_foundation::id_source::StoreId,
    ) -> Result<Vec<coven_foundation::id_source::DeviceId>, StorageError> {
        self.writer_folders(store).await
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.delete_key(&path.under(&self.prefix)).await
    }
    async fn grant_access(&self, _account: &str) -> Result<AccessGrant, StorageError> {
        Ok(AccessGrant::CreateAccessKey)
    }
    async fn revoke_access(&self, _account: &str) -> Result<Vec<RetainedAccess>, StorageError> {
        Err(StorageFailure::InvalidConfiguration.with_source("S3 has no account sharing"))
    }
    async fn begin_upload(
        &self,
        publication: &PublicationRequest,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        // A copy above the single-copy limit cannot publish atomically (§4).
        if let Some(condition) = self.settings.copy_publication.multipart() {
            if let Some(limit) = condition.single_copy_limit().filter(|limit| total > *limit) {
                return Err(StorageFailure::CopyTooLarge { size: total, limit }.into());
            }
        }
        let part_size = crate::session::s3_part_size(total)?;
        let token = SecretText::new(self.ids.new_id().to_string());
        // A copy profile's temporary is named before any request: the caller
        // records the session, and the first part initiates the upload (§4).
        let state = if self.settings.copy_publication.multipart().is_some() {
            SessionState::S3Planned { token }
        } else {
            let id = self
                .initiate(publication, &path.under(&self.prefix), &token)
                .await?;
            SessionState::S3 {
                id,
                token,
                parts: Vec::new(),
            }
        };
        Ok(UploadSession {
            location: self.config(),
            path: path.clone(),
            total,
            confirmed: 0,
            part_size,
            state,
        })
    }
    async fn resume_upload(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        if matches!(session.state, SessionState::S3Restart { .. }) {
            return self
                .require_restart(publication, session, StorageFailure::SessionExpired.into())
                .await;
        }
        if matches!(session.state, SessionState::S3Planned { .. }) {
            // An upload initiated under the recording's name, whose reply
            // and later recordings were lost, is aborted; a temporary it
            // assembled still publishes, as a completed upload's does.
            self.abort_initiated(publication, session).await?;
            return self.recover_completed_upload(publication, session).await;
        }
        let id = SecretText::new(self.upload_id(session)?.to_owned());
        let mut marker = None;
        let mut seen = Pagination::new();
        let mut parts = Vec::new();
        let mut confirmed = 0u64;
        loop {
            let response = match self
                .client
                .list_parts()
                .bucket(&self.bucket)
                .key(self.session_key(session)?)
                .upload_id(id.as_str())
                .set_part_number_marker(marker)
                .customize()
                .interceptor(publication.clone())
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) if error.code() == Some("NoSuchUpload") => {
                    return self.recover_completed_upload(publication, session).await
                }
                Err(error) => return Err(s3_error(error)),
            };
            for part in response.parts() {
                let number = part
                    .part_number()
                    .ok_or(StorageFailure::Protocol.with_source("S3 omitted part number"))?;
                let size = part
                    .size()
                    .and_then(|n| u64::try_from(n).ok())
                    .ok_or(StorageFailure::Protocol.with_source("S3 omitted part size"))?;
                let etag = part
                    .e_tag()
                    .filter(|value| !value.is_empty())
                    .ok_or(StorageFailure::Protocol.with_source("S3 omitted part ETag"))?
                    .to_owned();
                confirmed = confirmed
                    .checked_add(size)
                    .ok_or(StorageFailure::InvalidPart)?;
                let part = S3Part { number, size, etag };
                if !part.follows(parts.len(), confirmed, session.total, session.part_size) {
                    return Err(StorageFailure::Protocol.with_source("S3 upload has invalid parts"));
                }
                parts.push(part);
            }
            if !response
                .is_truncated()
                .ok_or(StorageFailure::Protocol.with_source("S3 omitted page completion"))?
            {
                break;
            }
            let next = response
                .next_part_number_marker()
                .filter(|value| !value.is_empty())
                .ok_or(StorageFailure::Protocol.with_source("S3 omitted part marker"))?
                .to_owned();
            seen.check(&next)?;
            marker = Some(next);
        }
        if confirmed < session.confirmed {
            return Err(StorageFailure::Protocol.with_source("S3 lost confirmed parts"));
        }
        if let SessionState::S3 { parts: stored, .. } = &mut session.state {
            *stored = parts;
        }
        session.confirmed = confirmed;
        Ok(())
    }
    async fn upload_part(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
        bytes: &[u8],
        end: u64,
    ) -> Result<(), StorageError> {
        if matches!(session.state, SessionState::S3Restart { .. }) {
            return Err(StorageFailure::SessionExpired.into());
        }
        if let SessionState::S3Planned { token } = &session.state {
            let token = token.clone();
            let id = self
                .initiate(publication, &self.session_key(session)?, &token)
                .await?;
            session.state = SessionState::S3 {
                id,
                token,
                parts: Vec::new(),
            };
        }
        let id = self.upload_id(session)?;
        let SessionState::S3 { parts, .. } = &session.state else {
            return Err(StorageFailure::SessionMismatch.into());
        };
        let number = i32::try_from(parts.len() + 1)
            .map_err(|error| StorageFailure::InvalidPart.with_source(error))?;
        if number > 10_000 {
            return Err(StorageFailure::InvalidPart.into());
        }
        let response = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(self.session_key(session)?)
            .upload_id(id)
            .part_number(number)
            .body(ByteStream::from(bytes.to_vec()))
            .customize()
            .interceptor(publication.clone())
            .send()
            .await
            .map_err(s3_error)?;
        let etag = response
            .e_tag()
            .filter(|value| !value.is_empty())
            .ok_or(StorageFailure::Protocol.with_source("S3 omitted uploaded ETag"))?
            .to_owned();
        if let SessionState::S3 { parts, .. } = &mut session.state {
            parts.push(S3Part {
                number,
                size: bytes.len() as u64,
                etag,
            });
        }
        session.confirmed = end;
        Ok(())
    }
    async fn finish_upload(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        if matches!(session.state, SessionState::S3Restart { .. }) {
            return self
                .require_restart(publication, session, StorageFailure::SessionExpired.into())
                .await;
        }
        if session.confirmed != session.total {
            return Err(StorageFailure::InvalidPart.into());
        }
        let id = self.upload_id(session)?;
        let SessionState::S3 { parts, .. } = &session.state else {
            return Err(StorageFailure::SessionMismatch.into());
        };
        let completed = parts
            .iter()
            .map(|part| {
                CompletedPart::builder()
                    .part_number(part.number)
                    .e_tag(&part.etag)
                    .build()
            })
            .collect();
        let mut request = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(self.session_key(session)?)
            .upload_id(id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed))
                    .build(),
            );
        if self.settings.copy_publication == S3CopyPublication::Never {
            request = request.if_none_match("*");
        }
        let result = request
            .customize()
            .interceptor(publication.clone())
            .send()
            .await;
        match result {
            Ok(_) => {
                if self.settings.copy_publication.multipart().is_some() {
                    return self.publish_copy(publication, session).await;
                }
                session.state = SessionState::Complete;
                Ok(())
            }
            Err(error) if matches!(error.code(), Some("NoSuchUpload" | "PreconditionFailed")) => {
                self.recover_completed_upload(publication, session).await
            }
            Err(error) => match s3_error(error) {
                // A concurrent mutation, under any service's code (§4).
                error if error.failure() == StorageFailure::Conflict => {
                    self.require_restart(
                        publication,
                        session,
                        StorageFailure::SessionExpired.with_source(error),
                    )
                    .await
                }
                error => Err(error),
            },
        }
    }
    async fn abort_upload(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        if matches!(session.state, SessionState::S3Planned { .. }) {
            return crate::transfer::with_cleanup(
                self.abort_initiated(publication, session).await,
                self.delete_temporary(session).await,
            );
        }
        let result = match self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(self.session_key(session)?)
            .upload_id(self.upload_id(session)?)
            .customize()
            .interceptor(publication.clone())
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if error.code() == Some("NoSuchUpload") => {
                tracing::debug!("S3 upload already absent");
                Ok(())
            }
            Err(error) => Err(s3_error(error)),
        };
        if self.settings.copy_publication.multipart().is_some() {
            crate::transfer::with_cleanup(result, self.delete_temporary(session).await)
        } else {
            result
        }
    }
}

fn aws_credentials(credentials: S3Credentials) -> aws_credential_types::Credentials {
    aws_credential_types::Credentials::new(
        credentials.access_key_id,
        credentials.secret_access_key.as_str(),
        None,
        None,
        "coven",
    )
}

#[cfg(test)]
#[path = "s3_tests.rs"]
mod tests;
