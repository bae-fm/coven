use super::pagination::Pagination;
use crate::session::{S3Part, SessionState};
use crate::*;
use async_trait::async_trait;
use aws_sdk_s3::{
    config::{Region, RequestChecksumCalculation, ResponseChecksumValidation},
    error::{ProvideErrorMetadata, SdkError},
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use coven_crypto::SecretText;
use coven_foundation::{clock::ClockRef, id_source::IdSourceRef};
use std::fmt;

/// S3 and compatible endpoints, using only object operations and manually supplied keys.
pub(crate) struct S3Storage {
    client: Client,
    credentials: SigningCredentials,
    config: StorageConfig,
    bucket: String,
    prefix: String,
    ids: IdSourceRef,
}
struct SigningClock(ClockRef);
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
impl fmt::Debug for SigningClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SigningClock")
    }
}
impl aws_smithy_async::time::TimeSource for SigningClock {
    fn now(&self) -> std::time::SystemTime {
        self.0.now()
    }
}

impl S3Storage {
    /// Construct at a composition root. No credential discovery, IAM calls, key
    /// creation or revocation occurs. The SDK computes protocol checksums itself.
    pub(crate) fn new(
        config: StorageConfig,
        credentials: S3Credentials,
        clock: ClockRef,
        ids: IdSourceRef,
    ) -> Result<Self, StorageError> {
        let StorageConfig::S3 {
            bucket,
            region,
            endpoint,
            prefix,
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
        let mut builder = aws_sdk_s3::config::Builder::new()
            .behavior_version_latest()
            .region(Region::new(region.clone()))
            .credentials_provider(credentials.clone())
            .identity_cache(aws_sdk_s3::config::IdentityCache::no_cache())
            .time_source(SigningClock(clock))
            .force_path_style(true)
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired);
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
        })
    }
    async fn put(&self, path: &ObjectPath, bytes: &[u8], create: bool) -> Result<(), StorageError> {
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(path.under(&self.prefix))
            .body(ByteStream::from(bytes.to_vec()));
        if create {
            request = request.if_none_match("*");
        }
        request.send().await.map_err(s3_error)?;
        Ok(())
    }
    async fn get(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<Vec<u8>, StorageError> {
        let mut request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(path.under(&self.prefix));
        if let Some(range) = range {
            request = request.range(range.header());
        }
        let response = request.send().await.map_err(s3_error)?;
        if let Some(range) = range {
            super::http::validate_content_range(
                response
                    .content_range()
                    .ok_or(StorageFailure::Protocol.with_source("S3 ignored byte range"))?,
                range,
            )?;
        }
        let length = response
            .content_length()
            .ok_or(StorageFailure::Protocol.with_source("S3 omitted content length"))?;
        let bytes = response
            .body
            .collect()
            .await
            .map_err(|source| StorageError::Provider {
                provider: CloudProvider::S3,
                failure: StorageFailure::Network,
                source: Box::new(source),
            })?
            .into_bytes()
            .to_vec();
        if u64::try_from(length).map_err(|error| StorageFailure::Protocol.with_source(error))?
            != bytes.len() as u64
            || range.is_some_and(|range| range.len() != bytes.len() as u64)
        {
            return Err(StorageFailure::Protocol.with_source("short S3 body"));
        }
        Ok(bytes)
    }
    fn upload_id<'a>(&self, session: &'a UploadSession) -> Result<&'a str, StorageError> {
        match &session.state {
            SessionState::S3 { id, .. } => Ok(id.as_str()),
            _ => Err(StorageFailure::SessionMismatch.into()),
        }
    }
    async fn completed(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        let SessionState::S3 { token, .. } = &session.state else {
            return Err(StorageFailure::SessionMismatch.into());
        };
        let response = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(session.path.under(&self.prefix))
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
            != Some(session.total)
            || response
                .metadata()
                .and_then(|metadata| metadata.get("coven-upload"))
                .map(String::as_str)
                != Some(token.as_str())
        {
            return Err(StorageFailure::AlreadyExists.into());
        }
        session.confirmed = session.total;
        session.state = SessionState::Complete;
        Ok(())
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
        5 * 1024 * 1024 * 1024
    }
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.put(path, bytes, true).await
    }
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.put(path, bytes, false).await
    }
    async fn read(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError> {
        self.get(path, None).await
    }
    async fn read_range(
        &self,
        path: &ObjectPath,
        range: ByteRange,
    ) -> Result<Vec<u8>, StorageError> {
        self.get(path, Some(range)).await
    }
    async fn list(&self, listing: &mut ObjectListing) -> Result<(), StorageError> {
        let prefix = listing.prefix().clone();
        let full = prefix.under(&self.prefix);
        let root = ObjectPrefix::all().under(&self.prefix);
        let mut marker = None;
        let mut seen = Pagination::new();
        loop {
            let response = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full)
                .set_continuation_token(marker)
                .send()
                .await
                .map_err(s3_error)?;
            for object in response.contents() {
                let key = object
                    .key()
                    .ok_or(StorageFailure::Protocol.with_source("S3 list entry omitted key"))?;
                let relative = key
                    .strip_prefix(&root)
                    .ok_or(StorageFailure::Protocol.with_source("S3 listed another location"))?;
                let path = ObjectPath::parse(relative)?;
                if !prefix.contains(&path) {
                    return Err(StorageFailure::Protocol.with_source("S3 listed outside prefix"));
                }
                let size = object
                    .size()
                    .and_then(|size| u64::try_from(size).ok())
                    .ok_or(StorageFailure::Protocol.with_source("S3 omitted object size"))?;
                let stored_at = (*object
                    .last_modified()
                    .ok_or(StorageFailure::Protocol.with_source("S3 omitted storage time"))?)
                .try_into()
                .map_err(|error| StorageFailure::Encoding.with_source(error))?;
                let object = StoredObject {
                    path,
                    size,
                    stored_at,
                };
                listing.insert(object)?;
            }
            if !response
                .is_truncated()
                .ok_or(StorageFailure::Protocol.with_source("S3 omitted page completion"))?
            {
                break;
            }
            let next = response
                .next_continuation_token()
                .filter(|value| !value.is_empty())
                .ok_or(StorageFailure::Protocol.with_source("S3 omitted continuation token"))?
                .to_owned();
            seen.check(&next)?;
            marker = Some(next);
        }
        Ok(())
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        match self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(path.under(&self.prefix))
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if matches!(error.code(), Some("NoSuchKey" | "NotFound")) => {
                tracing::debug!(path = path.as_str(), "S3 object already absent");
                Ok(())
            }
            Err(error) => Err(s3_error(error)),
        }
    }
    async fn grant_access(&self, _account: &str) -> Result<AccessGrant, StorageError> {
        Ok(AccessGrant::CreateAccessKey)
    }
    async fn revoke_access(
        &self,
        member: &MemberAccess,
    ) -> Result<ProviderRevocation, StorageError> {
        match member {
            MemberAccess::S3AccessKey { access_key_id } => Ok(ProviderRevocation::Reported(
                MemberRemoval::DeleteAccessKey {
                    access_key_id: access_key_id.clone(),
                },
            )),
            _ => Err(StorageFailure::InvalidConfiguration
                .with_source("S3 revocation requires the member's access key")),
        }
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        let part_size = crate::session::s3_part_size(total)?;
        let token = SecretText::new(self.ids.new_id().to_string());
        let response = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(path.under(&self.prefix))
            .metadata("coven-upload", token.as_str())
            .send()
            .await
            .map_err(s3_error)?;
        let id = response
            .upload_id()
            .filter(|id| !id.is_empty())
            .ok_or(StorageFailure::Protocol.with_source("S3 omitted upload id"))?;
        Ok(UploadSession {
            location: self.config(),
            path: path.clone(),
            total,
            confirmed: 0,
            part_size,
            state: SessionState::S3 {
                id: SecretText::new(id.into()),
                token,
                parts: Vec::new(),
            },
        })
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
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
                .key(session.path.under(&self.prefix))
                .upload_id(id.as_str())
                .set_part_number_marker(marker)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) if error.code() == Some("NoSuchUpload") => {
                    return self.completed(session).await
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
        session: &mut UploadSession,
        bytes: &[u8],
        end: u64,
    ) -> Result<(), StorageError> {
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
            .key(session.path.under(&self.prefix))
            .upload_id(id)
            .part_number(number)
            .body(ByteStream::from(bytes.to_vec()))
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
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
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
        let result = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(session.path.under(&self.prefix))
            .upload_id(id)
            .if_none_match("*")
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed))
                    .build(),
            )
            .send()
            .await;
        match result {
            Ok(_) => {
                session.state = SessionState::Complete;
                Ok(())
            }
            Err(error) if matches!(error.code(), Some("NoSuchUpload" | "PreconditionFailed")) => {
                self.completed(session).await
            }
            Err(error) => Err(s3_error(error)),
        }
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        match self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(session.path.under(&self.prefix))
            .upload_id(self.upload_id(session)?)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if error.code() == Some("NoSuchUpload") => {
                tracing::debug!("S3 upload already absent");
                Ok(())
            }
            Err(error) => Err(s3_error(error)),
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

fn s3_error<E>(error: SdkError<E>) -> StorageError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let status = match &error {
        SdkError::ServiceError(service) => Some(service.raw().status().as_u16()),
        _ => None,
    };
    let failure = match error.code() {
        Some(
            "InvalidAccessKeyId"
            | "SignatureDoesNotMatch"
            | "InvalidToken"
            | "ExpiredToken"
            | "InvalidClientTokenId",
        ) => StorageFailure::Authentication,
        Some("AccessDenied" | "AllAccessDisabled" | "AccountProblem") => {
            StorageFailure::PermissionDenied
        }
        Some("NoSuchBucket") => StorageFailure::ContainerNotFound,
        Some("NoSuchKey" | "NotFound") => StorageFailure::NotFound,
        Some("NoSuchUpload") => StorageFailure::SessionExpired,
        Some("PreconditionFailed") => StorageFailure::AlreadyExists,
        Some("ConditionalRequestConflict" | "SlowDown" | "Throttling") => {
            StorageFailure::RateLimited
        }
        Some(
            "PermanentRedirect"
            | "AuthorizationHeaderMalformed"
            | "IncorrectEndpoint"
            | "IllegalLocationConstraintException",
        ) => StorageFailure::RegionMismatch,
        Some("OverQuota" | "QuotaExceeded" | "InsufficientStorage") => {
            StorageFailure::QuotaExceeded
        }
        Some("InvalidRange") => StorageFailure::InvalidRange,
        _ => match status {
            Some(401) => StorageFailure::Authentication,
            Some(403) => StorageFailure::PermissionDenied,
            Some(404) => StorageFailure::NotFound,
            Some(412) => StorageFailure::AlreadyExists,
            Some(429) => StorageFailure::RateLimited,
            Some(408 | 500..=599) | None => StorageFailure::Network,
            _ => StorageFailure::Refused,
        },
    };
    StorageError::Provider {
        provider: CloudProvider::S3,
        failure,
        source: Box::new(S3ResponseError(error)),
    }
}

// SDK service errors can echo credentials in their message or response body.
// Keep the original typed cause available through Error::source, but do not
// include it in ordinary storage error formatting.
struct S3ResponseError<E>(SdkError<E>);
impl<E> fmt::Debug for S3ResponseError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("S3ResponseError")
    }
}
impl<E> fmt::Display for S3ResponseError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("S3 request failed")
    }
}
impl<E: std::error::Error + Send + Sync + 'static> std::error::Error for S3ResponseError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

#[cfg(test)]
#[path = "s3_tests.rs"]
mod tests;
