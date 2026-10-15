use super::*;
use crate::transfer::with_cleanup;
use aws_sdk_s3::error::ProvideErrorMetadata;

const TEMPORARIES: &str = ".coven-uploads";

impl S3Storage {
    pub(super) fn upload_key(
        &self,
        path: &ObjectPath,
        token: &SecretText,
    ) -> Result<String, StorageError> {
        match self.settings.copy_publication.multipart() {
            None => Ok(path.under(&self.prefix)),
            Some(_) => self.temporary_key(path, token),
        }
    }

    /// The reserved temporary a publication named by `token` stages, outside
    /// every object path of `path`'s store.
    pub(super) fn temporary_key(
        &self,
        path: &ObjectPath,
        token: &SecretText,
    ) -> Result<String, StorageError> {
        let id = uuid::Uuid::parse_str(token.as_str())
            .map_err(|source| StorageFailure::SessionMismatch.with_source(source))?;
        Ok(format!(
            "{}{TEMPORARIES}/{id}",
            ObjectPrefix::all(path.store()).under(&self.prefix)
        ))
    }

    pub(super) fn session_key(&self, session: &UploadSession) -> Result<String, StorageError> {
        let token = match &session.state {
            SessionState::S3 { token, .. }
            | SessionState::S3Restart { token, .. }
            | SessionState::S3Planned { token } => token,
            _ => return Err(StorageFailure::SessionMismatch.into()),
        };
        self.upload_key(&session.path, token)
    }

    /// The temporary a single-request create stages, named by the create
    /// itself: its destination and bytes. Every attempt of one create, a
    /// retry after a crash or a failed deletion included, stages and deletes
    /// this same name, so the caller's record of the create, which it keeps
    /// until the create settles, records the temporary too (§4).
    pub(super) fn create_temporary_key(
        &self,
        path: &ObjectPath,
        bytes: &[u8],
    ) -> Result<String, StorageError> {
        let mut hasher = coven_crypto::ObjectHasher::new();
        hasher.update(b"coven/copy-temporary/v1");
        hasher.update(&(path.as_str().len() as u64).to_be_bytes());
        hasher.update(path.as_str().as_bytes());
        hasher.update(bytes);
        let digest = hasher.finish();
        let mut id = [0; 16];
        id.copy_from_slice(&digest.as_bytes()[..16]);
        let token = SecretText::new(uuid::Uuid::from_bytes(id).to_string());
        self.temporary_key(path, &token)
    }

    /// Initiate a native multipart upload at `key`, carrying `token` as its
    /// marker.
    pub(super) async fn initiate(
        &self,
        publication: &PublicationRequest,
        key: &str,
        token: &SecretText,
    ) -> Result<SecretText, StorageError> {
        let response = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .metadata("coven-upload", token.as_str())
            .customize()
            .interceptor(publication.clone())
            .send()
            .await
            .map_err(s3_error)?;
        let id = response
            .upload_id()
            .filter(|id| !id.is_empty())
            .ok_or(StorageFailure::Protocol.with_source("S3 omitted upload id"))?;
        Ok(SecretText::new(id.into()))
    }

    /// Abort every native upload at a planned session's temporary: one its
    /// initiation started before the reply was lost. The name is the
    /// recording's own, so no other session's upload is there (§4).
    pub(super) async fn abort_initiated(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        let key = self.session_key(session)?;
        let mut markers: Option<(String, String)> = None;
        let mut seen = Pagination::new();
        loop {
            let response = self
                .client
                .list_multipart_uploads()
                .bucket(&self.bucket)
                .prefix(&key)
                .set_key_marker(markers.as_ref().map(|(key, _)| key.clone()))
                .set_upload_id_marker(markers.as_ref().map(|(_, id)| id.clone()))
                .customize()
                .interceptor(publication.clone())
                .send()
                .await
                .map_err(s3_error)?;
            for upload in response.uploads() {
                if upload.key() != Some(key.as_str()) {
                    continue;
                }
                let id = upload
                    .upload_id()
                    .filter(|id| !id.is_empty())
                    .ok_or(StorageFailure::Protocol.with_source("S3 omitted upload id"))?;
                match self
                    .client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&key)
                    .upload_id(id)
                    .customize()
                    .interceptor(publication.clone())
                    .send()
                    .await
                {
                    Ok(_) => tracing::debug!("aborted an unrecorded S3 upload initiation"),
                    Err(error) if error.code() == Some("NoSuchUpload") => {}
                    Err(error) => return Err(s3_error(error)),
                }
            }
            if !response.is_truncated().unwrap_or(false) {
                return Ok(());
            }
            let next = (
                response
                    .next_key_marker()
                    .filter(|value| !value.is_empty())
                    .ok_or(StorageFailure::Protocol.with_source("S3 omitted upload marker"))?
                    .to_owned(),
                response
                    .next_upload_id_marker()
                    .unwrap_or_default()
                    .to_owned(),
            );
            seen.check(&format!("{}\n{}", next.0, next.1))?;
            markers = Some(next);
        }
    }

    pub(super) async fn recover_completed_upload(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        if self.settings.copy_publication.multipart().is_none() {
            return self.completed(session).await;
        }
        // NoSuchUpload leaves exactly one unknown: did assembly create the
        // temporary? If not, only this session's already-published object counts.
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(self.session_key(session)?)
            .customize()
            .interceptor(publication.clone())
            .send()
            .await
            .map_err(s3_error)
        {
            Ok(response) => {
                let (SessionState::S3 { token, .. } | SessionState::S3Planned { token }) =
                    &session.state
                else {
                    return Err(StorageFailure::SessionMismatch.into());
                };
                let total = i64::try_from(session.total)
                    .map_err(|source| StorageFailure::InvalidPart.with_source(source))?;
                if response.content_length() != Some(total)
                    || response
                        .metadata()
                        .and_then(|metadata| metadata.get("coven-upload"))
                        .map(String::as_str)
                        != Some(token.as_str())
                {
                    return Err(StorageFailure::Protocol
                        .with_source("S3 upload temporary disagrees with session"));
                }
                self.publish_copy(publication, session).await
            }
            Err(error) if error.failure() == StorageFailure::NotFound => {
                match (self.completed(session).await, &session.state) {
                    // A planned session that never assembled starts over.
                    (Err(error), SessionState::S3Planned { .. })
                        if error.failure() == StorageFailure::SessionExpired =>
                    {
                        Ok(())
                    }
                    (result, _) => result,
                }
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn publish_copy(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        let Some(condition) = self.settings.copy_publication.multipart() else {
            return Err(
                StorageFailure::Protocol.with_source("copy publication requires a copy profile")
            );
        };
        let result = self
            .copy(
                publication,
                &self.session_key(session)?,
                &session.path,
                condition,
            )
            .await;
        let result = match result {
            Err(error)
                if matches!(
                    error.code(),
                    Some("ConditionalRequestConflict" | "OperationAborted")
                ) =>
            {
                return self
                    .require_restart(
                        publication,
                        session,
                        StorageFailure::SessionExpired.with_source(s3_error(error)),
                    )
                    .await;
            }
            result => result.map_err(s3_error),
        };
        let result = match result {
            Ok(_) => Ok(()),
            Err(error) if error.failure() == StorageFailure::AlreadyExists => {
                match self.confirm_published(session).await {
                    Ok(()) => Ok(()),
                    Err(confirmation)
                        if confirmation.failure() == StorageFailure::AlreadyExists =>
                    {
                        Err(error)
                    }
                    Err(confirmation) => Err(confirmation),
                }
            }
            // A lost reply or retryable failure leaves the temporary available
            // to the recorded session. A definitive refusal releases it.
            Err(error) if error.retryable() => return Err(error),
            Err(error) => Err(error),
        };
        with_cleanup(result, self.delete_temporary(session).await)?;
        session.confirmed = session.total;
        session.state = SessionState::Complete;
        Ok(())
    }

    /// One server-side copy of `temporary` to `path`, carrying the profile's
    /// destination condition.
    async fn copy(
        &self,
        publication: &PublicationRequest,
        temporary: &str,
        path: &ObjectPath,
        condition: S3CopyCondition,
    ) -> Result<
        aws_sdk_s3::operation::copy_object::CopyObjectOutput,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::copy_object::CopyObjectError>,
    > {
        let (header, value) = condition.header();
        self.client
            .copy_object()
            .bucket(&self.bucket)
            .key(path.under(&self.prefix))
            .copy_source(format!("/{}/{temporary}", self.bucket))
            .customize()
            .mutate_request(move |request| {
                request.headers_mut().insert(header, value);
            })
            .interceptor(publication.clone())
            .send()
            .await
    }

    /// A single-request create on a profile publishing every object by copy
    /// (§4): stage the bytes at the create's temporary carrying this
    /// attempt's fresh marker, then copy it with the destination condition.
    /// A refused copy, or one whose reply was lost, settles as stored when
    /// the destination carries its marker (this attempt's staged bytes, which
    /// another attempt's copy may have carried); otherwise the copy's own
    /// answer stands. A copy whose temporary another attempt
    /// of the same create already deleted is settled by the destination too:
    /// another object is an occupied path, an empty one a retryable conflict.
    /// The temporary is deleted whatever the outcome, and a failed deletion
    /// is reported with it; the caller's retry of the same create stages and
    /// deletes the same temporary, and finds its own bytes at the destination
    /// as an occupied path.
    pub(super) async fn create_by_copy(
        &self,
        publication: &PublicationRequest,
        path: &ObjectPath,
        bytes: &[u8],
        condition: S3CopyCondition,
    ) -> Result<(), StorageError> {
        let token = SecretText::new(self.ids.new_id().to_string());
        let temporary = self.create_temporary_key(path, bytes)?;
        let published = async {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&temporary)
                .body(ByteStream::from(bytes.to_vec()))
                .metadata("coven-upload", token.as_str())
                .customize()
                .interceptor(publication.clone())
                .send()
                .await
                .map_err(s3_error)?;
            match self
                .copy(publication, &temporary, path, condition)
                .await
                .map_err(s3_error)
            {
                Ok(_) => Ok(()),
                Err(error)
                    if error.failure() == StorageFailure::AlreadyExists || error.retryable() =>
                {
                    match self.confirm_marker(path, bytes.len() as u64, &token).await {
                        Ok(()) => Ok(()),
                        Err(_) => Err(error),
                    }
                }
                // No source: another attempt of this create, staging under
                // the same name, deleted it between this attempt's staging
                // and copy. The destination settles this one; an empty one
                // is retryable, the create still recorded by its caller.
                Err(error) if error.failure() == StorageFailure::NotFound => {
                    match self.confirm_marker(path, bytes.len() as u64, &token).await {
                        Ok(()) => Ok(()),
                        Err(empty) if empty.failure() == StorageFailure::SessionExpired => {
                            Err(StorageFailure::Conflict.with_source(error))
                        }
                        Err(confirmation) => Err(confirmation),
                    }
                }
                Err(error) => Err(error),
            }
        }
        .await;
        with_cleanup(published, self.delete_key(&temporary).await)
    }

    pub(super) async fn delete_temporary(
        &self,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        self.delete_key(&self.session_key(session)?).await
    }
}
