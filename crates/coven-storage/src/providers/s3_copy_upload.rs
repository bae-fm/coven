use super::*;
use crate::transfer::with_cleanup;

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
            SessionState::S3 { token, .. } | SessionState::S3Restart { token, .. } => token,
            _ => return Err(StorageFailure::SessionMismatch.into()),
        };
        self.upload_key(&session.path, token)
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
                let SessionState::S3 { token, .. } = &session.state else {
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
                self.completed(session).await
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
    /// (§4): stage the bytes at a fresh temporary carrying this attempt's
    /// marker, then copy it with the destination condition. A refused copy,
    /// or one whose reply was lost, is this attempt's publication exactly
    /// when the destination carries its marker; otherwise the copy's own
    /// answer stands. Nothing resumes a single request, so the temporary is
    /// deleted whatever the outcome, and a failed deletion is reported with it.
    pub(super) async fn create_by_copy(
        &self,
        publication: &PublicationRequest,
        path: &ObjectPath,
        bytes: &[u8],
        condition: S3CopyCondition,
    ) -> Result<(), StorageError> {
        let token = SecretText::new(self.ids.new_id().to_string());
        let temporary = self.temporary_key(path, &token)?;
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

pub(super) fn is_temporary(relative: &str, prefix: &ObjectPrefix) -> Result<bool, StorageError> {
    let directory = format!(
        "{}{TEMPORARIES}/",
        ObjectPrefix::all(prefix.store()).as_str()
    );
    let Some(id) = relative.strip_prefix(&directory) else {
        return Ok(false);
    };
    if !relative.starts_with(prefix.as_str()) {
        return Err(StorageFailure::Protocol.with_source("S3 listed temporary outside prefix"));
    }
    let parsed =
        uuid::Uuid::parse_str(id).map_err(|source| StorageFailure::Protocol.with_source(source))?;
    if parsed.to_string() != id {
        return Err(StorageFailure::Protocol.with_source("S3 listed malformed upload temporary"));
    }
    Ok(true)
}
