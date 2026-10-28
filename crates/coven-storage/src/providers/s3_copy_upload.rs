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
            None => Ok(self.prefix.key(path)),
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
        self.temporary_for_store(path.store(), token)
    }

    fn temporary_for_store(
        &self,
        store: coven_foundation::id_source::StoreId,
        token: &SecretText,
    ) -> Result<String, StorageError> {
        let id = uuid::Uuid::parse_str(token.as_str())
            .map_err(|source| StorageFailure::SessionMismatch.with_source(source))?;
        Ok(format!(
            "{}{TEMPORARIES}/{id}",
            self.prefix.key_prefix_of(&ObjectPrefix::all(store))
        ))
    }

    pub(super) fn session_key(&self, session: &UploadSession) -> Result<String, StorageError> {
        let token = match &session.state {
            SessionState::S3 { token, .. }
            | SessionState::S3Restart { token, .. }
            | SessionState::S3Planned { token, .. } => token,
            _ => return Err(StorageFailure::SessionMismatch.into()),
        };
        self.upload_key(session.path(), token)
    }

    /// The temporary a single-request create stages, named by the create
    /// itself: its destination and bytes. Every attempt of one create, a
    /// retry after a crash or a failed deletion included, stages and deletes
    /// this same name, so the caller's record of the create, which it keeps
    /// until the create settles, records the temporary too (Appendix E.10).
    pub(super) fn create_temporary_key<'a>(
        &self,
        path: impl Into<Destination<'a>>,
        bytes: &[u8],
    ) -> Result<String, StorageError> {
        self.publication_temporary_key(path.into(), bytes, None)
    }

    fn publication_temporary_key(
        &self,
        path: Destination<'_>,
        bytes: &[u8],
        expected: Option<&str>,
    ) -> Result<String, StorageError> {
        let mut hasher = coven_crypto::ObjectHasher::new();
        match expected {
            None => hasher.update(b"coven/copy-temporary/v1"),
            Some(revision) => {
                hasher.update(b"coven/replace-temporary/v1");
                hasher.update(&(revision.len() as u64).to_be_bytes());
                hasher.update(revision.as_bytes());
            }
        }
        hasher.update(&(path.as_str().len() as u64).to_be_bytes());
        hasher.update(path.as_str().as_bytes());
        hasher.update(bytes);
        let digest = hasher.finish();
        let mut id = [0; 16];
        id.copy_from_slice(&digest.as_bytes()[..16]);
        let token = SecretText::new(uuid::Uuid::from_bytes(id).to_string());
        self.temporary_for_store(path.store(), &token)
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
            .metadata("coven-revision", token.as_str())
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

    /// Abort every native upload at a planned session's name: one its
    /// initiation started before the reply was lost. Only a private staging
    /// name proves ownership; a shared destination does not (Appendix E.10).
    pub(super) async fn abort_initiated(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        if self.settings.copy_publication.multipart().is_none() {
            return Err(StorageFailure::SessionIdentityUnknown.into());
        }
        let key = self.session_key(session)?;
        self.abort_private_uploads(&key, Some(publication)).await
    }

    /// A fresh private name proves ownership of every native upload at that
    /// exact key, including an initiation whose reply never arrived. Probes
    /// have no sequence publication; ordinary uploads retain their gate.
    pub(super) async fn abort_private_uploads(
        &self,
        key: &str,
        publication: Option<&PublicationRequest>,
    ) -> Result<(), StorageError> {
        let mut markers: Option<(String, String)> = None;
        let mut seen = Pagination::new();
        let mut result = Ok(());
        loop {
            let mut request = self
                .client
                .list_multipart_uploads()
                .bucket(&self.bucket)
                .prefix(key)
                .set_key_marker(markers.as_ref().map(|(key, _)| key.clone()))
                .set_upload_id_marker(markers.as_ref().map(|(_, id)| id.clone()))
                .customize();
            if let Some(publication) = publication {
                request = request.interceptor(publication.clone());
            }
            let response = match request.send().await.map_err(s3_error) {
                Ok(response) => response,
                Err(error) => return with_cleanup(result, Err(error)),
            };
            for upload in response.uploads() {
                if upload.key() != Some(key) {
                    continue;
                }
                let id = match upload.upload_id().filter(|id| !id.is_empty()) {
                    Some(id) => id,
                    None => {
                        result = with_cleanup(
                            result,
                            Err(StorageFailure::Protocol.with_source("S3 omitted upload id")),
                        );
                        continue;
                    }
                };
                let mut request = self
                    .client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(id)
                    .customize();
                if let Some(publication) = publication {
                    request = request.interceptor(publication.clone());
                }
                let aborted = match request.send().await {
                    Ok(_) => Ok(()),
                    Err(error) if error.code() == Some("NoSuchUpload") => Ok(()),
                    Err(error) => Err(s3_error(error)),
                };
                result = with_cleanup(result, aborted);
            }
            if !response.is_truncated().unwrap_or(false) {
                return result;
            }
            let next = match response.next_key_marker().filter(|value| !value.is_empty()) {
                Some(key) => (
                    key.to_owned(),
                    response
                        .next_upload_id_marker()
                        .unwrap_or_default()
                        .to_owned(),
                ),
                None => {
                    return with_cleanup(
                        result,
                        Err(StorageFailure::Protocol.with_source("S3 omitted upload marker")),
                    )
                }
            };
            if let Err(error) = seen.check(&format!("{}\n{}", next.0, next.1)) {
                return with_cleanup(result, Err(error));
            }
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
                let (SessionState::S3 { token, .. } | SessionState::S3Planned { token, .. }) =
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
                Some(publication),
                &self.session_key(session)?,
                Destination::Object(session.path()),
                condition,
                match session.target() {
                    UploadTarget::Create(_) => None,
                    UploadTarget::Replace(previous) => Some(&previous.replace_token),
                },
            )
            .await;
        let result = match result.map_err(s3_error) {
            // A concurrent mutation, under any service's code (Appendix E.10).
            Err(error) if error.failure() == StorageFailure::Conflict => {
                return self
                    .require_restart(
                        publication,
                        session,
                        StorageFailure::SessionExpired.with_source(error),
                    )
                    .await;
            }
            result => result,
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
    pub(super) async fn copy(
        &self,
        publication: Option<&PublicationRequest>,
        temporary: &str,
        path: Destination<'_>,
        condition: S3CopyCondition,
        expected: Option<&str>,
    ) -> Result<
        aws_sdk_s3::operation::copy_object::CopyObjectOutput,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::copy_object::CopyObjectError>,
    > {
        let (header, value) = match expected {
            None => {
                let (header, value) = condition.header();
                (header, value.to_owned())
            }
            Some(revision) => (
                match condition {
                    S3CopyCondition::IfNoneMatch => "if-match",
                    S3CopyCondition::GenerationMatch => "x-goog-if-generation-match",
                    S3CopyCondition::CloudflareIfNoneMatch => "cf-copy-destination-if-match",
                },
                revision.to_owned(),
            ),
        };
        let mut request = self
            .client
            .copy_object()
            .bucket(&self.bucket)
            .key(path.key(&self.prefix))
            .copy_source(format!("/{}/{temporary}", self.bucket))
            .customize()
            .mutate_request(move |request| {
                request.headers_mut().insert(header, value.clone());
            });
        if let Some(publication) = publication {
            request = request.interceptor(publication.clone());
        }
        request.send().await
    }

    /// A single-request create on a profile publishing every object by copy
    /// (Appendix E.10): stage the bytes at the create's temporary carrying this
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
        publication: Option<&PublicationRequest>,
        path: Destination<'_>,
        bytes: &[u8],
        condition: S3CopyCondition,
        sent: &ByteProgress,
    ) -> Result<(), StorageError> {
        self.publish_by_copy(publication, path, bytes, condition, None, sent)
            .await
    }

    pub(super) async fn publish_by_copy(
        &self,
        publication: Option<&PublicationRequest>,
        path: Destination<'_>,
        bytes: &[u8],
        condition: S3CopyCondition,
        expected: Option<&str>,
        sent: &ByteProgress,
    ) -> Result<(), StorageError> {
        let token = SecretText::new(self.ids.new_id().to_string());
        let temporary = match expected {
            None => self.create_temporary_key(path, bytes)?,
            Some(_) => self.publication_temporary_key(path, bytes, expected)?,
        };
        let published = async {
            let mut put = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(&temporary)
                .body(ByteStream::from(bytes.to_vec()))
                .metadata("coven-upload", token.as_str());
            if path.is_replaceable() {
                put = put.metadata("coven-revision", token.as_str());
            }
            // The temporary carries the object's bytes; the copy sends none.
            let mut put = put.customize().mutate_request({
                let sent = sent.clone();
                move |request| request.add_extension(sent.clone())
            });
            if let Some(publication) = publication {
                put = put.interceptor(publication.clone());
            }
            put.send().await.map_err(s3_error)?;
            match self
                .copy(publication, &temporary, path, condition, expected)
                .await
                .map_err(s3_error)
            {
                Ok(_) => Ok(()),
                // Floor recovery authenticates the current floor after an
                // uncertain result. A shared temporary's marker cannot turn a
                // different attempt's refused revision into success.
                Err(error) if expected.is_some() => {
                    Err(if error.failure() == StorageFailure::NotFound {
                        StorageFailure::Conflict.with_source(error)
                    } else {
                        error
                    })
                }
                Err(error)
                    if matches!(
                        error.failure(),
                        StorageFailure::AlreadyExists | StorageFailure::Conflict
                    ) || error.retryable() =>
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
