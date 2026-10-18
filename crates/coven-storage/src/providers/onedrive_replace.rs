//! Replace the floor through an upload session bound to the observed revision.
use super::*;
use crate::session::ONEDRIVE_PART_SIZE;
use crate::transfer::with_cleanup;

pub(super) fn url(session: &UploadSession) -> Result<&str, StorageError> {
    match &session.state {
        SessionState::OneDriveReplacement { url } => Ok(url.as_str()),
        _ => Err(StorageFailure::SessionMismatch.into()),
    }
}

impl OneDriveStorage {
    /// A direct PUT /content does not exclude competing replacements. A native
    /// session retains the observed eTag through its final fragment (E.2).
    async fn replacement_url(
        &self,
        publication: &PublicationRequest,
        previous: &StoredObject,
        deferred: bool,
    ) -> Result<SecretText, StorageError> {
        let created = http::json(
            PROVIDER,
            self.session
                .publish(
                    publication,
                    Method::POST,
                    &self.item(&previous.object_id, &["createUploadSession"])?,
                    &[("If-Match", previous.replace_token.clone())],
                    Body::Json(
                        json!({"item": {"@microsoft.graph.conflictBehavior": "replace"},
                "deferCommit": deferred}),
                    ),
                    true,
                )
                .await?,
        )
        .await;
        let created = self.checked_folder_operation(created).await?;
        let url = http::string(&created, "uploadUrl")?;
        validate_download_url(&self.api, url)?;
        Ok(SecretText::new(url.into()))
    }

    pub(super) async fn begin_replacement(
        &self,
        publication: &PublicationRequest,
        previous: &StoredObject,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        let url = self.replacement_url(publication, previous, false).await?;
        Ok(UploadSession {
            location: self.config(),
            target: UploadTarget::Replace(previous.clone()),
            total,
            confirmed: 0,
            part_size: ONEDRIVE_PART_SIZE,
            state: SessionState::OneDriveReplacement { url },
        })
    }

    pub(super) async fn resume_replacement(
        &self,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        let response = self
            .session
            .send(Method::GET, url(session)?, &[], Body::Empty, false)
            .await?;
        let (value, receipt) = http::json_response(PROVIDER, response)
            .await
            .map_err(replacement_error)?;
        session.confirmed = upload::next_offset(
            &value["nextExpectedRanges"],
            session.confirmed,
            session.total,
            || {
                http::invalid_response(
                    PROVIDER,
                    receipt.status(),
                    receipt.headers().clone(),
                    receipt.body().to_vec(),
                )
            },
        )?;
        Ok(())
    }

    async fn replacement_fragment(
        &self,
        publication: &PublicationRequest,
        previous: &StoredObject,
        url: &str,
        start: u64,
        total: u64,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        let end = start + bytes.len() as u64;
        let body = Body::Bytes(bytes.to_vec());
        let mut headers = body_headers(&body);
        headers.push((
            "Content-Range",
            format!("bytes {start}-{}/{total}", end - 1),
        ));
        let response = self
            .session
            .publish(publication, Method::PUT, url, &headers, body, false)
            .await?;
        acknowledged(response, previous, end, total).await
    }

    pub(super) async fn replacement_part(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
        bytes: &[u8],
        end: u64,
    ) -> Result<(), StorageError> {
        let UploadTarget::Replace(previous) = session.target() else {
            return Err(StorageFailure::SessionMismatch.into());
        };
        self.replacement_fragment(
            publication,
            previous,
            url(session)?,
            session.confirmed,
            session.total,
            bytes,
        )
        .await?;
        session.confirmed = end;
        if end == session.total {
            session.state = SessionState::Complete;
        }
        Ok(())
    }

    pub(super) async fn cancel_replacement(&self, url: &str) -> Result<(), StorageError> {
        let response = self
            .session
            .send(Method::DELETE, url, &[], Body::Empty, false)
            .await?;
        if response.status().as_u16() == 404 {
            return Ok(());
        }
        match http::checked(PROVIDER, response).await {
            Err(error) if upload::native_code(&error, "resourceModified") => Ok(()),
            result => result.map(drop),
        }
    }

    pub(super) async fn replace_content(
        &self,
        publication: &PublicationRequest,
        previous: &StoredObject,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        // An empty replacement needs explicit completion; nonempty sessions
        // publish when their final fragment arrives.
        let url = self
            .replacement_url(publication, previous, bytes.is_empty())
            .await?;
        let result = async {
            if bytes.is_empty() {
                let response = self
                    .session
                    .publish(
                        publication,
                        Method::POST,
                        url.as_str(),
                        &[
                            ("If-Match", previous.replace_token.clone()),
                            ("Content-Length", "0".into()),
                        ],
                        Body::Bytes(Vec::new()),
                        false,
                    )
                    .await?;
                return acknowledged(response, previous, 0, 0).await;
            }
            let mut offset = 0;
            for part in bytes.chunks(ONEDRIVE_PART_SIZE) {
                self.replacement_fragment(
                    publication,
                    previous,
                    url.as_str(),
                    offset,
                    bytes.len() as u64,
                    part,
                )
                .await?;
                offset += part.len() as u64;
            }
            Ok(())
        }
        .await;
        if result.is_ok() {
            return result;
        }
        with_cleanup(result, self.cancel_replacement(url.as_str()).await)
    }
}

fn replacement_error(mut error: StorageError) -> StorageError {
    if let StorageError::Provider {
        failure: failure @ (StorageFailure::NotFound | StorageFailure::AlreadyExists),
        ..
    } = &mut error
    {
        // A changed item invalidates the old session. Never acquire a newer eTag.
        *failure = StorageFailure::Conflict;
    }
    error
}

/// Accept only what the session protocol promises: before the last byte, a 202
/// asking for exactly the next byte; after it, the observed item at its new size.
async fn acknowledged(
    response: reqwest::Response,
    previous: &StoredObject,
    end: u64,
    total: u64,
) -> Result<(), StorageError> {
    let (value, receipt) = http::json_response(PROVIDER, response)
        .await
        .map_err(replacement_error)?;
    let invalid = || {
        http::invalid_response(
            PROVIDER,
            receipt.status(),
            receipt.headers().clone(),
            receipt.body().to_vec(),
        )
    };
    let accepted = if end < total {
        receipt.status() == 202
            && upload::next_offset(&value["nextExpectedRanges"], end, total, invalid)? == end
    } else {
        matches!(receipt.status(), 200 | 201)
            && value["id"].as_str() == Some(previous.object_id.as_str())
            && value["name"].as_str() == Some(previous.path.file_name())
            && value["file"].is_object()
            && value["size"].as_u64() == Some(total)
    };
    if !accepted {
        return Err(invalid());
    }
    Ok(())
}
