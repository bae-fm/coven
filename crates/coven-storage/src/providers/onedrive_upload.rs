//! Transfer to an unpublished name, then publish once through Graph copy.
use super::*;
use crate::transfer::with_cleanup;
use std::time::Duration;

pub(super) const TEMPORARIES: &str = ".coven-uploads";

fn temporary(session: &UploadSession) -> Result<uuid::Uuid, StorageError> {
    match &session.state {
        SessionState::OneDrive { temporary, .. } => Ok(*temporary),
        _ => Err(StorageFailure::SessionMismatch.into()),
    }
}
fn upload(session: &UploadSession) -> Result<&OneDriveUpload, StorageError> {
    match &session.state {
        SessionState::OneDrive { upload, .. } => Ok(upload),
        _ => Err(StorageFailure::SessionMismatch.into()),
    }
}
fn set_upload(session: &mut UploadSession, next: OneDriveUpload) -> Result<(), StorageError> {
    match &mut session.state {
        SessionState::OneDrive { upload, .. } => {
            *upload = next;
            Ok(())
        }
        _ => Err(StorageFailure::SessionMismatch.into()),
    }
}

/// Whether Graph named `code` as the error's own or an inner error's code.
pub(super) fn native_code(error: &StorageError, code: &str) -> bool {
    let StorageError::Provider { source, .. } = error else {
        return false;
    };
    let Some(response) = source.downcast_ref::<http::ProviderResponse>() else {
        return false;
    };
    let Ok(value) = serde_json::from_slice::<Value>(response.body()) else {
        return false;
    };
    std::iter::successors(Some(&value["error"]), |error| {
        Some(&error["innererror"]).filter(|inner| inner.is_object())
    })
    .any(|error| error["code"].as_str() == Some(code))
}

/// Earliest byte the provider still needs after accepting a sequential fragment.
pub(super) fn next_offset(
    ranges: &Value,
    confirmed: u64,
    total: u64,
    invalid: impl Fn() -> StorageError,
) -> Result<u64, StorageError> {
    let ranges = ranges.as_array().ok_or_else(&invalid)?;
    let mut first: Option<u64> = None;
    for range in ranges {
        let (start, end) = range
            .as_str()
            .and_then(|s| s.split_once('-'))
            .ok_or_else(&invalid)?;
        let start = start.parse::<u64>().map_err(|_| invalid())?;
        if start < confirmed || start >= total {
            return Err(invalid());
        }
        if !end.is_empty() {
            let end = end.parse::<u64>().map_err(|_| invalid())?;
            if end < start || end >= total {
                return Err(invalid());
            }
        }
        first = Some(match first {
            Some(previous) => previous.min(start),
            None => start,
        });
    }
    first.ok_or_else(invalid)
}

impl OneDriveStorage {
    fn temporary_url(
        &self,
        store: coven_foundation::id_source::StoreId,
        id: uuid::Uuid,
    ) -> Result<String, StorageError> {
        Ok(format!(
            "{}:/{store}/{TEMPORARIES}/{id}:",
            self.item(&self.folder, &[])?
        ))
    }
    async fn delete_temporary(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        self.delete_temporary_at(publication, session.path.store(), temporary(session)?)
            .await
    }
    async fn delete_temporary_at(
        &self,
        publication: &PublicationRequest,
        store: coven_foundation::id_source::StoreId,
        id: uuid::Uuid,
    ) -> Result<(), StorageError> {
        let response = self
            .publish(
                publication,
                Method::DELETE,
                &self.temporary_url(store, id)?,
                Body::Empty,
            )
            .await?;
        if response.status().as_u16() == 404 {
            self.check_folder().await?;
        } else {
            http::checked(PROVIDER, response).await?;
        }
        Ok(())
    }
    pub(super) fn plan_temporary(&self, path: &ObjectPath, total: u64) -> UploadSession {
        UploadSession {
            location: self.config(),
            path: path.clone(),
            total,
            confirmed: 0,
            part_size: crate::session::ONEDRIVE_PART_SIZE,
            state: SessionState::OneDrive {
                temporary: self.ids.new_id(),
                upload: OneDriveUpload::Planned { unstarted: true },
            },
        }
    }

    async fn initiate_temporary(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        // The caller already recorded this identity (§20.6). Keep it even if
        // initiation succeeds at Graph but its reply never reaches us.
        set_upload(session, OneDriveUpload::Planned { unstarted: false })?;
        self.directory(
            publication,
            &[&session.path.store().to_string(), TEMPORARIES],
        )
        .await?;
        let temporary = temporary(session)?;
        let url = format!(
            "{}/createUploadSession",
            self.temporary_url(session.path.store(), temporary)?
        );
        let started = async {
            let response = self
                .publish(
                    publication,
                    Method::POST,
                    &url,
                    Body::Json(json!({"item": {
                        "@microsoft.graph.conflictBehavior": "fail",
                        "name": temporary.to_string()
                    }})),
                )
                .await?;
            let value = http::json(PROVIDER, response).await?;
            let url = http::string(&value, "uploadUrl")?;
            validate_download_url(&self.api, url)?;
            Ok::<_, StorageError>(SecretText::new(url.into()))
        }
        .await;
        let url = self.checked_folder_operation(started).await?;
        set_upload(session, OneDriveUpload::Receiving { url })
    }

    pub(super) async fn resume_temporary(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        match upload(session)? {
            OneDriveUpload::Planned { unstarted } => {
                if !unstarted {
                    match self.recover_temporary(publication, session).await {
                        Err(error) if error.failure() == StorageFailure::SessionExpired => {
                            tracing::debug!("OneDrive temporary absent; reusing its recorded name");
                        }
                        result => return result,
                    }
                }
                self.initiate_temporary(publication, session).await
            }
            OneDriveUpload::Receiving { url } => {
                validate_download_url(&self.api, url.as_str())?;
                let response = self
                    .session
                    .publish(
                        publication,
                        Method::GET,
                        url.as_str(),
                        &[],
                        Body::Empty,
                        false,
                    )
                    .await?;
                if response.status().as_u16() == 404 {
                    return self.recover_temporary(publication, session).await;
                }
                self.progress(session, response).await
            }
            OneDriveUpload::Uploaded { .. } | OneDriveUpload::Refused => {
                self.recover_temporary(publication, session).await
            }
            OneDriveUpload::Copying { .. } => self.await_copy(publication, session).await,
            OneDriveUpload::VerifyPublished => Ok(()),
            OneDriveUpload::Published => self.finish_cleanup(publication, session).await,
        }
    }

    async fn recover_temporary(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        // A recording may precede the final part, copy, or deletion reply.
        // The native upload is closed; resolve whether its temporary survived.
        let url = self.temporary_url(session.path.store(), temporary(session)?)?;
        let response = self
            .publish(publication, Method::GET, &url, Body::Empty)
            .await?;
        if response.status().as_u16() == 404 {
            self.check_folder().await?;
            let object = self.status(&session.path).await?.ok_or(
                StorageFailure::SessionExpired
                    .with_source("OneDrive session and destination are absent"),
            )?;
            return self.verify_existing(publication, session, object).await;
        }
        let value = http::json(PROVIDER, response).await?;
        if http::string(&value, "name")? != temporary(session)?.to_string()
            || !value["file"].is_object()
        {
            return Err(
                StorageFailure::Protocol.with_source("OneDrive returned another upload temporary")
            );
        }
        let size = value["size"]
            .as_u64()
            .ok_or(StorageFailure::Protocol.with_source("OneDrive temporary omitted size"))?;
        if size < session.total {
            // Do not return SessionExpired until cleanup succeeds: callers
            // replace expired recordings, losing their temporary identity.
            self.delete_temporary(publication, session).await?;
            return Err(StorageFailure::SessionExpired
                .with_source("OneDrive expired an unfinished temporary"));
        }
        if size != session.total {
            return Err(StorageFailure::Protocol
                .with_source("OneDrive temporary size disagrees with session"));
        }
        // The saved upload can predate a successful copy reply. An occupied
        // destination must be compared before either identity is forgotten.
        if let Some(object) = self.status(&session.path).await? {
            return self.verify_existing(publication, session, object).await;
        }
        let item = http::string(&value, "id")?.to_owned();
        set_upload(session, OneDriveUpload::Uploaded { item })?;
        session.confirmed = session.total;
        Ok(())
    }

    async fn verify_existing(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
        object: StoredObject,
    ) -> Result<(), StorageError> {
        if object.size != session.total {
            return with_cleanup(
                Err(StorageFailure::AlreadyExists.into()),
                self.delete_temporary(publication, session).await,
            );
        }
        set_upload(session, OneDriveUpload::VerifyPublished)?;
        session.confirmed = 0;
        Ok(())
    }

    pub(super) async fn send_part(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
        bytes: &[u8],
        end: u64,
    ) -> Result<(), StorageError> {
        if matches!(upload(session)?, OneDriveUpload::Planned { .. }) {
            self.resume_temporary(publication, session).await?;
        }
        match upload(session)? {
            OneDriveUpload::VerifyPublished => {
                if let Err(error) = http::compare_published_part(self, session, bytes, end).await {
                    if error.failure() == StorageFailure::AlreadyExists {
                        return with_cleanup(
                            Err(error),
                            self.delete_temporary(publication, session).await,
                        );
                    }
                    return Err(error);
                }
                session.confirmed = end;
                if end == session.total {
                    set_upload(session, OneDriveUpload::Published)?;
                    self.finish_cleanup(publication, session).await?;
                }
                Ok(())
            }
            OneDriveUpload::Receiving { url } => {
                validate_download_url(&self.api, url.as_str())?;
                let response = self
                    .session
                    .publish(
                        publication,
                        Method::PUT,
                        url.as_str(),
                        &[
                            (
                                "Content-Range",
                                format!(
                                    "bytes {}-{}/{}",
                                    session.confirmed,
                                    end - 1,
                                    session.total
                                ),
                            ),
                            ("Content-Type", "application/octet-stream".into()),
                        ],
                        Body::Bytes(bytes.to_vec()),
                        false,
                    )
                    .await?;
                if response.status().as_u16() == 416 {
                    let error = http::response_error(PROVIDER, response).await;
                    // Graph refuses a fragment overlapping bytes its session
                    // already received with 416 and `fragmentOverlap`.
                    if !native_code(&error, "fragmentOverlap") {
                        // An invalid range coven sent stays its error.
                        return Err(error);
                    }
                    // The session already holds bytes of this fragment, as after
                    // a lost reply: take its confirmed progress, never resend over it.
                    let status = self
                        .session
                        .publish(
                            publication,
                            Method::GET,
                            url.as_str(),
                            &[],
                            Body::Empty,
                            false,
                        )
                        .await?;
                    self.progress(session, status).await?;
                } else {
                    self.progress(session, response).await?;
                }
                if session.confirmed < end {
                    return Err(StorageFailure::Protocol
                        .with_source("OneDrive did not store the entire part"));
                }
                Ok(())
            }
            _ => Err(StorageFailure::InvalidPart.into()),
        }
    }

    pub(super) async fn publish_temporary(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        match upload(session)? {
            OneDriveUpload::Published => return self.finish_cleanup(publication, session).await,
            OneDriveUpload::Copying { .. } | OneDriveUpload::Refused => {
                return self.finish_copy(publication, session).await
            }
            OneDriveUpload::Planned { .. }
            | OneDriveUpload::VerifyPublished
            | OneDriveUpload::Receiving { .. } => return Err(StorageFailure::InvalidPart.into()),
            OneDriveUpload::Uploaded { .. } => {}
        }
        let OneDriveUpload::Uploaded { item } = upload(session)? else {
            unreachable!()
        };
        let parent = self.parents(publication, &session.path).await?;
        let url = format!(
            "{}?@microsoft.graph.conflictBehavior=fail",
            self.item(item, &["copy"])?
        );
        let response = self
            .publish(
                publication,
                Method::POST,
                &url,
                Body::Json(json!({
                    "parentReference": {"driveId": self.drive, "id": parent},
                    "name": session.path.file_name()
                })),
            )
            .await?;
        if !response.status().is_success() {
            let error = http::response_error(PROVIDER, response).await;
            if error.retryable() || error.failure() == StorageFailure::Conflict {
                return Err(error);
            }
            return self.copy_refused(publication, session, error).await;
        }
        if response.status().as_u16() != 202 {
            return Err(StorageFailure::Protocol
                .with_source("OneDrive copy omitted asynchronous acceptance"));
        }
        let monitor = response
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .ok_or(StorageFailure::Protocol.with_source("OneDrive copy omitted monitor URL"))?;
        validate_download_url(&self.api, monitor)?;
        set_upload(
            session,
            OneDriveUpload::Copying {
                monitor: SecretText::new(monitor.into()),
            },
        )?;
        self.finish_copy(publication, session).await
    }

    async fn finish_copy(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        match upload(session)? {
            OneDriveUpload::Copying { .. } => self.await_copy(publication, session).await?,
            OneDriveUpload::Refused => self.recover_temporary(publication, session).await?,
            _ => return Err(StorageFailure::SessionMismatch.into()),
        }
        if !session.is_complete() {
            return Err(
                StorageFailure::Network.with_source("OneDrive copy requires session resumption")
            );
        }
        Ok(())
    }

    async fn copy_refused(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
        error: StorageError,
    ) -> Result<(), StorageError> {
        set_upload(session, OneDriveUpload::Refused)?;
        let result = self.checked_folder_operation::<()>(Err(error)).await;
        with_cleanup(result, self.delete_temporary(publication, session).await)
    }

    async fn await_copy(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        let OneDriveUpload::Copying { monitor } = upload(session)? else {
            return Err(StorageFailure::SessionMismatch.into());
        };
        validate_download_url(&self.api, monitor.as_str())?;
        let monitor = monitor.clone();
        let copying = async {
            // Status checks back off like every automatic wait (§6.7).
            let mut checks = 0;
            loop {
                let response = self
                    .session
                    .publish(
                        publication,
                        Method::GET,
                        monitor.as_str(),
                        &[],
                        Body::Empty,
                        false,
                    )
                    .await?;
                if response.status().as_u16() == 404 {
                    // Monitor expiry does not establish publication. Complete
                    // bytes must be supplied again before confirming a name.
                    return self.recover_temporary(publication, session).await;
                }
                let completed = if response.status().as_u16() == 303 {
                    true
                } else {
                    if !response.status().is_success() {
                        let error = http::response_error(PROVIDER, response).await;
                        // A monitor request refusal is not a copy refusal: the
                        // native copy can still be running, so retain its source.
                        return Err(error);
                    }
                    let (value, receipt) = http::json_response(PROVIDER, response).await?;
                    match http::string(&value, "status")? {
                        "completed" => true,
                        "notStarted" | "inProgress" | "waiting" => false,
                        "failed" | "deleteFailed" => {
                            return self
                                .copy_refused(publication, session, receipt.into_error(PROVIDER))
                                .await
                        }
                        _ => {
                            return Err(StorageFailure::Protocol
                                .with_source("OneDrive returned unknown copy status"))
                        }
                    }
                };
                if completed {
                    let value = self.metadata(&session.path).await?;
                    let object = object_metadata::onedrive(&session.path, &value)?;
                    if object.size != session.total {
                        return Err(StorageFailure::Protocol
                            .with_source("OneDrive copied an incomplete object"));
                    }
                    set_upload(session, OneDriveUpload::Published)?;
                    return self.finish_cleanup(publication, session).await;
                }
                self.session.sleep(crate::retry_delay(checks)).await;
                checks += 1;
            }
        };
        tokio::select! {
            result = copying => result,
            () = self.session.sleep(Duration::from_secs(300)) => {
                Err(StorageFailure::Network.with_source("OneDrive copy monitor timed out"))
            }
        }
    }

    async fn finish_cleanup(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        self.delete_temporary(publication, session).await?;
        session.confirmed = session.total;
        session.state = SessionState::Complete;
        Ok(())
    }

    pub(super) async fn abort_temporary(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        let cancellation = match upload(session)? {
            OneDriveUpload::Receiving { url } => {
                validate_download_url(&self.api, url.as_str())?;
                async {
                    let response = self
                        .session
                        .publish(
                            publication,
                            Method::DELETE,
                            url.as_str(),
                            &[],
                            Body::Empty,
                            false,
                        )
                        .await?;
                    if response.status().as_u16() != 404 {
                        http::checked(PROVIDER, response).await?;
                    }
                    Ok(())
                }
                .await
            }
            _ => Ok(()),
        };
        with_cleanup(
            cancellation,
            self.delete_temporary(publication, session).await,
        )
    }
    async fn progress(
        &self,
        session: &mut UploadSession,
        response: reqwest::Response,
    ) -> Result<(), StorageError> {
        if response.status().as_u16() == 404 {
            let mut error = http::response_error(PROVIDER, response).await;
            if let StorageError::Provider {
                failure: failure @ StorageFailure::NotFound,
                ..
            } = &mut error
            {
                *failure = StorageFailure::SessionExpired;
            }
            return Err(error);
        }
        let response = http::checked(PROVIDER, response).await?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .await
            .map_err(|e| http::transport(PROVIDER, e))?;
        let invalid = || http::invalid_response(PROVIDER, status, headers.clone(), body.to_vec());
        let value: Value = serde_json::from_slice(&body).map_err(|_| invalid())?;
        if let Some(ranges) = value.get("nextExpectedRanges") {
            session.confirmed = next_offset(ranges, session.confirmed, session.total, invalid)?;
        } else {
            if value["size"].as_u64() != Some(session.total)
                || http::string(&value, "name")? != temporary(session)?.to_string()
            {
                return Err(StorageFailure::Protocol.with_source("OneDrive completed another file"));
            }
            let item = http::string(&value, "id")?.to_owned();
            set_upload(session, OneDriveUpload::Uploaded { item })?;
            session.confirmed = session.total;
        }
        Ok(())
    }
}
