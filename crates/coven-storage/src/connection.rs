//! One connected provider, shared by sync steps, operations and file transfers.

use crate::*;
use std::sync::Arc;

/// Applies the storage contract around native provider calls (E5). Every
/// consumer uses this capability and waits for the same provider cooldown.
pub struct StorageConnection<P: ?Sized = dyn ProviderOps> {
    pub(crate) provider: Arc<P>,
    pub(crate) publications: Arc<dyn PublicationGate>,
    pub(crate) cooldown: Arc<dyn Cooldown>,
    pub(crate) transfers: Arc<TransferBudget>,
    /// Every request this connection sent, timed by the store's clock.
    requests: crate::request_tally::RequestTally,
}

impl<P: ProviderOps + ?Sized> StorageConnection<P> {
    /// Compose once when connecting, before distributing the connection to owners.
    #[cfg(test)]
    pub(crate) fn from_provider(provider: Arc<P>) -> Self {
        Self::with_gate(
            provider,
            Arc::new(crate::test_utils::UnrestrictedPublications),
            Arc::new(NoCooldown),
            Arc::new(TransferBudget::new(TransferLimits::default())),
            Arc::new(coven_foundation::clock::FixedClock::new(
                std::time::UNIX_EPOCH,
            )),
        )
    }

    pub(crate) fn with_gate(
        provider: Arc<P>,
        publications: Arc<dyn PublicationGate>,
        cooldown: Arc<dyn Cooldown>,
        transfers: Arc<TransferBudget>,
        clock: coven_foundation::clock::ClockRef,
    ) -> Self {
        Self {
            provider,
            publications,
            cooldown,
            transfers,
            requests: crate::request_tally::RequestTally::new(clock),
        }
    }

    /// Every request this connection has sent so far, by kind.
    pub fn requests(&self) -> crate::RequestCounts {
        self.requests.counts()
    }

    /// Count a read's bytes that arrived with its response.
    pub(crate) fn received(&self, bytes: u64) {
        self.requests.received(bytes);
    }

    /// Send one native request of `kind`, counting it.
    pub(crate) async fn send<T>(
        &self,
        kind: crate::RequestKind,
        bytes: u64,
        request: impl std::future::Future<Output = T>,
    ) -> T {
        self.requests.send(kind, bytes, request).await
    }

    /// Refuse, without sending, while a provider cooldown is live. A refused
    /// request has not reached storage.
    pub(crate) fn cooling(&self) -> Result<(), StorageError> {
        crate::cooldown::cooling(self.cooldown.as_ref())
    }

    fn publication(&self, path: &ObjectPath) -> Result<PublicationRequest, StorageError> {
        let request = PublicationRequest::new(self.publications.clone(), path.clone());
        request.check()?;
        Ok(request)
    }

    /// Record a requested cooldown before any consumer sees the response
    /// that asked for it (§6.7).
    pub(crate) async fn observed<T>(
        &self,
        result: Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        crate::cooldown::observed(self.cooldown.as_ref(), result).await
    }
    async fn list_checked(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        let mut listing = ObjectListing::new(prefix.clone(), self.config().provider());
        self.send(RequestKind::List, 0, self.provider.list(&mut listing))
            .await?;
        Ok(listing.finish())
    }

    pub(crate) async fn replace_object(
        &self,
        previous: &StoredObject,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        self.cooling()?;
        previous.check(&previous.path)?;
        crate::transfer::check_single_request(bytes.len() as u64, self.single_request_limit())?;
        let publication = self.publication(&previous.path)?;
        let result = self
            .send(
                RequestKind::Replace,
                bytes.len() as u64,
                self.provider.replace(&publication, previous, bytes),
            )
            .await;
        self.observed(result.map_err(StorageError::replacement))
            .await
    }

    pub(crate) async fn begin_upload_checked(
        &self,
        target: &UploadTarget,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        target.check()?;
        if total == 0 {
            return Err(StorageFailure::InvalidPart.into());
        }
        let publication = self.publication(target.path())?;
        self.send(
            RequestKind::Upload,
            0,
            self.provider.begin_upload(&publication, target, total),
        )
        .await
        .map_err(|error| target.error(error))
    }
    pub(crate) async fn upload_part_checked(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
        sent: &ByteProgress,
    ) -> Result<(), StorageError> {
        let config = self.config();
        // Preserve native refusal order for a wrong location and an invalid part.
        let end = match config.provider() {
            CloudProvider::S3 | CloudProvider::CloudKit => {
                let end = session.end_of_part(bytes.len())?;
                session.check(&config)?;
                end
            }
            CloudProvider::Dropbox | CloudProvider::OneDrive => {
                session.check(&config)?;
                session.end_of_part(bytes.len())?
            }
        };
        // The part's bytes are heard from where it starts in the object.
        let sent = sent.starting_at(session.confirmed_bytes());
        let publication = self.publication(session.path())?;
        let result = self
            .send(
                RequestKind::Upload,
                bytes.len() as u64,
                self.provider
                    .upload_part(&publication, session, bytes, end, &sent),
            )
            .await;
        result.map_err(|error| session.target.error(error))
    }
    pub(crate) async fn finish_upload_checked(
        &self,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        session.check(&self.config())?;
        if session.is_complete() {
            return Ok(());
        }
        let publication = self.publication(session.path())?;
        let result = self
            .send(
                RequestKind::Upload,
                0,
                self.provider.finish_upload(&publication, session),
            )
            .await;
        result.map_err(|error| session.target.error(error))
    }
    pub(crate) async fn abort_upload_checked(
        &self,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        session.check(&self.config())?;
        if session.is_complete() {
            return Ok(());
        }
        let publication = self.publication(session.path())?;
        self.send(
            RequestKind::Upload,
            0,
            self.provider.abort_upload(&publication, session),
        )
        .await
    }
}

#[async_trait::async_trait]
impl<P: ProviderOps + ?Sized> Storage for StorageConnection<P> {
    fn config(&self) -> StorageConfig {
        self.provider.config()
    }
    fn single_request_limit(&self) -> u64 {
        self.provider.single_request_limit()
    }
    async fn set_s3_credentials(&self, credentials: S3Credentials) -> Result<(), StorageError> {
        self.provider.set_s3_credentials(credentials).await
    }
    async fn set_oauth_tokens(&self, tokens: OAuthTokens) -> Result<(), StorageError> {
        ConnectionCredentials {
            location: self.config(),
            credentials: StorageCredentials::OAuth(tokens.clone()),
        }
        .validate()?;
        self.provider.set_oauth_tokens(tokens).await
    }
    async fn account(&self) -> Result<String, StorageError> {
        self.cooling()?;
        self.observed(
            self.send(RequestKind::Account, 0, self.provider.account())
                .await,
        )
        .await
    }
    async fn create_reporting(
        &self,
        path: &ObjectPath,
        bytes: &[u8],
        sent: &ByteProgress,
    ) -> Result<Option<StoredObject>, StorageError> {
        self.cooling()?;
        let publication = self.publication(path)?;
        let created = crate::transfer::upload_bytes(self, path, bytes, sent, async {
            let object = self
                .send(
                    RequestKind::Create,
                    bytes.len() as u64,
                    self.provider.create(&publication, path, bytes, sent),
                )
                .await?;
            if let Some(object) = &object {
                object.check(path)?;
            }
            Ok(object)
        })
        .await;
        self.observed(created).await
    }
    async fn replace(&self, previous: &StoredObject, bytes: &[u8]) -> Result<(), StorageError> {
        if !previous.path.is_replaceable() {
            return Err(StorageFailure::InvalidPath.into());
        }
        self.replace_object(previous, bytes).await
    }
    async fn status(&self, path: &ObjectPath) -> Result<Option<StoredObject>, StorageError> {
        self.cooling()?;
        self.observed(
            async {
                let object = self
                    .send(RequestKind::Status, 0, self.provider.status(path))
                    .await?;
                if let Some(object) = &object {
                    object.check(path)?;
                }
                Ok(object)
            }
            .await,
        )
        .await
    }
    async fn stream_object_reporting(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
        received: &ByteProgress,
    ) -> Result<ObjectStream<'_>, StorageError> {
        self.cooling()?;
        // The download limit counts file downloads. The sequence's own reads,
        // its slots, snapshots and floor, are bounded by their one reader
        // instead (§21.1), so they never wait behind a pin's files.
        let download = match path.file_id() {
            Some(_) => Some(self.transfers.download().await),
            None => None,
        };
        self.cooling()?;
        // A failed response records its cooldown before another download
        // can acquire the capacity it releases.
        let stream = self
            .observed(
                self.send(
                    RequestKind::Read,
                    0,
                    self.provider.stream_object(path, range, received),
                )
                .await,
            )
            .await?;
        stream.check(path, range)?;
        let start = range.map_or(0, ByteRange::start);
        let stream = match download {
            Some(lease) => stream.with_download(lease),
            None => stream,
        };
        Ok(stream
            .heard_by(received.starting_at(start))
            .counted(&self.requests))
    }
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        self.cooling()?;
        self.observed(self.list_checked(prefix).await).await
    }

    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.cooling()?;
        self.observed(
            self.send(RequestKind::Delete, 0, self.provider.delete(path))
                .await,
        )
        .await
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        if self.config().provider() != CloudProvider::S3 {
            self.cooling()?;
        }
        // S3 returns console instructions without making a provider request.
        if self.config().provider() == CloudProvider::S3 {
            self.provider.grant_access(account).await
        } else {
            let result = self
                .send(RequestKind::Account, 0, self.provider.grant_access(account))
                .await;
            self.observed(result).await
        }
    }
    async fn revoke_access(&self, account: &str) -> Result<Vec<RetainedAccess>, StorageError> {
        if self.config().provider() != CloudProvider::S3 {
            self.cooling()?;
        }
        if self.config().provider() == CloudProvider::S3 {
            self.provider.revoke_access(account).await
        } else {
            let result = self
                .send(
                    RequestKind::Account,
                    0,
                    self.provider.revoke_access(account),
                )
                .await;
            self.observed(result).await
        }
    }
    async fn join(
        &self,
        store: coven_foundation::id_source::StoreId,
        invitation: &StorageInvitation,
    ) -> Result<(), StorageError> {
        self.cooling()?;
        let result = async {
            invitation.check(&self.config())?;
            self.send(RequestKind::Account, 0, self.provider.join(invitation))
                .await?;
            self.check_bucket(store).await?;
            self.status(&ObjectPath::floor(store)).await?;
            Ok(())
        }
        .await;
        self.observed(result).await
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        self.cooling()?;
        self.observed(
            self.begin_upload_checked(&UploadTarget::Create(path.clone()), total)
                .await,
        )
        .await
    }
    async fn begin_replace(
        &self,
        previous: &StoredObject,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        self.cooling()?;
        self.observed(
            self.begin_upload_checked(&UploadTarget::Replace(previous.clone()), total)
                .await,
        )
        .await
    }
    async fn restart_upload(&self, expired: &UploadSession) -> Result<UploadSession, StorageError> {
        self.cooling()?;
        let result = async {
            expired.check(&self.config())?;
            if expired.is_complete() {
                return Err(StorageFailure::InvalidPart.into());
            }
            if matches!(
                expired.state,
                crate::session::SessionState::S3Restart { .. }
            ) {
                self.abort_upload_checked(expired).await?;
            }
            self.begin_upload_checked(expired.target(), expired.total_bytes())
                .await
        }
        .await;
        self.observed(result).await
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.cooling()?;
        let result = async {
            session.check(&self.config())?;
            if session.is_complete() {
                return Ok(());
            }
            let publication = self.publication(session.path())?;
            let result = self
                .send(
                    RequestKind::Upload,
                    0,
                    self.provider.resume_upload(&publication, session),
                )
                .await;
            result.map_err(|error| session.target.error(error))
        }
        .await;
        self.observed(result).await
    }
    async fn upload_part_reporting(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
        sent: &ByteProgress,
    ) -> Result<(), StorageError> {
        self.cooling()?;
        self.observed(self.upload_part_checked(session, bytes, sent).await)
            .await
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.cooling()?;
        self.observed(self.finish_upload_checked(session).await)
            .await
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        self.cooling()?;
        self.observed(self.abort_upload_checked(session).await)
            .await
    }
}

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
