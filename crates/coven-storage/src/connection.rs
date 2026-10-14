//! One connected provider, shared by sync steps, operations and file transfers.

use crate::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Applies the storage contract around native provider calls and records whether
/// this connection has reached storage (E5). Every consumer uses this capability,
/// so a successful file read counts even before a sync succeeds, and every
/// consumer waits for the same provider cooldown. Reconnecting creates a new
/// observation lifetime.
pub struct StorageConnection<P: ?Sized = dyn ProviderOps> {
    pub(crate) provider: Arc<P>,
    reached: AtomicBool,
    pub(crate) publications: Arc<dyn PublicationGate>,
    pub(crate) cooldown: Arc<dyn Cooldown>,
}

impl<P: ProviderOps + ?Sized> StorageConnection<P> {
    /// Compose once when connecting, before distributing the connection to owners.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn from_provider(provider: Arc<P>) -> Self {
        Self::with_gate(
            provider,
            Arc::new(crate::test_utils::UnrestrictedPublications),
            Arc::new(NoCooldown),
        )
    }

    pub(crate) fn with_gate(
        provider: Arc<P>,
        publications: Arc<dyn PublicationGate>,
        cooldown: Arc<dyn Cooldown>,
    ) -> Self {
        Self {
            provider,
            publications,
            cooldown,
            reached: AtomicBool::new(false),
        }
    }

    /// Refuse, without sending, while a provider cooldown is live. A refused
    /// request has not reached storage.
    fn cooling(&self) -> Result<(), StorageError> {
        match self.cooldown.remaining() {
            Some(remaining) => {
                Err(StorageFailure::RateLimited.with_source(CoolingDown { remaining }))
            }
            None => Ok(()),
        }
    }

    fn publication(&self, path: &ObjectPath) -> Result<PublicationRequest, StorageError> {
        let request = PublicationRequest::new(self.publications.clone(), path.clone());
        request.check()?;
        Ok(request)
    }

    /// Begin the installed connection's observation after setup or bootstrap checks.
    pub fn reset_reachability(&self) {
        self.reached.store(false, Ordering::Release);
    }

    /// A provider operation has succeeded or returned a response other than a
    /// network failure. Installing credentials alone does not establish contact.
    pub fn reached(&self) -> bool {
        self.reached.load(Ordering::Acquire)
    }

    /// Record whether storage answered, and a requested cooldown before any
    /// consumer sees the response that asked for it (SP waiting).
    async fn observed<T>(&self, result: Result<T, StorageError>) -> Result<T, StorageError> {
        if !result.as_ref().is_err_and(|error| {
            matches!(
                error.failure(),
                StorageFailure::Network | StorageFailure::CatchUpRequired
            )
        }) {
            self.reached.store(true, Ordering::Release);
        }
        let error = match result {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        let Some(after) = error.retry_after() else {
            return Err(error);
        };
        if after == RetryAfter::NoTimeBasis {
            tracing::warn!(
                provider = ?self.config().provider(),
                "Retry-After names a date without a response Date; waiting the longest ordinary delay"
            );
        }
        let wait = after.wait();
        match self.cooldown.record(wait).await {
            Ok(()) => Err(error),
            Err(cause) => Err(StorageFailure::File.with_source(CooldownNotRecorded {
                wait,
                response: error,
                cause,
            })),
        }
    }
    async fn list_checked(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        let mut listing = ObjectListing::new(prefix.clone(), self.config().provider());
        self.provider.list(&mut listing).await?;
        Ok(listing.finish())
    }

    pub(crate) async fn begin_upload_checked(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        if path.is_replaceable() || path.store_log_position().is_some() {
            return Err(StorageFailure::InvalidPath.into());
        }
        if total == 0 {
            return Err(StorageFailure::InvalidPart.into());
        }
        self.provider
            .begin_upload(&self.publication(path)?, path, total)
            .await
    }
    pub(crate) async fn upload_part_checked(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        let config = self.config();
        // Preserve native refusal order for a wrong location and an invalid part.
        let end = match config.provider() {
            CloudProvider::S3 | CloudProvider::GoogleDrive | CloudProvider::CloudKit => {
                let end = session.end_of_part(bytes.len())?;
                session.check(&config)?;
                end
            }
            CloudProvider::Dropbox | CloudProvider::OneDrive => {
                session.check(&config)?;
                session.end_of_part(bytes.len())?
            }
        };
        self.provider
            .upload_part(&self.publication(session.path())?, session, bytes, end)
            .await
    }
    pub(crate) async fn finish_upload_checked(
        &self,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        session.check(&self.config())?;
        if session.is_complete() {
            return Ok(());
        }
        self.provider
            .finish_upload(&self.publication(session.path())?, session)
            .await
    }
    pub(crate) async fn abort_upload_checked(
        &self,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        session.check(&self.config())?;
        if session.is_complete() {
            return Ok(());
        }
        self.provider
            .abort_upload(&self.publication(session.path())?, session)
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
        self.observed(self.provider.account().await).await
    }
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.cooling()?;
        let _guard = self.provider.lock_create().await;
        let publication = self.publication(path)?;
        self.observed(
            crate::transfer::upload_bytes(
                self,
                path,
                bytes,
                self.provider.create(&publication, path, bytes),
            )
            .await,
        )
        .await
    }
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.cooling()?;
        let result = async {
            if !path.is_replaceable() {
                return Err(StorageFailure::InvalidPath.into());
            }
            crate::transfer::check_single_request(bytes.len() as u64, self.single_request_limit())?;
            self.provider
                .replace(&self.publication(path)?, path, bytes)
                .await
        }
        .await;
        self.observed(result).await
    }
    async fn status(&self, path: &ObjectPath) -> Result<Option<StoredObject>, StorageError> {
        self.cooling()?;
        self.observed(
            async {
                let object = self.provider.status(path).await?;
                if let Some(object) = &object {
                    object.check(path)?;
                }
                Ok(object)
            }
            .await,
        )
        .await
    }
    async fn stream_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<ObjectStream<'_>, StorageError> {
        self.cooling()?;
        self.observed(
            async {
                let stream = self.provider.stream_object(path, range).await?;
                stream.check(path, range)?;
                Ok(stream)
            }
            .await,
        )
        .await
    }
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        self.cooling()?;
        self.observed(self.list_checked(prefix).await).await
    }
    async fn list_writers(
        &self,
        store: coven_foundation::id_source::StoreId,
    ) -> Result<Vec<coven_foundation::id_source::DeviceId>, StorageError> {
        self.cooling()?;
        self.observed(self.provider.list_writers(store).await).await
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.cooling()?;
        self.observed(self.provider.delete(path).await).await
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        if self.config().provider() != CloudProvider::S3 {
            self.cooling()?;
        }
        let result = self.provider.grant_access(account).await;
        // S3 returns console instructions without making a provider request.
        if self.config().provider() == CloudProvider::S3 {
            result
        } else {
            self.observed(result).await
        }
    }
    async fn revoke_access(&self, account: &str) -> Result<Vec<RetainedAccess>, StorageError> {
        if self.config().provider() != CloudProvider::S3 {
            self.cooling()?;
        }
        let result = self.provider.revoke_access(account).await;
        if self.config().provider() == CloudProvider::S3 {
            result
        } else {
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
            self.provider.join(invitation).await?;
            self.list_writers(store).await?;
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
        self.observed(self.begin_upload_checked(path, total).await)
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
            self.begin_upload_checked(expired.path(), expired.total_bytes())
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
            self.provider
                .resume_upload(&self.publication(session.path())?, session)
                .await
        }
        .await;
        self.observed(result).await
    }
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        self.cooling()?;
        self.observed(self.upload_part_checked(session, bytes).await)
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
