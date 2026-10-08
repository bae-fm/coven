//! One connected provider, shared by sync steps, operations and file transfers.

use crate::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Applies the storage contract around native provider calls and records whether
/// this connection has reached storage (E5). Every consumer uses this capability,
/// so a successful file read counts even before a sync succeeds. Reconnecting
/// creates a new observation lifetime.
pub struct StorageConnection<P: ?Sized = dyn ProviderOps> {
    pub(crate) provider: Arc<P>,
    reached: AtomicBool,
}

impl<P: ProviderOps + ?Sized> StorageConnection<P> {
    /// Compose once when connecting, before distributing the connection to owners.
    pub(crate) fn from_provider(provider: Arc<P>) -> Self {
        Self {
            provider,
            reached: AtomicBool::new(false),
        }
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

    fn observed<T>(&self, result: Result<T, StorageError>) -> Result<T, StorageError> {
        if !result
            .as_ref()
            .is_err_and(|error| error.failure() == StorageFailure::Network)
        {
            self.reached.store(true, Ordering::Release);
        }
        result
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
        if path.is_replaceable() {
            return Err(StorageFailure::InvalidPath.into());
        }
        if total == 0 {
            return Err(StorageFailure::InvalidPart.into());
        }
        self.provider.begin_upload(path, total).await
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
        self.provider.upload_part(session, bytes, end).await
    }
    pub(crate) async fn finish_upload_checked(
        &self,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        session.check(&self.config())?;
        if session.is_complete() {
            return Ok(());
        }
        self.provider.finish_upload(session).await
    }
    pub(crate) async fn abort_upload_checked(
        &self,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        session.check(&self.config())?;
        if session.is_complete() {
            return Ok(());
        }
        self.provider.abort_upload(session).await
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
        self.provider.set_oauth_tokens(tokens).await
    }
    async fn account(&self) -> Result<String, StorageError> {
        self.observed(self.provider.account().await)
    }
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        let _guard = self.provider.lock_create().await;
        self.observed(
            crate::transfer::upload_bytes(self, path, bytes, self.provider.create(path, bytes))
                .await,
        )
    }
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        let result = async {
            if !path.is_replaceable() {
                return Err(StorageFailure::InvalidPath.into());
            }
            crate::transfer::check_single_request(bytes.len() as u64, self.single_request_limit())?;
            self.provider.replace(path, bytes).await
        }
        .await;
        self.observed(result)
    }
    async fn read(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError> {
        self.observed(self.provider.read(path).await)
    }
    async fn read_range(
        &self,
        path: &ObjectPath,
        range: ByteRange,
    ) -> Result<Vec<u8>, StorageError> {
        self.observed(self.provider.read_range(path, range).await)
    }
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        self.observed(self.list_checked(prefix).await)
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.observed(self.provider.delete(path).await)
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        let result = self.provider.grant_access(account).await;
        // S3 returns console instructions without making a provider request.
        if self.config().provider() == CloudProvider::S3 {
            result
        } else {
            self.observed(result)
        }
    }
    async fn revoke_access(&self, member: &MemberAccess) -> Result<MemberRemoval, StorageError> {
        let result = self
            .provider
            .revoke_access(member)
            .await
            .map(|response| match response {
                ProviderRevocation::Remaining(shares) if shares.is_empty() => {
                    MemberRemoval::Revoked
                }
                ProviderRevocation::Remaining(shares) => MemberRemoval::AccessRemains { shares },
                ProviderRevocation::Reported(result) => result,
            });
        if self.config().provider() == CloudProvider::S3 {
            result
        } else {
            self.observed(result)
        }
    }
    async fn join(&self, invitation: &StorageInvitation) -> Result<(), StorageError> {
        let result = async {
            invitation.check(&self.config())?;
            self.provider.join(invitation).await?;
            self.list_checked(&ObjectPrefix::all()).await?;
            Ok(())
        }
        .await;
        self.observed(result)
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        self.observed(self.begin_upload_checked(path, total).await)
    }
    async fn restart_upload(&self, expired: &UploadSession) -> Result<UploadSession, StorageError> {
        let result = async {
            expired.check(&self.config())?;
            if expired.is_complete() {
                return Err(StorageFailure::InvalidPart.into());
            }
            self.begin_upload_checked(expired.path(), expired.total_bytes())
                .await
        }
        .await;
        self.observed(result)
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        let result = async {
            session.check(&self.config())?;
            if session.is_complete() {
                return Ok(());
            }
            self.provider.resume_upload(session).await
        }
        .await;
        self.observed(result)
    }
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        self.observed(self.upload_part_checked(session, bytes).await)
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.observed(self.finish_upload_checked(session).await)
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        self.observed(self.abort_upload_checked(session).await)
    }
}

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
