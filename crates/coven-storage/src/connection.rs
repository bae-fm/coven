//! One connected provider, shared by sync steps, operations and file transfers.

use crate::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Records whether this connection has reached storage (E5). Every consumer
/// uses this capability, so a successful file read counts even before a sync
/// succeeds. Reconnecting creates a new observation lifetime.
pub struct StorageConnection {
    provider: Arc<dyn Storage>,
    reached: AtomicBool,
}

impl StorageConnection {
    /// Compose once when connecting, before distributing the connection to owners.
    pub fn new(provider: Arc<dyn Storage>) -> Self {
        Self {
            provider,
            reached: AtomicBool::new(false),
        }
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
}

#[async_trait::async_trait]
impl Storage for StorageConnection {
    fn config(&self) -> StorageConfig {
        self.provider.config()
    }
    fn single_request_limit(&self) -> u64 {
        self.provider.single_request_limit()
    }
    fn sign_out(&self) -> ProviderSignOut {
        self.provider.sign_out()
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
        self.observed(self.provider.create(path, bytes).await)
    }
    async fn create_once(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        // Providers can specialize immutable retries (Drive removes duplicate
        // copies from this writer). Preserve that behavior through the connection.
        self.observed(self.provider.create_once(path, bytes).await)
    }
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.observed(self.provider.replace(path, bytes).await)
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
        self.observed(self.provider.list(prefix).await)
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
        let result = self.provider.revoke_access(member).await;
        if self.config().provider() == CloudProvider::S3 {
            result
        } else {
            self.observed(result)
        }
    }
    async fn join(&self, invitation: &StorageInvitation) -> Result<(), StorageError> {
        self.observed(self.provider.join(invitation).await)
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        self.observed(self.provider.begin_upload(path, total).await)
    }
    async fn restart_upload(&self, expired: &UploadSession) -> Result<UploadSession, StorageError> {
        self.observed(self.provider.restart_upload(expired).await)
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.observed(self.provider.resume_upload(session).await)
    }
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        self.observed(self.provider.upload_part(session, bytes).await)
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.observed(self.provider.finish_upload(session).await)
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        self.observed(self.provider.abort_upload(session).await)
    }
}
