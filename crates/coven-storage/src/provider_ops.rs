//! Provider calls beneath the common storage contract.

use crate::*;

/// Native revocation responses before the connection interprets remaining access.
pub enum ProviderRevocation {
    /// HTTP account providers return the grants still reaching the account.
    /// An empty list means all access was removed.
    Remaining(Vec<RetainedAccess>),
    /// S3 supplies console instructions; the app's CloudKit bridge already
    /// supplies the full result. Preserve those existing boundary types.
    Reported(MemberRemoval),
}

/// Native calls used by [`StorageConnection`]. Consumers use [`Storage`];
/// implementations handle provider protocols after the connection checks paths,
/// request sizes and recorded sessions. Construction receives validated settings.
#[async_trait::async_trait]
pub trait ProviderOps: Send + Sync {
    /// This provider's location.
    fn config(&self) -> StorageConfig;
    /// Largest complete body accepted by `create` or `replace`.
    fn single_request_limit(&self) -> u64;
    /// Hold native create serialization across both complete and resumable uploads.
    async fn lock_create(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        None
    }
    /// Signed-in account; S3 has no sharing account.
    async fn account(&self) -> Result<String, StorageError> {
        Err(StorageFailure::InvalidConfiguration.with_source("provider has no sharing account"))
    }
    /// Replace tokens after credential custody commits them.
    async fn set_oauth_tokens(&self, _tokens: OAuthTokens) -> Result<(), StorageError> {
        Err(StorageFailure::InvalidConfiguration.with_source("provider does not use OAuth"))
    }
    /// Replace the S3 signing key after credential custody commits it.
    async fn set_s3_credentials(&self, _credentials: S3Credentials) -> Result<(), StorageError> {
        Err(StorageFailure::InvalidConfiguration.with_source("provider does not use S3 keys"))
    }
    /// Create in one request, including native duplicate cleanup on retries.
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError>;
    /// Replace a checked positions path in one request.
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError>;
    /// Read the complete object.
    async fn read(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError>;
    /// Read exactly the requested range.
    async fn read_range(
        &self,
        path: &ObjectPath,
        range: ByteRange,
    ) -> Result<Vec<u8>, StorageError>;
    /// Consume native pages into the connection's checked listing. Drive selects
    /// its earliest copy before adding each object.
    async fn list(&self, listing: &mut ObjectListing) -> Result<(), StorageError>;
    /// Delete, accepting an already absent object.
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError>;
    /// Grant account access, or return S3's console instructions.
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError>;
    /// Accept native sharing material. The connection checks location and lists
    /// the store afterwards to verify readability.
    async fn join(&self, invitation: &StorageInvitation) -> Result<(), StorageError> {
        if !matches!(
            invitation.acceptance,
            crate::invitation::InvitationAcceptance::Granted
        ) {
            return Err(
                StorageFailure::InvalidConfiguration.with_source("provider acceptance required")
            );
        }
        Ok(())
    }
    /// Remove exclusive grants and return grants that still reach the account.
    async fn revoke_access(
        &self,
        member: &MemberAccess,
    ) -> Result<ProviderRevocation, StorageError>;
    /// Begin a nonempty upload to a checked immutable destination.
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError>;
    /// Recover native progress of a validated, incomplete session.
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError>;
    /// Send a checked part ending at `end`. The connection validates its length,
    /// boundary and session; implementations enforce the native protocol.
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
        end: u64,
    ) -> Result<(), StorageError>;
    /// Publish an incomplete session, checking native progress where required.
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError>;
    /// Abandon an incomplete session without deleting published bytes.
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError>;
}
