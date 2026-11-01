//! Provider calls beneath the common storage contract.

use crate::*;
use coven_foundation::id_source::{DeviceId, StoreId};

/// Native calls used by [`StorageConnection`]. Consumers use [`Storage`];
/// implementations handle provider protocols after the connection checks paths,
/// request sizes and recorded sessions. Construction receives validated settings.
#[async_trait::async_trait]
pub trait ProviderOps: Send + Sync {
    /// This provider's location.
    fn config(&self) -> StorageConfig;
    /// Largest complete body accepted by `create` or `replace`.
    fn single_request_limit(&self) -> u64;
    /// Create D10's bucket-root marker with the provider's create-once primitive.
    /// Only first setup on a bucket-scoped S3 location calls this operation.
    /// Anything it stages sits in `device`'s reserved folder (D10).
    async fn create_bucket_claim(
        &self,
        _store: coven_foundation::id_source::StoreId,
        _device: DeviceId,
    ) -> Result<(), StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }
    /// Read the complete bucket marker, bounded to D10's UUID byte count.
    /// Only bucket-scoped S3 locations call this operation.
    async fn read_bucket_claim(&self) -> Result<Vec<u8>, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }
    /// Signed-in account; coven shares nothing on S3, so it has none.
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
    /// Create in one request, telling `sent` the body's bytes as the
    /// provider's client hands them over. A temporary it stages sits in
    /// `device`'s reserved folder (D10).
    /// Returns the created object's metadata when the reply carries it.
    async fn create(
        &self,
        publication: &PublicationRequest,
        path: &ObjectPath,
        bytes: &[u8],
        sent: &ByteProgress,
        device: DeviceId,
    ) -> Result<Option<StoredObject>, StorageError>;
    /// Replace the observed object in one atomic conditional publication. A
    /// temporary it stages sits in `device`'s reserved folder (D10).
    async fn replace(
        &self,
        publication: &PublicationRequest,
        previous: &StoredObject,
        bytes: &[u8],
        device: DeviceId,
    ) -> Result<(), StorageError>;
    /// Exact native lookup; only a confirmed missing object returns None.
    async fn status(&self, path: &ObjectPath) -> Result<Option<StoredObject>, StorageError>;
    /// Read complete bytes or a range, retaining metadata for those exact bytes.
    /// The connection hears the stream's reads itself; a client that fetches
    /// bytes before the stream yields them tells `received` how far through
    /// the object they reach as they arrive, counted from the object's start.
    async fn stream_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
        received: &ByteProgress,
    ) -> Result<crate::ObjectStream<'_>, StorageError>;
    /// Collect a bounded response through the streaming implementation. Large
    /// writes and snapshots consume `stream_object` directly. This convenience
    /// reader holds at most 32 MiB, regardless of provider metadata.
    async fn read_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<ObjectRead, StorageError> {
        self.stream_object(path, range, &ByteProgress::unread())
            .await?
            .collect(crate::object_stream::BUFFERED_READ_LIMIT)
            .await
    }
    /// Consume native pages into the connection's checked listing.
    async fn list(&self, listing: &mut ObjectListing) -> Result<(), StorageError>;

    /// Delete, accepting an already absent object.
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError>;
    /// Grant account access, or return S3's console instructions.
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError>;
    /// Accept native sharing material. The connection checks location and reads
    /// the exact floor status afterwards to verify readability.
    async fn join(&self, invitation: &StorageInvitation) -> Result<(), StorageError> {
        if !matches!(invitation.acceptance, crate::invitation::InvitationAcceptance::Granted) {
            return Err(
                StorageFailure::InvalidConfiguration.with_source("provider acceptance required")
            );
        }
        Ok(())
    }
    /// Remove exclusive grants and return grants that still reach the account.
    async fn revoke_access(&self, account: &str) -> Result<Vec<RetainedAccess>, StorageError>;
    /// Begin a nonempty upload with its original publication condition, for
    /// `device`, whose reserved folder holds its temporaries (D10).
    async fn begin_upload(
        &self,
        publication: &PublicationRequest,
        target: &UploadTarget,
        total: u64,
        device: DeviceId,
    ) -> Result<UploadSession, StorageError>;
    /// Recover native progress of a validated, incomplete session.
    async fn resume_upload(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError>;
    /// Send a checked part ending at `end`. The connection validates its length,
    /// boundary and session; implementations enforce the native protocol.
    /// `sent` already counts from the part's start in the object.
    async fn upload_part(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
        bytes: &[u8],
        end: u64,
        sent: &ByteProgress,
    ) -> Result<(), StorageError>;
    /// Publish an incomplete session, checking native progress where required.
    async fn finish_upload(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError>;
    /// Abandon an incomplete session without deleting published bytes.
    async fn abort_upload(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
    ) -> Result<(), StorageError>;
    /// The uploads `device` left in `store` that this sign-in can see: the
    /// sessions and temporaries in its reserved folder (D10), and, where the
    /// provider puts a session at its destination, those under the device's
    /// file and snapshot folders. Every page of each listing is followed.
    async fn device_uploads(
        &self,
        store: StoreId,
        device: DeviceId,
    ) -> Result<Vec<LeftUpload>, StorageError>;
    /// End one upload another device left, as this provider listed it: abort
    /// the session, or delete what it staged. One already ended succeeds,
    /// and a published object is never touched.
    async fn end_left_upload(&self, upload: &LeftUpload) -> Result<(), StorageError>;
}
