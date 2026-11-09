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
    /// The size of the parts a ranged read moves whole
    /// ([`crate::Storage::read_part_size`]); `None` when a range moves only
    /// its own bytes.
    fn read_part_size(&self) -> Option<std::num::NonZeroU64> {
        None
    }
    /// Create D10's location marker at the location's root with the
    /// provider's create-once request. Only first setup at a location that
    /// holds one store calls this operation. Anything it stages sits in
    /// `device`'s reserved folder (D10).
    async fn create_location_marker(
        &self,
        _store: coven_foundation::id_source::StoreId,
        _device: DeviceId,
    ) -> Result<(), StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }
    /// Read the complete location marker, bounded to D10's UUID byte count.
    /// Only a location that holds one store calls this operation. A marker
    /// read that finds nothing confirms the location is still there first.
    async fn read_location_marker(&self) -> Result<Vec<u8>, StorageError> {
        Err(StorageFailure::ProviderUnavailable.into())
    }
    /// The store's folder's name and web address as this connection's
    /// account names them now (§20.2). Only a Dropbox shared folder or a
    /// OneDrive folder has them; nothing asks another location.
    async fn folder_label(&self) -> Result<FolderLabel, StorageError> {
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
    /// Undo [`Self::join`]'s acceptance so the invite stays usable
    /// ([`crate::Storage::leave`]). Access an invite grants without native
    /// acceptance has nothing to undo.
    async fn leave(&self, invitation: &StorageInvitation) -> Result<(), StorageError> {
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
    /// Send part `offset..` of a session whose parts are independent
    /// ([`UploadSession::parts_independent`]), leaving the session as it
    /// is: the provider's confirmation of the part comes back, for the
    /// caller to add. Several may be in flight at once. The connection
    /// checks the part is a whole one the session lacks; `sent` already
    /// counts from the part's start in the object. A provider whose parts
    /// go in order has none to send this way.
    async fn send_independent_part(
        &self,
        _publication: &PublicationRequest,
        _session: &UploadSession,
        _offset: u64,
        _bytes: &[u8],
        _sent: &ByteProgress,
    ) -> Result<ConfirmedPart, StorageError> {
        Err(StorageFailure::InvalidPart.into())
    }
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
    /// The unfinished sessions at slots 1 to `through` of `store`, whoever
    /// began them: where a claim's session sits at its slot's path, one at
    /// a slot this device has applied can never publish, since create-once
    /// refuses it there. A provider that puts no session at a slot's path
    /// returns none.
    async fn settled_claim_uploads(
        &self,
        store: StoreId,
        through: u64,
    ) -> Result<Vec<LeftUpload>, StorageError>;
    /// Whether this provider dates an unfinished floor session's start so
    /// that one begun before the current floor's publication can be told
    /// apart (Appendix E.10). Asking sends nothing.
    fn dates_floor_sessions(&self) -> bool {
        false
    }
    /// The unfinished sessions at the floor's path begun before `floor`, the
    /// current floor, was published: each observed an older revision, so
    /// none can publish. Asked only where [`Self::dates_floor_sessions`].
    async fn superseded_floor_uploads(
        &self,
        _floor: &StoredObject,
    ) -> Result<Vec<LeftUpload>, StorageError> {
        Ok(Vec::new())
    }
    /// What S3 detection run `run` left in `store`'s `.coven-check` prefix
    /// (§20.7): its unfinished multipart uploads, then its probes. Only S3
    /// detects, so elsewhere no run left anything.
    async fn detection_leftovers(
        &self,
        _store: StoreId,
        _run: uuid::Uuid,
    ) -> Result<Vec<LeftUpload>, StorageError> {
        Ok(Vec::new())
    }
    /// End one upload another device left, as this provider listed it: abort
    /// the session, or delete what it staged. One already ended succeeds,
    /// and a published object is never touched.
    async fn end_left_upload(&self, upload: &LeftUpload) -> Result<(), StorageError>;
}
