use crate::{
    ByteProgress, CloudProvider, ObjectPath, ObjectPrefix, ObjectRead, PublicationRequest,
    StorageConfig, StorageError, StorageFailure, StoredObject, UploadSession,
};
use async_trait::async_trait;

/// A nonempty half-open byte range; end must not exceed the object's length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteRange {
    start: u64,
    end: u64,
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;

impl ByteRange {
    /// Validate the bounds before issuing a ranged request.
    pub fn new(start: u64, end: u64) -> Result<Self, StorageError> {
        if start >= end {
            return Err(StorageFailure::InvalidRange.into());
        }
        Ok(Self { start, end })
    }
    /// First byte included.
    pub fn start(self) -> u64 {
        self.start
    }
    /// First byte excluded.
    pub fn end(self) -> u64 {
        self.end
    }
    /// Number of requested bytes.
    pub fn len(self) -> u64 {
        self.end - self.start
    }
    /// A validated range is never empty.
    pub fn is_empty(self) -> bool {
        false
    }
    pub(crate) fn header(self) -> String {
        format!("bytes={}-{}", self.start, self.end - 1)
    }
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn select(self, bytes: &[u8]) -> Result<Vec<u8>, StorageError> {
        let start = usize::try_from(self.start)
            .map_err(|error| StorageFailure::InvalidRange.with_source(error))?;
        let end = usize::try_from(self.end)
            .map_err(|error| StorageFailure::InvalidRange.with_source(error))?;
        bytes
            .get(start..end)
            .map(<[u8]>::to_vec)
            .ok_or(StorageError::Failure(StorageFailure::InvalidRange))
    }
}

/// A part the connection admitted (§17.9): no provider cooldown was live, the
/// session names the connection's location, the part fits the session, and
/// its destination could be published. Admitting a planned session's first
/// part marked its plan. Sending the part consumes the permit.
#[must_use = "a part goes to the provider only through its permit"]
pub struct PartPermit<'a> {
    pub(crate) bytes: &'a [u8],
    /// Where the part starts and ends in the object.
    pub(crate) part: std::ops::Range<u64>,
    pub(crate) publication: PublicationRequest,
    marked: bool,
}

impl<'a> PartPermit<'a> {
    /// Admit `bytes` as the part `part` covers, marking a planned session's
    /// plan.
    pub(crate) fn admit(
        session: &mut UploadSession,
        bytes: &'a [u8],
        part: std::ops::Range<u64>,
        publication: PublicationRequest,
    ) -> Self {
        let marked = session.mark_start();
        Self { bytes, part, publication, marked }
    }
    /// Whether admitting the part marked the session's plan, changing what
    /// the caller records: save the session before sending the part, so a
    /// recording that may name an upload says so (Appendix E.10).
    pub fn marked(&self) -> bool {
        self.marked
    }
}

/// How a removed device's member cuts off its provider access (§14.2, E9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderSignOut {
    /// Remove the app's access, then sign in again on retained devices.
    RemoveAppAccess {
        /// Dropbox or OneDrive.
        provider: CloudProvider,
    },
    /// Remove the device from the Apple account.
    RemoveFromAppleAccount,
}

pub use coven_format::MemberAccess;

/// Granting access succeeds either through the provider or through an admin action.
pub enum AccessGrant {
    /// The provider shared the store with the account.
    Granted {
        /// Provider acceptance information to include in the encrypted invite code.
        invitation: crate::StorageInvitation,
    },
    /// The admin makes a key in the provider console and enters it in the invite.
    CreateAccessKey,
}

/// A grant that revocation left for the owner to inspect.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RetainedAccess {
    /// The native permission, membership or group id, scoped to this store.
    pub provider_id: String,
    /// Why removing this grant cannot revoke only the requested account.
    pub reason: RetainedAccessReason,
}

/// The provider's reason that a grant was retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RetainedAccessReason {
    /// The grant also reaches other accounts, including public or group access.
    OtherAccounts,
    /// The grant comes from a parent location.
    Inherited,
    /// The provider did not identify the recipient sufficiently to remove it safely.
    UnidentifiedAccount,
    /// The grant belongs to the store's owner, whose access cannot be removed.
    StoreOwner,
}

/// The operations coven needs from a provider (§20.1).
///
/// Implementations are capabilities assembled at composition roots. They own no
/// database or key custody. Every mutation is awaited and failures reach the
/// caller; no hidden background work repairs failed operations.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Install a member's replacement S3 key after credential custody commits.
    /// Other provider kinds refuse this operation without changing sign-in state.
    async fn set_s3_credentials(
        &self,
        _credentials: crate::S3Credentials,
    ) -> Result<(), StorageError> {
        Err(StorageFailure::InvalidConfiguration.with_source("provider does not use S3 keys"))
    }
    /// The id of the S3 key this connection signs with now, without its
    /// secret; `None` on a provider that doesn't use S3 keys.
    async fn s3_access_key_id(&self) -> Option<String> {
        None
    }
    /// This provider's nonsecret location settings.
    fn config(&self) -> StorageConfig;
    /// The signed-in sharing account, for the member's admin access entry.
    /// S3 uses its supplied access key id instead of an account lookup.
    async fn account(&self) -> Result<String, StorageError> {
        Err(StorageFailure::InvalidConfiguration.with_source("provider has no sharing account"))
    }
    /// The signed-in account as the provider identifies it: its id and primary
    /// address, which an invite's shared-with account is compared with.
    async fn account_identity(&self) -> Result<crate::ProviderAccount, StorageError> {
        Err(StorageFailure::InvalidConfiguration.with_source("provider has no sharing account"))
    }
    /// Largest complete encrypted body the provider takes in one request, in
    /// bytes. Objects past it, or past [`crate::BUFFERED_REQUEST_LIMIT`], go
    /// in parts through resumable or multipart uploads
    /// ([`crate::goes_in_parts`]). Floor pointer replacements accept
    /// complete bodies up to this limit; OneDrive stages
    /// the body in a revision-bound session before conditional publication.
    /// Callers retaining sessions across crashes record `begin_upload`
    /// sessions.
    /// S3 uses 5 GiB, Dropbox 150 MiB and OneDrive 250,000,000 bytes.
    /// CloudKit uses bounded 8 MiB assets.
    fn single_request_limit(&self) -> u64;
    /// The size of the parts a ranged read moves whole, counted from the
    /// object's start, the last shorter: a range moves every part it
    /// touches, as iCloud's reads do (Appendix E.1). `None` when a range
    /// moves only its own bytes.
    fn read_part_size(&self) -> Option<std::num::NonZeroU64> {
        None
    }
    /// Install replacement OAuth tokens after the owner commits them to key custody.
    /// S3 and CloudKit refuse OAuth tokens; they use different account credentials.
    async fn set_oauth_tokens(&self, _tokens: crate::OAuthTokens) -> Result<(), StorageError> {
        Err(StorageFailure::InvalidConfiguration.with_source("provider does not use OAuth"))
    }
    /// Create a complete encrypted object, refusing an occupied path with
    /// [`StorageFailure::AlreadyExists`]. Occupation alone proves nothing: a
    /// retry compares the occupant's complete bytes itself (§5.1, §19.3).
    /// Bodies [`crate::goes_in_parts`] sends in parts use a session; a writes
    /// slot can contain multiple writes in one object.
    /// A failed transfer aborts the session;
    /// if abort fails too, [`StorageError::Cleanup`] retains both causes.
    /// Returns the object's metadata when the provider's one-request reply
    /// carries it; otherwise exact status supplies it (§21.1).
    async fn create(
        &self,
        path: &ObjectPath,
        bytes: &[u8],
    ) -> Result<Option<StoredObject>, StorageError> {
        self.create_reporting(path, bytes, &ByteProgress::unread()).await
    }
    /// [`Self::create`], telling `sent` how many of the object's bytes the
    /// connection has taken, as it takes them: Dropbox's, OneDrive's and
    /// S3's bodies say as they go, CloudKit's client as the fraction of each
    /// asset sent (§6.7).
    async fn create_reporting(
        &self,
        path: &ObjectPath,
        bytes: &[u8],
        sent: &ByteProgress,
    ) -> Result<Option<StoredObject>, StorageError>;
    /// Replace the floor only if its observed native revision still matches.
    /// A stale observation returns `Conflict` without changing the object.
    /// The new bytes become visible together, or not at all. A body above
    /// [`Self::single_request_limit`] returns [`StorageFailure::SingleRequestTooLarge`].
    async fn replace(&self, previous: &StoredObject, bytes: &[u8]) -> Result<(), StorageError>;
    /// Exact lookup. None means confirmed absence; failed lookups remain errors.
    /// Pending multipart assets must not be reported as complete objects.
    async fn status(&self, path: &ObjectPath) -> Result<Option<StoredObject>, StorageError>;
    /// Read bytes and metadata from the same object revision. A range still
    /// reports the complete encrypted size and whole-object checksum semantics.
    async fn stream_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<crate::ObjectStream<'_>, StorageError> {
        self.stream_object_reporting(path, range, &ByteProgress::unread()).await
    }
    /// [`Self::stream_object`], telling `received` how far through the
    /// object the bytes received reach, as they arrive: as the stream reads
    /// them, and for CloudKit, whose client fetches a record's asset whole,
    /// as the fetch goes (§6.7).
    async fn stream_object_reporting(
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
        self.stream_object(path, range)
            .await?
            .collect(crate::object_stream::BUFFERED_READ_LIMIT)
            .await
    }
    /// Read a bounded whole encrypted object; the limit is the same as `read_object`.
    async fn read(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError> {
        Ok(self.read_object(path, None).await?.bytes)
    }
    /// Fetch only the requested bytes; never silently shorten a range.
    async fn read_range(
        &self,
        path: &ObjectPath,
        range: ByteRange,
    ) -> Result<Vec<u8>, StorageError> {
        Ok(self.read_object(path, Some(range)).await?.bytes)
    }
    /// List every object under the prefix, across all pages.
    /// Missing or malformed metadata fails the call.
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError>;

    /// Delete an object. An already absent object succeeds, making operation retries safe.
    /// Sync chooses the deleting device under §16.6; storage exposes no deletion-rights
    /// query. A provider refusal retains its cause, including a refused owned-object deletion.
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError>;
    /// [`Self::delete`] for a caller that wants the object gone and has no use
    /// for the difference between deleting it and finding nothing: a
    /// `NotFound` failure, such as a provider reporting its folder missing,
    /// is done too. Every other failure reaches the caller.
    async fn delete_if_present(&self, path: &ObjectPath) -> Result<(), StorageError> {
        match self.delete(path).await {
            Err(error) if error.failure() == StorageFailure::NotFound => Ok(()),
            result => result,
        }
    }
    /// Share the store using its owner's account, or tell an S3 admin to create a key.
    /// A signed-in sharing account that does not own the location gets `NotStoreOwner`.
    /// Ownership comes from Dropbox's folder `access_type`,
    /// OneDrive's current-account drive id, or CloudKit's current user record id compared with the zone owner.
    /// S3's console-key instructions need no sharing-account check.
    /// Inviting the account that holds the store gets
    /// [`StorageFailure::InviteeIsStoreOwner`]. Dropbox and OneDrive refuse
    /// before any sharing change; CloudKit learns it from the saved share,
    /// which then lists only the owner.
    ///
    /// Dropbox upgrades viewers in place with `update_folder_member`; a pending
    /// viewer without a native account id gets [`StorageFailure::AccountIdUnavailable`]
    /// and keeps its invitation.
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError>;
    /// Finish provider onboarding under the invited account, then check that the
    /// location can be read. The facade verifies coven's store identity and keys.
    /// Dropbox mounts the shared namespace. OneDrive checks the share's drive/folder
    /// before redeeming it; CloudKit checks share metadata before native acceptance.
    /// Native acceptance material comes from the encrypted [`crate::StorageInvitation`],
    /// not a revocation grant id. Removal during joining returns the provider's
    /// permission failure; an admin can invite again.
    async fn join(
        &self,
        store: coven_foundation::id_source::StoreId,
        invitation: &crate::StorageInvitation,
    ) -> Result<(), StorageError> {
        invitation.check(&self.config())?;
        if !matches!(invitation.acceptance, crate::invitation::InvitationAcceptance::Granted) {
            return Err(
                StorageFailure::InvalidConfiguration.with_source("provider acceptance required")
            );
        }
        self.status(&ObjectPath::floor(store)).await?;
        Ok(())
    }
    /// Undo [`Self::join`]'s acceptance under the invited account, for a join
    /// cancelled before its request was sent, keeping the invite usable
    /// (§13.2, E.2). Dropbox unmounts the shared namespace, which leaves the
    /// account a member, so joining again mounts it again; finding it
    /// unmounted already, or the account no longer a member, is done. S3
    /// accepts nothing. OneDrive and CloudKit undo nothing: Graph documents no
    /// way for the recipient to give up a redeemed link's access, and a
    /// CloudKit participant that deletes the share is removed from it.
    async fn leave(&self, invitation: &crate::StorageInvitation) -> Result<(), StorageError> {
        invitation.check(&self.config())?;
        if !matches!(invitation.acceptance, crate::invitation::InvitationAcceptance::Granted) {
            return Err(
                StorageFailure::InvalidConfiguration.with_source("provider acceptance required")
            );
        }
        Ok(())
    }
    /// Unshare through the owner's account. Coven shares nothing on S3, so
    /// there is nothing to take back.
    /// Ownership is checked as for [`Self::grant_access`]. Only grants exclusive to
    /// this account are removed; owner, inherited, shared or unidentified grants
    /// are returned for owner action. An empty list confirms revocation.
    ///
    /// OneDrive resolves native identities across all permission pages and keeps
    /// email-bearing grants until dependent ID-only removals succeed. Dropbox pages direct and
    /// inherited memberships separately and waits for the native removal job,
    /// preserving its failure cause; group and parent access remain reported.
    async fn revoke_access(&self, account: &str) -> Result<Vec<RetainedAccess>, StorageError>;
    /// Begin a create-once upload, returning the value to record before parts are sent.
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError>;
    /// Begin a fresh recorded session after resumption or publication reports
    /// `SessionExpired`. A conflicted S3 session must be aborted first; failed
    /// cleanup prevents a replacement from starting.
    /// The destination and total stay fixed; the caller records the returned value
    /// and supplies the retained encrypted bytes again, starting at offset zero.
    async fn restart_upload(&self, expired: &UploadSession) -> Result<UploadSession, StorageError> {
        expired.check(&self.config())?;
        if expired.is_complete() {
            return Err(StorageFailure::InvalidPart.into());
        }
        if matches!(expired.state, crate::session::SessionState::S3Restart { .. }) {
            self.abort_upload(expired).await?;
        }
        self.begin_upload(expired.path(), expired.total_bytes()).await
    }
    /// Refresh a recorded session from the provider after interruption.
    /// Persist the returned state even on failure, as with `finish_upload`.
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError>;
    /// Admit `bytes` as the session's next part, the first it has still to
    /// send ([`UploadSession::parts_to_send`]), sending nothing. Refused
    /// while a provider cooldown is live, for a session naming another
    /// location, for a part that doesn't fit the session, and while its
    /// destination can't be published (§17.9). Admitting a planned
    /// session's first part marks its plan, before any request that may
    /// start its upload: when [`PartPermit::marked`] says so, save the
    /// session before sending the part (Appendix E.10).
    fn admit_part<'a>(
        &self,
        session: &mut UploadSession,
        bytes: &'a [u8],
    ) -> Result<PartPermit<'a>, StorageError>;
    /// Send an admitted part, advancing the value only after confirmation,
    /// and telling `sent` how far through the object the part's bytes the
    /// connection has taken reach, as [`Self::create_reporting`] does. After
    /// a lost publication reply this verifies the stored bytes instead; see
    /// [`UploadSession`].
    async fn send_part(
        &self,
        permit: PartPermit<'_>,
        session: &mut UploadSession,
        sent: &ByteProgress,
    ) -> Result<(), StorageError>;
    /// Send part `offset..` of a session whose parts are independent
    /// ([`UploadSession::parts_independent`]), leaving the session as it is:
    /// the provider's confirmation comes back, for the caller to add with
    /// [`UploadSession::confirm`] and then record. Several parts may be in
    /// flight at once and come back in any order (§17.9). Refused, sending
    /// nothing, while a provider cooldown is live, for a session naming
    /// another location or whose parts go in order, for a part that isn't a
    /// whole one the session lacks, and while its destination can't be
    /// published. `sent` hears the part's bytes as [`Self::send_part`]'s do.
    async fn send_independent_part(
        &self,
        session: &UploadSession,
        offset: u64,
        bytes: &[u8],
        sent: &ByteProgress,
    ) -> Result<crate::ConfirmedPart, StorageError>;
    /// Admit and send the next part, for a caller that records the session
    /// only after the part: one that never resumes a planned session from a
    /// recording.
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        let permit = self.admit_part(session, bytes)?;
        self.send_part(permit, session, &ByteProgress::unread()).await
    }
    /// Publish all stored parts at the destination. A repeated completion is safe.
    /// Persist the session even on failure. `SessionExpired` requires a fresh
    /// session with the same bytes; a `Cleanup` error must not bypass failed abort.
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError>;
    /// Explicitly abandon an upload. Dropping a recorded value does not abort it.
    /// Already closed or absent sessions succeed; published objects are preserved.
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError>;
    /// End every unfinished session at slots 1 to `through` of `store`,
    /// which this device has applied, whoever began it (§17.10): create-once
    /// refuses each of them there, so none can ever publish, and a claimant
    /// whose session is ended settles its claim as lost, as a refused one
    /// does. Ending one already ended succeeds. Only S3 under conditional
    /// completion puts a claim's session at its slot's path.
    async fn end_settled_claims(
        &self,
        store: coven_foundation::id_source::StoreId,
        through: u64,
    ) -> Result<(), StorageError>;
    /// End every upload `device` left in `store` that this sign-in can see
    /// (§17.10): abort its unfinished sessions and delete what it staged in
    /// its reserved folder. One already ended succeeds, so devices may do it
    /// at once. A Dropbox session, and a OneDrive native one, is seen by no
    /// one else and expires on its own: on Dropbox this ends nothing, and on
    /// OneDrive only staged temporaries (Appendix E.2).
    async fn end_device_uploads(
        &self,
        store: coven_foundation::id_source::StoreId,
        device: coven_foundation::id_source::DeviceId,
    ) -> Result<(), StorageError>;
}
