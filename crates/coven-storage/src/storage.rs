use crate::{
    CloudProvider, ObjectPath, ObjectPrefix, StorageConfig, StorageError, StorageSetupError,
    UploadSession,
};
use async_trait::async_trait;

/// A nonempty half-open byte range; end must not exceed the object's length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteRange {
    start: u64,
    end: u64,
}
impl ByteRange {
    /// Validate the bounds before issuing a ranged request.
    pub fn new(start: u64, end: u64) -> Result<Self, StorageError> {
        if start >= end {
            return Err(StorageError::InvalidRange);
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
        let start = usize::try_from(self.start).map_err(|_| StorageError::InvalidRange)?;
        let end = usize::try_from(self.end).map_err(|_| StorageError::InvalidRange)?;
        bytes
            .get(start..end)
            .map(<[u8]>::to_vec)
            .ok_or(StorageError::InvalidRange)
    }
}

/// How a removed device's member cuts off its provider access (§13, E9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderSignOut {
    /// Remove the app's access, then sign in again on retained devices.
    RemoveAppAccess {
        /// Google Drive, Dropbox or OneDrive.
        provider: CloudProvider,
    },
    /// Remove the device from the Apple account.
    RemoveFromAppleAccount,
    /// Make and enter a new S3 key, distribute and write down the new restore
    /// code, then delete the old key in the provider console.
    ReplaceAccessKey,
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

/// Revocation includes S3's manual instruction; it is not an error (E9).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MemberRemoval {
    /// The removal is kept; an owner's device must apply it to revoke sharing.
    /// Other admins never attempt provider sharing calls.
    PendingOwner,
    /// Sharing must remain because an active member or another open invite uses
    /// the same account. No provider grant is revoked in this case.
    AccountInUse {
        /// The shared account whose grant remains necessary.
        account: String,
    },

    /// The provider no longer shares with the account.
    Revoked,
    /// Exclusive grants were removed; these grants remain for owner action.
    /// They cannot safely be removed for this account alone.
    AccessRemains {
        /// Native grants the owner can inspect in the provider's console.
        shares: Vec<RetainedAccess>,
    },
    /// The admin deletes this member's key in the provider console.
    DeleteAccessKey {
        /// Public identifier of the key to delete in the provider console.
        access_key_id: String,
    },
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

/// One complete object listed by its provider.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredObject {
    /// Validated path relative to the store's location.
    pub path: ObjectPath,
    /// Complete encrypted length in bytes.
    pub size: u64,
    /// Provider timestamp, never the uploading device's clock: Drive's createdTime,
    /// OneDrive's createdDateTime, Dropbox's server_modified, CloudKit's server
    /// publication time, or S3's Last-Modified. S3 reports multipart initiation
    /// time, including for device logs uploaded through recorded sessions; this
    /// timestamp therefore does not establish their publication age.
    pub stored_at: std::time::SystemTime,
}

/// The operations coven needs from a provider (§4).
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
        Err(StorageError::InvalidConfiguration(
            "provider does not use S3 keys",
        ))
    }
    /// This provider's nonsecret location settings.
    fn config(&self) -> StorageConfig;
    /// The signed-in sharing account, for the member's store-log access entry.
    /// S3 uses its supplied access key id instead of an account lookup.
    async fn account(&self) -> Result<String, StorageError> {
        Err(StorageError::InvalidConfiguration(
            "provider has no sharing account",
        ))
    }
    /// Largest complete encrypted body sent in one request, in bytes. Larger
    /// create-once objects use resumable or multipart uploads, for every path kind.
    /// Callers retaining sessions across crashes use `begin_upload` and record them.
    /// S3 uses 5 GiB, Drive 5,000,000 bytes, Dropbox 150 MiB and OneDrive
    /// 250,000,000 bytes. CloudKit uses the app bridge's nonzero native-call limit.
    fn single_request_limit(&self) -> u64;
    /// Install replacement OAuth tokens after the owner commits them to key custody.
    /// S3 and CloudKit refuse OAuth tokens; they use different account credentials.
    async fn set_oauth_tokens(&self, _tokens: crate::OAuthTokens) -> Result<(), StorageError> {
        Err(StorageError::InvalidConfiguration(
            "provider does not use OAuth",
        ))
    }
    /// Create a complete encrypted object, refusing an occupied path.
    /// Bodies above [`Self::single_request_limit`] use a session for any immutable
    /// path, including writes and snapshots. A failed transfer aborts the session;
    /// if abort fails too, [`StorageError::Cleanup`] retains both causes.
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError>;
    /// Replace a device's posted positions; immutable paths are refused.
    /// Always one request; an oversized body returns [`StorageError::SingleRequestTooLarge`].
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError>;
    /// Read the whole encrypted object.
    async fn read(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError>;
    /// Fetch only the requested bytes; never silently shorten a range.
    async fn read_range(
        &self,
        path: &ObjectPath,
        range: ByteRange,
    ) -> Result<Vec<u8>, StorageError>;
    /// List every object under the prefix, across all pages.
    /// Missing or malformed metadata fails the call. Drive returns the earliest
    /// stored copy of each path, breaking creation-time ties by native id.
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError>;
    /// Delete an object; on Drive, remove it from the folder when this account is not its owner.
    /// An already absent object succeeds, making operation retries safe.
    /// Sync chooses the deleting device under §15; storage exposes no deletion-rights
    /// query. A provider refusal retains its cause, including a refused owned-object deletion.
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError>;
    /// Share the store using its owner's account, or tell an S3 admin to create a key.
    /// A signed-in sharing account that does not own the location gets `NotStoreOwner`.
    /// Ownership comes from Drive's folder `ownedByMe`, Dropbox's folder `access_type`,
    /// OneDrive's current-account drive id, or CloudKit's native `is_owner` call.
    /// S3's console-key instructions need no sharing-account check.
    ///
    /// Dropbox upgrades viewers in place with `update_folder_member`; a pending
    /// viewer without a native account id gets [`StorageError::AccountIdUnavailable`]
    /// and keeps its invitation. Drive updates direct readers in place and adds a
    /// direct writer grant when read access is inherited.
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError>;
    /// Finish provider onboarding under the invited account, then check that the
    /// location can be read. The facade verifies coven's store identity and keys.
    /// Dropbox mounts the shared namespace. OneDrive checks the share's drive/folder
    /// before redeeming it; CloudKit checks share metadata before native acceptance.
    /// Native acceptance material comes from the encrypted [`crate::StorageInvitation`],
    /// not a revocation grant id. Removal during joining returns the provider's
    /// permission failure; an admin can invite again.
    async fn join(&self, invitation: &crate::StorageInvitation) -> Result<(), StorageError> {
        invitation.check(&self.config())?;
        if !matches!(
            invitation.acceptance,
            crate::invitation::InvitationAcceptance::Granted
        ) {
            return Err(StorageError::InvalidConfiguration(
                "provider acceptance required",
            ));
        }
        self.list(&ObjectPrefix::all()).await?;
        Ok(())
    }
    /// Unshare through the owner's account, or tell an S3 admin which key to delete.
    /// Ownership is checked as for [`Self::grant_access`]. Only grants exclusive to
    /// this account are removed; owner, inherited, shared or unidentified grants
    /// remain in [`MemberRemoval::AccessRemains`] for owner action.
    ///
    /// OneDrive resolves native identities across all permission pages and keeps
    /// email-bearing grants until dependent ID-only removals succeed. Drive removes
    /// direct access without changing parent permissions. Dropbox pages direct and
    /// inherited memberships separately and waits for the native removal job,
    /// preserving its failure cause; group and parent access remain reported.
    async fn revoke_access(&self, member: &MemberAccess) -> Result<MemberRemoval, StorageError>;
    /// Begin a create-once upload, returning the value to record before parts are sent.
    /// Posted positions are refused: replacement always sends complete bytes in one request.
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError>;
    /// Begin a fresh recorded session after `resume_upload` reports `SessionExpired`.
    /// The destination and total stay fixed; the caller records the returned value
    /// and supplies the retained encrypted bytes again, starting at offset zero.
    async fn restart_upload(&self, expired: &UploadSession) -> Result<UploadSession, StorageError> {
        expired.check(&self.config())?;
        if expired.is_complete() {
            return Err(StorageError::InvalidPart);
        }
        self.begin_upload(expired.path(), expired.total_bytes())
            .await
    }
    /// Refresh a recorded session from the provider after interruption.
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError>;
    /// Send the next part, advancing the value only after confirmation. After
    /// a lost publication reply this verifies the stored bytes instead; see
    /// [`UploadSession`].
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError>;
    /// Publish all stored parts at the destination. A repeated completion is safe.
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError>;
    /// Explicitly abandon an upload. Dropping a recorded value does not abort it.
    /// Already closed or absent sessions succeed; published objects are preserved.
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError>;

    /// Provider-specific sign-out instructions; coven never makes or deletes S3 keys.
    fn sign_out(&self) -> ProviderSignOut {
        match self.config().provider() {
            CloudProvider::S3 => ProviderSignOut::ReplaceAccessKey,
            CloudProvider::CloudKit => ProviderSignOut::RemoveFromAppleAccount,
            provider => ProviderSignOut::RemoveAppAccess { provider },
        }
    }

    /// Retry an operation's create with its fixed encrypted bytes (§18).
    /// An occupied path counts as stored, without a second read. Paths have one
    /// writer except dropped-removal key copies (§4, §11): competing sealed
    /// copies at those paths contain the same key, and the first stored wins.
    /// On Drive a retry keeps the earliest copy (createdTime, then id) and
    /// deletes later copies from this writer.
    async fn create_once(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        match self.create(path, bytes).await {
            Err(error) if error.failure() == crate::StorageFailure::AlreadyExists => Ok(()),
            result => result,
        }
    }

    /// Create the store's first encrypted store-log entry at an empty location.
    /// Reconnect when the location contains that same entry, including when later
    /// objects have been uploaded. The facade commits settings and credentials
    /// only after this call succeeds (E5).
    /// Another store or unrelated content returns [`StorageSetupError::LocationOccupied`].
    /// Provider folders must be ancestors in coven's layout; empty layout folders
    /// left by interrupted uploads contain no stored objects. Two setups observing
    /// an empty location may both succeed; sync detects their listed first entries.
    async fn setup(
        &self,
        first_entry: &ObjectPath,
        encrypted_entry: &[u8],
    ) -> Result<StorageConfig, StorageSetupError> {
        self.config().validate()?;
        if !first_entry.is_first_store_entry() {
            return Err(StorageError::InvalidPath.into());
        }
        let objects = match self.list(&ObjectPrefix::all()).await {
            Ok(objects) => objects,
            Err(StorageError::InvalidPath) => return Err(StorageSetupError::LocationOccupied),
            Err(error) => return Err(error.into()),
        };
        if !objects.is_empty() {
            if !objects.iter().any(|object| &object.path == first_entry)
                || self.read(first_entry).await? != encrypted_entry
            {
                return Err(StorageSetupError::LocationOccupied);
            }
        } else {
            self.create_once(first_entry, encrypted_entry).await?;
        }
        Ok(self.config())
    }
}
