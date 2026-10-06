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

/// How a removed device's member cuts off its provider access (§13, §20.9).
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

/// An account to share with, or the member's S3 key to revoke.
pub enum MemberAccess {
    /// The member's provider account email.
    ProviderAccount(String),
    /// An S3 key the admin made by hand.
    S3AccessKey {
        /// Public identifier the admin finds in the provider console.
        access_key_id: String,
    },
}

/// Granting access succeeds either through the provider or through an admin action.
pub enum AccessGrant {
    /// The provider shared the store with the account.
    Granted,
    /// The admin makes a key in the provider console and enters it in the invite.
    CreateAccessKey,
}

/// Revocation includes S3's manual instruction; it is not an error (§20.9).
pub enum MemberRemoval {
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetainedAccess {
    /// The native permission, membership or group id, scoped to this store.
    pub provider_id: String,
    /// Why removing this grant cannot revoke only the requested account.
    pub reason: RetainedAccessReason,
}

/// The provider's reason that a grant was retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    /// Provider timestamp, never the uploading device's clock. Drive and OneDrive
    /// return creation time; Dropbox returns server modification time; CloudKit
    /// returns publication time. S3 returns Last-Modified, which is initiation
    /// time for multipart objects and cannot establish their age since completion.
    pub stored_at: std::time::SystemTime,
}

/// The operations coven needs from a provider (§4).
///
/// Implementations are capabilities assembled at composition roots. They own no
/// database or key custody. Every mutation is awaited and failures reach the
/// caller; no hidden background work repairs failed operations.
#[async_trait]
pub trait Storage: Send + Sync {
    /// This provider's nonsecret location settings.
    fn config(&self) -> StorageConfig;
    /// Largest complete encrypted body sent in one request, in bytes. Larger
    /// create-once objects use resumable or multipart uploads, for every path kind.
    /// Callers retaining sessions across crashes use `begin_upload` and record them.
    fn single_request_limit(&self) -> u64;
    /// Install replacement OAuth tokens after the owner commits them to key custody.
    /// S3 and CloudKit refuse OAuth tokens; they use different account credentials.
    async fn set_oauth_tokens(&self, _tokens: crate::OAuthTokens) -> Result<(), StorageError> {
        Err(StorageError::InvalidConfiguration(
            "provider does not use OAuth",
        ))
    }
    /// Create a complete encrypted object, refusing an occupied path.
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError>;
    /// Replace a device's posted positions; immutable paths are refused.
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
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError>;
    /// Delete an object, or remove it from Drive's folder when only that is allowed.
    /// An already absent object succeeds, making operation retries safe.
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError>;
    /// Share the store using its owner's account, or tell an S3 admin to create a key.
    /// A signed-in sharing account that does not own the location gets `NotStoreOwner`.
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError>;
    /// Unshare through the owner's account, or tell an S3 admin which key to delete.
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
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError>;

    /// Provider-specific sign-out instructions; coven never makes or deletes S3 keys.
    fn sign_out(&self) -> ProviderSignOut {
        match self.config().provider() {
            CloudProvider::S3 => ProviderSignOut::ReplaceAccessKey,
            CloudProvider::CloudKit => ProviderSignOut::RemoveFromAppleAccount,
            provider => ProviderSignOut::RemoveAppAccess { provider },
        }
    }

    /// Retry an operation's create with the same encrypted bytes (§18).
    async fn create_once(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        match self.create(path, bytes).await {
            Err(error) if error.failure() == crate::StorageFailure::AlreadyExists => {
                if self.read(path).await? == bytes {
                    Ok(())
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }

    /// Check create, read, range, list and delete using a fresh encrypted probe
    /// supplied by the caller. The probe path must be unused; no connection or
    /// local settings are committed. A failed cleanup is returned too.
    async fn probe(&self, path: &ObjectPath, encrypted_bytes: &[u8]) -> Result<(), StorageError> {
        let range = ByteRange::new(0, encrypted_bytes.len() as u64)?;
        let created = self.create(path, encrypted_bytes).await;
        if created
            .as_ref()
            .is_err_and(|error| error.failure() == crate::StorageFailure::AlreadyExists)
        {
            return created;
        }
        let check = async {
            created?;
            if self.read(path).await? != encrypted_bytes
                || self.read_range(path, range).await? != encrypted_bytes
            {
                return Err(StorageError::Protocol("probe read disagrees with upload"));
            }
            match self.create(path, encrypted_bytes).await {
                Err(error) if error.failure() == crate::StorageFailure::AlreadyExists => {}
                Err(error) => return Err(error),
                Ok(()) => {
                    return Err(StorageError::Protocol(
                        "provider overwrote a create-once path",
                    ))
                }
            }
            if !self
                .list(&ObjectPrefix::all())
                .await?
                .iter()
                .any(|object| &object.path == path)
            {
                return Err(StorageError::Protocol("probe absent from listing"));
            }
            Ok(())
        }
        .await;
        match (check, self.delete(path).await) {
            (Ok(()), cleanup) => cleanup,
            (Err(error), Ok(())) => Err(error),
            (Err(operation), Err(cleanup)) => Err(StorageError::Cleanup {
                operation: Box::new(operation),
                cleanup: Box::new(cleanup),
            }),
        }
    }

    /// Create the store's first encrypted store-log entry at an empty location.
    /// Reconnect when the location contains that same entry, including when later
    /// objects have been uploaded. The facade commits settings and credentials
    /// only after this call succeeds (§20.5).
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
