use crate::session::SessionState;
use crate::*;
use async_trait::async_trait;
use coven_crypto::SecretText;
use std::sync::Arc;

/// A durable upload prepared by the app's CloudKit bridge.
pub struct CloudKitUpload {
    /// The bridge's recorded session capability, erased on drop.
    pub id: SecretText,
    /// Bounded part size accepted by the bridge's CKAsset representation.
    pub part_size: usize,
}

/// CloudKit's confirmed state, independent of the caller's last recorded part.
pub enum CloudKitUploadStatus {
    /// Bytes stored in the durable session, before publishing the complete object.
    Uploading {
        /// The contiguous stored prefix.
        confirmed: u64,
    },
    /// The exact session's object has been published in the zone.
    Complete,
}

/// Native CloudKit calls implemented by the app (§20.1).
///
/// The bridge owns CKAssets and native record handling; all byte payloads it is
/// given are already encrypted. It maps paths to stable record names, enforces
/// create-only saves on the server, and makes each object visible as a whole.
/// Large objects use bounded assets and a publication record committed after
/// the parts. Upload ids and stored parts must survive an app restart. Bridge
/// failures preserve native causes in `StorageError::Provider` with CloudKit's
/// classification. None of these calls starts unowned background work.
#[async_trait]
pub trait CloudKitOps: Send + Sync {
    /// Whether the signed-in Apple account owns this store's shared zone.
    async fn is_owner(&self, location: &StorageConfig) -> Result<bool, StorageError>;
    /// Largest encrypted object this bridge saves in one native call. Must be
    /// nonzero; larger objects use the bridge's durable bounded-asset upload.
    fn single_request_limit(&self) -> u64;
    /// Save complete encrypted bytes using the server's create-only policy.
    async fn create(
        &self,
        location: &StorageConfig,
        path: &ObjectPath,
        bytes: &[u8],
    ) -> Result<(), StorageError>;
    /// Replace a complete posted-positions object atomically.
    async fn replace(
        &self,
        location: &StorageConfig,
        path: &ObjectPath,
        bytes: &[u8],
    ) -> Result<(), StorageError>;
    /// Read complete bytes or only the asset parts covering a nonempty range.
    async fn read(
        &self,
        location: &StorageConfig,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<Vec<u8>, StorageError>;
    /// Follow every native query cursor. Return encrypted size and server publication
    /// time of each complete object; pending assets are not listed.
    async fn list(
        &self,
        location: &StorageConfig,
        prefix: &ObjectPrefix,
    ) -> Result<Vec<StoredObject>, StorageError>;
    /// Delete an object and its parts; repeat deletion succeeds when absent.
    async fn delete(&self, location: &StorageConfig, path: &ObjectPath)
        -> Result<(), StorageError>;
    /// Save read/write CKShare participation and return its server-issued share URL.
    async fn grant_access(
        &self,
        location: &StorageConfig,
        email: &str,
    ) -> Result<SecretText, StorageError>;
    /// Remove only the named account. Preserve the owner and grants reaching other
    /// accounts, returning those grants for owner action.
    async fn revoke_access(
        &self,
        location: &StorageConfig,
        email: &str,
    ) -> Result<MemberRemoval, StorageError>;
    /// Fetch CKShare metadata for this URL and verify its container, owner and zone
    /// against location before accepting it with the signed-in recipient account.
    /// Repeated acceptance succeeds; native permission errors retain their causes.
    async fn accept_share(
        &self,
        location: &StorageConfig,
        url: &SecretText,
    ) -> Result<(), StorageError>;
    /// Prepare a durable upload that has not yet published its destination.
    async fn begin_upload(
        &self,
        location: &StorageConfig,
        path: &ObjectPath,
        total: u64,
    ) -> Result<CloudKitUpload, StorageError>;
    /// Return progress from the recorded upload, including accepted lost replies.
    async fn upload_status(
        &self,
        location: &StorageConfig,
        id: &SecretText,
    ) -> Result<CloudKitUploadStatus, StorageError>;
    /// Store a part at its absolute offset; retrying identical bytes is idempotent.
    async fn upload_part(
        &self,
        location: &StorageConfig,
        id: &SecretText,
        offset: u64,
        bytes: &[u8],
    ) -> Result<(), StorageError>;
    /// Publish all parts atomically at their reserved object path. Repeating a
    /// completed session succeeds and never overwrites another object.
    async fn finish_upload(
        &self,
        location: &StorageConfig,
        id: &SecretText,
    ) -> Result<(), StorageError>;
    /// Discard pending parts without deleting a published object. Already closed
    /// or absent sessions may return success or a classified not-found error.
    async fn abort_upload(
        &self,
        location: &StorageConfig,
        id: &SecretText,
    ) -> Result<(), StorageError>;
}

/// iCloud storage through the app's native CloudKit bridge.
pub struct CloudKitStorage {
    config: StorageConfig,
    ops: Arc<dyn CloudKitOps>,
}
impl CloudKitStorage {
    /// Bind the injected bridge to one store's zone at the composition root.
    pub fn new(config: StorageConfig, ops: Arc<dyn CloudKitOps>) -> Result<Self, StorageError> {
        config.validate()?;
        if !matches!(config, StorageConfig::CloudKit { .. }) {
            return Err(StorageError::InvalidConfiguration("expected CloudKit zone"));
        }
        if ops.single_request_limit() == 0 {
            return Err(StorageError::InvalidConfiguration(
                "zero CloudKit request limit",
            ));
        }
        Ok(Self { config, ops })
    }
    async fn require_owner(&self) -> Result<(), StorageError> {
        if !self.ops.is_owner(&self.config).await? {
            return Err(StorageError::NotStoreOwner);
        }
        Ok(())
    }
    fn id<'a>(&self, session: &'a UploadSession) -> Result<&'a SecretText, StorageError> {
        session.check(&self.config)?;
        match &session.state {
            SessionState::CloudKit { id } => Ok(id),
            _ => Err(StorageError::SessionMismatch),
        }
    }
}
#[async_trait]
impl Storage for CloudKitStorage {
    fn config(&self) -> StorageConfig {
        self.config.clone()
    }
    fn single_request_limit(&self) -> u64 {
        self.ops.single_request_limit()
    }
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        crate::transfer::upload_bytes(
            self,
            path,
            bytes,
            self.ops.create(&self.config, path, bytes),
        )
        .await
    }
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        if !path.is_replaceable() {
            return Err(StorageError::InvalidPath);
        }
        crate::transfer::check_single_request(bytes.len() as u64, self.single_request_limit())?;
        self.ops.replace(&self.config, path, bytes).await
    }
    async fn read(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError> {
        self.ops.read(&self.config, path, None).await
    }
    async fn read_range(
        &self,
        path: &ObjectPath,
        range: ByteRange,
    ) -> Result<Vec<u8>, StorageError> {
        let bytes = self.ops.read(&self.config, path, Some(range)).await?;
        if bytes.len() as u64 != range.len() {
            return Err(StorageError::Protocol("CloudKit returned a short range"));
        }
        Ok(bytes)
    }
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        let paths = self.ops.list(&self.config, prefix).await?;
        let mut unique = std::collections::BTreeMap::new();
        for object in paths {
            if !prefix.contains(&object.path)
                || unique.insert(object.path.clone(), object).is_some()
            {
                return Err(StorageError::Protocol("invalid CloudKit prefix listing"));
            }
        }
        Ok(unique.into_values().collect())
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.ops.delete(&self.config, path).await
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        self.require_owner().await?;
        let url = self.ops.grant_access(&self.config, account).await?;
        Ok(AccessGrant::Granted {
            invitation: StorageInvitation::new(
                self.config(),
                crate::invitation::InvitationAcceptance::CloudKitShare { url },
            )?,
        })
    }
    async fn join(&self, invitation: &StorageInvitation) -> Result<(), StorageError> {
        invitation.check(&self.config)?;
        let crate::invitation::InvitationAcceptance::CloudKitShare { url } = &invitation.acceptance
        else {
            return Err(StorageError::InvitationMismatch);
        };
        self.ops.accept_share(&self.config, url).await?;
        self.list(&ObjectPrefix::all()).await?;
        Ok(())
    }
    async fn revoke_access(&self, member: &MemberAccess) -> Result<MemberRemoval, StorageError> {
        let MemberAccess::ProviderAccount(email) = member else {
            return Err(StorageError::InvalidConfiguration(
                "CloudKit requires an account",
            ));
        };
        self.require_owner().await?;
        self.ops.revoke_access(&self.config, email).await
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        if path.is_replaceable() {
            return Err(StorageError::InvalidPath);
        }
        if total == 0 {
            return Err(StorageError::InvalidPart);
        }
        let upload = self.ops.begin_upload(&self.config, path, total).await?;
        if upload.part_size == 0 || upload.id.as_str().is_empty() {
            return Err(StorageError::Protocol("invalid CloudKit session"));
        }
        Ok(UploadSession {
            location: self.config(),
            path: path.clone(),
            total,
            confirmed: 0,
            part_size: upload.part_size,
            state: SessionState::CloudKit { id: upload.id },
        })
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        session.check(&self.config)?;
        if session.is_complete() {
            return Ok(());
        }
        match self
            .ops
            .upload_status(&self.config, self.id(session)?)
            .await?
        {
            CloudKitUploadStatus::Complete => {
                session.confirmed = session.total;
                session.state = SessionState::Complete;
            }
            CloudKitUploadStatus::Uploading { confirmed } => {
                if confirmed < session.confirmed || confirmed > session.total {
                    return Err(StorageError::Protocol("CloudKit lost confirmed parts"));
                }
                session.confirmed = confirmed;
            }
        }
        Ok(())
    }
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        let end = session.end_of_part(bytes.len())?;
        self.ops
            .upload_part(&self.config, self.id(session)?, session.confirmed, bytes)
            .await?;
        session.confirmed = end;
        Ok(())
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        session.check(&self.config)?;
        if session.is_complete() {
            return Ok(());
        }
        if session.confirmed != session.total {
            return Err(StorageError::InvalidPart);
        }
        self.ops
            .finish_upload(&self.config, self.id(session)?)
            .await?;
        session.state = SessionState::Complete;
        Ok(())
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        session.check(&self.config)?;
        if session.is_complete() {
            return Ok(());
        }
        match self.ops.abort_upload(&self.config, self.id(session)?).await {
            Err(error) if error.failure() == StorageFailure::NotFound => Ok(()),
            result => result,
        }
    }
}

#[cfg(test)]
#[path = "cloudkit_tests.rs"]
mod tests;
