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
    /// Follow native query cursors through all records under the prefix.
    async fn list(
        &self,
        location: &StorageConfig,
        prefix: &ObjectPrefix,
    ) -> Result<Vec<ObjectPath>, StorageError>;
    /// Delete an object and its parts; repeat deletion succeeds when absent.
    async fn delete(&self, location: &StorageConfig, path: &ObjectPath)
        -> Result<(), StorageError>;
    /// Set read/write CKShare participation for the named Apple account.
    async fn set_access(
        &self,
        location: &StorageConfig,
        email: &str,
        granted: bool,
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
    /// Discard pending parts without deleting a published object.
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
        Ok(Self { config, ops })
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
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.ops.create(&self.config, path, bytes).await
    }
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        if !path.is_replaceable() {
            return Err(StorageError::InvalidPath);
        }
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
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectPath>, StorageError> {
        let paths = self.ops.list(&self.config, prefix).await?;
        let mut unique = std::collections::BTreeSet::new();
        for path in paths {
            if !prefix.contains(&path) || !unique.insert(path) {
                return Err(StorageError::Protocol("invalid CloudKit prefix listing"));
            }
        }
        Ok(unique.into_iter().collect())
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.ops.delete(&self.config, path).await
    }
    async fn deletion_rights(&self, path: &ObjectPath) -> Result<DeletionRights, StorageError> {
        self.ops.read(&self.config, path, None).await?;
        Ok(DeletionRights::Delete)
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        self.ops.set_access(&self.config, account, true).await?;
        Ok(AccessGrant::Granted)
    }
    async fn revoke_access(&self, member: &MemberAccess) -> Result<MemberRemoval, StorageError> {
        let MemberAccess::ProviderAccount(email) = member else {
            return Err(StorageError::InvalidConfiguration(
                "CloudKit requires an account",
            ));
        };
        self.ops.set_access(&self.config, email, false).await?;
        Ok(MemberRemoval::Revoked)
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
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
        self.ops.abort_upload(&self.config, self.id(session)?).await
    }
}

#[cfg(test)]
#[path = "cloudkit_tests.rs"]
mod tests;
