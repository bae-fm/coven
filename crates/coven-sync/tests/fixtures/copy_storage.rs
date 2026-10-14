//! A storage wrapper that interrupts, races, hides or damages chosen objects,
//! simulating provider behavior around key-copy publication.

use super::*;
use coven_storage::{
    AccessGrant, ByteRange, RetainedAccess, StorageError, StoredObject, UploadSession,
};
use std::sync::Mutex;
use tokio::sync::Barrier;

pub(super) enum CopyAttempt {
    InterruptOnce,
    LoseReplyOnce,
    FailComparison,
    Race(Barrier),
    /// These objects have not landed yet for this reader.
    Hidden,
    /// The provider stores different bytes for copies addressed to this member.
    CorruptFor(coven_crypto::MemberId),
}

pub(super) struct CopyStorage {
    storage: Arc<MemoryStorage>,
    paths: Vec<ObjectPath>,
    action: CopyAttempt,
    attempts: Mutex<Vec<(ObjectPath, Vec<u8>)>>,
}

impl CopyStorage {
    pub(super) fn new(
        storage: Arc<MemoryStorage>,
        paths: Vec<ObjectPath>,
        action: CopyAttempt,
    ) -> Self {
        Self {
            storage,
            paths,
            action,
            attempts: Mutex::new(Vec::new()),
        }
    }

    /// Every create attempted at a chosen path, in order.
    pub(super) fn attempts(&self) -> Vec<(ObjectPath, Vec<u8>)> {
        self.attempts.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Storage for CopyStorage {
    fn config(&self) -> StorageConfig {
        self.storage.config()
    }
    fn single_request_limit(&self) -> u64 {
        self.storage.single_request_limit()
    }
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        if self.paths.contains(path) || matches!(self.action, CopyAttempt::CorruptFor(_)) {
            let first = {
                let mut attempts = self.attempts.lock().unwrap();
                attempts.push((path.clone(), bytes.to_vec()));
                attempts.len() == 1
            };
            match &self.action {
                CopyAttempt::InterruptOnce if first => {
                    return Err(StorageError::Failure(StorageFailure::Network))
                }
                CopyAttempt::LoseReplyOnce if first => {
                    self.storage.create(path, bytes).await?;
                    return Err(StorageError::Failure(StorageFailure::Network));
                }
                CopyAttempt::Race(barrier) => {
                    barrier.wait().await;
                }
                CopyAttempt::CorruptFor(member)
                    if path.key_copy_position().is_some()
                        && &coven_format::sealed_key::SealedKey::decode(bytes)
                            .unwrap()
                            .prefix()
                            .member
                            == member =>
                {
                    let mut stored = bytes.to_vec();
                    let last = stored.len() - 1;
                    stored[last] ^= 1;
                    return self.storage.create(path, &stored).await;
                }
                _ => (),
            }
        }
        self.storage.create(path, bytes).await
    }
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.storage.replace(path, bytes).await
    }
    async fn status(&self, path: &ObjectPath) -> Result<Option<StoredObject>, StorageError> {
        if self.paths.contains(path) && matches!(self.action, CopyAttempt::Hidden) {
            return Ok(None);
        }
        self.storage.status(path).await
    }
    async fn stream_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<coven_storage::ObjectStream<'_>, StorageError> {
        if self.paths.contains(path) {
            match self.action {
                CopyAttempt::FailComparison => return Err(StorageFailure::Network.into()),
                CopyAttempt::Hidden => return Err(StorageFailure::NotFound.into()),
                _ => (),
            }
        }
        self.storage.stream_object(path, range).await
    }
    async fn list_writers(&self, store: StoreId) -> Result<Vec<DeviceId>, StorageError> {
        self.storage.list_writers(store).await
    }
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        self.storage.list(prefix).await
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.storage.delete(path).await
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        self.storage.grant_access(account).await
    }
    async fn revoke_access(&self, account: &str) -> Result<Vec<RetainedAccess>, StorageError> {
        self.storage.revoke_access(account).await
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        self.storage.begin_upload(path, total).await
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.storage.resume_upload(session).await
    }
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        self.storage.upload_part(session, bytes).await
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.storage.finish_upload(session).await
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        self.storage.abort_upload(session).await
    }
}

pub(crate) fn failing_creation(
    storage: Arc<MemoryStorage>,
    path: ObjectPath,
    lost_reply: bool,
) -> Arc<dyn Storage> {
    Arc::new(CopyStorage::new(
        storage,
        vec![path],
        if lost_reply {
            CopyAttempt::LoseReplyOnce
        } else {
            CopyAttempt::InterruptOnce
        },
    ))
}
