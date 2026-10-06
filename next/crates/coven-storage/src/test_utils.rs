//! An in-memory provider, fault injection and the shared provider conformance suite.
use crate::session::SessionState;
use crate::*;
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Faults applied to the next calls, including accepted parts with lost replies.
#[derive(Clone)]
pub struct Faults {
    /// Fail this many calls before doing their work.
    pub fail_next: usize,
    /// The classification of injected failures.
    pub failure: StorageFailure,
    /// Delay each call without blocking its executor.
    pub delay: Duration,
    /// Drop the next part before it is stored.
    pub drop_part: bool,
    /// Store the next part, but lose the reply before advancing the caller's session.
    pub lose_part_reply: bool,
}
impl Faults {
    /// No failures or delays.
    pub fn none() -> Self {
        Self {
            fail_next: 0,
            failure: StorageFailure::Network,
            delay: Duration::ZERO,
            drop_part: false,
            lose_part_reply: false,
        }
    }
}

struct Pending {
    path: ObjectPath,
    total: u64,
    bytes: Vec<u8>,
}
struct State {
    objects: BTreeMap<ObjectPath, Vec<u8>>,
    uploads: BTreeMap<u64, Pending>,
    next: u64,
    accounts: BTreeSet<String>,
    faults: Faults,
}

/// Clones share a provider's durable objects and sessions across simulated crashes.
#[derive(Clone)]
pub struct MemoryStorage {
    config: StorageConfig,
    state: Arc<Mutex<State>>,
}
impl MemoryStorage {
    /// A store at the supplied location, using four-byte parts for crash tests.
    pub fn new(config: StorageConfig) -> Result<Self, StorageError> {
        config.validate()?;
        Ok(Self {
            config,
            state: Arc::new(Mutex::new(State {
                objects: BTreeMap::new(),
                uploads: BTreeMap::new(),
                next: 1,
                accounts: BTreeSet::new(),
                faults: Faults::none(),
            })),
        })
    }
    /// Set faults absolutely, so repeating the command has the same effect.
    pub async fn set_faults(&self, faults: Faults) {
        self.state.lock().await.faults = faults;
    }
    async fn before(&self) -> Result<(), StorageError> {
        let (delay, failure) = {
            let mut state = self.state.lock().await;
            let failure = if state.faults.fail_next > 0 {
                state.faults.fail_next -= 1;
                Some(state.faults.failure)
            } else {
                None
            };
            (state.faults.delay, failure)
        };
        tokio::time::sleep(delay).await;
        match failure {
            Some(failure) => Err(StorageError::Injected(failure)),
            None => Ok(()),
        }
    }
    fn session_id(&self, session: &UploadSession) -> Result<u64, StorageError> {
        session.check(&self.config)?;
        match session.state {
            SessionState::Memory { id, .. } => Ok(id),
            _ => Err(StorageError::SessionMismatch),
        }
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    fn config(&self) -> StorageConfig {
        self.config.clone()
    }
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.before().await?;
        let mut state = self.state.lock().await;
        if state.objects.contains_key(path) {
            return Err(StorageError::AlreadyExists);
        }
        state.objects.insert(path.clone(), bytes.to_vec());
        Ok(())
    }
    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        if !path.is_replaceable() {
            return Err(StorageError::InvalidPath);
        }
        self.before().await?;
        self.state
            .lock()
            .await
            .objects
            .insert(path.clone(), bytes.to_vec());
        Ok(())
    }
    async fn read(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError> {
        self.before().await?;
        self.state
            .lock()
            .await
            .objects
            .get(path)
            .cloned()
            .ok_or(StorageError::NotFound)
    }
    async fn read_range(
        &self,
        path: &ObjectPath,
        range: ByteRange,
    ) -> Result<Vec<u8>, StorageError> {
        range.select(&self.read(path).await?)
    }
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectPath>, StorageError> {
        self.before().await?;
        Ok(self
            .state
            .lock()
            .await
            .objects
            .keys()
            .filter(|path| prefix.contains(path))
            .cloned()
            .collect())
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.before().await?;
        self.state.lock().await.objects.remove(path);
        Ok(())
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        self.before().await?;
        if self.config.provider() == CloudProvider::S3 {
            return Ok(AccessGrant::CreateAccessKey);
        }
        self.state.lock().await.accounts.insert(account.into());
        Ok(AccessGrant::Granted)
    }
    async fn revoke_access(&self, member: &MemberAccess) -> Result<MemberRemoval, StorageError> {
        self.before().await?;
        match (self.config.provider(), member) {
            (CloudProvider::S3, MemberAccess::S3AccessKey { access_key_id }) => {
                Ok(MemberRemoval::DeleteAccessKey {
                    access_key_id: access_key_id.clone(),
                })
            }
            (_, MemberAccess::ProviderAccount(account)) => {
                self.state.lock().await.accounts.remove(account);
                Ok(MemberRemoval::Revoked)
            }
            _ => Err(StorageError::InvalidConfiguration(
                "wrong member access kind",
            )),
        }
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        if path.is_replaceable() {
            return Err(StorageError::InvalidPath);
        }
        self.before().await?;
        if total == 0 {
            return Err(StorageError::InvalidPart);
        }
        let mut state = self.state.lock().await;
        if state.objects.contains_key(path) {
            return Err(StorageError::AlreadyExists);
        }
        let id = state.next;
        state.next = id
            .checked_add(1)
            .ok_or(StorageError::Protocol("session ids exhausted"))?;
        state.uploads.insert(
            id,
            Pending {
                path: path.clone(),
                total,
                bytes: Vec::new(),
            },
        );
        Ok(UploadSession {
            location: self.config(),
            path: path.clone(),
            total,
            confirmed: 0,
            part_size: 4,
            state: SessionState::Memory {
                id,
                complete: false,
            },
        })
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.before().await?;
        session.check(&self.config)?;
        if session.is_complete() {
            return Ok(());
        }
        let id = self.session_id(session)?;
        let state = self.state.lock().await;
        let pending = state.uploads.get(&id).ok_or(StorageError::SessionExpired)?;
        if pending.path != session.path || pending.total != session.total {
            return Err(StorageError::SessionMismatch);
        }
        session.confirmed = pending.bytes.len() as u64;
        Ok(())
    }
    async fn upload_part(
        &self,
        session: &mut UploadSession,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        self.before().await?;
        let id = self.session_id(session)?;
        let end = session.end_of_part(bytes.len())?;
        let mut state = self.state.lock().await;
        if std::mem::replace(&mut state.faults.drop_part, false) {
            return Err(StorageError::Injected(StorageFailure::Network));
        }
        let pending = state
            .uploads
            .get_mut(&id)
            .ok_or(StorageError::SessionExpired)?;
        if pending.bytes.len() as u64 != session.confirmed {
            return Err(StorageError::InvalidPart);
        }
        pending.bytes.extend_from_slice(bytes);
        if std::mem::replace(&mut state.faults.lose_part_reply, false) {
            return Err(StorageError::Injected(StorageFailure::Network));
        }
        session.confirmed = end;
        Ok(())
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.before().await?;
        session.check(&self.config)?;
        if session.is_complete() {
            return Ok(());
        }
        let id = self.session_id(session)?;
        let mut state = self.state.lock().await;
        let pending = state.uploads.get(&id).ok_or(StorageError::SessionExpired)?;
        if pending.bytes.len() as u64 != session.total || session.confirmed != session.total {
            return Err(StorageError::InvalidPart);
        }
        if let Some(existing) = state.objects.get(&session.path) {
            if existing != &pending.bytes {
                return Err(StorageError::AlreadyExists);
            }
        } else {
            let bytes = pending.bytes.clone();
            state.objects.insert(session.path.clone(), bytes);
        }
        session.state = SessionState::Memory { id, complete: true };
        Ok(())
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        self.before().await?;
        session.check(&self.config)?;
        if session.is_complete() {
            return Ok(());
        }
        self.state
            .lock()
            .await
            .uploads
            .remove(&self.session_id(session)?);
        Ok(())
    }
}

/// Run identical object, range, prefix and immutable-create checks on any provider.
/// The caller supplies the capability at construction, like production composition.
pub struct Conformance {
    storage: Arc<dyn Storage>,
}
impl Conformance {
    /// The provider under test, at an otherwise empty location.
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }
    /// Exercise the real provider operations, returning the first violated contract.
    pub async fn run(&self) -> Result<(), StorageError> {
        use coven_foundation::id_source::DeviceId;
        let path = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
        let other = ObjectPath::device_log(DeviceId(32), std::num::NonZeroU64::MIN);
        let data = b"encrypted bytes";
        self.storage.create(&path, data).await?;
        self.storage.create(&other, b"other").await?;
        if self.storage.read(&path).await? != data {
            return Err(StorageError::Protocol("whole read"));
        }
        for (start, end) in [(0, 3), (4, 8), (12, 15)] {
            let range = ByteRange::new(start, end)?;
            if self.storage.read_range(&path, range).await? != range.select(data)? {
                return Err(StorageError::Protocol("range read"));
            }
        }
        if self
            .storage
            .read_range(&path, ByteRange::new(15, 16)?)
            .await
            .is_ok()
        {
            return Err(StorageError::Protocol("past-end range accepted"));
        }
        if self
            .storage
            .create(&path, b"replacement")
            .await
            .err()
            .map(|e| e.failure())
            != Some(StorageFailure::AlreadyExists)
        {
            return Err(StorageError::Protocol("create-once refusal"));
        }
        self.storage.create_once(&path, data).await?;
        if self
            .storage
            .list(&ObjectPrefix::device_log(DeviceId(31)))
            .await?
            != [path.clone()]
        {
            return Err(StorageError::Protocol("prefix listing"));
        }
        self.storage.delete(&path).await?;
        self.storage.delete(&path).await?;
        self.storage.delete(&other).await?;
        if self.storage.read(&path).await.err().map(|e| e.failure())
            != Some(StorageFailure::NotFound)
        {
            return Err(StorageError::Protocol("deleted object read"));
        }
        let positions = ObjectPath::positions(DeviceId(31));
        if !matches!(
            self.storage.begin_upload(&positions, 1).await,
            Err(StorageError::InvalidPath)
        ) {
            return Err(StorageError::Protocol(
                "positions accepted a recorded upload",
            ));
        }
        if !matches!(
            self.storage.replace(&path, data).await,
            Err(StorageError::InvalidPath)
        ) {
            return Err(StorageError::Protocol(
                "immutable object accepted replacement",
            ));
        }
        self.storage.replace(&positions, b"first positions").await?;
        self.storage.replace(&positions, b"next positions").await?;
        if self.storage.read(&positions).await? != b"next positions" {
            return Err(StorageError::Protocol("posted positions replacement"));
        }
        self.storage.delete(&positions).await?;
        self.storage.probe(&path, data).await?;
        let first = ObjectPath::store_log(DeviceId(31), std::num::NonZeroU64::MIN);
        self.storage
            .setup(&first, data)
            .await
            .map_err(|error| match error {
                StorageSetupError::Storage(error) => error,
                _ => StorageError::Protocol("setup at an empty location failed"),
            })?;
        if self.storage.read(&first).await? != data {
            return Err(StorageError::Protocol(
                "setup did not create the first entry",
            ));
        }
        self.storage.delete(&first).await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "test_utils_tests.rs"]
mod tests;
