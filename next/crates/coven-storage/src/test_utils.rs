//! An in-memory provider, fault injection and the shared provider conformance suite.
use crate::session::SessionState;
use crate::*;
use async_trait::async_trait;
use coven_foundation::clock::ClockRef;
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
    /// Publish the next upload but lose the completion reply.
    pub lose_completion_reply: bool,
    /// Expire all pending sessions before the next call.
    pub expire_uploads: bool,
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
            lose_completion_reply: false,
            expire_uploads: false,
        }
    }
}

struct Object {
    upload_id: Option<u64>,
    bytes: Vec<u8>,
    stored_at: std::time::SystemTime,
}
struct Pending {
    path: ObjectPath,
    total: u64,
    bytes: Vec<u8>,
}
impl Pending {
    fn check(&self, session: &UploadSession) -> Result<(), StorageError> {
        if self.path != session.path || self.total != session.total {
            return Err(StorageError::SessionMismatch);
        }
        if (self.bytes.len() as u64) < session.confirmed || self.bytes.len() as u64 > self.total {
            return Err(StorageError::Protocol(
                "memory provider lost confirmed progress",
            ));
        }
        Ok(())
    }
}
fn confirm_published(
    state: &State,
    session: &mut UploadSession,
    id: u64,
) -> Result<(), StorageError> {
    let object = state
        .objects
        .get(&session.path)
        .ok_or(StorageError::SessionExpired)?;
    if object.upload_id != Some(id) || object.bytes.len() as u64 != session.total {
        return Err(StorageError::AlreadyExists);
    }
    session.confirmed = session.total;
    session.state = SessionState::Memory { id, complete: true };
    Ok(())
}

struct State {
    objects: BTreeMap<ObjectPath, Object>,
    uploads: BTreeMap<u64, Pending>,
    next: u64,
    accounts: BTreeSet<String>,
    faults: Faults,
}

/// Clones share a provider's durable objects and sessions across simulated crashes.
#[derive(Clone)]
pub struct MemoryStorage {
    config: StorageConfig,
    owns_location: bool,
    clock: ClockRef,
    tokens: Arc<Mutex<Option<OAuthTokens>>>,
    state: Arc<Mutex<State>>,
}
impl MemoryStorage {
    /// A store at the supplied location, with a sixteen-byte single-request limit
    /// and four-byte parts for transfer and crash tests.
    pub fn new(config: StorageConfig, clock: ClockRef) -> Result<Self, StorageError> {
        config.validate()?;
        Ok(Self {
            config,
            clock,
            tokens: Arc::new(Mutex::new(None)),
            owns_location: true,
            state: Arc::new(Mutex::new(State {
                objects: BTreeMap::new(),
                uploads: BTreeMap::new(),
                next: 1,
                accounts: BTreeSet::new(),
                faults: Faults::none(),
            })),
        })
    }
    /// Set whether this adapter's account owns the shared location. Clones keep
    /// their own account authority while sharing objects and pending uploads.
    pub fn set_owner(&mut self, owns_location: bool) {
        self.owns_location = owns_location;
    }
    /// Set faults absolutely, so repeating the command has the same effect.
    pub async fn set_faults(&self, faults: Faults) {
        self.state.lock().await.faults = faults;
    }
    async fn before(&self) -> Result<(), StorageError> {
        let (delay, failure) = {
            let mut state = self.state.lock().await;
            if std::mem::replace(&mut state.faults.expire_uploads, false) {
                state.uploads.clear();
            }
            let failure = if state.faults.fail_next > 0 {
                state.faults.fail_next -= 1;
                Some(state.faults.failure)
            } else {
                None
            };
            (state.faults.delay, failure)
        };
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        match failure {
            Some(failure) => Err(StorageError::Injected(failure)),
            None => {
                if self.tokens.lock().await.as_ref().is_some_and(|tokens| {
                    tokens
                        .expires_at
                        .is_some_and(|expiry| self.clock.now() >= expiry)
                }) {
                    return Err(StorageError::Injected(StorageFailure::Authentication));
                }
                Ok(())
            }
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
    async fn set_oauth_tokens(&self, tokens: OAuthTokens) -> Result<(), StorageError> {
        if !matches!(
            self.config.provider(),
            CloudProvider::GoogleDrive | CloudProvider::Dropbox | CloudProvider::OneDrive
        ) {
            return Err(StorageError::InvalidConfiguration(
                "provider does not use OAuth",
            ));
        }
        *self.tokens.lock().await = Some(tokens);
        Ok(())
    }
    fn single_request_limit(&self) -> u64 {
        16
    }
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        crate::transfer::upload_bytes(self, path, bytes, async {
            self.before().await?;
            let mut state = self.state.lock().await;
            if state.objects.contains_key(path) {
                return Err(StorageError::AlreadyExists);
            }
            state.objects.insert(
                path.clone(),
                Object {
                    upload_id: None,
                    bytes: bytes.to_vec(),
                    stored_at: self.clock.now(),
                },
            );
            Ok(())
        })
        .await
    }

    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        if !path.is_replaceable() {
            return Err(StorageError::InvalidPath);
        }
        crate::transfer::check_single_request(bytes.len() as u64, self.single_request_limit())?;
        self.before().await?;
        self.state.lock().await.objects.insert(
            path.clone(),
            Object {
                upload_id: None,
                bytes: bytes.to_vec(),
                stored_at: self.clock.now(),
            },
        );
        Ok(())
    }
    async fn read(&self, path: &ObjectPath) -> Result<Vec<u8>, StorageError> {
        self.before().await?;
        self.state
            .lock()
            .await
            .objects
            .get(path)
            .map(|object| object.bytes.clone())
            .ok_or(StorageError::NotFound)
    }
    async fn read_range(
        &self,
        path: &ObjectPath,
        range: ByteRange,
    ) -> Result<Vec<u8>, StorageError> {
        range.select(&self.read(path).await?)
    }
    async fn list(&self, prefix: &ObjectPrefix) -> Result<Vec<StoredObject>, StorageError> {
        self.before().await?;
        Ok(self
            .state
            .lock()
            .await
            .objects
            .iter()
            .filter(|(path, _)| prefix.contains(path))
            .map(|(path, object)| StoredObject {
                path: path.clone(),
                size: object.bytes.len() as u64,
                stored_at: object.stored_at,
            })
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
        if !self.owns_location {
            return Err(StorageError::NotStoreOwner);
        }
        self.state.lock().await.accounts.insert(account.into());
        let invitation = match self.config.provider() {
            CloudProvider::CloudKit => StorageInvitation::new(
                self.config(),
                crate::invitation::InvitationAcceptance::CloudKitShare {
                    url: coven_crypto::SecretText::new("https://icloud.com/share/memory".into()),
                },
            )?,
            _ => StorageInvitation::for_account(self.config())?,
        };
        Ok(AccessGrant::Granted { invitation })
    }
    async fn revoke_access(&self, member: &MemberAccess) -> Result<MemberRemoval, StorageError> {
        self.before().await?;
        match (self.config.provider(), member) {
            (CloudProvider::S3, MemberAccess::S3AccessKey { access_key_id }) => {
                Ok(MemberRemoval::DeleteAccessKey {
                    access_key_id: access_key_id.clone(),
                })
            }
            (
                CloudProvider::GoogleDrive
                | CloudProvider::Dropbox
                | CloudProvider::OneDrive
                | CloudProvider::CloudKit,
                MemberAccess::ProviderAccount(account),
            ) => {
                if !self.owns_location {
                    return Err(StorageError::NotStoreOwner);
                }
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
        let id = self.session_id(session)?;
        if session.is_complete() {
            return Ok(());
        }
        let state = self.state.lock().await;
        match state.uploads.get(&id) {
            Some(pending) => {
                pending.check(session)?;
                session.confirmed = pending.bytes.len() as u64;
            }
            None => confirm_published(&state, session, id)?,
        }
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
        let pending = state.uploads.get(&id).ok_or(StorageError::SessionExpired)?;
        pending.check(session)?;
        if pending.bytes.len() as u64 != session.confirmed {
            return Err(StorageError::InvalidPart);
        }
        if std::mem::replace(&mut state.faults.drop_part, false) {
            return Err(StorageError::Injected(StorageFailure::Network));
        }
        state
            .uploads
            .get_mut(&id)
            .ok_or(StorageError::SessionExpired)?
            .bytes
            .extend_from_slice(bytes);
        if std::mem::replace(&mut state.faults.lose_part_reply, false) {
            return Err(StorageError::Injected(StorageFailure::Network));
        }
        session.confirmed = end;
        Ok(())
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.before().await?;
        let id = self.session_id(session)?;
        if session.is_complete() {
            return Ok(());
        }
        let mut state = self.state.lock().await;
        let Some(pending) = state.uploads.get(&id) else {
            return confirm_published(&state, session, id);
        };
        pending.check(session)?;
        if pending.bytes.len() as u64 != session.total || session.confirmed != session.total {
            return Err(StorageError::InvalidPart);
        }
        if state.objects.contains_key(&session.path) {
            return Err(StorageError::AlreadyExists);
        }
        let pending = state
            .uploads
            .remove(&id)
            .ok_or(StorageError::SessionExpired)?;
        state.objects.insert(
            pending.path,
            Object {
                bytes: pending.bytes,
                stored_at: self.clock.now(),
                upload_id: Some(id),
            },
        );
        if std::mem::replace(&mut state.faults.lose_completion_reply, false) {
            return Err(StorageError::Injected(StorageFailure::Network));
        }
        session.state = SessionState::Memory { id, complete: true };
        Ok(())
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        self.before().await?;
        let id = self.session_id(session)?;
        if session.is_complete() {
            return Ok(());
        }
        let mut state = self.state.lock().await;
        if let Some(pending) = state.uploads.get(&id) {
            pending.check(session)?;
        }
        state.uploads.remove(&id);
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
            .into_iter()
            .map(|object| (object.path, object.size))
            .collect::<Vec<_>>()
            != [(path.clone(), data.len() as u64)]
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
