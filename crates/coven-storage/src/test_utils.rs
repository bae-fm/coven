//! An in-memory provider, fault injection and the shared provider conformance suite.
/// The store shared by the deterministic storage and format fixtures.
pub const STORE_ID: coven_foundation::id_source::StoreId =
    coven_foundation::id_source::StoreId(uuid::Uuid::from_bytes([0x10; 16]));

use crate::session::SessionState;
use crate::*;
use async_trait::async_trait;
use coven_foundation::clock::ClockRef;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[path = "memory_reads.rs"]
mod memory_reads;
pub use memory_reads::BodyFailure;
#[path = "memory_requests.rs"]
mod memory_requests;
#[path = "memory_writers.rs"]
mod memory_writers;
pub use memory_requests::MemoryRequest;
#[path = "memory_key_admin.rs"]
mod memory_key_admin;
pub use memory_key_admin::MemoryKeyAdmin;

/// Faults applied to the next calls, including accepted parts with lost replies.
#[derive(Clone)]
pub struct Faults {
    /// Fail this many calls before doing their work.
    pub fail_next: usize,
    /// Fail exact reads at these paths, retaining other in-flight responses.
    pub read_failures: BTreeMap<ObjectPath, StorageFailure>,
    /// Maximum bytes returned by each body read; nonzero.
    pub read_chunk_size: usize,
    /// Failures after metadata, independently of request-start failures.
    pub body_failures: BTreeMap<ObjectPath, BodyFailure>,
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
    /// Accept creates at occupied paths, violating the provider contract.
    pub overwrite_create: bool,
    /// Hide published objects from listings while exact reads and status still see them.
    pub hide_listings: bool,
    /// Hide objects no older than this lag; exact reads still see publication.
    pub listing_lag: Option<Duration>,
    /// Refuse a duplicate create but fail to abort its upload session.
    pub fail_duplicate_cleanup: bool,
}
impl Faults {
    /// No failures or delays.
    pub fn none() -> Self {
        Self {
            fail_next: 0,
            read_failures: BTreeMap::new(),
            read_chunk_size: 64 * 1024,
            body_failures: BTreeMap::new(),
            failure: StorageFailure::Network,
            delay: Duration::ZERO,
            drop_part: false,
            lose_part_reply: false,
            lose_completion_reply: false,
            expire_uploads: false,
            overwrite_create: false,
            fail_duplicate_cleanup: false,
            hide_listings: false,
            listing_lag: None,
        }
    }
}

struct Object {
    upload_id: Option<u64>,
    bytes: Arc<[u8]>,
    stored_at: std::time::SystemTime,
    revision: u64,
}
impl Object {
    fn metadata(&self, path: &ObjectPath, time_resolution: Duration) -> StoredObject {
        StoredObject {
            path: path.clone(),
            size: self.bytes.len() as u64,
            stored_at: self.stored_at,
            time_resolution,
            object_id: path.as_str().into(),
            revision: self.revision.to_string(),
            checksum: None,
        }
    }
}
struct Pending {
    path: ObjectPath,
    total: u64,
    bytes: Vec<u8>,
    part_size: usize,
}
impl Pending {
    fn check(&self, session: &UploadSession) -> Result<(), StorageError> {
        if self.path != session.path
            || self.total != session.total
            || self.part_size != session.part_size
        {
            return Err(StorageFailure::SessionMismatch.into());
        }
        if (self.bytes.len() as u64) < session.confirmed || self.bytes.len() as u64 > self.total {
            return Err(
                StorageFailure::Protocol.with_source("memory provider lost confirmed progress")
            );
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
        .ok_or(StorageFailure::SessionExpired)?;
    if object.upload_id != Some(id) || object.bytes.len() as u64 != session.total {
        return Err(StorageFailure::AlreadyExists.into());
    }
    session.confirmed = session.total;
    session.state = SessionState::Memory { id, complete: true };
    Ok(())
}

#[derive(Clone)]
enum Account {
    Owner,
    Recipient(String),
}
enum AccountAccess {
    Invited,
    Joined,
}
struct State {
    objects: BTreeMap<ObjectPath, Object>,
    uploads: BTreeMap<u64, Pending>,
    next: u64,
    accounts: BTreeMap<String, AccountAccess>,
    retained_access: BTreeMap<String, Vec<RetainedAccess>>,
    faults: Faults,
    ranges: Vec<ByteRange>,
    reads: Vec<(ObjectPath, u64, u64)>,
    received_buffers: Vec<usize>,
    sent_bytes: u64,
    largest_part: usize,
    held_listing: Option<HeldRequest>,
    held_read: Option<memory_reads::HeldRead>,
    held_creation: Option<HeldRequest>,
}

impl State {
    fn next_id(&mut self) -> Result<u64, StorageError> {
        let id = self.next;
        self.next = id
            .checked_add(1)
            .ok_or(StorageFailure::Protocol.with_source("memory ids exhausted"))?;
        Ok(id)
    }
}

struct HeldRequest {
    prefix: ObjectPrefix,
    listed: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
}

/// Clones share a provider's durable objects and sessions across simulated crashes.
/// Publication is timestamped by the injected clock and retains the upload's
/// identity after pending parts are discarded. [`Faults`] can lose part/completion
/// replies or expire pending sessions without losing published objects.
///
/// [`Self::for_recipient`] shares the backend with a separate account and sign-in.
/// Grants and acceptance govern that account's reads, writes and uploads; revoking
/// it leaves the owner and other recipients untouched. Clones share their account's
/// tokens, and replacement tokens govern subsequent calls on the same adapter.
/// S3 console keys are outside this account-sharing model.
/// Implements [`crate::providers::StorageConnector`] so application tests can
/// exercise saved settings and credential custody through the `test-utils`
/// builder's connector hook.
pub type MemoryStorage = StorageConnection<MemoryProvider>;

/// In-memory native calls used beneath the shared storage connection.
#[derive(Clone)]
pub struct MemoryProvider {
    config: StorageConfig,
    online: Arc<std::sync::atomic::AtomicBool>,
    single_limit: u64,
    part_size: usize,
    requests: Arc<tokio::sync::watch::Sender<u64>>,
    request_trace: Arc<std::sync::Mutex<Vec<MemoryRequest>>>,
    active_requests: Arc<std::sync::atomic::AtomicUsize>,
    peak_requests: Arc<std::sync::atomic::AtomicUsize>,
    account: Account,
    clock: ClockRef,
    time_resolution: Duration,
    tokens: Arc<Mutex<Option<OAuthTokens>>>,
    s3_key: Arc<Mutex<Option<S3Credentials>>>,
    key_admin: MemoryKeyAdmin,
    state: Arc<Mutex<State>>,
}
impl Clone for MemoryStorage {
    fn clone(&self) -> Self {
        Self::from_provider(self.provider.clone())
    }
}

impl MemoryStorage {
    /// An S3 test location at the Unix epoch, with four-MiB single requests
    /// and 64-KiB parts. Override only the choices relevant to the scenario.
    pub fn builder() -> MemoryStorageBuilder {
        MemoryStorageBuilder::new()
    }
    /// A separate non-owner account using the same provider location. Its sign-in
    /// is independent; access requires a grant and, where needed, recipient joining.
    /// S3 keys are administered outside the fake's account-sharing model.
    pub fn for_recipient(owner: &Self, email: &str) -> Result<Self, StorageError> {
        if owner.provider.config.provider() == CloudProvider::S3 || email.is_empty() {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("recipient requires a sharing account"));
        }
        Ok(Self::from_provider(Arc::new(MemoryProvider {
            config: owner.config(),
            online: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            single_limit: owner.provider.single_limit,
            part_size: owner.provider.part_size,
            requests: owner.provider.requests.clone(),
            request_trace: owner.provider.request_trace.clone(),
            active_requests: owner.provider.active_requests.clone(),
            peak_requests: owner.provider.peak_requests.clone(),
            account: Account::Recipient(email.to_ascii_lowercase()),
            clock: owner.provider.clock.clone(),
            time_resolution: owner.provider.time_resolution,
            tokens: Arc::new(Mutex::new(None)),
            s3_key: Arc::new(Mutex::new(None)),
            key_admin: owner.provider.key_admin.clone(),
            state: owner.provider.state.clone(),
        })))
    }
    /// Another device connected to the same durable backend, with independent
    /// credentials, request notifications and network availability.
    pub fn for_device(&self) -> Self {
        let mut device = (*self.provider).clone();
        device.online = Arc::new(std::sync::atomic::AtomicBool::new(true));
        device.tokens = Arc::new(Mutex::new(None));
        device.s3_key = Arc::new(Mutex::new(None));
        device.requests = Arc::new(tokio::sync::watch::channel(0).0);
        device.request_trace = Arc::new(std::sync::Mutex::new(Vec::new()));
        device.active_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        device.peak_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        Self::from_provider(Arc::new(device))
    }
    /// Construct a candidate device connection whose credentials can be inspected
    /// independently from the source's credentials and durable provider objects.
    pub async fn connect_device(
        &self,
        config: StorageConfig,
        credentials: StorageCredentials,
    ) -> Result<Self, StorageError> {
        crate::ConnectionCredentials {
            location: config.clone(),
            credentials: credentials.clone(),
        }
        .validate()?;
        if config != self.provider.config {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("memory connector location mismatch"));
        }
        let mut connection = (*self.provider).clone();
        // Candidate setup must not mutate the old adapter's sign-in. Network
        // controls and request observations still belong to this test device.
        connection.tokens = Arc::new(Mutex::new(None));
        connection.s3_key = Arc::new(Mutex::new(None));
        match credentials {
            StorageCredentials::OAuth(tokens) => connection.set_oauth_tokens(tokens).await?,
            StorageCredentials::S3(keys) => connection.set_s3_credentials(keys).await?,
            StorageCredentials::CloudKit => {}
        }
        Ok(Self::from_provider(Arc::new(connection)))
    }

    /// Erase the provider type while retaining this exact connection's credentials.
    pub fn shared_connection(&self) -> Arc<StorageConnection> {
        Arc::new(StorageConnection::from_provider(
            self.provider.clone() as Arc<dyn ProviderOps>
        ))
    }

    /// Set this connection's network availability without changing other devices.
    pub fn set_online(&self, online: bool) {
        self.provider
            .online
            .store(online, std::sync::atomic::Ordering::SeqCst);
    }
    /// Request notifications include failed attempts, so tests can wait without polling.
    pub fn subscribe_requests(&self) -> tokio::sync::watch::Receiver<u64> {
        self.provider.requests.subscribe()
    }
    /// Greatest number of requests overlapping the injected network delay.
    pub fn peak_requests(&self) -> usize {
        self.provider
            .peak_requests
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    /// Reset the peak for subsequent request starts, retaining active requests.
    pub fn reset_request_peak(&self) {
        self.provider
            .peak_requests
            .store(0, std::sync::atomic::Ordering::SeqCst);
    }
    /// Every attempted provider request, including injected failures.
    pub fn request_count(&self) -> u64 {
        *self.provider.requests.borrow()
    }
    /// The account's key administration that admin access reaches (§4).
    pub fn key_admin(&self) -> MemoryKeyAdmin {
        self.provider.key_admin.clone()
    }
    /// The key currently installed on this simulated provider connection.
    pub async fn s3_credentials(&self) -> Option<S3Credentials> {
        self.provider.s3_key.lock().await.clone()
    }
    /// The most recently installed S3 access-key id, without exposing its secret.
    pub async fn s3_access_key_id(&self) -> Option<String> {
        self.s3_credentials().await.map(|key| key.access_key_id)
    }
    /// Opened ranged responses, in order.
    pub async fn ranges(&self) -> Vec<ByteRange> {
        self.provider.state.lock().await.ranges.clone()
    }
    /// Opened responses as (path, offset, declared byte count), including whole objects.
    pub async fn reads(&self) -> Vec<(ObjectPath, u64, u64)> {
        self.provider.state.lock().await.reads.clone()
    }
    /// Part bytes with successful replies and their largest buffer, across sessions.
    pub async fn transferred(&self) -> (u64, usize) {
        let state = self.provider.state.lock().await;
        (state.sent_bytes, state.largest_part)
    }
    /// Damage one stored byte without issuing a provider request.
    pub async fn corrupt_byte(&self, path: &ObjectPath, offset: usize) -> Result<(), StorageError> {
        let mut state = self.provider.state.lock().await;
        let object = state
            .objects
            .get_mut(path)
            .ok_or(StorageFailure::NotFound)?;
        let byte = Arc::make_mut(&mut object.bytes)
            .get_mut(offset)
            .ok_or(StorageFailure::InvalidRange)?;
        *byte ^= 1;
        Ok(())
    }
    /// Set faults absolutely, so repeating the command has the same effect.
    pub async fn set_faults(&self, faults: Faults) {
        self.provider.state.lock().await.faults = faults;
    }
    /// Model native grants that remain until the owner changes them in the provider.
    pub async fn set_retained_access(&self, account: &str, shares: Vec<RetainedAccess>) {
        let mut state = self.provider.state.lock().await;
        let account = account.to_ascii_lowercase();
        if shares.is_empty() {
            state.retained_access.remove(&account);
        } else {
            state.retained_access.insert(account, shares);
        }
    }
}

impl MemoryProvider {
    async fn before_request(&self, request: MemoryRequest) -> Result<(), StorageError> {
        let active = self
            .active_requests
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let _request = ActiveMemoryRequest(&self.active_requests);
        self.peak_requests
            .fetch_max(active, std::sync::atomic::Ordering::SeqCst);
        self.request_trace
            .lock()
            .expect("request trace poisoned")
            .push(request);
        self.requests.send_modify(|count| *count += 1);
        if !self.online.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(StorageFailure::Network
                .with_source(std::io::Error::from(std::io::ErrorKind::NotConnected)));
        }
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
            self.clock.sleep(delay).await;
        }
        match failure {
            Some(failure) => Err(StorageError::Failure(failure)),
            None => {
                if self.tokens.lock().await.as_ref().is_some_and(|tokens| {
                    tokens
                        .expires_at
                        .is_some_and(|expiry| self.clock.now() >= expiry)
                }) {
                    return Err(StorageFailure::Authentication.into());
                }
                Ok(())
            }
        }
    }
    async fn before(&self, request: MemoryRequest) -> Result<(), StorageError> {
        self.before_request(request).await?;
        if let Account::Recipient(email) = &self.account {
            let state = self.state.lock().await;
            let allowed = match state.accounts.get(email) {
                Some(AccountAccess::Joined) => true,
                Some(AccountAccess::Invited) => {
                    self.config.provider() == CloudProvider::GoogleDrive
                }
                None => false,
            };
            if !allowed {
                return Err(StorageFailure::PermissionDenied.into());
            }
        }
        Ok(())
    }
    fn session_id(&self, session: &UploadSession) -> Result<u64, StorageError> {
        match session.state {
            SessionState::Memory { id, .. } => Ok(id),
            _ => Err(StorageFailure::SessionMismatch.into()),
        }
    }
}

struct ActiveMemoryRequest<'a>(&'a std::sync::atomic::AtomicUsize);
impl Drop for ActiveMemoryRequest<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait]
impl ProviderOps for MemoryProvider {
    async fn account(&self) -> Result<String, StorageError> {
        self.before(MemoryRequest::Other("account")).await?;
        Ok(match &self.account {
            Account::Owner => "owner@example.test".into(),
            Account::Recipient(account) => account.clone(),
        })
    }
    async fn set_s3_credentials(&self, credentials: S3Credentials) -> Result<(), StorageError> {
        if self.config.provider() != CloudProvider::S3
            || credentials.access_key_id.is_empty()
            || credentials.secret_access_key.as_str().is_empty()
        {
            return Err(StorageFailure::InvalidConfiguration.with_source("invalid S3 key"));
        }
        *self.s3_key.lock().await = Some(credentials);
        Ok(())
    }
    fn config(&self) -> StorageConfig {
        self.config.clone()
    }
    async fn set_oauth_tokens(&self, tokens: OAuthTokens) -> Result<(), StorageError> {
        if !matches!(
            self.config.provider(),
            CloudProvider::GoogleDrive | CloudProvider::Dropbox | CloudProvider::OneDrive
        ) {
            return Err(
                StorageFailure::InvalidConfiguration.with_source("provider does not use OAuth")
            );
        }
        *self.tokens.lock().await = Some(tokens);
        Ok(())
    }
    fn single_request_limit(&self) -> u64 {
        self.single_limit
    }
    async fn create(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.before(MemoryRequest::Create(path.clone())).await?;
        let mut state = self.state.lock().await;
        let held = if state
            .held_creation
            .as_ref()
            .is_some_and(|held| held.prefix.contains(path))
        {
            state.held_creation.take()
        } else {
            None
        };
        drop(state);
        if let Some(held) = held {
            held.listed
                .send(())
                .map_err(|_| StorageFailure::Protocol.with_source("creation observer dropped"))?;
            held.resume
                .await
                .map_err(|_| StorageFailure::Protocol.with_source("creation release dropped"))?;
        }
        let mut state = self.state.lock().await;
        if state.objects.contains_key(path) && !state.faults.overwrite_create {
            if state.faults.fail_duplicate_cleanup {
                return Err(StorageError::Cleanup {
                    operation: Box::new(StorageError::Failure(StorageFailure::AlreadyExists)),
                    cleanup: Box::new(StorageError::Failure(StorageFailure::Network)),
                });
            }
            return Err(StorageFailure::AlreadyExists.into());
        }
        let revision = state.next_id()?;
        state.objects.insert(
            path.clone(),
            Object {
                revision,
                upload_id: None,
                bytes: bytes.into(),
                stored_at: self.clock.now(),
            },
        );
        if std::mem::replace(&mut state.faults.lose_completion_reply, false) {
            return Err(StorageFailure::Network.into());
        }
        Ok(())
    }

    async fn replace(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        self.before(MemoryRequest::Replace(path.clone())).await?;
        let mut state = self.state.lock().await;
        let revision = state.next_id()?;
        state.objects.insert(
            path.clone(),
            Object {
                revision,
                upload_id: None,
                bytes: bytes.into(),
                stored_at: self.clock.now(),
            },
        );
        Ok(())
    }
    async fn status(&self, path: &ObjectPath) -> Result<Option<StoredObject>, StorageError> {
        self.before(MemoryRequest::Status(path.clone())).await?;
        Ok(self
            .state
            .lock()
            .await
            .objects
            .get(path)
            .map(|object| object.metadata(path, self.time_resolution)))
    }
    async fn stream_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
    ) -> Result<ObjectStream<'_>, StorageError> {
        self.stream_recorded(path, range).await
    }

    async fn list_writers(
        &self,
        store: coven_foundation::id_source::StoreId,
    ) -> Result<Vec<coven_foundation::id_source::DeviceId>, StorageError> {
        self.writer_folders(store).await
    }
    async fn list(&self, listing: &mut ObjectListing) -> Result<(), StorageError> {
        let prefix = listing.prefix();
        self.before(MemoryRequest::List(prefix.clone())).await?;
        let now = self.clock.now();
        let mut state = self.state.lock().await;
        let objects: Vec<_> = state
            .objects
            .iter()
            .filter(|(path, object)| {
                !state.faults.hide_listings
                    && prefix.contains(path)
                    && state.faults.listing_lag.is_none_or(|lag| {
                        now.duration_since(object.stored_at)
                            .is_ok_and(|age| age > lag)
                    })
            })
            .map(|(path, object)| object.metadata(path, self.time_resolution))
            .collect();
        let hold = if state
            .held_listing
            .as_ref()
            .is_some_and(|held| &held.prefix == prefix)
        {
            state.held_listing.take()
        } else {
            None
        };
        drop(state);
        if let Some(held) = hold {
            held.listed
                .send(())
                .map_err(|_| StorageFailure::Protocol.with_source("listing observer dropped"))?;
            held.resume
                .await
                .map_err(|_| StorageFailure::Protocol.with_source("listing release dropped"))?;
        }
        for object in objects {
            listing.insert(object)?;
        }
        Ok(())
    }
    async fn delete(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.before(MemoryRequest::Delete(path.clone())).await?;
        self.state.lock().await.objects.remove(path);
        Ok(())
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        self.before_request(MemoryRequest::Other("grant_access"))
            .await?;
        if self.config.provider() == CloudProvider::S3 {
            return Ok(AccessGrant::CreateAccessKey);
        }
        if !matches!(self.account, Account::Owner) {
            return Err(StorageFailure::NotStoreOwner.into());
        }
        if account.is_empty() {
            return Err(StorageFailure::InvalidConfiguration.with_source("empty sharing account"));
        }
        let invitation = match self.config.provider() {
            CloudProvider::CloudKit => StorageInvitation::new(
                self.config(),
                crate::invitation::InvitationAcceptance::CloudKitShare {
                    url: coven_crypto::SecretText::new("https://icloud.com/share/memory".into()),
                },
            )?,
            CloudProvider::OneDrive => StorageInvitation::new(
                self.config(),
                crate::invitation::InvitationAcceptance::OneDriveShare {
                    token: coven_crypto::SecretText::new(account.to_ascii_lowercase()),
                },
            )?,
            _ => StorageInvitation::for_account(self.config())?,
        };
        self.state
            .lock()
            .await
            .accounts
            .entry(account.to_ascii_lowercase())
            .or_insert(AccountAccess::Invited);
        Ok(AccessGrant::Granted { invitation })
    }
    async fn join(&self, invitation: &StorageInvitation) -> Result<(), StorageError> {
        self.before_request(MemoryRequest::Other("join")).await?;
        use crate::invitation::InvitationAcceptance as Acceptance;
        if matches!(invitation.acceptance, Acceptance::Granted) {
            return Ok(());
        }
        if let Acceptance::CloudKitShare { url } = &invitation.acceptance {
            if url.as_str() != "https://icloud.com/share/memory" {
                return Err(StorageFailure::InvitationMismatch.into());
            }
        }
        if let Account::Recipient(email) = &self.account {
            match &invitation.acceptance {
                Acceptance::OneDriveShare { token } if token.as_str() != email => {
                    return Err(StorageFailure::InvitationMismatch.into())
                }
                _ => {}
            }
            let mut state = self.state.lock().await;
            let access = state
                .accounts
                .get_mut(email)
                .ok_or(StorageFailure::PermissionDenied)?;
            *access = AccountAccess::Joined;
        }
        Ok(())
    }
    async fn revoke_access(
        &self,
        member: &MemberAccess,
    ) -> Result<ProviderRevocation, StorageError> {
        self.before_request(MemoryRequest::Other("revoke_access"))
            .await?;
        match (self.config.provider(), member) {
            (CloudProvider::S3, MemberAccess::S3AccessKey { access_key_id, .. }) => Ok(
                ProviderRevocation::Reported(MemberRemoval::DeleteAccessKey {
                    access_key_id: access_key_id.clone(),
                }),
            ),
            (
                CloudProvider::GoogleDrive
                | CloudProvider::Dropbox
                | CloudProvider::OneDrive
                | CloudProvider::CloudKit,
                MemberAccess::ProviderAccount(account),
            ) => {
                if !matches!(self.account, Account::Owner) {
                    return Err(StorageFailure::NotStoreOwner.into());
                }
                let mut state = self.state.lock().await;
                let account = account.to_ascii_lowercase();
                if let Some(shares) = state.retained_access.get(&account) {
                    return Ok(ProviderRevocation::Remaining(shares.clone()));
                }
                state.accounts.remove(&account);
                Ok(ProviderRevocation::Remaining(Vec::new()))
            }
            _ => Err(StorageFailure::InvalidConfiguration.with_source("wrong member access kind")),
        }
    }
    async fn begin_upload(
        &self,
        path: &ObjectPath,
        total: u64,
    ) -> Result<UploadSession, StorageError> {
        self.before(MemoryRequest::Other("begin_upload")).await?;
        let mut state = self.state.lock().await;
        if state.objects.contains_key(path) && !state.faults.overwrite_create {
            return Err(StorageFailure::AlreadyExists.into());
        }
        let id = state.next_id()?;
        state.uploads.insert(
            id,
            Pending {
                path: path.clone(),
                total,
                bytes: Vec::new(),
                part_size: self.part_size,
            },
        );
        Ok(UploadSession {
            location: self.config(),
            path: path.clone(),
            total,
            confirmed: 0,
            part_size: self.part_size,
            state: SessionState::Memory {
                id,
                complete: false,
            },
        })
    }
    async fn resume_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.before(MemoryRequest::Other("resume_upload")).await?;
        let id = self.session_id(session)?;
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
        end: u64,
    ) -> Result<(), StorageError> {
        self.before(MemoryRequest::Other("upload_part")).await?;
        let id = self.session_id(session)?;
        let mut state = self.state.lock().await;
        let pending = state
            .uploads
            .get(&id)
            .ok_or(StorageFailure::SessionExpired)?;
        pending.check(session)?;
        if pending.bytes.len() as u64 != session.confirmed {
            return Err(StorageFailure::InvalidPart.into());
        }
        if std::mem::replace(&mut state.faults.drop_part, false) {
            return Err(StorageFailure::Network.into());
        }
        state
            .uploads
            .get_mut(&id)
            .ok_or(StorageFailure::SessionExpired)?
            .bytes
            .extend_from_slice(bytes);
        if std::mem::replace(&mut state.faults.lose_part_reply, false) {
            return Err(StorageFailure::Network.into());
        }
        state.sent_bytes += bytes.len() as u64;
        state.largest_part = state.largest_part.max(bytes.len());
        session.confirmed = end;
        Ok(())
    }
    async fn finish_upload(&self, session: &mut UploadSession) -> Result<(), StorageError> {
        self.before(MemoryRequest::Other("finish_upload")).await?;
        let id = self.session_id(session)?;
        let mut state = self.state.lock().await;
        let Some(pending) = state.uploads.get(&id) else {
            return confirm_published(&state, session, id);
        };
        pending.check(session)?;
        if pending.bytes.len() as u64 != session.total || session.confirmed != session.total {
            return Err(StorageFailure::InvalidPart.into());
        }
        if state.objects.contains_key(&session.path) && !state.faults.overwrite_create {
            return Err(StorageFailure::AlreadyExists.into());
        }
        let revision = state.next_id()?;
        let pending = state
            .uploads
            .remove(&id)
            .ok_or(StorageFailure::SessionExpired)?;
        state.objects.insert(
            pending.path,
            Object {
                revision,
                bytes: pending.bytes.into(),
                stored_at: self.clock.now(),
                upload_id: Some(id),
            },
        );
        if std::mem::replace(&mut state.faults.lose_completion_reply, false) {
            return Err(StorageFailure::Network.into());
        }
        session.state = SessionState::Memory { id, complete: true };
        Ok(())
    }
    async fn abort_upload(&self, session: &UploadSession) -> Result<(), StorageError> {
        self.before(MemoryRequest::Other("abort_upload")).await?;
        let id = self.session_id(session)?;
        let mut state = self.state.lock().await;
        if let Some(pending) = state.uploads.get(&id) {
            pending.check(session)?;
        }
        state.uploads.remove(&id);
        Ok(())
    }
}

#[path = "conformance.rs"]
mod conformance;
pub use conformance::Conformance;

#[cfg(test)]
#[path = "test_utils_tests.rs"]
mod tests;

#[path = "memory_connector.rs"]
mod connector;
pub use connector::{conforming_detection, DETECTED_S3_SETTINGS};

#[path = "memory_storage_builder.rs"]
mod builder;
pub use builder::MemoryStorageBuilder;
