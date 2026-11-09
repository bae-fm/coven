//! An in-memory provider, fault injection and the shared provider conformance suite.
/// The store shared by the deterministic storage and format fixtures.
pub const STORE_ID: coven_foundation::id_source::StoreId =
    coven_foundation::id_source::StoreId(uuid::Uuid::from_bytes([0x10; 16]));
/// The device a test connection writes for, unless a test names another.
pub const DEVICE: coven_foundation::id_source::DeviceId =
    coven_foundation::id_source::DeviceId(4242);

use crate::session::SessionState;
use crate::*;
use async_trait::async_trait;
use coven_foundation::clock::ClockRef;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[path = "memory_upload.rs"]
mod memory_upload;
use memory_upload::Pending;

#[path = "memory_reads.rs"]
mod memory_reads;
pub use memory_reads::BodyFailure;
#[path = "memory_listing.rs"]
mod memory_listing;
#[path = "memory_requests.rs"]
mod memory_requests;
pub use memory_requests::MemoryRequest;

#[path = "memory_faults.rs"]
mod memory_faults;
#[path = "memory_objects.rs"]
mod memory_objects;
pub use memory_faults::{Faults, Garble};

struct Object {
    upload_id: Option<u64>,
    bytes: Arc<[u8]>,
    stored_at: coven_format::value::StorageTime,
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
            replace_token: self.revision.to_string(),
            checksum: None,
        }
    }
}
/// The account holding a fake store, as `account()` reports it to its owner.
const OWNER_ACCOUNT: &str = "owner@example.test";

#[derive(Clone)]
enum Account {
    Owner,
    Recipient(String),
}
enum AccountAccess {
    Invited,
    Joined,
}
struct MemoryState {
    location_marker: Option<Vec<u8>>,
    /// The store's folder's name, which `rename_folder` changes.
    folder_name: String,
    objects: memory_objects::MemoryObjects,
    uploads: BTreeMap<u64, Pending>,
    next: u64,
    accounts: BTreeMap<String, AccountAccess>,
    retained_access: BTreeMap<String, Vec<RetainedAccess>>,
    faults: Faults,
    /// The test's barrier and gate each create waits at, the gate each part
    /// waits at, and the gate each read waits at (`MemoryStorage::hold_creations`
    /// and the gates).
    creation_barrier: Option<Arc<tokio::sync::Barrier>>,
    creation_gate: Option<Arc<tokio::sync::Semaphore>>,
    part_gate: Option<Arc<tokio::sync::Semaphore>>,
    read_gate: Option<Arc<tokio::sync::Semaphore>>,
    ranges: Vec<ByteRange>,
    reads: Vec<(ObjectPath, u64, u64)>,
    /// The bytes reads of each object moved (`MemoryStorage::moved_bytes`).
    moved: BTreeMap<ObjectPath, u64>,
    received_buffers: Vec<usize>,
    sent_bytes: u64,
    largest_part: usize,
    held_listing: Option<HeldRequest>,
    held_read: Option<memory_reads::HeldRead>,
    held_body: Option<memory_reads::HeldBody>,
    held_creation: Option<HeldRequest>,
    held_request: Option<HeldNext>,
    held_access: Option<memory_requests::HeldAccess>,
    rejected_tokens: std::collections::BTreeSet<String>,
}

impl MemoryState {
    fn next_id(&mut self) -> Result<u64, StorageError> {
        let id = self.next;
        self.next = id
            .checked_add(1)
            .ok_or(StorageFailure::Protocol.with_source("memory ids exhausted"))?;
        Ok(id)
    }

    /// Answer a write that stored its object with a 429 while
    /// `refuse_after_storing` lasts.
    fn refused_after_storing(&mut self, provider: CloudProvider) -> Result<(), StorageError> {
        if self.faults.refuse_after_storing == 0 {
            return Ok(());
        }
        self.faults.refuse_after_storing -= 1;
        Err(crate::providers::ProviderResponse::injected(
            provider,
            StorageFailure::RateLimited,
            429,
            None,
        ))
    }
}

/// Hears each request this provider's connections attempt, counted from the
/// first, failed ones included (`MemoryStorage::subscribe_requests`).
pub struct RequestWatch(tokio::sync::watch::Receiver<u64>);

impl RequestWatch {
    /// How many requests have been attempted, which later waits count on
    /// from.
    pub fn current(&mut self) -> u64 {
        *self.0.borrow_and_update()
    }

    /// Resolves once another request is attempted; an error once the
    /// provider is gone.
    pub async fn changed(&mut self) -> Result<(), tokio::sync::watch::error::RecvError> {
        self.0.changed().await
    }

    /// Resolves once `reached` holds of the count of requests attempted; an
    /// error once the provider is gone first.
    pub async fn wait_for(
        &mut self,
        reached: impl FnMut(&u64) -> bool,
    ) -> Result<(), tokio::sync::watch::error::RecvError> {
        self.0.wait_for(reached).await.map(drop)
    }
}

/// The next request, of any kind or only `request`, held once it is
/// recorded.
struct HeldNext {
    request: Option<MemoryRequest>,
    started: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
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
/// A token named by [`Self::reject_access_token`] is refused as a provider 401
/// would be: the call refreshes once through its connection's custody and
/// retries once, as the HTTP adapters do (§20.2).
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
    /// Whether its sessions' parts are independent, as an S3 multipart
    /// upload's are, rather than taken in order.
    independent_parts: bool,
    /// Whether a read moves every part it touches whole, as CloudKit's do.
    whole_part_reads: bool,
    listing_page: usize,
    requests: Arc<tokio::sync::watch::Sender<u64>>,
    request_trace: Arc<std::sync::Mutex<Vec<MemoryRequest>>>,
    active_requests: Arc<std::sync::atomic::AtomicUsize>,
    peak_requests: Arc<std::sync::atomic::AtomicUsize>,
    account: Account,
    clock: ClockRef,
    time_resolution: Duration,
    tokens: Arc<Mutex<Option<Arc<crate::HeldTokens>>>>,
    s3_key: Arc<Mutex<Option<S3Credentials>>>,
    state: Arc<Mutex<MemoryState>>,
}
impl Clone for MemoryStorage {
    fn clone(&self) -> Self {
        Self::with_gate(
            self.provider.clone(),
            self.device,
            self.publications.clone(),
            self.cooldown.clone(),
            self.transfers.clone(),
            self.provider.clock.clone(),
        )
    }
}

/// Custody for tokens installed directly on a fake connection: a rejection
/// cannot be refreshed and reaches the caller as `Authentication`.
pub struct RefusedRefresh;
#[async_trait]
impl crate::TokenCustody for RefusedRefresh {
    async fn replace(&self, _: &OAuthTokens) -> Result<OAuthTokens, StorageError> {
        Err(StorageFailure::Authentication.with_source("this sign-in has no custody to refresh it"))
    }
}

/// Authority for raw provider fixtures, which seed objects without a sync owner.
pub struct UnrestrictedPublications;
impl PublicationGate for UnrestrictedPublications {
    fn check(&self, _: &ObjectPath) -> Result<(), StorageError> {
        Ok(())
    }
}

/// Admit `bytes` as `session`'s next part as a connection does with no
/// cooldown live and every destination open, for a fake [`Storage`] of a
/// test: the part must fit the session, and a plan's first part marks it.
pub fn admitted_part<'a>(
    session: &mut UploadSession,
    bytes: &'a [u8],
) -> Result<PartPermit<'a>, StorageError> {
    let part = session.next_part(bytes.len())?;
    let publication =
        PublicationRequest::new(Arc::new(UnrestrictedPublications), session.path().clone());
    Ok(PartPermit::admit(session, bytes, part, publication))
}

impl MemoryStorage {
    /// Compose a connection over `provider` as `from_provider` does, timing
    /// its requests by the provider's own clock.
    pub(crate) fn from_memory(provider: Arc<MemoryProvider>) -> Self {
        let clock = provider.clock.clone();
        Self::with_gate(
            provider,
            DEVICE,
            Arc::new(UnrestrictedPublications),
            Arc::new(crate::NoCooldown),
            Arc::new(crate::TransferBudget::new(crate::TransferLimits::default())),
            clock,
        )
    }
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
        Ok(Self::from_memory(Arc::new(MemoryProvider {
            config: owner.config(),
            online: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            single_limit: owner.provider.single_limit,
            part_size: owner.provider.part_size,
            independent_parts: owner.provider.independent_parts,
            whole_part_reads: owner.provider.whole_part_reads,
            listing_page: owner.provider.listing_page,
            requests: owner.provider.requests.clone(),
            request_trace: owner.provider.request_trace.clone(),
            active_requests: owner.provider.active_requests.clone(),
            peak_requests: owner.provider.peak_requests.clone(),
            account: Account::Recipient(email.to_ascii_lowercase()),
            clock: owner.provider.clock.clone(),
            time_resolution: owner.provider.time_resolution,
            tokens: Arc::new(Mutex::new(None)),
            s3_key: Arc::new(Mutex::new(None)),
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
        Self::from_memory(Arc::new(device))
    }
    /// Construct a candidate device connection whose credentials can be inspected
    /// independently from the source's credentials and durable provider objects.
    pub async fn connect_device(
        &self,
        config: StorageConfig,
        auth: crate::ProviderAuth,
    ) -> Result<Self, StorageError> {
        crate::ConnectionCredentials { location: config.clone(), credentials: auth.credentials() }
            .validate()?;
        if !config.same_location(&self.provider.config) {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("memory connector location mismatch"));
        }
        let mut connection = (*self.provider).clone();
        connection.config = config;
        // Candidate setup must not mutate the old adapter's sign-in. Network
        // controls and request observations still belong to this test device.
        connection.tokens = Arc::new(Mutex::new(None));
        connection.s3_key = Arc::new(Mutex::new(None));
        match auth {
            crate::ProviderAuth::OAuth { tokens, custody } => {
                *connection.tokens.lock().await =
                    Some(Arc::new(crate::HeldTokens::new(tokens, custody)));
            }
            crate::ProviderAuth::S3(keys) => connection.set_s3_credentials(keys).await?,
            crate::ProviderAuth::CloudKit => {}
        }
        Ok(Self::from_memory(Arc::new(connection)))
    }

    /// Set this connection's network availability without changing other devices.
    pub fn set_online(&self, online: bool) {
        self.provider.online.store(online, std::sync::atomic::Ordering::SeqCst);
    }
    /// Request notifications include failed attempts, so tests can wait without polling.
    pub fn subscribe_requests(&self) -> RequestWatch {
        RequestWatch(self.provider.requests.subscribe())
    }
    /// Greatest number of requests overlapping an injected delay or barrier.
    pub fn peak_requests(&self) -> usize {
        self.provider.peak_requests.load(std::sync::atomic::Ordering::SeqCst)
    }
    /// Reset the peak for subsequent request starts, retaining active requests.
    pub fn reset_request_peak(&self) {
        self.provider.peak_requests.store(0, std::sync::atomic::Ordering::SeqCst);
    }
    /// Every attempted provider request, including injected failures.
    pub fn request_count(&self) -> u64 {
        *self.provider.requests.borrow()
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
    /// The bytes the reads of `path` moved: each read's range, or, on a
    /// provider whose reads move whole parts, every part each read touched.
    pub async fn moved_bytes(&self, path: &ObjectPath) -> u64 {
        self.provider.state.lock().await.moved.get(path).copied().unwrap_or(0)
    }
    /// Part bytes with successful replies and their largest buffer, across sessions.
    pub async fn transferred(&self) -> (u64, usize) {
        let state = self.provider.state.lock().await;
        (state.sent_bytes, state.largest_part)
    }
    /// Damage one stored byte without issuing a provider request.
    pub async fn corrupt_byte(&self, path: &ObjectPath, offset: usize) -> Result<(), StorageError> {
        let mut state = self.provider.state.lock().await;
        let object = state.objects.get_mut(path).ok_or(StorageFailure::NotFound)?;
        let byte =
            Arc::make_mut(&mut object.bytes).get_mut(offset).ok_or(StorageFailure::InvalidRange)?;
        *byte ^= 1;
        Ok(())
    }
    /// Store an object without a request or this connection's size limits,
    /// as anyone holding the storage's credentials could (§3).
    pub async fn plant(&self, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError> {
        let stored_at = self.provider.storage_time()?;
        let mut state = self.provider.state.lock().await;
        let revision = state.next_id()?;
        state.objects.insert(
            path.clone(),
            Object { revision, upload_id: None, bytes: bytes.into(), stored_at },
        );
        Ok(())
    }
    /// Resolves once the provider holds an object at a path `wanted`
    /// accepts, with that path; no request is made. A request is in the
    /// trace as it goes out, before its object is stored, so the trace
    /// can't tell this.
    pub async fn wait_for_object(&self, wanted: impl Fn(&ObjectPath) -> bool) -> ObjectPath {
        let mut changes = self.provider.state.lock().await.objects.watch();
        loop {
            let state = self.provider.state.lock().await;
            if let Some(path) = state.objects.keys().find(|path| wanted(path)) {
                return path.clone();
            }
            drop(state);
            changes.changed().await;
        }
    }
    /// Refuse this access token from now on, on every connection, as a
    /// provider 401 would (§20.2).
    pub async fn reject_access_token(&self, token: &str) {
        self.provider.state.lock().await.rejected_tokens.insert(token.into());
    }
    /// Set faults absolutely, so repeating the command has the same effect.
    pub async fn set_faults(&self, faults: Faults) {
        self.provider.state.lock().await.faults = faults;
    }
    /// Hold each create, including injected failures, until all writers
    /// arrive at the test's `barrier`.
    pub async fn hold_creations(&self, barrier: Arc<tokio::sync::Barrier>) {
        self.provider.state.lock().await.creation_barrier = Some(barrier);
    }
    /// Hold each create until the test adds a permit to `gate`; each create
    /// takes one, oldest first, so a test releases held creates one at a time.
    pub async fn gate_creations(&self, gate: Arc<tokio::sync::Semaphore>) {
        self.provider.state.lock().await.creation_gate = Some(gate);
    }
    /// Hold each part, its bytes taken, until the test adds a permit to
    /// `gate`; each part takes one, oldest first.
    pub async fn gate_parts(&self, gate: Arc<tokio::sync::Semaphore>) {
        self.provider.state.lock().await.part_gate = Some(gate);
    }
    /// Hold each read until the test adds a permit to `gate`; each read takes
    /// one, oldest first.
    pub async fn gate_reads(&self, gate: Arc<tokio::sync::Semaphore>) {
        self.provider.state.lock().await.read_gate = Some(gate);
    }
    /// Whether the store is shared with `account`: granted and not since
    /// revoked.
    pub async fn shared_with(&self, account: &str) -> bool {
        self.provider.state.lock().await.accounts.contains_key(&account.to_ascii_lowercase())
    }
    /// Whether `account` has accepted the store's share, as a Dropbox mount
    /// does, and not since left it or lost it.
    pub async fn joined(&self, account: &str) -> bool {
        matches!(
            self.provider.state.lock().await.accounts.get(&account.to_ascii_lowercase()),
            Some(AccountAccess::Joined)
        )
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
    /// Refuse the upload of the file at `path` as it starts, while
    /// `Faults::refuse_file_uploads` counts refusals left, or panic in it
    /// while `Faults::panic_file_uploads` counts panics left; any other path
    /// goes through.
    async fn refuse_file_upload(&self, path: &ObjectPath) -> Result<(), StorageError> {
        if path.object_id().is_none() {
            return Ok(());
        }
        let mut state = self.state.lock().await;
        if state.faults.panic_file_uploads > 0 {
            state.faults.panic_file_uploads -= 1;
            drop(state);
            panic!("the upload of {path:?} panicked");
        }
        if state.faults.refuse_file_uploads == 0 {
            return Ok(());
        }
        state.faults.refuse_file_uploads -= 1;
        let faults = &state.faults;
        Err(self.injected((faults.failure, faults.response, faults.retry_after)))
    }
    fn session_id(&self, session: &UploadSession) -> Result<u64, StorageError> {
        match session.state {
            SessionState::Memory { id, .. } | SessionState::MemoryParts { id, .. } => Ok(id),
            _ => Err(StorageFailure::SessionMismatch.into()),
        }
    }
}

#[async_trait]
impl ProviderOps for MemoryProvider {
    async fn create_location_marker(
        &self,
        store: coven_foundation::id_source::StoreId,
        _device: coven_foundation::id_source::DeviceId,
    ) -> Result<(), StorageError> {
        self.before(MemoryRequest::Other("create_location_marker")).await?;
        let mut state = self.state.lock().await;
        if state.location_marker.is_some() {
            return Err(StorageFailure::AlreadyExists.into());
        }
        state.location_marker = Some(store.to_string().into_bytes());
        if std::mem::replace(&mut state.faults.lose_completion_reply, false) {
            return Err(StorageFailure::Network.into());
        }
        Ok(())
    }
    async fn read_location_marker(&self) -> Result<Vec<u8>, StorageError> {
        self.before(MemoryRequest::Other("read_location_marker")).await?;
        self.state.lock().await.location_marker.clone().ok_or(StorageFailure::NotFound.into())
    }
    /// A Dropbox shared folder's or a OneDrive folder's address is under
    /// `storage.example.test`, after its id.
    async fn folder_label(&self) -> Result<crate::FolderLabel, StorageError> {
        self.before(MemoryRequest::Other("folder_label")).await?;
        let id = match &self.config {
            StorageConfig::Dropbox {
                location: crate::DropboxLocation::SharedFolder { namespace_id },
                ..
            } => namespace_id,
            StorageConfig::OneDrive { folder_id, .. } => folder_id,
            _ => return Err(StorageFailure::ProviderUnavailable.into()),
        };
        let mut web_url = url::Url::parse("https://storage.example.test/folders/")
            .expect("a fixed address parses");
        web_url.path_segments_mut().expect("an https address holds paths").pop_if_empty().push(id);
        Ok(crate::FolderLabel { name: self.state.lock().await.folder_name.clone(), web_url })
    }
    async fn account(&self) -> Result<String, StorageError> {
        self.before_request(MemoryRequest::Other("account")).await?;
        Ok(match &self.account {
            Account::Owner => OWNER_ACCOUNT.into(),
            Account::Recipient(account) => account.clone(),
        })
    }
    async fn set_s3_credentials(&self, credentials: S3Credentials) -> Result<(), StorageError> {
        credentials.validate()?;
        if self.config.provider() != CloudProvider::S3 {
            return Err(StorageFailure::InvalidConfiguration.with_source("invalid S3 key"));
        }
        *self.s3_key.lock().await = Some(credentials);
        Ok(())
    }
    fn config(&self) -> StorageConfig {
        self.config.clone()
    }
    async fn set_oauth_tokens(&self, tokens: OAuthTokens) -> Result<(), StorageError> {
        if !self.config.provider().signs_in_with_oauth() {
            return Err(
                StorageFailure::InvalidConfiguration.with_source("provider does not use OAuth")
            );
        }
        let mut held = self.tokens.lock().await;
        match held.as_ref() {
            Some(held) => held.install(tokens).await,
            None => {
                *held = Some(Arc::new(crate::HeldTokens::new(tokens, Arc::new(RefusedRefresh))))
            }
        }
        Ok(())
    }
    fn single_request_limit(&self) -> u64 {
        self.single_limit
    }
    fn read_part_size(&self) -> Option<std::num::NonZeroU64> {
        std::num::NonZeroU64::new(self.part_size as u64).filter(|_| self.whole_part_reads)
    }
    #[expect(
        clippy::map_err_ignore,
        reason = "a send fails only when the receiver is gone, which this names"
    )]
    async fn create(
        &self,
        publication: &PublicationRequest,
        path: &ObjectPath,
        bytes: &[u8],
        sent: &ByteProgress,
        _device: coven_foundation::id_source::DeviceId,
    ) -> Result<Option<StoredObject>, StorageError> {
        publication.check()?;
        // The whole body is taken as the call starts; a held create waits
        // for its reply with every byte sent.
        sent.moved(bytes.len() as u64);
        self.before(MemoryRequest::Create(path.clone())).await?;
        self.refuse_file_upload(path).await?;
        let mut state = self.state.lock().await;
        if let Some((failure, status)) = state.faults.create_failures.get(path) {
            return Err(crate::providers::ProviderResponse::injected(
                self.config.provider(),
                *failure,
                *status,
                None,
            ));
        }
        let held = if state.held_creation.as_ref().is_some_and(|held| held.prefix.contains(path)) {
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
        let object = Object {
            revision,
            upload_id: None,
            bytes: bytes.into(),
            stored_at: self.storage_time()?,
        };
        let created = object.metadata(path, self.time_resolution);
        state.objects.insert(path.clone(), object);
        if std::mem::replace(&mut state.faults.lose_completion_reply, false) {
            return Err(StorageFailure::Network.into());
        }
        state.refused_after_storing(self.config.provider())?;
        Ok(Some(created))
    }

    async fn replace(
        &self,
        publication: &PublicationRequest,
        previous: &StoredObject,
        bytes: &[u8],
        _device: coven_foundation::id_source::DeviceId,
    ) -> Result<(), StorageError> {
        let path = &previous.path;
        publication.check()?;
        self.before(MemoryRequest::Replace(path.clone())).await?;
        let mut state = self.state.lock().await;
        if state
            .objects
            .get(path)
            .is_none_or(|object| object.revision.to_string() != previous.replace_token)
        {
            return Err(StorageFailure::Conflict.into());
        }
        if state.faults.refuse_replacements > 0 {
            state.faults.refuse_replacements -= 1;
            return Err(StorageFailure::Conflict.into());
        }
        let revision = state.next_id()?;
        state.objects.insert(
            path.clone(),
            Object {
                revision,
                upload_id: None,
                bytes: bytes.into(),
                stored_at: self.storage_time()?,
            },
        );
        if std::mem::replace(&mut state.faults.lose_completion_reply, false) {
            return Err(StorageFailure::Network.into());
        }
        state.refused_after_storing(self.config.provider())
    }
    async fn status(&self, path: &ObjectPath) -> Result<Option<StoredObject>, StorageError> {
        self.before(MemoryRequest::Status(path.clone())).await?;
        let state = self.state.lock().await;
        if let Some(failure) = state.faults.status_failures.get(path) {
            return Err((*failure).into());
        }
        Ok(state.objects.get(path).map(|object| object.metadata(path, self.time_resolution)))
    }
    async fn stream_object(
        &self,
        path: &ObjectPath,
        range: Option<ByteRange>,
        // The response body yields bytes as they arrive; the connection
        // hears its reads.
        _received: &ByteProgress,
    ) -> Result<ObjectStream<'_>, StorageError> {
        self.stream_recorded(path, range).await
    }

    #[expect(
        clippy::map_err_ignore,
        reason = "a send fails only when the receiver is gone, which this names"
    )]
    async fn list(&self, listing: &mut ObjectListing) -> Result<(), StorageError> {
        let prefix = listing.prefix();
        self.before(MemoryRequest::List(prefix.clone())).await?;
        let now = self.storage_time()?;
        let state = self.state.lock().await;
        if let Some(failure) = state.faults.list_failures.get(prefix) {
            return Err((*failure).into());
        }
        let objects: Vec<_> = state
            .objects
            .iter()
            .filter(|(path, object)| {
                !state.faults.hide_listings
                    && prefix.contains(path)
                    && state.faults.listing_lag.is_none_or(|lag| {
                        now.duration_since(object.stored_at).is_some_and(|age| age > lag)
                    })
            })
            .map(|(path, object)| object.metadata(path, self.time_resolution))
            .collect::<Vec<_>>();
        drop(state);
        self.further_pages(MemoryRequest::List(prefix.clone()), objects.len()).await?;
        let mut state = self.state.lock().await;
        let hold = if state.held_listing.as_ref().is_some_and(|held| &held.prefix == prefix) {
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
        let mut state = self.state.lock().await;
        if let Some(failure) = state.faults.delete_failures.get(path) {
            return Err((*failure).into());
        }
        state.objects.remove(path);
        Ok(())
    }
    async fn grant_access(&self, account: &str) -> Result<AccessGrant, StorageError> {
        self.hold_access(account, true).await;
        self.before_request(MemoryRequest::Other("grant_access")).await?;
        if self.config.provider() == CloudProvider::S3 {
            return Ok(AccessGrant::CreateAccessKey);
        }
        if !matches!(self.account, Account::Owner) {
            return Err(StorageFailure::NotStoreOwner.into());
        }
        if account.is_empty() {
            return Err(StorageFailure::InvalidConfiguration.with_source("empty sharing account"));
        }
        if account.eq_ignore_ascii_case(OWNER_ACCOUNT) {
            return Err(StorageFailure::InviteeIsStoreOwner.into());
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
            let access = state.accounts.get_mut(email).ok_or(StorageFailure::PermissionDenied)?;
            *access = AccountAccess::Joined;
        }
        Ok(())
    }
    /// As Dropbox's unmount, the only provider undo: a mounted recipient
    /// goes back to invited; one not joined, or no longer shared, is done.
    /// Other providers' acceptance stays, as theirs does.
    async fn leave(&self, invitation: &StorageInvitation) -> Result<(), StorageError> {
        self.before_request(MemoryRequest::Other("leave")).await?;
        use crate::invitation::InvitationAcceptance as Acceptance;
        if let (Acceptance::DropboxMount, Account::Recipient(email)) =
            (&invitation.acceptance, &self.account)
        {
            if let Some(access) = self.state.lock().await.accounts.get_mut(email) {
                *access = AccountAccess::Invited;
            }
        }
        Ok(())
    }
    async fn revoke_access(&self, account: &str) -> Result<Vec<RetainedAccess>, StorageError> {
        self.hold_access(account, false).await;
        self.before_request(MemoryRequest::Other("revoke_access")).await?;
        if self.config.provider() == CloudProvider::S3 {
            return Err(
                StorageFailure::InvalidConfiguration.with_source("S3 has no account sharing")
            );
        }
        if !matches!(self.account, Account::Owner) {
            return Err(StorageFailure::NotStoreOwner.into());
        }
        let mut state = self.state.lock().await;
        let account = account.to_ascii_lowercase();
        if let Some(shares) = state.retained_access.get(&account) {
            return Ok(shares.clone());
        }
        state.accounts.remove(&account);
        Ok(Vec::new())
    }
    async fn begin_upload(
        &self,
        publication: &PublicationRequest,
        target: &UploadTarget,
        total: u64,
        device: coven_foundation::id_source::DeviceId,
    ) -> Result<UploadSession, StorageError> {
        self.memory_begin_upload(publication, target, total, device).await
    }
    async fn device_uploads(
        &self,
        store: coven_foundation::id_source::StoreId,
        device: coven_foundation::id_source::DeviceId,
    ) -> Result<Vec<LeftUpload>, StorageError> {
        self.memory_device_uploads(store, device).await
    }
    async fn settled_claim_uploads(
        &self,
        store: coven_foundation::id_source::StoreId,
        through: u64,
    ) -> Result<Vec<LeftUpload>, StorageError> {
        self.memory_settled_claim_uploads(store, through).await
    }
    fn dates_floor_sessions(&self) -> bool {
        true
    }
    async fn superseded_floor_uploads(
        &self,
        floor: &StoredObject,
    ) -> Result<Vec<LeftUpload>, StorageError> {
        self.memory_superseded_floor_uploads(floor).await
    }
    async fn end_left_upload(&self, upload: &LeftUpload) -> Result<(), StorageError> {
        self.memory_end_left_upload(upload).await
    }
    async fn resume_upload(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        self.memory_resume_upload(publication, session).await
    }
    async fn upload_part(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
        bytes: &[u8],
        end: u64,
        sent: &ByteProgress,
    ) -> Result<(), StorageError> {
        self.memory_upload_part(publication, session, bytes, end, sent).await
    }
    async fn send_independent_part(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
        offset: u64,
        bytes: &[u8],
        sent: &ByteProgress,
    ) -> Result<crate::ConfirmedPart, StorageError> {
        self.memory_send_part(publication, session, offset, bytes, sent).await
    }
    async fn finish_upload(
        &self,
        publication: &PublicationRequest,
        session: &mut UploadSession,
    ) -> Result<(), StorageError> {
        self.memory_finish_upload(publication, session).await
    }
    async fn abort_upload(
        &self,
        publication: &PublicationRequest,
        session: &UploadSession,
    ) -> Result<(), StorageError> {
        self.memory_abort_upload(publication, session).await
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
