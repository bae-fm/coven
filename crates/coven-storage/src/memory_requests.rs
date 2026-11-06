use super::memory_upload::MemoryStart;
use super::*;

/// Each attempted native call in the in-memory provider, before fault injection.
/// This provider has one page per listing and no implicit metadata lookups.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemoryRequest {
    /// Read a whole object or an explicit byte range (including missing paths).
    Read(ObjectPath, Option<ByteRange>),
    /// Enumerate one prefix in one native call.
    List(ObjectPrefix),
    /// Publish an immutable object.
    Create(ObjectPath),
    /// Replace an object.
    Replace(ObjectPath),
    /// Query exact metadata.
    Status(ObjectPath),
    /// Delete an object.
    Delete(ObjectPath),
    /// Account, permission or multipart control/part call.
    Other(&'static str),
}

impl MemoryStorage {
    /// Attempted calls on this connection; `for_device` starts a separate trace.
    pub fn request_trace(&self) -> Vec<MemoryRequest> {
        self.provider.request_trace.lock().expect("request trace poisoned").clone()
    }
}

impl MemoryStorage {
    /// Hold one listing's reply after taking its snapshot. The test is notified
    /// when the result is fixed, and can mutate remote state before releasing it.
    pub async fn hold_next_listing(
        &self,
        prefix: ObjectPrefix,
        listed: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut state = self.provider.state.lock().await;
        assert!(state.held_listing.is_none(), "a listing is already held");
        state.held_listing = Some(HeldRequest { prefix, listed, resume });
    }
    /// Hold the next request, whatever it is, once it is recorded: tell
    /// `started`, then wait for `resume`.
    pub async fn hold_next_request(
        &self,
        started: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut state = self.provider.state.lock().await;
        assert!(state.held_request.is_none(), "a request is already held");
        state.held_request = Some(HeldNext { request: None, started, resume });
    }
    /// Hold the next request that is `request` once it is recorded, as
    /// [`Self::hold_next_request`] does; every other request goes through.
    pub async fn hold_next_request_of(
        &self,
        request: MemoryRequest,
        started: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut state = self.provider.state.lock().await;
        assert!(state.held_request.is_none(), "a request is already held");
        state.held_request = Some(HeldNext { request: Some(request), started, resume });
    }
    /// Hold a single-request creation before publication, notifying its observer.
    pub async fn hold_next_creation(
        &self,
        prefix: ObjectPrefix,
        started: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut state = self.provider.state.lock().await;
        assert!(state.held_creation.is_none(), "a creation is already held");
        state.held_creation = Some(HeldRequest { prefix, listed: started, resume });
    }
}

/// A provider permission request held before changing its durable grant.
pub(super) struct HeldAccess {
    account: String,
    grant: bool,
    started: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
}

impl MemoryStorage {
    /// Hold one account request before it reaches the provider's grant state.
    pub async fn hold_next_access(
        &self,
        account: &str,
        grant: bool,
        started: tokio::sync::oneshot::Sender<()>,
        resume: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut state = self.provider.state.lock().await;
        assert!(state.held_access.is_none(), "an access request is already held");
        state.held_access = Some(HeldAccess { account: account.into(), grant, started, resume });
    }
}

impl MemoryProvider {
    #[expect(
        clippy::let_underscore_must_use,
        reason = "a send fails only when the receiver is gone, which this names"
    )]
    pub(super) async fn hold_access(&self, account: &str, grant: bool) {
        let held = {
            let mut state = self.state.lock().await;
            if state.held_access.as_ref().is_some_and(|h| h.account == account && h.grant == grant)
            {
                state.held_access.take()
            } else {
                None
            }
        };
        if let Some(held) = held {
            let _ = held.started.send(());
            held.resume.await.expect("held access request was not released");
        }
    }
}

impl MemoryProvider {
    pub(super) async fn before_request(&self, request: MemoryRequest) -> Result<(), StorageError> {
        self.request_going(request, None).await
    }
    /// Record `request`, hold it if a test asked, then answer it as `Faults`
    /// say. A planned session's `start` is checked against the publication
    /// gate past the hold, as S3 checks it as the request goes out, and goes
    /// out once that check passes (Appendix E.10).
    async fn request_going(
        &self,
        request: MemoryRequest,
        start: Option<MemoryStart<'_>>,
    ) -> Result<(), StorageError> {
        let active = self.active_requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let _request = ActiveMemoryRequest(&self.active_requests);
        self.peak_requests.fetch_max(active, std::sync::atomic::Ordering::SeqCst);
        self.request_trace.lock().expect("request trace poisoned").push(request.clone());
        self.requests.send_modify(|count| *count += 1);
        let held = {
            let mut state = self.state.lock().await;
            match &state.held_request {
                Some(held) if held.request.as_ref().is_none_or(|kind| *kind == request) => {
                    state.held_request.take()
                }
                _ => None,
            }
        };
        if let Some(held) = held {
            held.started
                .send(())
                .map_err(|()| StorageFailure::Protocol.with_source("request observer dropped"))?;
            held.resume.await.map_err(|closed| StorageFailure::Protocol.with_source(closed))?;
        }
        if let Some(start) = start {
            start.go()?;
        }
        if !self.online.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(StorageFailure::Network
                .with_source(std::io::Error::from(std::io::ErrorKind::NotConnected)));
        }
        let (delay, failure, barrier) = {
            let mut state = self.state.lock().await;
            if std::mem::replace(&mut state.faults.expire_uploads, false) {
                state.uploads.clear();
            }
            let failure = if state.faults.fail_next > 0 {
                state.faults.fail_next -= 1;
                Some((state.faults.failure, state.faults.response, state.faults.retry_after))
            } else {
                None
            };
            let (barrier, gate) = match &request {
                MemoryRequest::Create(_) => {
                    (state.creation_barrier.clone(), state.creation_gate.clone())
                }
                MemoryRequest::Read(..) => (None, state.read_gate.clone()),
                _ => (None, None),
            };
            (state.faults.delay, failure, (barrier, gate))
        };
        let (barrier, gate) = barrier;
        if let Some(barrier) = barrier {
            barrier.wait().await;
        }
        if let Some(gate) = gate {
            gate.acquire()
                .await
                .map_err(|closed| StorageFailure::Protocol.with_source(closed))?
                .forget();
        }
        if !delay.is_zero() {
            self.clock.deadline(delay).await;
        }
        match failure {
            Some(failure) => Err(self.injected(failure)),
            None => self.authorize(request).await,
        }
    }
    /// An injected failure, as `Faults` describes it: no response without a
    /// status or a `Retry-After`, else a response, a 429 unless it names
    /// another status.
    pub(super) fn injected(
        &self,
        (failure, status, retry_after): (StorageFailure, Option<u16>, Option<u64>),
    ) -> StorageError {
        match (status, retry_after) {
            (None, None) => StorageError::Failure(failure),
            (status, retry_after) => crate::providers::ProviderResponse::injected(
                self.config.provider(),
                failure,
                status.unwrap_or(429),
                retry_after,
            ),
        }
    }
    /// Send the held token; a rejected one is replaced once and the request
    /// retried once, counted as a second request.
    async fn authorize(&self, request: MemoryRequest) -> Result<(), StorageError> {
        let Some(held) = self.tokens.lock().await.clone() else {
            return Ok(());
        };
        let rejected = |tokens: &OAuthTokens| {
            let token = tokens.access_token.as_str().to_owned();
            async move { self.state.lock().await.rejected_tokens.contains(&token) }
        };
        let used = held.current().await;
        if !rejected(&used).await {
            return Ok(());
        }
        let replacement = held.rejected(&used).await?;
        self.request_trace.lock().expect("request trace poisoned").push(request);
        self.requests.send_modify(|count| *count += 1);
        if rejected(&replacement).await {
            return Err(StorageFailure::Authentication.into());
        }
        Ok(())
    }
    pub(super) async fn before(&self, request: MemoryRequest) -> Result<(), StorageError> {
        self.before_going(request, None).await
    }
    /// [`Self::before`], for a planned session's `start` (`request_going`).
    pub(super) async fn before_going(
        &self,
        request: MemoryRequest,
        start: Option<MemoryStart<'_>>,
    ) -> Result<(), StorageError> {
        self.request_going(request, start).await?;
        if let Account::Recipient(email) = &self.account {
            let state = self.state.lock().await;
            let allowed = match state.accounts.get(email) {
                Some(AccountAccess::Joined) => true,
                Some(AccountAccess::Invited) | None => false,
            };
            if !allowed {
                return Err(StorageFailure::PermissionDenied.into());
            }
        }
        Ok(())
    }
}

struct ActiveMemoryRequest<'a>(&'a std::sync::atomic::AtomicUsize);
impl Drop for ActiveMemoryRequest<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}
