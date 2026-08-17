use super::{
    DevicePairingError, DevicePairingOffer, DevicePairingRequest, SealedDevicePairingRequest,
};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use coven_keys::keys::UserKeypair;
use coven_replication::sync::store::DeviceJoinTransportTiming;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::debug;

const MAX_PAIRING_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum HostResponse {
    AwaitingApproval,
    Invited(Vec<u8>),
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HostState {
    request: Option<DevicePairingRequest>,
    response: HostResponse,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedPairingHost {
    offer: DevicePairingOffer,
    pairing_key: String,
    state: HostState,
}

#[derive(Clone)]
struct PairingJournal {
    path: std::path::PathBuf,
}

impl PairingJournal {
    fn create(
        path: std::path::PathBuf,
        offer: DevicePairingOffer,
        pairing_key: &UserKeypair,
    ) -> Result<(Self, PersistedPairingHost), DevicePairingTransportError> {
        let journal = Self { path };
        if coven_foundation::atomic_file::AtomicFile::new(journal.path.clone())
            .read_optional()?
            .is_some()
        {
            return Err(DevicePairingTransportError::SessionAlreadyExists);
        }
        let persisted = PersistedPairingHost {
            offer,
            pairing_key: URL_SAFE_NO_PAD.encode(pairing_key.to_keypair_bytes()),
            state: HostState::awaiting_request(),
        };
        journal.replace(&persisted)?;
        Ok((journal, persisted))
    }

    fn open(
        path: std::path::PathBuf,
        now_unix_seconds: i64,
    ) -> Result<(Self, PersistedPairingHost, UserKeypair), DevicePairingTransportError> {
        let journal = Self { path };
        let bytes = coven_foundation::atomic_file::AtomicFile::new(journal.path.clone())
            .read_optional()?
            .ok_or(DevicePairingTransportError::SessionMissing)?;
        Self::open_bytes(journal, &bytes, now_unix_seconds)
    }

    fn open_bytes(
        journal: Self,
        bytes: &[u8],
        now_unix_seconds: i64,
    ) -> Result<(Self, PersistedPairingHost, UserKeypair), DevicePairingTransportError> {
        let persisted: PersistedPairingHost = serde_json::from_slice(bytes)?;
        if persisted.offer.expires_at_unix_seconds() <= now_unix_seconds {
            return Err(DevicePairingTransportError::Expired);
        }
        let key_bytes = URL_SAFE_NO_PAD.decode(&persisted.pairing_key)?;
        let key_bytes: [u8; 64] = key_bytes
            .try_into()
            .map_err(|_| DevicePairingTransportError::PairingKeyLength)?;
        let pairing_key = UserKeypair::from_signing_key_bytes(&key_bytes)?;
        if coven_keys::keys::public_key_hex(&pairing_key) != persisted.offer.pairing_public_key() {
            return Err(DevicePairingTransportError::PairingKeyMismatch);
        }
        Ok((journal, persisted, pairing_key))
    }

    fn replace(&self, state: &PersistedPairingHost) -> Result<(), DevicePairingTransportError> {
        let bytes = serde_json::to_vec(state)?;
        coven_foundation::atomic_file::AtomicFile::new(self.path.clone()).replace(&bytes)?;
        Ok(())
    }

    fn remove(&self) -> Result<(), DevicePairingTransportError> {
        coven_foundation::atomic_file::AtomicFile::new(self.path.clone()).remove()?;
        coven_foundation::atomic_file::sync_parent_dir_blocking(&self.path)?;
        Ok(())
    }
}

impl HostState {
    fn awaiting_request() -> Self {
        Self {
            request: None,
            response: HostResponse::AwaitingApproval,
        }
    }
}

struct DevicePairingHostInner {
    offer: DevicePairingOffer,
    state: Arc<Mutex<PersistedPairingHost>>,
    journal: PairingJournal,
    request_tx: watch::Sender<Option<DevicePairingRequest>>,
    server: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Drop for DevicePairingHostInner {
    fn drop(&mut self) {
        if let Some(server) = self.server.lock().expect("lock pairing server task").take() {
            server.abort();
        }
    }
}

/// A listener behind the one QR code shown by the existing device. It accepts
/// one exact signed identity, survives client reconnects, and returns the same
/// sealed invitation on every retry.
#[derive(Clone)]
pub struct DevicePairingHost {
    inner: Arc<DevicePairingHostInner>,
}

impl DevicePairingHost {
    pub async fn start(
        listener: TcpListener,
        offer: DevicePairingOffer,
        pairing_key: UserKeypair,
        journal_path: std::path::PathBuf,
        clock: coven_foundation::clock::ClockRef,
    ) -> Result<Self, DevicePairingTransportError> {
        let (journal, persisted) = PairingJournal::create(journal_path, offer, &pairing_key)?;
        Self::start_persisted(listener, journal, persisted, pairing_key, clock).await
    }

    pub async fn resume(
        listener: TcpListener,
        journal_path: std::path::PathBuf,
        clock: coven_foundation::clock::ClockRef,
    ) -> Result<Self, DevicePairingTransportError> {
        let (journal, persisted, pairing_key) =
            PairingJournal::open(journal_path, clock.now().timestamp())?;
        Self::start_persisted(listener, journal, persisted, pairing_key, clock).await
    }

    pub async fn start_or_resume(
        listener: TcpListener,
        offer: DevicePairingOffer,
        pairing_key: UserKeypair,
        journal_path: std::path::PathBuf,
        clock: coven_foundation::clock::ClockRef,
    ) -> Result<Self, DevicePairingTransportError> {
        let journal = PairingJournal {
            path: journal_path.clone(),
        };
        match coven_foundation::atomic_file::AtomicFile::new(journal_path).read_optional()? {
            Some(bytes) => {
                match PairingJournal::open_bytes(journal.clone(), &bytes, clock.now().timestamp()) {
                    Ok((journal, persisted, pairing_key)) => {
                        Self::start_persisted(listener, journal, persisted, pairing_key, clock)
                            .await
                    }
                    Err(DevicePairingTransportError::Expired) => {
                        journal.remove()?;
                        Self::start(listener, offer, pairing_key, journal.path, clock).await
                    }
                    Err(error) => Err(error),
                }
            }
            None => Self::start(listener, offer, pairing_key, journal.path, clock).await,
        }
    }

    async fn start_persisted(
        listener: TcpListener,
        journal: PairingJournal,
        persisted: PersistedPairingHost,
        pairing_key: UserKeypair,
        clock: coven_foundation::clock::ClockRef,
    ) -> Result<Self, DevicePairingTransportError> {
        let offer = persisted.offer.clone();
        let state = Arc::new(Mutex::new(persisted));
        let initial_request = state
            .lock()
            .expect("lock pairing host state")
            .state
            .request
            .clone();
        let request_tx = watch::channel(initial_request).0;
        let server_state = Arc::clone(&state);
        let server_request_tx = request_tx.clone();
        let server_offer = offer.clone();
        let server_journal = journal.clone();
        let server_clock = clock;
        let server = tokio::spawn(async move {
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        debug!(%error, "device pairing listener stopped accepting connections");
                        return;
                    }
                };
                let state = Arc::clone(&server_state);
                let request_tx = server_request_tx.clone();
                let offer = server_offer.clone();
                let pairing_key = pairing_key.clone();
                let journal = server_journal.clone();
                let clock = server_clock.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(
                        stream,
                        &offer,
                        &pairing_key,
                        state,
                        journal,
                        clock,
                        request_tx,
                    )
                    .await
                    {
                        debug!(%peer, %error, "device pairing connection refused");
                    }
                });
            }
        });
        Ok(Self {
            inner: Arc::new(DevicePairingHostInner {
                offer,
                state,
                journal,
                request_tx,
                server: Mutex::new(Some(server)),
            }),
        })
    }

    pub fn offer(&self) -> &DevicePairingOffer {
        &self.inner.offer
    }

    pub fn subscribe_request(&self) -> watch::Receiver<Option<DevicePairingRequest>> {
        self.inner.request_tx.subscribe()
    }

    pub async fn wait_for_request(
        &self,
    ) -> Result<DevicePairingRequest, DevicePairingTransportError> {
        let mut receiver = self.subscribe_request();
        loop {
            if let Some(request) = receiver.borrow().clone() {
                return Ok(request);
            }
            receiver
                .changed()
                .await
                .map_err(|_| DevicePairingTransportError::HostStopped)?;
        }
    }

    pub fn deliver_invitation(
        &self,
        request: &DevicePairingRequest,
        invitation: Vec<u8>,
    ) -> Result<(), DevicePairingTransportError> {
        let mut persisted = self.inner.state.lock().expect("lock pairing host state");
        if persisted.state.request.as_ref() != Some(request) {
            return Err(DevicePairingTransportError::RequestMismatch);
        }
        let mut next = persisted.clone();
        match &next.state.response {
            HostResponse::AwaitingApproval => {
                next.state.response = HostResponse::Invited(invitation);
            }
            HostResponse::Invited(existing) if existing == &invitation => {}
            HostResponse::Invited(_) | HostResponse::Cancelled => {
                return Err(DevicePairingTransportError::ResponseConflict)
            }
        }
        self.inner.journal.replace(&next)?;
        *persisted = next;
        drop(persisted);
        Ok(())
    }

    pub fn invitation(
        &self,
        request: &DevicePairingRequest,
    ) -> Result<Option<Vec<u8>>, DevicePairingTransportError> {
        let persisted = self.inner.state.lock().expect("lock pairing host state");
        if persisted.state.request.as_ref() != Some(request) {
            return Err(DevicePairingTransportError::RequestMismatch);
        }
        match &persisted.state.response {
            HostResponse::AwaitingApproval => Ok(None),
            HostResponse::Invited(invitation) => Ok(Some(invitation.clone())),
            HostResponse::Cancelled => Err(DevicePairingTransportError::Cancelled),
        }
    }

    pub fn cancel(&self) -> Result<(), DevicePairingTransportError> {
        let mut persisted = self.inner.state.lock().expect("lock pairing host state");
        let mut next = persisted.clone();
        match next.state.response {
            HostResponse::AwaitingApproval => next.state.response = HostResponse::Cancelled,
            HostResponse::Cancelled => {}
            HostResponse::Invited(_) => return Err(DevicePairingTransportError::ResponseConflict),
        }
        self.inner.journal.replace(&next)?;
        *persisted = next;
        drop(persisted);
        Ok(())
    }

    pub fn finish(&self) -> Result<(), DevicePairingTransportError> {
        let persisted = self.inner.state.lock().expect("lock pairing host state");
        if matches!(persisted.state.response, HostResponse::AwaitingApproval) {
            return Err(DevicePairingTransportError::ResponseConflict);
        }
        self.inner.journal.remove()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PairingWireRequest {
    request: SealedDevicePairingRequest,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum PairingWireResponse {
    AwaitingApproval,
    Invited { invitation: String },
    Cancelled,
    SessionClaimed,
    Expired,
}

async fn handle_connection(
    mut stream: TcpStream,
    offer: &DevicePairingOffer,
    pairing_key: &UserKeypair,
    state: Arc<Mutex<PersistedPairingHost>>,
    journal: PairingJournal,
    clock: coven_foundation::clock::ClockRef,
    request_tx: watch::Sender<Option<DevicePairingRequest>>,
) -> Result<(), DevicePairingTransportError> {
    let wire: PairingWireRequest = read_frame(&mut stream).await?;
    if clock.now().timestamp() >= offer.expires_at_unix_seconds() {
        write_frame(&mut stream, &PairingWireResponse::Expired).await?;
        return Ok(());
    }
    let request = wire.request.open(offer, pairing_key)?;
    let response = {
        let mut persisted = state.lock().expect("lock pairing host state");
        match &persisted.state.request {
            None => {
                let mut next = persisted.clone();
                next.state.request = Some(request.clone());
                journal.replace(&next)?;
                *persisted = next;
                request_tx.send_replace(Some(request));
                response_for(&persisted.state.response)
            }
            Some(existing) if existing == &request => response_for(&persisted.state.response),
            Some(_) => PairingWireResponse::SessionClaimed,
        }
    };
    write_frame(&mut stream, &response).await?;
    Ok(())
}

fn response_for(response: &HostResponse) -> PairingWireResponse {
    match response {
        HostResponse::AwaitingApproval => PairingWireResponse::AwaitingApproval,
        HostResponse::Invited(invitation) => PairingWireResponse::Invited {
            invitation: URL_SAFE_NO_PAD.encode(invitation),
        },
        HostResponse::Cancelled => PairingWireResponse::Cancelled,
    }
}

/// Submit the same signed request until the owner approves, cancels, or the
/// caller's deadline expires. Each retry may use another endpoint from the QR;
/// the host accepts the exact request idempotently and refuses a competing one.
pub async fn receive_device_invitation(
    offer: &DevicePairingOffer,
    request: &SealedDevicePairingRequest,
    timing: DeviceJoinTransportTiming,
    clock: coven_foundation::clock::ClockRef,
    cancel: &watch::Receiver<bool>,
) -> Result<Vec<u8>, DevicePairingTransportError> {
    let deadline = clock.now()
        + chrono::Duration::from_std(timing.deadline)
            .map_err(|_| DevicePairingTransportError::DeadlineOutOfRange)?;
    let mut cancellation = cancel.clone();
    let wire = PairingWireRequest {
        request: request.clone(),
    };
    let mut failures = Vec::new();
    loop {
        if *cancel.borrow() {
            return Err(DevicePairingTransportError::Cancelled);
        }
        if clock.now().timestamp() >= offer.expires_at_unix_seconds() {
            return Err(DevicePairingTransportError::Expired);
        }
        if clock.now() >= deadline {
            return Err(DevicePairingTransportError::Unavailable(failures));
        }
        failures.clear();
        for endpoint in offer.endpoints() {
            match exchange(*endpoint, &wire).await {
                Ok(PairingWireResponse::AwaitingApproval) => break,
                Ok(PairingWireResponse::Invited { invitation }) => {
                    return URL_SAFE_NO_PAD
                        .decode(invitation)
                        .map_err(DevicePairingTransportError::Ciphertext)
                }
                Ok(PairingWireResponse::Cancelled) => {
                    return Err(DevicePairingTransportError::Cancelled)
                }
                Ok(PairingWireResponse::SessionClaimed) => {
                    return Err(DevicePairingTransportError::SessionClaimed)
                }
                Ok(PairingWireResponse::Expired) => {
                    return Err(DevicePairingTransportError::Expired)
                }
                Err(error) => {
                    debug!(%endpoint, %error, "device pairing endpoint unavailable");
                    failures.push(format!("{endpoint}: {error}"));
                }
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(timing.poll) => {}
            changed = cancellation.changed() => {
                changed.map_err(|_| DevicePairingTransportError::CancellationChannelClosed)?;
            }
        }
    }
}

async fn exchange(
    endpoint: std::net::SocketAddr,
    request: &PairingWireRequest,
) -> Result<PairingWireResponse, DevicePairingTransportError> {
    let mut stream = TcpStream::connect(endpoint).await?;
    write_frame(&mut stream, request).await?;
    read_frame(&mut stream).await
}

async fn write_frame<T: Serialize>(
    stream: &mut TcpStream,
    value: &T,
) -> Result<(), DevicePairingTransportError> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > MAX_PAIRING_MESSAGE_BYTES {
        return Err(DevicePairingTransportError::MessageTooLarge(bytes.len()));
    }
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn read_frame<T: for<'de> Deserialize<'de>>(
    stream: &mut TcpStream,
) -> Result<T, DevicePairingTransportError> {
    let length = stream.read_u32().await? as usize;
    if length > MAX_PAIRING_MESSAGE_BYTES {
        return Err(DevicePairingTransportError::MessageTooLarge(length));
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

#[derive(Debug, thiserror::Error)]
pub enum DevicePairingTransportError {
    #[error("pairing protocol: {0}")]
    Pairing(#[from] DevicePairingError),
    #[error("pairing network: {0}")]
    Network(#[from] std::io::Error),
    #[error("pairing message JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("pairing journal: {0}")]
    Journal(#[from] coven_foundation::atomic_file::FileError),
    #[error("pairing journal key: {0}")]
    PairingKey(#[from] coven_keys::keys::KeyError),
    #[error("pairing response ciphertext: {0}")]
    Ciphertext(base64::DecodeError),
    #[error("pairing journal key encoding: {0}")]
    PairingKeyEncoding(#[from] base64::DecodeError),
    #[error("pairing message contains {0} bytes")]
    MessageTooLarge(usize),
    #[error("a pairing session is already durable at this path")]
    SessionAlreadyExists,
    #[error("the durable pairing session is absent")]
    SessionMissing,
    #[error("the pairing session expired")]
    Expired,
    #[error("the durable pairing key is not 64 bytes")]
    PairingKeyLength,
    #[error("the durable pairing key does not match the displayed offer")]
    PairingKeyMismatch,
    #[error("another joining identity already claimed this pairing session")]
    SessionClaimed,
    #[error("pairing request does not match the accepted identity")]
    RequestMismatch,
    #[error("pairing session already has another terminal response")]
    ResponseConflict,
    #[error("pairing was cancelled")]
    Cancelled,
    #[error("pairing cancellation channel closed")]
    CancellationChannelClosed,
    #[error("pairing host stopped")]
    HostStopped,
    #[error("pairing deadline cannot be represented by the injected clock")]
    DeadlineOutOfRange,
    #[error("no pairing endpoint responded before the deadline: {0:?}")]
    Unavailable(Vec<String>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use coven_foundation::config::CloudProvider;
    use std::time::Duration;

    async fn host() -> (DevicePairingHost, UserKeypair, tempfile::TempDir) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind pairing listener");
        let endpoint = listener.local_addr().expect("pairing endpoint");
        let pairing_key = UserKeypair::generate();
        let offer = DevicePairingOffer::new(
            &pairing_key,
            vec![endpoint],
            "Transport Test Store".to_string(),
            CloudProvider::S3,
            1_900_000_000,
        )
        .expect("pairing offer");
        let journal = tempfile::tempdir().expect("pairing journal directory");
        (
            DevicePairingHost::start(
                listener,
                offer,
                pairing_key,
                journal.path().join("pairing.json"),
                Arc::new(coven_foundation::clock::SystemClock),
            )
            .await
            .expect("start pairing host"),
            UserKeypair::generate(),
            journal,
        )
    }

    fn timing() -> DeviceJoinTransportTiming {
        DeviceJoinTransportTiming {
            poll: Duration::from_millis(2),
            deadline: Duration::from_secs(2),
        }
    }

    #[tokio::test]
    async fn one_request_reconnects_until_the_owner_returns_its_invitation() {
        let (host, joining_identity, _journal) = host().await;
        let request = DevicePairingRequest::signed(host.offer(), &joining_identity, None);
        let sealed = SealedDevicePairingRequest::new(host.offer(), &request).expect("seal request");
        let (_cancel_tx, cancel) = watch::channel(false);
        let receiving = tokio::spawn({
            let offer = host.offer().clone();
            let sealed = sealed.clone();
            async move {
                receive_device_invitation(
                    &offer,
                    &sealed,
                    timing(),
                    Arc::new(coven_foundation::clock::SystemClock),
                    &cancel,
                )
                .await
            }
        });

        let observed = host.wait_for_request().await.expect("receive request");
        assert_eq!(observed, request);
        host.deliver_invitation(&request, b"sealed invitation".to_vec())
            .expect("deliver invitation");

        assert_eq!(
            receiving
                .await
                .expect("join client task")
                .expect("invitation"),
            b"sealed invitation",
        );
    }

    #[tokio::test]
    async fn a_second_identity_cannot_replace_the_request_the_owner_is_reviewing() {
        let (host, first_identity, _journal) = host().await;
        let first = DevicePairingRequest::signed(host.offer(), &first_identity, None);
        let first_sealed =
            SealedDevicePairingRequest::new(host.offer(), &first).expect("seal first request");
        let (_first_cancel_tx, first_cancel) = watch::channel(false);
        let first_receive = tokio::spawn({
            let offer = host.offer().clone();
            async move {
                receive_device_invitation(
                    &offer,
                    &first_sealed,
                    timing(),
                    Arc::new(coven_foundation::clock::SystemClock),
                    &first_cancel,
                )
                .await
            }
        });
        assert_eq!(host.wait_for_request().await.expect("first request"), first);

        let second = DevicePairingRequest::signed(host.offer(), &UserKeypair::generate(), None);
        let second_sealed =
            SealedDevicePairingRequest::new(host.offer(), &second).expect("seal second request");
        let (_second_cancel_tx, second_cancel) = watch::channel(false);
        assert!(matches!(
            receive_device_invitation(
                host.offer(),
                &second_sealed,
                timing(),
                Arc::new(coven_foundation::clock::SystemClock),
                &second_cancel,
            )
            .await,
            Err(DevicePairingTransportError::SessionClaimed)
        ));

        host.deliver_invitation(&first, b"first invitation".to_vec())
            .expect("finish first request");
        assert_eq!(
            first_receive
                .await
                .expect("first client task")
                .expect("first invitation"),
            b"first invitation",
        );
    }

    #[tokio::test]
    async fn an_owner_restart_resumes_the_exact_request_and_response() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind first pairing listener");
        let endpoint = listener.local_addr().expect("pairing endpoint");
        let pairing_key = UserKeypair::generate();
        let offer = DevicePairingOffer::new(
            &pairing_key,
            vec![endpoint],
            "Restart Test Store".to_string(),
            CloudProvider::S3,
            1_900_000_000,
        )
        .expect("pairing offer");
        let journal = tempfile::tempdir().expect("pairing journal directory");
        let journal_path = journal.path().join("pairing.json");
        let clock: coven_foundation::clock::ClockRef =
            Arc::new(coven_foundation::clock::SystemClock);
        let host = DevicePairingHost::start(
            listener,
            offer.clone(),
            pairing_key,
            journal_path.clone(),
            clock.clone(),
        )
        .await
        .expect("start first pairing host");
        let request = DevicePairingRequest::signed(&offer, &UserKeypair::generate(), None);
        let sealed = SealedDevicePairingRequest::new(&offer, &request).expect("seal request");
        let (_cancel_tx, cancel) = watch::channel(false);
        let receiving = tokio::spawn({
            let offer = offer.clone();
            let clock = clock.clone();
            async move { receive_device_invitation(&offer, &sealed, timing(), clock, &cancel).await }
        });
        assert_eq!(
            host.wait_for_request().await.expect("first request"),
            request
        );

        drop(host);
        tokio::task::yield_now().await;
        let listener = TcpListener::bind(endpoint)
            .await
            .expect("rebind pairing listener");
        let resumed = DevicePairingHost::resume(listener, journal_path, clock)
            .await
            .expect("resume pairing host");
        assert_eq!(
            resumed.wait_for_request().await.expect("durable request"),
            request,
        );
        resumed
            .deliver_invitation(&request, b"resumed invitation".to_vec())
            .expect("persist invitation after restart");
        assert_eq!(
            receiving
                .await
                .expect("joining task")
                .expect("resumed invitation"),
            b"resumed invitation",
        );
    }

    #[tokio::test]
    async fn cancellation_is_durable_and_reaches_the_exact_waiting_identity() {
        let (host, joining_identity, journal) = host().await;
        let request = DevicePairingRequest::signed(host.offer(), &joining_identity, None);
        let sealed = SealedDevicePairingRequest::new(host.offer(), &request).expect("seal request");
        let (_cancel_tx, cancel) = watch::channel(false);
        let receiving = tokio::spawn({
            let offer = host.offer().clone();
            async move {
                receive_device_invitation(
                    &offer,
                    &sealed,
                    timing(),
                    Arc::new(coven_foundation::clock::SystemClock),
                    &cancel,
                )
                .await
            }
        });
        assert_eq!(host.wait_for_request().await.expect("request"), request);
        host.cancel().expect("persist cancellation");

        assert!(matches!(
            receiving.await.expect("joining task"),
            Err(DevicePairingTransportError::Cancelled)
        ));
        assert!(journal.path().join("pairing.json").exists());
    }
}
