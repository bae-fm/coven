//! A storage connector over in-memory storage that counts the clients it
//! makes and those dropped. Stopping sync returns after its release, which
//! finishes the active pass and the connection's transfers, drops the
//! provider's client, then records the location as custody holds it (E5);
//! a test checks that release through the drop.
use crate::*;
use coven_storage::{test_utils::MemoryStorage, PublicationGate, StorageError};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

/// The clients a connector made, and how many of them have been dropped.
pub(crate) struct Clients {
    made: AtomicUsize,
    dropped: Arc<tokio::sync::watch::Sender<usize>>,
}

impl Clients {
    pub(crate) fn new() -> Self {
        Self { made: AtomicUsize::new(0), dropped: Arc::new(tokio::sync::watch::channel(0).0) }
    }

    /// The publication gate for one new client, around `gate`: only the
    /// client holds it, so it drops with the client and counts it dropped.
    pub(crate) fn gate(&self, gate: Arc<dyn PublicationGate>) -> Arc<dyn PublicationGate> {
        self.made.fetch_add(1, Ordering::SeqCst);
        Arc::new(ClientGate { gate, dropped: self.dropped.clone() })
    }

    /// Wait until `count` clients have been dropped.
    pub(crate) async fn dropped(&self, count: usize) {
        let mut dropped = self.dropped.subscribe();
        coven_foundation::clock::within(dropped.wait_for(|n| *n >= count))
            .await
            .unwrap_or_else(|| panic!("{count} clients dropped"))
            .unwrap();
    }

    /// Wait until every client made so far has been dropped: a handle
    /// still holding one, such as a closed read-only handle not yet
    /// dropped, holds this until it is.
    pub(crate) async fn released(&self) {
        self.dropped(self.made.load(Ordering::SeqCst)).await;
    }

    /// Stop `handle`'s sync, whose connection is one of these clients: it
    /// returns once the whole release is done, every client dropped.
    pub(crate) async fn stop(&self, handle: &CovenHandle) {
        handle.stop_sync().await.unwrap();
        assert_eq!(
            *self.dropped.borrow(),
            self.made.load(Ordering::SeqCst),
            "stopping returned before its release"
        );
    }
}

struct ClientGate {
    gate: Arc<dyn PublicationGate>,
    dropped: Arc<tokio::sync::watch::Sender<usize>>,
}

impl PublicationGate for ClientGate {
    fn check(&self, path: &ObjectPath) -> Result<(), StorageError> {
        self.gate.check(path)
    }
}

impl Drop for ClientGate {
    fn drop(&mut self) {
        self.dropped.send_modify(|dropped| *dropped += 1);
    }
}

/// The in-memory storage as an app's connector, counting its clients and
/// keeping the runs each S3 detection was handed.
pub(crate) struct Connector {
    storage: Arc<MemoryStorage>,
    pub(crate) clients: Clients,
    /// Each detection's runs, in the order detections ran.
    detections: std::sync::Mutex<Vec<coven_storage::S3DetectionRuns>>,
    /// Cut the next detection short, as a crash cuts one: it confirms no
    /// run gone.
    cut_next_detection: AtomicBool,
}

impl Connector {
    pub(crate) fn new(storage: Arc<MemoryStorage>) -> Arc<Self> {
        Arc::new(Self {
            storage,
            clients: Clients::new(),
            detections: std::sync::Mutex::new(Vec::new()),
            cut_next_detection: AtomicBool::new(false),
        })
    }

    /// Detect S3 settings at `location` through `handle`, cut short when
    /// `cut`, and answer the runs the detection was handed.
    pub(crate) async fn detect(
        &self,
        handle: &CovenHandle,
        location: StorageConfig,
        cut: bool,
    ) -> coven_storage::S3DetectionRuns {
        self.cut_next_detection.store(cut, Ordering::SeqCst);
        let detected = handle
            .detect_s3_settings(location, "member-key".into(), SecretText::new("secret".into()))
            .await;
        assert_eq!(detected.is_err(), cut, "{:?}", detected.map(drop));
        self.detections.lock().unwrap().last().expect("a detection ran").clone()
    }

    /// Stop `handle`'s sync, which returns after the whole release.
    pub(crate) async fn stop(&self, handle: &CovenHandle) {
        self.clients.stop(handle).await;
    }
}

#[async_trait::async_trait]
impl coven_storage::providers::StorageConnector for Connector {
    async fn account_label(
        &self,
        auth: coven_storage::ProviderAuth,
        cooldown: Arc<dyn coven_storage::Cooldown>,
    ) -> Result<String, StorageError> {
        self.storage.account_label(auth, cooldown).await
    }
    async fn prepare_cloudkit(
        &self,
        container: &str,
        store: StoreId,
    ) -> Result<StorageConfig, StorageError> {
        self.storage.prepare_cloudkit(container, store).await
    }
    async fn connect(
        &self,
        config: StorageConfig,
        auth: coven_storage::ProviderAuth,
        device: DeviceId,
        publications: Arc<dyn PublicationGate>,
        cooldown: Arc<dyn coven_storage::Cooldown>,
        transfers: Arc<coven_storage::TransferBudget>,
    ) -> Result<Arc<coven_storage::StorageConnection>, StorageError> {
        let gate = self.clients.gate(publications);
        self.storage.connect(config, auth, device, gate, cooldown, transfers).await
    }
    async fn detect_s3(
        &self,
        config: StorageConfig,
        credentials: coven_storage::S3Credentials,
        store: StoreId,
        runs: &coven_storage::S3DetectionRuns,
        waits: &coven_storage::SetupWaits,
    ) -> coven_storage::S3DetectionRun {
        self.detections.lock().unwrap().push(runs.clone());
        if self.cut_next_detection.swap(false, Ordering::SeqCst) {
            return coven_storage::S3DetectionRun {
                detection: Err(coven_storage::StorageFailure::Network.into()),
                cleaned: Vec::new(),
            };
        }
        self.storage.detect_s3(config, credentials, store, runs, waits).await
    }
    async fn check_new_store(
        &self,
        config: StorageConfig,
        auth: coven_storage::ProviderAuth,
        store: StoreId,
        waits: &coven_storage::SetupWaits,
    ) -> Result<(), StorageError> {
        self.storage.check_new_store(config, auth, store, waits).await
    }
}
