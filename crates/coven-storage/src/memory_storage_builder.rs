use super::*;
use coven_foundation::clock::FixedClock;

/// Shared storage fixture choices; production providers do not use these defaults.
pub struct MemoryStorageBuilder {
    config: StorageConfig,
    clock: ClockRef,
    time_resolution: Duration,
    single_limit: u64,
    part_size: usize,
    listing_page: usize,
}

impl MemoryStorageBuilder {
    pub(super) fn new() -> Self {
        Self {
            config: location(CloudProvider::S3),
            clock: Arc::new(FixedClock::new(std::time::UNIX_EPOCH)),
            time_resolution: Duration::from_nanos(1),
            single_limit: 4 * 1024 * 1024,
            part_size: 64 * 1024,
            listing_page: usize::MAX,
        }
    }

    /// Use the canonical test location for this provider.
    pub fn provider(mut self, provider: CloudProvider) -> Self {
        self.config = location(provider);
        self
    }
    /// Choose an exact location when comparing or reconnecting locations.
    pub fn location(mut self, config: StorageConfig) -> Self {
        self.config = config;
        self
    }
    /// Control publication times and OAuth expiry with this clock.
    pub fn clock(mut self, clock: ClockRef) -> Self {
        self.clock = clock;
        self
    }
    /// Choose the reported provider tick. Set the fixture clock on this grid.
    pub fn time_resolution(mut self, resolution: Duration) -> Self {
        self.time_resolution = resolution;
        self
    }
    /// Choose the single-request threshold and resumable part size.
    pub fn transfer_limits(mut self, single_limit: u64, part_size: usize) -> Self {
        self.single_limit = single_limit;
        self.part_size = part_size;
        self
    }
    /// Return listings in native pages of this many entries, each page its own
    /// request in the trace, as a provider's continuation requests are (§3.1).
    pub fn listing_page(mut self, entries: std::num::NonZeroUsize) -> Self {
        self.listing_page = entries.get();
        self
    }
    /// Validate the choices and construct an isolated storage backend.
    pub fn build(self) -> Result<MemoryStorage, StorageError> {
        let Self {
            config,
            clock,
            time_resolution,
            single_limit,
            part_size,
            listing_page,
        } = self;
        config.validate()?;
        if time_resolution.is_zero() {
            return Err(StorageFailure::InvalidConfiguration.with_source("zero provider time unit"));
        }
        if single_limit == 0 || part_size == 0 {
            return Err(StorageFailure::InvalidPart.into());
        }
        Ok(MemoryStorage::from_provider(Arc::new(MemoryProvider {
            config,
            online: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            single_limit,
            part_size,
            listing_page,
            requests: Arc::new(tokio::sync::watch::channel(0).0),
            request_trace: Arc::new(std::sync::Mutex::new(Vec::new())),
            active_requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            peak_requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            clock,
            time_resolution,
            tokens: Arc::new(Mutex::new(None)),
            s3_key: Arc::new(Mutex::new(None)),
            key_admin: crate::test_utils::MemoryKeyAdmin::default(),
            account: Account::Owner,
            state: Arc::new(Mutex::new(State {
                objects: BTreeMap::new(),
                uploads: BTreeMap::new(),
                next: 1,
                accounts: BTreeMap::new(),
                retained_access: BTreeMap::new(),
                faults: Faults::none(),
                ranges: Vec::new(),
                reads: Vec::new(),
                received_buffers: Vec::new(),
                sent_bytes: 0,
                largest_part: 0,
                held_listing: None,
                held_read: None,
                held_body: None,
                held_creation: None,
                held_access: None,
                rejected_tokens: Default::default(),
            })),
        })))
    }
}

fn location(provider: CloudProvider) -> StorageConfig {
    match provider {
        CloudProvider::S3 => StorageConfig::S3 {
            bucket: "test".into(),
            region: "us-east-1".into(),
            endpoint: None,
            profile: None,
            prefix: "store".into(),
        },
        CloudProvider::Dropbox => StorageConfig::Dropbox {
            location: crate::DropboxLocation::SharedFolder {
                namespace_id: "store".into(),
            },
        },
        CloudProvider::OneDrive => StorageConfig::OneDrive {
            drive_id: "drive".into(),
            folder_id: "store".into(),
        },
        CloudProvider::CloudKit => StorageConfig::CloudKit {
            container: "container".into(),
            owner: "owner".into(),
            zone: "store".into(),
        },
    }
}
