use super::*;
use coven_foundation::clock::FixedClock;

/// Shared storage fixture choices; production providers do not use these defaults.
pub struct MemoryStorageBuilder {
    config: StorageConfig,
    clock: ClockRef,
    single_limit: u64,
    part_size: usize,
}

impl MemoryStorageBuilder {
    pub(super) fn new() -> Self {
        Self {
            config: location(CloudProvider::S3),
            clock: Arc::new(FixedClock::new(std::time::UNIX_EPOCH)),
            single_limit: 16,
            part_size: 4,
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
    /// Choose the single-request threshold and resumable part size.
    pub fn transfer_limits(mut self, single_limit: u64, part_size: usize) -> Self {
        self.single_limit = single_limit;
        self.part_size = part_size;
        self
    }
    /// Validate the choices and construct an isolated storage backend.
    pub fn build(self) -> Result<MemoryStorage, StorageError> {
        let Self {
            config,
            clock,
            single_limit,
            part_size,
        } = self;
        config.validate()?;
        if single_limit == 0 || part_size == 0 {
            return Err(StorageFailure::InvalidPart.into());
        }
        Ok(MemoryStorage::from_provider(Arc::new(MemoryProvider {
            config,
            online: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            single_limit,
            part_size,
            requests: Arc::new(tokio::sync::watch::channel(0).0),
            active_requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            peak_requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            clock,
            tokens: Arc::new(Mutex::new(None)),
            s3_key: Arc::new(Mutex::new(None)),
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
                sent_bytes: 0,
                largest_part: 0,
                held_listing: None,
                held_creation: None,
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
            prefix: "store".into(),
        },
        CloudProvider::GoogleDrive => StorageConfig::GoogleDrive {
            folder_id: "store".into(),
        },
        CloudProvider::Dropbox => StorageConfig::Dropbox {
            namespace_id: "store".into(),
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
