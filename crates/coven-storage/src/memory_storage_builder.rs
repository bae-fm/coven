use super::*;
use coven_foundation::clock::FixedClock;

/// Shared storage fixture choices; production providers do not use these defaults.
pub struct MemoryStorageBuilder {
    config: StorageConfig,
    clock: ClockRef,
    time_resolution: Duration,
    single_limit: u64,
    part_size: usize,
    independent_parts: bool,
    whole_part_reads: bool,
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
            independent_parts: false,
            whole_part_reads: false,
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
    /// Keep each session's parts by index, whatever order they arrive in,
    /// as an S3 multipart upload does, so they may go together (§17.9);
    /// otherwise the provider takes them in order.
    pub fn independent_parts(mut self) -> Self {
        self.independent_parts = true;
        self
    }
    /// Move every part a read touches whole, as CloudKit's reads do
    /// (Appendix E.1): an object of up to the part size is one part, a
    /// larger one a part per part size from its start, the last shorter.
    /// [`MemoryStorage::moved_bytes`] counts what each read moved.
    pub fn reads_whole_parts(mut self) -> Self {
        self.whole_part_reads = true;
        self
    }
    /// Return listings in native pages of this many entries, each page its own
    /// request in the trace, as a provider's continuation requests are (§21.1).
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
            independent_parts,
            whole_part_reads,
            listing_page,
        } = self;
        config.validate()?;
        if time_resolution.is_zero() {
            return Err(StorageFailure::InvalidConfiguration.with_source("zero provider time unit"));
        }
        if single_limit == 0 || part_size == 0 {
            return Err(StorageFailure::InvalidPart.into());
        }
        Ok(MemoryStorage::from_memory(Arc::new(MemoryProvider {
            config,
            online: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            single_limit,
            part_size,
            independent_parts,
            whole_part_reads,
            listing_page,
            requests: Arc::new(tokio::sync::watch::channel(0).0),
            request_trace: Arc::new(std::sync::Mutex::new(Vec::new())),
            active_requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            peak_requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            clock,
            time_resolution,
            tokens: Arc::new(Mutex::new(None)),
            s3_key: Arc::new(Mutex::new(None)),
            account: Account::Owner,
            state: Arc::new(Mutex::new(MemoryState {
                location_marker: None,
                folder_name: "Household".into(),
                objects: super::memory_objects::MemoryObjects::new(),
                uploads: BTreeMap::new(),
                next: 1,
                accounts: BTreeMap::new(),
                retained_access: BTreeMap::new(),
                faults: Faults::none(),
                creation_barrier: None,
                creation_gate: None,
                part_gate: None,
                read_gate: None,
                ranges: Vec::new(),
                reads: Vec::new(),
                moved: BTreeMap::new(),
                received_buffers: Vec::new(),
                sent_bytes: 0,
                largest_part: 0,
                held_listing: None,
                held_read: None,
                held_body: None,
                held_creation: None,
                held_request: None,
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
        },
        CloudProvider::Dropbox => StorageConfig::Dropbox {
            account_label: None,
            location: crate::DropboxLocation::SharedFolder { namespace_id: "store".into() },
            folder_label: None,
        },
        CloudProvider::OneDrive => StorageConfig::OneDrive {
            account_label: None,
            drive_id: "drive".into(),
            folder_id: "store".into(),
            folder_label: None,
        },
        CloudProvider::CloudKit => StorageConfig::CloudKit {
            container: "container".into(),
            owner: "owner".into(),
            zone: "store".into(),
        },
    }
}
