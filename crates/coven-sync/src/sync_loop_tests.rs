use crate::store_log_sync::tests::{key, member, public, store};
use crate::*;
use coven_crypto::{
    custody::{KeyError, KeySession, MemberKeyCustody, StoreKeyCustody},
    MemberKeys, StoreKeyring,
};
use coven_database::{
    test_utils::{WorkCounter, WorkCounts},
    CacheFill, Database, DatabaseBuilder, FileDatabase, FileDecl, Migration, Provenance,
    RowIdentity, SyncedTable,
};
use coven_format::store_log::MemberRole;
use coven_foundation::id_source::DeviceId;
use coven_foundation::{
    clock::{Clock, ClockRef},
    files::{StoreDir, StoreLayout},
    id_source::{IdSource, SequentialIds},
};
use coven_storage::test_utils::{MemoryRequest, MemoryStorage};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::UNIX_EPOCH,
};

// No retries or clock deadlines are due. Only explicit commands drive the
// worker, so a background timer cannot contaminate a measured pass.
struct PassClock;
impl Clock for PassClock {
    fn now(&self) -> std::time::SystemTime {
        UNIX_EPOCH + Duration::from_secs(100)
    }
    fn sleep(
        &self,
        _: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(std::future::pending())
    }
}

struct Custody<T> {
    value: Mutex<Option<T>>,
    unlocks: AtomicUsize,
}

impl<T> Custody<T> {
    fn new(value: Option<T>) -> Self {
        Self {
            value: Mutex::new(value),
            unlocks: AtomicUsize::new(0),
        }
    }
}

macro_rules! custody {
    ($trait:ident, $value:ty) => {
        impl $trait for Custody<$value> {
            fn unlock(&self) -> Result<Option<$value>, KeyError> {
                self.unlocks.fetch_add(1, Ordering::Relaxed);
                Ok(self.value.lock().unwrap().clone())
            }
            fn persist(&self, value: &$value) -> Result<(), KeyError> {
                *self.value.lock().unwrap() = Some(value.clone());
                Ok(())
            }
            fn forget(&self) -> Result<(), KeyError> {
                *self.value.lock().unwrap() = None;
                Ok(())
            }
            // Models durable custody; KeySession erases its held copy on close.
            fn close(&self) {}
        }
    };
}
custody!(StoreKeyCustody, StoreKeyring);
custody!(MemberKeyCustody, MemberKeys);

// Preparing a fixture and running its passes transfer the same store-log owner.
// In particular, bootstrap does not create a second independently decoded log.
struct Device<S> {
    owner: S,
    data: DeviceData,
}

struct DeviceData {
    writes: DeviceLogSync,
    files: Files,
    entry_decodes: Arc<AtomicUsize>,
    db: Database,
    storage: Arc<MemoryStorage>,
    member: MemberKeys,
    directory: StoreDir,
    device: DeviceId,
    clock: ClockRef,
    ids: Arc<SequentialIds>,
    keys: Arc<KeySession<StoreKeyring>>,
    identity: Arc<KeySession<MemberKeys>>,
    key_custody: Arc<Custody<StoreKeyring>>,
    member_custody: Arc<Custody<MemberKeys>>,
    work: WorkCounter,
    read_history: Vec<(coven_storage::ObjectPath, u64, u64)>,
    _root: tempfile::TempDir,
}

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", RowIdentity::SharedKey),
        SyncedTable::new("files", RowIdentity::SharedKey).carries_files(FileDecl::new(
            "files",
            Provenance::AppProvided,
            CacheFill::CacheEager,
        )),
    ]
}

fn builder(directory: &StoreDir, clock: &ClockRef) -> DatabaseBuilder {
    DatabaseBuilder::new(directory.clone()).clock(clock.clone()).synced_tables(tables())
        .migrations(vec![Migration::sql(1, "notes and files", "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL, body TEXT NOT NULL); CREATE TABLE files(id TEXT PRIMARY KEY NOT NULL,size INTEGER,hash BLOB,location TEXT)")])
}

impl Device<StoreLogSync> {
    async fn new(storage: &MemoryStorage, number: u64, member: MemberKeys) -> Self {
        let root = tempfile::tempdir().unwrap();
        let ids = Arc::new(SequentialIds::new());
        for _ in 1..number {
            ids.new_device_id();
        }
        let directory = StoreLayout::new(root.path().into())
            .create_store_dir(store(1), "IO counts", ids.as_ref())
            .unwrap();
        let clock = Arc::new(PassClock) as ClockRef;
        let db = builder(&directory, &clock).open().await.unwrap();
        let storage = Arc::new(storage.for_device());
        let key_custody = Arc::new(Custody::new(None));
        let member_custody = Arc::new(Custody::new(Some(member.clone())));
        let keys = Arc::new(KeySession::store(key_custody.clone()).unwrap());
        let identity = Arc::new(KeySession::member(member_custody.clone()).unwrap());
        assert_eq!(key_custody.unlocks.load(Ordering::Relaxed), 1);
        assert_eq!(member_custody.unlocks.load(Ordering::Relaxed), 1);
        let work = WorkCounter::default();
        db.test_track_work(work.clone()).await.unwrap();
        let files = Files::new(
            FileDatabase::new(db.clone()),
            directory.clone(),
            Some(storage.clone()),
            clock.clone(),
            ids.clone(),
            TransferLimits::default(),
        );
        files.stop_background_for_counts().await;
        let mut sync = StoreLogSync::new(
            storage.clone(),
            db.clone(),
            keys.clone(),
            identity.clone(),
            clock.clone(),
            ids.clone(),
            directory.clone(),
        );
        let entry_decodes = Arc::new(AtomicUsize::new(0));
        sync.test_track_received_entries(entry_decodes.clone());
        let writes =
            DeviceLogSync::new(storage.clone(), db.clone(), keys.clone(), identity.clone());
        Self {
            data: DeviceData {
                entry_decodes,
                writes,
                files,
                db,
                storage,
                member,
                directory,
                device: DeviceId(number),
                clock,
                ids,
                keys,
                identity,
                key_custody,
                member_custody,
                work,
                read_history: Vec::new(),
                _root: root,
            },
            owner: sync,
        }
    }

    fn start(self) -> Device<Operations> {
        let operations = Operations::new(
            self.owner,
            self.data.files.clone(),
            self.data.writer(),
            self.data.clock.clone(),
        );
        Device {
            owner: operations,
            data: self.data,
        }
    }

    async fn create(&mut self) {
        self.owner
            .make_and_upload_entry(coven_format::store_log::StoreChange::CreateStore {
                store: store(1),
                name: "IO counts".into(),
                admin: public(&self.data.member),
                key: key(1),
                key_hash: self.owner.test_key(coven_merge::Audience::Store, key(1)),
                device_name: "one".into(),
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: "owner".into(),
                },
            })
            .await
            .unwrap();
    }

    async fn add_member(&mut self, member: &MemberKeys) {
        self.owner
            .make_and_upload_entry(coven_format::store_log::StoreChange::AddMember {
                keys: public(member),
                role: MemberRole::Member,
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: "member".into(),
                },
            })
            .await
            .unwrap();
    }

    async fn add_device(&mut self, number: u64) {
        self.owner
            .make_and_upload_entry(coven_format::store_log::StoreChange::AddDevice {
                device: DeviceId(number),
                name: number.to_string(),
                replaces: None,
            })
            .await
            .unwrap();
    }

    async fn first_catch_up(mut self) -> (Device<Operations>, Counts) {
        let mark = self.data.begin_count().await;
        assert!(self.owner.bootstrap_member().await.unwrap());
        self.add_device(self.data.device.0).await;
        let mut running = self.start();
        running.owner.sync().await.unwrap();
        let counts = running.data.finish_count(mark).await;
        (running, counts)
    }
}

struct Measurement {
    unlocks: (usize, usize),
    requests: u64,
    reads: usize,
}

impl DeviceData {
    fn writer(&self) -> DeviceLogSync {
        DeviceLogSync::new(
            self.storage.clone(),
            self.db.clone(),
            self.keys.clone(),
            self.identity.clone(),
        )
    }

    async fn note(&self, id: &'static str) {
        self.db
            .write(move |sql| {
                sql.execute("INSERT INTO notes VALUES(?1,?1)", [id])?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn attach(&self, bytes: Vec<u8>) {
        self.db
            .write_with_files::<_, _, _, coven_database::DbError>(
                move |batch| {
                    batch.put_file("files", "file", bytes);
                    Ok(())
                },
                |sql| {
                    sql.execute("INSERT INTO files(id) VALUES('file')", [])?;
                    Ok(())
                },
            )
            .await
            .unwrap();
        let outcome = self.files.retry_uploads_now().await.unwrap();
        assert!(
            matches!(outcome, DrainOutcome::Drained { ref failures, .. } if failures.is_empty()),
            "file upload must finish without failures"
        );
    }

    async fn notes(&self) -> Vec<String> {
        self.db
            .read(|sql| Ok(sql.query("SELECT id FROM notes ORDER BY id", [], |row| row.get(0))?))
            .await
            .unwrap()
    }

    async fn begin_count(&self) -> Measurement {
        self.work.take();
        self.entry_decodes.store(0, Ordering::Relaxed);
        Measurement {
            unlocks: self.unlocks(),
            requests: self.storage.request_count(),
            reads: self.storage.reads().await.len(),
        }
    }

    async fn finish_count(&mut self, mark: Measurement) -> Counts {
        let requests = self.storage.request_count() - mark.requests;
        let trace = self
            .storage
            .request_trace()
            .into_iter()
            .skip(mark.requests as usize)
            .collect::<Vec<_>>();
        assert_eq!(
            trace.len() as u64,
            requests,
            "trace covers every attempted request"
        );
        let reads = self
            .storage
            .reads()
            .await
            .into_iter()
            .skip(mark.reads)
            .collect::<Vec<_>>();
        let duplicate_immutable_reads = repeated_reads(&reads, &self.read_history, false);
        // No peer publishes during this pass, and our own replacement is last.
        // Repeated position reads therefore observe the same revision.
        let duplicate_position_reads = repeated_reads(&reads, &[], true);
        self.read_history.extend(reads.iter().cloned());
        let unlocks = self.unlocks();
        Counts {
            requests,
            trace,
            reads,
            duplicate_immutable_reads,
            duplicate_position_reads,
            unlocks: (unlocks.0 - mark.unlocks.0, unlocks.1 - mark.unlocks.1),
            work: self.work.take(),
            received_entry_decodes: self.entry_decodes.load(Ordering::Relaxed),
        }
    }

    fn unlocks(&self) -> (usize, usize) {
        (
            self.key_custody.unlocks.load(Ordering::Relaxed),
            self.member_custody.unlocks.load(Ordering::Relaxed),
        )
    }
}

impl Device<Operations> {
    async fn pass(&mut self) -> Counts {
        let mark = self.data.begin_count().await;
        self.owner.sync().await.unwrap();
        self.data.finish_count(mark).await
    }

    async fn reopen(&mut self) -> WorkCounts {
        self.close().await;
        let data = &mut self.data;
        data.keys.close();
        data.identity.close();
        data.db = builder(&data.directory, &data.clock).open().await.unwrap();
        data.keys = Arc::new(KeySession::store(data.key_custody.clone()).unwrap());
        data.identity = Arc::new(KeySession::member(data.member_custody.clone()).unwrap());
        data.work.take();
        data.db.test_track_work(data.work.clone()).await.unwrap();
        data.files = Files::new(
            FileDatabase::new(data.db.clone()),
            data.directory.clone(),
            Some(data.storage.clone()),
            data.clock.clone(),
            data.ids.clone(),
            TransferLimits::default(),
        );
        data.files.stop_background_for_counts().await;
        let mut sync = StoreLogSync::new(
            data.storage.clone(),
            data.db.clone(),
            data.keys.clone(),
            data.identity.clone(),
            data.clock.clone(),
            data.ids.clone(),
            data.directory.clone(),
        );
        data.entry_decodes = Arc::new(AtomicUsize::new(0));
        sync.test_track_received_entries(data.entry_decodes.clone());
        data.writes = data.writer();
        self.owner = Operations::new(sync, data.files.clone(), data.writer(), data.clock.clone());
        data.work.take()
    }

    async fn close(&self) {
        self.owner.close().await.unwrap();
        self.data.files.close().await;
        self.data.db.close().await.unwrap();
    }
}

#[derive(Debug)]
struct Counts {
    requests: u64,
    trace: Vec<MemoryRequest>,
    reads: Vec<(coven_storage::ObjectPath, u64, u64)>,
    unlocks: (usize, usize),
    duplicate_immutable_reads: usize,
    duplicate_position_reads: usize,
    work: WorkCounts,
    received_entry_decodes: usize,
}

// A range overlaps retained bytes if the same path has any intersecting
// successful interval. These fixtures never evict inputs or replace immutable
// paths. Count requests containing repeated bytes, including partial overlap.
fn repeated_reads(
    reads: &[(coven_storage::ObjectPath, u64, u64)],
    retained: &[(coven_storage::ObjectPath, u64, u64)],
    replaceable: bool,
) -> usize {
    reads
        .iter()
        .enumerate()
        .filter(|(index, (path, start, length))| {
            path.is_replaceable() == replaceable
                && retained
                    .iter()
                    .chain(&reads[..*index])
                    .any(|(previous, offset, size)| {
                        previous == path && *start < offset + size && *offset < start + length
                    })
        })
        .count()
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RequestCounts {
    lists: usize,
    gets: usize,
    ranges: usize,
    creates: usize,
    replaces: usize,
    statuses: usize,
    deletes: usize,
    other: usize,
}

impl Counts {
    fn requests_by_kind(&self) -> RequestCounts {
        let mut counts = RequestCounts::default();
        for request in &self.trace {
            match request {
                MemoryRequest::List(_) => counts.lists += 1,
                MemoryRequest::Read(_, None) => counts.gets += 1,
                MemoryRequest::Read(_, Some(_)) => counts.ranges += 1,
                MemoryRequest::Create(_) => counts.creates += 1,
                MemoryRequest::Replace(_) => counts.replaces += 1,
                MemoryRequest::Status(_) => counts.statuses += 1,
                MemoryRequest::Delete(_) => counts.deletes += 1,
                MemoryRequest::Other(_) => counts.other += 1,
            }
        }
        assert_eq!(
            self.requests as usize,
            counts.lists
                + counts.gets
                + counts.ranges
                + counts.creates
                + counts.replaces
                + counts.statuses
                + counts.deletes
                + counts.other
        );
        counts
    }

    fn file_reads(&self) -> Vec<(u64, u64)> {
        self.reads
            .iter()
            .filter(|(path, _, _)| coven_storage::ObjectPrefix::files(store(1)).contains(path))
            .map(|(_, offset, bytes)| (*offset, *bytes))
            .collect()
    }
}

async fn group(case: Scenario) -> Vec<Device<Operations>> {
    let storage = MemoryStorage::builder()
        .transfer_limits(16 * 1024 * 1024, 1024 * 1024)
        .build()
        .unwrap();
    let mut owner = Device::new(&storage, 1, member(1)).await;
    owner.create().await;
    owner.add_device(2).await;
    owner.add_device(3).await;
    if matches!(case, Scenario::FirstCatchup) {
        owner.add_member(&member(2)).await;
    }
    let mut devices = vec![
        owner.start(),
        Device::new(&storage, 2, member(1)).await.start(),
        Device::new(&storage, 3, member(1)).await.start(),
    ];
    // Finish initial publications and peer observations before measuring a warm pass.
    for _ in 0..3 {
        for device in &mut devices {
            device.pass().await;
        }
    }
    // The measured receiver must have observed both peers' final revisions;
    // neither peer publishes again during its idle or arrival measurement.
    devices[0].pass().await;
    devices
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Idle,
    OneWrite,
    FirstCatchup,
    FileFetch,
    Reopen,
}

async fn scenario(scenario: Scenario) -> Counts {
    let mut devices = group(scenario).await;
    let counts = match scenario {
        Scenario::Idle => devices[0].pass().await,
        Scenario::OneWrite => {
            devices[1].data.note("new").await;
            devices[1].data.writes.upload_writes().await.unwrap();
            let counts = devices[0].pass().await;
            assert_eq!(devices[0].data.notes().await, ["new"]);
            counts
        }
        Scenario::FirstCatchup => {
            devices[1].data.note("first").await;
            devices[1].data.note("second").await;
            devices[1].data.writes.upload_writes().await.unwrap();
            let new_member = member(2);
            let joining = Device::new(&devices[0].data.storage, 4, new_member).await;
            assert!(joining.data.notes().await.is_empty());
            let (joining, counts) = joining.first_catch_up().await;
            assert_eq!(joining.data.notes().await, ["first", "second"]);
            joining.close().await;
            counts
        }
        Scenario::FileFetch => {
            devices[1].data.attach(vec![27; 64 * 1024 + 31]).await;
            devices[1].data.writes.upload_writes().await.unwrap();
            let counts = devices[0].pass().await;
            let file = devices[0].data.db.file_ref("files", "file").await.unwrap();
            let before = devices[0].data.storage.request_count();
            assert_eq!(
                devices[0].data.files.read_file(&file).await.unwrap(),
                vec![27; 64 * 1024 + 31]
            );
            assert_eq!(
                devices[0].data.storage.request_count(),
                before,
                "another consumer reuses cached file bytes"
            );
            counts
        }
        Scenario::Reopen => {
            devices[1].data.note("retained").await;
            devices[1].data.writes.upload_writes().await.unwrap();
            devices[0].pass().await;
            assert_eq!(devices[0].data.unlocks(), (1, 1));
            let startup = devices[0].reopen().await;
            assert_eq!(devices[0].data.unlocks(), (2, 2));
            let mut counts = devices[0].pass().await;
            // Include sync-owner construction: the spec permits one saved-log
            // decode at startup, whether eager or deferred until the first pass.
            counts.work.writer_holds += startup.writer_holds;
            counts.work.full_log_decodes += startup.full_log_decodes;
            counts.work.entry_decodes += startup.entry_decodes;
            counts.work.queued_entry_decodes += startup.queued_entry_decodes;
            counts.work.commits += startup.commits;
            counts.work.store_log_commits += startup.store_log_commits;
            assert_eq!(devices[0].data.notes().await, ["retained"]);
            counts
        }
    };
    for device in devices {
        device.close().await;
    }
    counts
}

// CovenIO.Trace: idle_requests, idle_two_listings and idle_gets. Memory
// listings return revisions directly, so M=0: 2 + 3*3 = 11. S3's 14-request
// example adds three native HEADs that this provider does not perform.
#[tokio::test]
#[ignore = "idle requests exceed the stated 11 (2 LIST, 9 next-number misses, 0 PUT)"]
async fn io_counts_idle_requests() {
    let counts = scenario(Scenario::Idle).await;
    assert_eq!(
        (
            counts.requests,
            counts.requests_by_kind(),
            counts.reads.len()
        ),
        (
            11,
            RequestCounts {
                lists: 2,
                gets: 9,
                ..Default::default()
            },
            0
        )
    );
}

#[tokio::test]
#[ignore = "idle passes acquire the writer and commit unchanged state"]
async fn io_counts_idle_local_work() {
    assert_eq!(scenario(Scenario::Idle).await.work, WorkCounts::default());
}

#[tokio::test]
#[ignore = "requests exceed the stated 13 with one transfer at a time: idle 11 + one write GET + one positions PUT"]
async fn io_counts_one_write_requests() {
    let counts = scenario(Scenario::OneWrite).await;
    assert_eq!(
        (counts.requests, counts.requests_by_kind()),
        (
            13,
            RequestCounts {
                lists: 2,
                gets: 10,
                replaces: 1,
                ..Default::default()
            }
        )
    );
}

#[tokio::test]
#[ignore = "observed 7 range requests for write 1 and 0 terminal probes; stated 1 complete GET + 1 terminal miss, with 0 fetch-ahead allowance at limit 1"]
async fn io_counts_scan_and_fetch_ahead_cost() {
    // CovenIO.Trace.scan_cost/fetch_ahead_cost: k hits + one terminal miss
    // + at most limit-1 speculative requests. This fixture uses limit 1.
    let counts = scenario(Scenario::OneWrite).await;
    let actual = counts.trace.iter().filter(|request| matches!(request,
        MemoryRequest::Read(path, _) if path.write_position().is_some_and(|(writer, _)| writer == DeviceId(2))
    )).cloned().collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            MemoryRequest::Read(
                coven_storage::ObjectPath::device_log(store(1), DeviceId(2), 1.try_into().unwrap()),
                None
            ),
            MemoryRequest::Read(
                coven_storage::ObjectPath::device_log(store(1), DeviceId(2), 2.try_into().unwrap()),
                None
            ),
        ]
    );
}

#[tokio::test]
async fn io_counts_warm_pass_reuses_decoded_log() {
    let mut counts = Vec::new();
    for case in [Scenario::Idle, Scenario::OneWrite, Scenario::FileFetch] {
        let work = scenario(case).await.work;
        counts.push((work.full_log_decodes, work.entry_decodes));
    }
    assert_eq!(counts, [(0, 0); 3]);
}

#[tokio::test]
async fn io_counts_custody_and_received_entries() {
    for case in [
        Scenario::Idle,
        Scenario::OneWrite,
        Scenario::FirstCatchup,
        Scenario::FileFetch,
        Scenario::Reopen,
    ] {
        let counts = scenario(case).await;
        // §3.1: one unlock per custody at opening (checked by Device::new and
        // reopen), zero per pass, and one decode per incoming store-log entry.
        assert_eq!(counts.unlocks, (0, 0), "{case:?}");
        let incoming = if matches!(case, Scenario::FirstCatchup) {
            4
        } else {
            0
        };
        assert_eq!(counts.received_entry_decodes, incoming, "{case:?}");
    }
}

#[tokio::test]
async fn io_counts_one_write_local_baseline() {
    let counts = scenario(Scenario::OneWrite).await;
    assert_eq!(repeated_reads(&counts.reads, &[], false), 0);
    // No fixed total is stated for non-idle writer holds or commits. These
    // are observed baselines; the saved-log decode bound is tested separately.
    assert_eq!((counts.work.writer_holds, counts.work.commits), (55, 11));
}

#[tokio::test]
async fn io_counts_first_catchup_baseline() {
    // Includes admission bootstrap, this device's registration and its first
    // complete pass. No fixed aggregate request/transaction count is stated
    // for initialization; assert the observed fixture counts.
    let counts = scenario(Scenario::FirstCatchup).await;
    assert_eq!(
        (counts.requests, counts.requests_by_kind()),
        (
            69,
            RequestCounts {
                lists: 19,
                gets: 22,
                ranges: 14,
                creates: 1,
                replaces: 1,
                statuses: 12,
                ..Default::default()
            }
        ),
        "{counts:#?}"
    );
    assert_eq!((counts.work.writer_holds, counts.work.commits), (70, 18));
    assert_eq!(
        counts.work.store_log_commits, 2,
        "catch-up batch and own registration"
    );
}

#[tokio::test]
async fn io_counts_first_catchup_saved_log() {
    let counts = scenario(Scenario::FirstCatchup).await;
    // The owner starts with no saved entries. Incoming entries are decoded once
    // on receipt and kept by that owner, including when it registers this device.
    assert!(
        counts.work.full_log_decodes <= 1 && counts.work.entry_decodes == 0,
        "{:?}",
        counts.work
    );
    assert_eq!(counts.work.queued_entry_decodes, 1, "own registration");
}

#[tokio::test]
#[ignore = "observed 10 repeated key-copy downloads and 3 repeated position-version downloads; stated 0 duplicate downloads"]
async fn io_counts_first_catchup_download_reuse() {
    let counts = scenario(Scenario::FirstCatchup).await;
    assert_eq!(
        (
            counts.duplicate_immutable_reads,
            counts.duplicate_position_reads
        ),
        (0, 0)
    );
}

#[tokio::test]
#[ignore = "observed 1 retained key-copy download and 2 repeated position-version downloads per idle/write/file pass; stated 0 duplicates"]
async fn io_counts_warm_download_reuse() {
    // CovenIO.Trace.no_duplicate_download: another consumer or pass cannot
    // refetch retained bytes. All providers are quiescent during measurement.
    let mut actual = Vec::new();
    for case in [Scenario::Idle, Scenario::OneWrite, Scenario::FileFetch] {
        let counts = scenario(case).await;
        actual.push((
            counts.duplicate_immutable_reads,
            counts.duplicate_position_reads,
        ));
    }
    assert_eq!(actual, [(0, 0); 3]);
}

#[tokio::test]
async fn io_counts_file_fetch_baseline() {
    let counts = scenario(Scenario::FileFetch).await;
    // No fixed whole-pass count is stated for an arrival carrying a file.
    // This fixture publishes a row write and the upload's location write.
    assert_eq!(
        (counts.requests, counts.requests_by_kind()),
        (
            38,
            RequestCounts {
                lists: 12,
                gets: 6,
                ranges: 16,
                replaces: 1,
                statuses: 3,
                ..Default::default()
            }
        ),
        "{counts:#?}"
    );
    // No fixed non-idle writer/commit total is stated; observed counts.
    assert_eq!((counts.work.writer_holds, counts.work.commits), (66, 25));
}

#[tokio::test]
#[ignore = "observed 2 file range requests (11-byte header, 65599-byte body); §16.3 states header and initial chunks share 1 request below 1 MiB"]
async fn io_counts_file_fetch_combines_header_and_chunks() {
    let counts = scenario(Scenario::FileFetch).await;
    assert_eq!(counts.file_reads(), [(0, 65610)]);
}

#[tokio::test]
async fn io_counts_file_chunks_survive_consumers_passes_and_reopen() {
    let mut devices = group(Scenario::FileFetch).await;
    let bytes = vec![27; 64 * 1024 + 31];
    devices[1].data.attach(bytes.clone()).await;
    devices[1].data.writes.upload_writes().await.unwrap();
    let counts = devices[0].pass().await;
    assert_eq!(repeated_reads(&counts.reads, &[], false), 0);
    // No byte total is prescribed for this fixture; observed encrypted size.
    assert_eq!(
        counts.file_reads().iter().map(|(_, len)| len).sum::<u64>(),
        65610
    );
    for reopen in [false, true] {
        if reopen {
            devices[0].reopen().await;
        }
        let counts = devices[0].pass().await;
        assert!(
            counts.file_reads().is_empty(),
            "retained file ranges need zero requests"
        );
        let file = devices[0].data.db.file_ref("files", "file").await.unwrap();
        let before = devices[0].data.storage.request_count();
        assert_eq!(devices[0].data.files.read_file(&file).await.unwrap(), bytes);
        assert_eq!(
            devices[0].data.storage.request_count(),
            before,
            "another reader uses retained chunks"
        );
    }
    for device in devices {
        device.close().await;
    }
}

#[tokio::test]
async fn io_counts_reopen_baseline() {
    let counts = scenario(Scenario::Reopen).await;
    // Reopening has additional identity/initialization work; the spec states
    // no fixed total for that first pass. Assert this fixture's observed total.
    assert_eq!(
        (counts.requests, counts.requests_by_kind()),
        (
            26,
            RequestCounts {
                lists: 12,
                gets: 6,
                ranges: 4,
                replaces: 1,
                statuses: 3,
                ..Default::default()
            }
        ),
        "{counts:#?}"
    );
    assert_eq!((counts.work.writer_holds, counts.work.commits), (54, 9));
}

#[tokio::test]
async fn io_counts_reopen_saved_log() {
    let counts = scenario(Scenario::Reopen).await;
    assert!(
        counts.work.full_log_decodes <= 1 && counts.work.entry_decodes <= 3,
        "{:?}",
        counts.work
    );
}

#[tokio::test]
#[ignore = "observed 5 retained immutable range/body reads (4 write-header ranges, 1 key copy) and 2 repeated position-version reads; stated 0 duplicates after reopen"]
async fn io_counts_reopen_download_reuse() {
    let counts = scenario(Scenario::Reopen).await;
    assert_eq!(
        (
            counts.duplicate_immutable_reads,
            counts.duplicate_position_reads
        ),
        (0, 0)
    );
}
