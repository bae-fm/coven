use crate::store_log_sync::tests::{key, member, public, store};
use crate::*;
use coven_crypto::{
    custody::{KeyError, KeySession, MemberKeyCustody, StoreKeyCustody},
    MemberIdentity, StoreKeyring,
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
    fn elapsed(&self) -> Duration {
        Duration::ZERO
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
custody!(MemberKeyCustody, MemberIdentity);

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
    member: MemberIdentity,
    directory: StoreDir,
    device: DeviceId,
    clock: ClockRef,
    send: Arc<SendGate>,
    ids: Arc<SequentialIds>,
    keys: Arc<KeySession<StoreKeyring>>,
    identity: Arc<KeySession<MemberIdentity>>,
    key_custody: Arc<Custody<StoreKeyring>>,
    member_custody: Arc<Custody<MemberIdentity>>,
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
    async fn new(storage: &MemoryStorage, number: u64, member: MemberIdentity) -> Self {
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
        let send = Arc::new(SendGate::new(clock.clone()));
        let storage = Arc::new(storage.for_device().with_publication_gate(send.clone()));
        let key_custody = Arc::new(Custody::new(None));
        let member_custody = Arc::new(Custody::new(Some(member.clone())));
        let keys = Arc::new(KeySession::store(key_custody.clone()).unwrap());
        let identity = Arc::new(KeySession::member(member_custody.clone()).unwrap());
        assert_eq!(key_custody.unlocks.load(Ordering::Relaxed), 1);
        assert_eq!(member_custody.unlocks.load(Ordering::Relaxed), 1);
        let work = WorkCounter::default();
        db.test_track_work(work.clone()).await.unwrap();
        let waits = Arc::new(RetryWaits::open(db.clone(), clock.clone()).await.unwrap());
        let files = Files::new(
            FileDatabase::new(db.clone()),
            directory.clone(),
            Some(storage.clone()),
            send.clone(),
            waits.clone(),
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
            send.clone(),
            ids.clone(),
            directory.clone(),
            waits,
            crate::RestoreCodeCustody::Unkept,
        );
        let entry_decodes = Arc::new(AtomicUsize::new(0));
        sync.test_track_received_entries(entry_decodes.clone());
        let writes = DeviceLogSync::new(
            storage.clone(),
            db.clone(),
            keys.clone(),
            identity.clone(),
            clock.clone(),
            send.clone(),
        );
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
                send,
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
                    principal: coven_format::S3Principal::Console,
                },
            })
            .await
            .unwrap();
        self.owner.sync_store_log().await.unwrap();
    }

    async fn add_member(&mut self, member: &MemberIdentity) {
        self.owner
            .make_and_upload_entry(coven_format::store_log::StoreChange::AddMember {
                member: member.member(),
                keys: public(member),
                role: MemberRole::Member,
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: "member".into(),
                    principal: coven_format::S3Principal::Console,
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
        assert!(self
            .owner
            .bootstrap_member(crate::BindingSource::Restore)
            .await
            .unwrap());
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
            self.clock.clone(),
            self.send.clone(),
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
        assert_eq!(
            outcome,
            DrainOutcome::Drained { uploaded: 1 },
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
        data.send = Arc::new(SendGate::new(data.clock.clone()));
        data.storage = Arc::new(data.storage.with_publication_gate(data.send.clone()));
        data.work.take();
        data.db.test_track_work(data.work.clone()).await.unwrap();
        let waits = Arc::new(
            RetryWaits::open(data.db.clone(), data.clock.clone())
                .await
                .unwrap(),
        );
        data.files = Files::new(
            FileDatabase::new(data.db.clone()),
            data.directory.clone(),
            Some(data.storage.clone()),
            data.send.clone(),
            waits.clone(),
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
            data.send.clone(),
            data.ids.clone(),
            data.directory.clone(),
            waits,
            crate::RestoreCodeCustody::Unkept,
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
    group_on(
        case,
        MemoryStorage::builder()
            .transfer_limits(16 * 1024 * 1024, 1024 * 1024)
            .build()
            .unwrap(),
    )
    .await
}

async fn group_on(case: Scenario, storage: MemoryStorage) -> Vec<Device<Operations>> {
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
    scenario_on(
        scenario,
        MemoryStorage::builder()
            .transfer_limits(16 * 1024 * 1024, 1024 * 1024)
            .build()
            .unwrap(),
    )
    .await
}

async fn scenario_on(scenario: Scenario, storage: MemoryStorage) -> Counts {
    let mut devices = group_on(scenario, storage).await;
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

// SP "Counting a pass": with a transfer limit of one, one new readable note
// and no other due work adds to an idle pass exactly its GET, whose body is
// the write, and one changed-positions PUT. The idle requests themselves are
// io_counts_idle_requests_charge_every_listing_page's bound.
#[tokio::test]
async fn io_counts_one_write_requests() {
    let idle = scenario(Scenario::Idle).await.requests_by_kind();
    let counts = scenario(Scenario::OneWrite).await;
    assert_eq!(
        counts.requests_by_kind(),
        RequestCounts {
            gets: idle.gets + 1,
            replaces: idle.replaces + 1,
            ..idle
        },
        "{counts:#?}"
    );
}

#[tokio::test]
async fn io_counts_scan_and_fetch_ahead_cost() {
    // CovenIO.Trace.scan_cost/fetch_ahead_cost: k hits + one terminal miss
    // + at most limit-1 speculative requests. This fixture uses limit 1.
    let counts = scenario(Scenario::OneWrite).await;
    let write = |n: u64| {
        coven_storage::ObjectPath::device_log(store(1), DeviceId(2), n.try_into().unwrap())
    };
    let actual: Vec<_> = counts.trace.iter().filter(|request| matches!(request,
        MemoryRequest::Read(path, _) | MemoryRequest::Status(path) if path.write_position().is_some_and(|(writer, _)| writer == DeviceId(2))
    )).cloned().collect();
    assert_eq!(
        actual,
        [
            MemoryRequest::Read(write(1), None),
            MemoryRequest::Read(write(2), None)
        ]
    );
}

// §3.1: an unchanged idle pass commits zero durable transactions.
#[tokio::test]
async fn io_counts_idle_commits_nothing() {
    let work = scenario(Scenario::Idle).await.work;
    assert_eq!((work.commits, work.store_log_commits), (0, 0));
}

// §3.1: an idle pass takes the database writer zero times.
#[tokio::test]
async fn io_counts_idle_takes_no_writer() {
    assert_eq!(scenario(Scenario::Idle).await.work, WorkCounts::default());
}

// CovenIO.Trace idle_requests and SP "Counting a pass": 2 + 3N, with every
// native listing page a request; memory listings carry revisions (M = 0). With two
// entries a page, the one store-log writer folder (devices 2 and 3 hold no
// entries) is one page and the three positions are two.
#[tokio::test]
#[ignore = "fails on main: step 8 growth and retention list snapshots/store/ and devices/ every pass (see io_counts_no_history_listing)"]
async fn io_counts_idle_requests_charge_every_listing_page() {
    let storage = MemoryStorage::builder()
        .transfer_limits(16 * 1024 * 1024, 1024 * 1024)
        .listing_page(2.try_into().unwrap())
        .build()
        .unwrap();
    let counts = scenario_on(Scenario::Idle, storage).await;
    assert_eq!(
        (counts.requests, counts.requests_by_kind()),
        (
            1 + 2 + 9,
            RequestCounts {
                lists: 2,
                gets: 9,
                other: 1,
                ..Default::default()
            }
        )
    );
}

// SP: discovery reads next numbers; no pass lists pages of log history, an
// idle pass performs no snapshot discovery, and files are never listed on
// an idle pass. A warm arrival, file fetch or reopen lists no history either.
#[tokio::test]
#[ignore = "fails on main: snapshot growth (snapshots.rs snapshot_grew) and retention (snapshot_retention.rs) list snapshots/<audience>/ and devices/ every pass instead of using recorded part lengths, cached prefixes and indexed candidates"]
async fn io_counts_no_history_listing() {
    for case in [
        Scenario::Idle,
        Scenario::OneWrite,
        Scenario::FileFetch,
        Scenario::Reopen,
    ] {
        let counts = scenario(case).await;
        let positions = coven_storage::ObjectPrefix::positions(store(1));
        let listed = |r: &MemoryRequest| matches!(r, MemoryRequest::List(p) if p != &positions);
        assert!(!counts.trace.iter().any(listed), "{case:?}: {counts:#?}");
    }
}

#[tokio::test]
async fn io_counts_first_catchup_commits_one_replay_per_batch() {
    let counts = scenario(Scenario::FirstCatchup).await;
    assert_eq!(
        counts.work.store_log_commits, 2,
        "catch-up batch and own registration"
    );
}

// SP "File uploads outside the pass": the upload worker's requests are
// counted on their own, and a pass makes no file upload.
#[tokio::test]
async fn io_counts_upload_worker_is_accounted_apart_from_the_pass() {
    let files = coven_storage::ObjectPrefix::files(store(1));
    let uploading = |request: &MemoryRequest| match request {
        MemoryRequest::Create(path) => files.contains(path),
        MemoryRequest::Other(call) => call.ends_with("_upload") || *call == "upload_part",
        _ => false,
    };
    let mut devices = group(Scenario::Idle).await;
    let mark = devices[0].data.begin_count().await;
    devices[0].data.attach(vec![9; 64 * 1024 + 5]).await;
    let upload = devices[0].data.finish_count(mark).await;
    assert_eq!(upload.trace.iter().filter(|r| uploading(r)).count(), 1);
    let pass = devices[0].pass().await;
    assert!(!pass.trace.iter().any(uploading), "{pass:#?}");
    for device in devices {
        device.close().await;
    }
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
async fn io_counts_reopen_saved_log() {
    let counts = scenario(Scenario::Reopen).await;
    assert!(
        counts.work.full_log_decodes <= 1 && counts.work.entry_decodes <= 3,
        "{:?}",
        counts.work
    );
}

#[tokio::test]
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

// CovenIO.Trace.idle (idle_requests, idle_two_listings, idle_gets): a warm
// idle pass makes exactly the model's requests for the undrained writers'
// next numbers, read from storage independently of the pass. A request
// outside the model's idle vocabulary keeps its description.
#[tokio::test]
#[ignore = "requires COVEN_IO_RUNNER; fails on main for the cause io_counts_idle_requests_charge_every_listing_page names"]
async fn io_differential_idle_trace() {
    use coven_storage::{ObjectPath, ObjectPrefix, Storage};
    type Position = fn(&ObjectPath) -> Option<(DeviceId, std::num::NonZeroU64)>;
    let logs: [(&str, Position); 3] = [
        ("entry", ObjectPath::store_log_position),
        ("copy", ObjectPath::key_copy_position),
        ("write", ObjectPath::write_position),
    ];
    let mut devices = group(Scenario::Idle).await;
    let stored = devices[0]
        .data
        .storage
        .list(&ObjectPrefix::all(store(1)))
        .await
        .unwrap();
    let writers: Vec<_> = (1..=3u64)
        .map(|device| {
            let mut next = serde_json::json!({ "writer": device });
            for (log, position) in logs {
                let count = stored
                    .iter()
                    .filter_map(|o| position(&o.path))
                    .filter(|(w, _)| w.0 == device)
                    .count();
                next[log] = (count + 1).into();
            }
            next
        })
        .collect();
    let counts = devices[0].pass().await;
    let name = |request: &MemoryRequest| match request {
        MemoryRequest::Other("list_writers") => "list store-log".to_owned(),
        MemoryRequest::List(prefix) if prefix == &ObjectPrefix::positions(store(1)) => {
            "list positions".to_owned()
        }
        MemoryRequest::Read(path, None) => logs
            .iter()
            .find_map(|(log, position)| {
                position(path).map(|(w, n)| format!("get {log} {} {n}", w.0))
            })
            .unwrap_or_else(|| format!("{request:?}")),
        other => format!("{other:?}"),
    };
    let mut actual: Vec<_> = counts.trace.iter().map(name).collect();
    let model = crate::discovery::tests::io_runner(&[
        serde_json::json!({"check": "idle", "writers": writers}),
    ]);
    let mut expected: Vec<String> = serde_json::from_value(model[0].clone()).unwrap();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
    for device in devices {
        device.close().await;
    }
}
