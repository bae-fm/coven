use super::*;
use coven_database::{
    Database, DatabaseBuilder, FileColumn, FileLocation, FilePlace, FileRef, FileUpload, Migration,
    Namespace, Provenance, RowIdentity, SyncedTable,
};
use coven_foundation::{
    clock::{ClockRef, ClosureClock},
    files::StoreDirectories,
    id_source::{StoreId, UuidIds},
};
use coven_storage::test_utils::MemoryStorage;
use std::time::{Duration, SystemTime};

const CHUNK: usize = 64 * 1024;
struct Fixture {
    root: tempfile::TempDir,
    directory: StoreDir,
    ids: IdSourceRef,
    clock: ClockRef,
    send: Arc<crate::SyncState>,
    database: Database,
    files: Files,
    storage: Arc<MemoryStorage>,
    namespaces: Vec<Namespace>,
}
impl Fixture {
    async fn new(provenance: Provenance) -> Self {
        let clock: ClockRef = Arc::new(ClosureClock(|| {
            SystemTime::UNIX_EPOCH + Duration::from_secs(100)
        }));
        Self::with_clock(provenance, clock).await
    }
    async fn with_clock(provenance: Provenance, clock: ClockRef) -> Self {
        Self::with_single_limit(provenance, clock, CHUNK as u64).await
    }
    /// A provider taking one request up to `single_limit` encrypted bytes.
    async fn with_single_limit(provenance: Provenance, clock: ClockRef, single_limit: u64) -> Self {
        let root = tempfile::tempdir().unwrap();
        let ids: IdSourceRef = Arc::new(UuidIds);
        let layout = StoreDirectories::new(root.path().to_owned());
        let directory = layout
            .create_store_dir(StoreId(uuid::Uuid::from_u128(1)), ids.as_ref())
            .unwrap();
        let namespaces = vec![
            Namespace::new("files", provenance, 1 << 30),
            Namespace::new("other", Provenance::AppProvided, 1 << 30),
        ];
        let database = builder(&directory, &ids, &clock, &namespaces)
            .open()
            .await
            .unwrap();
        let storage = Arc::new(
            MemoryStorage::builder()
                .provider(coven_storage::CloudProvider::Dropbox)
                .clock(clock.clone())
                .transfer_limits(single_limit, CHUNK)
                .build()
                .unwrap(),
        );
        let send = ready_state();
        let waits = Arc::new(
            crate::RetryWaits::open(database.clone(), clock.clone())
                .await
                .unwrap(),
        );
        let transfers = Arc::new(coven_storage::TransferBudget::new(
            crate::TransferLimits::default(),
        ));
        let storage = Arc::new(
            storage
                .with_transfers(transfers.clone())
                .with_publication_gate(send.clone())
                .with_cooldown(waits.clone()),
        );
        let files = paused_files(
            &database,
            &directory,
            storage.clone(),
            send.clone(),
            waits,
            ids.clone(),
            transfers,
        )
        .await;
        Self {
            root,
            directory,
            ids,
            clock,
            send,
            database,
            files,
            storage,
            namespaces,
        }
    }
    async fn reopen(&mut self) {
        self.files.close().await;
        self.database.close().await.unwrap();
        self.database = builder(&self.directory, &self.ids, &self.clock, &self.namespaces)
            .open()
            .await
            .unwrap();
        self.send = ready_state();
        let waits = Arc::new(
            crate::RetryWaits::open(self.database.clone(), self.clock.clone())
                .await
                .unwrap(),
        );
        let transfers = Arc::new(coven_storage::TransferBudget::new(
            crate::TransferLimits::default(),
        ));
        self.storage = Arc::new(
            self.storage
                .with_transfers(transfers.clone())
                .with_publication_gate(self.send.clone())
                .with_cooldown(waits.clone()),
        );
        self.files = paused_files(
            &self.database,
            &self.directory,
            self.storage.clone(),
            self.send.clone(),
            waits,
            self.ids.clone(),
            transfers,
        )
        .await;
    }
    /// Stage `bytes` in the namespace named like `table`, and point its row
    /// `id` at the new file.
    async fn attach(&self, table: &str, id: &str, bytes: Vec<u8>) -> FileRef {
        let namespace = table.to_owned();
        let insert = format!(
            "INSERT INTO {table}(id,file) VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET file=excluded.file"
        );
        let id = id.to_owned();
        let file = self
            .database
            .write_with_files::<_, _, _, _, coven_database::DbError>(
                move |batch| Ok(batch.put_file(&namespace, bytes)),
                move |sql, file| {
                    sql.execute(&insert, (id, file))?;
                    Ok(file)
                },
            )
            .await
            .unwrap();
        self.database.file_ref(file).await.unwrap()
    }
    /// Stage a user's original of these bytes and point row `id` of `files`
    /// at it.
    async fn original(&self, id: &str, bytes: &[u8]) -> FileRef {
        let path = self.root.path().join(id);
        std::fs::write(&path, bytes).unwrap();
        let prepared = coven_database::prepare_user_file(&path, |_| {})
            .await
            .unwrap();
        let id = id.to_owned();
        let file = self
            .database
            .write_with_files::<_, _, _, _, coven_database::DbError>(
                move |batch| Ok(batch.put_user_file("files", prepared)),
                move |sql, file| {
                    sql.execute("INSERT INTO files(id,file) VALUES(?1,?2)", (id, file))?;
                    Ok(file)
                },
            )
            .await
            .unwrap();
        self.database.file_ref(file).await.unwrap()
    }
    async fn drain(&self) {
        self.files.set_uploads_paused(false).await.unwrap();
        self.files.inner.drain_uploads_retrying().await.unwrap();
        for upload in self.queue().await.files {
            let file = self.database.file_ref(upload.file).await.unwrap();
            assert_eq!(self.upload_reason(&file).await, None);
        }
    }
    /// Stage these bytes for row `id` of `files` and upload them: the file
    /// is stored, under the id it was staged with.
    async fn uploaded(&self, id: &str, bytes: Vec<u8>) -> FileRef {
        let attached = self.attach("files", id, bytes).await;
        self.drain().await;
        let file = self.database.file_ref(attached.id()).await.unwrap();
        assert!(matches!(file.place(), FilePlace::Cloud { .. }));
        assert!(!self.queued(&file).await);
        file
    }
    /// The upload that switches `file` to the cloud, while it waits.
    async fn upload_of(&self, file: &FileRef) -> Option<FileUpload> {
        self.files
            .inner
            .database
            .uploads()
            .await
            .unwrap()
            .into_iter()
            .find(|upload| upload.files.contains(&file.id()))
    }
    async fn queued(&self, file: &FileRef) -> bool {
        self.upload_of(file).await.is_some()
    }
    /// Where a file's bytes are or go, as its record now stands: its stored
    /// location, or its upload's.
    async fn location(&self, file: &FileRef) -> FileLocation {
        let current = match self.database.file_ref(file.id()).await {
            Ok(current) => current,
            Err(coven_database::DbError::FileAbsent { .. }) => file.clone(),
            Err(error) => panic!("reading the record failed: {error}"),
        };
        match current.place() {
            FilePlace::Cloud { location, .. } => location.clone(),
            FilePlace::DeviceOnly { .. } => {
                self.upload_of(&current)
                    .await
                    .expect("an upload stores the file")
                    .location
            }
        }
    }
    /// The queue as the app reads it (E7).
    async fn queue(&self) -> UploadQueue {
        self.database.upload_queue().await.unwrap().into()
    }
    /// The queue as the app's SQL reads it from coven's views (E4), as a
    /// live query.
    fn subscribe_queue(&self) -> coven_database::LiveQuery<ViewedQueue> {
        self.database.subscribe(viewed_queue)
    }
    /// The same, read once.
    async fn viewed_queue(&self) -> ViewedQueue {
        self.database.read(viewed_queue).await.unwrap()
    }
    /// Every pending record as the app's SQL reads it from `coven_pending`,
    /// as a live query.
    fn subscribe_pending(&self) -> coven_database::LiveQuery<Vec<crate::PendingRecord>> {
        self.database.subscribe(|sql| {
            Ok(sql.query(
                "SELECT record FROM coven_pending ORDER BY position",
                [],
                |r| r.get::<_, crate::PendingRecord>(0),
            )?)
        })
    }
    /// The pending subjects of this device's upload and downloads of a
    /// file's bytes.
    async fn subjects(&self, file: &FileRef) -> [crate::PendingSubject; 2] {
        let location = self.location(file).await;
        let (device, file) = (location.uploader(), location.object());
        [
            crate::PendingSubject::Upload { device, file },
            crate::PendingSubject::Download { device, file },
        ]
    }
    /// This device's pending reason about one of its queued files.
    async fn upload_reason(&self, file: &FileRef) -> Option<crate::PendingReason> {
        let [subject, _] = self.subjects(file).await;
        self.database
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.subject == subject)
            .map(|record| record.reason)
    }
    /// The text of the error the pending record about the file's bytes
    /// carries, if any.
    async fn file_cause(&self, file: &FileRef) -> Option<String> {
        let subjects = self.subjects(file).await;
        let records = self
            .database
            .pending::<crate::PendingRecord>()
            .await
            .unwrap();
        self.files
            .subscribe_pending_causes()
            .borrow()
            .attach(records)
            .into_iter()
            .find(|record| subjects.contains(&record.subject))
            .and_then(|record| record.cause)
            .map(|cause| cause.to_string())
    }
    /// The path of a stored file.
    fn path(&self, file: &FileRef) -> coven_storage::ObjectPath {
        let location = stored(file);
        coven_storage::ObjectPath::file(self.directory.id(), location.uploader(), location.object())
    }
    async fn close(&self) {
        self.files.close().await;
        self.database.close().await.unwrap();
    }
}
/// Run the storage model's files runner over JSON cases.
/// The upload queue as the app's SQL reads it: the pause from
/// `coven_upload_pause`, and each queued file, oldest first, from
/// `coven_uploads` (E4).
#[derive(Clone, Debug, PartialEq)]
struct ViewedQueue {
    paused: bool,
    files: Vec<ViewedUpload>,
}
#[derive(Clone, Debug, PartialEq)]
struct ViewedUpload {
    file: coven_foundation::id_source::FileId,
    phase: UploadPhase,
    progress: Option<UploadProgress>,
    retry_at: Option<i64>,
    pending: bool,
}
fn viewed_queue(
    sql: coven_database::SqlReadContext<'_>,
) -> coven_database::CovenResult<ViewedQueue> {
    let paused = sql.query_row("SELECT paused FROM coven_upload_pause", [], |r| r.get(0))?;
    let files = sql.query(
        "SELECT file,sending,bytes_confirmed,bytes_total,retry_at,pending IS NOT NULL
         FROM coven_uploads ORDER BY position",
        [],
        |r| {
            let progress = match (r.get::<_, Option<i64>>(2)?, r.get::<_, Option<i64>>(3)?) {
                (Some(confirmed), Some(total)) => Some(UploadProgress {
                    bytes_confirmed: confirmed as u64,
                    bytes_total: total as u64,
                }),
                _ => None,
            };
            Ok(ViewedUpload {
                file: r.get(0)?,
                phase: if r.get(1)? {
                    UploadPhase::Uploading
                } else {
                    UploadPhase::Waiting
                },
                progress,
                retry_at: r.get(4)?,
                pending: r.get(5)?,
            })
        },
    )?;
    Ok(ViewedQueue { paused, files })
}

fn files_runner(cases: &[serde_json::Value]) -> Vec<serde_json::Value> {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let runner = std::env::var("COVEN_FILES_LEAN").expect("Lean files runner");
    let mut child = Command::new(runner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(cases).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    serde_json::from_slice(&output.stdout).unwrap()
}

/// A stored file's location.
fn stored(file: &FileRef) -> &FileLocation {
    match file.place() {
        FilePlace::Cloud { location, .. } => location,
        FilePlace::DeviceOnly { .. } => panic!("the file is not stored"),
    }
}

/// Two synced tables whose rows reference a file each, one per namespace.
fn tables() -> Vec<SyncedTable> {
    ["files", "other"]
        .map(|table| {
            SyncedTable::new(table, RowIdentity::SharedKey).file_column(FileColumn::new("file"))
        })
        .into()
}

fn builder(
    directory: &StoreDir,
    ids: &IdSourceRef,
    clock: &ClockRef,
    namespaces: &[Namespace],
) -> DatabaseBuilder {
    DatabaseBuilder::new(directory.clone())
        .id_source(ids.clone())
        .clock(clock.clone())
        .synced_tables(tables())
        .namespaces(namespaces.to_vec())
        .migrations(vec![Migration::addition(
            1,
            "files",
            "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT); CREATE TABLE other(id TEXT NOT NULL PRIMARY KEY,file TEXT)",
        )])
}

#[tokio::test]
async fn ranges_authenticate_cache_batch_and_fail_offline() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let bytes = (0..CHUNK * 40 + 31)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<_>>();
    let file = f.uploaded("one", bytes.clone()).await;
    let stream = f.files.open_file_stream(&file).await.unwrap();
    // Opening reads nothing; a first read elsewhere than chunk zero fetches
    // the header alone, then its chunk (§17.6).
    assert!(f.storage.ranges().await.is_empty());
    assert_eq!(
        stream.read_at(CHUNK as u64 * 4 + 5, 19).await.unwrap(),
        bytes[CHUNK * 4 + 5..CHUNK * 4 + 24]
    );
    let ranges = f.storage.ranges().await;
    assert_eq!(ranges.len(), 2);
    assert_eq!(ranges[0].start(), 0);
    assert_eq!(ranges[0].len(), 11);
    assert_eq!(ranges[1].start(), 11 + 4 * (CHUNK as u64 + 16));
    assert_eq!(ranges[1].len(), CHUNK as u64 + 16);
    let requests = f.storage.request_count();
    assert_eq!(
        stream.read_at(CHUNK as u64 * 4 + 7, 9).await.unwrap(),
        bytes[CHUNK * 4 + 7..CHUNK * 4 + 16]
    );
    assert_eq!(f.storage.request_count(), requests);
    stream
        .read_at(CHUNK as u64 * 10, CHUNK as u64 * 25)
        .await
        .unwrap();
    let ranges = f.storage.ranges().await;
    assert!(ranges.iter().all(|range| range.len() <= 1024 * 1024));
    assert_eq!(ranges.len(), 4); // 15 chunks, then ten chunks.
    f.storage.set_online(false);
    let Err(FileReadError::Storage(error)) = stream.read_at(0, 1).await else {
        panic!("read discarded the storage failure")
    };
    assert_eq!(error.failure(), coven_storage::StorageFailure::Network);
    assert!(std::error::Error::source(&error)
        .unwrap()
        .is::<std::io::Error>());
    assert_eq!(
        stream.read_at(CHUNK as u64 * 4, 1).await.unwrap(),
        bytes[CHUNK * 4..CHUNK * 4 + 1]
    );
    f.storage.set_online(true);
    let path = f.path(&file);
    f.storage
        .corrupt_byte(&path, 11 + CHUNK + 16 + 1)
        .await
        .unwrap();
    assert!(matches!(
        stream.read_at(CHUNK as u64, 1).await,
        Err(FileReadError::Integrity { .. })
    ));
    f.close().await;
}

#[tokio::test]
async fn budgets_pins_eviction_and_second_device_reads() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let bytes = vec![51; CHUNK * 3 + 21];
    let file = f.uploaded("first", bytes.clone()).await;
    let second = Fixture::new(Provenance::AppProvided).await;
    for write in f.database.test_queued_writes().await.unwrap() {
        crate::tests::apply_write(&second.database, write)
            .await
            .unwrap();
    }
    let files = Files::new(
        FileDatabase::new(second.database.clone()),
        second.directory.clone(),
        Some(Arc::new(
            f.storage
                .with_transfers(second.files.inner.transfers.clone()),
        )),
        second.send.clone(),
        second.files.inner.waits.clone(),
        second.ids.clone(),
        second.files.inner.transfers.clone(),
    );
    let ref2 = second.database.file_ref(file.id()).await.unwrap();
    assert_eq!(files.read_file(&ref2).await.unwrap(), bytes);
    f.files.set_cache_budget("files", 1).await.unwrap();
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    assert_eq!(
        f.database.files_pinned(vec![file.id()]).await.unwrap(),
        vec![Some(true)]
    );
    let progress = f.database.pin_progress(vec![file.clone()]).await.unwrap();
    assert_eq!(progress.files_completed, 1);
    assert_eq!(progress.bytes_cached, file.size());
    let requests = f.storage.request_count();
    assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
    assert_eq!(f.storage.request_count(), requests);
    f.files.unpin(std::slice::from_ref(&file)).await.unwrap();
    assert_eq!(
        f.database.files_pinned(vec![file.id()]).await.unwrap(),
        vec![Some(false)]
    );
    let stream = f.files.open_file_stream(&file).await.unwrap();
    stream.read_at(CHUNK as u64, 1).await.unwrap();
    let before = f.storage.request_count();
    f.files.evict_file(&file).await.unwrap();
    assert_eq!(f.storage.request_count(), before);
    files.close().await;
    second.close().await;
    f.close().await;
}

#[tokio::test]
async fn sequential_reading_starts_read_ahead_and_streams_a_whole_hash_check() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let bytes = vec![69; CHUNK * 40];
    let file = f.uploaded("ahead", bytes.clone()).await;
    let stream = f.files.open_file_stream(&file).await.unwrap();
    stream.read_at(0, CHUNK as u64).await.unwrap();
    let mut requests = f.storage.subscribe_requests();
    stream.read_at(CHUNK as u64, CHUNK as u64).await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let ranges = f.storage.ranges().await;
            if ranges
                .iter()
                .any(|range| range.start() == 11 + 2 * (CHUNK as u64 + 16))
            {
                break;
            }
            requests.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let mut range = stream.read_range(0, bytes.len() as u64).unwrap();
    let mut offset = 0;
    while let Some(part) = range.next().await.unwrap() {
        assert!(part.len() <= 1024 * 1024);
        assert_eq!(part, bytes[offset..offset + part.len()]);
        offset += part.len();
    }
    assert_eq!(offset, bytes.len());
    f.close().await;
}

#[tokio::test]
async fn cancelling_a_pin_releases_partial_budget_exemptions() {
    let mut f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("cancel-pin", vec![70; CHUNK * 40]).await;
    f.storage
        .set_faults(coven_storage::test_utils::Faults {
            delay: Duration::from_millis(100),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    let mut requests = f.storage.subscribe_requests();
    let before = *requests.borrow_and_update();
    let files = f.files.clone();
    let reference = file.clone();
    let task = tokio::spawn(async move { files.pin(&[reference]).await });
    // A range download has started for the whole-file pin.
    tokio::time::timeout(
        Duration::from_secs(20),
        requests.wait_for(|count| *count > before + 1),
    )
    .await
    .unwrap()
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    f.reopen().await;
    f.files.set_cache_budget("files", 0).await.unwrap();
    assert_eq!(
        f.files.inner.database.missing_bytes(&file).await.unwrap(),
        file.size()
    );
    f.close().await;
}

#[path = "file_cache_tests.rs"]
mod cache;
#[path = "file_status_tests.rs"]
mod status;
#[path = "file_upload_pool_tests.rs"]
mod upload_pool;
#[path = "file_upload_retry_tests.rs"]
mod upload_retry;
#[path = "file_upload_tests.rs"]
mod uploads;

#[tokio::test]
async fn whole_reads_and_pins_check_the_records_hash_after_authenticating_chunks() {
    let a = Fixture::new(Provenance::AppProvided).await;
    let uploaded = a.uploaded("hash", vec![73; CHUNK * 2]).await;
    let b = Fixture::new(Provenance::AppProvided).await;
    for mut write in a.database.test_queued_writes().await.unwrap() {
        for part in &mut write.parts {
            for row in &mut part.rows {
                if let coven_merge::Operation::Insert(columns)
                | coven_merge::Operation::Update(columns) = &mut row.change.operation
                {
                    // The completing write is the one storing the file.
                    if let Some(hash) = columns
                        .get_mut("hash")
                        .filter(|hash| hash.value != coven_format::value::Value::Null)
                    {
                        hash.value = coven_format::value::Value::Blob(vec![0; 32]);
                    }
                }
            }
        }
        crate::tests::apply_write(&b.database, write).await.unwrap();
    }
    let files = Files::new(
        FileDatabase::new(b.database.clone()),
        b.directory.clone(),
        Some(Arc::new(
            a.storage.with_transfers(b.files.inner.transfers.clone()),
        )),
        b.send.clone(),
        b.files.inner.waits.clone(),
        b.ids.clone(),
        b.files.inner.transfers.clone(),
    );
    let file = b.database.file_ref(uploaded.id()).await.unwrap();
    let stream = files.open_file_stream(&file).await.unwrap();
    assert_eq!(stream.read_at(23, 1).await.unwrap(), vec![73]);
    assert!(matches!(
        files.read_file(&file).await,
        Err(FileReadError::Integrity { .. })
    ));
    // The read's caller has the refusal; only a pin's is recorded.
    assert!(b
        .database
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .is_empty());
    assert!(matches!(
        files.pin(std::slice::from_ref(&file)).await,
        Err(FileReadError::Integrity { .. })
    ));
    let records = b.database.pending::<crate::PendingRecord>().await.unwrap();
    assert!(matches!(
        records.as_slice(),
        [crate::PendingRecord {
            reason: crate::PendingReason::Refused(crate::Refusal::ContentHash),
            ..
        }]
    ));
    assert_eq!(stream.read_at(23, 1).await.unwrap(), vec![73]);
    assert_eq!(
        b.database.pending::<crate::PendingRecord>().await.unwrap(),
        records,
        "a verified range cannot settle a whole-file hash refusal"
    );
    assert_eq!(
        b.database.files_pinned(vec![file.id()]).await.unwrap(),
        vec![Some(false)]
    );
    files.close().await;
    b.close().await;
    a.close().await;
}

#[tokio::test]
async fn a_file_from_a_newer_format_requests_an_update() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("future", vec![91; CHUNK]).await;
    let path = f.path(&file);
    let mut bytes = f.storage.read(&path).await.unwrap();
    bytes[1..3].copy_from_slice(&(coven_format::FORMAT_VERSION + 1).to_be_bytes());
    f.storage.delete(&path).await.unwrap();
    f.storage.create(&path, &bytes).await.unwrap();
    // The first read fetches the header with chunk zero and rejects both.
    let stream = f.files.open_file_stream(&file).await.unwrap();
    let error = match stream.read_at(0, 1).await {
        Ok(_) => panic!("future format was accepted"),
        Err(error) => error,
    };
    assert!(matches!(error, FileReadError::UpdateRequired(_)), "{error}");
    for index in [-1, 0] {
        assert!(f
            .files
            .inner
            .database
            .cached(&file, index)
            .await
            .unwrap()
            .is_none());
    }
    f.close().await;
}

/// §17.6: a one-byte uncached file read from offset zero fetches its header
/// and its chunk in one request, caches both, and a later open reads nothing.
#[tokio::test]
async fn a_first_read_from_zero_fetches_the_header_with_the_first_chunks() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("byte", vec![93]).await;
    let requests = f.storage.request_count();
    let stream = f.files.open_file_stream(&file).await.unwrap();
    assert_eq!(f.storage.request_count(), requests);
    assert_eq!(stream.read_at(0, 1).await.unwrap(), [93]);
    let ranges = f.storage.ranges().await;
    assert_eq!(ranges.len(), 1);
    assert_eq!((ranges[0].start(), ranges[0].len()), (0, 11 + 1 + 16));
    for index in [-1, 0] {
        assert!(f
            .files
            .inner
            .database
            .cached(&file, index)
            .await
            .unwrap()
            .is_some());
    }
    let requests = f.storage.request_count();
    assert_eq!(f.files.read_file(&file).await.unwrap(), [93]);
    assert_eq!(f.storage.request_count(), requests);
    f.close().await;
}

/// A second device receives the staging write with the file's record, on
/// the staging device only: nothing there can read it, and nothing waits for
/// it. The staging device reads it from its source and reports it uploading.
/// Once the upload completes, one more write switches the record to the
/// cloud, and the second device reads it at once (§17.3, §17.5).
#[tokio::test]
async fn a_file_is_on_its_device_only_until_its_upload_completes() {
    for provenance in [Provenance::AppProvided, Provenance::UserProvided] {
        let a = Fixture::new(provenance.clone()).await;
        let b = Fixture::new(provenance.clone()).await;
        a.files
            .set_storage(None, std::future::ready(Ok(())))
            .await
            .unwrap();
        a.files.set_uploads_paused(false).await.unwrap();
        let local = match provenance {
            Provenance::AppProvided => a.attach("files", "waiting", b"content".to_vec()).await,
            Provenance::UserProvided => a.original("waiting", b"content").await,
        };
        let FilePlace::DeviceOnly { device } = local.place() else {
            panic!("a staged file is on its device only");
        };
        let queued = a.files.inner.database.uploads().await.unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].files, [local.id()]);
        assert!(queued[0].retry.is_none());
        assert_eq!(a.upload_reason(&local).await, None);
        // The staging device reads it from its source, with no storage
        // connected.
        assert_eq!(a.files.read_file(&local).await.unwrap(), b"content");
        assert_eq!(
            a.files.file_status(&local).await.unwrap(),
            FileStatus::Uploading
        );
        let staging = a.database.test_queued_writes().await.unwrap();
        for write in staging.clone() {
            crate::tests::apply_write(&b.database, write).await.unwrap();
        }
        // The second device has the record, on the staging device only.
        let remote = b.database.file_ref(local.id()).await.unwrap();
        assert_eq!(remote.place(), FilePlace::DeviceOnly { device });
        assert_eq!(
            b.files.file_status(&remote).await.unwrap(),
            FileStatus::DeviceOnly { device }
        );
        assert!(matches!(
            b.files.read_file(&remote).await,
            Err(FileReadError::DeviceOnly { .. })
        ));
        assert!(matches!(
            b.files.pin(std::slice::from_ref(&remote)).await,
            Err(FileReadError::DeviceOnly { .. })
        ));
        assert!(b.files.inner.database.uploads().await.unwrap().is_empty());
        let mut uploads = a.subscribe_queue();
        a.files
            .set_storage(Some(a.storage.clone()), std::future::ready(Ok(())))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !uploads
                .next()
                .await
                .expect("the live query answers")
                .unwrap()
                .files
                .is_empty()
            {}
        })
        .await
        .unwrap();
        // Completion queued exactly one more write, storing the same file.
        let all = a.database.test_queued_writes().await.unwrap();
        assert_eq!(all.len(), staging.len() + 1);
        let stored_here = a.database.file_ref(local.id()).await.unwrap();
        assert!(matches!(stored_here.place(), FilePlace::Cloud { .. }));
        for write in all.into_iter().skip(staging.len()) {
            crate::tests::apply_write(&b.database, write).await.unwrap();
        }
        b.files
            .set_storage(
                Some(Arc::new(
                    a.storage.with_transfers(b.files.inner.transfers.clone()),
                )),
                std::future::ready(Ok(())),
            )
            .await
            .unwrap();
        let uploaded = b.database.file_ref(local.id()).await.unwrap();
        assert_eq!(uploaded, stored_here);
        assert_eq!(
            b.files.file_status(&uploaded).await.unwrap(),
            FileStatus::Available
        );
        assert_eq!(b.files.read_file(&uploaded).await.unwrap(), b"content");
        assert!(b.files.inner.database.uploads().await.unwrap().is_empty());
        b.files.pin(std::slice::from_ref(&uploaded)).await.unwrap();
        b.files
            .unpin(std::slice::from_ref(&uploaded))
            .await
            .unwrap();
        b.files.evict_file(&uploaded).await.unwrap();
        assert_eq!(b.database.file_ref(local.id()).await.unwrap(), uploaded);
        assert_eq!(b.files.read_file(&uploaded).await.unwrap(), b"content");
        b.close().await;
        a.close().await;
    }
}

impl Files {
    pub(crate) async fn stop_background_for_counts(&self) {
        // Isolate explicit runs and transfers from the independent workers,
        // retaining the real file owner and its database operations.
        self.worker.stop.send_replace(true);
        for task in std::mem::take(&mut *self.worker.tasks.lock().await) {
            join(task).await;
        }
    }
}

/// The fixture's file owner, with uploads paused before its worker starts.
/// The worker drains once as it starts, reading the persisted pause; paused
/// only afterwards, that drain could read the setting first and then upload a
/// file the test queues meanwhile.
async fn paused_files(
    database: &Database,
    directory: &StoreDir,
    storage: Arc<dyn Storage>,
    send: Arc<crate::SyncState>,
    waits: Arc<crate::RetryWaits>,
    ids: IdSourceRef,
    transfers: Arc<coven_storage::TransferBudget>,
) -> Files {
    FileDatabase::new(database.clone())
        .set_uploads_paused(true)
        .await
        .unwrap();
    let files = Files::new(
        FileDatabase::new(database.clone()),
        directory.clone(),
        Some(storage),
        send,
        waits,
        ids,
        transfers,
    );
    // The owner's own copy of the setting, which parts check between sends.
    files.set_uploads_paused(true).await.unwrap();
    files
}

fn ready_state() -> Arc<crate::SyncState> {
    let state = Arc::new(crate::SyncState::new());
    state.registered(true);
    state.set(crate::SequenceStatus::Synced { through: 0 });
    state
}
