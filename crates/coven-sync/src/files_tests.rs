use super::*;
use coven_database::{
    CacheFill, Database, DatabaseBuilder, FileDecl, FileRef, Migration, Provenance, RowIdentity,
    SyncedTable,
};
use coven_foundation::{
    clock::{ClockRef, ClosureClock},
    files::StoreLayout,
    id_source::{StoreId, UuidIds},
};
use coven_storage::test_utils::MemoryStorage;
use std::time::{Duration, SystemTime};

const CHUNK: usize = 64 * 1024;
#[path = "snapshot_file_retention_tests.rs"]
mod retention;
struct Fixture {
    root: tempfile::TempDir,
    directory: StoreDir,
    ids: IdSourceRef,
    clock: ClockRef,
    send: Arc<crate::SendGate>,
    database: Database,
    files: Files,
    storage: Arc<MemoryStorage>,
    declarations: Vec<SyncedTable>,
}
impl Fixture {
    async fn new(provenance: Provenance, fill: CacheFill) -> Self {
        let clock: ClockRef = Arc::new(ClosureClock(|| {
            SystemTime::UNIX_EPOCH + Duration::from_secs(100)
        }));
        Self::with_clock(provenance, fill, clock).await
    }
    async fn with_clock(provenance: Provenance, fill: CacheFill, clock: ClockRef) -> Self {
        let root = tempfile::tempdir().unwrap();
        let ids: IdSourceRef = Arc::new(UuidIds);
        let layout = StoreLayout::new(root.path().to_owned());
        let directory = layout
            .create_store_dir(StoreId(uuid::Uuid::from_u128(1)), "files", ids.as_ref())
            .unwrap();
        let declarations = vec![
            SyncedTable::new("files", RowIdentity::SharedKey)
                .carries_files(FileDecl::new("files", provenance, fill)),
            SyncedTable::new("other", RowIdentity::SharedKey).carries_files(FileDecl::new(
                "other",
                Provenance::AppProvided,
                CacheFill::CacheLazy,
            )),
        ];
        let database = builder(&directory, &ids, &clock, &declarations)
            .open()
            .await
            .unwrap();
        let storage = Arc::new(
            MemoryStorage::builder()
                .provider(coven_storage::CloudProvider::Dropbox)
                .clock(clock.clone())
                .transfer_limits(CHUNK as u64, CHUNK)
                .build()
                .unwrap(),
        );
        let send = completed_send_gate(clock.clone());
        let waits = Arc::new(
            crate::RetryWaits::open(database.clone(), clock.clone())
                .await
                .unwrap(),
        );
        let storage = Arc::new(
            storage
                .with_publication_gate(send.clone())
                .with_cooldown(waits.clone()),
        );
        let files = Files::new(
            FileDatabase::new(database.clone()),
            directory.clone(),
            Some(storage.clone()),
            send.clone(),
            waits,
            ids.clone(),
            crate::TransferLimits::default(),
        );
        files.set_uploads_paused(true).await.unwrap();
        Self {
            root,
            directory,
            ids,
            clock,
            send,
            database,
            files,
            storage,
            declarations,
        }
    }
    async fn reopen(&mut self) {
        self.files.close().await;
        self.database.close().await.unwrap();
        self.database = builder(&self.directory, &self.ids, &self.clock, &self.declarations)
            .open()
            .await
            .unwrap();
        self.send = completed_send_gate(self.clock.clone());
        let waits = Arc::new(
            crate::RetryWaits::open(self.database.clone(), self.clock.clone())
                .await
                .unwrap(),
        );
        self.storage = Arc::new(
            self.storage
                .with_publication_gate(self.send.clone())
                .with_cooldown(waits.clone()),
        );
        self.files = Files::new(
            FileDatabase::new(self.database.clone()),
            self.directory.clone(),
            Some(self.storage.clone()),
            self.send.clone(),
            waits,
            self.ids.clone(),
            crate::TransferLimits::default(),
        );
        self.files.set_uploads_paused(true).await.unwrap();
    }
    async fn attach(&self, table: &str, id: &str, bytes: Vec<u8>) -> FileRef {
        let namespace = table.to_owned();
        let fileid = id.to_owned();
        let inserted = fileid.clone();
        let table = table.to_owned();
        let name = table.clone();
        self.database
            .write_with_files::<_, _, _, coven_database::DbError>(
                move |batch| {
                    batch.put_file(namespace, fileid, bytes);
                    Ok(())
                },
                move |sql| {
                    sql.execute(
                        &format!("INSERT INTO {table}(id) VALUES(?1) ON CONFLICT(id) DO NOTHING"),
                        [inserted],
                    )?;
                    Ok(())
                },
            )
            .await
            .unwrap();
        self.database.file_ref(&name, id).await.unwrap()
    }
    async fn original(&self, id: &str, bytes: &[u8]) -> FileRef {
        let path = self.root.path().join(id);
        std::fs::write(&path, bytes).unwrap();
        let prepared = coven_database::prepare_user_file(&path, |_| {})
            .await
            .unwrap();
        let name = id.to_owned();
        let size = bytes.len() as i64;
        self.database
            .write(move |sql| {
                sql.execute("INSERT INTO files(id,size) VALUES(?1,?2)", (&name, size))?;
                sql.register_user_file("files", name.as_str(), prepared)?;
                Ok(())
            })
            .await
            .unwrap();
        self.database.file_ref("files", id).await.unwrap()
    }
    async fn drain(&self) {
        self.files.set_uploads_paused(false).await.unwrap();
        self.files.retry_uploads_now().await.unwrap();
        for upload in self.queue().await.files {
            assert_eq!(self.upload_reason(&upload.file).await, None);
        }
    }
    async fn uploaded(&self, id: &str, bytes: Vec<u8>) -> FileRef {
        let attached = self.attach("files", id, bytes).await;
        self.drain().await;
        // Upload completion changes no row: the attaching write's reference stays.
        let file = self.database.file_ref("files", id).await.unwrap();
        assert_eq!(file, attached);
        assert!(!self.queued(&file).await);
        file
    }
    async fn queued(&self, file: &FileRef) -> bool {
        self.files
            .inner
            .database
            .uploads()
            .await
            .unwrap()
            .iter()
            .any(|upload| upload.file == *file)
    }
    /// The committed queue as the app reads it (E7).
    async fn queue(&self) -> UploadQueue {
        UploadQueue::from_state(self.database.upload_queue().await.unwrap()).unwrap()
    }
    fn subscribe_queue(&self) -> coven_database::LiveQuery<UploadQueue> {
        self.database
            .subscribe_upload_queue()
            .process(UploadQueue::from_state)
    }
    /// This device's pending reason about one of its queued files.
    async fn upload_reason(&self, file: &FileRef) -> Option<crate::PendingReason> {
        let subject = crate::PendingSubject::File {
            device: file.uploader(),
            file: file.id(),
        };
        self.database
            .pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.subject == subject && record.reported_by == file.uploader())
            .map(|record| record.reason)
    }
    fn path(&self, file: &FileRef) -> coven_storage::ObjectPath {
        coven_storage::ObjectPath::file(self.directory.id(), file.uploader(), file.id())
    }
    async fn close(&self) {
        self.files.close().await;
        self.database.close().await.unwrap();
    }
}
/// Run the storage model's files runner over JSON cases.
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

fn builder(
    directory: &StoreDir,
    ids: &IdSourceRef,
    clock: &ClockRef,
    tables: &[SyncedTable],
) -> DatabaseBuilder {
    DatabaseBuilder::new(directory.clone()).id_source(ids.clone()).clock(clock.clone()).synced_tables(tables.to_vec()).migrations(vec![Migration::sql(1,"files","CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT); CREATE TABLE other(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT)")])
}

#[tokio::test]
async fn ranges_authenticate_cache_batch_and_fail_offline() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let bytes = (0..CHUNK * 40 + 31)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<_>>();
    let file = f.uploaded("one", bytes.clone()).await;
    let stream = f.files.open_file_stream(&file).await.unwrap();
    assert_eq!(f.storage.ranges().await.len(), 1);
    assert_eq!(f.storage.ranges().await[0].start(), 0);
    assert_eq!(f.storage.ranges().await[0].len(), 11);
    assert_eq!(
        stream.read_at(CHUNK as u64 * 4 + 5, 19).await.unwrap(),
        bytes[CHUNK * 4 + 5..CHUNK * 4 + 24]
    );
    let ranges = f.storage.ranges().await;
    assert_eq!(ranges.len(), 2);
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
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let bytes = vec![51; CHUNK * 3 + 21];
    let file = f.uploaded("first", bytes.clone()).await;
    let second = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    for write in f.database.test_queued_writes().await.unwrap() {
        second
            .database
            .apply_downloaded::<crate::PendingRecord>(
                write.into(),
                coven_format::value::StorageTime::UNIX_EPOCH,
            )
            .await
            .unwrap();
    }
    let files = Files::new(
        FileDatabase::new(second.database.clone()),
        second.directory.clone(),
        Some(f.storage.clone()),
        second.send.clone(),
        second.files.inner.waits.clone(),
        second.ids.clone(),
        crate::TransferLimits::default(),
    );
    let ref2 = second.database.file_ref("files", "first").await.unwrap();
    assert_eq!(files.read_file(&ref2).await.unwrap(), bytes);
    f.files.set_cache_budget("files", 1).await.unwrap();
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    assert_eq!(
        f.database
            .rows_pinned("files", vec!["first".into()])
            .await
            .unwrap(),
        vec![Some(true)]
    );
    let progress = f.database.pin_progress(vec![file.clone()]).await.unwrap();
    assert_eq!(progress.files_completed, 1);
    assert_eq!(progress.bytes_cached, file.plaintext_size());
    let requests = f.storage.request_count();
    assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
    assert_eq!(f.storage.request_count(), requests);
    f.files.unpin(std::slice::from_ref(&file)).await.unwrap();
    assert_eq!(
        f.database
            .rows_pinned("files", vec!["first".into()])
            .await
            .unwrap(),
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
async fn eviction_uses_chunk_recency_and_keeps_namespace_budgets_independent() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = f.uploaded("one", vec![67; CHUNK * 4]).await;
    f.attach("other", "other", vec![68; CHUNK]).await;

    f.drain().await;
    let other = f.database.file_ref("other", "other").await.unwrap();
    f.files.read_file(&other).await.unwrap();
    let stream = f.files.open_file_stream(&file).await.unwrap();
    f.files
        .set_cache_budget("files", 11 + 2 * (CHUNK as u64 + 16))
        .await
        .unwrap();
    for index in [0, 1, 0, 2] {
        stream.read_at(index * CHUNK as u64, 1).await.unwrap();
    }
    assert!(f
        .files
        .inner
        .database
        .cached(&file, 0)
        .await
        .unwrap()
        .is_some());
    assert!(f
        .files
        .inner
        .database
        .cached(&file, 1)
        .await
        .unwrap()
        .is_none());
    assert!(f
        .files
        .inner
        .database
        .cached(&file, 2)
        .await
        .unwrap()
        .is_some());
    assert!(f
        .files
        .inner
        .database
        .cached(&file, -1)
        .await
        .unwrap()
        .is_some());
    let requests = f.storage.request_count();
    f.files.read_file(&other).await.unwrap();
    assert_eq!(f.storage.request_count(), requests);
    f.close().await;
}

#[tokio::test]
async fn sequential_reading_starts_read_ahead_and_streams_a_whole_hash_check() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
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
    let mut f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
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
        file.plaintext_size()
    );
    f.close().await;
}

#[path = "file_cache_tests.rs"]
mod cache;
#[path = "file_status_tests.rs"]
mod status;
#[path = "file_upload_retry_tests.rs"]
mod upload_retry;
#[path = "file_upload_tests.rs"]
mod uploads;

#[tokio::test]
async fn opening_a_file_respects_a_zero_cache_budget_including_empty_files() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    f.files.set_cache_budget("files", 0).await.unwrap();
    for bytes in [Vec::new(), vec![71; CHUNK]] {
        let id = format!("length-{}", bytes.len());
        let file = f.uploaded(&id, bytes.clone()).await;
        let stream = f.files.open_file_stream(&file).await.unwrap();
        assert!(f
            .files
            .inner
            .database
            .cached(&file, -1)
            .await
            .unwrap()
            .is_none());
        assert_eq!(stream.read_at(0, bytes.len() as u64).await.unwrap(), bytes);
        assert!(f
            .files
            .inner
            .database
            .cached(&file, -1)
            .await
            .unwrap()
            .is_none());
    }
    f.close().await;
}

#[tokio::test]
async fn whole_reads_and_pins_check_the_row_hash_after_authenticating_chunks() {
    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    a.uploaded("hash", vec![73; CHUNK * 2]).await;
    let b = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    for mut write in a.database.test_queued_writes().await.unwrap() {
        for part in &mut write.parts {
            for row in &mut part.rows {
                // The attaching write is the only write carrying the file.
                if let coven_merge::Operation::Insert(columns) = &mut row.change.operation {
                    columns.get_mut("hash").unwrap().value =
                        coven_format::value::Value::Blob(vec![0; 32]);
                }
            }
        }
        b.database
            .apply_downloaded::<crate::PendingRecord>(
                write.into(),
                coven_format::value::StorageTime::UNIX_EPOCH,
            )
            .await
            .unwrap();
    }
    let files = Files::new(
        FileDatabase::new(b.database.clone()),
        b.directory.clone(),
        Some(a.storage.clone()),
        b.send.clone(),
        b.files.inner.waits.clone(),
        b.ids.clone(),
        crate::TransferLimits::default(),
    );
    let file = b.database.file_ref("files", "hash").await.unwrap();
    let stream = files.open_file_stream(&file).await.unwrap();
    assert_eq!(stream.read_at(23, 1).await.unwrap(), vec![73]);
    assert!(matches!(
        files.read_file(&file).await,
        Err(FileReadError::Integrity { .. })
    ));
    assert!(matches!(
        files.pin(std::slice::from_ref(&file)).await,
        Err(FileReadError::Integrity { .. })
    ));
    assert_eq!(
        b.database
            .rows_pinned("files", vec!["hash".into()])
            .await
            .unwrap(),
        vec![Some(false)]
    );
    files.close().await;
    b.close().await;
    a.close().await;
}

#[tokio::test]
async fn pinning_preserves_cached_tail_chunks_while_assembling_the_whole_file() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = f.uploaded("cached-tail", vec![74; CHUNK * 4]).await;
    f.files
        .set_cache_budget("files", 11 + 2 * (CHUNK as u64 + 16))
        .await
        .unwrap();
    let stream = f.files.open_file_stream(&file).await.unwrap();
    stream
        .read_at(3 * CHUNK as u64, CHUNK as u64)
        .await
        .unwrap();
    // Only the cached tail is verified before pinning; the pin publishes the
    // rest with it as one checked file.
    let before = f.database.pin_progress(vec![file.clone()]).await.unwrap();
    assert_eq!(before.bytes_total, 4 * CHUNK as u64);
    assert_eq!(before.bytes_cached, CHUNK as u64);
    assert_eq!(before.files_completed, 0);
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    let after = f.database.pin_progress(vec![file]).await.unwrap();
    assert_eq!(after.bytes_cached, 4 * CHUNK as u64);
    assert_eq!(after.files_completed, 1);
    f.close().await;
}

#[tokio::test]
async fn unused_headers_do_not_displace_recently_read_chunks() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let old = f.uploaded("old-header", vec![75]).await;
    f.files.open_file_stream(&old).await.unwrap();
    let recent = f.uploaded("recent", vec![76; CHUNK]).await;
    f.files
        .set_cache_budget("files", 11 + CHUNK as u64 + 16)
        .await
        .unwrap();
    f.files
        .open_file_stream(&recent)
        .await
        .unwrap()
        .read_at(0, 1)
        .await
        .unwrap();
    assert!(f
        .files
        .inner
        .database
        .cached(&recent, 0)
        .await
        .unwrap()
        .is_some());
    assert!(f
        .files
        .inner
        .database
        .cached(&recent, -1)
        .await
        .unwrap()
        .is_some());
    assert!(f
        .files
        .inner
        .database
        .cached(&old, -1)
        .await
        .unwrap()
        .is_none());
    f.close().await;
}

#[tokio::test]
async fn a_file_from_a_newer_format_requests_an_update() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = f.uploaded("future", vec![91; CHUNK]).await;
    let path = f.path(&file);
    let mut bytes = f.storage.read(&path).await.unwrap();
    bytes[1..3].copy_from_slice(&(coven_format::FORMAT_VERSION + 1).to_be_bytes());
    f.storage.delete(&path).await.unwrap();
    f.storage.create(&path, &bytes).await.unwrap();
    let error = match f.files.open_file_stream(&file).await {
        Ok(_) => panic!("future format was accepted"),
        Err(error) => error,
    };
    assert!(matches!(error, FileReadError::UpdateRequired(_)), "{error}");
    f.close().await;
}

#[tokio::test]
async fn waiting_files_name_the_attaching_device_until_its_automatic_upload() {
    for provenance in [Provenance::AppProvided, Provenance::UserProvided] {
        let a = Fixture::new(provenance.clone(), CacheFill::CacheLazy).await;
        let b = Fixture::new(provenance.clone(), CacheFill::CacheLazy).await;
        a.files
            .set_storage(None, std::future::ready(Ok(())))
            .await
            .unwrap();
        a.files.set_uploads_paused(false).await.unwrap();
        let local = match provenance {
            Provenance::AppProvided => a.attach("files", "waiting", b"content".to_vec()).await,
            Provenance::UserProvided => a.original("waiting", b"content").await,
        };
        let device = local.uploader();
        a.files.sync_files().await.unwrap();
        let queued = a.files.inner.database.uploads().await.unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].file, local);
        assert!(queued[0].retry.is_none());
        assert_eq!(a.upload_reason(&local).await, None);
        let attaching = a.database.test_queued_writes().await.unwrap();
        for write in attaching.clone() {
            b.database
                .apply_downloaded::<crate::PendingRecord>(
                    write.into(),
                    coven_format::value::StorageTime::UNIX_EPOCH,
                )
                .await
                .unwrap();
        }
        let remote = b.database.file_ref("files", "waiting").await.unwrap();
        assert_eq!((remote.uploader(), remote.id()), (device, local.id()));
        // B's storage confirms absence: the file is still uploading on A.
        assert!(matches!(b.files.read_file(&remote).await,
            Err(FileReadError::Uploading { device: owner, .. }) if owner == device));
        assert!(b.files.inner.database.uploads().await.unwrap().is_empty());
        let mut uploads = a.subscribe_queue();
        a.files
            .set_storage(Some(a.storage.clone()), std::future::ready(Ok(())))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !uploads.next().await.unwrap().files.is_empty() {}
        })
        .await
        .unwrap();
        // Completion queued no write: B's row and reference stay as they were.
        assert_eq!(
            a.database.test_queued_writes().await.unwrap().len(),
            attaching.len()
        );
        assert_eq!(
            a.database.file_ref("files", "waiting").await.unwrap(),
            local
        );
        b.files
            .set_storage(Some(a.storage.clone()), std::future::ready(Ok(())))
            .await
            .unwrap();
        let uploaded = b.database.file_ref("files", "waiting").await.unwrap();
        assert_eq!(uploaded, remote);
        assert_eq!(b.files.read_file(&uploaded).await.unwrap(), b"content");
        assert!(b.files.inner.database.uploads().await.unwrap().is_empty());
        b.files.pin(std::slice::from_ref(&uploaded)).await.unwrap();
        b.files
            .unpin(std::slice::from_ref(&uploaded))
            .await
            .unwrap();
        b.files.evict_file(&uploaded).await.unwrap();
        assert_eq!(
            b.database.file_ref("files", "waiting").await.unwrap(),
            uploaded
        );
        assert_eq!(b.files.read_file(&uploaded).await.unwrap(), b"content");
        b.close().await;
        a.close().await;
    }
}

impl Files {
    pub(crate) async fn stop_background_for_counts(&self) {
        // Isolate pass work from the independent file worker, retaining the real
        // file owner used by sync_pass and explicit file consumers.
        self.worker.stop.send_replace(true);
        if let Some(task) = self.worker.task.lock().await.take() {
            join(task).await;
        }
    }
}

fn completed_send_gate(clock: ClockRef) -> Arc<crate::SendGate> {
    let send = Arc::new(crate::SendGate::new(clock));
    send.begin();
    send.complete(crate::send_gate::Binding::Device, Vec::new())
        .unwrap();
    send
}
