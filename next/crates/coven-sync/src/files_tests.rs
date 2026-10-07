use super::*;
use coven_database::{
    CacheFill, CovenMigrationPolicy, Database, DatabaseBuilder, FileDecl, FileRef, Migration,
    Provenance, RowIdentity, SyncedTable, Uploads,
};
use coven_foundation::{
    clock::FixedClock,
    files::StoreLayout,
    id_source::{StoreId, UuidIds},
};
use coven_storage::{test_utils::MemoryStorage, StorageConfig};
use std::time::{Duration, SystemTime};

const CHUNK: usize = 64 * 1024;
struct Fixture {
    root: tempfile::TempDir,
    directory: StoreDir,
    ids: IdSourceRef,
    clock: ClockRef,
    database: Database,
    files: Files,
    storage: Arc<MemoryStorage>,
    declarations: Vec<SyncedTable>,
}
impl Fixture {
    async fn new(provenance: Provenance, uploads: Uploads, fill: CacheFill) -> Self {
        let root = tempfile::tempdir().unwrap();
        let ids: IdSourceRef = Arc::new(UuidIds);
        let clock: ClockRef = Arc::new(FixedClock::new(
            SystemTime::UNIX_EPOCH + Duration::from_secs(100),
        ));
        let layout = StoreLayout::new(root.path().to_owned());
        let directory = layout
            .create_store_dir(StoreId(ids.new_id()), "files", ids.as_ref())
            .unwrap();
        let declarations = vec![
            SyncedTable::new("files", RowIdentity::SharedKey)
                .carries_files(FileDecl::new("files", provenance, uploads, fill)),
            SyncedTable::new("other", RowIdentity::SharedKey).carries_files(FileDecl::new(
                "other",
                Provenance::AppProvided,
                Uploads::WhenAsked,
                CacheFill::CacheLazy,
            )),
        ];
        let database = builder(&directory, &ids, &clock, &declarations)
            .open()
            .await
            .unwrap();
        let storage = Arc::new(
            MemoryStorage::new(
                StorageConfig::Dropbox {
                    namespace_id: "files".into(),
                },
                clock.clone(),
            )
            .unwrap()
            .with_transfer_limits(CHUNK as u64, CHUNK)
            .unwrap(),
        );
        let files = Files::new(
            FileDatabase::new(database.clone()),
            directory.clone(),
            Some(storage.clone()),
            clock.clone(),
            ids.clone(),
        );
        files.set_uploads_paused(true);
        Self {
            root,
            directory,
            ids,
            clock,
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
        self.files = Files::new(
            FileDatabase::new(self.database.clone()),
            self.directory.clone(),
            Some(self.storage.clone()),
            self.clock.clone(),
            self.ids.clone(),
        );
        self.files.set_uploads_paused(true);
    }
    async fn attach(&self, table: &str, id: &str, bytes: Vec<u8>) -> FileRef {
        let namespace = table.to_owned();
        let fileid = id.to_owned();
        let inserted = fileid.clone();
        let table = table.to_owned();
        let name = table.clone();
        self.database
            .write_with_files(
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
    async fn enqueue(&self, file: &FileRef) {
        self.files
            .inner
            .database
            .enqueue(std::slice::from_ref(file), self.clock.now())
            .await
            .unwrap();
    }
    async fn drain(&self) {
        self.files.inner.state.lock().unwrap().paused = false;
        let result = self.files.retry_uploads_now().await.unwrap();
        if let DrainOutcome::Drained { failures, .. } = result {
            assert!(failures.is_empty(), "{failures:?}");
        }
    }
    async fn uploaded(&self, id: &str, bytes: Vec<u8>) -> FileRef {
        let file = self.attach("files", id, bytes).await;
        self.enqueue(&file).await;
        self.drain().await;
        let file = self.database.file_ref("files", id).await.unwrap();
        assert_eq!(file.location(), coven_database::FileLocation::Uploaded);
        file
    }
    async fn close(&self) {
        self.files.close().await;
        self.database.close().await.unwrap();
    }
}
fn builder(
    directory: &StoreDir,
    ids: &IdSourceRef,
    clock: &ClockRef,
    tables: &[SyncedTable],
) -> DatabaseBuilder {
    DatabaseBuilder::new(directory.clone()).id_source(ids.clone()).clock(clock.clone()).synced_tables(tables.to_vec()).migrations(vec![Migration::sql(1,"files","CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT); CREATE TABLE other(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT)")]).coven_migration_policy(CovenMigrationPolicy::ApplyPending)
}

#[tokio::test]
async fn ranges_authenticate_cache_batch_and_fail_offline() {
    let f = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let bytes = (0..CHUNK * 40 + 31)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<_>>();
    let file = f.uploaded("one", bytes.clone()).await;
    let stream = f.files.open_file_stream(&file).await.unwrap();
    assert_eq!(f.storage.ranges().await.len(), 1);
    assert_eq!(
        stream.read_at(CHUNK as u64 * 4 + 5, 19).await.unwrap(),
        bytes[CHUNK * 4 + 5..CHUNK * 4 + 24]
    );
    let ranges = f.storage.ranges().await;
    assert_eq!(ranges.len(), 2);
    assert_eq!(ranges[1].start(), 15 + 4 * (CHUNK as u64 + 16));
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
    f.storage
        .set_faults(coven_storage::test_utils::Faults {
            fail_next: 100,
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    assert!(matches!(
        stream.read_at(0, 1).await,
        Err(FileReadError::Offline { .. })
    ));
    assert_eq!(
        stream.read_at(CHUNK as u64 * 4, 1).await.unwrap(),
        bytes[CHUNK * 4..CHUNK * 4 + 1]
    );
    f.storage
        .set_faults(coven_storage::test_utils::Faults::none())
        .await;
    let path = coven_storage::ObjectPath::file(file.uploaded().unwrap().unwrap().0);
    f.storage
        .corrupt_byte(&path, 15 + CHUNK + 16 + 1)
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
    let f = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let bytes = vec![51; CHUNK * 3 + 21];
    let file = f.uploaded("first", bytes.clone()).await;
    let second = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    for write in f.database.test_queued_writes().await.unwrap() {
        second
            .database
            .apply_downloaded(write.into())
            .await
            .unwrap();
    }
    let files = Files::new(
        FileDatabase::new(second.database.clone()),
        second.directory.clone(),
        Some(f.storage.clone()),
        second.clock.clone(),
        second.ids.clone(),
    );
    let ref2 = second.database.file_ref("files", "first").await.unwrap();
    assert_eq!(files.read_file(&ref2).await.unwrap(), bytes);
    let progress = Mutex::new(Vec::new());
    f.files.set_cache_budget("files", 1).await.unwrap();
    f.files
        .pin(std::slice::from_ref(&file), &|p| {
            progress.lock().unwrap().push(p)
        })
        .await
        .unwrap();
    assert!(f
        .files
        .is_pinned(std::slice::from_ref(&file))
        .await
        .unwrap());
    assert_eq!(progress.lock().unwrap().last().unwrap().files_completed, 1);
    let requests = f.storage.request_count();
    assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
    assert_eq!(f.storage.request_count(), requests);
    f.files.unpin(std::slice::from_ref(&file)).await.unwrap();
    assert!(!f
        .files
        .is_pinned(std::slice::from_ref(&file))
        .await
        .unwrap());
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
async fn eager_commit_observation_handles_download_apply() {
    let a = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let file = a.uploaded("eager", vec![72; CHUNK * 2]).await;
    let b = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheEager,
    )
    .await;
    b.files.close().await;
    let files = Files::new(
        FileDatabase::new(b.database.clone()),
        b.directory.clone(),
        Some(a.storage.clone()),
        b.clock.clone(),
        b.ids.clone(),
    );
    let mut status = files.subscribe_eager_cache_fill_status();
    for write in a.database.test_queued_writes().await.unwrap() {
        b.database.apply_downloaded(write.into()).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if files.inner.database.missing_bytes(&file).await.unwrap() == 0 {
                break;
            }
            status.changed().await.unwrap();
            if let EagerCacheFillStatus::Failed { error, .. } = &*status.borrow() {
                panic!("{error:?}")
            }
        }
    })
    .await
    .unwrap();
    let requests = a.storage.request_count();
    let reference = b.database.file_ref("files", "eager").await.unwrap();
    assert_eq!(
        files.read_file(&reference).await.unwrap(),
        vec![72; CHUNK * 2]
    );
    assert_eq!(a.storage.request_count(), requests);
    files.close().await;
    b.database.close().await.unwrap();
    a.close().await;
}

#[tokio::test]
async fn eviction_uses_chunk_recency_and_keeps_namespace_budgets_independent() {
    let f = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let file = f.uploaded("one", vec![67; CHUNK * 4]).await;
    let other = f.attach("other", "other", vec![68; CHUNK]).await;
    f.enqueue(&other).await;
    f.drain().await;
    let other = f.database.file_ref("other", "other").await.unwrap();
    f.files.read_file(&other).await.unwrap();
    let stream = f.files.open_file_stream(&file).await.unwrap();
    f.files
        .set_cache_budget("files", 15 + 2 * (CHUNK as u64 + 16))
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
    let f = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
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
                .any(|range| range.start() == 15 + 2 * (CHUNK as u64 + 16))
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
    let mut f = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let file = f.uploaded("cancel-pin", vec![70; CHUNK * 40]).await;
    f.storage
        .set_faults(coven_storage::test_utils::Faults {
            delay: Duration::from_millis(100),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    let downloaded = Arc::new(tokio::sync::Notify::new());
    let signal = downloaded.clone();
    let files = f.files.clone();
    let reference = file.clone();
    let task = tokio::spawn(async move {
        files
            .pin(&[reference], &move |progress| {
                if progress.bytes_downloaded > 0 {
                    signal.notify_one();
                }
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), downloaded.notified())
        .await
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

#[path = "file_upload_tests.rs"]
mod uploads;

#[tokio::test]
async fn opening_a_file_respects_a_zero_cache_budget_including_empty_files() {
    let f = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
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
    let a = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    a.uploaded("hash", vec![73; CHUNK * 2]).await;
    let b = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    for mut write in a.database.test_queued_writes().await.unwrap() {
        for part in &mut write.parts {
            for row in &mut part.rows {
                if let coven_merge::Operation::Update(columns) = &mut row.change.operation {
                    columns.get_mut("hash").unwrap().value =
                        coven_format::value::Value::Blob(vec![0; 32]);
                }
            }
        }
        b.database.apply_downloaded(write.into()).await.unwrap();
    }
    let files = Files::new(
        FileDatabase::new(b.database.clone()),
        b.directory.clone(),
        Some(a.storage.clone()),
        b.clock.clone(),
        b.ids.clone(),
    );
    let file = b.database.file_ref("files", "hash").await.unwrap();
    let stream = files.open_file_stream(&file).await.unwrap();
    assert_eq!(stream.read_at(23, 1).await.unwrap(), vec![73]);
    assert!(matches!(
        files.read_file(&file).await,
        Err(FileReadError::Integrity { .. })
    ));
    assert!(matches!(
        files.pin(std::slice::from_ref(&file), &|_| {}).await,
        Err(FileReadError::Integrity { .. })
    ));
    assert!(!files.is_pinned(&[file]).await.unwrap());
    files.close().await;
    b.close().await;
    a.close().await;
}

#[tokio::test]
async fn pinning_preserves_cached_tail_chunks_while_assembling_the_whole_file() {
    let f = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let file = f.uploaded("cached-tail", vec![74; CHUNK * 4]).await;
    f.files
        .set_cache_budget("files", 15 + 2 * (CHUNK as u64 + 16))
        .await
        .unwrap();
    let stream = f.files.open_file_stream(&file).await.unwrap();
    stream
        .read_at(3 * CHUNK as u64, CHUNK as u64)
        .await
        .unwrap();
    let progress = Mutex::new(Vec::new());
    f.files
        .pin(&[file], &|value| progress.lock().unwrap().push(value))
        .await
        .unwrap();
    {
        let values = progress.lock().unwrap();
        assert_eq!(values[0].bytes_total, 3 * CHUNK as u64);
        assert_eq!(values.last().unwrap().bytes_downloaded, 3 * CHUNK as u64);
    }
    f.close().await;
}

#[tokio::test]
async fn unused_headers_do_not_displace_recently_read_chunks() {
    let f = Fixture::new(
        Provenance::AppProvided,
        Uploads::WhenAsked,
        CacheFill::CacheLazy,
    )
    .await;
    let old = f.uploaded("old-header", vec![75]).await;
    f.files.open_file_stream(&old).await.unwrap();
    let recent = f.uploaded("recent", vec![76; CHUNK]).await;
    f.files
        .set_cache_budget("files", 15 + CHUNK as u64 + 16)
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

#[path = "file_keep_tests.rs"]
mod keeps;
