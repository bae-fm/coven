use crate::*;
use coven_foundation::files::StoreDirAccess;
use std::sync::Arc;

/// The stored path of a file whose record says it is in storage.
fn stored_path(store: StoreId, file: &FileRef) -> ObjectPath {
    match file.place() {
        FilePlace::Cloud { location, .. } => {
            ObjectPath::file(store, location.uploader(), location.object())
        }
        FilePlace::DeviceOnly { .. } => panic!("the file is not stored"),
    }
}

/// One synced table whose rows reference app-provided files.
fn files_table() -> Vec<SyncedTable> {
    vec![SyncedTable::new("files", RowIdentity::SharedKey).file_column(FileColumn::new("file"))]
}

fn files_namespace() -> Vec<Namespace> {
    vec![Namespace::new("files", Provenance::AppProvided, 1 << 30)]
}

const FILES: &str = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT)";

/// Stage one file of these bytes for each row `0..count`, in one write.
async fn stage_rows(
    handle: &CovenHandle,
    count: u8,
    bytes: impl Fn(u8) -> FileSource + Send + 'static,
) -> Vec<FileId> {
    handle
        .write_with_files(
            move |batch| {
                Ok((0..count)
                    .map(|id| batch.put_file("files", bytes(id), crate::Place::Cloud))
                    .collect())
            },
            |sql, files: Vec<FileId>| {
                for (id, file) in files.iter().enumerate() {
                    sql.execute(
                        "INSERT INTO files(id,file) VALUES(?1,?2)",
                        (id.to_string(), file),
                    )?;
                }
                Ok(files)
            },
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn uncached_file_bodies_and_sequence_reads_share_the_download_limit() {
    use coven_storage::{test_utils::MemoryStorage, Storage};
    use std::time::Duration;
    tokio::time::timeout(Duration::from_secs(20), async {
        let root = tempfile::tempdir().unwrap();
        let app = TestCoven::new();
        let memory = Arc::new(MemoryStorage::builder().clock(Arc::new(SystemClock)).build().unwrap());
        let layout = StoreLayout::new(root.path().into());
        let directory = app.create_closed_store(&layout, "Downloads", Arc::new(UuidIds)).await.unwrap();
        let handle = app.builder(layout).storage_connector(memory.clone())
            .synced_tables(files_table())
            .namespaces(files_namespace())
            .migrations(vec![Migration::addition(1, "files", FILES)])
            .open(directory.id(), |_| ()).await.unwrap();
        handle.setup_s3_storage(memory.config(), "Device", "key".into(), SecretText::new("secret".into())).await.unwrap();
        let mut status = handle.subscribe_sync_status();
        status.wait_for(|s| matches!(s, SyncStatus::Synced { .. })).await.unwrap();
        handle.stop_sync();
        status.wait_for(|s| matches!(s, SyncStatus::Stopped)).await.unwrap();
        handle.unlock_store_key().await.unwrap();
        let ids = stage_rows(&handle, 2, |id| vec![id; 2048].into()).await;
        handle.retry_uploads_now().await.unwrap();
        handle.start_sync().await.unwrap();
        status.wait_for(|s| matches!(s, SyncStatus::Synced { .. })).await.unwrap();
        let first_file = handle.file_ref(ids[0]).await.unwrap();
        let second_file = handle.file_ref(ids[1]).await.unwrap();
        let (entered, entering) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        memory.hold_body_at(stored_path(directory.id(), &first_file), 1, entered, resumed).await;
        let first = handle.read_file(&first_file);
        tokio::pin!(first);
        tokio::select! {
            result = &mut first => panic!("first response was not held: {result:?}"),
            result = entering => result.unwrap(),
        }
        let reads = memory.reads().await.len();
        let second = handle.read_file(&second_file);
        tokio::pin!(second);
        handle.sync_now();
        tokio::select! {
            result = &mut second => panic!("second file bypassed the active download: {:?}", result.map(|bytes| bytes.len())),
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
        assert_eq!(memory.reads().await.len(), reads, "sequence and file requests both wait for the active response body");
        resume.send(()).unwrap();
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.unwrap(), vec![0; 2048]);
        assert_eq!(second.unwrap(), vec![1; 2048]);
        status.wait_for(|s| matches!(s, SyncStatus::Synced { .. })).await.unwrap();
        handle.close().await.unwrap();
    }).await.expect("shared downloads finished");
}

#[tokio::test]
async fn transfer_limits_bound_requests_and_a_changed_limit_applies_to_the_next_upload() {
    use coven_storage::{
        test_utils::{Faults, MemoryStorage},
        Storage,
    };
    use std::{
        num::NonZeroUsize,
        time::{Duration, UNIX_EPOCH},
    };
    tokio::time::timeout(Duration::from_secs(20), async {
        let root = tempfile::tempdir().unwrap();
        let app = TestCoven::new();
        let clock = Arc::new(coven_foundation::clock::ClosureClock(|| {
            UNIX_EPOCH + Duration::from_secs(1000)
        }));
        let memory = Arc::new(
            MemoryStorage::builder()
                .clock(clock.clone())
                .transfer_limits(1024 * 1024, 65536)
                .build()
                .unwrap(),
        );
        let layout = StoreLayout::new(root.path().into());
        let directory = app
            .create_closed_store(&layout, "Files", Arc::new(UuidIds))
            .await
            .unwrap();
        let handle = app
            .builder(layout)
            .clock(clock)
            .storage_connector(memory.clone())
            .max_concurrent_uploads(NonZeroUsize::new(2).unwrap())
            .max_concurrent_downloads(NonZeroUsize::new(2).unwrap())
            .synced_tables(files_table())
            .namespaces(files_namespace())
            .migrations(vec![Migration::addition(1, "files", FILES)])
            .open(directory.id(), |_| ())
            .await
            .unwrap();

        handle
            .setup_s3_storage(
                memory.config(),
                "Test device",
                "key".into(),
                SecretText::new("secret".into()),
            )
            .await
            .unwrap();
        let mut status = handle.subscribe_sync_status();
        status
            .wait_for(|s| matches!(s, SyncStatus::Synced { .. }))
            .await
            .unwrap();
        handle.stop_sync();
        status
            .wait_for(|s| matches!(s, SyncStatus::Stopped))
            .await
            .unwrap();
        handle.unlock_store_key().await.unwrap();
        handle.set_uploads_paused(true).await.unwrap();
        let ids = stage_rows(&handle, 4, |id| {
            FileSource::Stream(Box::pin(std::io::Cursor::new(vec![id; 2048])))
        })
        .await;
        memory
            .set_faults(Faults {
                delay: Duration::from_millis(100),
                ..Faults::none()
            })
            .await;
        memory.reset_request_peak();
        let mut requests = memory.subscribe_requests();
        handle.set_uploads_paused(false).await.unwrap();
        requests
            .wait_for(|_| memory.peak_requests() == 2)
            .await
            .unwrap();
        handle.set_transfer_limits(TransferLimits {
            uploads: NonZeroUsize::MIN,
            downloads: NonZeroUsize::new(2).unwrap(),
        });
        memory.reset_request_peak();
        let mut uploads = handle.subscribe(|sql| {
            Ok(
                sql.query_row("SELECT count(*) FROM coven_uploads", [], |r| {
                    r.get::<_, i64>(0)
                })?,
            )
        });
        while uploads
            .next()
            .await
            .expect("the live query answers")
            .unwrap()
            != 0
        {}
        // The two in flight finish; each later upload starts under the new limit.
        assert_eq!(
            memory.peak_requests(),
            1,
            "remaining files start one at a time"
        );
        let mut uploaded = Vec::new();
        for id in &ids {
            uploaded.push(handle.file_ref(*id).await.unwrap());
        }
        memory.reset_request_peak();
        let pin = handle.pin(&uploaded);
        tokio::pin!(pin);
        tokio::select! {
            result = &mut pin => panic!("pin ended before two downloads overlapped: {result:?}"),
            _ = requests.wait_for(|_| memory.peak_requests() == 2) => {},
        }
        handle.set_transfer_limits(TransferLimits::default());
        pin.await.unwrap();
        assert_eq!(memory.peak_requests(), 2);
        assert_eq!(
            handle
                .pin_progress(uploaded.clone())
                .await
                .unwrap()
                .files_completed,
            4
        );
        for file in &uploaded {
            handle.evict_file(file).await.unwrap();
        }
        memory.reset_request_peak();
        handle.pin(&uploaded).await.unwrap();
        assert_eq!(
            memory.peak_requests(),
            1,
            "the subsequent pin captures the changed limit"
        );
        handle.stop_sync();
        status
            .wait_for(|s| matches!(s, SyncStatus::Stopped))
            .await
            .unwrap();
        memory.set_faults(Faults::none()).await;
        memory.set_online(false);
        handle.start_sync().await.unwrap();
        status
            .wait_for(|s| matches!(s, SyncStatus::Offline { .. }))
            .await
            .unwrap();
        memory.set_online(true);
        handle.evict_file(&uploaded[0]).await.unwrap();
        assert_eq!(handle.read_file(&uploaded[0]).await.unwrap(), vec![0; 2048]);
        memory.set_online(false);
        let before = memory.request_count();
        status.borrow_and_update();
        handle.sync_now();
        requests.wait_for(|n| *n > before).await.unwrap();
        // The earlier Offline is stale; wait for this attempt's outcome.
        loop {
            status.changed().await.unwrap();
            if matches!(
                &*status.borrow(),
                SyncStatus::Offline { .. } | SyncStatus::Failed { .. }
            ) {
                break;
            }
        }
        assert!(
            matches!(&*status.borrow(), SyncStatus::Offline { .. }),
            "the failed attempt decides, not the file read that reached storage before it"
        );
        handle.close().await.unwrap();
    })
    .await
    .expect("bounded transfers finished");
}

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
        SyncedTable::new("attachments", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .file_column(FileColumn::new("file")),
        SyncedTable::new("thumbnails", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .file_column(FileColumn::new("file")),
        SyncedTable::new("tags", RowIdentity::SharedKey),
        SyncedTable::new("note_tags", RowIdentity::SharedKey)
            .key_columns(["note_id", "tag_id"])
            .audience_from("note_id"),
    ]
}

const SCHEMA: &str = "
CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,audience TEXT NOT NULL);
CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,title TEXT NOT NULL,file TEXT);
CREATE TABLE thumbnails(id TEXT NOT NULL PRIMARY KEY,note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,title TEXT NOT NULL,file TEXT);
CREATE TABLE tags(id TEXT NOT NULL PRIMARY KEY);
CREATE TABLE note_tags(note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,tag_id TEXT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,PRIMARY KEY(note_id,tag_id));";

fn builder(app: &TestCoven, layout: StoreLayout, ids: IdSourceRef) -> CovenBuilder {
    app.builder(layout)
        .synced_tables(tables())
        .namespaces(vec![
            Namespace::new("attachments", Provenance::UserProvided, 1 << 30),
            Namespace::new("thumbnails", Provenance::AppProvided, 1 << 30),
        ])
        .migrations(vec![Migration::addition(1, "initial", SCHEMA)])
        .id_source(ids)
}

/// Stage `bytes` as a thumbnail of `note`, in row `id`, inserting the note
/// first when asked.
async fn thumbnail(
    handle: &CovenHandle,
    note: &str,
    id: &str,
    bytes: Vec<u8>,
    insert_note: bool,
) -> FileId {
    let (note, id) = (note.to_owned(), id.to_owned());
    handle
        .write_with_files(
            move |batch| Ok(batch.put_file("thumbnails", bytes, crate::Place::Cloud)),
            move |sql, file| {
                if insert_note {
                    sql.execute("INSERT INTO notes VALUES(?1,'note','store')", [&note])?;
                }
                sql.execute(
                    "INSERT INTO thumbnails(id,note_id,title,file) VALUES(?1,?2,'preview',?3)
                     ON CONFLICT(id) DO UPDATE SET file=excluded.file",
                    (&id, &note, file),
                )?;
                Ok(file)
            },
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn originals_owned_copies_ranges_references_and_deletion_use_the_app_api() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let ids: IdSourceRef = Arc::new(SequentialIds::new());
    let directory = app
        .create_closed_store(&layout, "Household", ids.clone())
        .await
        .unwrap();
    let handle = builder(&app, layout.clone(), ids.clone())
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    let note_id = ids.new_id().to_string();
    let attachment_id = ids.new_id().to_string();
    let thumbnail_id = ids.new_id().to_string();
    let path = root.path().join("user-recording");
    let original = vec![42; 512 * 1024];
    std::fs::write(&path, &original).unwrap();
    let progress = std::sync::Mutex::new(Vec::new());
    let prepared = prepare_user_file(&path, |read| progress.lock().unwrap().push(read))
        .await
        .unwrap();
    assert_eq!(
        progress.lock().unwrap().last(),
        Some(&(original.len() as u64))
    );
    let note = note_id.clone();
    let attachment = attachment_id.clone();
    let recorded = handle
        .write_with_files(
            move |batch| Ok(batch.put_user_file("attachments", crate::UserSource::Cloud(prepared))),
            move |sql, file| {
                sql.execute(
                    "INSERT INTO notes (id,title,audience) VALUES (?1,?2,'store')",
                    (&note, "Paint colors"),
                )?;
                sql.execute(
                    "INSERT INTO attachments (id,note_id,title,file) VALUES (?1,?2,?3,?4)",
                    (&attachment, &note, "Swatches", file),
                )?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    let recording = handle.file_ref(recorded).await.unwrap();
    assert_eq!(recording.size(), original.len() as u64);
    assert_eq!(recording.namespace(), "attachments");
    assert_eq!(handle.read_file(&recording).await.unwrap(), original);
    assert_eq!(
        handle.user_file(recorded).await.unwrap().unwrap().path,
        path
    );
    let stream = handle.open_file_stream(&recording).await.unwrap();
    let header = stream.read_at(0, 64 * 1024).await.unwrap();
    assert_eq!(header, original[..64 * 1024]);
    assert_eq!(
        stream.read_at(128 * 1024, 256 * 1024).await.unwrap(),
        original[128 * 1024..384 * 1024]
    );
    assert!(matches!(
        stream.read_at(stream.plaintext_size(), 1).await,
        Err(FileReadError::RangeOutOfBounds { .. })
    ));
    assert!(matches!(
        stream.read_at(u64::MAX, 2).await,
        Err(FileReadError::RangeOutOfBounds { .. })
    ));
    let staged = thumbnail(
        &handle,
        &note_id,
        &thumbnail_id,
        b"thumbnail".to_vec(),
        false,
    )
    .await;
    let reference = handle.file_ref(staged).await.unwrap();
    assert_eq!(reference.size(), 9);
    assert_eq!(reference.audience(), Audience::Store);
    let old_stream = handle.open_file_stream(&reference).await.unwrap();
    assert_eq!(old_stream.read_at(2, 4).await.unwrap(), b"umbn");
    let readonly = builder(&app, layout.clone(), ids.clone())
        .open_read_only(directory.id())
        .await
        .unwrap();
    assert_eq!(readonly.read_file(&reference).await.unwrap(), b"thumbnail");
    // Pointing the row at another file deletes the first one's record: nothing
    // references it now. A stream already open keeps reading it.
    let replaced = thumbnail(
        &handle,
        &note_id,
        &thumbnail_id,
        b"replacement".to_vec(),
        false,
    )
    .await;
    assert!(matches!(
        handle.read_file(&reference).await,
        Err(FileReadError::Database(DbError::FileAbsent { .. }))
    ));
    assert_eq!(old_stream.read_at(0, 9).await.unwrap(), b"thumbnail");
    let current = handle.file_ref(replaced).await.unwrap();
    assert_eq!(handle.read_file(&current).await.unwrap(), b"replacement");
    std::fs::write(&path, b"changed").unwrap();
    assert!(matches!(
        handle.read_file(&recording).await,
        Err(FileReadError::UserFileChanged { .. })
    ));
    std::fs::remove_file(&path).unwrap();
    assert!(matches!(
        handle.read_file(&recording).await,
        Err(FileReadError::UserFileMissing { .. })
    ));
    std::fs::write(&path, b"the user's new original").unwrap();
    let current_stream = handle.open_file_stream(&current).await.unwrap();
    readonly.close().await.unwrap();
    handle.close().await.unwrap();
    assert!(matches!(
        app.discard_store(&directory).await,
        Err(StoreDeletionError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    assert_eq!(current_stream.read_at(0, 11).await.unwrap(), b"replacement");
    drop(current_stream);
    drop(old_stream);
    drop(stream);
    app.discard_store(&directory).await.unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"the user's new original");
}

#[tokio::test]
async fn a_restored_install_resets_as_a_new_device_before_its_first_write() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let original = TestCoven::new();
    let ids: IdSourceRef = Arc::new(SequentialIds::new());
    let directory = original
        .create_closed_store(&layout, "restored", ids.clone())
        .await
        .unwrap();
    let handle = builder(&original, layout.clone(), ids.clone())
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    let note = ids.new_id().to_string();
    let row = ids.new_id().to_string();
    let old = thumbnail(&handle, &note, &row, b"old".to_vec(), true).await;
    let old = handle.file_ref(old).await.unwrap();
    handle.close().await.unwrap();
    // Backup bytes survive, but the new installation has no device-only entry.
    // Opening refuses; an approved reset makes a new device, and nothing of
    // the old copy is renumbered (§19.3).
    let restored = TestCoven::new();
    let old_device = directory.settings().unwrap().device_id;
    assert!(matches!(
        builder(&restored, layout.clone(), ids.clone())
            .open(directory.id(), |_| ())
            .await,
        Err(CovenError::DeviceKeyMissing { .. })
    ));
    assert_eq!(directory.unfinished_replacement().unwrap(), None);
    let handle = builder(&restored, layout.clone(), ids.clone())
        .reset_device(directory.id(), "Restored")
        .await
        .unwrap();
    let reset = handle.device_reset().await.unwrap().unwrap();
    assert_eq!(reset.old_device, old_device);
    assert_eq!(directory.settings().unwrap().device_id, reset.new_device);
    // The discarded copy's unsent rows and files do not come back.
    assert!(handle.file_ref(old.id()).await.is_err());
    let new = thumbnail(&handle, &note, &row, b"new".to_vec(), true).await;
    let new = handle.file_ref(new).await.unwrap();
    assert_ne!(old.id(), new.id());
    assert_eq!(
        new.place(),
        FilePlace::DeviceOnly {
            device: reset.new_device
        }
    );
    assert_eq!(handle.read_file(&new).await.unwrap(), b"new");
    handle.close().await.unwrap();
    let handle = builder(&restored, layout.clone(), ids)
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    assert_eq!(handle.device_reset().await.unwrap(), Some(reset));
    assert_eq!(handle.file_ref(new.id()).await.unwrap(), new);
    handle.close().await.unwrap();
}

#[tokio::test]
async fn uploaded_files_pins_and_read_only_ranges_use_the_composed_owner() {
    let root = tempfile::tempdir().unwrap();
    let app = TestCoven::new();
    let ids: IdSourceRef = Arc::new(SequentialIds::new());
    let layout = StoreLayout::new(root.path().to_owned());
    let directory = app
        .create_closed_store(&layout, "uploaded", ids.clone())
        .await
        .unwrap();
    let storage = Arc::new(
        coven_storage::test_utils::MemoryStorage::builder()
            .clock(Arc::new(SystemClock))
            .transfer_limits(65536, 65536)
            .build()
            .unwrap(),
    );
    let handle = builder(&app, layout.clone(), ids.clone())
        .storage_connector(storage.clone())
        .open(directory.id(), |_| ())
        .await
        .unwrap();

    use coven_storage::Storage;
    handle
        .setup_s3_storage(
            storage.config(),
            "Laptop",
            "owner".into(),
            SecretText::new("secret".into()),
        )
        .await
        .unwrap();
    let mut status = handle.subscribe_sync_status();
    status
        .wait_for(|s| matches!(s, SyncStatus::Synced { .. }))
        .await
        .unwrap();
    handle.stop_sync();
    status
        .wait_for(|s| matches!(s, SyncStatus::Stopped))
        .await
        .unwrap();
    handle.unlock_store_key().await.unwrap();
    handle.set_uploads_paused(true).await.unwrap();
    let note = ids.new_id().to_string();
    let row = ids.new_id().to_string();
    let staged = thumbnail(&handle, &note, &row, vec![42; 200_000], true).await;
    let queued = || async {
        handle
            .read(|sql| {
                Ok(
                    sql.query_row("SELECT count(*) FROM coven_uploads", [], |r| {
                        r.get::<_, i64>(0)
                    })?,
                )
            })
            .await
            .unwrap()
    };
    assert_eq!(queued().await, 1);
    let absent = FileId(uuid::Uuid::from_u128(1));
    let files = [absent, staged, staged];
    // A file this device has no record of has no answer; one still uploading
    // isn't pinned, and can't be.
    assert_eq!(
        handle.files_pinned(&files).await.unwrap(),
        vec![None, Some(false), Some(false)]
    );
    assert!(handle.files_pinned(&[]).await.unwrap().is_empty());
    assert!(matches!(
        handle.pin(&[handle.file_ref(staged).await.unwrap()]).await,
        Err(FileReadError::DeviceOnly { .. })
    ));
    handle.set_uploads_paused(false).await.unwrap();
    handle.retry_uploads_now().await.unwrap();
    let file = handle.file_ref(staged).await.unwrap();
    assert!(matches!(file.place(), FilePlace::Cloud { .. }));
    assert_eq!(queued().await, 0);
    assert_eq!(
        handle.file_status(&file).await.unwrap(),
        FileStatus::Available
    );
    let mut pins = handle.subscribe_files_pinned(&[staged, absent]);
    assert_eq!(
        pins.next().await.expect("the live query answers").unwrap(),
        vec![Some(false), None]
    );
    handle.pin(std::slice::from_ref(&file)).await.unwrap();
    assert_eq!(
        pins.next().await.expect("the live query answers").unwrap(),
        vec![Some(true), None]
    );
    assert_eq!(
        handle.files_pinned(&files).await.unwrap(),
        vec![None, Some(true), Some(true)]
    );
    // Watching different files is a new subscription with those files.
    drop(pins);
    let mut pins = handle.subscribe_files_pinned(&[absent, staged]);
    assert_eq!(
        pins.next().await.expect("the live query answers").unwrap(),
        vec![None, Some(true)]
    );
    assert_eq!(handle.file_ref(staged).await.unwrap(), file);
    let wrong_location = Arc::new(
        coven_storage::test_utils::MemoryStorage::builder()
            .location(StorageConfig::S3 {
                bucket: "other".into(),
                region: "test".into(),
                endpoint: None,
                profile: None,
                prefix: crate::S3Prefix::parse("uploaded").unwrap(),
            })
            .clock(Arc::new(SystemClock))
            .build()
            .unwrap(),
    );
    assert!(matches!(
        builder(&app, layout.clone(), ids.clone())
            .storage_connector(wrong_location)
            .open_read_only(directory.id())
            .await,
        Err(ReadOnlyOpenError::Storage(SyncError::Storage(error)))
            if error.failure() == StorageFailure::InvalidConfiguration
    ));
    let readonly = builder(&app, layout.clone(), ids)
        .storage_connector(storage.clone())
        .open_read_only(directory.id())
        .await
        .unwrap();
    handle.evict_file(&file).await.unwrap();
    let reads = storage.reads().await.len();
    let stream = readonly.open_file_stream(&file).await.unwrap();
    assert_eq!(stream.read_at(1234, 37).await.unwrap(), vec![42; 37]);
    assert!(
        storage.reads().await.len() > reads,
        "uncached read-only ranges reach saved storage"
    );
    assert_eq!(
        pins.next().await.expect("the live query answers").unwrap(),
        vec![None, Some(false)]
    );
    assert_eq!(handle.file_ref(staged).await.unwrap(), file);
    handle.close().await.unwrap();
    assert!(matches!(
        app.discard_store(&directory).await,
        Err(StoreDeletionError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    readonly.close().await.unwrap();
    assert!(matches!(
        app.discard_store(&directory).await,
        Err(StoreDeletionError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    drop(stream);
    app.discard_store(&directory).await.unwrap();
}

/// One queued file as the app's SQL reads it from `coven_uploads`, joined
/// to the row that references it.
#[derive(Clone, Debug, PartialEq)]
struct QueuedRow {
    title: String,
    paused: bool,
    retrying: bool,
    pending: Option<PendingRecord>,
}

const QUEUED: &str = "SELECT r.title,u.paused,u.retry_at IS NOT NULL,u.pending
    FROM coven_uploads AS u
    JOIN (SELECT file,title FROM thumbnails UNION ALL SELECT file,title FROM attachments) AS r
      ON r.file=u.file
    ORDER BY u.position";

fn queued_rows(sql: SqlReadContext<'_>) -> CovenResult<Vec<QueuedRow>> {
    Ok(sql.query(QUEUED, [], |r| {
        Ok(QueuedRow {
            title: r.get(0)?,
            paused: r.get(1)?,
            retrying: r.get(2)?,
            pending: r.get(3)?,
        })
    })?)
}

/// The app's SQL reads the upload queue, pins and pending records as coven
/// keeps them, joined to its own rows by file id, and its live queries rerun
/// as coven changes them (Appendix C.1, E4).
#[tokio::test]
async fn the_apps_sql_reads_uploads_pins_and_pending_records() {
    let root = tempfile::tempdir().unwrap();
    let app = TestCoven::new();
    let ids: IdSourceRef = Arc::new(SequentialIds::new());
    let layout = StoreLayout::new(root.path().to_owned());
    let directory = app
        .create_closed_store(&layout, "uploaded", ids.clone())
        .await
        .unwrap();
    let storage = Arc::new(
        coven_storage::test_utils::MemoryStorage::builder()
            .clock(Arc::new(SystemClock))
            .build()
            .unwrap(),
    );
    let handle = builder(&app, layout.clone(), ids.clone())
        .storage_connector(storage.clone())
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    use coven_storage::Storage;
    handle
        .setup_s3_storage(
            storage.config(),
            "Laptop",
            "owner".into(),
            SecretText::new("secret".into()),
        )
        .await
        .unwrap();
    let mut status = handle.subscribe_sync_status();
    status
        .wait_for(|s| matches!(s, SyncStatus::Synced { .. }))
        .await
        .unwrap();
    handle.stop_sync();
    status
        .wait_for(|s| matches!(s, SyncStatus::Stopped))
        .await
        .unwrap();
    handle.unlock_store_key().await.unwrap();
    handle.set_uploads_paused(true).await.unwrap();

    let note = ids.new_id().to_string();
    let thumbnail = ids.new_id().to_string();
    let attachment = ids.new_id().to_string();
    let path = root.path().join("recording");
    std::fs::write(&path, b"the first take").unwrap();
    let prepared = prepare_user_file(&path, |_| {}).await.unwrap();
    let (row, recorded) = (thumbnail.clone(), attachment.clone());
    // Two writes, so two moves: the recording's failing leaves the
    // thumbnail's alone (§17.3).
    let parent = note.clone();
    let take = handle
        .write_with_files(
            move |batch| Ok(batch.put_user_file("attachments", crate::UserSource::Cloud(prepared))),
            move |sql, take| {
                sql.execute("INSERT INTO notes VALUES(?1,'note','store')", [&parent])?;
                sql.execute(
                    "INSERT INTO attachments(id,note_id,title,file) VALUES(?1,?2,'take',?3)",
                    (&recorded, &parent, take),
                )?;
                Ok(take)
            },
        )
        .await
        .unwrap();
    let preview = handle
        .write_with_files(
            move |batch| Ok(batch.put_file("thumbnails", b"preview".to_vec(), crate::Place::Cloud)),
            move |sql, preview| {
                sql.execute(
                    "INSERT INTO thumbnails(id,note_id,title,file) VALUES(?1,?2,'preview',?3)",
                    (&row, &note, preview),
                )?;
                Ok(preview)
            },
        )
        .await
        .unwrap();

    let mut queued = handle.subscribe(queued_rows);
    let waiting = |title: &str, paused| QueuedRow {
        title: title.into(),
        paused,
        retrying: false,
        pending: None,
    };
    // Oldest first, as uploads() lists them, both waiting on the pause.
    assert_eq!(
        queued
            .next()
            .await
            .expect("the live query answers")
            .unwrap(),
        [waiting("take", true), waiting("preview", true)]
    );
    let listed: Vec<FileId> = handle
        .uploads()
        .await
        .unwrap()
        .files
        .iter()
        .map(|upload| upload.file)
        .collect();
    assert_eq!(listed, [take, preview]);
    let pinned = |sql: SqlReadContext<'_>| -> CovenResult<Vec<String>> {
        Ok(sql.query(
            "SELECT r.title FROM coven_pins AS p
             JOIN (SELECT file,title FROM thumbnails UNION ALL SELECT file,title FROM attachments) AS r
               ON r.file=p.file
             ORDER BY r.title",
            [],
            |r| r.get(0),
        )?)
    };
    let mut pins = handle.subscribe(pinned);
    assert!(pins
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        .is_empty());

    // The pause ends; the recording changed since it was staged, so its
    // move fails and its record says so, and the thumbnail is stored.
    handle.set_uploads_paused(false).await.unwrap();
    assert_eq!(
        queued
            .next()
            .await
            .expect("the live query answers")
            .unwrap(),
        [waiting("take", false), waiting("preview", false)]
    );
    std::fs::write(&path, b"a second take").unwrap();
    handle.retry_uploads_now().await.unwrap();
    while !queued
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        .is_empty()
    {}
    let records = handle.pending().await.unwrap();
    let record = records
        .iter()
        .find(|record| matches!(record.subject, PendingSubject::Move { .. }))
        .expect("the failed move's record")
        .clone();
    assert_eq!(
        record.reason,
        PendingReason::Original {
            path: path.clone(),
            failure: FileSourceFailure::Changed
        }
    );
    assert_eq!(record.retry(), crate::Retry::AppAction);
    // The recording stays on this device only.
    let device = directory.settings().unwrap().device_id;
    assert_eq!(
        handle.file_ref(take).await.unwrap().place(),
        FilePlace::DeviceOnly { device }
    );
    // coven_pending lists the move's record, which names no file.
    let named: Vec<(PendingRecord, Option<FileId>)> = handle
        .read(|sql| {
            Ok(sql.query("SELECT record,file FROM coven_pending", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?)
        })
        .await
        .unwrap();
    assert!(named.contains(&(record, None)), "{named:?}");

    // The stored thumbnail is pinned once the app pins it, and unpinned again.
    let stored = handle.file_ref(preview).await.unwrap();
    handle.pin(std::slice::from_ref(&stored)).await.unwrap();
    let mut listed = pins.next().await.expect("the live query answers").unwrap();
    while listed.is_empty() {
        listed = pins.next().await.expect("the live query answers").unwrap();
    }
    assert_eq!(listed, ["preview"]);
    assert_eq!(handle.files_pinned(&[preview]).await.unwrap(), [Some(true)]);
    handle.unpin(std::slice::from_ref(&stored)).await.unwrap();
    assert!(pins
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        .is_empty());
    // Deleting the row deletes its file's record, so nothing of it stays
    // pinned.
    handle.pin(std::slice::from_ref(&stored)).await.unwrap();
    assert_eq!(
        pins.next().await.expect("the live query answers").unwrap(),
        ["preview"]
    );
    let row = thumbnail.clone();
    handle
        .write(move |sql| {
            sql.execute("DELETE FROM thumbnails WHERE id=?1", [&row])?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(pins
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        .is_empty());
    let listed: i64 = handle
        .read(move |sql| {
            Ok(sql.query_row(
                "SELECT count(*) FROM coven_pins WHERE file=?1",
                [preview],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(listed, 0);
    assert_eq!(handle.files_pinned(&[preview]).await.unwrap(), [None]);
    handle.close().await.unwrap();
}
