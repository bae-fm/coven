use crate::*;
use coven_foundation::files::StoreDirAccess;
use std::sync::Arc;

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
            .synced_tables(vec![SyncedTable::new("files", RowIdentity::SharedKey).carries_files(FileDecl::new(
                "files", 1 << 30, Provenance::AppProvided
            ))])
            .migrations(vec![Migration::addition(1, "files", "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT)")])
            .open(directory.id()).await.unwrap();
        handle.setup_s3_storage(memory.config(), "Device", "key".into(), SecretText::new("secret".into())).await.unwrap();
        let mut status = handle.subscribe_sync_status();
        status.wait_for(|s| matches!(s, SyncStatus::Synced { .. })).await.unwrap();
        handle.stop_sync();
        status.wait_for(|s| matches!(s, SyncStatus::Stopped)).await.unwrap();
        handle.unlock_store_key().await.unwrap();
        handle.write_with_files(|batch| {
            for id in 0..2 {
                batch.put_file("files", id.to_string(), vec![id as u8; 2048]);
            }
            Ok(())
        }, |sql| {
            for id in 0..2 { sql.execute("INSERT INTO files(id) VALUES(?1)", [id.to_string()])?; }
            Ok(())
        }).await.unwrap();
        handle.retry_uploads_now().await.unwrap();
        handle.start_sync().await.unwrap();
        status.wait_for(|s| matches!(s, SyncStatus::Synced { .. })).await.unwrap();
        let first_file = handle.file_ref("files", "0").await.unwrap();
        let second_file = handle.file_ref("files", "1").await.unwrap();
        let (entered, entering) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        memory.hold_body_at(ObjectPath::file(directory.id(), first_file.uploader(), first_file.id()), 1, entered, resumed).await;
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
        let clock = Arc::new(coven_foundation::clock::ClosureClock(|| UNIX_EPOCH + Duration::from_secs(1000)));
        let memory = Arc::new(MemoryStorage::builder().clock(clock.clone()).transfer_limits(1024 * 1024,65536).build().unwrap());
        let layout = StoreLayout::new(root.path().into());
        let directory = app.create_closed_store(&layout, "Files", Arc::new(UuidIds)).await.unwrap();
        let handle = app.builder(layout).clock(clock).storage_connector(memory.clone())
            .max_concurrent_uploads(NonZeroUsize::new(2).unwrap())
            .max_concurrent_downloads(NonZeroUsize::new(2).unwrap())
            .synced_tables(vec![SyncedTable::new("files", RowIdentity::SharedKey).carries_files(FileDecl::new(
                "files", 1 << 30, Provenance::AppProvided
            ))])
            .migrations(vec![Migration::addition(1, "files", "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT)")])
            .open(directory.id()).await.unwrap();

        handle.setup_s3_storage(memory.config(), "Test device", "key".into(), SecretText::new("secret".into())).await.unwrap();
        let mut status = handle.subscribe_sync_status();
        status.wait_for(|s| matches!(s, SyncStatus::Synced { .. })).await.unwrap();
        handle.stop_sync();
        status.wait_for(|s| matches!(s, SyncStatus::Stopped)).await.unwrap();
        handle.unlock_store_key().await.unwrap();
        handle.set_uploads_paused(true).await.unwrap();
        handle.write_with_files(|batch| {
            for id in 0..4 {
                batch.put_file("files", id.to_string(), FileSource::Stream(Box::pin(std::io::Cursor::new(vec![id as u8; 2048]))));
            }
            Ok(())
        }, |sql| {
            for id in 0..4 { sql.execute("INSERT INTO files(id) VALUES(?1)", [id.to_string()])?; }
            Ok(())
        }).await.unwrap();
        memory.set_faults(Faults { delay: Duration::from_millis(100), ..Faults::none() }).await;
        memory.reset_request_peak();
        let mut requests = memory.subscribe_requests();
        handle.set_uploads_paused(false).await.unwrap();
        requests.wait_for(|_| memory.peak_requests() == 2).await.unwrap();
        handle.set_transfer_limits(TransferLimits { uploads: NonZeroUsize::MIN, downloads: NonZeroUsize::new(2).unwrap() });
        memory.reset_request_peak();
        let mut uploads = handle.subscribe_uploads();
        while !uploads.next().await.unwrap().files.is_empty() {}
        // The two in flight finish; each later upload starts under the new limit.
        assert_eq!(memory.peak_requests(), 1, "remaining files start one at a time");
        let mut uploaded = Vec::new();
        for id in 0..4 { uploaded.push(handle.file_ref("files", id.to_string().as_str()).await.unwrap()); }
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
        assert_eq!(handle.pin_progress(uploaded.clone()).await.unwrap().files_completed, 4);
        for file in &uploaded { handle.evict_file(file).await.unwrap(); }
        memory.reset_request_peak();
        handle.pin(&uploaded).await.unwrap();
        assert_eq!(memory.peak_requests(), 1, "the subsequent pin captures the changed limit");
        handle.stop_sync();
        status.wait_for(|s| matches!(s, SyncStatus::Stopped)).await.unwrap();
        memory.set_faults(Faults::none()).await;
        memory.set_online(false);
        handle.start_sync().await.unwrap();
        status.wait_for(|s| matches!(s, SyncStatus::Offline { .. })).await.unwrap();
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
            if matches!(&*status.borrow(), SyncStatus::Offline { .. } | SyncStatus::Failed { .. }) { break; }
        }
        assert!(matches!(&*status.borrow(), SyncStatus::Offline { .. }), "the failed attempt decides, not the file read that reached storage before it");
        handle.close().await.unwrap();
    }).await.expect("bounded transfers finished");
}

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
        SyncedTable::new("attachments", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .carries_files(FileDecl::new(
                "attachments",
                1 << 30,
                Provenance::UserProvided,
            )),
        SyncedTable::new("thumbnails", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .carries_files(FileDecl::new(
                "thumbnails",
                1 << 30,
                Provenance::AppProvided,
            )),
        SyncedTable::new("tags", RowIdentity::SharedKey),
        SyncedTable::new("note_tags", RowIdentity::SharedKey)
            .key_columns(["note_id", "tag_id"])
            .audience_from("note_id"),
    ]
}

const SCHEMA: &str = "
CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,audience TEXT NOT NULL);
CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,title TEXT NOT NULL,size INTEGER NOT NULL,hash BLOB,location TEXT);
CREATE TABLE thumbnails(id TEXT NOT NULL PRIMARY KEY,note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,title TEXT NOT NULL,size INTEGER,hash BLOB,location TEXT);
CREATE TABLE tags(id TEXT NOT NULL PRIMARY KEY);
CREATE TABLE note_tags(note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,tag_id TEXT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,PRIMARY KEY(note_id,tag_id));";

fn builder(app: &TestCoven, layout: StoreLayout, ids: IdSourceRef) -> CovenBuilder {
    app.builder(layout)
        .synced_tables(tables())
        .migrations(vec![Migration::addition(1, "initial", SCHEMA)])
        .id_source(ids)
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
        .open(directory.id())
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
    let size = original.len() as i64;
    let note = note_id.clone();
    let attachment = attachment_id.clone();
    handle
        .write(move |sql| {
            sql.execute(
                "INSERT INTO notes (id,title,audience) VALUES (?1,?2,'store')",
                (&note, "Paint colors"),
            )?;
            sql.execute(
                "INSERT INTO attachments (id,note_id,title,size) VALUES (?1,?2,?3,?4)",
                (&attachment, &note, "Swatches", size),
            )?;
            sql.register_user_file("attachments", attachment.as_str(), prepared)?;
            Ok(())
        })
        .await
        .unwrap();
    let recording = handle
        .file_ref("attachments", attachment_id.as_str())
        .await
        .unwrap();
    assert_eq!(handle.read_file(&recording).await.unwrap(), original);
    assert_eq!(
        handle
            .user_file("attachments", attachment_id.as_str())
            .await
            .unwrap()
            .unwrap()
            .path,
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
    let note = note_id.clone();
    let thumbnail = thumbnail_id.clone();
    let supplied = thumbnail.clone();
    handle
        .write_with_files(
            move |batch| {
                batch.put_file(
                    "thumbnails",
                    supplied,
                    FileSource::Stream(Box::pin(std::io::Cursor::new(b"thumbnail".to_vec()))),
                );
                Ok(())
            },
            move |sql| {
                sql.execute(
                    "INSERT INTO thumbnails(id,note_id,title) VALUES (?1,?2,'preview')",
                    (&thumbnail, &note),
                )?;
                Ok(())
            },
        )
        .await
        .unwrap();
    let reference = handle
        .file_ref("thumbnails", thumbnail_id.as_str())
        .await
        .unwrap();
    assert_eq!(reference.plaintext_size(), 9);
    assert_eq!(reference.audience(), Audience::Store);
    let old_stream = handle.open_file_stream(&reference).await.unwrap();
    assert_eq!(old_stream.read_at(2, 4).await.unwrap(), b"umbn");
    let readonly = builder(&app, layout.clone(), ids.clone())
        .open_read_only(directory.id())
        .await
        .unwrap();
    assert_eq!(readonly.read_file(&reference).await.unwrap(), b"thumbnail");
    let supplied = thumbnail_id.clone();
    handle
        .write_with_files(
            move |batch| {
                batch.put_file("thumbnails", supplied, b"replacement".to_vec());
                Ok(())
            },
            |_| Ok(()),
        )
        .await
        .unwrap();
    assert!(matches!(
        handle.read_file(&reference).await,
        Err(FileReadError::Database(DbError::FileRefChanged { .. }))
    ));
    assert_eq!(old_stream.read_at(0, 9).await.unwrap(), b"thumbnail");
    let current = handle
        .file_ref("thumbnails", thumbnail_id.as_str())
        .await
        .unwrap();
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
        .open(directory.id())
        .await
        .unwrap();
    let note = ids.new_id().to_string();
    let thumbnail = ids.new_id().to_string();
    let supplied = thumbnail.clone();
    let inserted = thumbnail.clone();
    let parent = note.clone();
    handle
        .write_with_files(
            move |batch| {
                batch.put_file("thumbnails", supplied, b"old".to_vec());
                Ok(())
            },
            move |sql| {
                sql.execute("INSERT INTO notes VALUES(?1,'note','store')", [&parent])?;
                sql.execute(
                    "INSERT INTO thumbnails(id,note_id,title) VALUES(?1,?2,'old')",
                    (&inserted, &parent),
                )?;
                Ok(())
            },
        )
        .await
        .unwrap();
    let old = handle
        .file_ref("thumbnails", thumbnail.as_str())
        .await
        .unwrap();
    handle.close().await.unwrap();
    // Backup bytes survive, but the new installation has no device-only entry.
    // Opening refuses; an approved reset makes a new device, and nothing of
    // the old copy is renumbered (§19.3).
    let restored = TestCoven::new();
    let old_device = directory.settings().unwrap().device_id;
    assert!(matches!(
        builder(&restored, layout.clone(), ids.clone())
            .open(directory.id())
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
    assert!(handle
        .file_ref("thumbnails", thumbnail.as_str())
        .await
        .is_err());
    let supplied = thumbnail.clone();
    let inserted = thumbnail.clone();
    let parent = note.clone();
    handle
        .write_with_files(
            move |batch| {
                batch.put_file("thumbnails", supplied, b"new".to_vec());
                Ok(())
            },
            move |sql| {
                sql.execute("INSERT INTO notes VALUES(?1,'note','store')", [&parent])?;
                sql.execute(
                    "INSERT INTO thumbnails(id,note_id,title) VALUES(?1,?2,'new')",
                    (&inserted, &parent),
                )?;
                Ok(())
            },
        )
        .await
        .unwrap();
    let new = handle
        .file_ref("thumbnails", thumbnail.as_str())
        .await
        .unwrap();
    assert_ne!((old.uploader(), old.id()), (new.uploader(), new.id()));
    assert_eq!(handle.read_file(&new).await.unwrap(), b"new");
    handle.close().await.unwrap();
    let handle = builder(&restored, layout.clone(), ids)
        .open(directory.id())
        .await
        .unwrap();
    assert_eq!(handle.device_reset().await.unwrap(), Some(reset));
    assert_eq!(
        handle
            .file_ref("thumbnails", thumbnail.as_str())
            .await
            .unwrap(),
        new
    );
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
        .open(directory.id())
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
    let supplied = thumbnail.clone();
    let row = thumbnail.clone();
    let unattached = ids.new_id().to_string();
    let empty_row = unattached.clone();
    handle
        .write_with_files(
            move |batch| {
                batch.put_file("thumbnails", supplied, vec![42; 200_000]);
                Ok(())
            },
            move |sql| {
                sql.execute("INSERT INTO notes VALUES(?1,'note','store')", [&note])?;
                sql.execute(
                    "INSERT INTO thumbnails(id,note_id,title) VALUES(?1,?2,'preview')",
                    (&row, &note),
                )?;
                sql.execute(
                    "INSERT INTO thumbnails(id,note_id,title) VALUES(?1,?2,'without file')",
                    (&empty_row, &note),
                )?;
                Ok(())
            },
        )
        .await
        .unwrap();
    let mut uploads = handle.subscribe_uploads();
    assert_eq!(uploads.next().await.unwrap().files.len(), 1);
    let keys = vec![
        "absent".into(),
        unattached.as_str().into(),
        thumbnail.as_str().into(),
        thumbnail.as_str().into(),
    ];
    assert_eq!(
        handle
            .rows_pinned("thumbnails", keys.clone())
            .await
            .unwrap(),
        vec![None, None, Some(false), Some(false)]
    );
    assert!(handle
        .rows_pinned("thumbnails", vec![])
        .await
        .unwrap()
        .is_empty());
    handle.set_uploads_paused(false).await.unwrap();
    handle.retry_uploads_now().await.unwrap();
    let file = handle
        .file_ref("thumbnails", thumbnail.as_str())
        .await
        .unwrap();
    assert!(handle
        .subscribe_uploads()
        .next()
        .await
        .unwrap()
        .files
        .is_empty());
    assert_eq!(
        handle.file_status(&file).await.unwrap(),
        FileStatus::Available
    );
    let mut pins = handle.subscribe_rows_pinned(
        "thumbnails",
        vec![thumbnail.as_str().into(), "absent".into()],
    );
    assert_eq!(pins.next().await.unwrap(), vec![Some(false), None]);
    handle.pin(std::slice::from_ref(&file)).await.unwrap();
    assert_eq!(pins.next().await.unwrap(), vec![Some(true), None]);
    assert_eq!(
        handle.rows_pinned("thumbnails", keys).await.unwrap(),
        vec![None, None, Some(true), Some(true)]
    );
    // Watching different keys is a new subscription with those keys.
    drop(pins);
    let mut pins = handle.subscribe_rows_pinned(
        "thumbnails",
        vec!["absent".into(), thumbnail.as_str().into()],
    );
    assert_eq!(pins.next().await.unwrap(), vec![None, Some(true)]);
    assert_eq!(
        handle
            .file_ref("thumbnails", thumbnail.as_str())
            .await
            .unwrap(),
        file
    );
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
    assert_eq!(pins.next().await.unwrap(), vec![None, Some(false)]);
    assert_eq!(
        handle
            .file_ref("thumbnails", thumbnail.as_str())
            .await
            .unwrap(),
        file
    );
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
