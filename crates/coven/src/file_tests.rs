use crate::*;
use std::sync::Arc;

#[tokio::test]
async fn transfer_limits_bound_requests_and_an_active_batch_keeps_its_limit() {
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
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
        let memory = Arc::new(MemoryStorage::new(StorageConfig::S3 {
            bucket: "files".into(), region: "test".into(), endpoint: None, prefix: "store".into(),
        }, clock.clone()).unwrap().with_transfer_limits(1024 * 1024, 65536).unwrap());
        let layout = StoreLayout::new(root.path().into());
        let directory = app.create_store(&layout, "Files", Arc::new(UuidIds)).await.unwrap();
        let handle = app.builder(layout).clock(clock).storage_connector(memory.clone())
            .max_concurrent_uploads(NonZeroUsize::new(2).unwrap())
            .max_concurrent_downloads(NonZeroUsize::new(2).unwrap())
            .synced_tables(vec![SyncedTable::new("files", RowIdentity::SharedKey).carries_files(FileDecl::new(
                "files", Provenance::AppProvided, Uploads::WhenAsked, CacheFill::CacheLazy,
            ))])
            .migrations(vec![Migration::sql(1, "files", "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT)")])
            .coven_migration_policy(CovenMigrationPolicy::ApplyPending).open(directory.id()).await.unwrap();
        handle.initialize_identity().unwrap();
        handle.setup_s3_storage(memory.config(), "Test device", "key".into(), SecretText::new("secret".into())).await.unwrap();
        let mut status = handle.subscribe_sync_status();
        status.wait_for(|s| matches!(s, SyncStatus::Synced(_))).await.unwrap();
        handle.stop_sync();
        status.wait_for(|s| matches!(s, SyncStatus::Stopped)).await.unwrap();
        handle.unlock_store_key().await.unwrap();
        handle.set_uploads_paused(true);
        handle.write_with_files(|batch| {
            for id in 0..4 {
                batch.put_file("files", id.to_string(), FileSource::Stream(Box::pin(std::io::Cursor::new(vec![id as u8; 2048]))));
            }
            Ok(())
        }, |sql| {
            for id in 0..4 { sql.execute("INSERT INTO files(id) VALUES(?1)", [id.to_string()])?; }
            Ok(())
        }).await.unwrap();
        let mut files = Vec::new();
        for id in 0..4 { files.push(handle.file_ref("files", id.to_string().as_str()).await.unwrap()); }
        handle.upload_files(&files).await.unwrap();
        memory.set_faults(Faults { delay: Duration::from_millis(100), ..Faults::none() }).await;
        memory.reset_request_peak();
        let mut requests = memory.subscribe_requests();
        handle.set_uploads_paused(false);
        requests.wait_for(|_| memory.peak_requests() == 2).await.unwrap();
        handle.set_transfer_limits(TransferLimits { uploads: NonZeroUsize::MIN, downloads: NonZeroUsize::new(2).unwrap() });
        memory.reset_request_peak();
        let mut uploads = handle.subscribe_uploads();
        while !uploads.next().await.unwrap().files.is_empty() {}
        assert_eq!(memory.peak_requests(), 2, "remaining files use the active drain's original limit");
        let mut uploaded = Vec::new();
        for id in 0..4 { uploaded.push(handle.file_ref("files", id.to_string().as_str()).await.unwrap()); }
        memory.reset_request_peak();
        let progress = std::sync::Mutex::new(Vec::new());
        let on_progress = |p| progress.lock().unwrap().push(p);
        let pin = handle.pin(&uploaded, &on_progress);
        tokio::pin!(pin);
        tokio::select! {
            result = &mut pin => panic!("pin ended before two downloads overlapped: {result:?}"),
            _ = requests.wait_for(|_| memory.peak_requests() == 2) => {},
        }
        handle.set_transfer_limits(TransferLimits::default());
        pin.await.unwrap();
        assert_eq!(memory.peak_requests(), 2);
        assert_eq!(progress.lock().unwrap().last().unwrap().files_completed, 4);
        for file in &uploaded { handle.evict_file(file).await.unwrap(); }
        memory.reset_request_peak();
        handle.pin(&uploaded, &|_| {}).await.unwrap();
        assert_eq!(memory.peak_requests(), 1, "the subsequent pin captures the changed limit");
        handle.stop_sync();
        status.wait_for(|s| matches!(s, SyncStatus::Stopped)).await.unwrap();
        memory.set_faults(Faults::none()).await;
        memory.set_online(false);
        handle.start_sync().await.unwrap();
        status.wait_for(|s| matches!(s, SyncStatus::Offline)).await.unwrap();
        memory.set_online(true);
        handle.evict_file(&uploaded[0]).await.unwrap();
        assert_eq!(handle.read_file(&uploaded[0]).await.unwrap(), vec![0; 2048]);
        memory.set_online(false);
        let before = memory.request_count();
        handle.sync_now();
        requests.wait_for(|n| *n > before).await.unwrap();
        status.wait_for(|s| matches!(s, SyncStatus::Offline | SyncStatus::Failed { .. })).await.unwrap();
        assert!(matches!(&*status.borrow(), SyncStatus::Failed { .. }), "a file read reached this connection before the failed sync");
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
                Provenance::UserProvided,
                Uploads::WhenAsked,
                CacheFill::CacheLazy,
            )),
        SyncedTable::new("thumbnails", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .carries_files(FileDecl::new(
                "thumbnails",
                Provenance::AppProvided,
                Uploads::WhenAttached,
                CacheFill::CacheEager,
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
        .migrations(vec![Migration::sql(1, "initial", SCHEMA)])
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
        .id_source(ids)
}

#[tokio::test]
async fn originals_owned_copies_ranges_references_and_deletion_use_the_app_api() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let app = TestCoven::new();
    let ids: IdSourceRef = Arc::new(SequentialIds::new());
    let directory = app
        .create_store(&layout, "Household", ids.clone())
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
    handle.ensure_file_on_device(&recording).await.unwrap();
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
        app.delete_store(&directory, &[]).await,
        Err(StoreDeletionError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    assert_eq!(current_stream.read_at(0, 11).await.unwrap(), b"replacement");
    drop(current_stream);
    drop(old_stream);
    drop(stream);
    app.delete_store(&directory, &[]).await.unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"the user's new original");
}

#[tokio::test]
async fn a_restored_install_gets_a_new_id_before_its_first_write() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().to_owned());
    let original = TestCoven::new();
    let ids: IdSourceRef = Arc::new(SequentialIds::new());
    let directory = original
        .create_store(&layout, "restored", ids.clone())
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
    let restored = TestCoven::new();
    let handle = builder(&restored, layout.clone(), ids.clone())
        .open(directory.id())
        .await
        .unwrap();
    assert!(matches!(
        handle.read_file(&old).await,
        Err(FileReadError::OnOtherDevice { .. })
    ));
    let supplied = thumbnail.clone();
    handle
        .write_with_files(
            move |batch| {
                batch.put_file("thumbnails", supplied, b"new".to_vec());
                Ok(())
            },
            |_| Ok(()),
        )
        .await
        .unwrap();
    let new = handle
        .file_ref("thumbnails", thumbnail.as_str())
        .await
        .unwrap();
    assert_ne!(old.location(), new.location());
    assert_eq!(handle.read_file(&new).await.unwrap(), b"new");
    handle.close().await.unwrap();
    let handle = builder(&restored, layout.clone(), ids)
        .open(directory.id())
        .await
        .unwrap();
    assert_eq!(
        handle
            .file_ref("thumbnails", thumbnail.as_str())
            .await
            .unwrap()
            .location(),
        new.location()
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
        .create_store(&layout, "uploaded", ids.clone())
        .await
        .unwrap();
    let storage = Arc::new(
        coven_storage::test_utils::MemoryStorage::new(
            StorageConfig::Dropbox {
                namespace_id: "uploaded".into(),
            },
            Arc::new(SystemClock),
        )
        .unwrap()
        .with_transfer_limits(65536, 65536)
        .unwrap(),
    );
    let handle = builder(&app, layout.clone(), ids.clone())
        .storage(storage.clone())
        .open(directory.id())
        .await
        .unwrap();
    handle.set_uploads_paused(true);
    let note = ids.new_id().to_string();
    let thumbnail = ids.new_id().to_string();
    let supplied = thumbnail.clone();
    let row = thumbnail.clone();
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
                Ok(())
            },
        )
        .await
        .unwrap();
    let mut uploads = handle.subscribe_uploads();
    assert_eq!(uploads.next().await.unwrap().files.len(), 1);
    handle.set_uploads_paused(false);
    handle.retry_uploads_now().await.unwrap();
    let file = handle
        .file_ref("thumbnails", thumbnail.as_str())
        .await
        .unwrap();
    assert_eq!(file.location(), FileLocation::Uploaded);
    let mut pins = handle.subscribe_rows_pinned(
        "thumbnails",
        vec![thumbnail.as_str().into(), "absent".into()],
    );
    assert_eq!(pins.next().await.unwrap(), vec![Some(false), None]);
    handle
        .pin(std::slice::from_ref(&file), &|_| {})
        .await
        .unwrap();
    assert_eq!(pins.next().await.unwrap(), vec![Some(true), None]);
    let readonly = builder(&app, layout.clone(), ids)
        .storage(storage)
        .open_read_only(directory.id())
        .await
        .unwrap();
    let stream = readonly.open_file_stream(&file).await.unwrap();
    assert_eq!(stream.read_at(1234, 37).await.unwrap(), vec![42; 37]);
    handle.evict_file(&file).await.unwrap();
    assert_eq!(pins.next().await.unwrap(), vec![Some(false), None]);
    let row = thumbnail.clone();
    let mut locations = handle.subscribe(move |sql| {
        Ok(
            sql.query_row("SELECT location FROM thumbnails WHERE id=?1", [&row], |r| {
                r.get::<_, String>(0)
            })?,
        )
    });
    assert!(locations.next().await.unwrap().starts_with("uploaded "));
    handle
        .keep_files_on_this_device(
            std::slice::from_ref(&file),
            &std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    let location = tokio::time::timeout(std::time::Duration::from_secs(10), locations.next())
        .await
        .unwrap()
        .unwrap();
    assert!(!location.starts_with("uploaded "));
    let kept = handle
        .file_ref("thumbnails", thumbnail.as_str())
        .await
        .unwrap();
    assert!(matches!(kept.location(), FileLocation::OnDevice(_)));
    assert_eq!(handle.read_file(&kept).await.unwrap(), vec![42; 200_000]);
    handle.close().await.unwrap();
    assert!(matches!(
        app.delete_store(&directory, &[]).await,
        Err(StoreDeletionError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    readonly.close().await.unwrap();
    assert!(matches!(
        app.delete_store(&directory, &[]).await,
        Err(StoreDeletionError::Lock(StoreLockError::AlreadyOpen(_)))
    ));
    drop(stream);
    app.delete_store(&directory, &[]).await.unwrap();
}
