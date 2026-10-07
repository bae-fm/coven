use crate::*;
use std::sync::Arc;

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

fn builder(app: &TestCoven, directory: StoreDir, ids: IdSourceRef) -> CovenBuilder {
    app.builder(directory)
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
    let handle = builder(&app, directory.clone(), ids.clone())
        .open()
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
    let readonly = builder(&app, directory.clone(), ids.clone())
        .open_read_only()
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
    let handle = builder(&original, directory.clone(), ids.clone())
        .open()
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
    let handle = builder(&restored, directory.clone(), ids.clone())
        .open()
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
    let handle = builder(&restored, directory, ids).open().await.unwrap();
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
