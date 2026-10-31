use crate::file_write::tests::{complete_uploads, namespaces, open, stage, tables, SCHEMA};
use crate::{tests::TestStore, *};
use coven_foundation::id_source::{CircleId, DeviceId, FileId};

/// A file reference reads the file's record by its id: one that is still
/// uploading is on this device only, and one stored names its location.
#[tokio::test]
async fn a_reference_reads_the_files_record_by_its_id() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = stage(&db, b"old".to_vec(), true).await.unwrap();
    let uploading = db.file_ref(file).await.unwrap();
    assert_eq!(uploading.id(), file);
    assert_eq!(uploading.size(), 3);
    assert_eq!(uploading.namespace(), "files");
    assert_eq!(uploading.audience(), coven_merge::Audience::Store);
    assert_eq!(
        uploading.place(),
        FilePlace::DeviceOnly {
            device: store.device()
        }
    );
    complete_uploads(&db).await;
    let stored = db.file_ref(file).await.unwrap();
    let FilePlace::Cloud {
        location,
        uploader,
        content_hash,
    } = stored.place()
    else {
        panic!("stored");
    };
    assert_eq!(uploader, store.device());
    assert_eq!(location.uploader(), store.device());
    assert_eq!(
        *content_hash,
        coven_crypto::ContentHash::from_bytes(sha256(b"old"))
    );
    // The same bytes again are a new file, with a new id.
    let again = stage(&db, b"old".to_vec(), false).await.unwrap();
    assert_ne!(again, file);
    assert!(matches!(
        db.file_ref(file).await,
        Err(DbError::FileAbsent { file: absent }) if absent == file
    ));
    db.close().await.unwrap();
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = coven_crypto::ContentHasher::new();
    hasher.update(bytes);
    *hasher.finish().as_bytes()
}

#[tokio::test]
async fn a_malformed_record_is_damage_and_never_shows_its_key() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = stage(&db, b"original".to_vec(), true).await.unwrap();
    complete_uploads(&db).await;
    let id = uuid::Uuid::from_u128(7);
    let location = format!("file 1 {id} {}", "ab".repeat(32));
    // A committed fixture naming another uploader's object.
    db.commit_writer(|sql| {
        sql.internal_execute("UPDATE main.coven_files SET coordinates=?1", [&location])
            .unwrap()
    });
    let uploaded = db.file_ref(file).await.unwrap();
    let FilePlace::Cloud { location, .. } = uploaded.place() else {
        panic!("stored");
    };
    assert_eq!(location.uploader(), DeviceId(1));
    assert_eq!(location.object(), FileId(id));
    // Only a file this device uploads has a local source.
    assert!(db.open_local_file(&uploaded).await.unwrap().is_none());
    assert!(!format!("{uploaded:?}").contains(&"ab".repeat(32)));
    let valid = format!("file 1 {id} {}", "ab".repeat(32));
    let mut malformed: Vec<_> = (0..valid.len())
        .map(|end| valid[..end].to_owned())
        .collect();
    malformed.extend([
        format!("uploaded 1 {id} {}", "ab".repeat(32)),
        "on 1".into(),
        "device 01".into(),
        "device -1".into(),
        "device 18446744073709551616".into(),
        format!("{valid} "),
        format!("{valid}00"),
        format!("file 1 {id} {}", "AB".repeat(32)),
        format!("file 1 {} {}", id.simple(), "ab".repeat(32)),
        format!("file 1  {id} {}", "ab".repeat(32)),
    ]);
    for malformed in malformed {
        db.commit_writer(|sql| {
            sql.internal_execute("UPDATE main.coven_files SET coordinates=?1", [&malformed])
                .unwrap()
        });
        assert!(
            matches!(db.file_ref(file).await, Err(DbError::DamagedDatabase)),
            "{malformed}"
        );
    }
    // A device-only file has no content hash; a stored one has one.
    db.commit_writer(|sql| {
        sql.internal_execute("UPDATE main.coven_files SET coordinates='device 1'", [])
            .unwrap()
    });
    assert!(matches!(
        db.file_ref(file).await,
        Err(DbError::DamagedDatabase)
    ));
    db.close().await.unwrap();
}

#[tokio::test]
async fn stored_paths_are_found_by_retention() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = stage(&db, b"original".to_vec(), true).await.unwrap();
    let queued: (DeviceId, FileId) = db.inspect_writer(|sql| {
        sql.query_row("SELECT location FROM _coven_file_uploads", [], |r| {
            r.get::<_, String>(0)
        })
        .map(|text| {
            let location = crate::FileLocation::decode(&text).unwrap();
            (location.uploader(), location.object())
        })
        .unwrap()
    });
    let text = include_str!("../../coven-format/fixtures/file-reference.txt")
        .lines()
        .nth(1)
        .unwrap();
    db.inspect_writer_schema(|sql, schema| {
        sql.transaction(|sql| {
            sql.internal_execute(
                "UPDATE main.coven_files SET coordinates=?1,hash=zeroblob(32)",
                [text],
            )?;
            let key = ("coven_files".into(), crate::file_table::key(file)?);
            crate::file_protection::refresh(sql, schema, &key).map(drop)
        })
        .unwrap()
    });
    // The record's stored path, and the queued upload's.
    assert_eq!(
        db.retained_files().await.unwrap().references,
        [
            (DeviceId(1), FileId(uuid::Uuid::from_bytes([0x11; 16]))),
            queued
        ]
        .into()
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn original_facts_survive_reopen_and_are_not_replaced_by_new_disk_metadata() {
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
    let directory = tempfile::tempdir().unwrap();
    #[cfg(all(unix, not(target_os = "macos")))]
    let name = {
        use std::os::unix::ffi::OsStringExt;
        std::ffi::OsString::from_vec(b"original-\xff".to_vec())
    };
    #[cfg(any(windows, target_os = "macos"))]
    let name = std::ffi::OsString::from("original-音楽");
    let path = directory.path().join(name);
    std::fs::write(&path, b"original").unwrap();
    let metadata = std::fs::metadata(&path).unwrap();
    let prepared = prepare_user_file(&path, |_| {}).await.unwrap();
    let file = db
        .write_with_files::<_, _, _, _, crate::DbError>(
            move |batch| Ok(batch.put_user_file("files", crate::UserSource::Cloud(prepared))),
            |sql, file| {
                sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [file])?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    let reference = db.file_ref(file).await.unwrap();
    let expected = UserFile {
        path: path.clone(),
        size: metadata.len(),
        modified_at: metadata.modified().unwrap(),
    };
    assert_eq!(db.user_file(file).await.unwrap(), Some(expected.clone()));
    std::fs::write(&path, b"changed outside coven").unwrap();
    assert_eq!(db.user_file(file).await.unwrap(), Some(expected.clone()));
    let reader = store
        .builder(tables(), vec![Migration::addition(1, "files", SCHEMA)])
        .namespaces(namespaces(Provenance::UserProvided))
        .open_read_only()
        .await
        .unwrap();
    assert_eq!(reader.file_ref(file).await.unwrap(), reference);
    assert_eq!(
        reader.user_file(file).await.unwrap(),
        Some(expected.clone())
    );
    db.close().await.unwrap();
    assert_eq!(reader.file_ref(file).await.unwrap(), reference);
    reader.close().await.unwrap();
    let db = open(&store, Provenance::UserProvided).await;
    assert_eq!(db.user_file(file).await.unwrap(), Some(expected));
    db.write(|sql| {
        sql.execute("DELETE FROM files", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(db.user_file(file).await.unwrap(), None);
    assert!(matches!(
        db.file_ref(file).await,
        Err(DbError::FileAbsent { .. })
    ));
    assert_eq!(std::fs::read(&path).unwrap(), b"changed outside coven");
    db.close().await.unwrap();
}

/// A row whose audience comes from its parent stages its file in that
/// audience, and moving the parent gives the row a new file in the new one.
#[tokio::test]
async fn an_inherited_audience_rows_file_follows_its_parent() {
    let store = TestStore::new();
    let declarations = || {
        vec![
            SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
            SyncedTable::new("attachments", RowIdentity::SharedKey)
                .key_columns(["note_id", "name"])
                .audience_from("note_id")
                .file_column(FileColumn::new("TOKEN")),
        ]
    };
    let schema = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL); CREATE TABLE attachments(note_id TEXT NOT NULL REFERENCES notes(id),name TEXT NOT NULL,token TEXT,PRIMARY KEY(note_id,name)); CREATE INDEX attachments_token ON attachments(token)";
    let db = store
        .builder(
            declarations(),
            vec![Migration::addition(1, "schema", schema)],
        )
        .namespaces(namespaces(Provenance::AppProvided))
        .open()
        .await
        .unwrap();
    const NOTE: &str = "00000000-0000-4000-8000-000000000007";
    const CIRCLE: &str = "00000000-0000-4000-8000-000000000008";
    let circle = CircleId(uuid::Uuid::parse_str(CIRCLE).unwrap());
    store.join_circle(&db, circle);
    let file = db
        .write_with_files::<_, _, _, _, crate::DbError>(
            |batch| Ok(batch.put_file("files", b"original".to_vec(), crate::Place::Cloud)),
            |sql, file| {
                sql.execute("INSERT INTO notes VALUES(?1,'store')", [NOTE])?;
                sql.execute(
                    "INSERT INTO attachments(note_id,name,token) VALUES(?1,'song',?2)",
                    rusqlite::params![NOTE, file],
                )?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    let reference = db.file_ref(file).await.unwrap();
    assert_eq!(reference.size(), 8);
    assert_eq!(reference.audience(), coven_merge::Audience::Store);
    complete_uploads(&db).await;
    db.write(|sql| {
        sql.execute("UPDATE notes SET audience=?1", [CIRCLE])?;
        Ok(())
    })
    .await
    .unwrap();
    let moved: FileId = db
        .read(|sql| Ok(sql.query_row("SELECT token FROM attachments", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_ne!(moved, file);
    assert!(matches!(
        db.file_ref(file).await,
        Err(DbError::FileAbsent { .. })
    ));
    let moved = db.file_ref(moved).await.unwrap();
    assert_eq!(moved.audience(), coven_merge::Audience::Circle(circle));
    assert_eq!(moved.size(), 8);
    db.close().await.unwrap();
}

#[tokio::test]
async fn an_absent_file_returns_an_error_without_panicking() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let absent = FileId(uuid::Uuid::from_u128(9));
    assert!(matches!(
        db.file_ref(absent).await,
        Err(DbError::FileAbsent { file }) if file == absent
    ));
    assert!(db.user_file(absent).await.unwrap().is_none());
    db.close().await.unwrap();
    assert!(matches!(
        db.file_ref(absent).await,
        Err(DbError::StoreClosed)
    ));
    assert!(matches!(
        db.user_file(absent).await,
        Err(DbError::StoreClosed)
    ));
}
