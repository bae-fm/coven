use crate::{tests::TestStore, *};

const SCHEMA: &str = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT,title TEXT)";
fn tables(kind: Provenance) -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("files", RowIdentity::SharedKey).carries_files(FileDecl::new(
            "files",
            kind,
            Uploads::WhenAsked,
            CacheFill::CacheLazy,
        )),
    ]
}
async fn attach(db: &Database, bytes: Vec<u8>, insert: bool) {
    let size = bytes.len() as i64;
    db.write_with_files(
        move |batch| {
            batch.put_file("files", "7", bytes);
            Ok(())
        },
        move |sql| {
            sql.execute(
                if insert {
                    "INSERT INTO files(id,size) VALUES('7',?1)"
                } else {
                    "UPDATE files SET size=?1 WHERE id='7'"
                },
                [size],
            )?;
            Ok(())
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn changed_file_references_refuse_a_write_even_if_the_app_ignores_the_error() {
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    attach(&db, b"old".to_vec(), true).await;
    let reference = db.file_ref("FILES", "7").await.unwrap();
    assert_eq!(reference.table(), "files");
    assert_eq!(reference.column(), "id");
    assert_eq!(reference.key(), &RowKey::from("7"));
    assert_eq!(reference.plaintext_size(), 3);
    assert_eq!(reference.audience(), coven_merge::Audience::Store);
    let device = crate::write::tests::records(&db)[0].header.position.device;
    assert_eq!(reference.location(), FileLocation::OnDevice(device));
    let current = reference.clone();
    db.write(move |sql| {
        sql.validate_file_ref(&current)?;
        sql.execute("UPDATE files SET title='same file'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(db.file_ref("files", "7").await.unwrap(), reference);
    attach(&db, b"new bytes".to_vec(), false).await;
    for ignored in [false, true] {
        let stale = reference.clone();
        let error = db
            .write(move |sql| {
                let result = sql.validate_file_ref(&stale);
                if !ignored {
                    result?;
                } else {
                    assert!(matches!(result, Err(DbError::FileRefChanged { .. })));
                }
                sql.execute("DELETE FROM files", [])?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(matches!(error,DbError::FileRefChanged {table,..} if table=="files"));
        assert_eq!(db.file_ref("files", "7").await.unwrap().plaintext_size(), 9);
    }
    // Returning to the same bytes still has a different file version.
    attach(&db, b"old".to_vec(), false).await;
    let error = db
        .write(move |sql| sql.validate_file_ref(&reference))
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::FileRefChanged { .. }));
    db.close().await.unwrap();
}

#[tokio::test]
async fn uploaded_locations_retain_the_key_identity_and_refuse_malformed_encodings() {
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    attach(&db, b"original".to_vec(), true).await;
    let device_ref = db.file_ref("files", "7").await.unwrap();
    let id = uuid::Uuid::from_u128(7);
    let location = format!("uploaded {id} {}", "ab".repeat(32));
    // A committed uploaded-row fixture; file transfer and location-changing
    // operations belong to sync, not this database test.
    db.commit_writer(|sql| {
        sql.internal_execute("UPDATE files SET location=?1", [location])
            .unwrap()
    });
    let uploaded = db.file_ref("files", "7").await.unwrap();
    assert_eq!(uploaded.location(), FileLocation::Uploaded);
    assert!(!format!("{uploaded:?}").contains(&"ab".repeat(32)));
    let error = db
        .write(move |sql| sql.validate_file_ref(&device_ref))
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::FileRefChanged { .. }));
    db.commit_writer(|sql| {
        sql.internal_execute(
            "UPDATE files SET location=?1",
            [format!("uploaded {id} {}", "cd".repeat(32))],
        )
        .unwrap()
    });
    let error = db
        .write(move |sql| sql.validate_file_ref(&uploaded))
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::FileRefChanged { .. }));
    let uploaded = db.file_ref("files", "7").await.unwrap();
    db.commit_writer(|sql| {
        sql.internal_execute(
            "UPDATE files SET location=?1",
            [format!(
                "uploaded {} {}",
                uuid::Uuid::from_u128(8),
                "cd".repeat(32)
            )],
        )
        .unwrap()
    });
    assert!(matches!(
        db.write(move |sql| sql.validate_file_ref(&uploaded)).await,
        Err(DbError::FileRefChanged { .. })
    ));
    for device in [0, u64::MAX] {
        db.commit_writer(|sql| {
            sql.internal_execute("UPDATE files SET location=?1", [device.to_string()])
                .unwrap()
        });
        assert_eq!(
            db.file_ref("files", "7").await.unwrap().location(),
            FileLocation::OnDevice(coven_foundation::id_source::DeviceId(device))
        );
    }
    let valid = format!("uploaded {id} {}", "ab".repeat(32));
    let mut malformed: Vec<_> = (0..valid.len())
        .map(|end| valid[..end].to_owned())
        .collect();
    malformed.extend([
        "uploaded".into(),
        "uploaded:bad-key".into(),
        "-1".into(),
        "01".into(),
        "+1".into(),
        " 1".into(),
        "1 ".into(),
        "18446744073709551616".into(),
        "device:7".into(),
        format!("{valid} "),
        format!("{valid}00"),
        format!("uploaded {id} {}", "AB".repeat(32)),
        format!("uploaded {id} {}", "zz".repeat(32)),
        format!("uploaded {} {}", id.simple(), "ab".repeat(32)),
        format!("uploaded  {id} {}", "ab".repeat(32)),
        format!("uploaded {id} {}", "音".repeat(64)),
    ]);
    for malformed in malformed {
        db.commit_writer(|sql| {
            sql.internal_execute("UPDATE files SET location=?1", [&malformed])
                .unwrap()
        });
        assert!(
            matches!(
                db.file_ref("files", "7").await,
                Err(DbError::DamagedDatabase)
            ),
            "{malformed}"
        );
    }
    for value in [
        rusqlite::types::Value::Null,
        rusqlite::types::Value::Blob(vec![1, 2]),
    ] {
        db.commit_writer(|sql| {
            sql.internal_execute("UPDATE files SET location=?1", [value])
                .unwrap()
        });
        assert!(matches!(
            db.file_ref("files", "7").await,
            Err(DbError::DamagedDatabase)
        ));
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn original_facts_survive_reopen_and_are_not_replaced_by_new_disk_metadata() {
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::UserProvided), SCHEMA)
        .await
        .unwrap();
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
    db.write(move |sql| {
        sql.insert_user_file(
            "files",
            "7",
            prepared,
            "INSERT INTO files(id,size) VALUES('7',8)",
            &[],
        )
    })
    .await
    .unwrap();
    let reference = db.file_ref("files", "7").await.unwrap();
    let expected = UserFile {
        path: path.clone(),
        size: metadata.len(),
        modified_at: metadata.modified().unwrap(),
    };
    assert_eq!(
        db.user_file("files", "7").await.unwrap(),
        Some(expected.clone())
    );
    std::fs::write(&path, b"changed outside coven").unwrap();
    assert_eq!(
        db.user_file("files", "7").await.unwrap(),
        Some(expected.clone())
    );
    let reader = store
        .builder(
            tables(Provenance::UserProvided),
            vec![Migration::sql(1, "schema", SCHEMA)],
        )
        .open_read_only()
        .await
        .unwrap();
    assert_eq!(reader.file_ref("files", "7").await.unwrap(), reference);
    assert_eq!(
        reader.user_file("files", "7").await.unwrap(),
        Some(expected.clone())
    );
    db.close().await.unwrap();
    assert_eq!(reader.file_ref("files", "7").await.unwrap(), reference);
    reader.close().await.unwrap();
    let db = store
        .schema(tables(Provenance::UserProvided), SCHEMA)
        .await
        .unwrap();
    assert_eq!(db.user_file("files", "7").await.unwrap(), Some(expected));
    db.write(|sql| sql.clear_user_file("files", "7"))
        .await
        .unwrap();
    assert_eq!(db.user_file("files", "7").await.unwrap(), None);
    assert_eq!(db.user_file("files", "absent").await.unwrap(), None);
    assert!(matches!(
        db.file_ref("files", "7").await,
        Err(DbError::FileAttachment { .. })
    ));
    let stale = reference.clone();
    assert!(matches!(
        db.write(move |sql| sql.validate_file_ref(&stale)).await,
        Err(DbError::FileRefChanged { .. })
    ));
    db.write(|sql| {
        sql.execute("DELETE FROM files", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(matches!(
        db.write(move |sql| sql.validate_file_ref(&reference)).await,
        Err(DbError::FileRefChanged { .. })
    ));
    assert_eq!(std::fs::read(&path).unwrap(), b"changed outside coven");
    db.close().await.unwrap();
}

#[tokio::test]
async fn custom_columns_and_inherited_audience_are_read_together_with_composite_keys() {
    let store = TestStore::new();
    let declarations = || {
        vec![
            SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
            SyncedTable::new("files", RowIdentity::SharedKey)
                .key_columns(["note_id", "name"])
                .audience_from("note_id")
                .carries_files(
                    FileDecl::new(
                        "files",
                        Provenance::AppProvided,
                        Uploads::WhenAsked,
                        CacheFill::CacheLazy,
                    )
                    .with_id_column("TOKEN")
                    .with_size_column("BYTES")
                    .with_hash_column("DIGEST")
                    .with_location_column("WHERE_AT"),
                ),
        ]
    };
    let schema = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL); CREATE TABLE files(note_id TEXT NOT NULL REFERENCES notes(id),name TEXT NOT NULL,token TEXT,bytes INTEGER,digest BLOB,where_at TEXT,PRIMARY KEY(note_id,name))";
    let db = store.schema(declarations(), schema).await.unwrap();
    const NOTE: &str = "00000000-0000-4000-8000-000000000007";
    const CIRCLE: &str = "00000000-0000-4000-8000-000000000008";
    db.write_with_files(
        |batch| {
            batch.put_file("files", "content", b"original".to_vec());
            Ok(())
        },
        |sql| {
            sql.execute("INSERT INTO notes VALUES(?1,?2)", (NOTE, CIRCLE))?;
            sql.execute(
                "INSERT INTO files(note_id,name,token,bytes) VALUES(?1,'song','content',8)",
                [NOTE],
            )?;
            Ok(())
        },
    )
    .await
    .unwrap();
    let key = RowKey(vec![NOTE.to_owned().into(), "song".to_owned().into()]);
    let reference = db.file_ref("files", key.clone()).await.unwrap();
    assert_eq!(reference.column(), "token");
    assert_eq!(reference.key(), &key);
    assert_eq!(reference.plaintext_size(), 8);
    assert_eq!(
        reference.audience(),
        coven_merge::Audience::Circle(coven_foundation::id_source::CircleId(
            uuid::Uuid::parse_str(CIRCLE).unwrap()
        ))
    );
    let reader = store
        .builder(declarations(), vec![Migration::sql(1, "schema", schema)])
        .open_read_only()
        .await
        .unwrap();
    assert_eq!(
        reader.file_ref("files", key.clone()).await.unwrap(),
        reference
    );
    db.write(|sql| {
        sql.execute("UPDATE notes SET audience='store'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        reader.file_ref("files", key).await.unwrap().audience(),
        coven_merge::Audience::Store
    );
    assert!(matches!(
        db.write(move |sql| sql.validate_file_ref(&reference)).await,
        Err(DbError::FileRefChanged { .. })
    ));
    reader.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn references_read_one_committed_version_while_a_replacement_is_uncommitted() {
    use std::{sync::mpsc, time::Duration};
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::UserProvided), SCHEMA)
        .await
        .unwrap();
    let original = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(original.path(), b"old").unwrap();
    let prepared = prepare_user_file(original.path(), |_| {}).await.unwrap();
    db.write(move |sql| {
        sql.insert_user_file(
            "files",
            "7",
            prepared,
            "INSERT INTO files(id,size) VALUES('7',3)",
            &[],
        )
    })
    .await
    .unwrap();
    let before = db.file_ref("files", "7").await.unwrap();
    let replacement = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(replacement.path(), b"new bytes").unwrap();
    let prepared = prepare_user_file(replacement.path(), |_| {}).await.unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let writer = db.clone();
    let write = tokio::spawn(async move {
        writer
            .write(move |sql| {
                sql.execute("UPDATE files SET size=9", [])?;
                sql.register_user_file("files", "7", prepared)?;
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                Ok(())
            })
            .await
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let pending = db.file_ref("files", "7").await.unwrap();
    assert_eq!(pending, before);
    assert_eq!(
        db.user_file("files", "7").await.unwrap().unwrap().path,
        original.path()
    );
    release_tx.send(()).unwrap();
    write.await.unwrap().unwrap();
    let after = db.file_ref("files", "7").await.unwrap();
    assert_ne!(after, before);
    assert_eq!(after.plaintext_size(), 9);
    assert_eq!(
        db.user_file("files", "7").await.unwrap().unwrap().path,
        replacement.path()
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_downloaded_clear_drops_original_facts_and_invalidates_the_reference() {
    let ids = coven_foundation::id_source::SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store
        .schema(tables(Provenance::UserProvided), SCHEMA)
        .await
        .unwrap();
    let b = b_store
        .schema(tables(Provenance::UserProvided), SCHEMA)
        .await
        .unwrap();
    let original = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(original.path(), b"original").unwrap();
    let prepared = prepare_user_file(original.path(), |_| {}).await.unwrap();
    a.write(move |sql| {
        sql.insert_user_file(
            "files",
            "7",
            prepared,
            "INSERT INTO files(id,size) VALUES('7',8)",
            &[],
        )
    })
    .await
    .unwrap();
    let reference = a.file_ref("files", "7").await.unwrap();
    b.apply_downloaded(crate::write::tests::records(&a).remove(0).into())
        .await
        .unwrap();
    assert!(b.user_file("files", "7").await.unwrap().is_none());
    b.write(|sql| sql.clear_user_file("files", "7"))
        .await
        .unwrap();
    a.apply_downloaded(crate::write::tests::records(&b).remove(0).into())
        .await
        .unwrap();
    assert!(a.user_file("files", "7").await.unwrap().is_none());
    assert!(matches!(
        a.write(move |sql| sql.validate_file_ref(&reference)).await,
        Err(DbError::FileRefChanged { .. })
    ));
    assert_eq!(std::fs::read(original.path()).unwrap(), b"original");
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn absent_files_and_invalid_keys_return_errors_without_panicking() {
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    db.write(|sql| {
        sql.execute("INSERT INTO files(id) VALUES('empty')", [])?;
        Ok(())
    })
    .await
    .unwrap();
    for key in [
        RowKey::from("empty"),
        RowKey::from("absent"),
        RowKey(vec![rusqlite::types::Value::Null]),
    ] {
        assert!(matches!(
            db.file_ref("files", key).await,
            Err(DbError::FileAttachment { .. })
        ));
    }
    assert!(db.user_file("files", "empty").await.unwrap().is_none());
    db.close().await.unwrap();
    assert!(matches!(
        db.file_ref("files", "empty").await,
        Err(DbError::StoreClosed)
    ));
    assert!(matches!(
        db.user_file("files", "empty").await,
        Err(DbError::StoreClosed)
    ));
}
