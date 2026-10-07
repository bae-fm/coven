use crate::{tests::TestStore, write::tests::records, *};
use coven_foundation::{clock::FixedClock, id_source::SequentialIds};
use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

pub(crate) const SCHEMA: &str = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT,title TEXT)";

pub(crate) fn tables(kind: Provenance) -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("files", RowIdentity::SharedKey).carries_files(FileDecl::new(
            "files",
            kind,
            Uploads::WhenAsked,
            CacheFill::CacheLazy,
        )),
    ]
}

pub(crate) async fn attach(db: &Database, bytes: Vec<u8>, insert: bool) -> Result<(), DbError> {
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
}

#[tokio::test]
async fn size_ten_concurrent_replacements_keep_all_four_columns_from_the_later_write() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
    let a = a_store
        .builder(
            tables(Provenance::AppProvided),
            vec![Migration::sql(1, "files", SCHEMA)],
        )
        .clock(clock.clone())
        .open()
        .await
        .unwrap();
    let b = b_store
        .builder(
            tables(Provenance::AppProvided),
            vec![Migration::sql(1, "files", SCHEMA)],
        )
        .clock(clock.clone())
        .open()
        .await
        .unwrap();
    attach(&a, vec![b'I'; 10], true).await.unwrap();
    let initial = records(&a).remove(0);
    b.apply_downloaded(initial.clone().into()).await.unwrap();
    clock.set(UNIX_EPOCH + Duration::from_secs(2));
    attach(&a, vec![b'A'; 20], false).await.unwrap();
    clock.set(UNIX_EPOCH + Duration::from_secs(3));
    attach(&b, vec![b'B'; 10], false).await.unwrap();
    let ra = records(&a).remove(1);
    let rb = records(&b).remove(0);
    let expected = b
        .read(|sql| {
            Ok(
                sql.query_row("SELECT id,size,hash,location FROM files", [], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, Vec<u8>>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                })?,
            )
        })
        .await
        .unwrap();
    for changes in [[ra.clone(), rb.clone()], [rb.clone(), ra.clone()]] {
        let store = TestStore::with_ids(&ids);
        let db = store
            .schema(tables(Provenance::AppProvided), SCHEMA)
            .await
            .unwrap();
        db.apply_downloaded(initial.clone().into()).await.unwrap();
        for write in changes {
            db.apply_downloaded(write.into()).await.unwrap();
        }
        let size = db
            .read(|sql| Ok(sql.query_row("SELECT size FROM files", [], |r| r.get::<_, i64>(0))?))
            .await
            .unwrap();
        assert_eq!(size, 10);
        let actual = db
            .read(|sql| {
                Ok(
                    sql.query_row("SELECT id,size,hash,location FROM files", [], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, i64>(1)?,
                            r.get::<_, Vec<u8>>(2)?,
                            r.get::<_, String>(3)?,
                        ))
                    })?,
                )
            })
            .await
            .unwrap();
        assert_eq!(actual, expected);
        db.close().await.unwrap();
    }
    let coven_merge::Operation::Update(columns) = &rb.parts[0].rows[0].change.operation else {
        panic!("update");
    };
    assert_eq!(
        columns.keys().map(String::as_str).collect::<Vec<_>>(),
        ["hash", "id", "location", "size"]
    );
    a.close().await.unwrap();
    b.close().await.unwrap();
}

pub(crate) fn owned_paths(store: &TestStore) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(store.database_path().parent().unwrap().join("files"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect()
}

pub(crate) fn local_count(db: &Database, table: &str) -> i64 {
    db.inspect_writer(|sql| {
        sql.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    })
}

#[tokio::test]
async fn a_stream_exceeds_its_memory_budget_and_is_read_once() {
    use std::{
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll},
    };
    use tokio::io::{AsyncRead, ReadBuf};
    const BUDGET: usize = 64 * 1024;
    const LENGTH: usize = 32 * 1024 * 1024 + 17;
    struct Generated {
        read: Arc<AtomicUsize>,
        directory: std::path::PathBuf,
    }
    impl AsyncRead for Generated {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            out: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            assert!(
                out.remaining() <= BUDGET,
                "the stream buffer exceeded its budget"
            );
            let read = self.read.load(Ordering::SeqCst);
            let paths = std::fs::read_dir(&self.directory)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(paths.len(), 1);
            // A directory entry's cached size lags an open file on Windows;
            // the path's own metadata reads the file.
            assert_eq!(
                std::fs::metadata(paths[0].path()).unwrap().len(),
                read as u64,
                "bytes must reach disk before the next buffer is requested"
            );
            let count = out.remaining().min(LENGTH - read);
            out.put_slice(&[b'x'; BUDGET][..count]);
            self.read.fetch_add(count, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    let read = Arc::new(AtomicUsize::new(0));
    let source = FileSource::Stream(Box::pin(Generated {
        read: read.clone(),
        directory: store.database_path().parent().unwrap().join("files"),
    }));
    db.write_with_files(
        move |batch| {
            batch.put_file("files", "7", source);
            Ok(())
        },
        |sql| {
            sql.execute("INSERT INTO files(id,size) VALUES('7',?1)", [LENGTH as i64])?;
            Ok(())
        },
    )
    .await
    .unwrap();
    assert_eq!(read.load(Ordering::SeqCst), LENGTH);
    let paths = owned_paths(&store);
    assert_eq!(paths.len(), 1);
    assert_eq!(std::fs::metadata(&paths[0]).unwrap().len(), LENGTH as u64);
    let prepared = prepare_user_file(&paths[0], |_| {}).await.unwrap();
    let hash = db
        .read(|sql| Ok(sql.query_row("SELECT hash FROM files", [], |r| r.get::<_, Vec<u8>>(0))?))
        .await
        .unwrap();
    assert_eq!(hash, prepared.hashes.content.as_bytes());
    assert_eq!(local_count(&db, "_coven_device_files"), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn failed_sql_stream_and_commit_discard_new_bytes_and_preserve_old_bytes() {
    let store = TestStore::new();
    let schema = "CREATE TABLE parent(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE local_child(parent TEXT REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT,title TEXT)";
    let db = store
        .schema(tables(Provenance::AppProvided), schema)
        .await
        .unwrap();
    attach(&db, b"original".to_vec(), true).await.unwrap();
    let original = owned_paths(&store);
    for failure in ["sql", "commit", "stream", "unused"] {
        let error = db
            .write_with_files(
                move |batch| {
                    let source = if failure == "stream" {
                        FileSource::Stream(Box::pin(FailingReader { started: false }))
                    } else {
                        b"replacement".to_vec().into()
                    };
                    batch.put_file(
                        "files",
                        if failure == "unused" {
                            "unattached"
                        } else {
                            "7"
                        },
                        source,
                    );
                    Ok(())
                },
                move |sql| {
                    sql.execute("UPDATE files SET size=11", [])?;
                    if failure == "sql" {
                        return Err(DbError::StoreClosed);
                    }
                    if failure == "commit" {
                        sql.execute("INSERT INTO local_child VALUES('missing')", [])?;
                    }
                    Ok(())
                },
            )
            .await
            .unwrap_err();
        match failure {
            "sql" => assert!(matches!(error, DbError::StoreClosed)),
            "stream" => assert!(matches!(error, DbError::Disk(_))),
            "unused" => assert!(
                matches!(error, DbError::FileUnreferenced { namespace, id } if namespace == "files" && id == "unattached")
            ),
            "commit" => assert!(matches!(error, DbError::Sqlite(_)), "{error:?}"),
            _ => unreachable!(),
        }
        assert_eq!(local_count(&db, "_coven_file_removals"), 0);
        assert_eq!(owned_paths(&store), original);
        assert_eq!(std::fs::read(&original[0]).unwrap(), b"original");
        assert_eq!(local_count(&db, "_coven_device_files"), 1);
        assert_eq!(records(&db).len(), 1);
    }
    attach(&db, b"replacement".to_vec(), false).await.unwrap();
    assert!(!original[0].exists());
    assert_eq!(owned_paths(&store).len(), 1);
    db.write(|sql| {
        sql.execute("DELETE FROM files", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(owned_paths(&store).is_empty());
    assert_eq!(local_count(&db, "_coven_device_files"), 0);
    db.close().await.unwrap();
}

struct FailingReader {
    started: bool,
}
impl tokio::io::AsyncRead for FailingReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        out: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.started {
            std::task::Poll::Ready(Err(std::io::Error::other("source stopped")))
        } else {
            out.put_slice(b"partial");
            self.started = true;
            std::task::Poll::Ready(Ok(()))
        }
    }
}

#[tokio::test]
async fn originals_are_checked_during_preparation_at_registration_and_before_commit() {
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::UserProvided), SCHEMA)
        .await
        .unwrap();
    let original = tempfile::NamedTempFile::new().unwrap();
    let path = original.path().to_owned();
    std::fs::write(&path, vec![1; 256 * 1024]).unwrap();
    let changed = std::sync::atomic::AtomicBool::new(false);
    let error = prepare_user_file(&path, |_| {
        if !changed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            std::fs::write(&path, b"changed while reading").unwrap();
        }
    })
    .await
    .unwrap_err();
    assert!(matches!(error, DbError::UserFileChanged { .. }));
    for before_register in [true, false] {
        std::fs::write(&path, b"original").unwrap();
        let prepared = prepare_user_file(&path, |_| {}).await.unwrap();
        if before_register {
            std::fs::write(&path, b"changed").unwrap();
        }
        let change_path = path.clone();
        let error = db
            .write(move |sql| {
                sql.insert_user_file(
                    "files",
                    "7",
                    prepared,
                    "INSERT INTO files(id,size) VALUES('7',8)",
                    &[],
                )?;
                if !before_register {
                    std::fs::write(change_path, b"changed").unwrap();
                }
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(matches!(error, DbError::UserFileChanged { .. }));
        assert_eq!(local_count(&db, "files"), 0);
        assert_eq!(local_count(&db, "_coven_user_files"), 0);
    }
    assert!(owned_paths(&store).is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn original_size_is_checked_against_the_final_row_and_clearing_keeps_the_original() {
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::UserProvided), SCHEMA)
        .await
        .unwrap();
    let original = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(original.path(), b"original").unwrap();
    for after_register in [false, true] {
        let prepared = prepare_user_file(original.path(), |_| {}).await.unwrap();
        let error = db
            .write(move |sql| {
                sql.execute(
                    "INSERT INTO files(id,size) VALUES('7',?1)",
                    [if after_register { 8 } else { 9 }],
                )?;
                sql.register_user_file("files", "7", prepared)?;
                if after_register {
                    sql.execute("UPDATE files SET size=9", [])?;
                }
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            DbError::FileSizeMismatch {
                expected: 9,
                actual: 8
            }
        ));
        assert_eq!(local_count(&db, "files"), 0);
    }
    let prepared = prepare_user_file(original.path(), |_| {}).await.unwrap();
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
    assert_eq!(local_count(&db, "_coven_user_files"), 1);
    db.write(|sql| sql.clear_user_file("files", "7"))
        .await
        .unwrap();
    assert_eq!(local_count(&db, "_coven_user_files"), 0);
    let last = records(&db).pop().unwrap();
    let coven_merge::Operation::Update(columns) = &last.parts[0].rows[0].change.operation else {
        panic!("update");
    };
    assert_eq!(columns.len(), 4);
    assert_eq!(columns["hash"].value, coven_format::value::Value::Null);
    assert_eq!(columns["location"].value, coven_format::value::Value::Null);
    let prepared = prepare_user_file(original.path(), |_| {}).await.unwrap();
    db.write(move |sql| sql.register_user_file("files", "7", prepared))
        .await
        .unwrap();
    db.write(|sql| {
        sql.execute("DELETE FROM files", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(local_count(&db, "_coven_user_files"), 0);
    assert_eq!(std::fs::read(original.path()).unwrap(), b"original");
    assert!(owned_paths(&store).is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn write_once_refuses_replacing_bytes_even_under_the_same_id() {
    let store = TestStore::new();
    let mut declarations = tables(Provenance::AppProvided);
    declarations[0].files = declarations[0].files.take().map(FileDecl::write_once);
    let db = store.schema(declarations, SCHEMA).await.unwrap();
    attach(&db, b"original".to_vec(), true).await.unwrap();
    let paths = owned_paths(&store);
    let error = attach(&db, b"different".to_vec(), false).await.unwrap_err();
    assert!(matches!(error, DbError::FileWriteOnce { table, .. } if table=="files"));
    assert_eq!(owned_paths(&store), paths);
    db.write(|sql| {
        sql.execute("UPDATE files SET title='allowed'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let last = records(&db).pop().unwrap();
    let coven_merge::Operation::Update(columns) = &last.parts[0].rows[0].change.operation else {
        panic!("update");
    };
    assert_eq!(columns.keys().collect::<Vec<_>>(), ["title"]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn app_cannot_assign_hash_or_location_even_null_unchanged_or_ignored() {
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    attach(&db, b"original".to_vec(), true).await.unwrap();
    for statement in [
        "UPDATE files SET hash=hash",
        "UPDATE files SET location=NULL",
        "INSERT INTO files(id,size,hash) VALUES('8',0,NULL)",
        "INSERT INTO files(id,size,location) SELECT '8',0,NULL",
        "INSERT INTO files VALUES('8',0,NULL,NULL,NULL)",
        "INSERT INTO files(id,size) VALUES('8',0); UPDATE files SET hash=NULL",
        "INSERT INTO files(id,size) VALUES('7',0) ON CONFLICT DO UPDATE SET location=NULL",
    ] {
        for ignored in [false, true] {
            let error = db
                .write(move |sql| {
                    let error = sql.execute_batch(statement).unwrap_err();
                    if !ignored {
                        return Err(error.into());
                    }
                    sql.execute("UPDATE files SET title='must roll back'", [])?;
                    Ok(())
                })
                .await
                .unwrap_err();
            assert!(
                matches!(error, DbError::FileColumnWrite {table, ..} if table=="files"),
                "{statement}"
            );
        }
    }
    assert_eq!(records(&db).len(), 1);
    assert_eq!(local_count(&db, "files"), 1);
    db.write(|sql| {
        sql.execute("UPDATE files SET title='allowed'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn shared_triggers_cannot_write_managed_columns_and_allowed_inserts_can_attach() {
    for body in [
        "UPDATE files SET hash=NULL;",
        "INSERT INTO files(id,size,hash) VALUES('7',8,NULL);",
        "INSERT INTO files(id,size,location) VALUES('7',8,NULL);",
    ] {
        let store = TestStore::new();
        let mut declarations = tables(Provenance::AppProvided);
        declarations.push(
            SyncedTable::new("commands", RowIdentity::SharedKey).shared_trigger("assign_file"),
        );
        let db = store.builder(declarations, vec![Migration::run(1,"trigger",move |sql| {
            sql.execute_batch(&format!("{SCHEMA}; CREATE TABLE commands(id TEXT NOT NULL PRIMARY KEY); CREATE TRIGGER assign_file AFTER INSERT ON commands WHEN NOT coven_applying() BEGIN {body} END"))?;
            Ok(())
        })]).open().await.unwrap();
        let error = db
            .write(|sql| {
                sql.execute("INSERT INTO commands VALUES('x')", [])?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(matches!(error, DbError::FileColumnWrite {table,..} if table=="files"));
        assert_eq!(local_count(&db, "commands"), 0);
        db.close().await.unwrap();
    }
    let store = TestStore::new();
    let mut declarations = tables(Provenance::AppProvided);
    declarations
        .push(SyncedTable::new("commands", RowIdentity::SharedKey).shared_trigger("attach_file"));
    let db = store.builder(declarations, vec![Migration::run(1,"trigger", |sql| {
        sql.execute_batch(&format!("{SCHEMA}; CREATE TABLE commands(id TEXT NOT NULL PRIMARY KEY); CREATE TRIGGER attach_file AFTER INSERT ON commands WHEN NOT coven_applying() BEGIN INSERT INTO files(id,size) VALUES(new.id,8); END"))?;
        Ok(())
    })]).open().await.unwrap();
    db.write_with_files(
        |batch| {
            batch.put_file("files", "7", b"original".to_vec());
            Ok(())
        },
        |sql| {
            sql.execute("INSERT INTO commands VALUES('7')", [])?;
            Ok(())
        },
    )
    .await
    .unwrap();
    assert_eq!(local_count(&db, "_coven_device_files"), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn shared_bytes_last_until_the_final_row_deletion_commits() {
    let store = TestStore::new();
    let declaration = SyncedTable::new("files", RowIdentity::SharedKey).carries_files(
        FileDecl::new(
            "files",
            Provenance::AppProvided,
            Uploads::WhenAsked,
            CacheFill::CacheLazy,
        )
        .with_id_column("file"),
    );
    let schema = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT,size INTEGER,hash BLOB,location TEXT)";
    let db = store.schema(vec![declaration], schema).await.unwrap();
    db.write_with_files(
        |batch| {
            batch.put_file("files", "shared", b"original".to_vec());
            Ok(())
        },
        |sql| {
            sql.execute_batch(
                "INSERT INTO files(id,file,size) VALUES('one','shared',8),('two','shared',8)",
            )?;
            Ok(())
        },
    )
    .await
    .unwrap();
    let paths = owned_paths(&store);
    assert_eq!(paths.len(), 1);
    assert_eq!(local_count(&db, "_coven_device_files"), 2);
    db.write(|sql| {
        sql.execute("DELETE FROM files WHERE id='one'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(owned_paths(&store), paths);
    let held = paths[0].clone();
    let error = db
        .write(move |sql| {
            sql.execute("DELETE FROM files WHERE id='two'", [])?;
            assert_eq!(std::fs::read(held).unwrap(), b"original");
            Err::<(), _>(DbError::StoreClosed)
        })
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::StoreClosed));
    assert_eq!(owned_paths(&store), paths);
    let held = paths[0].clone();
    db.write(move |sql| {
        sql.execute("DELETE FROM files WHERE id='two'", [])?;
        assert_eq!(std::fs::read(held).unwrap(), b"original");
        Ok(())
    })
    .await
    .unwrap();
    assert!(owned_paths(&store).is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_move_preserves_the_owned_copy_and_a_panicking_write_removes_new_bytes() {
    let store = TestStore::new();
    let declaration = SyncedTable::new("files", RowIdentity::IndependentUuid)
        .audience_column("audience")
        .carries_files(FileDecl::new(
            "files",
            Provenance::AppProvided,
            Uploads::WhenAsked,
            CacheFill::CacheLazy,
        ));
    let schema = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT,audience TEXT NOT NULL)";
    let db = store.schema(vec![declaration], schema).await.unwrap();
    const ID: &str = "00000000-0000-4000-8000-000000000001";
    db.write_with_files(
        |batch| {
            batch.put_file("files", ID, b"original".to_vec());
            Ok(())
        },
        |sql| {
            sql.execute(
                "INSERT INTO files(id,size,audience) VALUES(?1,8,'store')",
                [ID],
            )?;
            Ok(())
        },
    )
    .await
    .unwrap();
    let paths = owned_paths(&store);
    db.write(|sql| {
        sql.execute(
            "UPDATE files SET audience='00000000-0000-4000-8000-000000000002'",
            [],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(owned_paths(&store), paths);
    assert_eq!(local_count(&db, "_coven_device_files"), 1);
    let other = db.clone();
    let failure = tokio::spawn(async move {
        other
            .write_with_files(
                |batch| {
                    batch.put_file("files", ID, b"different".to_vec());
                    Ok(())
                },
                |_| -> Result<(), DbError> {
                    panic!("app failed");
                },
            )
            .await
    })
    .await
    .unwrap_err();
    assert!(failure.is_panic());
    assert_eq!(owned_paths(&store), paths);
    db.close().await.unwrap();
}

#[tokio::test]
async fn downloaded_deletion_removes_records_and_bytes_after_commit() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    let b = b_store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    attach(&a, b"original".to_vec(), true).await.unwrap();
    b.apply_downloaded(records(&a).remove(0).into())
        .await
        .unwrap();
    b.write(|sql| {
        sql.execute("DELETE FROM files", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let deletion = records(&b).remove(0);
    a.inspect_writer(|sql| sql.batch("CREATE TRIGGER fail_delete AFTER DELETE ON _coven_device_files BEGIN SELECT RAISE(ABORT,'cannot forget file'); END").unwrap());
    let paths = owned_paths(&a_store);
    assert!(a.apply_downloaded(deletion.clone().into()).await.is_err());
    assert_eq!(local_count(&a, "files"), 1);
    assert_eq!(local_count(&a, "_coven_device_files"), 1);
    assert_eq!(owned_paths(&a_store), paths);
    a.inspect_writer(|sql| sql.batch("DROP TRIGGER fail_delete").unwrap());
    a.apply_downloaded(deletion.into()).await.unwrap();
    assert_eq!(local_count(&a, "files"), 0);
    assert_eq!(local_count(&a, "_coven_device_files"), 0);
    assert!(owned_paths(&a_store).is_empty());
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn constraint_removal_preserves_bytes_until_the_row_is_restored() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let schema = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT,title TEXT UNIQUE)";
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
    let mut databases = Vec::new();
    for store in [&a_store, &b_store] {
        databases.push(
            store
                .builder(
                    tables(Provenance::AppProvided),
                    vec![Migration::sql(1, "files", schema)],
                )
                .clock(clock.clone())
                .open()
                .await
                .unwrap(),
        );
    }
    let a = databases.remove(0);
    let b = databases.remove(0);
    attach(&a, b"original".to_vec(), true).await.unwrap();
    a.write(|sql| {
        sql.execute("UPDATE files SET title='same'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    clock.set(UNIX_EPOCH + Duration::from_millis(500));
    b.write(|sql| {
        sql.execute(
            "INSERT INTO files(id,size,title) VALUES('other',0,'same')",
            [],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    a.apply_downloaded(records(&b).remove(0).into())
        .await
        .unwrap();
    let present = a
        .read(|sql| Ok(sql.query_row("SELECT id FROM files", [], |r| r.get::<_, String>(0))?))
        .await
        .unwrap();
    assert_eq!(present, "other");
    assert_eq!(owned_paths(&a_store).len(), 1);
    assert_eq!(local_count(&a, "_coven_device_files"), 1);
    b.write(|sql| {
        sql.execute("DELETE FROM files", [])?;
        Ok(())
    })
    .await
    .unwrap();
    a.apply_downloaded(records(&b).remove(1).into())
        .await
        .unwrap();
    let present = a
        .read(|sql| Ok(sql.query_row("SELECT id FROM files", [], |r| r.get::<_, String>(0))?))
        .await
        .unwrap();
    assert_eq!(present, "7");
    assert_eq!(owned_paths(&a_store).len(), 1);
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn staged_bytes_supply_the_app_files_size() {
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    for (insert, bytes) in [
        (true, b"original".to_vec()),
        (false, b"replacement".to_vec()),
    ] {
        let size = bytes.len() as u64;
        db.write_with_files(
            move |batch| {
                batch.put_file("files", "7", bytes);
                Ok(())
            },
            move |sql| {
                sql.execute(
                    if insert {
                        "INSERT INTO files(id) VALUES('7')"
                    } else {
                        "UPDATE files SET size=999"
                    },
                    [],
                )?;
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(
            db.file_ref("files", "7").await.unwrap().plaintext_size(),
            size
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_trigger_cannot_leave_a_new_owned_file_without_its_row() {
    let store = TestStore::new();
    let mut declarations = tables(Provenance::AppProvided);
    declarations[0] = declarations[0].clone().shared_trigger("cancel_file");
    let db = store.builder(declarations, vec![Migration::run(1, "files", |sql| {
        sql.execute_batch(&format!("{SCHEMA}; CREATE TRIGGER cancel_file AFTER UPDATE OF hash ON files WHEN NOT coven_applying() BEGIN DELETE FROM files; END"))?;
        Ok(())
    })]).open().await.unwrap();
    assert!(matches!(
        attach(&db, b"original".to_vec(), true).await,
        Err(DbError::FileRowRemoved { table, key }) if table == "files" && key == RowKey::from("7")
    ));
    assert_eq!(local_count(&db, "files"), 0);
    assert_eq!(local_count(&db, "_coven_device_files"), 0);
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    assert!(owned_paths(&store).is_empty());
    db.close().await.unwrap();
}
