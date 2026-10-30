use crate::{tests::TestStore, write::tests::records, *};
use coven_foundation::{
    clock::FixedClock,
    id_source::{CircleId, FileId, SequentialIds},
};
use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

/// A synced table whose rows reference one file each, by id.
pub(crate) const SCHEMA: &str = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT,title TEXT); CREATE INDEX files_file ON files(file)";

pub(crate) fn tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("files", RowIdentity::SharedKey).file_column(FileColumn::new("file"))]
}

pub(crate) fn namespaces(kind: Provenance) -> Vec<Namespace> {
    vec![Namespace::new("files", kind, 1 << 30)]
}

/// A store with `tables()` and one namespace, `files`, of this kind.
pub(crate) async fn open(store: &TestStore, kind: Provenance) -> Database {
    store
        .builder(tables(), vec![Migration::addition(1, "files", SCHEMA)])
        .namespaces(namespaces(kind))
        .open()
        .await
        .unwrap()
}

/// Stage `bytes` and point row `7` at it, inserting the row or updating it.
pub(crate) async fn stage(db: &Database, bytes: Vec<u8>, insert: bool) -> Result<FileId, DbError> {
    db.write_with_files::<_, _, _, _, crate::DbError>(
        move |batch| Ok(batch.put_file("files", bytes)),
        move |sql, file| {
            sql.execute(
                if insert {
                    "INSERT INTO files(id,file) VALUES('7',?1)"
                } else {
                    "UPDATE files SET file=?1 WHERE id='7'"
                },
                [file],
            )?;
            Ok(file)
        },
    )
    .await
}

pub(crate) fn owned_paths(store: &TestStore) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(store.database_path().parent().unwrap().join("files"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect()
}

/// Complete every queued upload as confirmed storage would: its files switch
/// to the cloud, and the queue's owned copies are released.
pub(crate) async fn complete_uploads(db: &Database) {
    let owner = crate::FileDatabase::new(db.clone());
    // Uploads no file waits for too: their attempts end the same way.
    let ids = db.inspect_writer(|sql| {
        sql.query("SELECT id FROM _coven_file_uploads ORDER BY id", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap()
    });
    for id in ids {
        owner.complete_upload(id, ()).await.unwrap();
    }
}

pub(crate) fn local_count(db: &Database, table: &str) -> i64 {
    db.inspect_writer(|sql| {
        sql.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    })
}

/// Whether this device has a record of `file`.
pub(crate) fn has_record(db: &Database, file: FileId) -> bool {
    db.inspect_writer(|sql| {
        sql.query_row(
            "SELECT EXISTS(SELECT 1 FROM main.coven_files WHERE id=?1)",
            [file],
            |r| r.get(0),
        )
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
    let db = open(&store, Provenance::AppProvided).await;
    let read = Arc::new(AtomicUsize::new(0));
    let source = FileSource::Stream(Box::pin(Generated {
        read: read.clone(),
        directory: store.database_path().parent().unwrap().join("files"),
    }));
    let file = db
        .write_with_files::<_, _, _, _, crate::DbError>(
            move |batch| Ok(batch.put_file("files", source)),
            |sql, file| {
                sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [file])?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    assert_eq!(read.load(Ordering::SeqCst), LENGTH);
    let paths = owned_paths(&store);
    assert_eq!(paths.len(), 1);
    assert_eq!(std::fs::metadata(&paths[0]).unwrap().len(), LENGTH as u64);
    // The file is on this device until its upload completes (§17.3), with
    // the size of the bytes read once.
    let reference = db.file_ref(file).await.unwrap();
    assert_eq!(reference.size(), LENGTH as u64);
    assert_eq!(
        reference.place(),
        FilePlace::DeviceOnly {
            device: store.device()
        }
    );
    let prepared = prepare_user_file(&paths[0], |_| {}).await.unwrap();
    let hash: Vec<u8> = db.inspect_writer(|sql| {
        sql.query_row("SELECT hash FROM _coven_file_uploads", [], |r| r.get(0))
            .unwrap()
    });
    assert_eq!(hash, prepared.hashes.content.as_bytes());
    assert_eq!(local_count(&db, "_coven_device_files"), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn failed_sql_stream_and_commit_discard_new_bytes_and_preserve_old_bytes() {
    let store = TestStore::new();
    let schema = "CREATE TABLE parent(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE local_child(parent TEXT REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT,title TEXT); CREATE INDEX files_file ON files(file)";
    let db = store
        .builder(tables(), vec![Migration::addition(1, "files", schema)])
        .namespaces(namespaces(Provenance::AppProvided))
        .open()
        .await
        .unwrap();
    let original = stage(&db, b"original".to_vec(), true).await.unwrap();
    let paths = owned_paths(&store);
    for failure in ["sql", "commit", "stream", "unused"] {
        let error = db
            .write_with_files::<_, _, _, _, crate::DbError>(
                move |batch| {
                    let source = if failure == "stream" {
                        FileSource::Stream(Box::pin(FailingReader { started: false }))
                    } else {
                        b"replacement".to_vec().into()
                    };
                    Ok(batch.put_file("files", source))
                },
                move |sql, file| {
                    if failure != "unused" {
                        sql.execute("UPDATE files SET file=?1", [file])?;
                    }
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
                matches!(error, DbError::FileUnreferenced { namespace, .. } if namespace == "files")
            ),
            "commit" => assert!(matches!(error, DbError::Sqlite(_)), "{error:?}"),
            _ => unreachable!(),
        }
        assert_eq!(local_count(&db, "_coven_file_removals"), 0);
        assert_eq!(owned_paths(&store), paths);
        assert_eq!(std::fs::read(&paths[0]).unwrap(), b"original");
        assert_eq!(local_count(&db, "_coven_device_files"), 1);
        assert_eq!(local_count(&db, "coven_files"), 1);
        assert_eq!(records(&db).len(), 1);
    }
    // The replaced file goes with its last reference, in the replacing
    // write; its upload keeps its copy until its attempt ends.
    stage(&db, b"replacement".to_vec(), false).await.unwrap();
    assert!(!has_record(&db, original));
    assert!(paths[0].exists());
    assert_eq!(owned_paths(&store).len(), 2);
    db.write(|sql| {
        sql.execute("DELETE FROM files", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(local_count(&db, "coven_files"), 0);
    assert_eq!(owned_paths(&store).len(), 2);
    complete_uploads(&db).await;
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

/// An original is checked while it is prepared, before the writer is taken.
/// Staging and committing touch nothing at its path, so a FIFO in its place,
/// where a stalled mount would hang any stat or open, cannot hold back the
/// write; its upload finds the change (§17.8).
#[cfg(unix)]
#[tokio::test]
async fn originals_are_checked_while_prepared_and_never_under_the_writer() {
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
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
    std::fs::write(&path, b"original").unwrap();
    let prepared = prepare_user_file(&path, |_| {}).await.unwrap();
    std::fs::remove_file(&path).unwrap();
    assert!(std::process::Command::new("mkfifo")
        .arg(&path)
        .status()
        .unwrap()
        .success());
    tokio::time::timeout(
        Duration::from_secs(5),
        db.write_with_files::<_, _, _, _, crate::DbError>(
            move |batch| Ok(batch.put_user_file("files", prepared)),
            |sql, file| {
                sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [file])?;
                Ok(())
            },
        ),
    )
    .await
    .expect("a write waited on its original")
    .unwrap();
    assert_eq!(local_count(&db, "_coven_user_files"), 1);
    db.close().await.unwrap();
}

/// Deleting a user file's last reference forgets its original, which it
/// never touches.
#[tokio::test]
async fn a_user_files_last_reference_forgets_its_original_and_leaves_it() {
    let store = TestStore::new();
    let db = open(&store, Provenance::UserProvided).await;
    let original = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(original.path(), b"original").unwrap();
    let prepared = prepare_user_file(original.path(), |_| {}).await.unwrap();
    let file = db
        .write_with_files::<_, _, _, _, crate::DbError>(
            move |batch| Ok(batch.put_user_file("files", prepared)),
            |sql, file| {
                sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [file])?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    assert_eq!(
        db.user_file(file).await.unwrap().unwrap().path,
        original.path()
    );
    db.write(|sql| {
        sql.execute("UPDATE files SET file=NULL", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(!has_record(&db, file));
    assert_eq!(db.user_file(file).await.unwrap(), None);
    assert_eq!(local_count(&db, "_coven_user_files"), 0);
    assert_eq!(std::fs::read(original.path()).unwrap(), b"original");
    db.close().await.unwrap();
}

/// A file is staged into a declared namespace of its kind (§17.1).
#[tokio::test]
async fn the_wrong_kind_or_an_undeclared_namespace_fails() {
    let store = TestStore::new();
    let db = store
        .builder(tables(), vec![Migration::addition(1, "files", SCHEMA)])
        .namespaces(vec![
            Namespace::new("files", Provenance::AppProvided, 1 << 30),
            Namespace::new("originals", Provenance::UserProvided, 1 << 30),
        ])
        .open()
        .await
        .unwrap();
    let original = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(original.path(), b"original").unwrap();
    for (namespace, user, expected) in [
        ("photos", false, None),
        ("originals", false, Some(Provenance::UserProvided)),
        ("files", true, Some(Provenance::AppProvided)),
    ] {
        let prepared = prepare_user_file(original.path(), |_| {}).await.unwrap();
        let error = db
            .write_with_files::<_, _, _, _, crate::DbError>(
                move |batch| {
                    Ok(if user {
                        batch.put_user_file(namespace, prepared)
                    } else {
                        batch.put_file(namespace, b"bytes".to_vec())
                    })
                },
                |sql, file| {
                    sql.execute("INSERT INTO files(id,file) VALUES('7',?1)", [file])?;
                    Ok(())
                },
            )
            .await
            .unwrap_err();
        match expected {
            None => assert!(
                matches!(&error, DbError::UndeclaredNamespace { namespace: n } if n == namespace),
                "{error:?}"
            ),
            Some(kind) => assert!(
                matches!(&error, DbError::NamespaceKind { namespace: n, kind: k } if n == namespace && *k == kind),
                "{error:?}"
            ),
        }
    }
    assert_eq!(local_count(&db, "files"), 0);
    assert!(owned_paths(&store).is_empty());
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    db.close().await.unwrap();
}

/// A reference names a file of its own audience that this device has a
/// record of, or is NULL (§17.2).
#[tokio::test]
async fn a_reference_naming_no_file_or_another_audiences_fails_the_write() {
    let store = TestStore::new();
    let circle = CircleId(uuid::Uuid::parse_str("00000000-0000-4000-8000-000000000002").unwrap());
    let declaration = SyncedTable::new("photos", RowIdentity::IndependentUuid)
        .audience_column("audience")
        .file_column(FileColumn::new("file"));
    let schema = "CREATE TABLE photos(id TEXT NOT NULL PRIMARY KEY,file TEXT,audience TEXT NOT NULL); CREATE INDEX photos_file ON photos(file)";
    let db = store
        .builder(
            vec![declaration],
            vec![Migration::addition(1, "photos", schema)],
        )
        .namespaces(namespaces(Provenance::AppProvided))
        .open()
        .await
        .unwrap();
    store.join_circle(&db, circle);
    let file = db
        .write_with_files::<_, _, _, _, crate::DbError>(
            |batch| Ok(batch.put_file("files", b"photo".to_vec())),
            |sql, file| {
                sql.execute(
                    "INSERT INTO photos(id,file,audience) VALUES('00000000-0000-4000-8000-000000000011',?1,'store')",
                    [file],
                )?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    for (value, audience) in [
        (file.0.to_string(), circle.to_string()),
        (
            "00000000-0000-4000-8000-0000000000ff".to_owned(),
            "store".to_owned(),
        ),
        ("not a file".to_owned(), "store".to_owned()),
    ] {
        let named = value.clone();
        let error = db
            .write(move |sql| {
                sql.execute(
                    "INSERT INTO photos(id,file,audience) VALUES('00000000-0000-4000-8000-000000000012',?1,?2)",
                    [&value, &audience],
                )?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(
            matches!(&error, DbError::FileUnknown { table, column, file } if table == "photos" && column == "file" && *file == named),
            "{error:?}"
        );
    }
    // A staged file is made in the audience of the rows naming it, so rows
    // of two audiences can't name one.
    let error = db
        .write_with_files::<_, _, _, _, crate::DbError>(
            |batch| Ok(batch.put_file("files", b"photo".to_vec())),
            move |sql, file| {
                sql.execute(
                    "INSERT INTO photos(id,file,audience) VALUES('00000000-0000-4000-8000-000000000013',?1,'store'),('00000000-0000-4000-8000-000000000014',?1,?2)",
                    rusqlite::params![file, circle.to_string()],
                )?;
                Ok(())
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::FileUnknown { .. }), "{error:?}");
    assert_eq!(local_count(&db, "photos"), 1);
    assert_eq!(records(&db).len(), 1);
    db.close().await.unwrap();
}

/// A file's record is a row of `coven_files` in the file's audience: it
/// travels in that audience's part, encrypted with its key (§17.2).
#[tokio::test]
async fn a_record_reaches_its_circle_and_not_the_store() {
    let store = TestStore::new();
    let circle = CircleId(uuid::Uuid::parse_str("00000000-0000-4000-8000-000000000002").unwrap());
    let declaration = SyncedTable::new("photos", RowIdentity::IndependentUuid)
        .audience_column("audience")
        .file_column(FileColumn::new("file"));
    let schema = "CREATE TABLE photos(id TEXT NOT NULL PRIMARY KEY,file TEXT,audience TEXT NOT NULL); CREATE INDEX photos_file ON photos(file)";
    let db = store
        .builder(
            vec![declaration],
            vec![Migration::addition(1, "photos", schema)],
        )
        .namespaces(namespaces(Provenance::AppProvided))
        .open()
        .await
        .unwrap();
    store.join_circle(&db, circle);
    let file = db
        .write_with_files::<_, _, _, _, crate::DbError>(
            |batch| Ok(batch.put_file("files", b"gift".to_vec())),
            move |sql, file| {
                sql.execute(
                    "INSERT INTO photos(id,file,audience) VALUES('00000000-0000-4000-8000-000000000011',?1,?2)",
                    rusqlite::params![file, circle.to_string()],
                )?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    let record = records(&db).pop().unwrap();
    assert_eq!(record.parts.len(), 1);
    let part = &record.parts[0];
    assert_eq!(part.audience, coven_merge::Audience::Circle(circle));
    let tables: Vec<_> = part.rows.iter().map(|row| row.row.table.as_str()).collect();
    assert_eq!(tables, ["coven_files", "photos"]);
    let coven_merge::Operation::Insert(columns) = &part.rows[0].change.operation else {
        panic!("a new file's record is inserted");
    };
    assert_eq!(
        columns["coordinates"].value,
        coven_format::value::Value::Text(format!("device {}", store.device().0))
    );
    assert_eq!(columns["hash"].value, coven_format::value::Value::Null);
    // The row's reference names the record as its parent.
    let coven_merge::Operation::Insert(photo) = &part.rows[1].change.operation else {
        panic!("insert");
    };
    let parent = photo["file"].parents.values().next().unwrap();
    assert_eq!(
        parent.row,
        crate::file_table::row(file, coven_merge::Audience::Circle(circle)).unwrap()
    );
    // Completing the upload switches the record, in the circle too.
    complete_uploads(&db).await;
    let completion = records(&db).pop().unwrap();
    assert_eq!(completion.parts.len(), 1);
    assert_eq!(
        completion.parts[0].audience,
        coven_merge::Audience::Circle(circle)
    );
    let coven_merge::Operation::Update(place) = &completion.parts[0].rows[0].change.operation
    else {
        panic!("update");
    };
    assert_eq!(
        place.keys().map(String::as_str).collect::<Vec<_>>(),
        ["coordinates", "hash"]
    );
    let FilePlace::Cloud { uploader, .. } = db.file_ref(file).await.unwrap().place() else {
        panic!("stored");
    };
    assert_eq!(uploader, store.device());
    db.close().await.unwrap();
}

/// A deleted circle's records never come back (§15.9), so a file of it on
/// this device only is forgotten as a deleted record is: no file waits for
/// its upload any more, and the upload retires instead of sending bytes no
/// record will name.
#[tokio::test]
async fn a_deleted_circles_queued_upload_retires() {
    let store = TestStore::new();
    let circle = CircleId(uuid::Uuid::parse_str("00000000-0000-4000-8000-000000000002").unwrap());
    let declaration = SyncedTable::new("photos", RowIdentity::IndependentUuid)
        .audience_column("audience")
        .file_column(FileColumn::new("file"));
    let schema = "CREATE TABLE photos(id TEXT NOT NULL PRIMARY KEY,file TEXT,audience TEXT NOT NULL); CREATE INDEX photos_file ON photos(file)";
    let db = store
        .builder(
            vec![declaration],
            vec![Migration::addition(1, "photos", schema)],
        )
        .namespaces(namespaces(Provenance::AppProvided))
        .open()
        .await
        .unwrap();
    store.join_circle(&db, circle);
    db.write_with_files::<_, _, _, _, crate::DbError>(
        |batch| Ok(batch.put_file("files", b"gift".to_vec())),
        move |sql, file| {
            sql.execute(
                "INSERT INTO photos(id,file,audience) VALUES('00000000-0000-4000-8000-000000000011',?1,?2)",
                rusqlite::params![file, circle.to_string()],
            )?;
            Ok(file)
        },
    )
    .await
    .unwrap();
    let files = crate::FileDatabase::new(db.clone());
    assert_eq!(files.uploads().await.unwrap().len(), 1);
    crate::tests::delete_circle(&db, circle).await.unwrap();
    assert!(files.uploads().await.unwrap().is_empty());
    let retired = files
        .retire_unlinked(std::collections::BTreeSet::new(), |_| ())
        .await
        .unwrap();
    assert_eq!(retired.len(), 1);
    assert!(files.all_uploads().await.unwrap().is_empty());
    db.close().await.unwrap();
}

/// Several rows of a file's audience may reference it, as a foreign key
/// allows; its record goes in the write removing its last reference
/// (§17.2).
#[tokio::test]
async fn two_rows_share_a_file_and_it_stays_until_both_go() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = db
        .write_with_files::<_, _, _, _, crate::DbError>(
            |batch| Ok(batch.put_file("files", b"shared".to_vec())),
            |sql, file| {
                sql.execute(
                    "INSERT INTO files(id,file) VALUES('one',?1),('two',?1)",
                    [file],
                )?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    // One file, one upload and one copy for both rows.
    assert_eq!(local_count(&db, "coven_files"), 1);
    assert_eq!(local_count(&db, "_coven_file_uploads"), 1);
    assert_eq!(owned_paths(&store).len(), 1);
    complete_uploads(&db).await;
    db.write(|sql| {
        sql.execute("DELETE FROM files WHERE id='one'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(has_record(&db, file));
    let last = records(&db).pop().unwrap();
    assert_eq!(last.parts[0].rows.len(), 1);
    db.write(|sql| {
        sql.execute("UPDATE files SET file=NULL WHERE id='two'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(!has_record(&db, file));
    // The write that removed the last reference deleted the record.
    let last = records(&db).pop().unwrap();
    let changes: Vec<_> = last.parts[0]
        .rows
        .iter()
        .map(|row| {
            (
                row.row.table.as_str(),
                matches!(row.change.operation, coven_merge::Operation::Delete),
            )
        })
        .collect();
    assert_eq!(changes, [("coven_files", true), ("files", false)]);
    // Its stored bytes stay while the write naming them waits to publish.
    let candidate = || {
        db.inspect_writer(|sql| {
            sql.query_row("SELECT count(*) FROM _coven_file_candidates", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap()
        })
    };
    assert_eq!(candidate(), 0);
    crate::tests::publish(&db).await.unwrap();
    assert_eq!(candidate(), 1);
    db.close().await.unwrap();
}

/// A write-once reference, once it names a file, never changes (§17.2).
#[tokio::test]
async fn write_once_refuses_changing_a_reference_that_names_a_file() {
    let store = TestStore::new();
    let declarations = vec![SyncedTable::new("files", RowIdentity::SharedKey)
        .file_column(FileColumn::new("file").write_once())];
    let db = store
        .builder(declarations, vec![Migration::addition(1, "files", SCHEMA)])
        .namespaces(namespaces(Provenance::AppProvided))
        .open()
        .await
        .unwrap();
    stage(&db, b"original".to_vec(), true).await.unwrap();
    let paths = owned_paths(&store);
    let error = stage(&db, b"different".to_vec(), false).await.unwrap_err();
    assert!(matches!(error, DbError::FileWriteOnce { table, .. } if table=="files"));
    let error = db
        .write(|sql| {
            sql.execute("UPDATE files SET file=NULL", [])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::FileWriteOnce { .. }));
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

/// A move gives the moved row a new file in its new audience, over the same
/// bytes, and names it in the move's insert; the old file goes with its last
/// reference (§15.4, §17.2).
#[tokio::test]
async fn a_moved_row_gets_a_new_file_in_its_new_audience() {
    let store = TestStore::new();
    let circle = CircleId(uuid::Uuid::parse_str("00000000-0000-4000-8000-000000000002").unwrap());
    let declaration = SyncedTable::new("photos", RowIdentity::IndependentUuid)
        .audience_column("audience")
        .file_column(FileColumn::new("file"));
    let schema = "CREATE TABLE photos(id TEXT NOT NULL PRIMARY KEY,file TEXT,audience TEXT NOT NULL); CREATE INDEX photos_file ON photos(file)";
    let db = store
        .builder(
            vec![declaration],
            vec![Migration::addition(1, "photos", schema)],
        )
        .namespaces(namespaces(Provenance::AppProvided))
        .open()
        .await
        .unwrap();
    store.join_circle(&db, circle);
    db.write_with_files::<_, _, _, _, crate::DbError>(
        |batch| Ok(batch.put_file("files", b"photo".to_vec())),
        |sql, file| {
            sql.execute(
                "INSERT INTO photos(id,file,audience) VALUES('00000000-0000-4000-8000-000000000011',?1,'store')",
                [file],
            )?;
            Ok(())
        },
    )
    .await
    .unwrap();
    for stored in [false, true] {
        if stored {
            complete_uploads(&db).await;
            db.write(move |sql| {
                sql.execute("UPDATE photos SET audience='store'", [])?;
                Ok(())
            })
            .await
            .unwrap();
        }
        let before: FileId = db
            .read(|sql| Ok(sql.query_row("SELECT file FROM photos", [], |r| r.get(0))?))
            .await
            .unwrap();
        db.write(move |sql| {
            sql.execute("UPDATE photos SET audience=?1", [circle.to_string()])?;
            Ok(())
        })
        .await
        .unwrap();
        let after: FileId = db
            .read(|sql| Ok(sql.query_row("SELECT file FROM photos", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_ne!(after, before);
        assert!(!has_record(&db, before));
        let moved = db.file_ref(after).await.unwrap();
        assert_eq!(moved.audience(), coven_merge::Audience::Circle(circle));
        assert_eq!(moved.size(), 5);
        if stored {
            assert!(matches!(moved.place(), FilePlace::Cloud { .. }));
        } else {
            // Still uploading here: the upload switches the new file too.
            assert_eq!(
                moved.place(),
                FilePlace::DeviceOnly {
                    device: store.device()
                }
            );
        }
        // No second upload, and no second copy.
        assert!(local_count(&db, "_coven_file_uploads") <= 1);
        assert_eq!(owned_paths(&store).len(), usize::from(!stored));
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_downloaded_deletion_keeps_the_queued_copy_until_upload() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = open(&a_store, Provenance::AppProvided).await;
    let b = open(&b_store, Provenance::AppProvided).await;
    stage(&a, b"original".to_vec(), true).await.unwrap();
    a.receive(records(&a)[0].clone().into()).await.unwrap();
    b.receive(records(&a).remove(0).into()).await.unwrap();
    b.write(|sql| {
        sql.execute("DELETE FROM files", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let deletion = records(&b).remove(0);
    let paths = owned_paths(&a_store);
    a.receive(deletion.into()).await.unwrap();
    // The row and its file are gone, but the upload still owns the copy
    // until its attempt ends (§17.8).
    assert_eq!(local_count(&a, "files"), 0);
    assert_eq!(local_count(&a, "coven_files"), 0);
    assert_eq!(local_count(&a, "_coven_device_files"), 1);
    assert_eq!(owned_paths(&a_store), paths);
    complete_uploads(&a).await;
    assert_eq!(local_count(&a, "_coven_device_files"), 0);
    assert!(owned_paths(&a_store).is_empty());
    a.close().await.unwrap();
    b.close().await.unwrap();
}

/// A row removal rules took out keeps referencing its file: the file stays
/// until the row comes back or goes.
#[tokio::test]
async fn constraint_removal_keeps_the_file_until_the_row_is_restored() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let schema = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT,title TEXT UNIQUE); CREATE INDEX files_file ON files(file)";
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
    let mut databases = Vec::new();
    for store in [&a_store, &b_store] {
        databases.push(
            store
                .builder(tables(), vec![Migration::addition(1, "files", schema)])
                .namespaces(namespaces(Provenance::AppProvided))
                .clock(clock.clone())
                .open()
                .await
                .unwrap(),
        );
    }
    let a = databases.remove(0);
    let b = databases.remove(0);
    let file = stage(&a, b"original".to_vec(), true).await.unwrap();
    a.write(|sql| {
        sql.execute("UPDATE files SET title='same'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    clock.set(UNIX_EPOCH + Duration::from_millis(500));
    b.write(|sql| {
        sql.execute("INSERT INTO files(id,title) VALUES('other','same')", [])?;
        Ok(())
    })
    .await
    .unwrap();
    a.receive(records(&b).remove(0).into()).await.unwrap();
    let present = a
        .read(|sql| Ok(sql.query_row("SELECT id FROM files", [], |r| r.get::<_, String>(0))?))
        .await
        .unwrap();
    assert_eq!(present, "other");
    assert!(has_record(&a, file));
    assert_eq!(owned_paths(&a_store).len(), 1);
    b.write(|sql| {
        sql.execute("DELETE FROM files", [])?;
        Ok(())
    })
    .await
    .unwrap();
    a.receive(records(&b).remove(1).into()).await.unwrap();
    let present = a
        .read(|sql| Ok(sql.query_row("SELECT id FROM files", [], |r| r.get::<_, String>(0))?))
        .await
        .unwrap();
    assert_eq!(present, "7");
    assert!(has_record(&a, file));
    a.close().await.unwrap();
    b.close().await.unwrap();
}

/// A staged file's size is its bytes'; app rows hold no size of their own.
#[tokio::test]
async fn staged_bytes_supply_the_files_size() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    for (insert, bytes) in [
        (true, b"original".to_vec()),
        (false, b"replacement".to_vec()),
    ] {
        let size = bytes.len() as u64;
        let file = stage(&db, bytes, insert).await.unwrap();
        assert_eq!(db.file_ref(file).await.unwrap().size(), size);
    }
    db.close().await.unwrap();
}

/// A shared trigger that removes the row naming a staged file leaves the
/// file with no reference, which fails the write and keeps no bytes.
#[tokio::test]
async fn a_trigger_cannot_leave_a_new_owned_file_without_its_row() {
    let store = TestStore::new();
    let mut declarations = tables();
    declarations[0] = declarations[0].clone().shared_trigger("cancel_file");
    let db = store
        .builder(declarations, vec![Migration::addition_with(1, "files", |sql| {
            sql.execute_batch(&format!("{SCHEMA}; CREATE TRIGGER cancel_file AFTER INSERT ON files WHEN NOT coven_applying() BEGIN DELETE FROM files; END"))?;
            Ok(())
        })])
        .namespaces(namespaces(Provenance::AppProvided))
        .open()
        .await
        .unwrap();
    assert!(matches!(
        stage(&db, b"original".to_vec(), true).await,
        Err(DbError::FileUnreferenced { namespace, .. }) if namespace == "files"
    ));
    assert_eq!(local_count(&db, "files"), 0);
    assert_eq!(local_count(&db, "coven_files"), 0);
    assert_eq!(local_count(&db, "_coven_device_files"), 0);
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    assert!(owned_paths(&store).is_empty());
    db.close().await.unwrap();
}
