use crate::tests::{database_error, TestStore};
use crate::write::tests::sql;
use crate::{DbError, Migration, RowIdentity, SyncedTable};

fn notes() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
}

const NOTES: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)";

const VIEWS: [&str; 22] = [
    "coven_upload_pause",
    "coven_cache_bytes",
    "coven_library_bytes",
    "coven_archive_bytes",
    "coven_uploads",
    "coven_upload_queue",
    "coven_sending",
    "coven_downloading",
    "coven_device_only_downloading",
    "coven_device_only_placed",
    "coven_pins",
    "coven_pin_requests",
    "coven_moves",
    "coven_pending",
    "coven_invites",
    "coven_operations",
    "coven_files",
    "coven_file_reads",
    "coven_members",
    "coven_devices",
    "coven_circles",
    "coven_circle_members",
];

/// The views are the app's only window on coven's tables: it reads them,
/// in a read, a write and a migration, but still not the tables they read.
#[tokio::test]
async fn the_app_reads_coven_views_but_not_covens_tables() {
    let store = TestStore::new();
    let db = store
        .builder(
            notes(),
            vec![
                Migration::addition(1, "notes", NOTES),
                // A migration reads a view too.
                Migration::addition(
                    2,
                    "counted",
                    "CREATE TABLE counted(views INTEGER NOT NULL PRIMARY KEY); INSERT INTO counted SELECT count(*) FROM coven_members",
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    for view in VIEWS {
        // The pause view always holds its one row, and the library's table
        // holds a row for each part of the store's own files: the database,
        // the input database, coven's copies and the captured bodies.
        let expected = match view {
            "coven_upload_pause" => 1,
            "coven_library_bytes" => 4,
            _ => 0,
        };
        let statement = format!("SELECT * FROM {view}");
        let rows = db
            .read(move |sql| Ok::<_, DbError>(sql.query(&statement, [], |_| Ok(()))?))
            .await
            .unwrap();
        assert_eq!(rows.len(), expected, "{view}");
        // A count reads no column of the tables behind the view.
        let statement = format!("SELECT count(*) FROM {view}");
        let count: i64 = db
            .read(move |sql| Ok::<_, DbError>(sql.query_row(&statement, [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(count, expected as i64, "{view}");
    }
    db.write(|sql| {
        sql.execute("INSERT INTO notes SELECT 'pending', count(*) FROM coven_pending", [])?;
        Ok(())
    })
    .await
    .unwrap();
    // A view in an app subquery, as an order or a filter reads it.
    let first: Vec<String> = db
        .read(|sql| {
            Ok::<_, DbError>(sql.query(
                "SELECT id FROM notes ORDER BY EXISTS(SELECT 1 FROM coven_uploads AS u
                 WHERE u.file=notes.title) DESC, id",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(first, ["pending"]);
    for statement in [
        "SELECT * FROM _coven_file_uploads",
        "SELECT * FROM _coven_members",
        "SELECT * FROM _coven_copies",
        // A table counted without a view in the statement.
        "SELECT count(*) FROM _coven_members",
        "SELECT * FROM coven_uploads JOIN _coven_pending",
        // The tables of what coven keeps in memory.
        "SELECT * FROM _coven_copy_downloads",
        "SELECT * FROM _coven_reserved_copies",
    ] {
        let error =
            db.read(move |sql| Ok(sql.query(statement, [], |_| Ok(()))?)).await.unwrap_err();
        assert!(matches!(error, DbError::InternalTable { .. }), "{statement}: {error:?}");
    }
    db.close().await.unwrap();
}

/// Nothing the app runs writes a view, or names an object of its own after
/// one.
#[tokio::test]
async fn the_app_writes_no_view_and_makes_no_object_in_its_name() {
    let store = TestStore::new();
    let db =
        store.builder(notes(), vec![Migration::addition(1, "notes", NOTES)]).open().await.unwrap();
    for (statement, view) in [
        ("DELETE FROM coven_uploads", "coven_uploads"),
        ("DELETE FROM coven_pins", "coven_pins"),
        ("DELETE FROM coven_files", "coven_files"),
        ("UPDATE coven_files SET size=0", "coven_files"),
        (
            "INSERT INTO coven_members(member,role,removed,is_self) VALUES('m','admin',0,1)",
            "coven_members",
        ),
    ] {
        // SQLite refuses to change a view, or the authorizer refuses to
        // write a name in coven's namespace first.
        match sql(&db, statement).await {
            Err(DbError::Sqlite(sqlive::rusqlite::Error::SqliteFailure(_, Some(message)))) => {
                assert_eq!(message, format!("cannot modify {view} because it is a view"))
            }
            Err(DbError::InternalTable { table }) => assert_eq!(table, view),
            result => panic!("{statement}: expected a refusal, received {result:?}"),
        }
    }
    db.close().await.unwrap();
    for statement in [
        "CREATE TRIGGER coven_pins_changed AFTER INSERT ON notes BEGIN SELECT 1; END",
        "CREATE VIEW coven_files AS SELECT * FROM notes",
        "CREATE TRIGGER notes_uploads INSTEAD OF DELETE ON coven_uploads BEGIN SELECT 1; END",
    ] {
        let store = TestStore::new();
        let error = store
            .builder(
                notes(),
                vec![
                    Migration::addition(1, "notes", NOTES),
                    Migration::addition(2, "reserved", statement),
                ],
            )
            .open()
            .await
            .err()
            .expect("an object in coven's name is refused");
        assert!(matches!(database_error(error), DbError::InternalTable { .. }), "{statement}");
    }
}

/// `coven_key` gives a row's key as coven's views hold it, for one column or
/// several, and an app index can hold it.
#[tokio::test]
async fn coven_key_encodes_a_rows_key_and_an_index_can_hold_it() {
    let store = TestStore::new();
    let db = store
        .builder(
            notes(),
            vec![Migration::addition(
                1,
                "notes",
                "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL); CREATE INDEX notes_key ON notes(coven_key(id))",
            )],
        )
        .open()
        .await
        .unwrap();
    let (one, two): (Vec<u8>, Vec<u8>) = db
        .read(|sql| {
            Ok::<_, DbError>(sql.query_row("SELECT coven_key('a'), coven_key(7,'a')", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?)
        })
        .await
        .unwrap();
    use coven_format::value::Value;
    assert_eq!(one, coven_format::key::encode_key(&[Value::Text("a".into())]).unwrap());
    // format.md D3's example key.
    assert_eq!(two, [0x13, 0x04, 0x34, 0xE0, 0, 0, 0, 0, 0, 0, 0, 0x20, 0x61, 0, 0]);
    sql(&db, "INSERT INTO notes VALUES('a','apples')").await.unwrap();
    db.inspect_readers(|reader, _| {
        let plan: Vec<String> = reader
            .query(
                "EXPLAIN QUERY PLAN SELECT title FROM notes WHERE coven_key(id)=coven_key('a')",
                [],
                |r| r.get(3),
            )
            .unwrap();
        assert!(plan.join(";").contains("INDEX notes_key"), "{plan:?}");
    });
    db.close().await.unwrap();
}

/// `coven_files` holds each file's record, so one query names a row beside
/// its file's size and place: on this device only while it uploads, then
/// stored (§17.2, E4).
#[tokio::test]
async fn coven_files_holds_each_record_beside_the_apps_rows() {
    use crate::file_write::tests::{complete_uploads, open, stage};
    use crate::Provenance;
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let listed = || async {
        db.read(|sql| {
            Ok::<_, DbError>(sql.query(
                "SELECT f.id,c.size,coven_file_device(c.coordinates) IS NOT NULL,c.hash IS NOT NULL
                 FROM files AS f JOIN coven_files AS c ON c.id=f.file",
                [],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, bool>(2)?,
                        r.get::<_, bool>(3)?,
                    ))
                },
            )?)
        })
        .await
        .unwrap()
    };
    stage(&db, b"take".to_vec(), true).await.unwrap();
    assert_eq!(listed().await, [("7".to_owned(), 4, true, false)]);
    complete_uploads(&db).await;
    assert_eq!(listed().await, [("7".to_owned(), 4, false, true)]);
    db.close().await.unwrap();
}

/// A key column that isn't one of coven's keys is damage, not a key.
#[tokio::test]
async fn a_malformed_key_reads_as_damage() {
    let store = TestStore::new();
    let db =
        store.builder(notes(), vec![Migration::addition(1, "notes", NOTES)]).open().await.unwrap();
    let error = db
        .read(|sql| Ok(sql.query_row("SELECT x'ff'", [], |r| r.get::<_, crate::RowKey>(0))?))
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::DamagedDatabase), "{error:?}");
    db.close().await.unwrap();
}

/// `coven_uploads` names each queued file by its id, as the rows that
/// reference it do, so the app can find its own files in the queue (E4).
#[tokio::test]
async fn coven_uploads_names_each_queued_file() {
    use crate::file_write::tests::{open, stage};
    use crate::Provenance;
    use coven_foundation::id_source::FileId;
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = stage(&db, b"take".to_vec(), true).await.unwrap();
    let listed = db
        .read(|sql| {
            Ok::<_, DbError>(sql.query(
                "SELECT u.file FROM coven_uploads AS u JOIN files AS f ON f.file=u.file WHERE f.id='7'",
                [],
                |r| r.get::<_, FileId>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(listed, [file]);
    db.close().await.unwrap();
}

/// `coven_uploads` gives the bytes the provider confirmed in each file's
/// recorded session, NULL before it has one, so a progress bar, or one
/// summed over an album, is SQL (E4).
#[tokio::test]
async fn coven_uploads_gives_each_files_confirmed_bytes() {
    use crate::file_write::tests::{open, stage};
    use crate::{FileDatabase, Provenance, UploadProgress};
    use coven_crypto::SecretBytes;
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    stage(&db, b"take".to_vec(), true).await.unwrap();
    let bytes = || async {
        db.read(|sql| {
            Ok::<_, DbError>(sql.query(
                "SELECT bytes_confirmed,bytes_total FROM coven_uploads",
                [],
                |r| Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, Option<i64>>(1)?)),
            )?)
        })
        .await
        .unwrap()
    };
    assert_eq!(bytes().await, [(None, None)]);
    let owner = FileDatabase::new(db.clone());
    let id = db.upload_queue().await.unwrap().uploads[0].id;
    let progress = UploadProgress { bytes_confirmed: 65_536, bytes_total: 200_000 };
    owner.record_session(id, SecretBytes::new(b"session".to_vec()), progress).await.unwrap();
    assert_eq!(bytes().await, [(Some(65_536), Some(200_000))]);
    assert_eq!(db.upload_queue().await.unwrap().uploads[0].progress, Some(progress));
    // More bytes confirmed than the file holds is refused, and nothing changes.
    assert!(owner
        .record_session(
            id,
            SecretBytes::new(b"later".to_vec()),
            UploadProgress { bytes_confirmed: 200_001, bytes_total: 200_000 },
        )
        .await
        .is_err());
    assert_eq!(bytes().await, [(Some(65_536), Some(200_000))]);
    db.close().await.unwrap();
}

/// `coven_upload_pause` says whether uploads are paused in one row, with an
/// empty queue too, and a live query that reads it reruns as the app pauses
/// or resumes (E4, E7). The pause is a step of the app's write: written
/// beside an app row, it commits with it, and the live query runs once for
/// the write.
#[tokio::test]
async fn coven_upload_pause_says_whether_uploads_are_paused() {
    use crate::file_write::tests::open;
    use crate::Provenance;
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let mut paused: crate::LiveQuery<_, DbError> = db
        .subscribe(|sql| {
            Ok(sql
                .query_row("SELECT paused FROM coven_upload_pause", [], |r| r.get::<_, bool>(0))?)
        })
        .process(Ok);
    assert!(!paused.next_inline().await.unwrap().unwrap());
    db.write(|sql| {
        sql.execute("INSERT INTO files(id) VALUES('paused')", [])?;
        sql.set_uploads_paused(true)?;
        assert!(
            sql.query_row("SELECT paused FROM coven_upload_pause", [], |r| r.get::<_, bool>(0))?
        );
        Ok(())
    })
    .await
    .unwrap();
    assert!(paused.next_inline().await.unwrap().unwrap());
    assert_eq!(paused.runs().writes(), 1);
    db.write(|sql| sql.set_uploads_paused(false)).await.unwrap();
    assert!(!paused.next_inline().await.unwrap().unwrap());
    db.close().await.unwrap();
}

/// Stage `bytes` as a new file and insert row `id` pointing at it.
async fn stage_row(db: &crate::StoreDatabase, id: &'static str, bytes: &[u8]) {
    let bytes = bytes.to_vec();
    db.staged_write::<_, _, _, _, DbError>(
        move |batch| Ok(batch.put_file("files", bytes, crate::Place::Cloud)),
        move |sql, file| {
            sql.execute(
                "INSERT INTO files(id,file) VALUES(?1,?2)",
                sqlive::rusqlite::params![id, file],
            )?;
            Ok(())
        },
    )
    .await
    .unwrap();
}

/// A live query reads where one release's queued files are, through
/// `coven_upload_queue` and `coven_file_reads`: another release's upload
/// changes no row it reads, so it doesn't run again, and a change to its own
/// release's file does (§4).
#[tokio::test]
async fn a_read_of_one_releases_uploads_ignores_another_releases_upload() {
    use crate::file_write::tests::open;
    use crate::Provenance;
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    stage_row(&db, "a", b"first").await;
    let mut release: crate::LiveQuery<_, DbError> = db.subscribe(|sql| {
        Ok(sql.query(
            "SELECT r.source FROM files AS f
             JOIN coven_upload_queue AS q ON q.file=f.file
             JOIN coven_file_reads AS r ON r.file=f.file
             WHERE f.id='a'",
            [],
            |r| r.get::<_, String>(0),
        )?)
    });
    assert_eq!(release.next_inline().await.unwrap().unwrap(), ["here"]);
    stage_row(&db, "b", b"second").await;
    assert!(!release.is_marked_for_rerun());
    sql(&db, "UPDATE files SET file=NULL WHERE id='a'").await.unwrap();
    assert!(release.is_marked_for_rerun());
    assert!(release.next_inline().await.unwrap().unwrap().is_empty());
    db.close().await.unwrap();
}

/// A view that joins two of coven's own tables has a write to one of them ask
/// about the other, which app SQL reads only through the view. The write
/// commits, and reruns no live query of the view that it changes no row of
/// (§4).
#[tokio::test]
async fn a_write_commits_beside_a_read_of_a_view_joining_covens_tables() {
    use crate::file_write::tests::open;
    use crate::Provenance;
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let mut members: crate::LiveQuery<_, DbError> = db.subscribe(|read| {
        Ok(read.query("SELECT member FROM coven_circle_members WHERE circle='gifts'", [], |r| {
            r.get::<_, String>(0)
        })?)
    });
    assert!(members.next_inline().await.unwrap().unwrap().is_empty());
    // The circle doesn't have the new member: a lookup of
    // `_coven_circle_members`.
    TestStore::register(&db, coven_foundation::id_source::DeviceId(2), 2);
    assert!(!members.is_marked_for_rerun());
    db.close().await.unwrap();
}

/// A live query that reads which of one release's files this process is
/// sending and downloading runs again as a transfer of its own file starts or
/// ends, and not as one of another release's does. One that reads no view
/// that names the network doesn't run again as the network changes (§4).
#[tokio::test]
async fn a_read_of_one_releases_transfers_ignores_another_releases_transfers() {
    use crate::file_write::tests::open;
    use crate::Provenance;
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let files = crate::FileDatabase::new(db.clone());
    stage_row(&db, "a", b"first").await;
    let own = files.uploads().await.unwrap().remove(0).location;
    stage_row(&db, "b", b"second").await;
    let other = files
        .uploads()
        .await
        .unwrap()
        .into_iter()
        .map(|upload| upload.location)
        .find(|location| location.object() != own.object())
        .unwrap();
    let file: coven_foundation::id_source::FileId = db
        .read(|sql| {
            Ok::<_, DbError>(
                sql.query_row("SELECT file FROM files WHERE id='a'", [], |r| r.get(0))?,
            )
        })
        .await
        .unwrap();
    let mut release: crate::LiveQuery<_, DbError> = db.subscribe(|sql| {
        Ok(sql.query_row(
            "SELECT coalesce((SELECT u.sending FROM coven_uploads AS u WHERE u.file=f.file),0),
                    EXISTS(SELECT 1 FROM coven_sending AS s WHERE s.file=f.file)
             FROM files AS f WHERE f.id='a'",
            [],
            |r| Ok((r.get::<_, bool>(0)?, r.get::<_, bool>(1)?)),
        )?)
    });
    assert_eq!(crate::tests::next(&mut release).await, (false, false));
    let progress = crate::TransferProgress { bytes_transferred: 0, bytes_total: 1 };
    let start = |transfers: crate::InFlight, location: &crate::FileLocation, file| {
        transfers.start(location.uploader(), location.object(), vec![file], progress)
    };
    drop(start(db.uploads_in_flight(), &other, file));
    assert!(!release.is_marked_for_rerun(), "sending");
    let mut listed: crate::LiveQuery<_, DbError> = db.subscribe(|sql| {
        Ok(sql.query_row(
            "SELECT count(*) FROM files AS f JOIN coven_sending AS s ON s.file=f.file
             WHERE f.id='a'",
            [],
            |r| r.get::<_, i64>(0),
        )?)
    });
    assert_eq!(crate::tests::next(&mut listed).await, 0);
    db.record_network(coven_foundation::network_watch::NetworkState {
        usable: Some(true),
        metered: true,
        joined: 1,
    });
    assert!(!listed.is_marked_for_rerun());
    // `coven_uploads` says what the network holds, so a read of it follows
    // the network too.
    assert!(release.is_marked_for_rerun());
    let sending = start(db.uploads_in_flight(), &own, file);
    assert_eq!(crate::tests::next(&mut release).await, (true, true));
    drop(sending);
    assert_eq!(crate::tests::next(&mut release).await, (false, false));
    // A pin downloads stored files.
    crate::file_write::tests::complete_uploads(&db).await;
    let mut pinning: crate::LiveQuery<_, DbError> = db.subscribe(|sql| {
        Ok(sql.query(
            "SELECT d.file FROM files AS f JOIN coven_downloading AS d ON d.file=f.file
             WHERE f.id='a'",
            [],
            |r| r.get::<_, coven_foundation::id_source::FileId>(0),
        )?)
    });
    assert!(crate::tests::next(&mut pinning).await.is_empty());
    drop(start(db.downloads_in_flight(), &other, file));
    assert!(!pinning.is_marked_for_rerun(), "downloading");
    let downloading = start(db.downloads_in_flight(), &own, file);
    assert_eq!(crate::tests::next(&mut pinning).await, [file]);
    drop(downloading);
    assert!(crate::tests::next(&mut pinning).await.is_empty());
    db.close().await.unwrap();
}

/// A build analyzes app SQL against `readable_objects_sql`, naming no view
/// to read whole, and a store runs each plan it ships: a compiled read of
/// every table and view app SQL may read finds each as the build found it
/// (§4).
#[tokio::test]
async fn a_store_finds_what_a_build_found_in_every_readable_table_and_view() {
    use crate::file_write::tests::open;
    use crate::Provenance;
    let functions: Vec<sqlive::StandInFunction> = coven_schema::SQL_FUNCTIONS
        .iter()
        .map(|function| sqlive::StandInFunction {
            name: function.name.into(),
            arguments: function.arguments,
            deterministic: function.deterministic,
        })
        .collect();
    let probe = sqlive::SchemaProbe::open(&[coven_schema::readable_objects_sql()], &functions, &[])
        .unwrap();
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    for name in coven_schema::READABLE {
        let sql: &'static str = Box::leak(format!("SELECT count(*) FROM {name}").into_boxed_str());
        let plan = probe.compiled_plan(sql).unwrap().expect(name).encode();
        let statement = sqlive::CompiledStatement { sql, plan: Box::leak(plan.into_boxed_str()) };
        let mut query: crate::LiveQuery<_, DbError> = db
            .subscribe(move |read| Ok(read.query_compiled(statement, [], |r| r.get::<_, i64>(0))?));
        let answer = query.next_inline().await.unwrap();
        // A test of the database crate opens a store without sync's
        // computations, which these two views call.
        if !matches!(name, "coven_sync_state" | "coven_upload_phases") {
            answer.unwrap_or_else(|error| panic!("{name}: {error:?}"));
        } else {
            assert!(
                !matches!(answer, Err(DbError::CompiledSchemaDiffers(_))),
                "{name}: {answer:?}"
            );
        }
    }
    db.close().await.unwrap();
}
