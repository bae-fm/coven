use crate::tests::{database_error, TestStore};
use crate::write::tests::sql;
use crate::{CovenError, DbError, Migration, RowIdentity, SyncedTable};

fn notes() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
}

const NOTES: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)";

const VIEWS: [&str; 9] = [
    "coven_upload_pause",
    "coven_uploads",
    "coven_sending",
    "coven_downloading",
    "coven_pins",
    "coven_pending",
    "coven_abandoned",
    "coven_members",
    "coven_devices",
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
                    "CREATE TABLE counted(views INTEGER NOT NULL); INSERT INTO counted SELECT count(*) FROM coven_members",
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    for view in VIEWS {
        // The pause view always holds its one row.
        let expected = usize::from(view == "coven_upload_pause");
        let statement = format!("SELECT * FROM {view}");
        let rows = db
            .read(move |sql| Ok(sql.query(&statement, [], |_| Ok(()))?))
            .await
            .unwrap();
        assert_eq!(rows.len(), expected, "{view}");
        // A count reads no column of the tables behind the view.
        let statement = format!("SELECT count(*) FROM {view}");
        let count: i64 = db
            .read(move |sql| Ok(sql.query_row(&statement, [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(count, expected as i64, "{view}");
    }
    db.write(|sql| {
        sql.execute(
            "INSERT INTO notes SELECT 'pending', count(*) FROM coven_pending",
            [],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    // A view in an app subquery, as an order or a filter reads it.
    let first: Vec<String> = db
        .read(|sql| {
            Ok(sql.query(
                "SELECT id FROM notes ORDER BY EXISTS(SELECT 1 FROM coven_uploads AS u
                 WHERE u.table_name='notes' AND u.key=coven_key(notes.id)) DESC, id",
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
        // A table counted without a view in the statement.
        "SELECT count(*) FROM _coven_members",
        "SELECT * FROM coven_uploads JOIN _coven_pending",
    ] {
        let error = db
            .read(move |sql| Ok(sql.query(statement, [], |_| Ok(()))?))
            .await
            .unwrap_err();
        assert!(
            matches!(error, CovenError::Database(DbError::InternalTable { .. })),
            "{statement}: {error:?}"
        );
    }
    db.close().await.unwrap();
}

/// Nothing the app runs writes a view, or names an object of its own after
/// one.
#[tokio::test]
async fn the_app_writes_no_view_and_makes_no_object_in_its_name() {
    let store = TestStore::new();
    let db = store
        .builder(notes(), vec![Migration::addition(1, "notes", NOTES)])
        .open()
        .await
        .unwrap();
    for (statement, view) in [
        ("DELETE FROM coven_uploads", "coven_uploads"),
        ("UPDATE coven_pins SET kept=0", "coven_pins"),
        ("DELETE FROM coven_abandoned", "coven_abandoned"),
        (
            "INSERT INTO coven_members(member,role,removed,is_self) VALUES('m','admin',0,1)",
            "coven_members",
        ),
    ] {
        // SQLite refuses to change a view, or the authorizer refuses to
        // write a name in coven's namespace first.
        match sql(&db, statement).await {
            Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(_, Some(message)))) => {
                assert_eq!(
                    message,
                    format!("cannot modify {view} because it is a view")
                )
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
        assert!(
            matches!(database_error(error), DbError::InternalTable { .. }),
            "{statement}"
        );
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
            Ok(
                sql.query_row("SELECT coven_key('a'), coven_key(7,'a')", [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })?,
            )
        })
        .await
        .unwrap();
    use coven_format::value::Value;
    assert_eq!(
        one,
        coven_format::key::encode_key(&[Value::Text("a".into())]).unwrap()
    );
    // format.md D3's example key.
    assert_eq!(
        two,
        [0x13, 0x04, 0x34, 0xE0, 0, 0, 0, 0, 0, 0, 0, 0x20, 0x61, 0, 0]
    );
    sql(&db, "INSERT INTO notes VALUES('a','apples')")
        .await
        .unwrap();
    db.inspect_readers(|reader| {
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

/// `coven_abandoned` lists each attachment this device abandoned, as
/// `abandoned_attachments()` does, so one live query names the rows beside
/// them. The write that abandons an attachment is the commit that changes
/// the view, and dismissing it takes it off (§17.2, E4).
#[tokio::test]
async fn coven_abandoned_lists_each_abandonment_beside_the_apps_rows() {
    use crate::file_write::tests::{attach, tables, SCHEMA};
    use crate::{AbandonReason, Provenance};
    use coven_foundation::id_source::FileId;
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    let mut listed = db.subscribe(|sql| {
        Ok(sql.query(
            "SELECT f.id,a.file,a.reason FROM coven_abandoned AS a
             LEFT JOIN files AS f ON a.table_name='files' AND a.key=coven_key(f.id)
             ORDER BY a.position",
            [],
            |r| {
                Ok((
                    r.get::<_, Option<String>>(0)?,
                    r.get::<_, FileId>(1)?,
                    r.get::<_, AbandonReason>(2)?,
                ))
            },
        )?)
    });
    assert!(listed.next().await.unwrap().unwrap().is_empty());
    attach(&db, b"first".to_vec(), true).await.unwrap();
    let first = db.file_ref("files", "7").await.unwrap().id();
    // Attaching another file to row 7 abandons the first, and row 7 is
    // still there to name it.
    attach(&db, b"second".to_vec(), false).await.unwrap();
    let second = db.file_ref("files", "7").await.unwrap().id();
    assert_eq!(
        listed.next().await.unwrap().unwrap(),
        [(Some("7".to_owned()), first, AbandonReason::Replaced)]
    );
    // Deleting the row abandons the second in the same commit: the first
    // result after it shows both, and no row for either.
    db.write(|sql| {
        sql.execute("DELETE FROM files", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        listed.next().await.unwrap().unwrap(),
        [
            (None, first, AbandonReason::Replaced),
            (None, second, AbandonReason::RowGone)
        ]
    );
    // The view and the one-shot list agree, oldest first.
    let records = db.abandoned_attachments().await.unwrap();
    assert_eq!(
        records
            .iter()
            .map(|a| (a.file, a.reason))
            .collect::<Vec<_>>(),
        [
            (first, AbandonReason::Replaced),
            (second, AbandonReason::RowGone)
        ]
    );
    // A gone row's key reads back from the view as the row's key, and the
    // file alone names the record to dismiss.
    let keys = db
        .read(|sql| {
            Ok(sql.query(
                "SELECT key FROM coven_abandoned ORDER BY position",
                [],
                |r| r.get::<_, crate::RowKey>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(keys, [crate::RowKey::from("7"), crate::RowKey::from("7")]);
    assert_eq!(keys[1], records[1].key);
    db.dismiss_abandoned_attachments(vec![first]).await.unwrap();
    assert_eq!(
        listed.next().await.unwrap().unwrap(),
        [(None, second, AbandonReason::RowGone)]
    );
    db.close().await.unwrap();
}

/// A key column that isn't one of coven's keys is damage, not a key.
#[tokio::test]
async fn a_malformed_key_reads_as_damage() {
    let store = TestStore::new();
    let db = store
        .builder(notes(), vec![Migration::addition(1, "notes", NOTES)])
        .open()
        .await
        .unwrap();
    let error = db
        .read(|sql| Ok(sql.query_row("SELECT x'ff'", [], |r| r.get::<_, crate::RowKey>(0))?))
        .await
        .unwrap_err();
    assert!(
        matches!(error, CovenError::Database(DbError::DamagedDatabase)),
        "{error:?}"
    );
    db.close().await.unwrap();
}

/// `coven_uploads` names each queued file by its id, as `coven_abandoned`
/// does, so the app can find its own files in the queue (E4).
#[tokio::test]
async fn coven_uploads_names_each_queued_file() {
    use crate::file_write::tests::{attach, tables, SCHEMA};
    use crate::Provenance;
    use coven_foundation::id_source::FileId;
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    attach(&db, b"take".to_vec(), true).await.unwrap();
    let file = db.file_ref("files", "7").await.unwrap().id();
    let listed = db
        .read(|sql| {
            Ok(sql.query(
                "SELECT file FROM coven_uploads WHERE table_name='files' AND key=coven_key('7')",
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
    use crate::file_write::tests::{attach, tables, SCHEMA};
    use crate::{FileDatabase, Provenance, UploadProgress};
    use coven_crypto::SecretBytes;
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    attach(&db, b"take".to_vec(), true).await.unwrap();
    let bytes = || async {
        db.read(|sql| {
            Ok(sql.query(
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
    let progress = UploadProgress {
        bytes_confirmed: 65_536,
        bytes_total: 200_000,
    };
    owner
        .record_session(id, SecretBytes::new(b"session".to_vec()), progress)
        .await
        .unwrap();
    assert_eq!(bytes().await, [(Some(65_536), Some(200_000))]);
    assert_eq!(
        db.upload_queue().await.unwrap().uploads[0].progress,
        Some(progress)
    );
    // More bytes confirmed than the file holds is refused, and nothing changes.
    assert!(owner
        .record_session(
            id,
            SecretBytes::new(b"later".to_vec()),
            UploadProgress {
                bytes_confirmed: 200_001,
                bytes_total: 200_000,
            },
        )
        .await
        .is_err());
    assert_eq!(bytes().await, [(Some(65_536), Some(200_000))]);
    db.close().await.unwrap();
}

/// `coven_upload_pause` says whether uploads are paused in one row, with an
/// empty queue too, and a live query that reads it reruns as the app pauses
/// or resumes (E4, E7).
#[tokio::test]
async fn coven_upload_pause_says_whether_uploads_are_paused() {
    use crate::file_write::tests::{tables, SCHEMA};
    use crate::{FileDatabase, Provenance};
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    let mut paused = db.subscribe(|sql| {
        Ok(
            sql.query_row("SELECT paused FROM coven_upload_pause", [], |r| {
                r.get::<_, bool>(0)
            })?,
        )
    });
    assert!(!paused.next().await.unwrap().unwrap());
    let owner = FileDatabase::new(db.clone());
    owner.set_uploads_paused(true).await.unwrap();
    assert!(paused.next().await.unwrap().unwrap());
    owner.set_uploads_paused(false).await.unwrap();
    assert!(!paused.next().await.unwrap().unwrap());
    db.close().await.unwrap();
}
