use crate::file_write::tests::{attach, local_count, owned_paths, tables, SCHEMA};
use crate::snapshot_write::tests::{frames, id, load_one, stream, SnapshotFrames};
use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{Database, DbError, Provenance, RowIdentity, SyncedTable};
use coven_foundation::id_source::{CircleId, SequentialIds};
use coven_merge::Audience;
use std::io::Cursor;

async fn load(db: &Database, audience: Audience, frames: SnapshotFrames) {
    load_one(
        db,
        id(audience),
        frames.prefix.clone(),
        Cursor::new(frames.concat()),
    )
    .await
    .unwrap();
}

pub(crate) fn contents(
    db: &Database,
) -> std::collections::BTreeMap<String, Vec<Vec<crate::types::Value>>> {
    db.inspect_writer(|db| {
        let tables = db
            .query(
                "SELECT name FROM main.sqlite_schema WHERE type='table' ORDER BY name",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        tables
            .into_iter()
            .map(|table| {
                let values = db
                    .query(
                        &format!("SELECT * FROM {}", crate::sql::identifier(&table)),
                        [],
                        |r| (0..r.as_ref().column_count()).map(|i| r.get(i)).collect(),
                    )
                    .unwrap();
                (table, values)
            })
            .collect()
    })
}

#[tokio::test]
async fn store_at_40_and_circle_at_38_replay_only_the_circles_missing_parts() {
    const SCHEMA: &str="CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,value INTEGER NOT NULL)";
    let tables = || {
        vec![SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience")]
    };
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(tables(), SCHEMA).await.unwrap();
    let b = b_store.schema(tables(), SCHEMA).await.unwrap();
    let circle = Audience::Circle(CircleId(uuid::Uuid::from_u128(10)));
    sql(&a,"INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','store',1),('00000000-0000-4000-8000-000000000002','00000000-0000-0000-0000-00000000000a',1)").await.unwrap();
    for _ in 2..=38 {
        sql(&a, "UPDATE notes SET value=value+1").await.unwrap();
    }
    let gifts = frames(&a, circle.clone()).await;
    for _ in 39..=40 {
        sql(&a, "UPDATE notes SET value=value+1").await.unwrap();
    }
    let store = frames(&a, Audience::Store).await;
    let snapshots = || {
        vec![
            (
                id(Audience::Store),
                store.prefix.clone(),
                Cursor::new(store.concat()),
            ),
            (
                id(circle.clone()),
                gifts.prefix.clone(),
                Cursor::new(gifts.concat()),
            ),
        ]
    };
    let original = contents(&b);
    let writes = records(&a);
    for supplied in [vec![], vec![stream(&writes[39])], vec![stream(&writes[38])]] {
        assert!(matches!(
            b.load_snapshots(crate::SnapshotReload::new(snapshots(), supplied))
                .await,
            Err(crate::DbError::Snapshot(
                crate::SnapshotError::MissingWrites { .. }
            ))
        ));
        assert_eq!(contents(&b), original);
    }
    b.inspect_writer(|db|db.batch("CREATE TABLE store_changes(value INTEGER); CREATE TRIGGER store_change AFTER UPDATE ON notes WHEN new.audience='store' BEGIN INSERT INTO store_changes VALUES(new.value); END").unwrap());
    let mut query = b.subscribe(|db| {
        Ok(
            db.query("SELECT value FROM notes ORDER BY audience", [], |r| {
                r.get::<_, i64>(0)
            })?,
        )
    });
    assert!(query.next().await.unwrap().is_empty());
    // Supply the later write first: the loader chooses causal order.
    b.load_snapshots(crate::SnapshotReload::new(
        snapshots(),
        vec![stream(&writes[39]), stream(&writes[38])],
    ))
    .await
    .unwrap();
    assert_eq!(query.next().await.unwrap(), [40, 40]);
    assert!(!query.is_marked_for_rerun());
    assert_eq!(count(&b, "store_changes"), 0);
    assert_eq!(
        b.sync_state(vec![]).await.unwrap().positions.0[0].number,
        40
    );
    for audience in [Audience::Store, circle.clone()] {
        assert_eq!(
            frames(&b, audience.clone()).await,
            frames(&a, audience).await
        );
    }
    sql(&b, "UPDATE notes SET value=41").await.unwrap();
    let written = records(&b).pop().unwrap();
    assert_eq!(written.header.had_read.0, [writes[39].header.position]);
    a.apply_downloaded(written.into()).await.unwrap();
    for audience in [Audience::Store, circle] {
        assert_eq!(
            frames(&b, audience.clone()).await,
            frames(&a, audience).await
        );
    }
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn waiting_writes_keep_their_numbers_and_merge_as_late_writes_on_every_load() {
    use coven_foundation::clock::FixedClock;
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store
        .builder(notes(), vec![crate::Migration::sql(1, "notes", NOTES)])
        .clock(Arc::new(FixedClock::new(
            UNIX_EPOCH + Duration::from_secs(1),
        )))
        .open()
        .await
        .unwrap();
    let b = b_store
        .builder(notes(), vec![crate::Migration::sql(1, "notes", NOTES)])
        .clock(Arc::new(FixedClock::new(
            UNIX_EPOCH + Duration::from_secs(2),
        )))
        .open()
        .await
        .unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','base','')")
        .await
        .unwrap();
    b.apply_downloaded(records(&a).remove(0).into())
        .await
        .unwrap();
    a.inspect_writer(|db| {
        db.internal_execute("DELETE FROM coven_uploads", [])
            .unwrap()
    });
    sql(&a, "UPDATE notes SET title='offline'").await.unwrap();
    let waiting = records(&a);
    assert_eq!(waiting[0].header.position.number, 2);
    let encoder = coven_format::write_stream::WriteEncoder::new(&waiting[0]).unwrap();
    let length = coven_format::sealed_write::sealed_length(
        encoder.header_frame().len(),
        &encoder
            .header()
            .parts
            .iter()
            .map(|part| part.plaintext_length)
            .collect::<Vec<_>>(),
    )
    .unwrap() as usize;
    crate::upload::tests::attempt(&a, vec![17; length])
        .await
        .unwrap();
    let sealed = crate::upload::tests::sealed(&a).await;
    sql(&b, "UPDATE notes SET title='later'").await.unwrap();
    let snapshot = frames(&b, Audience::Store).await;
    for _ in 0..2 {
        load(&a, Audience::Store, snapshot.clone()).await;
        assert_eq!(records(&a), waiting);
        assert_eq!(crate::upload::tests::sealed(&a).await, sealed);
        assert_eq!(
            a.read(
                |db| Ok(db.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?)
            )
            .await
            .unwrap(),
            "later"
        );
        let losses = a.lost_values().await.unwrap();
        assert_eq!(losses.len(), 1);
        assert!(
            matches!(&losses[0].lost,crate::Lost::Cell(cell) if cell.value==crate::types::Value::Text("offline".into()))
        );
    }
    sql(&a, "UPDATE notes SET body='after reload'")
        .await
        .unwrap();
    assert_eq!(records(&a)[1].header.position.number, 3);
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn malformed_or_inconsistent_snapshots_roll_back_every_table() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('from snapshot','new','')")
        .await
        .unwrap();
    sql(
        &b,
        "INSERT INTO notes VALUES('waiting','keep',''); INSERT INTO local_rows VALUES('local')",
    )
    .await
    .unwrap();
    let original = contents(&b);
    let frames = frames(&a, Audience::Store).await;
    let bytes = frames.concat();
    let mut damaged = vec![
        bytes[..bytes.len() - 1].to_vec(),
        bytes[..frames.frames[0].len() + 1].to_vec(),
    ];
    let mut trailing = bytes.clone();
    trailing.push(0);
    damaged.push(trailing);
    let (header, mut records) = crate::snapshot_write::tests::decode(&frames);
    if let coven_format::snapshot::SnapshotRecord::Synced(row) = &mut records[0] {
        row.columns.get_mut("title").unwrap().value =
            coven_format::value::Value::Text("different from merge".into());
    } else {
        panic!("synced row first");
    }
    let (mut encoder, header) = coven_format::snapshot::SnapshotEncoder::start(header).unwrap();
    let mut inconsistent = header;
    for record in records {
        inconsistent.extend(encoder.record(record).unwrap());
    }
    inconsistent.extend(encoder.finish().unwrap());
    damaged.push(inconsistent);
    for bytes in damaged {
        assert!(load_one(
            &b,
            id(Audience::Store),
            frames.prefix.clone(),
            Cursor::new(bytes)
        )
        .await
        .is_err());
        assert_eq!(contents(&b), original);
    }
    load(&b, Audience::Store, frames).await;
    assert_eq!(count(&b, "notes"), 2);
    assert_eq!(count(&b, "local_rows"), 1);
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn loading_twice_preserves_values_losses_and_fingerprint() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','snapshot','')")
        .await
        .unwrap();
    let source = frames(&a, Audience::Store).await;
    for _ in 0..2 {
        load(&b, Audience::Store, source.clone()).await;
        assert_eq!(frames(&b, Audience::Store).await, source);
        assert_eq!(count(&b, "coven_uploads"), 0);
    }
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn loading_a_parent_snapshot_recomputes_other_audiences_without_rewriting_their_history() {
    for action in ["CASCADE", "SET NULL", "SET DEFAULT"] {
        let tables = || {
            vec![
                SyncedTable::new("parents", RowIdentity::SharedKey),
                SyncedTable::new("children", RowIdentity::IndependentUuid)
                    .audience_column("audience"),
            ]
        };
        let migrations = || {
            vec![crate::Migration::run(1, "references", move |db| {
                db.execute_batch(&format!("CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY COLLATE NOCASE); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,parent TEXT DEFAULT 'Inbox' REFERENCES parents(id) ON DELETE {action}); CREATE INDEX child_parent ON children(parent)"))?;
                Ok(())
            })]
        };
        let ids = SequentialIds::new();
        let a_store = TestStore::with_ids(&ids);
        let b_store = TestStore::with_ids(&ids);
        let c_store = TestStore::with_ids(&ids);
        let a = a_store
            .builder(tables(), migrations())
            .open()
            .await
            .unwrap();
        let b = b_store
            .builder(tables(), migrations())
            .open()
            .await
            .unwrap();
        let c = c_store
            .builder(tables(), migrations())
            .open()
            .await
            .unwrap();
        sql(&a, "INSERT INTO parents VALUES('parent'),('Inbox')")
            .await
            .unwrap();
        b.apply_downloaded(records(&a).remove(0).into())
            .await
            .unwrap();
        c.apply_downloaded(records(&a)[0].clone().into())
            .await
            .unwrap();
        sql(&c, "INSERT INTO parents VALUES('reset branch')")
            .await
            .unwrap();
        let before = frames(&c, Audience::Store).await;
        sql(&b,"INSERT INTO children VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-0000-0000-00000000000a','PaReNt')").await.unwrap();
        let history = |db: &Database| {
            db.inspect_writer(|db|db.query("SELECT generation,write_id FROM coven_rows WHERE table_name='children' ORDER BY generation",[],|r|Ok((r.get::<_,Vec<u8>>(0)?,r.get::<_,i64>(1)?))).unwrap())
        };
        let child_history = history(&b);
        sql(&a, "INSERT INTO parents VALUES('unseen predecessor')")
            .await
            .unwrap();
        sql(&a, "DELETE FROM parents WHERE id='parent'")
            .await
            .unwrap();
        let missing = records(&a).into_iter().skip(1).collect::<Vec<_>>();
        b.load_snapshots(crate::SnapshotReload::new(
            vec![frames(&a, Audience::Store).await.input()],
            missing.iter().map(stream).collect(),
        ))
        .await
        .unwrap();
        assert_eq!(history(&b), child_history);
        let visible = b
            .read(|db| {
                Ok(db.query("SELECT parent FROM children", [], |r| {
                    r.get::<_, Option<String>>(0)
                })?)
            })
            .await
            .unwrap();
        assert_eq!(
            visible,
            match action {
                "CASCADE" => vec![],
                "SET NULL" => vec![None],
                _ => vec![Some("Inbox".into())],
            }
        );
        b.apply_reset(
            crate::EntryId {
                device: coven_foundation::id_source::DeviceId(99),
                number: 1,
            },
            Audience::Store,
            crate::snapshot_write::tests::decode(&before).0.writes,
        )
        .await
        .unwrap();
        b.load_snapshots(crate::SnapshotReload::new(
            vec![(
                id(Audience::Store),
                before.prefix.clone(),
                Cursor::new(before.concat()),
            )],
            missing
                .iter()
                .chain(records(&c).iter())
                .map(stream)
                .collect(),
        ))
        .await
        .unwrap();
        assert_eq!(history(&b), child_history);
        assert_eq!(
            b.read(|db| Ok(
                db.query_row("SELECT parent FROM children", [], |r| r.get::<_, String>(0))?
            ))
            .await
            .unwrap(),
            "PaReNt"
        );
        for db in [a, b, c] {
            db.close().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readers_and_live_queries_observe_only_the_committed_reload() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
    use std::time::Duration;
    struct GatedRead {
        bytes: Cursor<Vec<u8>>,
        started: Option<tokio::sync::oneshot::Sender<()>>,
        wait: std::sync::mpsc::Receiver<()>,
    }
    impl std::io::Read for GatedRead {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if let Some(started) = self.started.take() {
                started.send(()).unwrap();
                self.wait.recv_timeout(Duration::from_secs(10)).unwrap();
            }
            let size = buffer.len().min(17);
            std::io::Read::read(&mut self.bytes, &mut buffer[..size])
        }
    }
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','after','')")
        .await
        .unwrap();
    let snapshot = frames(&a, Audience::Store).await;
    let snapshot_prefix = snapshot.prefix.clone();
    let snapshot = snapshot.concat();
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let seen = observed.clone();
    let mut query = b.subscribe(move |db| {
        let values = db.query("SELECT title FROM notes ORDER BY id", [], |r| {
            r.get::<_, String>(0)
        })?;
        called.fetch_add(1, Ordering::SeqCst);
        seen.lock().unwrap().push(values.clone());
        Ok(values)
    });
    assert!(query.next().await.unwrap().is_empty());
    let (started, notice) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let target = b.clone();
    let task = tokio::spawn(async move {
        load_one(
            &target,
            id(Audience::Store),
            snapshot_prefix,
            GatedRead {
                bytes: Cursor::new(snapshot),
                started: Some(started),
                wait,
            },
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(10), notice)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        b.read(|db| Ok(db.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?))
            .await
            .unwrap(),
        0
    );
    assert!(!query.is_marked_for_rerun());
    release.send(()).unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(query.next().await.unwrap(), ["after"]);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(!query.is_marked_for_rerun());
    assert_eq!(
        *observed.lock().unwrap(),
        vec![Vec::<String>::new(), vec!["after".to_string()]]
    );
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn older_snapshots_cross_additions_but_not_breaking_schema_changes() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','old schema','')")
        .await
        .unwrap();
    let snapshot = frames(&a, Audience::Store).await;
    let b = b_store
        .builder(
            notes(),
            vec![
                crate::Migration::sql(1, "notes", NOTES),
                crate::Migration::sql(
                    2,
                    "color",
                    "ALTER TABLE notes ADD COLUMN color TEXT DEFAULT 'blue'",
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    load(&b, Audience::Store, snapshot.clone()).await;
    assert_eq!(
        b.read(
            |db| Ok(db.query_row("SELECT title,color FROM notes", [], |r| Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?
            )))?)
        )
        .await
        .unwrap(),
        ("old schema".into(), "blue".into())
    );
    b.close().await.unwrap();
    let b = b_store
        .builder(
            notes(),
            vec![
                crate::Migration::sql(1, "notes", NOTES),
                crate::Migration::sql(
                    2,
                    "color",
                    "ALTER TABLE notes ADD COLUMN color TEXT DEFAULT 'blue'",
                ),
                crate::Migration::sql(3, "rename", "ALTER TABLE notes RENAME COLUMN title TO name"),
            ],
        )
        .open()
        .await
        .unwrap();
    let before = contents(&b);
    assert!(matches!(
        load_one(
            &b,
            id(Audience::Store),
            snapshot.prefix.clone(),
            Cursor::new(snapshot.concat())
        )
        .await,
        Err(crate::DbError::Snapshot(
            crate::SnapshotError::Schema { .. }
        ))
    ));
    assert_eq!(contents(&b), before);
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn input_errors_and_panics_release_the_loading_transaction() {
    struct Refuse;
    impl std::io::Read for Refuse {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("read failed"))
        }
    }
    struct Panic;
    impl std::io::Read for Panic {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            panic!("reader panic")
        }
    }
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','keep','')")
        .await
        .unwrap();
    let before = contents(&db);
    assert!(matches!(
        load_one(
            &db,
            id(Audience::Store),
            frames(&db, Audience::Store).await.prefix,
            Refuse
        )
        .await,
        Err(crate::DbError::Snapshot(crate::SnapshotError::Read(_)))
    ));
    assert_eq!(contents(&db), before);
    let clone = db.clone();
    assert!(tokio::spawn(async move {
        load_one(
            &clone,
            id(Audience::Store),
            frames(&clone, Audience::Store).await.prefix,
            Panic,
        )
        .await
    })
    .await
    .unwrap_err()
    .is_panic());
    assert_eq!(contents(&db), before);
    sql(&db, "UPDATE notes SET title='still writable'")
        .await
        .unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn an_empty_loaded_audience_still_limits_later_snapshot_reloads() {
    const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,value INTEGER NOT NULL)";
    let tables = || {
        vec![SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience")]
    };
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(tables(), SCHEMA).await.unwrap();
    let b = b_store.schema(tables(), SCHEMA).await.unwrap();
    let circle = Audience::Circle(CircleId(uuid::Uuid::from_u128(10)));
    sql(
        &a,
        "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','store',1)",
    )
    .await
    .unwrap();
    for audience in [Audience::Store, circle.clone()] {
        load(&b, audience.clone(), frames(&a, audience).await).await;
    }
    sql(&a, "UPDATE notes SET value=2").await.unwrap();
    b.apply_downloaded(records(&a).remove(1).into())
        .await
        .unwrap();
    b.close().await.unwrap();
    let b = b_store.schema(tables(), SCHEMA).await.unwrap();
    sql(&a,"UPDATE notes SET value=3; INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000002','00000000-0000-0000-0000-00000000000a',3)").await.unwrap();
    b.load_snapshots(crate::SnapshotReload::new(
        vec![frames(&a, Audience::Store).await.input()],
        vec![stream(&records(&a)[2])],
    ))
    .await
    .unwrap();
    assert_eq!(b.sync_state(vec![]).await.unwrap().positions.0[0].number, 3);
    assert_eq!(frames(&b, circle.clone()).await, frames(&a, circle).await);
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn reloading_keeps_uploaded_own_history_before_authoring_another() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','first','')")
        .await
        .unwrap();
    let snapshot = frames(&db, Audience::Store).await;
    sql(&db, "UPDATE notes SET title='second'").await.unwrap();
    let uploaded = records(&db);
    db.inspect_writer(|db| {
        db.internal_execute("DELETE FROM coven_uploads", [])
            .unwrap()
    });
    db.load_snapshots(crate::SnapshotReload::new(
        vec![(
            id(Audience::Store),
            snapshot.prefix.clone(),
            Cursor::new(snapshot.concat()),
        )],
        vec![stream(&uploaded[1])],
    ))
    .await
    .unwrap();
    sql(&db, "UPDATE notes SET title='third'").await.unwrap();
    assert_eq!(records(&db)[0].header.position.number, 3);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waiting_for_a_snapshot_reader_keeps_the_writer_available() {
    use std::time::Duration;
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','before','')")
        .await
        .unwrap();
    let snapshot = frames(&db, Audience::Store).await;
    let snapshot_prefix = snapshot.prefix.clone();
    let snapshot = snapshot.concat();
    let mut readers = Vec::new();
    let mut releases = Vec::new();
    for _ in 0..4 {
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        releases.push(release);
        let reader = db.clone();
        readers.push(tokio::spawn(async move {
            let mut started = Some(started);
            reader
                .write_snapshot(
                    id(Audience::Store),
                    |_| Ok(()),
                    move |_| {
                        if let Some(started) = started.take() {
                            started.send(()).unwrap();
                            wait.recv_timeout(Duration::from_secs(10)).unwrap();
                        }
                        Ok::<_, std::convert::Infallible>(())
                    },
                )
                .await
                .unwrap();
        }));
        ready.await.unwrap();
    }
    let target = db.clone();
    let loading = tokio::spawn(async move {
        load_one(
            &target,
            id(Audience::Store),
            snapshot_prefix.clone(),
            Cursor::new(snapshot),
        )
        .await
    });
    // Every reader is reserved; allow the blocking loader to reach that wait.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let committed = tokio::time::timeout(
        Duration::from_secs(1),
        sql(&db, "UPDATE notes SET title='while readers are occupied'"),
    )
    .await;
    for release in releases {
        release.send(()).unwrap();
    }
    for reader in readers {
        reader.await.unwrap();
    }
    loading.await.unwrap().unwrap();
    db.close().await.unwrap();
    committed
        .expect("waiting for a reader must not hold the writer lock")
        .unwrap();
}

#[tokio::test]
async fn replaying_waiting_files_keeps_the_bytes_used_by_the_final_state() {
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    let empty = frames(&db, Audience::Store).await;
    let empty_prefix = empty.prefix.clone();
    let empty = empty.concat();
    attach(&db, b"first".to_vec(), true).await.unwrap();
    attach(&db, b"second".to_vec(), false).await.unwrap();
    let paths = owned_paths(&store);
    assert_eq!(paths.len(), 1);
    assert_eq!(std::fs::read(&paths[0]).unwrap(), b"second");
    let original = contents(&db);
    for _ in 0..2 {
        load_one(
            &db,
            id(Audience::Store),
            empty_prefix.clone(),
            Cursor::new(empty.clone()),
        )
        .await
        .unwrap();
        assert_eq!(local_count(&db, "coven_device_files"), 1);
        assert_eq!(owned_paths(&store), paths);
        assert_eq!(std::fs::read(&paths[0]).unwrap(), b"second");
        assert_eq!(contents(&db)["coven_uploads"], original["coven_uploads"]);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn removing_a_file_in_a_snapshot_discards_bytes_only_after_a_successful_commit() {
    let ids = SequentialIds::new();
    let store = TestStore::with_ids(&ids);
    let other = TestStore::with_ids(&ids);
    let source = other
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    attach(&db, b"keep until commit".to_vec(), true)
        .await
        .unwrap();
    source
        .apply_downloaded(records(&db)[0].clone().into())
        .await
        .unwrap();
    sql(&source, "DELETE FROM files").await.unwrap();
    let empty = frames(&source, Audience::Store).await;
    let empty_prefix = empty.prefix.clone();
    let empty = empty.concat();
    db.inspect_writer(|sql| {
        sql.internal_execute("DELETE FROM coven_uploads", [])
            .unwrap()
    });
    let paths = owned_paths(&store);
    let before = contents(&db);
    let truncated = empty[..empty.len() - 1].to_vec();
    assert!(load_one(
        &db,
        id(Audience::Store),
        empty_prefix.clone(),
        Cursor::new(truncated)
    )
    .await
    .is_err());
    assert_eq!(contents(&db), before);
    assert_eq!(std::fs::read(&paths[0]).unwrap(), b"keep until commit");
    db.inspect_writer(|sql| sql.batch("CREATE TEMP TRIGGER refuse_forgetting_file BEFORE DELETE ON coven_device_files BEGIN SELECT RAISE(ABORT,'keep bytes'); END").unwrap());
    assert!(matches!(
        load_one(
            &db,
            id(Audience::Store),
            empty_prefix.clone(),
            Cursor::new(empty.clone())
        )
        .await,
        Err(DbError::Sqlite(_))
    ));
    assert_eq!(contents(&db), before);
    assert_eq!(std::fs::read(&paths[0]).unwrap(), b"keep until commit");
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER refuse_forgetting_file").unwrap());
    load_one(
        &db,
        id(Audience::Store),
        empty_prefix.clone(),
        Cursor::new(empty),
    )
    .await
    .unwrap();
    assert_eq!(local_count(&db, "coven_device_files"), 0);
    assert_eq!(local_count(&db, "coven_file_removals"), 0);
    assert!(owned_paths(&store).is_empty());
    db.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn waiting_changes_replay_after_their_missing_own_predecessor() {
    for (uploaded_sql, waiting_sql) in [
        (
            "UPDATE notes SET title='second'",
            "UPDATE notes SET title='waiting'",
        ),
        (
            "DELETE FROM notes",
            "INSERT INTO notes VALUES('n','waiting','')",
        ),
    ] {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('n','first','')")
            .await
            .unwrap();
        let old = frames(&db, Audience::Store).await;
        let old_prefix = old.prefix.clone();
        let old = old.concat();
        sql(&db, uploaded_sql).await.unwrap();
        let missing = records(&db).pop().unwrap();
        let ready = frames(&db, Audience::Store).await;
        let ready_prefix = ready.prefix.clone();
        let ready = ready.concat();
        db.inspect_writer(|db| {
            db.internal_execute("DELETE FROM coven_uploads", [])
                .unwrap()
        });
        sql(&db, waiting_sql).await.unwrap();
        let waiting = records(&db);
        let expected = frames(&db, Audience::Store).await;
        for _ in 0..2 {
            db.load_snapshots(crate::SnapshotReload::new(
                vec![(
                    id(Audience::Store),
                    old_prefix.clone(),
                    Cursor::new(old.clone()),
                )],
                vec![stream(&missing)],
            ))
            .await
            .unwrap();
            assert_eq!(records(&db), waiting);
        }
        assert_eq!(frames(&db, Audience::Store).await, expected);
        load_one(
            &db,
            id(Audience::Store),
            ready_prefix.clone(),
            Cursor::new(ready),
        )
        .await
        .unwrap();
        assert_eq!(frames(&db, Audience::Store).await, expected);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn an_edit_after_reload_reads_the_generation_from_the_newer_snapshot() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    let circle = Audience::Circle(CircleId(uuid::Uuid::from_u128(10)));
    let empty = frames(&a, circle.clone()).await;
    let empty_prefix = empty.prefix.clone();
    let empty = empty.concat();
    sql(
        &a,
        "INSERT INTO notes VALUES('n','born after circle snapshot','')",
    )
    .await
    .unwrap();
    b.load_snapshots(crate::SnapshotReload::new(
        vec![
            (id(circle), empty_prefix.clone(), Cursor::new(empty)),
            frames(&a, Audience::Store).await.input(),
        ],
        vec![stream(&records(&a)[0])],
    ))
    .await
    .unwrap();
    sql(&b, "UPDATE notes SET title='edited after reload'")
        .await
        .unwrap();
    let edited = records(&b).pop().unwrap();
    assert_eq!(edited.header.had_read.0, [records(&a)[0].header.position]);
    a.apply_downloaded(edited.into()).await.unwrap();
    assert_eq!(
        frames(&b, Audience::Store).await,
        frames(&a, Audience::Store).await
    );
    for db in [a, b] {
        db.close().await.unwrap();
    }
}
