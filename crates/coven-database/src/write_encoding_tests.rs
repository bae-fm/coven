use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use coven_format::{
    merge_fields,
    value::{EntryPositions, WritePositions},
    write::WriteRecord,
};
use coven_foundation::{
    clock::FixedClock,
    id_source::{DeviceId, SequentialIds},
};
use coven_merge::{Audience, Timestamp, WriteId};
use serde_json::{json, Value};

use crate::snapshot_write::tests::{frames, stream};
use crate::tests::{contents, TestStore};
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{ApplyOutcome, Database, DbError, Migration, SnapshotError, SnapshotReload, WriteWait};

fn applied(database: &crate::Database, stamp: Timestamp, number: u64) {
    database.inspect_writer(|db| {
        db.internal_execute(
            "INSERT INTO _coven_writes(timestamp,number,had_read) VALUES(?1,?2,?3)",
            crate::params![
                merge_fields::encode_timestamp(&stamp).unwrap(),
                number.to_be_bytes().as_slice(),
                merge_fields::encode_write_positions(&WritePositions(vec![])).unwrap(),
            ],
        )
        .unwrap();
        db.internal_execute("INSERT INTO _coven_positions(device,number) VALUES(?1,?2) ON CONFLICT(device) DO UPDATE SET number=excluded.number",crate::params![stamp.device().0.to_be_bytes().as_slice(),number.to_be_bytes().as_slice()]).unwrap();
    });
}

#[tokio::test]
async fn bens_clock_behind_anas_write_raises_the_counter_and_records_every_applied_log() {
    let store = TestStore::new();
    let clock = Arc::new(FixedClock::new(
        UNIX_EPOCH + Duration::from_millis(3_600_000),
    ));
    let database = store
        .builder(notes(), vec![Migration::sql(1, "notes", NOTES)])
        .clock(clock.clone())
        .open()
        .await
        .unwrap();
    sql(
        &database,
        "INSERT INTO notes VALUES('42','Grocery list','')",
    )
    .await
    .unwrap();
    let device = records(&database)[0].header.position.device;
    let ana = DeviceId(device.0.wrapping_add(1));
    let carol = DeviceId(device.0.wrapping_add(2));
    for number in 1..=4 {
        applied(
            &database,
            Timestamp::new(3_660_000, number as u16 - 1, ana).unwrap(),
            number,
        );
    }
    applied(&database, Timestamp::new(3_500_000, 0, carol).unwrap(), 1);
    sql(&database, "UPDATE notes SET title='Weekly groceries'")
        .await
        .unwrap();
    let write = &records(&database)[1].header;
    assert_eq!(
        (write.timestamp.milliseconds(), write.timestamp.counter()),
        (3_660_000, 4)
    );
    assert_eq!(write.position.number, 2);
    let mut expected = vec![
        WriteId {
            device: ana,
            number: 4,
        },
        WriteId {
            device: carol,
            number: 1,
        },
    ];
    expected.sort();
    assert_eq!(write.had_read.0, expected);
    clock.set(UNIX_EPOCH - Duration::from_secs(1));
    sql(&database, "UPDATE notes SET title='Groceries'")
        .await
        .unwrap();
    assert_eq!(records(&database)[2].header.timestamp.counter(), 5);
    database.close().await.unwrap();
    let reopened = store
        .builder(notes(), vec![Migration::sql(1, "notes", NOTES)])
        .clock(clock)
        .open()
        .await
        .unwrap();
    sql(&reopened, "UPDATE notes SET title='Hardware store'")
        .await
        .unwrap();
    let header = &records(&reopened)[3].header;
    assert_eq!(header.position.number, 4);
    assert_eq!(header.timestamp.counter(), 6);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn clock_and_counter_limits_roll_back_without_using_a_number() {
    let store = TestStore::new();
    let clock = Arc::new(FixedClock::new(
        UNIX_EPOCH + Duration::from_millis(Timestamp::MAX_MILLISECONDS + 1),
    ));
    let database = store
        .builder(notes(), vec![Migration::sql(1, "notes", NOTES)])
        .clock(clock.clone())
        .open()
        .await
        .unwrap();
    assert!(matches!(
        sql(
            &database,
            "INSERT INTO notes VALUES('42','Groceries',''); INSERT INTO local_rows VALUES('x')"
        )
        .await,
        Err(DbError::ClockOutOfRange)
    ));
    for table in ["notes", "local_rows", "_coven_writes", "_coven_uploads"] {
        assert_eq!(count(&database, table), 0);
    }
    // A transaction changing only local rows has no timestamp to allocate.
    sql(&database, "INSERT INTO local_rows VALUES('x')")
        .await
        .unwrap();
    clock.set(UNIX_EPOCH + Duration::from_millis(50));
    sql(&database, "INSERT INTO notes VALUES('42','Groceries','')")
        .await
        .unwrap();
    let device = records(&database)[0].header.position.device;
    let other = DeviceId(device.0.wrapping_add(1));
    applied(&database, Timestamp::new(50, u16::MAX, other).unwrap(), 1);
    sql(&database, "UPDATE notes SET title='Hardware store'")
        .await
        .unwrap();
    let stamp = records(&database)[1].header.timestamp;
    assert_eq!((stamp.milliseconds(), stamp.counter()), (51, 0));
    applied(
        &database,
        Timestamp::new(Timestamp::MAX_MILLISECONDS, u16::MAX, other).unwrap(),
        2,
    );
    assert!(matches!(
        sql(&database, "UPDATE notes SET title='Weekly groceries'").await,
        Err(DbError::ClockOutOfRange)
    ));
    assert_eq!(records(&database).len(), 2);
    database.inspect_writer(|db| {
        assert_eq!(
            db.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "Hardware store"
        )
    });
    database.close().await.unwrap();
}

#[tokio::test]
async fn format_limits_are_typed_and_the_failed_write_is_absent() {
    let store = TestStore::new();
    let database = store.schema(notes(), NOTES).await.unwrap();
    for (title, body) in [
        (vec![b'x'; 8 * 1024 * 1024 + 1], vec![]),
        (vec![b'x'; 8 * 1024 * 1024], vec![b'y'; 8 * 1024 * 1024]),
    ] {
        let result = database
            .write(move |context| {
                context.execute(
                    "INSERT INTO notes VALUES('42',?1,?2)",
                    crate::params![
                        String::from_utf8(title).unwrap(),
                        String::from_utf8(body).unwrap()
                    ],
                )?;
                context.execute("INSERT INTO local_rows VALUES('x')", [])?;
                Ok(())
            })
            .await;
        assert!(
            matches!(result, Err(DbError::TooLarge { actual, maximum, .. }) if actual > maximum)
        );
        assert_eq!(count(&database, "notes"), 0);
        assert_eq!(count(&database, "local_rows"), 0);
        assert!(records(&database).is_empty());
    }
    sql(&database, "INSERT INTO notes VALUES('42','Groceries','')")
        .await
        .unwrap();
    assert_eq!(records(&database)[0].header.position.number, 1);
    database.close().await.unwrap();
}

#[test]
fn other_local_format_errors_panic_naming_the_invariant() {
    let panic = std::panic::catch_unwind(|| {
        crate::write_encoding::encoded::<()>(Err(coven_format::Error::Invalid {
            field: "old columns",
            rule: coven_format::error::Rule::ColumnOperation,
        }))
    })
    .unwrap_err();
    let message = panic.downcast::<String>().unwrap();
    assert!(message.contains("old columns") && message.contains("ColumnOperation"));
}

async fn open(store: &TestStore, clock: &Arc<FixedClock>) -> Database {
    store
        .builder(notes(), vec![Migration::sql(1, "notes", NOTES)])
        .clock(clock.clone())
        .open()
        .await
        .unwrap()
}

async fn receive(db: &Database, write: &WriteRecord, streamed: bool) -> ApplyOutcome {
    if streamed {
        db.apply_downloaded_stream(stream(write), EntryPositions(vec![]), || Ok(()))
            .await
            .unwrap()
    } else {
        db.apply_downloaded(write.clone().into()).await.unwrap()
    }
}

#[tokio::test]
async fn far_future_writes_wait_only_for_causes_and_advance_stamps_after_clock_rollback() {
    for streamed in [false, true] {
        let ids = SequentialIds::new();
        let source_store = TestStore::with_ids(&ids);
        let receiver_store = TestStore::with_ids(&ids);
        let source_clock = Arc::new(FixedClock::new(
            UNIX_EPOCH + Duration::from_secs(365 * 86400),
        ));
        let receiver_clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
        let source = open(&source_store, &source_clock).await;
        let receiver = open(&receiver_store, &receiver_clock).await;
        sql(&source, "INSERT INTO notes VALUES('n','future','')")
            .await
            .unwrap();
        source_clock.set(UNIX_EPOCH - Duration::from_secs(1));
        sql(&source, "UPDATE notes SET title='after rollback'")
            .await
            .unwrap();
        let writes = records(&source);
        assert!(writes[1].header.timestamp > writes[0].header.timestamp);
        assert_eq!(
            receive(&receiver, &writes[1], streamed).await,
            ApplyOutcome::Waiting(WriteWait::Writes(vec![writes[0].header.position]))
        );
        sql(
            &receiver,
            "INSERT INTO notes VALUES('local','before receiving','')",
        )
        .await
        .unwrap();
        assert_eq!(records(&receiver)[0].header.timestamp.milliseconds(), 1_000);
        receiver_clock.set(UNIX_EPOCH - Duration::from_secs(1));
        for write in &writes {
            assert_eq!(
                receive(&receiver, write, streamed).await,
                ApplyOutcome::Applied
            );
        }
        assert_eq!(
            receive(&receiver, &writes[1], streamed).await,
            ApplyOutcome::AlreadyApplied
        );
        assert_eq!(
            receiver
                .read(|db| Ok(db
                    .query_row("SELECT title FROM notes WHERE id='n'", [], |r| r
                        .get::<_, String>(0))?))
                .await
                .unwrap(),
            "after rollback"
        );
        receiver.close().await.unwrap();
        let receiver = open(&receiver_store, &receiver_clock).await;
        sql(&receiver, "UPDATE notes SET title='received' WHERE id='n'")
            .await
            .unwrap();
        let outgoing = records(&receiver).pop().unwrap();
        assert!(outgoing.header.timestamp > writes[1].header.timestamp);
        assert_eq!(outgoing.header.had_read.0, [writes[1].header.position]);
        assert_eq!(
            receive(&source, &records(&receiver)[0], streamed).await,
            ApplyOutcome::Applied
        );
        assert_eq!(
            receive(&source, &outgoing, streamed).await,
            ApplyOutcome::Applied
        );
        assert_eq!(
            frames(&source, Audience::Store).await,
            frames(&receiver, Audience::Store).await
        );
        source.close().await.unwrap();
        receiver.close().await.unwrap();
    }
}

#[tokio::test]
async fn far_future_snapshot_and_tail_apply_with_their_causes_and_preserve_queued_writes() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let source_clock = Arc::new(FixedClock::new(
        UNIX_EPOCH + Duration::from_secs(365 * 86400),
    ));
    let receiver_clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
    let source = open(&source_store, &source_clock).await;
    let receiver = open(&receiver_store, &receiver_clock).await;
    sql(&source, "INSERT INTO notes VALUES('n','snapshot','')")
        .await
        .unwrap();
    let snapshot = frames(&source, Audience::Store).await;
    source_clock.set(UNIX_EPOCH - Duration::from_secs(1));
    sql(&source, "UPDATE notes SET title='tail cause'")
        .await
        .unwrap();
    sql(&source, "UPDATE notes SET title='tail effect'")
        .await
        .unwrap();
    let writes = records(&source);
    let before = contents(&receiver);
    assert!(matches!(
        receiver
            .load_snapshots(SnapshotReload::new(
                vec![snapshot.input()],
                vec![stream(&writes[2])]
            ))
            .await,
        Err(crate::DbError::Snapshot(
            SnapshotError::MissingWrites { .. }
        ))
    ));
    assert_eq!(contents(&receiver), before);
    receiver
        .load_snapshots(SnapshotReload::new(
            vec![snapshot.input()],
            vec![] as Vec<crate::DownloadedWriteStream<std::io::Cursor<Vec<u8>>>>,
        ))
        .await
        .unwrap();
    sql(&receiver, "INSERT INTO notes VALUES('local','queued','')")
        .await
        .unwrap();
    let queued = records(&receiver);
    assert!(queued[0].header.timestamp > writes[0].header.timestamp);
    receiver_clock.set(UNIX_EPOCH - Duration::from_secs(1));
    receiver
        .load_snapshots(SnapshotReload::new(
            vec![snapshot.input()],
            vec![stream(&writes[2]), stream(&writes[1])],
        ))
        .await
        .unwrap();
    assert_eq!(records(&receiver), queued);
    assert_eq!(
        receiver
            .read(
                |db| Ok(db.query("SELECT title FROM notes ORDER BY id", [], |r| r
                    .get::<_, String>(0))?)
            )
            .await
            .unwrap(),
        ["queued", "tail effect"]
    );
    receiver.close().await.unwrap();
    let receiver = open(&receiver_store, &receiver_clock).await;
    sql(
        &receiver,
        "UPDATE notes SET title='after reload' WHERE id='n'",
    )
    .await
    .unwrap();
    let outgoing = records(&receiver).pop().unwrap();
    assert!(outgoing.header.timestamp > writes[2].header.timestamp);
    assert!(outgoing.header.timestamp > queued[0].header.timestamp);
    source.close().await.unwrap();
    receiver.close().await.unwrap();
}

fn timestamp_json(timestamp: Timestamp) -> Value {
    json!([
        timestamp.milliseconds(),
        timestamp.counter(),
        timestamp.device().0
    ])
}

fn model(runner: &std::ffi::OsStr, input: Value) -> Value {
    let mut child = Command::new(runner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.to_string().as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn latest(db: &Database) -> Timestamp {
    db.inspect_writer(|db| {
        crate::write_encoding::latest_timestamp(db)
            .unwrap()
            .unwrap()
    })
}

#[tokio::test]
#[ignore = "requires the Lean executable; scripts/check.sh supplies COVEN_CLOCK_LEAN"]
async fn lean_differential_clocks() {
    let runner = std::env::var_os("COVEN_CLOCK_LEAN")
        .expect("COVEN_CLOCK_LEAN must name the built Lean runner");
    let ids = SequentialIds::new();
    for milliseconds in [301_001, 31_536_000_000, Timestamp::MAX_MILLISECONDS] {
        let source_store = TestStore::with_ids(&ids);
        let source_clock = Arc::new(FixedClock::new(
            UNIX_EPOCH + Duration::from_millis(milliseconds),
        ));
        let source = open(&source_store, &source_clock).await;
        sql(&source, "INSERT INTO notes VALUES('n','first','')")
            .await
            .unwrap();
        source_clock.set(UNIX_EPOCH);
        sql(&source, "UPDATE notes SET title='second'")
            .await
            .unwrap();
        let writes = records(&source);
        let snapshot = frames(&source, Audience::Store).await;
        for wall in [-1_000_i64, 0, 1_000] {
            for mode in 0..3 {
                let receiver_store = TestStore::with_ids(&ids);
                let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
                let db = open(&receiver_store, &clock).await;
                sql(&db, "INSERT INTO notes VALUES('local','local','')")
                    .await
                    .unwrap();
                let device = records(&db)[0].header.position.device;
                clock.set(if wall < 0 {
                    UNIX_EPOCH - Duration::from_millis(wall.unsigned_abs())
                } else {
                    UNIX_EPOCH + Duration::from_millis(wall as u64)
                });
                let expected = if mode == 2 {
                    let expected = model(
                        &runner,
                        json!({
                            "wall": wall, "device": device.0, "latest": timestamp_json(latest(&db)),
                            "incoming": writes.iter().map(|w| timestamp_json(w.header.timestamp)).collect::<Vec<_>>(),
                            "snapshot": true, "causes": true,
                        }),
                    );
                    db.load_snapshots(SnapshotReload::new(
                        vec![snapshot.input()],
                        Vec::<crate::DownloadedWriteStream<std::io::Cursor<Vec<u8>>>>::new(),
                    ))
                    .await
                    .unwrap();
                    assert_eq!(expected["latest"], timestamp_json(latest(&db)));
                    expected
                } else {
                    let mut expected = Value::Null;
                    for (index, causes) in [(1, false), (0, true), (1, true)] {
                        expected = model(
                            &runner,
                            json!({
                                "wall": wall, "device": device.0, "latest": timestamp_json(latest(&db)),
                                "incoming": [timestamp_json(writes[index].header.timestamp)],
                                "snapshot": false, "causes": causes,
                            }),
                        );
                        let applied =
                            receive(&db, &writes[index], mode == 1).await == ApplyOutcome::Applied;
                        assert_eq!(expected["applied"], json!(usize::from(applied)));
                        assert_eq!(expected["latest"], timestamp_json(latest(&db)));
                    }
                    expected
                };
                sql(&db, "UPDATE notes SET title='after receiving' WHERE id='n'")
                    .await
                    .unwrap();
                assert_eq!(
                    expected["next"],
                    timestamp_json(records(&db).pop().unwrap().header.timestamp)
                );
                db.close().await.unwrap();
            }
        }
        source.close().await.unwrap();
    }
}
