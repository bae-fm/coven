use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use coven_format::{merge_fields, value::WritePositions};
use coven_foundation::{clock::FixedClock, id_source::DeviceId};
use coven_merge::{Timestamp, WriteId};

use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{DbError, Migration};

fn applied(database: &crate::Database, stamp: Timestamp, number: u64) {
    database.inspect_writer(|db| {
        db.internal_execute(
            "INSERT INTO coven_writes(timestamp,number,had_read) VALUES(?1,?2,?3)",
            crate::params![
                merge_fields::encode_timestamp(&stamp).unwrap(),
                number.to_be_bytes().as_slice(),
                merge_fields::encode_write_positions(&WritePositions(vec![])).unwrap(),
            ],
        )
        .unwrap();
        db.internal_execute("INSERT INTO coven_positions(device,number) VALUES(?1,?2) ON CONFLICT(device) DO UPDATE SET number=excluded.number",crate::params![stamp.device().0.to_be_bytes().as_slice(),number.to_be_bytes().as_slice()]).unwrap();
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
    for table in ["notes", "local_rows", "coven_writes", "coven_uploads"] {
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
