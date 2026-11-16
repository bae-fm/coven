use super::*;
use crate::tests::{next, TestDatabase, TestError, TestOptions};
use crate::HandleFailure;
use std::convert::Infallible;

const SCHEMA: &str = "CREATE TABLE parent(id TEXT NOT NULL PRIMARY KEY);
CREATE TABLE child(id TEXT NOT NULL PRIMARY KEY, parent TEXT REFERENCES parent(id) ON DELETE CASCADE);
CREATE TABLE log(id INTEGER PRIMARY KEY, note TEXT);
CREATE TRIGGER logged AFTER DELETE ON child BEGIN INSERT INTO log(note) VALUES(old.id); END;";

/// A database of `schema` that records each operation of a write on
/// `tables`, with every column's value before and after it.
fn captured(schema: &str, tables: &[&str]) -> TestDatabase {
    TestDatabase::open_with(
        schema,
        TestOptions {
            captured: tables.iter().map(|table| (*table).to_owned()).collect(),
            ..TestOptions::default()
        },
    )
}

/// A database of `SCHEMA` that records each operation of a write on `parent`
/// and `child`, with every column's value before and after it.
fn family() -> TestDatabase {
    captured(SCHEMA, &["parent", "child"])
}

fn summary(events: &[Operation]) -> Vec<(&str, Action)> {
    events.iter().map(|event| (event.table.as_str(), event.action)).collect()
}

/// The capture records every operation on a captured table in order: a
/// statement's own rows and the foreign-key actions they cause. The capture
/// leaves out the `log` row the trigger inserts, since `log` isn't
/// captured.
#[test]
fn operations_keep_their_order() {
    let db = family();
    db.write("INSERT INTO parent VALUES('p'); INSERT INTO child VALUES('c','p')").unwrap();
    let events = db.host(|c| {
        let mut transaction = WriteTransaction::begin(c, false).unwrap();
        let capture = EventCapture::begin(&mut transaction).unwrap();
        transaction
            .within(|db| {
                db.batch("UPDATE child SET id='d' WHERE id='c'; DELETE FROM parent WHERE id='p'")
            })
            .unwrap();
        capture.events()
    });
    assert_eq!(
        summary(&events),
        [
            ("child", Action::SQLITE_UPDATE),
            ("parent", Action::SQLITE_DELETE),
            ("child", Action::SQLITE_DELETE),
        ]
    );
    let text = |s: &str| Some(SqlValue::from(rusqlite::types::ValueRef::Text(s.as_bytes())));
    assert_eq!(events[0].old, [text("c"), text("p")]);
    assert_eq!(events[0].new, [text("d"), text("p")]);
    assert!(events[1].new.is_empty());
    assert_eq!(events[2].old, [text("d"), text("p")]);
    // Removing the hook leaves later statements unobserved.
    db.host(|c| {
        let mut transaction = WriteTransaction::begin(c, false).unwrap();
        let capture = EventCapture::begin(&mut transaction).unwrap();
        transaction.stop_observing_operations().unwrap();
        transaction.within(|db| db.batch("DELETE FROM parent")).unwrap();
        assert!(capture.events().is_empty());
    });
    db.close();
}

/// The capture copies only a captured table's rows into the write's
/// operations.
#[test]
fn an_uncaptured_tables_rows_are_not_copied() {
    let db = family();
    let events = db.host(|c| {
        let mut transaction = WriteTransaction::begin(c, false).unwrap();
        let capture = EventCapture::begin(&mut transaction).unwrap();
        transaction
            .within(|db| {
                db.batch("INSERT INTO parent VALUES('p'); INSERT INTO log(note) VALUES('a'),('b')")
            })
            .unwrap();
        capture.events()
    });
    assert_eq!(summary(&events), [("parent", Action::SQLITE_INSERT)]);
    db.close();
}

/// What a write to an uncaptured table costs through sqlive, against the
/// same SQL in plain SQLite with the same durability settings, with a live
/// query that reads the whole table and with one that narrows by a column,
/// whose values each write then records. Run explicitly.
#[tokio::test]
#[ignore = "one timing measurement, run explicitly"]
async fn local_table_writes_against_plain_sqlite() {
    const QUEUE: &str = "CREATE TABLE queue(lane INTEGER NOT NULL, position INTEGER NOT NULL, track TEXT NOT NULL, PRIMARY KEY(lane, position))";
    let fill = |rows: u32| {
        format!(
            "WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<{}) INSERT INTO queue SELECT 0, i, printf('track-%08d', i) FROM n",
            rows - 1
        )
    };
    for rows in [5_000, 50_000, 1_000_000] {
        let directory = tempfile::tempdir().unwrap();
        let plain = rusqlite::Connection::open(directory.path().join("plain.db")).unwrap();
        plain
            .execute_batch(&format!(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; {QUEUE}"
            ))
            .unwrap();
        let started = std::time::Instant::now();
        plain.execute_batch(&format!("BEGIN; {}; COMMIT", fill(rows))).unwrap();
        let plain_insert = started.elapsed();
        let started = std::time::Instant::now();
        plain.execute_batch("BEGIN; DELETE FROM queue; COMMIT").unwrap();
        let plain_delete = started.elapsed();

        let db = captured(&format!("{SCHEMA} {QUEUE}"), &["parent", "child"]);
        let mut live = db.subscribe(|sql| {
            Ok(sql.query_row("SELECT count(*) FROM queue", [], |r| r.get::<_, i64>(0))?)
        });
        assert_eq!(next(&mut live).await, 0);
        let insert = fill(rows);
        let started = std::time::Instant::now();
        db.write(&insert).unwrap();
        let sqlive_insert = started.elapsed();
        assert_eq!(next(&mut live).await, i64::from(rows));
        let started = std::time::Instant::now();
        db.write("DELETE FROM queue").unwrap();
        let sqlive_delete = started.elapsed();
        assert_eq!(next(&mut live).await, 0);
        drop(live);
        let mut narrow = db.subscribe(|sql| {
            Ok(sql.query("SELECT position FROM queue WHERE track = 'none'", [], |r| {
                r.get::<_, i64>(0)
            })?)
        });
        assert!(next(&mut narrow).await.is_empty());
        let started = std::time::Instant::now();
        db.write(&insert).unwrap();
        let narrowed_insert = started.elapsed();
        let started = std::time::Instant::now();
        db.write("DELETE FROM queue").unwrap();
        let narrowed_delete = started.elapsed();
        // Past the bound, the write keeps only that the table changed.
        assert_eq!(narrow.is_marked_for_rerun(), rows as usize > crate::ROWS_PER_TABLE);
        db.close();
        for (metric, time) in [
            ("sqlive_insert", sqlive_insert),
            ("narrowed_insert", narrowed_insert),
            ("plain_insert", plain_insert),
            ("sqlive_delete", sqlive_delete),
            ("narrowed_delete", narrowed_delete),
            ("plain_delete", plain_delete),
        ] {
            crate::report_time(&format!("local_table_{rows}_rows/{metric}"), time);
        }
        eprintln!(
            "{rows} rows: insert {sqlive_insert:?} through sqlive, {narrowed_insert:?} narrowed, {plain_insert:?} plain; delete {sqlive_delete:?} through sqlive, {narrowed_delete:?} narrowed, {plain_delete:?} plain"
        );
    }
}

/// A read of a pre-update value: a virtual generated column keeps none, and
/// SQLite answers `SQLITE_RANGE` for it; any other failure is a failed read,
/// never a missing value.
#[test]
fn only_a_virtual_generated_column_reads_as_absent() {
    let column_value =
        crate::connection::pre_update_changes::column_value::<HandleFailure, Infallible>;
    let failure = |code| Err(rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None));
    assert_eq!(column_value(failure(rusqlite::ffi::SQLITE_RANGE)).unwrap(), None);
    assert!(column_value(failure(rusqlite::ffi::SQLITE_MISUSE)).is_err());
    assert!(column_value(failure(rusqlite::ffi::SQLITE_NOMEM)).is_err());
    let value = rusqlite::types::ValueRef::Integer(7);
    assert_eq!(column_value(Ok(value)).unwrap(), Some(value));
}

/// An update of a row with generated columns: the virtual one reads as
/// absent before and after, the stored one with its values.
#[test]
fn a_virtual_generated_column_is_recorded_absent() {
    const GENERATED: &str = "CREATE TABLE parent(id TEXT NOT NULL PRIMARY KEY, n INTEGER,
        doubled INTEGER GENERATED ALWAYS AS (n*2) VIRTUAL,
        tripled INTEGER GENERATED ALWAYS AS (n*3) STORED);";
    let db = captured(GENERATED, &["parent"]);
    db.write("INSERT INTO parent(id,n) VALUES('p',1)").unwrap();
    let events = db.host(|c| {
        let mut transaction = WriteTransaction::begin(c, false).unwrap();
        let capture = EventCapture::begin(&mut transaction).unwrap();
        transaction.within(|db| db.batch("UPDATE parent SET n=2")).unwrap();
        capture.events()
    });
    let integer = |n| Some(SqlValue::from(rusqlite::types::ValueRef::Integer(n)));
    let text = |s: &str| Some(SqlValue::from(rusqlite::types::ValueRef::Text(s.as_bytes())));
    assert_eq!(events[0].old, [text("p"), integer(1), None, integer(3)]);
    assert_eq!(events[0].new, [text("p"), integer(2), None, integer(6)]);
    db.close();
}

/// A hook that fails, as a read of the row it observes can, fails the
/// transaction with that failure: nothing it missed commits.
#[test]
fn a_failing_hook_rolls_its_transaction_back() {
    let db = family();
    db.host(|c| {
        let mut transaction = WriteTransaction::begin(c, false).unwrap();
        transaction
            .observe_operations(|_, _, _, _| {
                Err(crate::Error::Invariant { what: "the row's old value could be read" })
            })
            .unwrap();
        transaction.within(|db| db.batch("INSERT INTO parent VALUES('p')")).unwrap();
        let committed = transaction.commit(Ok::<_, TestError>(((), Vec::new())));
        assert!(matches!(committed, Err(crate::Error::Invariant { .. })), "{committed:?}");
    });
    assert_eq!(db.value::<i64>("SELECT count(*) FROM parent"), 0);
    db.close();
}
