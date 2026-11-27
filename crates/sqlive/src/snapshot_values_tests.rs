use crate::tests::{HeardBugs, TestError};
use crate::{Database, Handle, NoPolicy, Options, Setup};
use rusqlite::functions::FunctionFlags;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// A database of notes whose connections read `held` through
/// `host_value()`, as each transaction's snapshot saw it.
fn open(directory: &tempfile::TempDir, held: &Arc<AtomicI64>) -> Database<NoPolicy> {
    let bugs: panic_as_error::BugSinkRef = Arc::new(HeardBugs::default());
    let held = Arc::clone(held);
    let setup = move |setup: &Setup<'_, NoPolicy>, _: &NoPolicy| {
        let held = Arc::clone(&held);
        let value = setup.snapshot_value(move || held.load(Ordering::SeqCst));
        setup.scalar_function("host_value", 0, FunctionFlags::SQLITE_UTF8, move |_| Ok(value.get()))
    };
    let (database, ()) = Database::open_with(
        &directory.path().join("values.sqlite"),
        Handle::recording_all(bugs),
        Options {
            settings: crate::SqliteSettings::default(),
            readers: std::num::NonZeroUsize::MIN,
            prepare_on_reader: false,
            policy: &|| NoPolicy,
            setup: &setup,
            capture: Some(Default::default()),
            checkpoint_pages: None,
            statistics: false,
        },
        |writer| writer.batch("CREATE TABLE notes(id INTEGER PRIMARY KEY)"),
    )
    .expect("the database opens");
    database
}

/// The notes counted and the host's value, as one statement reads them.
const READ: &str = "SELECT (SELECT count(*) FROM notes),host_value()";

/// A read takes the host's value as its snapshot is fixed. A commit and a
/// change of the value after that, before the read's statement runs, reach
/// neither the tables it reads nor the value; the next read sees both.
#[test]
fn a_read_takes_the_value_as_its_snapshot_is_fixed() {
    let directory = tempfile::tempdir().unwrap();
    let held = Arc::new(AtomicI64::new(1));
    let database = open(&directory, &held);
    let read = |during: &dyn Fn()| -> (i64, i64) {
        database.reader().host(|reader| {
            reader
                .read_snapshot(|reader| {
                    during();
                    reader.query_row(READ, [], |r| Ok((r.get(0)?, r.get(1)?)))
                })
                .map_err(|error: TestError| error)
                .unwrap()
        })
    };
    let seen = read(&|| {
        database
            .host(|writer| {
                writer.transaction(|writer| writer.batch("INSERT INTO notes(id) VALUES(1)"))
            })
            .unwrap();
        held.store(2, Ordering::SeqCst);
    });
    assert_eq!(seen, (0, 1));
    assert_eq!(read(&|| {}), (1, 2));
    assert!(database.close().is_empty());
}

/// A write takes the value as it begins and reads it so until it commits;
/// a statement outside any transaction reads the value as it is.
#[test]
fn a_write_takes_the_value_as_it_begins() {
    let directory = tempfile::tempdir().unwrap();
    let held = Arc::new(AtomicI64::new(1));
    let database = open(&directory, &held);
    let seen: i64 = database
        .host(|writer| {
            writer.transaction(|writer| {
                held.store(2, Ordering::SeqCst);
                writer.query_row("SELECT host_value()", [], |r| r.get(0))
            })
        })
        .map_err(|error: TestError| error)
        .unwrap();
    assert_eq!(seen, 1);
    let now: i64 = database
        .host(|writer| writer.query_row("SELECT host_value()", [], |r| r.get(0)))
        .map_err(|error: TestError| error)
        .unwrap();
    assert_eq!(now, 2);
    assert!(database.close().is_empty());
}
