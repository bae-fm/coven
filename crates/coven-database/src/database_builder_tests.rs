use crate::test_utils::{notes_migrations, notes_tables};
use crate::tests::TestStore;
use crate::{DbError, Migration};
use std::io::{Read, Seek, SeekFrom, Write};

/// The notes table, and a device-local table whose rows coven copies
/// nowhere else, so its bytes are only in its own pages.
fn migrations() -> Vec<Migration> {
    let mut migrations = notes_migrations();
    migrations.push(Migration::addition(
        2,
        "drafts",
        "CREATE TABLE drafts(id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
    ));
    migrations
}

fn page_size(store: &TestStore) -> usize {
    let raw = rusqlite::Connection::open(store.database_path()).unwrap();
    raw.query_row("PRAGMA page_size", [], |r| r.get::<_, u32>(0)).unwrap() as usize
}

/// Point the first full overflow page of a value made of `filler` at a page
/// past the end of the file, as damage on disk would. Nothing reads the
/// value's overflow pages unless a statement reads the value itself.
fn damage_overflow(store: &TestStore, filler: u8) {
    let size = page_size(store);
    let mut file =
        std::fs::OpenOptions::new().read(true).write(true).open(store.database_path()).unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    let page = bytes
        .chunks(size)
        .position(|page| page[4..].iter().all(|b| *b == filler))
        .expect("an overflow page of the value");
    file.seek(SeekFrom::Start((page * size) as u64)).unwrap();
    file.write_all(&u32::MAX.to_be_bytes()).unwrap();
    file.sync_all().unwrap();
}

/// A draft four pages long, all 'A'.
async fn long_draft(store: &TestStore) {
    let database = store.builder(notes_tables(), migrations()).open().await.unwrap();
    let body = "A".repeat(page_size(store) * 4);
    database
        .write(move |sql| {
            sql.execute("INSERT INTO drafts VALUES(1, ?1)", [body])?;
            Ok(())
        })
        .await
        .unwrap();
    database.close().await.unwrap();
}

/// Opening reads none of the database to look for damage: damage only a
/// later statement meets fails that statement, with `DamagedDatabase`, and
/// the store keeps opening, read-only too. A reset reads the replaced
/// database's journal, which the damage doesn't reach (§19.3).
#[tokio::test]
async fn damage_a_statement_meets_after_opening_fails_that_statement() {
    let store = TestStore::new();
    long_draft(&store).await;
    damage_overflow(&store, b'A');

    let database = store.builder(notes_tables(), migrations()).open().await.unwrap();
    let ids = database
        .read(|sql| {
            Ok::<_, DbError>(sql.query("SELECT id FROM drafts", [], |r| r.get::<_, i64>(0))?)
        })
        .await
        .unwrap();
    assert_eq!(ids, [1]);
    let read = database
        .read(|sql| Ok(sql.query_row("SELECT body FROM drafts", [], |r| r.get::<_, String>(0))?))
        .await;
    assert!(matches!(read, Err(DbError::DamagedDatabase)), "{read:?}");
    let written = database
        .write(|sql| {
            sql.execute("UPDATE drafts SET body='short' WHERE id=1", [])?;
            Ok(())
        })
        .await;
    assert!(matches!(written, Err(DbError::DamagedDatabase)), "{written:?}");
    // The handle still serves what the damage doesn't reach.
    crate::write::tests::sql(&database, "INSERT INTO drafts VALUES(2, 'kept')").await.unwrap();
    database.close().await.unwrap();

    let reading = store.builder(notes_tables(), migrations()).open_read_only().await.unwrap();
    let read = reading
        .read(|sql| {
            Ok(sql.query("SELECT body FROM drafts ORDER BY id", [], |r| r.get::<_, String>(0))?)
        })
        .await;
    assert!(matches!(read, Err(DbError::DamagedDatabase)), "{read:?}");
    reading.close().await.unwrap();

    let operations = crate::discarded_operations(
        store.database_path(),
        coven_foundation::files::StoreDirAccess::bugs(&store.directory()),
    )
    .await
    .unwrap();
    assert!(operations.is_empty());
}

/// Damage the open's own statements meet fails the open the same way: here
/// the page every statement starts from, the root of SQLite's schema.
#[tokio::test]
async fn damage_the_open_meets_fails_the_open() {
    let store = TestStore::new();
    store.builder(notes_tables(), notes_migrations()).open().await.unwrap().close().await.unwrap();
    // The first page's b-tree header follows the 100-byte file header.
    let mut file = std::fs::OpenOptions::new().write(true).open(store.database_path()).unwrap();
    file.seek(SeekFrom::Start(100)).unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_all().unwrap();
    for read_only in [false, true] {
        let builder = store.builder(notes_tables(), notes_migrations());
        let opened = if read_only {
            builder.open_read_only().await.map(|_| ())
        } else {
            builder.open().await.map(|_| ())
        };
        assert!(
            matches!(opened, Err(crate::OpenError::Database(DbError::DamagedDatabase))),
            "read only {read_only}: {opened:?}"
        );
    }
}

/// A replaced database's journal is read in place and read-only: damage to a
/// table of the app's doesn't stop that, and a journal whose own page is
/// damaged is `DamagedDatabase`, which supplies no records (§19.3).
#[tokio::test]
async fn a_discarded_journal_is_read_past_damage_elsewhere_but_not_when_it_is_damaged() {
    let store = TestStore::new();
    let database = store.builder(notes_tables(), migrations()).open().await.unwrap();
    let body = "A".repeat(page_size(&store) * 4);
    database
        .write(move |sql| {
            sql.execute("INSERT INTO drafts VALUES(1, ?1)", [body])?;
            Ok(())
        })
        .await
        .unwrap();
    let owed = serde_json::to_vec("owed-account@example.org").unwrap();
    database
        .start_operation(crate::NewOperation {
            kind: crate::operation_kind::PROVIDER_ACCESS.into(),
            data: owed.clone(),
            started_by: crate::operation_kind::COVEN.into(),
            pauses_writes: false,
        })
        .await
        .unwrap();
    database.close().await.unwrap();

    damage_overflow(&store, b'A');
    let records = crate::discarded_operations(
        store.database_path(),
        coven_foundation::files::StoreDirAccess::bugs(&store.directory()),
    )
    .await
    .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].kind, crate::operation_kind::PROVIDER_ACCESS);
    assert_eq!(records[0].data, owed);

    // The page holding the journal's row: its b-tree page kind becomes one
    // SQLite refuses.
    let size = page_size(&store);
    let mut file =
        std::fs::OpenOptions::new().read(true).write(true).open(store.database_path()).unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    let needle = b"owed-account@example.org";
    let at = bytes
        .windows(needle.len())
        .position(|window| window == needle)
        .expect("the journal row is in the file");
    file.seek(SeekFrom::Start((at / size * size) as u64)).unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_all().unwrap();
    let damaged = crate::discarded_operations(
        store.database_path(),
        coven_foundation::files::StoreDirAccess::bugs(&store.directory()),
    )
    .await;
    assert!(matches!(damaged, Err(DbError::DamagedDatabase)), "{damaged:?}");
}

const ONE_NOTE: &str =
    "INSERT INTO notes(id,title) VALUES('00000000-0000-4000-8000-000000000001','Note')";

/// The notes `store`'s database holds, read through a fresh open.
async fn notes_after_opening(store: &TestStore) -> i64 {
    let database = store.builder(notes_tables(), notes_migrations()).open().await.unwrap();
    let notes = database
        .read(|sql| {
            Ok::<_, DbError>(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get(0))?)
        })
        .await
        .unwrap();
    database.close().await.unwrap();
    notes
}

/// The system refuses each of the store's upkeep threads in turn: the input
/// database's checkpointer, the main one's, the planner's statistics. The
/// store doesn't open (§4); what had started stops and lets go of the store,
/// so the next open finds it as it was.
#[tokio::test]
async fn a_refused_upkeep_thread_fails_the_open_and_the_store_opens_again() {
    let store = TestStore::new();
    let database = store.builder(notes_tables(), notes_migrations()).open().await.unwrap();
    crate::write::tests::sql(&database, ONE_NOTE).await.unwrap();
    database.close().await.unwrap();
    for started in 0..3 {
        let error = store
            .builder(notes_tables(), notes_migrations())
            .refuse_upkeep_thread_after(started)
            .open()
            .await
            .err()
            .unwrap_or_else(|| panic!("the thread after {started} is refused"));
        let error = crate::tests::database_error(error);
        assert!(matches!(error, DbError::UpkeepThread(_)), "{error:?}");
        assert_eq!(notes_after_opening(&store).await, 1);
    }
}

/// SQLite refuses to close every reader and the writer, which closes after
/// them. Closing tries each and reports each failure (E1). The store is
/// closed all the same: every later call says so, and it opens again.
#[tokio::test]
async fn a_connection_that_wont_close_fails_close_after_every_one_was_tried() {
    let store = TestStore::new();
    let database = store.builder(notes_tables(), notes_migrations()).open().await.unwrap();
    crate::write::tests::sql(&database, ONE_NOTE).await.unwrap();
    database.inspect_readers(|reader| reader.leave_statement_open());
    database.inspect_writer(|writer| writer.leave_statement_open());
    let error = database.close().await.unwrap_err();
    let DbError::Closing { failures } = error else { panic!("closing failed, not {error:?}") };
    assert_eq!(failures.len(), 5, "{failures:?}");
    assert!(
        failures.iter().all(|failure| matches!(
            failure,
            DbError::Sqlite(error)
                if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy)
        )),
        "{failures:?}"
    );
    assert!(matches!(database.close().await, Err(DbError::StoreClosed)));
    assert!(matches!(database.write(|_| Ok(())).await, Err(DbError::StoreClosed)));
    assert_eq!(notes_after_opening(&store).await, 1);
}
