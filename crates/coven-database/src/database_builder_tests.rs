use crate::test_utils::{notes_migrations, notes_tables};
use crate::tests::TestStore;
use crate::{CovenError, DbError, Migration};
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
        .read(|sql| Ok(sql.query("SELECT id FROM drafts", [], |r| r.get::<_, i64>(0))?))
        .await
        .unwrap();
    assert_eq!(ids, [1]);
    let read = database
        .read(|sql| Ok(sql.query_row("SELECT body FROM drafts", [], |r| r.get::<_, String>(0))?))
        .await;
    assert!(matches!(read, Err(CovenError::Database(DbError::DamagedDatabase))), "{read:?}");
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
    assert!(matches!(read, Err(CovenError::Database(DbError::DamagedDatabase))), "{read:?}");
    reading.close().await.unwrap();

    let operations = crate::discarded_operations(store.database_path()).await.unwrap();
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
            matches!(opened, Err(CovenError::Database(DbError::DamagedDatabase))),
            "read only {read_only}: {opened:?}"
        );
    }
}
