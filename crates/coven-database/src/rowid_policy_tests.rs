use crate::test_utils::{notes_migrations, notes_tables};
use crate::tests::TestStore;

/// The steps of reading which tables' rowids are hidden, while one batch of
/// app SQL creates `tables` tables, each with an index.
async fn reading_steps(tables: usize) -> i64 {
    let store = TestStore::new();
    let db = store.builder(notes_tables(), notes_migrations()).open().await.unwrap();
    let sql: String = (0..tables)
        .map(|i| {
            format!("CREATE TABLE t{i}(id TEXT PRIMARY KEY, v); CREATE INDEX t{i}_v ON t{i}(v);")
        })
        .collect();
    let steps = db.inspect_writer(|writer, _| {
        writer.batch("BEGIN").unwrap();
        writer.with_profile(|writer| writer.app_batch(&sql)).unwrap();
        writer.batch("ROLLBACK").unwrap();
        // The connection keeps the read prepared, and SQLite counts its
        // steps from its preparation: the reads in the batch took what the
        // count grew by after the first.
        let counts: Vec<i64> = writer
            .profiled_statements()
            .iter()
            .filter(|statement| statement.sql.contains("pragma_table_list"))
            .map(|statement| i64::from(statement.vm_steps))
            .collect();
        counts.last().zip(counts.first()).map_or(0, |(last, first)| last - first)
    });
    db.close().await.unwrap();
    steps
}

/// A table app SQL creates is checked once, not again after each later
/// statement: bae's schema of 173 tables spent half its open re-reading
/// every table after each one.
#[tokio::test]
async fn creating_tables_reads_each_ones_rowid_once() {
    let (hundred, two_hundred) = (reading_steps(100).await, reading_steps(200).await);
    assert!(hundred > 0, "the batch must read rowids");
    assert!(two_hundred < 3 * hundred, "{hundred} steps for 100 tables, {two_hundred} for 200");
}

/// Changing `temp_store` drops temporary objects, so the app's SQLite
/// settings apply before coven's temporary rowid guards, which every
/// connection keeps, a read-only store's too (§4). The store reports the
/// settings in effect.
#[tokio::test]
async fn every_connection_keeps_its_rowid_guards_under_the_apps_settings() {
    let store = TestStore::new();
    let mut migrations = notes_migrations();
    // A local table whose declared `rowid` column hides its address.
    migrations.push(crate::Migration::addition(
        2,
        "shadowed",
        "CREATE TABLE shadowed(id TEXT NOT NULL PRIMARY KEY, rowid INTEGER)",
    ));
    let settings = || crate::SqliteSettings {
        temp_store: Some(crate::TempStore::Memory),
        ..crate::SqliteSettings::default()
    };
    let guards = |connection: &crate::sqlite::StoreSql| {
        let guards: i64 = connection
            .query_row(
                "SELECT count(*) FROM temp.sqlite_schema WHERE name GLOB '_coven_rowid_guard_*'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(guards > 0);
    };
    let db = store
        .builder(notes_tables(), migrations.clone())
        .sqlite_settings(settings())
        .open()
        .await
        .unwrap();
    assert_eq!(db.sqlite_settings().await.unwrap().temp_store, crate::TempStore::Memory);
    db.inspect_writer(|writer, _| guards(writer));
    db.inspect_readers(|reader, _| guards(reader));
    let reading = store
        .builder(notes_tables(), migrations)
        .sqlite_settings(settings())
        .open_read_only()
        .await
        .unwrap();
    assert_eq!(reading.sqlite_settings().await.unwrap().temp_store, crate::TempStore::Memory);
    reading.inspect_writer(|writer, _| guards(writer));
    reading.inspect_readers(|reader, _| guards(reader));
    reading.close().await.unwrap();
    db.close().await.unwrap();
}
