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
    let steps = db.inspect_writer(|writer| {
        writer.batch("BEGIN").unwrap();
        writer.start_profile();
        writer.app_batch(&sql).unwrap();
        writer.end_profile();
        writer.batch("ROLLBACK").unwrap();
        writer
            .profiled_statements()
            .iter()
            .filter(|statement| statement.sql.contains("pragma_table_list"))
            .map(|statement| i64::from(statement.vm_steps))
            .sum::<i64>()
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
