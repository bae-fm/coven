use super::*;
use crate::tests::{TestDatabase, TestOptions};
use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;

/// The tables a changeset names.
fn tables(changeset: &[u8]) -> Vec<String> {
    let mut input = changeset;
    let input: &mut dyn std::io::Read = &mut input;
    let mut items = rusqlite::session::ChangesetIter::start_strm(&input).unwrap();
    let mut tables = Vec::new();
    while let Some(item) = items.next().unwrap() {
        tables.push(item.op().unwrap().table_name().to_owned());
    }
    tables.sort();
    tables.dedup();
    tables
}

/// The tables whose changes a captured transaction on `writer` records as
/// `sql` runs.
fn recorded<P: Policy>(writer: &mut HostSql<P>, sql: &str) -> Vec<String> {
    let mut transaction = WriteTransaction::begin(writer, false).unwrap();
    let supplemental = SupplementalChanges::observe(&mut transaction).unwrap();
    let changeset = transaction.within(|writer| {
        let mut capture = ChangeCapture::begin(writer, supplemental).unwrap();
        let _internal = writer.authorization.internal();
        writer.batch(sql).unwrap();
        capture.changeset().unwrap()
    });
    drop(transaction);
    tables(&changeset)
}

/// A captured transaction's session records `ANALYZE`'s changes to
/// `sqlite_stat1`, which SQLite's session keys by its own columns, unlike
/// the rowid sqlive expects of a table without a primary key; it doesn't
/// record `sqlite_stat4`. So sqlive runs `PRAGMA optimize` only outside a
/// captured transaction, and app writes can't analyze (§4).
#[test]
fn a_captured_transaction_records_analyzes_statistics() {
    let db = TestDatabase::open(
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL); CREATE INDEX notes_title ON notes(title)",
    );
    db.commit("INSERT INTO notes VALUES ('1','a'),('2','b')");
    db.host(|writer| {
        let _internal = writer.authorization.internal();
        writer.batch("ANALYZE notes").unwrap();
    });
    // The test adds rows, so analyzing again changes the statistics.
    db.commit("INSERT INTO notes VALUES ('3','c'),('4','c')");
    assert_eq!(db.host(|writer| recorded(writer, "ANALYZE notes")), ["sqlite_stat1"]);
    db.close();
}

/// A captured transaction records the tables a live query can read, and
/// none of the tables its handle marks as unrecorded, which no live query
/// reads (§4).
#[test]
fn a_captured_transaction_skips_tables_no_live_query_reads() {
    let db = TestDatabase::open_with(
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
         CREATE TABLE beside(id INTEGER PRIMARY KEY);
         CREATE TABLE pauses(id INTEGER PRIMARY KEY)",
        TestOptions { unrecorded: vec!["beside".to_owned()], ..TestOptions::default() },
    );
    let tables = db.host(|writer| {
        recorded(
            writer,
            "INSERT INTO notes VALUES ('1','a'); INSERT INTO beside VALUES (1); INSERT INTO pauses VALUES (1)",
        )
    });
    assert_eq!(tables, ["notes", "pauses"]);
    db.close();
}
