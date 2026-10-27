use super::*;
use crate::tests::TestStore;
use crate::{Migration, RowIdentity, SyncedTable};
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

/// A captured transaction's session records `ANALYZE`'s changes to
/// `sqlite_stat1`, which SQLite's session keys by its own columns, unlike
/// the rowid coven expects of a table without a primary key; it doesn't
/// record `sqlite_stat4`. So coven runs `PRAGMA optimize` only outside a
/// captured transaction, and app writes can't analyze (§4).
#[tokio::test]
async fn a_captured_transaction_records_analyzes_statistics() {
    let store = TestStore::new();
    let db = store
        .builder(
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            vec![Migration::addition(
                1,
                "notes",
                "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL); CREATE INDEX notes_title ON notes(title)",
            )],
        )
        .open()
        .await
        .unwrap();
    crate::write::tests::sql(&db, "INSERT INTO notes VALUES ('1','a'),('2','b')")
        .await
        .unwrap();
    db.inspect_writer(|writer| {
        let _internal = writer.authorization.internal();
        writer.batch("ANALYZE notes").unwrap();
    });
    // More rows, so analyzing again changes the statistics.
    crate::write::tests::sql(&db, "INSERT INTO notes VALUES ('3','c'),('4','c')")
        .await
        .unwrap();
    let recorded = db.inspect_writer(|writer| {
        writer.batch("BEGIN IMMEDIATE").unwrap();
        let mut capture =
            ChangeCapture::begin(writer, writer.observation.as_ref().unwrap()).unwrap();
        let changeset = {
            let _internal = writer.authorization.internal();
            writer.batch("ANALYZE notes").unwrap();
            capture.changeset().unwrap()
        };
        drop(capture);
        writer.batch("ROLLBACK").unwrap();
        tables(&changeset)
    });
    assert_eq!(recorded, ["sqlite_stat1"]);
    db.close().await.unwrap();
}
