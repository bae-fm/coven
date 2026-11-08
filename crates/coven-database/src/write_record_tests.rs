use super::*;
use crate::tests::TestStore;
use crate::write::tests::{notes, NOTES};
use coven_merge::Timestamp;
use std::time::UNIX_EPOCH;

#[tokio::test]
async fn exhausted_write_numbers_return_an_error_before_timestamp_exhaustion() {
    let store = TestStore::new();
    let database = store.schema(notes(), NOTES).await.unwrap();
    database.inspect_writer(|db| {
        let device = DeviceId(41);
        // Starting at timestamp zero permits one more timestamp than the
        // positive write-number representation has identities.
        let latest = Timestamp::new(Timestamp::MAX_MILLISECONDS, u16::MAX - 1, device).unwrap();
        let timestamp = coven_format::merge_fields::encode_timestamp(&latest).unwrap();
        crate::write_commit::tests::record_write(db, &timestamp, u64::MAX, 0, None);
        let error = changed(db, device, UNIX_EPOCH, BTreeMap::new()).unwrap_err();
        assert!(matches!(error, DbError::WriteNumberExhausted));
    });
    database.close().await.unwrap();
}

/// Finding the rows a write moves reads only rows that were shown before
/// it: an inserted row's shown values are read once, for its change, and
/// its generation and stored audiences once.
#[tokio::test]
async fn an_inserted_row_is_read_once() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![
                crate::SyncedTable::new("parents", crate::RowIdentity::SharedKey),
                crate::SyncedTable::new("children", crate::RowIdentity::SharedKey),
            ],
            "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY);
             CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,
                 parent TEXT NOT NULL REFERENCES parents(id) ON DELETE CASCADE)",
        )
        .await
        .unwrap();
    db.write(|sql| {
        sql.execute("INSERT INTO parents VALUES('p')", [])?;
        for i in 0..20 {
            sql.execute("INSERT INTO children VALUES(?1,'p')", [format!("c{i}")])?;
        }
        Ok(())
    })
    .await
    .unwrap();
    let statements = db.inspect_writer(|writer| writer.profiled_statements());
    let reads = statements
        .iter()
        .filter(|s| s.sql.starts_with("SELECT \"id\",\"parent\" FROM main.\"children\""))
        .count();
    assert_eq!(reads, 20);
    // Its stored generations are read once too, by the merge store that its
    // operations, its record and its merge share.
    let generations = statements
        .iter()
        .filter(|s| s.sql.starts_with("SELECT r.id,r.generation,w.id,w.timestamp,w.number,w.read_through,w.slot FROM _coven_rows r"))
        .count();
    assert_eq!(generations, 21);
    // And the audiences its keys are stored in, once for every key the write
    // changed together, for the merge's views before and after the write, and
    // by no key's own statement.
    let audiences = statements
        .iter()
        .filter(|s| {
            s.sql.starts_with("WITH k(t,k) AS (VALUES (?,?)")
                && s.sql.contains("FROM k JOIN _coven_rows r")
        })
        .count();
    assert_eq!(audiences, 1);
    let one_key_at_a_time = statements
        .iter()
        .filter(|s| s.sql.starts_with("SELECT DISTINCT table_name,key,audience FROM _coven_rows"))
        .count();
    assert_eq!(one_key_at_a_time, 0);
    db.close().await.unwrap();
}

/// A write's rows are recorded on statements SQLite prepares once a write,
/// not once a row: binding each row's table, key and audience to coven's
/// cached reads of its stored generations and losses never makes SQLite
/// parse and plan them again, so a write of twenty rows prepares its
/// statements again exactly as often as one of five.
#[tokio::test]
async fn recording_a_writes_rows_prepares_no_statement_again_per_row() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![crate::SyncedTable::new("parents", crate::RowIdentity::SharedKey)],
            "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY)",
        )
        .await
        .unwrap();
    let mut prepared_again = Vec::new();
    for rows in [0..1, 1..6, 6..26] {
        db.write(move |sql| {
            for i in rows {
                sql.execute("INSERT INTO parents VALUES(?1)", [format!("p{i}")])?;
            }
            Ok(())
        })
        .await
        .unwrap();
        prepared_again.push(reprepares(&db.inspect_writer(|writer| writer.profiled_statements())));
    }
    assert_eq!(prepared_again[2], prepared_again[1]);
    db.close().await.unwrap();
}

/// How many times SQLite prepared each statement again during one write.
fn reprepares(statements: &[crate::sqlite::tests::ProfiledStatement]) -> BTreeMap<String, i32> {
    let mut first = BTreeMap::new();
    let mut again = BTreeMap::new();
    for statement in statements {
        let at = *first.entry(statement.sql.clone()).or_insert(statement.reprepares);
        if statement.reprepares > at {
            again.insert(statement.sql.clone(), statement.reprepares - at);
        }
    }
    again
}

/// A reference already of the type its parent's key column keeps names the
/// parent as it is, so it isn't staged for SQLite to convert.
#[tokio::test]
async fn a_reference_of_its_parents_type_is_not_staged() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![
                crate::SyncedTable::new("parents", crate::RowIdentity::SharedKey),
                crate::SyncedTable::new("children", crate::RowIdentity::SharedKey),
            ],
            "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY);
             CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,
                 parent TEXT NOT NULL REFERENCES parents(id) ON DELETE CASCADE)",
        )
        .await
        .unwrap();
    db.write(|sql| {
        sql.execute("INSERT INTO parents VALUES('p')", [])?;
        for i in 0..20 {
            sql.execute("INSERT INTO children VALUES(?1,'p')", [format!("c{i}")])?;
        }
        Ok(())
    })
    .await
    .unwrap();
    let statements = db.inspect_writer(|writer| writer.profiled_statements());
    let staged = statements.iter().filter(|s| s.sql.contains("_coven_affinity_")).count();
    assert_eq!(staged, 0);
    db.close().await.unwrap();
}
