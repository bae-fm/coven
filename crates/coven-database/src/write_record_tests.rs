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
        db.internal_execute(
            "INSERT INTO _coven_writes(timestamp,number,read_through) VALUES(?1,?2,?3)",
            params![timestamp, u64::MAX.to_be_bytes().as_slice(), 0_u64.to_be_bytes().as_slice()],
        )
        .unwrap();
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
    // Its stored generation is read once too, as its operations are
    // recorded; its change takes it from the row's stored state.
    let generations = statements
        .iter()
        .filter(|s| s.sql.starts_with("SELECT max(generation) FROM _coven_rows"))
        .count();
    assert_eq!(generations, 21);
    // And the audiences its key is stored in, once for the merge's views
    // before and after the write.
    let audiences = statements
        .iter()
        .filter(|s| s.sql.starts_with("SELECT DISTINCT table_name,key,audience FROM _coven_rows"))
        .count();
    assert_eq!(audiences, 21);
    db.close().await.unwrap();
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
