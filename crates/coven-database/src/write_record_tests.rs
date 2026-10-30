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
            params![
                timestamp,
                u64::MAX.to_be_bytes().as_slice(),
                0_u64.to_be_bytes().as_slice()
            ],
        )
        .unwrap();
        let error = changed(db, device, UNIX_EPOCH, BTreeMap::new()).unwrap_err();
        assert!(matches!(error, DbError::WriteNumberExhausted));
    });
    database.close().await.unwrap();
}

/// Finding the rows a write moves reads only rows that were shown before
/// it: an inserted row's shown values are read once, for its change.
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
        .filter(|s| {
            s.sql
                .starts_with("SELECT \"id\",\"parent\" FROM main.\"children\"")
        })
        .count();
    assert_eq!(reads, 20);
    db.close().await.unwrap();
}
