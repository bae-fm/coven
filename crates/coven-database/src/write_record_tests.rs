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
