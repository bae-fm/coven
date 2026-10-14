use crate::device_log_sync::tests::{device_of, group, rows, sql, storage};
use coven_database::{EntryOutcome, StoreLog};
use coven_format::store_log::{MemberRole, StoreChange};
use coven_format::value::EntryId;
use coven_foundation::id_source::DeviceId;

/// A write passes the authority check in its author's recorded view, so it
/// still counts after a later entry drops its device's addition (§7.1).
#[tokio::test]
async fn a_write_counts_after_a_later_entry_drops_its_devices_addition() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    let ben = crate::store_log_sync::tests::member(4);
    devices[0]
        .sync
        .make_and_upload_entry(StoreChange::AddMember {
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "ben-key".into(),
                principal: coven_format::S3Principal::Console,
            },
            member: ben.member(),
            keys: ben.keys().public_keys(),
            role: MemberRole::Member,
        })
        .await
        .unwrap();
    let mut phone = device_of(storage.clone(), 3, ben.clone()).await;
    phone.sync.sync_store_log().await.unwrap();
    phone
        .sync
        .load_new_device("Ben's phone", None)
        .await
        .unwrap();
    let addition = EntryId {
        device: DeviceId(3),
        number: 1,
    };
    sql(
        &phone.db,
        "INSERT INTO notes VALUES('one','from ben','body')",
    )
    .await;
    assert_eq!(phone.writes.upload_writes().await.unwrap().len(), 1);
    let reader = &mut devices[1];
    reader.sync.sync_store_log().await.unwrap();
    reader.writes.download_writes().await.unwrap();
    let applied = [("one".into(), "from ben".into(), "body".into())];
    assert_eq!(rows(&reader.db).await, applied);

    // Ana removes Ben without having read his phone's addition. The removal
    // is concurrent with that addition and drops it (§9).
    devices[0]
        .sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: ben.member(),
        })
        .await
        .unwrap();
    let reader = &mut devices[1];
    reader.sync.sync_store_log().await.unwrap();
    let log: StoreLog = reader.db.local_store_log().await.unwrap().log;
    assert!(matches!(
        log.replay.entries[&addition],
        EntryOutcome::Dropped(_)
    ));
    assert!(!log.replay.state.devices.contains_key(&DeviceId(3)));
    reader.writes.download_writes().await.unwrap();
    assert_eq!(rows(&reader.db).await, applied);
    assert!(reader
        .db
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .into_iter()
        .all(|record| record.as_log_refusal().is_none()));
}
