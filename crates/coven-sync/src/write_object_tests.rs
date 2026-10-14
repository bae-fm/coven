use crate::device_log_sync::tests::{device_of, group, rows, sql, storage};
use crate::store_log_sync::tests::Device;
use coven_crypto::MemberIdentity;
use coven_database::{EntryOutcome, StoreLog};
use coven_format::store_log::{MemberRole, StoreChange};
use coven_format::value::EntryId;
use coven_foundation::id_source::DeviceId;
use coven_storage::test_utils::MemoryStorage;
use std::sync::Arc;

/// Ben's phone, device 3, registers itself.
const ADDITION: EntryId = EntryId {
    device: DeviceId(3),
    number: 1,
};

fn applied() -> [(String, String, String); 1] {
    [("one".into(), "from ben".into(), "body".into())]
}

/// `ana` adds Ben; his phone registers and uploads one write.
async fn phone_writes(storage: &Arc<MemoryStorage>, ana: &mut Device) -> MemberIdentity {
    let ben = crate::store_log_sync::tests::member(4);
    ana.sync
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
    sql(
        &phone.db,
        "INSERT INTO notes VALUES('one','from ben','body')",
    )
    .await;
    assert_eq!(phone.writes.upload_writes().await.unwrap().len(), 1);
    ben
}

/// `ana` removes Ben without having read his phone's addition. The removal is
/// concurrent with that addition and drops it (§9).
async fn remove(ana: &mut Device, ben: &MemberIdentity) {
    ana.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: ben.member(),
        })
        .await
        .unwrap();
}

async fn addition_dropped(reader: &Device) {
    let log: StoreLog = reader.db.local_store_log().await.unwrap().log;
    assert!(matches!(
        log.replay.entries[&ADDITION],
        EntryOutcome::Dropped(_)
    ));
    assert!(!log.replay.state.devices.contains_key(&DeviceId(3)));
}

/// A write passes the authority check in its author's recorded view, so it
/// still counts after a later entry drops its device's addition (§7.1).
#[tokio::test]
async fn a_write_counts_after_a_later_entry_drops_its_devices_addition() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    let ben = phone_writes(&storage, &mut devices[0]).await;
    let reader = &mut devices[1];
    reader.sync.sync_store_log().await.unwrap();
    reader.writes.download_writes().await.unwrap();
    assert_eq!(rows(&reader.db).await, applied());

    remove(&mut devices[0], &ben).await;
    let reader = &mut devices[1];
    reader.sync.sync_store_log().await.unwrap();
    addition_dropped(reader).await;
    reader.writes.download_writes().await.unwrap();
    assert_eq!(rows(&reader.db).await, applied());
    assert!(reader
        .db
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .into_iter()
        .all(|record| record.as_log_refusal().is_none()));
}

/// Every reader downloads that write in either arrival order. Device 2 reads
/// it before the removal; device 1, which removed Ben, receives the phone's
/// addition only once replay drops it. Write discovery follows every device a
/// received entry registers, kept or dropped (§6,
/// `Writers.counting_write_discovered`).
#[tokio::test]
async fn every_reader_downloads_a_write_whose_devices_addition_dropped() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    let ben = phone_writes(&storage, &mut devices[0]).await;
    let early = &mut devices[1];
    early.sync.sync_store_log().await.unwrap();
    early.writes.download_writes().await.unwrap();
    assert_eq!(rows(&early.db).await, applied());

    remove(&mut devices[0], &ben).await;
    for reader in &mut devices {
        reader.sync.sync_store_log().await.unwrap();
        addition_dropped(reader).await;
        reader.writes.download_writes().await.unwrap();
        assert_eq!(rows(&reader.db).await, applied());
    }
    let mut positions = Vec::new();
    for reader in &devices {
        positions.push(reader.db.sync_state(Vec::new()).await.unwrap().positions);
    }
    assert_eq!(positions[0], positions[1]);
    assert!(positions[0].covers(coven_merge::WriteId {
        device: DeviceId(3),
        number: 1,
    }));
}

/// The writers `reader`'s write discovery probes, and the IO case projecting
/// its received entries and remembered folders.
async fn probed_writers(
    storage: &MemoryStorage,
    reader: &mut Device,
    own: DeviceId,
) -> (serde_json::Value, Vec<u64>) {
    use coven_storage::test_utils::MemoryRequest;
    let log: StoreLog = reader.db.local_store_log().await.unwrap().log;
    let entries: Vec<_> = log
        .entries
        .iter()
        .map(|applied| {
            let registers = match &applied.entry.change {
                StoreChange::AddDevice {
                    device, replaces, ..
                } => {
                    assert!(replaces.is_none(), "replacement is outside this case");
                    Some(device.0)
                }
                _ => None,
            };
            serde_json::json!({"writer": applied.entry.position.device.0, "registers": registers})
        })
        .collect();
    let listed: Vec<_> = reader
        .db
        .log_downloads(coven_database::LogKind::Entry)
        .await
        .unwrap()
        .into_iter()
        .map(|(writer, _)| writer.0)
        .collect();
    let before = storage.request_trace().len();
    reader.writes.download_writes().await.unwrap();
    let mut probed: Vec<_> = storage.request_trace()[before..]
        .iter()
        .filter_map(|request| match request {
            MemoryRequest::Status(path) => path.write_id().map(|write| write.device.0),
            _ => None,
        })
        .collect();
    probed.sort_unstable();
    probed.dedup();
    let case = serde_json::json!({"check": "writers", "entries": entries, "listed": listed,
        "drained": [], "self": own.0});
    (case, probed)
}

/// IO `Writers.known`: in both arrival orders, the write logs each reader
/// actually probes are the model's known writers less itself, including the
/// phone whose addition its replay dropped.
#[test]
#[ignore = "requires COVEN_IO_RUNNER"]
fn io_differential_dropped_registration() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (cases, results): (Vec<_>, Vec<_>) = runtime.block_on(async {
        let storage = storage();
        let mut devices = group(storage.clone(), 2).await;
        let ben = phone_writes(&storage, &mut devices[0]).await;
        let mut observed = Vec::new();
        devices[1].sync.sync_store_log().await.unwrap();
        observed.push(probed_writers(&storage, &mut devices[1], DeviceId(2)).await);
        remove(&mut devices[0], &ben).await;
        for (index, reader) in devices.iter_mut().enumerate() {
            reader.sync.sync_store_log().await.unwrap();
            addition_dropped(reader).await;
            let own = DeviceId(index as u64 + 1);
            observed.push(probed_writers(&storage, reader, own).await);
        }
        observed
            .into_iter()
            .map(|(case, probed)| (case, serde_json::json!(probed)))
            .unzip()
    });
    assert_eq!(results, crate::discovery::tests::io_runner(&cases));
}
