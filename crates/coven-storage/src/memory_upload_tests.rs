use super::*;
use serde_json::{json, Value};

async fn floor_bytes(storage: &MemoryStorage, path: &ObjectPath) -> Option<Vec<u8>> {
    match storage.status(path).await.unwrap() {
        None => None,
        Some(_) => Some(storage.read(path).await.unwrap()),
    }
}

async fn execute(initial: Option<Vec<u8>>, total: usize, winner: usize) -> (Value, Value) {
    let storage = MemoryStorage::builder().transfer_limits(4, 4).build().unwrap();
    let path = ObjectPath::floor(STORE_ID);
    if let Some(bytes) = &initial {
        storage.create(&path, bytes).await.unwrap();
    }
    let target = match storage.status(&path).await.unwrap() {
        None => UploadTarget::Create(path.clone()),
        Some(previous) => UploadTarget::Replace(previous),
    };
    let mut sessions = Vec::new();
    for _ in 0..2 {
        sessions.push(match &target {
            UploadTarget::Create(path) => storage.begin_upload(path, total as u64).await.unwrap(),
            UploadTarget::Replace(previous) => {
                storage.begin_replace(previous, total as u64).await.unwrap()
            }
        });
    }
    let mut ops = Vec::new();
    // All incomplete completions must preserve the previous floor. Staging
    // both contenders first also prevents a begin-time check from hiding CAS.
    for index in [0, 1] {
        ops.push(json!({"op":"publish", "session":index}));
        for (part_index, part) in vec![index as u8 + 1; total].chunks(4).enumerate() {
            ops.push(json!({"op":"part", "session":index, "bytes":part}));
            if (part_index + 1) * 4 < total {
                ops.push(json!({"op":"publish", "session":index}));
            }
        }
    }
    ops.push(json!({"op":"publish", "session":winner}));
    ops.push(json!({"op":"publish", "session":1-winner}));
    let input = json!({"initial":initial, "totals":[total,total], "ops":ops});
    let mut results = Vec::new();
    for op in &ops {
        let index = op["session"].as_u64().unwrap() as usize;
        let session = &mut sessions[index];
        *session = UploadSession::decode(session.encode().unwrap().as_bytes()).unwrap();
        storage.resume_upload(session).await.unwrap();
        assert_eq!(session.target(), &target);
        let result = if op["op"] == "part" {
            let bytes: Vec<u8> = serde_json::from_value(op["bytes"].clone()).unwrap();
            storage.upload_part(session, &bytes).await.unwrap();
            "received"
        } else {
            match storage.finish_upload(session).await {
                Ok(()) => "stored",
                Err(error) if error.failure() == StorageFailure::InvalidPart => "incomplete",
                Err(error)
                    if matches!(
                        error.failure(),
                        StorageFailure::AlreadyExists | StorageFailure::Conflict
                    ) =>
                {
                    "conflict"
                }
                Err(error) => panic!("{error:?}"),
            }
        };
        results.push(json!({"result":result, "bytes":floor_bytes(&storage, &path).await}));
    }
    assert_eq!(storage.read(&path).await.unwrap(), vec![winner as u8 + 1; total]);
    for session in sessions {
        storage.abort_upload(&session).await.unwrap();
    }
    assert!(storage.provider.state.lock().await.uploads.is_empty());
    (input, json!(results))
}

#[tokio::test]
async fn conditional_parts_publish_whole_bytes_once_across_recording_boundaries() {
    for initial in [None, Some(b"old".to_vec())] {
        for total in [1, 4, 5, 8, 9] {
            for winner in [0, 1] {
                execute(initial.clone(), total, winner).await;
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires COVEN_FLOOR_UPLOAD_LEAN"]
async fn conditional_floor_upload_matches_lean() {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let runner = std::env::var("COVEN_FLOOR_UPLOAD_LEAN").expect("floorUploadRunner executable");
    for initial in [None, Some(b"old".to_vec())] {
        for total in [1, 4, 5, 8, 9] {
            for winner in [0, 1] {
                let (input, actual) = execute(initial.clone(), total, winner).await;
                let mut child = Command::new(&runner)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap();
                child.stdin.take().unwrap().write_all(input.to_string().as_bytes()).unwrap();
                let output = child.wait_with_output().unwrap();
                assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
                let expected: Value = serde_json::from_slice(&output.stdout).unwrap();
                assert_eq!(actual, expected, "{input}");
            }
        }
    }
}

#[tokio::test]
async fn a_replacement_restart_keeps_the_original_revision_after_expiry() {
    let storage = MemoryStorage::builder().transfer_limits(4, 4).build().unwrap();
    let path = ObjectPath::floor(STORE_ID);
    storage.create(&path, b"old").await.unwrap();
    let previous = storage.status(&path).await.unwrap().unwrap();
    let session = storage.begin_replace(&previous, 5).await.unwrap();
    storage.set_faults(Faults { expire_uploads: true, ..Faults::none() }).await;
    storage.replace(&previous, b"next").await.unwrap();
    let error = storage.restart_upload(&session).await.err().unwrap();
    assert_eq!(error.failure(), StorageFailure::Conflict);
    assert_eq!(storage.read(&path).await.unwrap(), b"next");
}

/// Ana's laptop ends the uploads her phone left: the sessions the phone's
/// connection began in the store, and neither the laptop's own nor one of
/// another store's. Ending them again succeeds, and the phone's session can
/// never publish (§17.10).
#[tokio::test]
async fn another_device_ends_the_uploads_a_device_left() {
    use coven_foundation::id_source::{DeviceId, ObjectId, StoreId};
    let storage = MemoryStorage::builder().build().unwrap();
    let (phone, laptop) = (DeviceId(7), DeviceId(8));
    let file = |store, device: DeviceId| {
        ObjectPath::file(store, device, ObjectId(uuid::Uuid::from_u128(u128::from(device.0))))
    };
    let elsewhere = StoreId(uuid::Uuid::from_u128(3));
    let phones = storage.with_device(phone);
    let mut left = phones.started_upload(&file(STORE_ID, phone)).await.unwrap();
    phones.started_upload(&file(elsewhere, phone)).await.unwrap();
    let laptops = storage.with_device(laptop);
    laptops.started_upload(&file(STORE_ID, laptop)).await.unwrap();
    for _ in 0..2 {
        laptops.end_device_uploads(STORE_ID, phone).await.unwrap();
        assert_eq!(
            storage.unfinished_uploads().await,
            vec![(phone, file(elsewhere, phone)), (laptop, file(STORE_ID, laptop))]
        );
    }
    assert_eq!(
        phones.resume_upload(&mut left).await.unwrap_err().failure(),
        StorageFailure::SessionExpired
    );
}

/// A session at a slot this device has applied can never publish, whoever
/// began it, and is ended; one at a later slot, at a file's path or in
/// another store stays (§17.10).
#[tokio::test]
async fn sessions_at_applied_slots_are_ended_whoever_began_them() {
    use coven_foundation::id_source::{DeviceId, ObjectId, StoreId};
    let storage = MemoryStorage::builder().build().unwrap();
    let slot = |store, number| ObjectPath::slot(store, std::num::NonZeroU64::new(number).unwrap());
    let elsewhere = StoreId(uuid::Uuid::from_u128(3));
    let file = ObjectPath::file(STORE_ID, DeviceId(7), ObjectId(uuid::Uuid::from_u128(1)));
    for path in
        [slot(STORE_ID, 1), slot(STORE_ID, 2), slot(STORE_ID, 3), slot(elsewhere, 1), file.clone()]
    {
        storage.with_device(DeviceId(7)).started_upload(&path).await.unwrap();
    }
    storage.end_settled_claims(STORE_ID, 2).await.unwrap();
    let left: Vec<_> =
        storage.unfinished_uploads().await.into_iter().map(|(_, path)| path).collect();
    assert_eq!(left, [slot(STORE_ID, 3), slot(elsewhere, 1), file]);
}

/// A floor session begun before the current floor was published observed an
/// older revision and can never publish, so it is ended; one begun in the
/// floor's second or later stays (Appendix E.10).
#[tokio::test]
async fn floor_sessions_begun_before_the_floor_are_ended() {
    use coven_foundation::clock::{Clock, FixedClock};
    let clock =
        Arc::new(FixedClock::new(std::time::UNIX_EPOCH + std::time::Duration::from_secs(100)));
    let storage = MemoryStorage::builder().clock(clock.clone()).build().unwrap();
    let floor = ObjectPath::floor(STORE_ID);
    storage.started_upload(&floor).await.unwrap();
    clock.set(clock.now() + std::time::Duration::from_secs(10));
    storage.create(&floor, b"floor").await.unwrap();
    let current = storage.status(&floor).await.unwrap().unwrap();
    let mut replacement = storage.begin_replace(&current, 4).await.unwrap();
    storage.upload_part(&mut replacement, b"data").await.unwrap();
    storage.end_superseded_floor_sessions(STORE_ID).await.unwrap();
    assert_eq!(storage.unfinished_uploads().await.len(), 1);
    storage.end_superseded_floor_sessions(STORE_ID).await.unwrap();
    assert_eq!(storage.unfinished_uploads().await.len(), 1);
}
