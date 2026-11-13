use super::*;
use serde_json::{json, Value};

async fn floor_bytes(storage: &MemoryStorage, path: &ObjectPath) -> Option<Vec<u8>> {
    match storage.status(path).await.unwrap() {
        None => None,
        Some(_) => Some(storage.read(path).await.unwrap()),
    }
}

/// Two writers that observed the same state of the floor each try to store
/// their bytes: the first is stored and the second is refused, whatever it
/// carries.
async fn execute(initial: Option<Vec<u8>>, writes: &[Vec<u8>]) -> (Value, Value) {
    let storage = MemoryStorage::builder().build().unwrap();
    let path = ObjectPath::floor(STORE_ID);
    if let Some(bytes) = &initial {
        storage.create(&path, bytes).await.unwrap();
    }
    let previous = storage.status(&path).await.unwrap();
    let ops: Vec<Value> = writes.iter().map(|bytes| json!({"bytes": bytes})).collect();
    let input = json!({"initial": initial, "ops": ops});
    let mut results = Vec::new();
    for bytes in writes {
        let stored = match &previous {
            Some(previous) => storage.replace(previous, bytes).await,
            None => storage.create(&path, bytes).await.map(drop),
        };
        let result = match stored {
            Ok(()) => "stored",
            Err(error)
                if matches!(
                    error.failure(),
                    StorageFailure::AlreadyExists | StorageFailure::Conflict
                ) =>
            {
                "conflict"
            }
            Err(error) => panic!("{error:?}"),
        };
        results.push(json!({"result": result, "bytes": floor_bytes(&storage, &path).await}));
    }
    assert_eq!(storage.read(&path).await.unwrap(), writes[0]);
    (input, json!(results))
}

fn floor_writes() -> Vec<Vec<u8>> {
    vec![vec![1], vec![2, 2], vec![3, 3, 3]]
}

#[tokio::test]
async fn the_floor_pointer_is_stored_once_per_observed_state() {
    for initial in [None, Some(b"old".to_vec())] {
        execute(initial, &floor_writes()).await;
    }
}

#[tokio::test]
#[ignore = "requires COVEN_FLOOR_UPLOAD_LEAN"]
async fn conditional_floor_upload_matches_lean() {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let runner = std::env::var("COVEN_FLOOR_UPLOAD_LEAN").expect("floorUploadRunner executable");
    for initial in [None, Some(b"old".to_vec())] {
        let (input, actual) = execute(initial, &floor_writes()).await;
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

/// One floor pointer case driven through the memory provider: body creates
/// (and delayed creates at a used name), conditional pointer replacements and
/// deletes of bodies. Each operation here is one the model would take.
async fn pointer_case(ops: &[Value]) -> (Value, Value) {
    use coven_foundation::id_source::DeviceId;
    let storage = MemoryStorage::builder().build().unwrap();
    let floor = ObjectPath::floor(STORE_ID);
    let body = |owner: u64, number: u64| {
        ObjectPath::floor_body(STORE_ID, DeviceId(owner), number.try_into().unwrap())
    };
    let named = |op: &Value| -> (u64, u64) {
        (op["owner"].as_u64().unwrap(), op["number"].as_u64().unwrap())
    };
    let mut observed: Vec<Option<StoredObject>> = vec![None];
    let mut pointer: Option<(u64, u64)> = None;
    let mut journaled: Vec<(u64, u64)> = Vec::new();
    let mut used: Vec<(u64, u64)> = Vec::new();
    let mut gone: Vec<u64> = Vec::new();
    let mut results = Vec::new();
    for op in ops {
        match op["op"].as_str().unwrap() {
            "upload" => {
                let (owner, number) = named(op);
                if !used.contains(&(owner, number)) {
                    storage.create(&body(owner, number), b"body").await.unwrap();
                    used.push((owner, number));
                    journaled.push((owner, number));
                }
            }
            "late_create" => {
                let (owner, number) = named(op);
                if used.contains(&(owner, number)) {
                    // Create-once: the delayed create lands only on an empty name.
                    match storage.create(&body(owner, number), b"body").await {
                        Ok(_) => (),
                        Err(error) => assert_eq!(error.failure(), StorageFailure::AlreadyExists),
                    }
                }
            }
            "replace" => {
                let (owner, number) = named(op);
                let at = op["observed"].as_u64().unwrap() as usize;
                let exists = storage.status(&body(owner, number)).await.unwrap().is_some();
                if journaled.contains(&(owner, number)) && exists && at == observed.len() - 1 {
                    let bytes = [owner as u8, number as u8];
                    match &observed[at] {
                        Some(previous) => storage.replace(previous, &bytes).await.unwrap(),
                        None => drop(storage.create(&floor, &bytes).await.unwrap()),
                    }
                    pointer = Some((owner, number));
                    observed.push(storage.status(&floor).await.unwrap());
                }
            }
            "settle" => {
                let name = named(op);
                journaled.retain(|kept| *kept != name);
            }
            "delete_own" => {
                let name = named(op);
                if pointer != Some(name) && !journaled.contains(&name) {
                    storage.delete(&body(name.0, name.1)).await.unwrap();
                }
            }
            "delete_gone" => {
                let name = named(op);
                if gone.contains(&name.0) && pointer != Some(name) {
                    storage.delete(&body(name.0, name.1)).await.unwrap();
                }
            }
            "gone" => gone.push(op["device"].as_u64().unwrap()),
            other => panic!("unknown operation {other}"),
        }
        let mut bodies = Vec::new();
        for &(owner, number) in &used {
            if storage.status(&body(owner, number)).await.unwrap().is_some() {
                bodies.push(json!([owner, number]));
            }
        }
        bodies.sort_by_key(|name| (name[0].as_u64(), name[1].as_u64()));
        results.push(json!({
            "bodies": bodies,
            "pointer": pointer.map(|(owner, number)| json!([owner, number])),
            "revision": observed.len() - 1,
        }));
    }
    (json!({"ops": ops}), json!(results))
}

fn pointer_cases() -> Vec<Vec<Value>> {
    let up = |owner: u64, number: u64| json!({"op": "upload", "owner": owner, "number": number});
    let late =
        |owner: u64, number: u64| json!({"op": "late_create", "owner": owner, "number": number});
    let replace = |owner: u64, number: u64, observed: u64| json!({"op": "replace", "owner": owner, "number": number, "observed": observed});
    let settle =
        |owner: u64, number: u64| json!({"op": "settle", "owner": owner, "number": number});
    let own =
        |owner: u64, number: u64| json!({"op": "delete_own", "owner": owner, "number": number});
    let gone =
        |owner: u64, number: u64| json!({"op": "delete_gone", "owner": owner, "number": number});
    vec![
        // A floor lands, the next one replaces it, and the first body goes.
        vec![
            up(1, 1),
            replace(1, 1, 0),
            settle(1, 1),
            up(1, 2),
            replace(1, 2, 1),
            settle(1, 2),
            own(1, 1),
            own(1, 2),
        ],
        // Two devices race for the same revision: the second is refused and
        // deletes its own body.
        vec![up(1, 1), up(2, 1), replace(1, 1, 0), replace(2, 1, 0), settle(2, 1), own(2, 1)],
        // A delayed create at a deleted name lands, and the pointer never
        // names it.
        vec![
            up(1, 1),
            replace(1, 1, 0),
            settle(1, 1),
            up(1, 2),
            replace(1, 2, 1),
            settle(1, 2),
            own(1, 1),
            late(1, 1),
            own(1, 1),
        ],
        // A gone device's body goes once the pointer names another.
        vec![
            up(7, 1),
            replace(7, 1, 0),
            settle(7, 1),
            json!({"op": "gone", "device": 7}),
            gone(7, 1),
            up(1, 1),
            replace(1, 1, 1),
            settle(1, 1),
            gone(7, 1),
        ],
    ]
}

#[tokio::test]
async fn the_floor_pointer_follows_the_rules_of_its_bodies() {
    for ops in pointer_cases() {
        pointer_case(&ops).await;
    }
}

#[tokio::test]
#[ignore = "requires COVEN_FLOOR_POINTER_LEAN"]
async fn floor_pointer_matches_lean() {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let runner = std::env::var("COVEN_FLOOR_POINTER_LEAN").expect("floorPointerRunner executable");
    for ops in pointer_cases() {
        let (input, actual) = pointer_case(&ops).await;
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

/// The deterministic provider standing in for S3 keeps a session's parts
/// by index: a later part confirmed first is recorded, a part whose reply
/// was lost is found again by resuming, and publishing hands the parts over
/// in order (§17.9).
#[tokio::test]
async fn independent_parts_go_in_any_order_and_resume_after_a_lost_reply() {
    let storage =
        MemoryStorage::builder().transfer_limits(4, 4).independent_parts().build().unwrap();
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    let bytes = b"aaaabbbbccccd";
    let mut session = storage.begin_upload(&path, bytes.len() as u64).await.unwrap();
    assert!(!session.parts_independent(), "the first part starts the session alone");
    storage.upload_part(&mut session, &bytes[..4]).await.unwrap();
    assert!(session.parts_independent());
    let send = |session: &UploadSession, offset: usize, length: usize| {
        let session = session.clone();
        let storage = storage.clone();
        async move {
            storage
                .send_independent_part(
                    &session,
                    offset as u64,
                    &bytes[offset..offset + length],
                    &ByteProgress::unread(),
                )
                .await
        }
    };
    let last = send(&session, 12, 1).await.unwrap();
    session.confirm(last).unwrap();
    let recorded = session.encode().unwrap();
    storage.set_faults(Faults { lose_part_reply: true, ..Faults::none() }).await;
    send(&session, 8, 4).await.unwrap_err();
    let mut session = UploadSession::decode(recorded.as_bytes()).unwrap();
    assert_eq!(session.parts_to_send(), [4..8, 8..12]);
    storage.resume_upload(&mut session).await.unwrap();
    assert_eq!(session.parts_to_send(), std::slice::from_ref(&(4..8)));
    assert_eq!(session.confirmed_bytes(), 9);
    let second = send(&session, 4, 4).await.unwrap();
    session.confirm(second).unwrap();
    storage.finish_upload(&mut session).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), bytes);
}
