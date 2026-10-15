use crate::device_log_sync::tests::{group, rows, sql, storage, STORE_ID};
use coven_foundation::id_source::DeviceId;
use coven_storage::test_utils::{BodyFailure, Faults, MemoryRequest, MemoryStorage};
use coven_storage::{ByteRange, ObjectPath, Storage};

fn write(device: u64, number: u64) -> ObjectPath {
    ObjectPath::device_log(STORE_ID, DeviceId(device), number.try_into().unwrap())
}

/// Requests naming `writer`'s write log since `before`.
fn write_requests(storage: &MemoryStorage, before: usize, writer: u64) -> Vec<MemoryRequest> {
    storage.request_trace()[before..]
        .iter()
        .filter(|request| match request {
            MemoryRequest::Read(path, _) | MemoryRequest::Status(path) => path
                .write_position()
                .is_some_and(|(device, _)| device == DeviceId(writer)),
            _ => false,
        })
        .cloned()
        .collect()
}

async fn note(device: &mut crate::store_log_sync::tests::Device, id: &str, body: usize) {
    let id = id.to_owned();
    device
        .db
        .write(move |sql| {
            sql.execute(
                "INSERT INTO notes VALUES(?1,'title',?2)",
                (id, "x".repeat(body)),
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(device.writes.upload_writes().await.unwrap().len(), 1);
}

// §6 and SP step 6: a hit is one GET whose body is the write; the first
// number goes alone, later ones fetch ahead up to the limit, and nothing is
// scheduled past the first miss. The probe in flight beside it is charged.
#[tokio::test]
async fn a_hit_is_one_get_and_fetch_ahead_stops_at_the_miss() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    for id in ["a", "b", "c"] {
        note(&mut devices[0], id, 10).await;
    }
    devices[1].writes.set_download_limit(2.try_into().unwrap());
    let before = storage.request_trace().len();
    devices[1].writes.download_writes().await.unwrap();
    assert_eq!(rows(&devices[1].db).await.len(), 3);
    let requests = write_requests(&storage, before, 1);
    let read = |n| MemoryRequest::Read(write(1, n), None);
    assert_eq!(requests[0], read(1), "{requests:?}");
    let mut rest = requests[1..].to_vec();
    rest.sort_by_key(|request| format!("{request:?}"));
    assert_eq!(rest, [read(2), read(3), read(4), read(5)]);
    assert!(
        !storage.request_trace()[before..]
            .iter()
            .any(|request| matches!(request, MemoryRequest::Status(_))),
        "discovery never asks for status"
    );
}

// SP "Log download positions": a write waiting for its cause is retained
// whole, so later discoveries read past it without downloading it again.
#[tokio::test]
async fn a_waiting_write_is_not_downloaded_again() {
    let storage = storage();
    let mut devices = group(storage.clone(), 3).await;
    sql(
        &devices[0].db,
        "INSERT INTO notes VALUES('one','first','body')",
    )
    .await;
    devices[0].writes.upload_writes().await.unwrap();
    devices[1].writes.download_writes().await.unwrap();
    sql(&devices[1].db, "UPDATE notes SET title='second'").await;
    devices[1].writes.upload_writes().await.unwrap();
    let cause = write(1, 1);
    let bytes = storage.read(&cause).await.unwrap();
    storage.delete(&cause).await.unwrap();
    let before = storage.request_trace().len();
    devices[2].writes.download_writes().await.unwrap();
    assert!(rows(&devices[2].db).await.is_empty());
    assert_eq!(
        write_requests(&storage, before, 2),
        [
            MemoryRequest::Read(write(2, 1), None),
            MemoryRequest::Read(write(2, 2), None)
        ]
    );
    let before = storage.request_trace().len();
    devices[2].writes.download_writes().await.unwrap();
    assert_eq!(
        write_requests(&storage, before, 2),
        [MemoryRequest::Read(write(2, 2), None)],
        "the waiting write comes from retained input"
    );
    storage.create(&cause, &bytes).await.unwrap();
    let before = storage.request_trace().len();
    devices[2].writes.download_writes().await.unwrap();
    assert_eq!(rows(&devices[2].db).await, rows(&devices[1].db).await);
    assert_eq!(
        write_requests(&storage, before, 2),
        [MemoryRequest::Read(write(2, 2), None)]
    );
}

// §6: discovery never advances over input not durably retained. A body that
// failed partway keeps its received prefix; the next discovery completes it
// with one range from that prefix's end before reading the next number.
#[tokio::test]
async fn a_partly_received_write_completes_from_its_retained_prefix() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    note(&mut devices[0], "large", 150_000).await;
    let path = write(1, 1);
    let size = storage.status(&path).await.unwrap().unwrap().size;
    let mut faults = Faults::none();
    faults
        .body_failures
        .insert(path.clone(), BodyFailure::Interrupted(100_000));
    storage.set_faults(faults).await;
    assert!(devices[1].writes.download_writes().await.is_err());
    assert!(rows(&devices[1].db).await.is_empty());
    storage.set_faults(Faults::none()).await;
    let before = storage.request_trace().len();
    devices[1].writes.download_writes().await.unwrap();
    assert_eq!(rows(&devices[1].db).await.len(), 1);
    assert_eq!(
        write_requests(&storage, before, 1),
        [
            MemoryRequest::Read(path, Some(ByteRange::new(100_000, size).unwrap())),
            MemoryRequest::Read(write(1, 2), None)
        ]
    );
}

// A retained prefix whose object is gone when discovery completes it is a
// miss, as a whole GET's 404 is, never a failure that stops the pass short
// of the snapshot discovery that can cover it (§15).
#[tokio::test]
async fn a_partly_received_write_since_deleted_is_a_miss() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    note(&mut devices[0], "large", 150_000).await;
    let path = write(1, 1);
    let mut faults = Faults::none();
    faults
        .body_failures
        .insert(path.clone(), BodyFailure::Interrupted(100_000));
    storage.set_faults(faults).await;
    assert!(devices[1].writes.download_writes().await.is_err());
    storage.set_faults(Faults::none()).await;
    storage.delete(&path).await.unwrap();
    let before = storage.request_trace().len();
    let discovery = devices[1].writes.download_writes().await.unwrap();
    assert!(!discovery.complete);
    assert!(rows(&devices[1].db).await.is_empty());
    assert_eq!(write_requests(&storage, before, 1).len(), 1);
}

// A catch-up larger than the retained-input budget succeeds: each round
// applies and releases its ready writes before the next is fetched.
#[tokio::test]
async fn ready_writes_are_released_before_more_are_fetched() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    for id in ["a", "b", "c", "d"] {
        note(&mut devices[0], id, 100_000).await;
    }
    devices[1].db.set_input_budget(250_000).await.unwrap();
    devices[1].writes.download_writes().await.unwrap();
    assert_eq!(rows(&devices[1].db).await.len(), 4);
}

// §19.1: nothing at or past a refused write is read, including in the
// discovery that first refuses it.
#[tokio::test]
async fn a_write_refused_in_this_discovery_stops_its_log() {
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    for id in ["a", "b", "c"] {
        note(&mut devices[0], id, 10).await;
    }
    let path = write(1, 1);
    let size = storage.status(&path).await.unwrap().unwrap().size;
    storage
        .corrupt_byte(&path, usize::try_from(size).unwrap() - 1)
        .await
        .unwrap();
    let before = storage.request_trace().len();
    devices[1].writes.download_writes().await.unwrap();
    assert!(rows(&devices[1].db).await.is_empty());
    assert_eq!(
        write_requests(&storage, before, 1),
        [MemoryRequest::Read(path, None)]
    );
    let before = storage.request_trace().len();
    devices[1].writes.download_writes().await.unwrap();
    assert!(write_requests(&storage, before, 1).is_empty());
}
