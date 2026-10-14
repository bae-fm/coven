use super::*;
use crate::store_log_sync::tests::{device, member, store};
use coven_foundation::id_source::DeviceId;
use coven_storage::{
    test_utils::{MemoryRequest, MemoryStorage},
    ObjectPath,
};
use std::sync::Arc;

#[tokio::test]
async fn overlapping_ranges_reopen_and_changed_native_ids_do_not_fetch_retained_bytes() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut receiver = device(storage.clone(), 9, member(1), store(1)).await;
    let path = ObjectPath::device_log(store(1), DeviceId(1), 1.try_into().unwrap());
    storage.create(&path, b"abcdefghij").await.unwrap();
    let object = storage.status(&path).await.unwrap().unwrap();
    let input = ObjectInput::observe(&receiver.db, &object).await.unwrap();
    assert_eq!(
        input
            .read(storage.as_ref(), ByteRange::new(2, 6).unwrap())
            .await
            .unwrap(),
        b"cdef"
    );
    let before = storage.request_trace().len();
    assert_eq!(
        input
            .read(storage.as_ref(), ByteRange::new(0, 8).unwrap())
            .await
            .unwrap(),
        b"abcdefgh"
    );
    let reads: Vec<_> = storage.request_trace()[before..]
        .iter()
        .filter_map(|request| match request {
            MemoryRequest::Read(_, Some(range)) => Some((range.start(), range.end())),
            _ => None,
        })
        .collect();
    assert_eq!(reads, [(0, 2), (6, 8)]);
    receiver.reopen(storage.clone(), vec![], vec![]).await;
    let before = storage.request_trace().len();
    let mut native = object.clone();
    native.object_id.push_str("-listing");
    native.revision.push_str("-listing");
    let input = ObjectInput::observe(&receiver.db, &native).await.unwrap();
    assert_eq!(
        input
            .read(storage.as_ref(), ByteRange::new(0, 8).unwrap())
            .await
            .unwrap(),
        b"abcdefgh"
    );
    assert_eq!(storage.request_trace().len(), before);
    native.size += 1;
    assert!(
        matches!(ObjectInput::observe(&receiver.db,&native).await,Err(SyncError::Storage(error)) if error.failure()==StorageFailure::Protocol)
    );
}

#[tokio::test]
async fn same_size_same_time_positions_replacement_fetches_the_new_revision() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let receiver = device(storage.clone(), 9, member(1), store(1)).await;
    let path = ObjectPath::positions(store(1), DeviceId(1));
    storage.replace(&path, b"a").await.unwrap();
    let first = storage.status(&path).await.unwrap().unwrap();
    let input = ObjectInput::observe(&receiver.db, &first).await.unwrap();
    assert_eq!(
        input
            .read(storage.as_ref(), ByteRange::new(0, 1).unwrap())
            .await
            .unwrap(),
        b"a"
    );
    storage.replace(&path, b"b").await.unwrap();
    let second = storage.status(&path).await.unwrap().unwrap();
    assert_eq!(first.stored_at, second.stored_at);
    assert_ne!(first.revision, second.revision);
    let input = ObjectInput::observe(&receiver.db, &second).await.unwrap();
    assert_eq!(
        input
            .read(storage.as_ref(), ByteRange::new(0, 1).unwrap())
            .await
            .unwrap(),
        b"b"
    );
}

/// A peer replaces its post between this device's listing and its GET. The
/// response is the newer observation, not a protocol failure, and its bytes
/// are retained under that revision rather than the listed one.
#[tokio::test]
async fn positions_replaced_after_listing_are_read_as_the_newer_revision() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let receiver = device(storage.clone(), 9, member(1), store(1)).await;
    let path = ObjectPath::positions(store(1), DeviceId(1));
    storage.replace(&path, b"listed").await.unwrap();
    let listed = storage.status(&path).await.unwrap().unwrap();
    storage.replace(&path, b"newer!").await.unwrap();
    let newer = storage.status(&path).await.unwrap().unwrap();
    assert_ne!(listed.revision, newer.revision);
    let before = storage.request_trace().len();
    let (input, bytes) = ObjectInput::whole_replaceable(&receiver.db, storage.as_ref(), &listed)
        .await
        .unwrap();
    assert_eq!(bytes, b"newer!");
    assert_eq!(input.object().revision, newer.revision);
    assert_eq!(
        storage.request_trace()[before..],
        [MemoryRequest::Read(path.clone(), None)]
    );
    // The next listing names the revision already read: no request.
    let before = storage.request_trace().len();
    let (input, bytes) = ObjectInput::whole_replaceable(&receiver.db, storage.as_ref(), &newer)
        .await
        .unwrap();
    assert_eq!(bytes, b"newer!");
    assert_eq!(input.object().revision, newer.revision);
    assert_eq!(storage.request_trace().len(), before);
}

#[tokio::test]
async fn simultaneous_consumers_share_the_in_flight_range() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let receiver = device(storage.clone(), 9, member(1), store(1)).await;
    let path = ObjectPath::device_log(store(1), DeviceId(1), 1.try_into().unwrap());
    storage.create(&path, b"abcdefghij").await.unwrap();
    let object = storage.status(&path).await.unwrap().unwrap();
    let first = ObjectInput::observe(&receiver.db, &object).await.unwrap();
    let second = ObjectInput::observe(&receiver.db, &object).await.unwrap();
    let before = storage.request_trace().len();
    let range = ByteRange::new(0, 10).unwrap();
    let (first, second) = tokio::join!(
        first.read(storage.as_ref(), range),
        second.read(storage.as_ref(), range)
    );
    assert_eq!(first.unwrap(), second.unwrap());
    assert_eq!(storage.request_trace().len() - before, 1);
}

#[tokio::test]
#[ignore = "requires COVEN_IO_RUNNER"]
async fn io_differential_retained_ranges_and_refusal_eviction() {
    use serde_json::json;
    let mut cases = Vec::new();
    let mut results = Vec::new();
    for start in 0..4u64 {
        let storage = Arc::new(MemoryStorage::builder().build().unwrap());
        let mut receiver = device(storage.clone(), 9, member(1), store(1)).await;
        let path = ObjectPath::device_log(store(1), DeviceId(1), 1.try_into().unwrap());
        storage.create(&path, b"abcdefghij").await.unwrap();
        let object = storage.status(&path).await.unwrap().unwrap();
        let refusal = coven_database::LogRefusal {
            object: coven_database::LogObject::Write(path.write_id().unwrap()),
            failure: coven_format::pending::RefusalCode::Signature,
        };
        crate::pending_logs::record_refusal(&receiver.db, refusal)
            .await
            .unwrap();
        let actions = vec![
            json!({"op":"read","start":start,"end":start+3}),
            json!({"op":"read","start":0,"end":8}),
            json!({"op":"wait"}),
            json!({"op":"reopen"}),
            json!({"op":"read","start":start,"end":8}),
            json!({"op":"evict"}),
            json!({"op":"wait"}),
            json!({"op":"reopen"}),
        ];
        let mut output = Vec::new();
        for action in &actions {
            let before = storage.request_trace().len();
            match action["op"].as_str().unwrap() {
                "read" => {
                    let input = ObjectInput::observe(&receiver.db, &object).await.unwrap();
                    input
                        .read(
                            storage.as_ref(),
                            ByteRange::new(
                                action["start"].as_u64().unwrap(),
                                action["end"].as_u64().unwrap(),
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                }
                "reopen" => receiver.reopen(storage.clone(), vec![], vec![]).await,
                "evict" => {
                    ObjectInput::observe(&receiver.db, &object)
                        .await
                        .unwrap()
                        .retained
                        .release_ranges()
                        .await
                        .unwrap();
                    receiver.db.set_input_budget(0).await.unwrap();
                }
                "wait" => (),
                action => panic!("unexpected action {action}"),
            }
            let fetched: u64 = storage.request_trace()[before..]
                .iter()
                .map(|request| match request {
                    MemoryRequest::Read(_, Some(range)) => range.len(),
                    request => panic!("unexpected request {request:?}"),
                })
                .sum();
            let budget = receiver.db.input_budget().await.unwrap();
            let refused = crate::pending_logs::local_refusals(&receiver.db)
                .await
                .unwrap()
                .contains(&refusal);
            output.push(json!({"fetched":fetched,"retained":budget.retained,"refused":refused}));
        }
        cases.push(json!({"check":"retained","actions":actions}));
        results.push(json!(output));
    }
    assert_eq!(results, crate::discovery::tests::io_runner(&cases));
}
