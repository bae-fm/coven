use super::*;
use coven_foundation::{
    files::{StoreDir, StoreLayout},
    id_source::{SequentialIds, StoreId},
};
use coven_storage::{
    test_utils::{MemoryRequest, MemoryStorage},
    ObjectPath,
};
use std::sync::Arc;

#[tokio::test]
async fn overlapping_ranges_reopen_and_changed_native_ids_do_not_fetch_retained_bytes() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut receiver = Fixture::new().await;
    let path = ObjectPath::slot(store(), 1.try_into().unwrap());
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
    receiver.reopen().await;
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
async fn same_size_same_time_floor_replacement_fetches_the_new_revision() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let receiver = Fixture::new().await;
    let path = ObjectPath::floor(store());
    storage.create(&path, b"a").await.unwrap();
    let first = storage.status(&path).await.unwrap().unwrap();
    let input = ObjectInput::observe(&receiver.db, &first).await.unwrap();
    assert_eq!(
        input
            .read(storage.as_ref(), ByteRange::new(0, 1).unwrap())
            .await
            .unwrap(),
        b"a"
    );
    storage.replace(&first, b"b").await.unwrap();
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

#[tokio::test]
async fn simultaneous_consumers_share_the_in_flight_range() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let receiver = Fixture::new().await;
    let path = ObjectPath::slot(store(), 1.try_into().unwrap());
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
        let mut receiver = Fixture::new().await;
        let path = ObjectPath::slot(store(), 1.try_into().unwrap());
        storage.create(&path, b"abcdefghij").await.unwrap();
        let object = storage.status(&path).await.unwrap().unwrap();
        let refusal = crate::PendingRecord {
            subject: crate::PendingSubject::Slot(coven_merge::Slot(1)),
            reported_by: receiver.directory.settings().unwrap().device_id,
            reason: crate::PendingReason::Refused(crate::Refusal::Signature { cause: None }),
        };
        receiver.db.record_pending(refusal.clone()).await.unwrap();
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
                "reopen" => receiver.reopen().await,
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
            let refused = receiver
                .db
                .pending::<crate::PendingRecord>()
                .await
                .unwrap()
                .contains(&refusal);
            output.push(json!({"fetched":fetched,"retained":budget.retained,"refused":refused}));
        }
        cases.push(json!({"check":"retained","actions":actions}));
        results.push(json!(output));
    }
    assert_eq!(results, crate::tests::io_runner(&cases));
}

pub(crate) fn store() -> StoreId {
    coven_storage::test_utils::STORE_ID
}
#[cfg(test)]
pub(crate) struct Fixture {
    _temp: tempfile::TempDir,
    directory: StoreDir,
    pub(crate) db: Database,
}
impl Fixture {
    pub(crate) async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let directory = StoreLayout::new(temp.path().into())
            .create_store_dir(store(), "input", &SequentialIds::new())
            .unwrap();
        let db = coven_database::DatabaseBuilder::new(directory.clone())
            .synced_tables(vec![])
            .migrations(vec![])
            .open()
            .await
            .unwrap();
        Self {
            _temp: temp,
            directory,
            db,
        }
    }
    pub(crate) async fn reopen(&mut self) {
        self.db.close().await.unwrap();
        self.db = coven_database::DatabaseBuilder::new(self.directory.clone())
            .synced_tables(vec![])
            .migrations(vec![])
            .open()
            .await
            .unwrap();
    }
}
