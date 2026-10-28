use super::*;
use crate::{
    test_utils::{BodyFailure, Faults, MemoryStorage},
    Storage,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

async fn object(storage: &MemoryStorage) -> (ObjectPath, StoredObject) {
    let path = ObjectPath::floor(crate::test_utils::STORE_ID);
    storage.create(&path, b"abcdef").await.unwrap();
    let metadata = storage.status(&path).await.unwrap().unwrap();
    (path, metadata)
}

#[tokio::test]
async fn a_collector_rejects_oversized_metadata_before_reading_the_body() {
    let storage = MemoryStorage::builder().build().unwrap();
    let (_, mut metadata) = object(&storage).await;
    metadata.size = BUFFERED_READ_LIMIT + 1;
    let count = Arc::new(AtomicUsize::new(0));
    struct Body(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl ObjectBody for Body {
        async fn read(&mut self, _: &mut [u8]) -> Result<usize, StorageError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(0)
        }
    }
    let stream = ObjectStream::new(metadata, None, Body(count.clone())).unwrap();
    assert_eq!(
        stream
            .collect(BUFFERED_READ_LIMIT)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::Protocol
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn exact_length_requires_successful_eof_and_failed_streams_cannot_resume() {
    for (fault, expected) in [
        (BodyFailure::Truncated(5), StorageFailure::Protocol),
        (BodyFailure::ExtraByte, StorageFailure::Protocol),
        (BodyFailure::Interrupted(6), StorageFailure::Network),
    ] {
        let storage = MemoryStorage::builder().build().unwrap();
        let (path, _) = object(&storage).await;
        let mut faults = Faults::none();
        faults.body_failures.insert(path.clone(), fault);
        storage.set_faults(faults).await;
        let mut stream = storage.stream_object(&path, None).await.unwrap();
        let mut bytes = [0; 16];
        let first = stream.read(&mut bytes).await.unwrap();
        assert_eq!(
            first,
            if matches!(fault, BodyFailure::Truncated(_)) {
                5
            } else {
                6
            }
        );
        assert_eq!(
            stream.read(&mut bytes).await.unwrap_err().failure(),
            expected
        );
        assert_eq!(
            stream.read(&mut bytes).await.unwrap_err().failure(),
            StorageFailure::Protocol
        );
    }
}

#[tokio::test]
async fn replacement_check_finishes_before_eof_is_delivered() {
    let storage = MemoryStorage::builder().build().unwrap();
    let (_, metadata) = object(&storage).await;
    let checked = Arc::new(AtomicUsize::new(0));
    let observed = checked.clone();
    let mut stream = ObjectStream::new(metadata, None, std::io::Cursor::new(b"abcdef"))
        .unwrap()
        .after(async move {
            observed.fetch_add(1, Ordering::SeqCst);
            Err(StorageFailure::Protocol.with_source("replacement changed"))
        });
    assert_eq!(stream.read(&mut [0; 6]).await.unwrap(), 6);
    assert_eq!(checked.load(Ordering::SeqCst), 0);
    assert_eq!(
        stream.read(&mut [0; 1]).await.unwrap_err().failure(),
        StorageFailure::Protocol
    );
    assert_eq!(checked.load(Ordering::SeqCst), 1);
}

/// A reporting read hears how far through the object each read reaches,
/// counted from where its range starts.
#[tokio::test]
async fn a_reporting_read_hears_each_read_through_the_object() {
    let storage = MemoryStorage::builder().build().unwrap();
    let (path, _) = object(&storage).await;
    let mut faults = Faults::none();
    faults.read_chunk_size = 2;
    storage.set_faults(faults).await;
    let heard = Arc::new(std::sync::Mutex::new(Vec::new()));
    let received = crate::ByteProgress::new({
        let heard = heard.clone();
        move |count| heard.lock().unwrap().push(count)
    });
    let range = ByteRange::new(1, 6).unwrap();
    let stream = storage
        .stream_object_reporting(&path, Some(range), &received)
        .await
        .unwrap();
    assert_eq!(stream.collect(16).await.unwrap().bytes, b"bcdef");
    assert_eq!(*heard.lock().unwrap(), [3, 5, 6]);
}
