use super::*;
use crate::{test_utils::MemoryStorage, Storage};

#[tokio::test]
async fn observations_reject_wrong_identity_and_inconsistent_body_lengths() {
    let storage = MemoryStorage::builder().build().unwrap();
    let path = ObjectPath::floor(crate::test_utils::STORE_ID);
    storage.create(&path, b"data").await.unwrap();
    let read = storage.read_object(&path, None).await.unwrap();
    read.check(&path, None).unwrap();
    for change in [0, 1, 2, 3, 4] {
        let mut invalid = read.clone();
        match change {
            0 => invalid.metadata.object_id.clear(),
            1 => invalid.metadata.revision.clear(),
            2 => invalid.metadata.time_resolution = Duration::ZERO,
            3 => {
                invalid.metadata.path =
                    ObjectPath::slot(crate::test_utils::STORE_ID, std::num::NonZeroU64::MIN)
            }
            4 => invalid.metadata.size += 1,
            _ => unreachable!(),
        }
        assert_eq!(invalid.check(&path, None).unwrap_err().failure(), StorageFailure::Protocol);
    }
    let range = ByteRange::new(1, 3).unwrap();
    let mut read = storage.read_object(&path, Some(range)).await.unwrap();
    assert_eq!(read.metadata.size, 4);
    read.bytes.pop();
    assert_eq!(read.check(&path, Some(range)).unwrap_err().failure(), StorageFailure::Protocol);
}
