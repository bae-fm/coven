use super::*;
use crate::Refusal;
use coven_storage::test_utils::{Faults, MemoryStorage};

fn path() -> ObjectPath {
    ObjectPath::slot(coven_format::test_utils::STORE_ID, std::num::NonZeroU64::new(3).unwrap())
}

/// An occupied path that never changes is this device's only if its complete
/// bytes equal the recorded attempt. Bytes of the same length with one byte
/// changed fail the attempt, a failed read stays pending, and the listing's
/// metadata alone never proves equality (§19.3).
#[tokio::test]
async fn an_occupied_path_is_this_attempts_only_with_equal_bytes() {
    let storage = MemoryStorage::builder().build().unwrap();
    let attempt = b"the bytes this copy meant to send".to_vec();
    let recorded = digest(&attempt);

    storage.plant(&path(), &attempt).await.unwrap();
    settle_occupied(&storage, &path(), recorded).await.unwrap();

    let mut other = attempt.clone();
    other[7] ^= 1;
    assert_eq!(other.len(), attempt.len());
    storage.plant(&path(), &other).await.unwrap();
    assert_eq!(storage.status(&path()).await.unwrap().unwrap().size, attempt.len() as u64);
    assert!(matches!(
        settle_occupied(&storage, &path(), recorded).await,
        Err(SyncError::Damaged(damaged)) if matches!(damaged.failure, Refusal::WrongIdentity { .. })
    ));

    storage.plant(&path(), &attempt).await.unwrap();
    let mut faults = Faults::none();
    faults.read_failures.insert(path(), coven_storage::StorageFailure::Network);
    storage.set_faults(faults).await;
    assert!(matches!(
        settle_occupied(&storage, &path(), recorded).await,
        Err(SyncError::Storage(_))
    ));
}
