use super::*;
use coven_foundation::id_source::{DeviceId, StoreId};
use coven_merge::Audience;
use coven_storage::test_utils::MemoryStorage;

fn path() -> ObjectPath {
    ObjectPath::snapshot(
        StoreId(uuid::Uuid::from_u128(1)),
        Audience::Store,
        DeviceId(7),
        std::num::NonZeroU64::new(3).unwrap(),
    )
}

/// After upload the stored object is compared with the sealed bytes: equal
/// bytes confirm publication; a different size or different bytes at the
/// same size are an integrity failure, not success. Without a complete-object
/// checksum the comparison reads the stored bytes back (§15).
#[tokio::test]
async fn stored_bytes_must_equal_the_sealed_file() {
    let storage = MemoryStorage::builder().build().unwrap();
    let sealed: Vec<u8> = (0..3 * 1024 * 1024).map(|i| (i % 253) as u8).collect();
    storage.create(&path(), &sealed).await.unwrap();
    let read = |bytes: &[u8]| {
        let bytes = bytes.to_vec();
        move |offset: u64, length: usize| -> Result<Vec<u8>, SyncError> {
            Ok(bytes[offset as usize..offset as usize + length].to_vec())
        }
    };
    confirm_stored(&storage, &path(), sealed.len() as u64, read(&sealed))
        .await
        .unwrap();
    let mut changed = sealed.clone();
    changed[2 * 1024 * 1024] ^= 1;
    assert!(
        confirm_stored(&storage, &path(), changed.len() as u64, read(&changed))
            .await
            .is_err()
    );
    let shorter = &sealed[..sealed.len() - 1];
    assert!(
        confirm_stored(&storage, &path(), shorter.len() as u64, read(shorter))
            .await
            .is_err()
    );
    storage.delete(&path()).await.unwrap();
    assert!(
        confirm_stored(&storage, &path(), sealed.len() as u64, read(&sealed))
            .await
            .is_err(),
        "an absent object is not published"
    );
}
