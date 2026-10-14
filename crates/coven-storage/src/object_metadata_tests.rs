use super::*;
use crate::{test_utils::MemoryStorage, Storage};
use coven_foundation::id_source::DeviceId;

#[tokio::test]
async fn observations_reject_wrong_identity_and_inconsistent_body_lengths() {
    let storage = MemoryStorage::builder().build().unwrap();
    let path = ObjectPath::positions(crate::test_utils::STORE_ID, DeviceId(1));
    storage.replace(&path, b"data").await.unwrap();
    let read = storage.read_object(&path, None).await.unwrap();
    read.check(&path, None).unwrap();
    for change in [0, 1, 2, 3, 4] {
        let mut invalid = read.clone();
        match change {
            0 => invalid.metadata.object_id.clear(),
            1 => invalid.metadata.revision.clear(),
            2 => invalid.metadata.time_resolution = Duration::ZERO,
            3 => {
                invalid.metadata.path = ObjectPath::clock(crate::test_utils::STORE_ID, DeviceId(1))
            }
            4 => invalid.metadata.size += 1,
            _ => unreachable!(),
        }
        assert_eq!(
            invalid.check(&path, None).unwrap_err().failure(),
            StorageFailure::Protocol
        );
    }
    let range = ByteRange::new(1, 3).unwrap();
    let mut read = storage.read_object(&path, Some(range)).await.unwrap();
    assert_eq!(read.metadata.size, 4);
    read.bytes.pop();
    assert_eq!(
        read.check(&path, Some(range)).unwrap_err().failure(),
        StorageFailure::Protocol
    );
}

/// A full-object SHA-256 or Dropbox content hash confirms exactly the local
/// bytes; a composite never does, and other algorithms defer to a readback.
#[test]
fn checksums_confirm_only_complete_objects_in_known_algorithms() {
    use base64::Engine;
    let bytes: Vec<u8> = (0..5 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let read = |offset: u64, length: usize| {
        Ok::<_, ()>(bytes[offset as usize..offset as usize + length].to_vec())
    };
    let mut whole = coven_crypto::ContentHasher::new();
    whole.update(&bytes);
    let sha = base64::engine::general_purpose::STANDARD.encode(whole.finish().as_bytes());
    let full = ObjectChecksum::FullObject(ChecksumValue::Sha256Base64(sha.clone()));
    assert_eq!(full.confirms(bytes.len() as u64, read), Ok(Some(true)));
    let other = ObjectChecksum::FullObject(ChecksumValue::Sha256Base64("AAAA".into()));
    assert_eq!(other.confirms(bytes.len() as u64, read), Ok(Some(false)));
    let composite = ObjectChecksum::Composite(ChecksumValue::Sha256Base64(sha));
    assert_eq!(composite.confirms(bytes.len() as u64, read), Ok(None));
    let mut blocks = coven_crypto::ContentHasher::new();
    for block in bytes.chunks(4 * 1024 * 1024) {
        let mut hasher = coven_crypto::ContentHasher::new();
        hasher.update(block);
        blocks.update(hasher.finish().as_bytes());
    }
    let dropbox = ObjectChecksum::FullObject(ChecksumValue::DropboxContentHash(hex::encode(
        blocks.finish().as_bytes(),
    )));
    assert_eq!(dropbox.confirms(bytes.len() as u64, read), Ok(Some(true)));
    let md5 = ObjectChecksum::FullObject(ChecksumValue::Md5Hex("00".into()));
    assert_eq!(md5.confirms(bytes.len() as u64, read), Ok(None));
}
