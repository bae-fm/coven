use super::*;
use crate::StoreKey;
use coven_foundation::id_source::{IdSource, KeyId};

#[test]
fn incremental_content_hash_matches_sha256_known_answers() {
    let mut hash = ContentHasher::new();
    hash.update(b"a");
    hash.update(b"");
    hash.update(b"bc");
    assert_eq!(
        hex::encode(hash.finish().as_bytes()),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(
        hex::encode(ContentHasher::new().finish().as_bytes()),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    // A million-byte stream with a reused buffer; no whole-file allocation.
    let mut million = ContentHasher::new();
    for _ in 0..1000 {
        million.update(&[b'a'; 1000]);
    }
    assert_eq!(
        hex::encode(million.finish().as_bytes()),
        "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
    );
}

#[test]
fn fingerprints_are_incremental_and_depend_on_the_audience_key() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let key = StoreKey::generate(KeyId(key_ids.new_id()))
        .unwrap()
        .derive();
    let mut whole = key.fingerprint_hasher();
    whole.update(b"agreed state");
    let mut stream = key.fingerprint_hasher();
    for byte in b"agreed state" {
        stream.update(&[*byte]);
    }
    let fingerprint = whole.finish();
    assert_eq!(fingerprint, stream.finish());
    let other = StoreKey::generate(KeyId(key_ids.new_id()))
        .unwrap()
        .derive();
    let mut foreign = other.fingerprint_hasher();
    foreign.update(b"agreed state");
    assert_ne!(fingerprint, foreign.finish());
    assert_eq!(
        fingerprint,
        Fingerprint::from_bytes(*fingerprint.as_bytes())
    );
}
