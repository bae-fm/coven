use super::*;

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

/// A kept value's digest is SHA-256 over the labelled, length-framed value
/// bytes (D7.3); these answers come from Python's hashlib.
#[test]
fn kept_value_digest_matches_known_answers() {
    assert_eq!(
        hex::encode(kept_value_digest(b"abc")),
        "84586de44f427985ffb727218d430ad5566729693025a492e2e803d7d4a693bc"
    );
    assert_eq!(
        hex::encode(kept_value_digest(b"")),
        "d24710d26dc5e9bb39c78c57e7d9471885cbda047b5c03f2580fa4c911c1f4c4"
    );
}
