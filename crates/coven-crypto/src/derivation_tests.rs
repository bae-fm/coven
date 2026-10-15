use super::*;
use crate::{InviteSecret, StoreKey};
use coven_foundation::id_source::KeyId;

#[test]
fn purpose_labels_and_hkdf_answers_are_pinned() {
    // Answers independently calculated with Python's hashlib/hmac (RFC 5869).
    let vectors: &[(&[u8], &[u8], &str)] = &[
        (
            ENCRYPTION,
            b"coven/encryption/v1",
            "5c09ce5876164485843f2637bb370094c8dfd799637cda80fbc4c88c5b63229f",
        ),
        (
            FINGERPRINTS,
            b"coven/fingerprints/v1",
            "795d9756ca6212a6f38d5cd68fccbd5952ec59e52d8f5aa4bdd76e8bb95ceff6",
        ),
        (
            JOIN_REQUEST,
            b"coven/join-request/v1",
            "80dc795abb07ec2ebea022802dcd893f27e018e45157d78fa7ac42bf691440f5",
        ),
        (
            SEALED_BOX,
            b"coven/sealed-box/v1",
            "2507974c84457b2d7fb35034f7de475379c05d8b428ab43e1d22ecebff2f3b07",
        ),
        (
            AUTHORSHIP,
            b"coven/row-authorship/v1",
            "8f0b41cb3dcf6da503cbb4fa43a556111b2ef687406693eab31cdea5a483fd52",
        ),
    ];
    for (label, expected, answer) in vectors {
        assert_eq!(label, expected);
        assert_eq!(
            hex::encode(derive_label(&[17; 32], label).as_ref()),
            *answer
        );
    }
}

#[test]
fn fingerprint_key_answer_is_pinned() {
    let keys = StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [17; 32]).derive();
    let mut fingerprint = keys.fingerprint_hasher();
    fingerprint.update(b"agreed state");
    assert_eq!(
        hex::encode(fingerprint.finish().as_bytes()),
        "e03357024e1ed6e2bb91ce154bb87a62e77f452a810274561035e99551516440"
    );
}

#[test]
fn a_join_request_is_bound_to_its_invite_and_path() {
    let invite = InviteSecret::generate().unwrap();
    let key = invite.join_request_key();
    let sealed = key
        .seal_object_chunk("10101010-1010-1010-1010-101010101010/join-requests/42424242-4242-4242-4242-424242424242", b"cleartext prefix", 0, 0, b"Carol")
        .unwrap();
    assert_eq!(
        key.open_object_chunk("10101010-1010-1010-1010-101010101010/join-requests/42424242-4242-4242-4242-424242424242", b"cleartext prefix", 0, 0, &sealed)
            .unwrap(),
        b"Carol"
    );
    assert!(matches!(
        key.open_object_chunk("10101010-1010-1010-1010-101010101010/join-requests/43434343-4343-4343-4343-434343434343", b"cleartext prefix", 0, 0, &sealed),
        Err(CryptoError::Authentication)
    ));
    assert!(matches!(
        InviteSecret::generate()
            .unwrap()
            .join_request_key()
            .open_object_chunk("10101010-1010-1010-1010-101010101010/join-requests/42424242-4242-4242-4242-424242424242", b"cleartext prefix", 0, 0, &sealed),
        Err(CryptoError::Authentication)
    ));
    let same = InviteSecret::from_bytes(invite.to_secret_bytes().as_bytes().try_into().unwrap());
    assert_eq!(
        same.join_request_key()
            .open_object_chunk("10101010-1010-1010-1010-101010101010/join-requests/42424242-4242-4242-4242-424242424242", b"cleartext prefix", 0, 0, &sealed)
            .unwrap(),
        b"Carol"
    );
}

#[test]
fn retry_nonces_are_pinned_and_separate_keys_paths_sections_and_chunks() {
    let key = DerivedKeys::new(&[17; 32]);
    let path = "10101010-1010-1010-1010-101010101010/devices/1/3";
    let sealed = key.reseal_object_chunk(path, b"prefix", 1, 2, b"payload");
    // Independently calculated with Python's hashlib/hmac and D11's encoding.
    assert_eq!(
        hex::encode(&sealed[..24]),
        "9ee2b2bf9cdd2e4b609f10628507dd610a44c6e5185d5781"
    );
    assert_eq!(
        sealed,
        key.reseal_object_chunk(path, b"prefix", 1, 2, b"payload")
    );
    assert_eq!(
        key.open_object_chunk(path, b"prefix", 1, 2, &sealed)
            .unwrap(),
        b"payload"
    );
    for other in [
        key.reseal_object_chunk(
            "20202020-2020-2020-2020-202020202020/devices/1/3",
            b"prefix",
            1,
            2,
            b"payload",
        ),
        key.reseal_object_chunk(
            "10101010-1010-1010-1010-101010101010/devices/1/4",
            b"prefix",
            1,
            2,
            b"payload",
        ),
        key.reseal_object_chunk(
            "10101010-1010-1010-1010-101010101010/store-log/1/3",
            b"prefix",
            1,
            2,
            b"payload",
        ),
        key.reseal_object_chunk(path, b"prefix", 2, 2, b"payload"),
        key.reseal_object_chunk(path, b"prefix", 1, 3, b"payload"),
        DerivedKeys::new(&[18; 32]).reseal_object_chunk(path, b"prefix", 1, 2, b"payload"),
    ] {
        assert_ne!(sealed[..24], other[..24]);
    }
    assert!(matches!(
        key.open_object_chunk(path, b"changed prefix", 1, 2, &sealed),
        Err(CryptoError::Authentication)
    ));
}

#[test]
fn changed_plaintext_at_the_same_chunk_position_changes_the_nonce() {
    let key = DerivedKeys::new(&[17; 32]);
    let path = "10101010-1010-1010-1010-101010101010/devices/1/3";
    let first = key.reseal_object_chunk(path, b"prefix", 1, 2, b"payload");
    let changed = key.reseal_object_chunk(path, b"prefix", 1, 2, b"payloae");
    assert_ne!(first[..24], changed[..24]);
    assert_eq!(
        hex::encode(&changed[..24]),
        "555e6c3798346d2671ef7e89dc86dd9ac45d38b2749aa311"
    );
}

#[test]
fn changed_clear_prefix_at_the_same_chunk_position_changes_the_nonce() {
    let key = DerivedKeys::new(&[17; 32]);
    let path = "10101010-1010-1010-1010-101010101010/devices/1/3";
    let first = key.reseal_object_chunk(path, b"prefix", 1, 2, b"payload");
    let changed = key.reseal_object_chunk(path, b"prefiy", 1, 2, b"payload");
    assert_ne!(first[..24], changed[..24]);
    assert_eq!(
        hex::encode(&changed[..24]),
        "15aa0a84d82e41793b0361ad13837207b80c002b87596d0c"
    );
}

#[test]
fn retry_nonce_vectors_pin_binary_fields_and_big_endian_u64_coordinates() {
    // Independently calculated with Python hashlib/hmac, including SHA-256
    // of the complete plaintext and each field's separate u64 length prefix.
    let key = DerivedKeys::new(&[17; 32]);
    let path = "10101010-1010-1010-1010-101010101010/devices/1/3";
    for (prefix, section, index, plain, expected) in [
        (
            b"\x20\x00\x01\xff\x00".as_slice(),
            0x0102030405060708,
            0x1112131415161718,
            b"\x00\xff\x80\x00".as_slice(),
            "8c886b9f9901c6fcbda0e3cbf502b915a41bfd2c9e6c5dc3",
        ),
        (
            b"",
            0,
            0,
            b"",
            "a5983baa739401fe568e54f7adf6422ea949d0783f6097a6",
        ),
    ] {
        let sealed = key.reseal_object_chunk(path, prefix, section, index, plain);
        assert_eq!(hex::encode(&sealed[..24]), expected);
        assert_eq!(
            sealed,
            key.reseal_object_chunk(path, prefix, section, index, plain)
        );
        assert_eq!(
            key.open_object_chunk(path, prefix, section, index, &sealed)
                .unwrap(),
            plain
        );
    }
}

#[test]
fn authorship_leaves_are_keyed_and_branches_are_pinned() {
    // HMAC-SHA256 and SHA-256 answers independently calculated with Python.
    let keys = StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [17; 32]).derive();
    let hasher = keys.authorship_hasher();
    assert_eq!(
        hex::encode(hasher.leaf(b"leaf")),
        "b78c882d405ca07181c1a3f7d15e544268ee5c6ddc37dea035c27b663fe9f63e"
    );
    assert_eq!(
        hex::encode(crate::authorship_node(&[0; 32], &[1; 32])),
        "2ad82c3a51e8ed6418cb5bf267c5f9e521b99f7ab4fce657f460a8f1a3e87b2e"
    );
    assert_eq!(
        hex::encode(crate::authorship_empty()),
        "dbc1b4c900ffe48d575b5da5c638040125f65db0fe3e24494b76ea986457d986"
    );
    // A branch's two children never hash as one leaf of the same bytes.
    let mut joined = vec![0; 32];
    joined.extend([1; 32]);
    assert_ne!(
        hasher.leaf(&joined),
        crate::authorship_node(&[0; 32], &[1; 32])
    );
    // Another audience's key gives unrelated hashes for the same leaf.
    let other = StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([2; 16])), [18; 32]).derive();
    assert_ne!(
        other.authorship_hasher().leaf(b"leaf"),
        hasher.leaf(b"leaf")
    );
}
