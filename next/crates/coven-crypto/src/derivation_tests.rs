use super::*;
use crate::{CircleId, CircleKey, ContentHasher, InviteSecret, StoreKey};
use coven_foundation::id_source::{IdSource, KeyId};
use uuid::Uuid;

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
            APP_DATA,
            b"coven/app-data/v1",
            "cf37721dcdb2ed070d0ce5179c10bfbaa55338c73225231228a9a6a70cd2e59a",
        ),
        (
            NAMING,
            b"coven/naming/v1",
            "42980d4d4896ec07579d1d248022928c916c470fb648942fbb5691e5cee5cf42",
        ),
        (
            FILE_NONCES,
            b"coven/file-nonces/v1",
            "366f97a8e43e06089799b56c049f03b004ff2adce8e26b78615a8d0576577a55",
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
fn stored_file_name_nonce_chunk_and_fingerprint_answers_are_pinned() {
    let keys = StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [17; 32]).derive();
    let mut hash = ContentHasher::new();
    hash.update(b"hello file");
    let name = keys.file_name(&hash.finish());
    assert_eq!(
        name.to_string(),
        "0601bf90cd7e035ecfa03e23d033f2b669686c3bd4f3215da114f71487193d37"
    );
    assert_eq!(
        hex::encode(keys.chunk_nonce(&name, 7)),
        "c52f55f0a718bd6019d69b29ca74c0a00700000000000000"
    );
    // Independently calculated with libsodium's XChaCha20-Poly1305, using
    // big-endian context lengths, raw name bytes and little-endian chunk index.
    assert_eq!(
        hex::encode(keys.seal_chunk(&name, 7, b"hello file")),
        "4cdb8d3d0695280a9543c7ea803ccd795e76383acd9b32e4ec46"
    );
    let mut fingerprint = keys.fingerprint_hasher();
    fingerprint.update(b"agreed state");
    assert_eq!(
        hex::encode(fingerprint.finish().as_bytes()),
        "e03357024e1ed6e2bb91ce154bb87a62e77f452a810274561035e99551516440"
    );
}

#[test]
fn store_and_circle_objects_bind_path_and_use_random_nonces() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let keys = [
        StoreKey::generate(KeyId(key_ids.new_id()))
            .unwrap()
            .derive(),
        CircleKey::generate(CircleId(Uuid::from_u128(5)), KeyId(key_ids.new_id()))
            .unwrap()
            .derive(),
    ];
    for key in keys {
        for plaintext in [b"".as_slice(), b"object payload"] {
            let sealed = key
                .seal_object_chunk("devices/phone/3", b"cleartext prefix", 0, 0, plaintext)
                .unwrap();
            assert_eq!(
                key.open_object_chunk("devices/phone/3", b"cleartext prefix", 0, 0, &sealed)
                    .unwrap(),
                plaintext
            );
            assert_ne!(
                sealed,
                key.seal_object_chunk("devices/phone/3", b"cleartext prefix", 0, 0, plaintext)
                    .unwrap()
            );
            assert!(matches!(
                key.open_object_chunk("devices/phone/4", b"cleartext prefix", 0, 0, &sealed),
                Err(CryptoError::Authentication)
            ));
            for i in 0..sealed.len() {
                let mut altered = sealed.clone();
                altered[i] ^= 1;
                assert!(matches!(
                    key.open_object_chunk("devices/phone/3", b"cleartext prefix", 0, 0, &altered),
                    Err(CryptoError::Authentication)
                ));
                assert!(key
                    .open_object_chunk("devices/phone/3", b"cleartext prefix", 0, 0, &sealed[..i])
                    .is_err());
            }
            let mut trailing = sealed;
            trailing.push(0);
            assert!(key
                .open_object_chunk("devices/phone/3", b"cleartext prefix", 0, 0, &trailing)
                .is_err());
        }
    }
}

#[test]
fn a_join_request_is_bound_to_its_invite_and_path() {
    let invite = InviteSecret::generate().unwrap();
    let key = invite.join_request_key();
    let sealed = key
        .seal_object_chunk("join-requests/42", b"cleartext prefix", 0, 0, b"Carol")
        .unwrap();
    assert_eq!(
        key.open_object_chunk("join-requests/42", b"cleartext prefix", 0, 0, &sealed)
            .unwrap(),
        b"Carol"
    );
    assert!(key
        .open_object_chunk("join-requests/43", b"cleartext prefix", 0, 0, &sealed)
        .is_err());
    assert!(InviteSecret::generate()
        .unwrap()
        .join_request_key()
        .open_object_chunk("join-requests/42", b"cleartext prefix", 0, 0, &sealed)
        .is_err());
    let same = InviteSecret::from_bytes(invite.to_secret_bytes().as_bytes().try_into().unwrap());
    assert_eq!(
        same.join_request_key()
            .open_object_chunk("join-requests/42", b"cleartext prefix", 0, 0, &sealed)
            .unwrap(),
        b"Carol"
    );
}

#[test]
fn file_chunks_repeat_only_for_the_same_key_name_index_and_bytes() {
    let key = StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [17; 32]).derive();
    let other_key = StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [18; 32]).derive();
    let name = key.file_name(&ContentHash::from_bytes([4; 32]));
    let other_name = key.file_name(&ContentHash::from_bytes([5; 32]));
    for index in [0, 1, 7, u64::MAX] {
        for plaintext in [b"".as_slice(), b"one chunk"] {
            let sealed = key.seal_chunk(&name, index, plaintext);
            assert_eq!(key.open_chunk(&name, index, &sealed).unwrap(), plaintext);
            assert_eq!(sealed, key.seal_chunk(&name, index, plaintext));
            assert_ne!(sealed, other_key.seal_chunk(&name, index, plaintext));
            assert_ne!(sealed, key.seal_chunk(&other_name, index, plaintext));
            assert_ne!(sealed, key.seal_chunk(&name, index ^ 1, plaintext));
            for result in [
                key.open_chunk(&other_name, index, &sealed),
                key.open_chunk(&name, index ^ 1, &sealed),
                other_key.open_chunk(&name, index, &sealed),
            ] {
                assert!(matches!(result, Err(CryptoError::Authentication)));
            }
            for i in 0..sealed.len() {
                let mut altered = sealed.clone();
                altered[i] ^= 1;
                assert!(matches!(
                    key.open_chunk(&name, index, &altered),
                    Err(CryptoError::Authentication)
                ));
            }
        }
    }
}

#[test]
fn chunks_authenticate_the_name_and_index_in_addition_to_the_nonce() {
    let keys = StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [17; 32]).derive();
    let name = StoredFileName::from_bytes([8; 32]);
    let sealed = keys.seal_chunk(&name, 7, b"payload");
    let nonce = keys.chunk_nonce(&name, 7);
    let aad = cipher::context(&[b"coven/chunk/v1", name.as_bytes(), &7u64.to_le_bytes()]);
    assert_eq!(
        cipher::open(&keys.encryption.0, &nonce, &aad, &sealed).unwrap(),
        b"payload"
    );
    // Keep the right nonce and key: these failures specifically exercise AAD.
    for aad in [
        chunk_aad(&name, 8),
        chunk_aad(&StoredFileName::from_bytes([9; 32]), 7),
        cipher::context(&[b"coven/object/v1", name.as_bytes(), &7u64.to_le_bytes()]),
    ] {
        assert!(matches!(
            cipher::open(&keys.encryption.0, &nonce, &aad, &sealed),
            Err(CryptoError::Authentication)
        ));
    }
}

#[test]
#[should_panic(expected = "storage paths must be nonempty")]
fn objects_cannot_be_sealed_without_a_storage_path() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let key = StoreKey::generate(KeyId(key_ids.new_id()))
        .unwrap()
        .derive();
    let _sealed = key.seal_object_chunk("", b"cleartext prefix", 0, 0, b"payload");
}

#[test]
#[should_panic(expected = "storage paths must be nonempty")]
fn objects_cannot_be_opened_without_a_storage_path() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let key = StoreKey::generate(KeyId(key_ids.new_id()))
        .unwrap()
        .derive();
    let sealed = key
        .seal_object_chunk("objects/1", b"cleartext prefix", 0, 0, b"payload")
        .unwrap();
    let _opened = key.open_object_chunk("", b"cleartext prefix", 0, 0, &sealed);
}
