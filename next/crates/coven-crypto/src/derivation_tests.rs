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
            "6e4806cf1fac1920e0d53eaca1d210cf46e8f507ae4f145db33af65af7a81f60",
        ),
        (
            APP_DATA,
            b"coven/app-data/v1",
            "871d415cbde4e9545645d52de9825f4db26b81419b460bf8f746edf5e39c4bf9",
        ),
        (
            NAMING,
            b"coven/naming/v1",
            "5bc6b49fd5a2bf93a95d600007f9964ad8ce2c2e3eccb9f708b444fc62522a6a",
        ),
        (
            FILE_NONCES,
            b"coven/file-nonces/v1",
            "2a9c072dee57a1c050372bdd247cde778f55c82943b8f51e6c1fb4d4365bd68a",
        ),
        (
            FINGERPRINTS,
            b"coven/fingerprints/v1",
            "06e608064126fc629ea0520d046942bab83c691d600a95a2f8f0368b8627d0fa",
        ),
        (
            JOIN_REQUEST,
            b"coven/join-request/v1",
            "2692bf8b7e07bf0df6a8d63d23214ffaccd0e1556fbe51a467b443da48d07a40",
        ),
        (
            SEALED_BOX,
            b"coven/sealed-box/v1",
            "6a17f4475434ab900c563f04d475aa0ecd60f1bca93bb3eb8cdaf10a73d3949d",
        ),
    ];
    for (label, expected, answer) in vectors {
        assert_eq!(label, expected);
        assert_eq!(hex::encode(derive(&[17; 32], label).as_ref()), *answer);
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
        "418312d742b95e056668ba4f91c034c0a49cb5159067a43f67b7cf73ce8c6956"
    );
    assert_eq!(
        hex::encode(keys.chunk_nonce(&name, 7)),
        "12358369de655ceb9f1358b5720a43ce0700000000000000"
    );
    // Independently calculated with libsodium's XChaCha20-Poly1305, using
    // length-framed domain, raw name bytes and the little-endian chunk index.
    assert_eq!(
        hex::encode(keys.seal_chunk(&name, 7, b"hello file")),
        "6ec239349217077c8df98772b3ef4c2dc6c69d9a26f9cae3363e"
    );
    let mut fingerprint = keys.fingerprint_hasher();
    fingerprint.update(b"agreed state");
    assert_eq!(
        hex::encode(fingerprint.finish().as_bytes()),
        "0a4f4296e6d9572ff2ccc6bbd7b70a470c7335fe3eda75e2d958922d365dd091"
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
            let sealed = key.seal_object("devices/phone/3", plaintext).unwrap();
            assert_eq!(
                key.open_object("devices/phone/3", &sealed).unwrap(),
                plaintext
            );
            assert_ne!(
                sealed,
                key.seal_object("devices/phone/3", plaintext).unwrap()
            );
            assert!(matches!(
                key.open_object("devices/phone/4", &sealed),
                Err(CryptoError::Authentication)
            ));
            for i in 0..sealed.len() {
                let mut altered = sealed.clone();
                altered[i] ^= 1;
                assert!(matches!(
                    key.open_object("devices/phone/3", &altered),
                    Err(CryptoError::Authentication)
                ));
                assert!(key.open_object("devices/phone/3", &sealed[..i]).is_err());
            }
            let mut trailing = sealed;
            trailing.push(0);
            assert!(key.open_object("devices/phone/3", &trailing).is_err());
        }
    }
}

#[test]
fn a_join_request_is_bound_to_its_invite_and_path() {
    let invite = InviteSecret::generate().unwrap();
    let key = invite.join_request_key();
    let sealed = key.seal_object("join-requests/42", b"Carol").unwrap();
    assert_eq!(
        key.open_object("join-requests/42", &sealed).unwrap(),
        b"Carol"
    );
    assert!(key.open_object("join-requests/43", &sealed).is_err());
    assert!(InviteSecret::generate()
        .unwrap()
        .join_request_key()
        .open_object("join-requests/42", &sealed)
        .is_err());
    let same = InviteSecret::from_bytes(invite.to_secret_bytes().as_bytes().try_into().unwrap());
    assert_eq!(
        same.join_request_key()
            .open_object("join-requests/42", &sealed)
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
    let _sealed = key.seal_object("", b"payload");
}

#[test]
#[should_panic(expected = "storage paths must be nonempty")]
fn objects_cannot_be_opened_without_a_storage_path() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let key = StoreKey::generate(KeyId(key_ids.new_id()))
        .unwrap()
        .derive();
    let sealed = key.seal_object("objects/1", b"payload").unwrap();
    let _opened = key.open_object("", &sealed);
}
