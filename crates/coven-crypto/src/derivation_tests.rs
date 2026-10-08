use super::*;
use crate::{CircleId, CircleKey, InviteSecret, StoreKey};
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

#[test]
fn retry_nonces_are_pinned_and_separate_keys_paths_sections_and_chunks() {
    let key = DerivedKeys::new(&[17; 32]);
    let path = "devices/1/3";
    let sealed = key.reseal_object_chunk(path, b"prefix", 1, 2, b"payload");
    // Independently calculated with Python's hashlib/hmac and D11's encoding.
    assert_eq!(
        hex::encode(&sealed[..24]),
        "384981a444444a653c9ae5752659daf9613bf9c415916342"
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
        key.reseal_object_chunk("devices/1/4", b"prefix", 1, 2, b"payload"),
        key.reseal_object_chunk("store-log/1/3", b"prefix", 1, 2, b"payload"),
        key.reseal_object_chunk(path, b"prefix", 2, 2, b"payload"),
        key.reseal_object_chunk(path, b"prefix", 1, 3, b"payload"),
        DerivedKeys::new(&[18; 32]).reseal_object_chunk(path, b"prefix", 1, 2, b"payload"),
    ] {
        assert_ne!(sealed[..24], other[..24]);
    }
    assert!(key
        .open_object_chunk(path, b"changed prefix", 1, 2, &sealed)
        .is_err());
}
