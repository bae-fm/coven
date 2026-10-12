use super::*;
use crate::sealed_single::SingleChunkPrefix;
use crate::test_utils::{member, member_keys, store_key, STORE_ID};
use crate::tests::{hex, mutations};
use coven_foundation::id_source::{DeviceId, KeyId, StoreId};

fn path() -> ObjectPath {
    ObjectPath::clock(STORE_ID, DeviceId(1))
}

// Compose the production envelope and crypto primitives so malformed plaintext
// and identities can carry valid encryption and signatures into the real decoder.
fn seal(prefix: SingleChunkPrefix, path: &ObjectPath, frame: &[u8]) -> Vec<u8> {
    let chunk = store_key()
        .derive()
        .seal_object_chunk(path.as_str(), &prefix.encode().unwrap(), 0, 0, frame)
        .unwrap();
    sign(path, prefix.encode_chunk(&chunk).unwrap())
}

fn sign(path: &ObjectPath, mut bytes: Vec<u8>) -> Vec<u8> {
    let mut hash = ObjectHasher::new();
    hash.update(&bytes);
    bytes.extend_from_slice(
        member_keys()
            .sign_object(path.as_str(), &hash.finish())
            .as_bytes(),
    );
    bytes
}

#[test]
fn clock_fixture_authenticates_and_preserves_its_wire_bytes() {
    // Independently generated with Python hashlib/hmac and libsodium: store key
    // 0x11, key id 0x01, signing seed 0x33, nonce 0x39 (all public test material).
    let bytes = hex(include_str!("../fixtures/sealed-clock.hex"));
    assert_eq!(bytes.len(), 19 + 4 + 24 + 15 + 16 + 64);
    assert_eq!(&bytes[..3], &[39, 0, 1]);
    let clock = open(&bytes, &path(), &store_key(), &member().signing).unwrap();
    assert_eq!(
        clock,
        ClockObservation {
            device: DeviceId(1)
        }
    );
    assert_eq!(
        Object::ClockObservation(clock).encode().unwrap(),
        hex(include_str!("../fixtures/clock.hex"))
    );
    let envelope = SingleChunkObject::decode(&bytes).unwrap();
    assert_eq!(envelope.encode().unwrap(), bytes);
    assert_eq!(
        envelope.prefix(),
        SingleChunkPrefix::ClockObservation(store_key().id())
    );
    assert_eq!(&envelope.chunk()[..24], &[0x39; 24]);
}

#[test]
fn clock_refuses_every_truncation_bit_change_and_extra_byte() {
    let bytes = hex(include_str!("../fixtures/sealed-clock.hex"));
    let key = store_key();
    let author = member().signing;
    mutations(&bytes, |changed| {
        assert!(open(changed, &path(), &key, &author).is_err());
    });
    assert!(matches!(
        open(&[], &path(), &key, &author),
        Err(ClockError::Format(Error::Truncated))
    ));
}

#[test]
fn clock_signature_binds_the_member_device_store_and_object_kind() {
    let bytes = hex(include_str!("../fixtures/sealed-clock.hex"));
    let key = store_key();
    let author = member().signing;
    for other in [
        ObjectPath::clock(STORE_ID, DeviceId(2)),
        ObjectPath::clock(StoreId(uuid::Uuid::from_bytes([0x20; 16])), DeviceId(1)),
        ObjectPath::positions(STORE_ID, DeviceId(1)),
    ] {
        assert!(matches!(
            open(&bytes, &other, &key, &author),
            Err(ClockError::Crypto(CryptoError::Signature))
        ));
    }
    let mut secret = b"CVMK\x01".to_vec();
    secret.extend_from_slice(&[0x44; 64]);
    let other = coven_crypto::MemberKeys::from_secret_bytes(&secret).unwrap();
    assert!(matches!(
        open(&bytes, &path(), &key, &other.member_id()),
        Err(ClockError::Crypto(CryptoError::Signature))
    ));
}

#[test]
fn clock_requires_the_declared_key_and_authenticated_ciphertext() {
    let bytes = hex(include_str!("../fixtures/sealed-clock.hex"));
    let other_id = StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([2; 16])), [0x11; 32]);
    assert!(matches!(
        open(&bytes, &path(), &other_id, &member().signing),
        Err(ClockError::WrongIdentity)
    ));
    let other_secret = StoreKey::from_bytes(store_key().id(), [0x12; 32]);
    assert!(matches!(
        open(&bytes, &path(), &other_secret, &member().signing),
        Err(ClockError::Crypto(CryptoError::Authentication))
    ));
    let mut damaged = bytes[..bytes.len() - 64].to_vec();
    damaged[19 + 4 + 24] ^= 1;
    assert!(matches!(
        open(
            &sign(&path(), damaged),
            &path(),
            &store_key(),
            &member().signing
        ),
        Err(ClockError::Crypto(CryptoError::Authentication))
    ));
}

#[test]
fn authenticated_clock_payload_must_match_the_exact_clock_path() {
    let frame = hex(include_str!("../fixtures/clock.hex"));
    for wrong in [
        ObjectPath::clock(STORE_ID, DeviceId(2)),
        ObjectPath::positions(STORE_ID, DeviceId(1)),
        ObjectPath::store_log(STORE_ID, DeviceId(1), std::num::NonZeroU64::MIN),
    ] {
        let bytes = seal(
            SingleChunkPrefix::ClockObservation(store_key().id()),
            &wrong,
            &frame,
        );
        assert!(matches!(
            open(&bytes, &wrong, &store_key(), &member().signing),
            Err(ClockError::WrongIdentity)
        ));
    }
}

#[test]
fn clock_rejects_other_envelopes_frames_and_payload_lengths() {
    let key = store_key();
    let author = member().signing;
    let frame = hex(include_str!("../fixtures/clock.hex"));
    let wrong_envelope = seal(
        SingleChunkPrefix::PostedPositions(key.id()),
        &path(),
        &frame,
    );
    assert!(matches!(
        open(&wrong_envelope, &path(), &key, &author),
        Err(ClockError::Format(Error::Invalid {
            field: "clock object",
            rule: Rule::Kind
        }))
    ));
    let other_frame = Object::StoreLog(crate::test_utils::store_log())
        .encode()
        .unwrap();
    let wrong_frame = seal(
        SingleChunkPrefix::ClockObservation(key.id()),
        &path(),
        &other_frame,
    );
    assert!(matches!(
        open(&wrong_frame, &path(), &key, &author),
        Err(ClockError::Format(Error::Invalid {
            field: "clock frame",
            rule: Rule::Kind
        }))
    ));
    for length in [0, 7, 9, 16] {
        let mut malformed = frame[..7].to_vec();
        malformed[3..7].copy_from_slice(&(length as u32).to_be_bytes());
        malformed.extend(vec![0; length]);
        let bytes = seal(
            SingleChunkPrefix::ClockObservation(key.id()),
            &path(),
            &malformed,
        );
        assert!(matches!(
            open(&bytes, &path(), &key, &author),
            Err(ClockError::Format(Error::Truncated | Error::TrailingBytes))
        ));
    }
}

#[test]
fn clock_device_uses_the_full_unsigned_range() {
    for device in [0, u64::MAX] {
        let clock = ClockObservation {
            device: DeviceId(device),
        };
        let frame = Object::ClockObservation(clock).encode().unwrap();
        assert_eq!(&frame[..7], &[12, 0, 1, 0, 0, 0, 8]);
        assert_eq!(&frame[7..], &device.to_be_bytes());
        let path = ObjectPath::clock(STORE_ID, clock.device);
        let bytes = seal(
            SingleChunkPrefix::ClockObservation(store_key().id()),
            &path,
            &frame,
        );
        assert_eq!(
            open(&bytes, &path, &store_key(), &member().signing).unwrap(),
            clock
        );
    }
}
