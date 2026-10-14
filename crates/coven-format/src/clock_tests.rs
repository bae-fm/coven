use super::*;
use crate::sealed_single::SingleChunkPrefix;
use crate::test_utils::{member, member_keys, replacement, store_key, STORE_ID};
use crate::tests::{hex, mutations};
use crate::value::{EntryId, EntryPositions};
use coven_foundation::id_source::{DeviceId, KeyId, StoreId};

fn path() -> ObjectPath {
    ObjectPath::clock(STORE_ID, DeviceId(1))
}

// Decrypt, then verify with the signer the caller resolved from the view.
fn open_signed(
    bytes: &[u8],
    path: &ObjectPath,
    key: &StoreKey,
    signer: &coven_crypto::SigningKey,
) -> Result<ClockObservation, ClockError> {
    open(bytes, path, key)?.verify(signer)
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
    assert_eq!(bytes.len(), 19 + 4 + 24 + 35 + 16 + 64);
    assert_eq!(&bytes[..3], &[39, 0, 1]);
    // The decrypted view locates the signer before the signature is checked.
    let opened = open(&bytes, &path(), &store_key()).unwrap();
    assert_eq!(opened.observation, crate::test_utils::clock());
    let clock = opened.verify(&member().signing).unwrap();
    assert_eq!(
        clock,
        ClockObservation {
            device: DeviceId(1),
            store_log_read: EntryPositions(vec![EntryId {
                device: DeviceId(1),
                number: 2
            }]),
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
        assert!(open_signed(changed, &path(), &key, &author).is_err());
    });
    assert!(matches!(
        open_signed(&[], &path(), &key, &author),
        Err(ClockError::Format(Error::Truncated))
    ));
}

#[test]
fn clock_binds_its_device_store_object_kind_and_signing_key() {
    let bytes = hex(include_str!("../fixtures/sealed-clock.hex"));
    let key = store_key();
    let author = member().signing;
    for other in [
        ObjectPath::clock(STORE_ID, DeviceId(2)),
        ObjectPath::clock(StoreId(uuid::Uuid::from_bytes([0x20; 16])), DeviceId(1)),
        ObjectPath::positions(STORE_ID, DeviceId(1)),
    ] {
        // The chunk's associated data binds the path before any signer is used.
        assert!(matches!(
            open_signed(&bytes, &other, &key, &author),
            Err(ClockError::Crypto(CryptoError::Authentication))
        ));
    }
    // The same member's replacement key is not the key that signed it.
    for other in [
        replacement().signing,
        crate::test_utils::keys_from_seed(0x44)
            .public_keys()
            .signing,
    ] {
        assert!(matches!(
            open_signed(&bytes, &path(), &key, &other),
            Err(ClockError::Crypto(CryptoError::Signature))
        ));
    }
}

#[test]
fn clock_requires_the_declared_key_and_authenticated_ciphertext() {
    let bytes = hex(include_str!("../fixtures/sealed-clock.hex"));
    let other_id = StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([2; 16])), [0x11; 32]);
    assert!(matches!(
        open_signed(&bytes, &path(), &other_id, &member().signing),
        Err(ClockError::WrongIdentity)
    ));
    let other_secret = StoreKey::from_bytes(store_key().id(), [0x12; 32]);
    assert!(matches!(
        open_signed(&bytes, &path(), &other_secret, &member().signing),
        Err(ClockError::Crypto(CryptoError::Authentication))
    ));
    let mut damaged = bytes[..bytes.len() - 64].to_vec();
    damaged[19 + 4 + 24] ^= 1;
    assert!(matches!(
        open_signed(
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
            open_signed(&bytes, &wrong, &store_key(), &member().signing),
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
        open_signed(&wrong_envelope, &path(), &key, &author),
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
        open_signed(&wrong_frame, &path(), &key, &author),
        Err(ClockError::Format(Error::Invalid {
            field: "clock frame",
            rule: Rule::Kind
        }))
    ));
    for length in [0, 7, 9, 16, 27, 29] {
        let mut malformed = frame[..7].to_vec();
        malformed[3..7].copy_from_slice(&(length as u32).to_be_bytes());
        malformed.extend(vec![0; length]);
        let bytes = seal(
            SingleChunkPrefix::ClockObservation(key.id()),
            &path(),
            &malformed,
        );
        assert!(matches!(
            open_signed(&bytes, &path(), &key, &author),
            Err(ClockError::Format(Error::Truncated | Error::TrailingBytes))
        ));
    }
}

#[test]
fn clock_device_uses_the_full_unsigned_range() {
    for device in [0, u64::MAX] {
        let clock = ClockObservation {
            device: DeviceId(device),
            store_log_read: EntryPositions(Vec::new()),
        };
        let frame = Object::ClockObservation(clock.clone()).encode().unwrap();
        assert_eq!(&frame[..7], &[12, 0, 1, 0, 0, 0, 12]);
        assert_eq!(&frame[7..15], &device.to_be_bytes());
        assert_eq!(&frame[15..], &[0; 4]);
        let path = ObjectPath::clock(STORE_ID, clock.device);
        let bytes = seal(
            SingleChunkPrefix::ClockObservation(store_key().id()),
            &path,
            &frame,
        );
        assert_eq!(
            open_signed(&bytes, &path, &store_key(), &member().signing).unwrap(),
            clock
        );
    }
}
