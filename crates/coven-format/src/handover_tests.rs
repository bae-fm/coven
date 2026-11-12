use super::*;
use crate::test_utils;
use crate::tests::hex;

fn fixture() -> Vec<u8> {
    hex(include_str!("../fixtures/handover.hex"))
}

#[test]
fn the_fixture_decodes_and_reproduces_its_bytes() {
    let bytes = fixture();
    assert_eq!(bytes.len(), LENGTH);
    assert_eq!(LENGTH, 164);
    assert_eq!(bytes[0], 41);
    assert_eq!(Handover::decode(&bytes).unwrap().encode(), bytes);
}

#[test]
fn the_fixture_opens_at_its_path_with_the_requests_sealing_secret() {
    let handover = Handover::decode(&fixture()).unwrap();
    let (member, keys) = handover
        .open(&test_utils::pairing_request_path(), &test_utils::replacement_keys())
        .unwrap();
    assert_eq!(member, test_utils::member_id());
    assert_eq!(keys.public_keys(), test_utils::member());
}

#[test]
fn the_fixture_fails_at_another_path_with_another_key_and_with_a_zero_ephemeral() {
    let handover = Handover::decode(&fixture()).unwrap();
    let path = test_utils::pairing_request_path();
    let other_path = request_path_of(0x58);
    assert!(matches!(
        handover.open(&other_path, &test_utils::replacement_keys()),
        Err(CryptoError::Authentication)
    ));
    // Eve saw the code and holds the member's old pair, or her own.
    for keys in [test_utils::member_keys(), test_utils::keys_from_seed(0x77)] {
        assert!(matches!(handover.open(&path, &keys), Err(CryptoError::Authentication)));
    }
    let mut zero = fixture();
    zero[3..35].fill(0);
    assert!(matches!(
        Handover::decode(&zero).unwrap().open(&path, &test_utils::replacement_keys()),
        Err(CryptoError::WeakSealingKey)
    ));
}

fn request_path_of(pairing: u8) -> crate::path::ObjectPath {
    crate::path::ObjectPath::join_request(
        test_utils::STORE_ID,
        coven_foundation::id_source::DeviceId(1),
        coven_foundation::id_source::InviteId(uuid::Uuid::from_bytes([pairing; 16])),
    )
}

#[test]
fn every_truncation_and_single_bit_change_fails_without_panicking() {
    let bytes = fixture();
    let path = test_utils::pairing_request_path();
    let asking = test_utils::replacement_keys();
    for end in 0..bytes.len() {
        assert!(Handover::decode(&bytes[..end]).is_err());
    }
    let mut changed = bytes.clone();
    for index in 0..bytes.len() {
        for bit in 0..8 {
            changed[index] ^= 1 << bit;
            // The clear kind and version are the prefix, and the box
            // authenticates every other byte.
            match Handover::decode(&changed) {
                Err(_) => assert!(index < 3),
                Ok(handover) => assert!(handover.open(&path, &asking).is_err()),
            }
            changed[index] ^= 1 << bit;
        }
    }
    changed.push(0);
    assert_eq!(Handover::decode(&changed), Err(Error::TrailingBytes));
}

#[test]
fn another_kinds_bytes_and_an_unsupported_version_are_refused() {
    let mut changed = fixture();
    for kind in [36, 37, 40] {
        changed[0] = kind;
        assert!(Handover::decode(&changed).is_err());
    }
    changed[0] = 41;
    changed[2] = 2;
    assert_eq!(Handover::decode(&changed), Err(Error::UnsupportedVersion(2)));
    assert!(Handover::decode(&crate::invite_end::InviteEnd { kept_until: 1 }.encode()).is_err());
}

#[test]
fn a_handover_sealed_here_opens_only_at_its_path_for_its_key() {
    let path = test_utils::pairing_request_path();
    let asking = test_utils::replacement_keys();
    let sealed = Handover::seal(
        test_utils::member_id(),
        &test_utils::member_keys(),
        &asking.public_keys().sealing,
        &path,
    )
    .unwrap();
    let bytes = sealed.encode();
    assert_eq!(bytes.len(), LENGTH);
    assert_eq!(Handover::decode(&bytes).unwrap(), sealed);
    let (member, keys) = sealed.open(&path, &asking).unwrap();
    assert_eq!(member, test_utils::member_id());
    assert_eq!(keys.public_keys(), test_utils::member());
    assert!(sealed.open(&request_path_of(0x58), &asking).is_err());
    assert!(sealed.open(&path, &test_utils::member_keys()).is_err());
    // A retry of the create repeats the bytes the write fixed: sealing again
    // chooses new randomness, so they are held, not recomputed.
    let again = Handover::seal(
        test_utils::member_id(),
        &test_utils::member_keys(),
        &asking.public_keys().sealing,
        &path,
    )
    .unwrap();
    assert_ne!(again.encode(), bytes);
}
