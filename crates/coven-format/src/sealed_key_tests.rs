use super::*;
use crate::{test_utils, tests::hex};
use coven_crypto::StoreKeyring;
use coven_crypto::{seal_circle_key, seal_store_key, MaterialError, MemberKeys};
use coven_foundation::id_source::CircleId;
use uuid::Uuid;

const PATH: &str = "10101010-1010-1010-1010-101010101010/keys/7/9";

fn circle() -> Audience {
    Audience::Circle(CircleId(Uuid::from_u128(2)))
}

fn prefix(audience: Audience) -> KeyCopyPrefix {
    let member = crate::test_utils::identity();
    KeyCopyPrefix {
        audience,
        key: KeyId(Uuid::from_u128(3)),
        member: member.member(),
        sealing: sealing(&member),
    }
}

fn sealing(member: &MemberIdentity) -> SealingKey {
    member.keys().public_keys().sealing
}

// The test member's id with another sealing pair: a different credential.
fn other_credential() -> MemberIdentity {
    let mut seeds = crate::test_utils::member_keys()
        .to_secret_bytes()
        .as_bytes()
        .to_vec();
    seeds[37..69].fill(0x35);
    MemberIdentity::new(
        test_utils::member_id(),
        MemberKeys::from_secret_bytes(&seeds).unwrap(),
    )
}

// An anonymous sender can seal any material under any claimed routing facts.
fn seal(
    routing: &KeyCopyPrefix,
    audience: &Audience,
    key: KeyId,
    material: [u8; 32],
    recipient: &SealingKey,
) -> Vec<u8> {
    let prefix = routing.encode().unwrap();
    let body = match audience {
        Audience::Store => seal_store_key(
            &StoreKey::from_bytes(key, material),
            recipient,
            PATH,
            &prefix,
        ),
        Audience::Circle(circle) => seal_circle_key(
            &CircleKey::from_bytes(*circle, key, material),
            recipient,
            PATH,
            &prefix,
        ),
    }
    .unwrap();
    routing.encode_box(&body).unwrap()
}

fn open(envelope: &SealedKey<'_>, member: &MemberIdentity, path: &str) -> Result<(), CryptoError> {
    match envelope.prefix().audience {
        Audience::Store => envelope.open_store_key(member, path).map(|_| ()),
        Audience::Circle(_) => envelope.open_circle_key(member, path).map(|_| ()),
    }
}

// Open the box under exactly these prefix bytes, skipping the recipient check.
fn open_box(audience: &Audience, member: &MemberIdentity, bytes: &[u8]) -> Result<(), CryptoError> {
    let length = prefix(audience.clone()).length();
    let (prefix, sealed) = bytes.split_at(length);
    match audience {
        Audience::Store => member
            .keys()
            .open_store_key(PATH, prefix, sealed)
            .map(|_| ()),
        Audience::Circle(_) => member
            .keys()
            .open_circle_key(PATH, prefix, sealed)
            .map(|_| ()),
    }
}

#[test]
fn every_routing_field_and_exact_numbered_path_are_authenticated() {
    let member = crate::test_utils::identity();
    // Another member holding the same sealing pair: only the id differs.
    let other_identity =
        MemberIdentity::new(test_utils::other_member_id(), test_utils::member_keys());
    assert_eq!(sealing(&member), sealing(&other_identity));
    let other_credential = other_credential();
    assert_eq!(member.member(), other_credential.member());
    assert_ne!(sealing(&member), sealing(&other_credential));
    for audience in [Audience::Store, circle()] {
        let prefix = prefix(audience.clone());
        let bytes = seal(&prefix, &audience, prefix.key, [17; 32], &sealing(&member));
        let envelope = SealedKey::decode(&bytes).unwrap();
        assert_eq!(envelope.prefix(), &prefix);
        open(&envelope, &member, PATH).unwrap();
        for path in [
            "10101010-1010-1010-1010-101010101010/keys/8/9",
            "10101010-1010-1010-1010-101010101010/keys/7/10",
            "20202020-2020-2020-2020-202020202020/keys/7/9",
        ] {
            assert!(matches!(
                open(&envelope, &member, path),
                Err(CryptoError::Authentication)
            ));
        }
        for changed in [
            KeyCopyPrefix {
                key: KeyId(Uuid::from_u128(4)),
                ..prefix.clone()
            },
            KeyCopyPrefix {
                member: other_identity.member(),
                ..prefix.clone()
            },
            KeyCopyPrefix {
                sealing: sealing(&other_credential),
                ..prefix.clone()
            },
        ] {
            let changed_bytes = changed.encode_box(&bytes[prefix.length()..]).unwrap();
            let recipient = [&member, &other_identity, &other_credential]
                .into_iter()
                .find(|keys| changed.addressed_to(&keys.member(), &sealing(keys)))
                .unwrap();
            assert!(matches!(
                open(&SealedKey::decode(&changed_bytes).unwrap(), recipient, PATH),
                Err(CryptoError::Authentication)
            ));
            // The box itself binds every routing field, not only the recipient check.
            assert!(matches!(
                open_box(&audience, &member, &changed_bytes),
                Err(CryptoError::Authentication)
            ));
        }
        if let Audience::Circle(_) = audience {
            let changed = KeyCopyPrefix {
                audience: Audience::Circle(CircleId(Uuid::from_u128(8))),
                ..prefix.clone()
            };
            let changed_bytes = changed.encode_box(&bytes[prefix.length()..]).unwrap();
            assert!(matches!(
                open(&SealedKey::decode(&changed_bytes).unwrap(), &member, PATH),
                Err(CryptoError::Authentication)
            ));
        }
        // Changing the store/circle tag cannot reuse the other kind's payload length.
        let mut changed = bytes.clone();
        changed[3] ^= 1;
        assert!(SealedKey::decode(&changed).is_err());
    }
}

#[test]
fn foreign_recipient_routing_is_visible_even_when_the_box_is_forged() {
    let member = crate::test_utils::identity();
    let foreign = MemberIdentity::generate(test_utils::other_member_id()).unwrap();
    for audience in [Audience::Store, circle()] {
        let prefix = KeyCopyPrefix {
            member: foreign.member(),
            ..prefix(audience.clone())
        };
        let mut bytes = seal(&prefix, &audience, prefix.key, [17; 32], &sealing(&foreign));
        for forged in [false, true] {
            if forged {
                bytes[prefix.length()..].fill(0);
            }
            let envelope = SealedKey::decode(&bytes).unwrap();
            assert_eq!(envelope.prefix(), &prefix);
            assert_eq!(envelope.encode(), bytes);
            assert!(matches!(
                open(&envelope, &member, PATH),
                Err(CryptoError::Authentication)
            ));
        }
    }
}

#[test]
fn authenticated_payload_must_match_its_claimed_key_and_circle() {
    let member = crate::test_utils::identity();
    for audience in [Audience::Store, circle()] {
        let prefix = prefix(audience.clone());
        let bytes = seal(
            &prefix,
            &audience,
            KeyId(Uuid::from_u128(4)),
            [17; 32],
            &sealing(&member),
        );
        assert!(matches!(
            open(&SealedKey::decode(&bytes).unwrap(), &member, PATH),
            Err(CryptoError::KeyIdentity)
        ));
    }
    let prefix = prefix(circle());
    let bytes = seal(
        &prefix,
        &Audience::Circle(CircleId(Uuid::from_u128(8))),
        prefix.key,
        [17; 32],
        &sealing(&member),
    );
    assert!(matches!(
        open(&SealedKey::decode(&bytes).unwrap(), &member, PATH),
        Err(CryptoError::KeyIdentity)
    ));
}

#[test]
fn matching_sealing_keys_do_not_override_the_addressed_member() {
    let member = crate::test_utils::identity();
    let other = test_utils::other_member_id();
    let prefix = KeyCopyPrefix {
        member: other,
        ..prefix(Audience::Store)
    };
    let bytes = seal(
        &prefix,
        &Audience::Store,
        prefix.key,
        [17; 32],
        &sealing(&member),
    );
    // This box really decrypts with our sealing secret, but names another member.
    member
        .keys()
        .open_store_key(PATH, &bytes[..prefix.length()], &bytes[prefix.length()..])
        .unwrap();
    assert!(matches!(
        SealedKey::decode(&bytes)
            .unwrap()
            .open_store_key(&member, PATH),
        Err(CryptoError::Authentication)
    ));
}

#[test]
fn opened_material_must_match_the_introductions_secret_hash() {
    let member = crate::test_utils::identity();
    for audience in [Audience::Store, circle()] {
        let prefix = prefix(audience.clone());
        let expected = StoreKey::from_bytes(prefix.key, [17; 32]).hash();
        for material in [[17; 32], [18; 32]] {
            let bytes = seal(&prefix, &audience, prefix.key, material, &sealing(&member));
            let envelope = SealedKey::decode(&bytes).unwrap();
            let result = match audience {
                Audience::Store => envelope
                    .open_store_key(&member, PATH)
                    .unwrap()
                    .check_hash(&expected),
                Audience::Circle(_) => envelope
                    .open_circle_key(&member, PATH)
                    .unwrap()
                    .check_hash(&expected),
            };
            if material == [17; 32] {
                result.unwrap();
            } else {
                assert!(matches!(
                    result,
                    Err(MaterialError::StoreKeyConflict(_)
                        | MaterialError::CircleKeyConflict { .. })
                ));
            }
        }
    }
}

#[test]
fn malformed_routing_and_box_lengths_are_rejected_without_decryption() {
    for audience in [Audience::Store, circle()] {
        let prefix = prefix(audience);
        let bytes = prefix.encode_box(&vec![0; prefix.box_length()]).unwrap();
        for end in 0..bytes.len() {
            assert!(SealedKey::decode(&bytes[..end]).is_err());
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(matches!(
            SealedKey::decode(&extra),
            Err(Error::TrailingBytes)
        ));
        for tag in [2, 255] {
            let mut changed = bytes.clone();
            changed[3] = tag;
            assert!(matches!(
                SealedKey::decode(&changed),
                Err(Error::UnknownTag {
                    field: "audience",
                    ..
                })
            ));
        }
        // A member id is any UUID: no key validation applies to it.
        let mut changed = bytes.clone();
        changed[prefix.length() - 48..prefix.length() - 32].fill(0);
        assert_eq!(
            SealedKey::decode(&changed).unwrap().prefix().member,
            MemberId(Uuid::nil())
        );
        for length in [prefix.box_length() - 1, prefix.box_length() + 1] {
            assert!(prefix.encode_box(&vec![0; length]).is_err());
        }
    }
}

#[test]
fn key_copy_routing_is_visible_without_the_recipient_secret() {
    let member = test_utils::identity();
    let key = test_utils::store_key();
    let circle_key = CircleKey::from_bytes(CircleId(Uuid::from_u128(2)), key.id(), [18; 32]);
    for audience in [Audience::Store, circle()] {
        let routing = KeyCopyPrefix {
            audience: audience.clone(),
            key: key.id(),
            member: member.member(),
            sealing: sealing(&member),
        };
        let sealed = match audience {
            Audience::Store => {
                seal_store_key(&key, &routing.sealing, PATH, &routing.encode().unwrap())
            }
            Audience::Circle(_) => seal_circle_key(
                &circle_key,
                &routing.sealing,
                PATH,
                &routing.encode().unwrap(),
            ),
        }
        .unwrap();
        let bytes = routing.encode_box(&sealed).unwrap();
        // kind | version | audience | key | member | sealing, then the box.
        let mut prefix = vec![37, 0, 1];
        match audience {
            Audience::Store => prefix.push(0),
            Audience::Circle(circle) => {
                prefix.push(1);
                prefix.extend_from_slice(circle.0.as_bytes());
            }
        }
        prefix.extend_from_slice(key.id().0.as_bytes());
        prefix.extend_from_slice(member.member().0.as_bytes());
        prefix.extend_from_slice(sealing(&member).as_bytes());
        assert_eq!(routing.encode().unwrap(), prefix);
        assert_eq!(&bytes[..prefix.len()], prefix);
        assert_eq!(bytes.len(), prefix.len() + routing.box_length());
        assert_eq!(&bytes[prefix.len()..], sealed);
        assert_eq!(SealedKey::decode(&bytes).unwrap().prefix(), &routing);
    }
}

#[test]
fn a_copy_to_another_sealing_key_of_the_member_is_not_addressed_to_this_one() {
    let member = crate::test_utils::identity();
    let other = other_credential();
    for audience in [Audience::Store, circle()] {
        let routing = KeyCopyPrefix {
            sealing: sealing(&other),
            ..prefix(audience.clone())
        };
        let bytes = seal(&routing, &audience, routing.key, [17; 32], &sealing(&other));
        let envelope = SealedKey::decode(&bytes).unwrap();
        assert!(!envelope
            .prefix()
            .addressed_to(&member.member(), &sealing(&member)));
        assert!(envelope
            .prefix()
            .addressed_to(&other.member(), &sealing(&other)));
        assert!(matches!(
            open(&envelope, &member, PATH),
            Err(CryptoError::Authentication)
        ));
        open(&envelope, &other, PATH).unwrap();
    }
}

// Public test material, independently sealed using Python hashlib/hmac and libsodium.
// Both copies go to Ana's one stable id: the store key to her original
// credential (seeds 0x33), the circle key to her replacement (seeds 0x34).
// Ephemeral seeds are 0x55/0x56, nonces 0x44/0x45.
#[test]
fn sealed_key_fixtures_open_and_reencode_without_losing_their_random_bytes() {
    let original = crate::test_utils::identity();
    let replaced = MemberIdentity::new(test_utils::member_id(), test_utils::replacement_keys());
    for (circle, text) in [
        (false, include_str!("../fixtures/sealed-store-key.hex")),
        (true, include_str!("../fixtures/sealed-circle-key.hex")),
    ] {
        let bytes = hex(text);
        let key_id = KeyId(Uuid::from_bytes([if circle { 3 } else { 1 }; 16]));
        let circle_id = CircleId(Uuid::from_bytes([2; 16]));
        let path = if circle {
            "10101010-1010-1010-1010-101010101010/keys/7/10"
        } else {
            "10101010-1010-1010-1010-101010101010/keys/7/9"
        };
        assert_eq!(SealedKey::decode(&bytes).unwrap().encode(), bytes);
        let (member, other) = if circle {
            (&replaced, &original)
        } else {
            (&original, &replaced)
        };
        let routing = SealedKey::decode(&bytes).unwrap().prefix().clone();
        assert_eq!(routing.member, test_utils::member_id());
        assert_eq!(routing.sealing, sealing(member));
        // The same id's other credential is not this copy's recipient.
        assert!(!routing.addressed_to(&other.member(), &sealing(other)));
        let initial = StoreKey::from_bytes(KeyId(Uuid::from_bytes([1; 16])), [17; 32]);
        let mut expected = StoreKeyring::new(initial.clone());
        let actual = if circle {
            let key = SealedKey::decode(&bytes)
                .unwrap()
                .open_circle_key(member, path)
                .unwrap();
            assert_eq!((key.circle(), key.id()), (circle_id, key_id));
            expected
                .insert_circle_key(CircleKey::from_bytes(circle_id, key_id, [18; 32]))
                .unwrap();
            let mut actual = StoreKeyring::new(initial);
            actual.insert_circle_key(key).unwrap();
            actual
        } else {
            let key = SealedKey::decode(&bytes)
                .unwrap()
                .open_store_key(member, path)
                .unwrap();
            assert_eq!(key.id(), key_id);
            StoreKeyring::new(key)
        };
        assert_eq!(
            actual.to_secret_bytes().as_bytes(),
            expected.to_secret_bytes().as_bytes()
        );
        let open = |bytes: &[u8]| -> Result<(), Box<dyn std::error::Error>> {
            let envelope = SealedKey::decode(bytes)?;
            if circle {
                envelope.open_circle_key(member, path)?;
            } else {
                envelope.open_store_key(member, path)?;
            }
            Ok(())
        };
        for end in 0..bytes.len() {
            assert!(open(&bytes[..end]).is_err());
            check_encoding(&bytes[..end]);
        }
        let mut changed = bytes.clone();
        for index in 0..bytes.len() {
            for bit in 0..8 {
                changed[index] ^= 1 << bit;
                assert!(open(&changed).is_err());
                check_encoding(&changed);
                changed[index] ^= 1 << bit;
            }
        }
        changed.push(0);
        assert!(open(&changed).is_err());
        assert!(SealedKey::decode(&changed).is_err());
        changed = bytes.clone();
        changed[2] = 2;
        assert!(matches!(
            SealedKey::decode(&changed),
            Err(Error::UnsupportedVersion(2))
        ));
    }
}

fn check_encoding(bytes: &[u8]) {
    if let Ok(key) = SealedKey::decode(bytes) {
        assert_eq!(key.encode(), bytes);
    }
}

#[test]
fn a_circle_copy_is_the_longest_copy() {
    let member = crate::test_utils::identity();
    for audience in [Audience::Store, circle()] {
        let prefix = prefix(audience.clone());
        let bytes = seal(&prefix, &audience, prefix.key, [17; 32], &sealing(&member));
        match audience {
            Audience::Store => assert!(bytes.len() < MAX_KEY_COPY_OBJECT),
            Audience::Circle(_) => assert_eq!(bytes.len(), MAX_KEY_COPY_OBJECT),
        }
    }
}
