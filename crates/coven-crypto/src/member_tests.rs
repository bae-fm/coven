use super::*;
use crate::CircleId;
use coven_foundation::id_source::{IdSource, KeyId, MemberId};
use uuid::Uuid;

#[test]
fn member_identity_is_independent_of_its_public_keys() {
    assert_eq!(std::mem::size_of::<MemberId>(), 16);
    assert_eq!(std::mem::size_of::<SigningKey>(), 32);
    let keys = MemberKeys::generate().unwrap();
    let ana = MemberIdentity::new(MemberId(Uuid::from_u128(1)), keys.clone());
    let ben = MemberIdentity::new(MemberId(Uuid::from_u128(2)), keys);
    assert_ne!(ana.member(), ben.member());
    assert_eq!(ana.keys().public_keys(), ben.keys().public_keys());
}

#[test]
fn wrong_key_lengths_are_refused_before_opening_the_box() {
    let member = MemberKeys::generate().unwrap();
    let recipient = member.public_keys().sealing;
    let store = StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [17; 32]);
    let circle = CircleKey::from_bytes(
        CircleId(Uuid::from_u128(11)),
        KeyId(uuid::Uuid::from_bytes([1; 16])),
        [18; 32],
    );
    let mut store_bytes = Zeroizing::new(Vec::with_capacity(48));
    store.encode_into(&mut store_bytes);
    store_bytes.pop();
    let sealed = seal_box(
        b"store",
        &recipient,
        "10101010-1010-1010-1010-101010101010/keys/7/9",
        b"clear routing prefix",
        &store_bytes,
    )
    .unwrap();
    assert!(matches!(
        member.open_store_key(
            "10101010-1010-1010-1010-101010101010/keys/7/9",
            b"clear routing prefix",
            &sealed
        ),
        Err(CryptoError::Malformed)
    ));
    let mut circle_bytes = Zeroizing::new(Vec::with_capacity(64));
    circle.encode_into(&mut circle_bytes);
    circle_bytes.pop();
    let sealed = seal_box(
        b"circle",
        &recipient,
        "10101010-1010-1010-1010-101010101010/keys/7/9",
        b"clear routing prefix",
        &circle_bytes,
    )
    .unwrap();
    assert!(matches!(
        member.open_circle_key(
            "10101010-1010-1010-1010-101010101010/keys/7/9",
            b"clear routing prefix",
            &sealed
        ),
        Err(CryptoError::Malformed)
    ));
}

#[test]
fn signing_key_bytes_round_trip_and_match_the_private_pair() {
    let text = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
    let bytes: [u8; 32] = hex::decode(text).unwrap().try_into().unwrap();
    let signing = SigningKey::from_bytes(bytes).unwrap();
    assert_eq!(*signing.as_bytes(), bytes);
    assert_eq!(
        SigningKey::from_bytes(*signing.as_bytes()).unwrap(),
        signing
    );

    let keys = MemberKeys::generate().unwrap();
    let public = keys.public_keys();
    assert_eq!(
        *public.signing.as_bytes(),
        keys.signing.verifying_key().to_bytes()
    );
    assert_eq!(
        SigningKey::from_bytes(*public.signing.as_bytes()).unwrap(),
        public.signing
    );
}

#[test]
fn member_signatures_and_both_key_pairs_survive_custody_encoding() {
    let keys = MemberKeys::generate().unwrap();
    let restored = MemberKeys::from_secret_bytes(keys.to_secret_bytes().as_bytes()).unwrap();
    assert_eq!(keys.public_keys(), restored.public_keys());
    let signing = keys.public_keys().signing;
    for bytes in [b"".as_slice(), b"member's signed write"] {
        let signature = restored.sign(bytes);
        signing.verify(bytes, &signature).unwrap();
        assert!(matches!(
            signing.verify(b"altered", &signature),
            Err(CryptoError::Signature)
        ));
        assert!(matches!(
            MemberKeys::generate()
                .unwrap()
                .public_keys()
                .signing
                .verify(bytes, &signature),
            Err(CryptoError::Signature)
        ));
        for index in 0..64 {
            let mut changed = *signature.as_bytes();
            changed[index] ^= 1;
            assert!(matches!(
                signing.verify(bytes, &Signature::from_bytes(changed)),
                Err(CryptoError::Signature)
            ));
        }
    }
    let encoded = keys.to_secret_bytes();
    for end in 0..encoded.as_bytes().len() {
        assert!(MemberKeys::from_secret_bytes(&encoded.as_bytes()[..end]).is_err());
    }
    let mut extra = encoded.as_bytes().to_vec();
    extra.push(0);
    assert!(MemberKeys::from_secret_bytes(&extra).is_err());
}

#[test]
fn identity_custody_pins_the_id_before_the_unchanged_private_pair() {
    let pair = hex::decode(concat!(
        "43564d4b01",
        "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
    ))
    .unwrap();
    let member = MemberId(Uuid::from_u128(0x00112233_4455_6677_8899_aabbccddeeff));
    let identity = MemberIdentity::new(member, MemberKeys::from_secret_bytes(&pair).unwrap());
    let mut expected = b"CVMI\x01".to_vec();
    expected.extend_from_slice(member.0.as_bytes());
    expected.extend_from_slice(&pair);
    // No retired pair and no candidate.
    expected.extend_from_slice(&[0, 0]);
    assert_eq!(identity.to_secret_bytes().as_bytes(), expected);
    let restored = MemberIdentity::from_secret_bytes(&expected).unwrap();
    assert_eq!(restored.member(), member);
    assert_eq!(restored.keys().public_keys(), identity.keys().public_keys());
    for end in 0..expected.len() {
        assert!(MemberIdentity::from_secret_bytes(&expected[..end]).is_err());
    }
    let mut extra = expected.clone();
    extra.push(0);
    assert!(MemberIdentity::from_secret_bytes(&extra).is_err());
    // The custody envelope is not the restore code's private-pair blob.
    assert!(MemberIdentity::from_secret_bytes(&pair).is_err());
    assert!(MemberKeys::from_secret_bytes(&expected).is_err());
    for offset in 0..5 {
        let mut changed = expected.clone();
        changed[offset] ^= 1;
        assert_eq!(
            MemberIdentity::from_secret_bytes(&changed).unwrap_err(),
            MaterialError::Encoding
        );
    }
    let debug = format!("{identity:?}");
    assert!(debug.contains(&member.to_string()));
    assert!(!debug.contains("9d61b19d"));
}

/// Custody holds the active pair, retired ones and a candidate, each once,
/// in that order. A copy opens with the exact credential it names; replay's
/// current binding picks the active pair, and every other one stays held.
#[test]
fn identity_custody_keeps_retired_and_candidate_pairs() {
    let pair = |seed: u8| {
        let mut bytes = b"CVMK\x01".to_vec();
        bytes.extend([seed; 64]);
        MemberKeys::from_secret_bytes(&bytes).unwrap()
    };
    let member = MemberId(Uuid::from_u128(9));
    let (old, new, other) = (pair(1), pair(2), pair(3));
    let identity = MemberIdentity::new(member, old.clone()).with_candidate(new.clone());
    assert!(identity.holds(&new.public_keys()));
    assert!(identity
        .credential(&new.public_keys().sealing)
        .is_some_and(|keys| keys.public_keys() == new.public_keys()));
    assert!(identity.credential(&other.public_keys().sealing).is_none());
    let mut expected = b"CVMI\x01".to_vec();
    expected.extend_from_slice(member.0.as_bytes());
    expected.extend_from_slice(old.to_secret_bytes().as_bytes());
    expected.push(0);
    expected.push(1);
    expected.extend_from_slice(new.to_secret_bytes().as_bytes());
    assert_eq!(identity.to_secret_bytes().as_bytes(), expected);
    // The kept binding installs the candidate; the old pair stays held.
    let installed = identity.activate(&new.public_keys()).unwrap();
    assert_eq!(installed.keys().public_keys(), new.public_keys());
    assert!(installed.candidate().is_none());
    assert!(installed.holds(&old.public_keys()));
    let mut expected = b"CVMI\x01".to_vec();
    expected.extend_from_slice(member.0.as_bytes());
    expected.extend_from_slice(new.to_secret_bytes().as_bytes());
    expected.push(1);
    expected.extend_from_slice(old.to_secret_bytes().as_bytes());
    expected.push(0);
    let bytes = installed.to_secret_bytes();
    assert_eq!(bytes.as_bytes(), expected);
    let restored = MemberIdentity::from_secret_bytes(bytes.as_bytes()).unwrap();
    assert_eq!(restored.keys().public_keys(), new.public_keys());
    assert!(restored.holds(&old.public_keys()));
    assert!(identity.activate(&other.public_keys()).is_none());
    // A losing candidate stays held; a cancelled one never sent a copy.
    let lost = identity.with_candidate(other.clone()).retire_candidate();
    assert!(lost.candidate().is_none());
    assert!(lost.holds(&new.public_keys()) && lost.holds(&other.public_keys()));
    assert!(!identity.without_candidate().holds(&new.public_keys()));
    // A credential held twice, or an unknown candidate flag, is refused.
    let mut twice = b"CVMI\x01".to_vec();
    twice.extend_from_slice(member.0.as_bytes());
    twice.extend_from_slice(old.to_secret_bytes().as_bytes());
    twice.push(0);
    twice.push(1);
    twice.extend_from_slice(old.to_secret_bytes().as_bytes());
    assert!(MemberIdentity::from_secret_bytes(&twice).is_err());
    let mut flag = MemberIdentity::new(member, old)
        .to_secret_bytes()
        .as_bytes()
        .to_vec();
    *flag.last_mut().unwrap() = 2;
    assert!(MemberIdentity::from_secret_bytes(&flag).is_err());
    for end in 0..expected.len() {
        assert!(MemberIdentity::from_secret_bytes(&expected[..end]).is_err());
    }
}

#[test]
fn ed25519_rfc8032_vector_verifies() {
    let member = SigningKey::from_bytes(
        hex::decode("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
            .unwrap()
            .try_into()
            .unwrap(),
    )
    .unwrap();
    let bytes = hex::decode(concat!(
        "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e06522490155",
        "5fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
    ))
    .unwrap();
    member
        .verify(b"", &Signature::from_bytes(bytes.try_into().unwrap()))
        .unwrap();
}

#[test]
fn private_pair_encoding_pins_seed_order_and_independent_public_keys() {
    // RFC 8032's first signing seed and a distinct X25519 secret, with public
    // keys independently calculated using libsodium. D13 fixes this blob.
    let bytes = hex::decode(concat!(
        "43564d4b01",
        "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
    ))
    .unwrap();
    let keys = MemberKeys::from_secret_bytes(&bytes).unwrap();
    assert_eq!(keys.to_secret_bytes().as_bytes(), bytes);
    assert_eq!(
        hex::encode(keys.public_keys().signing.as_bytes()),
        "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
    );
    assert_eq!(
        hex::encode(keys.public_keys().sealing.as_bytes()),
        "8f40c5adb68f25624ae5b214ea767a6ec94d829d3d7b5e1ad1ba6f3e2138285f"
    );
    assert_eq!(
        hex::encode(keys.sign(b"").as_bytes()),
        concat!(
            "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e06522490155",
            "5fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
        )
    );
    for offset in 0..5 {
        let mut changed = bytes.clone();
        changed[offset] ^= 1;
        assert!(matches!(
            MemberKeys::from_secret_bytes(&changed),
            Err(MaterialError::Encoding)
        ));
    }
}

#[test]
fn invalid_and_weak_signing_keys_are_rejected() {
    let mut identity = [0; 32];
    identity[0] = 1;
    assert!(matches!(
        SigningKey::from_bytes(identity),
        Err(CryptoError::InvalidSigningKey)
    ));
    // Compressed y = 2 has no Ed25519 point, unlike the weak identity above.
    let mut invalid = [0; 32];
    invalid[0] = 2;
    assert!(matches!(
        SigningKey::from_bytes(invalid),
        Err(CryptoError::InvalidSigningKey)
    ));
}

#[test]
fn sealed_box_domains_are_checked_even_when_payload_lengths_match() {
    let member = MemberKeys::generate().unwrap();
    let recipient = member.public_keys().sealing;
    let path = "10101010-1010-1010-1010-101010101010/keys/7/9";
    let prefix = b"clear routing prefix";
    let key = StoreKey::from_bytes(KeyId(Uuid::from_u128(3)), [17; 32]);
    let mut plaintext = Zeroizing::new(Vec::with_capacity(48));
    key.encode_into(&mut plaintext);
    // A correctly sized store payload sealed in the circle domain must fail
    // authentication, independently of the envelope's length checks.
    let sealed = seal_box(b"circle", &recipient, path, prefix, &plaintext).unwrap();
    assert!(matches!(
        member.open_store_key(path, prefix, &sealed),
        Err(CryptoError::Authentication)
    ));

    let circle = CircleKey::from_bytes(CircleId(Uuid::from_u128(2)), key.id(), [18; 32]);
    let mut plaintext = Zeroizing::new(Vec::with_capacity(64));
    circle.encode_into(&mut plaintext);
    let sealed = seal_box(b"store", &recipient, path, prefix, &plaintext).unwrap();
    assert!(matches!(
        member.open_circle_key(path, prefix, &sealed),
        Err(CryptoError::Authentication)
    ));
}

#[test]
fn anonymous_store_boxes_bind_recipient_path_and_all_ciphertext_bytes() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let member = MemberKeys::generate().unwrap();
    let foreign = MemberKeys::generate().unwrap();
    let store = StoreKey::generate(KeyId(key_ids.new_id())).unwrap();
    let path = "10101010-1010-1010-1010-101010101010/keys/7/9".to_owned();
    let sealed = seal_store_key(
        &store,
        &member.public_keys().sealing,
        &path,
        b"clear routing prefix",
    )
    .unwrap();
    assert_eq!(sealed.len(), 120);
    let repeated = seal_store_key(
        &store,
        &member.public_keys().sealing,
        &path,
        b"clear routing prefix",
    )
    .unwrap();
    assert_ne!(&sealed[..32], &repeated[..32], "fresh ephemeral public key");
    assert_ne!(&sealed[32..56], &repeated[32..56], "fresh random nonce");
    let opened = member
        .open_store_key(&path, b"clear routing prefix", &sealed)
        .unwrap();
    assert_eq!(opened.id(), store.id());
    let ciphertext = store
        .derive()
        .seal_object_chunk("write", b"cleartext prefix", 0, 0, b"secret")
        .unwrap();
    assert_eq!(
        opened
            .derive()
            .open_object_chunk("write", b"cleartext prefix", 0, 0, &ciphertext)
            .unwrap(),
        b"secret"
    );
    assert!(foreign
        .open_store_key(&path, b"clear routing prefix", &sealed)
        .is_err());
    assert!(member
        .open_store_key(
            "10101010-1010-1010-1010-101010101010/keys/8/9",
            b"clear routing prefix",
            &sealed
        )
        .is_err());
    assert!(member
        .open_circle_key(&path, b"clear routing prefix", &sealed)
        .is_err());
    for index in 0..sealed.len() {
        let mut changed = sealed.clone();
        changed[index] ^= 1;
        assert!(member
            .open_store_key(&path, b"clear routing prefix", &changed)
            .is_err());
        assert!(member
            .open_store_key(&path, b"clear routing prefix", &sealed[..index])
            .is_err());
    }
    let mut trailing = sealed;
    trailing.push(0);
    assert!(member
        .open_store_key(&path, b"clear routing prefix", &trailing)
        .is_err());
}

#[test]
fn anonymous_circle_boxes_preserve_circle_and_id() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let member = MemberKeys::generate().unwrap();
    let circle =
        CircleKey::generate(CircleId(Uuid::from_u128(11)), KeyId(key_ids.new_id())).unwrap();
    let sealed = seal_circle_key(
        &circle,
        &member.public_keys().sealing,
        "10101010-1010-1010-1010-101010101010/keys/7/9",
        b"clear routing prefix",
    )
    .unwrap();
    let opened = member
        .open_circle_key(
            "10101010-1010-1010-1010-101010101010/keys/7/9",
            b"clear routing prefix",
            &sealed,
        )
        .unwrap();
    assert_eq!(opened.circle(), circle.circle());
    assert_eq!(opened.id(), circle.id());
    let ciphertext = circle
        .derive()
        .seal_object_chunk("write", b"cleartext prefix", 0, 0, b"circle secret")
        .unwrap();
    assert_eq!(
        opened
            .derive()
            .open_object_chunk("write", b"cleartext prefix", 0, 0, &ciphertext)
            .unwrap(),
        b"circle secret"
    );
    assert!(member
        .open_circle_key(
            "10101010-1010-1010-1010-101010101010/keys/7/10",
            b"clear routing prefix",
            &sealed
        )
        .is_err());
    assert!(member
        .open_store_key(
            "10101010-1010-1010-1010-101010101010/keys/7/9",
            b"clear routing prefix",
            &sealed
        )
        .is_err());
    let mut tampered = sealed;
    tampered[64] ^= 1;
    assert!(matches!(
        member.open_circle_key(
            "10101010-1010-1010-1010-101010101010/keys/7/9",
            b"clear routing prefix",
            &tampered
        ),
        Err(CryptoError::Authentication)
    ));
}

#[test]
fn low_order_x25519_keys_are_rejected_in_both_directions() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let member = MemberKeys::generate().unwrap();
    let store = StoreKey::generate(KeyId(key_ids.new_id())).unwrap();
    for first_byte in [0, 1] {
        let mut low = [0; 32];
        low[0] = first_byte;
        assert!(matches!(
            seal_store_key(
                &store,
                &SealingKey::from_bytes(low),
                "10101010-1010-1010-1010-101010101010/keys/7/9",
                b"clear routing prefix"
            ),
            Err(CryptoError::WeakSealingKey)
        ));
        let mut sealed = seal_store_key(
            &store,
            &member.public_keys().sealing,
            "10101010-1010-1010-1010-101010101010/keys/7/9",
            b"clear routing prefix",
        )
        .unwrap();
        sealed[..32].copy_from_slice(&low);
        assert!(matches!(
            member.open_store_key(
                "10101010-1010-1010-1010-101010101010/keys/7/9",
                b"clear routing prefix",
                &sealed
            ),
            Err(CryptoError::WeakSealingKey)
        ));
    }
}

#[test]
fn access_key_boxes_open_only_for_their_member_path_and_prefix() {
    let ana = MemberKeys::generate().unwrap();
    let ben = MemberKeys::generate().unwrap();
    let plaintext = b"key id, secret and principal";
    let sealed = seal_access_key(
        plaintext,
        &ana.public_keys().sealing,
        "s/access-keys/a/1",
        b"prefix",
    )
    .unwrap();
    assert_eq!(
        ana.open_access_key("s/access-keys/a/1", b"prefix", &sealed)
            .unwrap()
            .as_slice(),
        plaintext
    );
    assert!(ben
        .open_access_key("s/access-keys/a/1", b"prefix", &sealed)
        .is_err());
    assert!(ana
        .open_access_key("s/access-keys/a/2", b"prefix", &sealed)
        .is_err());
    assert!(ana
        .open_access_key("s/access-keys/a/1", b"other", &sealed)
        .is_err());
    assert!(ana
        .open_access_key("s/access-keys/a/1", b"prefix", &sealed[..40])
        .is_err());
    assert!(seal_access_key(
        &vec![0; MAX_ACCESS_KEY_BOX + 1],
        &ana.public_keys().sealing,
        "p",
        b""
    )
    .is_err());
}
