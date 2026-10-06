use super::*;
use crate::CircleId;
use coven_foundation::id_source::{IdSource, KeyId};
use uuid::Uuid;

#[test]
fn member_identity_retains_only_the_public_key_bytes() {
    assert_eq!(std::mem::size_of::<MemberId>(), 32);
}

#[test]
fn wrong_key_lengths_are_refused_before_opening_the_box() {
    let member = MemberKeys::generate().unwrap();
    let recipient = member.sealing_public_key();
    let store = StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [17; 32]);
    let circle = CircleKey::from_bytes(
        CircleId(Uuid::from_u128(11)),
        KeyId(uuid::Uuid::from_bytes([1; 16])),
        [18; 32],
    );
    let mut store_bytes = Zeroizing::new(Vec::with_capacity(48));
    store.encode_into(&mut store_bytes);
    store_bytes.pop();
    let sealed = seal_box(b"store", &recipient, "keys/store/1/member", &store_bytes).unwrap();
    assert!(matches!(
        member.open_store_key("keys/store/1/member", &sealed),
        Err(CryptoError::Malformed)
    ));
    let mut circle_bytes = Zeroizing::new(Vec::with_capacity(64));
    circle.encode_into(&mut circle_bytes);
    circle_bytes.pop();
    let sealed = seal_box(
        b"circle",
        &recipient,
        "keys/circle/11/1/member",
        &circle_bytes,
    )
    .unwrap();
    assert!(matches!(
        member.open_circle_key("keys/circle/11/1/member", &sealed),
        Err(CryptoError::Malformed)
    ));
}

#[test]
#[should_panic(expected = "storage paths must be nonempty")]
fn sealed_keys_require_a_storage_path() {
    let member = MemberKeys::generate().unwrap();
    let store = StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [17; 32]);
    let _sealed = seal_store_key(&store, &member.sealing_public_key(), "");
}

#[test]
fn member_public_bytes_round_trip_through_each_constructor() {
    let text = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
    let bytes: [u8; 32] = hex::decode(text).unwrap().try_into().unwrap();
    let member = MemberId::from_bytes(bytes).unwrap();
    assert_eq!(member.to_bytes(), bytes);
    assert_eq!(member.to_string(), text);
    assert_eq!(text.parse::<MemberId>().unwrap().to_bytes(), bytes);
    assert_eq!(MemberId::from_bytes(member.to_bytes()).unwrap(), member);

    let keys = MemberKeys::generate().unwrap();
    let member = keys.member_id();
    assert_eq!(member.to_bytes(), keys.signing.verifying_key().to_bytes());
    assert_eq!(MemberId::from_bytes(member.to_bytes()).unwrap(), member);
}

#[test]
fn member_signatures_and_both_key_pairs_survive_custody_encoding() {
    let keys = MemberKeys::generate().unwrap();
    let restored = MemberKeys::from_secret_bytes(keys.to_secret_bytes().as_bytes()).unwrap();
    assert_eq!(keys.member_id(), restored.member_id());
    assert_eq!(keys.sealing_public_key(), restored.sealing_public_key());
    let member: MemberId = keys.member_id().to_string().parse().unwrap();
    for bytes in [b"".as_slice(), b"member's signed write"] {
        let signature = restored.sign(bytes);
        member.verify(bytes, &signature).unwrap();
        assert!(matches!(
            member.verify(b"altered", &signature),
            Err(CryptoError::Signature)
        ));
        assert!(matches!(
            MemberKeys::generate()
                .unwrap()
                .member_id()
                .verify(bytes, &signature),
            Err(CryptoError::Signature)
        ));
        for index in 0..64 {
            let mut changed = *signature.as_bytes();
            changed[index] ^= 1;
            assert!(matches!(
                member.verify(bytes, &Signature::from_bytes(changed)),
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
fn ed25519_rfc8032_vector_verifies() {
    let member: MemberId = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
        .parse()
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
fn invalid_and_weak_member_ids_are_rejected() {
    for value in ["", "00", &"0".repeat(64), &"z".repeat(64), &"A".repeat(64)] {
        assert!(value.parse::<MemberId>().is_err());
    }
    let mut identity = [0; 32];
    identity[0] = 1;
    assert!(matches!(
        MemberId::from_bytes(identity),
        Err(CryptoError::InvalidMemberId)
    ));
}

#[test]
fn anonymous_store_boxes_bind_recipient_path_and_all_ciphertext_bytes() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let member = MemberKeys::generate().unwrap();
    let foreign = MemberKeys::generate().unwrap();
    let store = StoreKey::generate(KeyId(key_ids.new_id())).unwrap();
    let path = format!("keys/store/{}/{}", store.id(), member.member_id());
    let sealed = seal_store_key(&store, &member.sealing_public_key(), &path).unwrap();
    assert_eq!(&sealed[..3], &[37, 0, 1]);
    assert_eq!(sealed.len(), 123);
    assert_ne!(
        sealed,
        seal_store_key(&store, &member.sealing_public_key(), &path).unwrap()
    );
    let opened = member.open_store_key(&path, &sealed).unwrap();
    assert_eq!(opened.id(), store.id());
    let ciphertext = store.derive().seal_object("write", b"secret").unwrap();
    assert_eq!(
        opened.derive().open_object("write", &ciphertext).unwrap(),
        b"secret"
    );
    assert!(foreign.open_store_key(&path, &sealed).is_err());
    assert!(member
        .open_store_key("keys/store/8/member", &sealed)
        .is_err());
    assert!(member.open_circle_key(&path, &sealed).is_err());
    for index in 0..sealed.len() {
        let mut changed = sealed.clone();
        changed[index] ^= 1;
        assert!(member.open_store_key(&path, &changed).is_err());
        assert!(member.open_store_key(&path, &sealed[..index]).is_err());
    }
    let mut trailing = sealed;
    trailing.push(0);
    assert!(member.open_store_key(&path, &trailing).is_err());
}

#[test]
fn anonymous_circle_boxes_preserve_circle_and_id() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let member = MemberKeys::generate().unwrap();
    let circle =
        CircleKey::generate(CircleId(Uuid::from_u128(11)), KeyId(key_ids.new_id())).unwrap();
    let sealed = seal_circle_key(
        &circle,
        &member.sealing_public_key(),
        "keys/circle/11/3/member",
    )
    .unwrap();
    let opened = member
        .open_circle_key("keys/circle/11/3/member", &sealed)
        .unwrap();
    assert_eq!(opened.circle(), circle.circle());
    assert_eq!(opened.id(), circle.id());
    let ciphertext = circle
        .derive()
        .seal_object("write", b"circle secret")
        .unwrap();
    assert_eq!(
        opened.derive().open_object("write", &ciphertext).unwrap(),
        b"circle secret"
    );
    assert!(member
        .open_circle_key("keys/circle/12/3/member", &sealed)
        .is_err());
    assert!(member
        .open_store_key("keys/circle/11/3/member", &sealed)
        .is_err());
    let mut tampered = sealed;
    tampered[64] ^= 1;
    assert!(matches!(
        member.open_circle_key("keys/circle/11/3/member", &tampered),
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
                &SealingPublicKey::from_bytes(low),
                "keys/store/1/member"
            ),
            Err(CryptoError::WeakSealingKey)
        ));
        let mut sealed =
            seal_store_key(&store, &member.sealing_public_key(), "keys/store/1/member").unwrap();
        sealed[3..35].copy_from_slice(&low);
        assert!(matches!(
            member.open_store_key("keys/store/1/member", &sealed),
            Err(CryptoError::WeakSealingKey)
        ));
    }
}
