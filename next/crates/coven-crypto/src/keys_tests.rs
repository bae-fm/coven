use super::*;
use crate::{ContentHasher, MemberKeys};

#[test]
fn conflicts_at_every_secret_byte_preserve_both_key_kinds() {
    let circle = CircleId(Uuid::from_u128(8));
    let mut ring = StoreKeyring::new(StoreKey::from_bytes(1, [17; 32]).unwrap());
    ring.insert_circle_key(CircleKey::from_bytes(circle, 1, [17; 32]).unwrap())
        .unwrap();
    let original = ring.to_secret_bytes();
    for index in 0..32 {
        let mut changed = [17; 32];
        changed[index] ^= 1;
        assert_eq!(
            ring.insert_store_key(StoreKey::from_bytes(1, changed).unwrap()),
            Err(MaterialError::StoreKeyConflict(1))
        );
        assert_eq!(
            ring.insert_circle_key(CircleKey::from_bytes(circle, 1, changed).unwrap()),
            Err(MaterialError::CircleKeyConflict { circle, number: 1 })
        );
        assert_eq!(ring.to_secret_bytes().as_bytes(), original.as_bytes());
    }
    ring.insert_store_key(StoreKey::from_bytes(1, [17; 32]).unwrap())
        .unwrap();
    ring.insert_circle_key(CircleKey::from_bytes(circle, 1, [17; 32]).unwrap())
        .unwrap();
    assert_eq!(ring.to_secret_bytes().as_bytes(), original.as_bytes());
}

#[test]
fn app_data_uses_its_own_key_in_both_directions() {
    let ring = StoreKeyring::new(StoreKey::from_bytes(7, [17; 32]).unwrap());
    let sealed = ring.seal_app_data(b"private field", b"row/42").unwrap();
    assert_eq!(&sealed[..5], b"CVAD\x01");
    assert_eq!(&sealed[5..13], &7u64.to_le_bytes());
    let context = cipher::context(&[&sealed[..13], b"row/42"]);
    let app_key = derivation::derive(&[17; 32], b"coven/app-data/v1").unwrap();
    let object_key = derivation::derive(&[17; 32], derivation::ENCRYPTION).unwrap();
    assert_eq!(
        cipher::open_random(&app_key, &context, &sealed[13..]).unwrap(),
        b"private field"
    );
    assert!(matches!(
        cipher::open_random(&object_key, &context, &sealed[13..]),
        Err(CryptoError::Authentication)
    ));
    let mut object_sealed = sealed[..13].to_vec();
    object_sealed.extend(cipher::seal_random(&object_key, &context, b"private field").unwrap());
    assert!(matches!(
        ring.open_app_data(&object_sealed, b"row/42"),
        Err(SealError::Crypto(CryptoError::Authentication))
    ));
}

#[test]
fn keyring_keeps_numbered_store_and_circle_history_without_overwriting_conflicts() {
    let circle = CircleId(Uuid::from_u128(8));
    let mut ring = StoreKeyring::new(StoreKey::from_bytes(1, [1; 32]).unwrap());
    ring.insert_store_key(StoreKey::from_bytes(2, [2; 32]).unwrap())
        .unwrap();
    ring.insert_store_key(StoreKey::from_bytes(1, [1; 32]).unwrap())
        .unwrap();
    assert_eq!(ring.current_store_key().unwrap().number(), 2);
    assert!(matches!(
        ring.insert_store_key(StoreKey::from_bytes(1, [9; 32]).unwrap()),
        Err(MaterialError::StoreKeyConflict(1))
    ));
    for n in [2, 1] {
        ring.insert_circle_key(CircleKey::from_bytes(circle, n, [n as u8; 32]).unwrap())
            .unwrap();
    }
    assert_eq!(ring.current_circle_key(circle).unwrap().number(), 2);
    assert_eq!(ring.circle_key(circle, 1).unwrap().number(), 1);
    assert!(matches!(
        ring.insert_circle_key(CircleKey::from_bytes(circle, 1, [9; 32]).unwrap()),
        Err(MaterialError::CircleKeyConflict { number: 1, .. })
    ));
    let stored = ring.to_secret_bytes();
    let restored = StoreKeyring::from_secret_bytes(stored.as_bytes()).unwrap();
    assert_eq!(restored.to_secret_bytes().as_bytes(), stored.as_bytes());
    assert_eq!(restored.current_store_key().unwrap().number(), 2);
    assert_eq!(restored.current_circle_key(circle).unwrap().number(), 2);
    assert!(matches!(
        restored.store_key(3),
        Err(MaterialError::UnknownStoreKey(3))
    ));
    assert!(matches!(
        restored.circle_key(circle, 3),
        Err(MaterialError::UnknownCircleKey { number: 3, .. })
    ));
    assert!(restored
        .current_circle_key(CircleId(Uuid::from_u128(9)))
        .is_none());
}

#[test]
fn app_data_opens_after_key_replacement_but_not_with_another_context() {
    let mut ring = StoreKeyring::new(StoreKey::generate(1).unwrap());
    let old = ring
        .seal_app_data(b"local private field", b"notes/42")
        .unwrap();
    ring.insert_store_key(StoreKey::generate(2).unwrap())
        .unwrap();
    assert_eq!(
        ring.open_app_data(&old, b"notes/42").unwrap(),
        b"local private field"
    );
    assert!(matches!(
        ring.open_app_data(&old, b"notes/43"),
        Err(SealError::Crypto(CryptoError::Authentication))
    ));
    let new = ring.seal_app_data(b"new field", b"notes/42").unwrap();
    assert_eq!(ring.open_app_data(&new, b"notes/42").unwrap(), b"new field");
    for i in 0..old.len() {
        let mut altered = old.clone();
        altered[i] ^= 1;
        assert!(ring.open_app_data(&altered, b"notes/42").is_err());
        assert!(ring.open_app_data(&old[..i], b"notes/42").is_err());
    }
}

#[test]
fn app_data_authenticates_its_key_number_even_if_key_bytes_repeat() {
    let mut ring = StoreKeyring::new(StoreKey::from_bytes(1, [17; 32]).unwrap());
    let mut sealed = ring.seal_app_data(b"secret", b"row").unwrap();
    ring.insert_store_key(StoreKey::from_bytes(2, [17; 32]).unwrap())
        .unwrap();
    sealed[5..13].copy_from_slice(&2u64.to_le_bytes());
    assert!(matches!(
        ring.open_app_data(&sealed, b"row"),
        Err(SealError::Crypto(CryptoError::Authentication))
    ));
}

#[test]
fn malformed_keyrings_and_zero_key_numbers_are_typed_errors() {
    assert!(matches!(
        StoreKey::from_bytes(0, [0; 32]),
        Err(MaterialError::ZeroKeyNumber)
    ));
    assert!(matches!(
        StoreKey::generate(0),
        Err(CryptoError::Material(MaterialError::ZeroKeyNumber))
    ));
    let ring = StoreKeyring::new(StoreKey::generate(1).unwrap());
    let stored = ring.to_secret_bytes();
    for len in 0..stored.as_bytes().len() {
        assert!(StoreKeyring::from_secret_bytes(&stored.as_bytes()[..len]).is_err());
    }
    let mut trailing = stored.as_bytes().to_vec();
    trailing.push(0);
    assert!(StoreKeyring::from_secret_bytes(&trailing).is_err());
    for count in [0, u64::MAX] {
        let mut invalid = stored.as_bytes().to_vec();
        invalid[5..13].copy_from_slice(&count.to_le_bytes());
        assert!(StoreKeyring::from_secret_bytes(&invalid).is_err());
    }
    let mut duplicate = b"CVKR\x01".to_vec();
    duplicate.extend_from_slice(&2u64.to_le_bytes());
    duplicate.extend_from_slice(&stored.as_bytes()[13..53]);
    duplicate.extend_from_slice(&stored.as_bytes()[13..53]);
    duplicate.extend_from_slice(&0u64.to_le_bytes());
    assert!(matches!(
        StoreKeyring::from_secret_bytes(&duplicate),
        Err(MaterialError::Encoding)
    ));
}

#[test]
fn every_secret_value_redacts_debug_output() {
    let sentinel = *b"secret-marker-must-never-print!!";
    let key = StoreKey::from_bytes(1, sentinel).unwrap();
    let circle = CircleKey::from_bytes(CircleId(Uuid::from_u128(5)), 1, sentinel).unwrap();
    let invite = InviteSecret::from_bytes(sentinel);
    let derived = key.derive().unwrap();
    let member = MemberKeys::generate().unwrap();
    let outputs = [
        format!("{key:?}"),
        format!("{circle:?}"),
        format!("{invite:?}"),
        format!("{derived:?}"),
        format!("{:?}", invite.join_request_key().unwrap()),
        format!("{:?}", derived.fingerprint_hasher().unwrap()),
        format!("{:?}", StoreKeyring::new(key)),
        format!("{member:?}"),
        format!("{:?}", member.to_secret_bytes()),
        format!("{:?}", SecretBytes::new(sentinel.to_vec())),
        format!("{:?}", ContentHasher::new()),
    ];
    for output in outputs {
        assert!(!output.contains("secret-marker"));
        assert!(!output.contains(&hex::encode(sentinel)));
        assert!(!output.contains(&format!("{sentinel:?}")));
        assert!(!output.contains(&hex::encode(member.to_secret_bytes().as_bytes())));
    }
}
