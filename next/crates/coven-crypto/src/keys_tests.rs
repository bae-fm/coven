use super::*;
use crate::{ContentHasher, MemberKeys};

#[test]
fn decoded_zero_key_numbers_and_empty_keyrings_are_malformed_encoding() {
    let mut empty = b"CVKR\x01".to_vec();
    empty.extend_from_slice(&[0; 16]);
    assert!(matches!(
        StoreKeyring::from_secret_bytes(&empty),
        Err(MaterialError::Encoding)
    ));
    let circle = CircleId(Uuid::from_u128(8));
    let mut ring = StoreKeyring::new(StoreKey::from_bytes(NonZeroU64::MIN, [17; 32]));
    ring.insert_circle_key(CircleKey::from_bytes(circle, NonZeroU64::MIN, [18; 32]))
        .unwrap();
    let stored = ring.to_secret_bytes();
    // Store key number and circle key number, respectively.
    for offset in [13, 77] {
        let mut invalid = stored.as_bytes().to_vec();
        invalid[offset..offset + 8].fill(0);
        assert!(matches!(
            StoreKeyring::from_secret_bytes(&invalid),
            Err(MaterialError::Encoding)
        ));
    }
}

#[test]
fn conflicts_at_every_secret_byte_preserve_both_key_kinds() {
    let circle = CircleId(Uuid::from_u128(8));
    let mut ring = StoreKeyring::new(StoreKey::from_bytes(NonZeroU64::MIN, [17; 32]));
    ring.insert_circle_key(CircleKey::from_bytes(circle, NonZeroU64::MIN, [17; 32]))
        .unwrap();
    let original = ring.to_secret_bytes();
    for index in 0..32 {
        let mut changed = [17; 32];
        changed[index] ^= 1;
        assert_eq!(
            ring.insert_store_key(StoreKey::from_bytes(NonZeroU64::MIN, changed)),
            Err(MaterialError::StoreKeyConflict(1))
        );
        assert_eq!(
            ring.insert_circle_key(CircleKey::from_bytes(circle, NonZeroU64::MIN, changed)),
            Err(MaterialError::CircleKeyConflict { circle, number: 1 })
        );
        assert_eq!(ring.to_secret_bytes().as_bytes(), original.as_bytes());
    }
    ring.insert_store_key(StoreKey::from_bytes(NonZeroU64::MIN, [17; 32]))
        .unwrap();
    ring.insert_circle_key(CircleKey::from_bytes(circle, NonZeroU64::MIN, [17; 32]))
        .unwrap();
    assert_eq!(ring.to_secret_bytes().as_bytes(), original.as_bytes());
}

#[test]
fn app_data_uses_its_own_key_in_both_directions() {
    let ring = StoreKeyring::new(StoreKey::from_bytes(NonZeroU64::new(7).unwrap(), [17; 32]));
    let sealed = ring.seal_app_data(b"private field", b"row/42").unwrap();
    assert_eq!(&sealed[..5], b"CVAD\x01");
    assert_eq!(&sealed[5..13], &7u64.to_le_bytes());
    let context = cipher::context(&[&sealed[..13], b"row/42"]);
    let app_key = derivation::derive(&[17; 32], b"coven/app-data/v1");
    let object_key = derivation::derive(&[17; 32], derivation::ENCRYPTION);
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
    let mut ring = StoreKeyring::new(StoreKey::from_bytes(NonZeroU64::MIN, [1; 32]));
    ring.insert_store_key(StoreKey::from_bytes(NonZeroU64::new(2).unwrap(), [2; 32]))
        .unwrap();
    ring.insert_store_key(StoreKey::from_bytes(NonZeroU64::MIN, [1; 32]))
        .unwrap();
    assert_eq!(ring.current_store_key().number(), 2);
    assert!(matches!(
        ring.insert_store_key(StoreKey::from_bytes(NonZeroU64::MIN, [9; 32])),
        Err(MaterialError::StoreKeyConflict(1))
    ));
    for n in [2, 1] {
        ring.insert_circle_key(CircleKey::from_bytes(
            circle,
            NonZeroU64::new(n).unwrap(),
            [n as u8; 32],
        ))
        .unwrap();
    }
    assert_eq!(ring.current_circle_key(circle).unwrap().number(), 2);
    assert_eq!(ring.circle_key(circle, 1).unwrap().number(), 1);
    assert!(matches!(
        ring.insert_circle_key(CircleKey::from_bytes(circle, NonZeroU64::MIN, [9; 32])),
        Err(MaterialError::CircleKeyConflict { number: 1, .. })
    ));
    let stored = ring.to_secret_bytes();
    let restored = StoreKeyring::from_secret_bytes(stored.as_bytes()).unwrap();
    assert_eq!(restored.to_secret_bytes().as_bytes(), stored.as_bytes());
    assert_eq!(restored.current_store_key().number(), 2);
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
    let mut ring = StoreKeyring::new(StoreKey::generate(NonZeroU64::MIN).unwrap());
    let old = ring
        .seal_app_data(b"local private field", b"notes/42")
        .unwrap();
    ring.insert_store_key(StoreKey::generate(NonZeroU64::new(2).unwrap()).unwrap())
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
    let mut ring = StoreKeyring::new(StoreKey::from_bytes(NonZeroU64::MIN, [17; 32]));
    let mut sealed = ring.seal_app_data(b"secret", b"row").unwrap();
    ring.insert_store_key(StoreKey::from_bytes(NonZeroU64::new(2).unwrap(), [17; 32]))
        .unwrap();
    sealed[5..13].copy_from_slice(&2u64.to_le_bytes());
    assert!(matches!(
        ring.open_app_data(&sealed, b"row"),
        Err(SealError::Crypto(CryptoError::Authentication))
    ));
}

#[test]
fn malformed_keyrings_are_encoding_errors() {
    let ring = StoreKeyring::new(StoreKey::generate(NonZeroU64::MIN).unwrap());
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
    let key = StoreKey::from_bytes(NonZeroU64::MIN, sentinel);
    let circle = CircleKey::from_bytes(CircleId(Uuid::from_u128(5)), NonZeroU64::MIN, sentinel);
    let invite = InviteSecret::from_bytes(sentinel);
    let derived = key.derive();
    let member = MemberKeys::generate().unwrap();
    let outputs = [
        format!("{key:?}"),
        format!("{circle:?}"),
        format!("{invite:?}"),
        format!("{derived:?}"),
        format!("{:?}", invite.join_request_key()),
        format!("{:?}", derived.fingerprint_hasher()),
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
