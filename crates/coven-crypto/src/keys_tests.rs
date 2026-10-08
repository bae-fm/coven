use super::*;
use crate::{ContentHasher, MemberKeys};
use coven_foundation::id_source::{IdSource, KeyId};

#[test]
fn concurrent_rotations_keep_both_generated_keys() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let mut ring = StoreKeyring::new(StoreKey::generate(KeyId(key_ids.new_id())).unwrap());
    let circle = CircleId(Uuid::from_u128(8));
    let mut ids = Vec::new();
    for _ in 0..2 {
        let store_id = KeyId(key_ids.new_id());
        let circle_id = KeyId(key_ids.new_id());
        let store_key = StoreKey::generate(store_id).unwrap();
        let circle_key = CircleKey::generate(circle, circle_id).unwrap();
        assert_eq!(store_key.id(), store_id);
        assert_eq!(circle_key.id(), circle_id);
        ids.push((store_key.id(), circle_key.id()));
        ring.insert_store_key(store_key).unwrap();
        ring.insert_circle_key(circle_key).unwrap();
    }
    assert_ne!(ids[0].0, ids[1].0);
    assert_ne!(ids[0].1, ids[1].1);
    for (store_id, circle_id) in ids {
        assert_eq!(ring.store_key(store_id).unwrap().id(), store_id);
        assert_eq!(ring.circle_key(circle, circle_id).unwrap().id(), circle_id);
    }
}

#[test]
fn empty_keyrings_are_malformed_but_zero_ids_are_valid() {
    let mut empty = b"CVKR\x01".to_vec();
    empty.extend_from_slice(&[0; 16]);
    assert!(matches!(
        StoreKeyring::from_secret_bytes(&empty),
        Err(MaterialError::Encoding)
    ));
    let ring = StoreKeyring::new(StoreKey::from_bytes(
        KeyId(uuid::Uuid::from_bytes([0; 16])),
        [17; 32],
    ));
    let bytes = ring.to_secret_bytes();
    let restored = StoreKeyring::from_secret_bytes(bytes.as_bytes()).unwrap();
    assert_eq!(
        restored
            .store_key(KeyId(uuid::Uuid::from_bytes([0; 16])))
            .unwrap()
            .id(),
        KeyId(uuid::Uuid::from_bytes([0; 16]))
    );
}

#[test]
fn conflicts_at_every_secret_byte_preserve_both_key_kinds() {
    let circle = CircleId(Uuid::from_u128(8));
    let mut ring = StoreKeyring::new(StoreKey::from_bytes(
        KeyId(uuid::Uuid::from_bytes([1; 16])),
        [17; 32],
    ));
    ring.insert_circle_key(CircleKey::from_bytes(
        circle,
        KeyId(uuid::Uuid::from_bytes([1; 16])),
        [17; 32],
    ))
    .unwrap();
    let original = ring.to_secret_bytes();
    for index in 0..32 {
        let mut changed = [17; 32];
        changed[index] ^= 1;
        assert_eq!(
            ring.insert_store_key(StoreKey::from_bytes(
                KeyId(uuid::Uuid::from_bytes([1; 16])),
                changed
            )),
            Err(MaterialError::StoreKeyConflict(KeyId(
                uuid::Uuid::from_bytes([1; 16])
            )))
        );
        assert_eq!(
            ring.insert_circle_key(CircleKey::from_bytes(
                circle,
                KeyId(uuid::Uuid::from_bytes([1; 16])),
                changed
            )),
            Err(MaterialError::CircleKeyConflict {
                circle,
                key: KeyId(uuid::Uuid::from_bytes([1; 16]))
            })
        );
        assert_eq!(ring.to_secret_bytes().as_bytes(), original.as_bytes());
    }
    ring.insert_store_key(StoreKey::from_bytes(
        KeyId(uuid::Uuid::from_bytes([1; 16])),
        [17; 32],
    ))
    .unwrap();
    ring.insert_circle_key(CircleKey::from_bytes(
        circle,
        KeyId(uuid::Uuid::from_bytes([1; 16])),
        [17; 32],
    ))
    .unwrap();
    assert_eq!(ring.to_secret_bytes().as_bytes(), original.as_bytes());
}

#[test]
fn keyring_keeps_store_and_circle_keys_by_id_in_any_insertion_order() {
    let circle = CircleId(Uuid::from_u128(8));
    let mut ring = StoreKeyring::new(StoreKey::from_bytes(
        KeyId(uuid::Uuid::from_bytes([2; 16])),
        [2; 32],
    ));
    ring.insert_store_key(StoreKey::from_bytes(
        KeyId(uuid::Uuid::from_bytes([1; 16])),
        [1; 32],
    ))
    .unwrap();
    for n in [2, 1] {
        ring.insert_circle_key(CircleKey::from_bytes(
            circle,
            KeyId(uuid::Uuid::from_bytes([n; 16])),
            [n; 32],
        ))
        .unwrap();
    }
    let stored = ring.to_secret_bytes();
    let restored = StoreKeyring::from_secret_bytes(stored.as_bytes()).unwrap();
    assert_eq!(restored.to_secret_bytes().as_bytes(), stored.as_bytes());
    for n in [1, 2] {
        let id = KeyId(uuid::Uuid::from_bytes([n; 16]));
        assert_eq!(restored.store_key(id).unwrap().id(), id);
        assert_eq!(restored.circle_key(circle, id).unwrap().id(), id);
    }
    let missing = KeyId(uuid::Uuid::from_bytes([3; 16]));
    assert!(
        matches!(restored.store_key(missing), Err(MaterialError::UnknownStoreKey(key)) if key == missing)
    );
    assert!(
        matches!(restored.circle_key(circle, missing), Err(MaterialError::UnknownCircleKey { key, .. }) if key == missing)
    );
    assert!(restored
        .circle_key(
            CircleId(Uuid::from_u128(9)),
            KeyId(uuid::Uuid::from_bytes([1; 16]))
        )
        .is_err());
}

#[test]
fn malformed_keyrings_are_encoding_errors() {
    let key_ids = coven_foundation::id_source::SequentialIds::new();
    let ring = StoreKeyring::new(StoreKey::generate(KeyId(key_ids.new_id())).unwrap());
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
    duplicate.extend_from_slice(&stored.as_bytes()[13..61]);
    duplicate.extend_from_slice(&stored.as_bytes()[13..61]);
    duplicate.extend_from_slice(&0u64.to_le_bytes());
    assert!(matches!(
        StoreKeyring::from_secret_bytes(&duplicate),
        Err(MaterialError::Encoding)
    ));
}

#[test]
fn every_secret_value_redacts_debug_output() {
    let sentinel = *b"secret-marker-must-never-print!!";
    let key = StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), sentinel);
    let circle = CircleKey::from_bytes(
        CircleId(Uuid::from_u128(5)),
        KeyId(uuid::Uuid::from_bytes([1; 16])),
        sentinel,
    );
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
