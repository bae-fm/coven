use super::*;
use crate::store_log::tests::member;

/// A member pair; the check keeps the replaced pair whole.
fn pair() -> coven_crypto::MemberPublicKeys {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([7; 64]);
    coven_crypto::MemberKeys::from_secret_bytes(&bytes)
        .unwrap()
        .public_keys()
}

#[test]
fn author_view_checks_retain_all_facts_and_reject_damaged_encodings() {
    let circle = CircleId(uuid::Uuid::from_u128(u128::MAX));
    for check in [
        StoreLogCheck::Allowed,
        StoreLogCheck::LandedTooLate { authorized: false },
        StoreLogCheck::LandedTooLate { authorized: true },
        StoreLogCheck::NotAllowed,
        StoreLogCheck::KeysAlreadyUsed,
        StoreLogCheck::DeviceOwner(member(1)),
        StoreLogCheck::KeyReplacement(pair()),
        StoreLogCheck::MemberRemoval(BTreeMap::new()),
        StoreLogCheck::MemberRemoval([(circle, CircleRemoval::Member)].into()),
        StoreLogCheck::MemberRemoval([(circle, CircleRemoval::Circle)].into()),
    ] {
        assert_eq!(StoreLogCheck::decode(&check.encode()).unwrap(), check);
    }
    let record = [circle.0.as_bytes().as_slice(), &[0]].concat();
    let duplicate = [vec![3], record.repeat(2)].concat();
    for bytes in [
        vec![],
        vec![2],
        vec![5, 0],
        vec![6],
        [&[6][..], &[0; 32], pair().sealing.as_bytes()].concat(),
        [&[6][..], pair().signing.as_bytes(), &[0; 31]].concat(),
        [&[6][..], pair().signing.as_bytes(), &[0; 33]].concat(),
        vec![4],
        vec![4, 2],
        vec![0, 0],
        vec![2, 0],
        vec![3, 0],
        duplicate,
        [vec![3], circle.0.as_bytes().to_vec(), vec![2]].concat(),
    ] {
        assert!(matches!(
            StoreLogCheck::decode(&bytes),
            Err(DbError::DamagedDatabase)
        ));
    }
}
