use super::*;
use crate::store_log::tests::keys;

#[test]
fn author_view_checks_retain_all_facts_and_reject_damaged_encodings() {
    let circle = CircleId(uuid::Uuid::from_u128(u128::MAX));
    for check in [
        StoreLogCheck::Allowed,
        StoreLogCheck::NotAllowed,
        StoreLogCheck::DeviceOwner(keys(1).signing),
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
        vec![4],
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
