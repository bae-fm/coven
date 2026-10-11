use super::*;
use crate::store_log::tests::keys;

#[test]
fn author_view_checks_retain_all_facts_and_reject_damaged_encodings() {
    let circle = CircleId(uuid::Uuid::from_u128(u128::MAX));
    for check in [
        StoreLogCheck::Allowed,
        StoreLogCheck::NotAllowed,
        StoreLogCheck::DeviceOwner(keys(1).signing),
        StoreLogCheck::DeletedCircles(BTreeSet::new()),
        StoreLogCheck::DeletedCircles([circle].into()),
    ] {
        assert_eq!(StoreLogCheck::decode(&check.encode()).unwrap(), check);
    }
    let duplicate = [vec![3], circle.0.as_bytes().repeat(2)].concat();
    for bytes in [
        vec![],
        vec![2],
        vec![4],
        vec![0, 0],
        vec![2, 0],
        vec![3, 0],
        duplicate,
    ] {
        assert!(matches!(
            StoreLogCheck::decode(&bytes),
            Err(DbError::DamagedDatabase)
        ));
    }
}
