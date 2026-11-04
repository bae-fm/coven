use super::*;
use crate::custody::{InMemoryCustody, Keychain, MemberKeyCustody};
use coven_foundation::id_source::{IdSource, StoreId, UuidIds};

fn custody_holding(
    held: Option<MemberIdentity>,
) -> (StoreCustody, Arc<InMemoryCustody<MemberIdentity>>) {
    let saved = Arc::new(match held {
        Some(identity) => InMemoryCustody::new(identity),
        None => InMemoryCustody::empty(),
    });
    let keychain = Arc::new(StoreKeychain::new(
        Keychain::in_memory("store-custody").unwrap(),
        StoreId(UuidIds.new_id()),
    ));
    let custody = StoreCustody::new(
        Arc::new(KeySession::store(Arc::new(InMemoryCustody::<StoreKeyring>::empty())).unwrap()),
        Arc::new(KeySession::member(saved.clone()).unwrap()),
        keychain,
    );
    (custody, saved)
}

/// Making a member's identity writes nothing to custody, so a creation
/// that fails or panics there has nothing to undo; and one already held is
/// refused, so a creation never replaces it (E11).
#[test]
fn making_an_identity_writes_nothing_and_refuses_one_already_held() {
    let (custody, saved) = custody_holding(None);
    let member = MemberId(UuidIds.new_id());
    let made = custody.new_identity(member).unwrap();
    assert_eq!(made.member(), member);
    assert!(saved.unlock().unwrap().is_none(), "making wrote custody");

    let held = MemberIdentity::generate(MemberId(UuidIds.new_id())).unwrap();
    let (custody, saved) = custody_holding(Some(held.clone()));
    assert!(matches!(
        custody.new_identity(MemberId(UuidIds.new_id())),
        Err(IdentityError::AlreadyInitialized)
    ));
    assert_eq!(saved.unlock().unwrap().unwrap().keys().public_keys(), held.keys().public_keys());
}
