use super::*;
use crate::custody::MemberKeyCustody;
use crate::MemberIdentity;
use coven_foundation::id_source::MemberId;

fn poisoned_custody() -> InMemoryCustody<MemberIdentity> {
    let custody =
        InMemoryCustody::new(MemberIdentity::generate(MemberId(uuid::Uuid::from_u128(1))).unwrap());
    let poisoned = std::panic::catch_unwind(|| {
        let _guard = custody.secret.lock().unwrap();
        panic!("poison custody secret");
    });
    assert!(poisoned.is_err());
    custody
}

/// Each change to the held secret is one assignment, so a panic while it
/// was held leaves it whole: custody goes on with it (api.md E15).
#[test]
fn a_poisoned_secret_lock_keeps_the_held_secret() {
    let custody = poisoned_custody();
    assert_eq!(
        custody.unlock().unwrap().map(|identity| identity.member()),
        Some(MemberId(uuid::Uuid::from_u128(1)))
    );
    let replaced = MemberIdentity::generate(MemberId(uuid::Uuid::from_u128(2))).unwrap();
    custody.persist(&replaced).unwrap();
    assert_eq!(
        custody.unlock().unwrap().map(|identity| identity.member()),
        Some(MemberId(uuid::Uuid::from_u128(2)))
    );
    custody.forget().unwrap();
    assert!(custody.unlock().unwrap().is_none());
}
