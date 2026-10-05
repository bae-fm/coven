use super::*;
use crate::custody::MemberKeyCustody;
use crate::MemberKeys;

fn poisoned_custody() -> InMemoryCustody<MemberKeys> {
    let custody = InMemoryCustody::new(MemberKeys::generate().unwrap());
    let poisoned = std::panic::catch_unwind(|| {
        let _guard = custody.secret.lock().unwrap();
        panic!("poison custody secret");
    });
    assert!(poisoned.is_err());
    custody
}

#[test]
#[should_panic(expected = "in-memory custody secret lock is poisoned")]
fn unlocking_propagates_a_poisoned_secret_lock() {
    let _keys = poisoned_custody().unlock();
}

#[test]
#[should_panic(expected = "in-memory custody secret lock is poisoned")]
fn persisting_propagates_a_poisoned_secret_lock() {
    let _persisted = poisoned_custody().persist(&MemberKeys::generate().unwrap());
}

#[test]
#[should_panic(expected = "in-memory custody secret lock is poisoned")]
fn forgetting_propagates_a_poisoned_secret_lock() {
    let _forgotten = poisoned_custody().forget();
}
