use super::*;
use std::error::Error;

#[test]
fn unavailable_randomness_preserves_the_operating_system_cause() {
    let mut bytes = [0; 32];
    let error = fill_with(&mut bytes, |_| Err(getrandom::Error::UNSUPPORTED)).unwrap_err();
    assert!(matches!(&error, CryptoError::Unavailable(_)));
    let cause = error
        .source()
        .unwrap()
        .downcast_ref::<getrandom::Error>()
        .unwrap();
    assert_eq!(*cause, getrandom::Error::UNSUPPORTED);
}
