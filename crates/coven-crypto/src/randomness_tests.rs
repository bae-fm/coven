use super::*;
use std::error::Error;

#[test]
fn unavailable_randomness_preserves_the_operating_system_cause() {
    let mut bytes = [0; 32];
    let error = fill_with(&mut bytes, |_| Err(getrandom::Error::UNSUPPORTED)).unwrap_err();
    assert!(matches!(&error, CryptoError::Unavailable(_)));
    let cause = error.source().unwrap().downcast_ref::<getrandom::Error>().unwrap();
    assert_eq!(*cause, getrandom::Error::UNSUPPORTED);
}

#[test]
fn random_secrets_are_fresh_hex() {
    let first = random_secret().unwrap();
    let second = random_secret().unwrap();
    assert_eq!(first.as_str().len(), 64);
    assert!(first.as_str().bytes().all(|b| b.is_ascii_hexdigit()));
    assert_ne!(first.as_str(), second.as_str());
}
