use super::*;
use std::error::Error;

#[test]
fn encoding_preserves_an_opaque_cause() {
    let error = StorageFailure::Encoding.with_source(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "invalid encoded data",
    ));
    assert_eq!(error.failure(), StorageFailure::Encoding);
    assert!(!error.retryable());
    assert_eq!(error.to_string(), "Encoding: invalid encoded data");
    let source = error
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(source.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(source.to_string(), "invalid encoded data");
}

#[test]
fn setup_preserves_sign_in_and_custody_causes() {
    let network = StorageError::Provider {
        provider: CloudProvider::Dropbox,
        failure: StorageFailure::Network,
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "token endpoint",
        )),
    };
    let setup = StorageSetupError::from(OAuthError::Storage(network));
    let StorageSetupError::OAuth(OAuthError::Storage(error)) = setup else {
        panic!("sign-in cause was lost")
    };
    assert_eq!(error.failure(), StorageFailure::Network);
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .kind(),
        std::io::ErrorKind::TimedOut,
    );
    let setup = StorageSetupError::from(KeyError::ServiceNotRegistered);
    assert!(matches!(
        setup,
        StorageSetupError::SecureStorage(KeyError::ServiceNotRegistered)
    ));
}
