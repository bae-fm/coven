use super::*;
use std::error::Error;

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
    assert_eq!(setup.failure(), StorageSetupFailure::Network);
    let StorageSetupError::OAuth(OAuthError::Storage(error)) = setup else {
        panic!("sign-in cause was lost")
    };
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
    assert_eq!(setup.failure(), StorageSetupFailure::SecureStorage);
    assert!(matches!(
        setup,
        StorageSetupError::SecureStorage(KeyError::ServiceNotRegistered)
    ));
    assert_eq!(
        StorageSetupError::from(OAuthError::Cancelled).failure(),
        StorageSetupFailure::Authentication
    );
    assert_eq!(
        StorageSetupError::from(OAuthError::Unavailable(CloudProvider::S3)).failure(),
        StorageSetupFailure::InvalidConfiguration
    );
    assert_eq!(
        StorageSetupError::Internal(Box::new(std::io::Error::other("local setup"))).failure(),
        StorageSetupFailure::Internal
    );
}
