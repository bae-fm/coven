use super::*;
use std::error::Error as _;

#[test]
fn transport_and_io_are_retryable_config_and_not_found_are_not() {
    assert!(CloudHomeError::Transport("timeout".to_string()).is_retryable());
    assert!(CloudHomeError::Io(std::io::Error::other("disk")).is_retryable());
    assert!(!CloudHomeError::Configuration("bucket not set".to_string()).is_retryable());
    assert!(!CloudHomeError::NotFound("key".to_string()).is_retryable());
    assert!(!CloudHomeError::AlreadyExists("key".to_string()).is_retryable());
}

#[test]
fn transport_source_survives_the_protocol_storage_boundary() {
    let cloud_error = CloudHomeError::transport(
        "read provider response",
        std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset"),
    );

    let storage_error = coven_protocol::objects::StorageError::from(cloud_error);
    let cloud_source = storage_error
        .source()
        .and_then(|source| source.downcast_ref::<CloudHomeError>())
        .expect("storage error should retain the cloud error");
    let io_source = cloud_source
        .source()
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .expect("cloud error should retain the provider source");

    assert_eq!(io_source.kind(), std::io::ErrorKind::ConnectionReset);
    assert!(storage_error.is_transport());
}
