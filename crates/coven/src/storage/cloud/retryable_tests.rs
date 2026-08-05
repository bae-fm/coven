use super::*;

#[test]
fn transport_and_io_are_retryable_config_and_not_found_are_not() {
    assert!(CloudHomeError::Transport("timeout".to_string()).is_retryable());
    assert!(CloudHomeError::Io(std::io::Error::other("disk")).is_retryable());
    assert!(!CloudHomeError::Configuration("bucket not set".to_string()).is_retryable());
    assert!(!CloudHomeError::NotFound("key".to_string()).is_retryable());
    assert!(!CloudHomeError::AlreadyExists("key".to_string()).is_retryable());
}
