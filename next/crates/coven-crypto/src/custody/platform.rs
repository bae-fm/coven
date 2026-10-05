//! Native credential stores are built only while registering the service.

use super::{KeyError, KeychainError};
use std::sync::Arc;

#[cfg(any(target_os = "macos", target_os = "ios"))]
pub(crate) fn store() -> Result<Arc<keyring_core::CredentialStore>, KeyError> {
    let configuration = std::collections::HashMap::from([("cloud-sync", "true")]);
    Ok(
        apple_native_keyring_store::protected::Store::new_with_configuration(&configuration)
            .map_err(KeychainError::from)?,
    )
}

#[cfg(target_os = "android")]
pub(crate) fn store() -> Result<Arc<keyring_core::CredentialStore>, KeyError> {
    Ok(android_native_keyring_store::Store::new().map_err(KeychainError::from)?)
}

#[cfg(target_os = "windows")]
pub(crate) fn store() -> Result<Arc<keyring_core::CredentialStore>, KeyError> {
    Ok(windows_native_keyring_store::Store::new().map_err(KeychainError::from)?)
}

#[cfg(target_os = "linux")]
pub(crate) fn store() -> Result<Arc<keyring_core::CredentialStore>, KeyError> {
    Ok(zbus_secret_service_keyring_store::Store::new().map_err(KeychainError::from)?)
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "android",
    target_os = "windows",
    target_os = "linux"
)))]
pub(crate) fn store() -> Result<Arc<keyring_core::CredentialStore>, KeyError> {
    Err(KeyError::UnsupportedKeyringPlatform)
}
