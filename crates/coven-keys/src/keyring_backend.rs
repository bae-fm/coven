//! Installs the process-wide keyring-core default store. Without a store,
//! keyring-core does nothing and every key operation fails; coven bundles the
//! platform store per target and installs it as part of keyring registration.

use crate::keys::KeyError;

/// Ensure the process-wide keyring store is installed. Idempotent: if a store is
/// already installed (a prior registration, or a test's mock), it is kept.
/// Returns [`KeyError::StoreNotInstalled`] on a target with no bundled store.
pub(crate) fn install_platform_store() -> Result<(), KeyError> {
    if keyring_core::get_default_store().is_some() {
        return Ok(());
    }
    install_bundled_store()
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn install_bundled_store() -> Result<(), KeyError> {
    let configuration = std::collections::HashMap::from([("cloud-sync", "true")]);
    let store =
        apple_native_keyring_store::protected::Store::new_with_configuration(&configuration)
            .map_err(KeyError::Keyring)?;
    keyring_core::set_default_store(store);
    Ok(())
}

#[cfg(target_os = "android")]
fn install_bundled_store() -> Result<(), KeyError> {
    let store = android_native_keyring_store::Store::new().map_err(KeyError::Keyring)?;
    keyring_core::set_default_store(store);
    Ok(())
}

#[cfg(target_os = "windows")]
fn install_bundled_store() -> Result<(), KeyError> {
    let store = windows_native_keyring_store::Store::new().map_err(KeyError::Keyring)?;
    keyring_core::set_default_store(store);
    Ok(())
}

#[cfg(target_os = "linux")]
fn install_bundled_store() -> Result<(), KeyError> {
    let store = zbus_secret_service_keyring_store::Store::new().map_err(KeyError::Keyring)?;
    keyring_core::set_default_store(store);
    Ok(())
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "android",
    target_os = "windows",
    target_os = "linux"
)))]
fn install_bundled_store() -> Result<(), KeyError> {
    Err(KeyError::UnsupportedKeyringPlatform)
}
