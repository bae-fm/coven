//! Device-only custody and Apple's separate synced restore-code store.

#[cfg(any(target_os = "macos", target_os = "ios"))]
use super::keychain::restore_code_store;
use super::keychain::EntryScope;
use super::{KeyError, KeychainError};
use crate::SecretBytes;
use coven_foundation::id_source::StoreId;
use std::sync::Arc;

pub(crate) struct NativeKeychain {
    device_only: Arc<keyring_core::CredentialStore>,
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    synced: Arc<keyring_core::CredentialStore>,
}

impl NativeKeychain {
    pub(crate) fn new() -> Result<Self, KeyError> {
        Ok(Self {
            device_only: store()?,
            #[cfg(any(target_os = "macos", target_os = "ios"))]
            synced: {
                let configuration = std::collections::HashMap::from([("cloud-sync", "true")]);
                apple_native_keyring_store::protected::Store::new_with_configuration(&configuration)
                    .map_err(KeychainError::from)?
            },
        })
    }

    fn entry(
        &self,
        scope: EntryScope,
        service: &str,
        account: &str,
    ) -> Result<keyring_core::Entry, KeyError> {
        match scope {
            EntryScope::DeviceOnly => {
                #[cfg(any(target_os = "macos", target_os = "ios"))]
                let modifiers = Some(std::collections::HashMap::from([(
                    "access-policy",
                    "when-unlocked-this-device-only",
                )]));
                #[cfg(not(any(target_os = "macos", target_os = "ios")))]
                let modifiers = None;
                self.device_only
                    .build(service, account, modifiers.as_ref())
                    .map_err(|error| KeychainError::from(error).into())
            }
            #[cfg(any(target_os = "macos", target_os = "ios"))]
            EntryScope::Synced => self
                .synced
                .build(service, account, None)
                .map_err(|error| KeychainError::from(error).into()),
            #[cfg(not(any(target_os = "macos", target_os = "ios")))]
            EntryScope::Synced => Err(KeyError::Unsupported),
        }
    }

    pub(crate) fn read(
        &self,
        scope: EntryScope,
        service: &str,
        account: &str,
    ) -> Result<Option<SecretBytes>, KeyError> {
        match self.entry(scope, service, account)?.get_secret() {
            Ok(bytes) => Ok(Some(SecretBytes::new(bytes))),
            Err(keyring_core::Error::NoEntry) => {
                tracing::debug!(account, ?scope, "keychain entry is absent");
                Ok(None)
            }
            Err(error) => Err(KeychainError::from(error).into()),
        }
    }

    pub(crate) fn write(
        &self,
        scope: EntryScope,
        service: &str,
        account: &str,
        bytes: &[u8],
    ) -> Result<(), KeyError> {
        self.entry(scope, service, account)?
            .set_secret(bytes)
            .map_err(|error| KeychainError::from(error).into())
    }

    pub(crate) fn delete(
        &self,
        scope: EntryScope,
        service: &str,
        account: &str,
    ) -> Result<(), KeyError> {
        match self.entry(scope, service, account)?.delete_credential() {
            Ok(()) => Ok(()),
            Err(keyring_core::Error::NoEntry) => {
                tracing::debug!(account, ?scope, "keychain entry is already absent");
                Ok(())
            }
            Err(error) => Err(KeychainError::from(error).into()),
        }
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub(crate) fn synced_restore_codes(
        &self,
        service: &str,
    ) -> Result<Vec<(StoreId, SecretBytes)>, KeyError> {
        let query = std::collections::HashMap::from([("service", service)]);
        let entries = self.synced.search(&query).map_err(KeychainError::from)?;
        let mut codes = Vec::new();
        for entry in entries {
            let (found_service, account) = entry.get_specifiers().ok_or_else(|| {
                KeychainError::from(keyring_core::Error::Invalid(
                    "search result".into(),
                    "missing service and account".into(),
                ))
            })?;
            if found_service != service {
                return Err(KeychainError::from(keyring_core::Error::Invalid(
                    "search result".into(),
                    "unexpected service".into(),
                ))
                .into());
            }
            if let Some(store) = restore_code_store(&account)? {
                let bytes = entry.get_secret().map_err(KeychainError::from)?;
                codes.push((store, SecretBytes::new(bytes)));
            }
        }
        Ok(codes)
    }

    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    pub(crate) fn synced_restore_codes(
        &self,
        _service: &str,
    ) -> Result<Vec<(StoreId, SecretBytes)>, KeyError> {
        Err(KeyError::Unsupported)
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn store() -> Result<Arc<keyring_core::CredentialStore>, KeyError> {
    Ok(apple_native_keyring_store::protected::Store::new().map_err(KeychainError::from)?)
}

#[cfg(target_os = "android")]
fn store() -> Result<Arc<keyring_core::CredentialStore>, KeyError> {
    Ok(android_native_keyring_store::Store::new().map_err(KeychainError::from)?)
}

#[cfg(target_os = "windows")]
fn store() -> Result<Arc<keyring_core::CredentialStore>, KeyError> {
    Ok(windows_native_keyring_store::Store::new().map_err(KeychainError::from)?)
}

#[cfg(target_os = "linux")]
fn store() -> Result<Arc<keyring_core::CredentialStore>, KeyError> {
    Ok(zbus_secret_service_keyring_store::Store::new().map_err(KeychainError::from)?)
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "android",
    target_os = "windows",
    target_os = "linux"
)))]
fn store() -> Result<Arc<keyring_core::CredentialStore>, KeyError> {
    Err(KeyError::UnsupportedKeyringPlatform)
}

#[cfg(test)]
#[path = "platform_tests.rs"]
mod tests;
