//! Device-only custody, and the person's restore-code and S3-key entries:
//! iCloud Keychain on Apple, Block Store on Android (§13.1).

#[cfg(target_os = "android")]
use super::block_store::BlockStore;
#[cfg(target_os = "android")]
use super::block_store_jni::JniBlockStore;
use super::keychain::{EntryScope, KeychainBackend};
#[cfg(any(target_os = "macos", target_os = "ios"))]
use super::person_entries::restore_code_store;
use super::{KeyError, KeychainError};
use crate::SecretBytes;
use coven_foundation::id_source::StoreId;
use std::sync::Arc;

pub(crate) struct NativeKeychain {
    device_only: Arc<keyring_core::CredentialStore>,
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    synced: Arc<keyring_core::CredentialStore>,
    /// Block Store, or `None` on a phone without Google Play services, which
    /// keeps no saved entries (§13.1).
    #[cfg(target_os = "android")]
    block_store: Option<BlockStore<JniBlockStore>>,
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
            #[cfg(target_os = "android")]
            block_store: JniBlockStore::new()?.map(BlockStore::new),
        })
    }

    /// The phone's Block Store, or `Unsupported` without Google Play services.
    #[cfg(target_os = "android")]
    fn block_store(&self) -> Result<&BlockStore<JniBlockStore>, KeyError> {
        self.block_store.as_ref().ok_or(KeyError::Unsupported)
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
            EntryScope::Person => self
                .synced
                .build(service, account, None)
                .map_err(|error| KeychainError::from(error).into()),
            #[cfg(not(any(target_os = "macos", target_os = "ios")))]
            EntryScope::Person => Err(KeyError::Unsupported),
        }
    }
}

impl KeychainBackend for NativeKeychain {
    #[cfg(target_os = "android")]
    fn supports_person_entries(&self) -> bool {
        self.block_store.is_some()
    }

    #[cfg(not(target_os = "android"))]
    fn supports_person_entries(&self) -> bool {
        cfg!(any(target_os = "macos", target_os = "ios"))
    }

    /// Block Store holds 1,024 bytes for an entry, its name included.
    #[cfg(target_os = "android")]
    fn person_value_limit(&self, service: &str, account: &str) -> Option<usize> {
        Some(BlockStore::<JniBlockStore>::value_limit(service, account))
    }

    /// iCloud Keychain sets no limit a restore code reaches.
    #[cfg(not(target_os = "android"))]
    fn person_value_limit(&self, _service: &str, _account: &str) -> Option<usize> {
        None
    }

    fn read(
        &self,
        scope: EntryScope,
        service: &str,
        account: &str,
    ) -> Result<Option<SecretBytes>, KeyError> {
        #[cfg(target_os = "android")]
        if scope == EntryScope::Person {
            return self.block_store()?.read(service, account);
        }
        match self.entry(scope, service, account)?.get_secret() {
            Ok(bytes) => Ok(Some(SecretBytes::new(bytes))),
            Err(keyring_core::Error::NoEntry) => {
                tracing::debug!(account, ?scope, "keychain entry is absent");
                Ok(None)
            }
            Err(error) => Err(KeychainError::from(error).into()),
        }
    }

    fn write(
        &self,
        scope: EntryScope,
        service: &str,
        account: &str,
        bytes: &[u8],
    ) -> Result<(), KeyError> {
        #[cfg(target_os = "android")]
        if scope == EntryScope::Person {
            return self.block_store()?.write(service, account, bytes);
        }
        self.entry(scope, service, account)?
            .set_secret(bytes)
            .map_err(|error| KeychainError::from(error).into())
    }

    fn delete(&self, scope: EntryScope, service: &str, account: &str) -> Result<(), KeyError> {
        #[cfg(target_os = "android")]
        if scope == EntryScope::Person {
            return self.block_store()?.delete(service, account);
        }
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
    fn person_restore_codes(&self, service: &str) -> Result<Vec<(StoreId, SecretBytes)>, KeyError> {
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

    #[cfg(target_os = "android")]
    fn person_restore_codes(&self, service: &str) -> Result<Vec<(StoreId, SecretBytes)>, KeyError> {
        self.block_store()?.restore_codes(service)
    }

    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "android")))]
    fn person_restore_codes(
        &self,
        _service: &str,
    ) -> Result<Vec<(StoreId, SecretBytes)>, KeyError> {
        Err(KeyError::Unsupported)
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    fn person_copies(&self, service: &str, account: &str) -> Result<Vec<SecretBytes>, KeyError> {
        self.copies(service, account)?
            .iter()
            .map(|entry| {
                Ok(SecretBytes::new(
                    entry.get_secret().map_err(KeychainError::from)?,
                ))
            })
            .collect()
    }

    /// Block Store keeps one value per key: never more than one copy.
    #[cfg(target_os = "android")]
    fn person_copies(&self, service: &str, account: &str) -> Result<Vec<SecretBytes>, KeyError> {
        Ok(self
            .block_store()?
            .read(service, account)?
            .into_iter()
            .collect())
    }

    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "android")))]
    fn person_copies(&self, _service: &str, _account: &str) -> Result<Vec<SecretBytes>, KeyError> {
        Err(KeyError::Unsupported)
    }

    /// Each search result names its keychain group, so writing or deleting
    /// through it reaches only that group's copy.
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    fn replace_person_entry(
        &self,
        service: &str,
        account: &str,
        bytes: &[u8],
    ) -> Result<(), KeyError> {
        let copies = self.copies(service, account)?;
        let Some((first, others)) = copies.split_first() else {
            return self.write(EntryScope::Person, service, account, bytes);
        };
        first.set_secret(bytes).map_err(KeychainError::from)?;
        for other in others {
            match other.delete_credential() {
                Ok(()) | Err(keyring_core::Error::NoEntry) => {}
                Err(error) => return Err(KeychainError::from(error).into()),
            }
        }
        Ok(())
    }

    #[cfg(target_os = "android")]
    fn replace_person_entry(
        &self,
        service: &str,
        account: &str,
        bytes: &[u8],
    ) -> Result<(), KeyError> {
        self.block_store()?.write(service, account, bytes)
    }

    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "android")))]
    fn replace_person_entry(
        &self,
        _service: &str,
        _account: &str,
        _bytes: &[u8],
    ) -> Result<(), KeyError> {
        Err(KeyError::Unsupported)
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
impl NativeKeychain {
    /// The synced entries for exactly this service and account, one per
    /// keychain group that holds one. Apple matches both exactly; the result
    /// is filtered again so a store matching loosely finds no other entry.
    fn copies(&self, service: &str, account: &str) -> Result<Vec<keyring_core::Entry>, KeyError> {
        let query = std::collections::HashMap::from([("service", service), ("account", account)]);
        let entries = self.synced.search(&query).map_err(KeychainError::from)?;
        Ok(entries
            .into_iter()
            .filter(|entry| {
                entry
                    .get_specifiers()
                    .is_some_and(|(found_service, found)| {
                        found_service == service && found == account
                    })
            })
            .collect())
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
