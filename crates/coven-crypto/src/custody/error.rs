//! Custody failures, retaining causes without printing secret-bearing buffers.

use zeroize::Zeroize;

/// A failure unlocking, keeping or forgetting keys or host secrets (E11).
#[derive(Debug, thiserror::Error)]
pub enum KeyError {
    /// The device-only installation id is malformed.
    #[error("invalid device identity entry")]
    DeviceIdEncoding,
    /// The handle that owns this custody has closed.
    #[error("store is closed")]
    StoreClosed,
    /// A filesystem operation failed; the cause says whether bytes changed.
    #[error("custody file failed: {0}")]
    File(#[from] coven_foundation::files::FileError),
    /// A cryptographic service was unavailable or stored bytes failed validation.
    #[error("custody cryptography failed: {0}")]
    Crypto(#[from] crate::CryptoError),
    /// Decrypted or keychain-provided material was malformed.
    #[error("invalid custody material: {0}")]
    Material(#[from] crate::MaterialError),
    /// A wrong passphrase or altered file failed authentication.
    #[error("wrong passphrase or damaged custody file")]
    PassphraseAuthentication,
    /// The passphrase file's header was malformed or unsupported.
    #[error("invalid passphrase file header")]
    PassphraseHeader,
    /// Recorded Argon2id parameters exceed the accepted resource bounds.
    #[error("Argon2id parameters are outside the accepted bounds")]
    PassphraseParameters,
    /// The device lacks resources needed to unlock or keep keys.
    #[error("key custody resources unavailable: {0}")]
    Unavailable(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The OS credential store refused an operation.
    #[error("keychain failed: {0}")]
    Keychain(#[source] KeychainError),
    /// Custody cannot be accessed right now, but the same call succeeds once
    /// the session unlocks: native NoStorageAccess, or on Apple
    /// errSecInteractionNotAllowed (OSStatus -25308), which a locked device,
    /// a sleeping display or a session that can't show UI answers (§12.4).
    /// The native cause says why access was denied.
    #[error("keychain is unavailable in this session: {0}")]
    KeychainTemporarilyUnavailable(#[source] KeychainError),
    /// Apple's errSecMissingEntitlement (OSStatus -34018): the app has no
    /// keychain-access-groups entitlement backed by a provisioning profile.
    /// Every keychain call fails the same way until the app is signed with
    /// one; retrying changes nothing.
    #[error(
        "the app lacks its keychain-access-groups entitlement \
         (errSecMissingEntitlement, OSStatus -34018): {0}"
    )]
    MissingKeychainEntitlement(#[source] KeychainError),
    /// The startup registration has not happened.
    #[error("call set_keyring_service before constructing keychain custody")]
    ServiceNotRegistered,
    /// A process cannot change its service name after registration.
    #[error("keyring service is already registered under a different name")]
    ServiceAlreadyRegistered,
    /// The service name is empty or contains a NUL.
    #[error("keyring service requires a nonempty name without NUL")]
    InvalidServiceName,
    /// This target has no native credential store implementation.
    #[error("unsupported keyring platform")]
    UnsupportedKeyringPlatform,
    /// The person's entries need iCloud Keychain or Block Store: Apple
    /// platforms, or an Android phone with Google Play services.
    #[error("saved restore codes need iCloud Keychain or Block Store")]
    Unsupported,
    /// Block Store already holds its 16 entries for the app, so a new entry
    /// can't be saved (§13.1). Forgetting a saved code makes room.
    #[error("Block Store already holds its 16 entries for this app")]
    SavedEntriesFull,
    /// A saved code doesn't fit in Block Store's 1,024 bytes for an entry,
    /// even with the store's name shortened (§13.1).
    #[error("the saved restore code doesn't fit in a Block Store entry")]
    SavedEntryTooLarge,
    /// The person left the store, so their saved entries stay deleted (§14.1).
    #[error("the person left the store; their saved entries stay deleted")]
    Left,
    /// A saved restore-code account's id part is not a store id as coven
    /// writes it.
    #[error("invalid store id in saved restore-code entry")]
    RestoreCodeStoreId,
}

/// A native keychain error whose diagnostic output never includes secret bytes.
/// Its original typed cause remains available through `std::error::Error`.
pub struct KeychainError(keyring_core::Error);

/// errSecInteractionNotAllowed: the device or session is locked.
#[cfg(any(target_os = "macos", target_os = "ios"))]
const ERR_SEC_INTERACTION_NOT_ALLOWED: i32 = -25308;
/// errSecMissingEntitlement: no keychain-access-groups entitlement.
#[cfg(any(target_os = "macos", target_os = "ios"))]
const ERR_SEC_MISSING_ENTITLEMENT: i32 = -34018;

impl From<KeychainError> for KeyError {
    fn from(error: KeychainError) -> Self {
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        match error.apple_status() {
            Some(ERR_SEC_INTERACTION_NOT_ALLOWED) => {
                return Self::KeychainTemporarilyUnavailable(error)
            }
            Some(ERR_SEC_MISSING_ENTITLEMENT) => return Self::MissingKeychainEntitlement(error),
            _ => {}
        }
        match &error.0 {
            keyring_core::Error::NoStorageAccess(_) => Self::KeychainTemporarilyUnavailable(error),
            _ => Self::Keychain(error),
        }
    }
}

impl KeychainError {
    /// The OSStatus behind an Apple keychain failure. apple-native-keyring-store
    /// gives its own variant only to NoStorageAccess (-25291, -25292) and
    /// NoEntry (-25300); every other status, the locked keychain's -25308
    /// included, arrives as a PlatformFailure boxing security-framework's
    /// error, whose code is the status.
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    fn apple_status(&self) -> Option<i32> {
        let keyring_core::Error::PlatformFailure(cause) = &self.0 else {
            return None;
        };
        cause
            .downcast_ref::<security_framework::base::Error>()
            .map(|status| status.code())
    }
}

impl From<keyring_core::Error> for KeychainError {
    fn from(mut error: keyring_core::Error) -> Self {
        // Native error variants may carry the secret they failed to decode.
        // Preserve the error kind and cause, but erase those bytes immediately.
        match &mut error {
            keyring_core::Error::BadEncoding(bytes)
            | keyring_core::Error::BadDataFormat(bytes, _) => bytes.zeroize(),
            _ => {}
        }
        Self(error)
    }
}

impl std::fmt::Debug for KeychainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A status code carries no secret, and names the refusal.
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        if let Some(status) = self.apple_status() {
            return write!(f, "PlatformFailure(OSStatus {status})");
        }
        let kind = match &self.0 {
            keyring_core::Error::PlatformFailure(_) => "PlatformFailure",
            keyring_core::Error::NoStorageAccess(_) => "NoStorageAccess",
            keyring_core::Error::NoEntry => "NoEntry",
            keyring_core::Error::BadEncoding(_) => "BadEncoding",
            keyring_core::Error::BadDataFormat(_, _) => "BadDataFormat",
            keyring_core::Error::BadStoreFormat(_) => "BadStoreFormat",
            keyring_core::Error::TooLong(_, _) => "TooLong",
            keyring_core::Error::Invalid(_, _) => "Invalid",
            keyring_core::Error::Ambiguous(_) => "Ambiguous",
            keyring_core::Error::NoDefaultStore => "NoDefaultStore",
            keyring_core::Error::NotSupportedByStore(_) => "NotSupportedByStore",
            _ => "NativeError",
        };
        f.write_str(kind)
    }
}

impl std::fmt::Display for KeychainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for KeychainError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
