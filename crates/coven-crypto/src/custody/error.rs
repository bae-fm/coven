//! Custody failures, retaining causes without printing secret-bearing buffers.

use zeroize::Zeroize;

/// Why an app-supplied host secret name is refused (E11).
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SecretNameError {
    /// Names cannot be empty.
    #[error("host secret name is empty")]
    Empty,
    /// Colon separates an entry's name from its store id.
    #[error("host secret name contains ':'")]
    Separator,
    /// The name belongs to coven's own entries.
    #[error("host secret name is reserved for coven")]
    Reserved,
    /// A NUL cannot be represented by native credential APIs.
    #[error("host secret name contains NUL")]
    Nul,
}

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
    Keychain(#[from] KeychainError),
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
    /// Synced restore codes require an Apple keychain.
    #[error("synced restore codes are unsupported on this platform")]
    Unsupported,
    /// A synced restore-code account contains an invalid store id.
    #[error("invalid store id in synced restore-code entry")]
    RestoreCodeStoreId,
    /// Multiple accessible keychain groups contain a restore code for one store.
    #[error("multiple synced restore codes for store {0}")]
    AmbiguousRestoreCode(coven_foundation::id_source::StoreId),
    /// The host name could collide with a coven entry or cannot name an entry.
    #[error("invalid host secret name: {0}")]
    SecretName(#[from] SecretNameError),
    /// An app secret was stored with bytes that are not UTF-8.
    #[error("host secret is not UTF-8")]
    HostSecretEncoding,
}

/// A native keychain error whose diagnostic output never includes secret bytes.
/// Its original typed cause remains available through `std::error::Error`.
pub struct KeychainError(keyring_core::Error);

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
