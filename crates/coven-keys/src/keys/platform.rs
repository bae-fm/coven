use tracing::{error, info};

use super::core::{
    public_key_hex, CloudHomeCredentials, DeviceIdentityCustody, KeyError, UserKeypair,
    SIGN_SECRETKEYBYTES,
};

/// Why importing or staging a master key failed.
#[derive(Debug, thiserror::Error)]
pub enum MasterKeyError {
    /// Cloud-home setup found a master key already established while staging
    /// a fresh one. coven never generates over an existing key.
    #[error("a master key is already established for this store")]
    AlreadyEstablished,
    #[error("cannot import a master key while a cloud home is connected")]
    CloudHomeConnected,
    #[error("key error: {0}")]
    Key(#[from] KeyError),
    #[error("invalid master key material: {0}")]
    Encryption(#[from] crate::encryption::EncryptionError),
}

/// Why a host's `initialize_identity` call failed.
#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    /// `initialize_identity` found an identity already established for this
    /// store — custody `unlock()` returned `Some`. coven never generates over
    /// an existing identity; a store's identity is established exactly once,
    /// by whichever of create/join/restore established it first.
    #[error("an identity is already established for this store")]
    AlreadyEstablished,
    #[error("key error: {0}")]
    Key(#[from] KeyError),
}

struct KeyringService {
    name: String,
    worker: KeyringWorker,
}

/// Serializes platform credential-store calls on a stack Coven controls.
/// Hosts can enter the synchronous key API from foreign runtimes whose worker
/// stacks are smaller than Security.framework's call chain requires.
struct KeyringWorker {
    operations: Option<std::sync::mpsc::Sender<KeyringOperation>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

enum KeyringOperation {
    Read {
        account: String,
        reply: std::sync::mpsc::SyncSender<Result<Option<String>, KeyError>>,
    },
    Write {
        account: String,
        value: String,
        reply: std::sync::mpsc::SyncSender<Result<(), KeyError>>,
    },
    Delete {
        account: String,
        reply: std::sync::mpsc::SyncSender<Result<bool, KeyError>>,
    },
    #[cfg(all(
        any(test, feature = "test-utils"),
        any(target_os = "macos", target_os = "ios")
    ))]
    AppleEntryFacts {
        account: String,
        reply: std::sync::mpsc::SyncSender<Result<AppleKeyringEntryFacts, KeyError>>,
    },
    #[cfg(any(test, feature = "test-utils"))]
    SetNextError {
        account: String,
        error: keyring_core::Error,
        reply: std::sync::mpsc::SyncSender<Result<(), KeyError>>,
    },
}

struct KeyringBackend {
    name: String,
    store: std::sync::Arc<keyring_core::CredentialStore>,
}

impl KeyringWorker {
    /// Matches Coven's provider runtimes: enough stack for platform SDK call
    /// chains without depending on the host thread's stack allocation.
    const STACK_SIZE: usize = 16 * 1024 * 1024;

    fn start(backend: KeyringBackend) -> Result<Self, KeyError> {
        let (operations, receiver) = std::sync::mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("coven-keyring".to_string())
            .stack_size(Self::STACK_SIZE)
            .spawn(move || {
                while let Ok(operation) = receiver.recv() {
                    backend.execute(operation);
                }
            })
            .map_err(KeyError::KeyringWorkerStart)?;
        Ok(Self {
            operations: Some(operations),
            thread: Some(thread),
        })
    }

    fn execute<T: Send + 'static>(
        &self,
        operation_name: &'static str,
        operation: impl FnOnce(std::sync::mpsc::SyncSender<Result<T, KeyError>>) -> KeyringOperation,
    ) -> Result<T, KeyError> {
        let (reply, receiver) = std::sync::mpsc::sync_channel(1);
        self.operations
            .as_ref()
            .expect("the keyring worker sender exists until drop")
            .send(operation(reply))
            .map_err(|_| KeyError::KeyringWorkerStopped {
                operation: operation_name,
            })?;
        receiver
            .recv()
            .map_err(|_| KeyError::KeyringWorkerStopped {
                operation: operation_name,
            })?
    }
}

impl Drop for KeyringWorker {
    fn drop(&mut self) {
        self.operations.take();
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                error!("Keyring worker terminated with a panic");
            }
        }
    }
}

#[derive(Clone, Copy)]
enum KeyringBinding {
    Registered(&'static KeyringService),
    Unregistered,
}

impl KeyringBinding {
    fn service(self) -> Result<&'static KeyringService, KeyError> {
        match self {
            Self::Registered(service) => Ok(service),
            Self::Unregistered => Err(KeyError::ServiceNotRegistered),
        }
    }
}

static KEYRING_SERVICE: std::sync::OnceLock<KeyringService> = std::sync::OnceLock::new();

/// Register the process-wide keyring: the service name every entry is stored
/// under, and the platform keyring store that backs it. Both are one-time
/// startup registration and must run before any key operation. The store is
/// installed before the name is recorded, so a failed installation leaves no
/// registration behind. Re-registering the same name is a no-op; a different
/// name is a startup contradiction and fails. Fails with
/// [`KeyError::UnsupportedKeyringPlatform`] on a target with no bundled store.
pub fn set_keyring_service(name: impl Into<String>) -> Result<(), KeyError> {
    crate::keyring_backend::install_platform_store()?;
    let name = name.into();
    let store = keyring_core::get_default_store().ok_or(KeyError::StoreNotInstalled)?;
    let service = KeyringService::new(name.clone(), store)?;
    if KEYRING_SERVICE.set(service).is_err() {
        let registered = KEYRING_SERVICE
            .get()
            .map(|service| service.name.as_str())
            .expect("a keyring service is registered when set() fails");
        if registered != name {
            return Err(KeyError::ServiceAlreadyRegistered {
                registered: registered.to_string(),
                requested: name,
            });
        }
    }
    Ok(())
}

/// The registered keyring service name. `Err` when the host never ran the
/// startup [`set_keyring_service`] call — surfaced so a mis-ordered host gets a
/// typed error, not a panic deep inside a key operation.
pub fn keyring_service() -> Result<&'static str, KeyError> {
    KEYRING_SERVICE
        .get()
        .map(|service| service.name.as_str())
        .ok_or(KeyError::ServiceNotRegistered)
}

fn registered_keyring() -> Result<&'static KeyringService, KeyError> {
    KEYRING_SERVICE.get().ok_or(KeyError::ServiceNotRegistered)
}

fn map_keyring_error(e: keyring_core::Error) -> KeyError {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    if is_missing_keychain_entitlement(&e) {
        return KeyError::MissingKeychainEntitlement;
    }
    match e {
        keyring_core::Error::NoDefaultStore => KeyError::StoreNotInstalled,
        other => KeyError::Keyring(other),
    }
}

/// `errSecMissingEntitlement` (OSStatus -34018) arrives from
/// `apple-native-keyring-store`'s `protected::decode_error` as
/// `keyring_core::Error::PlatformFailure`, whose payload is a
/// `Box<dyn std::error::Error + Send + Sync>` — the OSStatus is not exposed as
/// a field on `keyring_core::Error` itself. The box's concrete type is always
/// `security_framework::base::Error` on this path (verified by reading
/// `apple-native-keyring-store`'s `protected.rs`: every `decode_error` arm
/// boxes the `security_framework::base::Error` it received), so `downcast_ref`
/// recovers it and `.code()` reads the real OSStatus — a structured match, not
/// a string search over the formatted error.
#[cfg(any(target_os = "macos", target_os = "ios"))]
fn is_missing_keychain_entitlement(e: &keyring_core::Error) -> bool {
    let keyring_core::Error::PlatformFailure(inner) = e else {
        return false;
    };
    inner
        .downcast_ref::<security_framework::base::Error>()
        .is_some_and(|err| err.code() == -34018)
}

/// The base account name [`KeyringSlot::DeviceSigningKey`] renders as
/// `{base}:{store_id}` under.
const DEVICE_SIGNING_KEY_BASE: &str = "coven_user_signing_key";
/// The base account name [`KeyringSlot::EncryptionMasterKey`] renders as
/// `{base}:{store_id}` under.
const ENCRYPTION_MASTER_KEY_BASE: &str = "encryption_master_key";
/// The base account name [`KeyringSlot::CloudHomeCredentials`] renders as
/// `{base}:{store_id}` under.
const CLOUD_HOME_CREDENTIALS_BASE: &str = "cloud_home_credentials";
/// The base account name [`KeyringSlot::PendingIdentity`] renders as
/// `{base}:{pending_public_key_hex}` under.
const PENDING_IDENTITY_BASE: &str = "coven_pending_identity";

/// Every name coven's own [`KeyringSlot`] variants reserve for themselves —
/// built from the same constants [`KeyringSlot::account`] renders accounts
/// from, so this list cannot drift from what coven actually stores under. A
/// host secret's `name` must not equal any of these: see
/// [`validate_host_secret_name`].
pub(crate) const RESERVED_HOST_SECRET_NAMES: &[&str] = &[
    DEVICE_SIGNING_KEY_BASE,
    ENCRYPTION_MASTER_KEY_BASE,
    CLOUD_HOME_CREDENTIALS_BASE,
    PENDING_IDENTITY_BASE,
];

/// Which key a keyring entry holds, and the sole owner of the account name it
/// is stored under. The device signing key, the encryption master key, the
/// cloud-home credentials, and a host secret are all per store; a pending
/// identity is keyed by its own public key instead of a store (it exists while
/// the device pairing has not established the Store-scoped identity — see
/// [`crate::keys::mint_pending_identity`]). Every keyring read/write/delete
/// names its entry with one of these variants, so the on-disk account
/// strings live in exactly one place: [`KeyringSlot::account`].
pub(crate) enum KeyringSlot {
    /// A store's Ed25519 signing identity.
    DeviceSigningKey(String),
    /// A store's encryption master key.
    EncryptionMasterKey(String),
    /// A store's cloud-home credentials.
    CloudHomeCredentials(String),
    /// A pairing attempt's not-yet-store-scoped signing identity, keyed by its
    /// own public key.
    PendingIdentity(String),
    /// A host's own store-scoped secret, named by the host and validated
    /// against [`RESERVED_HOST_SECRET_NAMES`] before it ever reaches this
    /// variant (see [`validate_host_secret_name`]).
    HostSecret { name: String, store_id: String },
}

impl KeyringSlot {
    /// The keyring account name this slot is stored under. These strings are a
    /// durable storage contract: a device's already-stored keys are found only
    /// at these exact accounts, so changing any of them strands stored keys.
    /// `HostSecret`'s rendering is a storage contract with the *host*, not
    /// just coven: it must stay byte-identical to whatever account a host
    /// already wrote its secrets under before this API existed.
    pub(crate) fn account(&self) -> String {
        match self {
            KeyringSlot::DeviceSigningKey(store_id) => {
                format!("{DEVICE_SIGNING_KEY_BASE}:{store_id}")
            }
            KeyringSlot::EncryptionMasterKey(store_id) => {
                format!("{ENCRYPTION_MASTER_KEY_BASE}:{store_id}")
            }
            KeyringSlot::CloudHomeCredentials(store_id) => {
                format!("{CLOUD_HOME_CREDENTIALS_BASE}:{store_id}")
            }
            KeyringSlot::PendingIdentity(pending_public_key_hex) => {
                format!("{PENDING_IDENTITY_BASE}:{pending_public_key_hex}")
            }
            KeyringSlot::HostSecret { name, store_id } => format!("{name}:{store_id}"),
        }
    }
}

/// Reject a host secret name that would collide with, or otherwise misuse,
/// coven's own keyring account scheme: one of coven's own reserved names, the
/// empty string, or a name containing `:` (the scheme's separator — allowing
/// one would let a host secret's name forge another store's account). Called
/// at the API boundary before a [`KeyringSlot::HostSecret`] is ever built.
pub(crate) fn validate_host_secret_name(name: &str) -> Result<(), KeyError> {
    if name.is_empty() {
        return Err(KeyError::InvalidSecretName {
            name: name.to_string(),
            reason: "a host secret name must not be empty".to_string(),
        });
    }
    if name.contains(':') {
        return Err(KeyError::InvalidSecretName {
            name: name.to_string(),
            reason: "a host secret name must not contain ':', the keyring account scheme's \
                     separator"
                .to_string(),
        });
    }
    if RESERVED_HOST_SECRET_NAMES.contains(&name) {
        return Err(KeyError::InvalidSecretName {
            name: name.to_string(),
            reason: "reserved for coven's own keyring entries".to_string(),
        });
    }
    Ok(())
}

/// The sole entry-construction point for the OS keyring. Every read, write,
/// and delete delegates entry construction to the installed credential store,
/// so the platform configuration selected during registration applies to every
/// Coven key and host secret.
impl KeyringBackend {
    fn execute(&self, operation: KeyringOperation) {
        match operation {
            KeyringOperation::Read { account, reply } => {
                drop(reply.send(self.read(account)));
            }
            KeyringOperation::Write {
                account,
                value,
                reply,
            } => {
                drop(reply.send(self.write(account, value)));
            }
            KeyringOperation::Delete { account, reply } => {
                drop(reply.send(self.delete(account)));
            }
            #[cfg(all(
                any(test, feature = "test-utils"),
                any(target_os = "macos", target_os = "ios")
            ))]
            KeyringOperation::AppleEntryFacts { account, reply } => {
                drop(reply.send(self.apple_entry_facts(account)));
            }
            #[cfg(any(test, feature = "test-utils"))]
            KeyringOperation::SetNextError {
                account,
                error,
                reply,
            } => {
                drop(reply.send(self.set_next_error(account, error)));
            }
        }
    }

    fn entry(&self, account: &str) -> Result<keyring_core::Entry, KeyError> {
        self.store
            .build(&self.name, account, None)
            .map_err(map_keyring_error)
    }

    fn read(&self, account: String) -> Result<Option<String>, KeyError> {
        let entry = self.entry(&account)?;
        match entry.get_password() {
            Ok(password) if password.is_empty() => Err(KeyError::EmptyKeyringEntry { account }),
            Ok(password) => Ok(Some(password)),
            Err(keyring_core::Error::NoEntry) => Ok(None),
            Err(error) => Err(map_keyring_error(error)),
        }
    }

    fn write(&self, account: String, value: String) -> Result<(), KeyError> {
        self.entry(&account)?
            .set_password(&value)
            .map_err(map_keyring_error)
    }

    fn delete(&self, account: String) -> Result<bool, KeyError> {
        match self.entry(&account)?.delete_credential() {
            Ok(()) => Ok(true),
            Err(keyring_core::Error::NoEntry) => Ok(false),
            Err(error) => Err(map_keyring_error(error)),
        }
    }

    #[cfg(all(
        any(test, feature = "test-utils"),
        any(target_os = "macos", target_os = "ios")
    ))]
    fn apple_entry_facts(&self, account: String) -> Result<AppleKeyringEntryFacts, KeyError> {
        let entry = self.entry(&account)?;
        let credential = entry
            .as_any()
            .downcast_ref::<apple_native_keyring_store::protected::Cred>()
            .ok_or(KeyError::UnexpectedAppleKeyringEntry)?;
        Ok(AppleKeyringEntryFacts {
            access_policy: credential.access_policy.clone(),
            cloud_synchronize: credential.cloud_synchronize,
            service: credential.service.clone(),
            account: credential.account.clone(),
        })
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn set_next_error(&self, account: String, error: keyring_core::Error) -> Result<(), KeyError> {
        let entry = self.entry(&account)?;
        let credential = entry
            .as_any()
            .downcast_ref::<keyring_core::mock::Cred>()
            .ok_or(KeyError::UnexpectedTestKeyringEntry)?;
        credential.set_error(error);
        Ok(())
    }
}

impl KeyringService {
    fn new(
        name: String,
        store: std::sync::Arc<keyring_core::CredentialStore>,
    ) -> Result<Self, KeyError> {
        Ok(Self {
            name: name.clone(),
            worker: KeyringWorker::start(KeyringBackend { name, store })?,
        })
    }

    fn read(&self, slot: &KeyringSlot) -> Result<Option<String>, KeyError> {
        let account = slot.account();
        self.worker.execute("read a keyring entry", move |reply| {
            KeyringOperation::Read { account, reply }
        })
    }

    fn write(&self, slot: &KeyringSlot, value: &str) -> Result<(), KeyError> {
        let account = slot.account();
        let value = value.to_string();
        self.worker.execute("write a keyring entry", move |reply| {
            KeyringOperation::Write {
                account,
                value,
                reply,
            }
        })
    }

    fn delete(&self, slot: &KeyringSlot) -> Result<bool, KeyError> {
        let account = slot.account();
        self.worker.execute("delete a keyring entry", move |reply| {
            KeyringOperation::Delete { account, reply }
        })
    }

    #[cfg(all(
        any(test, feature = "test-utils"),
        any(target_os = "macos", target_os = "ios")
    ))]
    fn apple_entry_facts(&self, account: String) -> Result<AppleKeyringEntryFacts, KeyError> {
        self.worker
            .execute("inspect an Apple keyring entry", move |reply| {
                KeyringOperation::AppleEntryFacts { account, reply }
            })
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn set_next_error(
        &self,
        slot: &KeyringSlot,
        error: keyring_core::Error,
    ) -> Result<(), KeyError> {
        let account = slot.account();
        self.worker
            .execute("configure a test keyring entry", move |reply| {
                KeyringOperation::SetNextError {
                    account,
                    error,
                    reply,
                }
            })
    }
}

#[cfg(all(
    any(test, feature = "test-utils"),
    any(target_os = "macos", target_os = "ios")
))]
pub struct AppleKeyringEntryFacts {
    pub access_policy: apple_native_keyring_store::protected::AccessPolicy,
    pub cloud_synchronize: bool,
    pub service: String,
    pub account: String,
}

/// Test-only facts about the entry produced by the real Apple construction
/// boundary. The raw keyring entry remains inside the keys module.
#[cfg(all(
    any(test, feature = "test-utils"),
    any(target_os = "macos", target_os = "ios")
))]
pub fn apple_keyring_entry_facts_for_test(
    account: &str,
) -> Result<AppleKeyringEntryFacts, KeyError> {
    registered_keyring()?.apple_entry_facts(account.to_string())
}

/// This store's established signing identity through `custody`, or
/// [`KeyError::NoDeviceIdentity`] when none is established — the caller must
/// complete create/join/restore for this store first. Never mints: a
/// connect/join precondition, not a query.
pub fn require_identity(custody: &dyn DeviceIdentityCustody) -> Result<UserKeypair, KeyError> {
    custody.unlock()?.ok_or(KeyError::NoDeviceIdentity)
}

/// Mint a fresh identity for a device-pairing attempt that has not joined a
/// store yet. The joiner signs its pairing request with this keypair and holds it
/// under a pending slot keyed by its own public key. The join establishes it
/// in the joined store's own identity custody (via
/// [`DeviceIdentityCustody::establish`],
/// before the store's completion marker) and discards the pending slot only
/// once the whole join succeeds; [`discard_pending_identity`] also removes it
/// if the pairing is abandoned instead. Always the OS keyring: unlike an
/// established store's identity, there is no store yet to select a custody
/// policy for, and a pending identity's lifetime is short (a join round trip,
/// not a store's lifetime).
pub fn mint_pending_identity() -> Result<UserKeypair, KeyError> {
    let keypair = UserKeypair::generate();
    registered_keyring()?.write(
        &KeyringSlot::PendingIdentity(public_key_hex(&keypair)),
        &hex::encode(keypair.to_keypair_bytes()),
    )?;
    info!("Minted a pending identity for device pairing");
    Ok(keypair)
}

/// Read (without consuming) the pending identity keyed by
/// `pending_public_key_hex` — what a pairing in progress signs its bootstrap
/// traffic with, and what it establishes in the store's own custody before
/// the completion marker. [`KeyError::NoPendingIdentity`] if none is held
/// under that key.
pub fn peek_pending_identity(pending_public_key_hex: &str) -> Result<UserKeypair, KeyError> {
    read_pending_identity_slot(&KeyringSlot::PendingIdentity(
        pending_public_key_hex.to_string(),
    ))
}

fn read_pending_identity_slot(slot: &KeyringSlot) -> Result<UserKeypair, KeyError> {
    let KeyringSlot::PendingIdentity(pending_public_key_hex) = slot else {
        unreachable!("read_pending_identity_slot is only ever called with a PendingIdentity slot");
    };
    let sk_hex = registered_keyring()?
        .read(slot)?
        .ok_or_else(|| KeyError::NoPendingIdentity {
            pending_public_key_hex: pending_public_key_hex.clone(),
        })?;
    let signing_key = hex::decode(&sk_hex).map_err(|source| KeyError::Hex {
        subject: "pending identity",
        source,
    })?;
    let actual = signing_key.len();
    let signing_key: [u8; SIGN_SECRETKEYBYTES] =
        signing_key
            .try_into()
            .map_err(|_| KeyError::InvalidLength {
                subject: "pending identity",
                expected: SIGN_SECRETKEYBYTES,
                actual,
            })?;
    UserKeypair::from_signing_key_bytes(&signing_key)
}

/// Discard the pending identity keyed by `pending_public_key_hex` — a pairing
/// abandoned without completing, or one whose identity the completed
/// join has already established in the store's own custody. `Ok` whether or
/// not one was pending.
pub fn discard_pending_identity(pending_public_key_hex: &str) -> Result<(), KeyError> {
    registered_keyring()?
        .delete(&KeyringSlot::PendingIdentity(
            pending_public_key_hex.to_string(),
        ))
        .map(|_| ())
}

/// One store's key material: the encryption master key, cloud-home credentials,
/// and OAuth tokens, each stored under a store-scoped keyring account
/// (`{base}:{store_id}`). The store's signing identity is not here — it goes
/// through [`crate::identity_custody::IdentityCustody`], the same way the
/// master key goes through [`crate::custody::KeyCustody`].
#[derive(Clone)]
pub struct StoreKeys {
    keyring: KeyringBinding,
    store_id: String,
}

impl StoreKeys {
    pub fn bind(store_id: String) -> Self {
        let keyring = match KEYRING_SERVICE.get() {
            Some(service) => KeyringBinding::Registered(service),
            None => KeyringBinding::Unregistered,
        };
        Self { keyring, store_id }
    }

    pub fn store_id(&self) -> &str {
        &self.store_id
    }

    pub fn get_encryption_key(&self) -> Result<Option<String>, KeyError> {
        self.keyring
            .service()?
            .read(&KeyringSlot::EncryptionMasterKey(self.store_id.clone()))
    }

    pub fn set_encryption_key(&self, value: &str) -> Result<(), KeyError> {
        self.keyring.service()?.write(
            &KeyringSlot::EncryptionMasterKey(self.store_id.clone()),
            value,
        )?;
        info!("Encryption key saved to keyring");
        Ok(())
    }

    pub fn delete_encryption_key(&self) -> Result<(), KeyError> {
        if self
            .keyring
            .service()?
            .delete(&KeyringSlot::EncryptionMasterKey(self.store_id.clone()))?
        {
            info!("Encryption key deleted from keyring");
        }
        Ok(())
    }

    pub fn get_cloud_home_credentials(&self) -> Result<Option<CloudHomeCredentials>, KeyError> {
        match self
            .keyring
            .service()?
            .read(&KeyringSlot::CloudHomeCredentials(self.store_id.clone()))?
        {
            None => Ok(None),
            Some(j) => serde_json::from_str(&j)
                .map(Some)
                .map_err(|source| KeyError::Json {
                    operation: "parse cloud home credentials JSON",
                    source,
                }),
        }
    }

    pub fn set_cloud_home_credentials(&self, creds: &CloudHomeCredentials) -> Result<(), KeyError> {
        let json = serde_json::to_string(creds).map_err(|source| KeyError::Json {
            operation: "serialize cloud home credentials",
            source,
        })?;
        self.keyring.service()?.write(
            &KeyringSlot::CloudHomeCredentials(self.store_id.clone()),
            &json,
        )?;
        info!("Cloud home credentials saved to keyring");
        Ok(())
    }

    #[cfg(feature = "oauth-providers")]
    pub fn get_cloud_home_oauth_tokens(
        &self,
    ) -> Result<Option<crate::keys::OAuthTokens>, KeyError> {
        Ok(match self.get_cloud_home_credentials()? {
            Some(CloudHomeCredentials::OAuth { tokens }) => Some(tokens),
            _ => None,
        })
    }

    #[cfg(feature = "oauth-providers")]
    pub fn set_cloud_home_oauth_tokens(
        &self,
        tokens: &crate::keys::OAuthTokens,
    ) -> Result<(), KeyError> {
        self.set_cloud_home_credentials(&CloudHomeCredentials::OAuth {
            tokens: tokens.clone(),
        })
    }

    pub fn delete_cloud_home_credentials(&self) -> Result<(), KeyError> {
        if self
            .keyring
            .service()?
            .delete(&KeyringSlot::CloudHomeCredentials(self.store_id.clone()))?
        {
            info!("Cloud home credentials deleted from keyring");
        }
        Ok(())
    }

    fn host_secret_slot(&self, name: &str) -> KeyringSlot {
        KeyringSlot::HostSecret {
            name: name.to_string(),
            store_id: self.store_id.clone(),
        }
    }

    /// A host's own store-scoped secret — an API token, a service credential
    /// — read from the same keyring service and access policy as coven's own
    /// key material. `None` if never set. [`KeyError::InvalidSecretName`] if
    /// `name` collides with one of coven's own reserved slot names, is
    /// empty, or contains `:` (see `validate_host_secret_name`).
    pub fn get_host_secret(&self, name: &str) -> Result<Option<String>, KeyError> {
        validate_host_secret_name(name)?;
        self.keyring.service()?.read(&self.host_secret_slot(name))
    }

    /// Set a host's own store-scoped secret. Same name restrictions as
    /// [`get_host_secret`](Self::get_host_secret).
    pub fn set_host_secret(&self, name: &str, value: &str) -> Result<(), KeyError> {
        validate_host_secret_name(name)?;
        self.keyring
            .service()?
            .write(&self.host_secret_slot(name), value)?;
        info!("Host secret {name:?} saved to keyring");
        Ok(())
    }

    /// Remove a host secret. `Ok` whether or not one was set. Same name
    /// restrictions as [`get_host_secret`](Self::get_host_secret).
    pub fn delete_host_secret(&self, name: &str) -> Result<(), KeyError> {
        validate_host_secret_name(name)?;
        if self
            .keyring
            .service()?
            .delete(&self.host_secret_slot(name))?
        {
            info!("Host secret {name:?} deleted from keyring");
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn write_empty_encryption_key_for_test(&self) -> Result<(), KeyError> {
        self.keyring
            .service()?
            .write(&KeyringSlot::EncryptionMasterKey(self.store_id.clone()), "")
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn write_cloud_home_credentials_json_for_test(&self, json: &str) -> Result<(), KeyError> {
        self.keyring.service()?.write(
            &KeyringSlot::CloudHomeCredentials(self.store_id.clone()),
            json,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn fail_next_cloud_home_credentials_operation_for_test(
        &self,
        error: keyring_core::Error,
    ) -> Result<(), KeyError> {
        self.keyring.service()?.set_next_error(
            &KeyringSlot::CloudHomeCredentials(self.store_id.clone()),
            error,
        )
    }
}

impl DeviceIdentityCustody for StoreKeys {
    fn unlock(&self) -> Result<Option<UserKeypair>, KeyError> {
        let slot = KeyringSlot::DeviceSigningKey(self.store_id.clone());
        let Some(signing_key_hex) = self.keyring.service()?.read(&slot)? else {
            return Ok(None);
        };
        let signing_key = hex::decode(&signing_key_hex).map_err(|source| KeyError::Hex {
            subject: "signing key",
            source,
        })?;
        let actual = signing_key.len();
        let signing_key: [u8; SIGN_SECRETKEYBYTES] =
            signing_key
                .try_into()
                .map_err(|_| KeyError::InvalidLength {
                    subject: "signing key",
                    expected: SIGN_SECRETKEYBYTES,
                    actual,
                })?;
        Ok(Some(UserKeypair::from_signing_key_bytes(&signing_key)?))
    }

    fn persist(&self, keypair: &UserKeypair) -> Result<(), KeyError> {
        self.keyring.service()?.write(
            &KeyringSlot::DeviceSigningKey(self.store_id.clone()),
            &hex::encode(keypair.to_keypair_bytes()),
        )
    }

    fn forget(&self) -> Result<(), KeyError> {
        self.keyring
            .service()?
            .delete(&KeyringSlot::DeviceSigningKey(self.store_id.clone()))
            .map(|_| ())
    }
}

/// Installs keyring-core's in-memory mock store and registers the key service
/// against it, once per process. Every crate that tests against the key
/// service uses this rather than mirroring the mock, so no test reaches the
/// real OS keychain.
#[cfg(any(test, feature = "test-utils", debug_assertions))]
pub mod test_keyring {
    use std::sync::Once;

    static INSTALL: Once = Once::new();

    pub fn install() {
        install_for_service("coven-tests").expect("register test keyring service");
    }

    pub fn install_for_service(service_name: &str) -> Result<(), super::KeyError> {
        INSTALL.call_once(|| {
            // Install the in-memory mock before registering the service so
            // `set_keyring_service` keeps it instead of reaching for the OS
            // keychain — a platform mechanism these tests never touch.
            keyring_core::set_default_store(
                keyring_core::mock::Store::new().expect("create mock keyring store"),
            );
        });
        super::set_keyring_service(service_name)
    }
}

#[cfg(test)]
#[path = "platform_tests.rs"]
mod tests;
